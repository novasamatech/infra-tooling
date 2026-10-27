#!/usr/bin/env python3

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""
Prometheus exporter for Cloudflare HTTP request analytics.

Every cycle the exporter lists all zones visible to the API token and queries
httpRequestsAdaptiveGroups for the current UTC day (midnight to now), splitting
full pages into disjoint time slices. Results are published as one snapshot, so a
scrape never observes a half-finished cycle, and a failing zone or a Cloudflare
outage never stops the exporter.
"""

import logging
import math
import os
import signal
import sys
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import partial
from itertools import pairwise
from socketserver import ThreadingMixIn
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

import requests
from prometheus_client import REGISTRY, make_wsgi_app
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.registry import Collector
from requests.adapters import HTTPAdapter
from urllib3.exceptions import MaxRetryError, ResponseError
from urllib3.util.retry import Retry

API_BASE_URL = "https://api.cloudflare.com/client/v4"
ZONES_PER_PAGE = 50  # maximum allowed by GET /zones
# Maximum page size of httpRequestsAdaptiveGroups (settings.maxPageSize).
GROUPS_LIMIT = 10000
MIN_SLICE_SECONDS = 60
# Analytics requests allowed per selection, zone and cycle, so that one zone
# with a burst of unique groups cannot use up the cycle budget below.
MAX_SLICE_REQUESTS = 32
# Cloudflare's documented GraphQL quota is 300 queries per 5 minutes per user,
# where every selection (node) of every zone in a request counts as one query.
GRAPHQL_QUOTA = 300
GRAPHQL_QUOTA_WINDOW_SECONDS = 300
# Share of that quota one cycle may plan for, leaving room for retries and for
# other clients of the same user.
GRAPHQL_QUOTA_SHARE = 2 / 3
# Selections each zone needs per cycle: settings, four datasets and totals.
BASELINE_SELECTIONS = 6
# The cycle before UTC midnight is moved earlier when it would not finish this
# many seconds before midnight, so the day's final minutes are still exported.
DAY_CLOSE_MARGIN_SECONDS = 30
# A cycle due after UTC midnight starts this long after it instead. Its window,
# truncated to whole seconds, is still empty and needs no analytics query, and
# a slightly early wake-up cannot repeat the finished day.
NEW_DAY_START_SECONDS = 0.5
# /readyz requires snapshot ages below this many collection intervals.
READINESS_MAX_AGE_INTERVALS = 3
# Longest Retry-After worth waiting for inside a cycle. Cloudflare's rate limits
# reset over 5 minutes; waiting that long would stall every remaining zone, so
# such a response fails at once and the next cycle tries again.
MAX_RETRY_AFTER_SECONDS = 30
UNKNOWN = "unknown"
UINT16_MAX = (1 << 16) - 1

# Prometheus label -> httpRequestsAdaptiveGroups dimension, in label order.
VISITS_DIMENSIONS = {
    "host_name": "clientRequestHTTPHost",
    "path": "clientRequestPath",
    "client_country_name": "clientCountryName",
    "client_request_referer": "clientRequestReferer",
    "user_agent_browser": "userAgentBrowser",
    "user_agent_os": "userAgentOS",
}
REQUESTS_DIMENSIONS = {
    "host_name": "clientRequestHTTPHost",
    "method_name": "clientRequestHTTPMethodName",
    "path": "clientRequestPath",
    "query": "clientRequestQuery",
    "client_country_name": "clientCountryName",
    "client_request_referer": "clientRequestReferer",
    "user_agent_browser": "userAgentBrowser",
    "user_agent_os": "userAgentOS",
    "cache_status": "cacheStatus",
    "origin_response_status": "originResponseStatus",
}
CACHE_DIMENSIONS = {
    label: REQUESTS_DIMENSIONS[label]
    for label in ("host_name", "method_name", "path", "cache_status", "origin_response_status")
}
EDGE_DIMENSIONS = {
    "host_name": "clientRequestHTTPHost",
    "method_name": "clientRequestHTTPMethodName",
    "path": "clientRequestPath",
    "edge_response_status": "edgeResponseStatus",
    "origin_response_status": "originResponseStatus",
}
VISITS_LABELS = ("zone_name", *VISITS_DIMENSIONS)
REQUESTS_LABELS = ("zone_name", *REQUESTS_DIMENSIONS)
CACHE_LABELS = ("zone_name", *CACHE_DIMENSIONS)
EDGE_LABELS = ("zone_name", *EDGE_DIMENSIONS)
DATASETS = {
    "visits": VISITS_DIMENSIONS,
    "requests": REQUESTS_DIMENSIONS,
    "cache": CACHE_DIMENSIONS,
    "edge": EDGE_DIMENSIONS,
}
# Fields the query needs regardless of plan (names as in availableFields).
REQUIRED_FIELDS = frozenset({"count", "sum_visits"})
# Restricts a selection to requests from clients, excluding Cloudflare's own
# subrequests (e.g. from Workers). Only the edge dataset uses it; the other
# datasets keep counting every request source.
EYEBALL_FILTER = 'requestSource: "eyeball"'
FILTERED_DATASETS = {"edge": EYEBALL_FILTER}
# Fields without which a dataset is not collected for the zone at all, rather
# than reported as "unknown" or from unfiltered traffic.
DATASET_FIELDS = {
    "edge": frozenset({"dimensions_edgeResponseStatus", "dimensions_requestSource"}),
}
# Strict response types are dataset-specific: older families retain their
# existing label conversion policy even when they query the same dimensions.
DIMENSION_TYPES = {
    "edge": {
        "edgeResponseStatus": int,
        "originResponseStatus": int,
        "clientRequestHTTPHost": str,
        "clientRequestHTTPMethodName": str,
        "clientRequestPath": str,
    },
}

# Which fields a zone may query depends on its plan (e.g. Free zones lack
# clientRequestQuery and clientRequestReferer), so it is checked every cycle.
AVAILABLE_FIELDS_QUERY = """
query AvailableFields($zoneTag: string!) {
  viewer {
    zones(filter: {zoneTag: $zoneTag}) {
      settings {
        httpRequestsAdaptiveGroups {
          enabled
          availableFields
          maxPageSize
        }
      }
    }
  }
}
"""

LabelValues = tuple[str, ...]


class ConfigError(Exception):
    """Invalid exporter configuration."""


class CloudflareAPIError(Exception):
    """The Cloudflare API failed or returned an unusable response."""


class RateLimitedError(CloudflareAPIError):
    """Cloudflare rejected a request for its rate limit; later ones in this cycle would fail too."""


def cycle_query_budget(scrape_interval: float) -> int:
    """GraphQL selections one cycle may use: a share of the quota, scaled to the interval."""
    window = min(scrape_interval, GRAPHQL_QUOTA_WINDOW_SECONDS)
    return int(GRAPHQL_QUOTA * GRAPHQL_QUOTA_SHARE * window / GRAPHQL_QUOTA_WINDOW_SECONDS)


def _is_budget_error(errors) -> bool:
    """Cloudflare's 'rate limiter budget depleted', in case it comes without HTTP 429."""
    for error in errors if isinstance(errors, list) else [errors]:
        if not isinstance(error, dict):
            continue
        extensions = error.get("extensions")
        code = extensions.get("code") if isinstance(extensions, dict) else None
        if code == "budget" or "budget depleted" in str(error.get("message", "")).lower():
            return True
    return False


class BoundedRetry(Retry):
    """Retry that gives up at once when the server asks to wait too long."""

    def increment(
        self, method=None, url=None, response=None, error=None, _pool=None, _stacktrace=None
    ):
        retry_after = self.get_retry_after(response) if response is not None else None
        if retry_after is not None and retry_after > MAX_RETRY_AFTER_SECONDS:
            # With raise_on_status=False, urllib3 returns this response as is.
            raise MaxRetryError(
                _pool,
                url,
                ResponseError(f"Retry-After {retry_after:.0f}s exceeds {MAX_RETRY_AFTER_SECONDS}s"),
            )
        return super().increment(method, url, response, error, _pool, _stacktrace)


@dataclass(frozen=True)
class Config:
    api_token: str = field(repr=False)
    request_timeout: int
    scrape_interval: int
    metrics_port: int
    group_limit: int = 0


@dataclass(frozen=True)
class ZoneMetrics:
    visits: dict[LabelValues, float]
    requests: dict[LabelValues, float]
    cache: dict[LabelValues, float] = field(default_factory=dict)
    total_requests: float = 0
    total_visits: float = 0
    limited: dict[str, int] = field(default_factory=dict)
    edge: dict[LabelValues, float] = field(default_factory=dict)
    # Whether the zone's plan supports each dataset (1) or it was skipped (0).
    available: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class DatasetSettings:
    fields: frozenset[str]
    max_page_size: int


@dataclass(frozen=True)
class Snapshot:
    """Everything one collection cycle produced; replaced as a whole."""

    visits: dict[LabelValues, float] = field(default_factory=dict)
    requests: dict[LabelValues, float] = field(default_factory=dict)
    cache: dict[LabelValues, float] = field(default_factory=dict)
    edge: dict[LabelValues, float] = field(default_factory=dict)
    zone_requests: dict[str, float] = field(default_factory=dict)
    zone_visits: dict[str, float] = field(default_factory=dict)
    groups: dict[LabelValues, int] = field(default_factory=dict)
    limited: dict[LabelValues, int] = field(default_factory=dict)
    available: dict[LabelValues, int] = field(default_factory=dict)
    window_start: datetime | None = None
    window_end: datetime | None = None
    zone_success: dict[str, int] = field(default_factory=dict)
    # None until the first cycle has finished.
    cycle_success: int | None = None
    cycle_timestamp: float | None = None
    cycle_duration: float | None = None


def format_errors(errors) -> str:
    """Render a Cloudflare REST or GraphQL `errors` array as one line."""
    if not errors:
        return "unknown error"
    if not isinstance(errors, list):
        errors = [errors]
    parts = []
    for error in errors:
        if not isinstance(error, dict):
            parts.append(str(error))
            continue
        message = str(error.get("message") or "unknown error")
        extensions = error.get("extensions")
        code = error.get("code") or (
            extensions.get("code") if isinstance(extensions, dict) else None
        )
        parts.append(f"{message} ({code})" if code else message)
    return "; ".join(parts)


class CloudflareAPI:
    """
    Thin client for the Cloudflare REST and GraphQL APIs.
    """

    def __init__(
        self,
        api_token: str,
        request_timeout: int = 30,
        base_url: str = API_BASE_URL,
        group_limit: int = 0,
        query_budget: int | None = None,
    ):
        api_token = _validate_token(api_token)
        self.base_url = base_url.rstrip("/")
        self.request_timeout = request_timeout
        self.group_limit = group_limit
        self.slice_hints: dict[tuple[str, str], int] = {}
        self.hint_day: datetime | None = None
        self.query_budget = (
            cycle_query_budget(GRAPHQL_QUOTA_WINDOW_SECONDS)
            if query_budget is None
            else query_budget
        )
        # Extra slice selections still allowed in this cycle; see start_cycle().
        self.slice_allowance = self.query_budget
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {api_token}",
                "Content-Type": "application/json",
                "User-Agent": "cf-exporter",
            }
        )
        # Retry rate limiting and transient server errors with backoff. POST is
        # included because the exporter only POSTs read-only GraphQL queries.
        # A Retry-After above MAX_RETRY_AFTER_SECONDS is not waited for.
        retry = BoundedRetry(
            total=3,
            backoff_factor=2,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "POST"}),
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))

    def start_cycle(self, zones: int) -> None:
        """Share the cycle's budget: every zone keeps its baseline selections,
        and slicing across all zones may use only what is left."""
        self.slice_allowance = max(0, self.query_budget - zones * BASELINE_SELECTIONS)

    def _redact(self, message: str) -> str:
        token = self.session.headers["Authorization"].removeprefix("Bearer ")
        return message.replace(token, "[REDACTED]")

    def _request(self, method: str, path: str, **kwargs) -> dict:
        try:
            response = self.session.request(
                method, f"{self.base_url}{path}", timeout=self.request_timeout, **kwargs
            )
        except requests.RequestException as e:
            raise CloudflareAPIError(self._redact(f"{method} {path}: {e}")) from None
        try:
            payload = response.json()
        except ValueError:
            payload = None
        if response.status_code == 429:
            errors = payload.get("errors") if isinstance(payload, dict) else None
            raise RateLimitedError(
                self._redact(
                    f"{method} {path}: HTTP 429: "
                    f"{format_errors(errors) if errors else 'rate limited'}"
                )
            )
        if payload is None:
            raise CloudflareAPIError(
                f"{method} {path}: HTTP {response.status_code}, response is not JSON"
            )
        if not isinstance(payload, dict):
            raise CloudflareAPIError(f"{method} {path}: unexpected response (expected an object)")
        if not response.ok or payload.get("success") is False:
            raise CloudflareAPIError(
                self._redact(
                    f"{method} {path}: HTTP {response.status_code}: "
                    f"{format_errors(payload.get('errors'))}"
                )
            )
        return payload

    def list_zones(self) -> list[dict]:
        """List all zones (paginated) using the REST API."""
        zones = []
        page = 1
        while True:
            payload = self._request(
                "GET",
                "/zones",
                params={
                    "page": page,
                    "per_page": ZONES_PER_PAGE,
                    "order": "name",
                    "direction": "asc",
                },
            )
            result, info = payload.get("result"), payload.get("result_info")
            if payload.get("success") is not True or not isinstance(result, list):
                raise CloudflareAPIError("GET /zones: response contains no valid zone list")
            if not isinstance(info, dict):
                raise CloudflareAPIError("GET /zones: response contains no pagination information")
            total_pages = info.get("total_pages")
            if (
                type(total_pages) is not int
                or total_pages < 0
                or (total_pages == 0 and (result or page > 1))
                or info.get("page", page) != page
                or (page < total_pages and not result)
            ):
                raise CloudflareAPIError("GET /zones: invalid pagination information")
            if any(
                not isinstance(zone, dict)
                or not isinstance(zone.get("id"), str)
                or not zone["id"]
                or not isinstance(zone.get("name"), str)
                or not zone["name"]
                for zone in result
            ):
                raise CloudflareAPIError("GET /zones: zone without a valid id or name")
            zones.extend(result)
            if page >= total_pages:
                return zones
            page += 1

    def graphql(self, query: str, variables: Mapping) -> dict:
        """Execute a GraphQL query and return its `data`; any error raises."""
        payload = self._request(
            "POST", "/graphql", json={"query": query, "variables": dict(variables)}
        )
        if payload.get("errors"):
            message = self._redact(f"GraphQL: {format_errors(payload['errors'])}")
            if _is_budget_error(payload["errors"]):
                raise RateLimitedError(message)
            raise CloudflareAPIError(message)
        data = payload.get("data")
        if not isinstance(data, dict):
            raise CloudflareAPIError("GraphQL: response contains no data")
        return data


def day_window(now: datetime) -> tuple[datetime, datetime]:
    """Return [midnight UTC, now) truncated to whole seconds."""
    until = now.astimezone(UTC).replace(microsecond=0)
    return until.replace(hour=0, minute=0, second=0), until


def rfc3339(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_analytics_query(
    visits_dimensions: list[str],
    requests_dimensions: list[str],
    cache_dimensions: Sequence[str] = (),
    edge_dimensions: Sequence[str] = (),
    datasets: tuple[str, ...] = (*DATASETS, "totals"),
) -> str:
    """Build an aliased query for just the selections needed by this window."""
    dimensions = dict(
        zip(
            DATASETS,
            (visits_dimensions, requests_dimensions, cache_dimensions, edge_dimensions),
            strict=True,
        )
    )
    selections = []
    for kind in datasets:
        totals = kind == "totals"
        fields = dimensions.get(kind, ())
        selection = "dimensions { " + " ".join(fields) + " }" if fields else ""
        value = (
            "count sum { visits }"
            if totals
            else ("sum { visits }" if kind == "visits" else "count")
        )
        limit = "1" if totals else "$limit"
        order = "sum_visits_DESC" if kind == "visits" else "count_DESC"
        extra = f", {FILTERED_DATASETS[kind]}" if kind in FILTERED_DATASETS else ""
        selections.append(f"""
      {kind}: httpRequestsAdaptiveGroups(
        limit: {limit}
        filter: {{datetime_geq: $since, datetime_lt: $until{extra}}}
        orderBy: [{order}]
      ) {{ {value} {selection} }}""")
    # GraphQL rejects declarations of unused variables (e.g. a totals-only query).
    limit_variable = ", $limit: uint64!" if any(k != "totals" for k in datasets) else ""
    return f"""
query HttpRequests($zoneTag: string!, $since: Time!, $until: Time!{limit_variable}) {{
  viewer {{
    zones(filter: {{zoneTag: $zoneTag}}) {{
      {"".join(selections)}
    }}
  }}
}}
"""


def zone_node(data: dict, zone_id: str) -> dict:
    """Return the single requested zone node or raise CloudflareAPIError."""
    viewer = data.get("viewer")
    zones = viewer.get("zones") if isinstance(viewer, dict) else None
    if not isinstance(zones, list) or len(zones) != 1 or not isinstance(zones[0], dict):
        raise CloudflareAPIError(f"GraphQL: no data returned for zone {zone_id}")
    return zones[0]


def fetch_dataset_settings(api: CloudflareAPI, zone_id: str) -> DatasetSettings:
    """Return the dataset fields and page size the zone's plan allows."""
    data = api.graphql(AVAILABLE_FIELDS_QUERY, {"zoneTag": zone_id})
    node = zone_node(data, zone_id).get("settings")
    settings = node.get("httpRequestsAdaptiveGroups") if isinstance(node, dict) else None
    if not isinstance(settings, dict) or settings.get("enabled") is not True:
        raise CloudflareAPIError("httpRequestsAdaptiveGroups is not enabled for the zone")
    fields = settings.get("availableFields")
    page_size = settings.get("maxPageSize")
    if not isinstance(fields, list) or any(not isinstance(f, str) for f in fields):
        raise CloudflareAPIError("httpRequestsAdaptiveGroups returned invalid availableFields")
    if type(page_size) is not int or page_size < 1:
        raise CloudflareAPIError("httpRequestsAdaptiveGroups returned invalid maxPageSize")
    return DatasetSettings(frozenset(fields), page_size)


def fetch_available_fields(api: CloudflareAPI, zone_id: str) -> frozenset[str]:
    """Return the httpRequestsAdaptiveGroups fields the zone's plan allows."""
    return fetch_dataset_settings(api, zone_id).fields


def _label_values(
    zone_name: str,
    dimension_map: Mapping[str, str],
    dimensions: Mapping,
    available: list[str],
    label_pool: dict[str, str] | None = None,
) -> LabelValues:
    values = [zone_name]
    for label, dimension in dimension_map.items():
        # Dimensions the zone's plan does not offer are not queried at all.
        value = dimensions.get(dimension) if dimension in available else None
        value = UNKNOWN if value is None else str(value)
        if label == "client_request_referer" and value == "":
            value = "direct"
        # Share repeated hosts, paths, countries and user agents within this
        # zone's collection. Unlike sys.intern(), this pool has a bounded life.
        values.append(label_pool.setdefault(value, value) if label_pool is not None else value)
    return tuple(values)


def checked_value(kind: str, group, value_of: Callable[[dict], float | None]) -> float:
    """Return a group's count or visits, rejecting anything but a finite number >= 0."""
    if not isinstance(group, dict):
        raise CloudflareAPIError(f"GraphQL: {kind} contains an invalid group")
    try:
        value = value_of(group)
    except (AttributeError, TypeError):
        value = None
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise CloudflareAPIError(f"GraphQL: {kind} contains an invalid count")
    return value


def group_list(kind: str, groups) -> list:
    """Return a group list, rejecting missing or non-list data with CloudflareAPIError."""
    if not isinstance(groups, list):
        raise CloudflareAPIError(f"GraphQL: {kind} contains no valid group list")
    return groups


def _group_dimensions(kind: str, group: dict, available: list[str]) -> dict:
    dimensions = group.get("dimensions", {})
    if not isinstance(dimensions, dict):
        raise CloudflareAPIError(f"GraphQL: {kind} contains invalid dimensions")
    types = DIMENSION_TYPES.get(kind, {})
    required_fields = DATASET_FIELDS.get(kind, ())
    if types or required_fields:
        for dimension in available:
            value = dimensions.get(dimension)
            if value is None:
                if f"dimensions_{dimension}" in required_fields:
                    raise CloudflareAPIError(
                        f"GraphQL: {kind} contains a group without {dimension}"
                    )
                continue
            expected_type = types.get(dimension)
            if expected_type is not None and (
                type(value) is not expected_type
                or (expected_type is int and not 0 <= value <= UINT16_MAX)
            ):
                raise CloudflareAPIError(f"GraphQL: {kind} contains invalid {dimension}")
    return dimensions


def _aggregate(
    zone_name: str,
    groups: list[dict],
    dimension_map: Mapping[str, str],
    available: list[str],
    value_of: Callable[[dict], float | None],
    label_pool: dict[str, str] | None = None,
) -> dict[LabelValues, float]:
    """Aggregate previously validated groups without repeating response checks."""
    samples: dict[LabelValues, float] = {}
    for group in groups:
        value = value_of(group)
        dimensions = group.get("dimensions", {})
        if not value:
            continue
        labels = _label_values(zone_name, dimension_map, dimensions, available, label_pool)
        samples[labels] = samples.get(labels, 0) + value
    return samples


def _windows(since: datetime, until: datetime, count: int) -> list[tuple[datetime, datetime]]:
    """Disjoint minute-aligned slices; only the trailing slice can be under 60s."""
    seconds = int((until - since).total_seconds())
    minutes = (seconds + MIN_SLICE_SECONDS - 1) // MIN_SLICE_SECONDS
    count = min(count, minutes)
    edges = [
        since + timedelta(seconds=min(seconds, i * minutes // count * MIN_SLICE_SECONDS))
        for i in range(count + 1)
    ]
    return list(pairwise(edges))


def collect_zone(
    api: CloudflareAPI,
    zone_id: str,
    zone_name: str,
    since: datetime,
    until: datetime,
    limit: int | None = None,
) -> ZoneMetrics:
    """Retrieve complete groups, retrying full selections over smaller windows."""
    settings = fetch_dataset_settings(api, zone_id)
    fields = settings.fields
    limit = api.group_limit if limit is None else limit
    max_page = min(GROUPS_LIMIT, settings.max_page_size)
    # A cap within one page is served by a single query. One extra group, where
    # the plan allows it, shows whether the cap actually cut anything.
    single_page = bool(limit and limit <= max_page)
    page_size = min(limit + 1, max_page) if single_page else max_page
    missing = REQUIRED_FIELDS - fields
    if missing:
        raise CloudflareAPIError(f"fields not available for the zone: {', '.join(sorted(missing))}")
    # A dataset the plan cannot serve is skipped explicitly; the others are collected.
    supported = {kind: int(DATASET_FIELDS.get(kind, frozenset()) <= fields) for kind in DATASETS}
    for kind, ok in supported.items():
        if not ok:
            logging.warning(
                "Zone %s: plan does not offer %s; the %s dataset is not collected",
                zone_name,
                ", ".join(sorted(DATASET_FIELDS[kind] - fields)),
                kind,
            )
    collected = tuple(kind for kind, ok in supported.items() if ok)
    if api.hint_day != since:
        api.slice_hints.clear()
        api.hint_day = since
    if since == until:
        return ZoneMetrics(
            visits={}, requests={}, limited=dict.fromkeys(collected, 0), available=supported
        )

    def available(dimension_map: Mapping[str, str]) -> list[str]:
        return [d for d in dimension_map.values() if f"dimensions_{d}" in fields]

    dimensions = {kind: available(mapping) for kind, mapping in DATASETS.items()}
    unavailable = sorted(
        {d for mapping in DATASETS.values() for d in mapping.values()}
        - {d for kind in collected for d in dimensions[kind]}
    )
    if unavailable:
        logging.debug("Zone %s: plan does not offer %s", zone_name, ", ".join(unavailable))

    def fetch(kinds: tuple[str, ...], start: datetime, end: datetime) -> dict:
        variables = {
            "zoneTag": zone_id,
            "since": rfc3339(start),
            "until": rfc3339(end),
        }
        if any(k != "totals" for k in kinds):
            variables["limit"] = page_size
        query = build_analytics_query(*dimensions.values(), datasets=kinds)
        return zone_node(api.graphql(query, variables), zone_id)

    def value_of(kind: str, group: dict) -> float | None:
        return (group.get("sum") or {}).get("visits") if kind == "visits" else group.get("count")

    samples: dict[str, dict[LabelValues, float]] = {kind: {} for kind in collected}
    label_pool: dict[str, str] = {}
    # Why each selection is incomplete. Decided from the pages themselves: a
    # separately fetched total can differ from the slices for the current day.
    cuts: dict[str, str] = {}
    leaves = dict.fromkeys(collected, 0)
    budget = dict.fromkeys(collected, MAX_SLICE_REQUESTS)
    scheduled: dict[tuple[datetime, datetime], list[str]] = {(since, until): ["totals"]}
    for kind in collected:
        hint = 1 if single_page else api.slice_hints.get((zone_id, kind), 1)
        count = min(hint, MAX_SLICE_REQUESTS, 1 + api.slice_allowance)
        windows = _windows(since, until, count)
        budget[kind] -= len(windows)
        api.slice_allowance -= len(windows) - 1
        for window in windows:
            scheduled.setdefault(window, []).append(kind)

    def merge(node: dict, kinds: tuple[str, ...], start: datetime, end: datetime) -> None:
        split = []
        seconds = (end - start).total_seconds()
        for kind in kinds:
            groups = group_list(kind, node.pop(kind, None))
            read = partial(value_of, kind)
            # Validate once, including parent/probe groups that will not be aggregated.
            # A malformed response is not proof of a complete or empty dataset.
            for group in groups:
                checked_value(kind, group, read)
                _group_dimensions(kind, group, dimensions[kind])
            # Ordered largest first: a page cut nothing if its last group is zero.
            full = len(groups) >= page_size and read(groups[-1]) > 0
            if (
                full
                and not single_page
                and seconds > MIN_SLICE_SECONDS
                and budget[kind] >= 2
                and api.slice_allowance >= 2
            ):
                budget[kind] -= 2
                api.slice_allowance -= 2
                split.append(kind)
                continue  # Discard the validated parent page; only its children count.
            leaves[kind] += 1
            if full:
                cuts.setdefault(
                    kind,
                    "configured cap"
                    if single_page
                    else "60s slice"
                    if seconds <= MIN_SLICE_SECONDS
                    else "slice budget",
                )
            values = _aggregate(
                zone_name,
                groups[:limit] if single_page else groups,
                DATASETS[kind],
                dimensions[kind],
                read,
                label_pool,
            )
            for labels, value in values.items():
                samples[kind][labels] = samples[kind].get(labels, 0) + value
            del values, groups
        if split:
            for child_start, child_end in _windows(start, end, 2):
                merge(
                    fetch(tuple(split), child_start, child_end),
                    tuple(split),
                    child_start,
                    child_end,
                )

    total_requests = total_visits = 0
    for (start, end), kinds in scheduled.items():
        node = fetch(tuple(kinds), start, end)
        if "totals" in kinds:
            totals = node.get("totals")
            if not isinstance(totals, list) or len(totals) > 1:
                raise CloudflareAPIError("GraphQL: totals contains no valid aggregate list")
            if totals:  # An empty list means no traffic in the window.
                total_requests = checked_value("totals", totals[0], partial(value_of, "requests"))
                total_visits = checked_value("totals", totals[0], partial(value_of, "visits"))
        merge(node, tuple(k for k in kinds if k != "totals"), start, end)

    limited = dict.fromkeys(collected, 0)
    for kind, values in samples.items():
        if limit and len(values) > limit:
            samples[kind] = dict(
                sorted(values.items(), key=lambda item: item[1], reverse=True)[:limit]
            )
            cuts[kind] = "configured cap"
        if kind in cuts:
            limited[kind] = 1
            logging.warning(
                "Zone %s: %s hit the %d group limit (%s), additional groups may not be exported",
                zone_name,
                kind,
                limit if cuts[kind] == "configured cap" else page_size,
                cuts[kind],
            )
    # Publish hints only after every selection and slice succeeded.
    api.slice_hints.update({(zone_id, kind): leaves[kind] for kind in collected})
    return ZoneMetrics(
        visits=samples["visits"],
        requests=samples["requests"],
        cache=samples["cache"],
        total_requests=total_requests,
        total_visits=total_visits,
        limited=limited,
        edge=samples.get("edge", {}),
        available=supported,
    )


def collect_once(api: CloudflareAPI, now: datetime | None = None) -> Snapshot:
    """
    Run one collection cycle over all zones. Never raises for API failures:
    they are logged and reported through the cf_exporter_* metrics instead.
    """
    started = time.monotonic()
    since, until = day_window(now or datetime.now(UTC))
    visits: dict[LabelValues, float] = {}
    requests_: dict[LabelValues, float] = {}
    cache: dict[LabelValues, float] = {}
    edge: dict[LabelValues, float] = {}
    zone_requests: dict[str, float] = {}
    zone_visits: dict[str, float] = {}
    groups: dict[LabelValues, int] = {}
    limited: dict[LabelValues, int] = {}
    available: dict[LabelValues, int] = {}
    zone_success: dict[str, int] = {}

    try:
        zones = api.list_zones()
        cycle_ok = True
        logging.info("Discovered %d zones", len(zones))
    except CloudflareAPIError as e:
        logging.error("Failed to list zones: %s", e)
        zones, cycle_ok = [], False
    api.start_cycle(len(zones))

    rate_limited = False
    for zone in zones:
        zone_id, zone_name = zone.get("id"), zone.get("name")
        if not zone_id or not zone_name:
            logging.warning("Skipping zone without id or name (id=%s)", zone_id)
            cycle_ok = False
            continue
        if rate_limited:
            zone_success[zone_name] = 0
            continue
        try:
            result = collect_zone(api, zone_id, zone_name, since, until)
        except RateLimitedError as e:
            # Every later request would be rejected as well; retry next cycle.
            logging.error(
                "Zone %s: collection failed: %s; skipping the remaining zones this cycle",
                zone_name,
                e,
            )
            zone_success[zone_name], cycle_ok, rate_limited = 0, False, True
            continue
        except CloudflareAPIError as e:
            logging.error("Zone %s: collection failed: %s", zone_name, e)
            zone_success[zone_name], cycle_ok = 0, False
            continue
        except Exception:
            logging.exception("Zone %s: unexpected error", zone_name)
            zone_success[zone_name], cycle_ok = 0, False
            continue
        zone_success[zone_name] = 1
        visits.update(result.visits)
        requests_.update(result.requests)
        cache.update(result.cache)
        edge.update(result.edge)
        zone_requests[zone_name] = result.total_requests
        zone_visits[zone_name] = result.total_visits
        for kind in DATASETS:
            available[(zone_name, kind)] = result.available[kind]
            # An unsupported dataset has no group count: zero would mean no traffic.
            if result.available[kind]:
                groups[(zone_name, kind)] = len(getattr(result, kind))
                limited[(zone_name, kind)] = result.limited[kind]
        logging.info(
            "Zone %s processed: %d visits, %d requests, %d cache and %d edge metrics",
            zone_name,
            len(result.visits),
            len(result.requests),
            len(result.cache),
            len(result.edge),
        )

    duration = time.monotonic() - started
    logging.info(
        "Cycle finished in %.2f seconds, time range: %s to %s",
        duration,
        rfc3339(since),
        rfc3339(until),
    )
    return Snapshot(
        visits=visits,
        requests=requests_,
        cache=cache,
        edge=edge,
        zone_requests=zone_requests,
        zone_visits=zone_visits,
        groups=groups,
        limited=limited,
        available=available,
        window_start=since,
        window_end=until,
        zone_success=zone_success,
        cycle_success=int(cycle_ok),
        cycle_timestamp=time.time(),
        cycle_duration=duration,
    )


class CloudflareCollector(Collector):
    """
    Serves the latest Snapshot. Scrapes only ever see a complete cycle, and
    series that disappeared from the data disappear from /metrics as well.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._snapshot = Snapshot()

    @property
    def snapshot(self) -> Snapshot:
        with self._lock:
            return self._snapshot

    def update(self, snapshot: Snapshot) -> None:
        with self._lock:
            self._snapshot = snapshot

    def readiness_error(self, max_age: float) -> str | None:
        """Why one atomic snapshot is not ready, without collecting or encoding metrics."""
        snapshot = self.snapshot
        if snapshot.cycle_timestamp is None:
            return "no collection cycle has finished"
        failed = sum(not success for success in snapshot.zone_success.values())
        if failed:
            return f"{failed} of {len(snapshot.zone_success)} zones failed in the last cycle"
        if snapshot.cycle_success != 1:
            return "the last collection cycle failed"
        if not snapshot.zone_success:
            return "no zones were discovered"
        now = datetime.now(UTC)
        if (
            snapshot.window_start is None
            or snapshot.window_end is None
            or snapshot.window_start.date() != now.date()
        ):
            return "no snapshot for the current UTC day"
        cycle_age = now.timestamp() - snapshot.cycle_timestamp
        window_age = (now - snapshot.window_end).total_seconds()
        if not (0 <= cycle_age < max_age and 0 <= window_age < max_age):
            return (
                f"snapshot is stale or in the future: cycle finished {cycle_age:.0f}s and "
                f"window ended {window_age:.0f}s ago, limit {max_age:.0f}s"
            )
        return None

    def collect(self) -> Iterator[Metric]:
        snapshot = self.snapshot
        if self._day_visible(snapshot):
            yield from self._day_metrics(snapshot)
        yield from self._health_metrics(snapshot)

    @staticmethod
    def _day_visible(snapshot: Snapshot) -> bool:
        """Whether the captured snapshot's analytics belong to the current UTC day."""
        return (
            snapshot.window_start is None
            or snapshot.window_start.date() == datetime.now(UTC).date()
        )

    @staticmethod
    def _health_metrics(snapshot: Snapshot) -> Iterator[Metric]:
        zone_success = GaugeMetricFamily(
            "cf_exporter_zone_scrape_success",
            "Whether the last collection of the zone succeeded (1) or failed (0)",
            labels=("zone_name",),
        )
        for zone_name, value in snapshot.zone_success.items():
            zone_success.add_metric((zone_name,), value)
        yield zone_success

        if snapshot.cycle_timestamp is None:
            return
        yield GaugeMetricFamily(
            "cf_exporter_last_cycle_success",
            "Whether the last collection cycle succeeded for all zones (1) or not (0)",
            value=snapshot.cycle_success,
        )
        yield GaugeMetricFamily(
            "cf_exporter_last_cycle_timestamp_seconds",
            "Unix time when the last collection cycle finished",
            value=snapshot.cycle_timestamp,
        )
        yield GaugeMetricFamily(
            "cf_exporter_cycle_duration_seconds",
            "Duration of the last collection cycle",
            value=snapshot.cycle_duration,
        )

    @staticmethod
    def _day_metrics(snapshot: Snapshot) -> Iterator[Metric]:
        window = GaugeMetricFamily(
            "cf_exporter_snapshot_window_end_seconds",
            "Exclusive API window end as Unix seconds; window_start identifies the UTC window",
            labels=("zone_name", "window_start"),
        )
        if snapshot.window_start is not None and snapshot.window_end is not None:
            start, end = rfc3339(snapshot.window_start), snapshot.window_end.timestamp()
            for zone_name, success in snapshot.zone_success.items():
                if success:
                    window.add_metric((zone_name, start), end)
        yield window
        for kind, label_names, name, description in (
            ("visits", VISITS_LABELS, "cf_visits", "Total visits since midnight UTC"),
            ("requests", REQUESTS_LABELS, "cf_requests", "Total requests since midnight UTC"),
            (
                "cache",
                CACHE_LABELS,
                "cf_cache_requests",
                "Total requests by cache status since midnight UTC",
            ),
            (
                "edge",
                EDGE_LABELS,
                "cf_edge_requests",
                "Client requests by edge and origin response status since midnight UTC",
            ),
        ):
            metric = GaugeMetricFamily(name, description, labels=label_names)
            for labels, value in getattr(snapshot, kind).items():
                metric.add_metric(labels, value)
            yield metric
        for name, description, label_names, values in (
            (
                "cf_zone_requests",
                "Total zone requests since midnight UTC",
                ("zone_name",),
                {(zone,): value for zone, value in snapshot.zone_requests.items()},
            ),
            (
                "cf_zone_visits",
                "Total zone visits since midnight UTC",
                ("zone_name",),
                {(zone,): value for zone, value in snapshot.zone_visits.items()},
            ),
            (
                "cf_exporter_groups",
                "Number of exported nonzero label sets",
                ("zone_name", "dataset"),
                snapshot.groups,
            ),
            (
                "cf_exporter_group_limit_reached",
                "Whether groups were truncated (1) or complete (0)",
                ("zone_name", "dataset"),
                snapshot.limited,
            ),
            (
                "cf_exporter_dataset_available",
                "Whether the zone's plan supports the dataset (1) or it is not collected (0)",
                ("zone_name", "dataset"),
                snapshot.available,
            ),
        ):
            metric = GaugeMetricFamily(name, description, labels=label_names)
            for labels, value in values.items():
                metric.add_metric(labels, value)
            yield metric


class MetricsHTTPServer(ThreadingMixIn, WSGIServer):
    """Keep metrics and readiness responsive to concurrent requests."""

    daemon_threads = True


class MetricsRequestHandler(WSGIRequestHandler):
    def log_message(self, message, *args):
        # Match prometheus_client's quiet HTTP server.
        pass


def start_http_server(
    port: int, collector: CloudflareCollector, max_age: float
) -> tuple[WSGIServer, threading.Thread]:
    """Serve readiness alongside the unchanged Prometheus WSGI application."""
    metrics_app = make_wsgi_app(REGISTRY)

    def app(environ, start_response):
        # Every other path keeps serving metrics, as prometheus_client's server does.
        if environ["PATH_INFO"] != "/readyz":
            return metrics_app(environ, start_response)
        headers = [("Content-Type", "text/plain; charset=utf-8"), ("Cache-Control", "no-store")]
        if environ["REQUEST_METHOD"] not in ("GET", "HEAD"):
            status, body = "405 Method Not Allowed", b"use GET or HEAD\n"
            headers.append(("Allow", "GET, HEAD"))
        else:
            error = collector.readiness_error(max_age)
            status = "503 Service Unavailable" if error else "200 OK"
            body = ((error or "ready") + "\n").encode()
        headers.append(("Content-Length", str(len(body))))
        start_response(status, headers)
        return [] if environ["REQUEST_METHOD"] == "HEAD" else [body]

    server = make_server("", port, app, MetricsHTTPServer, handler_class=MetricsRequestHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def run(
    api: CloudflareAPI,
    collector: CloudflareCollector,
    scrape_interval: float,
    stop_event: threading.Event,
) -> None:
    """
    Main loop: collect, publish, sleep until the next cycle is due.

    Once per UTC day the next cycle is moved earlier if it would not finish
    DAY_CLOSE_MARGIN_SECONDS before midnight. Otherwise the traffic between the
    last cycle and midnight would never be exported: after midnight the
    previous day's series are omitted by design. A cycle due after midnight
    starts right after it instead, with an empty window, so the new day's
    snapshot and readiness do not wait for a whole interval.
    """
    closed_day = None
    while not stop_event.is_set():
        started = time.monotonic()
        try:
            collector.update(collect_once(api))
        except Exception:
            # Last-resort guard: a bug must not take the exporter down.
            logging.exception("Collection cycle failed unexpectedly")
            collector.update(
                Snapshot(
                    cycle_success=0,
                    cycle_timestamp=time.time(),
                    cycle_duration=time.monotonic() - started,
                )
            )
        elapsed = time.monotonic() - started
        delay = max(0.0, scrape_interval - elapsed)
        now = datetime.now(UTC)
        midnight = datetime.combine(now.date() + timedelta(days=1), datetime.min.time(), UTC)
        # Midnight is a wall-clock instant; the cycle durations stay monotonic.
        to_midnight = (midnight - now).total_seconds()
        close = to_midnight - elapsed - DAY_CLOSE_MARGIN_SECONDS
        if closed_day != now.date() and 0 < close < delay:
            closed_day, delay = now.date(), close
        else:
            delay = min(delay, to_midnight + NEW_DAY_START_SECONDS)
        stop_event.wait(delay)


LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


class RedactingFormatter(logging.Formatter):
    def __init__(self, token: str):
        super().__init__("%(asctime)s [%(levelname)s] %(message)s")
        self.token = token.strip()

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        if self.token:
            message = message.replace(self.token, "[REDACTED]")
            message = message.replace(repr(self.token)[1:-1], "[REDACTED]")
        return message


def configure_logging(environ: Mapping[str, str] = os.environ) -> None:
    """
    Set up logging with CF_EXPORTER_LOGLEVEL from env or default to INFO.
    """
    level = environ.get("CF_EXPORTER_LOGLEVEL", "INFO").strip().upper()
    valid = level in LOG_LEVELS
    handler = logging.StreamHandler()
    handler.setFormatter(RedactingFormatter(environ.get("CF_EXPORTER_API_TOKEN", "")))
    logging.basicConfig(
        level=getattr(logging, level) if valid else logging.INFO,
        handlers=[handler],
        force=True,
    )
    if valid:
        logging.info("Log level set to %s.", level)
    else:
        logging.warning("Invalid CF_EXPORTER_LOGLEVEL %r, using INFO.", level)


def _int_env(
    environ: Mapping[str, str], name: str, default: int, minimum: int, maximum: int | None = None
) -> int:
    raw = environ.get(name, "").strip() or str(default)
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from None
    if value < minimum or (maximum is not None and value > maximum):
        bounds = f">= {minimum}" if maximum is None else f"between {minimum} and {maximum}"
        raise ConfigError(f"{name} must be {bounds}, got {value}")
    return value


def _validate_token(raw: str) -> str:
    token = raw.strip()
    if not token:
        raise ConfigError("CF_EXPORTER_API_TOKEN must be set.")
    if not token.isascii() or not token.isprintable() or any(char.isspace() for char in token):
        raise ConfigError("CF_EXPORTER_API_TOKEN must contain only printable ASCII without spaces.")
    return token


def parse_env(environ: Mapping[str, str] = os.environ) -> Config:
    """
    Read and validate the configuration from the environment.
    """
    # Secrets mounted from files often carry a trailing newline, which
    # Cloudflare rejects as a malformed Authorization header.
    api_token = _validate_token(environ.get("CF_EXPORTER_API_TOKEN", ""))
    return Config(
        api_token=api_token,
        request_timeout=_int_env(environ, "CF_EXPORTER_REQUEST_TIMEOUT", 30, 1),
        scrape_interval=_int_env(environ, "CF_EXPORTER_SCRAPE_INTERVAL", 300, 1),
        metrics_port=_int_env(environ, "CF_EXPORTER_METRICS_PORT", 8000, 1, 65535),
        group_limit=_int_env(environ, "CF_EXPORTER_GROUP_LIMIT", 0, 0),
    )


def _handle_signal(signum, frame):
    """
    Handle termination signals and exit cleanly.
    """
    logging.info("Signal %s received, shutting down.", signal.Signals(signum).name)
    sys.exit(0)


def main() -> int:
    configure_logging()
    try:
        config = parse_env()
    except ConfigError as e:
        logging.error("%s", e)
        return 1

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    collector = CloudflareCollector()
    REGISTRY.register(collector)
    start_http_server(
        config.metrics_port, collector, READINESS_MAX_AGE_INTERVALS * config.scrape_interval
    )
    logging.info(
        "Exporter running on port %d, scrape interval %d seconds",
        config.metrics_port,
        config.scrape_interval,
    )
    api = CloudflareAPI(
        config.api_token,
        config.request_timeout,
        group_limit=config.group_limit,
        query_budget=cycle_query_budget(config.scrape_interval),
    )
    run(api, collector, config.scrape_interval, threading.Event())
    return 0


if __name__ == "__main__":
    sys.exit(main())
