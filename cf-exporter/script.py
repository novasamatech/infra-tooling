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

import json
import logging
import math
import os
import signal
import sys
import threading
import time
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import partial
from itertools import pairwise
from socketserver import ThreadingMixIn
from types import MappingProxyType
from urllib.parse import urlsplit
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

import requests
from prometheus_client import REGISTRY, make_wsgi_app
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, Metric
from prometheus_client.registry import Collector
from requests.adapters import HTTPAdapter
from urllib3.exceptions import MaxRetryError, NewConnectionError, ResponseError
from urllib3.exceptions import TimeoutError as TransportTimeoutError
from urllib3.util.retry import Retry

API_BASE_URL = "https://api.cloudflare.com/client/v4"
ZONES_PER_PAGE = 50  # maximum allowed by GET /zones
# Slices are whole seconds, the resolution of the window contract, unless the
# operator configures a longer minimum.
SLICE_RESOLUTION_SECONDS = 1
# The cycle before UTC midnight is moved earlier when it would not finish this
# many seconds before midnight, so the day's final minutes are still exported.
DAY_CLOSE_MARGIN_SECONDS = 30
# /readyz requires snapshot ages below this many collection intervals.
READINESS_MAX_AGE_INTERVALS = 3
# Optional limits of the exporter's own work: Config field -> environment
# variable. Unset, empty or 0 imposes no limit; Cloudflare's own limits apply.
LIMITS = {
    # Top N nonzero groups per zone and dataset.
    "group_limit": "CF_EXPORTER_GROUP_LIMIT",
    # Analytics reads per zone, dataset and cycle, including discarded parents.
    "max_slice_requests": "CF_EXPORTER_MAX_SLICE_REQUESTS",
    # Selections per cycle beyond one read per zone and dataset, for all zones.
    "max_extra_selections": "CF_EXPORTER_MAX_EXTRA_SELECTIONS",
    # Shortest slice that is split further.
    "min_slice_seconds": "CF_EXPORTER_MIN_SLICE_SECONDS",
    # Longest Retry-After that is waited for; a longer one fails the request.
    "max_retry_after_seconds": "CF_EXPORTER_MAX_RETRY_AFTER_SECONDS",
    # Groups per page, at most the plan's maxPageSize.
    "page_size": "CF_EXPORTER_PAGE_SIZE",
}
UNKNOWN = "unknown"
# `api` labels of the API counters: REST (zone listing) and GraphQL (analytics).
APIS = ("rest", "graphql")
# `error` labels of cf_exporter_api_errors_total, a fixed set exported from zero.
API_ERRORS = (
    "rate_limited",  # HTTP 429
    "budget_depleted",  # GraphQL rate-limiter budget error without HTTP 429
    "server_error",  # other HTTP 5xx
    "client_error",  # other HTTP 4xx, e.g. an invalid or insufficient token
    "timeout",  # connect or read timeout
    "connection",  # DNS, TCP, TLS or HTTP protocol failure
    "invalid_response",  # not JSON, not an object, success: false or no data
    "graphql",  # any other error in a GraphQL response
)
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
# Analytics family of every dataset.
METRICS = {
    "visits": "cf_visits",
    "requests": "cf_requests",
    "cache": "cf_cache_requests",
    "edge": "cf_edge_requests",
}
# Every label each analytics family can export, in exposition order. This is
# the catalog; an instance exports it minus its disabled labels (Schema).
METRIC_LABELS = {
    "cf_visits": VISITS_LABELS,
    "cf_requests": REQUESTS_LABELS,
    "cf_cache_requests": CACHE_LABELS,
    "cf_edge_requests": EDGE_LABELS,
    "cf_zone_requests": ("zone_name",),
    "cf_zone_visits": ("zone_name",),
}
# Fields the query needs regardless of plan (names as in availableFields).
REQUIRED_FIELDS = frozenset({"count", "sum_visits"})
# Restricts a selection to requests from clients, excluding Cloudflare's own
# subrequests (e.g. from Workers). Only the edge dataset uses it; the other
# datasets keep counting every request source.
EYEBALL_FILTER = 'requestSource: "eyeball"'
FILTERED_DATASETS = {"edge": EYEBALL_FILTER}
# Fields a dataset's mandatory filter needs. Without them the dataset is not
# collected for the zone at all, rather than reported from unfiltered traffic.
FILTER_FIELDS = {"edge": frozenset({"dimensions_requestSource"})}
# Dimensions that are never reported as "unknown": while their label is
# exported, a plan without them does not collect the dataset, and a group
# without them fails the zone.
REQUIRED_DIMENSIONS = {"edge": frozenset({"edgeResponseStatus"})}
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


@dataclass(frozen=True)
class Schema:
    """Labels an instance exports: the METRIC_LABELS catalog minus disabled labels.

    A disabled label is absent from the exposition, and its dimension is not
    queried, so Cloudflare aggregates over it before applying the page size.
    """

    # Metric name -> labels disabled for that metric.
    disabled: Mapping[str, frozenset[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # A caller's mutable mapping or sets must not change an already
        # collected snapshot's schema while it is being rendered.
        object.__setattr__(
            self,
            "disabled",
            MappingProxyType(
                {metric: frozenset(labels) for metric, labels in self.disabled.items()}
            ),
        )

    def labels(self, metric: str) -> tuple[str, ...]:
        """The metric's exported labels, in catalog order."""
        disabled = self.disabled.get(metric, frozenset())
        return tuple(label for label in METRIC_LABELS[metric] if label not in disabled)

    def dimensions(self, kind: str) -> dict[str, str]:
        """Label -> GraphQL dimension of the dataset's exported labels."""
        disabled = self.disabled.get(METRICS[kind], frozenset())
        return {label: d for label, d in DATASETS[kind].items() if label not in disabled}


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


def api_name(url: str) -> str:
    """The `api` label of a request path or URL."""
    return "graphql" if urlsplit(url).path.endswith("/graphql") else "rest"


def status_error(status: int) -> str | None:
    """The error kind of an HTTP status, or None for a non-error status."""
    if status == 429:
        return "rate_limited"
    if status >= 500:
        return "server_error"
    if status >= 400:
        return "client_error"
    return None


def transport_error(error: Exception) -> str:
    """The error kind of a requests or urllib3 transport exception."""
    # requests reports exhausted retries, e.g. of read timeouts, as a
    # ConnectionError around urllib3's MaxRetryError and the last actual error.
    cause = error.args[0] if error.args else None
    if isinstance(cause, MaxRetryError) and cause.reason is not None:
        error = cause.reason
    # urllib3 derives a refused connection from its connect timeout error.
    if not isinstance(error, NewConnectionError) and isinstance(
        error, (requests.Timeout, TransportTimeoutError)
    ):
        return "timeout"
    return "connection"


class APIStats(Collector):
    """Process counters of Cloudflare API attempts, including transport retries.

    Every attempt is counted once: the transport reports the attempts it retries,
    CloudflareAPI the final one. Labels never carry URLs, messages or zones.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._responses: dict[tuple[str, str], int] = {}
        self._errors = dict.fromkeys(((api, error) for api in APIS for error in API_ERRORS), 0)

    def record(self, api: str, status: int | None = None, error: str | None = None) -> None:
        with self._lock:
            if status is not None:
                key = (api, str(status))
                self._responses[key] = self._responses.get(key, 0) + 1
            if error is not None:
                self._errors[(api, error)] += 1

    def collect(self) -> Iterator[Metric]:
        with self._lock:
            responses, errors = dict(self._responses), dict(self._errors)
        for name, description, labels, values in (
            (
                "cf_exporter_api_responses",
                "HTTP responses from the Cloudflare API by status code, including retried ones",
                ("api", "code"),
                responses,
            ),
            (
                "cf_exporter_api_errors",
                "Failed Cloudflare API attempts by error kind, including retried ones",
                ("api", "error"),
                errors,
            ),
        ):
            metric = CounterMetricFamily(name, description, labels=labels)
            for key, value in sorted(values.items()):
                metric.add_metric(key, value)
            yield metric


class BoundedRetry(Retry):
    """Retry that reports every attempt it retries and, with a configured maximum,
    gives up at once when the server asks to wait longer."""

    # Called with (url, response, error) for each retried attempt.
    on_retry: Callable[[str, object, Exception | None], None] | None = None
    # Longest Retry-After to wait for, in seconds; None waits for any.
    max_retry_after: int | None = None

    def new(self, **kw):
        # urllib3 creates a Retry per attempt from its own parameters only.
        retry = super().new(**kw)
        retry.on_retry = self.on_retry
        retry.max_retry_after = self.max_retry_after
        return retry

    def increment(
        self, method=None, url=None, response=None, error=None, _pool=None, _stacktrace=None
    ):
        retry_after = self.get_retry_after(response) if response is not None else None
        if (
            self.max_retry_after is not None
            and retry_after is not None
            and retry_after > self.max_retry_after
        ):
            # With raise_on_status=False, urllib3 returns this response as is.
            raise MaxRetryError(
                _pool,
                url,
                ResponseError(f"Retry-After {retry_after:.0f}s exceeds {self.max_retry_after}s"),
            )
        retry = super().increment(method, url, response, error, _pool, _stacktrace)
        # Only a retried attempt gets here. A final one raises above or returns
        # its response, and CloudflareAPI._request reports it.
        if self.on_retry is not None:
            self.on_retry(url, response, error)
        return retry


@dataclass(frozen=True)
class Config:
    api_token: str = field(repr=False)
    request_timeout: int
    scrape_interval: int
    metrics_port: int
    # Optional limits, see LIMITS; 0 imposes none.
    group_limit: int = 0
    max_slice_requests: int = 0
    max_extra_selections: int = 0
    min_slice_seconds: int = 0
    max_retry_after_seconds: int = 0
    page_size: int = 0
    schema: Schema = field(default_factory=Schema)


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
    # The labels of this snapshot's analytics, rendered with them.
    schema: Schema = field(default_factory=Schema)


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
        *,
        group_limit: int = 0,
        max_slice_requests: int = 0,
        max_extra_selections: int = 0,
        min_slice_seconds: int = 0,
        max_retry_after_seconds: int = 0,
        page_size: int = 0,
        schema: Schema | None = None,
    ):
        api_token = _validate_token(api_token)
        self.base_url = base_url.rstrip("/")
        self.request_timeout = request_timeout
        # Optional limits (see LIMITS); 0 imposes none.
        self.group_limit = group_limit
        self.max_slice_requests = max_slice_requests
        self.max_extra_selections = max_extra_selections
        self.min_slice_seconds = min_slice_seconds
        self.page_size = page_size
        self.schema = Schema() if schema is None else schema
        # (zone ID, dataset) -> (query signature, leaf slice starts after midnight)
        # of the last successful collection: a cache of the partition, not of data.
        self.slice_hints: dict[tuple[str, str], tuple[tuple, tuple[datetime, ...]]] = {}
        self.hint_day: datetime | None = None
        # Extra slice selections still allowed in this cycle, None if unlimited.
        self.slice_allowance: int | None = None
        self.start_cycle()
        self.stats = APIStats()
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
        # Only a configured maximum makes a longer Retry-After fail at once.
        retry = BoundedRetry(
            total=3,
            backoff_factor=2,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "POST"}),
            raise_on_status=False,
        )
        retry.on_retry = self._retried
        retry.max_retry_after = max_retry_after_seconds or None
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))

    def start_cycle(self) -> None:
        """Renew the shared budget of extra slice selections for a new cycle."""
        self.slice_allowance = self.max_extra_selections or None

    def _retried(self, url: str, response, error: Exception | None) -> None:
        if response is not None:
            self.stats.record(api_name(url), response.status, status_error(response.status))
        else:
            self.stats.record(api_name(url), error=transport_error(error))

    def _invalid(self, api: str, message: str) -> CloudflareAPIError:
        """Count a response without usable content and return its exception."""
        self.stats.record(api, error="invalid_response")
        return CloudflareAPIError(message)

    def _redact(self, message: str) -> str:
        token = self.session.headers["Authorization"].removeprefix("Bearer ")
        return message.replace(token, "[REDACTED]")

    def _request(self, method: str, path: str, **kwargs) -> dict:
        api = api_name(path)
        try:
            response = self.session.request(
                method, f"{self.base_url}{path}", timeout=self.request_timeout, **kwargs
            )
        except requests.RequestException as e:
            self.stats.record(api, error=transport_error(e))
            raise CloudflareAPIError(self._redact(f"{method} {path}: {e}")) from None
        try:
            payload = response.json()
        except ValueError:
            payload = None
        # Exactly the responses rejected below count as errors, each once.
        error = status_error(response.status_code)
        if error is None and (not isinstance(payload, dict) or payload.get("success") is False):
            error = "invalid_response"
        self.stats.record(api, response.status_code, error)
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
                raise self._invalid("rest", "GET /zones: response contains no valid zone list")
            if not isinstance(info, dict):
                raise self._invalid(
                    "rest", "GET /zones: response contains no pagination information"
                )
            total_pages = info.get("total_pages")
            if (
                type(total_pages) is not int
                or total_pages < 0
                or (total_pages == 0 and (result or page > 1))
                or info.get("page", page) != page
                or (page < total_pages and not result)
            ):
                raise self._invalid("rest", "GET /zones: invalid pagination information")
            if any(
                not isinstance(zone, dict)
                or not isinstance(zone.get("id"), str)
                or not zone["id"]
                or not isinstance(zone.get("name"), str)
                or not zone["name"]
                for zone in result
            ):
                raise self._invalid("rest", "GET /zones: zone without a valid id or name")
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
            budget = _is_budget_error(payload["errors"])
            self.stats.record("graphql", error="budget_depleted" if budget else "graphql")
            if budget:
                raise RateLimitedError(message)
            raise CloudflareAPIError(message)
        data = payload.get("data")
        if not isinstance(data, dict):
            raise self._invalid("graphql", "GraphQL: response contains no data")
        return data


def day_window(now: datetime) -> tuple[datetime, datetime]:
    """Return [midnight UTC, now) truncated to whole seconds."""
    until = now.astimezone(UTC).replace(microsecond=0)
    return until.replace(hour=0, minute=0, second=0), until


def rfc3339(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_analytics_query(
    dimensions: Mapping[str, Sequence[str]],
    datasets: Sequence[str] = (*DATASETS, "totals"),
) -> str:
    """Build an aliased query for just the selections needed by this window.

    `dimensions` maps each dataset to the dimensions it groups by; dimensionless
    zone totals are the "totals" selection.
    """
    selections = []
    for kind in datasets:
        totals = kind == "totals"
        fields = () if totals else dimensions.get(kind, ())
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
    prefix: LabelValues,
    dimension_map: Mapping[str, str],
    dimensions: Mapping,
    available: Collection[str],
    label_pool: dict[str, str] | None = None,
) -> LabelValues:
    """Label values of a group: `prefix` (the zone name, if exported), then one per
    exported dimension."""
    values = list(prefix)
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


def _group_dimensions(kind: str, group: dict, available: Collection[str]) -> dict:
    """Validate a group's dimensions; `available` are the dimensions queried."""
    dimensions = group.get("dimensions", {})
    if not isinstance(dimensions, dict):
        raise CloudflareAPIError(f"GraphQL: {kind} contains invalid dimensions")
    types = DIMENSION_TYPES.get(kind, {})
    required = REQUIRED_DIMENSIONS.get(kind, frozenset())
    if types or required:
        for dimension in available:
            value = dimensions.get(dimension)
            if value is None:
                if dimension in required:
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
    prefix: LabelValues,
    groups: list[dict],
    dimension_map: Mapping[str, str],
    available: Collection[str],
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
        labels = _label_values(prefix, dimension_map, dimensions, available, label_pool)
        samples[labels] = samples.get(labels, 0) + value
    return samples


def _add_samples(target: dict[LabelValues, float], samples: Mapping[LabelValues, float]) -> None:
    """Sum samples into target; identical label sets of different zones combine."""
    for labels, value in samples.items():
        target[labels] = target.get(labels, 0) + value


def _windows(
    since: datetime, until: datetime, count: int, step: int = SLICE_RESOLUTION_SECONDS
) -> list[tuple[datetime, datetime]]:
    """At most `count` disjoint, nonempty slices covering [since, until), with
    boundaries at whole multiples of `step` seconds from `since`; only the
    trailing slice can be shorter than `step`. An empty window has no slices."""
    seconds = int((until - since).total_seconds())
    units = -(-seconds // step)
    count = min(count, units)
    if count < 1:
        return []
    edges = [
        since + timedelta(seconds=min(seconds, i * units // count * step)) for i in range(count + 1)
    ]
    return list(pairwise(edges))


def _coarsen(edges: Sequence[datetime], count: int) -> list[tuple[datetime, datetime]]:
    """Merge the adjacent slices between `edges` into at most `count` slices,
    keeping only existing boundaries."""
    leaves = len(edges) - 1
    count = min(count, leaves)
    return list(pairwise(edges[i * leaves // count] for i in range(count + 1)))


def _remaining(budget: int | None) -> str:
    return "unlimited" if budget is None else str(budget)


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
    schema = api.schema
    limit = api.group_limit if limit is None else limit
    # A configured page size never exceeds the plan's.
    max_page = min(api.page_size or settings.max_page_size, settings.max_page_size)
    # A cap within one page is served by a single query. One extra group, where
    # the plan allows it, shows whether the cap actually cut anything.
    single_page = bool(limit and limit <= max_page)
    page_size = min(limit + 1, max_page) if single_page else max_page
    step = api.min_slice_seconds or SLICE_RESOLUTION_SECONDS
    missing = REQUIRED_FIELDS - fields
    if missing:
        raise CloudflareAPIError(f"fields not available for the zone: {', '.join(sorted(missing))}")
    exported = {kind: schema.dimensions(kind) for kind in DATASETS}
    # A dataset the plan cannot serve is skipped explicitly; the others are
    # collected. A required dimension matters only while its label is exported.
    needed = {
        kind: FILTER_FIELDS.get(kind, frozenset())
        | {
            f"dimensions_{d}"
            for d in REQUIRED_DIMENSIONS.get(kind, frozenset())
            if d in exported[kind].values()
        }
        for kind in DATASETS
    }
    supported = {kind: int(needed[kind] <= fields) for kind in DATASETS}
    for kind, ok in supported.items():
        if not ok:
            logging.warning(
                "Zone %s: plan does not offer %s; the %s dataset is not collected",
                zone_name,
                ", ".join(sorted(needed[kind] - fields)),
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

    # Exported dimensions the plan offers; the others are reported as unknown.
    dimensions = {
        kind: [d for d in mapping.values() if f"dimensions_{d}" in fields]
        for kind, mapping in exported.items()
    }
    unavailable = sorted(
        {d for kind in collected for d in exported[kind].values()}
        - {d for kind in collected for d in dimensions[kind]}
    )
    if unavailable:
        logging.debug("Zone %s: plan does not offer %s", zone_name, ", ".join(unavailable))
    # Without zone_name, collect_once() sums identical label sets of all zones.
    prefixes = {
        kind: (zone_name,) if "zone_name" in schema.labels(METRICS[kind]) else ()
        for kind in DATASETS
    }
    # A partition found for other dimensions or another page size is not reused.
    signatures = {kind: (tuple(dimensions[kind]), page_size) for kind in DATASETS}

    def fetch(
        kinds: tuple[str, ...], start: datetime, end: datetime, dimensionless: bool = False
    ) -> dict:
        variables = {
            "zoneTag": zone_id,
            "since": rfc3339(start),
            "until": rfc3339(end),
        }
        if any(k != "totals" for k in kinds):
            variables["limit"] = 1 if dimensionless else page_size
        query = build_analytics_query({} if dimensionless else dimensions, kinds)
        return zone_node(api.graphql(query, variables), zone_id)

    def value_of(kind: str, group: dict) -> float | None:
        return (group.get("sum") or {}).get("visits") if kind == "visits" else group.get("count")

    def hinted_cuts(kind: str) -> list[datetime]:
        """Leaf boundaries of the previous successful cycle within this window.

        A grown window extends the last leaf; every slice is fetched again, so
        late revisions of earlier slices are included.
        """
        signature, starts = api.slice_hints.get((zone_id, kind), (None, ()))
        if single_page or signature != signatures[kind]:
            return []
        return sorted({start for start in starts if since < start < until})

    samples: dict[str, dict[LabelValues, float]] = {kind: {} for kind in collected}
    label_pool: dict[str, str] = {}
    # Why each selection is incomplete. Decided from the pages themselves: a
    # separately fetched total can differ from the slices for the current day.
    cuts: dict[str, str] = {}
    leaves: dict[str, list[datetime]] = {kind: [] for kind in collected}
    # Remaining reads per dataset; None is unlimited, as is api.slice_allowance.
    budget = dict.fromkeys(collected, api.max_slice_requests or None)
    scheduled: dict[tuple[datetime, datetime], list[str]] = {(since, until): ["totals"]}
    for kind in collected:
        edges = [since, *hinted_cuts(kind), until]
        count = len(edges) - 1
        if budget[kind] is not None:
            count = min(count, budget[kind])
        if api.slice_allowance is not None:
            count = min(count, 1 + api.slice_allowance)
        windows = _coarsen(edges, count)
        if budget[kind] is not None:
            budget[kind] -= len(windows)
        if api.slice_allowance is not None:
            api.slice_allowance -= len(windows) - 1
        for window in windows:
            scheduled.setdefault(window, []).append(kind)

    def unsplittable(kind: str, seconds: float) -> str | None:
        """Why a full page of the slice cannot be split, or None if it can."""
        if seconds <= step:
            reason = (
                f"configured minimum slice of {step}s"
                if api.min_slice_seconds
                else f"{SLICE_RESOLUTION_SECONDS}s slice at the time resolution"
            )
        elif budget[kind] is not None and budget[kind] < 2:
            reason = "dataset slice budget"
        elif api.slice_allowance is not None and api.slice_allowance < 2:
            reason = "shared slice budget"
        else:
            return None
        return (
            f"{reason}; dataset reads left: {_remaining(budget[kind])}, "
            f"shared extra selections left: {_remaining(api.slice_allowance)}"
        )

    def hidden_traffic(kinds: list[str], start: datetime, end: datetime) -> set[str]:
        """Datasets whose empty page hides traffic: Cloudflare can return no groups
        for a window that has some. One dimensionless read of the same window and
        filter, within the budgets, tells this apart from a window without traffic."""
        checked = []
        for kind in kinds:
            if (budget[kind] is not None and budget[kind] < 1) or (
                api.slice_allowance is not None and api.slice_allowance < 1
            ):
                reason = (
                    "dataset slice budget"
                    if budget[kind] is not None and budget[kind] < 1
                    else "shared slice budget"
                )
                cuts.setdefault(
                    kind,
                    "returned an empty page that could not be checked "
                    f"({reason}; dataset reads left: {_remaining(budget[kind])}, "
                    f"shared extra selections left: {_remaining(api.slice_allowance)})",
                )
                continue
            if budget[kind] is not None:
                budget[kind] -= 1
            if api.slice_allowance is not None:
                api.slice_allowance -= 1
            checked.append(kind)
        if not checked:
            return set()
        node = fetch(tuple(checked), start, end, dimensionless=True)
        hidden = set()
        for kind in checked:
            aggregate = group_list(kind, node.get(kind))
            if len(aggregate) > 1:
                raise CloudflareAPIError(f"GraphQL: {kind} contains no valid aggregate list")
            if aggregate and checked_value(kind, aggregate[0], partial(value_of, kind)) > 0:
                hidden.add(kind)
        return hidden

    def merge(node: dict, kinds: tuple[str, ...], start: datetime, end: datetime) -> None:
        split = []
        seconds = (end - start).total_seconds()
        pages = {}
        for kind in kinds:
            groups = group_list(kind, node.pop(kind, None))
            read = partial(value_of, kind)
            # Validate once, including parent/probe groups that will not be aggregated.
            # A malformed response is not proof of a complete or empty dataset.
            for group in groups:
                checked_value(kind, group, read)
                _group_dimensions(kind, group, dimensions[kind])
            pages[kind] = groups
        # A zero whole-window total rules out traffic in its matching slices.
        # In particular, requests do not establish whether visits exist.
        empty = [
            kind
            for kind, groups in pages.items()
            if not groups and (total_visits if kind == "visits" else total_requests)
        ]
        hidden = hidden_traffic(empty, start, end) if empty else set()
        for kind in kinds:
            groups = pages.pop(kind)
            read = partial(value_of, kind)
            # Ordered largest first: a page cut nothing if its last group is zero.
            # Without queried dimensions the API returns at most one group.
            # Its one aggregate is complete even when page_size is one.
            full = bool(dimensions[kind]) and len(groups) >= page_size and read(groups[-1]) > 0
            if full or kind in hidden:
                page = (
                    f"returned a full page of {page_size} groups"
                    if full
                    else "returned an empty page for a window with traffic"
                )
                if single_page:
                    reason = "configured cap" if full else f"{page} (configured cap, one query)"
                else:
                    reason = unsplittable(kind, seconds)
                    if reason is not None:
                        reason = f"{page} that was not split ({reason})"
                if reason is None:
                    if budget[kind] is not None:
                        budget[kind] -= 2
                    if api.slice_allowance is not None:
                        api.slice_allowance -= 2
                    split.append(kind)
                    # Discard the validated parent page; only its children count.
                    del groups
                    continue
                cuts.setdefault(kind, reason)
            leaves[kind].append(start)
            values = _aggregate(
                prefixes[kind],
                groups[:limit] if single_page else groups,
                exported[kind],
                dimensions[kind],
                read,
                label_pool,
            )
            _add_samples(samples[kind], values)
            del values, groups
        if split:
            for child_start, child_end in _windows(start, end, 2, step):
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
            # A full page is not a limit on the number of groups: only its slice is cut.
            if cuts[kind] == "configured cap":
                logging.warning(
                    "Zone %s: %s hit the %d group limit (configured cap), "
                    "additional groups may not be exported",
                    zone_name,
                    kind,
                    limit,
                )
            else:
                logging.warning(
                    "Zone %s: %s %s, additional groups may not be exported",
                    zone_name,
                    kind,
                    cuts[kind],
                )
    # Publish hints only after every selection and slice succeeded.
    api.slice_hints.update(
        {
            (zone_id, kind): (signatures[kind], tuple(s for s in leaves[kind] if s != since))
            for kind in collected
        }
    )
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
    schema = api.schema
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
    api.start_cycle()

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
        _add_samples(visits, result.visits)
        _add_samples(requests_, result.requests)
        _add_samples(cache, result.cache)
        _add_samples(edge, result.edge)
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
        schema=schema,
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
        if snapshot.window_start is None or snapshot.window_end is None:
            return "the last snapshot has no API window"
        # Deliberately independent of the UTC day: after midnight a fresh snapshot
        # of the previous day stays ready while the new day is collected.
        now = datetime.now(UTC)
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
        schema = snapshot.schema
        for kind, description in (
            ("visits", "Total visits since midnight UTC"),
            ("requests", "Total requests since midnight UTC"),
            ("cache", "Total requests by cache status since midnight UTC"),
            ("edge", "Client requests by edge and origin response status since midnight UTC"),
        ):
            name = METRICS[kind]
            metric = GaugeMetricFamily(name, description, labels=schema.labels(name))
            for labels, value in getattr(snapshot, kind).items():
                metric.add_metric(labels, value)
            yield metric
        for name, description, totals in (
            ("cf_zone_requests", "Total zone requests since midnight UTC", snapshot.zone_requests),
            ("cf_zone_visits", "Total zone visits since midnight UTC", snapshot.zone_visits),
        ):
            label_names = schema.labels(name)
            metric = GaugeMetricFamily(name, description, labels=label_names)
            if label_names:
                for zone, value in totals.items():
                    metric.add_metric((zone,), value)
            elif totals:
                # Without zone_name, one series sums the successfully collected zones.
                metric.add_metric((), sum(totals.values()))
            yield metric
        for name, description, label_names, values in (
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
    previous day's series are omitted by design.
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
        close = (midnight - now).total_seconds() - elapsed - DAY_CLOSE_MARGIN_SECONDS
        if closed_day != now.date() and 0 < close < delay:
            closed_day, delay = now.date(), close
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


DISABLED_LABELS = "CF_EXPORTER_DISABLED_LABELS"


def parse_disabled_labels(raw: str) -> Schema:
    """The schema for CF_EXPORTER_DISABLED_LABELS: a JSON object mapping metric
    names to lists of labels to disable. Empty means every label is exported."""
    if not raw.strip():
        return Schema()

    def unique(pairs: list[tuple[str, object]]) -> dict:
        keys = [key for key, _ in pairs]
        if len(set(keys)) != len(keys):
            raise ConfigError(f"{DISABLED_LABELS} must not repeat a key")
        return dict(pairs)

    try:
        value = json.loads(raw, object_pairs_hook=unique)
    except ValueError:
        raise ConfigError(f"{DISABLED_LABELS} must be valid JSON") from None
    if not isinstance(value, dict):
        raise ConfigError(f"{DISABLED_LABELS} must be a JSON object of metric names")
    disabled = {}
    for metric, labels in value.items():
        if metric not in METRIC_LABELS:
            raise ConfigError(
                f"{DISABLED_LABELS} names unknown metric {metric!r}; "
                f"expected one of {', '.join(METRIC_LABELS)}"
            )
        if not isinstance(labels, list) or not all(isinstance(label, str) for label in labels):
            raise ConfigError(f"{DISABLED_LABELS}: {metric} must map to a list of label names")
        unknown = [label for label in labels if label not in METRIC_LABELS[metric]]
        if unknown:
            raise ConfigError(
                f"{DISABLED_LABELS}: {metric} has no label {unknown[0]!r}; "
                f"expected any of {', '.join(METRIC_LABELS[metric])}"
            )
        if labels:
            disabled[metric] = frozenset(labels)
    return Schema(disabled)


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
        **{field: _int_env(environ, name, 0, 0) for field, name in LIMITS.items()},
        schema=parse_disabled_labels(environ.get(DISABLED_LABELS, "")),
    )


def create_api(config: Config) -> CloudflareAPI:
    """The API client for a configuration; main(), profiling and live checks share it."""
    return CloudflareAPI(
        config.api_token,
        config.request_timeout,
        **{field: getattr(config, field) for field in LIMITS},
        schema=config.schema,
    )


def describe(config: Config) -> str:
    """The configured limits and disabled labels, for the startup log."""
    limits = ", ".join(
        f"{name}={getattr(config, field)}"
        for field, name in LIMITS.items()
        if getattr(config, field)
    )
    disabled = "; ".join(
        f"{metric}: {', '.join(label for label in labels if label in removed)}"
        for metric, labels in METRIC_LABELS.items()
        if (removed := config.schema.disabled.get(metric))
    )
    return f"optional limits: {limits or 'none'}; disabled labels: {disabled or 'none'}"


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
    api = create_api(config)
    REGISTRY.register(collector)
    REGISTRY.register(api.stats)
    start_http_server(
        config.metrics_port, collector, READINESS_MAX_AGE_INTERVALS * config.scrape_interval
    )
    logging.info(
        "Exporter running on port %d, scrape interval %d seconds, %s",
        config.metrics_port,
        config.scrape_interval,
        describe(config),
    )
    run(api, collector, config.scrape_interval, threading.Event())
    return 0


if __name__ == "__main__":
    sys.exit(main())
