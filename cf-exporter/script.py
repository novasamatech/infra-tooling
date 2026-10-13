#!/usr/bin/env python3

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""
Prometheus exporter for Cloudflare HTTP request analytics.

Every cycle the exporter lists all zones visible to the API token and runs one
GraphQL query per zone against the httpRequestsAdaptiveGroups dataset for the
current UTC day (midnight to now). Results are published as one snapshot, so a
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
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

import requests
from prometheus_client import REGISTRY, start_http_server
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.registry import Collector
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_BASE_URL = "https://api.cloudflare.com/client/v4"
ZONES_PER_PAGE = 50  # maximum allowed by GET /zones
# Maximum page size of httpRequestsAdaptiveGroups (settings.maxPageSize).
GROUPS_LIMIT = 10000
UNKNOWN = "unknown"

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
VISITS_LABELS = ("zone_name", *VISITS_DIMENSIONS)
REQUESTS_LABELS = ("zone_name", *REQUESTS_DIMENSIONS)
# Fields the query needs regardless of plan (names as in availableFields).
REQUIRED_FIELDS = frozenset({"count", "sum_visits"})

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


@dataclass(frozen=True)
class Config:
    api_token: str = field(repr=False)
    request_timeout: int
    scrape_interval: int
    metrics_port: int


@dataclass(frozen=True)
class ZoneMetrics:
    visits: dict[LabelValues, float]
    requests: dict[LabelValues, float]


@dataclass(frozen=True)
class DatasetSettings:
    fields: frozenset[str]
    max_page_size: int


@dataclass(frozen=True)
class Snapshot:
    """Everything one collection cycle produced; replaced as a whole."""

    visits: dict[LabelValues, float] = field(default_factory=dict)
    requests: dict[LabelValues, float] = field(default_factory=dict)
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

    def __init__(self, api_token: str, request_timeout: int = 30, base_url: str = API_BASE_URL):
        api_token = _validate_token(api_token)
        self.base_url = base_url.rstrip("/")
        self.request_timeout = request_timeout
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
        retry = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "POST"}),
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))

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
            raise CloudflareAPIError(
                f"{method} {path}: HTTP {response.status_code}, response is not JSON"
            ) from None
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
            raise CloudflareAPIError(self._redact(f"GraphQL: {format_errors(payload['errors'])}"))
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


def build_analytics_query(visits_dimensions: list[str], requests_dimensions: list[str]) -> str:
    """
    Build one query that fetches both metrics for a zone.

    Groups are ordered by their value, so if a zone ever exceeds the page size
    the smallest groups are the ones that get cut off.
    """

    def selection(dimensions: list[str]) -> str:
        return "dimensions { " + " ".join(dimensions) + " }" if dimensions else ""

    return f"""
query HttpRequests($zoneTag: string!, $since: Time!, $until: Time!, $limit: uint64!) {{
  viewer {{
    zones(filter: {{zoneTag: $zoneTag}}) {{
      visits: httpRequestsAdaptiveGroups(
        limit: $limit
        filter: {{datetime_geq: $since, datetime_lt: $until}}
        orderBy: [sum_visits_DESC]
      ) {{
        sum {{
          visits
        }}
        {selection(visits_dimensions)}
      }}
      requests: httpRequestsAdaptiveGroups(
        limit: $limit
        filter: {{datetime_geq: $since, datetime_lt: $until}}
        orderBy: [count_DESC]
      ) {{
        count
        {selection(requests_dimensions)}
      }}
    }}
  }}
}}
"""


def _zone_node(data: dict, zone_id: str) -> dict:
    viewer = data.get("viewer")
    zones = viewer.get("zones") if isinstance(viewer, dict) else None
    if not isinstance(zones, list) or len(zones) != 1 or not isinstance(zones[0], dict):
        raise CloudflareAPIError(f"GraphQL: no data returned for zone {zone_id}")
    return zones[0]


def fetch_dataset_settings(api: CloudflareAPI, zone_id: str) -> DatasetSettings:
    """Return the dataset fields and page size the zone's plan allows."""
    data = api.graphql(AVAILABLE_FIELDS_QUERY, {"zoneTag": zone_id})
    node = _zone_node(data, zone_id).get("settings")
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
) -> LabelValues:
    values = [zone_name]
    for label, dimension in dimension_map.items():
        # Dimensions the zone's plan does not offer are not queried at all.
        value = dimensions.get(dimension) if dimension in available else None
        value = UNKNOWN if value is None else str(value)
        if label == "client_request_referer" and value == "":
            value = "direct"
        values.append(value)
    return tuple(values)


def _aggregate(
    zone_name: str,
    kind: str,
    groups: list[dict] | None,
    dimension_map: Mapping[str, str],
    available: list[str],
    value_of: Callable[[dict], float | None],
    limit: int,
) -> dict[LabelValues, float]:
    if not isinstance(groups, list):
        raise CloudflareAPIError(f"GraphQL: {kind} contains no valid group list")
    if len(groups) >= limit:
        logging.warning(
            "Zone %s: %s hit the %d group limit, additional groups may not be exported",
            zone_name,
            kind,
            limit,
        )
    samples: dict[LabelValues, float] = {}
    for group in groups:
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
        dimensions = group.get("dimensions", {})
        if not isinstance(dimensions, dict):
            raise CloudflareAPIError(f"GraphQL: {kind} contains invalid dimensions")
        if not value:
            continue
        labels = _label_values(zone_name, dimension_map, dimensions, available)
        samples[labels] = samples.get(labels, 0) + value
    return samples


def collect_zone(
    api: CloudflareAPI,
    zone_id: str,
    zone_name: str,
    since: datetime,
    until: datetime,
    limit: int = GROUPS_LIMIT,
) -> ZoneMetrics:
    """
    Retrieve visits and requests of a zone for [since, until).
    """
    settings = fetch_dataset_settings(api, zone_id)
    fields = settings.fields
    limit = min(limit, settings.max_page_size)
    missing = REQUIRED_FIELDS - fields
    if missing:
        raise CloudflareAPIError(f"fields not available for the zone: {', '.join(sorted(missing))}")
    if since == until:
        return ZoneMetrics(visits={}, requests={})

    def available(dimension_map: Mapping[str, str]) -> list[str]:
        return [d for d in dimension_map.values() if f"dimensions_{d}" in fields]

    visits_dimensions = available(VISITS_DIMENSIONS)
    requests_dimensions = available(REQUESTS_DIMENSIONS)
    unavailable = sorted(
        (set(VISITS_DIMENSIONS.values()) | set(REQUESTS_DIMENSIONS.values()))
        - set(visits_dimensions)
        - set(requests_dimensions)
    )
    if unavailable:
        logging.debug("Zone %s: plan does not offer %s", zone_name, ", ".join(unavailable))

    data = api.graphql(
        build_analytics_query(visits_dimensions, requests_dimensions),
        {
            "zoneTag": zone_id,
            "since": rfc3339(since),
            "until": rfc3339(until),
            "limit": limit,
        },
    )
    node = _zone_node(data, zone_id)
    return ZoneMetrics(
        visits=_aggregate(
            zone_name,
            "visits",
            node.get("visits"),
            VISITS_DIMENSIONS,
            visits_dimensions,
            lambda group: (group.get("sum") or {}).get("visits"),
            limit,
        ),
        requests=_aggregate(
            zone_name,
            "requests",
            node.get("requests"),
            REQUESTS_DIMENSIONS,
            requests_dimensions,
            lambda group: group.get("count"),
            limit,
        ),
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
    zone_success: dict[str, int] = {}

    try:
        zones = api.list_zones()
        cycle_ok = True
        logging.info("Discovered %d zones", len(zones))
    except CloudflareAPIError as e:
        logging.error("Failed to list zones: %s", e)
        zones, cycle_ok = [], False

    for zone in zones:
        zone_id, zone_name = zone.get("id"), zone.get("name")
        if not zone_id or not zone_name:
            logging.warning("Skipping zone without id or name (id=%s)", zone_id)
            cycle_ok = False
            continue
        try:
            result = collect_zone(api, zone_id, zone_name, since, until)
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
        logging.info(
            "Zone %s processed: %d visits metrics, %d requests metrics",
            zone_name,
            len(result.visits),
            len(result.requests),
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

    def collect(self) -> Iterator[Metric]:
        snapshot = self.snapshot

        visits = GaugeMetricFamily(
            "cf_visits", "Total visits since midnight UTC", labels=VISITS_LABELS
        )
        for labels, value in snapshot.visits.items():
            visits.add_metric(labels, value)
        yield visits

        requests_ = GaugeMetricFamily(
            "cf_requests", "Total requests since midnight UTC", labels=REQUESTS_LABELS
        )
        for labels, value in snapshot.requests.items():
            requests_.add_metric(labels, value)
        yield requests_

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


def run(
    api: CloudflareAPI,
    collector: CloudflareCollector,
    scrape_interval: float,
    stop_event: threading.Event,
) -> None:
    """
    Main loop: collect, publish, sleep until the next cycle is due.
    """
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
        stop_event.wait(max(0.0, scrape_interval - elapsed))


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
    start_http_server(config.metrics_port)
    logging.info(
        "Exporter running on port %d, scrape interval %d seconds",
        config.metrics_port,
        config.scrape_interval,
    )
    api = CloudflareAPI(config.api_token, config.request_timeout)
    run(api, collector, config.scrape_interval, threading.Event())
    return 0


if __name__ == "__main__":
    sys.exit(main())
