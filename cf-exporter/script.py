#!/usr/bin/env python3

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""
Prometheus exporter for Cloudflare HTTP request analytics in closed 5-minute buckets.

The exporter reads httpRequestsAdaptiveGroups of every zone the API token can see
in buckets of 300 seconds aligned to UTC. A bucket is accepted only when its groups
add up to totals read independently in the same request and no selection was
answered at a reduced resolution; otherwise it is read again later. Accepted
buckets are kept in a local SQLite database and served on /analytics, each sample
carrying the bucket end as its timestamp, until they have been scraped. The
database heals itself: a missing, foreign or corrupt file is recreated and the
history is read again from Cloudflare. Health metrics are served on /metrics.
"""

import gzip
import hashlib
import hmac
import json
import logging
import math
import os
import signal
import socket
import sqlite3
import sys
import threading
import time
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from socketserver import ThreadingMixIn
from types import MappingProxyType
from urllib.parse import urlsplit
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

import requests
from prometheus_client import REGISTRY, make_wsgi_app
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, Metric
from prometheus_client.registry import Collector
from requests.adapters import HTTPAdapter
from urllib3.exceptions import InvalidHeader, MaxRetryError, NewConnectionError, ResponseError
from urllib3.exceptions import TimeoutError as TransportTimeoutError
from urllib3.util.retry import Retry

API_BASE_URL = "https://api.cloudflare.com/client/v4"
ZONES_PER_PAGE = 50  # maximum allowed by GET /zones
DEFAULT_REQUEST_TIMEOUT = 30
DEFAULT_METRICS_PORT = 8000
HOUR_SECONDS = 3600
DAY_SECONDS = 86400

# Every analytics sample counts the events of one bucket of this many seconds,
# aligned to the Unix epoch (and so to UTC days), and carries the bucket end.
BUCKET_SECONDS = 300
# A bucket is read this long after it ended; Cloudflare's own ingestion delay.
SETTLE_SECONDS = 300
# An accepted bucket is read once more this long after it ended, so that late
# events still reach it; a changed bucket is served again with the same timestamps.
RECHECK_SECONDS = HOUR_SECONDS
# A recheck that keeps failing verification is given up this long after the bucket.
RECHECK_GIVE_UP_SECONDS = 6 * HOUR_SECONDS
RECHECK_RETRY_SECONDS = 900
# Pending buckets that ended within this span are read before rechecks and history.
FRESH_SECONDS = HOUR_SECONDS
# Under load Cloudflare answers a selection from a table holding every tenth or
# hundredth event instead of its full data. Every group of such an answer has a
# sample interval of 10 or more, and buckets with few events have no group at
# all, so that a reduced answer for a quiet hour is simply empty. Groups from
# the full data are far below this value, which separates the two.
SAMPLED_INTERVAL = 5.0
# An empty answer proves nothing. The totals, which every bucket is checked
# against, are therefore read together with the buckets of this many seconds
# around the window: one group below SAMPLED_INTERVAL anywhere in that answer
# proves that all of it, empty buckets included, comes from the full data.
PROOF_SECONDS = DAY_SECONDS
# Totals without any group in that range are read over this many seconds
# instead: a quiet zone. Not longer, because Cloudflare answers ranges of a
# month from a reduced table every time, which would prove nothing ever.
LONG_PROOF_SECONDS = 7 * DAY_SECONDS
# A zone stays in that mode until such an answer has at least this many groups.
LONG_PROOF_GROUPS = 50
# Totals without any group even there are believed, as an idle zone, after
# this many answers in a row. Each is computed on its own, so that all of them
# being reduced and empty by chance is very unlikely. An answer without groups
# over a range in which an earlier answer had events is never such a look.
EMPTY_LOOKS = 3
# A bucket that never verifies but returns the same answer for this long is what
# Cloudflare stored, not a transient reduced-resolution answer; it is accepted
# and counted as unverified.
UNVERIFIED_HOLD_SECONDS = 6 * HOUR_SECONDS
# Group sums equal the totals within this relative and absolute tolerance;
# counts are integers, so it only absorbs floating-point rounding.
SUM_TOLERANCE = 1e-9
# Hex digits kept of the SHA-256 of a bucket's samples and of a zone's layout.
DIGEST_LENGTH = 16
# Delays before reading a rejected bucket again, by attempt.
RETRY_DELAYS = (120, 240, 480, 900, 1800)
# Selections that were answered at a reduced resolution or prove nothing are
# read again at once, this many times. Cloudflare caches answers by query and
# variables but not by alias, so every read of the same window uses a slightly
# different page size.
REPAIR_ROUNDS = 3
# Page sizes used one after the other, so that a request repeated minutes later
# is rarely answered from the cache of an earlier one; never below half a page.
LIMIT_VARIANTS = 50
# A part of a split read that was answered at a reduced resolution is asked for
# up to this many times, each with another page size.
PART_READS = 3
# Buckets that failed verification before are read in windows of at most this
# many buckets.
RETRY_WINDOW_BUCKETS = 12
# Delay after a failed read, before its zone and its buckets are tried again. A
# bucket whose read failed is then read alone, and one that keeps failing waits
# RETRY_DELAYS, so that it cannot hold back the other buckets of its zone.
ERROR_RETRY_SECONDS = 60
# After this many failed reads of a zone in a row, the first read that works
# makes the zone's postponed buckets due at once: Cloudflare was failing, not
# the buckets.
OUTAGE_FAILURES = 2
# A bucket stays on /analytics until it was part of this many complete responses
# and accepted at least this long ago. Then its samples are deleted.
DELIVERY_RESPONSES = 2
DELIVERY_SECONDS = 600
# Samples per /analytics response by default (CF_EXPORTER_RESPONSE_SAMPLES).
# Buckets that still need a response go first, the newest first; those only
# waiting for DELIVERY_SECONDS fill what is left. Not a limit of the exported
# data: the rest follows in later responses.
RESPONSE_SAMPLE_BUDGET = 50_000
# Compression level of a gzip /analytics response.
GZIP_LEVEL = 6
# History is read only while fewer undelivered samples are stored, so that the
# database follows the scrapes; fresh buckets and rechecks are always read.
BACKLOG_SAMPLE_LIMIT = 500_000
# Accepted buckets that no scraper consumed this long after their acceptance are
# dropped; it bounds the database without scrapes.
UNDELIVERED_KEEP_SECONDS = 35 * DAY_SECONDS
ZONES_REFRESH_SECONDS = 300
# The state of a zone missing from the listing is kept this long, so that an
# incomplete listing does not make the exporter read the zone's history again.
ZONE_KEEP_SECONDS = DAY_SECONDS
# Settings are fetched again this often; a zone whose plan cannot be read is
# tried again after the same time.
SETTINGS_REFRESH_SECONDS = HOUR_SECONDS
LOOP_SECONDS = 15
PRUNE_SECONDS = 30
# Least time between two history reads by default
# (CF_EXPORTER_HISTORY_INTERVAL_SECONDS); fresh buckets and rechecks are not
# paced. Cloudflare allows 300 GraphQL queries in 5 minutes for all users of a
# token together and counts every selection of a request: two instances
# reading history at this pace, with their fresh buckets, stay below that.
HISTORY_INTERVAL_SECONDS = 15
# Each time Cloudflare's rate limit is reached the interval doubles, up to this
# factor; it halves again after every HISTORY_RECOVER_SECONDS without one.
HISTORY_SLOWDOWN_MAX = 16
HISTORY_RECOVER_SECONDS = HOUR_SECONDS
# Buckets per history read: starts here, halves after a split, doubles while
# pages stay less than WINDOW_GROW_FILL full, never above the maximum or the
# dataset's maxDuration.
INITIAL_WINDOW_BUCKETS = 12
MAX_WINDOW_BUCKETS = 72
WINDOW_GROW_FILL = 0.25
# Keep away from the edge of the dataset's notOlderThan.
RETENTION_MARGIN_SECONDS = HOUR_SECONDS
# Cloudflare's documented cooldown when a rate-limit response has no Retry-After.
RATE_LIMIT_RETRY_SECONDS = 300
# A full page of one second is paginated rather than split.
SLICE_RESOLUTION_SECONDS = 1
SCHEMA_VERSION = 2
# Marks the database file as the exporter's own ("cfex").
APPLICATION_ID = 0x63666578
# The SQLite file header: its size and where the application id lies in it.
SQLITE_HEADER_BYTES = 100
SQLITE_APPLICATION_ID_OFFSET = 68
# How long an operation waits for another process holding the database lock.
STORAGE_LOCK_SECONDS = 30
# How often the file is tried again while the state is kept in memory.
STORAGE_RETRY_SECONDS = 60
DEFAULT_DB_PATH = "/var/lib/cf-exporter/cf-exporter.sqlite3"
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
# `result` labels of cf_exporter_bucket_reads_total, a fixed set exported from zero.
READ_RESULTS = (
    "accepted",  # verified and stored
    "accepted_unverified",  # never verified, same answer for UNVERIFIED_HOLD_SECONDS
    "sampled",  # a selection was answered at a reduced resolution; read again later
    "inconsistent",  # groups do not add up to the totals; read again later
    "recheck_unchanged",
    "recheck_changed",  # late events: the bucket is served again
    "recheck_failed",  # the recheck did not verify; tried again or given up
    "recheck_skipped",  # labels or dimensions changed since; not served again
    "abandoned",  # tried but never verified before it left the dataset's reach
    "expired",  # accepted but not scraped within UNDELIVERED_KEEP_SECONDS
    "error",  # the read failed (API or response error)
)
UINT16_MAX = (1 << 16) - 1
# Removed in 2.0.0; a deployment that still sets them gets a warning.
REMOVED_SETTINGS = (
    "CF_EXPORTER_SCRAPE_INTERVAL",
    "CF_EXPORTER_GROUP_LIMIT",
    "CF_EXPORTER_MAX_SLICE_REQUESTS",
    "CF_EXPORTER_MAX_EXTRA_SELECTIONS",
    "CF_EXPORTER_MIN_SLICE_SECONDS",
)

# The bucket of a group: its start, as Cloudflare reports it.
BUCKET_DIMENSION = "datetimeFiveMinutes"
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
DATASETS = {
    "visits": VISITS_DIMENSIONS,
    "requests": REQUESTS_DIMENSIONS,
    "cache": CACHE_DIMENSIONS,
    "edge": EDGE_DIMENSIONS,
}
# Analytics family of every dataset.
METRICS = {
    "visits": "cf_visits_5m",
    "requests": "cf_requests_5m",
    "cache": "cf_cache_requests_5m",
    "edge": "cf_edge_requests_5m",
}
ZONE_REQUESTS = "cf_zone_requests_5m"
ZONE_VISITS = "cf_zone_visits_5m"
# Every label each analytics family can export, in exposition order. This is
# the catalog; an instance exports it minus its disabled labels (Schema).
METRIC_LABELS = {
    **{METRICS[kind]: ("zone_name", *dimensions) for kind, dimensions in DATASETS.items()},
    ZONE_REQUESTS: ("zone_name",),
    ZONE_VISITS: ("zone_name",),
}
DESCRIPTIONS = {
    "cf_visits_5m": "Visits in the 5-minute bucket ending at the sample timestamp",
    "cf_requests_5m": "Requests in the 5-minute bucket ending at the sample timestamp",
    "cf_cache_requests_5m": (
        "Requests by cache status in the 5-minute bucket ending at the sample timestamp"
    ),
    "cf_edge_requests_5m": (
        "Client requests by edge and origin response status in the 5-minute bucket "
        "ending at the sample timestamp"
    ),
    ZONE_REQUESTS: "All requests of the zone in the 5-minute bucket ending at the sample timestamp",
    ZONE_VISITS: "All visits of the zone in the 5-minute bucket ending at the sample timestamp",
}
# Datasets aggregated from the one `groups` selection, which queries the union
# of their exported dimensions; edge has its own filtered selection.
GROUP_DATASETS = ("visits", "requests", "cache")
# Fields every read needs regardless of plan (names as in availableFields).
REQUIRED_FIELDS = frozenset(
    {"count", "sum_visits", "avg_sampleInterval", f"dimensions_{BUCKET_DIMENSION}"}
)
# Restricts a selection to requests from clients, excluding Cloudflare's own
# subrequests (e.g. from Workers). Only the edge dataset and its totals use it;
# the other datasets and zone totals count every request source.
EYEBALL_FILTER = 'requestSource: "eyeball"'
# Fields the edge dataset's mandatory filter needs. Without them the dataset is
# not collected for the zone at all, rather than reported from unfiltered traffic.
FILTER_FIELDS = {"edge": frozenset({"dimensions_requestSource"})}
# Dimensions that are never reported as "unknown": while their label is
# exported, a plan without them does not collect the dataset, and a group
# without them fails the read.
REQUIRED_DIMENSIONS = {"edge": frozenset({"edgeResponseStatus"})}
# Strict response types of the edge selection.
DIMENSION_TYPES = {
    "edge": {
        "edgeResponseStatus": int,
        "originResponseStatus": int,
        "clientRequestHTTPHost": str,
        "clientRequestHTTPMethodName": str,
        "clientRequestPath": str,
    },
}
# Selections of one read: alias -> (values, filter, dataset whose types apply).
SELECTIONS = {
    "groups": ("count sum { visits } avg { sampleInterval }", "", None),
    "edge": ("count avg { sampleInterval }", EYEBALL_FILTER, "edge"),
    "totals": ("count sum { visits } avg { sampleInterval }", "", None),
    "edge_totals": ("count avg { sampleInterval }", EYEBALL_FILTER, None),
}
# Selections whose groups carry dimensions and can fill a page.
GROUP_SELECTIONS = ("groups", "edge")
# Selections without dimensions, one group per bucket, and the selection each
# of them verifies. They are read over the proof range, see PROOF_SECONDS.
TOTAL_SELECTIONS = {"totals": "groups", "edge_totals": "edge"}

SETTINGS_QUERY = """
query Settings($zoneTag: string!) {
  viewer {
    zones(filter: {zoneTag: $zoneTag}) {
      settings {
        httpRequestsAdaptiveGroups {
          enabled
          availableFields
          maxPageSize
          maxDuration
          notOlderThan
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
    """An explicitly configured limit prevents waiting for Cloudflare's rate limit."""


class ZonePlanError(CloudflareAPIError):
    """The zone's plan cannot serve the reads; asking again soon changes nothing."""


@dataclass(frozen=True)
class Schema:
    """Labels an instance exports: the METRIC_LABELS catalog minus disabled labels.

    A disabled label is absent from the exposition, and its dimension is not
    queried unless another family still exports it, so Cloudflare aggregates
    over it before applying the page size. `zone_name` cannot be disabled.
    """

    # Metric name -> labels disabled for that metric.
    disabled: Mapping[str, frozenset[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # A caller's mutable mapping or sets must not change the schema later.
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

    def limited(self) -> int:
        """Attempts that Cloudflare's rate limit rejected so far."""
        with self._lock:
            return sum(
                self._errors[(api, error)]
                for api in APIS
                for error in ("rate_limited", "budget_depleted")
            )

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

    def parse_retry_after(self, retry_after: str) -> float:
        # urllib3 otherwise silently caps Retry-After at six hours. Only the
        # operator's explicit maximum may limit how long this exporter waits.
        raw = retry_after.strip()
        if raw.isascii() and raw.isdecimal():
            return int(raw)
        try:
            date = parsedate_to_datetime(raw)
        except (TypeError, ValueError, OverflowError):
            raise InvalidHeader(f"Invalid Retry-After header: {retry_after}") from None
        if date.tzinfo is None:
            date = date.replace(tzinfo=UTC)
        return max(0, date.timestamp() - time.time())

    def sleep(self, response=None) -> None:
        if response is not None and response.status == 429 and self.max_retry_after is None:
            delay = self.get_retry_after(response) or RATE_LIMIT_RETRY_SECONDS
            logging.warning(
                "Cloudflare rate limit: waiting %.0fs before retrying the request", delay
            )
            time.sleep(delay)
        else:
            super().sleep(response)

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
        if response is not None and response.status == 429 and self.max_retry_after is None:
            # Rate limiting must not spend the transient-error retry allowance or
            # abandon the current read. Keep history bounded.
            retry = self.new()
        else:
            retry = super().increment(method, url, response, error, _pool, _stacktrace)
        # Only a retried attempt gets here. A final one raises above or returns
        # its response, and CloudflareAPI._request reports it.
        if self.on_retry is not None:
            self.on_retry(url, response, error)
        return retry


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
    """Thin client for the Cloudflare REST and GraphQL APIs."""

    def __init__(
        self,
        api_token: str,
        request_timeout: int = DEFAULT_REQUEST_TIMEOUT,
        base_url: str = API_BASE_URL,
        *,
        max_retry_after_seconds: int = 0,
        page_size: int = 0,
    ):
        api_token = _validate_token(api_token)
        self.base_url = base_url.rstrip("/")
        self.request_timeout = request_timeout
        self.max_retry_after_seconds = max_retry_after_seconds or None
        # Groups per page, at most the plan's maxPageSize; 0 uses the plan's.
        self.page_size = page_size
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
        while True:
            payload = self._request(
                "POST", "/graphql", json={"query": query, "variables": dict(variables)}
            )
            if not payload.get("errors"):
                break
            message = self._redact(f"GraphQL: {format_errors(payload['errors'])}")
            budget = _is_budget_error(payload["errors"])
            self.stats.record("graphql", error="budget_depleted" if budget else "graphql")
            if not budget:
                raise CloudflareAPIError(message)
            if (
                self.max_retry_after_seconds is not None
                and self.max_retry_after_seconds < RATE_LIMIT_RETRY_SECONDS
            ):
                raise RateLimitedError(message)
            logging.warning(
                "Cloudflare GraphQL budget depleted: waiting %ds before retrying the request",
                RATE_LIMIT_RETRY_SECONDS,
            )
            time.sleep(RATE_LIMIT_RETRY_SECONDS)
        data = payload.get("data")
        if not isinstance(data, dict):
            raise self._invalid("graphql", "GraphQL: response contains no data")
        return data


def rfc3339(moment: float) -> str:
    """A Unix time as RFC3339 UTC with a `Z`, in whole seconds."""
    return datetime.fromtimestamp(int(moment), UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_rfc3339(value) -> int:
    """Cloudflare's RFC3339 `Time` as whole Unix seconds; CloudflareAPIError if invalid."""
    if not isinstance(value, str):
        raise CloudflareAPIError("GraphQL: invalid bucket time")
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise CloudflareAPIError(f"GraphQL: invalid bucket time {value!r}") from None
    if moment.tzinfo is None:
        raise CloudflareAPIError(f"GraphQL: bucket time without a time zone {value!r}")
    return int(moment.timestamp())


def bucket_floor(moment: float) -> int:
    """The end of the last bucket that ended at or before `moment`."""
    return int(moment) // BUCKET_SECONDS * BUCKET_SECONDS


@dataclass(frozen=True)
class ZoneSettings:
    fields: frozenset[str]
    max_page_size: int
    max_duration: int
    not_older_than: int


def zone_node(data: dict, zone_id: str) -> dict:
    """Return the single requested zone node or raise CloudflareAPIError."""
    viewer = data.get("viewer")
    zones = viewer.get("zones") if isinstance(viewer, dict) else None
    if not isinstance(zones, list) or len(zones) != 1 or not isinstance(zones[0], dict):
        raise CloudflareAPIError(f"GraphQL: no data returned for zone {zone_id}")
    return zones[0]


def fetch_settings(api: CloudflareAPI, zone_id: str) -> ZoneSettings:
    """Return the dataset fields and limits the zone's plan allows."""
    data = api.graphql(SETTINGS_QUERY, {"zoneTag": zone_id})
    node = zone_node(data, zone_id).get("settings")
    settings = node.get("httpRequestsAdaptiveGroups") if isinstance(node, dict) else None
    if not isinstance(settings, dict) or settings.get("enabled") is not True:
        raise ZonePlanError("httpRequestsAdaptiveGroups is not enabled for the zone")
    fields = settings.get("availableFields")
    if not isinstance(fields, list) or any(not isinstance(f, str) for f in fields):
        raise CloudflareAPIError("httpRequestsAdaptiveGroups returned invalid availableFields")
    limits = {}
    for name in ("maxPageSize", "maxDuration", "notOlderThan"):
        value = settings.get(name)
        if type(value) is not int or value < 1:
            raise CloudflareAPIError(f"httpRequestsAdaptiveGroups returned invalid {name}")
        limits[name] = value
    if limits["maxDuration"] < BUCKET_SECONDS or limits["notOlderThan"] < 2 * BUCKET_SECONDS:
        raise ZonePlanError("httpRequestsAdaptiveGroups does not cover a 5-minute bucket")
    return ZoneSettings(
        frozenset(fields), limits["maxPageSize"], limits["maxDuration"], limits["notOlderThan"]
    )


@dataclass(frozen=True)
class ZonePlan:
    """How one zone is read: its queried dimensions, page size and reach."""

    zone_id: str
    zone_name: str
    # Groups per page of the selections with dimensions.
    page_size: int
    # Queried dimensions of each selection, without the bucket dimension;
    # edge selections are absent when the plan cannot serve the edge dataset.
    dimensions: Mapping[str, tuple[str, ...]]
    # Dimensions the plan offers (names without the `dimensions_` prefix).
    available: frozenset[str]
    # Whether the zone's plan supports each dataset (1) or it is not collected (0).
    supported: Mapping[str, int]
    max_window_buckets: int
    # How far back the dataset can still be queried, relative to now: the limit
    # of every range, proof ranges included.
    reach_seconds: int
    # How far back buckets are read and exported: the reach, or less with
    # CF_EXPORTER_BACKFILL_DAYS.
    history_seconds: int
    # The plan's own page size, for the totals: they have one group per bucket
    # and their whole proof range has to fit one page.
    totals_page_size: int
    # Longest time range of one query.
    max_duration: int

    @property
    def edge(self) -> bool:
        return "edge" in self.dimensions

    @property
    def long_proof_seconds(self) -> int:
        """The widest range totals are read over to find a group."""
        return min(LONG_PROOF_SECONDS, self.max_duration, self.reach_seconds)

    @property
    def proof_room_seconds(self) -> int:
        """The longest proof range that one page and one query can hold with any
        page-size variant."""
        rows = self.totals_page_size - page_variants(self.totals_page_size)
        return max(BUCKET_SECONDS, min(self.max_duration, rows * BUCKET_SECONDS))


def page_variants(size: int) -> int:
    """How many page sizes a selection with pages of `size` groups cycles through."""
    return max(1, min(LIMIT_VARIANTS, size // 2))


def page_variant(size: int, variant: int) -> int:
    """The page size of read number `variant`: Cloudflare caches answers by query
    and variables but not by alias, so that consecutive reads of the same window
    differ slightly."""
    return size - variant % page_variants(size)


def make_plan(
    zone_id: str,
    zone_name: str,
    settings: ZoneSettings,
    schema: Schema,
    page_size: int = 0,
    backfill_days: int = 0,
) -> ZonePlan:
    """The read plan of a zone; CloudflareAPIError if the plan cannot serve reads."""
    fields = settings.fields
    missing = REQUIRED_FIELDS - fields
    if missing:
        raise ZonePlanError(f"fields not available for the zone: {', '.join(sorted(missing))}")
    available = frozenset(f.removeprefix("dimensions_") for f in fields if f.startswith("dim"))
    exported = {kind: schema.dimensions(kind) for kind in DATASETS}
    needed = FILTER_FIELDS["edge"] | {
        f"dimensions_{d}" for d in REQUIRED_DIMENSIONS["edge"] if d in exported["edge"].values()
    }
    edge = needed <= fields
    if not edge:
        logging.warning(
            "Zone %s: plan does not offer %s; the edge dataset is not collected",
            zone_name,
            ", ".join(sorted(needed - fields)),
        )
    union: list[str] = []
    for kind in GROUP_DATASETS:
        for dimension in exported[kind].values():
            if dimension in available and dimension not in union:
                union.append(dimension)
    dimensions = {"groups": tuple(union), "totals": ()}
    if edge:
        dimensions["edge"] = tuple(d for d in exported["edge"].values() if d in available)
        dimensions["edge_totals"] = ()
    max_page = min(page_size or settings.max_page_size, settings.max_page_size)
    reach = max(BUCKET_SECONDS, settings.not_older_than - RETENTION_MARGIN_SECONDS)
    history = min(reach, backfill_days * DAY_SECONDS) if backfill_days else reach
    return ZonePlan(
        zone_id=zone_id,
        zone_name=zone_name,
        page_size=max_page,
        dimensions=MappingProxyType(dimensions),
        available=available,
        supported=MappingProxyType({**dict.fromkeys(GROUP_DATASETS, 1), "edge": int(edge)}),
        max_window_buckets=max(1, min(MAX_WINDOW_BUCKETS, settings.max_duration // BUCKET_SECONDS)),
        reach_seconds=reach,
        history_seconds=history,
        totals_page_size=settings.max_page_size,
        max_duration=settings.max_duration,
    )


def build_query(dimensions: Mapping[str, Sequence[str]], *, paging: bool = False) -> str:
    """An aliased query of the given selections (alias -> dimensions without the
    bucket dimension), each grouped by bucket.

    Selections with dimensions share `$since`, `$until` and `$limit`. Every
    totals selection has its own `$<alias>Since` and `$<alias>Until`, its proof
    range, and they share `$totalsLimit`. With `paging`, the one selection is
    ordered by its complete dimension tuple and filtered by `$after`.
    """
    selections = []
    variables = ["$zoneTag: string!"]
    for alias, fields in dimensions.items():
        values, source, _ = SELECTIONS[alias]
        extra = f", {source}" if source else ""
        if alias in TOTAL_SELECTIONS:
            since, until, limit = f"${alias}Since", f"${alias}Until", "$totalsLimit"
        else:
            since, until, limit = "$since", "$until", "$limit"
        for declaration in (f"{since}: Time!", f"{until}: Time!", f"{limit}: uint64!"):
            if declaration not in variables:
                variables.append(declaration)
        if paging:
            # Within one second the bucket is fixed; the dimensions order the groups.
            extra += ", AND: [$after]"
            order = ", ".join(f"{d}_ASC" for d in fields)
        else:
            order = f"{BUCKET_DIMENSION}_ASC"
        selections.append(f"""
      {alias}: httpRequestsAdaptiveGroups(
        limit: {limit}
        filter: {{datetime_geq: {since}, datetime_lt: {until}{extra}}}
        orderBy: [{order}]
      ) {{ {values} dimensions {{ {" ".join((BUCKET_DIMENSION, *fields))} }} }}""")
    if paging:
        variables.append("$after: ZoneHttpRequestsAdaptiveGroupsFilter_InputObject!")
    # GraphQL rejects declarations of unused variables, so only the used ones.
    return f"""
query Buckets({", ".join(variables)}) {{
  viewer {{
    zones(filter: {{zoneTag: $zoneTag}}) {{
      {"".join(selections)}
    }}
  }}
}}
"""


def checked_number(alias: str, group, value_of: Callable[[dict], object]) -> float:
    """Return a group's count, visits or sample interval, rejecting anything but a
    finite number >= 0."""
    if not isinstance(group, dict):
        raise CloudflareAPIError(f"GraphQL: {alias} contains an invalid group")
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
        raise CloudflareAPIError(f"GraphQL: {alias} contains an invalid number")
    return value


def group_list(alias: str, groups) -> list:
    """Return a group list, rejecting missing or non-list data with CloudflareAPIError."""
    if not isinstance(groups, list):
        raise CloudflareAPIError(f"GraphQL: {alias} contains no valid group list")
    return groups


def _count(group: dict):
    return group.get("count")


def _visits(group: dict):
    return (group.get("sum") or {}).get("visits")


def _sample_interval(group: dict):
    return (group.get("avg") or {}).get("sampleInterval")


def validate_group(alias: str, group, queried: Collection[str]) -> int:
    """Validate one group of a selection and return its bucket start."""
    count = checked_number(alias, group, _count)
    if "visits" in SELECTIONS[alias][0]:
        checked_number(alias, group, _visits)
    interval = checked_number(alias, group, _sample_interval)
    if count > 0 and interval <= 0:
        raise CloudflareAPIError(f"GraphQL: {alias} contains an invalid sample interval")
    dimensions = group.get("dimensions")
    if not isinstance(dimensions, dict):
        raise CloudflareAPIError(f"GraphQL: {alias} contains invalid dimensions")
    kind = SELECTIONS[alias][2]
    types = DIMENSION_TYPES.get(kind, {})
    required = REQUIRED_DIMENSIONS.get(kind, frozenset())
    for dimension in queried:
        value = dimensions.get(dimension)
        if value is None:
            if dimension in required:
                raise CloudflareAPIError(f"GraphQL: {alias} contains a group without {dimension}")
            continue
        expected = types.get(dimension)
        if expected is not None and (
            type(value) is not expected or (expected is int and not 0 <= value <= UINT16_MAX)
        ):
            raise CloudflareAPIError(f"GraphQL: {alias} contains invalid {dimension}")
    start = parse_rfc3339(dimensions.get(BUCKET_DIMENSION))
    if start % BUCKET_SECONDS:
        raise CloudflareAPIError(f"GraphQL: {alias} contains an unaligned bucket")
    return start


@dataclass
class BucketData:
    """Validated groups of one bucket, by selection."""

    groups: list[dict] = field(default_factory=list)
    edge: list[dict] = field(default_factory=list)
    totals: dict | None = None
    edge_totals: dict | None = None


@dataclass
class ReadStats:
    """How a read went: to size the next window and to judge its selections."""

    requests: int = 0
    split: bool = False
    fill: float = 0.0
    # Selection -> "full", "sampled", "empty" or "split", see answer_state().
    states: dict[str, str] = field(default_factory=dict)
    # Selection with dimensions -> ends of the buckets with groups from an answer
    # at a reduced resolution: among the full parts of a split bucket their
    # sample interval would go unnoticed.
    reduced: dict[str, set[int]] = field(default_factory=dict)
    # Totals selection -> groups in its answer, those outside the window included.
    groups: dict[str, int] = field(default_factory=dict)
    # Totals selection -> the range its answer covers, and the end of the newest
    # bucket with events in it.
    ranges: dict[str, tuple[int, int]] = field(default_factory=dict)
    newest: dict[str, int] = field(default_factory=dict)
    # Totals selection -> answers in a row without any group over the long proof range.
    looks: dict[str, int] = field(default_factory=dict)


def page_cursor(dimensions: Sequence[str], last: Mapping) -> dict:
    """The filter for the page after the group with the dimension values `last`, in
    the ascending order of the complete dimension tuple: each OR branch
    advances one dimension after fixing its predecessors."""
    prefix: dict = {}
    branches = []
    for dimension in dimensions:
        branches.append({**prefix, f"{dimension}_gt": last[dimension]})
        prefix[dimension] = last[dimension]
    return {"OR": branches}


def answer_state(groups: Sequence[dict]) -> str:
    """What one answer tells about the data it was computed from, see
    SAMPLED_INTERVAL: "full" with a group below it, which proves the full data
    for the whole answer; "sampled" with groups that are all above it; "empty"
    without groups, which proves nothing."""
    intervals = [_sample_interval(group) for group in groups if _count(group) > 0]
    if not intervals:
        return "empty"
    return "full" if min(intervals) < SAMPLED_INTERVAL else "sampled"


def read_window(
    api: CloudflareAPI,
    plan: ZonePlan,
    start: int,
    end: int,
    aliases: Sequence[str] | None = None,
    variant: int = 0,
    proof: Mapping[str, tuple[int, int]] | None = None,
) -> tuple[dict[int, BucketData], ReadStats]:
    """Read the selections (all by default) of the buckets in [start, end) and return
    them by bucket end, with the state of every selection's answer.

    A full page of a selection with dimensions is split into halves on bucket
    boundaries, then within the bucket down to one second, which is paginated
    by its dimension tuple. Each totals selection is read over its `proof`
    range, which contains the window and by default is the window; its groups
    outside the window only tell how the selection was answered. An answer in
    several parts has the state "split": no part proves another. `variant`
    lowers the page sizes slightly so that the request is not answered from
    Cloudflare's cache of an earlier one.
    """
    buckets = {b: BucketData() for b in range(start + BUCKET_SECONDS, end + 1, BUCKET_SECONDS)}
    stats = ReadStats()
    limits = {
        alias: page_variant(
            plan.totals_page_size if alias in TOTAL_SELECTIONS else plan.page_size, variant
        )
        for alias in plan.dimensions
    }
    answers: dict[str, list[str]] = {}

    def proof_range(alias: str) -> tuple[int, int]:
        """The alias's proof range, cut to what one page and one query can hold:
        the window is kept, then the time before it."""
        since, until = (proof or {}).get(alias, (start, end))
        since, until = min(since, start), max(until, end)
        room = min((limits[alias] - 1) * BUCKET_SECONDS, plan.max_duration)
        if until - since > room:
            since = max(since, min(start, end - room))
            until = min(until, max(end, since + room))
        return since, until

    def fetch(
        names: Sequence[str],
        since: int,
        until: int,
        after: dict | None = None,
        ranges: Mapping[str, tuple[int, int]] | None = None,
        limit: int | None = None,
    ):
        stats.requests += 1
        variables: dict = {"zoneTag": plan.zone_id}
        for alias in names:
            size = limits[alias] if limit is None else limit
            if alias in TOTAL_SELECTIONS:
                alias_since, alias_until = (ranges or {}).get(alias, (since, until))
                variables[f"{alias}Since"] = rfc3339(alias_since)
                variables[f"{alias}Until"] = rfc3339(alias_until)
                variables["totalsLimit"] = size
            else:
                variables.update(since=rfc3339(since), until=rfc3339(until), limit=size)
        if after is not None:
            variables["after"] = after
        query = build_query({a: plan.dimensions[a] for a in names}, paging=after is not None)
        return zone_node(api.graphql(query, variables), plan.zone_id)

    def validated(alias: str, groups, since: int, until: int) -> list[tuple[int, dict]]:
        queried = (BUCKET_DIMENSION, *plan.dimensions[alias])
        result = []
        for group in group_list(alias, groups):
            bucket_start = validate_group(alias, group, queried)
            # The group's bucket must overlap the queried window.
            if not (bucket_start < until and bucket_start + BUCKET_SECONDS > since):
                raise CloudflareAPIError(f"GraphQL: {alias} contains a bucket outside the window")
            result.append((bucket_start + BUCKET_SECONDS, group))
        return result

    def assign(alias: str, groups: list[tuple[int, dict]]) -> None:
        for bucket_end, group in groups:
            data = buckets[bucket_end]
            if alias in GROUP_SELECTIONS:
                getattr(data, alias).append(group)
            elif getattr(data, alias) is not None:
                raise CloudflareAPIError(f"GraphQL: {alias} repeats a bucket")
            else:
                setattr(data, alias, group)

    def paginate(alias: str, since: int, until: int) -> list[tuple[int, dict]]:
        """Read a saturated second by the complete dimension tuple. Counts can tie and
        change, so they cannot be pagination keys. Each OR branch advances one
        dimension after fixing its predecessors; the filters stay unchanged."""
        tuple_ = plan.dimensions[alias]
        # Cloudflare rejects an empty object inside AND, so the first ordered
        # page repeats the already-required lower time bound as its predicate.
        after: dict = {"datetime_geq": rfc3339(since)}
        previous: tuple | None = None
        result: list[tuple[int, dict]] = []
        while True:
            page, limit = page_of(alias, since, until, after)
            for _, group in page:
                raw = group["dimensions"]
                key = tuple(raw.get(d) for d in tuple_)
                if any(value is None for value in key):
                    raise CloudflareAPIError(f"GraphQL: {alias} pagination lacks a dimension")
                try:
                    advances = previous is None or key > previous
                except TypeError:
                    advances = False
                if not advances:
                    raise CloudflareAPIError(f"GraphQL: {alias} pagination did not advance")
                previous = key
            note(alias, page)
            result.extend(page)
            if len(page) < limit:
                return result
            after = page_cursor(tuple_, page[-1][1]["dimensions"])

    def note(alias: str, groups: list[tuple[int, dict]]) -> None:
        """Record how one answer of a selection was computed."""
        state = answer_state([group for _, group in groups])
        answers.setdefault(alias, []).append(state)
        if state == "sampled":
            stats.reduced.setdefault(alias, set()).update(end for end, _ in groups)

    def page_of(
        alias: str, since: int, until: int, after: dict | None = None
    ) -> tuple[list[tuple[int, dict]], int]:
        """A further page of a selection and the page size it was read with. A page
        answered at a reduced resolution is asked for again at once, with a page
        size Cloudflare has no cached answer for: a split read has many parts,
        and reading all of them again because of one would rarely end."""
        step = LIMIT_VARIANTS if limits[alias] > PART_READS * LIMIT_VARIANTS else 1
        for attempt in range(PART_READS):
            limit = max(1, limits[alias] - attempt * step)
            node = fetch((alias,), since, until, after, limit=limit)
            page = validated(alias, node.get(alias), since, until)
            if answer_state([group for _, group in page]) != "sampled":
                break
        return page, limit

    def collect(alias: str, since: int, until: int, groups=None) -> None:
        limit = limits[alias]
        if groups is None:
            groups, limit = page_of(alias, since, until)
        # Without dimensions a selection has at most one group per bucket, so a
        # page of one bucket is complete whatever the page size.
        dimensionless = not plan.dimensions[alias] and until - since <= BUCKET_SECONDS
        if len(groups) < limit or dimensionless:
            note(alias, groups)
            assign(alias, groups)
            return
        stats.split = True
        if until - since <= SLICE_RESOLUTION_SECONDS:
            assign(alias, paginate(alias, since, until))
            return
        if until - since > BUCKET_SECONDS:
            middle = since + (until - since) // BUCKET_SECONDS // 2 * BUCKET_SECONDS
        else:
            middle = since + (until - since) // 2
        # The validated parent page is discarded; only its halves count, also
        # for how the selection was answered.
        del groups
        collect(alias, since, middle)
        collect(alias, middle, until)

    aliases = tuple(plan.dimensions) if aliases is None else tuple(aliases)
    ranges = {alias: proof_range(alias) for alias in aliases if alias in TOTAL_SELECTIONS}
    node = fetch(aliases, start, end, ranges=ranges)
    for alias in aliases:
        if alias in TOTAL_SELECTIONS:
            since, until = ranges[alias]
            groups = validated(alias, node.get(alias), since, until)
            stats.groups[alias] = len(groups)
            stats.ranges[alias] = since, until
            events = [bucket_end for bucket_end, group in groups if _count(group) > 0]
            if events:
                stats.newest[alias] = max(events)
            if len(groups) < limits[alias]:
                answers[alias] = [answer_state([group for _, group in groups])]
                assign(alias, [(e, group) for e, group in groups if start < e <= end])
            else:
                # Only a window that does not fit a page by itself gets here:
                # it is read in parts, and its range is the window.
                collect(alias, since, until, groups)
        else:
            groups = validated(alias, node.get(alias), start, end)
            stats.fill = max(stats.fill, len(groups) / limits[alias])
            collect(alias, start, end, groups)
    for alias in aliases:
        parts = answers[alias]
        if "sampled" in parts:
            stats.states[alias] = "sampled"
        else:
            stats.states[alias] = parts[0] if len(parts) == 1 else "split"
    return buckets, stats


def render_labels(names: Sequence[str], values: Sequence[str]) -> str:
    """Prometheus text-format label set, escaped."""
    if not names:
        return ""
    parts = []
    for name, value in zip(names, values, strict=True):
        escaped = value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
        parts.append(f'{name}="{escaped}"')
    return "{" + ",".join(parts) + "}"


def _label_values(
    zone_name: str,
    dimension_map: Mapping[str, str],
    dimensions: Mapping,
    available: Collection[str],
) -> LabelValues:
    """Label values of a group: the zone name, then one per exported dimension."""
    values = [zone_name]
    for label, dimension in dimension_map.items():
        # Dimensions the zone's plan does not offer are not queried at all.
        value = dimensions.get(dimension) if dimension in available else None
        value = UNKNOWN if value is None else str(value)
        if label == "client_request_referer" and value == "":
            value = "direct"
        values.append(value)
    return tuple(values)


def effective_interval(groups: Sequence[dict]) -> float:
    """Events per stored row of a selection; 1.0 without sampling, 0 without groups.

    `count` sums the rows' sample intervals and `avg { sampleInterval }` is their
    mean, so count / mean is the number of rows a group was estimated from.
    """
    events = rows = 0.0
    for group in groups:
        count, interval = _count(group), _sample_interval(group)
        if count > 0:
            events += count
            rows += count / interval
    return events / rows if rows else 0.0


@dataclass(frozen=True)
class BucketResult:
    bucket_end: int
    # (family, rendered labels, value), sorted.
    samples: tuple[tuple[str, str, float], ...]
    signature: str
    # None when verified. Otherwise "sampled": an answer came at a reduced
    # resolution or is not proven complete; or "inconsistent": the selections
    # contradict each other; and why.
    problem: str | None = None
    reason: str = ""
    # Whether the same answer may be accepted unverified after a while: only
    # one whose totals have groups, all at a high sample interval, one of them
    # for this bucket, and add up. That is how Cloudflare answers a zone it
    # stores at such an interval; an empty or contradictory answer, or one
    # without the bucket, proves nothing however often it comes.
    holdable: bool = False
    # Requests and client requests of the zone in the bucket.
    requests: float = 0
    client_requests: float = 0


def evaluate(
    plan: ZonePlan,
    schema: Schema,
    bucket_end: int,
    data: BucketData,
    states: Mapping[str, str],
    reduced: Mapping[str, Collection[int]] | None = None,
) -> BucketResult:
    """Aggregate a bucket into samples and decide whether it can be trusted.

    `states` are the states of the selections' answers (see answer_state();
    "idle" marks totals of a zone without any traffic, "missing" totals
    without the events known to lie in their range) and `reduced` the buckets
    with groups from a part answered at a reduced resolution, by selection.
    The bucket is verified when its totals are proven to come from the full
    data, its groups add up to them, no group shows a reduced resolution, and
    the client requests do not exceed all requests.
    """
    totals = data.totals or {}
    count = _count(totals) or 0
    visits = _visits(totals) or 0
    edge_count = _count(data.edge_totals or {}) or 0
    problems: list[str] = []
    unproven: list[str] = []
    # A reduced answer without a group for the bucket may have left out its
    # events; only one that shows the bucket may stand after the hold.
    holdable = count > 0

    def compare(name: str, actual: float, expected: float) -> None:
        if not math.isclose(actual, expected, rel_tol=SUM_TOLERANCE, abs_tol=SUM_TOLERANCE):
            problems.append(f"{name} {actual:g} != totals {expected:g}")

    compare("groups count", sum(_count(g) for g in data.groups), count)
    compare("groups visits", sum(_visits(g) for g in data.groups), visits)
    if plan.edge:
        compare("edge count", sum(_count(g) for g in data.edge), edge_count)
        if edge_count > count:
            problems.append(f"edge totals {edge_count:g} above totals {count:g}")
    for total, alias in TOTAL_SELECTIONS.items():
        if total not in plan.dimensions:
            continue
        state = states.get(total)
        if state == "sampled":
            unproven.append(f"{total} at a reduced resolution")
        elif state == "missing":
            unproven.append(f"{total} without events known in its range")
            holdable = False
        elif state not in ("full", "idle"):
            unproven.append(f"{total} not proven complete")
            holdable = False
        # A selection in one answer with a group from the full data is all from
        # the full data; otherwise every bucket's groups have to show it.
        if states.get(alias) == "full":
            continue
        interval = effective_interval(getattr(data, alias))
        if interval >= SAMPLED_INTERVAL:
            unproven.append(f"{alias} sample interval {interval:.2f}")
        elif bucket_end in (reduced or {}).get(alias, ()):
            unproven.append(f"{alias} partly at a reduced resolution")

    samples: dict[tuple[str, str], float] = {}
    for kind in (*GROUP_DATASETS, "edge"):
        if not plan.supported[kind]:
            continue
        name = METRICS[kind]
        labels = schema.labels(name)
        dimension_map = schema.dimensions(kind)
        value_of = _visits if kind == "visits" else _count
        for group in data.edge if kind == "edge" else data.groups:
            value = value_of(group)
            if not value:
                continue
            values = _label_values(
                plan.zone_name, dimension_map, group["dimensions"], plan.available
            )
            key = (name, render_labels(labels, values))
            samples[key] = samples.get(key, 0) + value
    zone = render_labels(("zone_name",), (plan.zone_name,))
    # Zone totals are served for every accepted bucket, also without traffic.
    samples[(ZONE_REQUESTS, zone)] = count
    samples[(ZONE_VISITS, zone)] = visits
    ordered = tuple(sorted((name, labels, value) for (name, labels), value in samples.items()))
    digest = hashlib.sha256(repr(ordered).encode()).hexdigest()[:DIGEST_LENGTH]
    result = BucketResult(bucket_end, ordered, digest, requests=count, client_requests=edge_count)
    if unproven:
        return replace(
            result,
            problem="sampled",
            reason="; ".join(unproven),
            holdable=holdable and not problems,
        )
    if problems:
        return replace(result, problem="inconsistent", reason="; ".join(problems))
    return result


SCHEMA = """
CREATE TABLE buckets (
    zone_id TEXT NOT NULL,
    bucket_end INTEGER NOT NULL,
    -- 0 pending, 1 accepted
    state INTEGER NOT NULL DEFAULT 0,
    -- reads that did not verify
    attempts INTEGER NOT NULL DEFAULT 0,
    -- reads that failed
    errors INTEGER NOT NULL DEFAULT 0,
    next_attempt REAL NOT NULL DEFAULT 0,
    signature TEXT,
    signature_since REAL,
    accepted_at REAL,
    unverified INTEGER NOT NULL DEFAULT 0,
    -- complete consuming /analytics responses with the bucket since acceptance
    served INTEGER NOT NULL DEFAULT 0,
    delivered INTEGER NOT NULL DEFAULT 0,
    samples INTEGER NOT NULL DEFAULT 0,
    recheck_due REAL,
    -- the zone's requests and client requests in an accepted bucket, kept
    -- after delivery: a range with events must not be answered empty
    requests REAL NOT NULL DEFAULT 0,
    client_requests REAL NOT NULL DEFAULT 0,
    -- labels and dimensions the samples were rendered with
    layout TEXT,
    PRIMARY KEY (zone_id, bucket_end)
) WITHOUT ROWID;
CREATE INDEX buckets_undelivered ON buckets (delivered, state, bucket_end);
CREATE TABLE samples (
    zone_id TEXT NOT NULL,
    bucket_end INTEGER NOT NULL,
    family TEXT NOT NULL,
    labels TEXT NOT NULL,
    value REAL NOT NULL
);
CREATE INDEX samples_bucket ON samples (zone_id, bucket_end);
"""
# Errors of an operation that say nothing about the database's health.
STORAGE_BUGS = (sqlite3.IntegrityError, sqlite3.ProgrammingError, sqlite3.InterfaceError)
STORAGE_BUSY = (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
# Selection of the totals -> the column with its events in an accepted bucket.
EVENT_COLUMNS = {"totals": "requests", "edge_totals": "client_requests"}
SQLITE_MAGIC = b"SQLite format 3\x00"


def _primary_code(error: BaseException) -> int | None:
    """SQLite reports extended codes such as SQLITE_BUSY_RECOVERY; their low byte
    is the primary code."""
    code = getattr(error, "sqlite_errorcode", None)
    return None if code is None else code & 0xFF


def storage_busy(error: BaseException) -> bool:
    """Whether another process holds the database, whose data is then fine."""
    return _primary_code(error) in STORAGE_BUSY


def storage_full(error: BaseException) -> bool:
    """Whether the disk is full: the database is fine, there is no room to write."""
    return _primary_code(error) == sqlite3.SQLITE_FULL


def recognizably_ours(path: str) -> bool:
    """Whether a file that SQLite cannot use may be replaced: an SQLite file with
    the exporter's application id in its header, or one of zero bytes only, as
    a crash can leave. Anything else, damaged or not, may be someone's data."""
    try:
        with open(path, "rb") as file:
            header = file.read(SQLITE_HEADER_BYTES)
            offset = SQLITE_APPLICATION_ID_OFFSET
            if header.startswith(SQLITE_MAGIC) and len(header) >= offset + 4:
                return int.from_bytes(header[offset : offset + 4], "big") == APPLICATION_ID
            while header:
                if header.strip(b"\x00"):
                    return False
                header = file.read(1 << 16)
    except OSError:
        return False
    return True


def file_identity(path: str) -> tuple[int, int] | None:
    try:
        stat = os.stat(path)
    except OSError:
        return None
    return stat.st_dev, stat.st_ino


@dataclass(frozen=True)
class BucketRow:
    zone_id: str
    bucket_end: int
    state: int
    attempts: int
    errors: int
    signature: str | None
    signature_since: float | None
    layout: str | None = None


class ForeignDatabaseError(Exception):
    """The database file belongs to another application and is left untouched."""


class Storage:
    """SQLite state of buckets and the samples not yet delivered.

    Everything stored can be read again from Cloudflare, so the storage heals
    itself instead of needing care. A missing file is created. A corrupt or
    outdated database of the exporter, one that fails an operation, and one
    removed or replaced while running are recreated, and the exporter reads
    the history again. While the path cannot be used at all, the state is kept
    in memory and the file is tried again regularly. What is not recognizably
    the exporter's is never deleted, a lock held by another process and a full
    disk never cause a wipe. All access goes through one connection under a
    lock.
    """

    def __init__(self, path: str | None, monotonic: Callable[[], float] = time.monotonic):
        self.path = path
        self.persistent = False
        self.resets = 0
        self._lock = threading.RLock()
        self._monotonic = monotonic
        # Until when the disk counts as full after a write failed for lack of space.
        self._full_until = 0.0
        self._identity: tuple[int, int] | None = None
        # The opened database's own write-ahead files, by suffix.
        self._sidecars: dict[str, tuple[int, int] | None] = {}
        # Why the file is not used, to log a lasting problem only once.
        self._problem: str | None = None
        self._retry_at = 0.0
        self._conn = self._open()

    # Opening and healing

    def _remove_files(self) -> None:
        for suffix in ("", "-wal", "-shm", "-journal"):
            with suppress(FileNotFoundError):
                os.remove(self.path + suffix)

    @staticmethod
    def _inspect(conn: sqlite3.Connection) -> str | None:
        """Why an opened database has to be recreated, or None if it can be used.
        A healthy database of another application raises ForeignDatabaseError."""
        if not conn.execute("SELECT count(*) FROM sqlite_master").fetchone()[0]:
            return None
        application = conn.execute("PRAGMA application_id").fetchone()[0]
        if application != APPLICATION_ID:
            raise ForeignDatabaseError(
                f"it is a database of another application (application_id {application})"
            )
        check = conn.execute("PRAGMA quick_check").fetchone()
        if not check or check[0] != "ok":
            return "integrity check failed"
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version != SCHEMA_VERSION:
            return f"schema version {version}, expected {SCHEMA_VERSION}"
        return None

    @staticmethod
    def _prepare(conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        if not conn.execute("SELECT count(*) FROM sqlite_master").fetchone()[0]:
            conn.executescript(
                f"BEGIN;{SCHEMA}PRAGMA application_id={APPLICATION_ID};"
                f"PRAGMA user_version={SCHEMA_VERSION};COMMIT;"
            )
        conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS pick "
            "(zone_id TEXT, bucket_end INTEGER, PRIMARY KEY (zone_id, bucket_end))"
        )

    @staticmethod
    def _probe(conn: sqlite3.Connection) -> None:
        """Prove that the database can be written. One that can only be read, as on
        a file system that became read-only, would fail every operation later."""
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        conn.execute("COMMIT")

    @staticmethod
    def _connect(target: str) -> sqlite3.Connection:
        return sqlite3.connect(
            target, timeout=STORAGE_LOCK_SECONDS, check_same_thread=False, isolation_level=None
        )

    def _open_file(self) -> sqlite3.Connection:
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        for attempt in range(2):
            conn = self._connect(self.path)
            try:
                try:
                    problem = self._inspect(conn)
                    if problem is None:
                        # The schema and the application id are written before
                        # the switch to the write-ahead log, so that the file's
                        # own header shows whose it is from the first write on.
                        self._prepare(conn)
                        conn.execute("PRAGMA journal_mode=WAL")
                        self._probe(conn)
                except sqlite3.DatabaseError as e:
                    # The data is fine; a full disk is not repaired by a new file.
                    if storage_busy(e) or storage_full(e) or isinstance(e, STORAGE_BUGS):
                        raise
                    problem = str(e)
                if problem is None:
                    self._identity = file_identity(self.path)
                    self._sidecars = {
                        suffix: file_identity(self.path + suffix) for suffix in ("-wal", "-shm")
                    }
                    return conn
            except BaseException:
                conn.close()
                raise
            conn.close()
            if attempt:
                raise sqlite3.DatabaseError(problem)
            if not recognizably_ours(self.path):
                raise ForeignDatabaseError(
                    f"{problem}, and the file is not recognizably the exporter's database"
                )
            try:
                self._remove_files()
            except OSError as e:
                # Neither usable nor replaceable: the caller keeps the state in memory.
                raise OSError(f"{problem}; it cannot be removed: {e}") from e
            logging.warning("Storage %s was unusable (%s); creating a new one", self.path, problem)
            self.resets += 1
        raise AssertionError("unreachable")  # pragma: no cover

    def _open_memory(self, reason: str) -> sqlite3.Connection:
        if reason != self._problem:
            logging.error(
                "Storage %s cannot be used (%s); keeping the state in memory and trying "
                "the file again every %ds",
                self.path,
                reason,
                STORAGE_RETRY_SECONDS,
            )
            self._problem = reason
        self.persistent = False
        self._identity = None
        self._retry_at = self._monotonic() + STORAGE_RETRY_SECONDS
        conn = self._connect(":memory:")
        self._prepare(conn)
        return conn

    def _open(self) -> sqlite3.Connection:
        if not self.path:
            self.persistent = False
            conn = self._connect(":memory:")
            self._prepare(conn)
            return conn
        try:
            conn = self._open_file()
        except (OSError, sqlite3.Error, ForeignDatabaseError) as e:
            return self._open_memory(str(e))
        self.persistent = True
        self._problem = None
        return conn

    def _close(self) -> None:
        with suppress(sqlite3.Error):
            self._conn.close()

    def close(self) -> None:
        """Close the database at shutdown; SQLite then folds its write-ahead log
        into the file."""
        with self._lock:
            self._close()

    def _reset(self, reason: str) -> None:
        logging.error("Storage failed (%s); recreating it, history will be read again", reason)
        self.resets += 1
        self._close()
        if self.persistent:
            try:
                self._remove_files()
            except OSError as e:
                logging.error("Storage %s cannot be removed: %s", self.path, e)
        self._conn = self._open()

    def _heal(self) -> None:
        """Notice a database file removed or replaced while running, and return from
        memory to the file once it can be used."""
        if self.persistent:
            if file_identity(self.path) != self._identity:
                logging.warning("Storage %s was removed or replaced; opening it again", self.path)
                self.resets += 1
                self._close()
                # The write-ahead log of the database that is gone must not be
                # applied to whatever file is at the path now. Only this
                # database's own files are removed: others came with the new
                # file and may hold its data.
                for suffix, identity in self._sidecars.items():
                    if identity is not None and file_identity(self.path + suffix) == identity:
                        with suppress(OSError):
                            os.remove(self.path + suffix)
                self._conn = self._open()
        elif self.path and self._monotonic() >= self._retry_at:
            self._retry_at = self._monotonic() + STORAGE_RETRY_SECONDS
            try:
                conn = self._open_file()
            except (OSError, sqlite3.Error, ForeignDatabaseError) as e:
                if str(e) != self._problem:
                    logging.error("Storage %s still cannot be used (%s)", self.path, e)
                    self._problem = str(e)
                return
            logging.warning(
                "Storage %s can be used again; leaving memory, buckets will be read again",
                self.path,
            )
            self._close()
            self._conn, self.persistent, self._problem = conn, True, None

    @property
    def full(self) -> bool:
        """Whether a write failed for lack of disk space within the retry interval."""
        return self._monotonic() < self._full_until

    def _run(self, operation: Callable[[sqlite3.Connection], object]):
        """Run an operation. A database error recreates the storage and retries; if the
        new file fails as well, the state moves to memory."""
        with self._lock:
            self._heal()
            for last in (False, True):
                try:
                    return operation(self._conn)
                except sqlite3.DatabaseError as e:
                    with suppress(sqlite3.Error):
                        if self._conn.in_transaction:
                            self._conn.execute("ROLLBACK")
                    if storage_full(e):
                        # A new file would fill the disk again after the same
                        # history: keep the data and let the reads wait.
                        if not self.full:
                            logging.error(
                                "Storage %s: the disk is full (%s); reads wait", self.path, e
                            )
                        self._full_until = self._monotonic() + STORAGE_RETRY_SECONDS
                        raise
                    # A bug or another process holding the lock: the data is fine.
                    if isinstance(e, STORAGE_BUGS) or storage_busy(e):
                        raise
                    if not self.persistent:
                        raise
                    if last:
                        self._close()
                        self._conn = self._open_memory(str(e))
                        return operation(self._conn)
                    self._reset(str(e))
        raise AssertionError("unreachable")  # pragma: no cover

    @staticmethod
    @contextmanager
    def _transaction(conn: sqlite3.Connection) -> Iterator[None]:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            with suppress(sqlite3.Error):
                conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")

    # Buckets

    def ensure(self, zone_id: str, first_end: int, last_end: int) -> int:
        """Add pending buckets so that every bucket end in [first_end, last_end] exists,
        and return how many were added. Usually only the edges are missing; a
        gap, as rows lost with a zone or a database leave it, is filled too."""

        def operation(conn):
            expected = (last_end - first_end) // BUCKET_SECONDS + 1
            low, high, count = conn.execute(
                "SELECT min(bucket_end), max(bucket_end), count(*) FROM buckets "
                "WHERE zone_id = ? AND bucket_end BETWEEN ? AND ?",
                (zone_id, first_end, last_end),
            ).fetchone()
            if count >= expected:
                return 0
            if count and count == (high - low) // BUCKET_SECONDS + 1:
                ranges = [(first_end, low - BUCKET_SECONDS), (high + BUCKET_SECONDS, last_end)]
            else:
                ranges = [(first_end, last_end)]
            with self._transaction(conn):
                conn.executemany(
                    "INSERT OR IGNORE INTO buckets (zone_id, bucket_end) VALUES (?, ?)",
                    [
                        (zone_id, end)
                        for start, stop in ranges
                        for end in range(start, stop + 1, BUCKET_SECONDS)
                    ],
                )
            return expected - count

        return self._run(operation)

    def pending(
        self, zone_id: str, now: float, low: int, high: int, limit: int
    ) -> list[tuple[int, int, int]]:
        """(bucket end, attempts, errors) of pending buckets in [low, high] that are due,
        newest first."""
        return self._run(
            lambda conn: conn.execute(
                "SELECT bucket_end, attempts, errors FROM buckets WHERE zone_id = ? AND state = 0 "
                "AND next_attempt <= ? AND bucket_end BETWEEN ? AND ? "
                "ORDER BY bucket_end DESC LIMIT ?",
                (zone_id, now, low, high, limit),
            ).fetchall()
        )

    def rechecks(self, zone_id: str, now: float, limit: int) -> list[int]:
        """Accepted buckets whose recheck is due, newest first."""
        return [
            row[0]
            for row in self._run(
                lambda conn: conn.execute(
                    "SELECT bucket_end FROM buckets WHERE zone_id = ? AND state = 1 "
                    "AND recheck_due IS NOT NULL AND recheck_due <= ? "
                    "ORDER BY bucket_end DESC LIMIT ?",
                    (zone_id, now, limit),
                ).fetchall()
            )
        ]

    def bucket(self, zone_id: str, bucket_end: int) -> BucketRow | None:
        row = self._run(
            lambda conn: conn.execute(
                "SELECT zone_id, bucket_end, state, attempts, errors, signature, signature_since, "
                "layout FROM buckets WHERE zone_id = ? AND bucket_end = ?",
                (zone_id, bucket_end),
            ).fetchone()
        )
        return BucketRow(*row) if row else None

    def accept(
        self,
        zone_id: str,
        result: BucketResult,
        now: float,
        *,
        unverified: bool = False,
        recheck_due: float | None = None,
        layout: str | None = None,
    ) -> None:
        """Store a bucket's samples, rendered with `layout`, and serve them until
        delivered."""

        def operation(conn):
            with self._transaction(conn):
                conn.execute(
                    "DELETE FROM samples WHERE zone_id = ? AND bucket_end = ?",
                    (zone_id, result.bucket_end),
                )
                conn.executemany(
                    "INSERT INTO samples (zone_id, bucket_end, family, labels, value) "
                    "VALUES (?, ?, ?, ?, ?)",
                    [(zone_id, result.bucket_end, *sample) for sample in result.samples],
                )
                conn.execute(
                    "INSERT INTO buckets (zone_id, bucket_end) VALUES (?, ?) "
                    "ON CONFLICT DO NOTHING",
                    (zone_id, result.bucket_end),
                )
                conn.execute(
                    "UPDATE buckets SET state = 1, signature = ?, signature_since = ?, "
                    "accepted_at = ?, unverified = ?, served = 0, delivered = 0, samples = ?, "
                    "recheck_due = ?, requests = ?, client_requests = ?, layout = ? "
                    "WHERE zone_id = ? AND bucket_end = ?",
                    (
                        result.signature,
                        now,
                        now,
                        int(unverified),
                        len(result.samples),
                        recheck_due,
                        result.requests,
                        result.client_requests,
                        layout,
                        zone_id,
                        result.bucket_end,
                    ),
                )

        self._run(operation)

    def nearest_event(
        self, zone_id: str, alias: str, start: int, end: int, low: int, high: int
    ) -> int | None:
        """End of the accepted bucket in [low, high] with events of the totals
        selection `alias` that lies nearest to the window [start, end), or None."""
        column = EVENT_COLUMNS[alias]
        row = self._run(
            lambda conn: conn.execute(
                f"SELECT bucket_end FROM buckets WHERE zone_id = ? AND state = 1 "  # noqa: S608
                f"AND {column} > 0 AND bucket_end BETWEEN ? AND ? ORDER BY CASE "
                "WHEN bucket_end > ? THEN bucket_end - ? "
                "WHEN bucket_end <= ? THEN ? - bucket_end ELSE 0 END, bucket_end DESC LIMIT 1",
                (zone_id, low, high, end, end, start, start),
            ).fetchone()
        )
        return row[0] if row else None

    def has_event(self, zone_id: str, alias: str, low: int, high: int) -> bool:
        """Whether an accepted bucket ending in [low, high] has events of `alias`."""
        column = EVENT_COLUMNS[alias]
        return bool(
            self._run(
                lambda conn: conn.execute(
                    f"SELECT 1 FROM buckets WHERE zone_id = ? AND state = 1 "  # noqa: S608
                    f"AND {column} > 0 AND bucket_end BETWEEN ? AND ? LIMIT 1",
                    (zone_id, low, high),
                ).fetchone()
            )
        )

    def skip_rechecks(self, zone_id: str, layout: str) -> int:
        """Cancel the rechecks of buckets rendered with another layout and return how
        many: serving such a bucket again would add series next to the ones a
        scraper already has, and sums over labels would count it twice."""
        return self._run(
            lambda conn: (
                conn.execute(
                    "UPDATE buckets SET recheck_due = NULL WHERE zone_id = ? AND state = 1 "
                    "AND recheck_due IS NOT NULL AND layout IS NOT ?",
                    (zone_id, layout),
                ).rowcount
            )
        )

    def zone_ids(self) -> set[str]:
        """Zones with stored buckets."""
        return {
            row[0]
            for row in self._run(
                lambda conn: conn.execute("SELECT DISTINCT zone_id FROM buckets").fetchall()
            )
        }

    def reject(self, zone_id: str, result: BucketResult, now: float, next_attempt: float) -> None:
        """Count a failed verification and schedule the next read. The read itself
        worked, which ends a row of failed reads."""
        self._run(
            lambda conn: conn.execute(
                "UPDATE buckets SET attempts = attempts + 1, errors = 0, next_attempt = ?, "
                "signature_since = CASE WHEN signature IS ? THEN signature_since ELSE ? END, "
                "signature = ? WHERE zone_id = ? AND bucket_end = ? AND state = 0",
                (next_attempt, result.signature, now, result.signature, zone_id, result.bucket_end),
            )
        )

    def defer(
        self, zone_id: str, bucket_ends: Sequence[int], next_attempt: float, failed: bool = True
    ) -> None:
        """Postpone pending buckets whose read did not take place or, counted as an
        error of each bucket, `failed`."""
        self._run(
            lambda conn: conn.executemany(
                "UPDATE buckets SET next_attempt = ?, errors = errors + ? "
                "WHERE zone_id = ? AND bucket_end = ? AND state = 0",
                [(next_attempt, int(failed), zone_id, end) for end in bucket_ends],
            )
        )

    def release(self, zone_id: str, now: float) -> None:
        """Make the zone's buckets that wait after failed reads due."""
        self._run(
            lambda conn: conn.execute(
                "UPDATE buckets SET next_attempt = ? "
                "WHERE zone_id = ? AND state = 0 AND errors > 0 AND next_attempt > ?",
                (now, zone_id, now),
            )
        )

    def set_recheck(self, zone_id: str, bucket_ends: Sequence[int], due: float | None) -> None:
        self._run(
            lambda conn: conn.executemany(
                "UPDATE buckets SET recheck_due = ? "
                "WHERE zone_id = ? AND bucket_end = ? AND state = 1",
                [(due, zone_id, end) for end in bucket_ends],
            )
        )

    def prune(
        self,
        horizons: Mapping[str, int],
        listed: Collection[str] | None,
        now: float,
        expire_before: float,
    ) -> tuple[int, int]:
        """Finish delivered buckets and forget what is out of reach.

        A bucket is delivered after DELIVERY_RESPONSES consuming responses and
        DELIVERY_SECONDS since it was accepted; its samples are deleted. Rows
        behind a zone's horizon and rows of zones no longer `listed` (None: not
        known yet) are dropped unless their samples are still to be delivered;
        those are kept until they were accepted before `expire_before`, however
        old the bucket itself is. Returns how many pending buckets that had been
        tried were abandoned and how many undelivered expired.
        """

        def operation(conn):
            with self._transaction(conn):
                done = conn.execute(
                    "SELECT zone_id, bucket_end FROM buckets WHERE delivered = 0 AND state = 1 "
                    "AND served >= ? AND accepted_at <= ?",
                    (DELIVERY_RESPONSES, now - DELIVERY_SECONDS),
                ).fetchall()
                expired = conn.execute(
                    "SELECT zone_id, bucket_end FROM buckets WHERE delivered = 0 AND state = 1 "
                    "AND accepted_at < ?",
                    (expire_before,),
                ).fetchall()
                conn.executemany(
                    "DELETE FROM samples WHERE zone_id = ? AND bucket_end = ?", done + expired
                )
                conn.executemany(
                    "UPDATE buckets SET delivered = 1, samples = 0 "
                    "WHERE zone_id = ? AND bucket_end = ?",
                    done + expired,
                )
                abandoned = 0
                for zone_id, horizon in horizons.items():
                    abandoned += conn.execute(
                        "SELECT count(*) FROM buckets WHERE zone_id = ? AND bucket_end < ? "
                        "AND state = 0 AND (attempts > 0 OR errors > 0)",
                        (zone_id, horizon),
                    ).fetchone()[0]
                    conn.execute(
                        "DELETE FROM buckets WHERE zone_id = ? AND bucket_end < ? "
                        "AND (state = 0 OR delivered = 1)",
                        (zone_id, horizon),
                    )
                if listed is not None:
                    known = set(listed)
                    stored = conn.execute("SELECT DISTINCT zone_id FROM buckets").fetchall()
                    conn.executemany(
                        "DELETE FROM buckets WHERE zone_id = ? AND (state = 0 OR delivered = 1)",
                        [row for row in stored if row[0] not in known],
                    )
            return abandoned, len(expired)

        return self._run(operation)

    # Exposition

    def exposition(self, budget: int) -> tuple[list[tuple[str, int, float]], list[tuple]]:
        """Undelivered buckets within the sample budget (at least one), as (zone ID,
        bucket end, time of acceptance), and their samples ordered by family,
        label set and time.

        Buckets that still need a response come first, the newest first. Those
        that had their responses and only wait for the delivery time fill what
        is left of the budget; they must never keep others out of a response.
        """

        def operation(conn):
            picked, total = [], 0
            for zone_id, bucket_end, accepted_at, count in conn.execute(
                "SELECT zone_id, bucket_end, accepted_at, samples FROM buckets "
                "WHERE delivered = 0 AND state = 1 "
                "ORDER BY served >= ?, bucket_end DESC, zone_id",
                (DELIVERY_RESPONSES,),
            ).fetchall():
                if picked and total + count > budget:
                    break
                picked.append((zone_id, bucket_end, accepted_at))
                total += count
            with self._transaction(conn):
                conn.execute("DELETE FROM temp.pick")
                conn.executemany("INSERT INTO temp.pick VALUES (?, ?)", [row[:2] for row in picked])
                rows = conn.execute(
                    "SELECT s.family, s.labels, s.value, s.bucket_end FROM samples s "
                    "JOIN temp.pick p ON p.zone_id = s.zone_id AND p.bucket_end = s.bucket_end "
                    "ORDER BY s.family, s.labels, s.bucket_end"
                ).fetchall()
            return picked, rows

        return self._run(operation)

    def mark_served(self, picked: Sequence[tuple[str, int, float]]) -> None:
        """Count a complete response for the buckets it carried. A bucket accepted
        again meanwhile is not counted: the response had its earlier samples."""
        self._run(
            lambda conn: conn.executemany(
                "UPDATE buckets SET served = served + 1 "
                "WHERE zone_id = ? AND bucket_end = ? AND state = 1 AND accepted_at = ?",
                picked,
            )
        )

    # Health

    def summary(self) -> dict:
        """Per-zone pending, newest and oldest accepted buckets, and undelivered totals."""

        def operation(conn):
            zones = {}
            for zone_id, pending, newest, oldest in conn.execute(
                "SELECT zone_id, sum(state = 0), "
                "max(CASE WHEN state = 1 THEN bucket_end END), "
                "min(CASE WHEN state = 1 THEN bucket_end END) FROM buckets GROUP BY zone_id"
            ).fetchall():
                zones[zone_id] = (pending or 0, newest, oldest)
            buckets, samples = conn.execute(
                "SELECT count(*), coalesce(sum(samples), 0) FROM buckets "
                "WHERE state = 1 AND delivered = 0"
            ).fetchone()
            return {"zones": zones, "undelivered_buckets": buckets, "undelivered_samples": samples}

        return self._run(operation)

    def undelivered_samples(self) -> int:
        return self._run(
            lambda conn: conn.execute(
                "SELECT coalesce(sum(samples), 0) FROM buckets WHERE state = 1 AND delivered = 0"
            ).fetchone()[0]
        )

    def usable(self) -> bool:
        try:
            self._run(lambda conn: conn.execute("SELECT 1").fetchone())
        except sqlite3.Error:
            return False
        return True


def retry_delay(attempts: int) -> int:
    """Delay before the next read after `attempts` failed verifications (>= 1)."""
    return RETRY_DELAYS[min(attempts, len(RETRY_DELAYS)) - 1]


def recheck_time(bucket_end: int) -> int:
    """When an accepted bucket is read once more: RECHECK_SECONDS after the end of
    its UTC hour, so that one read rechecks the buckets of an hour."""
    return -(-bucket_end // HOUR_SECONDS) * HOUR_SECONDS + RECHECK_SECONDS


@dataclass
class ZoneState:
    """What the exporter keeps in memory about one zone; the buckets are in the
    storage. All of it lives and goes with the zone."""

    name: str
    plan: ZonePlan | None = None
    # When the settings are to be fetched (again).
    settings_due: float = 0.0
    # Whether the last settings request or read worked (1) or failed (0);
    # None before either.
    success: int | None = None
    # Buckets per history read, and their maximum after a history read of full
    # size failed, until the settings are fetched again.
    window: int = INITIAL_WINDOW_BUCKETS
    window_ceiling: int | None = None
    # No reads of the zone before this time, after a failed read.
    paused_until: float = 0.0
    # Failed reads in a row, and the window of the last one.
    failures: int = 0
    failed_window: tuple[int, int] | None = None
    # When the zone was first missing from the listing; such a zone is not
    # read, but its state is kept for ZONE_KEEP_SECONDS.
    gone_since: float | None = None
    # Totals selections read over the long proof range, because the usual range
    # had no group: zones with little traffic.
    long_proof: set[str] = field(default_factory=set)
    # Totals selection -> end of the newest bucket any answer had events in,
    # accepted or not; the storage knows the accepted ones. An answer without
    # groups over a range with known events left them out: a reduced one.
    last_event: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class ExporterSnapshot:
    """A consistent view of the exporter for the health metrics."""

    zones: Mapping[str, str]  # listed zone ID -> name
    plans: Mapping[str, ZonePlan]  # listed zone ID -> plan
    success: Mapping[str, int]  # listed zone name -> 1 or 0
    results: Mapping[str, int]
    analytics_responses: int
    analytics_samples: int
    heartbeat: float | None


class Exporter:
    """Discovers zones, reads due buckets, verifies them and stores the accepted ones.

    Work is chosen read by read: fresh buckets first, then rechecks, then history,
    always for the zone with the newest such bucket. Reads never overlap.
    """

    def __init__(
        self,
        api: CloudflareAPI,
        storage: Storage,
        schema: Schema | None = None,
        *,
        backfill_days: int = 0,
        history_interval: float | None = None,
        clock: Callable[[], float] = time.time,
    ):
        self.api = api
        self.storage = storage
        self.schema = Schema() if schema is None else schema
        self.backfill_days = backfill_days
        # Least time between history reads; None: HISTORY_INTERVAL_SECONDS.
        self.history_interval = history_interval
        self.clock = clock
        self._lock = threading.Lock()
        self.zones: dict[str, str] = {}  # zone ID -> name, as last listed
        self.zones_listed = False
        self.zones_at: float | None = None
        # Zone ID -> state, for the listed zones and those missing from the
        # listing for less than ZONE_KEEP_SECONDS.
        self.zone_states: dict[str, ZoneState] = {}
        self.results = dict.fromkeys(READ_RESULTS, 0)
        self.heartbeat: float | None = None
        self.analytics_responses = 0
        self.analytics_samples = 0
        # Every read uses the next page-size variant. Replicas start apart, so
        # that they do not receive each other's cached answers.
        self._reads = sum(socket.gethostname().encode())
        self._pruned_at: float | None = None
        self._history_at: float | None = None
        # History is read this many times more slowly after rate-limit rejections.
        self._slowdown = 1
        self._slowdown_at = 0.0
        self._limited_seen = 0
        # No reads before this time, after an explicit rate-limit maximum.
        self._paused_until = 0.0

    # State for other threads

    @property
    def plans(self) -> dict[str, ZonePlan]:
        """Zone ID -> plan, of every zone whose settings were fetched."""
        return {zone_id: s.plan for zone_id, s in self.zone_states.items() if s.plan}

    def snapshot(self) -> ExporterSnapshot:
        """The state the health metrics show, taken at once."""
        with self._lock:
            zones = dict(self.zones)
            states = [(zone_id, self.zone_states.get(zone_id)) for zone_id in zones]
            return ExporterSnapshot(
                zones=zones,
                plans={zone_id: s.plan for zone_id, s in states if s and s.plan},
                success={
                    zones[zone_id]: s.success
                    for zone_id, s in states
                    if s and s.success is not None
                },
                results=dict(self.results),
                analytics_responses=self.analytics_responses,
                analytics_samples=self.analytics_samples,
                heartbeat=self.heartbeat,
            )

    def count_delivery(self, samples: int) -> None:
        """Count a complete consuming /analytics response."""
        with self._lock:
            self.analytics_responses += 1
            self.analytics_samples += samples

    def _succeeded(self, state: ZoneState, value: int) -> None:
        with self._lock:
            state.success = value

    # Zones and plans

    def refresh(self, now: float) -> None:
        """List the zones and fetch each zone's settings when they are due. Nothing
        is requested during a rate-limit pause, and a rejection starts one."""
        if now < self._paused_until:
            return
        if self.zones_at is None or now - self.zones_at >= ZONES_REFRESH_SECONDS:
            try:
                zones = self.api.list_zones()
            except RateLimitedError as e:
                self._rate_limited(e)
                return
            except CloudflareAPIError as e:
                logging.error("Failed to list zones: %s", e)
                # Retry soon without hammering the API.
                self.zones_at = now - ZONES_REFRESH_SECONDS + ERROR_RETRY_SECONDS
            else:
                listed = {zone["id"]: zone["name"] for zone in zones}
                if listed != self.zones:
                    logging.info("Discovered %d zones: %s", len(listed), ", ".join(listed.values()))
                self._follow_listing(listed, now)
                self.zones_at = now
        for zone_id, zone_name in list(self.zones.items()):
            state = self.zone_states[zone_id]
            if now < state.settings_due:
                continue
            try:
                plan = make_plan(
                    zone_id,
                    zone_name,
                    fetch_settings(self.api, zone_id),
                    self.schema,
                    self.api.page_size,
                    self.backfill_days,
                )
            except RateLimitedError as e:
                # Every further request would be rejected as well.
                self._rate_limited(e)
                return
            except ZonePlanError as e:
                # The zone's plan, not the API: asking again soon changes nothing.
                logging.error(
                    "Zone %s: cannot be read: %s; settings are fetched again in %ds",
                    zone_name,
                    e,
                    SETTINGS_REFRESH_SECONDS,
                )
                self._succeeded(state, 0)
                state.settings_due = now + SETTINGS_REFRESH_SECONDS
                continue
            except CloudflareAPIError as e:
                logging.error("Zone %s: settings failed: %s", zone_name, e)
                self._succeeded(state, 0)
                state.settings_due = now + ERROR_RETRY_SECONDS
                continue
            with self._lock:
                state.plan = plan
                # Known and readable from now on; a failed read keeps its 0.
                if state.success is None:
                    state.success = 1
            state.settings_due = now + SETTINGS_REFRESH_SECONDS
            # Larger history windows get another chance.
            state.window_ceiling = None
            skipped = self.storage.skip_rechecks(zone_id, self.layout(plan))
            if skipped:
                logging.warning(
                    "Zone %s: labels or dimensions changed; %d buckets already served are "
                    "not rechecked, so that no series is served twice",
                    zone_name,
                    skipped,
                )
                self._count("recheck_skipped", skipped)

    def _follow_listing(self, listed: Mapping[str, str], now: float) -> None:
        """Take over a new zone listing. A zone missing from it, also one only the
        database still knows after a restart, keeps its state for
        ZONE_KEEP_SECONDS: a listing can lack a zone that is not gone."""
        with self._lock:
            if not self.zones_listed:
                # After a restart only the database knows the zones.
                for zone_id in self.storage.zone_ids() - set(listed):
                    self.zone_states.setdefault(zone_id, ZoneState(name=zone_id))
            for zone_id, state in list(self.zone_states.items()):
                if zone_id in listed:
                    state.gone_since = None
                    continue
                if state.gone_since is None:
                    state.gone_since = now
                if now - state.gone_since >= ZONE_KEEP_SECONDS:
                    del self.zone_states[zone_id]
            for zone_id, name in listed.items():
                state = self.zone_states.setdefault(zone_id, ZoneState(name=name))
                state.name = name
            self.zones = dict(listed)
            self.zones_listed = True

    def gone_zones(self) -> list[str]:
        """Zones missing from the listing whose state is still kept."""
        return [z for z, state in self.zone_states.items() if state.gone_since is not None]

    def layout(self, plan: ZonePlan) -> str:
        """What the labels of a zone's samples depend on: the exported labels and
        which of their dimensions the zone's plan offers."""
        families = [(name, tuple(self.schema.labels(name))) for name in sorted(METRIC_LABELS)]
        used = sorted({d for kind in DATASETS for d in self.schema.dimensions(kind).values()})
        offered = [d for d in used if d in plan.available]
        state = repr((families, offered, sorted(plan.supported.items())))
        return hashlib.sha256(state.encode()).hexdigest()[:DIGEST_LENGTH]

    def listed_plans(self) -> dict[str, ZonePlan]:
        """Plans of the zones in the current listing; zones missing from it are
        not read."""
        return {
            zone_id: state.plan
            for zone_id, state in self.zone_states.items()
            if state.plan and zone_id in self.zones
        }

    def horizons(self, now: float) -> dict[str, int]:
        """Oldest bucket end still read and exported, per listed zone with a plan."""
        return {
            zone_id: bucket_floor(now - plan.history_seconds) + BUCKET_SECONDS
            for zone_id, plan in self.listed_plans().items()
        }

    # Choosing work

    @staticmethod
    def _window(candidates: Sequence[tuple[int, int, int]], limit: int) -> tuple[int, int] | None:
        """[start, end) of contiguous due buckets from the newest candidate down.

        A bucket whose last read failed is read alone: if the failure is its
        own, it must not take its neighbours with it again. A window with
        buckets that failed verification before stays small, so that a retry
        is cheap however large history windows have grown.
        """
        if not candidates:
            return None
        end, _, failed = candidates[0]
        if failed:
            return end - BUCKET_SECONDS, end
        count = 0
        for bucket_end, attempts, errors in candidates:
            if bucket_end != end - count * BUCKET_SECONDS or errors:
                break
            if attempts:
                limit = min(limit, RETRY_WINDOW_BUCKETS)
            if count >= limit:
                break
            count += 1
        return end - count * BUCKET_SECONDS, end

    def next_read(self, now: float) -> tuple[str, str, int, int] | None:
        """(purpose, zone ID, start, end) of the most urgent read, or None."""
        newest = bucket_floor(now - SETTLE_SECONDS)
        horizons = self.horizons(now)
        # A zone whose last read failed is left alone for a while.
        zones = {
            zone_id: plan
            for zone_id, plan in self.listed_plans().items()
            if now >= self.zone_states[zone_id].paused_until
        }
        best = None

        def offer(purpose, rank, zone_id, window):
            nonlocal best
            if window is not None and (best is None or (rank, window[1]) > best[0]):
                best = ((rank, window[1]), (purpose, zone_id, *window))

        for zone_id, plan in zones.items():
            limit = min(INITIAL_WINDOW_BUCKETS, plan.max_window_buckets)
            low = max(horizons[zone_id], newest - FRESH_SECONDS + BUCKET_SECONDS)
            offer(
                "fresh",
                3,
                zone_id,
                self._window(self.storage.pending(zone_id, now, low, newest, limit), limit),
            )
        if best is None:
            for zone_id, plan in zones.items():
                limit = min(INITIAL_WINDOW_BUCKETS, plan.max_window_buckets)
                ends = self.storage.rechecks(zone_id, now, limit)
                offer("recheck", 2, zone_id, self._window([(e, 0, 0) for e in ends], limit))
        if best is None and self.storage.undelivered_samples() < BACKLOG_SAMPLE_LIMIT:
            for zone_id, plan in zones.items():
                window = self.zone_states[zone_id].window
                limit = min(window, plan.max_window_buckets)
                low = horizons[zone_id]
                high = max(low - BUCKET_SECONDS, newest - FRESH_SECONDS)
                offer(
                    "history",
                    1,
                    zone_id,
                    self._window(self.storage.pending(zone_id, now, low, high, limit), limit),
                )
        return best[1] if best else None

    # Reading

    def _variant(self) -> int:
        self._reads += 1
        return self._reads

    def _proof(self, plan: ZonePlan, start: int, end: int) -> dict[str, tuple[int, int]]:
        """The range each totals selection is read over to prove its answer.

        The usual range is the window with a day before it, or after it where
        the dataset's reach ends; a week for the selections of a quiet zone. It
        is widened to the nearest bucket known to have events, if one lies
        within a week of the window: an answer from the full data then has a
        group, and an answer without any is known to be a reduced one.
        """
        now = self.clock()
        oldest = bucket_floor(now - plan.reach_seconds)
        newest = max(end, bucket_floor(now - SETTLE_SECONDS))
        widest = min(plan.long_proof_seconds, plan.proof_room_seconds)
        ranges = {}
        for alias in TOTAL_SELECTIONS:
            if alias not in plan.dimensions:
                continue
            long = alias in self.zone_states[plan.zone_id].long_proof
            span = plan.long_proof_seconds if long else PROOF_SECONDS
            since = min(start, max(end - span, oldest))
            until = max(end, min(since + span, newest))
            known = self._nearest_event(
                plan,
                alias,
                start,
                end,
                max(oldest + BUCKET_SECONDS, end - widest + BUCKET_SECONDS),
                min(newest, start + widest),
            )
            if known is not None and not (since <= known - BUCKET_SECONDS and known <= until):
                low, high = min(since, known - BUCKET_SECONDS), max(until, known)
                if high - low > widest:
                    low, high = min(start, known - BUCKET_SECONDS), max(end, known)
                since, until = low, high
            ranges[alias] = since, until
        return ranges

    def _nearest_event(
        self, plan: ZonePlan, alias: str, start: int, end: int, low: int, high: int
    ) -> int | None:
        """End of the bucket in [low, high] known to have events of `alias` that is
        nearest to the window [start, end), or None."""
        candidates = []
        stored = self.storage.nearest_event(plan.zone_id, alias, start, end, low, high)
        if stored is not None:
            candidates.append(stored)
        seen = self.zone_states[plan.zone_id].last_event.get(alias)
        if seen is not None and low <= seen <= high:
            candidates.append(seen)

        def distance(bucket_end: int) -> int:
            return max(bucket_end - end, start + BUCKET_SECONDS - bucket_end, 0)

        return min(candidates, key=distance, default=None)

    def _events_known(self, plan: ZonePlan, alias: str, since: int, until: int) -> bool:
        """Whether a bucket of the range [since, until) is known to have events."""
        seen = self.zone_states[plan.zone_id].last_event.get(alias)
        if seen is not None and since < seen <= until:
            return True
        return self.storage.has_event(plan.zone_id, alias, since + BUCKET_SECONDS, until)

    def _settle_proof(self, plan: ZonePlan, stats: ReadStats) -> set[str]:
        """Judge the newest answers of totals without any group and return the
        selections to read again.

        Over a range with a bucket known to have events, such an answer left
        them out: it is a reduced one, "missing". Over the usual range it
        proves nothing otherwise: the selection is read over the long range
        from now on. Without a group there in EMPTY_LOOKS answers in a row the
        zone is idle, which is accepted. The long range is left again once an
        answer has plenty of groups.
        """
        again = set()
        zone = self.zone_states[plan.zone_id]
        for alias in TOTAL_SELECTIONS:
            state = stats.states.get(alias)
            seen = stats.newest.get(alias)
            if seen is not None and seen > zone.last_event.get(alias, 0):
                zone.last_event[alias] = seen
            if state == "empty":
                since, until = stats.ranges.get(alias, (0, 0))
                if self._events_known(plan, alias, since, until):
                    stats.states[alias] = "missing"
                    stats.looks.pop(alias, None)
                elif alias not in zone.long_proof:
                    zone.long_proof.add(alias)
                    again.add(alias)
                else:
                    stats.looks[alias] = stats.looks.get(alias, 0) + 1
                    if stats.looks[alias] >= EMPTY_LOOKS:
                        stats.states[alias] = "idle"
                    else:
                        again.add(alias)
            elif state not in ("idle", "missing"):
                # Any other answer ends a row of empty ones.
                stats.looks.pop(alias, None)
                if state == "full" and stats.groups.get(alias, 0) >= LONG_PROOF_GROUPS:
                    zone.long_proof.discard(alias)
        return again

    def _repair(
        self, plan: ZonePlan, start: int, end: int, buckets: dict[int, BucketData], stats: ReadStats
    ) -> None:
        """Read again, at once, what the answers left open: selections answered at a
        reduced resolution, totals that prove nothing, and every selection if
        the buckets disagree for another reason."""
        for _ in range(REPAIR_ROUNDS):
            suspects = self._settle_proof(plan, stats)
            suspects |= {
                alias for alias, state in stats.states.items() if state in ("sampled", "missing")
            }
            if not suspects:
                if not any(
                    evaluate(
                        plan, self.schema, bucket_end, data, stats.states, stats.reduced
                    ).problem
                    for bucket_end, data in buckets.items()
                ):
                    return
                suspects = set(plan.dimensions)
            again, more = read_window(
                self.api,
                plan,
                start,
                end,
                [alias for alias in plan.dimensions if alias in suspects],
                self._variant(),
                self._proof(plan, start, end),
            )
            stats.requests += more.requests
            stats.states.update(more.states)
            stats.groups.update(more.groups)
            stats.ranges.update(more.ranges)
            stats.newest.update(more.newest)
            for alias in more.states:
                stats.reduced[alias] = more.reduced.get(alias, set())
            for bucket_end, data in again.items():
                for alias in suspects:
                    setattr(buckets[bucket_end], alias, getattr(data, alias))
        self._settle_proof(plan, stats)

    def read(self, purpose: str, zone_id: str, start: int, end: int) -> None:
        """Read one window and settle or recheck its buckets. A failed read is
        tried again later; only an explicit rate-limit maximum raises."""
        zone = self.zone_states[zone_id]
        plan = zone.plan
        ends = list(range(start + BUCKET_SECONDS, end + 1, BUCKET_SECONDS))
        try:
            buckets, stats = read_window(
                self.api,
                plan,
                start,
                end,
                variant=self._variant(),
                proof=self._proof(plan, start, end),
            )
            self._repair(plan, start, end, buckets, stats)
            results = [
                evaluate(plan, self.schema, bucket_end, data, stats.states, stats.reduced)
                for bucket_end, data in buckets.items()
            ]
        except Exception as e:
            limited = isinstance(e, RateLimitedError)
            if isinstance(e, CloudflareAPIError):
                logging.error(
                    "Zone %s: reading %s to %s failed: %s",
                    plan.zone_name,
                    rfc3339(start),
                    rfc3339(end),
                    e,
                )
            else:
                # A bug must not stop the other zones and buckets.
                logging.exception(
                    "Zone %s: reading %s to %s failed unexpectedly",
                    plan.zone_name,
                    rfc3339(start),
                    rfc3339(end),
                )
            with self._lock:
                zone.success = 0
                self.results["error"] += 1
            self._postpone(purpose, zone, ends, limited)
            if limited:
                raise
            return
        self._succeeded(zone, 1)
        now = self.clock()
        zone.failed_window = None
        failures, zone.failures = zone.failures, 0
        if failures >= OUTAGE_FAILURES:
            # Cloudflare was failing, not single buckets: do not keep them
            # waiting, and let the history windows grow again.
            self.storage.release(zone_id, now)
            zone.window_ceiling = None
        if purpose == "history":
            window = zone.window
            if stats.split:
                window = max(1, window // 2)
            elif stats.fill < WINDOW_GROW_FILL and len(ends) >= window:
                ceiling = zone.window_ceiling or plan.max_window_buckets
                window = max(window, min(plan.max_window_buckets, ceiling, window * 2))
            zone.window = window
        layout = self.layout(plan)
        if purpose == "recheck":
            for result in results:
                self._recheck(plan, result, now, layout)
            return
        rejected = [result for result in results if not self._settle(plan, result, now, layout)]
        logging.log(
            logging.WARNING if rejected else logging.INFO,
            "Zone %s: %s %s to %s: %d of %d buckets accepted (%d requests)%s",
            plan.zone_name,
            purpose,
            rfc3339(start),
            rfc3339(end),
            len(ends) - len(rejected),
            len(ends),
            stats.requests,
            f"; not accepted: {rejected[0].problem} ({rejected[0].reason}), read again later"
            if rejected
            else "",
        )

    def _postpone(self, purpose: str, zone: ZoneState, ends: Sequence[int], limited: bool) -> None:
        """Schedule the buckets of a failed read again and leave the zone alone for
        a while.

        Most failures pass, so a window is first tried again as it is; the
        zone's history windows are halved after a failed history read of full
        size, as that may have been too much for one request. A window that
        fails twice is read bucket by bucket afterwards, see _window(). A bucket
        that fails alone waits longer every time: one that cannot be read costs
        a request now and then instead of holding back its zone. A rate-limit
        rejection says nothing about the buckets.
        """
        now = self.clock()
        zone_id = zone.plan.zone_id
        delay = RATE_LIMIT_RETRY_SECONDS if limited else ERROR_RETRY_SECONDS
        zone.paused_until = now + delay
        if not limited:
            zone.failures += 1
        if purpose == "recheck":
            kept = [end for end in ends if now - end < RECHECK_GIVE_UP_SECONDS]
            self.storage.set_recheck(zone_id, kept, now + max(delay, RECHECK_RETRY_SECONDS))
            if len(kept) < len(ends):
                self.storage.set_recheck(zone_id, [end for end in ends if end not in kept], None)
                self._count("recheck_failed", len(ends) - len(kept))
            return
        if limited:
            self.storage.defer(zone_id, ends, now + delay, failed=False)
            return
        window = (ends[0], ends[-1])
        repeated = zone.failed_window == window
        zone.failed_window = window
        if len(ends) == 1:
            row = self.storage.bucket(zone_id, ends[0])
            if row is not None and row.errors:
                delay = retry_delay(row.errors)
        elif purpose == "history":
            # A smaller read that failed, a gap or a retry, says nothing about
            # the size of the windows.
            size = min(zone.window, zone.plan.max_window_buckets)
            if len(ends) >= size:
                zone.window = zone.window_ceiling = max(1, size // 2)
        self.storage.defer(zone_id, ends, now + delay, failed=len(ends) == 1 or repeated)

    def _count(self, result: str, amount: int = 1) -> None:
        with self._lock:
            self.results[result] += amount

    def _settle(self, plan: ZonePlan, result: BucketResult, now: float, layout: str) -> bool:
        """Accept or reject a pending bucket; True if it was accepted."""
        # A bucket first read long after it ended has received its late events.
        late = now - result.bucket_end >= RECHECK_SECONDS
        recheck_due = None if late else recheck_time(result.bucket_end)
        if result.problem is None:
            self.storage.accept(plan.zone_id, result, now, recheck_due=recheck_due, layout=layout)
            self._count("accepted")
            return True
        row = self.storage.bucket(plan.zone_id, result.bucket_end)
        if (
            result.holdable
            and row is not None
            and row.signature == result.signature
            and row.signature_since is not None
            and now - row.signature_since >= UNVERIFIED_HOLD_SECONDS
        ):
            logging.warning(
                "Zone %s: bucket ending %s accepted unverified after %.0fh of the same "
                "answer (%s: %s)",
                plan.zone_name,
                rfc3339(result.bucket_end),
                (now - row.signature_since) / HOUR_SECONDS,
                result.problem,
                result.reason,
            )
            self.storage.accept(
                plan.zone_id,
                result,
                now,
                unverified=True,
                recheck_due=recheck_due,
                layout=layout,
            )
            self._count("accepted_unverified")
            return True
        attempts = (row.attempts if row else 0) + 1
        delay = retry_delay(attempts)
        logging.debug(
            "Zone %s: bucket ending %s not accepted (%s: %s); reading it again in %ds",
            plan.zone_name,
            rfc3339(result.bucket_end),
            result.problem,
            result.reason,
            delay,
        )
        self.storage.reject(plan.zone_id, result, now, now + delay)
        self._count(result.problem)
        return False

    def _recheck(self, plan: ZonePlan, result: BucketResult, now: float, layout: str) -> None:
        row = self.storage.bucket(plan.zone_id, result.bucket_end)
        if row is None or row.state != 1:
            return
        if row.layout != layout:
            # Served with other labels: serving it again would add a second
            # series for the same events.
            self._count("recheck_skipped")
            self.storage.set_recheck(plan.zone_id, [result.bucket_end], None)
            return
        if result.problem is not None:
            self._count("recheck_failed")
            give_up = now - result.bucket_end >= RECHECK_GIVE_UP_SECONDS
            self.storage.set_recheck(
                plan.zone_id, [result.bucket_end], None if give_up else now + RECHECK_RETRY_SECONDS
            )
            return
        if result.signature == row.signature:
            self._count("recheck_unchanged")
            self.storage.set_recheck(plan.zone_id, [result.bucket_end], None)
            return
        logging.info(
            "Zone %s: bucket ending %s changed on recheck; serving it again",
            plan.zone_name,
            rfc3339(result.bucket_end),
        )
        self._count("recheck_changed")
        self.storage.accept(plan.zone_id, result, now, layout=layout)

    # Loop

    def prune(self, now: float) -> None:
        abandoned, expired = self.storage.prune(
            self.horizons(now),
            # Zones that left the listing only recently keep their state.
            list(self.zone_states) if self.zones_listed else None,
            now,
            now - UNDELIVERED_KEEP_SECONDS,
        )
        if abandoned:
            logging.warning(
                "%d buckets left the dataset's reach without ever being verified", abandoned
            )
            self._count("abandoned", abandoned)
        if expired:
            logging.warning(
                "%d accepted buckets expired after %d days without being scraped",
                expired,
                UNDELIVERED_KEEP_SECONDS // DAY_SECONDS,
            )
            self._count("expired", expired)
        self._pruned_at = now

    def plan(self, now: float) -> int:
        """Refresh zones and settings, add the buckets that became due and prune.
        Returns the newest bucket end that can be read."""
        self.refresh(now)
        newest = bucket_floor(now - SETTLE_SECONDS)
        for zone_id, horizon in self.horizons(now).items():
            self.storage.ensure(zone_id, horizon, newest)
        self.prune(now)
        return newest

    def tick(self, stop: threading.Event) -> bool:
        """Plan, then read until nothing is due. Returns True if it stopped because a
        new bucket became due, so that the caller plans again at once."""
        # Before the zones are listed, so that a ready process reports its loop.
        self.heartbeat = self.clock()
        newest = self.plan(self.heartbeat)
        while not stop.is_set():
            now = self.heartbeat = self.clock()
            if now < self._paused_until:
                return False
            if bucket_floor(now - SETTLE_SECONDS) != newest:
                return True
            if now - self._pruned_at >= PRUNE_SECONDS:
                self.prune(now)
            if self.storage.full:
                # What is read could not be stored.
                return False
            self._watch_rate_limit(now)
            work = self.next_read(now)
            if work is None:
                return False
            if work[0] == "history":
                # History is paced, so that fresh buckets and other users of the
                # API token keep their share of Cloudflare's rate limit. The wait
                # is cut into pieces: a new bucket, a retry or a recheck that
                # falls due meanwhile goes first.
                interval = self.history_pace()
                wait = (self._history_at or 0) + interval - now
                if wait > 0:
                    settles = newest + BUCKET_SECONDS + SETTLE_SECONDS - now
                    stop.wait(max(0.0, min(wait, LOOP_SECONDS, settles)))
                    continue
                self._history_at = now
            try:
                self.read(*work)
            except RateLimitedError as e:
                # Every further request would be rejected as well.
                self._rate_limited(e)
                return False
        return False

    def _watch_rate_limit(self, now: float) -> None:
        """Read history more slowly after Cloudflare's rate limit was reached, and at
        the usual pace again when that has not happened for a while.

        A request that meets the limit waits for minutes, fresh buckets behind
        it. The limit is shared with the other instances and every other user
        of the token, so the pace that fits is only found by slowing down.
        """
        limited = self.api.stats.limited()
        if limited > self._limited_seen:
            self._limited_seen = limited
            self._slowdown_at = now
            if self._slowdown < HISTORY_SLOWDOWN_MAX:
                self._slowdown *= 2
                logging.warning(
                    "Cloudflare's rate limit was reached; history is read every %gs for now",
                    self.history_pace(),
                )
        elif self._slowdown > 1 and now - self._slowdown_at >= HISTORY_RECOVER_SECONDS:
            self._slowdown //= 2
            self._slowdown_at = now

    def history_pace(self) -> float:
        """Least seconds between two history reads now: the configured interval,
        slowed down after rate-limit rejections."""
        base = HISTORY_INTERVAL_SECONDS if self.history_interval is None else self.history_interval
        return base * self._slowdown

    def _rate_limited(self, error: RateLimitedError) -> None:
        self._paused_until = self.clock() + RATE_LIMIT_RETRY_SECONDS
        logging.error(
            "Cloudflare rate limit: %s; pausing reads for %ds", error, RATE_LIMIT_RETRY_SECONDS
        )

    def run(self, stop: threading.Event) -> None:
        while not stop.is_set():
            again = False
            try:
                again = self.tick(stop)
            except sqlite3.Error as e:
                # A full disk or a lock held by another process: the buckets
                # not stored are read again.
                logging.error("Collection loop: storage operation failed: %s", e)
            except Exception:
                # Last-resort guard: a bug must not take the exporter down.
                logging.exception("Collection loop failed unexpectedly")
            self.heartbeat = self.clock()
            if not again:
                stop.wait(LOOP_SECONDS)

    def readiness_error(self) -> str | None:
        """Why this process cannot serve yet. Deliberately independent of how fresh
        the data is and of Cloudflare's current state."""
        if not self.storage.usable():
            return "storage is not usable"
        if not self.zones_listed:
            return "zones have not been listed yet"
        return None


class HealthCollector(Collector):
    """Process and collection health, served on /metrics."""

    def __init__(self, exporter: Exporter, storage: Storage):
        self.exporter = exporter
        self.storage = storage

    def collect(self) -> Iterator[Metric]:
        # Only the zones of the current listing; a missing zone's state is kept
        # but not reported.
        view = self.exporter.snapshot()
        zones, plans, zone_success, results = view.zones, view.plans, view.success, view.results
        responses, served = view.analytics_responses, view.analytics_samples
        summary = self.storage.summary()
        success = GaugeMetricFamily(
            "cf_exporter_zone_success",
            "Whether the zone's last settings request or read succeeded (1) or failed (0)",
            labels=("zone_name",),
        )
        for name, value in sorted(zone_success.items()):
            success.add_metric((name,), value)
        yield success
        available = GaugeMetricFamily(
            "cf_exporter_dataset_available",
            "Whether the zone's plan supports the dataset (1) or it is not collected (0)",
            labels=("zone_name", "dataset"),
        )
        for plan in plans.values():
            for kind, value in plan.supported.items():
                available.add_metric((plan.zone_name, kind), value)
        yield available
        pending = GaugeMetricFamily(
            "cf_exporter_pending_buckets",
            "Buckets within reach that are not accepted yet, including scheduled retries",
            labels=("zone_name",),
        )
        newest = GaugeMetricFamily(
            "cf_exporter_newest_bucket_end_seconds",
            "End of the newest accepted bucket as Unix seconds",
            labels=("zone_name",),
        )
        oldest = GaugeMetricFamily(
            "cf_exporter_oldest_bucket_end_seconds",
            "End of the oldest accepted bucket still tracked as Unix seconds",
            labels=("zone_name",),
        )
        for zone_id, (count, newest_end, oldest_end) in sorted(summary["zones"].items()):
            name = zones.get(zone_id)
            if name is None:
                continue
            pending.add_metric((name,), count)
            if newest_end is not None:
                newest.add_metric((name,), newest_end)
                oldest.add_metric((name,), oldest_end)
        yield pending
        yield newest
        yield oldest
        yield GaugeMetricFamily(
            "cf_exporter_undelivered_buckets",
            "Accepted zone buckets still served on /analytics",
            value=summary["undelivered_buckets"],
        )
        yield GaugeMetricFamily(
            "cf_exporter_undelivered_samples",
            "Samples of accepted buckets still served on /analytics",
            value=summary["undelivered_samples"],
        )
        reads = CounterMetricFamily(
            "cf_exporter_bucket_reads",
            "Outcomes of bucket reads and of buckets that were given up",
            labels=("result",),
        )
        for result in READ_RESULTS:
            reads.add_metric((result,), results[result])
        yield reads
        yield CounterMetricFamily(
            "cf_exporter_analytics_responses",
            "Complete consuming /analytics responses",
            value=responses,
        )
        yield CounterMetricFamily(
            "cf_exporter_analytics_samples",
            "Samples sent in complete consuming /analytics responses",
            value=served,
        )
        yield GaugeMetricFamily(
            "cf_exporter_storage_persistent",
            "Whether the state is kept in the database file (1) or only in memory (0)",
            value=int(self.storage.persistent),
        )
        yield GaugeMetricFamily(
            "cf_exporter_storage_full",
            "Whether a write failed for lack of disk space within the last minute (1); "
            "reads wait meanwhile",
            value=int(self.storage.full),
        )
        yield CounterMetricFamily(
            "cf_exporter_storage_resets",
            "Times the storage was recreated after a failure, removal or corruption",
            value=self.storage.resets,
        )
        if view.heartbeat is not None:
            yield GaugeMetricFamily(
                "cf_exporter_loop_timestamp_seconds",
                "Unix time of the last collection loop iteration",
                value=view.heartbeat,
            )


def format_value(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else repr(float(value))


def render_analytics(rows: Sequence[tuple]) -> bytes:
    """Prometheus text format of stored samples ordered by family, labels and time.
    Each sample carries its bucket end in milliseconds."""
    out = []
    family = None
    for name, labels, value, bucket_end in rows:
        if name != family:
            description = DESCRIPTIONS.get(name, "Cloudflare analytics in 5-minute buckets")
            out.append(f"# HELP {name} {description}\n# TYPE {name} gauge\n")
            family = name
        out.append(f"{name}{labels} {format_value(value)} {bucket_end * 1000}\n")
    return "".join(out).encode()


class MetricsHTTPServer(ThreadingMixIn, WSGIServer):
    """Keep metrics, analytics and readiness responsive to concurrent requests."""

    daemon_threads = True


class MetricsRequestHandler(WSGIRequestHandler):
    def log_message(self, message, *args):
        # Match prometheus_client's quiet HTTP server.
        pass


def accepts_gzip(header: str) -> bool:
    """Whether an Accept-Encoding header allows gzip: named, or covered by `*`,
    with a quality above zero."""
    named = wildcard = None
    for item in header.split(","):
        coding, *params = (part.strip() for part in item.split(";"))
        quality = 1.0
        for param in params:
            key, _, value = param.partition("=")
            if key.strip().lower() == "q":
                try:
                    quality = float(value)
                except ValueError:
                    quality = 0.0
        coding = coding.lower()
        if coding in ("gzip", "x-gzip"):
            named = max(named or 0.0, quality)
        elif coding == "*":
            wildcard = quality
    if named is not None:
        return named > 0
    return wildcard is not None and wildcard > 0


def _plain(start_response, status: str, text: str, extra=()) -> list[bytes]:
    body = text.encode()
    start_response(
        status,
        [
            ("Content-Type", "text/plain; charset=utf-8"),
            *extra,
            ("Content-Length", str(len(body))),
        ],
    )
    return [body]


def make_app(
    exporter: Exporter,
    storage: Storage,
    registry=REGISTRY,
    *,
    response_samples: int | None = None,
    consume_token: str = "",
):
    """WSGI app: /analytics, /readyz, and health metrics on every other path.

    /analytics serves the undelivered buckets, `response_samples` samples per
    response at most (None: RESPONSE_SAMPLE_BUDGET). Only a request with
    `consume=1`, which the scraper sends, counts towards their delivery, so that
    looking at the endpoint by hand never takes samples away from the scraper.
    With a `consume_token`, such a request needs it as a bearer token, so that
    nothing else that reaches the port can consume.
    """
    metrics_app = make_wsgi_app(registry)
    expected = f"Bearer {consume_token}".encode() if consume_token else b""

    def analytics(environ, start_response):
        consume = "consume=1" in environ.get("QUERY_STRING", "").split("&")
        if consume and expected:
            given = environ.get("HTTP_AUTHORIZATION", "").encode("latin-1", "replace")
            if not hmac.compare_digest(given, expected):
                body = _plain(
                    start_response,
                    "403 Forbidden",
                    "consume=1 needs the bearer token of CF_EXPORTER_CONSUME_TOKEN\n",
                )
                return [] if environ["REQUEST_METHOD"] == "HEAD" else body
        budget = RESPONSE_SAMPLE_BUDGET if response_samples is None else response_samples
        picked, rows = storage.exposition(budget)
        body = render_analytics(rows)
        headers = [("Content-Type", "text/plain; version=0.0.4; charset=utf-8")]
        if accepts_gzip(environ.get("HTTP_ACCEPT_ENCODING", "")):
            body = gzip.compress(body, compresslevel=GZIP_LEVEL)
            headers.append(("Content-Encoding", "gzip"))
        headers.append(("Content-Length", str(len(body))))
        start_response("200 OK", headers)
        if environ["REQUEST_METHOD"] == "HEAD":
            return []

        if not consume:
            return [body]

        def stream():
            yield body
            # Only reached after the body was written completely.
            storage.mark_served(picked)
            exporter.count_delivery(len(rows))

        return stream()

    def app(environ, start_response):
        path = environ["PATH_INFO"]
        if path not in ("/analytics", "/readyz"):
            return metrics_app(environ, start_response)
        if environ["REQUEST_METHOD"] not in ("GET", "HEAD"):
            return _plain(
                start_response,
                "405 Method Not Allowed",
                "use GET or HEAD\n",
                [("Allow", "GET, HEAD")],
            )
        if path == "/analytics":
            return analytics(environ, start_response)
        error = exporter.readiness_error()
        body = ((error or "ready") + "\n").encode()
        start_response(
            "503 Service Unavailable" if error else "200 OK",
            [
                ("Content-Type", "text/plain; charset=utf-8"),
                ("Cache-Control", "no-store"),
                ("Content-Length", str(len(body))),
            ],
        )
        return [] if environ["REQUEST_METHOD"] == "HEAD" else [body]

    return app


def start_http_server(
    port: int,
    exporter: Exporter,
    storage: Storage,
    registry=REGISTRY,
    *,
    response_samples: int | None = None,
    consume_token: str = "",
) -> tuple[WSGIServer, threading.Thread]:
    server = make_server(
        "",
        port,
        make_app(
            exporter,
            storage,
            registry,
            response_samples=response_samples,
            consume_token=consume_token,
        ),
        MetricsHTTPServer,
        handler_class=MetricsRequestHandler,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


class RedactingFormatter(logging.Formatter):
    def __init__(self, *tokens: str):
        super().__init__("%(asctime)s [%(levelname)s] %(message)s")
        self.tokens = [token.strip() for token in tokens if token.strip()]

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        for token in self.tokens:
            message = message.replace(token, "[REDACTED]")
            message = message.replace(repr(token)[1:-1], "[REDACTED]")
        return message


# Secrets of the configuration, redacted from every log line.
SECRET_SETTINGS = ("CF_EXPORTER_API_TOKEN", "CF_EXPORTER_CONSUME_TOKEN")


def configure_logging(environ: Mapping[str, str] = os.environ) -> None:
    """Set up logging with CF_EXPORTER_LOGLEVEL from env or default to INFO."""
    level = environ.get("CF_EXPORTER_LOGLEVEL", "INFO").strip().upper()
    valid = level in LOG_LEVELS
    handler = logging.StreamHandler()
    handler.setFormatter(RedactingFormatter(*(environ.get(name, "") for name in SECRET_SETTINGS)))
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


def _validate_token(raw: str, name: str = "CF_EXPORTER_API_TOKEN", required: bool = True) -> str:
    token = raw.strip()
    if not token:
        if required:
            raise ConfigError(f"{name} must be set.")
        return ""
    if not token.isascii() or not token.isprintable() or any(char.isspace() for char in token):
        raise ConfigError(f"{name} must contain only printable ASCII without spaces.")
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
        if "zone_name" in labels:
            raise ConfigError(
                f"{DISABLED_LABELS}: zone_name cannot be disabled; "
                "aggregate zones in queries instead"
            )
        if labels:
            disabled[metric] = frozenset(labels)
    return Schema(disabled)


@dataclass(frozen=True)
class Config:
    api_token: str = field(repr=False)
    request_timeout: int = DEFAULT_REQUEST_TIMEOUT
    metrics_port: int = DEFAULT_METRICS_PORT
    db_path: str = DEFAULT_DB_PATH
    # Optional limits; 0 imposes none.
    backfill_days: int = 0
    max_retry_after_seconds: int = 0
    page_size: int = 0
    schema: Schema = field(default_factory=Schema)
    # Tuning with defaults.
    history_interval: int = HISTORY_INTERVAL_SECONDS
    response_samples: int = RESPONSE_SAMPLE_BUDGET
    # Bearer token a consuming /analytics request needs; empty: none.
    consume_token: str = field(default="", repr=False)


def parse_env(environ: Mapping[str, str] = os.environ) -> Config:
    """Read and validate the configuration from the environment."""
    # Secrets mounted from files often carry a trailing newline, which
    # Cloudflare rejects as a malformed Authorization header.
    api_token = _validate_token(environ.get("CF_EXPORTER_API_TOKEN", ""))
    return Config(
        api_token=api_token,
        request_timeout=_int_env(
            environ, "CF_EXPORTER_REQUEST_TIMEOUT", DEFAULT_REQUEST_TIMEOUT, 1
        ),
        metrics_port=_int_env(environ, "CF_EXPORTER_METRICS_PORT", DEFAULT_METRICS_PORT, 1, 65535),
        db_path=environ.get("CF_EXPORTER_DB_PATH", "").strip() or DEFAULT_DB_PATH,
        backfill_days=_int_env(environ, "CF_EXPORTER_BACKFILL_DAYS", 0, 0),
        max_retry_after_seconds=_int_env(environ, "CF_EXPORTER_MAX_RETRY_AFTER_SECONDS", 0, 0),
        page_size=_int_env(environ, "CF_EXPORTER_PAGE_SIZE", 0, 0),
        schema=parse_disabled_labels(environ.get(DISABLED_LABELS, "")),
        # The defaults as the class fixed them at import.
        history_interval=_int_env(
            environ, "CF_EXPORTER_HISTORY_INTERVAL_SECONDS", Config.history_interval, 1
        ),
        response_samples=_int_env(
            environ, "CF_EXPORTER_RESPONSE_SAMPLES", Config.response_samples, 1
        ),
        consume_token=_validate_token(
            environ.get("CF_EXPORTER_CONSUME_TOKEN", ""), "CF_EXPORTER_CONSUME_TOKEN", False
        ),
    )


def create_api(config: Config) -> CloudflareAPI:
    """The API client for a configuration; main() and live checks share it."""
    return CloudflareAPI(
        config.api_token,
        config.request_timeout,
        max_retry_after_seconds=config.max_retry_after_seconds,
        page_size=config.page_size,
    )


def describe(config: Config) -> str:
    """The storage, optional limits and disabled labels, for the startup log."""
    limits = ", ".join(
        f"{name}={value}"
        for name, value in (
            ("CF_EXPORTER_BACKFILL_DAYS", config.backfill_days),
            ("CF_EXPORTER_MAX_RETRY_AFTER_SECONDS", config.max_retry_after_seconds),
            ("CF_EXPORTER_PAGE_SIZE", config.page_size),
        )
        if value
    )
    disabled = "; ".join(
        f"{metric}: {', '.join(label for label in labels if label in removed)}"
        for metric, labels in METRIC_LABELS.items()
        if (removed := config.schema.disabled.get(metric))
    )
    return (
        f"storage {config.db_path}; optional limits: {limits or 'none'}; "
        f"disabled labels: {disabled or 'none'}; history every {config.history_interval}s; "
        f"up to {config.response_samples} samples per response; "
        f"consume token {'required' if config.consume_token else 'not set'}"
    )


def _handle_signal(signum, frame):
    """Handle termination signals and exit cleanly."""
    logging.info("Signal %s received, shutting down.", signal.Signals(signum).name)
    sys.exit(0)


def main() -> int:
    configure_logging()
    try:
        config = parse_env()
    except ConfigError as e:
        logging.error("%s", e)
        return 1
    for name in REMOVED_SETTINGS:
        if os.environ.get(name, "").strip():
            logging.warning("%s was removed in 2.0.0 and is ignored", name)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    storage = Storage(config.db_path)
    api = create_api(config)
    exporter = Exporter(
        api,
        storage,
        config.schema,
        backfill_days=config.backfill_days,
        history_interval=config.history_interval,
    )
    REGISTRY.register(HealthCollector(exporter, storage))
    REGISTRY.register(api.stats)
    start_http_server(
        config.metrics_port,
        exporter,
        storage,
        response_samples=config.response_samples,
        consume_token=config.consume_token,
    )
    logging.info(
        "Exporter running on port %d (/analytics, /metrics, /readyz), %s",
        config.metrics_port,
        describe(config),
    )
    try:
        exporter.run(threading.Event())
    finally:
        # Also on SIGTERM: the handler raises SystemExit in this thread.
        storage.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
