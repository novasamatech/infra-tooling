#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

import os
import sys
import logging
import datetime as dt
import threading
import time
import jwt
import requests
import csv
import io
from datetime import date, timedelta
from urllib.parse import urlparse

from collections import defaultdict
from wsgiref.simple_server import make_server, WSGIRequestHandler

LOG = logging.getLogger("appstore_exporter")

# Dedicated app logger: do not alter root logger or third‑party loggers.
_app_log_level = (
    os.environ.get("APPSTORE_EXPORTER_LOG_LEVEL", "INFO") or "INFO"
).upper()
LOG.setLevel(_app_log_level)
LOG.propagate = False
if not LOG.handlers:
    _handler = logging.StreamHandler()
    _handler.setLevel(_app_log_level)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    LOG.addHandler(_handler)

# ------------ Configuration ------------
# Required credentials
ISSUER_ID = os.environ.get("APPSTORE_EXPORTER_ISSUER_ID")
KEY_ID = os.environ.get("APPSTORE_EXPORTER_KEY_ID")
PRIVATE_KEY_PATH = os.environ.get("APPSTORE_EXPORTER_PRIVATE_KEY") or ""

if not all([ISSUER_ID, KEY_ID, PRIVATE_KEY_PATH]):
    LOG.error(
        "Missing required environment variables: APPSTORE_EXPORTER_ISSUER_ID, APPSTORE_EXPORTER_KEY_ID, APPSTORE_EXPORTER_PRIVATE_KEY"
    )
    sys.exit(2)


# App configuration - support both single and multiple apps
def _parse_app_config():
    """Parse app configuration from environment variables."""
    apps = []

    # Single app configuration
    single_app_id = os.environ.get("APPSTORE_EXPORTER_APP_ID")
    if single_app_id:
        apps.append(
            {
                "id": single_app_id,
                "name": os.environ.get(
                    "APPSTORE_EXPORTER_BUNDLE_ID", f"App_{single_app_id}"
                ),
                "bundle_id": os.environ.get("APPSTORE_EXPORTER_BUNDLE_ID", "unknown"),
            }
        )
        return apps

    # Multiple apps configuration
    app_ids = os.environ.get("APPSTORE_EXPORTER_APP_IDS", "").split(",")

    bundle_ids = os.environ.get("APPSTORE_EXPORTER_BUNDLE_IDS", "").split(",")

    if app_ids and app_ids[0]:
        for i, app_id in enumerate(app_ids):
            app_id = app_id.strip()
            if not app_id:
                continue
            bundle_id = (
                bundle_ids[i].strip()
                if i < len(bundle_ids) and bundle_ids[i].strip()
                else f"App_{app_id}"
            )
            apps.append({"id": app_id, "name": bundle_id, "bundle_id": bundle_id})

    return apps


APPS = _parse_app_config()
if not APPS:
    LOG.error(
        "No apps configured. Set APPSTORE_EXPORTER_APP_ID or APPSTORE_EXPORTER_APP_IDS"
    )
    sys.exit(2)

# Optional settings
PORT = int(os.environ.get("APPSTORE_EXPORTER_PORT", "8000"))
COLLECTION_INTERVAL = int(
    os.environ.get("APPSTORE_EXPORTER_COLLECTION_INTERVAL_SECONDS", "43200")
)
DAYS_TO_FETCH = int(os.environ.get("APPSTORE_EXPORTER_DAYS_TO_FETCH", "14"))
TEST_MODE = os.environ.get("APPSTORE_EXPORTER_TEST_MODE")

API_BASE = "https://api.appstoreconnect.apple.com"
_REPORTS_CACHE = {}

# Metrics storage - similar to gplay-exporter
_metrics_lock = threading.Lock()
_metrics_data = {}  # Format: {metric_name: {(labels...): (value, timestamp_ms)}}
CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"

# Health check state management with thread safety
_health_state = {
    "healthy": False,
    "last_successful_collection": None,
    "last_error": None,
    "collections_count": 0,
}
_health_state_lock = threading.Lock()

# ------------ Prometheus Metrics Config ------------

METRICS = [
    {
        "key": "daily_user_installs",
        "prom_name": "appstore_daily_user_installs_v2",
        "help": "Daily user installs (App Units) by country",
        "labels": {
            "country": "Territory",
            "platform_version": "Platform Version",
            "source_type": "Source Type",
        },
        "report_type": "App Downloads Standard",
        "value_patterns": ["Counts"],
        "granularity": "DAILY",
        "row_filter": {"column": "Download Type", "equals": "First-time download"},
    },
    {
        "key": "daily_user_non_first_time_installs",
        "prom_name": "appstore_daily_user_non_first_time_installs_v2",
        "help": "Daily user non first-time installs (App Units) by country",
        "labels": {
            "country": "Territory",
            "platform_version": "Platform Version",
            "source_type": "Source Type",
        },
        "report_type": "App Downloads Standard",
        "value_patterns": ["Counts"],
        "granularity": "DAILY",
        "row_filter": {"column": "Download Type", "equals": "Redownload"},
    },
    {
        "key": "active_devices",
        "prom_name": "appstore_active_devices_v2",
        "help": "Active devices by country (proxy for active device installs)",
        "labels": {
            "country": "Territory",
            "device": "Device",
            "platform_version": "Platform Version",
            "source_type": "Source Type",
        },
        "report_type": "App Sessions Standard",
        "value_patterns": ["Unique Devices"],
        "granularity": "DAILY",
    },
    {
        "key": "uninstalls",
        "prom_name": "appstore_uninstalls_v2",
        "help": "Uninstalls by country (Installation and Deletion). May be WEEKLY depending on availability",
        "labels": {
            "country": "Territory",
            "device": "Device",
            "platform_version": "Platform Version",
            "source_type": "Source Type",
        },
        "report_type": "App Store Installation and Deletion Standard",
        "value_patterns": ["Counts"],
        "granularity": "WEEKLY",
        "row_filter": {"column": "Event", "equals": "Delete"},
    },
]


def _format_prometheus_output() -> str:
    """
    Format metrics data as Prometheus text format with timestamps.

    Returns:
        String in Prometheus text exposition format
    """
    lines = []

    with _metrics_lock:
        # Group metrics by name for proper formatting
        metrics_by_name = defaultdict(list)
        for key, values in _metrics_data.items():
            if isinstance(key, tuple) and len(key) >= 1:
                metric_name = key[0]
                metrics_by_name[metric_name].append((key, values))

        # Add regular metrics
        for m in METRICS:
            metric_name = m["prom_name"]
            lines.append(f"# HELP {metric_name} {m['help']}")
            lines.append(f"# TYPE {metric_name} gauge")

            # Get all entries for this metric
            for key, (value, timestamp_ms) in _metrics_data.items():
                if isinstance(key, tuple) and len(key) >= 1 and key[0] == metric_name:
                    # Skip zero values
                    if value <= 0:
                        continue

                    # Build labels string
                    labels_parts = []
                    # key is (metric_name, package, label1, label2, ..., date)
                    # Last element is date, not a label
                    if len(key) >= 2:
                        labels_parts.append(f'package="{key[1]}"')

                    # Add other labels based on metric config
                    metric_config = next(
                        (m for m in METRICS if m["prom_name"] == metric_name), None
                    )
                    if metric_config and "labels" in metric_config:
                        label_names = list(metric_config["labels"].keys())
                        for i, label_name in enumerate(label_names):
                            # +2 for metric_name and package, -1 to exclude date at the end
                            if len(key) > i + 2 and i + 2 < len(key) - 1:
                                labels_parts.append(f'{label_name}="{key[i + 2]}"')

                    labels = ",".join(labels_parts)
                    lines.append(f"{metric_name}{{{labels}}} {value} {timestamp_ms}")

        # Add exporter internal metrics
        lines.append(
            "# HELP appstore_exporter_parsing_errors_total Total number of parsing errors per app and report"
        )
        lines.append("# TYPE appstore_exporter_parsing_errors_total gauge")
        for key, (value, timestamp_ms) in _metrics_data.items():
            if (
                isinstance(key, tuple)
                and len(key) >= 1
                and key[0] == "appstore_exporter_parsing_errors_total"
            ):
                if value > 0:
                    # key is (metric_name, package, report_type)
                    if len(key) >= 3:
                        labels = f'package="{key[1]}",report_type="{key[2]}"'
                        lines.append(
                            f"appstore_exporter_parsing_errors_total{{{labels}}} {value} {timestamp_ms}"
                        )

        lines.append(
            "# HELP appstore_exporter_last_collection_timestamp Timestamp of last successful collection"
        )
        lines.append("# TYPE appstore_exporter_last_collection_timestamp gauge")
        for key, (value, timestamp_ms) in _metrics_data.items():
            if (
                isinstance(key, str)
                and key == "appstore_exporter_last_collection_timestamp"
            ):
                lines.append(
                    f"appstore_exporter_last_collection_timestamp {value} {timestamp_ms}"
                )

    # Add empty line at the end
    lines.append("")
    return "\n".join(lines)


# ------------ Core Functions ------------
def _make_token():
    """Generate JWT token for App Store Connect API.

    Creates a JWT token with ES256 algorithm valid for 20 minutes.
    The token is required for all App Store Connect API calls.

    Returns:
        str: Signed JWT token

    Raises:
        Exception: If private key cannot be read or JWT encoding fails
    """
    try:
        with open(PRIVATE_KEY_PATH, "r") as f:
            private_key = f.read()

        now = int(time.time())
        return jwt.encode(
            {
                "iss": ISSUER_ID,
                "iat": now,
                "exp": now + 1200,
                "aud": "appstoreconnect-v1",
            },
            private_key,
            algorithm="ES256",
            headers={"kid": KEY_ID, "typ": "JWT"},
        )
    except Exception as e:
        LOG.error("Failed to generate JWT token: %s", e)
        raise


def _asc_api_call(method, path, params=None, payload=None, retries=3):
    """Make API call to App Store Connect with retry logic.

    Handles rate limiting (429) with exponential backoff and retries
    transient failures. Generates a fresh JWT token for each attempt.

    Args:
        method: HTTP method (GET or POST)
        path: API endpoint path (e.g., '/v1/apps')
        params: Query parameters for GET requests
        payload: JSON payload for POST requests
        retries: Number of retry attempts (default: 3)

    Returns:
        dict: Parsed JSON response from the API

    Raises:
        HTTPError: For non-retryable HTTP errors
        RuntimeError: After all retry attempts are exhausted
    """
    token = _make_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{API_BASE}{path}"

    for attempt in range(retries):
        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=headers, params=params, timeout=30)
            elif method.upper() == "POST":
                headers["Content-Type"] = "application/json"
                response = requests.post(url, headers=headers, json=payload, timeout=30)
            else:
                raise ValueError(f"Unsupported method: {method}")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            if (
                hasattr(e, "response")
                and e.response
                and getattr(e.response, "status_code", 0) == 429
            ):
                wait_time = 2**attempt
                LOG.warning("Rate limited, waiting %s seconds", wait_time)
                time.sleep(wait_time)
                continue
            else:
                LOG.error("API call failed: %s %s - %s", method, path, e)
                raise
        except Exception as e:
            LOG.error("API call failed: %s %s - %s", method, path, e)
            if attempt == retries - 1:
                raise
            time.sleep(1)

    raise RuntimeError(f"Failed after {retries} attempts")


def _find_existing_report_request(app_id):
    """Find existing analytics report request for an app.

    Searches for ONGOING report requests first, then falls back to
    any available report request. This is required to access analytics data.

    Args:
        app_id: App Store Connect app ID

    Returns:
        str: Report request ID if found, None otherwise
    """
    try:
        # First try to find ongoing requests
        response = _asc_api_call(
            "GET",
            f"/v1/apps/{app_id}/analyticsReportRequests",
            params={"filter[accessType]": "ONGOING", "limit": 5},
        )

        if response.get("data"):
            return response["data"][0]["id"]

        # If no ongoing requests, try to find any report requests
        LOG.info(
            "No ongoing report requests found for app %s, checking for any report requests...",
            app_id,
        )
        response = _asc_api_call(
            "GET", f"/v1/apps/{app_id}/analyticsReportRequests", params={"limit": 10}
        )

        if response.get("data"):
            # Log available report requests for debugging
            for report_request in response["data"]:
                req_id = report_request["id"]
                access_type = report_request.get("attributes", {}).get(
                    "accessType", "UNKNOWN"
                )
                LOG.info(
                    "Found report request: %s (accessType: %s)", req_id, access_type
                )
            return response["data"][0]["id"]

        LOG.warning("No report requests found for app %s", app_id)
        return None

    except Exception as e:
        LOG.error("Failed to find report request for app %s: %s", app_id, e)
        return None


def _find_report_id(report_request_id, name_pattern):
    """Find report ID by exact name match, using cache to minimize API calls.

    Caches the report catalog per request ID to avoid repeated API calls.
    Performs case-insensitive exact matching on report names.

    Args:
        report_request_id: ID of the analytics report request
        name_pattern: Exact report name to search for

    Returns:
        str: Report ID if found, None otherwise
    """
    try:
        # Use cached catalog if available; fetch and cache otherwise
        items = _REPORTS_CACHE.get(report_request_id)
        if items is None:
            response = _asc_api_call(
                "GET",
                f"/v1/analyticsReportRequests/{report_request_id}/reports",
                params={"limit": 200},
            )
            data = response.get("data", []) or []
            items = []
            for report in data:
                attrs = report.get("attributes") or {}
                items.append(
                    {
                        "id": report["id"],
                        "name": attrs.get("name", "") or "",
                        "category": attrs.get("category", "") or "",
                    }
                )
            _REPORTS_CACHE[report_request_id] = items
            available_reports = [f"{it['name']} ({it['category']})" for it in items]
            LOG.debug(
                "Report catalog for request %s: %s",
                report_request_id,
                available_reports,
            )

        candidates = (
            [name_pattern.strip()]
            if isinstance(name_pattern, str) and name_pattern.strip()
            else []
        )
        LOG.debug("Trying report name candidates (exact match only): %s", candidates)
        for candidate in candidates:
            lp = candidate.lower()

            # Exact name match (case-insensitive)
            for it in items:
                if it["name"].lower() == lp:
                    LOG.info(
                        "Matched report by exact name '%s' (%s)",
                        it["name"],
                        it["category"],
                    )
                    return it["id"]

        available_reports = [f"{it['name']} ({it['category']})" for it in items]
        LOG.warning(
            "Report with pattern '%s' not found. Available reports: %s",
            name_pattern,
            ", ".join(available_reports),
        )
        return None

    except Exception as e:
        LOG.error("Failed to find report: %s", e)
        return None


def _find_freshest_instance(report_id, granularity="DAILY", lookback_days=14):
    """Find the most recent report instance with data.

    Searches for instances within the lookback window, starting with the
    newest. Downloads segments for each instance until finding one with data.

    Args:
        report_id: Analytics report ID
        granularity: 'DAILY' or 'WEEKLY' granularity filter
        lookback_days: Number of days to look back (default: 14)

    Returns:
        tuple: (instance_dict, rows_list) if found, (None, []) otherwise
    """
    try:
        resp = _asc_api_call(
            "GET",
            f"/v1/analyticsReports/{report_id}/instances",
            params={"filter[granularity]": granularity, "limit": 200},
        )
    except Exception as e:
        LOG.debug(
            "Failed to list %s instances for report %s: %s", granularity, report_id, e
        )
        return None, []

    instances = (resp or {}).get("data") or []
    wanted = [
        inst
        for inst in instances
        if ((inst.get("attributes") or {}).get("granularity") == granularity)
    ]
    # Apply lookback window
    cutoff_iso = (date.today() - timedelta(days=lookback_days)).isoformat()
    wanted = [
        inst
        for inst in wanted
        if (
            ((inst.get("attributes") or {}).get("processingDate") or "")[:10]
            >= cutoff_iso
        )
    ]
    LOG.debug(
        "Instances fetched for report %s: total=%d %s=%d within %sd window",
        report_id,
        len(instances),
        granularity,
        len(wanted),
        lookback_days,
    )
    if not wanted:
        LOG.debug(
            "Report %s has no %s instances in the last %s days — skipping",
            report_id,
            granularity,
            lookback_days,
        )
        return None, []

    def _inst_processing_date(inst: dict) -> str:
        return (inst.get("attributes") or {}).get("processingDate") or ""

    # Check newest-first
    for inst in sorted(wanted, key=_inst_processing_date, reverse=True):
        inst_id = inst["id"]
        LOG.debug(
            "Checking instance %s (processingDate=%s, granularity=%s)",
            inst_id,
            _inst_processing_date(inst),
            granularity,
        )
        rows = _download_report_segments(inst_id)
        if rows:
            LOG.info(
                "Found non-empty %s instance %s (processingDate=%s)",
                granularity,
                inst_id,
                _inst_processing_date(inst),
            )
            return inst, rows
        LOG.debug(
            "Instance %s (processingDate=%s, granularity=%s) had no segments data; trying earlier",
            inst_id,
            _inst_processing_date(inst),
            granularity,
        )

    return None, []


def _parse_iso_date(date_str):
    """Parse ISO date string to date object."""
    try:
        # Handle ISO format with timezone
        if "T" in date_str:
            dt_obj = dt.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt_obj.date()
        else:
            # Try common date formats
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
                try:
                    return dt.datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
            raise ValueError(f"Unsupported date format: {date_str}")
    except Exception:
        raise ValueError(f"Unable to parse date: {date_str}")


def _download_report_segments(instance_id):
    """Download and parse CSV segments from report instance.

    Handles pagination through segment links, downloads each segment
    (with GZIP decompression if needed), and parses CSV/TSV data.
    Adds segment metadata to each row for tracking.

    Args:
        instance_id: Analytics report instance ID

    Returns:
        list: List of dictionaries, each representing a data row with
              segment metadata (__segment_id, __segment_start, __segment_end)
    """
    try:
        response = _asc_api_call(
            "GET", f"/v1/analyticsReportInstances/{instance_id}/segments"
        )
        # Process segments incrementally to reduce memory usage
        segments_data = []
        resp = response
        while True:
            segments_data.extend((resp.get("data") or []))
            next_url = (resp.get("links") or {}).get("next")
            if not next_url:
                break
            try:
                token = _make_token()
                r = requests.get(
                    next_url, headers={"Authorization": f"Bearer {token}"}, timeout=60
                )
                r.raise_for_status()
                resp = r.json()
            except Exception as e:
                LOG.warning("Failed to fetch next page of segments: %s", e)
                break

        if not segments_data:
            LOG.debug("No segments returned for instance %s", instance_id)
        LOG.debug(
            "Segments for instance %s: %s",
            instance_id,
            [s.get("id") for s in segments_data],
        )

        # Process segments in chunks to reduce memory usage
        all_rows = []

        for segment in segments_data:
            seg_attrs = segment.get("attributes", {}) or {}
            segment_url = (
                seg_attrs.get("url")
                or seg_attrs.get("downloadUrl")
                or seg_attrs.get("signedUrl")
                or seg_attrs.get("fileUrl")
            )
            if not segment_url:
                LOG.debug(
                    "Segment %s has no downloadable url in attributes",
                    segment.get("id"),
                )
                continue

            # Download segment payload (gzip or plain CSV)
            # Precompute host info outside try to avoid unbound in except
            host = urlparse(segment_url).netloc.lower()
            host_is_apple = "appstoreconnect.apple.com" in host
            try:
                token = _make_token()
                headers = {
                    "User-Agent": "appstore-exporter/1.0",
                    "Accept": "*/*",
                }
                if host_is_apple:
                    headers["Authorization"] = f"Bearer {token}"
                download_response = requests.get(
                    segment_url, headers=headers, timeout=120
                )
                download_response.raise_for_status()
                content = download_response.content
                ctype = (download_response.headers.get("Content-Type") or "").lower()
            except requests.exceptions.HTTPError as he:
                status = (
                    he.response.status_code
                    if getattr(he, "response", None) is not None
                    else None
                )
                if (not host_is_apple and status and 400 <= status < 500) or (
                    host_is_apple and status in (401, 403)
                ):
                    try:
                        headers_no_auth = {
                            "User-Agent": "appstore-exporter/1.0",
                            "Accept": "*/*",
                        }
                        download_response = requests.get(
                            segment_url, headers=headers_no_auth, timeout=120
                        )
                        download_response.raise_for_status()
                        content = download_response.content
                        ctype = (
                            download_response.headers.get("Content-Type") or ""
                        ).lower()
                    except Exception as e2:
                        LOG.warning(
                            "Retry without Authorization failed for segment %s: %s",
                            segment.get("id"),
                            e2,
                        )
                        continue
                else:
                    LOG.warning(
                        "Failed HTTP GET for segment %s: %s", segment.get("id"), he
                    )
                    continue
            except Exception as e:
                LOG.warning("Failed HTTP GET for segment %s: %s", segment.get("id"), e)
                continue

            # Determine compression from segment attributes if available; fallback to magic header and Content-Type
            compression = (
                (
                    seg_attrs.get("compression")
                    or seg_attrs.get("compressionAlgorithm")
                    or seg_attrs.get("fileCompression")
                    or ""
                )
                .strip()
                .lower()
            )
            try:
                path_only = urlparse(segment_url).path
                ext = os.path.splitext(path_only)[1].lower()
            except Exception:
                ext = ""
            LOG.debug(
                "Segment %s download: ctype=%s, compression_attr=%s, url_ext=%s",
                segment.get("id"),
                ctype,
                compression,
                ext,
            )
            LOG.debug(
                "Segment %s url host=%s size=%dB",
                segment.get("id"),
                urlparse(segment_url).netloc,
                len(content),
            )

            def _parse_csv_bytes(data: bytes) -> list:
                """Parse CSV/TSV text bytes; return list of parsed rows."""
                # Decode text with common encodings
                text = None
                for enc in ("utf-8-sig", "utf-8", "utf-16", "utf-16le", "latin-1"):
                    try:
                        text = data.decode(enc)
                        break
                    except Exception:
                        continue
                if text is None:
                    raise UnicodeDecodeError(
                        "unknown", b"", 0, 0, "Could not decode CSV text"
                    )
                buf = io.StringIO(text)
                sample = buf.read(8192)
                buf.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters="\t,;")
                    reader = csv.DictReader(buf, dialect=dialect)
                except Exception:
                    delimiter = (
                        "\t" if "\t" in sample else (";" if ";" in sample else ",")
                    )
                    reader = csv.DictReader(buf, delimiter=delimiter)
                rows_parsed_local = []
                for row in reader:
                    # Skip completely empty rows
                    if not row or all(
                        (v is None or (isinstance(v, str) and not v.strip()))
                        for v in row.values()
                    ):
                        continue
                    row_dict = {
                        (k.strip() if isinstance(k, str) else k): (
                            v.strip() if isinstance(v, str) else v
                        )
                        for k, v in row.items()
                    }
                    row_dict["__segment_id"] = segment["id"]
                    row_dict["__segment_start"] = seg_attrs.get("startDate", "")
                    row_dict["__segment_end"] = seg_attrs.get("endDate", "")
                    rows_parsed_local.append(row_dict)
                LOG.debug(
                    "Parsed %d rows from segment %s",
                    len(rows_parsed_local),
                    segment.get("id"),
                )
                return rows_parsed_local

            try:
                data_bytes = content

                # Detect and handle GZIP payloads (file-level gzip such as .gz)
                if (
                    compression == "gzip"
                    or "gzip" in ctype
                    or ext.endswith(".gz")
                    or (len(data_bytes) >= 2 and data_bytes[:2] == b"\x1f\x8b")
                ):
                    import gzip

                    try:
                        with gzip.GzipFile(fileobj=io.BytesIO(data_bytes)) as gz:
                            data_bytes = gz.read()
                        LOG.debug(
                            "Decompressed gzip payload for segment %s",
                            segment.get("id"),
                        )
                    except OSError:
                        # Already decompressed or not a valid gzip; continue with data_bytes as-is
                        pass

                # Parse and add rows
                parsed_rows = _parse_csv_bytes(data_bytes)
                all_rows.extend(parsed_rows)
                LOG.debug(
                    "Parsed %d rows from segment %s (csv/plain)",
                    len(parsed_rows),
                    segment.get("id"),
                )

            except UnicodeDecodeError as e:
                LOG.warning(
                    "Failed to decode segment %s content as text: %s (ctype=%s, len=%s)",
                    segment.get("id"),
                    e,
                    ctype,
                    len(content),
                )
            except csv.Error as e:
                LOG.warning(
                    "Failed to parse segment %s as CSV: %s (ctype=%s, len=%s)",
                    segment.get("id"),
                    e,
                    ctype,
                    len(content),
                )
            except Exception as e:
                LOG.warning(
                    "Unexpected error parsing segment %s: %s (ctype=%s, len=%s)",
                    segment.get("id"),
                    e,
                    ctype,
                    len(content),
                )

        LOG.debug("Total rows parsed for instance %s: %d", instance_id, len(all_rows))
        return all_rows

    except requests.RequestException as e:
        LOG.error("Network error downloading segments: %s", e)
        return []
    except Exception as e:
        LOG.error("Unexpected error downloading segments: %s", e)
        return []


def _extract_number(value):
    """Extract numeric value from string."""
    if not value:
        return 0.0
    try:
        return float(value.replace(",", "").strip())
    except ValueError:
        return 0.0


def _export_metrics(app_info, row, metric_name, value, row_date):
    """Export metrics to Prometheus."""
    try:
        report_date = _parse_iso_date(row_date)
        timestamp_ms = int(
            dt.datetime.combine(report_date, dt.time.min).timestamp() * 1000
        )
    except Exception:
        # Fallback to current date if parsing fails
        report_date = date.today()
        timestamp_ms = int(
            dt.datetime.combine(report_date, dt.time.min).timestamp() * 1000
        )

    metric_config = next((m for m in METRICS if m["key"] == metric_name), None)
    if metric_config:
        # Build key tuple: (metric_name, package, label1_value, label2_value, ..., date)
        key_parts = [metric_config["prom_name"], app_info["name"]]

        # Add label values in the order defined in metric config
        if "labels" in metric_config:
            for label_name, field_name in metric_config["labels"].items():
                key_parts.append(row.get(field_name, ""))

        # Add date to key to preserve historical data points
        key_parts.append(row_date)
        key = tuple(key_parts)

        # Store value with timestamp
        with _metrics_lock:
            _metrics_data[key] = (value, timestamp_ms)

        if LOG.isEnabledFor(logging.DEBUG) and value > 0:
            LOG.debug("Exported %s=%s with key %s", metric_name, value, key)


def _process_analytics_data(
    app_info,
    report_type,
    metric_name,
    value_patterns,
    granularity,
    country_column,
    row_filter=None,
):
    """Process analytics data for a specific report type and export to Prometheus.

    Main processing pipeline:
    1. Finds the report request and specific report by name
    2. Gets the freshest instance with the specified granularity
    3. Applies row filters (e.g., only First-time downloads)
    4. Groups data by schema to handle different dimensional cuts
    5. Deduplicates data across segments to prevent double-counting
    6. Exports metrics with proper labels to Prometheus

    Args:
        app_info: App configuration dict with 'id', 'name', 'bundle_id'
        report_type: Exact name of the Apple analytics report
        metric_name: Internal metric key (e.g., 'daily_user_installs')
        value_patterns: List with column name containing the metric value
        granularity: 'DAILY' or 'WEEKLY' instance granularity
        country_column: Column name for country/territory data
        row_filter: Optional dict with 'column' and 'equals' for filtering rows
    """
    app_id = app_info["id"]
    app_name = app_info["name"]

    rows_by_schema = {}
    report_date = ""
    exported_count = 0
    total_duplicates = 0

    try:
        # Track errors for this app
        parse_error_count = 0

        # Find existing report request
        report_request_id = _find_existing_report_request(app_id)
        if not report_request_id:
            LOG.warning("No report request found for %s", app_name)
            return

        candidates = [report_type] if report_type else []
        LOG.debug("Report candidate(s) to try (exact match): %s", candidates)

        for candidate in candidates:
            # Find specific report for this candidate
            report_id = _find_report_id(report_request_id, candidate)
            if not report_id:
                LOG.warning("No %s report found for %s", candidate, app_name)
                continue

            # Get ALL instances within the date range and process them all
            try:
                resp = _asc_api_call(
                    "GET",
                    f"/v1/analyticsReports/{report_id}/instances",
                    params={"filter[granularity]": granularity, "limit": 200},
                )
            except Exception as e:
                LOG.warning("Failed to get instances for report %s: %s", report_id, e)
                continue

            all_instances = (resp or {}).get("data") or []

            # Filter instances within the lookback window
            cutoff_iso = (date.today() - timedelta(days=DAYS_TO_FETCH)).isoformat()
            wanted_instances = [
                inst
                for inst in all_instances
                if ((inst.get("attributes") or {}).get("processingDate") or "")[:10]
                >= cutoff_iso
            ]

            if not wanted_instances:
                LOG.debug(
                    "No instances found for %s report in last %s days",
                    candidate,
                    DAYS_TO_FETCH,
                )
                continue

            LOG.info(
                "Found %d instances for %s report in last %s days",
                len(wanted_instances),
                candidate,
                DAYS_TO_FETCH,
            )

            # Aggregate data from all instances with deduplication
            all_rows = []
            global_seen_keys = set()  # Track unique data points across ALL instances

            for instance in sorted(
                wanted_instances,
                key=lambda x: x.get("attributes", {}).get("processingDate", ""),
            ):
                instance_id = instance["id"]
                processing_date = (instance.get("attributes") or {}).get(
                    "processingDate", ""
                )
                LOG.debug(
                    "Processing instance %s (processingDate=%s)",
                    instance_id,
                    processing_date,
                )

                instance_rows = _download_report_segments(instance_id)
                if instance_rows:
                    # Deduplicate across instances
                    unique_rows = []
                    for row in instance_rows:
                        # Create unique key based on all dimensions
                        dimension_keys = []
                        for k, v in sorted(row.items()):
                            if not k.startswith("__"):
                                dimension_keys.append(f"{k}={v}")
                        unique_key = "|".join(dimension_keys)

                        if unique_key not in global_seen_keys:
                            global_seen_keys.add(unique_key)
                            unique_rows.append(row)

                    all_rows.extend(unique_rows)
                    LOG.debug(
                        "Added %d unique rows from instance %s",
                        len(unique_rows),
                        instance_id,
                    )

            if not all_rows:
                LOG.debug("No data found in any instance for %s report", candidate)
                continue

            rows = all_rows
            processing_date = ""  # Aggregated from multiple instances
            LOG.info(
                "Processing %d total unique rows from %d instances for %s",
                len(rows),
                len(wanted_instances),
                candidate,
            )
            headers = list(rows[0].keys()) if rows else []
            LOG.debug("Processing %d rows; fields: %s", len(rows), headers)
            country_col = country_column or "Territory"
            value_col = value_patterns[0] if value_patterns else None
            # Log distinct values for row_filter column (to confirm exact wording)
            if row_filter and headers and row_filter.get("column") in headers:
                try:
                    rf_col = row_filter.get("column")
                    rf_vals = sorted({(r.get(rf_col) or "") for r in rows})
                    LOG.debug(
                        "Row filter values for column '%s' in '%s': %s",
                        rf_col,
                        candidate,
                        rf_vals,
                    )
                except Exception as _e_rf:
                    LOG.debug("Failed to collect row filter values: %s", _e_rf)
            # One-time warnings if configured columns are not present in the CSV headers
            _warned_missing_cols = set()
            if headers:
                if country_col and country_col not in headers:
                    key = f"{candidate}|country|{country_col}"
                    if key not in _warned_missing_cols:
                        LOG.warning(
                            "Configured country column '%s' not found in report '%s'; available headers: %s",
                            country_col,
                            candidate,
                            headers,
                        )
                        _warned_missing_cols.add(key)
                if value_col and value_col not in headers:
                    key = f"{candidate}|value|{value_col}"
                    if key not in _warned_missing_cols:
                        LOG.warning(
                            "Configured value column '%s' not found in report '%s'; available headers: %s",
                            value_col,
                            candidate,
                            headers,
                        )
                        _warned_missing_cols.add(key)

            # Select the single best segment (aggregate if exists, else the segment with fewest populated dimension columns),
            # then sum metric values per country for the chosen segment on the instance processing date.
            # Using segment startDate for date alignment; instance processingDate may not equal row date

            # Prepare filtered rows: only rows that match the instance processing date and have a country
            filtered_rows = []
            for i, row in enumerate(rows):
                if i == 0 and LOG.isEnabledFor(logging.DEBUG):
                    LOG.debug("CSV headers: %s", list(row.keys()))
                    LOG.debug("Skipping sample row output (debug sanitization)")
                # Do not filter by date to avoid dropping valid rows
                # Check that all label fields are present in the row
                missing_labels = []
                metric_config = next(
                    (m for m in METRICS if m["key"] == metric_name), None
                )
                if metric_config and "labels" in metric_config:
                    for field_name in metric_config["labels"].values():
                        if not row.get(field_name):
                            missing_labels.append(field_name)

                if missing_labels:
                    if LOG.isEnabledFor(logging.DEBUG):
                        LOG.debug(
                            "Skipping row missing required label fields: %s",
                            missing_labels,
                        )
                    continue
                if row_filter:
                    rf_col = row_filter.get("column")
                    rf_val = row_filter.get("equals")
                    if rf_col and rf_val is not None and row.get(rf_col) != rf_val:
                        continue
                seg_id = row.get("__segment_id") or "NO_SEGMENT"
                filtered_rows.append((seg_id, row))

            # Group rows by schema (set of non-metadata columns) to handle different data slices separately
            LOG.debug(
                "Built filtered_rows=%d for candidate '%s', instance %s",
                len(filtered_rows),
                candidate,
                instance_id,
            )

            # Group rows by their schema signature (excluding internal metadata columns)
            for seg_id, row in filtered_rows:
                # Get schema signature (all non-metadata columns)
                schema_cols = tuple(
                    sorted(k for k in row.keys() if not k.startswith("__"))
                )
                if schema_cols not in rows_by_schema:
                    rows_by_schema[schema_cols] = []
                rows_by_schema[schema_cols].append((seg_id, row))

            # Process each schema group separately
            for schema_cols, schema_rows in rows_by_schema.items():
                LOG.debug(
                    "Processing schema group with %d rows, columns: %s",
                    len(schema_rows),
                    schema_cols,
                )

                schema_seen_keys = set()
                schema_duplicates = 0

                for seg_id, row in schema_rows:
                    # Strict value extraction: use the exact configured value column only
                    if not value_col or (row.get(value_col) in (None, "")):
                        continue

                    # Create comprehensive unique key including all dimensions
                    date_val = (
                        row.get("Date")
                        or row.get("Processing Date")
                        or row.get("__segment_start")
                        or processing_date
                        or ""
                    ).strip()[:10]

                    # Build key from all dimension columns except value column (no metadata)
                    dimension_keys = []
                    for col in schema_cols:
                        if col != value_col and not col.startswith("__"):
                            dim_value = str(row.get(col, "")).strip()
                            dimension_keys.append(f"{col}={dim_value}")

                    unique_key = f"{date_val}_" + "_".join(sorted(dimension_keys))

                    # Check for duplicates within this schema group
                    if unique_key in schema_seen_keys:
                        if LOG.isEnabledFor(logging.DEBUG):
                            LOG.debug(
                                "Duplicate detected for key %s - skipping to avoid double-counting",
                                unique_key,
                            )
                        schema_duplicates += 1
                        continue

                    schema_seen_keys.add(unique_key)

                    # Extract value and export immediately with proper labels
                    value = _extract_number(row.get(value_col))
                    if value < 0:
                        continue

                    _export_metrics(app_info, row, metric_name, value, date_val)
                    exported_count += 1

                    if LOG.isEnabledFor(logging.DEBUG):
                        LOG.debug(
                            "Exported %s=%s from segment %s (key: %s)",
                            value_col,
                            value,
                            seg_id,
                            unique_key,
                        )

                total_duplicates += schema_duplicates
                if schema_duplicates > 0:
                    LOG.info(
                        "Found %d duplicates in schema group with columns %s",
                        schema_duplicates,
                        schema_cols,
                    )

            # Use current date as fallback for reporting since we're aggregating multiple instances
            report_date = date.today().isoformat()

            segments_used = len({seg_id for seg_id, _ in filtered_rows})
            schema_groups = len(rows_by_schema)
            LOG.debug(
                "Exported %d data points for metric %s (report=%s, segments_used=%d, schema_groups=%d, duplicates_found=%d)",
                exported_count,
                metric_name,
                candidate,
                segments_used,
                schema_groups,
                total_duplicates,
            )

            if exported_count > 0:
                LOG.info(
                    "Exported %d %s data points for candidate '%s' on %s",
                    exported_count,
                    metric_name,
                    candidate,
                    report_date,
                )
                return
            else:
                LOG.debug(
                    "Candidate '%s' had %s instances but produced no rows after filtering/export; trying next candidate",
                    candidate,
                    granularity,
                )
                continue

        LOG.warning(
            "No non-empty %s instance found for %s report in last %s days",
            granularity,
            report_type,
            DAYS_TO_FETCH,
        )
        return

    except Exception as e:
        LOG.error("Failed to process %s for %s: %s", report_type, app_name, e)
        # Increment error counter for this app and report type
        with _metrics_lock:
            error_key = (
                "appstore_exporter_parsing_errors_total",
                app_name,
                report_type,
            )
            current_value, _ = _metrics_data.get(error_key, (0, 0))
            timestamp_ms = int(dt.datetime.now().timestamp() * 1000)
            _metrics_data[error_key] = (current_value + 1, timestamp_ms)


def _process_app_metrics(app_info):
    """Process all configured metrics for a single app.

    Iterates through all metrics defined in METRICS configuration
    and processes each one with its specific report type and filters.

    Args:
        app_info: App configuration dict with 'id', 'name', 'bundle_id'
    """
    app_name = app_info["name"]
    LOG.info("Processing metrics for: %s", app_name)

    for m in METRICS:
        _process_analytics_data(
            app_info,
            m["report_type"],
            m["key"],
            m["value_patterns"],
            m.get("granularity", "DAILY"),
            m.get("country_column", "Territory"),
            m.get("row_filter"),
        )


def _run_metrics_collection():
    """Run metrics collection for all configured apps with health state tracking."""
    global _health_state
    # Clear old metrics data before new collection, but preserve error counters
    with _metrics_lock:
        # Save error counters
        error_metrics = {
            k: v
            for k, v in _metrics_data.items()
            if isinstance(k, tuple)
            and len(k) > 0
            and k[0] == "appstore_exporter_parsing_errors_total"
        }
        _metrics_data.clear()
        # Restore error counters
        _metrics_data.update(error_metrics)

    LOG.info("Starting metrics collection for %d apps", len(APPS))

    collection_errors = []
    for app_info in APPS:
        try:
            _process_app_metrics(app_info)
        except Exception as e:
            LOG.error("Failed to process app %s: %s", app_info["name"], e)
            collection_errors.append(str(e))

    # Update health state based on collection results (thread-safe)
    with _health_state_lock:
        _health_state["collections_count"] += 1

        if not collection_errors:
            _health_state["healthy"] = True
            _health_state["last_successful_collection"] = dt.datetime.now()
            _health_state["last_error"] = None
            LOG.info("Metrics collection completed successfully")
        else:
            # Partial success - some apps collected successfully
            if len(collection_errors) < len(APPS):
                _health_state["healthy"] = True
                _health_state["last_successful_collection"] = dt.datetime.now()
            _health_state["last_error"] = (
                f"Failed to collect metrics for {len(collection_errors)}/{len(APPS)} apps"
            )
            LOG.info(
                "Metrics collection completed with %d errors", len(collection_errors)
            )

        # Update last collection timestamp
        if _health_state["healthy"]:
            with _metrics_lock:
                timestamp_now = time.time()
                timestamp_ms = int(timestamp_now * 1000)
                _metrics_data["appstore_exporter_last_collection_timestamp"] = (
                    timestamp_now,
                    timestamp_ms,
                )


# ------------ Background Collection ------------
_collection_thread = None
_stop_event = threading.Event()


def _background_collection():
    """Background collection thread main loop.

    Runs metrics collection at regular intervals, handling both
    normal operation and test mode. Updates health state based
    on collection success/failure.
    """
    LOG.info("Starting background collection (interval: %ss)", COLLECTION_INTERVAL)

    if TEST_MODE:
        LOG.info("TEST_MODE enabled - running single collection")
        try:
            _run_metrics_collection()

            # Print summary in test mode
            metrics_summary = []
            for m in METRICS:
                prom_name = m.get("prom_name")
                # Count series for this metric
                series = sum(
                    1
                    for k in _metrics_data.keys()
                    if isinstance(k, tuple) and len(k) > 0 and k[0] == prom_name
                )
                metrics_summary.append((prom_name, series))

            LOG.info("Collection summary:")
            for prom_name, count in metrics_summary:
                LOG.info("  %s: %d series", prom_name, count)

            LOG.info("Test completed - exiting")
            os._exit(0)

        except Exception as e:
            LOG.exception("Test failed: %s", e)
            os._exit(1)
        return

    while not _stop_event.is_set():
        try:
            _run_metrics_collection()
        except Exception as e:
            LOG.exception("Collection failed: %s", e)
            # Update health state on total collection failure (thread-safe)
            with _health_state_lock:
                _health_state["healthy"] = False
                _health_state["last_error"] = str(e)

        _stop_event.wait(COLLECTION_INTERVAL)


def start_background_collection():
    """Start background collection."""
    global _collection_thread
    if _collection_thread and _collection_thread.is_alive():
        LOG.warning("Collection already running")
        return

    _collection_thread = threading.Thread(target=_background_collection, daemon=True)
    _collection_thread.start()
    LOG.info("Background collection started")


def stop_background_collection():
    """Stop background collection."""
    _stop_event.set()
    if _collection_thread:
        _collection_thread.join(timeout=10)
        LOG.info("Background collection stopped")


# ------------ HTTP Server ------------
def app(environ, start_response):
    """WSGI application handler with health-aware status reporting."""
    path = environ.get("PATH_INFO", "/")

    # Log requests only in DEBUG mode
    if LOG.isEnabledFor(logging.DEBUG):
        LOG.debug("HTTP request: %s %s", environ.get("REQUEST_METHOD", "GET"), path)

    if path == "/healthz":
        # Check actual health state (thread-safe read)
        with _health_state_lock:
            is_healthy = (
                _health_state["healthy"] and _health_state["collections_count"] > 0
            )
            collections_count = _health_state["collections_count"]
            last_collection = _health_state.get("last_successful_collection", "never")
            last_error = _health_state.get("last_error", "unknown error")

        if is_healthy:
            start_response("200 OK", [("Content-Type", "text/plain; charset=utf-8")])
            response = "ok\n"
            if LOG.isEnabledFor(logging.DEBUG):
                response = f"ok - last successful collection: {last_collection}\n"
            return [response.encode("utf-8")]
        else:
            # Not healthy yet - either no collections or last collection failed
            status = "503 Service Unavailable"
            start_response(status, [("Content-Type", "text/plain; charset=utf-8")])
            if collections_count == 0:
                response = "not ready - no collections completed yet\n"
            else:
                response = f"unhealthy - {last_error}\n"
            return [response.encode("utf-8")]

    if path != "/metrics":
        start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
        return [b"not found\n"]

    # Log metrics access only in DEBUG mode
    if LOG.isEnabledFor(logging.DEBUG):
        LOG.debug("Serving metrics endpoint")

    # Generate Prometheus format output
    output = _format_prometheus_output().encode("utf-8")
    start_response("200 OK", [("Content-Type", CONTENT_TYPE_LATEST)])
    return [output]


def main():
    """Main entry point."""
    LOG.info("Starting Apple App Store Connect Exporter")
    LOG.info(
        "Port: %s | Interval: %ss | Days: %s", PORT, COLLECTION_INTERVAL, DAYS_TO_FETCH
    )
    LOG.info("Configured apps: %s", [app["name"] for app in APPS])

    start_background_collection()

    if TEST_MODE:
        LOG.info("Test mode - waiting for completion")
        try:
            if _collection_thread:
                _collection_thread.join()
        except KeyboardInterrupt:
            LOG.info("Test interrupted")
        return

    # Create custom request handler that suppresses logs except in DEBUG mode
    class QuietRequestHandler(WSGIRequestHandler):
        def log_message(self, format, *args):
            """Override to suppress access logs except in DEBUG mode."""
            if LOG.isEnabledFor(logging.DEBUG):
                # Use the default logging in DEBUG mode
                super().log_message(format, *args)
            # Otherwise suppress the log

    httpd = make_server("0.0.0.0", PORT, app, handler_class=QuietRequestHandler)
    try:
        LOG.info("HTTP server started on port %s", PORT)
        httpd.serve_forever()
    except KeyboardInterrupt:
        LOG.info("Shutting down")
    finally:
        stop_background_collection()


if __name__ == "__main__":
    main()
