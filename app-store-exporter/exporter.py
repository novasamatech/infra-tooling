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
import zipfile
from datetime import date, timedelta

from prometheus_client import Counter, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
from wsgiref.simple_server import make_server

LOG = logging.getLogger("appstore_exporter")
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# ------------ Configuration ------------
# Required credentials
ISSUER_ID = os.environ.get("APPSTORE_ISSUER_ID")
KEY_ID = os.environ.get("APPSTORE_KEY_ID")
PRIVATE_KEY_PATH = os.environ.get("APPSTORE_PRIVATE_KEY")

if not all([ISSUER_ID, KEY_ID, PRIVATE_KEY_PATH]):
    LOG.error("Missing required environment variables: APPSTORE_ISSUER_ID, APPSTORE_KEY_ID, APPSTORE_PRIVATE_KEY")
    sys.exit(2)

# App configuration - support both single and multiple apps
def _parse_app_config():
    """Parse app configuration from environment variables."""
    apps = []

    # Single app configuration
    single_app_id = os.environ.get("APPSTORE_APP_ID")
    if single_app_id:
        apps.append({
            "id": single_app_id,
            "name": os.environ.get("APPSTORE_BUNDLE_ID", f"App_{single_app_id}"),
            "bundle_id": os.environ.get("APPSTORE_BUNDLE_ID", "unknown")
        })
        return apps

    # Multiple apps configuration
    app_ids = os.environ.get("APPSTORE_APP_IDS", "").split(",")

    bundle_ids = os.environ.get("APPSTORE_BUNDLE_IDS", "").split(",")

    if app_ids and app_ids[0]:
        for i, app_id in enumerate(app_ids):
            app_id = app_id.strip()
            if not app_id:
                continue

            bundle_id = bundle_ids[i].strip() if i < len(bundle_ids) and bundle_ids[i].strip() else f"App_{app_id}"
            app_name = bundle_id

            apps.append({
                "id": app_id,
                "name": bundle_id,
                "bundle_id": bundle_id
            })

    return apps

APPS = _parse_app_config()
if not APPS:
    LOG.error("No apps configured. Set APPSTORE_APP_ID or APPSTORE_APP_IDS")
    sys.exit(2)

# Optional settings
PORT = int(os.environ.get("PORT", "8000"))
COLLECTION_INTERVAL = int(os.environ.get("COLLECTION_INTERVAL_SECONDS", "43200"))
DAYS_TO_FETCH = int(os.environ.get("DAYS_TO_FETCH", "14"))
TEST_MODE = os.environ.get("TEST_MODE")

API_BASE = "https://api.appstoreconnect.apple.com"
REGISTRY = CollectorRegistry()

# ------------ Prometheus Counters ------------
def _create_prometheus_counters():
    return {
        "daily_installs": Counter(
            "appstore_daily_installs", "Daily installs by country", ["app", "country"], registry=REGISTRY
        ),
        "daily_deletions": Counter(
            "appstore_daily_deletions", "Daily deletions by country", ["app", "country"], registry=REGISTRY
        ),
        "active_devices": Counter(
            "appstore_active_devices", "Active devices by country", ["app", "country"], registry=REGISTRY
        ),
        "daily_sessions": Counter(
            "appstore_daily_sessions", "Daily sessions by country", ["app", "country"], registry=REGISTRY
        ),
        "daily_page_views": Counter(
            "appstore_daily_page_views", "Daily page views by country", ["app", "country"], registry=REGISTRY
        )
    }

counters = _create_prometheus_counters()

# ------------ Core Functions ------------
def _make_token():
    """Generate JWT token for App Store Connect API."""
    try:
        with open(PRIVATE_KEY_PATH, "r") as f:
            private_key = f.read()

        now = int(time.time())
        return jwt.encode(
            {"iss": ISSUER_ID, "iat": now, "exp": now + 1200, "aud": "appstoreconnect-v1"},
            private_key,
            algorithm="ES256",
            headers={"kid": KEY_ID, "typ": "JWT"},
        )
    except Exception as e:
        LOG.error("Failed to generate JWT token: %s", e)
        raise

def _asc_api_call(method, path, params=None, payload=None, retries=3):
    """Make API call with retry logic."""
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
            if hasattr(e, 'response') and e.response and getattr(e.response, 'status_code', 0) == 429:
                wait_time = 2 ** attempt
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
    """Find existing analytics report request for an app."""
    try:
        # First try to find ongoing requests
        response = _asc_api_call("GET", f"/v1/apps/{app_id}/analyticsReportRequests",
                               params={"filter[accessType]": "ONGOING", "limit": 5})

        if response.get("data"):
            return response["data"][0]["id"]

        # If no ongoing requests, try to find any report requests
        LOG.info("No ongoing report requests found for app %s, checking for any report requests...", app_id)
        response = _asc_api_call("GET", f"/v1/apps/{app_id}/analyticsReportRequests",
                               params={"limit": 10})

        if response.get("data"):
            # Log available report requests for debugging
            for report_request in response["data"]:
                req_id = report_request["id"]
                access_type = report_request.get("attributes", {}).get("accessType", "UNKNOWN")
                LOG.info("Found report request: %s (accessType: %s)", req_id, access_type)
            return response["data"][0]["id"]

        LOG.warning("No report requests found for app %s", app_id)
        return None

    except Exception as e:
        LOG.error("Failed to find report request for app %s: %s", app_id, e)
        return None

def _find_report_id(report_request_id, name_pattern):
    """Find report ID by name pattern."""
    try:
        response = _asc_api_call("GET", f"/v1/analyticsReportRequests/{report_request_id}/reports",
                               params={"limit": 50})

        available_reports = []
        for report in response.get("data", []):
            report_attrs = report.get("attributes", {}) or {}
            report_name = report_attrs.get("name", "")
            report_category = report_attrs.get("category", "")
            available_reports.append(f"{report_name} ({report_category})")

            # More flexible matching for available report names
            if (name_pattern.lower() in report_name.lower() or
                report_name.lower() in name_pattern.lower() or
                any(part.lower() in report_name.lower() for part in name_pattern.split())):
                return report["id"]

        LOG.warning("Report with pattern '%s' not found. Available reports: %s",
                   name_pattern, ", ".join(available_reports))
        return None

    except Exception as e:
        LOG.error("Failed to find report: %s", e)
        return None

def _get_daily_instances(report_id, days=14):
    """Get daily instances for a report."""
    try:
        response = _asc_api_call("GET", f"/v1/analyticsReports/{report_id}/instances",
                               params={"filter[granularity]": "DAILY", "limit": days + 7})

        instances = response.get("data", [])
        cutoff_date = date.today() - timedelta(days=days + 3)

        filtered_instances = []
        for instance in instances:
            processing_date = instance.get("attributes", {}).get("processingDate")
            if processing_date:
                instance_date = _parse_iso_date(processing_date)
                if instance_date >= cutoff_date:
                    filtered_instances.append(instance)

        return filtered_instances[-days:] if filtered_instances else []

    except Exception as e:
        LOG.error("Failed to get daily instances: %s", e)
        return []

def _parse_iso_date(date_str):
    """Parse ISO date string to date object."""
    try:
        # Handle ISO format with timezone
        if 'T' in date_str:
            dt_obj = dt.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
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
    """Download and parse CSV segments from report instance."""
    try:
        response = _asc_api_call("GET", f"/v1/analyticsReportInstances/{instance_id}/segments")
        all_rows = []

        for segment in response.get("data", []):
            segment_url = (segment.get("attributes", {}) or {}).get("url")
            if not segment_url:
                continue

            # Download zip file
            download_response = requests.get(segment_url, timeout=120)
            download_response.raise_for_status()

            # Extract CSV files
            with zipfile.ZipFile(io.BytesIO(download_response.content)) as zip_file:
                for file_name in zip_file.namelist():
                    if file_name.lower().endswith(".csv"):
                        with zip_file.open(file_name) as csv_file:
                            text = io.TextIOWrapper(csv_file, encoding="utf-8")
                            reader = csv.DictReader(text)
                            for row in reader:
                                all_rows.append({k.strip(): v.strip() for k, v in row.items()})

        return all_rows

    except Exception as e:
        LOG.error("Failed to download segments: %s", e)
        return []

def _extract_number(value):
    """Extract numeric value from string."""
    if not value:
        return 0.0
    try:
        return float(value.replace(",", "").strip())
    except ValueError:
        return 0.0

def _export_metrics(app_name, country, metrics, report_date):
    """Export metrics to Prometheus."""
    timestamp_ms = int(dt.datetime.combine(report_date, dt.time.min).timestamp() * 1000)
    labels = {"app": app_name, "country": country}

    for metric_name, value in metrics.items():
        if metric_name in counters:
            counter = counters[metric_name]
            if hasattr(counter.labels(**labels), '_value'):
                counter.labels(**labels)._value.set(value, timestamp=timestamp_ms)

            if LOG.isEnabledFor(logging.DEBUG) and value > 0:
                LOG.debug("Exported %s=%s for %s/%s", metric_name, value, app_name, country)

def _process_analytics_data(app_info, report_type, metric_name, value_patterns):
    """Process analytics data for a specific report type."""
    app_id = app_info["id"]
    app_name = app_info["name"]

    try:
        # Find existing report request
        report_request_id = _find_existing_report_request(app_id)
        if not report_request_id:
            LOG.warning("No report request found for %s", app_name)
            return

        # Find specific report
        report_id = _find_report_id(report_request_id, report_type)
        if not report_id:
            LOG.warning("No %s report found for %s", report_type, app_name)
            return

        # Get daily instances
        instances = _get_daily_instances(report_id, DAYS_TO_FETCH)
        if not instances:
            LOG.warning("No daily instances found for %s report", report_type)
            return

        LOG.info("Found %d daily instances for %s report", len(instances), report_type)

        # Process each instance
        for instance in instances:
            instance_id = instance["id"]
            processing_date = instance.get("attributes", {}).get("processingDate", "unknown")
            LOG.debug("Processing instance %s (date: %s)", instance_id, processing_date)

            rows = _download_report_segments(instance_id)
            if not rows:
                LOG.debug("No data in instance %s", instance_id)
                continue

            LOG.debug("Processing %d rows from instance %s", len(rows), instance_id)

            # Process rows and extract metrics
            for i, row in enumerate(rows):
                if i == 0 and LOG.isEnabledFor(logging.DEBUG):
                    LOG.debug("CSV headers: %s", list(row.keys()))

                country = (row.get("Country") or row.get("Country Code") or "").upper()
                date_str = row.get("Date") or row.get("Processing Date")

                if not country or not date_str:
                    continue

                # Extract value
                value = 0
                for pattern in value_patterns:
                    if pattern in row:
                        value = _extract_number(row[pattern])
                        if value > 0:
                            break

                if value > 0:
                    try:
                        report_date = _parse_iso_date(date_str)
                        _export_metrics(app_name, country, {metric_name: value}, report_date)
                        if LOG.isEnabledFor(logging.DEBUG):
                            LOG.debug("Processed %s=%s for %s/%s on %s",
                                     metric_name, value, app_name, country, date_str)
                    except Exception as e:
                        LOG.debug("Failed to process row: %s", e)

    except Exception as e:
        LOG.error("Failed to process %s for %s: %s", report_type, app_name, e)

def _process_app_metrics(app_info):
    """Process all metrics for an app."""
    app_name = app_info["name"]
    LOG.info("Processing metrics for: %s", app_name)

    # Installations and Deletions - using available report names
    _process_analytics_data(
        app_info,
        "App Store Installation and Deletion",
        "daily_installs",
        ["Installations", "Installs", "Count", "Event Count"]
    )

    _process_analytics_data(
        app_info,
        "App Store Installation and Deletion",
        "daily_deletions",
        ["Deletions", "Deletes", "Count", "Event Count"]
    )

    # Active Devices - from App Sessions report (Unique Devices column)
    _process_analytics_data(
        app_info,
        "App Sessions",
        "active_devices",
        ["Unique Devices", "Devices", "Active Devices", "Active Device Count"]
    )

    # Sessions - using available report names
    _process_analytics_data(
        app_info,
        "App Sessions",
        "daily_sessions",
        ["Sessions", "Session Count"]
    )

    # Page Views - from App Store Discovery and Engagement report (Product Page Views)
    _process_analytics_data(
        app_info,
        "App Store Discovery and Engagement",
        "daily_page_views",
        ["Product Page Views", "Page Views", "Page View Count", "Views"]
    )

def _run_metrics_collection():
    """Run metrics collection for all configured apps."""
    global REGISTRY, counters
    REGISTRY = CollectorRegistry()
    counters = _create_prometheus_counters()

    LOG.info("Starting metrics collection for %d apps", len(APPS))

    for app_info in APPS:
        try:
            _process_app_metrics(app_info)
        except Exception as e:
            LOG.error("Failed to process app %s: %s", app_info["name"], e)

    LOG.info("Metrics collection completed")

# ------------ Background Collection ------------
_collection_thread = None
_stop_event = threading.Event()

def _background_collection():
    """Background collection thread."""
    LOG.info("Starting background collection (interval: %ss)", COLLECTION_INTERVAL)

    if TEST_MODE:
        LOG.info("TEST_MODE enabled - running single collection")
        try:
            _run_metrics_collection()

            # Print summary in test mode
            if TEST_MODE:
                metrics_output = generate_latest(REGISTRY).decode('utf-8')
                metrics_count = {}

                for line in metrics_output.split('\n'):
                    if line.startswith('appstore_') and '{' in line:
                        metric_name = line.split('{')[0]
                        metrics_count[metric_name] = metrics_count.get(metric_name, 0) + 1

                LOG.info("Collection summary:")
                for metric, count in metrics_count.items():
                    LOG.info("  %s: %d data points", metric, count)

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
    """WSGI application handler."""
    path = environ.get("PATH_INFO", "/")

    if path == "/healthz":
        start_response("200 OK", [("Content-Type", "text/plain; charset=utf-8")])
        return [b"ok\n"]

    if path != "/metrics":
        start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
        return [b"not found\n"]

    output = generate_latest(REGISTRY)
    start_response("200 OK", [("Content-Type", CONTENT_TYPE_LATEST)])
    return [output]

def main():
    """Main entry point."""
    LOG.info("Starting Apple App Store Connect Exporter")
    LOG.info("Port: %s | Interval: %ss | Days: %s", PORT, COLLECTION_INTERVAL, DAYS_TO_FETCH)
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

    httpd = make_server("0.0.0.0", PORT, app)
    try:
        LOG.info("HTTP server started on port %s", PORT)
        httpd.serve_forever()
    except KeyboardInterrupt:
        LOG.info("Shutting down")
    finally:
        stop_background_collection()

if __name__ == "__main__":
    main()
