#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

import os
import io
import re
import csv
import sys
import logging
import datetime as dt
import threading
from typing import Dict, List, Optional, Set

from prometheus_client import Counter, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
from wsgiref.simple_server import make_server

# Google libs
from google.cloud import storage  # pip install google-cloud-storage
from google.oauth2 import service_account

LOG = logging.getLogger("gplay_exporter")
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# ------------ Config ------------
GOOGLE_CREDS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
BUCKET_ID = os.environ.get("GPLAY_BUCKET_ID")
if not GOOGLE_CREDS or not BUCKET_ID:
    LOG.error("Missing env vars: GOOGLE_APPLICATION_CREDENTIALS and/or GPLAY_BUCKET_ID")
    sys.exit(2)

PORT = int(os.environ.get("PORT", "8000"))

COLLECTION_INTERVAL = int(os.environ.get("COLLECTION_INTERVAL_SECONDS", "43200")) # default 12h
GCS_PROJECT = os.environ.get("GCS_PROJECT")
TEST_MODE = os.environ.get("TEST_MODE")

# ------------ Prometheus registry & counters ------------
REGISTRY = CollectorRegistry()

def _create_prometheus_counters():
    """Create fresh Prometheus counter instances for all metrics."""
    counters = {
        "daily_device_installs": Counter(
            "gplay_daily_device_installs",
            "Daily device installs by country from Google Play Console.",
            ["package", "country"],
            registry=REGISTRY,
        ),
        "daily_device_uninstalls": Counter(
            "gplay_daily_device_uninstalls",
            "Daily device uninstalls by country from Google Play Console.",
            ["package", "country"],
            registry=REGISTRY,
        ),
        "active_device_installs": Counter(
            "gplay_active_device_installs",
            "Active device installs by country from Google Play Console.",
            ["package", "country"],
            registry=REGISTRY,
        ),
        "daily_user_installs": Counter(
            "gplay_daily_user_installs",
            "Daily user installs by country from Google Play Console.",
            ["package", "country"],
            registry=REGISTRY,
        ),
        "daily_user_uninstalls": Counter(
            "gplay_daily_user_uninstalls",
            "Daily user uninstalls by country from Google Play Console.",
            ["package", "country"],
            registry=REGISTRY,
        )
    }
    return counters

counters = _create_prometheus_counters()



def _set_counter_absolute_value(counter: Counter, labels: Dict[str, str], value: float, timestamp: Optional[float] = None):
    """Set absolute value for Prometheus counter with optional timestamp."""
    # For daily metrics, we set the counter to the absolute value
    if timestamp is not None:
        counter.labels(**labels)._value.set(value, timestamp=timestamp)
    else:
        counter.labels(**labels)._value.set(value)

def _export_metrics(package: str, country: str, metrics: Dict[str, float], date: dt.date):
    """Export multiple metrics for a country with proper timestamping."""
    timestamp_ms = int(dt.datetime.combine(date, dt.time.min).timestamp() * 1000)
    labels = {"package": package, "country": country}

    for metric_name, value in metrics.items():
        if metric_name in counters:
            # Always export metrics even if zero, as long as other metrics in the same row are non-zero
            _set_counter_absolute_value(counters[metric_name], labels, value, timestamp_ms)
            if LOG.isEnabledFor(logging.DEBUG):
                if value > 0:
                    LOG.debug("Exported metric: %s=%s for %s/%s", metric_name, value, package, country)
                else:
                    LOG.debug("Exported zero metric: %s for %s/%s", metric_name, package, country)
        else:
            LOG.warning("Unknown metric %s for %s/%s", metric_name, package, country)

def _load_credentials():
    """Load Google Cloud credentials from file or default authentication."""
    if GOOGLE_CREDS and os.path.exists(GOOGLE_CREDS):
        return service_account.Credentials.from_service_account_file(GOOGLE_CREDS)
    else:
        import google.auth
        creds, _ = google.auth.default()
        return creds

def _storage_client():
    """Create and return Google Cloud Storage client with credentials."""
    creds = _load_credentials()
    return storage.Client(project=GCS_PROJECT, credentials=creds)

_pkgs: Set[str] = set()
_pkg_regex = re.compile(r"^stats/installs/installs_(?P<pkg>[^_]+)_(?P<yyyymm>\d{6})_(country|overview)\.csv$")
_country_regex = re.compile(r"^stats/installs/installs_(?P<pkg>[^_]+)_(?P<yyyymm>\d{6})_(country|overview)\.csv$")

def _discover_packages_from_gcs(client) -> Set[str]:
    """Scan GCS bucket for package CSV files and extract package names."""
    pkgs: Set[str] = set()
    try:
        for blob in client.list_blobs(BUCKET_ID, prefix="stats/installs/"):
            m = _pkg_regex.match(blob.name)
            if m:
                pkgs.add(m.group("pkg"))
    except Exception as e:
        LOG.error("GCS scan for packages failed: %s", e)
    return pkgs

def _discover_packages() -> Set[str]:
    """Discover all available packages in GCS bucket and cache results."""
    client = _storage_client()
    found = _discover_packages_from_gcs(client)

    if not found:
        LOG.warning("No packages discovered via GCS. Check permissions and bucket id.")
    else:
        LOG.info("Discovered %d packages: %s", len(found), ", ".join(sorted(found)))
    _pkgs.clear()
    _pkgs.update(found)
    return set(_pkgs)

def _parse_date(s: str) -> Optional[dt.date]:
    """Parse date string using multiple supported formats."""
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%m/%d/%Y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None

def _extract_number(s: str) -> float:
    """Extract numeric value from string, handling commas and empty values."""
    if s is None:
        return 0.0
    s = s.replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return 0.0

def _download_csv(client: storage.Client, blob_name: str) -> List[dict]:
    """Download and parse CSV file from GCS with automatic encoding detection."""
    data = client.bucket(BUCKET_ID).blob(blob_name).download_as_bytes()
    text = None
    for enc in ("utf-16", "utf-16le", "utf-8-sig", "utf-8"):
        try:
            text = data.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise UnicodeDecodeError("unknown", b"", 0, 0, "Could not decode CSV")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    reader = csv.DictReader(io.StringIO(text))
    rows = [{(k or "").strip(): (v or "").strip() for k, v in r.items()} for r in reader]

    # Debug: log available columns from first row
    if rows and LOG.isEnabledFor(logging.DEBUG):
        LOG.debug("Available columns in %s: %s", blob_name, list(rows[0].keys()))

    return rows

def _process_package_csv(client: storage.Client, package: str):
    """Collect and process metrics from latest CSV for a specific package."""
    prefix = f"stats/installs/installs_{package}_"
    blobs = list(client.list_blobs(BUCKET_ID, prefix=prefix))
    if not blobs:
        LOG.debug("No installs CSVs for %s", package)
        return

    # Filter for country CSV files and take the latest one
    country_blobs = [b for b in blobs if '_country.csv' in b.name]
    if not country_blobs:
        LOG.debug("No country CSVs for %s", package)
        return

    # Sort by name (which includes date) and take only the latest file
    latest_blob = sorted(country_blobs, key=lambda b: b.name, reverse=True)[0]
    m = _country_regex.match(latest_blob.name)
    if not m or m.group("pkg") != package:
        LOG.debug("Latest blob %s doesn't match package %s", latest_blob.name, package)
        return

    LOG.info("Processing CSV for %s: %s", package, latest_blob.name)
    rows = _download_csv(client, latest_blob.name)

    # Extract all unique dates using list comprehension - column is always "Date"
    date_strings = [r.get("Date", "") for r in rows]
    unique_dates = {_parse_date(date_str) for date_str in date_strings if date_str.strip()}
    unique_dates.discard(None)  # Remove None values from failed parsing

    if not unique_dates:
        LOG.info("No valid dates found in %s", latest_blob.name)
        return

    max_date = max(unique_dates)
    LOG.info("Max date found in %s: %s", latest_blob.name, max_date)

    # Process only rows with the latest date and extract multiple metrics
    country_metrics: Dict[str, Dict[str, float]] = {}
    for i, r in enumerate(rows):
        date_str = r.get("Date", "")
        date = _parse_date(date_str)
        if not date or date != max_date:
            continue

        country = (r.get("Country") or "").upper()
        if not country:
            continue

        # Extract multiple metrics from the row with debug logging
        daily_installs = r.get("Daily Device Installs") or "0"
        daily_uninstalls = r.get("Daily Device Uninstalls") or "0"
        active_installs = r.get("Active Device Installs") or "0"
        daily_user_installs = r.get("Daily User Installs") or "0"
        daily_user_uninstalls = r.get("Daily User Uninstalls") or "0"

        metrics = {
            "daily_device_installs": _extract_number(daily_installs),
            "daily_device_uninstalls": _extract_number(daily_uninstalls),
            "active_device_installs": _extract_number(active_installs),
            "daily_user_installs": _extract_number(daily_user_installs),
            "daily_user_uninstalls": _extract_number(daily_user_uninstalls)
        }

        # Debug log if we found non-zero values in other metrics
        if LOG.isEnabledFor(logging.DEBUG):
            if metrics["daily_device_uninstalls"] > 0 or metrics["active_device_installs"] > 0:
                LOG.debug("Non-zero metrics for %s: %s", country, metrics)
            elif metrics["daily_device_uninstalls"] == 0:
                LOG.debug("Zero uninstalls for %s, all metrics: %s", country, metrics)

        # Skip only if all metrics are zero (don't skip if some metrics are zero but others are not)
        if all(val <= 0 for val in metrics.values()):
            continue

        # Aggregate metrics by country
        if country not in country_metrics:
            country_metrics[country] = {k: 0.0 for k in metrics.keys()}

        for metric_name, value in metrics.items():
            country_metrics[country][metric_name] += value

        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Row %d: country=%s date=%s metrics=%s", i, country, date, metrics)

    LOG.info("Processed %d countries for date %s in package %s", len(country_metrics), max_date, package)

    # Debug: log what metrics we're about to export
    if LOG.isEnabledFor(logging.DEBUG):
        total_metrics = {metric: 0 for metric in counters.keys()}
        for country_metric in country_metrics.values():
            for metric_name, value in country_metric.items():
                if metric_name in total_metrics:
                    total_metrics[metric_name] += 1

        LOG.debug("Metrics to export: %s", total_metrics)
        LOG.debug("Country metrics sample: %s", dict(list(country_metrics.items())[:3]))

    for country, metrics in country_metrics.items():
        _export_metrics(package, country, metrics, max_date)

def _run_metrics_collection():
    """Initialize fresh metrics registry and collect data for all packages."""
    # Create fresh registry and counters for each collection
    global REGISTRY, counters
    REGISTRY = CollectorRegistry()
    counters = _create_prometheus_counters()

    client = _storage_client()
    for pkg in _discover_packages():
        try:
            _process_package_csv(client, pkg)
        except Exception as e:
            LOG.exception("Metrics collection failed for %s: %s", pkg, e)

_collection_thread = None
_stop_event = threading.Event()

def _background_collection():
    """Background thread for periodic metrics collection with configurable interval."""
    LOG.info("Starting background collection with interval %s seconds", COLLECTION_INTERVAL)

    # For test mode, run only once
    if TEST_MODE:
        LOG.info("TEST_MODE enabled - running single collection cycle")
        try:
            LOG.info("Starting metrics collection...")
            _run_metrics_collection()
            LOG.info("Metrics collection finished")

            # Print metrics statistics in test mode instead of full output
            if TEST_MODE:
                from prometheus_client import generate_latest
                metrics_output = generate_latest(REGISTRY).decode('utf-8')

                # Count metrics by type instead of showing full output
                metrics_count = {}
                for line in metrics_output.split('\n'):
                    if line.startswith('gplay_') and '{' in line:
                        metric_name = line.split('{')[0]
                        metrics_count[metric_name] = metrics_count.get(metric_name, 0) + 1

                LOG.info("Metrics collection completed. Summary:")
                for metric_name, count in metrics_count.items():
                    LOG.info("  %s: %d data points", metric_name, count)

            LOG.info("Test collection completed - exiting")
            os._exit(0)
        except Exception as e:
            LOG.exception("Test collection failed: %s", e)
            os._exit(1)
        return

    while not _stop_event.is_set():
        try:
            LOG.info("Starting metrics collection...")
            _run_metrics_collection()
            LOG.info("Metrics collection finished")
        except Exception as e:
            LOG.exception("Collection cycle failed: %s", e)

        # Wait for next collection cycle or stop signal
        _stop_event.wait(COLLECTION_INTERVAL)

def start_background_collection():
    """Start background metrics collection thread if not already running."""
    global _collection_thread
    if _collection_thread and _collection_thread.is_alive():
        LOG.warning("Background collection already running")
        return

    _collection_thread = threading.Thread(target=_background_collection, daemon=True)
    _collection_thread.start()
    LOG.info("Background collection thread started")

def stop_background_collection():
    """Stop background collection thread and wait for completion."""
    _stop_event.set()
    if _collection_thread:
        _collection_thread.join(timeout=10)
        LOG.info("Background collection stopped")

def app(environ, start_response):
    """WSGI application handler for metrics and health check endpoints."""
    path = environ.get("PATH_INFO", "/")
    if path == "/healthz":
        start_response("200 OK", [("Content-Type", "text/plain; charset=utf-8")])
        return [b"ok\n"]
    if path != "/metrics":
        start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
        return [b"not found\n"]

    # Just return the current metrics - no collection on request
    output = generate_latest(REGISTRY)
    start_response("200 OK", [("Content-Type", CONTENT_TYPE_LATEST)])
    return [output]

def main():
    """Main entry point: start background collection and HTTP server."""
    LOG.info(
        "Starting exporter :%s | bucket=%s | collection_interval=%ss | test_mode=%s",
        PORT, BUCKET_ID, COLLECTION_INTERVAL, bool(TEST_MODE)
    )

    # Start background collection
    start_background_collection()

    # For test mode, we don't need HTTP server
    if TEST_MODE:
        LOG.info("Test mode - waiting for collection to complete")
        try:
            if _collection_thread:
                _collection_thread.join()
        except KeyboardInterrupt:
            LOG.info("Test interrupted")
        return

    # Start HTTP server
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
