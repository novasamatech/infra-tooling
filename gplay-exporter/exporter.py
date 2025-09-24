#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""
Google Play Console Metrics Exporter for Prometheus

This exporter fetches daily statistics from Google Play Console CSV reports
stored in Google Cloud Storage and exposes them as Prometheus counter metrics.
"""

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
from wsgiref.simple_server import make_server, WSGIRequestHandler

# Google Cloud Storage libraries
from google.cloud import storage  # pip install google-cloud-storage
from google.oauth2 import service_account

# Configure logging
LOG = logging.getLogger("gplay_exporter")
logging.basicConfig(
    level=os.environ.get("GPLAY_EXPORTER_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# ------------ Configuration from environment variables ------------
GOOGLE_CREDS = os.environ.get("GPLAY_EXPORTER_GOOGLE_APPLICATION_CREDENTIALS")
BUCKET_ID = os.environ.get("GPLAY_EXPORTER_BUCKET_ID")

if not GOOGLE_CREDS or not BUCKET_ID:
    LOG.error("Missing env vars: GPLAY_EXPORTER_GOOGLE_APPLICATION_CREDENTIALS and/or GPLAY_EXPORTER_BUCKET_ID")
    sys.exit(2)

# Optional configuration with defaults
PORT = int(os.environ.get("GPLAY_EXPORTER_PORT", "8000"))
COLLECTION_INTERVAL = int(os.environ.get("GPLAY_EXPORTER_COLLECTION_INTERVAL_SECONDS", "43200"))  # default 12h
GCS_PROJECT = os.environ.get("GPLAY_EXPORTER_GCS_PROJECT")
TEST_MODE = os.environ.get("GPLAY_EXPORTER_TEST_MODE")

# ------------ Health check state ------------
# Simple health tracking - service is healthy after first successful collection
_health_lock = threading.Lock()
_health_status = {
    "healthy": False,
    "first_collection_done": False,
    "last_collection_time": None,
    "last_error": None
}

def _update_health_status(success: bool, error: Optional[str] = None) -> None:
    """
    Update health check status based on collection results.

    Args:
        success: Whether the collection was successful
        error: Error message if collection failed
    """
    with _health_lock:
        _health_status["last_collection_time"] = dt.datetime.now()

        if success:
            _health_status["healthy"] = True
            _health_status["first_collection_done"] = True
            _health_status["last_error"] = None
            LOG.debug("Health status updated: healthy=True")
        else:
            _health_status["last_error"] = error
            # Only mark unhealthy if we never had a successful collection
            if not _health_status["first_collection_done"]:
                _health_status["healthy"] = False
            LOG.debug("Health status updated: healthy=%s, error=%s",
                     _health_status["healthy"], error)

def _is_healthy() -> bool:
    """Check if the service is healthy."""
    with _health_lock:
        return _health_status["healthy"]

# ------------ Prometheus registry & counters ------------
# Global registry for metrics collection
REGISTRY = CollectorRegistry()
registry_lock = threading.Lock()

def _create_prometheus_counters():
    """
    Create fresh Prometheus counter instances for all metrics.

    Counters are used instead of gauges because these metrics represent
    cumulative values that only increase over time for a given date.
    We recreate them on each collection cycle to handle date changes.

    Returns:
        Dict of counter name to Counter object
    """
    counters = {
        "daily_device_installs": Counter(
            "gplay_daily_device_installs_total",
            "Daily device installs by country from Google Play Console",
            ["package", "country"],
            registry=REGISTRY,
        ),
        "daily_device_uninstalls": Counter(
            "gplay_daily_device_uninstalls_total",
            "Daily device uninstalls by country from Google Play Console",
            ["package", "country"],
            registry=REGISTRY,
        ),
        "active_device_installs": Counter(
            "gplay_active_device_installs_total",
            "Active device installs by country from Google Play Console",
            ["package", "country"],
            registry=REGISTRY,
        ),
        "daily_user_installs": Counter(
            "gplay_daily_user_installs_total",
            "Daily user installs by country from Google Play Console",
            ["package", "country"],
            registry=REGISTRY,
        ),
        "daily_user_uninstalls": Counter(
            "gplay_daily_user_uninstalls_total",
            "Daily user uninstalls by country from Google Play Console",
            ["package", "country"],
            registry=REGISTRY,
        )
    }
    return counters

# Initialize counters
counters = _create_prometheus_counters()

def _export_metrics(package: str, country: str, metrics: Dict[str, float], date: dt.date):
    """
    Export multiple metrics for a country.

    Only exports metrics with non-zero values to avoid creating empty series.

    Args:
        package: Android package name
        country: Country code (e.g., "US")
        metrics: Dict of metric name to value
        date: Date of the metrics
    """
    labels = {"package": package, "country": country}

    for metric_name, value in metrics.items():
        if metric_name in counters:
            # Skip zero values to avoid creating empty series
            if value <= 0:
                if LOG.isEnabledFor(logging.DEBUG):
                    LOG.debug("Skipping zero metric: %s for %s/%s", metric_name, package, country)
                continue

            # Set the counter to the absolute value from the CSV
            # This is safe because we recreate counters on each collection
            counters[metric_name].labels(**labels)._value.set(value)

            if LOG.isEnabledFor(logging.DEBUG):
                LOG.debug("Exported metric: %s=%s for %s/%s on %s",
                         metric_name, value, package, country, date)
        else:
            LOG.warning("Unknown metric %s for %s/%s", metric_name, package, country)

def _load_credentials():
    """
    Load Google Cloud credentials from file or default authentication.

    Returns:
        Google Cloud credentials object
    """
    if GOOGLE_CREDS and os.path.exists(GOOGLE_CREDS):
        LOG.debug("Loading credentials from file: %s", GOOGLE_CREDS)
        return service_account.Credentials.from_service_account_file(GOOGLE_CREDS)
    else:
        LOG.debug("Using default Google Cloud authentication")
        import google.auth
        creds, _ = google.auth.default()
        return creds

def _storage_client():
    """
    Create and return Google Cloud Storage client with credentials.

    Returns:
        storage.Client instance
    """
    creds = _load_credentials()
    return storage.Client(project=GCS_PROJECT, credentials=creds)

# Package discovery cache
_pkgs: Set[str] = set()

# Regex patterns for parsing GCS blob names
_pkg_regex = re.compile(r"^stats/installs/installs_(?P<pkg>[^_]+)_(?P<yyyymm>\d{6})_(country|overview)\.csv$")
_country_regex = re.compile(r"^stats/installs/installs_(?P<pkg>[^_]+)_(?P<yyyymm>\d{6})_country\.csv$")

def _discover_packages_from_gcs(client) -> Set[str]:
    """
    Scan GCS bucket for package CSV files and extract package names.

    Args:
        client: Google Cloud Storage client

    Returns:
        Set of discovered package names
    """
    pkgs: Set[str] = set()
    try:
        # List all blobs in the stats/installs/ prefix
        for blob in client.list_blobs(BUCKET_ID, prefix="stats/installs/"):
            m = _pkg_regex.match(blob.name)
            if m:
                pkgs.add(m.group("pkg"))
    except Exception as e:
        LOG.error("GCS scan for packages failed: %s", e)
    return pkgs

def _discover_packages() -> Set[str]:
    """
    Discover all available packages in GCS bucket and cache results.

    Returns:
        Set of package names found in GCS
    """
    client = _storage_client()
    found = _discover_packages_from_gcs(client)

    if not found:
        LOG.warning("No packages discovered via GCS. Check permissions and bucket id.")
    else:
        LOG.info("Discovered %d packages: %s", len(found), ", ".join(sorted(found)))

    # Update cache
    _pkgs.clear()
    _pkgs.update(found)
    return set(_pkgs)

def _parse_date(s: str) -> Optional[dt.date]:
    """
    Parse date string using multiple supported formats.

    Args:
        s: Date string to parse

    Returns:
        Parsed date object or None if parsing failed
    """
    # Try common date formats used in Play Console exports
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%m/%d/%Y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None

def _extract_number(s: str) -> float:
    """
    Extract numeric value from string, handling commas and empty values.

    Args:
        s: String containing a number

    Returns:
        Float value extracted from string, or 0.0 if extraction failed
    """
    if s is None:
        return 0.0
    # Remove commas and whitespace
    s = s.replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return 0.0

def _download_csv(client: storage.Client, blob_name: str) -> List[dict]:
    """
    Download and parse CSV file from GCS with automatic encoding detection.

    Args:
        client: Google Cloud Storage client
        blob_name: Name of the blob to download

    Returns:
        List of dicts representing CSV rows
    """
    # Download blob content
    data = client.bucket(BUCKET_ID).blob(blob_name).download_as_bytes()

    # Try different encodings (Play Console uses UTF-16)
    text = None
    for enc in ("utf-16", "utf-16le", "utf-8-sig", "utf-8"):
        try:
            text = data.decode(enc)
            break
        except UnicodeDecodeError:
            continue

    if text is None:
        raise UnicodeDecodeError("unknown", b"", 0, 0, "Could not decode CSV")

    # Normalize line endings
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # Parse CSV
    reader = csv.DictReader(io.StringIO(text))
    # Strip whitespace from keys and values
    rows = [{(k or "").strip(): (v or "").strip() for k, v in r.items()} for r in reader]

    # Log available columns for debugging
    if rows and LOG.isEnabledFor(logging.DEBUG):
        LOG.debug("Available columns in %s: %s", blob_name, list(rows[0].keys()))

    return rows

def _process_package_csv(client: storage.Client, package: str):
    """
    Collect and process metrics from latest CSV for a specific package.

    Args:
        client: Google Cloud Storage client
        package: Android package name to process
    """
    # Build prefix for this package's files
    prefix = f"stats/installs/installs_{package}_"
    blobs = list(client.list_blobs(BUCKET_ID, prefix=prefix))

    if not blobs:
        LOG.debug("No installs CSVs for %s", package)
        return

    # Filter for country CSV files (not overview files)
    country_blobs = [b for b in blobs if '_country.csv' in b.name]
    if not country_blobs:
        LOG.debug("No country CSVs for %s", package)
        return

    # Sort by name (which includes date YYYYMM) and take only the latest file
    latest_blob = sorted(country_blobs, key=lambda b: b.name, reverse=True)[0]

    # Validate blob name matches expected pattern
    m = _country_regex.match(latest_blob.name)
    if not m or m.group("pkg") != package:
        LOG.debug("Latest blob %s doesn't match package %s", latest_blob.name, package)
        return

    LOG.info("Processing CSV for %s: %s", package, latest_blob.name)

    # Download and parse CSV
    rows = _download_csv(client, latest_blob.name)

    # Extract all unique dates from the CSV
    date_strings = [r.get("Date", "") for r in rows]
    unique_dates = {_parse_date(date_str) for date_str in date_strings if date_str.strip()}
    unique_dates.discard(None)  # Remove None values from failed parsing

    if not unique_dates:
        LOG.info("No valid dates found in %s", latest_blob.name)
        return

    # Find the most recent date in the file
    max_date = max(unique_dates)
    LOG.info("Processing metrics for date %s from %s", max_date, latest_blob.name)

    # Process only rows with the latest date and extract multiple metrics
    country_metrics: Dict[str, Dict[str, float]] = {}

    for r in rows:
        # Check if this row is for the latest date
        date_str = r.get("Date", "")
        date = _parse_date(date_str)
        if not date or date != max_date:
            continue

        # Extract country code
        country = (r.get("Country") or "").upper()
        if not country:
            continue

        # Extract all metric values from the row
        metrics = {
            "daily_device_installs": _extract_number(r.get("Daily Device Installs") or "0"),
            "daily_device_uninstalls": _extract_number(r.get("Daily Device Uninstalls") or "0"),
            "active_device_installs": _extract_number(r.get("Active Device Installs") or "0"),
            "daily_user_installs": _extract_number(r.get("Daily User Installs") or "0"),
            "daily_user_uninstalls": _extract_number(r.get("Daily User Uninstalls") or "0")
        }

        # Skip rows where all metrics are zero (no data)
        if all(val <= 0 for val in metrics.values()):
            continue

        # Aggregate metrics by country (in case of duplicate rows)
        if country not in country_metrics:
            country_metrics[country] = {k: 0.0 for k in metrics.keys()}

        for metric_name, value in metrics.items():
            country_metrics[country][metric_name] += value

    LOG.info("Processed %d countries for date %s in package %s",
             len(country_metrics), max_date, package)

    # Export metrics to Prometheus
    for country, metrics in country_metrics.items():
        _export_metrics(package, country, metrics, max_date)

def _run_metrics_collection():
    """
    Initialize fresh metrics registry and collect data for all packages.

    This function recreates the metrics registry to ensure we start with
    clean counters for each collection cycle.

    Returns:
        bool: True if collection was successful, False otherwise
    """
    global REGISTRY, counters

    success = False
    packages_processed = 0

    try:
        # Create fresh registry and counters for each collection
        # This ensures counters are reset for new dates
        with registry_lock:
            REGISTRY = CollectorRegistry()
            counters = _create_prometheus_counters()

        # Create GCS client
        client = _storage_client()

        # Discover all packages
        packages = _discover_packages()

        if not packages:
            LOG.warning("No packages found to process")
            _update_health_status(False, "No packages discovered")
            return False

        # Process each package
        for pkg in packages:
            try:
                _process_package_csv(client, pkg)
                packages_processed += 1
            except Exception as e:
                LOG.exception("Metrics collection failed for %s: %s", pkg, e)

        # Collection is successful if at least one package was processed
        success = packages_processed > 0

    except Exception as e:
        LOG.exception("Metrics collection cycle failed: %s", e)
        success = False

    # Update health status
    if success:
        _update_health_status(True)
        LOG.info("Collection successful, processed %d packages", packages_processed)
    else:
        _update_health_status(False, "No packages processed")
        LOG.warning("Collection failed or incomplete")

    return success

# Background collection thread management
_collection_thread = None
_stop_event = threading.Event()

def _background_collection():
    """
    Background thread for periodic metrics collection with configurable interval.

    Runs collection cycles at regular intervals until stopped.
    """
    LOG.info("Starting background collection with interval %s seconds", COLLECTION_INTERVAL)

    # Special handling for test mode - run once and exit
    if TEST_MODE:
        LOG.info("TEST_MODE enabled - running single collection cycle")
        try:
            LOG.info("Starting metrics collection...")
            success = _run_metrics_collection()
            LOG.info("Metrics collection finished, success: %s", success)

            # In test mode, print summary instead of full metrics output
            if TEST_MODE:
                from prometheus_client import generate_latest
                metrics_output = generate_latest(REGISTRY).decode('utf-8')

                # Count metrics by type
                metrics_count = {}
                for line in metrics_output.split('\n'):
                    if line.startswith('gplay_') and '{' in line:
                        metric_name = line.split('{')[0]
                        metrics_count[metric_name] = metrics_count.get(metric_name, 0) + 1

                LOG.info("Metrics collection completed. Summary:")
                for metric_name, count in metrics_count.items():
                    LOG.info("  %s: %d data points", metric_name, count)

            LOG.info("Test collection completed - exiting")
            sys.exit(0 if success else 1)
        except Exception as e:
            LOG.exception("Test collection failed: %s", e)
            sys.exit(1)
        return

    # Normal operation - run collection in a loop
    while not _stop_event.is_set():
        try:
            LOG.info("Starting metrics collection...")
            _run_metrics_collection()
            LOG.info("Metrics collection finished")
        except Exception as e:
            LOG.exception("Collection cycle failed: %s", e)
            _update_health_status(False, str(e))

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

# Custom request handler to reduce logging verbosity
class QuietWSGIRequestHandler(WSGIRequestHandler):
    """Custom WSGI request handler that only logs in DEBUG mode."""

    def log_message(self, format, *args):
        """Override to only log requests in DEBUG mode."""
        if LOG.isEnabledFor(logging.DEBUG):
            super().log_message(format, *args)

def app(environ, start_response):
    """
    WSGI application handler for metrics and health check endpoints.

    Args:
        environ: WSGI environment dictionary
        start_response: WSGI response callback

    Returns:
        Response body as list of bytes
    """
    path = environ.get("PATH_INFO", "/")

    # Log requests only in DEBUG mode
    if LOG.isEnabledFor(logging.DEBUG):
        LOG.debug("Request: %s %s", environ.get("REQUEST_METHOD", "GET"), path)

    # Health check endpoint - simple OK/NOT OK response
    if path == "/healthz":
        if _is_healthy():
            start_response("200 OK", [("Content-Type", "text/plain; charset=utf-8")])
            return [b"ok\n"]
        else:
            start_response("503 Service Unavailable", [("Content-Type", "text/plain; charset=utf-8")])
            return [b"not ok\n"]

    # Metrics endpoint
    if path == "/metrics":
        # Generate metrics output from current registry
        with registry_lock:
            output = generate_latest(REGISTRY)
        start_response("200 OK", [("Content-Type", CONTENT_TYPE_LATEST)])
        return [output]

    # Unknown endpoint
    start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
    return [b"not found\n"]

def main():
    """Main entry point: start background collection and HTTP server."""
    LOG.info(
        "Starting exporter :%s | bucket=%s | collection_interval=%ss | test_mode=%s",
        PORT, BUCKET_ID, COLLECTION_INTERVAL, bool(TEST_MODE)
    )

    # Start background collection thread
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

    # Start HTTP server for normal operation
    httpd = make_server("0.0.0.0", PORT, app, handler_class=QuietWSGIRequestHandler)
    try:
        LOG.info("HTTP server started on port %s", PORT)
        httpd.serve_forever()
    except KeyboardInterrupt:
        LOG.info("Shutting down")
    finally:
        stop_background_collection()

if __name__ == "__main__":
    main()
