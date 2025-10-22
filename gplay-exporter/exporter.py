#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""
Google Play Console Metrics Exporter

This exporter fetches monthly statistics from Google Play Console CSV reports
stored in Google Cloud Storage and exposes them as Prometheus counter metrics
with proper timestamp support.
"""

import os
import io
import re
import csv
import sys
import logging
import datetime as dt
import threading
import time
from typing import Dict, List, Optional, Set, Tuple

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
    LOG.error(
        "Missing env vars: GPLAY_EXPORTER_GOOGLE_APPLICATION_CREDENTIALS and/or GPLAY_EXPORTER_BUCKET_ID"
    )
    sys.exit(2)

# Optional configuration with defaults
PORT = int(os.environ.get("GPLAY_EXPORTER_PORT", "8000"))
COLLECTION_INTERVAL = int(
    os.environ.get("GPLAY_EXPORTER_COLLECTION_INTERVAL_SECONDS", "43200")
)  # default 12h
GCS_PROJECT = os.environ.get("GPLAY_EXPORTER_GCS_PROJECT")
TEST_MODE = os.environ.get("GPLAY_EXPORTER_TEST_MODE")

# ------------ Health check state ------------
# Simple health tracking - service is healthy after first successful collection
_health_lock = threading.Lock()
_health_status = {
    "healthy": False,
    "first_collection_done": False,
    "last_collection_time": None,
    "last_error": None,
}

# ------------ Metrics storage ------------
# Store metrics data with timestamps
_metrics_lock = threading.Lock()
_metrics_data = {}  # Format: {metric_name: {(package, country): (value, timestamp_ms)}}

# Metric definitions with aggregation strategy
METRIC_DEFINITIONS = {
    "gplay_device_installs_v2": {
        "help": "Device installs by country from Google Play Console",
        "type": "counter",
        "csv_column": "Daily Device Installs",
        "aggregation": "sum",  # Sum all days in the month
    },
    "gplay_device_uninstalls_v2": {
        "help": "Device uninstalls by country from Google Play Console",
        "type": "counter",
        "csv_column": "Daily Device Uninstalls",
        "aggregation": "sum",  # Sum all days in the month
    },
    "gplay_active_device_installs_v2": {
        "help": "Active device installs by country from Google Play Console",
        "type": "counter",
        "csv_column": "Active Device Installs",
        "aggregation": "last",  # Take last value (absolute/cumulative metric)
    },
    "gplay_user_installs_v2": {
        "help": "User installs by country from Google Play Console",
        "type": "counter",
        "csv_column": "Daily User Installs",
        "aggregation": "sum",  # Sum all days in the month
    },
    "gplay_user_uninstalls_v2": {
        "help": "User uninstalls by country from Google Play Console",
        "type": "counter",
        "csv_column": "Daily User Uninstalls",
        "aggregation": "sum",  # Sum all days in the month
    },
}


# ------------ Health check functions ------------
def _update_health_status(success: bool, error: Optional[str] = None):
    """
    Update the health status of the service.

    Args:
        success: Whether the last collection was successful
        error: Optional error message if collection failed
    """
    with _health_lock:
        _health_status["last_collection_time"] = dt.datetime.now()

        if success:
            # Once healthy, always healthy (cached metrics can still be served)
            _health_status["healthy"] = True
            _health_status["first_collection_done"] = True
            _health_status["last_error"] = None
            LOG.debug("Health status updated: healthy")
        else:
            _health_status["last_error"] = error
            # Only mark as unhealthy if no successful collection has been done yet
            if not _health_status["first_collection_done"]:
                _health_status["healthy"] = False
                LOG.debug(
                    "Health status updated: not healthy (no successful collections yet)"
                )
            else:
                LOG.debug(
                    "Health status: remaining healthy despite error (serving cached metrics)"
                )


def _is_healthy() -> bool:
    """Check if the service is healthy."""
    with _health_lock:
        return _health_status["healthy"]


# ------------ Metric formatting functions ------------
def _format_prometheus_output() -> str:
    """
    Format metrics data as Prometheus text format with timestamps.

    Returns:
        String in Prometheus text exposition format
    """
    lines = []

    with _metrics_lock:
        # Group metrics by name for proper formatting
        for metric_name, metric_info in METRIC_DEFINITIONS.items():
            # Add HELP and TYPE lines
            lines.append(f"# HELP {metric_name} {metric_info['help']}")
            lines.append(f"# TYPE {metric_name} {metric_info['type']}")

            # Add metric values if they exist
            if metric_name in _metrics_data:
                for (package, country), (value, timestamp_ms) in _metrics_data[
                    metric_name
                ].items():
                    # Skip zero values to avoid empty series
                    if value <= 0:
                        continue

                    # Format: metric_name{label1="value1",label2="value2"} value timestamp
                    labels = f'package="{package}",country="{country}"'
                    lines.append(f"{metric_name}{{{labels}}} {value} {timestamp_ms}")

    # Add empty line at the end
    lines.append("")
    return "\n".join(lines)


# ------------ Google Cloud Storage functions ------------
def _load_credentials():
    """
    Load Google Cloud credentials from service account file.

    Returns:
        Service account credentials object
    """
    if not os.path.exists(GOOGLE_CREDS):
        raise FileNotFoundError(f"Credentials file not found: {GOOGLE_CREDS}")

    try:
        creds = service_account.Credentials.from_service_account_file(
            GOOGLE_CREDS, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        return creds
    except Exception as e:
        LOG.error("Failed to load credentials: %s", e)
        raise


def _storage_client() -> storage.Client:
    """
    Create and return a Google Cloud Storage client.

    Returns:
        Configured GCS client
    """
    creds = _load_credentials()
    client = storage.Client(credentials=creds, project=GCS_PROJECT)
    return client


# Regex patterns for parsing GCS blob names
_pkg_regex = re.compile(
    r"^stats/installs/installs_(?P<pkg>[^_]+)_(?P<yyyymm>\d{6})_(country|overview)\.csv$"
)
_country_regex = re.compile(
    r"^stats/installs/installs_(?P<pkg>[^_]+)_(?P<yyyymm>\d{6})_country\.csv$"
)


def _discover_packages_from_gcs(client) -> Set[str]:
    """
    Discover all unique package names from GCS bucket.

    Args:
        client: Google Cloud Storage client

    Returns:
        Set of discovered package names
    """
    prefix = "stats/installs/"
    blobs = client.list_blobs(BUCKET_ID, prefix=prefix)

    packages = set()
    for blob in blobs:
        m = _pkg_regex.match(blob.name)
        if m:
            packages.add(m.group("pkg"))

    LOG.info("Discovered %d packages in GCS", len(packages))
    return packages


def _discover_packages() -> Set[str]:
    """
    Main package discovery function.

    Returns:
        Set of package names to process
    """
    try:
        client = _storage_client()
        packages = _discover_packages_from_gcs(client)

        if not packages:
            LOG.warning("No packages discovered from GCS")
            return set()

        return packages

    except Exception as e:
        LOG.exception("Package discovery failed: %s", e)
        return set()


def _parse_date(date_str: str) -> Optional[dt.date]:
    """
    Parse various date formats found in CSV files.

    Args:
        date_str: Date string to parse

    Returns:
        Parsed date object or None if parsing fails
    """
    if not date_str or not date_str.strip():
        return None

    # Try different date formats
    for fmt in ["%Y-%m-%d", "%d-%b-%Y", "%m/%d/%Y"]:
        try:
            return dt.datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue

    LOG.debug("Could not parse date: %s", date_str)
    return None


def _extract_number(s: str) -> float:
    """
    Extract numeric value from string, handling commas and decimals.

    Args:
        s: String containing a number

    Returns:
        Extracted float value or 0.0 if extraction fails
    """
    if not s or not s.strip():
        return 0.0

    try:
        # Remove commas and convert to float
        cleaned = s.replace(",", "").strip()
        return float(cleaned)
    except (ValueError, AttributeError):
        LOG.debug("Could not extract number from: %s", s)
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
    rows = [
        {(k or "").strip(): (v or "").strip() for k, v in r.items()} for r in reader
    ]

    # Log available columns for debugging
    if rows and LOG.isEnabledFor(logging.DEBUG):
        LOG.debug("Available columns in %s: %s", blob_name, list(rows[0].keys()))

    return rows


def _process_package_csv(client: storage.Client, package: str):
    """
    Collect and process metrics from latest CSV for a specific package.
    Uses different aggregation strategies based on metric type.

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
    country_blobs = [b for b in blobs if "_country.csv" in b.name]
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
    unique_dates = {
        _parse_date(date_str) for date_str in date_strings if date_str.strip()
    }
    unique_dates.discard(None)  # Remove None values from failed parsing

    if not unique_dates:
        LOG.info("No valid dates found in %s", latest_blob.name)
        return

    # Find the most recent date in the file for timestamp
    max_date = max(unique_dates)
    # Convert date to milliseconds timestamp (Unix timestamp * 1000)
    timestamp_ms = int(dt.datetime.combine(max_date, dt.time.min).timestamp() * 1000)

    LOG.info("Processing data from %s, latest date: %s", latest_blob.name, max_date)

    # Process metrics based on aggregation strategy
    country_metrics: Dict[str, Dict[str, float]] = {}

    # For metrics that need summing
    sum_metrics = {
        name: info
        for name, info in METRIC_DEFINITIONS.items()
        if info["aggregation"] == "sum"
    }

    # For metrics that need last value
    last_metrics = {
        name: info
        for name, info in METRIC_DEFINITIONS.items()
        if info["aggregation"] == "last"
    }

    # Process all rows for sum aggregation
    for r in rows:
        # Parse date to ensure it's valid
        date_str = r.get("Date", "")
        date = _parse_date(date_str)
        if not date:
            continue

        # Extract country code
        country = (r.get("Country") or "").upper()
        if not country:
            continue

        # Initialize country metrics if needed
        if country not in country_metrics:
            country_metrics[country] = {}
            for metric_name in METRIC_DEFINITIONS:
                country_metrics[country][metric_name] = 0.0

        # Sum metrics that need aggregation
        for metric_name, metric_info in sum_metrics.items():
            csv_column = metric_info["csv_column"]
            value = _extract_number(r.get(csv_column) or "0")
            country_metrics[country][metric_name] += value

        # For last-value metrics, only use data from the max date
        if date == max_date:
            for metric_name, metric_info in last_metrics.items():
                csv_column = metric_info["csv_column"]
                value = _extract_number(r.get(csv_column) or "0")
                # For last value metrics, replace rather than sum
                country_metrics[country][metric_name] = value

    LOG.info(
        "Processed %d countries from %s",
        len(country_metrics),
        latest_blob.name,
    )

    # Store metrics with timestamp
    with _metrics_lock:
        for country, metrics in country_metrics.items():
            for metric_name, value in metrics.items():
                # Skip zero values
                if value <= 0:
                    continue

                # Initialize metric dict if needed
                if metric_name not in _metrics_data:
                    _metrics_data[metric_name] = {}

                # Store value with timestamp
                _metrics_data[metric_name][(package, country)] = (value, timestamp_ms)

                if LOG.isEnabledFor(logging.DEBUG):
                    LOG.debug(
                        "Stored metric: %s=%s for %s/%s with timestamp %s",
                        metric_name,
                        value,
                        package,
                        country,
                        timestamp_ms,
                    )


# ------------ Background thread ------------
collection_thread = None
stop_event = threading.Event()


def _run_metrics_collection():
    """
    Initialize fresh metrics and collect data for all packages.

    Returns:
        bool: True if collection was successful, False otherwise
    """
    global _metrics_data

    success = False
    packages_processed = 0

    try:
        # Clear existing metrics
        with _metrics_lock:
            _metrics_data = {}

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


def _background_collection():
    """
    Background thread function that runs periodic collections.
    """
    LOG.info("Background collection thread started")

    while not stop_event.is_set():
        try:
            LOG.info("Starting metrics collection cycle")
            start_time = time.time()

            # Run the collection
            _run_metrics_collection()

            elapsed = time.time() - start_time
            LOG.info("Collection cycle completed in %.2f seconds", elapsed)

        except Exception as e:
            LOG.exception("Unexpected error in collection thread: %s", e)
            _update_health_status(False, str(e))

        # Wait for next collection interval (or until stop event)
        if not stop_event.wait(COLLECTION_INTERVAL):
            LOG.debug(
                "Starting next collection cycle after %d seconds", COLLECTION_INTERVAL
            )

    LOG.info("Background collection thread stopped")


def start_background_collection():
    """Start the background metrics collection thread."""
    global collection_thread

    if collection_thread and collection_thread.is_alive():
        LOG.warning("Collection thread already running")
        return

    stop_event.clear()
    collection_thread = threading.Thread(target=_background_collection, daemon=True)
    collection_thread.start()
    LOG.info("Started background collection thread")


def stop_background_collection():
    """Stop the background metrics collection thread."""
    LOG.info("Stopping background collection thread")
    stop_event.set()
    if collection_thread:
        collection_thread.join(timeout=5)


# ------------ HTTP Server ------------
class QuietWSGIRequestHandler(WSGIRequestHandler):
    """Custom request handler that only logs in DEBUG mode."""

    def log_message(self, format, *args):
        """Override to only log requests in DEBUG mode."""
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug(f"HTTP: {format}", *args)


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
            start_response(
                "503 Service Unavailable",
                [("Content-Type", "text/plain; charset=utf-8")],
            )
            return [b"not ok\n"]

    # Metrics endpoint
    if path == "/metrics":
        # Generate metrics output with timestamps
        output = _format_prometheus_output()
        start_response("200 OK", [("Content-Type", "text/plain; version=0.0.4")])
        return [output.encode("utf-8")]

    # Unknown endpoint
    start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
    return [b"not found\n"]


def main():
    """Main entry point."""
    LOG.info("Google Play Console Metrics Exporter starting...")
    LOG.info("Port: %d", PORT)
    LOG.info("Collection interval: %d seconds", COLLECTION_INTERVAL)
    LOG.info("GCS bucket: %s", BUCKET_ID)

    # Test mode: run one collection and exit
    if TEST_MODE:
        LOG.info("TEST MODE: Running single collection cycle")
        success = _run_metrics_collection()

        # Display collected metrics in test mode
        if LOG.isEnabledFor(logging.DEBUG):
            output = _format_prometheus_output()
            LOG.debug("Collected metrics:\n%s", output)

        sys.exit(0 if success else 1)

    # Start background collection thread
    start_background_collection()

    # Create and start WSGI server
    try:
        with make_server("", PORT, app, handler_class=QuietWSGIRequestHandler) as httpd:
            LOG.info("HTTP server listening on port %d", PORT)
            httpd.serve_forever()
    except KeyboardInterrupt:
        LOG.info("Shutting down...")
        stop_background_collection()
    except Exception as e:
        LOG.exception("Server error: %s", e)
        stop_background_collection()
        sys.exit(1)


if __name__ == "__main__":
    main()
