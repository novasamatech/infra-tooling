#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""
Google Play Console Metrics Exporter

This exporter fetches monthly statistics from Google Play Console CSV reports
stored in Google Cloud Storage and exposes them as Prometheus gauge metrics
with proper timestamp support for each individual date.
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
MONTHS_LOOKBACK = int(
    os.environ.get("GPLAY_EXPORTER_MONTHS_LOOKBACK", "1")
)  # default 1 month

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
# Key format: {metric_name: {(package, country, date_str): (value, timestamp_ms)}}
_metrics_lock = threading.Lock()
_metrics_data = {}

# Metric definitions - all metrics are now gauges
METRIC_DEFINITIONS = {
    "gplay_device_installs_v3": {
        "help": "Device installs by country and date from Google Play Console",
        "type": "gauge",
        "csv_column": "Daily Device Installs",
    },
    "gplay_device_uninstalls_v3": {
        "help": "Device uninstalls by country and date from Google Play Console",
        "type": "gauge",
        "csv_column": "Daily Device Uninstalls",
    },
    "gplay_active_device_installs_v3": {
        "help": "Active device installs by country and date from Google Play Console",
        "type": "gauge",
        "csv_column": "Active Device Installs",
    },
    "gplay_user_installs_v3": {
        "help": "User installs by country and date from Google Play Console",
        "type": "gauge",
        "csv_column": "Daily User Installs",
    },
    "gplay_user_uninstalls_v3": {
        "help": "User uninstalls by country and date from Google Play Console",
        "type": "gauge",
        "csv_column": "Daily User Uninstalls",
    },
}


# ------------ Health check functions ------------


def _update_health_status(
    error: Optional[Exception] = None, collection_done: bool = False
):
    """
    Update the health status of the exporter.

    Args:
        error: Exception if collection failed, None if successful
        collection_done: True if collection cycle completed (success or failure)
    """
    with _health_lock:
        if collection_done:
            _health_status["last_collection_time"] = dt.datetime.utcnow().isoformat()

            if error is None:
                # Successful collection
                _health_status["healthy"] = True
                _health_status["first_collection_done"] = True
                _health_status["last_error"] = None
                LOG.debug("Health status: healthy after successful collection")
            else:
                # Failed collection
                _health_status["last_error"] = str(error)
                # Stay healthy if we've had at least one successful collection
                if _health_status["first_collection_done"]:
                    LOG.debug("Health status: staying healthy despite error: %s", error)
                else:
                    _health_status["healthy"] = False
                    LOG.debug(
                        "Health status: not healthy, first collection failed: %s",
                        error,
                    )


def _is_healthy() -> bool:
    """Check if the exporter is healthy."""
    with _health_lock:
        return _health_status.get("healthy", False)


# ------------ Prometheus format generation ------------
def _format_prometheus_output() -> str:
    """
    Manually generate Prometheus text exposition format with timestamps.

    Returns:
        String in Prometheus text format with inline timestamps (milliseconds)
    """
    output_lines = []

    with _metrics_lock:
        # Generate output for each metric type
        for metric_name, metric_info in METRIC_DEFINITIONS.items():
            # Add HELP and TYPE lines
            output_lines.append(f"# HELP {metric_name} {metric_info['help']}")
            output_lines.append(f"# TYPE {metric_name} {metric_info['type']}")

            # Add metric values if present
            if metric_name in _metrics_data:
                for (package, country, date_str), (value, timestamp_ms) in sorted(
                    _metrics_data[metric_name].items()
                ):
                    # Skip zero and negative values
                    if value <= 0:
                        continue

                    # Format: metric_name{label1="value1",label2="value2"} value timestamp
                    output_lines.append(
                        f'{metric_name}{{package="{package}",country="{country}"}} {value} {timestamp_ms}'
                    )

    # Join with newlines and add final newline
    return "\n".join(output_lines) + "\n"


# ------------ Google Cloud Storage functions ------------


def _load_credentials() -> service_account.Credentials:
    """
    Load Google Cloud credentials from the JSON file.

    Returns:
        Google service account credentials

    Raises:
        Exception if credentials cannot be loaded
    """
    try:
        credentials = service_account.Credentials.from_service_account_file(
            GOOGLE_CREDS
        )
        LOG.debug("Loaded credentials from %s", GOOGLE_CREDS)
        return credentials
    except Exception as e:
        LOG.error("Failed to load credentials from %s: %s", GOOGLE_CREDS, e)
        raise


def _storage_client() -> storage.Client:
    """
    Create a Google Cloud Storage client with credentials.

    Returns:
        Configured storage client
    """
    credentials = _load_credentials()
    return storage.Client(credentials=credentials, project=GCS_PROJECT)


# ------------ CSV discovery and parsing ------------
# Precompile regex for package discovery from file names
# Format: installs_<package>_<YYYYMM>_country.csv
_country_regex = re.compile(
    r"^stats/installs/installs_(?P<pkg>[\w\.]+)_(?P<date>\d{6})_country\.csv$"
)


def _discover_packages_from_gcs(client: storage.Client) -> Set[str]:
    """
    Discover all Android packages from Google Cloud Storage bucket.

    Args:
        client: Google Cloud Storage client

    Returns:
        Set of discovered package names
    """
    packages = set()
    prefix = "stats/installs/"

    for blob in client.list_blobs(BUCKET_ID, prefix=prefix):
        m = _country_regex.match(blob.name)
        if m:
            packages.add(m.group("pkg"))

    LOG.info("Discovered %d packages in GCS", len(packages))
    if packages:
        LOG.debug("Packages: %s", sorted(packages))

    return packages


def _discover_packages() -> Set[str]:
    """
    Discover packages with error handling.

    Returns:
        Set of package names, empty set on error
    """
    try:
        client = _storage_client()
        return _discover_packages_from_gcs(client)
    except Exception as e:
        LOG.error("Failed to discover packages: %s", e)
        return set()


def _parse_date(date_str: str) -> Optional[dt.date]:
    """
    Parse date string from CSV to date object.

    Args:
        date_str: Date string in format "yyyy-MM-dd"

    Returns:
        Date object or None if parsing fails
    """
    if not date_str or not date_str.strip():
        return None

    date_str = date_str.strip()

    # Try common date formats
    for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"]:
        try:
            return dt.datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue

    # If no format matches, log and return None
    LOG.debug("Unable to parse date: '%s'", date_str)
    return None


def _extract_number(value: str) -> float:
    """
    Extract numeric value from string, handling various formats.

    Args:
        value: String potentially containing a number

    Returns:
        Float value or 0.0 if extraction fails
    """
    if not value:
        return 0.0

    # Remove thousand separators and normalize decimal separator
    value = value.replace(",", "").replace(" ", "").strip()

    # Handle negative numbers in parentheses
    if value.startswith("(") and value.endswith(")"):
        value = "-" + value[1:-1]

    try:
        return float(value)
    except (ValueError, TypeError):
        LOG.debug("Unable to extract number from: '%s'", value)
        return 0.0


def _download_csv(client: storage.Client, blob_name: str) -> List[Dict]:
    """
    Download and parse a CSV file from Google Cloud Storage.

    Args:
        client: Google Cloud Storage client
        blob_name: Full path to the blob in the bucket

    Returns:
        List of dictionaries representing CSV rows
    """
    bucket = client.bucket(BUCKET_ID)
    blob = bucket.blob(blob_name)

    # Try different encodings
    encodings = ["utf-16", "utf-8", "latin-1", "cp1252"]
    content = blob.download_as_bytes()

    for encoding in encodings:
        try:
            text = content.decode(encoding)

            # Use csv.DictReader to parse
            reader = csv.DictReader(io.StringIO(text))
            rows = list(reader)

            LOG.debug(
                "Successfully decoded %s with %s encoding (%d rows)",
                blob_name,
                encoding,
                len(rows),
            )
            return rows

        except (UnicodeDecodeError, csv.Error) as e:
            LOG.debug("Failed to decode %s with %s: %s", blob_name, encoding, e)
            continue

    # If all encodings fail, return empty list
    LOG.error("Failed to decode CSV %s with any encoding", blob_name)
    return []


def _get_months_to_process() -> List[str]:
    """
    Get list of YYYYMM strings for the months to process based on MONTHS_LOOKBACK.

    Returns:
        List of YYYYMM strings to process
    """
    months = []
    now = dt.datetime.utcnow()

    for i in range(MONTHS_LOOKBACK):
        # Calculate the target month (current month minus i)
        month_offset = now.month - i
        year_offset = 0

        while month_offset <= 0:
            month_offset += 12
            year_offset += 1

        target_date = dt.date(now.year - year_offset, month_offset, 1)
        months.append(target_date.strftime("%Y%m"))

    LOG.debug("Will process months: %s", months)
    return months


def _process_package_csv(client: storage.Client, package: str):
    """
    Collect and process metrics from CSV files for a specific package.
    Each date's data becomes a separate gauge metric with appropriate timestamp.

    Args:
        client: Google Cloud Storage client
        package: Android package name to process
    """
    months_to_process = _get_months_to_process()

    # Process each month
    for month_str in months_to_process:
        # Build the exact filename for this package and month
        blob_name = f"stats/installs/installs_{package}_{month_str}_country.csv"

        # Check if blob exists
        bucket = client.bucket(BUCKET_ID)
        blob = bucket.blob(blob_name)

        if not blob.exists():
            LOG.debug("No CSV found for %s in month %s", package, month_str)
            continue

        LOG.info("Processing CSV for %s: %s", package, blob_name)

        # Download and parse CSV
        rows = _download_csv(client, blob_name)

        if not rows:
            LOG.warning("No rows found in %s", blob_name)
            continue

        # Process each row independently - each date gets its own metric entry
        rows_processed = 0
        for row in rows:
            # Parse date to ensure it's valid
            date_str = row.get("Date", "")
            date = _parse_date(date_str)
            if not date:
                continue

            # Extract country code
            country = (row.get("Country") or "").upper()
            if not country:
                continue

            # Convert date to milliseconds timestamp for this specific date
            timestamp_ms = int(
                dt.datetime.combine(date, dt.time.min).timestamp() * 1000
            )

            # Process each metric for this row
            for metric_name, metric_info in METRIC_DEFINITIONS.items():
                csv_column = metric_info["csv_column"]
                value = _extract_number(row.get(csv_column) or "0")

                # Skip zero values
                if value <= 0:
                    continue

                # Initialize metric dict if needed
                if metric_name not in _metrics_data:
                    _metrics_data[metric_name] = {}

                # Store value with date-specific key and timestamp
                # Using date.isoformat() to make date part of the key
                key = (package, country, date.isoformat())
                _metrics_data[metric_name][key] = (value, timestamp_ms)

                if LOG.isEnabledFor(logging.DEBUG):
                    LOG.debug(
                        "Stored metric: %s=%s for %s/%s/%s with timestamp %s",
                        metric_name,
                        value,
                        package,
                        country,
                        date.isoformat(),
                        timestamp_ms,
                    )

            rows_processed += 1

        LOG.info(
            "Processed %d rows from %s",
            rows_processed,
            blob_name,
        )


# ------------ Main collection logic ------------
# Background thread for periodic collection
_collection_thread = None
_stop_collection = threading.Event()


def _run_metrics_collection():
    """
    Run a single metrics collection cycle.
    Clears all existing metrics and repopulates from current CSV files.
    """
    start_time = time.time()
    LOG.info("Starting metrics collection cycle")

    try:
        # Clear all existing metrics - complete refresh
        with _metrics_lock:
            _metrics_data.clear()
            LOG.debug("Cleared all existing metrics for fresh collection")

        # Create storage client
        client = _storage_client()

        # Discover packages
        packages = _discover_packages_from_gcs(client)

        if not packages:
            LOG.warning("No packages discovered, skipping collection")
            _update_health_status(collection_done=True)
            return

        # Collect metrics for each package
        for i, package in enumerate(sorted(packages), 1):
            LOG.info("Processing package %d/%d: %s", i, len(packages), package)
            try:
                _process_package_csv(client, package)
            except Exception as e:
                LOG.error("Failed to process package %s: %s", package, e)
                # Continue with other packages

        # Update health status - successful collection
        _update_health_status(collection_done=True)

        elapsed = time.time() - start_time

        # Count total metrics
        total_metrics = 0
        with _metrics_lock:
            for metric_data in _metrics_data.values():
                total_metrics += len(metric_data)

        LOG.info(
            "Metrics collection completed in %.2f seconds. Total metrics: %d",
            elapsed,
            total_metrics,
        )

    except Exception as e:
        LOG.error("Metrics collection failed: %s", e)
        _update_health_status(error=e, collection_done=True)


def _background_collection():
    """
    Background thread function for periodic metrics collection.
    """
    LOG.info(
        "Starting background collection thread (interval: %d seconds, months lookback: %d)",
        COLLECTION_INTERVAL,
        MONTHS_LOOKBACK,
    )

    while not _stop_collection.is_set():
        try:
            # Run collection
            _run_metrics_collection()

            # Wait for next collection or stop signal
            _stop_collection.wait(COLLECTION_INTERVAL)

        except Exception as e:
            LOG.error("Unexpected error in collection thread: %s", e)
            # Wait a bit before retrying
            _stop_collection.wait(60)

    LOG.info("Background collection thread stopped")


def start_background_collection():
    """Start the background metrics collection thread."""
    global _collection_thread

    if _collection_thread and _collection_thread.is_alive():
        LOG.warning("Collection thread already running")
        return

    _stop_collection.clear()
    _collection_thread = threading.Thread(
        target=_background_collection, daemon=True, name="metrics-collector"
    )
    _collection_thread.start()


def stop_background_collection():
    """Stop the background collection thread."""
    if _collection_thread:
        LOG.info("Stopping collection thread...")
        _stop_collection.set()


# ------------ HTTP Server ------------
class QuietWSGIRequestHandler(WSGIRequestHandler):
    """Custom request handler that only logs in DEBUG mode."""

    def log_message(self, format, *args):
        """Override to only log HTTP requests in DEBUG mode."""
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("HTTP: " + format, *args)


def app(environ, start_response):
    """
    WSGI application for serving metrics and health check.

    Endpoints:
        /metrics - Prometheus metrics endpoint
        /healthz - Health check endpoint (returns 200/503)
        / - Redirect to /metrics
    """
    path = environ.get("PATH_INFO", "/")

    if path == "/metrics":
        # Generate Prometheus format output
        output = _format_prometheus_output()

        start_response(
            "200 OK",
            [
                ("Content-Type", "text/plain; version=0.0.4"),
                ("Content-Length", str(len(output))),
            ],
        )
        return [output.encode("utf-8")]

    elif path == "/healthz":
        # Simple health check - just return status
        if _is_healthy():
            start_response("200 OK", [("Content-Type", "text/plain")])
            return [b"ok"]
        else:
            start_response("503 Service Unavailable", [("Content-Type", "text/plain")])
            return [b"not ok"]

    elif path == "/":
        # Redirect root to /metrics
        start_response("302 Found", [("Location", "/metrics")])
        return [b""]

    else:
        # 404 for unknown paths
        start_response("404 Not Found", [("Content-Type", "text/plain")])
        return [b"Not Found"]


def main():
    """Main entry point for the exporter."""
    LOG.info("Starting Google Play Console Metrics Exporter v3.0.0")
    LOG.info("Configuration:")
    LOG.info("  Port: %d", PORT)
    LOG.info("  Collection interval: %d seconds", COLLECTION_INTERVAL)
    LOG.info("  Months lookback: %d", MONTHS_LOOKBACK)
    LOG.info("  Bucket: %s", BUCKET_ID)
    LOG.info("  Credentials: %s", GOOGLE_CREDS)

    # Start background collection
    start_background_collection()

    # Test mode - run once and exit
    if TEST_MODE:
        LOG.info("TEST MODE: Running single collection and exiting")
        time.sleep(2)  # Give collection thread time to start
        # Wait for first collection to complete (max 5 minutes)
        for _ in range(300):
            if _health_status.get("first_collection_done"):
                break
            time.sleep(1)
        # Print metrics and exit
        print(_format_prometheus_output())
        stop_background_collection()
        return

    # Start HTTP server
    LOG.info("Starting HTTP server on port %d", PORT)
    with make_server("", PORT, app, handler_class=QuietWSGIRequestHandler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            LOG.info("Shutting down...")
            stop_background_collection()


if __name__ == "__main__":
    main()
