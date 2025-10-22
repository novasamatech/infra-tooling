#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test suite for Google Play Console Metrics Exporter

Tests the new functionality including:
- Manual Prometheus format generation with timestamps
- Summing data across all days for daily metrics
- Using last value for absolute metrics (active_device_installs)
- Health check endpoints
- Date parsing utilities
- Number extraction utilities
"""

import os
import sys
import unittest
import datetime as dt
import threading
from unittest.mock import patch, MagicMock, Mock

# Set up test environment variables before importing the exporter
os.environ["GPLAY_EXPORTER_GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/test-creds.json"
os.environ["GPLAY_EXPORTER_BUCKET_ID"] = "test-bucket"
os.environ["GPLAY_EXPORTER_LOG_LEVEL"] = "WARNING"  # Reduce log noise during tests

# Add the parent directory to sys.path to import the exporter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import exporter


class TestPrometheusFormatting(unittest.TestCase):
    """Test Prometheus format generation with timestamps"""

    def setUp(self):
        """Clear metrics before each test"""
        with exporter._metrics_lock:
            exporter._metrics_data = {}

    def test_empty_metrics(self):
        """Test formatting when no metrics are present"""
        output = exporter._format_prometheus_output()

        # Should contain HELP and TYPE for each metric but no values
        for metric_name, metric_info in exporter.METRIC_DEFINITIONS.items():
            self.assertIn(f"# HELP {metric_name}", output)
            self.assertIn(f"# TYPE {metric_name}", output)
            # Should not contain actual metric values
            self.assertNotIn(f"{metric_name}{{", output)

    def test_metrics_with_timestamp(self):
        """Test that metrics are formatted with timestamps"""
        # Add test metrics with timestamp
        test_timestamp = 1737734400000  # 2025-01-24 00:00:00 UTC in milliseconds

        with exporter._metrics_lock:
            exporter._metrics_data = {
                "gplay_device_installs_v2": {
                    ("com.test.app", "US"): (1234.0, test_timestamp),
                    ("com.test.app", "GB"): (567.0, test_timestamp),
                }
            }

        output = exporter._format_prometheus_output()

        # Check that metrics contain timestamps
        self.assertIn(
            'gplay_device_installs_v2{package="com.test.app",country="US"} 1234.0 1737734400000',
            output,
        )
        self.assertIn(
            'gplay_device_installs_v2{package="com.test.app",country="GB"} 567.0 1737734400000',
            output,
        )

    def test_zero_values_filtered(self):
        """Test that zero values are not included in output"""
        test_timestamp = 1737734400000

        with exporter._metrics_lock:
            exporter._metrics_data = {
                "gplay_device_installs_v2": {
                    ("com.test.app", "US"): (100.0, test_timestamp),
                    ("com.test.app", "GB"): (0.0, test_timestamp),  # Zero value
                    ("com.test.app", "FR"): (-5.0, test_timestamp),  # Negative value
                }
            }

        output = exporter._format_prometheus_output()

        # Should include positive value
        self.assertIn(
            'gplay_device_installs_v2{package="com.test.app",country="US"} 100.0',
            output,
        )
        # Should not include zero or negative values
        self.assertNotIn('country="GB"', output)
        self.assertNotIn('country="FR"', output)

    def test_multiple_metrics_types(self):
        """Test formatting multiple metric types"""
        test_timestamp = 1737734400000

        with exporter._metrics_lock:
            exporter._metrics_data = {
                "gplay_device_installs_v2": {
                    ("com.app1", "US"): (100.0, test_timestamp),
                },
                "gplay_device_uninstalls_v2": {
                    ("com.app1", "US"): (10.0, test_timestamp),
                },
                "gplay_active_device_installs_v2": {
                    ("com.app2", "GB"): (5000.0, test_timestamp),
                },
            }

        output = exporter._format_prometheus_output()

        # Check all metrics are present
        self.assertIn(
            'gplay_device_installs_v2{package="com.app1",country="US"} 100.0',
            output,
        )
        self.assertIn(
            'gplay_device_uninstalls_v2{package="com.app1",country="US"} 10.0',
            output,
        )
        self.assertIn(
            'gplay_active_device_installs_v2{package="com.app2",country="GB"} 5000.0',
            output,
        )


class TestHealthCheck(unittest.TestCase):
    """Test health check functionality"""

    def setUp(self):
        """Reset health status before each test"""
        with exporter._health_lock:
            exporter._health_status = {
                "healthy": False,
                "first_collection_done": False,
                "last_collection_time": None,
                "last_error": None,
            }

    def test_initially_unhealthy(self):
        """Test that service starts as unhealthy"""
        self.assertFalse(exporter._is_healthy())

    def test_becomes_healthy_after_success(self):
        """Test that service becomes healthy after successful collection"""
        exporter._update_health_status(True)
        self.assertTrue(exporter._is_healthy())

    def test_stays_healthy_after_failure(self):
        """Test that service stays healthy even after subsequent failures"""
        # First successful collection
        exporter._update_health_status(True)
        self.assertTrue(exporter._is_healthy())

        # Subsequent failure
        exporter._update_health_status(False, "Test error")
        # Should still be healthy (serving cached metrics)
        self.assertTrue(exporter._is_healthy())

    def test_unhealthy_if_never_succeeded(self):
        """Test that service stays unhealthy if no successful collection"""
        exporter._update_health_status(False, "Error 1")
        self.assertFalse(exporter._is_healthy())

        exporter._update_health_status(False, "Error 2")
        self.assertFalse(exporter._is_healthy())


class TestDateParsing(unittest.TestCase):
    """Test date parsing functionality"""

    def test_parse_iso_date(self):
        """Test parsing ISO format dates (YYYY-MM-DD)"""
        result = exporter._parse_date("2025-01-24")
        self.assertEqual(result, dt.date(2025, 1, 24))

    def test_parse_dmy_date(self):
        """Test parsing DD-MMM-YYYY format"""
        result = exporter._parse_date("24-Jan-2025")
        self.assertEqual(result, dt.date(2025, 1, 24))

    def test_parse_us_date(self):
        """Test parsing US format (MM/DD/YYYY)"""
        result = exporter._parse_date("01/24/2025")
        self.assertEqual(result, dt.date(2025, 1, 24))

    def test_parse_with_whitespace(self):
        """Test parsing dates with surrounding whitespace"""
        result = exporter._parse_date("  2025-01-24  ")
        self.assertEqual(result, dt.date(2025, 1, 24))

    def test_parse_invalid_date(self):
        """Test that invalid dates return None"""
        self.assertIsNone(exporter._parse_date("not-a-date"))
        self.assertIsNone(exporter._parse_date(""))
        self.assertIsNone(exporter._parse_date(None))
        self.assertIsNone(exporter._parse_date("2025/13/45"))  # Invalid date


class TestNumberExtraction(unittest.TestCase):
    """Test number extraction functionality"""

    def test_extract_integer(self):
        """Test extracting integer values"""
        self.assertEqual(exporter._extract_number("123"), 123.0)
        self.assertEqual(exporter._extract_number("0"), 0.0)
        self.assertEqual(exporter._extract_number("-456"), -456.0)

    def test_extract_with_commas(self):
        """Test extracting numbers with thousand separators"""
        self.assertEqual(exporter._extract_number("1,234"), 1234.0)
        self.assertEqual(exporter._extract_number("1,234,567"), 1234567.0)

    def test_extract_decimal(self):
        """Test extracting decimal numbers"""
        self.assertEqual(exporter._extract_number("123.45"), 123.45)
        self.assertEqual(exporter._extract_number("1,234.56"), 1234.56)

    def test_extract_with_whitespace(self):
        """Test extracting numbers with surrounding whitespace"""
        self.assertEqual(exporter._extract_number("  123  "), 123.0)
        self.assertEqual(exporter._extract_number(" 1,234.56 "), 1234.56)

    def test_extract_invalid(self):
        """Test that invalid inputs return 0.0"""
        self.assertEqual(exporter._extract_number("not-a-number"), 0.0)
        self.assertEqual(exporter._extract_number(""), 0.0)
        self.assertEqual(exporter._extract_number(None), 0.0)
        self.assertEqual(exporter._extract_number("abc123"), 0.0)


class TestProcessPackageCSV(unittest.TestCase):
    """Test CSV processing with different aggregation strategies"""

    @patch("exporter._download_csv")
    @patch("exporter.storage.Client")
    def test_sum_aggregation_for_daily_metrics(
        self, mock_client_class, mock_download_csv
    ):
        """Test that daily metrics are summed across all dates"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock blob listing
        mock_blob = MagicMock()
        mock_blob.name = "stats/installs/installs_com.test.app_202501_country.csv"
        mock_client.list_blobs.return_value = [mock_blob]

        # Mock CSV data with multiple dates
        mock_download_csv.return_value = [
            {
                "Date": "2025-01-01",
                "Country": "US",
                "Daily Device Installs": "100",
                "Daily Device Uninstalls": "10",
                "Active Device Installs": "1000",
                "Daily User Installs": "90",
                "Daily User Uninstalls": "8",
            },
            {
                "Date": "2025-01-02",
                "Country": "US",
                "Daily Device Installs": "150",
                "Daily Device Uninstalls": "15",
                "Active Device Installs": "1100",  # This should NOT be summed
                "Daily User Installs": "140",
                "Daily User Uninstalls": "12",
            },
            {
                "Date": "2025-01-03",
                "Country": "US",
                "Daily Device Installs": "200",
                "Daily Device Uninstalls": "20",
                "Active Device Installs": "1200",  # Only this value should be used
                "Daily User Installs": "180",
                "Daily User Uninstalls": "18",
            },
        ]

        # Clear metrics
        with exporter._metrics_lock:
            exporter._metrics_data = {}

        # Process the package
        exporter._process_package_csv(mock_client, "com.test.app")

        # Check that daily metrics were summed correctly
        with exporter._metrics_lock:
            # Daily device installs: 100 + 150 + 200 = 450
            self.assertEqual(
                exporter._metrics_data["gplay_device_installs_v2"][
                    ("com.test.app", "US")
                ][0],
                450.0,
            )
            # Daily device uninstalls: 10 + 15 + 20 = 45
            self.assertEqual(
                exporter._metrics_data["gplay_device_uninstalls_v2"][
                    ("com.test.app", "US")
                ][0],
                45.0,
            )
            # Active device installs: Should use ONLY the last value (1200)
            self.assertEqual(
                exporter._metrics_data["gplay_active_device_installs_v2"][
                    ("com.test.app", "US")
                ][0],
                1200.0,  # NOT summed, just the last value
            )
            # User installs: 90 + 140 + 180 = 410
            self.assertEqual(
                exporter._metrics_data["gplay_user_installs_v2"][
                    ("com.test.app", "US")
                ][0],
                410.0,
            )

    @patch("exporter._download_csv")
    @patch("exporter.storage.Client")
    def test_last_value_aggregation_for_active_installs(
        self, mock_client_class, mock_download_csv
    ):
        """Test that active_device_installs uses last value, not sum"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock blob listing
        mock_blob = MagicMock()
        mock_blob.name = "stats/installs/installs_com.test.app_202501_country.csv"
        mock_client.list_blobs.return_value = [mock_blob]

        # Mock CSV data with different values for active installs
        mock_download_csv.return_value = [
            {
                "Date": "2025-01-10",  # Latest date but appears first in CSV
                "Country": "US",
                "Active Device Installs": "5000",  # This should be used
                "Daily Device Installs": "100",
            },
            {
                "Date": "2025-01-05",
                "Country": "US",
                "Active Device Installs": "3000",  # This should be ignored
                "Daily Device Installs": "200",
            },
            {
                "Date": "2025-01-01",
                "Country": "US",
                "Active Device Installs": "1000",  # This should be ignored
                "Daily Device Installs": "300",
            },
        ]

        # Clear metrics
        with exporter._metrics_lock:
            exporter._metrics_data = {}

        # Process the package
        exporter._process_package_csv(mock_client, "com.test.app")

        # Check that active installs uses only the last date's value
        with exporter._metrics_lock:
            # Active device installs should be 5000 (from 2025-01-10)
            self.assertEqual(
                exporter._metrics_data["gplay_active_device_installs_v2"][
                    ("com.test.app", "US")
                ][0],
                5000.0,
            )
            # Daily installs should be summed: 100 + 200 + 300 = 600
            self.assertEqual(
                exporter._metrics_data["gplay_device_installs_v2"][
                    ("com.test.app", "US")
                ][0],
                600.0,
            )

    @patch("exporter._download_csv")
    @patch("exporter.storage.Client")
    def test_timestamp_uses_latest_date(self, mock_client_class, mock_download_csv):
        """Test that timestamp is set to the latest date in the file"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock blob listing
        mock_blob = MagicMock()
        mock_blob.name = "stats/installs/installs_com.test.app_202501_country.csv"
        mock_client.list_blobs.return_value = [mock_blob]

        # Mock CSV data with multiple dates (not in order)
        mock_download_csv.return_value = [
            {
                "Date": "2025-01-05",
                "Country": "US",
                "Daily Device Installs": "100",
            },
            {
                "Date": "2025-01-03",
                "Country": "US",
                "Daily Device Installs": "150",
            },
            {
                "Date": "2025-01-10",  # Latest date
                "Country": "US",
                "Daily Device Installs": "200",
            },
        ]

        # Clear metrics
        with exporter._metrics_lock:
            exporter._metrics_data = {}

        # Process the package
        exporter._process_package_csv(mock_client, "com.test.app")

        # Check that timestamp corresponds to 2025-01-10
        with exporter._metrics_lock:
            # Get the timestamp
            _, timestamp_ms = exporter._metrics_data["gplay_device_installs_v2"][
                ("com.test.app", "US")
            ]

            # Convert back to date
            timestamp_date = dt.datetime.fromtimestamp(timestamp_ms / 1000).date()
            self.assertEqual(timestamp_date, dt.date(2025, 1, 10))


class TestWSGIApp(unittest.TestCase):
    """Test WSGI application endpoints"""

    def setUp(self):
        """Setup mock environment"""
        self.start_response = Mock()

    def test_healthz_endpoint_healthy(self):
        """Test /healthz endpoint when healthy"""
        with exporter._health_lock:
            exporter._health_status["healthy"] = True

        environ = {"PATH_INFO": "/healthz", "REQUEST_METHOD": "GET"}
        response = exporter.app(environ, self.start_response)

        self.start_response.assert_called_once_with(
            "200 OK", [("Content-Type", "text/plain; charset=utf-8")]
        )
        self.assertEqual(response, [b"ok\n"])

    def test_healthz_endpoint_unhealthy(self):
        """Test /healthz endpoint when unhealthy"""
        with exporter._health_lock:
            exporter._health_status["healthy"] = False

        environ = {"PATH_INFO": "/healthz", "REQUEST_METHOD": "GET"}
        response = exporter.app(environ, self.start_response)

        self.start_response.assert_called_once_with(
            "503 Service Unavailable", [("Content-Type", "text/plain; charset=utf-8")]
        )
        self.assertEqual(response, [b"not ok\n"])

    def test_metrics_endpoint(self):
        """Test /metrics endpoint"""
        # Add some test metrics
        with exporter._metrics_lock:
            exporter._metrics_data = {
                "gplay_device_installs_v2": {
                    ("com.test.app", "US"): (1000.0, 1737734400000),
                }
            }

        environ = {"PATH_INFO": "/metrics", "REQUEST_METHOD": "GET"}
        response = exporter.app(environ, self.start_response)

        self.start_response.assert_called_once_with(
            "200 OK", [("Content-Type", "text/plain; version=0.0.4")]
        )

        # Check response contains metrics
        response_text = b"".join(response).decode("utf-8")
        self.assertIn("gplay_device_installs_v2", response_text)
        self.assertIn('package="com.test.app"', response_text)
        self.assertIn('country="US"', response_text)
        self.assertIn("1000.0", response_text)
        self.assertIn("1737734400000", response_text)

    def test_404_endpoint(self):
        """Test unknown endpoint returns 404"""
        environ = {"PATH_INFO": "/unknown", "REQUEST_METHOD": "GET"}
        response = exporter.app(environ, self.start_response)

        self.start_response.assert_called_once_with(
            "404 Not Found", [("Content-Type", "text/plain; charset=utf-8")]
        )
        self.assertEqual(response, [b"not found\n"])


class TestMetricDefinitions(unittest.TestCase):
    """Test metric definitions and aggregation strategies"""

    def test_aggregation_strategies_defined(self):
        """Test that all metrics have aggregation strategy defined"""
        for metric_name, metric_info in exporter.METRIC_DEFINITIONS.items():
            self.assertIn(
                "aggregation",
                metric_info,
                f"Metric {metric_name} missing aggregation strategy",
            )
            self.assertIn(
                metric_info["aggregation"],
                ["sum", "last"],
                f"Invalid aggregation strategy for {metric_name}",
            )

    def test_active_installs_uses_last_aggregation(self):
        """Test that active_device_installs uses 'last' aggregation"""
        self.assertEqual(
            exporter.METRIC_DEFINITIONS["gplay_active_device_installs_v2"][
                "aggregation"
            ],
            "last",
            "Active device installs should use 'last' aggregation, not 'sum'",
        )

    def test_daily_metrics_use_sum_aggregation(self):
        """Test that daily metrics use 'sum' aggregation"""
        daily_metrics = [
            "gplay_device_installs_v2",
            "gplay_device_uninstalls_v2",
            "gplay_user_installs_v2",
            "gplay_user_uninstalls_v2",
        ]
        for metric_name in daily_metrics:
            self.assertEqual(
                exporter.METRIC_DEFINITIONS[metric_name]["aggregation"],
                "sum",
                f"{metric_name} should use 'sum' aggregation",
            )


if __name__ == "__main__":
    # Run tests
    unittest.main()
