#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test suite for Google Play Console Metrics Exporter v3.0.0

Tests the new functionality including:
- Gauge metrics for each individual date
- Manual Prometheus format generation with timestamps
- Date-based metric keys
- Months lookback configuration
- Complete metric storage refresh on each collection
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
os.environ["GPLAY_EXPORTER_MONTHS_LOOKBACK"] = "2"  # Test with 2 months lookback

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
            # All metrics should be gauges in v3
            self.assertIn(f"# TYPE {metric_name} gauge", output)
            # Should not contain actual metric values
            self.assertNotIn(f"{metric_name}{{", output)

    def test_metrics_with_timestamp_and_date_key(self):
        """Test that metrics are formatted with timestamps and date-based keys"""
        # Add test metrics with timestamp
        test_timestamp1 = 1737734400000  # 2025-01-24 00:00:00 UTC in milliseconds
        test_timestamp2 = 1737820800000  # 2025-01-25 00:00:00 UTC in milliseconds

        with exporter._metrics_lock:
            exporter._metrics_data = {
                "gplay_device_installs_v3": {
                    ("com.test.app", "US", "2025-01-24"): (100.0, test_timestamp1),
                    ("com.test.app", "US", "2025-01-25"): (150.0, test_timestamp2),
                    ("com.test.app", "GB", "2025-01-24"): (50.0, test_timestamp1),
                }
            }

        output = exporter._format_prometheus_output()

        # Check that metrics contain timestamps and proper formatting
        self.assertIn(
            'gplay_device_installs_v3{package="com.test.app",country="US"} 100.0 1737734400000',
            output,
        )
        self.assertIn(
            'gplay_device_installs_v3{package="com.test.app",country="US"} 150.0 1737820800000',
            output,
        )
        self.assertIn(
            'gplay_device_installs_v3{package="com.test.app",country="GB"} 50.0 1737734400000',
            output,
        )

    def test_zero_values_filtered(self):
        """Test that zero values are not included in output"""
        test_timestamp = 1737734400000

        with exporter._metrics_lock:
            exporter._metrics_data = {
                "gplay_device_installs_v3": {
                    ("com.test.app", "US", "2025-01-24"): (100.0, test_timestamp),
                    ("com.test.app", "GB", "2025-01-24"): (
                        0.0,
                        test_timestamp,
                    ),  # Zero value
                    ("com.test.app", "FR", "2025-01-24"): (
                        -5.0,
                        test_timestamp,
                    ),  # Negative value
                }
            }

        output = exporter._format_prometheus_output()

        # Should include positive value
        self.assertIn(
            'gplay_device_installs_v3{package="com.test.app",country="US"} 100.0',
            output,
        )
        # Should not include zero or negative values
        self.assertNotIn('country="GB"', output)
        self.assertNotIn('country="FR"', output)

    def test_all_metrics_are_gauges(self):
        """Test that all metrics are declared as gauges in v3"""
        output = exporter._format_prometheus_output()

        for metric_name in exporter.METRIC_DEFINITIONS.keys():
            # All metrics should be gauge type in v3
            self.assertIn(f"# TYPE {metric_name} gauge", output)
            # Should not be counter anymore
            self.assertNotIn(f"# TYPE {metric_name} counter", output)


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

    def test_initial_unhealthy_state(self):
        """Test that exporter starts in unhealthy state"""
        self.assertFalse(exporter._is_healthy())

    def test_healthy_after_successful_collection(self):
        """Test that exporter becomes healthy after successful collection"""
        exporter._update_health_status(collection_done=True)
        self.assertTrue(exporter._is_healthy())

    def test_stays_healthy_after_subsequent_error(self):
        """Test that exporter stays healthy after first success despite errors"""
        # First successful collection
        exporter._update_health_status(collection_done=True)
        self.assertTrue(exporter._is_healthy())

        # Subsequent failed collection
        exporter._update_health_status(
            error=Exception("Test error"), collection_done=True
        )
        # Should still be healthy
        self.assertTrue(exporter._is_healthy())

    def test_unhealthy_if_first_collection_fails(self):
        """Test that exporter is unhealthy if first collection fails"""
        exporter._update_health_status(
            error=Exception("Test error"), collection_done=True
        )
        self.assertFalse(exporter._is_healthy())

    def test_health_endpoint_response(self):
        """Test health check endpoint responses"""
        environ = {"PATH_INFO": "/healthz"}

        # Test unhealthy response
        start_response = Mock()
        response = exporter.app(environ, start_response)
        start_response.assert_called_with(
            "503 Service Unavailable", [("Content-Type", "text/plain")]
        )
        self.assertEqual(response, [b"not ok"])

        # Make healthy
        exporter._update_health_status(collection_done=True)

        # Test healthy response
        start_response = Mock()
        response = exporter.app(environ, start_response)
        start_response.assert_called_with("200 OK", [("Content-Type", "text/plain")])
        self.assertEqual(response, [b"ok"])


class TestDateParsing(unittest.TestCase):
    """Test date parsing functionality"""

    def test_parse_standard_format(self):
        """Test parsing standard YYYY-MM-DD format"""
        result = exporter._parse_date("2025-01-24")
        self.assertEqual(result, dt.date(2025, 1, 24))

    def test_parse_slash_format_us(self):
        """Test parsing MM/DD/YYYY format"""
        result = exporter._parse_date("01/24/2025")
        self.assertEqual(result, dt.date(2025, 1, 24))

    def test_parse_slash_format_eu(self):
        """Test parsing DD/MM/YYYY format"""
        result = exporter._parse_date("24/01/2025")
        self.assertEqual(result, dt.date(2025, 1, 24))

    def test_parse_with_whitespace(self):
        """Test parsing with leading/trailing whitespace"""
        result = exporter._parse_date("  2025-01-24  ")
        self.assertEqual(result, dt.date(2025, 1, 24))

    def test_parse_empty_string(self):
        """Test parsing empty string returns None"""
        self.assertIsNone(exporter._parse_date(""))
        self.assertIsNone(exporter._parse_date("   "))

    def test_parse_invalid_format(self):
        """Test parsing invalid format returns None"""
        self.assertIsNone(exporter._parse_date("not-a-date"))
        self.assertIsNone(exporter._parse_date("2025-13-32"))  # Invalid date


class TestNumberExtraction(unittest.TestCase):
    """Test number extraction functionality"""

    def test_extract_simple_number(self):
        """Test extracting simple number"""
        self.assertEqual(exporter._extract_number("123"), 123.0)
        self.assertEqual(exporter._extract_number("123.45"), 123.45)

    def test_extract_with_commas(self):
        """Test extracting number with thousand separators"""
        self.assertEqual(exporter._extract_number("1,234"), 1234.0)
        self.assertEqual(exporter._extract_number("1,234,567.89"), 1234567.89)

    def test_extract_with_spaces(self):
        """Test extracting number with spaces"""
        self.assertEqual(exporter._extract_number(" 123 "), 123.0)
        self.assertEqual(exporter._extract_number("1 234 567"), 1234567.0)

    def test_extract_negative_number(self):
        """Test extracting negative number"""
        self.assertEqual(exporter._extract_number("-123"), -123.0)

    def test_extract_parentheses_negative(self):
        """Test extracting negative number in parentheses"""
        self.assertEqual(exporter._extract_number("(123)"), -123.0)
        self.assertEqual(exporter._extract_number("(1,234.56)"), -1234.56)

    def test_extract_empty_string(self):
        """Test extracting from empty string returns 0"""
        self.assertEqual(exporter._extract_number(""), 0.0)
        self.assertEqual(exporter._extract_number("   "), 0.0)

    def test_extract_non_numeric(self):
        """Test extracting from non-numeric string returns 0"""
        self.assertEqual(exporter._extract_number("abc"), 0.0)
        self.assertEqual(exporter._extract_number("N/A"), 0.0)


class TestMonthsLookback(unittest.TestCase):
    """Test months lookback functionality"""

    def test_get_months_to_process_single(self):
        """Test getting months with lookback=1"""
        # Save original value
        original_lookback = exporter.MONTHS_LOOKBACK
        try:
            exporter.MONTHS_LOOKBACK = 1
            months = exporter._get_months_to_process()
            # Should return exactly 1 month
            self.assertEqual(len(months), 1)
            # Should be in YYYYMM format
            self.assertTrue(all(len(m) == 6 and m.isdigit() for m in months))
        finally:
            # Restore original value
            exporter.MONTHS_LOOKBACK = original_lookback

    def test_get_months_to_process_multiple(self):
        """Test getting months with lookback=3"""
        # Save original value
        original_lookback = exporter.MONTHS_LOOKBACK
        try:
            exporter.MONTHS_LOOKBACK = 3
            months = exporter._get_months_to_process()
            # Should return exactly 3 months
            self.assertEqual(len(months), 3)
            # Should be in YYYYMM format
            self.assertTrue(all(len(m) == 6 and m.isdigit() for m in months))
            # Months should be in descending order (current month first)
            for i in range(len(months) - 1):
                # Convert to int for comparison (e.g., 202501 > 202412)
                if months[i][:4] == months[i + 1][:4]:  # Same year
                    self.assertGreater(int(months[i]), int(months[i + 1]))
        finally:
            # Restore original value
            exporter.MONTHS_LOOKBACK = original_lookback

    def test_get_months_to_process_cross_year(self):
        """Test getting months with various lookback values"""
        # Save original value
        original_lookback = exporter.MONTHS_LOOKBACK
        try:
            # Test different lookback values
            for lookback_value in [2, 6, 12]:
                exporter.MONTHS_LOOKBACK = lookback_value
                months = exporter._get_months_to_process()
                # Should return exactly the requested number of months
                self.assertEqual(len(months), lookback_value)
                # All should be valid YYYYMM format
                self.assertTrue(all(len(m) == 6 and m.isdigit() for m in months))
        finally:
            # Restore original value
            exporter.MONTHS_LOOKBACK = original_lookback


class TestMetricsCollection(unittest.TestCase):
    """Test metrics collection logic"""

    def test_metrics_cleared_on_collection(self):
        """Test that metrics are completely cleared before new collection"""
        # Add some existing metrics
        with exporter._metrics_lock:
            exporter._metrics_data = {
                "gplay_device_installs_v3": {
                    ("com.old.app", "US", "2025-01-20"): (100.0, 1737331200000),
                }
            }

        # Verify initial state has metrics
        with exporter._metrics_lock:
            self.assertEqual(len(exporter._metrics_data), 1)

        # Mock the necessary functions to test clearing behavior
        with patch("exporter._storage_client") as mock_storage_client:
            with patch("exporter._discover_packages_from_gcs") as mock_discover:
                # Return no packages - this should still clear metrics
                mock_discover.return_value = set()

                # Run collection
                exporter._run_metrics_collection()

                # Verify metrics were cleared at start of collection
                with exporter._metrics_lock:
                    self.assertEqual(exporter._metrics_data, {})

    def test_date_specific_metrics_storage(self):
        """Test that metrics are stored with date-specific keys"""
        test_csv_data = [
            {"Date": "2025-01-24", "Country": "US", "Daily Device Installs": "100"},
            {"Date": "2025-01-25", "Country": "US", "Daily Device Installs": "150"},
            {"Date": "2025-01-24", "Country": "GB", "Daily Device Installs": "50"},
        ]

        with patch("exporter._download_csv") as mock_download:
            mock_download.return_value = test_csv_data

            with patch("exporter._get_months_to_process") as mock_months:
                mock_months.return_value = ["202501"]

                # Create mock client and bucket
                mock_client = Mock()
                mock_bucket = Mock()
                mock_blob = Mock()
                mock_blob.exists.return_value = True
                mock_bucket.blob.return_value = mock_blob
                mock_client.bucket.return_value = mock_bucket

                # Process package
                exporter._process_package_csv(mock_client, "com.test.app")

                # Check stored metrics
                with exporter._metrics_lock:
                    installs = exporter._metrics_data.get(
                        "gplay_device_installs_v3", {}
                    )

                    # Should have 3 separate entries with date-specific keys
                    self.assertEqual(len(installs), 3)

                    # Check specific entries
                    key1 = ("com.test.app", "US", "2025-01-24")
                    key2 = ("com.test.app", "US", "2025-01-25")
                    key3 = ("com.test.app", "GB", "2025-01-24")

                    self.assertIn(key1, installs)
                    self.assertIn(key2, installs)
                    self.assertIn(key3, installs)

                    # Check values
                    self.assertEqual(installs[key1][0], 100.0)
                    self.assertEqual(installs[key2][0], 150.0)
                    self.assertEqual(installs[key3][0], 50.0)


class TestWSGIApp(unittest.TestCase):
    """Test WSGI application endpoints"""

    def test_metrics_endpoint(self):
        """Test /metrics endpoint returns Prometheus format"""
        environ = {"PATH_INFO": "/metrics"}
        start_response = Mock()

        response = exporter.app(environ, start_response)

        # Should return 200 OK with Prometheus content type
        start_response.assert_called_once()
        status, headers = start_response.call_args[0]
        self.assertEqual(status, "200 OK")

        # Check content type header
        headers_dict = dict(headers)
        self.assertEqual(headers_dict["Content-Type"], "text/plain; version=0.0.4")

        # Response should be bytes
        self.assertIsInstance(response[0], bytes)

    def test_root_redirect(self):
        """Test / redirects to /metrics"""
        environ = {"PATH_INFO": "/"}
        start_response = Mock()

        response = exporter.app(environ, start_response)

        # Should redirect to /metrics
        start_response.assert_called_with("302 Found", [("Location", "/metrics")])
        self.assertEqual(response, [b""])

    def test_unknown_path(self):
        """Test unknown paths return 404"""
        environ = {"PATH_INFO": "/unknown"}
        start_response = Mock()

        response = exporter.app(environ, start_response)

        # Should return 404
        start_response.assert_called_with(
            "404 Not Found", [("Content-Type", "text/plain")]
        )
        self.assertEqual(response, [b"Not Found"])


if __name__ == "__main__":
    unittest.main()
