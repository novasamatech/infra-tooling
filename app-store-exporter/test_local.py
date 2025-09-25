#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""
Local testing script for Apple App Store Connect Exporter

This script provides comprehensive testing for the exporter functionality
including API mocking, health checks, and metrics collection.

Usage:
    python test_exporter.py [--debug] [--real-api]

Options:
    --debug     Enable debug logging
    --real-api  Run tests against real API (requires valid credentials)
"""

import os
import sys
import json
import time
import threading
import unittest
import logging
from unittest.mock import Mock, patch, MagicMock, mock_open
from datetime import datetime, timedelta
import tempfile
import requests

# Set up test environment variables before importing exporter
os.environ.update({
    "APPSTORE_EXPORTER_ISSUER_ID": "test-issuer-id",
    "APPSTORE_EXPORTER_KEY_ID": "test-key-id",
    "APPSTORE_EXPORTER_PRIVATE_KEY": "/tmp/test-key.p8",
    "APPSTORE_EXPORTER_APP_ID": "123456789",
    "APPSTORE_EXPORTER_BUNDLE_ID": "com.test.app",
    "APPSTORE_EXPORTER_PORT": "8001",
    "APPSTORE_EXPORTER_COLLECTION_INTERVAL_SECONDS": "60",
    "APPSTORE_EXPORTER_DAYS_TO_FETCH": "7",
    "APPSTORE_EXPORTER_LOG_LEVEL": "INFO"
})

# Create a dummy private key file for testing
TEST_PRIVATE_KEY = """-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgevZzL1gdAFr88hb2
OF/2NxApJCzGCEDdfSp6VQO30hyhRANCAAQRWz+jn65BtOMvdyHKcvjBeBSDZH2r
1RTwjmYSi9R/zpBnuQ4EiMnCqfMPWiZqB4QdbAd0E7oH50VpuZ1P087G
-----END PRIVATE KEY-----"""

with open("/tmp/test-key.p8", "w") as f:
    f.write(TEST_PRIVATE_KEY)

# Now import the exporter module
sys.path.insert(0, os.path.dirname(__file__))
import exporter


class TestExporterConfig(unittest.TestCase):
    """Test configuration and environment variable handling"""

    def test_parse_app_config_single(self):
        """Test single app configuration parsing"""
        # Save original environment
        original_env = {}
        for key in ["APPSTORE_EXPORTER_APP_ID", "APPSTORE_EXPORTER_APP_IDS",
                    "APPSTORE_EXPORTER_BUNDLE_ID", "APPSTORE_EXPORTER_BUNDLE_IDS"]:
            original_env[key] = os.environ.get(key)

        try:
            # Ensure we're testing single app config
            os.environ["APPSTORE_EXPORTER_APP_ID"] = "123456789"
            os.environ["APPSTORE_EXPORTER_BUNDLE_ID"] = "com.test.app"
            os.environ.pop("APPSTORE_EXPORTER_APP_IDS", None)
            os.environ.pop("APPSTORE_EXPORTER_BUNDLE_IDS", None)

            apps = exporter._parse_app_config()
            self.assertEqual(len(apps), 1)
            self.assertEqual(apps[0]["id"], "123456789")
            self.assertEqual(apps[0]["bundle_id"], "com.test.app")
        finally:
            # Restore original environment
            for key, value in original_env.items():
                if value is not None:
                    os.environ[key] = value
                elif key in os.environ:
                    del os.environ[key]

    def test_parse_app_config_multiple(self):
        """Test multiple apps configuration parsing"""
        # Save original environment
        original_env = {}
        for key in ["APPSTORE_EXPORTER_APP_ID", "APPSTORE_EXPORTER_APP_IDS",
                    "APPSTORE_EXPORTER_BUNDLE_ID", "APPSTORE_EXPORTER_BUNDLE_IDS"]:
            original_env[key] = os.environ.get(key)

        try:
            os.environ["APPSTORE_EXPORTER_APP_IDS"] = "111,222,333"
            os.environ["APPSTORE_EXPORTER_BUNDLE_IDS"] = "com.app1,com.app2,com.app3"
            os.environ.pop("APPSTORE_EXPORTER_APP_ID", None)
            os.environ.pop("APPSTORE_EXPORTER_BUNDLE_ID", None)

            # Reload the config
            apps = exporter._parse_app_config()

            self.assertEqual(len(apps), 3)
            self.assertEqual(apps[0]["id"], "111")
            self.assertEqual(apps[0]["bundle_id"], "com.app1")
            self.assertEqual(apps[2]["id"], "333")
            self.assertEqual(apps[2]["bundle_id"], "com.app3")
        finally:
            # Restore original environment
            for key, value in original_env.items():
                if value is not None:
                    os.environ[key] = value
                elif key in os.environ:
                    del os.environ[key]

    def test_health_state_initialization(self):
        """Test health state is properly initialized"""
        self.assertFalse(exporter._health_state["healthy"])
        self.assertIsNone(exporter._health_state["last_successful_collection"])
        self.assertEqual(exporter._health_state["collections_count"], 0)


class TestJWTGeneration(unittest.TestCase):
    """Test JWT token generation"""

    def test_make_token(self):
        """Test JWT token is generated correctly"""
        token = exporter._make_token()
        self.assertIsNotNone(token)
        self.assertIsInstance(token, str)
        # JWT should have 3 parts separated by dots
        parts = token.split('.')
        self.assertEqual(len(parts), 3)

    @patch('builtins.open', side_effect=FileNotFoundError)
    def test_make_token_file_not_found(self, mock_file):
        """Test token generation fails gracefully when key file not found"""
        with self.assertRaises(FileNotFoundError):
            exporter._make_token()


class TestAPIFunctions(unittest.TestCase):
    """Test App Store Connect API interaction functions"""

    @patch('exporter.requests.get')
    @patch('exporter._make_token')
    def test_asc_api_call_get_success(self, mock_token, mock_get):
        """Test successful GET API call"""
        mock_token.return_value = "test-token"
        mock_response = Mock()
        mock_response.json.return_value = {"data": [{"id": "123"}]}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        result = exporter._asc_api_call("GET", "/v1/apps", params={"limit": 10})

        self.assertEqual(result["data"][0]["id"], "123")
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        self.assertIn("Authorization", kwargs["headers"])
        self.assertEqual(kwargs["params"]["limit"], 10)

    @patch('exporter.requests.get')
    @patch('exporter._make_token')
    def test_asc_api_call_retry_on_429(self, mock_token, mock_get):
        """Test API call retries on rate limiting (429)"""
        mock_token.return_value = "test-token"

        # First call returns 429, second succeeds
        mock_response_429 = Mock()
        mock_response_429.raise_for_status.side_effect = requests.HTTPError(response=Mock(status_code=429))

        mock_response_success = Mock()
        mock_response_success.json.return_value = {"data": []}
        mock_response_success.raise_for_status = Mock()

        mock_get.side_effect = [mock_response_429, mock_response_success]

        with patch('time.sleep'):  # Don't actually sleep in tests
            result = exporter._asc_api_call("GET", "/v1/apps")

        self.assertEqual(result, {"data": []})
        self.assertEqual(mock_get.call_count, 2)

    @patch('exporter._asc_api_call')
    def test_find_existing_report_request(self, mock_api):
        """Test finding existing report request"""
        mock_api.return_value = {
            "data": [
                {"id": "request-123", "attributes": {"accessType": "ONGOING"}}
            ]
        }

        request_id = exporter._find_existing_report_request("app-123")

        self.assertEqual(request_id, "request-123")
        mock_api.assert_called()

    @patch('exporter._asc_api_call')
    def test_find_report_id_with_cache(self, mock_api):
        """Test report ID finding with caching"""
        mock_api.return_value = {
            "data": [
                {"id": "report-1", "attributes": {"name": "App Downloads Standard", "category": "METRICS"}},
                {"id": "report-2", "attributes": {"name": "App Sessions Standard", "category": "METRICS"}}
            ]
        }

        # Clear cache
        exporter._REPORTS_CACHE.clear()

        # First call should hit API
        report_id = exporter._find_report_id("request-123", "App Downloads Standard")
        self.assertEqual(report_id, "report-1")
        self.assertEqual(mock_api.call_count, 1)

        # Second call should use cache
        report_id = exporter._find_report_id("request-123", "App Sessions Standard")
        self.assertEqual(report_id, "report-2")
        self.assertEqual(mock_api.call_count, 1)  # Still 1, used cache


class TestHealthCheck(unittest.TestCase):
    """Test health check functionality"""

    def setUp(self):
        """Reset health state before each test"""
        exporter._health_state.update({
            "healthy": False,
            "last_successful_collection": None,
            "last_error": None,
            "collections_count": 0
        })

    def test_health_check_not_ready(self):
        """Test health check when no collections have run"""
        environ = {"PATH_INFO": "/healthz", "REQUEST_METHOD": "GET"}
        start_response = Mock()

        response = exporter.app(environ, start_response)

        start_response.assert_called_once()
        status, _ = start_response.call_args[0]
        self.assertEqual(status, "503 Service Unavailable")
        self.assertIn(b"not ready", response[0])

    def test_health_check_healthy(self):
        """Test health check when service is healthy"""
        exporter._health_state["healthy"] = True
        exporter._health_state["collections_count"] = 1
        exporter._health_state["last_successful_collection"] = datetime.now()

        environ = {"PATH_INFO": "/healthz", "REQUEST_METHOD": "GET"}
        start_response = Mock()

        response = exporter.app(environ, start_response)

        start_response.assert_called_once()
        status, _ = start_response.call_args[0]
        self.assertEqual(status, "200 OK")
        self.assertIn(b"ok", response[0])

    def test_health_check_unhealthy_with_error(self):
        """Test health check when service is unhealthy with error"""
        exporter._health_state["healthy"] = False
        exporter._health_state["collections_count"] = 1
        exporter._health_state["last_error"] = "API connection failed"

        environ = {"PATH_INFO": "/healthz", "REQUEST_METHOD": "GET"}
        start_response = Mock()

        response = exporter.app(environ, start_response)

        start_response.assert_called_once()
        status, _ = start_response.call_args[0]
        self.assertEqual(status, "503 Service Unavailable")
        self.assertIn(b"unhealthy", response[0])


class TestMetricsEndpoint(unittest.TestCase):
    """Test metrics endpoint functionality"""

    def test_metrics_endpoint(self):
        """Test /metrics endpoint returns Prometheus metrics"""
        environ = {"PATH_INFO": "/metrics", "REQUEST_METHOD": "GET"}
        start_response = Mock()

        response = exporter.app(environ, start_response)

        start_response.assert_called_once()
        status, headers = start_response.call_args[0]
        self.assertEqual(status, "200 OK")

        # Check Content-Type header
        content_type = next((h[1] for h in headers if h[0] == "Content-Type"), None)
        self.assertIsNotNone(content_type)

        # Response should contain bytes
        self.assertIsInstance(response[0], bytes)

    def test_404_for_unknown_path(self):
        """Test 404 response for unknown paths"""
        environ = {"PATH_INFO": "/unknown", "REQUEST_METHOD": "GET"}
        start_response = Mock()

        response = exporter.app(environ, start_response)

        start_response.assert_called_once()
        status, _ = start_response.call_args[0]
        self.assertEqual(status, "404 Not Found")
        self.assertIn(b"not found", response[0])


class TestDataProcessing(unittest.TestCase):
    """Test data processing and parsing functions"""

    def test_extract_number(self):
        """Test number extraction from various formats"""
        self.assertEqual(exporter._extract_number("1,234"), 1234.0)
        self.assertEqual(exporter._extract_number("567.89"), 567.89)
        self.assertEqual(exporter._extract_number(" 100 "), 100.0)
        self.assertEqual(exporter._extract_number(""), 0.0)
        self.assertEqual(exporter._extract_number(None), 0.0)
        self.assertEqual(exporter._extract_number("not a number"), 0.0)

    def test_parse_iso_date(self):
        """Test ISO date parsing"""
        from datetime import date

        # Test ISO format
        result = exporter._parse_iso_date("2024-01-15")
        self.assertEqual(result, date(2024, 1, 15))

        # Test with timezone
        result = exporter._parse_iso_date("2024-01-15T00:00:00Z")
        self.assertEqual(result, date(2024, 1, 15))

        # Test US format
        result = exporter._parse_iso_date("01/15/2024")
        self.assertEqual(result, date(2024, 1, 15))

        # Test invalid format
        with self.assertRaises(ValueError):
            exporter._parse_iso_date("invalid-date")


class TestCollectionLogic(unittest.TestCase):
    """Test metrics collection logic"""

    @patch('exporter._process_app_metrics')
    @patch('exporter._init_metrics')
    def test_run_metrics_collection_success(self, mock_init_metrics, mock_process):
        """Test successful metrics collection updates health state"""
        # Reset health state
        exporter._health_state["collections_count"] = 0
        exporter._health_state["healthy"] = False

        # Ensure metrics are initialized
        mock_init_metrics.return_value = None
        mock_process.return_value = None  # Success

        exporter._run_metrics_collection()

        self.assertTrue(exporter._health_state["healthy"])
        self.assertEqual(exporter._health_state["collections_count"], 1)
        self.assertIsNotNone(exporter._health_state["last_successful_collection"])
        self.assertIsNone(exporter._health_state["last_error"])

    @patch('exporter._process_app_metrics')
    @patch('exporter._init_metrics')
    def test_run_metrics_collection_partial_failure(self, mock_init_metrics, mock_process):
        """Test partial collection failure still marks as healthy"""
        # Save original APPS
        original_apps = exporter.APPS

        try:
            # Setup multiple apps
            exporter.APPS = [
                {"id": "1", "name": "app1", "bundle_id": "com.app1"},
                {"id": "2", "name": "app2", "bundle_id": "com.app2"}
            ]

            # Ensure metrics are initialized
            mock_init_metrics.return_value = None
            # First app succeeds, second fails
            mock_process.side_effect = [None, Exception("API Error")]

            exporter._run_metrics_collection()

            # Should still be healthy with partial success
            self.assertTrue(exporter._health_state["healthy"])
            self.assertIn("Failed to collect metrics for 1/2 apps", exporter._health_state["last_error"])
        finally:
            # Restore original APPS
            exporter.APPS = original_apps


def run_integration_test():
    """Run a simple integration test with mocked API responses"""
    print("\n" + "="*60)
    print("Running Integration Test")
    print("="*60)

    with patch('exporter._asc_api_call') as mock_api:
        # Mock API responses
        mock_api.side_effect = [
            # Response for finding report request
            {"data": [{"id": "req-123"}]},
            # Response for finding report
            {"data": [
                {"id": "report-123", "attributes": {"name": "App Downloads Standard"}}
            ]},
            # Response for finding instances
            {"data": [
                {"id": "inst-123", "attributes": {"processingDate": "2024-01-15", "granularity": "DAILY"}}
            ]},
            # Response for segments
            {"data": []}  # Empty segments for simplicity
        ]

        # Run collection
        print("Testing metrics collection...")
        exporter._run_metrics_collection()

        if exporter._health_state["healthy"]:
            print("✓ Health state is healthy")
        else:
            print("✗ Health state is not healthy")

        print(f"Collections count: {exporter._health_state['collections_count']}")

    print("\n" + "="*60)
    print("Integration Test Complete")
    print("="*60)


def main():
    """Main test runner"""
    import argparse

    parser = argparse.ArgumentParser(description='Test Apple App Store Connect Exporter')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--real-api', action='store_true', help='Run against real API (requires credentials)')
    parser.add_argument('--integration', action='store_true', help='Run integration tests')
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        os.environ["APPSTORE_EXPORTER_LOG_LEVEL"] = "DEBUG"
    else:
        logging.basicConfig(level=logging.INFO)

    if args.real_api:
        print("Real API testing requires valid credentials in environment variables:")
        print("  APPSTORE_EXPORTER_ISSUER_ID")
        print("  APPSTORE_EXPORTER_KEY_ID")
        print("  APPSTORE_EXPORTER_PRIVATE_KEY")
        print("  APPSTORE_EXPORTER_APP_ID")
        print("\nWARNING: This will make real API calls!\n")

        response = input("Continue with real API testing? (y/n): ")
        if response.lower() != 'y':
            print("Aborted.")
            return

        # Run real collection
        exporter._run_metrics_collection()
        print(f"Health state: {exporter._health_state}")
        return

    # Run unit tests
    print("Running Unit Tests...")
    print("="*60)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add all test cases
    suite.addTests(loader.loadTestsFromTestCase(TestExporterConfig))
    suite.addTests(loader.loadTestsFromTestCase(TestJWTGeneration))
    suite.addTests(loader.loadTestsFromTestCase(TestAPIFunctions))
    suite.addTests(loader.loadTestsFromTestCase(TestHealthCheck))
    suite.addTests(loader.loadTestsFromTestCase(TestMetricsEndpoint))
    suite.addTests(loader.loadTestsFromTestCase(TestDataProcessing))
    suite.addTests(loader.loadTestsFromTestCase(TestCollectionLogic))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Run integration test if requested
    if args.integration:
        run_integration_test()

    # Print summary
    print("\n" + "="*60)
    print("Test Summary")
    print("="*60)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")

    # Cleanup
    try:
        os.remove("/tmp/test-key.p8")
    except:
        pass

    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
