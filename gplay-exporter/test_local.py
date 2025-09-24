#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""
Local test script for gplay-exporter validation.
Tests core functionality without requiring actual GCS access.
"""

import os
import sys
import tempfile
import logging

# Setup logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s: %(message)s")
LOG = logging.getLogger(__name__)

def test_environment_variables():
    """Test environment variable handling."""
    print("\n=== Testing Environment Variables ===")

    # Test with new prefixed variables
    os.environ["GPLAY_EXPORTER_GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/fake-creds.json"
    os.environ["GPLAY_EXPORTER_BUCKET_ID"] = "test-bucket-123"
    os.environ["GPLAY_EXPORTER_PORT"] = "9999"
    os.environ["GPLAY_EXPORTER_LOG_LEVEL"] = "DEBUG"
    os.environ["GPLAY_EXPORTER_TEST_MODE"] = "1"
    os.environ["GPLAY_EXPORTER_COLLECTION_INTERVAL_SECONDS"] = "3600"

    # Import after setting env vars
    try:
        import exporter
        print("✓ Environment variables loaded successfully")
        print(f"  - GOOGLE_CREDS: {exporter.GOOGLE_CREDS}")
        print(f"  - BUCKET_ID: {exporter.BUCKET_ID}")
        print(f"  - PORT: {exporter.PORT}")
        print(f"  - COLLECTION_INTERVAL: {exporter.COLLECTION_INTERVAL}")
        print(f"  - TEST_MODE: {exporter.TEST_MODE}")

        # Verify values
        assert exporter.BUCKET_ID == "test-bucket-123", "BUCKET_ID not set correctly"
        assert exporter.PORT == 9999, "PORT not set correctly"
        assert exporter.COLLECTION_INTERVAL == 3600, "COLLECTION_INTERVAL not set correctly"
        assert exporter.TEST_MODE == "1", "TEST_MODE not set correctly"

        return True
    except SystemExit as e:
        print(f"✗ Failed to load environment variables: {e}")
        return False
    except AssertionError as e:
        print(f"✗ Environment variable validation failed: {e}")
        return False

def test_counter_creation():
    """Test Prometheus counter creation."""
    print("\n=== Testing Counter Creation ===")

    from prometheus_client import CollectorRegistry, Counter

    registry = CollectorRegistry()

    try:
        # Create test counter similar to the exporter
        counter = Counter(
            "test_metric_total",
            "Test metric description",
            ["label1", "label2"],
            registry=registry
        )

        # Test setting values using internal API (as exporter does)
        counter.labels(label1="value1", label2="value2")._value.set(100.0)
        counter.labels(label1="value3", label2="value4")._value.set(200.0)

        print("✓ Counters created and values set successfully")
        return True
    except Exception as e:
        print(f"✗ Failed to create counters: {e}")
        return False

def test_health_check():
    """Test health check logic."""
    print("\n=== Testing Health Check ===")

    import exporter

    # Reset health status for testing
    exporter._health_status = {
        "healthy": False,
        "first_collection_done": False,
        "last_collection_time": None,
        "last_error": None
    }

    # Test initial state (should be unhealthy)
    assert exporter._is_healthy() == False, "Should be unhealthy initially"
    print("✓ Initial unhealthy state correct")

    # Simulate successful collection
    exporter._update_health_status(True)
    assert exporter._is_healthy() == True, "Should be healthy after success"
    assert exporter._health_status["first_collection_done"] == True
    print("✓ Healthy state after successful collection")

    # Simulate failed collection after success (should remain healthy)
    exporter._update_health_status(False, "Test error")
    assert exporter._is_healthy() == True, "Should remain healthy after first success"
    assert exporter._health_status["last_error"] == "Test error"
    print("✓ Remains healthy after failure when first collection was successful")

    # Reset and test failure before any success
    exporter._health_status = {
        "healthy": False,
        "first_collection_done": False,
        "last_collection_time": None,
        "last_error": None
    }
    exporter._update_health_status(False, "Initial failure")
    assert exporter._is_healthy() == False, "Should be unhealthy when no success yet"
    print("✓ Unhealthy when no successful collection yet")

    return True

def test_date_parsing():
    """Test date parsing functionality."""
    print("\n=== Testing Date Parsing ===")

    import exporter
    from datetime import date

    test_dates = [
        ("2025-01-15", date(2025, 1, 15)),
        ("15-Jan-2025", date(2025, 1, 15)),
        ("01/15/2025", date(2025, 1, 15)),
        ("invalid", None),
        ("", None),
    ]

    for date_str, expected in test_dates:
        result = exporter._parse_date(date_str)
        if result == expected:
            print(f"✓ Parsed '{date_str}' -> {result}")
        else:
            print(f"✗ Failed to parse '{date_str}': got {result}, expected {expected}")
            return False

    return True

def test_number_extraction():
    """Test number extraction from strings."""
    print("\n=== Testing Number Extraction ===")

    import exporter

    test_cases = [
        ("123", 123.0),
        ("1,234", 1234.0),
        ("1,234,567", 1234567.0),
        ("", 0.0),
        (None, 0.0),
        ("invalid", 0.0),
        ("  456  ", 456.0),
        ("12.34", 12.34),
    ]

    for input_val, expected in test_cases:
        result = exporter._extract_number(input_val)
        if result == expected:
            print(f"✓ Extracted '{input_val}' -> {result}")
        else:
            print(f"✗ Failed to extract '{input_val}': got {result}, expected {expected}")
            return False

    return True

def test_wsgi_endpoints():
    """Test WSGI application endpoints."""
    print("\n=== Testing WSGI Endpoints ===")

    import exporter

    # Mock environment for testing
    def create_environ(path, method="GET"):
        return {
            "PATH_INFO": path,
            "REQUEST_METHOD": method,
        }

    # Mock start_response
    responses = []
    def start_response(status, headers):
        responses.append({"status": status, "headers": headers})

    # Test /metrics endpoint
    environ = create_environ("/metrics")
    result = exporter.app(environ, start_response)
    assert responses[-1]["status"] == "200 OK", "Metrics endpoint should return 200"
    print("✓ /metrics endpoint returns 200 OK")

    # Test /healthz endpoint (unhealthy state)
    exporter._health_status["healthy"] = False
    environ = create_environ("/healthz")
    result = exporter.app(environ, start_response)
    response_body = b"".join(result).decode('utf-8')
    assert responses[-1]["status"] == "503 Service Unavailable", "Health check should return 503 when unhealthy"
    assert "not ok" in response_body or "unhealthy" in response_body
    print("✓ /healthz endpoint returns 503 when unhealthy")

    # Test /healthz endpoint (healthy state)
    exporter._health_status["healthy"] = True
    environ = create_environ("/healthz")
    result = exporter.app(environ, start_response)
    response_body = b"".join(result).decode('utf-8')
    assert responses[-1]["status"] == "200 OK", "Health check should return 200 when healthy"
    assert "ok" in response_body
    print("✓ /healthz endpoint returns 200 when healthy")

    # Test 404 for unknown endpoint
    environ = create_environ("/unknown")
    result = exporter.app(environ, start_response)
    assert responses[-1]["status"] == "404 Not Found", "Unknown endpoint should return 404"
    print("✓ Unknown endpoints return 404")

    return True

def test_metric_export():
    """Test metric export functionality."""
    print("\n=== Testing Metric Export ===")

    import exporter
    from datetime import date
    from prometheus_client import generate_latest

    # Reset registry and counters
    exporter.REGISTRY = exporter.CollectorRegistry()
    exporter.counters = exporter._create_prometheus_counters()

    # Export test metrics
    test_metrics = {
        "daily_device_installs": 100.0,
        "daily_device_uninstalls": 20.0,
        "active_device_installs": 500.0,
        "daily_user_installs": 80.0,
        "daily_user_uninstalls": 15.0,
    }

    exporter._export_metrics("com.test.app", "US", test_metrics, date(2025, 1, 15))

    # Generate and check output
    output = generate_latest(exporter.REGISTRY).decode('utf-8')

    # Verify all metrics are present
    for metric_name in test_metrics:
        full_metric_name = f"gplay_{metric_name}_total"
        if full_metric_name in output:
            print(f"✓ Metric {full_metric_name} exported")
        else:
            print(f"✗ Metric {full_metric_name} not found in output")
            return False

    # Check for proper labels
    if 'package="com.test.app"' in output and 'country="US"' in output:
        print("✓ Labels properly set in metrics")
    else:
        print("✗ Labels not found in metrics output")
        return False

    # Verify that metrics are counters (should have _total suffix in output)
    if "gplay_daily_device_installs_total" in output:
        print("✓ Metrics correctly named with _total suffix for counters")
    else:
        print("✗ Counter metrics not properly named")
        return False

    return True



def main():
    """Run all tests."""
    print("=" * 50)
    print("Google Play Exporter Local Test Suite")
    print("=" * 50)

    tests = [
        ("Environment Variables", test_environment_variables),
        ("Counter Creation", test_counter_creation),
        ("Health Check", test_health_check),
        ("Date Parsing", test_date_parsing),
        ("Number Extraction", test_number_extraction),
        ("WSGI Endpoints", test_wsgi_endpoints),
        ("Metric Export", test_metric_export),
    ]

    results = []
    for name, test_func in tests:
        try:
            success = test_func()
            results.append((name, success))
        except Exception as e:
            print(f"\n✗ Test '{name}' failed with exception: {e}")
            import traceback
            traceback.print_exc()
            results.append((name, False))

    print("\n" + "=" * 50)
    print("Test Results Summary")
    print("=" * 50)

    passed = 0
    failed = 0
    for name, success in results:
        status = "PASS" if success else "FAIL"
        symbol = "✓" if success else "✗"
        print(f"{symbol} {name}: {status}")
        if success:
            passed += 1
        else:
            failed += 1

    print("=" * 50)
    print(f"Total: {passed} passed, {failed} failed")

    return 0 if failed == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
