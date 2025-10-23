#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Integration test for Google Play Console Metrics Exporter v3.0.0

This test simulates real-world scenarios with mock GCS data to ensure
the exporter correctly handles gauge metrics with date-specific keys
and generates proper Prometheus format with timestamps.
"""

import os
import sys
import unittest
import datetime as dt
import io
from unittest.mock import patch, MagicMock, Mock

# Set up test environment variables before importing the exporter
os.environ["GPLAY_EXPORTER_GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/test-creds.json"
os.environ["GPLAY_EXPORTER_BUCKET_ID"] = "test-bucket"
os.environ["GPLAY_EXPORTER_LOG_LEVEL"] = "WARNING"
os.environ["GPLAY_EXPORTER_MONTHS_LOOKBACK"] = "2"  # Test with 2 months

# Add the parent directory to sys.path to import the exporter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import exporter


class TestIntegrationScenarios(unittest.TestCase):
    """Integration tests for realistic scenarios"""

    def setUp(self):
        """Clear metrics before each test"""
        with exporter._metrics_lock:
            exporter._metrics_data = {}

    @patch("exporter._get_months_to_process")
    @patch("exporter._download_csv")
    @patch("exporter.storage.Client")
    def test_complete_monthly_report_processing(
        self, mock_client_class, mock_download_csv, mock_get_months
    ):
        """Test processing a complete monthly report with multiple dates and countries"""

        # Setup months to process
        mock_get_months.return_value = ["202501"]

        # Setup mock storage client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Setup bucket and blob
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob

        # Create mock CSV data for multiple days
        csv_data = [
            {
                "Date": "2025-01-01",
                "Country": "US",
                "Daily Device Installs": "1000",
                "Daily Device Uninstalls": "50",
                "Active Device Installs": "100000",
                "Daily User Installs": "900",
                "Daily User Uninstalls": "45",
            },
            {
                "Date": "2025-01-01",
                "Country": "GB",
                "Daily Device Installs": "500",
                "Daily Device Uninstalls": "25",
                "Active Device Installs": "50000",
                "Daily User Installs": "450",
                "Daily User Uninstalls": "20",
            },
            {
                "Date": "2025-01-02",
                "Country": "US",
                "Daily Device Installs": "1100",
                "Daily Device Uninstalls": "55",
                "Active Device Installs": "101000",
                "Daily User Installs": "1000",
                "Daily User Uninstalls": "50",
            },
            {
                "Date": "2025-01-02",
                "Country": "GB",
                "Daily Device Installs": "550",
                "Daily Device Uninstalls": "30",
                "Active Device Installs": "51000",
                "Daily User Installs": "500",
                "Daily User Uninstalls": "25",
            },
            {
                "Date": "2025-01-03",
                "Country": "US",
                "Daily Device Installs": "1200",
                "Daily Device Uninstalls": "60",
                "Active Device Installs": "102000",
                "Daily User Installs": "1100",
                "Daily User Uninstalls": "55",
            },
        ]

        mock_download_csv.return_value = csv_data

        # Process the package
        exporter._process_package_csv(mock_client, "com.example.app")

        # Verify results - each date should have its own entry
        with exporter._metrics_lock:
            device_installs = exporter._metrics_data.get("gplay_device_installs_v3", {})
            device_uninstalls = exporter._metrics_data.get(
                "gplay_device_uninstalls_v3", {}
            )
            active_installs = exporter._metrics_data.get(
                "gplay_active_device_installs_v3", {}
            )

            # Check that we have entries for each date
            # US entries
            self.assertIn(("com.example.app", "US", "2025-01-01"), device_installs)
            self.assertIn(("com.example.app", "US", "2025-01-02"), device_installs)
            self.assertIn(("com.example.app", "US", "2025-01-03"), device_installs)

            # GB entries
            self.assertIn(("com.example.app", "GB", "2025-01-01"), device_installs)
            self.assertIn(("com.example.app", "GB", "2025-01-02"), device_installs)

            # Check values are individual (not summed)
            self.assertEqual(
                device_installs[("com.example.app", "US", "2025-01-01")][0], 1000.0
            )
            self.assertEqual(
                device_installs[("com.example.app", "US", "2025-01-02")][0], 1100.0
            )
            self.assertEqual(
                device_installs[("com.example.app", "US", "2025-01-03")][0], 1200.0
            )

            # Check active installs (no longer using last value, each date has its own)
            self.assertEqual(
                active_installs[("com.example.app", "US", "2025-01-01")][0], 100000.0
            )
            self.assertEqual(
                active_installs[("com.example.app", "US", "2025-01-02")][0], 101000.0
            )
            self.assertEqual(
                active_installs[("com.example.app", "US", "2025-01-03")][0], 102000.0
            )

            # Check timestamps are date-specific (using UTC explicitly)
            timestamp_jan1 = int(
                dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc).timestamp() * 1000
            )
            timestamp_jan2 = int(
                dt.datetime(2025, 1, 2, tzinfo=dt.timezone.utc).timestamp() * 1000
            )
            timestamp_jan3 = int(
                dt.datetime(2025, 1, 3, tzinfo=dt.timezone.utc).timestamp() * 1000
            )

            self.assertEqual(
                device_installs[("com.example.app", "US", "2025-01-01")][1],
                timestamp_jan1,
            )
            self.assertEqual(
                device_installs[("com.example.app", "US", "2025-01-02")][1],
                timestamp_jan2,
            )
            self.assertEqual(
                device_installs[("com.example.app", "US", "2025-01-03")][1],
                timestamp_jan3,
            )

    @patch("exporter._get_months_to_process")
    @patch("exporter._download_csv")
    @patch("exporter.storage.Client")
    def test_multiple_months_lookback(
        self, mock_client_class, mock_download_csv, mock_get_months
    ):
        """Test processing multiple months with MONTHS_LOOKBACK"""

        # Setup months to process (2 months)
        mock_get_months.return_value = ["202501", "202412"]

        # Setup mock storage client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        # Setup blob mocks for two different months
        mock_blob_jan = MagicMock()
        mock_blob_jan.exists.return_value = True
        mock_blob_dec = MagicMock()
        mock_blob_dec.exists.return_value = True

        # Return different blob objects based on the requested month
        def get_blob(name):
            if "202501" in name:
                return mock_blob_jan
            elif "202412" in name:
                return mock_blob_dec
            return MagicMock()

        mock_bucket.blob.side_effect = get_blob

        # Mock CSV data for different months
        csv_data_jan = [
            {
                "Date": "2025-01-15",
                "Country": "US",
                "Daily Device Installs": "2000",
                "Daily Device Uninstalls": "100",
                "Active Device Installs": "150000",
                "Daily User Installs": "1800",
                "Daily User Uninstalls": "90",
            }
        ]

        csv_data_dec = [
            {
                "Date": "2024-12-15",
                "Country": "US",
                "Daily Device Installs": "1500",
                "Daily Device Uninstalls": "75",
                "Active Device Installs": "140000",
                "Daily User Installs": "1350",
                "Daily User Uninstalls": "70",
            }
        ]

        # Return different CSV data based on call order
        mock_download_csv.side_effect = [csv_data_jan, csv_data_dec]

        # Process the package
        exporter._process_package_csv(mock_client, "com.multimonth.app")

        # Verify both months' data are present
        with exporter._metrics_lock:
            device_installs = exporter._metrics_data.get("gplay_device_installs_v3", {})

            # Check January data
            self.assertIn(("com.multimonth.app", "US", "2025-01-15"), device_installs)
            self.assertEqual(
                device_installs[("com.multimonth.app", "US", "2025-01-15")][0], 2000.0
            )

            # Check December data
            self.assertIn(("com.multimonth.app", "US", "2024-12-15"), device_installs)
            self.assertEqual(
                device_installs[("com.multimonth.app", "US", "2024-12-15")][0], 1500.0
            )

    def test_prometheus_format_with_date_keys(self):
        """Test that Prometheus format is correctly generated with date-based entries"""

        # Simulate metric data with different dates (using UTC explicitly)
        timestamp_jan1 = int(
            dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc).timestamp() * 1000
        )
        timestamp_jan2 = int(
            dt.datetime(2025, 1, 2, tzinfo=dt.timezone.utc).timestamp() * 1000
        )
        timestamp_jan3 = int(
            dt.datetime(2025, 1, 3, tzinfo=dt.timezone.utc).timestamp() * 1000
        )

        with exporter._metrics_lock:
            exporter._metrics_data = {
                "gplay_device_installs_v3": {
                    ("com.app1", "US", "2025-01-01"): (1000.0, timestamp_jan1),
                    ("com.app1", "US", "2025-01-02"): (1100.0, timestamp_jan2),
                    ("com.app1", "US", "2025-01-03"): (1200.0, timestamp_jan3),
                    ("com.app1", "GB", "2025-01-01"): (500.0, timestamp_jan1),
                },
                "gplay_device_uninstalls_v3": {
                    ("com.app1", "US", "2025-01-01"): (50.0, timestamp_jan1),
                    ("com.app1", "US", "2025-01-02"): (55.0, timestamp_jan2),
                },
                "gplay_active_device_installs_v3": {
                    ("com.app1", "US", "2025-01-01"): (100000.0, timestamp_jan1),
                    ("com.app1", "US", "2025-01-02"): (101000.0, timestamp_jan2),
                    ("com.app1", "US", "2025-01-03"): (102000.0, timestamp_jan3),
                },
                "gplay_user_installs_v3": {
                    ("com.app1", "US", "2025-01-01"): (900.0, timestamp_jan1),
                    ("com.app1", "US", "2025-01-02"): (
                        0.0,
                        timestamp_jan2,
                    ),  # Zero value should be filtered
                },
                "gplay_user_uninstalls_v3": {
                    ("com.app1", "US", "2025-01-01"): (45.0, timestamp_jan1),
                },
            }

        output = exporter._format_prometheus_output()

        # Check format includes gauge type (not counter)
        self.assertIn("# TYPE gplay_device_installs_v3 gauge", output)
        self.assertIn("# TYPE gplay_active_device_installs_v3 gauge", output)

        # Verify metric lines with proper timestamps for each date
        self.assertIn(
            f'gplay_device_installs_v3{{package="com.app1",country="US"}} 1000.0 {timestamp_jan1}',
            output,
        )
        self.assertIn(
            f'gplay_device_installs_v3{{package="com.app1",country="US"}} 1100.0 {timestamp_jan2}',
            output,
        )
        self.assertIn(
            f'gplay_device_installs_v3{{package="com.app1",country="US"}} 1200.0 {timestamp_jan3}',
            output,
        )

        # Verify active installs have individual entries per date
        self.assertIn(
            f'gplay_active_device_installs_v3{{package="com.app1",country="US"}} 100000.0 {timestamp_jan1}',
            output,
        )
        self.assertIn(
            f'gplay_active_device_installs_v3{{package="com.app1",country="US"}} 101000.0 {timestamp_jan2}',
            output,
        )
        self.assertIn(
            f'gplay_active_device_installs_v3{{package="com.app1",country="US"}} 102000.0 {timestamp_jan3}',
            output,
        )

        # Verify zero values are filtered
        self.assertNotIn("900.0 " + str(timestamp_jan2), output)

        # Count total metric lines (excluding comments and empty lines)
        metric_lines = [
            line.strip()
            for line in output.split("\n")
            if line.strip() and not line.strip().startswith("#")
        ]

        # Should have 11 metric lines (12 total - 1 zero value filtered)
        self.assertEqual(len(metric_lines), 11)

    @patch("exporter._storage_client")
    def test_metrics_cleared_on_collection(self, mock_storage_client):
        """Test that metrics are completely cleared on each collection cycle"""

        # Setup initial metrics (old data)
        with exporter._metrics_lock:
            exporter._metrics_data = {
                "gplay_device_installs_v3": {
                    ("com.old.app", "US", "2025-01-10"): (5000.0, 1736553600000),
                    ("com.old.app", "GB", "2025-01-10"): (2000.0, 1736553600000),
                },
                "gplay_active_device_installs_v3": {
                    ("com.old.app", "US", "2025-01-10"): (100000.0, 1736553600000),
                },
            }

        # Verify old metrics exist
        with exporter._metrics_lock:
            self.assertEqual(len(exporter._metrics_data), 2)
            self.assertIn("gplay_device_installs_v3", exporter._metrics_data)

        # Mock storage client for collection
        mock_client = MagicMock()
        mock_client.list_blobs.return_value = []  # No packages found

        with patch("exporter._discover_packages_from_gcs") as mock_discover:
            mock_discover.return_value = set()  # No packages

            # Run collection
            exporter._run_metrics_collection()

        # Verify all metrics were cleared
        with exporter._metrics_lock:
            self.assertEqual(len(exporter._metrics_data), 0)

    @patch("exporter._get_months_to_process")
    @patch("exporter._download_csv")
    @patch("exporter.storage.Client")
    def test_multiple_packages_with_dates(
        self, mock_client_class, mock_download_csv, mock_get_months
    ):
        """Test processing multiple packages where each has date-specific metrics"""

        mock_get_months.return_value = ["202501"]

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock discovery of multiple packages
        blob1 = Mock()
        blob1.name = "stats/installs/installs_com.app1_202501_country.csv"
        blob2 = Mock()
        blob2.name = "stats/installs/installs_com.app2_202501_country.csv"

        mock_client.list_blobs.return_value = [blob1, blob2]

        # Mock bucket and blob
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob

        # Mock CSV data for each package
        csv_app1 = [
            {
                "Date": "2025-01-20",
                "Country": "US",
                "Daily Device Installs": "1000",
                "Daily Device Uninstalls": "50",
                "Active Device Installs": "100000",
                "Daily User Installs": "900",
                "Daily User Uninstalls": "45",
            },
            {
                "Date": "2025-01-21",
                "Country": "US",
                "Daily Device Installs": "1100",
                "Daily Device Uninstalls": "55",
                "Active Device Installs": "101000",
                "Daily User Installs": "990",
                "Daily User Uninstalls": "50",
            },
        ]

        csv_app2 = [
            {
                "Date": "2025-01-20",
                "Country": "FR",
                "Daily Device Installs": "800",
                "Daily Device Uninstalls": "40",
                "Active Device Installs": "80000",
                "Daily User Installs": "720",
                "Daily User Uninstalls": "36",
            }
        ]

        mock_download_csv.side_effect = [csv_app1, csv_app2]

        # Discover packages
        packages = exporter._discover_packages_from_gcs(mock_client)
        self.assertEqual(packages, {"com.app1", "com.app2"})

        # Process packages
        for package in sorted(packages):
            exporter._process_package_csv(mock_client, package)

        # Verify metrics for both packages with date-specific entries
        with exporter._metrics_lock:
            device_installs = exporter._metrics_data.get("gplay_device_installs_v3", {})

            # Check app1 metrics (2 dates)
            self.assertIn(("com.app1", "US", "2025-01-20"), device_installs)
            self.assertIn(("com.app1", "US", "2025-01-21"), device_installs)
            self.assertEqual(
                device_installs[("com.app1", "US", "2025-01-20")][0], 1000.0
            )
            self.assertEqual(
                device_installs[("com.app1", "US", "2025-01-21")][0], 1100.0
            )

            # Check app2 metrics
            self.assertIn(("com.app2", "FR", "2025-01-20"), device_installs)
            self.assertEqual(
                device_installs[("com.app2", "FR", "2025-01-20")][0], 800.0
            )

            # Check active installs are also date-specific
            active_installs = exporter._metrics_data.get(
                "gplay_active_device_installs_v3", {}
            )
            self.assertEqual(
                active_installs[("com.app1", "US", "2025-01-20")][0], 100000.0
            )
            self.assertEqual(
                active_installs[("com.app1", "US", "2025-01-21")][0], 101000.0
            )
            self.assertEqual(
                active_installs[("com.app2", "FR", "2025-01-20")][0], 80000.0
            )

    def test_v3_no_aggregation_each_date_separate(self):
        """Test that v3 does NOT aggregate data - each date is a separate metric"""

        with exporter._metrics_lock:
            exporter._metrics_data = {}

        # Create test data with multiple dates for same country
        test_csv_data = [
            {
                "Date": "2025-01-20",
                "Country": "US",
                "Daily Device Installs": "100",
                "Active Device Installs": "50000",
            },
            {
                "Date": "2025-01-21",
                "Country": "US",
                "Daily Device Installs": "200",
                "Active Device Installs": "50100",
            },
            {
                "Date": "2025-01-22",
                "Country": "US",
                "Daily Device Installs": "300",
                "Active Device Installs": "50300",
            },
        ]

        with patch("exporter._download_csv") as mock_download:
            mock_download.return_value = test_csv_data

            with patch("exporter._get_months_to_process") as mock_months:
                mock_months.return_value = ["202501"]

                mock_client = Mock()
                mock_bucket = Mock()
                mock_blob = Mock()
                mock_blob.exists.return_value = True
                mock_bucket.blob.return_value = mock_blob
                mock_client.bucket.return_value = mock_bucket

                exporter._process_package_csv(mock_client, "com.test.app")

                with exporter._metrics_lock:
                    installs = exporter._metrics_data.get(
                        "gplay_device_installs_v3", {}
                    )
                    active = exporter._metrics_data.get(
                        "gplay_active_device_installs_v3", {}
                    )

                    # Each date should have its own entry - NO aggregation
                    self.assertEqual(len(installs), 3)  # 3 separate dates
                    self.assertEqual(
                        installs[("com.test.app", "US", "2025-01-20")][0], 100.0
                    )
                    self.assertEqual(
                        installs[("com.test.app", "US", "2025-01-21")][0], 200.0
                    )
                    self.assertEqual(
                        installs[("com.test.app", "US", "2025-01-22")][0], 300.0
                    )

                    # Active installs also separate for each date
                    self.assertEqual(len(active), 3)
                    self.assertEqual(
                        active[("com.test.app", "US", "2025-01-20")][0], 50000.0
                    )
                    self.assertEqual(
                        active[("com.test.app", "US", "2025-01-21")][0], 50100.0
                    )
                    self.assertEqual(
                        active[("com.test.app", "US", "2025-01-22")][0], 50300.0
                    )

    def test_v3_all_metrics_are_gauges_not_counters(self):
        """Test that all v3 metrics are declared as gauge type, not counter"""
        output = exporter._format_prometheus_output()

        # Check all metric definitions
        for metric_name in exporter.METRIC_DEFINITIONS.keys():
            # All should be gauge
            self.assertIn(f"# TYPE {metric_name} gauge", output)
            # None should be counter
            self.assertNotIn(f"# TYPE {metric_name} counter", output)
            # All should have v3 suffix
            self.assertTrue(
                metric_name.endswith("_v3"),
                f"Metric {metric_name} should have _v3 suffix",
            )

    def test_v3_months_lookback_environment_variable(self):
        """Test that MONTHS_LOOKBACK environment variable works correctly"""

        # Save original value
        original = exporter.MONTHS_LOOKBACK

        try:
            # Test different values
            for lookback in [1, 3, 6, 12]:
                exporter.MONTHS_LOOKBACK = lookback
                months = exporter._get_months_to_process()

                self.assertEqual(
                    len(months), lookback, f"Should return {lookback} months"
                )

                # Verify format YYYYMM
                for month in months:
                    self.assertEqual(len(month), 6)
                    self.assertTrue(month.isdigit())
                    year = int(month[:4])
                    month_num = int(month[4:])
                    self.assertGreaterEqual(year, 2020)
                    self.assertLessEqual(year, 2030)
                    self.assertGreaterEqual(month_num, 1)
                    self.assertLessEqual(month_num, 12)
        finally:
            exporter.MONTHS_LOOKBACK = original

    def test_v3_complete_storage_refresh_on_collection(self):
        """Test that v3 completely clears and refreshes storage on each collection"""

        # Setup old metrics from previous collection
        with exporter._metrics_lock:
            exporter._metrics_data = {
                "gplay_device_installs_v3": {
                    ("com.old.app", "US", "2025-01-01"): (1000.0, 1735689600000),
                    ("com.old.app", "GB", "2025-01-01"): (500.0, 1735689600000),
                },
                "gplay_active_device_installs_v3": {
                    ("com.old.app", "US", "2025-01-01"): (100000.0, 1735689600000),
                },
            }
            # Verify we have old data
            self.assertEqual(len(exporter._metrics_data), 2)
            total_old_metrics = sum(len(v) for v in exporter._metrics_data.values())
            self.assertEqual(total_old_metrics, 3)

        # Setup mock for new collection with different data
        new_csv_data = [
            {"Date": "2025-01-15", "Country": "FR", "Daily Device Installs": "2000"},
        ]

        with patch("exporter._storage_client") as mock_client_func:
            with patch("exporter._download_csv") as mock_download:
                with patch("exporter._get_months_to_process") as mock_months:
                    mock_months.return_value = ["202501"]
                    mock_download.return_value = new_csv_data

                    mock_client = Mock()
                    mock_bucket = Mock()
                    mock_blob = Mock()
                    mock_blob.exists.return_value = True
                    mock_bucket.blob.return_value = mock_blob
                    mock_client.bucket.return_value = mock_bucket

                    # Mock package discovery
                    blob1 = Mock()
                    blob1.name = (
                        "stats/installs/installs_com.new.app_202501_country.csv"
                    )
                    mock_client.list_blobs.return_value = [blob1]

                    mock_client_func.return_value = mock_client

                    # Run complete collection cycle
                    exporter._run_metrics_collection()

                    with exporter._metrics_lock:
                        # Old metrics should be completely gone
                        installs = exporter._metrics_data.get(
                            "gplay_device_installs_v3", {}
                        )

                        # Should not have any old.app metrics
                        for key in installs.keys():
                            self.assertNotIn("com.old.app", key[0])

                        # Should only have new.app metrics
                        self.assertIn(("com.new.app", "FR", "2025-01-15"), installs)

    def test_v3_consistent_timestamps_for_same_date(self):
        """Test that all metrics for the same date have identical timestamps at midnight UTC"""

        # Setup test data with multiple metrics for the same dates
        test_csv_data = [
            {
                "Date": "2025-01-20",
                "Country": "US",
                "Daily Device Installs": "100",
                "Daily Device Uninstalls": "10",
                "Active Device Installs": "50000",
                "Daily User Installs": "90",
                "Daily User Uninstalls": "9",
            },
            {
                "Date": "2025-01-20",  # Same date, different country
                "Country": "GB",
                "Daily Device Installs": "50",
                "Daily Device Uninstalls": "5",
                "Active Device Installs": "25000",
                "Daily User Installs": "45",
                "Daily User Uninstalls": "4",
            },
            {
                "Date": "2025-01-21",  # Different date
                "Country": "US",
                "Daily Device Installs": "110",
                "Daily Device Uninstalls": "11",
                "Active Device Installs": "50100",
                "Daily User Installs": "99",
                "Daily User Uninstalls": "10",
            },
        ]

        with patch("exporter._download_csv") as mock_download:
            mock_download.return_value = test_csv_data

            with patch("exporter._get_months_to_process") as mock_months:
                mock_months.return_value = ["202501"]

                mock_client = Mock()
                mock_bucket = Mock()
                mock_blob = Mock()
                mock_blob.exists.return_value = True
                mock_bucket.blob.return_value = mock_blob
                mock_client.bucket.return_value = mock_bucket

                # Clear existing metrics
                with exporter._metrics_lock:
                    exporter._metrics_data = {}

                exporter._process_package_csv(mock_client, "com.test.app")

                # Verify timestamps
                with exporter._metrics_lock:
                    # Collect all timestamps for each date
                    timestamps_by_date = {}

                    for metric_name, metric_data in exporter._metrics_data.items():
                        for (package, country, date_str), (
                            value,
                            timestamp_ms,
                        ) in metric_data.items():
                            if date_str not in timestamps_by_date:
                                timestamps_by_date[date_str] = set()
                            timestamps_by_date[date_str].add(timestamp_ms)

                    # Each date should have exactly one unique timestamp
                    for date_str, timestamps in timestamps_by_date.items():
                        self.assertEqual(
                            len(timestamps),
                            1,
                            f"Date {date_str} has multiple different timestamps: {timestamps}",
                        )

                        # Verify it's midnight UTC
                        timestamp_ms = next(iter(timestamps))
                        dt_from_ts = dt.datetime.fromtimestamp(
                            timestamp_ms / 1000, tz=dt.timezone.utc
                        )
                        self.assertEqual(
                            dt_from_ts.hour,
                            0,
                            f"Timestamp for {date_str} is not at midnight",
                        )
                        self.assertEqual(dt_from_ts.minute, 0)
                        self.assertEqual(dt_from_ts.second, 0)
                        self.assertEqual(dt_from_ts.microsecond, 0)

                    # Verify different dates have different timestamps
                    self.assertEqual(
                        len(timestamps_by_date), 2
                    )  # We have 2 unique dates
                    all_timestamps = set()
                    for ts_set in timestamps_by_date.values():
                        all_timestamps.update(ts_set)
                    self.assertEqual(
                        len(all_timestamps),
                        2,
                        "Different dates should have different timestamps",
                    )

    def test_v3_timestamp_format_in_output(self):
        """Test that timestamps in Prometheus output are consistent for same date"""

        # Setup metrics with same date but different metrics/countries
        date_str = "2025-01-24"
        expected_timestamp = int(
            dt.datetime(2025, 1, 24, 0, 0, 0, tzinfo=dt.timezone.utc).timestamp() * 1000
        )

        with exporter._metrics_lock:
            exporter._metrics_data = {
                "gplay_device_installs_v3": {
                    ("com.app", "US", date_str): (100.0, expected_timestamp),
                    ("com.app", "GB", date_str): (50.0, expected_timestamp),
                },
                "gplay_device_uninstalls_v3": {
                    ("com.app", "US", date_str): (10.0, expected_timestamp),
                    ("com.app", "GB", date_str): (5.0, expected_timestamp),
                },
                "gplay_active_device_installs_v3": {
                    ("com.app", "US", date_str): (10000.0, expected_timestamp),
                    ("com.app", "GB", date_str): (5000.0, expected_timestamp),
                },
            }

        output = exporter._format_prometheus_output()

        # Extract all timestamps from output
        timestamps_in_output = set()
        for line in output.split("\n"):
            if line and not line.startswith("#"):
                # Parse timestamp from line (it's the last number after space)
                parts = line.split()
                if len(parts) >= 3:
                    try:
                        timestamp = int(parts[-1])
                        timestamps_in_output.add(timestamp)
                    except ValueError:
                        pass

        # All timestamps should be the same since all metrics are for the same date
        self.assertEqual(
            len(timestamps_in_output),
            1,
            f"Expected one unique timestamp, found: {timestamps_in_output}",
        )

        # And it should be the expected timestamp
        self.assertEqual(
            next(iter(timestamps_in_output)),
            expected_timestamp,
            f"Timestamp doesn't match expected midnight UTC timestamp",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
