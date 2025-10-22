#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Integration test for Google Play Console Metrics Exporter

This test simulates real-world scenarios with mock GCS data to ensure
the exporter correctly handles different aggregation strategies and
generates proper Prometheus format with timestamps.
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

# Add the parent directory to sys.path to import the exporter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import exporter


class TestIntegrationScenarios(unittest.TestCase):
    """Integration tests for realistic scenarios"""

    def setUp(self):
        """Clear metrics before each test"""
        with exporter._metrics_lock:
            exporter._metrics_data = {}

    @patch("exporter._storage_client")
    @patch("exporter._load_credentials")
    def test_complete_monthly_report_processing(self, mock_creds, mock_storage_client):
        """Test processing a complete monthly report with multiple packages and countries"""

        # Setup mock credentials
        mock_creds.return_value = MagicMock()

        # Setup mock storage client
        mock_client = MagicMock()
        mock_storage_client.return_value = mock_client

        # Mock bucket and blobs
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        # Create mock CSV data for multiple days
        csv_data = """Date,Country,Daily Device Installs,Daily Device Uninstalls,Active Device Installs,Daily User Installs,Daily User Uninstalls
2025-01-01,US,1000,50,100000,900,45
2025-01-01,GB,500,25,50000,450,20
2025-01-02,US,1100,55,101000,1000,50
2025-01-02,GB,550,30,51000,500,25
2025-01-03,US,1200,60,102000,1100,55
2025-01-03,GB,600,35,52000,550,30
2025-01-04,US,1300,65,103000,1200,60
2025-01-04,GB,650,40,53000,600,35
2025-01-05,US,1400,70,104000,1300,65
2025-01-05,GB,700,45,54000,650,40"""

        # Setup blob mock
        mock_blob = MagicMock()
        mock_blob.download_as_bytes.return_value = csv_data.encode("utf-8")
        mock_bucket.blob.return_value = mock_blob

        # Setup blob listing
        blob1 = MagicMock()
        blob1.name = "stats/installs/installs_com.example.app_202501_country.csv"
        mock_client.list_blobs.return_value = [blob1]

        # Process the package
        exporter._process_package_csv(mock_client, "com.example.app")

        # Verify results
        with exporter._metrics_lock:
            # Check US metrics
            # Device installs should be summed: 1000+1100+1200+1300+1400 = 6000
            self.assertEqual(
                exporter._metrics_data["gplay_device_installs_v2"][
                    ("com.example.app", "US")
                ][0],
                6000.0,
            )
            # Device uninstalls should be summed: 50+55+60+65+70 = 300
            self.assertEqual(
                exporter._metrics_data["gplay_device_uninstalls_v2"][
                    ("com.example.app", "US")
                ][0],
                300.0,
            )
            # Active device installs should be last value: 104000
            self.assertEqual(
                exporter._metrics_data["gplay_active_device_installs_v2"][
                    ("com.example.app", "US")
                ][0],
                104000.0,
            )

            # Check GB metrics
            # Device installs should be summed: 500+550+600+650+700 = 3000
            self.assertEqual(
                exporter._metrics_data["gplay_device_installs_v2"][
                    ("com.example.app", "GB")
                ][0],
                3000.0,
            )
            # Active device installs should be last value: 54000
            self.assertEqual(
                exporter._metrics_data["gplay_active_device_installs_v2"][
                    ("com.example.app", "GB")
                ][0],
                54000.0,
            )

            # Check timestamp (should be from 2025-01-05)
            expected_timestamp = int(dt.datetime(2025, 1, 5).timestamp() * 1000)
            self.assertEqual(
                exporter._metrics_data["gplay_device_installs_v2"][
                    ("com.example.app", "US")
                ][1],
                expected_timestamp,
            )

    @patch("exporter._storage_client")
    @patch("exporter._load_credentials")
    def test_retroactive_data_addition(self, mock_creds, mock_storage_client):
        """Test scenario where data is added retroactively for multiple days"""

        # This simulates the problem v1 had where it would only capture the last day
        # v2 should capture all days correctly

        mock_creds.return_value = MagicMock()
        mock_client = MagicMock()
        mock_storage_client.return_value = mock_client
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        # Simulate a report where days 10-15 were just added all at once
        csv_data = """Date,Country,Daily Device Installs,Daily Device Uninstalls,Active Device Installs,Daily User Installs,Daily User Uninstalls
2025-01-10,US,100,10,90000,90,9
2025-01-11,US,110,11,91000,100,10
2025-01-12,US,120,12,92000,110,11
2025-01-13,US,130,13,93000,120,12
2025-01-14,US,140,14,94000,130,13
2025-01-15,US,150,15,95000,140,14"""

        mock_blob = MagicMock()
        mock_blob.download_as_bytes.return_value = csv_data.encode("utf-8")
        mock_bucket.blob.return_value = mock_blob

        blob1 = MagicMock()
        blob1.name = "stats/installs/installs_com.retroactive.app_202501_country.csv"
        mock_client.list_blobs.return_value = [blob1]

        exporter._process_package_csv(mock_client, "com.retroactive.app")

        with exporter._metrics_lock:
            # v1 would only capture 150 (last day)
            # v2 should capture 100+110+120+130+140+150 = 750
            self.assertEqual(
                exporter._metrics_data["gplay_device_installs_v2"][
                    ("com.retroactive.app", "US")
                ][0],
                750.0,
            )
            # Active installs should be only the last value: 95000
            self.assertEqual(
                exporter._metrics_data["gplay_active_device_installs_v2"][
                    ("com.retroactive.app", "US")
                ][0],
                95000.0,
            )

    def test_prometheus_format_with_real_data(self):
        """Test that Prometheus format is correctly generated with timestamps"""

        # Simulate real metric data
        test_timestamp = int(dt.datetime(2025, 1, 15).timestamp() * 1000)

        with exporter._metrics_lock:
            exporter._metrics_data = {
                "gplay_device_installs_v2": {
                    ("com.app1", "US"): (5000.0, test_timestamp),
                    ("com.app1", "GB"): (2000.0, test_timestamp),
                    ("com.app2", "US"): (3000.0, test_timestamp),
                },
                "gplay_device_uninstalls_v2": {
                    ("com.app1", "US"): (250.0, test_timestamp),
                    ("com.app1", "GB"): (100.0, test_timestamp),
                },
                "gplay_active_device_installs_v2": {
                    ("com.app1", "US"): (95000.0, test_timestamp),
                    ("com.app1", "GB"): (45000.0, test_timestamp),
                    ("com.app2", "US"): (120000.0, test_timestamp),
                },
                "gplay_user_installs_v2": {
                    ("com.app1", "US"): (4500.0, test_timestamp),
                    ("com.app1", "GB"): (
                        0.0,
                        test_timestamp,
                    ),  # Zero value should be filtered
                },
                "gplay_user_uninstalls_v2": {
                    ("com.app1", "US"): (200.0, test_timestamp),
                    ("com.app1", "GB"): (80.0, test_timestamp),
                },
            }

        output = exporter._format_prometheus_output()

        # Check format includes timestamps
        lines = output.split("\n")

        # Verify HELP and TYPE lines exist
        self.assertIn("# HELP gplay_device_installs_v2", output)
        self.assertIn("# TYPE gplay_device_installs_v2 counter", output)
        self.assertIn("# HELP gplay_active_device_installs_v2", output)
        self.assertIn("# TYPE gplay_active_device_installs_v2 counter", output)

        # Verify metric lines with timestamps
        self.assertIn(
            f'gplay_device_installs_v2{{package="com.app1",country="US"}} 5000.0 {test_timestamp}',
            output,
        )
        self.assertIn(
            f'gplay_device_installs_v2{{package="com.app1",country="GB"}} 2000.0 {test_timestamp}',
            output,
        )
        self.assertIn(
            f'gplay_active_device_installs_v2{{package="com.app1",country="US"}} 95000.0 {test_timestamp}',
            output,
        )

        # Verify zero values are filtered
        self.assertNotIn(
            'gplay_user_installs_v2{package="com.app1",country="GB"}', output
        )

        # Verify all metrics have timestamps
        for line in lines:
            if line and not line.startswith("#") and line.strip():
                # Each metric line should end with a timestamp
                self.assertTrue(
                    line.strip().endswith(str(test_timestamp)),
                    f"Line missing timestamp: {line}",
                )

    @patch("exporter._storage_client")
    @patch("exporter._load_credentials")
    def test_multiple_packages_processing(self, mock_creds, mock_storage_client):
        """Test processing multiple packages in a single collection cycle"""

        mock_creds.return_value = MagicMock()
        mock_client = MagicMock()
        mock_storage_client.return_value = mock_client

        # Mock discovery of multiple packages
        blob1 = Mock()
        blob1.name = "stats/installs/installs_com.app1_202501_country.csv"
        blob2 = Mock()
        blob2.name = "stats/installs/installs_com.app2_202501_country.csv"
        blob3 = Mock()
        blob3.name = (
            "stats/installs/installs_com.app3_202501_overview.csv"  # Should be ignored
        )
        discovery_blobs = [blob1, blob2, blob3]

        mock_client.list_blobs.side_effect = [
            discovery_blobs,  # For package discovery
            [discovery_blobs[0]],  # For app1 processing
            [discovery_blobs[1]],  # For app2 processing
        ]

        # Discover packages
        packages = exporter._discover_packages_from_gcs(mock_client)
        self.assertEqual(packages, {"com.app1", "com.app2", "com.app3"})

        # Mock CSV data for each package
        csv_app1 = """Date,Country,Daily Device Installs,Daily Device Uninstalls,Active Device Installs,Daily User Installs,Daily User Uninstalls
2025-01-20,US,1000,50,100000,900,45
2025-01-20,GB,500,25,50000,450,20"""

        csv_app2 = """Date,Country,Daily Device Installs,Daily Device Uninstalls,Active Device Installs,Daily User Installs,Daily User Uninstalls
2025-01-20,US,2000,100,200000,1800,90
2025-01-20,FR,800,40,80000,720,36"""

        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        mock_blob = MagicMock()
        mock_blob.download_as_bytes.side_effect = [
            csv_app1.encode("utf-8"),
            csv_app2.encode("utf-8"),
        ]
        mock_bucket.blob.return_value = mock_blob

        # Process only packages with country CSV files (app1 and app2)
        for package in ["com.app1", "com.app2"]:
            exporter._process_package_csv(mock_client, package)

        # Verify both packages have metrics
        with exporter._metrics_lock:
            # Check app1 metrics exist
            self.assertIn(
                ("com.app1", "US"), exporter._metrics_data["gplay_device_installs_v2"]
            )
            self.assertIn(
                ("com.app1", "GB"), exporter._metrics_data["gplay_device_installs_v2"]
            )

            # Check app2 metrics exist
            self.assertIn(
                ("com.app2", "US"), exporter._metrics_data["gplay_device_installs_v2"]
            )
            self.assertIn(
                ("com.app2", "FR"), exporter._metrics_data["gplay_device_installs_v2"]
            )

            # Verify values
            self.assertEqual(
                exporter._metrics_data["gplay_device_installs_v2"][("com.app1", "US")][
                    0
                ],
                1000.0,
            )
            self.assertEqual(
                exporter._metrics_data["gplay_device_installs_v2"][("com.app2", "US")][
                    0
                ],
                2000.0,
            )
            self.assertEqual(
                exporter._metrics_data["gplay_active_device_installs_v2"][
                    ("com.app1", "US")
                ][0],
                100000.0,
            )
            self.assertEqual(
                exporter._metrics_data["gplay_active_device_installs_v2"][
                    ("com.app2", "US")
                ][0],
                200000.0,
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
