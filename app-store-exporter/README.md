# Apple App Store Connect Metrics Exporter

Prometheus exporter for Apple App Store Connect analytics metrics. This exporter fetches daily metrics from the App Store Connect API and exposes them as Prometheus counters with built-in observability metrics.


## Dependencies

* prometheus_client
* requests
* pyjwt
* cryptography
* python-dateutil

## ⚠️ Critical Information: Report Creation Requirements

**IMPORTANT**: By default, no analytics reports are available through the API. Reports must be explicitly created before the exporter can access any data.

### Key Requirements:

1. **Reports cannot be created through App Store Connect UI** - They must be created programmatically via API
2. **Admin privileges required** - API key must have "Admin" or "Account Holder" permissions to create reports
3. **Reports-only access is insufficient** - API keys with only "Reports" permission cannot create reports

## Features

- **Multi-metric Support**: Exports 3 distinct metrics simultaneously with proper filtering
- **Flexible Label System**: Metrics include multiple dimensions (country, device, platform version, source type)
- **Granularity-aware Processing**: Handles both DAILY and WEEKLY report instances based on metric requirements
- **Advanced Filtering**: Applies row-level filtering (e.g., First-time downloads only, Delete events only)
- **Segment-aware Processing**: Handles multiple report segments with different schemas
- **Duplicate Protection**: Prevents double-counting across segments and dimensions
- **Report Management**: Includes utility for managing analytics report requests

## Exported Metrics

**Note**: Each metric includes additional dimensions beyond country for more detailed segmentation. The exporter automatically handles Apple's complex multi-segment data structure with proper deduplication across different dimensional cuts.

| Metric Name | Description | Labels |
|-------------|-------------|---------|
| `appstore_daily_user_installs` | Daily user installs (App Units) by country | `package`, `country`, `platform_version`, `source_type` |
| `appstore_active_devices` | Active devices by country (proxy for active device installs) | `package`, `country`, `device`, `platform_version`, `source_type` |
| `appstore_uninstalls` | Uninstalls by country (Installation and Deletion) | `package`, `country`, `device`, `platform_version`, `source_type` |
| **Exporter Metrics** | | |
| `appstore_exporter_parsing_errors_total` | Total parsing errors encountered | `package`, `report_type` |
| `appstore_exporter_last_collection_timestamp` | Unix timestamp of last successful collection | - |

## Prerequisites

1. **App Store Connect API Access**:
   - Generate API key in App Store Connect → Users and Access → Keys
   - Download the `.p8` private key file
   - Note the Issuer ID and Key ID

2. **Python 3.8+** with required dependencies

3. **API Key with Admin Privileges**: Required for report creation (see Permission Requirements below)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd infra-tooling/app-store-exporter
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Report Setup Instructions

### Automatic Report Creation with analytics-requests-manager.py

For API keys with Admin privileges, you can automate report creation using the included utility:

```bash
# List current report requests and available reports
python analytics-requests-manager.py \
  --issuer "your_issuer_id" \
  --key-id "your_key_id" \
  --p8 "/path/to/AuthKey.p8" \
  --bundles "com.your.app.bundleid" \
  --list

# Create ONGOING report requests for your apps
python analytics-requests-manager.py \
  --issuer "your_issuer_id" \
  --key-id "your_key_id" \
  --p8 "/path/to/AuthKey.p8" \
  --bundles "com.your.app.bundleid" \
  --create

# Delete existing report requests
python analytics-requests-manager.py \
  --issuer "your_issuer_id" \
  --key-id "your_key_id" \
  --p8 "/path/to/AuthKey.p8" \
  --bundles "com.your.app.bundleid" \
  --delete
```

**Note**: The `analytics-requests-manager.py` utility requires an API key with **Admin** or **Account Holder** privileges. API keys with only "Reports" access cannot create or delete report requests programmatically.

### Required Report Types for Exporter

The exporter looks for these specific report types and extracts data from corresponding columns:

| Metric | Report Name | Data Column | Granularity | Notes |
|--------|-------------|-------------|-------------|-------|
| Daily User Installs | App Downloads Standard | `Counts` | DAILY | Filtered to "First-time download" only |
| Active Devices | App Sessions Standard | `Unique Devices` | DAILY | Proxy for active device installs |
| Uninstalls | App Store Installation and Deletion Standard | `Counts` | WEEKLY | Filtered to "Delete" events only |

**Note**: The exporter automatically handles Apple's complex data structure with multiple segments and ensures proper deduplication across different dimensional cuts. The filtering logic ensures only relevant data is extracted (e.g., only first-time downloads for user installs).

**Important**: These are Apple's official report names and column names. The exporter automatically maps them to the appropriate Prometheus metrics and applies necessary filtering logic.

### API Key Permission Requirements

Different API key permissions enable different capabilities:

| Permission Level | Can Create Reports | Can Read Reports | Can Delete Reports | Notes |
|------------------|-------------------|------------------|-------------------|--------|
| **Reports Only** | ❌ No | ✅ Yes | ❌ No | Most common for monitoring |
| **Admin** | ✅ Yes | ✅ Yes | ✅ Yes | Full automation |
| **Account Holder** | ✅ Yes | ✅ Yes | ✅ Yes | Full access |

### Report Parameters

When creating reports, they are automatically configured with:
- **Access Type**: ONGOING (continuous data collection)
- **Granularity**: metric-specific (DAILY or WEEKLY), configured per metric in the exporter

### Data Processing Time

Apple typically processes reports within 24-48 hours after creation. The exporter will show "No daily instances found" until data is available.

## Configuration

### Required Environment Variables

```bash
# App Store Connect API credentials
export APPSTORE_EXPORTER_ISSUER_ID="your_issuer_id_here"
export APPSTORE_EXPORTER_KEY_ID="your_key_id_here"
export APPSTORE_EXPORTER_PRIVATE_KEY="/path/to/AuthKey_XXXXXX.p8"

# Optional: Customize behavior
export APPSTORE_EXPORTER_PORT="8000"  # HTTP server port (default: 8000)
export APPSTORE_EXPORTER_COLLECTION_INTERVAL_SECONDS="43200"  # Collection interval in seconds (default: 12 hours)
export APPSTORE_EXPORTER_DAYS_TO_FETCH="14"  # Number of days to fetch data for (default: 14)
export APPSTORE_EXPORTER_LOG_LEVEL="INFO"  # Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL
export APPSTORE_EXPORTER_TEST_MODE=""  # Set to "1" for test mode (run once and exit)

### App Configuration

Configure one or multiple apps using these environment variables:

**Single app configuration:**
```bash
export APPSTORE_EXPORTER_APP_ID="your_app_id_here"  # Required: App Store Connect App ID (numeric)
export APPSTORE_EXPORTER_BUNDLE_ID="your.bundle.id"  # Optional: Bundle ID for better logging and metrics labels
```

**Multiple apps configuration (comma-separated lists):**
```bash
export APPSTORE_EXPORTER_APP_IDS="app_id_1,app_id_2,app_id_3"  # Comma-separated list of App IDs
export APPSTORE_EXPORTER_BUNDLE_IDS="bundle.id.1,bundle.id.2,bundle.id.3"  # Optional: Comma-separated list of corresponding bundle IDs
```

**Important notes:**
- `APPSTORE_EXPORTER_APP_ID` or `APPSTORE_EXPORTER_APP_IDS` is required for API calls
- `APPSTORE_EXPORTER_BUNDLE_ID`/`APPSTORE_EXPORTER_BUNDLE_IDS` are optional but recommended for better readability
- If bundle ID is not provided, the exporter will use "App_{app_id}" format
- For multiple apps, the order in `APPSTORE_EXPORTER_APP_IDS` and `APPSTORE_EXPORTER_BUNDLE_IDS` must match
- If `APPSTORE_EXPORTER_BUNDLE_IDS` has fewer items than `APPSTORE_EXPORTER_APP_IDS`, missing bundle IDs will use fallback names
```

### Generating App Store Connect API Credentials

**Warning**: For report creation, you need an API key with **Admin** or **Account Holder** privileges!

1. Go to [App Store Connect](https://appstoreconnect.apple.com)
2. Navigate to **Users and Access** → **Keys**
3. Click the **+** button to create a new key
4. Select the appropriate permissions:
   - **For report creation**: Admin or Account Holder privileges
   - **For read-only access**: Reports or Analytics Read access
5. Download the `.p8` private key file
6. Note the **Issuer ID** and **Key ID**

## Usage

### Production Deployment

```bash
APPSTORE_EXPORTER_ISSUER_ID="your_issuer_id" \
APPSTORE_EXPORTER_KEY_ID="your_key_id" \
APPSTORE_EXPORTER_PRIVATE_KEY="/path/to/AuthKey.p8" \
APPSTORE_EXPORTER_APP_ID="your_app_id" \
APPSTORE_EXPORTER_BUNDLE_ID="your.bundle.id" \
python exporter.py

# Multiple apps with custom settings:
APPSTORE_EXPORTER_PORT="9090" \
APPSTORE_EXPORTER_COLLECTION_INTERVAL_SECONDS="21600" \
APPSTORE_EXPORTER_DAYS_TO_FETCH="7" \
APPSTORE_EXPORTER_ISSUER_ID="your_issuer_id" \
APPSTORE_EXPORTER_KEY_ID="your_key_id" \
APPSTORE_EXPORTER_PRIVATE_KEY="/path/to/AuthKey.p8" \
APPSTORE_EXPORTER_APP_IDS="1234567890,9876543210" \
APPSTORE_EXPORTER_BUNDLE_IDS="com.company.app1,com.company.app2" \
python exporter.py
```

### Debug Mode

```bash
APPSTORE_EXPORTER_LOG_LEVEL="DEBUG" \
APPSTORE_EXPORTER_ISSUER_ID="your_issuer_id" \
APPSTORE_EXPORTER_KEY_ID="your_key_id" \
APPSTORE_EXPORTER_PRIVATE_KEY="/path/to/AuthKey.p8" \
APPSTORE_EXPORTER_APP_ID="your_app_id" \
APPSTORE_EXPORTER_BUNDLE_ID="your.bundle.id" \
APPSTORE_EXPORTER_TEST_MODE="1" \
python exporter.py
```

### Testing

Run unit tests and integration tests:

```bash
# Run unit tests
python test_exporter.py

# Run with debug output
python test_exporter.py --debug

# Run integration tests with mocked API
python test_exporter.py --integration

# Test against real API (requires valid credentials)
python test_exporter.py --real-api
```

## API Endpoints

- **`/metrics`**: Prometheus metrics endpoint
- **`/healthz`**: Health check endpoint (returns 200 OK only after successful collection)

## Docker Deployment

```bash
docker build -t appstore-exporter .
docker run -d \
  -p 8000:8000 \
  -e APPSTORE_EXPORTER_ISSUER_ID="your_issuer_id" \
  -e APPSTORE_EXPORTER_KEY_ID="your_key_id" \
  -e APPSTORE_EXPORTER_PRIVATE_KEY="/path/to/AuthKey.p8" \
  -e APPSTORE_EXPORTER_APP_ID="your_app_id" \
  -e APPSTORE_EXPORTER_BUNDLE_ID="your.bundle.id" \
  appstore-exporter
```

## License

Apache 2.0 License - See LICENSE file for details.

## Support

For issues and feature requests, please create an issue in the project repository.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Changelog

### v1.2.0
- **Breaking Change**: The metric label `app` was renamed to `package` for consistency with other exporters.
- **Enhanced Logging**: Improved deduplication logic

### v1.1.1
- **WSGI Log Suppression**: Fixed HTTP access logs to only appear in DEBUG mode (including WSGI server logs)

### v1.1.0
- **Breaking Change**: All environment variables now use `APPSTORE_EXPORTER_` prefix for consistency
- **Improved Health Check**: Service is healthy only after first successful collection
- **Enhanced Logging**: HTTP endpoint access logged only in DEBUG mode to reduce noise
- **Better Error Handling**: Improved health state tracking with partial failure support
- **Code Documentation**: Added comprehensive docstrings to all major functions
- **Testing Framework**: Added comprehensive test suite with unit and integration tests
- **Performance**: Optimized report caching to reduce API calls
- **Stability**: Added retry logic with exponential backoff for transient failures
- **Thread Safety**: Added registry lock to prevent race conditions during metric collection
- **Self-Monitoring Metrics**: Added exporter's own metrics for observability:
  - `appstore_exporter_parsing_errors_total`: Track parsing errors per app and report type
  - `appstore_exporter_last_collection_timestamp`: Monitor freshness of collected data
- **Improved Error Handling**: More granular exception handling for CSV parsing (UnicodeDecodeError, csv.Error)

### v1.0.0
- **Init**: Initial release with comprehensive Apple App Store Connect metrics export
- **Multi-metric Support**: 3 core metrics with proper filtering logic
- **Enhanced Label System**: Multi-dimensional labels (country, device, platform version, source type)
- **Advanced Filtering**: Row-level filtering for accurate metric extraction
- **Duplicate Protection**: Prevents double-counting across segments
- **Segment-aware Processing**: Handles multiple segments with different schemas
- **Granularity Support**: DAILY and WEEKLY processing based on metric requirements
- **Flexible App Configuration**: Support for single and multiple apps
- **Prometheus Integration**: Standard /metrics endpoint with proper label support
- **Report Management**: Includes utility for managing analytics report requests