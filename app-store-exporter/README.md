# Apple App Store Connect Metrics Exporter

Prometheus exporter for Apple App Store Connect analytics metrics. This exporter fetches daily metrics from the App Store Connect API and exposes them as Prometheus counters.


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

- **Multi-metric Support**: Exports 5 distinct metrics simultaneously
- **Country-level Granularity**: Metrics are exported per country with proper labels
- **Latest Data Processing**: Processes the most recent daily reports
- **Zero-value Handling**: Exports metrics even when values are zero
- **Performance Optimized**: Debug calculations only performed when debug logging is enabled
- **Report Management**: Includes utility for managing analytics report requests

## Exported Metrics

| Metric Name | Description | Labels |
|-------------|-------------|---------|
| `appstore_daily_installs` | Daily installs by country | `app`, `country` |
| `appstore_daily_deletions` | Daily deletions by country | `app`, `country` |
| `appstore_active_devices` | Active devices by country | `app`, `country` |
| `appstore_daily_sessions` | Daily sessions by country | `app`, `country` |
| `appstore_daily_page_views` | Daily page views by country | `app`, `country` |

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

| Metric | Report Name | Data Column | Notes |
|--------|-------------|-------------|-------|
| Daily Installs | App Store Installation and Deletion | `Installations` | Standard or Detailed report |
| Daily Deletions | App Store Installation and Deletion | `Deletions` | Standard or Detailed report |
| Active Devices | App Sessions | `Unique Devices` | From App Sessions report |
| Daily Sessions | App Sessions | `Sessions` | Standard or Detailed report |
| Page Views | App Store Discovery and Engagement | `Product Page Views` | Standard or Detailed report |

**Important**: These are Apple's official report names and column names. The exporter automatically maps them to the appropriate Prometheus metrics.

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
- **Time Granularity**: DAY (daily detail)
- **Frequency**: DAILY (daily updates)

### Data Processing Time

Apple typically processes reports within 24-48 hours after creation. The exporter will show "No daily instances found" until data is available.

## Configuration

### Required Environment Variables

```bash
# App Store Connect API credentials
export APPSTORE_ISSUER_ID="your_issuer_id_here"
export APPSTORE_KEY_ID="your_key_id_here"
export APPSTORE_PRIVATE_KEY="/path/to/AuthKey_XXXXXX.p8"

# Optional: Customize behavior
export PORT="8000"  # HTTP server port (default: 8000)
export COLLECTION_INTERVAL_SECONDS="43200"  # Collection interval in seconds (default: 12 hours)
export DAYS_TO_FETCH="14"  # Number of days to fetch data for (default: 14)
export LOG_LEVEL="INFO"  # Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL
export TEST_MODE=""  # Set to "1" for test mode (run once and exit)

### App Configuration

Configure one or multiple apps using these environment variables:

**Single app configuration:**
```bash
export APPSTORE_APP_ID="your_app_id_here"  # Required: App Store Connect App ID (numeric)
export APPSTORE_BUNDLE_ID="your.bundle.id"  # Optional: Bundle ID for better logging and metrics labels
```

**Multiple apps configuration (comma-separated lists):**
```bash
export APPSTORE_APP_IDS="app_id_1,app_id_2,app_id_3"  # Comma-separated list of App IDs
export APPSTORE_BUNDLE_IDS="bundle.id.1,bundle.id.2,bundle.id.3"  # Optional: Comma-separated list of corresponding bundle IDs
```

**Important notes:**
- `APPSTORE_APP_ID` or `APPSTORE_APP_IDS` is required for API calls
- `APPSTORE_BUNDLE_ID`/`APPSTORE_BUNDLE_IDS` are optional but recommended for better readability
- If bundle ID is not provided, the exporter will use "App_{app_id}" format
- For multiple apps, the order in `APPSTORE_APP_IDS` and `APPSTORE_BUNDLE_IDS` must match
- If `APPSTORE_BUNDLE_IDS` has fewer items than `APPSTORE_APP_IDS`, missing bundle IDs will use fallback names
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
APPSTORE_ISSUER_ID="your_issuer_id" \
APPSTORE_KEY_ID="your_key_id" \
APPSTORE_PRIVATE_KEY="/path/to/AuthKey.p8" \
APPSTORE_APP_ID="your_app_id" \
APPSTORE_BUNDLE_ID="your.bundle.id" \
python exporter.py

# Multiple apps with custom settings:
PORT="9090" \
COLLECTION_INTERVAL_SECONDS="21600" \
DAYS_TO_FETCH="7" \
APPSTORE_ISSUER_ID="your_issuer_id" \
APPSTORE_KEY_ID="your_key_id" \
APPSTORE_PRIVATE_KEY="/path/to/AuthKey.p8" \
APPSTORE_APP_IDS="1234567890,9876543210" \
APPSTORE_BUNDLE_IDS="com.company.app1,com.company.app2" \
python exporter.py

# Multiple apps example:
APPSTORE_ISSUER_ID="your_issuer_id" \
APPSTORE_KEY_ID="your_key_id" \
APPSTORE_PRIVATE_KEY="/path/to/AuthKey.p8" \
APPSTORE_APP_IDS="1234567890,9876543210" \
APPSTORE_BUNDLE_IDS="com.company.app1,com.company.app2" \
python exporter.py
```

### Debug Mode

```bash
LOG_LEVEL="DEBUG" \
APPSTORE_ISSUER_ID="your_issuer_id" \
APPSTORE_KEY_ID="your_key_id" \
APPSTORE_PRIVATE_KEY="/path/to/AuthKey.p8" \
APPSTORE_APP_ID="your_app_id" \
APPSTORE_BUNDLE_ID="your.bundle.id" \
TEST_MODE="1" \
python exporter.py

# Multiple apps test example:
APPSTORE_ISSUER_ID="your_issuer_id" \
APPSTORE_KEY_ID="your_key_id" \
APPSTORE_PRIVATE_KEY="/path/to/AuthKey.p8" \
APPSTORE_APP_IDS="1234567890,9876543210" \
APPSTORE_BUNDLE_IDS="com.company.app1,com.company.app2" \
TEST_MODE="1" \
python exporter.py
```

### Custom Configuration

```bash
PORT="9090" \
COLLECTION_INTERVAL_SECONDS="21600" \
DAYS_TO_FETCH="7" \
APPSTORE_ISSUER_ID="your_issuer_id" \
APPSTORE_KEY_ID="your_key_id" \
APPSTORE_PRIVATE_KEY="/path/to/AuthKey.p8" \
APPSTORE_APP_ID="your_app_id" \
APPSTORE_BUNDLE_ID="your.bundle.id" \
python exporter.py
```

## API Endpoints

- **`/metrics`**: Prometheus metrics endpoint
- **`/healthz`**: Health check endpoint

## Docker Deployment

```bash
docker build -t appstore-exporter .
docker run -d \
  -p 8000:8000 \
  -e APPSTORE_ISSUER_ID="your_issuer_id" \
  -e APPSTORE_KEY_ID="your_key_id" \
  -e APPSTORE_PRIVATE_KEY="/path/to/AuthKey.p8" \
  -e APPSTORE_APP_ID="your_app_id" \
  -e APPSTORE_BUNDLE_ID="your.bundle.id" \
  appstore-exporter

# Multiple apps Docker example:
docker run -d \
  -p 8000:8000 \
  -e APPSTORE_ISSUER_ID="your_issuer_id" \
  -e APPSTORE_KEY_ID="your_key_id" \
  -e APPSTORE_PRIVATE_KEY="/path/to/AuthKey.p8" \
  -e APPSTORE_APP_IDS="1234567890,9876543210" \
  -e APPSTORE_BUNDLE_IDS="com.company.app1,com.company.app2" \
  appstore-exporter
```

### Debug Logging

Enable debug logging for detailed information:

```bash
LOG_LEVEL="DEBUG" python exporter.py
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

### v1.0.0
- Initial release
- Support for 5 core metrics
- Automatic app discovery
- Country-level granularity
- Prometheus metrics endpoint
- Report management utility