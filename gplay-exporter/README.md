Google Play Console Metrics Exporter for Prometheus

This exporter extracts and exposes multiple metrics from Google Play Console CSV reports
as Prometheus counters. It automatically discovers packages, processes the latest daily
CSV files, and exports metrics for monitoring and alerting.

## Dependencies

* prometheus_client
* google-cloud-storage
* google-auth

## Key Features

- **Multi-metric Support**: Exports three distinct counter metrics simultaneously
- **Automatic Package Discovery**: Scans GCS bucket to find all available packages
- **Latest Data Processing**: Always processes the most recent CSV file for each package
- **Country-level Granularity**: Metrics are exported per country with proper labels
- **Zero-value Handling**: Exports metrics even when values are zero (except when all metrics are zero)
- **Performance Optimized**: Debug calculations only performed when debug logging is enabled

## Exported Metrics

1. **gplay_daily_device_installs_total{package,country}**
   - Daily device installs by country from Google Play Console
   - **CSV Column**: "Daily Device Installs"
   - Counter type: GAUGE (absolute values for each day)

2. **gplay_daily_device_uninstalls_total{package,country}**
   - Daily device uninstalls by country from Google Play Console
   - **CSV Column**: "Daily Device Uninstalls"
   - Counter type: GAUGE (absolute values for each day)

3. **gplay_active_device_installs_total{package,country}**
   - Active device installs by country from Google Play Console
   - **CSV Column**: "Active Device Installs"
   - Counter type: GAUGE (absolute values for each day)

4. **gplay_daily_user_installs_total{package,country}**
   - Daily user installs by country from Google Play Console
   - **CSV Column**: "Daily User Installs"
   - Counter type: GAUGE (absolute values for each day)

5. **gplay_daily_user_uninstalls_total{package,country}**
   - Daily user uninstalls by country from Google Play Console
   - **CSV Column**: "Daily User Uninstalls"
   - Counter type: GAUGE (absolute values for each day)

## Data Source & Processing

- **Source**: Google Cloud Storage bucket containing Play Console CSV exports
- **File Pattern**: `stats/installs/installs_<package>_<YYYYMM>_country.csv`
- **Processing**: Only processes rows with the latest date found in each CSV
- **Column Mapping**: Automatically handles different column name variations

## Architecture

- **Background Collection**: Periodic collection runs in background thread
- **Fresh Counters**: Creates new counter instances for each collection cycle
- **HTTP Server**: Exposes metrics on `/metrics` endpoint (Prometheus format)
- **Health Check**: Provides `/healthz` endpoint for monitoring

## Environment Variables

### REQUIRED:
- **GOOGLE_APPLICATION_CREDENTIALS**: Path to Google service account credentials JSON file
- **GPLAY_BUCKET_ID**: Google Cloud Storage bucket ID containing Play Console CSVs
  - Example: "pubsite_prod_rev_01234567890987654321"

### OPTIONAL:
- **PORT**: HTTP server port (default: 8000)
- **COLLECTION_INTERVAL_SECONDS**: Metrics collection interval in seconds (default: 43200 = 12 hours)
- **GCS_PROJECT**: Google Cloud project ID (optional, uses default project from credentials)
- **TEST_MODE**: If set, runs one collection cycle and exits (values: "1", "true", etc.)
- **LOG_LEVEL**: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL - default: INFO)

## Performance Characteristics

- **Memory Efficient**: Processes CSV files in memory with streaming
- **Debug Optimization**: Expensive debug calculations only performed when LOG_LEVEL=DEBUG
- **Set-based Operations**: Uses efficient set operations for date parsing and deduplication
- **Single Pass Processing**: Extracts all three metrics in a single pass through CSV data

## Error Handling

- **Graceful Degradation**: Continues processing other packages if one fails
- **Comprehensive Logging**: Detailed debug logs available when needed
- **Input Validation**: Handles malformed CSV data and missing columns
- **Authentication Errors**: Proper error messages for credential issues

## Usage Examples

1. **Production Deployment**:
   ```bash
   GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json \
   GPLAY_BUCKET_ID=pubsite_prod_123456789 \
   python exporter.py
   ```

2. **Debug Mode**:
   ```bash
   LOG_LEVEL=DEBUG \
   GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json \
   GPLAY_BUCKET_ID=pubsite_prod_123456789 \
   TEST_MODE=1 \
   python exporter.py
   ```

3. **Custom Configuration**:
   ```bash
   PORT=9090 \
   COLLECTION_INTERVAL_SECONDS=21600 \
   GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json \
   GPLAY_BUCKET_ID=pubsite_prod_123456789 \
   python exporter.py
   ```

## Monitoring & Alerting

The exporter provides Prometheus metrics that can be used for:
- Data freshness monitoring (latest date processed)
- Package discovery status
- Collection cycle timing
- Error rate monitoring

## CSV Column Support

Automatically handles these column name :
- Date: "Date"
- Country: "Country"
- Daily Device Installs: "Daily Device Installs"
- Daily Device Uninstalls: "Daily Device Uninstalls"
- Active Device Installs: "Active Device Installs"
- Daily User Installs: "Daily User Installs"
- Daily User Uninstalls: "Daily User Uninstalls"

## Date Format Support

Parses multiple date formats:
- YYYY-MM-DD (2025-08-19)
- DD-MMM-YYYY (19-Aug-2025)
- MM/DD/YYYY (08/19/2025)