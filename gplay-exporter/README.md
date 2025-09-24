# Google Play Console Metrics Exporter for Prometheus

This exporter extracts and exposes multiple metrics from Google Play Console CSV reports
as Prometheus counters. It automatically discovers packages, processes the latest daily
CSV files, and exports metrics for monitoring and alerting.

## Dependencies

* prometheus_client
* google-cloud-storage
* google-auth

## Key Features

- **Multi-metric Support**: Exports five distinct counter metrics simultaneously
- **Automatic Package Discovery**: Scans GCS bucket to find all available packages
- **Latest Data Processing**: Always processes the most recent CSV file for each package
- **Country-level Granularity**: Metrics are exported per country with proper labels
- **Zero-value Handling**: Exports metrics even when values are zero (except when all metrics are zero)
- **Performance Optimized**: Debug calculations only performed when debug logging is enabled
- **Health Check**: Simple health status endpoint for monitoring
- **Quiet Operation**: HTTP request logging only in DEBUG mode

## Exported Metrics

All metrics are exported as Prometheus counters with absolute daily values:

1. **gplay_daily_device_installs_total{package,country}**
   - Daily device installs by country from Google Play Console
   - **CSV Column**: "Daily Device Installs"
   - Type: Counter (absolute daily value)

2. **gplay_daily_device_uninstalls_total{package,country}**
   - Daily device uninstalls by country from Google Play Console
   - **CSV Column**: "Daily Device Uninstalls"
   - Type: Counter (absolute daily value)

3. **gplay_active_device_installs_total{package,country}**
   - Active device installs by country from Google Play Console
   - **CSV Column**: "Active Device Installs"
   - Type: Counter (absolute daily value)

4. **gplay_daily_user_installs_total{package,country}**
   - Daily user installs by country from Google Play Console
   - **CSV Column**: "Daily User Installs"
   - Type: Counter (absolute daily value)

5. **gplay_daily_user_uninstalls_total{package,country}**
   - Daily user uninstalls by country from Google Play Console
   - **CSV Column**: "Daily User Uninstalls"
   - Type: Counter (absolute daily value)

## Why Counters Instead of Gauges?

The exporter uses Prometheus Counters despite setting absolute values because:
- Google Play Console provides cumulative daily statistics that reset each day
- The metrics represent cumulative counts for each date (installs, uninstalls, etc.)
- Counters are recreated on each collection cycle to handle date transitions
- This approach maintains the semantic meaning of the metrics as cumulative values

## Data Source & Processing

- **Source**: Google Cloud Storage bucket containing Play Console CSV exports
- **File Pattern**: `stats/installs/installs_<package>_<YYYYMM>_country.csv`
- **Processing**: Only processes rows with the latest date found in each CSV
- **Column Mapping**: Automatically handles different column name variations
- **Encoding Support**: Handles UTF-16, UTF-8 and other common encodings

## Architecture

- **Background Collection**: Periodic collection runs in background thread
- **Fresh Counters**: Creates new counter instances for each collection cycle
- **HTTP Server**: Exposes metrics on `/metrics` endpoint (Prometheus format)
- **Health Check**: Provides `/healthz` endpoint returning 200 (ok) or 503 (not ok)
- **Thread Safety**: Uses locks to ensure thread-safe registry updates

## Environment Variables

### REQUIRED:
- **GPLAY_EXPORTER_GOOGLE_APPLICATION_CREDENTIALS**: Path to Google service account credentials JSON file
- **GPLAY_EXPORTER_BUCKET_ID**: Google Cloud Storage bucket ID containing Play Console CSVs
  - Example: "pubsite_prod_rev_01234567890987654321"

### OPTIONAL:
- **GPLAY_EXPORTER_PORT**: HTTP server port (default: 8000)
- **GPLAY_EXPORTER_COLLECTION_INTERVAL_SECONDS**: Metrics collection interval in seconds (default: 43200 = 12 hours)
- **GPLAY_EXPORTER_GCS_PROJECT**: Google Cloud project ID (optional, uses default project from credentials)
- **GPLAY_EXPORTER_TEST_MODE**: If set, runs one collection cycle and exits (values: "1", "true", etc.)
- **GPLAY_EXPORTER_LOG_LEVEL**: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL - default: INFO)

## Health Check

The `/healthz` endpoint provides simple health status:
- **200 OK**: Returns "ok" - Service is healthy (at least one successful collection)
- **503 Service Unavailable**: Returns "not ok" - Service is unhealthy (no successful collections yet)

The service becomes healthy after the first successful collection and remains healthy even if
subsequent collections fail (as cached metrics are still being served).

## Performance Characteristics

- **Memory Efficient**: Processes CSV files in memory with streaming
- **Debug Optimization**: Expensive debug calculations only performed when LOG_LEVEL=DEBUG
- **Set-based Operations**: Uses efficient set operations for date parsing and deduplication
- **Single Pass Processing**: Extracts all five metrics in a single pass through CSV data

## Error Handling

- **Graceful Degradation**: Continues processing other packages if one fails
- **Comprehensive Logging**: Detailed debug logs available when needed
- **Input Validation**: Handles malformed CSV data and missing columns
- **Authentication Errors**: Proper error messages for credential issues
- **Health Status**: Tracks collection success for monitoring

## Usage Examples

1. **Production Deployment**:
   ```bash
   GPLAY_EXPORTER_GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json \
   GPLAY_EXPORTER_BUCKET_ID=pubsite_prod_123456789 \
   python exporter.py
   ```

2. **Debug Mode**:
   ```bash
   GPLAY_EXPORTER_LOG_LEVEL=DEBUG \
   GPLAY_EXPORTER_GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json \
   GPLAY_EXPORTER_BUCKET_ID=pubsite_prod_123456789 \
   GPLAY_EXPORTER_TEST_MODE=1 \
   python exporter.py
   ```

3. **Custom Configuration**:
   ```bash
   GPLAY_EXPORTER_PORT=9090 \
   GPLAY_EXPORTER_COLLECTION_INTERVAL_SECONDS=21600 \
   GPLAY_EXPORTER_GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json \
   GPLAY_EXPORTER_BUCKET_ID=pubsite_prod_123456789 \
   python exporter.py
   ```

## Docker Usage

The exporter includes a Dockerfile with:
- Alpine-based Python 3.13 image
- Non-root user execution (uid/gid 1000)

Build and run:
```bash
docker build -t gplay-exporter .
docker run -e GPLAY_EXPORTER_GOOGLE_APPLICATION_CREDENTIALS=/creds/key.json \
           -e GPLAY_EXPORTER_BUCKET_ID=pubsite_prod_123456789 \
           -v /path/to/creds:/creds:ro \
           -p 8000:8000 \
           gplay-exporter
```

## Testing

Run the included test suite to validate functionality:
```bash
python test_local.py
```

The test suite validates:
- Environment variable handling
- Counter creation and value setting
- Health check logic
- Date parsing (multiple formats)
- Number extraction (with commas and decimals)
- WSGI endpoints (/metrics, /healthz, 404 handling)
- Metric export functionality

## Monitoring & Alerting

The exporter provides Prometheus metrics that can be used for:
- Data freshness monitoring (track when metrics stop updating)
- Package discovery status (number of packages found)
- Collection cycle timing (via logs)
- Service health (via /healthz endpoint)

Example Prometheus queries:
```promql
# Total installs per package
sum by (package) (gplay_daily_device_installs_total)

# Uninstall rate by country
gplay_daily_device_uninstalls_total / gplay_daily_device_installs_total

# Active installs trend
rate(gplay_active_device_installs_total[7d])
```

## CSV Column Support

Automatically handles these column names:
- Date: "Date"
- Country: "Country"
- Daily Device Installs: "Daily Device Installs"
- Daily Device Uninstalls: "Daily Device Uninstalls"
- Active Device Installs: "Active Device Installs"
- Daily User Installs: "Daily User Installs"
- Daily User Uninstalls: "Daily User Uninstalls"

## Date Format Support

Parses multiple date formats:
- YYYY-MM-DD (2025-01-15)
- DD-MMM-YYYY (15-Jan-2025)
- MM/DD/YYYY (01/15/2025)

## Changelog

### Version 1.1.0 (2025-01-24)

#### Breaking Changes
- **Environment Variables**: Removed support for old unprefixed environment variable names. All environment variables now require the `GPLAY_EXPORTER_` prefix.

#### Improvements
- **Code Documentation**: Added comprehensive docstrings and inline comments throughout the codebase
- **Health Check Simplified**: `/healthz` endpoint now returns simple "ok"/"not ok" text instead of JSON
- **Logging Optimization**: HTTP request logs now only appear in DEBUG mode
- **C Extension Support**: Added build dependencies in Dockerfile to compile google-crc32c C extension for better performance
- **Thread Safety**: Improved thread safety with proper locking mechanisms
- **Test Coverage**: Test script added to ensure code quality and reliability