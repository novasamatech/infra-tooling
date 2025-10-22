# Google Play Console Metrics Exporter

This exporter extracts and exposes multiple metrics from Google Play Console CSV reports
as Prometheus counters. It automatically discovers packages, processes CSV files, and exports metrics for monitoring and alerting.

## Exported Metrics

All metrics are exported as Prometheus counters with proper timestamps:

1. **gplay_device_installs_v2{package,country}**
   - Sum of device installs for the month
   - Aggregation: SUM of all days
   - Type: Counter with timestamp

2. **gplay_device_uninstalls_v2{package,country}**
   - Sum of device uninstalls for the month
   - Aggregation: SUM of all days
   - Type: Counter with timestamp

3. **gplay_active_device_installs_v2{package,country}**
   - Current active device installs (absolute value)
   - Aggregation: LAST value from the latest date
   - Type: Counter with timestamp

4. **gplay_user_installs_v2{package,country}**
   - Sum of user installs for the month
   - Aggregation: SUM of all days
   - Type: Counter with timestamp

5. **gplay_user_uninstalls_v2{package,country}**
   - Sum of user uninstalls for the month
   - Aggregation: SUM of all days
   - Type: Counter with timestamp

## How It Works

### Data Processing Flow

1. **File Discovery**: Finds the latest monthly CSV file for each package (e.g., `installs_com.app_202501_country.csv`)

2. **Data Aggregation**: 
   - For **daily metrics** (installs, uninstalls): Sums values across all dates in the file
   - For **absolute metrics** (active_device_installs): Takes only the value from the latest date
   - This ensures correct representation of both flow and stock metrics

3. **Timestamp Assignment**: 
   - Identifies the maximum (latest) date in the file
   - Converts this date to milliseconds timestamp
   - Applies this timestamp to all metrics from that file

4. **Format Generation**: 
   - Manually creates Prometheus text format
   - Includes timestamp on the same line as the metric value
   - Filters out zero values to avoid empty series

### Example Output

```prometheus
# HELP gplay_device_installs_v2 Device installs by country from Google Play Console
# TYPE gplay_device_installs_v2 counter
gplay_device_installs_v2{package="com.example.app",country="US"} 12450.0 1737734400000
gplay_device_installs_v2{package="com.example.app",country="GB"} 5678.0 1737734400000

# HELP gplay_active_device_installs_v2 Active device installs by country from Google Play Console
# TYPE gplay_active_device_installs_v2 counter
gplay_active_device_installs_v2{package="com.example.app",country="US"} 850000.0 1737734400000
```

## Dependencies

* google-cloud-storage

## Environment Variables

### REQUIRED:
- **GPLAY_EXPORTER_GOOGLE_APPLICATION_CREDENTIALS**: Path to Google service account credentials JSON file
- **GPLAY_EXPORTER_BUCKET_ID**: Google Cloud Storage bucket ID containing Play Console CSVs

### OPTIONAL:
- **GPLAY_EXPORTER_PORT**: HTTP server port (default: 8000)
- **GPLAY_EXPORTER_COLLECTION_INTERVAL_SECONDS**: Metrics collection interval (default: 43200 = 12 hours)
- **GPLAY_EXPORTER_GCS_PROJECT**: Google Cloud project ID (optional)
- **GPLAY_EXPORTER_TEST_MODE**: Run single collection and exit
- **GPLAY_EXPORTER_LOG_LEVEL**: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

## Usage

### Production Deployment
```bash
GPLAY_EXPORTER_GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json \
GPLAY_EXPORTER_BUCKET_ID=pubsite_prod_123456789 \
python exporter.py
```

### Test Mode
```bash
GPLAY_EXPORTER_LOG_LEVEL=DEBUG \
GPLAY_EXPORTER_GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json \
GPLAY_EXPORTER_BUCKET_ID=pubsite_prod_123456789 \
GPLAY_EXPORTER_TEST_MODE=1 \
python exporter.py
```

### Docker Usage
```bash
docker build -f Dockerfile -t gplay-exporter .
docker run -e GPLAY_EXPORTER_GOOGLE_APPLICATION_CREDENTIALS=/creds/key.json \
           -e GPLAY_EXPORTER_BUCKET_ID=pubsite_prod_123456789 \
           -v /path/to/creds:/creds:ro \
           -p 8000:8000 \
           gplay-exporter
```

## Prometheus Configuration

### Scrape Configuration
```yaml
scrape_configs:
  - job_name: 'gplay-exporter'
    static_configs:
      - targets: ['exporter-host:8000']
    # Honor timestamps from the exporter
    honor_timestamps: true
```

### Important: Honor Timestamps
Make sure your Prometheus configuration includes `honor_timestamps: true` to use the timestamps provided by the exporter. This ensures metrics are stored with the correct time from the Play Console reports, not the scrape time.

## Aggregation Strategy

The exporter uses different aggregation strategies based on the metric type:

### Sum Aggregation
Used for flow metrics that represent events over time:
- `gplay_device_installs_v2` - each day adds to the monthly total
- `gplay_device_uninstalls_v2` - each day adds to the monthly total  
- `gplay_user_installs_v2` - each day adds to the monthly total
- `gplay_user_uninstalls_v2` - each day adds to the monthly total

### Last Value Aggregation
Used for stock metrics that represent current state:
- `gplay_active_device_installs_v2` - represents the current number of active installations

This approach ensures that:
- Daily statistics are properly accumulated for the entire month
- Absolute values (like active installs) aren't incorrectly summed

## Testing

Run the test suite to validate functionality:
```bash
python test_local.py
python test_integration.py
```

The test suite validates:
- Prometheus format generation with timestamps
- Correct aggregation strategies (sum vs last value)
- Timestamp selection (latest date)
- Health check functionality
- Date parsing and number extraction
- Zero value filtering

## Monitoring

### Example Prometheus Queries

```promql
# Total installs for the current month
gplay_device_installs_v2

# Current active installations
gplay_active_device_installs_v2

# Uninstall rate by country
gplay_device_uninstalls_v2 / gplay_device_installs_v2

# Growth in active installs over time
increase(gplay_active_device_installs_v2[30d])
```

### Data Freshness Monitoring
Since metrics include explicit timestamps, you can monitor data freshness:
```promql
# Alert if data is older than 48 hours
time() - timestamp(gplay_device_installs_v2) > 172800
```

## Troubleshooting

### Metrics appear with old timestamps
- **Cause**: Play Console reports may be delayed
- **Solution**: This is expected behavior; timestamp reflects the actual data date

### Active installs value seems low
- **Cause**: correctly uses only the latest value, not a sum
- **Solution**: This is correct behavior for absolute metrics

### Daily metrics values are high
- **Cause**: sums all days in the month for flow metrics
- **Solution**: This is correct behavior; values represent monthly totals

### Prometheus not using timestamps
- **Cause**: Missing `honor_timestamps: true` in scrape config
- **Solution**: Update Prometheus configuration and reload

## Technical Details

### Why Different Aggregation Strategies?

**Flow Metrics** (installs/uninstalls per day):
- Represent events that occur over time
- Should be summed to get total monthly activity
- Example: 100 installs on day 1 + 150 on day 2 = 250 total installs

**Stock Metrics** (active installations):
- Represent current state at a point in time
- Should use only the latest value
- Example: 1000 active on day 1, 1100 active on day 2 = current value is 1100 (not 2100)

### Manual Format Generation

The prometheus_client Python library has a limitation where it cannot set timestamps in the standard Prometheus format. By generating the format manually, we achieve:
- Correct Prometheus text exposition format
- Proper inline timestamp support
- Full control over metric output

### CSV Data Source

- **Source**: Google Cloud Storage bucket with Play Console exports
- **File Pattern**: `stats/installs/installs_<package>_<YYYYMM>_country.csv`
- **Processing**: Aggregates data based on metric type
- **Encoding**: Handles UTF-16, UTF-8 and other common encodings

## Changelog

### Version 2.1.0 (2025-01-24)

#### Changes
- **Added _v2 suffix to all metric names** to clearly distinguish from v1 metrics:
  - `gplay_device_installs_v2`
  - `gplay_device_uninstalls_v2`
  - `gplay_active_device_installs_v2`
  - `gplay_user_installs_v2`
  - `gplay_user_uninstalls_v2`
- This allows running v1 and v2 exporters in parallel during migration

### Version 2.0.0 (2025-01-24)

#### Major Changes
- **Timestamp Support**: Manually generates Prometheus text format to support proper inline timestamps (milliseconds)
- **Intelligent Aggregation**: 
  - Daily metrics (installs/uninstalls) are summed across all days in monthly report
  - Absolute metrics (active_device_installs) use only the latest value
  - Fixes data loss issue when multiple days are added to reports at once
- **Improved Metric Names**:
  - Removed redundant "daily" prefix from v1 names
  - Removed unnecessary "total" suffix except for semantically appropriate metrics
- **No prometheus_client dependency**: Generates Prometheus format manually

#### Important Notes
- Metric values will be different from v1 (higher for daily metrics due to summing)
- Requires `honor_timestamps: true` in Prometheus scrape configuration
- Active device installs now correctly shows current value, not sum

### Version 1.1.2 (2025-01-24)

#### Improvements
- Zero value metrics were exported from export

### Version 1.1.1 (2025-01-24)

#### Improvements
- Dockerfile was updated
- Tests were updated

### Version 1.1.0 (2025-01-24)

#### Breaking Changes
- **Environment Variables**: Removed support for old unprefixed environment variable names. All variables must now use the `GPLAY_EXPORTER_` prefix:
  - `GOOGLE_APPLICATION_CREDENTIALS` → `GPLAY_EXPORTER_GOOGLE_APPLICATION_CREDENTIALS`
  - `GPLAY_BUCKET_ID` → `GPLAY_EXPORTER_BUCKET_ID`
  - `PORT` → `GPLAY_EXPORTER_PORT`
  - `COLLECTION_INTERVAL_SECONDS` → `GPLAY_EXPORTER_COLLECTION_INTERVAL_SECONDS`
  - `GCS_PROJECT` → `GPLAY_EXPORTER_GCS_PROJECT`
  - `TEST_MODE` → `GPLAY_EXPORTER_TEST_MODE`
  - `LOG_LEVEL` → `GPLAY_EXPORTER_LOG_LEVEL`

#### Improvements
- **Health Check**: Simplified `/healthz` endpoint to return only status code (200/503) and simple text response ("ok"/"not ok"). Detailed information moved to debug logs
- **Logging**: HTTP request logs now only appear in DEBUG mode via custom `QuietWSGIRequestHandler`
- **Documentation**: Added extensive code comments and docstrings in English throughout the codebase
- **Docker**: Fixed google-crc32c warning by adding build dependencies (gcc, musl-dev, python3-dev, libffi-dev) to Dockerfile for C extension compilation
- **Performance**: Optimized Dockerfile layer ordering for better caching
- **Metrics Export**: Zero-value metrics are no longer exported to prevent creating empty Prometheus series

#### Technical Details
- Metrics remain as Prometheus Counters (not Gauges) by design, as they represent cumulative daily values
- Registry is recreated on each collection cycle to handle date changes properly
- Health status becomes "healthy" after first successful collection and remains healthy even if subsequent collections fail
- Empty metric series prevention reduces memory usage in Prometheus

### Version 1.0.0 (Initial Release)
- Initial implementation with Google Play Console metrics export
- Support for 5 metric types from CSV files
- Automatic package discovery from GCS bucket
- Background collection with configurable intervals
- Prometheus metrics endpoint
- Basic health check endpoint

## License

This project is licensed under the [Apache License 2.0](../LICENSE).