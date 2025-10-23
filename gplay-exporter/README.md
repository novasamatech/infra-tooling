# Google Play Console Metrics Exporter

This exporter extracts and exposes metrics from Google Play Console CSV reports
as Prometheus gauge metrics. It automatically discovers packages, processes CSV files for configurable time periods, and exports metrics with proper timestamps for each individual date.

## Exported Metrics

All metrics are exported as Prometheus gauges with proper timestamps. Each date in the CSV reports generates a separate metric entry with the corresponding timestamp:

1. **gplay_device_installs_v3{package,country}**
   - Daily device installs for a specific date
   - Type: Gauge with timestamp

2. **gplay_device_uninstalls_v3{package,country}**
   - Daily device uninstalls for a specific date
   - Type: Gauge with timestamp

3. **gplay_active_device_installs_v3{package,country}**
   - Active device installations on a specific date
   - Type: Gauge with timestamp

4. **gplay_user_installs_v3{package,country}**
   - Daily user installs for a specific date
   - Type: Gauge with timestamp

5. **gplay_user_uninstalls_v3{package,country}**
   - Daily user uninstalls for a specific date
   - Type: Gauge with timestamp

## How It Works

### Data Processing Flow

1. **File Discovery**: Finds monthly CSV files for each package based on the configured lookback period (e.g., `installs_com.app_202501_country.csv`)

2. **Date-Specific Metrics**: 
   - Each row in the CSV (representing a specific date) becomes a separate gauge metric
   - No aggregation is performed - each date's data is preserved individually
   - Metrics include the proper timestamp for their specific date

3. **Timestamp Assignment**: 
   - Each date in the CSV gets its own millisecond timestamp
   - This timestamp is applied to the metric for that specific date
   - Ensures accurate time-series data in Prometheus

4. **Storage Refresh**: 
   - Metrics are completely cleared and refreshed on each collection cycle
   - Prevents infinite accumulation of historical data
   - Keeps only the data from the configured lookback period

### Example Output

```prometheus
# HELP gplay_device_installs_v3 Device installs by country and date from Google Play Console
# TYPE gplay_device_installs_v3 gauge
gplay_device_installs_v3{package="com.example.app",country="US"} 1250.0 1737676800000
gplay_device_installs_v3{package="com.example.app",country="US"} 1300.0 1737763200000
gplay_device_installs_v3{package="com.example.app",country="GB"} 567.0 1737676800000

# HELP gplay_active_device_installs_v3 Active device installs by country and date from Google Play Console
# TYPE gplay_active_device_installs_v3 gauge
gplay_active_device_installs_v3{package="com.example.app",country="US"} 850000.0 1737676800000
gplay_active_device_installs_v3{package="com.example.app",country="US"} 851300.0 1737763200000
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
- **GPLAY_EXPORTER_MONTHS_LOOKBACK**: Number of months to look back for reports (default: 1)
- **GPLAY_EXPORTER_GCS_PROJECT**: Google Cloud project ID (optional)
- **GPLAY_EXPORTER_TEST_MODE**: Run single collection and exit
- **GPLAY_EXPORTER_LOG_LEVEL**: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

## Usage

### Production Deployment
```bash
GPLAY_EXPORTER_GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json \
GPLAY_EXPORTER_BUCKET_ID=pubsite_prod_123456789 \
GPLAY_EXPORTER_MONTHS_LOOKBACK=2 \
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
           -e GPLAY_EXPORTER_MONTHS_LOOKBACK=3 \
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
    # CRITICAL: Must be true for correct timestamp handling
    honor_timestamps: true
```

### ⚠️ CRITICAL: Honor Timestamps Configuration
**Your Prometheus configuration MUST include `honor_timestamps: true`!** 

Without this setting:
- Metrics will use the scrape time instead of the actual data date
- Graphs will show incorrect fluctuations throughout the day
- Daily metrics will appear to change every scrape interval
- Historical data points will be incorrectly positioned in time

With `honor_timestamps: true`:
- Each metric uses its exact date timestamp (midnight UTC)
- Daily data appears as single points per day
- Historical trends are accurately represented

## Data Model

### Individual Date Metrics
In v3, each date in the CSV reports generates its own metric entry:
- No aggregation across dates
- Each metric represents the exact value for that specific date
- Timestamps correspond to the actual date of the data

### Storage Refresh
The exporter completely refreshes its metric storage on each collection cycle:
- Old metrics are cleared before new ones are added
- Only metrics from the configured lookback period are retained
- Prevents unbounded memory growth

### Months Lookback
The `GPLAY_EXPORTER_MONTHS_LOOKBACK` variable controls how far back to look for reports:
- Default: 1 (current month only)
- Setting to 3 would process the current month and 2 previous months
- Each month's CSV file is processed independently

## Testing

Run the test suite to validate functionality:
```bash
python test_local.py
python test_integration.py
```

The test suite validates:
- Gauge metric type for all metrics
- Date-specific metric entries
- Proper timestamp assignment per date
- Metric storage refresh on collection
- Months lookback functionality
- Health check functionality
- Date parsing and number extraction
- Zero value filtering

## Monitoring

### Example Prometheus Queries

```promql
# View all device installs for the latest available dates
gplay_device_installs_v3

# Current active installations by date
gplay_active_device_installs_v3

# Device installs for a specific package over time
gplay_device_installs_v3{package="com.example.app"}

# Uninstall rate by country (for specific timestamps)
gplay_device_uninstalls_v3 / gplay_device_installs_v3
```

### Data Freshness Monitoring
Since metrics include explicit timestamps, you can monitor data freshness:
```promql
# Alert if data is older than 48 hours
time() - timestamp(gplay_device_installs_v3) > 172800
```

## Troubleshooting

### Many duplicate-looking metrics
- **Cause**: Each date generates its own metric entry
- **Solution**: This is expected behavior in v3; use Prometheus queries to aggregate if needed

### Metrics disappear after collection
- **Cause**: Storage is refreshed on each collection cycle
- **Solution**: This is by design; adjust MONTHS_LOOKBACK if you need more historical data

### Missing historical data
- **Cause**: MONTHS_LOOKBACK is set too low
- **Solution**: Increase MONTHS_LOOKBACK to include desired historical period

### Prometheus not using timestamps
- **Cause**: Missing `honor_timestamps: true` in scrape config
- **Solution**: Update Prometheus configuration and reload

## Technical Details

### Why Gauge Metrics?

In v3, all metrics are gauges because:
- Each metric represents a point-in-time value for a specific date
- No aggregation is performed - raw daily values are preserved
- Allows for more flexible querying in Prometheus

### Storage Refresh Strategy

The complete refresh approach ensures:
- No infinite accumulation of historical metrics
- Predictable memory usage
- Fresh data on each collection cycle
- Simpler logic without complex state management

### CSV Data Source

- **Source**: Google Cloud Storage bucket with Play Console exports
- **File Pattern**: `stats/installs/installs_<package>_<YYYYMM>_country.csv`
- **Processing**: Each row becomes an individual gauge metric
- **Encoding**: Handles UTF-16, UTF-8 and other common encodings

## Changelog

### Version 3.0.1 (2025-01-25)

#### Fixes
- Use UTC for timestamps' generation

### Version 3.0.0 (2025-01-24)

#### Major Changes
- **All Metrics Are Now Gauges**: Changed from counter to gauge type for all metrics
- **Date-Specific Metrics**: Each date in CSV reports generates its own metric entry with proper timestamp
- **No Aggregation**: Removed sum/last value logic - each metric preserves its exact daily value
- **Storage Refresh**: Metrics are completely cleared and refreshed on each collection cycle
- **Configurable Lookback**: Added `GPLAY_EXPORTER_MONTHS_LOOKBACK` environment variable (default: 1)
- **Metric Name Change**: All metrics now use `_v3` suffix

#### Breaking Changes
- Metric type changed from counter to gauge
- Aggregation logic removed (no more summing or last-value selection)
- Metric storage no longer accumulates indefinitely
- Different data model requiring query adjustments

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

### Version 1.1.2 (2025-01-24)

#### Improvements
- Zero value metrics were exported from export

### Version 1.1.1 (2025-01-24)

#### Improvements
- Dockerfile was updated
- Tests were updated

### Version 1.1.0 (2025-01-24)

#### Breaking Changes
- **Environment Variables**: Removed support for old unprefixed environment variable names. All variables must now use the `GPLAY_EXPORTER_` prefix

#### Improvements
- **Health Check**: Simplified `/healthz` endpoint
- **Logging**: HTTP request logs now only appear in DEBUG mode
- **Documentation**: Added extensive code comments and docstrings
- **Docker**: Fixed google-crc32c warning
- **Performance**: Optimized Dockerfile layer ordering
- **Metrics Export**: Zero-value metrics are no longer exported

### Version 1.0.0 (Initial Release)
- Initial implementation with Google Play Console metrics export
- Support for 5 metric types from CSV files
- Automatic package discovery from GCS bucket
- Background collection with configurable intervals
- Prometheus metrics endpoint
- Basic health check endpoint

## License

This project is licensed under the [Apache License 2.0](../LICENSE).