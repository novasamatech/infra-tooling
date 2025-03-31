# Cloudflare Exporter for Prometheus

This service exports metrics from Cloudflare to Prometheus. It queries Cloudflare’s GraphQL and REST APIs to retrieve information about HTTP requests for each zone (since midnight UTC), and exposes these as Prometheus metrics.

## How It Works

1. On startup, the exporter will:  
   • Fetch a list of all zones configured in your Cloudflare account (via the REST API).  
   • For each zone, it will perform two queries against the Cloudflare GraphQL Analytics API:  
     – One to retrieve visit counts (specifically for requests to the path “/”).  
     – One to retrieve total request counts (for all paths).  
   • The corresponding metrics are labeled with details such as zone name, host, client country, user agent, etc.  

2. The exporter repeats this process on a configurable interval. Metrics are served via a built-in HTTP server on the specified port.

## Prometheus Metrics

Two primary metrics are exposed:

1. cf_visits:  
   – Labels: zone_name, host_name, client_country_name, client_request_referer, user_agent_browser, user_agent_os  
   – Description: Total visits since midnight (UTC) specifically for requests to path “/”.  
   – Type: Gauge (the exporter sets the count at each scrape).

2. cf_requests:  
   – Labels: zone_name, host_name, method_name, path, query, client_country_name, client_request_referer, user_agent_browser, user_agent_os, cache_status, origin_response_status  
   – Description: Total HTTP requests since midnight (UTC) for all paths.  
   – Type: Gauge (the exporter sets the count at each scrape).

Both metrics are reset and recalculated on each scrape cycle.

## Environment Variables

Configure the exporter by setting these environment variables:

• CF_EXPORTER_API_TOKEN  
  – The Cloudflare API token with the necessary permissions.  
  – Example: CF_EXPORTER_API_TOKEN="12345abcde"  

• CF_EXPORTER_REQUEST_TIMEOUT  
  – The request timeout (in seconds) for HTTP calls to the Cloudflare API.  
  – Defaults to "30".  
  – Example: CF_EXPORTER_REQUEST_TIMEOUT="60"  

• CF_EXPORTER_SCRAPE_INTERVAL  
  – The interval (in seconds) at which the exporter will gather metrics.  
  – Defaults to "300".  
  – Example: CF_EXPORTER_SCRAPE_INTERVAL="600"  

• CF_EXPORTER_METRICS_PORT  
  – The port on which the Prometheus metrics are exposed.  
  – Defaults to "8000".  
  – Example: CF_EXPORTER_METRICS_PORT="9000"  

• CF_EXPORTER_LOGLEVEL  
  – The logging verbosity. Possible values are DEBUG, INFO, WARNING, ERROR, CRITICAL.  
  – Defaults to INFO.  
  – Example: CF_EXPORTER_LOGLEVEL="DEBUG"  

## Usage with Docker

Below are some examples of how you can run the exporter using Docker.

### Example 1 (minimal)
Run with the default configuration (scrape interval: 300s, port: 8000, request timeout: 30s):
  
```bash
docker run -d \
  -p 8000:8000 \
  --name cf-exporter \
  -e CF_EXPORTER_API_TOKEN="YOUR_CLOUDFLARE_API_TOKEN" \
  ghcr.io/novasamatech/infra-tooling/cf-exporter:latest
```

### Example 2
Run with a custom request timeout and scrape interval:
  
```bash
docker run -d \
  -p 8000:8000 \
  --name cf-exporter \
  -e CF_EXPORTER_API_TOKEN="YOUR_CLOUDFLARE_API_TOKEN" \
  -e CF_EXPORTER_REQUEST_TIMEOUT="60" \
  -e CF_EXPORTER_SCRAPE_INTERVAL="600" \
  -e CF_EXPORTER_METRICS_PORT="8000" \
  -e CF_EXPORTER_LOGLEVEL="DEBUG" \
  ghcr.io/novasamatech/infra-tooling/cf-exporter:latest
```

In this second example:  
• The request timeout is set to 60 seconds (instead of 30).  
• The exporter runs every 600 seconds (10 minutes).  
• Metrics are served on port 8000.  
• Logging verbosity is set to DEBUG.

Adjust the hostname and port according to your own setup.

## License

This project is licensed under the [Apache License 2.0](../LICENSE).
