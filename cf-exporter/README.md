This service exports metrics from Cloudflare to Prometheus. It queries Cloudflare’s GraphQL and REST APIs to retrieve information about HTTP requests by zone, including total visit counts since midnight (UTC), and writes them to a Prometheus endpoint.

## How It Works

- On startup, the exporter will:
  1. Fetch a list of all zones configured in your Cloudflare account (paginated).
  2. For each zone, it will query the Cloudflare GraphQL Analytics API to retrieve HTTP request information, specifically for the path “/” since midnight UTC on the current date.
  3. Labels such as zone name, host, client country, etc., will be populated, and the “cloudflare_visits” gauge metric will be updated accordingly.  

- The exporter repeats this process on a configurable interval. Metrics are served via a built-in HTTP server on the specified port.

## Environment Variables

The following environment variables can be set to configure the exporter:

- CF_EXPORTER_API_TOKEN  
  The Cloudflare API token with the necessary permissions.  
  Example: CF_EXPORTER_API_TOKEN="12345abcde"

- CF_EXPORTER_REQUEST_TIMEOUT  
  The timeout (in seconds) for HTTP requests to the Cloudflare API. Defaults to "30".  
  Example: CF_EXPORTER_REQUEST_TIMEOUT="60"

- CF_EXPORTER_SCRAPE_INTERVAL  
  The interval (in seconds) at which the exporter gathers metrics. Defaults to "300".  
  Example: CF_EXPORTER_SCRAPE_INTERVAL="600"

- CF_EXPORTER_METRICS_PORT  
  The port on which the Prometheus metrics are exposed. Defaults to "8000".  
  Example: CF_EXPORTER_METRICS_PORT="9000"

- CF_EXPORTER_LOGLEVEL  
  The logging verbosity. Possible values are DEBUG, INFO, WARNING, ERROR, CRITICAL. Defaults to INFO.  
  Example: CF_EXPORTER_LOGLEVEL="DEBUG"


## Usage with Docker

Below are some examples of how you can run the exporter using Docker. Replace "YOUR_DOCKER_IMAGE_PATH" with the actual path to your Docker image.

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

Run with custom request timeout and scrape interval:

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
• The request timeout is set to 60 seconds instead of 30.  
• The exporter runs every 600 seconds (10 minutes).  
• Metrics are served on port 8000.  
• Logging verbosity is set to DEBUG.

Adjust the hostname and port according to your own setup.

## License

This project is licensed under the [Apache License 2.0](../LICENSE).