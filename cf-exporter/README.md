# Cloudflare Exporter for Prometheus

This service exports Cloudflare HTTP request analytics through a Prometheus HTTP endpoint.

## How It Works

On every cycle the exporter lists all zones the API token can access using REST `GET /zones`. For each zone it makes two GraphQL HTTP requests: one reads the plan's available fields and page size from `settings.httpRequestsAdaptiveGroups`, and one aliased query retrieves both visits and requests from `httpRequestsAdaptiveGroups`.

Every zone uses the same time window: midnight UTC to the start of the cycle. The exporter publishes the complete snapshot after collection, so a scrape never sees a half-finished cycle. Series absent from the new snapshot disappear, including series belonging to a zone that failed to collect.

API failures are logged and reported through health metrics. HTTP 429 and transient 5xx responses are retried with backoff, and collection continues after a failed cycle. SIGTERM and SIGINT exit cleanly.

## Prometheus Metrics

### Analytics

| Metric | Type | Description |
| --- | --- | --- |
| `cf_visits` | Gauge | Cloudflare's `sum.visits` since midnight UTC, for all paths |
| `cf_requests` | Gauge | Cloudflare's `count` since midnight UTC, for all paths |

`cf_visits` labels: `zone_name`, `host_name`, `path`, `client_country_name`, `client_request_referer`, `user_agent_browser`, `user_agent_os`.

`cf_requests` labels: `zone_name`, `host_name`, `method_name`, `path`, `query`, `client_country_name`, `client_request_referer`, `user_agent_browser`, `user_agent_os`, `cache_status`, `origin_response_status`.

- Only groups with nonzero values are exported.
- An empty referer is reported as `direct`.
- Dimensions unavailable on the zone's plan are reported as `unknown`. Free plans currently lack `query` and `client_request_referer`.
- Each aliased dataset selection requests up to the smaller of 10000 groups and the zone's `maxPageSize`. Groups are ordered largest first; reaching the limit logs a warning that additional groups may have been omitted.
- An absent or malformed analytics result is reported as a failure, rather than an empty successful collection.

### Exporter Health

| Metric | Labels | Description |
| --- | --- | --- |
| `cf_exporter_zone_scrape_success` | `zone_name` | 1 if the last collection of the zone succeeded, 0 otherwise |
| `cf_exporter_last_cycle_success` | None | 1 if zone listing and every zone succeeded in the last cycle |
| `cf_exporter_last_cycle_timestamp_seconds` | None | Unix time when the last cycle finished |
| `cf_exporter_cycle_duration_seconds` | None | Duration of the last cycle |

Cycle metrics appear after the first cycle finishes. Example alert conditions for the default 300-second interval:

```promql
cf_exporter_last_cycle_success == 0
time() - cf_exporter_last_cycle_timestamp_seconds > 900
```

## Configuration

Create an API token with **Zone / Zone / Read** and **Zone / Analytics / Read** permissions for the zones to export.

| Environment Variable | Default | Accepted Values |
| --- | --- | --- |
| `CF_EXPORTER_API_TOKEN` | Required | API token; surrounding whitespace is stripped, embedded whitespace and non-printable or non-ASCII characters are rejected |
| `CF_EXPORTER_REQUEST_TIMEOUT` | `30` | Positive integer, HTTP timeout in seconds |
| `CF_EXPORTER_SCRAPE_INTERVAL` | `300` | Positive integer, collection interval in seconds |
| `CF_EXPORTER_METRICS_PORT` | `8000` | Integer from 1 to 65535 |
| `CF_EXPORTER_LOGLEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |

Invalid numeric settings or a missing/malformed token cause startup to fail. An unknown log level falls back to INFO with a warning. Logs and configuration representations redact the token.

Supply the token through the environment at execution time. For an interactive Bash session:

```bash
read -r -s -p 'Cloudflare API token: ' CF_EXPORTER_API_TOKEN
export CF_EXPORTER_API_TOKEN
```

## Containers

`make image` builds `ghcr.io/novasamatech/infra-tooling/cf-exporter` with the tag in `.version` using Podman. Override `CONTAINER_ENGINE=docker` to use Docker.

```bash
make image
make image-run PORT=8000
make test-image PORT=8000 SMOKE_TIMEOUT=120
```

`image-run` runs in the foreground. Both run targets send the token to the container through stdin and export it inside the running process, so it is absent from container metadata and command-line arguments. The smoke test reads the complete `/metrics` response, checks container metadata, waits for a successful cycle, and removes the container on exit.

To run a published image with Docker:

```bash
printf '%s\n' "$CF_EXPORTER_API_TOKEN" | docker run --rm -i \
  -p 8000:8000 \
  ghcr.io/novasamatech/infra-tooling/cf-exporter:latest \
  /bin/sh -c 'IFS= read -r CF_EXPORTER_API_TOKEN; export CF_EXPORTER_API_TOKEN; exec python script.py'
```

Other settings can be passed with Docker's `-e`, for example `-e CF_EXPORTER_SCRAPE_INTERVAL=600`.

## Development

All routine tasks go through the [Makefile](Makefile); `make help` lists them. Two local virtualenvs live next to the script and rebuild automatically when their lock files change:

| Directory | Dependencies | Used By |
| --- | --- | --- |
| `venv/` | `requirements.txt` | `make run` |
| `venv-dev/` | `requirements-dev.txt` | Tests, lint, formatting |

Use Python 3.13 or later to create the virtualenvs, matching the container's Python 3.13 runtime.

```bash
make test                 # offline tests with coverage
make lint                 # ruff lint and format checks
make fmt                  # automatic formatting and lint fixes
make coverage             # offline tests and HTML coverage report
make test-live            # real Cloudflare API; requires token in the environment
make test-all             # offline and live tests
make run                  # exporter from the runtime virtualenv
make lock                 # resolve requirements*.in into pinned requirements*.txt
make outdated             # list dependencies with newer releases
make clean                # remove virtualenvs, caches and coverage output
```

Direct dependencies are declared in `requirements.in` and `requirements-dev.in`. Regenerate the pinned files with `make lock`; temporary lock files and the resolver virtualenv are removed even if resolution fails.

Offline tests mock external HTTP requests. Retry tests use a local HTTP server to exercise the actual transport and backoff. Live tests check that the queried GraphQL dataset, settings, dimensions and sort values exist and are not deprecated, collect every accessible zone (including Free zones), and run the exporter as a process through SIGTERM. Process logs stay in memory. Pytest skips live tests without `CF_EXPORTER_API_TOKEN`; the explicit Makefile live targets require it.

## License

This project is licensed under the [Apache License 2.0](../LICENSE).
