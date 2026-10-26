# Cloudflare Exporter for Prometheus

This service exports Cloudflare HTTP request analytics through a Prometheus HTTP endpoint.

## How It Works

On every cycle the exporter lists all zones the API token can access using REST `GET /zones`. For each zone, a separate GraphQL HTTP request discovers the plan's available fields and page size from `settings.httpRequestsAdaptiveGroups`. An aliased analytics query retrieves visits, requests, cache requests and dimensionless zone totals from `httpRequestsAdaptiveGroups`. No host list or host filter is required.

All groups are fetched by default. A full page with a nonzero last group is retried over smaller, disjoint time windows, and values for identical label sets are summed. Only full selections are retried; a full visits page ending in zero needs no retry. Parent pages are discarded before merging their children. Slices use `datetime_geq` / `datetime_lt` and stop at 60 seconds (a trailing partial minute can be shorter). Slicing is bounded by a GraphQL budget shared by all zones of a cycle. Cloudflare documents 300 queries per 5 minutes per user, where every selection of every zone counts. A cycle plans for two thirds of that, scaled to the scrape interval: 200 selections at the default 300 seconds. Every zone keeps its baseline (settings, three datasets and totals); slicing in all zones shares the rest, and one selection may use at most 32 requests per zone and cycle. If a smallest slice, or a slice that the budget no longer allows to split, is still full, the exporter retains its page, warns and sets the truncation metric.

The number of completed slices per zone and selection is remembered for the next cycle, avoiding repeated full-day queries for large selections. The hint resets at UTC midnight. Hinted selections start with slices; zone totals always use one dimensionless full-window selection with `limit: 1`. The API's per-selection page size remains at most 10000, subject to the plan's `maxPageSize`. More than one analytics HTTP request is needed when slicing.

Every zone uses the same UTC window: midnight to the start of the cycle. An empty window at midnight sends no analytics query and publishes zero zone totals. The exporter publishes the complete snapshot after collection, so a scrape never sees a half-finished cycle. Series absent from the new snapshot disappear. Failure of any slice omits all analytics for that zone for the cycle, while other zones are preserved.

After midnight, scrapes omit yesterday's day-scoped families until a new-day snapshot is published: `cf_visits`, `cf_requests`, `cf_cache_requests`, `cf_zone_requests`, `cf_zone_visits`, `cf_exporter_groups`, `cf_exporter_group_limit_reached` and `cf_exporter_snapshot_window_end_seconds`. Zone health and cycle metrics stay available, including if a cycle started before midnight and finished afterwards. The cycle before midnight is moved earlier once a day if, judged by the previous cycle's duration, it would not finish 30 seconds before midnight. The last observed API window still ends at that cycle's **start**, so the unobserved tail includes its collection duration, the closing margin, any missed final scrape and data Cloudflare has not ingested yet. There is no final reconciliation of the previous day.

API failures are logged and reported through health metrics. HTTP 429 and transient 5xx responses are retried with backoff, and collection continues after a failed cycle. A `Retry-After` of up to 30 seconds is waited for; a longer one (Cloudflare's rate limits reset over 5 minutes) fails the request at once instead of stalling the cycle. After an HTTP 429, or a GraphQL error that reports a depleted rate-limit budget with another status, the remaining zones are skipped for that cycle and reported as failed, since their requests would be rejected too; the next cycle tries every zone again. SIGTERM and SIGINT exit cleanly.

## Prometheus Metrics

### Analytics

| Metric | Type | Description |
| --- | --- | --- |
| `cf_visits` | Gauge | Cloudflare's `sum.visits` since midnight UTC, for all paths |
| `cf_requests` | Gauge | Cloudflare's `count` since midnight UTC, for all paths |
| `cf_cache_requests` | Gauge | Request count since midnight UTC with cache dimensions, for all hosts and paths |
| `cf_zone_requests` | Gauge | Dimensionless request count since midnight UTC; label: `zone_name` |
| `cf_zone_visits` | Gauge | Dimensionless visit count since midnight UTC; label: `zone_name` |

`cf_visits` labels: `zone_name`, `host_name`, `path`, `client_country_name`, `client_request_referer`, `user_agent_browser`, `user_agent_os`.

`cf_requests` labels: `zone_name`, `host_name`, `method_name`, `path`, `query`, `client_country_name`, `client_request_referer`, `user_agent_browser`, `user_agent_os`, `cache_status`, `origin_response_status`.

`cf_cache_requests` labels: `zone_name`, `host_name`, `method_name`, `path`, `cache_status`, `origin_response_status`. Keeping `origin_response_status` explains origin behavior such as `bypass`; measured on 2026-10-25, it added only 0.3% cache groups. All hosts of every discovered zone are included dynamically.

- Only groups with nonzero values are exported.
- An empty referer is reported as `direct`.
- Dimensions unavailable on the zone's plan are reported as `unknown`. Free plans currently lack `query` and `client_request_referer`.
- `CF_EXPORTER_GROUP_LIMIT` can cap each selection per zone. Limits up to the page size use one full-window query. Larger limits use slicing and select the largest N nonzero groups after merging. Unset, empty or `0` means unlimited.
- Truncation is decided from the pages themselves, never from the zone totals, which are fetched separately and can differ from the slices for the current day. A full visits page ending in zero is complete. A configured cap requests one extra group to see whether anything was cut; a cap equal to the page size leaves no room for that, so a full page is reported. A full smallest or unsplittable slice is reported. A cut logs a warning and sets `cf_exporter_group_limit_reached`.
- An absent or malformed analytics result is reported as a failure, rather than an empty successful collection.

These are estimates for the current UTC window, not process counters. A smaller value can be a revised estimate; a new day can start with a value greater than or equal to yesterday's last value. Generic counter-reset detection cannot identify the UTC boundary reliably. Use the snapshot window contract below when interpreting changes over time.

To estimate origin traffic, count every status except `hit`, including `none`: in a check against an origin's own request counts, only all non-`hit` statuses together matched. To diagnose the day-to-date share absent from `cf_requests`:

```promql
1 - sum by (zone_name) (cf_requests) / cf_zone_requests
```

Cloudflare can still ingest recent data while sliced queries are running, so very small differences from zone totals can occur for a current-day window. Completed-day sums are checked separately. Slicing removes exporter group truncation; it does not change Cloudflare's adaptive sampling.

### Snapshot window contract

`cf_exporter_snapshot_window_end_seconds{zone_name, window_start}` describes the exact API window for a successfully collected zone:

- `window_start` is midnight UTC as an RFC3339 string with `Z`. It identifies the window independently of the exporter process. A restart during the same day keeps this identity; UTC midnight changes it even if an analytics value stays the same or increases.
- The gauge value is the exclusive window end as Unix seconds, truncated to whole seconds. Thus the interval is `[window_start, value)`. This is the cycle start passed to the API, not the time collection finished or the endpoint was scraped.
- All analytics and this metadata are rendered from one atomic snapshot, and all successful zones of a cycle share the same window. Metadata is present for successful zones even if they have no nonzero groups or the window is empty at midnight. A failed zone has neither analytics nor window metadata for that cycle; its `cf_exporter_zone_scrape_success` is `0`. Other zones remain independent. If zone listing fails, no zone has analytics, metadata or `cf_exporter_zone_scrape_success`, and `cf_exporter_last_cycle_success` is `0`.
- A successful API response can still be truncated. Check `cf_exporter_group_limit_reached` for the relevant dataset before interpreting an absent group as zero.
- Yesterday's window metadata disappears together with yesterday's analytics. An interval crossing midnight needs no observed zero to distinguish the two windows.

Day visibility is checked at the start of a scrape. A response that spans UTC midnight still describes one consistent snapshot; its window is identified by the metadata, rather than inferred from the response's arrival time.

The metadata describes values from the **same zone and the same source exposition**. When storing samples from several exporter instances, retain source identity and match the metadata to the analytics sample's scrape before combining sources. An old retained analytics sample must not acquire the window of a newer metadata sample. Overlapping windows from independent instances describe overlapping traffic and must not be added as separate traffic. A missing sample or failed collection is not a measurement of zero requests.

The exporter preserves the API estimates without converting them to a process counter, clamping revisions or accumulating overlapping windows. For example, `100` followed by `95` within one window remains `100`, then `95`; it is a downward correction, not a counter reset. A newly observed group with value `3` retains those three requests in its daily estimate. On first startup during a day, the initial values already include the observable day-to-date history; there is no information about when those requests occurred before observation. The window end, zone health and truncation metrics allow consumers to identify stale or incomplete observations without assuming a fixed scrape schedule.

Late-ingested data for the current day can revise a later snapshot, including data whose event time predates the previous window end. Data arriving for a finished day after its last observed snapshot is not recovered. No interval count or cache hit ratio reconstructed from these snapshots has event-level timing precision. Retain all six cache dimensions while interpreting changes; aggregate independent groups afterwards. All cache statuses, including `none`, belong in total-traffic denominators. An empty traffic window has no defined hit ratio.

### Exporter Health

| Metric | Labels | Description |
| --- | --- | --- |
| `cf_exporter_zone_scrape_success` | `zone_name` | 1 if the last collection of the zone succeeded, 0 otherwise |
| `cf_exporter_last_cycle_success` | None | 1 if zone listing and every zone succeeded in the last cycle |
| `cf_exporter_last_cycle_timestamp_seconds` | None | Unix time when the last cycle finished |
| `cf_exporter_cycle_duration_seconds` | None | Duration of the last cycle |
| `cf_exporter_snapshot_window_end_seconds` | `zone_name`, `window_start` | Exclusive API window end as Unix seconds; `window_start` identifies the UTC day |
| `cf_exporter_groups` | `zone_name`, `dataset` | Exported nonzero label sets; dataset is `visits`, `requests` or `cache` |
| `cf_exporter_group_limit_reached` | `zone_name`, `dataset` | 1 when the configured cap, a full smallest slice or the spent slicing budget cut data, otherwise 0 |

Cycle metrics appear after the first cycle finishes. Example alert conditions for the default 300-second interval:

```promql
cf_exporter_last_cycle_success == 0
time() - cf_exporter_last_cycle_timestamp_seconds > 900
max by (zone_name, dataset) (cf_exporter_group_limit_reached) == 1
cf_exporter_cycle_duration_seconds > 240
```

## Configuration

Create an API token with **Zone / Zone / Read** and **Zone / Analytics / Read** permissions for the zones to export.

| Environment Variable | Default | Accepted Values |
| --- | --- | --- |
| `CF_EXPORTER_API_TOKEN` | Required | API token; surrounding whitespace is stripped, embedded whitespace and non-printable or non-ASCII characters are rejected |
| `CF_EXPORTER_REQUEST_TIMEOUT` | `30` | Positive integer, HTTP timeout in seconds |
| `CF_EXPORTER_SCRAPE_INTERVAL` | `300` | Positive integer, collection interval in seconds |
| `CF_EXPORTER_METRICS_PORT` | `8000` | Integer from 1 to 65535 |
| `CF_EXPORTER_GROUP_LIMIT` | `0` (unlimited) | Nonnegative integer; empty also means unlimited |
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
make test-image PORT=8000 SMOKE_TIMEOUT=180
```

`image-run` runs in the foreground. `run`, `image-run` and live-test targets send the token to the container through stdin and export it inside the running process, so it is absent from container metadata and command-line arguments. The smoke test reads the complete `/metrics` response, checks container metadata and new metric families, waits for a successful cycle, validates the window sample of every successful zone, and removes the container on exit.

To run a published image with Docker:

```bash
printf '%s\n' "$CF_EXPORTER_API_TOKEN" | docker run --rm -i \
  -p 8000:8000 \
  ghcr.io/novasamatech/infra-tooling/cf-exporter:latest \
  /bin/sh -c 'IFS= read -r CF_EXPORTER_API_TOKEN; export CF_EXPORTER_API_TOKEN; exec python script.py'
```

Other settings can be passed with Docker's `-e`, for example `-e CF_EXPORTER_SCRAPE_INTERVAL=600`.

## Development

All routine tasks go through the [Makefile](Makefile); `make help` lists them. [Dockerfile.dev](Dockerfile.dev) uses `python:3.13-alpine`, matching the runtime image, and installs `requirements-dev.txt`. Its image tag hashes the pinned runtime/development requirements and the dev Dockerfile; source changes do not rebuild it. No host Python environment or package installation is needed.

Podman is the default engine; use `CONTAINER_ENGINE=docker` for Docker. Sources are mounted at `/src`; rootless Podman uses `--userns=keep-id`, and Docker uses the host UID/GID, preserving ownership of generated files. On hosts without a systemd user bus, use `CONTAINER_ENGINE='podman --cgroup-manager=cgroupfs'`.

```bash
make test                 # offline tests with coverage
make lint                 # ruff lint and format checks
make fmt                  # automatic formatting and lint fixes
make coverage             # offline tests and HTML coverage report
make test-live            # real Cloudflare API; requires token in the environment
make test-all             # offline and live tests
make run PORT=8000        # exporter in the Python 3.13 dev image
make lock                 # resolve requirements*.in into pinned requirements*.txt
make outdated             # list dependencies with newer releases
make profile-live         # live cycle, memory and exposition measurements; requires token
make clean                # remove caches, coverage output and all dev images
```

Direct dependencies are declared in `requirements.in` and `requirements-dev.in`. Regenerate the pinned files with `make lock`, which resolves in a throwaway `python:3.13-alpine` container. Runtime pins constrain development resolution. A trap removes temporary lock files on failure; the temporary resolver is removed with the container. `test-image` uses any host `python3` only for its stdlib smoke driver; the tested exporter runs in the Python 3.13 runtime image.

Offline tests mock external HTTP requests, including time-filtered regression tests for slicing, caps and midnight. Window-contract tests inspect the actual Prometheus exposition across startup, independent instances, UTC boundaries, revisions, failures and recovery. Retry tests use a local HTTP server to exercise actual transport and backoff. Live tests check every queried GraphQL dataset, setting, dimension and sort value for existence and deprecation, plus filter-field existence (Cloudflare does not expose input-field deprecation). They verify exact sums for a completed UTC day, collect every accessible zone including Free zones, and run the exporter through SIGTERM. Process logs stay in memory. Pytest skips live tests without `CF_EXPORTER_API_TOKEN`; explicit Makefile live targets require it.

`make profile-live` runs [tests/profile_live.py](tests/profile_live.py) in the dev image and prints one JSON line per cycle: cycle duration, GraphQL requests and analytics selections, groups per dataset, truncated selections, equality with the zone totals, exposition size, samples, in-process encoding time and peak RSS. It prints no label values or credentials. Pass `PROFILE_ARGS='--day YYYY-MM-DD --cycles 2'` to profile a completed UTC day, where every sum must equal its zone total; only that profiling process renders the past day's snapshot, preserving its original window boundaries.

## License

This project is licensed under the [Apache License 2.0](../LICENSE).
