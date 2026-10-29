# Cloudflare Exporter for Prometheus

This service exports Cloudflare HTTP request analytics through a Prometheus HTTP endpoint.

## How It Works

On every cycle the exporter lists all zones the API token can access using REST `GET /zones`. For each zone, a separate GraphQL HTTP request discovers the plan's available fields and page size from `settings.httpRequestsAdaptiveGroups`. An aliased analytics query retrieves visits, requests, cache requests, client-facing edge requests and dimensionless zone totals from `httpRequestsAdaptiveGroups`. Only the edge selection is restricted to requests from clients. No host list or host filter is required.

All groups are fetched by default. A full page with a nonzero last group is retried over two smaller, disjoint time windows, recursively, and values for identical label sets are summed. Only full selections are retried; a full visits page ending in zero needs no retry. Parent pages are discarded before merging their children. Slices use `datetime_geq` / `datetime_lt` with whole-second boundaries, the resolution of the window contract, so the smallest slice is one second. The page size is the plan's `maxPageSize`. A page is a property of one slice, not a limit on the number of groups.

The API can return an empty page for a window that has traffic, for minutes and also on repeated identical requests, while the halves of that window and other dimension sets are answered correctly. An empty page is therefore checked with one dimensionless read of the same window and filter, together for datasets empty in that window, unless their matching whole-window total is zero. Visits use the visits total for this decision. A window that has traffic is split like a full page; if it cannot be split, the dataset is reported as truncated. If an enabled budget prevents the check, the dataset is also flagged as incomplete, since zero traffic could not be established. A selection without dimensions returns at most one aggregate: a page size of one does not truncate that aggregate or require slicing.

The exporter imposes no limit of its own on this work by default: no group cap, no slicing budget, no minimum slice above one second, no page size below the plan's and no maximum `Retry-After`. Cloudflare's own limits still apply. Each of these can be enabled separately; see [Collection limits](#collection-limits). If a one-second slice is still full, or an enabled limit stops splitting, the exporter keeps that page, logs a warning with the reason and the remaining budgets, and sets the truncation metric of that dataset only.

Successful slices are remembered per zone and dataset until UTC midnight: the next cycle starts from the same slice boundaries instead of the whole day, and every slice is fetched again, so late revisions are included. A grown window extends the last slice; a slice that became full is split again. An unchanged burst therefore costs the same selections every cycle after the first. Only a successful zone collection updates the remembered boundaries; they are not reused when the queried dimensions or the page size change. Zone totals always use one dimensionless full-window selection with `limit: 1`. More than one analytics HTTP request is needed when slicing.

Every zone uses the same UTC window: midnight to the start of the cycle. An empty window at midnight sends no analytics query and publishes zero zone totals. The exporter publishes the complete snapshot after collection, so a scrape never sees a half-finished cycle. Series absent from the new snapshot disappear. Failure of any slice omits all analytics for that zone for the cycle, while other zones are preserved.

After midnight, scrapes omit yesterday's day-scoped families until a new-day snapshot is published: `cf_visits`, `cf_requests`, `cf_cache_requests`, `cf_edge_requests`, `cf_zone_requests`, `cf_zone_visits`, `cf_exporter_groups`, `cf_exporter_group_limit_reached`, `cf_exporter_dataset_available` and `cf_exporter_snapshot_window_end_seconds`. Zone health and cycle metrics stay available, including if a cycle started before midnight and finished afterwards. The cycle before midnight is moved earlier once a day if, judged by the previous cycle's duration, it would not finish 30 seconds before midnight. The last observed API window still ends at that cycle's **start**, so the unobserved tail includes its collection duration, the closing margin, any missed final scrape and data Cloudflare has not ingested yet. There is no final reconciliation of the previous day.

API failures are logged and reported through health metrics. HTTP 429 and transient 5xx responses are retried up to three times with backoff, and collection continues after a failed cycle. A `Retry-After` is waited for whatever its length, unless `CF_EXPORTER_MAX_RETRY_AFTER_SECONDS` sets a maximum; a longer one then fails the request at once. Cloudflare's rate limits reset over 5 minutes, so waiting can stall the cycle that long. After an HTTP 429, or a GraphQL error that reports a depleted rate-limit budget with another status, the remaining zones are skipped for that cycle and reported as failed, since their requests would be rejected too; the next cycle tries every zone again after the regular interval. A failed zone is never reported as a truncated or empty success. Cycles never overlap: one that takes longer than the interval is followed by the next at once. SIGTERM and SIGINT exit cleanly.

## Prometheus Metrics

### Analytics

| Metric | Type | Description |
| --- | --- | --- |
| `cf_visits` | Gauge | Cloudflare's `sum.visits` since midnight UTC, for all paths |
| `cf_requests` | Gauge | Cloudflare's `count` since midnight UTC, for all paths |
| `cf_cache_requests` | Gauge | Request count since midnight UTC with cache dimensions, for all hosts and paths |
| `cf_edge_requests` | Gauge | Client request count since midnight UTC by edge and origin status, for all hosts and paths |
| `cf_zone_requests` | Gauge | Dimensionless request count since midnight UTC; label: `zone_name` |
| `cf_zone_visits` | Gauge | Dimensionless visit count since midnight UTC; label: `zone_name` |

`cf_visits` labels: `zone_name`, `host_name`, `path`, `client_country_name`, `client_request_referer`, `user_agent_browser`, `user_agent_os`.

`cf_requests` labels: `zone_name`, `host_name`, `method_name`, `path`, `query`, `client_country_name`, `client_request_referer`, `user_agent_browser`, `user_agent_os`, `cache_status`, `origin_response_status`.

`cf_cache_requests` labels: `zone_name`, `host_name`, `method_name`, `path`, `cache_status`, `origin_response_status`. Keeping `origin_response_status` explains origin behavior such as `bypass`; measured on 2026-10-25, it added only 0.3% cache groups. All hosts of every discovered zone are included dynamically.

`cf_edge_requests` labels: `zone_name`, `host_name`, `method_name`, `path`, `edge_response_status`, `origin_response_status`. It counts client requests only; see [Client-facing responses](#client-facing-responses).

- Only groups with nonzero values are exported.
- An empty referer is reported as `direct`.
- Optional dimensions unavailable on the zone's plan are reported as `unknown`. Free plans currently lack `query` and `client_request_referer`. The edge dataset's required fields are handled separately below.
- Every label above is exported by default. Any of them can be disabled per metric; see [Label selection](#label-selection).
- Truncation is decided from the pages themselves, never from the zone totals, which are fetched separately and can differ from the slices for the current day. A full visits page ending in zero is complete. A configured cap requests one extra group to see whether anything was cut; a cap equal to the page size leaves no room for that, so a full page is reported. A full one-second slice, or a full slice that an enabled limit does not allow to split, is reported. A cut logs a warning and sets `cf_exporter_group_limit_reached`.
- An absent or malformed analytics result is reported as a failure, rather than an empty successful collection.

These are estimates for the current UTC window, not process counters. A smaller value can be a revised estimate; a new day can start with a value greater than or equal to yesterday's last value. Generic counter-reset detection cannot identify the UTC boundary reliably. Use the snapshot window contract below when interpreting changes over time.

To estimate origin traffic, count every status except `hit`, including `none`: in a check against an origin's own request counts, only all non-`hit` statuses together matched. To diagnose the day-to-date share absent from `cf_requests`:

```promql
1 - sum by (zone_name) (cf_requests) / cf_zone_requests
```

Cloudflare can still ingest recent data while sliced queries are running, so very small differences from zone totals can occur for a current-day window. Completed-day sums are checked separately. Slicing removes exporter group truncation; it does not change Cloudflare's adaptive sampling.

### Client-facing responses

`cf_edge_requests` counts requests from clients only. Its selection and every slice use the filter `requestSource: "eyeball"`, which excludes Cloudflare's own subrequests, for example from Workers or Early Hints. All other analytics families keep counting every request source. Do not mix `cf_edge_requests` with `cf_requests`, `cf_cache_requests` or `cf_zone_requests` in one ratio, and do not expect equal sums.

- `edge_response_status` is the HTTP status Cloudflare returned to the client. It includes responses Cloudflare produced without an origin response: cache hits, redirects, blocked requests, `499` (the client closed the connection) and Cloudflare's 52x and `530` errors.
- `origin_response_status` is the status Cloudflare received from the origin. `0` means there was no origin response: the request was served from cache or by a Worker, or the origin could not be reached. It never replaces or determines the edge status. Both are kept when they differ, for example origin `200` with edge `520`.
- All methods, response codes, hosts and paths are included. An edge status of `0` stays `"0"`; it is not inferred to be a successful response. Query strings, countries, user agents and referers are not.
- A non-2xx status is not necessarily a failure: `101` (protocol switch), redirects and `304` are normal responses.
- For the share of a status or status class, use all statuses for the same zone, host, method, path and window as the denominator. Retain both status dimensions until interpreting per-group changes over time, then aggregate. The denominator is complete only for a successfully collected zone with `cf_exporter_group_limit_reached{dataset="edge"}` at `0`. An absent sample is not zero traffic, and a window without traffic has no defined share. The exporter fetches no unconditional client total; a check of an empty edge page can fetch its dimensionless client count with the same filter.
- The dataset requires `dimensions_requestSource` for its filter, and `dimensions_edgeResponseStatus` while `edge_response_status` is exported, in the plan's `availableFields`. Without them, the zone's other datasets are still collected, `cf_exporter_dataset_available{dataset="edge"}` is `0`, there are neither edge samples nor `dataset="edge"` group metrics, and a warning is logged every cycle. An API error is not taken as missing support: the zone fails as usual and reports no availability. An exported edge status is never reported as `unknown`; a group without it fails the zone. Malformed dimensions, including a non-integer or out-of-range status (the API declares statuses as unsigned 16-bit integers), fail the zone even in zero-valued groups or discarded probe/parent pages.

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

### Label selection

`CF_EXPORTER_DISABLED_LABELS` disables labels per metric. It is a JSON object whose keys are full metric names and whose values are lists of labels to disable. Unset, empty or `{}` disables nothing; an empty list for a metric keeps all of that metric's labels. A top-level list is invalid. For example, without query strings in `cf_requests`:

```json
{"cf_requests": ["query"]}
```

or without query strings and referers in `cf_requests`, while `cf_visits` keeps its referer:

```json
{"cf_requests": ["query", "client_request_referer"]}
```

| Metric | Labels that can be disabled |
| --- | --- |
| `cf_visits` | `zone_name`, `host_name`, `path`, `client_country_name`, `client_request_referer`, `user_agent_browser`, `user_agent_os` |
| `cf_requests` | `zone_name`, `host_name`, `method_name`, `path`, `query`, `client_country_name`, `client_request_referer`, `user_agent_browser`, `user_agent_os`, `cache_status`, `origin_response_status` |
| `cf_cache_requests` | `zone_name`, `host_name`, `method_name`, `path`, `cache_status`, `origin_response_status` |
| `cf_edge_requests` | `zone_name`, `host_name`, `method_name`, `path`, `edge_response_status`, `origin_response_status` |
| `cf_zone_requests`, `cf_zone_visits` | `zone_name` |

- A disabled label is absent from the metric, and its dimension is not queried for that metric. Cloudflare aggregates over it before applying the page size, so fewer groups are fetched and sliced, not fetched and dropped. The other metrics keep the label. The remaining labels keep their order.
- An unknown metric, a label the metric does not have, a repeated key or a value of the wrong JSON type fails startup.
- An enabled label that the zone's plan does not offer is still `unknown`; an enabled empty referer is still `direct`.
- `zone_name` is not a Cloudflare dimension. Zones are still collected, checked and capped separately; without `zone_name`, identical label sets of all successfully collected zones are summed into one series. A metric without any labels is one series. `cf_exporter_groups` and the other health metrics keep `zone_name`; `cf_exporter_groups` then counts a zone's label sets before zones are combined.
- Disabling `edge_response_status` removes its field from the edge dataset's requirements. The client-request filter and its field stay required, and every exported status is still validated.
- No filters are added and the traffic scope of every metric is unchanged.

### Exporter Health

| Metric | Labels | Description |
| --- | --- | --- |
| `cf_exporter_zone_scrape_success` | `zone_name` | 1 if the last collection of the zone succeeded, 0 otherwise |
| `cf_exporter_last_cycle_success` | None | 1 if zone listing and every zone succeeded in the last cycle |
| `cf_exporter_last_cycle_timestamp_seconds` | None | Unix time when the last cycle finished |
| `cf_exporter_cycle_duration_seconds` | None | Duration of the last cycle |
| `cf_exporter_snapshot_window_end_seconds` | `zone_name`, `window_start` | Exclusive API window end as Unix seconds; `window_start` identifies the UTC day |
| `cf_exporter_groups` | `zone_name`, `dataset` | Nonzero label sets of the zone; dataset is `visits`, `requests`, `cache` or `edge` |
| `cf_exporter_group_limit_reached` | `zone_name`, `dataset` | 1 when a configured cap, a full one-second slice or an enabled slicing limit cut data, otherwise 0 |
| `cf_exporter_dataset_available` | `zone_name`, `dataset` | 1 if the zone's plan supports the dataset, 0 if it is not collected; only for successfully collected zones |
| `cf_exporter_api_responses_total` | `api`, `code` | Counter of HTTP responses from the Cloudflare API by status code, including retried attempts |
| `cf_exporter_api_errors_total` | `api`, `error` | Counter of failed Cloudflare API attempts by error kind, including retried attempts |

Every successfully collected zone has all four availability samples: visits, requests and cache are always `1`; edge is `1` or `0`. Missing availability means there is no successful current-day snapshot for the zone (for example startup, collection failure or expiry at UTC midnight), not an unsupported plan. Availability is day-scoped and disappears with the analytics; zone and cycle health remain available.

Cycle metrics appear after the first cycle finishes. Example alert conditions for the default 300-second interval:

```promql
cf_exporter_last_cycle_success == 0
time() - cf_exporter_last_cycle_timestamp_seconds > 900
max by (zone_name, dataset) (cf_exporter_group_limit_reached) == 1
cf_exporter_dataset_available == 0
cf_exporter_cycle_duration_seconds > 240
```

### Cloudflare API errors

`cf_exporter_api_responses_total` and `cf_exporter_api_errors_total` are process counters that start at zero with the exporter. `api` is `rest` for the zone listing and `graphql` for settings and analytics. Every request attempt is counted once, including attempts the transport retries before a request succeeds, so the counters also show rate limits and outages that a successful cycle hides. Every `error` series is exported from zero:

| `error` | Meaning |
| --- | --- |
| `rate_limited` | HTTP 429 |
| `budget_depleted` | A GraphQL error reporting a depleted rate-limit budget without HTTP 429 |
| `server_error` | Another HTTP 5xx |
| `client_error` | Another HTTP 4xx, for example an invalid token or missing permissions |
| `timeout` | Connect or read timeout |
| `connection` | DNS, TCP, TLS or HTTP protocol failure, including a refused connection |
| `invalid_response` | A response that is not a JSON object, reports `success: false`, has no GraphQL data or an invalid zone listing |
| `graphql` | Any other error in a GraphQL response, which usually has HTTP 200 |

A response with an error status is counted in both counters; transport failures have no response code. After HTTP 429 or a depleted budget, the cycle's remaining zones are skipped without requests, so they appear as failed zones rather than as more errors. Analytics content that fails validation, such as a malformed group, fails the zone without being counted here. Labels never contain URLs, messages, zone names or credentials.

```promql
# Rate limiting, even when a retry later succeeded
increase(cf_exporter_api_errors_total{error=~"rate_limited|budget_depleted"}[15m]) > 0
# API unavailability
increase(cf_exporter_api_errors_total{error=~"server_error|timeout|connection"}[15m]) > 0
# Error rates by kind
sum by (api, error) (rate(cf_exporter_api_errors_total[1h]))
```

### Readiness

`GET /readyz` on the metrics port returns HTTP 200 with `ready` only when the
last published snapshot comes from a successful cycle with at least one zone and
every discovered zone succeeded. Both the API window end and the cycle
completion must be less than three collection intervals old and not in the
future; the window age includes the cycle's own duration. With the default
300-second interval, the limit is 900 seconds. Otherwise it returns HTTP 503
with a one-line reason: before the first cycle, after a failed zone listing or
zone collection, without discovered zones, or with a stale snapshot. A later
successful cycle restores readiness.

Readiness does not depend on the UTC day. After midnight, scrapes omit the
previous day's day-scoped families until the new day's first snapshot is
published. This refill is normal operation: a fresh previous-day snapshot stays
ready, and the freshness limit still applies.

An empty successful traffic window is ready. A plan's unsupported dataset or a
reported group truncation does not fail readiness; the existing availability
and truncation metrics describe those conditions. Readiness reads the atomic
in-memory snapshot without API calls or metrics serialization. It answers during
a concurrent large metrics scrape, and responses carry `Cache-Control: no-store`.
`HEAD /readyz` reports the same status without a body; other methods return 405.
Every other path still serves metrics.

`/metrics` keeps serving metrics and health diagnostics while `/readyz` returns
503. Use `/readyz` for a Kubernetes readiness probe rather than a TCP probe or
the metrics status, but not for liveness: a Cloudflare failure should not
restart the exporter. A not-ready pod is removed from its Service endpoints, and
instances sharing one Cloudflare account usually become unready together. Scrape
the exporter through a target discovery that keeps not-ready pods; otherwise the
health metrics disappear exactly when collection fails. The endpoint introduces
no coordination between instances.

## Configuration

Create an API token with **Zone / Zone / Read** and **Zone / Analytics / Read** permissions for the zones to export.

| Environment Variable | Default | Accepted Values |
| --- | --- | --- |
| `CF_EXPORTER_API_TOKEN` | Required | API token; surrounding whitespace is stripped, embedded whitespace and non-printable or non-ASCII characters are rejected |
| `CF_EXPORTER_REQUEST_TIMEOUT` | `30` | Positive integer, HTTP timeout in seconds |
| `CF_EXPORTER_SCRAPE_INTERVAL` | `300` | Positive integer, collection interval in seconds |
| `CF_EXPORTER_METRICS_PORT` | `8000` | Integer from 1 to 65535 |
| `CF_EXPORTER_DISABLED_LABELS` | `{}` (every label) | JSON object of metric names to lists of labels; see [Label selection](#label-selection) |
| `CF_EXPORTER_LOGLEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |

Optional limits of the exporter's own work are listed under [Collection limits](#collection-limits). Invalid settings or a missing/malformed token cause startup to fail. An unknown log level falls back to INFO with a warning. Logs and configuration representations redact the token; the startup log lists the enabled limits and disabled labels.

### Collection limits

Each setting is a nonnegative integer. Unset, empty or `0` imposes no limit, which is the default; a positive value enables the limit. Negative or non-integer values fail startup. All limits apply per exporter process: instances sharing an API token do not coordinate, and none of these values is derived from Cloudflare's quota, the number of instances or the scrape interval.

| Environment Variable | Limit when positive |
| --- | --- |
| `CF_EXPORTER_GROUP_LIMIT` | Exports the largest N nonzero groups per zone and dataset. Up to the page size, one full-window query with one extra probe group shows whether the cap cut anything; larger values slice and select after merging. |
| `CF_EXPORTER_MAX_SLICE_REQUESTS` | Analytics reads per zone and dataset in one cycle, including the first windows, discarded parent pages and checks of empty pages. |
| `CF_EXPORTER_MAX_EXTRA_SELECTIONS` | Selections per cycle beyond one read per zone and dataset, shared by all zones and datasets, including checks of empty pages. Every aliased selection counts, also inside one HTTP request; settings, zone totals and transport retries do not. Renewed every cycle. |
| `CF_EXPORTER_MIN_SLICE_SECONDS` | Slices this long or shorter are not split further. |
| `CF_EXPORTER_MAX_RETRY_AFTER_SECONDS` | A longer `Retry-After` fails the request at once instead of being waited for. |
| `CF_EXPORTER_PAGE_SIZE` | Groups per page; a value above a zone's `maxPageSize` uses `maxPageSize`. |

When a slicing limit cuts data, the warning names it, for example `dataset slice budget` or `configured minimum slice of 60s`, with the reads and shared selections left. Remembered slices are merged to fit a smaller budget, keeping their boundaries. An empty page that cannot be checked within the budget is flagged as incomplete. A limit never turns an API failure into a successful truncated collection, and the request timeout and transport retries are separate network settings.

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

`image-run` runs in the foreground. `run`, `image-run` and live-test targets send the token to the container through stdin and export it inside the running process, so it is absent from container metadata and command-line arguments. Other settings set in the environment, such as `CF_EXPORTER_DISABLED_LABELS` or a limit, are passed by name to these targets and to the smoke test's container (except the port). The smoke test reads the complete `/metrics` response, checks container metadata and new metric families, requires `/readyz` to stay unready until a cycle is published, waits for a successful cycle, validates the window sample of every successful zone, the labels of every analytics sample for the configured schema, the matching readiness and the Cloudflare API counters, prints nonzero API error counts and series per family, and removes the container on exit.

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

Offline tests mock external HTTP requests, including time-filtered regression tests for slicing, repeated bursts, optional limits and midnight. Label tests use a fake API that groups by the requested dimensions before the page size, and check queries and the exposition for every label configuration. Window-contract tests inspect the actual Prometheus exposition across startup, independent instances, UTC boundaries, revisions, failures and recovery. Edge tests check the client-request filter in every selection and slice, the preserved client and origin statuses, plan support and dataset health. Retry tests use a local HTTP server to exercise actual transport and backoff. Readiness tests use the real metrics HTTP server with synthetic and fake-API snapshots, covering failure recovery, freshness limits, the midnight refill and concurrent scrapes. API counter tests count every attempt of real transport retries against a local server, including read timeouts and refused connections. Live tests check every queried GraphQL dataset, setting, dimension and sort value for existence and deprecation, plus filter-field existence (Cloudflare does not expose input-field deprecation). They verify exact sums for a completed UTC day, comparing edge groups with an independently fetched client-request total and checking the filter value against requests grouped by source. They collect every accessible zone including Free zones, check a configuration with disabled labels against the zone totals, and run the exporter process through readiness and SIGTERM. Live tests and profiling use the same settings from the environment as the exporter. Process logs stay in memory. Pytest skips live tests without `CF_EXPORTER_API_TOKEN`; explicit Makefile live targets require it.

`make profile-live` runs [tests/profile_live.py](tests/profile_live.py) in the dev image with the exporter's settings from the environment and prints one JSON line per cycle: the window, enabled limits and disabled labels, cycle duration, GraphQL, settings and analytics HTTP requests, analytics selections, groups per dataset, series per family, truncated selections, zones without the edge dataset, equality with the zone totals, `strict_totals`, `source_filter_valid`, exposition size, samples, in-process encoding time and peak RSS. The request counts measure GraphQL calls before transport retries; the API counters count every attempt. It prints no credentials and no label values except, for a sum that differs from its total, the zone with both sums, its groups, slices and truncation. Pass `PROFILE_ARGS='--day YYYY-MM-DD --cycles 2'` to profile a completed UTC day. The date must be strictly before today and `--cycles` must be positive. `--until YYYY-MM-DDTHH:MM:SSZ` fixes the window to that day's midnight up to the given time, so label configurations can be compared on the same window. Both `--day` and `--until` on a past UTC day require every sum to match and return a failure on a mismatch; `strict_totals` reports this policy. For the current day, differences are reported without failing on totals alone because ingestion can continue. The edge reference is an independently fetched client-request total outside the measured selections; its filter is checked against raw sources, including zones with only subrequests, and queried only where the edge dataset is supported. Across the checked zones, some zone with traffic must report client requests: a renamed source value would empty the filter and its reference in every zone at once. Per-zone validation retains scalar summaries rather than a second set of series. Only historical profiling bypasses the visibility gate, preserving the original window boundaries.

## License

This project is licensed under the [Apache License 2.0](../LICENSE).
