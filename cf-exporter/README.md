# Cloudflare Exporter for Prometheus

This service exports Cloudflare HTTP request analytics in closed 5-minute buckets. Every exported number is the count of one finished bucket, checked against independently read totals, and is served until a scraper has taken it.

## How It Works

Time is cut into buckets of 300 seconds aligned to UTC. A bucket is read once it has ended and settled, 5 minutes later. Its samples are exported with the **end of the bucket as their timestamp**, so the numbers of an hour, a day or a month are the sum of their buckets and do not depend on when or how often the exporter is scraped.

The exporter lists all zones the API token can access with REST `GET /zones` and reads each zone's plan from `settings.httpRequestsAdaptiveGroups`: available fields, page size, longest query range and how far back data can be read. No host list or host filter is required.

### Reading a window

One GraphQL request reads a window of buckets of one zone from `httpRequestsAdaptiveGroups` with four aliased selections, each grouped by `datetimeFiveMinutes`:

| Selection | Content |
| --- | --- |
| `groups` | Requests and visits by the union of the dimensions of `cf_visits_5m`, `cf_requests_5m` and `cf_cache_requests_5m`; the three families are aggregated from it |
| `totals` | Requests and visits per bucket, without dimensions |
| `edge` | Requests from clients (`requestSource: "eyeball"`) by the dimensions of `cf_edge_requests_5m` |
| `edge_totals` | Requests from clients per bucket, without dimensions |

All groups are fetched. A full page of `groups` or `edge` is read again in two halves, on bucket boundaries and then within the bucket down to one second. A full second is read with dimension pagination: all queried dimensions are sorted ascending and each page continues after the complete dimension tuple of the previous one. Counts are never pagination keys. A part of such a split read that Cloudflare answers at a reduced resolution (see below) is asked for again at once, up to three times.

### Why a bucket is trusted

Under load Cloudflare answers a selection from a table that holds every tenth or hundredth event instead of its full data. Such an answer differs in two ways:

- every group has a sample interval (`avg { sampleInterval }`) of 10 or more, and its count is an estimate;
- a bucket with few events has **no group at all**. A reduced answer for a quiet hour is simply empty, and two reduced selections agree on zero.

Each selection is answered on its own, also within one request. An empty answer therefore proves nothing. A bucket is accepted only when all of this holds:

1. **The totals are proven to come from the full data.** `totals` and `edge_totals` are read together with the buckets of the 24 hours before the window (after it at the far end of the dataset's reach). One group with a sample interval below 5 anywhere in that answer proves the full data for the whole answer, empty buckets included.
2. The groups add up to the totals: requests and visits of `groups` to `totals`, requests of `edge` to `edge_totals`.
3. No group of the bucket shows a reduced resolution, unless its selection is proven full by another group of the same answer. A bucket read in several parts is not accepted if one of its parts came at a reduced resolution.
4. Client requests do not exceed all requests.

What happens otherwise:

| Answer | Reaction |
| --- | --- |
| A selection with groups at a sample interval of 5 or more only | Read again at once, up to three times, with a slightly different page size (Cloudflare caches answers by query and variables) |
| Totals without any group over a range with a bucket known to have events | A reduced answer that left them out: read again like one |
| Totals without any group in the 24 hours | Read again over 7 days. Never longer: Cloudflare answers ranges of a month from a reduced table every time |
| Totals without any group in 7 days, three answers in a row | The zone is idle; its empty buckets are accepted |
| Buckets that contradict each other | Every selection is read again at once |
| Still not acceptable | The bucket stays pending and is read again after 2, 4, 8, 15 and then every 30 minutes |

Known events are the accepted buckets with requests, which the database keeps after their delivery, and the newest bucket with events in any answer. The proof range of the totals is widened to the nearest such bucket within a week of the window. An answer from the full data then has a group there, and an answer without any is known to be a reduced one. This covers the second read of a bucket, which lies in its own range, and history read behind newer buckets after a lost database.

Pending buckets are not exported. A bucket appears late rather than wrong.

One case cannot be proven: a zone that Cloudflare itself stores at a sample interval of 5 or more throughout. Its buckets return the same unverifiable answer every time; after 6 hours of that they are accepted and counted in `cf_exporter_bucket_reads_total{result="accepted_unverified"}`. Only an answer whose totals have groups, all at such an interval, one of them for the bucket itself, and add up is accepted this way. An empty answer, one without the bucket, one without known events or one that contradicts itself is never accepted, however often it comes; such a bucket stays pending until it leaves the dataset's reach and is counted as `abandoned`.

### Late events and history

- An accepted bucket is read once more one to two hours after it ended, all buckets of an hour in one request. If late events changed it, the bucket is served again with the same timestamps and the new values. A bucket served with other labels than the exporter uses now (`CF_EXPORTER_DISABLED_LABELS` or the zone's plan changed) is not read again: serving it once more would add a second series for the same events. Such skipped rechecks are counted as `recheck_skipped`.
- On first start, after a lost database and after an outage, the exporter reads everything the dataset still has, newest first, up to `notOlderThan` minus one hour, or `CF_EXPORTER_BACKFILL_DAYS`. History is read at most once per 15 seconds (`CF_EXPORTER_HISTORY_INTERVAL_SECONDS`) and only while fewer than 500 000 samples wait for the scraper, so fresh buckets always come first.
- Cloudflare allows 300 GraphQL queries in 5 minutes for all users of a token together and counts every selection of a request. A history read costs about 4 queries, and the fresh buckets of an instance about 50 per 5 minutes. The default interval of 15 seconds leaves room for two instances; for `N` instances on one token, use at least `1200 × N / (300 − 50 × N)` seconds (24 for three, 48 for four), more if the token has other users. Whenever the limit is reached anyway, the interval between history reads doubles, up to 4 minutes, and halves again after every hour without a rejection. The pause is waited in pieces of at most 15 seconds and never past the moment the next bucket settles: a new bucket is read when it settles, a due retry or recheck at most 15 seconds later.
- Buckets that were tried but never verified before they left the dataset's reach are counted as `abandoned`.
- A zone that is missing from the zone listing, also one that only the database still knows after a restart, keeps its state for a day but is not read. If it is listed again within that time, only the buckets that became due meanwhile are read. Health metrics show the listed zones only.

### Failures

API failures are logged and reported through health metrics; the process keeps running. Transient 5xx and transport errors have up to three retries with backoff. By default HTTP 429 is waited for as long as `Retry-After` says, or 300 seconds without one, without spending the retry allowance; a GraphQL error reporting a depleted budget is handled the same way. `CF_EXPORTER_MAX_RETRY_AFTER_SECONDS` bounds that wait; when it rejects a request, all requests, including settings and the zone listing, pause for 300 seconds. A failed read never yields partial data. SIGTERM and SIGINT exit cleanly.

A zone whose settings request fails is tried again after 60 seconds. A zone whose plan cannot serve the reads (the dataset is not enabled, a required field is missing, or the plan's range is shorter than a bucket) is reported with `cf_exporter_zone_success` 0 and asked again only hourly: asking sooner changes nothing and costs the shared rate limit.

After any other failed read the zone is left alone for 60 seconds; other zones continue. What follows keeps one bucket that cannot be read from holding back its zone:

| Failed read | Next |
| --- | --- |
| A window of several buckets, first failure | Tried again as it is. After a history read of the zone's full window size, its history windows are halved and stay at most that size until its settings are fetched again, within the hour; a smaller window that fails changes nothing |
| The same window again | Its buckets are read one by one |
| A single bucket | Tried again after 1, 2, 4, 8, 15 and then every 30 minutes; the rest of the zone is read meanwhile |
| A recheck | Tried again every 15 minutes, for up to 6 hours after the bucket |

When a read works again after two or more failed reads of the zone in a row, its postponed buckets are read at once.

## Prometheus Metrics

### Analytics

Served on `/analytics`. Every sample is the number of events in one bucket and carries the bucket end as its timestamp.

| Metric | Type | Value per bucket |
| --- | --- | --- |
| `cf_visits_5m` | Gauge | Cloudflare's `sum.visits` |
| `cf_requests_5m` | Gauge | Cloudflare's `count`, all request sources |
| `cf_cache_requests_5m` | Gauge | The same requests by cache dimensions |
| `cf_edge_requests_5m` | Gauge | Requests from clients by edge and origin status |
| `cf_zone_requests_5m` | Gauge | All requests of the zone, without dimensions |
| `cf_zone_visits_5m` | Gauge | All visits of the zone, without dimensions |

`cf_visits_5m` labels: `zone_name`, `host_name`, `path`, `client_country_name`, `client_request_referer`, `user_agent_browser`, `user_agent_os`.

`cf_requests_5m` labels: `zone_name`, `host_name`, `method_name`, `path`, `query`, `client_country_name`, `client_request_referer`, `user_agent_browser`, `user_agent_os`, `cache_status`, `origin_response_status`.

`cf_cache_requests_5m` labels: `zone_name`, `host_name`, `method_name`, `path`, `cache_status`, `origin_response_status`.

`cf_edge_requests_5m` labels: `zone_name`, `host_name`, `method_name`, `path`, `edge_response_status`, `origin_response_status`.

`cf_zone_requests_5m` and `cf_zone_visits_5m` labels: `zone_name`.

- Only label sets with events in the bucket have a sample. The zone totals have one for **every** accepted bucket, also with value 0: a zone total of 0 means no traffic, a missing zone total means the bucket is not accepted yet.
- In every accepted bucket `cf_requests_5m` and `cf_cache_requests_5m` add up to `cf_zone_requests_5m`, and `cf_visits_5m` to `cf_zone_visits_5m`.
- An empty referer is reported as `direct`. Dimensions the zone's plan does not offer are not queried and reported as `unknown`; Free plans currently lack `query` and `client_request_referer`.
- To estimate origin traffic, count every cache status except `hit`, including `none`.

Sums over time are sums of buckets. With a range that is a multiple of 5 minutes:

```promql
# Visits per host and day
sum by (host_name) (sum_over_time(cf_visits_5m[1d]))
# Share of 5xx among client requests, last hour
sum(sum_over_time(cf_edge_requests_5m{edge_response_status=~"5.."}[1h]))
  / sum(sum_over_time(cf_edge_requests_5m[1h]))
# Newest accepted bucket of every zone
max by (zone_name) (timestamp(cf_zone_requests_5m))
```

Do not use `rate()` or `increase()`: these are not counters. Data arrives 6 to 8 minutes after a bucket ends, later while Cloudflare answers at a reduced resolution. A panel or alert should look at closed buckets, for example with an `offset` of 10 minutes.

### Client-facing responses

`cf_edge_requests_5m` counts requests from clients only: its selections use the filter `requestSource: "eyeball"`, which excludes Cloudflare's own subrequests, for example from Workers. All other families count every request source. Do not mix `cf_edge_requests_5m` with them in one ratio.

- `edge_response_status` is the HTTP status Cloudflare returned to the client, including responses without an origin response: cache hits, redirects, blocked requests, `499` and Cloudflare's 52x and `530` errors.
- `origin_response_status` is the status Cloudflare received from the origin; `0` means there was none. Both are kept when they differ, for example origin `200` with edge `520`.
- A non-2xx status is not necessarily a failure: `101`, redirects and `304` are normal responses.
- The dataset needs `dimensions_requestSource` and, while `edge_response_status` is exported, `dimensions_edgeResponseStatus` in the plan's `availableFields`. Without them the zone's other families are still collected, `cf_exporter_dataset_available{dataset="edge"}` is `0` and a warning is logged. An API error is never taken as missing support. An exported edge status is never `unknown`; statuses must be unsigned 16-bit integers, and a malformed group fails the read.

### Label selection

`CF_EXPORTER_DISABLED_LABELS` disables labels per metric. It is a JSON object whose keys are metric names and whose values are lists of labels. For example, without query strings and referers in `cf_requests_5m`, while `cf_visits_5m` keeps its referer:

```json
{"cf_requests_5m": ["query", "client_request_referer"]}
```

- A disabled label is absent from the metric and its bucket values are summed over it. Its dimension is left out of the query once no family reads it from the same selection, so Cloudflare aggregates before the page size.
- Every label except `zone_name` can be disabled. Zones are accepted bucket by bucket at different times, so a sum over zones is left to the query: `sum without (zone_name) (...)`.
- An unknown metric or label, a repeated key or a value of the wrong JSON type fails startup.

### Scraping

`GET /analytics` returns the buckets that are not delivered yet, up to 50 000 samples per response (`CF_EXPORTER_RESPONSE_SAMPLES`) and always whole buckets. Buckets that still need a response come first, the newest first; older ones follow in later responses. Buckets that had their responses and only wait for the delivery time fill what is left of a response, so they never keep others out. The response is gzip-compressed if `Accept-Encoding` allows gzip with a quality above zero.

| Requirement | Reason |
| --- | --- |
| Request `/analytics?consume=1` | Only such a response counts as a delivery. Without the parameter the endpoint can be read by hand at any time without taking samples away from the scraper |
| One consuming scraper per exporter instance, or several that write into the same deduplicating store | Delivery is counted per bucket, not per scraper. Under a backlog a bucket goes out in exactly two responses; if both reach one scraper, another scraper never sees the bucket |
| Only the scraper may consume: restrict the port to it, or set `CF_EXPORTER_CONSUME_TOKEN` and send `Authorization: Bearer <token>` | A consuming request is destructive: two of them and ten minutes delete a bucket from the exporter. With the token, `?consume=1` without it is answered 403; reading without the parameter stays open |
| Honor the exposed timestamps | The timestamps are the data. In Prometheus-compatible scrapers this is `honor_timestamps: true` |
| No staleness markers for this job | Series leave the response after every delivered bucket. A marker written then is no bucket value, and next to the bucket samples it can interfere with deduplication |
| Accept samples up to the dataset's reach in the past | History and late buckets are hours to weeks old |
| A response size limit above the largest response, at least 64 MiB with the default `CF_EXPORTER_RESPONSE_SAMPLES` | One response has up to 50 000 samples, usually 10 to 15 MB uncompressed, more with long label values or a very large bucket. A response the scraper drops after reading it still counts as sent |
| A store that keeps one sample per series and timestamp, preferably the larger value | See below |

A bucket leaves `/analytics` after two complete consuming responses and 10 minutes since it was accepted; its samples are then deleted. A response counts once its body was written; whether the scraper then keeps it is not known to the exporter. A response that was not sent completely does not count, nor does one for a bucket accepted again while it was on its way. If nobody scrapes, buckets wait: 35 days after their acceptance at most, then they are dropped and counted as `expired`.

The same sample arrives many times: in every response for at least ten minutes, from every replica, and again after the history is read again. A bucket changed by its second read arrives with the same timestamp and a new value. The store therefore has to keep one sample per series and timestamp:

- With deduplication that keeps the larger of two values at one timestamp, sums are right and late corrections win.
- A store that rejects a second sample at an existing timestamp with another value, as Prometheus' own storage does, keeps the first value: nothing is counted twice, but the correction is lost.
- A store that keeps every sample counts each bucket several times in `sum_over_time`.

Several replicas can run side by side. Each reads Cloudflare on its own and keeps its own database. Scrape every replica and drop the labels that identify the replica from the analytics samples; the replicas then produce the same series with the same timestamps, which such a store keeps once. Replicas can disagree for a bucket whose late events only one of them has seen; the store's rule decides which value stays.

A database or query layer that caches the results of range queries may keep answers computed before a late sample arrived. Bypass or disable that cache for these metrics.

### Exporter health

Served on `/metrics` and every other path, without timestamps.

| Metric | Labels | Description |
| --- | --- | --- |
| `cf_exporter_zone_success` | `zone_name` | 1 if the zone's last settings request or read succeeded, 0 otherwise |
| `cf_exporter_dataset_available` | `zone_name`, `dataset` | 1 if the zone's plan supports the dataset (`visits`, `requests`, `cache`, `edge`), 0 if it is not collected |
| `cf_exporter_pending_buckets` | `zone_name` | Buckets within reach that are not accepted yet, including scheduled retries and unread history |
| `cf_exporter_newest_bucket_end_seconds` | `zone_name` | End of the newest accepted bucket |
| `cf_exporter_oldest_bucket_end_seconds` | `zone_name` | End of the oldest accepted bucket still tracked |
| `cf_exporter_undelivered_buckets` | None | Accepted zone buckets still served on `/analytics` |
| `cf_exporter_undelivered_samples` | None | Their samples |
| `cf_exporter_bucket_reads_total` | `result` | Counter of bucket outcomes, see below |
| `cf_exporter_analytics_responses_total` | None | Counter of complete consuming `/analytics` responses |
| `cf_exporter_analytics_samples_total` | None | Counter of samples in them |
| `cf_exporter_storage_persistent` | None | 1 if the state is kept in the database file, 0 if only in memory |
| `cf_exporter_storage_full` | None | 1 if a write failed for lack of disk space within the last minute; reads wait meanwhile |
| `cf_exporter_storage_resets_total` | None | Counter of database recreations |
| `cf_exporter_loop_timestamp_seconds` | None | Unix time of the last collection loop iteration |
| `cf_exporter_api_responses_total` | `api`, `code` | Counter of HTTP responses from the Cloudflare API, including retried attempts |
| `cf_exporter_api_errors_total` | `api`, `error` | Counter of failed Cloudflare API attempts by kind, including retried attempts |

`result` of `cf_exporter_bucket_reads_total`, all exported from zero:

| `result` | Meaning |
| --- | --- |
| `accepted` | Verified and stored |
| `accepted_unverified` | Never verified; the same consistent answer at a high sample interval for 6 hours |
| `sampled` | An answer came at a reduced resolution, lacked known events or its totals were not proven complete; read again later |
| `inconsistent` | The selections contradict each other; read again later |
| `recheck_unchanged`, `recheck_changed` | The later second read; a changed bucket is served again |
| `recheck_failed` | The second read did not verify; tried again for up to 6 hours |
| `recheck_skipped` | The second read was cancelled because the bucket was served with other labels |
| `abandoned` | Tried but never verified before it left the dataset's reach |
| `expired` | Accepted but not scraped within 35 days after its acceptance |
| `error` | The read failed; counted per read, not per bucket |

Example alert conditions:

```promql
# No accepted bucket for 30 minutes
time() - cf_exporter_newest_bucket_end_seconds > 1800
# The scraper does not take the samples
cf_exporter_undelivered_buckets > 0 and increase(cf_exporter_analytics_responses_total[15m]) == 0
# Data lost for good
increase(cf_exporter_bucket_reads_total{result=~"abandoned|expired"}[1h]) > 0
# The database file cannot be used, or its disk is full
cf_exporter_storage_persistent == 0 or cf_exporter_storage_full == 1
# The collection loop is stuck
time() - cf_exporter_loop_timestamp_seconds > 900
```

`cf_exporter_api_responses_total` and `cf_exporter_api_errors_total` count every request attempt once, including attempts the transport retries before a request succeeds. `api` is `rest` for the zone listing and `graphql` for settings and analytics. Every `error` series is exported from zero:

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

Labels never contain URLs, messages, zone names or credentials.

### Readiness

`GET /readyz` returns HTTP 200 with `ready` once the database can be used and the zones were listed for the first time; otherwise HTTP 503 with a one-line reason. `HEAD` reports the same status without a body; other methods return 405.

Readiness deliberately does not depend on how fresh the data is or on Cloudflare's current state: a slow or failing Cloudflare must not take all replicas out of service at once. Old data is an alert on `cf_exporter_newest_bucket_end_seconds`. Do not use `/readyz` for liveness.

## Storage

The state is one SQLite database: which buckets are accepted, pending or delivered, and the samples not delivered yet. Everything in it can be read again from Cloudflare, so the exporter looks after the file itself:

| Situation | What the exporter does |
| --- | --- |
| No file or directory | Creates them and reads the history within reach again |
| A corrupt database of the exporter, or one of another schema version | Replaces it and reads the history again |
| A file that is not recognizably the exporter's: not an SQLite file, or one without the exporter's application id in its header, damaged or not | Leaves it untouched and keeps the state in memory. Only a file of zero bytes alone, as a crash can leave, is replaced as well |
| A database operation fails while running | Recreates the database and repeats the operation; if the new file fails as well, continues in memory |
| The file is removed or replaced while running | Notices it at the next operation and opens the path again. Of the write-ahead files next to it, only its own leftovers are removed |
| The path cannot be used at all | Keeps the state in memory and tries the file again every 60 seconds |
| The database can be read but not written, as on a file system that became read-only | Replaces the file if it can; otherwise keeps the state in memory and tries the file again every 60 seconds. A write is tested every time the file is opened |
| The disk is full | Keeps the database as it is and does not read for a minute, then tries again; `cf_exporter_storage_full` is 1 meanwhile. A full disk at start keeps the state in memory |
| Another process holds the database lock | Waits up to 30 seconds and fails that operation; nothing is deleted |

History that is read again is exported again with the same timestamps and values, which a database that deduplicates identical samples stores once. No vacuuming or other maintenance is needed: delivered samples are deleted, and the file stops growing at its peak.

## Configuration

Create an API token with **Zone / Zone / Read** and **Zone / Analytics / Read** permissions for the zones to export.

| Environment Variable | Default | Accepted Values |
| --- | --- | --- |
| `CF_EXPORTER_API_TOKEN` | Required | API token; surrounding whitespace is stripped, embedded whitespace and non-printable or non-ASCII characters are rejected |
| `CF_EXPORTER_DB_PATH` | `/var/lib/cf-exporter/cf-exporter.sqlite3` | Path of the database file |
| `CF_EXPORTER_METRICS_PORT` | `8000` | Integer from 1 to 65535 |
| `CF_EXPORTER_REQUEST_TIMEOUT` | `30` | Positive integer, HTTP timeout in seconds |
| `CF_EXPORTER_DISABLED_LABELS` | `{}` (every label) | JSON object of metric names to lists of labels; see [Label selection](#label-selection) |
| `CF_EXPORTER_LOGLEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `CF_EXPORTER_HISTORY_INTERVAL_SECONDS` | `15` | Positive integer: least seconds between two history reads; see [Late events and history](#late-events-and-history) for more instances on one token |
| `CF_EXPORTER_RESPONSE_SAMPLES` | `50000` | Positive integer: samples per `/analytics` response, whole buckets, at least one. Size the scraper's response limit to it. A smaller value delivers a backlog more slowly: each bucket needs two responses |
| `CF_EXPORTER_CONSUME_TOKEN` | Not set | Bearer token a `?consume=1` request has to send; same character rules as the API token. Unset, any consuming request counts |

Optional limits. Each is a nonnegative integer; unset, empty or `0` imposes none:

| Environment Variable | Limit when positive |
| --- | --- |
| `CF_EXPORTER_BACKFILL_DAYS` | History is read and exported this many days back instead of everything the dataset has. Proof ranges still use the dataset's whole reach |
| `CF_EXPORTER_MAX_RETRY_AFTER_SECONDS` | A longer `Retry-After` fails the request at once instead of being waited for |
| `CF_EXPORTER_PAGE_SIZE` | Groups per page of the selections with dimensions; a value above a zone's `maxPageSize` uses `maxPageSize` |

Invalid settings or a missing or malformed token fail startup. An unknown log level falls back to INFO with a warning. Logs and configuration representations redact both tokens; the startup log lists the storage path, enabled limits, disabled labels, the history interval, the response size and whether a consume token is required. Settings removed in 2.0.0 (`CF_EXPORTER_SCRAPE_INTERVAL`, `CF_EXPORTER_GROUP_LIMIT`, `CF_EXPORTER_MAX_SLICE_REQUESTS`, `CF_EXPORTER_MAX_EXTRA_SELECTIONS`, `CF_EXPORTER_MIN_SLICE_SECONDS`) are ignored with a warning.

## Containers

`make image` builds `ghcr.io/novasamatech/infra-tooling/cf-exporter` with the tag in `.version` using Podman. Override `CONTAINER_ENGINE=docker` to use Docker.

The image runs as UID and GID 1000 and declares `/var/lib/cf-exporter` as a volume. Mount a volume writable by that user or group to keep the database across restarts. Without one the exporter works the same and reads the history again after every restart. Each instance needs a directory of its own; the database holds the bucket bookkeeping of the reach and the samples not scraped yet, and grows while nobody scrapes.

```bash
printf '%s\n' "$CF_EXPORTER_API_TOKEN" | docker run --rm -i \
  -p 8000:8000 -v cf-exporter-state:/var/lib/cf-exporter \
  ghcr.io/novasamatech/infra-tooling/cf-exporter:2.0.0 \
  /bin/sh -c 'IFS= read -r CF_EXPORTER_API_TOKEN; export CF_EXPORTER_API_TOKEN; exec python script.py'
```

Pin a version tag. `:latest` follows every release, and 2.0.0 changes every metric family and the scrape requirements; see the [CHANGELOG](CHANGELOG.md).

Other settings can be passed with `-e`, for example `-e CF_EXPORTER_BACKFILL_DAYS=7`.

## Development

Everything for running, checking and debugging goes through the [Makefile](Makefile); `make help` lists the targets. [Dockerfile.dev](Dockerfile.dev) uses `python:3.13-alpine`, matching the runtime image, and installs `requirements-dev.txt`. No host Python environment is needed.

Token-dependent targets take the token from `CF_EXPORTER_API_TOKEN` or, if that is unset, from the first line of the file `TOKEN_FILE`, and pass it to the container through stdin, so it is absent from container metadata and command lines. Settings of one machine can be kept in an untracked `local.mk` next to the Makefile:

```make
CONTAINER_ENGINE := podman --cgroup-manager=cgroupfs
TOKEN_FILE := /path/to/token
PORT := 18000
```

| Command | Purpose |
| --- | --- |
| `make ci` | Lint and offline tests, as in the repository CI |
| `make test` | Offline tests with coverage; `PYTEST_ARGS='tests/test_storage.py -k wiped --no-cov'` narrows the run |
| `make lint`, `make fmt` | Ruff checks; formatting and fixes |
| `make test-live` | Real Cloudflare API and process tests |
| `make stop-check` | Stop a running live check (`test-live`, `test-all`, `profile-live`) |
| `make profile-live` | Compares exported buckets of closed hours with independently read hourly totals; `PROFILE_ARGS='--hours 6'` |
| `make run` | The exporter in the dev image, container `cf-exporter-run`, database inside the container |
| `make status` | Readiness and health metrics of that exporter |
| `make scrape` | Its `/analytics` response, without consuming it |
| `make logs`, `make stop` | Its last log lines; stop it |
| `make image`, `make image-run` | Build and run the runtime image |
| `make test-image` | Build the image and check it end to end |
| `make lock`, `make outdated` | Pin dependencies from `requirements*.in`; list updates |
| `make coverage`, `make clean` | HTML coverage report; remove caches and dev images |

`make profile-live` exits with 1 if a zone's exported sums differ from the reference or a zone has no settings, and with 2 if no zone could be judged. Run it during business hours of Europe and North America as well, when Cloudflare answers at a reduced resolution most often.

Offline tests run against an in-memory fake of the Cloudflare API that stores single events and answers bucket queries the way Cloudflare does, including answers from a reduced table. They cover bucket acceptance and repair, the proof ranges, splitting and pagination, scheduling, the self-healing storage with real files, the HTTP endpoints with a real server, labels, the edge dataset, API error counters with real transport retries, and the checks of the validation tool and the image smoke test themselves. Live tests check every queried dataset, setting, dimension, filter and sort value against the schema, compare a closed hour with independent totals, read a busy bucket in parts, page through it by its dimensions, run the exporter as a process through a restart on the same database, and delete the database under the running process.

## License

This project is licensed under the [Apache License 2.0](../LICENSE).
