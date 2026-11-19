# cf-exporter: Agent Entry Point

These instructions apply to `cf-exporter/` and its descendants. Start here before
working on the exporter, and follow applicable repository instructions and the
operator's current request. Keep this file current when the workflow changes.

## Project Map

This is a single-script Python service that exports Cloudflare HTTP analytics in
closed 5-minute buckets through a Prometheus endpoint, with its state in a local
SQLite database. It is separate from the Go-based lablabs cloudflare-exporter;
establish which service produced an error before fixing it.

| File | Purpose |
| --- | --- |
| [README.md](README.md) | Behavior, metrics, scraping, storage, configuration and usage |
| [script.py](script.py) | API client, queries, window reads, bucket verification, storage, scheduling loop, HTTP endpoints |
| [Makefile](Makefile) | Every task for running, checking and debugging |
| [pyproject.toml](pyproject.toml) | Pytest, coverage and Ruff configuration |
| [tests/conftest.py](tests/conftest.py) | Fake Cloudflare API built from single events, reduced-answer transforms, shared fixtures |
| [tests/test_script.py](tests/test_script.py) | Configuration, logging, API client with localhost transport checks, settings and plans, queries, `main()` |
| [tests/test_slicing.py](tests/test_slicing.py) | Reading a window: proof ranges of the totals, splitting, pagination, response validation |
| [tests/test_verification.py](tests/test_verification.py) | Bucket acceptance: proof of the totals, reduced and empty answers, repairs, consistency, rechecks |
| [tests/test_windows.py](tests/test_windows.py) | Scheduling: settling, priorities, history, pacing, retries, failures, zone changes |
| [tests/test_storage.py](tests/test_storage.py) | Self-healing SQLite storage with real files, delivery and pruning |
| [tests/test_readiness.py](tests/test_readiness.py) | `/analytics` exposition and delivery, `/readyz`, health metrics, concurrent HTTP |
| [tests/test_labels.py](tests/test_labels.py) | Label configuration in queries, aggregation and the exposition |
| [tests/test_edge.py](tests/test_edge.py) | Client-facing edge dataset, its filter, plan support and types |
| [tests/test_api_errors.py](tests/test_api_errors.py) | Cloudflare API response/error counters and real transport retries |
| [tests/test_profile.py](tests/test_profile.py) | The validation tool and the image smoke checks themselves |
| [tests/test_live.py](tests/test_live.py) | Real API: schema, closed-hour sums, pagination, process restart, database wipe |
| [tests/image_smoke.py](tests/image_smoke.py) | Container end-to-end check, standard library only |
| [tests/profile_live.py](tests/profile_live.py) | Live validation of exported buckets against independent hourly totals, with measurements |
| [Dockerfile](Dockerfile), [.dockerignore](.dockerignore) | Runtime image and build context |
| [Dockerfile.dev](Dockerfile.dev) | Cached Python 3.13 development image |
| [CHANGELOG.md](CHANGELOG.md), [.version](.version) | Release notes and container version |

## Local Workflow

Everything for running, checking and debugging is a Makefile target. If a step
has no target, add one instead of running a long ad-hoc command. Run the targets
from `cf-exporter/` or with `make -C cf-exporter`. All Python tasks run in a
Python 3.13 Alpine container.

- Do not create host virtualenvs or install into system Python.
- The local dev image `cf-exporter-dev` is tagged by a hash of the pinned
  requirements and Dockerfile.dev. It is never published.
- Sources mount at `/src`; Podman uses `--userns=keep-id`, Docker the host UID/GID.
- Settings of one machine belong in the untracked `local.mk` next to the Makefile:
  `CONTAINER_ENGINE` (`podman --cgroup-manager=cgroupfs` without a systemd user
  bus, or `docker`), `TOKEN_FILE` and `PORT`.
- Token-dependent targets take the token from `CF_EXPORTER_API_TOKEN` or the
  first line of `TOKEN_FILE` and send it through stdin, never container metadata
  or command arguments. Never print it.
- Change direct dependencies in `requirements.in` or `requirements-dev.in`, then
  run `make lock`. Do not edit generated `requirements*.txt` files by hand.

| Command | Purpose |
| --- | --- |
| `make help` | List supported tasks |
| `make ci` | Repository CI checks (lint + offline tests, see `../AGENTS.md`) |
| `make lint`, `make fmt` | Ruff checks; formatting and lint fixes |
| `make test` | Offline tests with coverage; `PYTEST_ARGS` narrows the run |
| `make coverage` | Offline tests and HTML coverage report |
| `make test-live` | Live API and process checks |
| `make stop-check` | Stop a running live check (`test-live`, `test-all`, `profile-live`), which runs as container `cf-exporter-check` |
| `make test-all` | Offline and live checks |
| `make profile-live` | Compare exported buckets with Cloudflare's hourly totals (`PROFILE_ARGS`) |
| `make run` | Run the service in the dev image as container `cf-exporter-run` |
| `make status`, `make scrape`, `make logs`, `make stop` | Health, `/analytics` without consuming, logs and stop of that container |
| `make image`, `make image-run` | Build the image; run it in the foreground |
| `make test-image` | Build and check the container end to end |
| `make lock`, `make outdated` | Pin dependencies; list updates |
| `make clean` | Remove caches, test output and all local dev images |

Settings such as limits and disabled labels reach `run`, live, validation and
image targets from the environment by name; live tests and the validation tool
build the client with `script.create_api(script.parse_env())`. For live checks,
ask the operator to provide any missing execution configuration. If a required
check cannot run, report that explicitly. `SMOKE_TIMEOUT` controls the smoke
deadline.

## Behavior To Preserve

- Hard default contract: exported numbers are Cloudflare's real numbers.
  Completeness and correctness take priority over memory, request count and
  speed. Never add automatic caps, quotas, deadlines, label removal or top-N
  selection. A bucket is exported late rather than wrong, and never partially.
- The unit is a closed bucket of `BUCKET_SECONDS` aligned to the Unix epoch. A
  sample is the count of one bucket and carries the bucket end as its timestamp.
  Never export running totals, "since midnight" values or process counters for
  analytics, and never let a value depend on when the exporter was scraped.
- Families are `cf_visits_5m`, `cf_requests_5m`, `cf_cache_requests_5m`,
  `cf_edge_requests_5m`, `cf_zone_requests_5m` and `cf_zone_visits_5m`, all
  gauges. Preserve names and label names/order unless the operator requests a
  breaking change. Zone totals have a sample for every accepted bucket, also 0;
  the other families only for label sets with events.
- Export every label by default. `CF_EXPORTER_DISABLED_LABELS` disables labels
  per family; `zone_name` cannot be disabled. Keep the catalog (`METRIC_LABELS`,
  `DATASETS`) separate from an instance's `Schema`, and reject unknown metrics,
  labels and malformed JSON at startup. Never disable labels automatically or
  ship deployment-specific choices.
- Use current, non-deprecated Cloudflare APIs with GraphQL variables only. Keep
  the live schema checks aligned with every queried dataset, setting, dimension,
  filter and sort value. Discover each zone's fields, page size, `maxDuration`
  and `notOlderThan`; query only supported dimensions.
- One request reads a window with the aliased selections `groups` (union of the
  exported dimensions of visits, requests and cache), `totals`, `edge` and
  `edge_totals`, each grouped by `datetimeFiveMinutes`. A full page of a
  selection with dimensions is split on bucket boundaries, then within the
  bucket down to one second, which is paginated by the complete dimension tuple
  with a lexicographic filter. Counts are never pagination keys. A nonadvancing
  page or a missing cursor dimension fails the read. A part of a split read
  answered at a reduced resolution is asked for again at once (`PART_READS`),
  with a page size no other request of the read uses; a page that was split is
  discarded and says nothing about how the selection was answered.
- **An empty answer proves nothing.** Under load Cloudflare answers a selection
  from a table of every tenth or hundredth event: its groups have a sample
  interval of 10 or more, and buckets with few events have no group at all. Each
  selection is answered on its own. Never accept a bucket on agreement between
  selections alone, and never take "no groups" as "no traffic" without proof.
- The proof rule (`answer_state`, `evaluate`, `Exporter._proof`,
  `_settle_proof`): a totals selection is trusted only if its one answer
  contains a group with a sample interval below `SAMPLED_INTERVAL`. Totals are
  read with `PROOF_SECONDS` around the window for that. Without any group they
  are read over `LONG_PROOF_SECONDS`; without one there in `EMPTY_LOOKS` answers
  in a row the zone is idle. "In a row" is literal: any other answer between
  them starts the count again. Known events are the accepted buckets with
  requests (`buckets.requests`, `client_requests`, kept after delivery) and the
  newest event of any answer (`Exporter._last_event`). `_proof` widens the range
  to the nearest known event within the long range of the window; an answer
  without groups over a range with a known event left them out: state
  "missing", a reduced answer, never a look at an idle zone. A recheck always
  has its own bucket in range. Never extend a range towards a month: such
  ranges are always answered reduced. Totals use the plan's own page size and
  one answer; an answer in several parts proves nothing.
- A bucket is accepted only with proven totals, groups that add up to them
  (requests and visits; edge requests to edge totals), no group at a reduced
  resolution unless its selection is proven full, no part of it from a reduced
  answer (`ReadStats.reduced`: among the full parts of a split bucket the
  sample interval of a reduced part is diluted), and edge totals not above
  totals. Everything else is read again: at once (`REPAIR_ROUNDS`, another page
  size each time, because Cloudflare caches by query and variables; the
  `LIMIT_VARIANTS` sizes never go below half a page) and then with
  `RETRY_DELAYS`. Only the same unverifiable answer for
  `UNVERIFIED_HOLD_SECONDS` is accepted, counted as `accepted_unverified`, and
  only if it is `holdable`: totals with groups, all at a high interval, one for
  the bucket itself, that add up. An empty, "missing", split or contradicting
  answer, or one without the bucket, is never accepted that way; it stays
  pending until `abandoned`.
- Restrict only the edge selections to client requests with `EYEBALL_FILTER`;
  the other selections keep every request source. Keep both edge and origin
  status; an edge status is never `unknown`, and origin `0` never stands in for
  it. Edge dimensions are type-checked (`DIMENSION_TYPES`), statuses as unsigned
  16-bit integers. Decide edge support from `availableFields`; an unsupported
  plan skips only that dataset, reports `cf_exporter_dataset_available` `0` and
  logs a warning. An API error is never taken as missing support.
- Keep unavailable dimensions as `unknown` and empty referers as `direct`.
  Validate response structure: a missing or malformed selection or group fails
  the read rather than producing an empty result.
- Read fresh buckets first (after `SETTLE_SECONDS`), then rechecks, then history
  newest first, one read at a time. Fresh and recheck windows stay within the
  plan's `max_window_buckets`. History is paced and waits while too many
  samples are undelivered; its pause is waited in pieces of at most
  `LOOP_SECONDS` and never past the next bucket's settling, so fresh buckets and
  rechecks never wait for it. Every accepted young bucket is read once more
  after its hour (`recheck_time`), and a changed bucket is served again with the
  same timestamps, but only with the labels it was served with (`layout`):
  otherwise its recheck is cancelled (`recheck_skipped`, `skip_rechecks`), or a
  sum over labels would count it twice.
- `CF_EXPORTER_BACKFILL_DAYS` shortens `ZonePlan.history_seconds` only; every
  query range, proof ranges included, is bounded by `reach_seconds`.
- The storage heals itself and needs no operator: a missing, corrupt, outdated,
  removed, replaced or failing database of the exporter is recreated and filled
  again from Cloudflare; an unusable path falls back to memory and is retried.
  Fail closed for what is not ours: only a file that is recognizably the
  exporter's (`recognizably_ours`: its application id in the file header, or
  zero bytes only) is ever deleted, and the schema is created before the switch
  to the write-ahead log so that the header carries the id. A lock held by
  another process and a full disk never cause a wipe (SQLite reports extended
  codes; compare the primary one, `storage_busy`, `storage_full`); while the
  disk is full nothing is read (`Storage.full`). A programming error must not
  be mistaken for a broken database.
- A database is used only after a write to it worked (`Storage._probe`): one
  that can be read but not written would otherwise be reopened from memory
  every retry interval, dropping the state in memory each time. When the file
  was replaced while running, remove only the write-ahead files recorded as
  this database's own; others came with the new file and hold its data.
- `Storage.ensure` must leave no gap in a zone's range: a bucket without a row
  is never read, and no metric shows it.
- `/analytics` serves whole undelivered buckets within the response budget:
  first those that still need a response, the newest first, then those only
  waiting for `DELIVERY_SECONDS`. A bucket that had its responses must never
  keep another out of a response. Only a complete response to `?consume=1`
  counts as a delivery, and only for a bucket not accepted again meanwhile; a
  bucket is delivered after `DELIVERY_RESPONSES` of them and
  `DELIVERY_SECONDS` since acceptance. Undelivered buckets expire
  `UNDELIVERED_KEEP_SECONDS` after their acceptance, never by the bucket's own
  age: a dataset can reach further back. Reading the endpoint without the
  parameter must never consume data.
- `/metrics` and every other path serve health metrics only, without timestamps.
  Keep `READ_RESULTS` and `API_ERRORS` fixed, exported from zero and mirrored in
  `image_smoke.py`. Labels never carry URLs, messages or credentials. Only
  `main()` registers collectors; imports must not install handlers or register
  global metrics.
- `/readyz` means the process can serve: usable storage and one zone listing.
  It must not depend on data freshness or on Cloudflare's current state, so that
  a slow Cloudflare cannot make every replica unready together. No liveness use.
- Instances are independent and must stay so: no coordination, identical series
  from identical buckets.
- Count every Cloudflare API attempt once in `cf_exporter_api_responses_total`
  and `cf_exporter_api_errors_total`: retried attempts through
  `BoundedRetry.on_retry`, the final one in `CloudflareAPI._request`. Wait for
  any `Retry-After` unless `CF_EXPORTER_MAX_RETRY_AFTER_SECONDS` is set; do not
  inherit urllib3's six-hour cap. A failed read pauses its zone, an explicit
  rate-limit rejection pauses every request, settings and the zone listing
  included; neither discards accepted buckets.
- One bucket that cannot be read must not hold back its zone
  (`Exporter._postpone`, `_window`): a window is tried again once as it is; a
  failed history read of the zone's full window size halves its windows
  (`window_ceilings`), a smaller one changes nothing; a window that fails twice is read
  bucket by bucket; a bucket that fails alone waits `RETRY_DELAYS` while the
  rest of the zone is read. Failed reads are counted per bucket (`errors`) and
  make it `abandoned` like failed verifications. After `OUTAGE_FAILURES` failed
  reads of a zone in a row, the first read that works releases its postponed
  buckets; it must not reset their error counts, or a bucket that really
  cannot be read would be mixed into windows again.
- Cloudflare's limit of 300 GraphQL queries in 5 minutes is shared by every
  instance and user of the token and counts each selection of a request. A
  request that meets it waits for minutes with the fresh buckets behind it, so
  only history is paced (`HISTORY_INTERVAL_SECONDS`, sized for two instances)
  and slowed down after rejections (`Exporter._watch_rate_limit`). Keep live
  checks cheap for the same reason: a read with pages of a few groups costs
  hundreds of queries, and under load its small parts come back empty from the
  reduced table, which nothing can tell from quiet parts.
- A zone missing from the listing keeps its stored state and its plan and
  in-memory state for `ZONE_KEEP_SECONDS` but is not read; also a zone only the
  database knows after a restart (`Storage.zone_ids`). An incomplete listing
  must not make the exporter read and serve a zone's history again. Health
  metrics show the listed zones only.
- Keep the process running after failures, including bugs in one read. Use the
  injected clock for scheduling. Preserve clean signal exits; close the database
  on exit.

## Validation And Scope

Keep this exporter independent of the operator's infrastructure and consumers.
Validate through the actual exposition, the offline fake API, live API checks,
`make profile-live` and the container smoke test. Do not add a metrics database,
visualization service, query-engine harness, dashboard queries or
consumer-specific test configuration unless the operator explicitly requests
them.

For behavior changes, add focused regression coverage and run `make ci`. The
configured coverage minimum is 90%; test counts and exact coverage are outcomes
of the current run, not permanent requirements. Test destructive storage
scenarios in pytest with `tmp_path`, never on real files.

Run `make test-live` and `make profile-live` when changing API queries, response
handling or bucket verification, preferably also during business hours of
Europe and North America, when reduced answers are frequent. A change is not
verified while `profile-live` reports a mismatch. Run `make test-image` when
changing runtime dependencies, packaging or container execution. Keep README and
CHANGELOG aligned with resulting behavior, and keep release notes and `.version`
consistent when preparing a release.

Review the final diff and run `git diff --check`. Preserve existing worktree
changes. Changes to the separate `infrastructure` repository require explicit
operator agreement; read-only diagnosis does not authorize deployment edits.

Do not commit or push without an explicit request. For commits, use the configured
Git author and committer identity without AI/agent attribution or agent
`Co-authored-by` trailers. If asked to commit without a narrower scope, include all current
repository changes: tracked edits and non-ignored new files, including changes
made before your work.
