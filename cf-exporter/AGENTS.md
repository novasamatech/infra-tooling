# cf-exporter: Agent Entry Point

These instructions apply to `cf-exporter/` and its descendants. Start here before
working on the exporter, and follow applicable repository instructions and the
operator's current request. Keep this file current when the workflow changes.

## Project Map

This is a single-script Python service that exports Cloudflare HTTP analytics
through a Prometheus endpoint. It is separate from the Go-based lablabs
cloudflare-exporter; establish which service produced an error before fixing it.

| File | Purpose |
| --- | --- |
| [README.md](README.md) | Behavior, metrics, configuration and usage |
| [script.py](script.py) | API client, query construction, snapshots, collector and main loop |
| [Makefile](Makefile) | Environment setup and routine commands |
| [pyproject.toml](pyproject.toml) | Pytest, coverage and Ruff configuration |
| [tests/conftest.py](tests/conftest.py) | Fake Cloudflare responses and shared fixtures |
| [tests/test_script.py](tests/test_script.py) | Offline regression tests, including localhost transport checks |
| [tests/test_slicing.py](tests/test_slicing.py) | Time-filtered slicing, caps, totals and midnight regressions |
| [tests/test_windows.py](tests/test_windows.py) | Snapshot window contract and actual Prometheus rendering |
| [tests/test_edge.py](tests/test_edge.py) | Client-facing edge dataset, its filter and dataset availability |
| [tests/test_live.py](tests/test_live.py) | Real API, schema and process checks |
| [tests/image_smoke.py](tests/image_smoke.py) | Container lifecycle and metrics smoke check |
| [tests/profile_live.py](tests/profile_live.py) | Live resource measurements and completed-day sum validation |
| [Dockerfile](Dockerfile), [.dockerignore](.dockerignore) | Runtime image and build context |
| [Dockerfile.dev](Dockerfile.dev) | Cached Python 3.13 development image |
| [CHANGELOG.md](CHANGELOG.md), [.version](.version) | Release notes and container version |

## Local Workflow

Run the commands below from `cf-exporter/`, or use `make -C cf-exporter` from the
repository root. All development commands run in a Python 3.13 Alpine container.

- Do not create host virtualenvs or install into system Python.
- The local dev image `cf-exporter-dev` is tagged by a hash of the pinned
  requirements and Dockerfile.dev. It is never published.
- Sources mount at `/src`; Podman uses `--userns=keep-id`, Docker the host UID/GID.
- Use `CONTAINER_ENGINE=docker` for Docker, or
  `CONTAINER_ENGINE='podman --cgroup-manager=cgroupfs'` without a systemd user bus.
- Token-dependent targets send `CF_EXPORTER_API_TOKEN` through stdin, never
  container metadata or command arguments.
- Change direct dependencies in `requirements.in` or `requirements-dev.in`, then
  run `make lock`. Do not edit generated `requirements*.txt` files by hand.

| Command | Purpose |
| --- | --- |
| `make help` | List supported tasks |
| `make dev-image` | Prepare the cached Python 3.13 development image |
| `make lint` | Ruff lint and formatting checks |
| `make fmt` | Format and apply lint fixes |
| `make test` | Offline tests with coverage |
| `make coverage` | Offline tests and HTML coverage report |
| `make test-live` | Live API and process checks |
| `make test-all` | Offline and live checks |
| `make run` | Run the service in the dev image; `PORT` defaults to 8000 |
| `make image` | Build the image using Podman by default |
| `make image-run` | Run the image in the foreground |
| `make test-image` | Build and check the container through a full collection cycle |
| `make lock` | Resolve and pin dependencies in a throwaway Python 3.13 container |
| `make outdated` | List dependency updates |
| `make profile-live` | Live cycle, memory and exposition measurements (`PROFILE_ARGS`) |
| `make clean` | Remove caches, test output and all local dev images |

For live checks, ask the operator to provide any missing execution configuration.
If a required check cannot run, report that explicitly. Override `PORT` when the
default container port is occupied; `SMOKE_TIMEOUT` controls the smoke deadline.

## Behavior To Preserve

- Preserve public metric names and label names/order unless the operator requests
  a breaking change. `cf_visits` and `cf_requests` are gauges, not counters.
- Use one UTC window per cycle, from midnight to the cycle start, formatted as
  RFC3339 with `Z`. Handle an empty window at midnight without an invalid query.
- Use current, non-deprecated Cloudflare APIs. Keep the live schema checks aligned
  with every queried dataset, setting, dimension and sort value.
- Discover each zone's available fields and page size. Query only supported
  dimensions, omit an empty dimensions selection, and respect `maxPageSize`.
- Use GraphQL variables and aliased analytics selections for visits, requests,
  cache, edge and dimensionless totals. Settings use a separate HTTP request. Sliced
  retries use additional requests for only the full selections.
- Export all nonzero groups by default with disjoint, half-open time slices.
  Reuse slice-count hints per zone/selection until UTC midnight; a failed slice
  fails that zone. Stop at 60s (a trailing partial minute can be shorter) or when
  slicing would exceed the cycle's GraphQL budget: `cycle_query_budget()`, shared
  by all zones after each zone's `BASELINE_SELECTIONS`, plus `MAX_SLICE_REQUESTS`
  per selection and zone. Count selections, not HTTP requests, as Cloudflare does.
  Expose residual truncation and support the optional `CF_EXPORTER_GROUP_LIMIT`.
- Keep cache metrics dynamic across all hosts, including `origin_response_status`.
  Keep dimensionless zone totals and per-dataset group/truncation metrics aligned.
- Restrict only the edge dataset (`cf_edge_requests`) to client requests with
  `EYEBALL_FILTER`, in its first selection and every slice; the other datasets and
  zone totals keep every request source. Never compare edge sums with unfiltered
  totals, and fetch no production client total. Keep both edge and origin status;
  an edge status is never `unknown`, and origin `0` never stands in for it.
  Validate edge dimensions even in zero-valued groups and discarded probe/parent
  pages; status values must match the API's unsigned 16-bit integer type.
  Keep strict type policies dataset-scoped (`DIMENSION_TYPES`) so shared field
  names do not silently change older families' label conversion.
  Validation totals must not reuse the production filter constant as their oracle.
- Decide dataset support from `availableFields` (`DATASET_FIELDS`). An unsupported
  dataset is not queried, reports `cf_exporter_dataset_available` `0` without
  group or truncation series, logs a warning and leaves the zone successful. An
  API error is never taken as missing support.
- Keep unavailable dimensions as `unknown`, empty referers as `direct`, and omit
  zero-valued groups. Order capped results largest first and warn on actual cuts;
  decide cuts from the pages only (a zero visits tail is complete; a configured cap
  probes one extra group), never from the separately fetched zone totals.
- Validate response structure. Missing or malformed data must report a failed
  collection rather than a successful empty result. Validate every fetched group
  once before splitting or capping; aggregation consumes only validated groups.
- Publish complete snapshots atomically after collection. Remove stale series;
  omit a failed zone's analytics for that cycle and preserve other zones' results.
- Export `cf_exporter_snapshot_window_end_seconds` only for successful zones,
  with `zone_name` and RFC3339 `window_start` labels from that same snapshot.
  Its value is the exclusive API window end, not collection completion time.
  Historical profiling must preserve these bounds when rendering a past day and
  override only the visibility gate, not the collector's family enumeration.
- Omit all day-scoped families when a snapshot's window belongs to an earlier
  UTC day, including if collection crossed midnight. Always serve zone/cycle health.
- Keep the process running after collection failures and expose failures through
  health metrics. Imports must not install handlers or register global metrics.
- Never wait longer than `MAX_RETRY_AFTER_SECONDS` for a `Retry-After`. After an
  HTTP 429 or a GraphQL budget error, skip the cycle's remaining zones and report
  them as failed.
- Move the last cycle before UTC midnight earlier when it would not finish
  `DAY_CLOSE_MARGIN_SECONDS` before midnight; never publish a finished day after it.
- Use monotonic time for durations and scheduling. Preserve clean signal exits.

## Validation And Scope

Keep this exporter independent of the operator's infrastructure and consumers.
Validate metric semantics through the actual Prometheus exposition, existing
offline fixtures, live API checks and the container smoke test. Do not add a
metrics database, visualization service, query-engine harness, dashboard queries
or consumer-specific test configuration/dependencies unless the operator
explicitly requests them. Reading and checking the exported samples is sufficient
for exporter-contract regression coverage.

For behavior changes, add focused regression coverage and run `make lint` and
`make test`. The configured coverage minimum is 90%; test counts and exact coverage
are outcomes of the current run, not permanent requirements.

Run relevant live checks when changing API queries or response handling. Run
`make test-image` when changing runtime dependencies, packaging or container
execution. Keep README and CHANGELOG aligned with resulting behavior, and keep
release notes and `.version` consistent when preparing a release.

Review the final diff and run `git diff --check`. Preserve existing worktree
changes. Changes to the separate `infrastructure` repository require explicit
operator agreement; read-only diagnosis does not authorize deployment edits.

Do not commit or push without an explicit request. For commits, use the configured
Git author and committer identity without AI/agent attribution or agent
`Co-authored-by` trailers. If asked to commit without a narrower scope, include all current
repository changes: tracked edits and non-ignored new files, including changes
made before your work.
