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
| [tests/test_live.py](tests/test_live.py) | Real API, schema and process checks |
| [tests/image_smoke.py](tests/image_smoke.py) | Container lifecycle and metrics smoke check |
| [Dockerfile](Dockerfile), [.dockerignore](.dockerignore) | Runtime image and build context |
| [CHANGELOG.md](CHANGELOG.md), [.version](.version) | Release notes and container version |

## Local Workflow

Run the commands below from `cf-exporter/`, or use `make -C cf-exporter` from the
repository root. Use Python 3.13 and manage environments through the Makefile.

- `venv/` contains runtime dependencies and is used by `make run`.
- `venv-dev/` contains development dependencies and is used for tests and lint.
- `.venv-lock/` is a temporary resolver environment managed by `make lock`.
- Do not create environments outside this directory or install into system Python.
- Change direct dependencies in `requirements.in` or `requirements-dev.in`, then
  run `make lock`. Do not edit generated `requirements*.txt` files by hand.

| Command | Purpose |
| --- | --- |
| `make help` | List supported tasks |
| `make venv venv-dev` | Prepare both local environments |
| `make lint` | Ruff lint and formatting checks |
| `make fmt` | Format and apply lint fixes |
| `make test` | Offline tests with coverage |
| `make coverage` | Offline tests and HTML coverage report |
| `make test-live` | Live API and process checks |
| `make test-all` | Offline and live checks |
| `make run` | Run the service locally |
| `make image` | Build the image using Podman by default |
| `make image-run` | Run the image in the foreground |
| `make test-image` | Build and check the container through a full collection cycle |
| `make lock` | Resolve and pin runtime and development dependencies |
| `make outdated` | List dependency updates |
| `make clean` | Remove local environments and generated test output |

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
- Use GraphQL variables and one aliased analytics query for both metrics. The
  settings lookup is a separate HTTP request; document this distinction accurately.
- Keep unavailable dimensions as `unknown`, empty referers as `direct`, and omit
  zero-valued groups. Order capped results largest first and warn at the limit.
- Validate response structure. Missing or malformed data must report a failed
  collection rather than a successful empty result.
- Publish complete snapshots atomically after collection. Remove stale series;
  omit a failed zone's analytics for that cycle and preserve other zones' results.
- Keep the process running after collection failures and expose failures through
  health metrics. Imports must not install handlers or register global metrics.
- Use monotonic time for durations and scheduling. Preserve clean signal exits.

## Validation And Scope

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
