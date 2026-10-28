# subquery-db: Agent Entry Point

These instructions apply to `subquery-db/` and its descendants. Start here,
follow applicable repository instructions and the operator's current request,
and keep this file current when the workflow changes.

## Project Map

`sqdb` migrates SubQuery PostgreSQL databases logically, one database at a
time, with a four-phase restore built for index-heavy schemas. It runs in a
container; the host needs only make and Podman or Docker.

| File | Purpose |
| --- | --- |
| [README.md](README.md) | Problem, restore phases, operator workflow, config and harness |
| [config.example.yaml](config.example.yaml) | Annotated config; loaded by the unit tests |
| [sqdb/config.py](sqdb/config.py) | Strict YAML loading, validation, option inheritance |
| [sqdb/toc.py](sqdb/toc.py) | `pg_restore -l` parsing and the phase 2/4 lists |
| [sqdb/storage.py](sqdb/storage.py) | S3 multipart upload with abort, resumable reads, local dumps |
| [sqdb/proc.py](sqdb/proc.py) | Client tool supervision, stream threads, interruption |
| [sqdb/pg.py](sqdb/pg.py) | Connections, target inspection (`app` guard), cleanup, opt-in database creation |
| [sqdb/pipeline.py](sqdb/pipeline.py) | Per-database target checks, probe, backup, restore, verify, check |
| [sqdb/verify.py](sqdb/verify.py) | Catalog and row-count comparison |
| [sqdb/state.py](sqdb/state.py), [sqdb/log.py](sqdb/log.py) | Run state, logging with secret masking |
| [sqdb/cli.py](sqdb/cli.py) | Commands and the batch runner |
| [tests/](tests) | Offline unit tests, fake S3 in `conftest.py`, TOC sample in `data/` |
| [tests/harness/](tests/harness) | Harness topology, SubQuery-like fixture and seeder |
| [tests/e2e/](tests/e2e) | End-to-end tests against the harness |
| [Dockerfile](Dockerfile) | Stages `base`, `dev` (tests, legacy client) and `runtime` (last) |
| [Makefile](Makefile) | Operator, harness and development targets |
| [CHANGELOG.md](CHANGELOG.md), [.version](.version) | Release notes and image version (`sqdb/__init__.py` matches) |
| [../AGENTS.md](../AGENTS.md) | Repository rules, including the generic CI and its `ci` / `ci-integration` targets |

## Local Workflow

Run commands from `subquery-db/` (or `make -C subquery-db`). Everything runs in
containers: do not create host virtualenvs or install into system Python. Use
`CONTAINER_ENGINE='podman --cgroup-manager=cgroupfs'` without a systemd user bus.
Change direct dependencies in `requirements*.in` and run `make lock`; never edit
the generated `requirements*.txt` by hand.

| Command | Purpose |
| --- | --- |
| `make lint` / `make fmt` | Ruff checks / formatting |
| `make test` | Offline unit tests |
| `make test-e2e` | Harness end-to-end tests (reseeds the harness) |
| `make test-all` | Lint, unit and e2e tests, combined coverage (minimum 85%) |
| `make harness-up` / `harness-seed` / `harness-psql` / `harness-down` | Harness lifecycle |
| `make rehearse CONFIG=...` | Replay a config's topology in the harness |
| `make image` | Build the runtime image as `IMAGE:local` (`BUILD_ENGINE=buildah` works) |
| `make ci` / `make ci-integration` | The repository CI checks, runnable locally |

Operator targets and `rehearse` run `IMAGE:IMAGE_TAG`, by default the
published `latest`, pulled before each run. Use `IMAGE_TAG=local` after
`make image` to run local changes.

The repository CI (see [../AGENTS.md](../AGENTS.md)) runs `make ci` (lint, unit
tests) and `make ci-integration` (unit and e2e tests with the coverage minimum,
runtime image build, rehearsal of `config.example.yaml`) for every change to
this directory. It publishes `latest` and the version tag only when `.version`
changes on `main` and the checks pass: a release bumps `.version`,
`sqdb/__init__.py` and CHANGELOG together.

## Behavior To Preserve

- Keep the four restore phases and their order: pre-data; plain indexes on
  empty tables; data; remaining post-data. Phase 2 contains `INDEX` entries,
  `INDEX ATTACH` and `COMMENT ON INDEX` only for plain indexes; anything tied
  to a constraint stays in phase 4. Restore with `--no-owner --no-privileges
  --exit-on-error`. Do not move constraints before the data load.
- Use verbose TOC dependencies for the phase split. TOC display names are not
  escaped and can contain spaces; extract schema names from generated SQL
  before inspecting or cleaning a target.
- Bind progress to endpoints/users, archive generation/fingerprint and target
  database OID. New dumps invalidate restore/verify state. Keep the work-directory
  file lock and the target database advisory lock; do not resume phases with a
  different archive or reuse a weaker verification result.
- Keep the S3 key layout `<prefix><db>/<backup_name>.dump` (compatible with the
  earlier scripts) and the manifest beside it. An S3 object may only appear
  after `pg_dump` succeeded: complete the multipart upload after the exit code
  is known, abort it otherwise. Streamed restores use one job.
- Never pass passwords in argv or conninfo strings; client tools get
  `PGPASSWORD` in their environment. Every handler masks configured secrets.
  The config is plain YAML with inline passwords by design.
- Target databases and users exist by default (a managed service creates them
  through its API). Creating databases happens only with `--create-databases`;
  never make it the default or require an admin user.
- A target with schema `app` (or another schema the dump creates) is not empty:
  stop with an error before any dump or load. Never restore into it or reuse an
  unrecorded dump without the explicit flag. `--clean-target` drops only those
  schemas; never use `DROP OWNED` (it revokes the group user's privileges on
  other databases).
- Roll back cleanup if CASCADE removes dependent objects outside the selected
  schemas. Keep existing local dumps until the replacement dump succeeds.
- `--clean-target` fails closed: when the source cannot be reached to rule out
  that the target is a source database on the same server, refuse to clean.
- Keep long-idle connections alive on PostgreSQL 14+: sqdb connections set
  `idle_session_timeout = 0`, and pg_restore gets `-c idle_session_timeout=0`
  (its `-j` leader waits idle while workers load data).
- `check` stays read-only apart from the rolled-back DDL probe and an aborted
  multipart upload that tests write access.
- Keep the harness provider-neutral and close to a managed service: the seeder
  creates users and empty target databases, group users have no CREATEDB, no
  admin unless a test needs one, trusted extensions only, objects created by
  the group user.

## Validation And Scope

For behavior changes, add regression coverage (unit tests for parsing and
storage logic, e2e tests for anything touching PostgreSQL or S3) and run
`make lint`, `make test` and `make test-e2e` (or `make test-all`). Run
`make image && make rehearse CONFIG=config.example.yaml IMAGE_TAG=local` after
changing the runtime image or operator targets. Test counts and coverage are outcomes, not requirements.

This repository is public: keep provider, host, network and capacity details of
real environments out of code, docs, tests, commits and PR texts. Real configs
live only under `work/` (git-ignored) or outside the repository.

Do not commit or push without an explicit request. Commits use the configured
Git identity without AI/agent attribution or `Co-authored-by` trailers.
