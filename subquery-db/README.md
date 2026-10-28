# Logical Migration of SubQuery Databases Between Managed PostgreSQL Instances

We run on managed PostgreSQL platforms that already provide low-impact, incremental, full-instance backups (e.g., snapshots/PITR with WAL) suitable for disaster recovery. However, these same platforms typically restrict superuser/OS-level access and WAL/archive configuration, which prevents deploying many open-source solutions that implement custom backup logic at the cluster or filesystem layer.

Our operational need is different:
* We co-locate many unrelated databases in a single instance and must back up and restore them independently (per-database selection, mobility between instances, targeted rollbacks/migrations).
* That implies using logical backups of individual databases (schema + data) for portability and selectivity—not cloning the entire instance.
* Off-the-shelf OSS tools that support logical backups often assume conventional restore ordering (bulk data load → post-data index builds) and/or require privileges unavailable in managed environments.
* Our datasets have a disproportionately large index footprint, so a standard, one-shot index build after data load creates unsustainable resource spikes and tends to fail under managed constraints.

Therefore, we need a provider-agnostic, automation-friendly workflow that:
* Works with logical per-database dumps without superuser/WAL access.
* Decouples databases so each can be backed up and restored on its own schedule.
* Orchestrates a non-typical restore order tailored to large indexes to keep resource usage predictable and avoid monolithic index build failures.

This approach complements the platform’s full-instance DR (snapshots/PITR). It is not a replacement for cluster-wide recovery; it fills the gap for selective, per-database migrations and restores in privilege-restricted, managed PostgreSQL environments.

## Why Databases Are Reloaded, Not Copied

A managed (cloud) PostgreSQL service gives no access to the server's data files.
Databases cannot be moved to another instance as files, and allocated storage
can grow but cannot shrink. So moving databases to another instance means
reloading them logically: dump schema and data, restore both on the target, and
rebuild every index there. Typical reasons for such a move:

* a new PostgreSQL major version;
* less storage after many databases were dropped;
* moving single databases between instances.

That index rebuild is where a standard restore of SubQuery databases breaks, as
described below. The four-phase restore exists because of it.

# Problem Statement

The standard logical DB restore flow (pre-data → data → post-data) fails after many hours during post-data index creation on very large indexes:
* Index pages are not dumped in logical database backups; indexes are rebuilt on target.
* Building massive B-tree indexes after bulk table load can trigger extreme CPU/IO/memory usage and eventual timeouts/failures, even with:
  * Larger instance sizes/resources,
  * Longer timeouts,
  * Available tuning (where permitted).

## Solution: Four-Phase Restore (Pre-Create Empty Indexes)

We restructured the restore into four explicit phases:
1. Create schema only. Restore DDL for schemas, tables, types, etc. (no data, no constraints, no indexes).
2. Create empty indexes on empty tables. Issue CREATE INDEX statements before loading data, so indexes exist but are empty.
3. Load table data. Use pg_restore --data-only. As rows are inserted, indexes are maintained incrementally.
  * Trade-off: Slower ingest vs bulk load without indexes.
  * Benefit: Avoids the single, massive post-data index build that was failing.
4. Apply remaining post-data (excluding indexes). Create constraints, FKs, triggers, and other post-data objects except indexes (already created in Phase 2).

## Why This Works

It transforms a monolithic index build into incremental maintenance during inserts, greatly reducing peak resource spikes.
Creating constraints before data load slows ingestion even more; deferring them to Phase 4 is beneficial.

Two refinements apply to schemas outside plain SubQuery projects: an index that
belongs to a constraint (for example the primary key of a partitioned table) is
created with its constraint in phase 4, together with its partition attachments
and comments; and object names that merely contain the word `INDEX` do not move
an entry to phase 2.

# sqdb: the migration tool

`sqdb` (Python, run in a container) implements the four-phase restore for one
database or for a whole instance described in a YAML config. It replaces the
earlier `migrate_db_s3.sh` and `migrate_db_local.sh` scripts and keeps the
S3 key layout, so existing S3 dumps can still be restored.

Features:

* **One config for many databases.** Databases are grouped by credentials: a
  group has one user and password, used on the source and (unless overridden)
  on the destination. Targets can be renamed; options are inherited
  defaults → group → database.
* **S3 or local storage.** S3 streams the dump with no local disk: the dump is
  uploaded as it is produced and restored straight from the object. Local
  storage keeps a directory (parallel `pg_dump -j`/`pg_restore -j`) or a single
  file in the work directory.
* **Safe uploads.** Uploads use the multipart API directly. The object only
  appears after `pg_dump` exits successfully; a failed or interrupted dump aborts
  the upload instead of leaving a truncated archive. Part sizes follow the
  source database size and grow if a dump turns out larger; the upload aborts
  if it still exceeds the configured part limit. Downloads resume from the failed offset, pinned
  to the object's ETag.
* **Existing targets, never overwritten.** Target databases and their users are
  expected to exist: a managed database service creates them through its own
  API. As in the earlier scripts, a target that has schema `app` is not empty,
  and the restore stops with an error instead of loading into it. Creating
  missing databases is an opt-in (`--create-databases`).
* **Fail before spending hours.** `check` connects with every group's
  credentials, compares server and client versions, reports database sizes and
  their total, target state and storage access, and runs a *DDL probe*: the
  dump's schema, indexes and constraints are applied to the real target inside
  a transaction that is rolled back.
* **Verification.** After a restore, `verify` compares source and target
  catalogs (schemas, tables, columns, indexes, constraints, triggers, routines,
  types, views, sequences, comments), checks that all target indexes are valid
  and compares exact row counts.
* **Restartable.** A state file records dumps, finished phases and checks.
  Reruns skip finished work, refuse to reuse unrecorded dumps or to restore into
  non-empty targets unless told to, and `--clean-target` drops what a failed
  attempt left behind.

## Requirements

* `make` and Podman (or Docker with `CONTAINER_ENGINE=docker`). Everything else,
  including the PostgreSQL client tools (17 by default, `PG_MAJOR`), runs in the
  runtime image.
* Network access from the host to the source and destination servers and to
  the object storage. Operator targets use host networking (`NETWORK=host`).
* The client major version must be at least the newest server major version
  (`check` verifies it); the target server must not be older than the source.
* Writers to the source databases (for example indexers) should be stopped
  before the dump: the dump is a consistent snapshot, and `verify` compares row
  counts with the live source.

## Runtime image

The repository's CI builds the image for `linux/amd64` and `linux/arm64` and
publishes `ghcr.io/novasamatech/infra-tooling/subquery-db:latest` and
`:<version>` whenever `.version` changes on `main` and the checks pass. Operator targets run
`IMAGE_TAG=latest` by default and pull it before every run (`PULL=always`);
`IMAGE_TAG=1.0.0` pins a release.

To test local changes, build the same runtime stage from these sources and
select it; a local build is never pulled:

```sh
make image                            # podman build; BUILD_ENGINE=buildah or docker also work
make check IMAGE_TAG=local
make rehearse IMAGE_TAG=local
```

## Operator workflow

All commands run from this directory; `CONFIG` defaults to `work/config.yaml`
and `WORK` to `work/` (both git-ignored). Extra CLI flags go in `ARGS`.

```sh
mkdir -p work && cp config.example.yaml work/config.yaml
chmod 600 work/config.yaml             # edit the config
make validate                         # offline: config errors and the plan
make rehearse                         # replay the config's topology in the local harness
make check                            # read-only preflight against the real servers
make migrate                          # target checks, probe, dump, restore, verify per database
make status                           # progress recorded in WORK
```

Run long migrations in `tmux` or `screen`. Useful arguments:

```sh
make migrate ARGS='--group accounts --parallel 2'
make migrate ARGS='--db chain-a-accounts --clean-target'
make backup ARGS='--overwrite-dumps' && make restore
make restore ARGS='--db big-db --from-phase 4'
make migrate ARGS='--create-databases'      # only if targets do not exist yet
make verify ARGS='--rows none'
```

Mutating commands ask for confirmation; `--yes` skips it (required without a
terminal). `--fail-fast` starts no new database after a failure;
`--progress-interval` sets how often long steps report progress (seconds).

## Config

See [config.example.yaml](config.example.yaml). The file holds plain-text
credentials and is meant for a single migration: keep it out of git and delete
it afterwards. Unknown keys, duplicate keys and values YAML would coerce (for
example an unquoted numeric password) are rejected.

| Key | Meaning |
| --- | --- |
| `backup_name` | Dump name: `<prefix><source db>/<backup_name>.dump` |
| `source`, `destination` | `host`, `port`, `sslmode`, `connect_timeout` |
| `destination.admin` | Only with `--create-databases`: `user`/`password` that creates missing targets with `OWNER <group user>`; when `OWNER` is not permitted it creates them and grants `ALL` on the database and schema `public`. Without it the group user runs `CREATE DATABASE` |
| `destination.maintenance_db` | Only with `--create-databases`: database to connect to for `CREATE DATABASE` (default `postgres`) |
| `storage.type` | `s3` or `local` |
| `storage.s3` | `bucket`, `prefix`, `endpoint_url`, `region`, `access_key_id`, `secret_access_key`, `session_token`, `force_path_style`, `verify_ssl`, `chunk_size` (`auto` or e.g. `256MiB`; units are binary), `max_parts` (default 1000), `upload_concurrency` (default 2), `max_attempts` |
| `storage.local` | `path` (relative to the work directory), `format`: `directory` or `custom` |
| `parallel` | Databases processed at the same time (default 1) |
| `options` | `dump_compress` (`pg_dump -Z`, default `0`), `dump_jobs`, `restore_jobs`, `restore_pgoptions`, `probe_ddl`, `verify_rows` (`exact`/`none`) |
| `groups.<name>` | `user`, `password`, optional `destination: {user, password}`, `options`, `databases` |
| `groups.<name>.databases[]` | A name, or `{name, target, options}` |

An S3 upload buffers about `chunk_size × (upload_concurrency + 1)` bytes per
database. `restore_jobs > 1` needs local storage: a streamed archive is restored
with one job. `dump_jobs > 1` needs the local `directory` format.

## Commands

| Command | Effect |
| --- | --- |
| `validate` | Parse the config and print the plan |
| `check` | Read-only preflight; `--no-probe` skips the DDL probe |
| `backup` | Dump to storage, then write a manifest next to the dump |
| `restore` | Four-phase restore; `--to-phase N`, `--from-phase N`, `--clean-target` |
| `migrate` | Per database: target checks, probe, backup, restore, verify |
| `verify` | Compare source and target; writes a JSON report |
| `status` | Show the recorded state |

`--group` and `--db` select databases (repeatable). `check`, `restore` and
`migrate` accept `--create-databases`. Exit codes: 0 success, 1 failure,
2 config error, 130 interrupted.

## Restore details

Every phase runs `pg_restore --no-owner --no-privileges --exit-on-error`, so
restored objects belong to the group user and the first error stops the phase
(the earlier scripts also failed on any error, through the exit code).
`restore_pgoptions` is passed as `PGOPTIONS`. Phases 2 and 4 use
`--use-list` files built from the archive TOC; the lists, the TOC and every
tool's output are kept under the work directory.

By default every target database must already exist, owned by or granted to
its group user; a missing one is an error. With `--create-databases` a missing
target is created as described for `destination.admin`.

A target is not empty when it has schema `app` (the check of the earlier
scripts) or another schema the dump creates (for projects with a custom schema
name, whose restore would otherwise fail in phase 1). Then nothing is loaded and
the database fails with an error. `--clean-target` drops exactly those schemas
in one transaction, for reruns after a failed attempt; other schemas are left
alone and reported.

`--from-phase N` continues at phase N and trusts the target to match the state
after phase N-1, for example after phase 4 failed on an object that is now
fixed. Prefer `--clean-target` when unsure.

## Reruns and state

`WORK/state/<backup_name>.json` records each database's dump (location, size,
ETag or local file fingerprint, generation), restore (target database OID,
finished phases, status, error) and verification. Progress is bound to the source,
target and storage endpoints and the database users. A rerun of
`migrate` skips verified databases, reuses recorded dumps and restarts nothing
silently:

* An existing dump that this work directory did not record needs
  `--reuse-dumps` or `--overwrite-dumps`.
* `--overwrite-dumps` always produces a new dump, including when the previous
  one is recorded. It invalidates restore/verify progress. A phase continuation
  refuses an archive different from the one used by the earlier phases.
* A target that has schema `app` or a schema the dump creates stops with an
  error. `--clean-target` drops those schemas in a transaction and rolls back
  if CASCADE would remove dependent objects outside them. Objects in `public`
  are not removed: a project that stores objects there needs an empty target,
  and a fresh database for a full retry.
* One command at a time may write to a work directory. An advisory lock also
  prevents simultaneous restores into the same database from different work
  directories. Configured source databases cannot also be targets on the same
  server; runtime checks detect server aliases when the source is reachable.
* A stronger row-count setting reruns verification rather than accepting an
  earlier catalog-only result. A restarted restore invalidates earlier checks.
* An interrupted run (Ctrl-C, SIGTERM) stops its tools and SQL queries, aborts open uploads,
  marks the database `interrupted` and exits with 130.

Logs are in `WORK/logs/<backup_name>/` (one file per run plus one per database
with the client tools' output), verification reports in `WORK/reports/`.
Configured secrets are masked in every log line.

Dumps made by the earlier S3 script (`s3://<bucket>/<db>/<name>.dump`, no
manifest) restore with `backup_name: <name>`; `restore` warns about the
missing manifest. For an older local directory dump, place the directory at
`<local.path>/<db>/<backup_name>.dir` before running `restore`.

Verification compares full routine definitions and aggregate settings, trigger
enablement, sequence parameters, ownership links and `last_value`/`is_called`,
RLS policies, partition definitions, column order and domain defaults as well
as the catalogs above. Counts use a read-only, repeatable-read transaction on
each server and avoid counting inherited rows twice. Keep source writers
stopped: the two servers do not share a snapshot.

# Harness and development

The harness runs PostgreSQL 16 (`<HARNESS>-old`), PostgreSQL 17
(`<HARNESS>-new`) and an S3 server ([versitygw](https://github.com/versity/versitygw))
on a private Podman network. `make harness-seed` (and every e2e session) resets
it from [tests/harness/config.yaml](tests/harness/config.yaml): three groups with
their own users and passwords, a renamed target and a mixed-case user. Like a
managed database service's API, the seeder creates the users and the empty
target databases; the `--create-databases` tests add a destination admin that
is not a superuser. Each source database gets the
SubQuery-like [fixture](tests/harness/fixture.sql), created by the group user:
historical entities with `_id`/`_block_range` and GiST indexes, `_metadata`,
`_poi`, hashed enum names, notification triggers, `@foreignKey` comments, plus
partitioned tables, exclusion and unique constraints, deferrable foreign keys,
identity and generated columns, materialized views and many index kinds.

`make rehearse CONFIG=...` replays a real config's groups, users, passwords,
database names and admin in the harness (fixture data, harness endpoints) and
runs `validate`, `check`, `migrate` and `verify` through the runtime image.
`CREATE_DATABASES=1` leaves the targets missing and rehearses `--create-databases`.
Local dump paths in the generated rehearsal config are reset to `dumps` under
the rehearsal work directory.

The repository CI runs `make ci` (lint, unit tests) and `make ci-integration`
(unit and e2e tests with combined coverage, runtime image build and a rehearsal
of `config.example.yaml`) for every change to this directory; both run locally
the same way.

| Command | Purpose |
| --- | --- |
| `make lint` / `make fmt` | Ruff checks / formatting |
| `make test` | Offline unit tests with coverage |
| `make test-e2e` | End-to-end tests against the harness (PostgreSQL 16 → 17) |
| `make test-all` | Lint, unit and end-to-end tests with combined coverage |
| `make harness-up` / `harness-seed` / `harness-psql SERVER=old\|new` / `harness-down` | Manage the harness |
| `make rehearse` | Replay `CONFIG` in the harness |
| `make image` | Build the runtime image locally as `:local` (`BUILD_ENGINE`) |
| `make lock` / `make outdated` | Pin / list dependencies |
| `make clean` | Remove caches and dev images |

Without a systemd user bus, use `CONTAINER_ENGINE='podman --cgroup-manager=cgroupfs'`.

# License

This project is licensed under the [Apache License 2.0](../LICENSE).
