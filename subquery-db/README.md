# Custom Logical Backups/Restores of subquery DBs in Managed PostgreSQL Environments

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

# Bash Backup/Migration Scripts

This directory provides helper scripts for logical, per-database backups and restores in managed PostgreSQL environments with restricted privileges. Two flows are supported:
- migrate_db_s3.sh — fully containerized streaming to/from S3 (no local dump files) with a four-phase restore (schema → empty indexes → data → post-data).
- migrate_db_local.sh — creates a local directory-format dump (-Fd) and restores in four phases.
- run_migrate_db_s3.sh — a small wrapper showing how to export environment variables and run the S3 script.

Purpose
- Produce portable logical backups of individual databases.
- Restore with empty indexes pre-created to avoid massive post-data index builds that can fail or spike resources.

Host dependencies
- Podman available in PATH.
  - Debian/Ubuntu: apt install podman runc slirp4netns uidmap
- Network access to:
  - source/target PostgreSQL,
  - container registry (docker.io/library/postgres:16),
  - S3-compatible endpoint (for S3 flow).
- No extra tools required on the host; psql, pg_dump, and awscli run inside the container.

migrate_db_s3.sh — CLI
- Modes (exactly one):
  - --all       run backup to S3 and then restore from it
  - --backup    backup to S3 only
  - --restore   restore from S3 only
- Options:
  - --help        print help
  - --skip-checks skip connectivity checks
  - --force       skip confirmation prompts
  - --quiet       reduce verbosity (no progress indicators)
  - --debug       enable debug logs (secrets are masked)
  - --s3-chunk-size SIZE  S3 multipart chunk size (e.g., 64MB, 128MB, 512MB)
                         Default: 512MB (supports files up to 512GB)
- Environment variables:
  - Source (required for --backup/--all):
    - SRC_PGHOST
    - SRC_PGPORT
    - SRC_PGUSER
    - SRC_PGPASSWORD
    - SRC_DBNAME
  - Destination (required for --restore/--all):
    - DST_PGHOST
    - DST_PGPORT
    - DST_PGUSER
    - DST_PGPASSWORD
    - DST_DBNAME
  - Backup naming:
    - BACKUP_NAME — required; S3 path is built as s3://subquery-dumps/${SRC_DBNAME}/${BACKUP_NAME}.dump (override by editing S3_URI in the script if needed)
  - S3/AWS:
    - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY (required)
    - AWS_SESSION_TOKEN (optional)
    - AWS_DEFAULT_REGION (required)
    - S3_ENDPOINT_URL (required)
    - AWS_S3_FORCE_PATH_STYLE (useful for MinIO/Ceph)
    - AWS_NO_VERIFY_SSL=true (if needed)
  - Tuning:
    - RESTORE_JOBS (default: 1; higher values may cause spooling when streaming)
    - DUMP_COMPRESS (0–9, default: 0)
    - S3_MAX_RETRIES (default: 3)
    - SHOW_PROGRESS=true|false
    - S3_CHUNK_SIZE (default: 512MB; S3 multipart chunk size for large files)
    - PG_BASE_IMG, PG_AWS_IMG (optional image overrides)

migrate_db_local.sh — how to run
- No CLI. Edit variables at the top of the file:
  - SOURCE section for backup
  - DESTINATION section for restore
  - Dump directory: ./pgdump/<SRC_DBNAME>
- Phases:
  - 0: pg_dump (directory format)
  - 1: pre-data (schema)
  - 2: empty indexes
  - 3: data
  - 4: post-data (without indexes)

Example (S3 backup)
```/dev/null/example.sh#L1-20
# Minimal variables for a backup
export SRC_DBNAME=your_db
export SRC_PGUSER=your_user
export SRC_PGPASSWORD='secret'

# S3 credentials and file name
export AWS_ACCESS_KEY_ID='...'
export AWS_SECRET_ACCESS_KEY='...'
export BACKUP_NAME=20251001-001

# Optional: region/endpoint
export AWS_DEFAULT_REGION=''
export S3_ENDPOINT_URL=''

# Run backup
./migrate_db_s3.sh --backup
```

# License

This project is licensed under the [Apache License 2.0](../LICENSE).