# Changelog

## 1.0.0 — 2026-10-27

Replaces `migrate_db_s3.sh`, `migrate_db_local.sh` and `run_migrate_db_s3.sh`
with `sqdb`, a containerized Python tool driven by a YAML config and make
targets. The four-phase restore order is unchanged.

- One config for many databases, grouped by credentials, with target renames
  and per-group/per-database options.
- S3 and local storage in one tool. S3 dumps keep the earlier key layout, so
  dumps of the earlier scripts can be restored.
- S3 uploads complete only after a successful `pg_dump`; failed or interrupted
  dumps abort the multipart upload. Part sizes follow the database size and
  grow before the part limit. Downloads resume after broken connections.
- Restore phases stop at the first error (`--exit-on-error`). Partition
  attachments and comments of constraint indexes are restored in phase 4 with
  their constraints.
- Target databases and users must exist, as a managed service creates them;
  a target with schema `app` stops the restore, as before. `--create-databases`
  opts into creating missing targets (through an optional admin user).
- `check` preflight with a rolled-back DDL probe on the target.
- `verify` compares catalogs, index validity and exact row counts.
- State file for restartable runs, `--clean-target`, `--from-phase`,
  `--to-phase`, `--reuse-dumps`, `--overwrite-dumps`, parallel databases,
  secret masking and per-database logs.
- PostgreSQL 16 → 17 harness with versitygw, end-to-end tests and
  `make rehearse` to replay a real config's topology locally.
- Operator targets run the published image (`latest` by default, `IMAGE_TAG`);
  `make image` builds it locally as `:local` with podman, buildah or docker.

Initial implementation review (2026-10-28):

- Bind progress to connection/storage context, archive identity and target
  database OID; honor explicit overwrite and invalidate stale restore/verify results.
- Protect work directories and target databases against concurrent runs; reject
  source/target overlap and roll back cleanup that cascades outside selected schemas.
- Parse verbose TOC dependencies and generated schema SQL for names with spaces.
- Compare complete routine/aggregate definitions, trigger state, sequence state
  and parameters, domains, RLS policies, partitioning and column order.
- Cancel active SQL on interruption, wake streaming retries, stop children after
  logging failures and reap workers that outlive their parent.
- Preserve local dumps until a replacement succeeds, close manifest streams,
  refill upload workers as individual parts finish, harden config/CLI validation
  and protect generated files and error messages.
- Add `make ci` and `make ci-integration` for the repository CI, regression tests
  and isolated local rehearsal paths.
- Keep target connections from being closed by a server-side
  `idle_session_timeout` (the target lock, verify and the `pg_restore -j` leader
  stay idle for long periods), and refuse `--clean-target` when the source
  cannot be reached to rule out that the target is the source.
