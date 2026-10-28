# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Per-database steps: target checks, probe, backup, four-phase restore and verify.

Restore phases (unchanged from the original scripts):
  1. pre-data: schemas, types, functions, empty tables;
  2. index entries of the archive TOC, built on the still empty tables;
  3. data: rows are loaded and the indexes are maintained incrementally;
  4. the remaining post-data: constraints, foreign keys, triggers, ...
Every phase runs pg_restore with --no-owner --no-privileges --exit-on-error,
so objects belong to the group user and the first error stops the phase.

Target databases and users are expected to exist (a managed database service
creates them through its own API); --create-databases opts into creating
missing databases.
"""

import json
import os
import re
import subprocess
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import psycopg

from . import __version__
from .config import Config, Credentials, Database, format_size
from .log import MASKER, db_logger
from .pg import (
    DatabaseMissing,
    clean_target,
    connect,
    conninfo,
    create_database,
    create_privileges,
    database_facts,
    inspect_target,
    same_cluster,
    server_facts,
    user_schemas,
)
from .proc import SUPERVISOR, Interrupted, ToolError, collect_stdout, run_tool
from .state import State, now
from .storage import DumpInfo, LocalStore, Progress, S3Store, auto_chunk_size
from .toc import Toc, parse_toc, schema_names
from .verify import compare_databases

PHASES = (1, 2, 3, 4)
PHASE_NAMES = {
    1: "schema (pre-data)",
    2: "indexes on empty tables",
    3: "data",
    4: "constraints, triggers and other post-data",
}
LOCK_LIMIT_RE = re.compile(r"out of shared memory|max_locks_per_transaction")


MISSING_TARGET_HINT = (
    "create it with its user first (for example through the database service's API), "
    "or pass --create-databases"
)


class JobError(Exception):
    """A database step failed with an operator-facing explanation."""


def pg_bin(name: str) -> str:
    bindir = os.environ.get("PG_BINDIR")
    if bindir and Path(bindir, name).exists():
        return str(Path(bindir, name))
    return name


def client_version() -> tuple[int, str]:
    output = subprocess.run(
        [pg_bin("pg_dump"), "--version"], capture_output=True, text=True, check=True
    ).stdout.strip()
    match = re.search(r"(\d+)(?:\.(\d+))?", output)
    if not match:
        raise JobError(f"cannot parse pg_dump version from {output!r}")
    return int(match.group(1)), output


@dataclass
class RunOptions:
    reuse_dumps: bool = False
    overwrite_dumps: bool = False
    clean_target: bool = False
    from_phase: int | None = None
    to_phase: int = 4
    probe: bool = True
    create_databases: bool = False
    rows: str | None = None
    heartbeat: float = 60.0


@dataclass
class Context:
    config: Config
    work_dir: Path
    options: RunOptions
    state: State
    store: S3Store | LocalStore
    client_major: int
    client_version: str

    @property
    def run_dir(self) -> str:
        return self.config.backup_name

    def logs_dir(self) -> Path:
        return self.work_dir / "logs" / self.run_dir

    def reports_dir(self) -> Path:
        return self.work_dir / "reports" / self.run_dir

    def tmp_dir(self) -> Path:
        return self.work_dir / "tmp" / self.run_dir


@dataclass
class CheckResult:
    db: Database
    source_size: int | None = None
    lines: list[str] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class DatabaseJob:
    def __init__(self, ctx: Context, db: Database):
        self.ctx = ctx
        self.db = db
        self.config = ctx.config
        self.log = db_logger(db.name)
        self.tool_log = ctx.logs_dir() / f"{db.name}.log"
        self.tmp = ctx.tmp_dir() / db.name
        self.minimum_major = 0
        self.target_major = 0
        self._target_locked = False
        source = {
            "host": self.config.source.host,
            "port": self.config.source.port,
            "database": db.name,
            "user": db.source_credentials.user,
        }
        target = {
            "host": self.config.destination.host,
            "port": self.config.destination.port,
            "database": db.target,
            "user": db.target_credentials.user,
        }
        storage = {"type": ctx.store.kind, "location": ctx.store.location(db, self.backup_name)}
        if self.config.s3:
            storage["endpoint_url"] = self.config.s3.endpoint_url
        if ctx.state.bind(db.name, source, target, storage):
            self.log.warning(
                "config context changed; discarded progress from other endpoints or archives"
            )

    @staticmethod
    def dump_identity(info: DumpInfo) -> dict:
        return {
            "location": info.location,
            "size": info.size,
            "etag": info.etag,
            "backup_id": (info.manifest or {}).get("backup_id"),
        }

    def _restored(self, info: DumpInfo | None) -> bool:
        recorded = self.ctx.state.get(self.db.name, "restore")
        if not (
            info
            and recorded.get("status") == "done"
            and recorded.get("dump") == self.dump_identity(info)
        ):
            return False
        try:
            with self.target_conn() as conn:
                return self.target_oid(conn) == recorded.get("target_oid")
        except DatabaseMissing:
            return False

    @staticmethod
    def target_oid(conn) -> int:
        return conn.execute(
            "SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database()"
        ).fetchone()[0]

    def guard_source(self, conn) -> None:
        if self.db.target not in {db.name for db in self.config.databases}:
            return
        try:
            with self.source_conn() as source:
                same = same_cluster(source, conn)
        except (DatabaseMissing, psycopg.OperationalError) as exc:
            # Cleaning drops schemas: never do it without ruling out the source.
            if self.ctx.options.clean_target:
                raise JobError(
                    "cannot reach the source to confirm that the target is not a source "
                    f"database on the same server ({_first_line(exc)}); --clean-target "
                    "needs that check"
                ) from None
            self.log.warning(
                "source unavailable; cannot check for source/destination server aliases"
            )
            return
        if same:
            raise JobError(
                "target is a configured source database on the same server; refusing to modify it"
            )

    def validate_manifest(self, info: DumpInfo) -> None:
        manifest = info.manifest
        if manifest is None:
            return
        for key, expected in (
            ("source_database", self.db.name),
            ("backup_name", self.backup_name),
            ("size", info.size),
            ("etag", info.etag),
        ):
            if key in manifest and manifest[key] != expected:
                raise JobError(
                    f"dump manifest {key} does not match the archive; refuse to reuse or restore it"
                )

    # ----------------------------------------------------------- plumbing

    def _env(self, creds: Credentials, pgoptions: str | None = None) -> dict[str, str]:
        env = {key: value for key, value in os.environ.items() if not key.startswith("PG")}
        if "PG_BINDIR" in os.environ:
            env["PG_BINDIR"] = os.environ["PG_BINDIR"]
        env["PGPASSWORD"] = creds.password
        if pgoptions:
            env["PGOPTIONS"] = pgoptions
        return env

    def _source_dsn(self) -> str:
        return conninfo(self.config.source, self.db.name, self.db.source_credentials.user)

    def _target_dsn(self) -> str:
        return conninfo(self.config.destination, self.db.target, self.db.target_credentials.user)

    def source_conn(self) -> psycopg.Connection:
        return connect(self.config.source, self.db.name, self.db.source_credentials)

    def target_conn(self) -> psycopg.Connection:
        return connect(self.config.destination, self.db.target, self.db.target_credentials)

    @contextmanager
    def target_lock(self, *, create: bool = True):
        if self._target_locked:
            yield
            return
        try:
            conn = self.target_conn()
        except DatabaseMissing:
            if not create:
                raise JobError("target disappeared before the DDL probe; rerun check") from None
            self.ensure_target(set())
            conn = self.target_conn()
        with conn:
            acquired = conn.execute(
                "SELECT pg_catalog.pg_try_advisory_lock(pg_catalog.hashtext('sqdb migration'))"
            ).fetchone()[0]
            if not acquired:
                raise JobError("another sqdb migration is using this target database")
            self._target_locked = True
            try:
                yield
            finally:
                self._target_locked = False
                try:
                    held = conn.execute(
                        "SELECT pg_catalog.pg_advisory_unlock("
                        "pg_catalog.hashtext('sqdb migration'))"
                    ).fetchone()[0]
                except psycopg.Error:
                    held = False
                if not held:
                    self.log.warning(
                        "the target lock connection was lost during the run; other sqdb "
                        "runs on this target were not excluded after that"
                    )

    def _run(self, argv: list[str], label: str, env: dict[str, str], **kwargs):
        self.log.debug("running %s", label)
        return run_tool(
            argv,
            label=label,
            env=env,
            log=self.log,
            tool_log=self.tool_log,
            heartbeat=self.ctx.options.heartbeat,
            **kwargs,
        )

    @property
    def backup_name(self) -> str:
        return self.config.backup_name

    # ------------------------------------------------------------- target

    def ensure_target(self, expected_schemas: set[str]):
        """Create the target database when allowed; return what it contains."""
        try:
            conn = self.target_conn()
        except DatabaseMissing:
            if not self.ctx.options.create_databases:
                raise JobError(
                    f"target database {self.db.target} does not exist; {MISSING_TARGET_HINT}"
                ) from None
            how = create_database(
                self.config.destination, self.db.target_credentials, self.db.target, self.log
            )
            self.log.info("target database %s %s", self.db.target, how)
            conn = self.target_conn()
        with conn:
            self.guard_source(conn)
            server = server_facts(conn)
            self.target_major = server.major
            if server.major > self.ctx.client_major or server.major < self.minimum_major:
                raise JobError(
                    f"incompatible target server {server.major}: "
                    f"needs source <= target <= client {self.ctx.client_major}"
                )
            target = inspect_target(conn, expected_schemas)
        if target.other_schemas:
            self.log.warning(
                "target has schemas the dump does not create (left untouched): %s",
                ", ".join(sorted(target.other_schemas)),
            )
        return target

    def ensure_empty_target(self, expected_schemas: set[str]) -> None:
        target = self.ensure_target(expected_schemas)
        if target.empty:
            return
        if not self.ctx.options.clean_target:
            raise JobError(
                f"target database {self.db.target} is not empty ({target.describe()}); "
                "nothing was loaded. Use an empty database, or rerun with --clean-target "
                "to drop the schema left by an earlier attempt"
            )
        self.log.warning("cleaning target: %s", target.describe())
        with self.target_conn() as conn:
            clean_target(conn, target, self.log)
            remaining = inspect_target(conn, expected_schemas)
        if not remaining.empty:
            raise JobError(f"target still not empty after cleaning: {remaining.describe()}")
        self.ctx.state.clear(self.db.name, "restore")
        self.ctx.state.clear(self.db.name, "verify")

    def source_schemas(self) -> set[str]:
        with self.source_conn() as conn:
            self.minimum_major = server_facts(conn).major
            if self.minimum_major > self.ctx.client_major:
                raise JobError("source server is newer than the PostgreSQL client")
            return user_schemas(conn)

    # -------------------------------------------------------------- probe

    def probe(self) -> bool:
        with self.target_lock(create=False):
            return self._probe()

    def _probe(self) -> bool:
        """Run the dump's DDL on the target in one transaction and roll it back."""
        self.tmp.mkdir(parents=True, exist_ok=True)
        schema_dump = self.tmp / "schema.dump"
        schema_dump.unlink(missing_ok=True)
        source_env = self._env(self.db.source_credentials)
        self.log.info("probe: dumping the source schema")
        self._run(
            [
                pg_bin("pg_dump"),
                "--schema-only",
                "-Fc",
                "-f",
                str(schema_dump),
                "-d",
                self._source_dsn(),
            ],
            "pg_dump --schema-only",
            source_env,
        )
        toc = self._toc_from_file(schema_dump, source_env)
        lists = self._write_lists(toc, "probe")
        sections: list[bytes] = []
        for phase in (1, 2, 4):
            if phase == 2 and not toc.index_entries:
                continue
            args = (
                ["--section=pre-data"]
                if phase == 1
                else ["--section=post-data", f"--use-list={lists[phase]}"]
            )
            result = self._run(
                [
                    pg_bin("pg_restore"),
                    "--no-owner",
                    "--no-privileges",
                    *args,
                    "-f",
                    "-",
                    str(schema_dump),
                ],
                f"pg_restore script phase {phase}",
                source_env,
                stdout_consumer=collect_stdout,
            )
            sections.append(result.output)
        script = b"BEGIN;\n" + b"\n".join(sections) + b"\nROLLBACK;\n"
        target_env = self._env(self.db.target_credentials, self.db.options.restore_pgoptions)
        self.log.info(
            "probe: applying %d DDL entries on the target and rolling back", len(toc.entries)
        )
        try:
            self._run(
                [pg_bin("psql"), "-X", "-q", "-v", "ON_ERROR_STOP=1", "-d", self._target_dsn()],
                "psql probe",
                target_env,
                stdin_source=script,
            )
        except ToolError as exc:
            if any(LOCK_LIMIT_RE.search(line) for line in exc.tail):
                self.log.warning(
                    "probe inconclusive: one transaction exceeds the server lock table; "
                    "the restore itself does not run in one transaction"
                )
                return False
            raise JobError("DDL probe failed on the target:\n" + "\n".join(exc.tail[-10:])) from exc
        self.log.info("probe passed")
        return True

    # ---------------------------------------------------------------- TOC

    def _toc_from_file(self, path: Path, env: dict[str, str]) -> Toc:
        result = self._run(
            [pg_bin("pg_restore"), "-lv", str(path)],
            "pg_restore -l",
            env,
            stdout_consumer=collect_stdout,
        )
        toc = parse_toc(result.output.decode("utf-8", "replace"))
        self._read_schema_names(toc, path, env)
        return toc

    def read_toc(self, info: DumpInfo) -> Toc:
        env = self._env(self.db.target_credentials)
        if isinstance(self.ctx.store, LocalStore):
            return self._toc_from_file(Path(info.location), env)
        with self.ctx.store.reader(self.db, self.backup_name, info.etag, self.log) as reader:
            result = self._run(
                [pg_bin("pg_restore"), "-lv"],
                "pg_restore -l",
                env,
                stdin_source=reader,
                stdout_consumer=collect_stdout,
            )
        toc = parse_toc(result.output.decode("utf-8", "replace"))
        self._read_schema_names(toc, info, env)
        return toc

    def _read_schema_names(self, toc: Toc, archive: Path | DumpInfo, env: dict) -> None:
        self.tmp.mkdir(parents=True, exist_ok=True)
        path = self.tmp / "schemas.list"
        path.write_text(
            "".join(f"{e.dump_id};\n" for e in toc.entries if e.type == "SCHEMA"), encoding="utf-8"
        )
        argv = [
            pg_bin("pg_restore"),
            "--no-owner",
            "--no-privileges",
            "--section=pre-data",
            f"--use-list={path}",
            "-f",
            "-",
        ]
        if isinstance(archive, Path):
            result = self._run(
                [*argv, str(archive)], "pg_restore schemas", env, stdout_consumer=collect_stdout
            )
        else:
            with self.ctx.store.reader(self.db, self.backup_name, archive.etag, self.log) as reader:
                result = self._run(
                    argv,
                    "pg_restore schemas",
                    env,
                    stdin_source=reader,
                    stdout_consumer=collect_stdout,
                )
        toc.schema_names = schema_names(result.output.decode("utf-8"))
        if len(toc.schema_names) != sum(e.type == "SCHEMA" for e in toc.entries):
            raise JobError(
                "cannot determine all schema names from the archive; refusing unsafe cleanup"
            )

    def _write_lists(self, toc: Toc, prefix: str) -> dict[int, Path]:
        self.tmp.mkdir(parents=True, exist_ok=True)
        paths = {}
        for phase in (2, 4):
            path = self.tmp / f"{prefix}.phase{phase}.list"
            path.write_text(toc.phase_list(phase), encoding="utf-8")
            paths[phase] = path
        (self.tmp / f"{prefix}.toc.list").write_text(
            "".join(f"{entry.line}\n" for entry in toc.entries), encoding="utf-8"
        )
        return paths

    # ------------------------------------------------------------- backup

    def backup(self) -> DumpInfo:
        state, store = self.ctx.state, self.ctx.store
        recorded = state.get(self.db.name, "backup")
        info = store.info(self.db, self.backup_name)
        if info is not None:
            confirmed = (
                recorded.get("status") == "done"
                and recorded.get("dump") == self.dump_identity(info)
                and info.manifest is not None
            )
            if not self.ctx.options.overwrite_dumps and (confirmed or self.ctx.options.reuse_dumps):
                self.validate_manifest(info)
                self.log.info("reusing dump %s (%s)", info.location, format_size(info.size or 0))
                if not confirmed:
                    state.update(
                        self.db.name,
                        "backup",
                        replace=True,
                        status="done",
                        location=info.location,
                        size=info.size,
                        dump=self.dump_identity(info),
                        reused=True,
                    )
                return info
            if not self.ctx.options.overwrite_dumps:
                raise JobError(
                    f"dump {info.location} already exists but this work directory has no "
                    "record of creating it; pass --reuse-dumps to use it or "
                    "--overwrite-dumps to dump again"
                )
            self.log.warning("overwriting dump %s", info.location)

        with self.source_conn() as conn:
            facts = database_facts(conn)
            server = server_facts(conn)
        if server.major > self.ctx.client_major:
            raise JobError("source server is newer than the PostgreSQL client")
        location = store.location(self.db, self.backup_name)
        state.update(
            self.db.name,
            "backup",
            replace=True,
            status="running",
            location=location,
            started_at=now(),
        )
        self.log.info(
            "backup: dumping %s (%s on disk) to %s", self.db.name, format_size(facts.size), location
        )
        env = self._env(self.db.source_credentials)
        options = self.db.options
        if state.get(self.db.name, "restore"):
            state.update(self.db.name, "restore", status="stale")
        state.clear(self.db.name, "verify")
        try:
            if isinstance(store, S3Store):
                chunk = store.settings.chunk_size or auto_chunk_size(
                    facts.size, store.settings.max_parts
                )
                self.log.info("backup: S3 part size %s", format_size(chunk))
                progress = Progress()

                def upload(stream, succeeded: Callable[[], bool]) -> int:
                    return store.upload(
                        self.db, self.backup_name, stream, succeeded, chunk, self.log, progress
                    )

                result = self._run(
                    [
                        pg_bin("pg_dump"),
                        "-Fc",
                        "-Z",
                        options.dump_compress,
                        "--verbose",
                        "-d",
                        self._source_dsn(),
                    ],
                    "pg_dump",
                    env,
                    stdout_consumer=upload,
                    progress=progress,
                )
                size = result.output
            else:
                partial = store.prepare(self.db, self.backup_name)
                fmt = "d" if store.settings.format == "directory" else "c"
                argv = [
                    pg_bin("pg_dump"),
                    "-F",
                    fmt,
                    "-Z",
                    options.dump_compress,
                    "--verbose",
                    "-f",
                    str(partial),
                    "-d",
                    self._source_dsn(),
                ]
                if fmt == "d" and options.dump_jobs > 1:
                    argv += ["-j", str(options.dump_jobs)]
                self._run(argv, "pg_dump", env)
                size = store.commit(self.db, self.backup_name)
            info = store.info(self.db, self.backup_name)
            toc = self.read_toc(info)
            manifest = {
                "backup_id": str(uuid.uuid4()),
                "etag": info.etag,
                "tool": f"sqdb {__version__}",
                "client": self.ctx.client_version,
                "backup_name": self.backup_name,
                "created_at": now(),
                "group": self.db.group,
                "source_database": self.db.name,
                "source_server_version": server.version,
                "source_database_size": facts.size,
                "size": size,
                "format": "directory"
                if isinstance(store, LocalStore) and store.settings.format == "directory"
                else "custom",
                "toc_entries": len(toc.entries),
                "toc_counts": toc.counts(),
            }
            store.write_manifest(self.db, self.backup_name, manifest)
        except BaseException as exc:
            status = "interrupted" if isinstance(exc, Interrupted | KeyboardInterrupt) else "failed"
            state.update(self.db.name, "backup", status=status, error=_short(exc))
            raise
        state.update(
            self.db.name,
            "backup",
            status="done",
            size=size,
            finished_at=now(),
            toc_entries=len(toc.entries),
            dump=self.dump_identity(store.info(self.db, self.backup_name)),
        )
        self.log.info("backup done: %s, %d TOC entries", format_size(size), len(toc.entries))
        return store.info(self.db, self.backup_name)

    # ------------------------------------------------------------ restore

    def _restore_pgoptions(self) -> str:
        options = self.db.options.restore_pgoptions
        # pg_restore -j keeps its leader connection idle while the workers load
        # data; a server-side idle_session_timeout (PostgreSQL 14+) must not end it.
        if self.target_major >= 14:
            options = f"{options} -c idle_session_timeout=0".strip()
        return options

    def _restore_argv(self, phase: int, lists: dict[int, Path]) -> list[str]:
        argv = [
            pg_bin("pg_restore"),
            "--no-owner",
            "--no-privileges",
            "--exit-on-error",
            "--verbose",
        ]
        if phase == 1:
            argv.append("--section=pre-data")
        elif phase == 3:
            argv.append("--section=data")
        else:
            argv += [f"--use-list={lists[phase]}", "--section=post-data"]
        if self.db.options.restore_jobs > 1:
            argv += ["-j", str(self.db.options.restore_jobs)]
        argv += ["-d", self._target_dsn()]
        return argv

    def restore(self) -> str:
        with self.target_lock():
            return self._restore()

    def _restore(self) -> str:
        options, state = self.ctx.options, self.ctx.state
        recorded = state.get(self.db.name, "restore")
        start = options.from_phase or 1
        end = options.to_phase
        info = self.ctx.store.info(self.db, self.backup_name)
        if info is None:
            raise JobError(f"no dump at {self.ctx.store.location(self.db, self.backup_name)}")
        self.validate_manifest(info)
        if options.from_phase is None and self._restored(info) and not options.clean_target:
            self.log.info("restore already done into %s; skipping", recorded.get("target"))
            return "skipped"
        if info.manifest is None:
            self.log.warning("dump %s has no manifest (made before sqdb 1.0.0?)", info.location)
        toc = self.read_toc(info)
        version = toc.header.get("Dumped from database version", "")
        match = re.match(r"(\d+)", version)
        self.minimum_major = int(match.group(1)) if match else 0
        lists = self._write_lists(toc, "restore")
        self.log.info(
            "restore: %s → %s, %d TOC entries, %d index entries",
            info.location,
            self.db.target,
            len(toc.entries),
            len(toc.index_entries),
        )

        if start == 1:
            self.ensure_empty_target(toc.schemas)
            done: list[int] = []
        else:
            if recorded.get("dump") and recorded["dump"] != self.dump_identity(info):
                raise JobError(
                    "the dump changed since the earlier restore; "
                    "restart at phase 1 with --clean-target"
                )
            try:
                with self.target_conn() as conn:
                    self.guard_source(conn)
                    server = server_facts(conn)
                    self.target_major = server.major
                    if not self.minimum_major <= server.major <= self.ctx.client_major:
                        raise JobError("incompatible target server version")
            except DatabaseMissing:
                raise JobError(
                    f"--from-phase {start}: target {self.db.target} does not exist"
                ) from None
            done = [phase for phase in recorded.get("phases_done", []) if phase < start]
            missing = [phase for phase in range(1, start) if phase not in done]
            if missing:
                self.log.warning(
                    "phases %s are not recorded as done; continuing as requested",
                    ", ".join(map(str, missing)),
                )
                done.extend(missing)  # Explicit --from-phase accepts the operator's assertion.
        state.clear(self.db.name, "verify")
        with self.target_conn() as conn:
            target_oid = self.target_oid(conn)
        state.update(
            self.db.name,
            "restore",
            replace=True,
            status="running",
            target=self.db.target,
            target_oid=target_oid,
            location=info.location,
            dump=self.dump_identity(info),
            phases_done=done,
            started_at=now(),
        )
        env = self._env(self.db.target_credentials, self._restore_pgoptions())
        phase = start
        try:
            for phase in range(start, end + 1):
                label = f"phase {phase}/4 {PHASE_NAMES[phase]}"
                if phase == 2 and not toc.index_entries:
                    self.log.info("%s: no index entries, skipped", label)
                else:
                    self.log.info("%s: started", label)
                    self._restore_phase(phase, lists, info, env, label)
                    self.log.info("%s: done", label)
                done.append(phase)
                state.update(self.db.name, "restore", phases_done=list(done))
        except BaseException as exc:
            status = "interrupted" if isinstance(exc, Interrupted | KeyboardInterrupt) else "failed"
            state.update(
                self.db.name, "restore", status=status, failed_phase=phase, error=_short(exc)
            )
            raise
        final = "done" if all(phase in done for phase in PHASES) else "partial"
        state.update(self.db.name, "restore", status=final, finished_at=now())
        return final

    def _restore_phase(
        self, phase: int, lists: dict[int, Path], info: DumpInfo, env: dict[str, str], label: str
    ) -> None:
        argv = self._restore_argv(phase, lists)
        if isinstance(self.ctx.store, LocalStore):
            self._run([*argv, info.location], label, env)
            return
        # Phases 1, 2 and 4 read only the archive header; phase 3 streams it all.
        progress = Progress(info.size) if phase == 3 else None
        store = self.ctx.store
        with store.reader(self.db, self.backup_name, info.etag, self.log, progress) as reader:
            self._run(argv, label, env, stdin_source=reader, progress=progress)

    # ------------------------------------------------------------- verify

    def verify(self) -> bool:
        self.ctx.state.update(self.db.name, "verify", replace=True, status="running")
        try:
            return self._verify()
        except BaseException as exc:
            status = (
                "interrupted"
                if SUPERVISOR.stop.is_set() or isinstance(exc, Interrupted | KeyboardInterrupt)
                else "failed"
            )
            self.ctx.state.update(self.db.name, "verify", status=status, error=_short(exc))
            raise

    def _verify(self) -> bool:
        rows = (self.ctx.options.rows or self.db.options.verify_rows) == "exact"
        with (
            self.source_conn() as source,
            self.target_conn() as target,
            source.transaction(),
            target.transaction(),
        ):
            source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            target.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            report = compare_databases(source, target, self.db.name, self.db.target, rows, self.log)
        reports = self.ctx.reports_dir()
        reports.mkdir(parents=True, exist_ok=True)
        path = reports / f"{self.db.name}.verify.json"
        path.write_text(
            json.dumps(_mask_report(report.as_dict()), indent=2, sort_keys=True), encoding="utf-8"
        )
        for warning in report.warnings:
            self.log.warning("verify: %s", warning)
        self.ctx.state.update(
            self.db.name,
            "verify",
            replace=True,
            status="passed" if report.ok else "failed",
            report=str(path),
            rows_checked=report.rows_checked,
            tables=report.tables,
            total_rows=report.total_rows,
        )
        if not report.ok:
            for line in report.summary_lines():
                self.log.error("verify: %s", line)
            raise JobError(f"verification failed; see {path}")
        detail = (
            f"{report.tables} tables, {report.total_rows} rows match"
            if report.rows_checked
            else "row counts not checked"
        )
        self.log.info("verify passed: structure matches, %s", detail)
        return True

    # ------------------------------------------------------- composite steps

    def migrate(self) -> str:
        with self.target_lock():
            return self._migrate()

    def _migrate(self) -> str:
        state = self.ctx.state
        verify = state.get(self.db.name, "verify")
        info = self.ctx.store.info(self.db, self.backup_name)
        restored = (
            self._restored(info)
            and not self.ctx.options.clean_target
            and not self.ctx.options.overwrite_dumps
        )
        rows_sufficient = self.db.options.verify_rows == "none" or verify.get("rows_checked")
        if restored and verify.get("status") == "passed" and rows_sufficient:
            self.log.info("already migrated and verified; skipping")
            return "skipped"
        if not restored:
            # Fail before the dump when the target cannot take the restore.
            self.ensure_empty_target(self.source_schemas())
            if self.db.options.probe_ddl and self.ctx.options.probe:
                self.probe()
            self.backup()
            self.restore()
        self.verify()
        return "migrated"

    def check(self) -> CheckResult:
        result = CheckResult(self.db)
        dest = self.config.destination
        source_schemas: set[str] = set()
        src_facts = None
        # Source: credentials, size, version, schemas.
        try:
            with self.source_conn() as conn:
                src_server = server_facts(conn)
                src_facts = database_facts(conn)
                source_schemas = user_schemas(conn)
            result.source_size = src_facts.size
            result.lines.append(
                f"source {src_server.version}, {format_size(src_facts.size)}, schemas: "
                f"{', '.join(sorted(source_schemas)) or '-'}"
            )
            if src_server.major > self.ctx.client_major:
                result.problems.append(
                    f"source server {src_server.major} is newer than client {self.ctx.client_major}"
                )
            if src_facts.settings:
                result.warnings.append(
                    "source has per-database settings pg_dump does not migrate: "
                    + ", ".join(src_facts.settings)
                )
        except DatabaseMissing:
            result.problems.append(f"source database {self.db.name} does not exist")
            return result
        except psycopg.Error as exc:
            result.problems.append(f"source: {_first_line(exc)}")
            return result

        # Target: existence, content, creation rights, version, locale.
        target_ok = False
        try:
            with self.target_conn() as conn:
                dst_server = server_facts(conn)
                dst_facts = database_facts(conn)
                target = inspect_target(conn, source_schemas)
                recorded_restore = self.ctx.state.get(self.db.name, "restore")
                restored = recorded_restore.get("status") == "done" and recorded_restore.get(
                    "target_oid"
                ) == self.target_oid(conn)
            if target.empty:
                target_ok = True
                result.lines.append(
                    f"target {self.db.target}: exists, empty ({dst_server.version})"
                )
            elif restored:
                verified = self.ctx.state.get(self.db.name, "verify").get("status", "not run")
                result.lines.append(
                    f"target {self.db.target}: restored by this work dir (verify: {verified})"
                )
            else:
                result.problems.append(
                    f"target {self.db.target} is not empty ({target.describe()}); "
                    "migrate needs --clean-target"
                )
            if target.other_schemas:
                result.warnings.append(
                    f"target has unrelated schemas: {', '.join(sorted(target.other_schemas))}"
                )
        except DatabaseMissing:
            dst_server, dst_facts = None, None
            creator = dest.admin or self.db.target_credentials
            if not self.ctx.options.create_databases:
                result.problems.append(
                    f"target database {self.db.target} does not exist; {MISSING_TARGET_HINT}"
                )
                return result
            try:
                with connect(dest, dest.maintenance_db, creator) as conn:
                    dst_server = server_facts(conn)
                    dst_facts = database_facts(conn)
                    privileges = create_privileges(conn, self.db.target_credentials.user)
                if not privileges.createdb:
                    result.problems.append(
                        f"target {self.db.target} is missing and "
                        f"{creator.user} cannot create databases"
                    )
                else:
                    how = (
                        "with OWNER"
                        if privileges.can_set_owner or not dest.admin
                        else "with grants (OWNER not permitted)"
                    )
                    result.lines.append(
                        f"target {self.db.target}: missing, {creator.user} will create it {how} "
                        f"({dst_server.version})"
                    )
            except psycopg.Error as exc:
                result.problems.append(
                    f"target {self.db.target} is missing; cannot check creation rights as "
                    f"{creator.user}: {_first_line(exc)}"
                )
        except psycopg.Error as exc:
            dst_server, dst_facts = None, None
            result.problems.append(f"target: {_first_line(exc)}")
        if dst_server is not None:
            if dst_server.major > self.ctx.client_major:
                result.problems.append(
                    f"target server {dst_server.major} is newer than client {self.ctx.client_major}"
                )
            if dst_server.major < src_server.major:
                result.problems.append(
                    f"target server {dst_server.major} is older than source {src_server.major}"
                )
        if dst_facts is not None:
            for attr in ("encoding", "collate", "ctype"):
                if getattr(src_facts, attr) != getattr(dst_facts, attr):
                    result.warnings.append(
                        f"{attr} differs: source {getattr(src_facts, attr)}, "
                        f"target {getattr(dst_facts, attr)}"
                    )

        # Dump.
        try:
            info = self.ctx.store.info(self.db, self.backup_name)
        except Exception as exc:
            result.problems.append(f"storage: {_first_line(exc)}")
            info = None
        if info is not None:
            recorded = self.ctx.state.get(self.db.name, "backup")
            known = (
                recorded.get("status") == "done"
                and recorded.get("dump") == self.dump_identity(info)
                and info.manifest is not None
            )
            try:
                self.validate_manifest(info)
            except JobError as exc:
                result.problems.append(str(exc))
            note = (
                "recorded in this work dir" if known else "needs --reuse-dumps or --overwrite-dumps"
            )
            result.warnings.append(
                f"dump {info.location} exists ({format_size(info.size or 0)}, {note})"
            )

        # DDL probe on an existing empty target.
        if self.ctx.options.probe and self.db.options.probe_ddl:
            if target_ok:
                try:
                    passed = self.probe()
                    (result.lines if passed else result.warnings).append(
                        "DDL probe passed"
                        if passed
                        else "DDL probe inconclusive (server lock limit)"
                    )
                except (JobError, ToolError) as exc:
                    result.problems.append(str(exc))
            else:
                result.lines.append("DDL probe skipped (target missing or not empty)")
        return result


def _first_line(exc: BaseException) -> str:
    text = str(exc).strip()
    return text.splitlines()[0] if text else type(exc).__name__


def _short(exc: BaseException) -> str:
    text = str(exc).strip() or type(exc).__name__
    return text[-2000:]


def _mask_report(value):
    if isinstance(value, str):
        return MASKER(value)
    if isinstance(value, dict):
        return {MASKER(key): _mask_report(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_mask_report(item) for item in value]
    return value
