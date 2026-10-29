# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Command line interface and the batch runner."""

import argparse
import math
import os
import signal
import sys
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path

import psycopg

from . import __version__
from .config import Config, ConfigError, Database, format_size, load_config
from .log import MASKER, db_logger, setup_logging
from .pg import CleanupError, isolate_libpq_environment
from .pipeline import CheckResult, Context, DatabaseJob, JobError, RunOptions, client_version
from .proc import SUPERVISOR, Interrupted, ToolError
from .state import State
from .storage import LocalStore, S3Store, StorageError

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_CONFIG = 2
EXIT_INTERRUPTED = 130
MUTATING = {"backup", "restore", "migrate"}

log = db_logger("-")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sqdb",
        description="Logical migration of SubQuery PostgreSQL databases with a four-phase "
        "restore (schema, indexes on empty tables, data, remaining post-data).",
    )
    parser.add_argument("--version", action="version", version=f"sqdb {__version__}")
    parser.add_argument("--config", default="config.yaml", help="migration config (YAML)")
    parser.add_argument(
        "--work-dir", default="work", help="state, logs, reports and local dumps (default: work)"
    )

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("-y", "--yes", action="store_true", help="do not ask for confirmation")
    common.add_argument("-v", "--verbose", action="store_true", help="debug output on the console")
    common.add_argument("--group", action="append", default=[], help="only this group (repeatable)")
    common.add_argument(
        "--db", action="append", default=[], help="only this source database (repeatable)"
    )

    batch = argparse.ArgumentParser(add_help=False)
    batch.add_argument("--parallel", type=int, help="databases at a time (default: config)")
    batch.add_argument(
        "--fail-fast", action="store_true", help="start no new database after a failure"
    )
    batch.add_argument(
        "--progress-interval",
        type=float,
        default=60.0,
        help="seconds between progress lines of long steps (default: 60)",
    )

    dumps = argparse.ArgumentParser(add_help=False)
    group = dumps.add_mutually_exclusive_group()
    group.add_argument(
        "--reuse-dumps", action="store_true", help="use existing dumps this work dir did not record"
    )
    group.add_argument(
        "--overwrite-dumps", action="store_true", help="dump again over existing dumps"
    )

    clean = argparse.ArgumentParser(add_help=False)
    clean.add_argument(
        "--clean-target",
        action="store_true",
        help="drop the schema a previous restore left in the target, then restore again",
    )

    # Off by default: target databases and their users normally exist already.
    create = argparse.ArgumentParser(add_help=False)
    create.add_argument(
        "--create-databases",
        action="store_true",
        help="create missing target databases (as destination.admin if configured, "
        "else as the group user)",
    )

    sub = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")
    sub.add_parser("validate", parents=[common], help="offline config validation and plan")
    check = sub.add_parser(
        "check",
        parents=[common, batch, create],
        help="read-only preflight of servers, targets, storage and DDL",
    )
    check.add_argument("--no-probe", action="store_true", help="skip the DDL probe")
    sub.add_parser("backup", parents=[common, batch, dumps], help="dump source databases")
    restore = sub.add_parser(
        "restore",
        parents=[common, batch, clean, create],
        help="four-phase restore of existing dumps",
    )
    restore.add_argument(
        "--from-phase",
        type=int,
        choices=range(1, 5),
        help="continue at this phase (expert use; target must match)",
    )
    restore.add_argument(
        "--to-phase", type=int, choices=range(1, 5), default=4, help="stop after this phase"
    )
    migrate = sub.add_parser(
        "migrate",
        parents=[common, batch, dumps, clean, create],
        help="target checks, probe, backup, restore and verify",
    )
    migrate.add_argument("--no-probe", action="store_true", help="skip the DDL probe")
    verify = sub.add_parser("verify", parents=[common, batch], help="compare source and target")
    verify.add_argument("--rows", choices=("exact", "none"), help="override options.verify_rows")
    sub.add_parser("status", parents=[common], help="show recorded progress")
    return parser


def describe_storage(config: Config) -> str:
    if config.s3:
        return f"s3://{config.s3.bucket}/{config.s3.prefix}"
    return f"local {config.local.path} ({config.local.format})"


def print_plan(config: Config, dbs: list[Database]) -> None:
    print(f"config:      {config.path}")
    print(f"backup name: {config.backup_name}")
    print(f"source:      {config.source.host}:{config.source.port}")
    print(f"destination: {config.destination.host}:{config.destination.port}")
    print(f"storage:     {describe_storage(config)}")
    print(f"parallel:    {config.parallel}")
    for group in config.groups:
        members = [db for db in dbs if db.group == group]
        if not members:
            continue
        creds = members[0].source_credentials.user
        target_user = members[0].target_credentials.user
        users = creds if creds == target_user else f"{creds} → {target_user}"
        print(f"group {group} (user {users}):")
        for db in members:
            rename = "" if db.name == db.target else f" → {db.target}"
            print(f"  {db.name}{rename}")
    print(f"{len(dbs)} database(s)")


def print_status(state: State, dbs: list[Database]) -> None:
    data = state.all()
    rows = [("database", "group", "backup", "restore", "verify")]
    for db in dbs:
        entry = data.get(db.name, {})
        backup = entry.get("backup", {})
        restore = entry.get("restore", {})
        verify = entry.get("verify", {})
        backup_text = backup.get("status", "-")
        if backup.get("size"):
            backup_text += f" {format_size(backup['size'])}"
        restore_text = restore.get("status", "-")
        if restore.get("phases_done") is not None and restore.get("status") != "done":
            restore_text += f" phases {restore.get('phases_done')}"
        rows.append((db.name, db.group, backup_text, restore_text, verify.get("status", "-")))
    widths = [max(len(str(row[i])) for row in rows) for i in range(5)]
    for row in rows:
        print("  ".join(str(cell).ljust(width) for cell, width in zip(row, widths, strict=True)))


def confirm(command: str, config: Config, dbs: list[Database], assume_yes: bool) -> bool:
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        log.error(
            "%s changes databases or storage; pass --yes when running without a terminal", command
        )
        return False
    print(
        f"\nAbout to {command} {len(dbs)} database(s): {config.source.host} → "
        f"{config.destination.host} via {describe_storage(config)}",
        file=sys.stderr,
    )
    try:
        answer = input('Type "yes" to continue: ')
    except EOFError:
        return False
    return answer.strip().lower() == "yes"


def run_batch(
    ctx: Context,
    dbs: list[Database],
    step: Callable[[DatabaseJob], str],
    parallel: int,
    fail_fast: bool,
) -> dict[str, tuple[str, str]]:
    results: dict[str, tuple[str, str]] = {}
    queue = list(dbs)
    pending: dict[Future, Database] = {}
    stop_scheduling = False
    with ThreadPoolExecutor(max_workers=parallel) as pool:
        while queue or pending:
            while (
                queue
                and len(pending) < parallel
                and not stop_scheduling
                and not SUPERVISOR.stop.is_set()
            ):
                db = queue.pop(0)
                pending[pool.submit(_run_job, ctx, db, step)] = db
            if not pending:
                break
            done, _ = wait(pending, timeout=1, return_when=FIRST_COMPLETED)
            for future in done:
                db = pending.pop(future)
                results[db.name] = future.result()
                if results[db.name][0] == "failed" and fail_fast:
                    stop_scheduling = True
    for db in queue:
        results[db.name] = ("not started", "")
    return results


def _run_job(ctx: Context, db: Database, step: Callable[[DatabaseJob], str]) -> tuple[str, str]:
    job = None
    try:
        SUPERVISOR.check()
        job = DatabaseJob(ctx, db)
        return "ok", step(job) or "done"
    except Interrupted:
        db_logger(db.name).error("interrupted")
        return "interrupted", ""
    except ToolError as exc:
        job.log.error("%s", exc)
        return "failed", exc.summary
    except (JobError, CleanupError, StorageError, psycopg.Error, OSError) as exc:
        if SUPERVISOR.stop.is_set():
            db_logger(db.name).error("interrupted")
            return "interrupted", ""
        db_logger(db.name).error("%s", exc)
        return "failed", str(exc).strip().splitlines()[0] if str(exc).strip() else type(
            exc
        ).__name__
    except Exception as exc:
        db_logger(db.name).exception("unexpected error: %s", exc)
        return "failed", f"unexpected {type(exc).__name__}: {exc}"


def run_checks(ctx: Context, dbs: list[Database], parallel: int, fail_fast: bool = False) -> int:
    problems = 0
    log.info("client: %s", ctx.client_version)
    try:
        ctx.store.check_access()
        log.info("storage: %s is writable", describe_storage(ctx.config))
    except Exception as exc:
        problems += 1
        log.error("storage: %s", MASKER(str(exc)))
        if fail_fast:
            return EXIT_FAILED
    results: dict[str, CheckResult] = {}

    def check(job: DatabaseJob) -> str:
        results[job.db.name] = job.check()
        if fail_fast and results[job.db.name].problems:
            raise JobError("preflight failed; stopping new checks (--fail-fast)")
        return "checked"

    outcome = run_batch(ctx, dbs, check, parallel, fail_fast=fail_fast)
    total = 0
    for db in dbs:
        dblog = db_logger(db.name)
        result = results.get(db.name)
        if result is None:
            problems += 1
            dblog.error("check did not complete: %s", outcome.get(db.name, ("", ""))[1])
            continue
        for line in result.lines:
            dblog.info("%s", line)
        for warning in result.warnings:
            dblog.warning("%s", warning)
        for problem in result.problems:
            dblog.error("%s", problem)
        problems += len(result.problems)
        total += result.source_size or 0
    log.info("total source size: %s in %d database(s)", format_size(total), len(dbs))
    if isinstance(ctx.store, LocalStore):
        free = ctx.store.free_bytes()
        message = f"local storage free space: {format_size(free)}"
        if free < total:
            log.warning("%s, less than the total source size", message)
        else:
            log.info(message)
    if problems:
        log.error("check found %d problem(s)", problems)
        return EXIT_FAILED
    log.info("check passed")
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    isolate_libpq_environment()
    previous_umask = os.umask(0o077)
    try:
        return _main(argv)
    except KeyboardInterrupt:
        log.info("interrupted")
        return EXIT_INTERRUPTED
    finally:
        os.umask(previous_umask)


def _main(argv: list[str] | None = None) -> int:
    SUPERVISOR.stop.clear()
    args = build_parser().parse_args(argv)
    setup_logging(args.verbose)
    parallel = getattr(args, "parallel", None)
    if parallel is not None and not 1 <= parallel <= 64:
        log.error("--parallel must be between 1 and 64")
        return EXIT_CONFIG
    heartbeat = getattr(args, "progress_interval", 60.0)
    if not math.isfinite(heartbeat) or heartbeat < 0:
        log.error("--progress-interval must be a finite non-negative number")
        return EXIT_CONFIG
    start, end = getattr(args, "from_phase", None), getattr(args, "to_phase", 4)
    if start and start > end:
        log.error("--from-phase must not be after --to-phase")
        return EXIT_CONFIG
    if start and start > 1 and getattr(args, "clean_target", False):
        log.error("--clean-target requires starting at phase 1")
        return EXIT_CONFIG
    try:
        config = load_config(args.config)
        dbs = config.select(args.group, args.db)
    except ConfigError as exc:
        log.error("config: %s", exc)
        return EXIT_CONFIG
    MASKER.update(config.secrets())
    if not dbs:
        log.error("no databases selected")
        return EXIT_CONFIG
    if args.command == "validate":
        print_plan(config, dbs)
        return EXIT_OK

    work_dir = Path(args.work_dir).resolve()
    try:
        state = State(work_dir / "state" / f"{config.backup_name}.json")
        if args.command == "status":
            print_status(state, dbs)
            return EXIT_OK
        with state.lock_run():
            return _execute(args, config, dbs, work_dir, state)
    except (ValueError, OSError) as exc:
        log.error("work directory: %s", exc)
        return EXIT_FAILED


def _execute(args, config: Config, dbs: list[Database], work_dir: Path, state: State) -> int:

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_log = work_dir / "logs" / config.backup_name / f"sqdb-{stamp}-{args.command}.log"
    setup_logging(args.verbose, run_log)
    log.info("sqdb %s %s: %d database(s), log %s", __version__, args.command, len(dbs), run_log)

    try:
        major, version_text = client_version()
        store = S3Store(config.s3) if config.s3 else LocalStore(config.local, work_dir)
    except Exception as exc:
        log.error("setup: %s", MASKER(str(exc)))
        return EXIT_FAILED
    options = RunOptions(
        reuse_dumps=getattr(args, "reuse_dumps", False),
        overwrite_dumps=getattr(args, "overwrite_dumps", False),
        clean_target=getattr(args, "clean_target", False),
        from_phase=getattr(args, "from_phase", None),
        to_phase=getattr(args, "to_phase", 4),
        probe=not getattr(args, "no_probe", False),
        create_databases=getattr(args, "create_databases", False),
        rows=getattr(args, "rows", None),
        heartbeat=args.progress_interval,
    )
    if options.from_phase and options.from_phase > options.to_phase:
        log.error("--from-phase must not be after --to-phase")
        return EXIT_CONFIG
    ctx = Context(config, work_dir, options, state, store, major, version_text)
    parallel = args.parallel or config.parallel
    # Keep the normal SIGINT behavior while input() is waiting for a reply.
    # No database jobs have started yet.
    if args.command in MUTATING and not confirm(args.command, config, dbs, args.yes):
        log.info("cancelled")
        return EXIT_FAILED

    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}

    def on_signal(signum, _frame) -> None:
        name = signal.Signals(signum).name
        if SUPERVISOR.stop.is_set():
            # A second signal does not wait for running queries (e.g. row counts).
            log.error("received %s again; exiting immediately", name)
            os._exit(EXIT_INTERRUPTED)
        log.warning("received %s; stopping running tools (again to exit at once)", name)
        SUPERVISOR.request_stop()

    for sig in previous:
        signal.signal(sig, on_signal)
    try:
        if args.command == "check":
            result = run_checks(ctx, dbs, parallel, args.fail_fast)
            return EXIT_INTERRUPTED if SUPERVISOR.stop.is_set() else result
        steps: dict[str, Callable[[DatabaseJob], str]] = {
            "backup": lambda job: job.backup().location,
            "restore": DatabaseJob.restore,
            "migrate": DatabaseJob.migrate,
            "verify": lambda job: "passed" if job.verify() else "failed",
        }
        results = run_batch(ctx, dbs, steps[args.command], parallel, args.fail_fast)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)

    log.info("summary of %s:", args.command)
    for db in dbs:
        status, detail = results[db.name]
        target = f" → {db.target}" if db.target != db.name else ""
        line = f"  {db.name}{target} [{db.group}]: {status}" + (f" ({detail})" if detail else "")
        (log.info if status == "ok" else log.error)(line)
    statuses = {status for status, _ in results.values()}
    if SUPERVISOR.stop.is_set() or "interrupted" in statuses:
        return EXIT_INTERRUPTED
    return EXIT_OK if statuses == {"ok"} else EXIT_FAILED
