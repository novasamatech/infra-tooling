# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Harness fixtures for the end-to-end tests (make test-e2e).

The session reseeds the harness from tests/harness/config.yaml, so every run
starts from the same source databases, an empty destination and an empty bucket.
The CLI runs as a subprocess, exactly as an operator runs it.
"""

import os
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
import pytest
import yaml
from psycopg import sql

from sqdb.config import load_config
from tests.harness.seed import Harness, create_target, harness_config, seed

ROOT = Path(__file__).resolve().parents[2]
BASE_CONFIG = ROOT / "tests" / "harness" / "config.yaml"
# Destination admin of the --create-databases tests; the base topology has none.
ADMIN_USER = "db_admin"
ADMIN_PASSWORD = "Adm1n-p@ss"
PG_BINDIR = os.environ.get("PG_BINDIR", "")


def pytest_collection_modifyitems(config, items):
    if os.environ.get("SQDB_HARNESS"):
        return
    skip = pytest.mark.skip(reason="needs the harness network (make test-e2e)")
    for item in items:
        if "e2e" in item.keywords:
            item.add_marker(skip)


@dataclass
class Run:
    returncode: int
    output: str


@dataclass
class E2E:
    harness: Harness
    raw: dict
    tmp: Path
    outputs: list[str] = field(default_factory=list)
    work_dirs: list[Path] = field(default_factory=list)

    def config(self, name: str, change: Callable[[dict], None] | None = None) -> Path:
        data = yaml.safe_load(yaml.safe_dump(self.raw))
        if change:
            change(data)
        path = self.tmp / f"{name}.yaml"
        path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True))
        return path

    def work(self, name: str) -> Path:
        path = self.tmp / "work" / name
        path.mkdir(parents=True, exist_ok=True)
        self.work_dirs.append(path)
        return path

    def sqdb(self, config: Path, work: Path, *args: str, timeout: int = 600) -> Run:
        proc = subprocess.run(
            [sys.executable, "-m", "sqdb", "--config", str(config), "--work-dir", str(work), *args],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, "PYTHONPATH": str(ROOT)},
        )
        output = proc.stdout + proc.stderr
        self.outputs.append(output)
        return Run(proc.returncode, output)

    def connect(self, server: str, dbname: str) -> psycopg.Connection:
        host = self.harness.old_host if server == "old" else self.harness.new_host
        return self.harness.superuser(host, dbname)

    def secrets(self) -> set[str]:
        return (load_config(BASE_CONFIG).secrets() | {ADMIN_PASSWORD}) - {self.harness.s3_secret}

    def create_target(self, name: str, owner: str) -> None:
        """An empty target database, as the database service's API creates it."""
        with self.connect("new", "postgres") as conn:
            create_target(conn, name, owner)


def schema_dump(e2e: E2E, server: str, dbname: str) -> list[str]:
    """pg_dump --schema-only text without comments and version-specific settings."""
    host = e2e.harness.old_host if server == "old" else e2e.harness.new_host
    pg_dump = str(Path(PG_BINDIR, "pg_dump")) if PG_BINDIR else "pg_dump"
    proc = subprocess.run(
        [
            pg_dump,
            "--schema-only",
            "--no-owner",
            "--no-privileges",
            "-d",
            f"host={host} dbname='{dbname}' user=postgres sslmode=disable",
        ],
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ, "PGPASSWORD": e2e.harness.pg_password},
    )
    skip = ("--", "\\restrict", "\\unrestrict", "SET ", "SELECT pg_catalog.set_config")
    return [line for line in proc.stdout.splitlines() if line and not line.startswith(skip)]


def data_digest(conn: psycopg.Connection) -> dict[str, tuple[int, str]]:
    """Row count and an order-independent digest of every table's rows."""
    tables = conn.execute(
        """
        SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE (c.relkind = 'r' OR (c.relkind = 'm' AND c.relispopulated))
          AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname !~ '^pg_'
        ORDER BY 1, 2
        """
    ).fetchall()
    digest = {}
    for schema, table in tables:
        query = sql.SQL(
            "SELECT count(*), md5(coalesce(string_agg(t::text, E'\\n' ORDER BY t::text), '')) "
            "FROM {}.{} AS t"
        ).format(sql.Identifier(schema), sql.Identifier(table))
        digest[f"{schema}.{table}"] = tuple(conn.execute(query).fetchone())
    return digest


@pytest.fixture(scope="session")
def e2e(tmp_path_factory) -> E2E:
    harness = Harness.from_env()
    raw = harness_config(yaml.safe_load(BASE_CONFIG.read_text()), harness)
    config = load_config(BASE_CONFIG)
    seed(config, harness, rows=harness.rows, large_rows=harness.rows * 20)
    return E2E(harness=harness, raw=raw, tmp=tmp_path_factory.mktemp("e2e"))
