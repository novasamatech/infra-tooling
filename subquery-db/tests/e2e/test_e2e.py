# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""End-to-end migration PostgreSQL 16 → 17 through S3 and local storage.

Tests run in file order and share the seeded harness: the main migration runs
once and later tests build on its dumps and targets.
"""

import json
import os
import subprocess
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

from sqdb.config import load_config
from tests.harness.seed import create_role, grant_admin_set

from .conftest import ADMIN_PASSWORD, ADMIN_USER, BASE_CONFIG, data_digest, schema_dump

pytestmark = pytest.mark.e2e

CONFIG = load_config(BASE_CONFIG)
DATABASES = {db.name: db for db in CONFIG.databases}


@pytest.fixture(scope="module")
def migrated(e2e):
    config = e2e.config("main")
    work = e2e.work("main")
    check = e2e.sqdb(config, work, "check")
    assert check.returncode == 0, check.output
    run = e2e.sqdb(config, work, "migrate", "--yes")
    assert run.returncode == 0, run.output
    return config, work, run


def test_validate_prints_plan(e2e):
    run = e2e.sqdb(e2e.config("main"), e2e.work("validate"), "validate")
    assert run.returncode == 0, run.output
    assert "chain-b-transfers → chain-b-transfers-v2" in run.output
    assert "group nfts (user NftUser)" in run.output


def test_check_reports_wrong_password_without_leaking_it(e2e):
    wrong = "not-the-nft-password"

    def change(data):
        data["groups"]["nfts"]["password"] = wrong

    run = e2e.sqdb(
        e2e.config("wrong-password", change), e2e.work("wrong-password"), "check", "--no-probe"
    )
    assert run.returncode == 1
    assert 'password authentication failed for user "NftUser"' in run.output
    assert "[chain-c-nfts]" in run.output and "[123]" in run.output
    assert wrong not in run.output


def test_migrate_reproduces_every_database(e2e, migrated):
    for db in CONFIG.databases:
        assert schema_dump(e2e, "new", db.target) == schema_dump(e2e, "old", db.name), db.name
        with e2e.connect("old", db.name) as src, e2e.connect("new", db.target) as dst:
            source_digest = data_digest(src)
            assert source_digest == data_digest(dst), db.name
            assert len(source_digest) >= 10
            owners = dst.execute(
                """
                SELECT DISTINCT pg_get_userbyid(c.relowner) FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'public')
                  AND n.nspname !~ '^pg_'
                """
            ).fetchall()
            assert owners == [(db.target_credentials.user,)], db.name


def test_migrate_records_state_reports_and_manifests(e2e, migrated):
    _, work, _ = migrated
    state = json.loads((work / "state" / "e2e.json").read_text())["databases"]
    client = e2e.harness.s3()
    for db in CONFIG.databases:
        entry = state[db.name]
        assert entry["backup"]["status"] == "done"
        assert entry["restore"]["status"] == "done"
        assert entry["restore"]["phases_done"] == [1, 2, 3, 4]
        assert entry["verify"]["status"] == "passed"
        report = json.loads((work / "reports" / "e2e" / f"{db.name}.verify.json").read_text())
        assert report["ok"] and report["rows_checked"]
        manifest = json.loads(
            client.get_object(Bucket="sqdb-dumps", Key=f"{db.name}/e2e.manifest.json")[
                "Body"
            ].read()
        )
        head = client.head_object(Bucket="sqdb-dumps", Key=f"{db.name}/e2e.dump")
        assert manifest["size"] == head["ContentLength"]
        assert manifest["source_server_version"].startswith("16.")
    # The first database is larger than one 5 MiB part: a real multipart upload.
    first = CONFIG.databases[0].name
    head = client.head_object(Bucket="sqdb-dumps", Key=f"{first}/e2e.dump", PartNumber=1)
    assert int(head["PartsCount"]) >= 2


def test_rerun_skips_finished_databases(e2e, migrated):
    config, work, _ = migrated
    client = e2e.harness.s3()
    before = {
        db: client.head_object(Bucket="sqdb-dumps", Key=f"{db}/e2e.dump")["ETag"]
        for db in DATABASES
    }
    run = e2e.sqdb(config, work, "migrate", "--yes")
    assert run.returncode == 0, run.output
    assert run.output.count("already migrated and verified; skipping") == len(DATABASES)
    after = {
        db: client.head_object(Bucket="sqdb-dumps", Key=f"{db}/e2e.dump")["ETag"]
        for db in DATABASES
    }
    assert before == after


def test_verify_detects_damage_and_clean_target_repairs_it(e2e, migrated):
    config, work, _ = migrated
    db = DATABASES["chain-c-nfts"]
    with e2e.connect("new", db.target) as conn:
        conn.execute('DELETE FROM "nfts-project".events WHERE id = 7')
        conn.execute('DROP INDEX "nfts-project".transfers_tags')
        conn.execute("""COMMENT ON TABLE "nfts-project".transfers IS 'changed'""")
    run = e2e.sqdb(config, work, "verify", "--db", db.name)
    assert run.returncode == 1
    assert 'indexes: 1 missing in target: "nfts-project".transfers_tags' in run.output
    assert 'row_counts: "nfts-project".events differs' in run.output
    assert 'comments: table "nfts-project".transfers differs' in run.output
    report = json.loads((work / "reports" / "e2e" / f"{db.name}.verify.json").read_text())
    assert not report["ok"] and set(report["differences"]) >= {"indexes", "row_counts"}

    run = e2e.sqdb(config, work, "migrate", "--db", db.name, "--clean-target", "--yes")
    assert run.returncode == 0, run.output
    assert "reusing dump" in run.output


def test_existing_dump_needs_an_explicit_choice(e2e, migrated):
    config, _, _ = migrated
    run = e2e.sqdb(config, e2e.work("foreign-dumps"), "backup", "--db", "chain-b-accounts", "--yes")
    assert run.returncode == 1
    assert "--reuse-dumps" in run.output and "--overwrite-dumps" in run.output


def test_non_empty_target_is_refused_then_cleaned(e2e, migrated):
    config, _, _ = migrated
    work = e2e.work("clean-target")
    run = e2e.sqdb(config, work, "restore", "--db", "chain-b-accounts", "--yes")
    assert run.returncode == 1
    assert "is not empty" in run.output and "--clean-target" in run.output
    run = e2e.sqdb(config, work, "restore", "--db", "chain-b-accounts", "--clean-target", "--yes")
    assert run.returncode == 0, run.output
    assert "dropping schema accounts-project" in run.output
    run = e2e.sqdb(config, work, "verify", "--db", "chain-b-accounts")
    assert run.returncode == 0, run.output


def test_app_schema_marks_a_target_as_not_empty(e2e, migrated):
    """As in the original scripts: a target with schema app is never loaded into."""
    db = DATABASES["chain-a-transfers"]  # its dump creates schema subql_2, not app
    target = "chain-a-transfers-app"

    def change(data):
        data["groups"] = {"transfers": data["groups"]["transfers"]}
        data["groups"]["transfers"]["databases"] = [{"name": db.name, "target": target}]

    e2e.create_target(target, db.target_credentials.user)
    with e2e.connect("new", target) as conn:
        conn.execute(
            sql.SQL("CREATE SCHEMA app AUTHORIZATION {}").format(
                sql.Identifier(db.target_credentials.user)
            )
        )
    config, work = e2e.config("app-guard", change), e2e.work("app-guard")
    run = e2e.sqdb(config, work, "check", "--no-probe")
    assert run.returncode == 1
    assert f"target {target} is not empty (schema app exists)" in run.output
    run = e2e.sqdb(config, work, "migrate", "--reuse-dumps", "--yes")
    assert run.returncode == 1
    assert "is not empty (schema app exists); nothing was loaded" in run.output
    assert "backup:" not in run.output  # stopped before any dump work
    with e2e.connect("new", target) as conn:
        schemas = {row[0] for row in conn.execute("SELECT nspname FROM pg_namespace").fetchall()}
        assert "app" in schemas and "subql_2" not in schemas


def test_missing_target_is_an_error_without_create_databases(e2e, migrated):
    target = "chain-b-accounts-missing"

    def change(data):
        data["groups"] = {"accounts": data["groups"]["accounts"]}
        data["groups"]["accounts"]["databases"] = [{"name": "chain-b-accounts", "target": target}]

    config, work = e2e.config("missing", change), e2e.work("missing")
    for args in (
        ("check", "--no-probe"),
        ("restore", "--yes"),
        ("migrate", "--reuse-dumps", "--yes"),
    ):
        run = e2e.sqdb(config, work, *args)
        assert run.returncode == 1, args
        assert f"target database {target} does not exist" in run.output, args
        assert "--create-databases" in run.output, args
    with e2e.connect("new", "postgres") as conn:
        assert not conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (target,)
        ).fetchone()


def test_indexes_are_built_before_data_and_constraints(e2e, migrated):
    target = "chain-a-transfers-phases"

    def change(data):
        data["groups"] = {"transfers": data["groups"]["transfers"]}
        data["groups"]["transfers"]["databases"] = [{"name": "chain-a-transfers", "target": target}]

    e2e.create_target(target, DATABASES["chain-a-transfers"].target_credentials.user)
    config, work = e2e.config("phases", change), e2e.work("phases")
    run = e2e.sqdb(config, work, "restore", "--to-phase", "2", "--yes")
    assert run.returncode == 0, run.output
    with e2e.connect("new", target) as conn:
        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname = 'subql_2'"
            ).fetchall()
        }
        assert {"transfers_tags", "rewards_amount", "0x1f2e3d4c5b6a7980"} <= indexes
        assert not any(name.endswith("_pkey") for name in indexes)
        assert data_digest(conn) and all(count == 0 for count, _ in data_digest(conn).values())
        constraints = conn.execute(
            "SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace "
            "WHERE n.nspname = 'subql_2' AND c.contype IN ('p', 'f', 'u', 'x')"
        ).fetchone()[0]
        triggers = conn.execute(
            "SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal"
        ).fetchone()[0]
        assert (constraints, triggers) == (0, 0)
    state = json.loads((work / "state" / "e2e.json").read_text())["databases"]
    assert state["chain-a-transfers"]["restore"]["phases_done"] == [1, 2]
    assert state["chain-a-transfers"]["restore"]["status"] == "partial"

    run = e2e.sqdb(config, work, "restore", "--from-phase", "3", "--yes")
    assert run.returncode == 0, run.output
    run = e2e.sqdb(config, work, "verify")
    assert run.returncode == 0, run.output


@pytest.mark.parametrize("fmt", ["directory", "custom"])
def test_local_storage(e2e, fmt):
    def change(data):
        data["storage"] = {"type": "local", "local": {"format": fmt}}
        data["groups"] = {"nfts": data["groups"]["nfts"]}
        group = data["groups"]["nfts"]
        group["databases"] = [
            {"name": name, "target": f"{name}-{fmt}"} for name in ("chain-c-nfts", "123")
        ]
        if fmt == "directory":
            group["options"].update(dump_jobs=2, restore_jobs=2)

    for name in ("chain-c-nfts", "123"):
        e2e.create_target(f"{name}-{fmt}", DATABASES[name].target_credentials.user)
    config, work = e2e.config(f"local-{fmt}", change), e2e.work(f"local-{fmt}")
    run = e2e.sqdb(config, work, "migrate", "--yes")
    assert run.returncode == 0, run.output
    for name in ("chain-c-nfts", "123"):
        base = work / "dumps" / name
        dump = base / ("e2e.dir" if fmt == "directory" else "e2e.dump")
        assert (dump / "toc.dat").exists() if fmt == "directory" else dump.is_file()
        assert (base / "e2e.manifest.json").exists()
        assert schema_dump(e2e, "new", f"{name}-{fmt}") == schema_dump(e2e, "old", name)


def test_restore_of_dump_made_by_pg_dump_16(e2e, tmp_path):
    db = DATABASES["chain-b-accounts"]
    dump = tmp_path / "legacy.dump"
    pg_dump = Path(os.environ["LEGACY_PG_BINDIR"], "pg_dump")
    subprocess.run(
        [
            str(pg_dump),
            "-Fc",
            "-Z",
            "0",
            "-f",
            str(dump),
            "-d",
            f"host={e2e.harness.old_host} dbname={db.name} user={db.source_credentials.user} "
            "sslmode=disable",
        ],
        check=True,
        env={**os.environ, "PGPASSWORD": db.source_credentials.password},
    )
    e2e.harness.s3().upload_file(str(dump), "sqdb-dumps", f"{db.name}/legacy.dump")

    def change(data):
        data["backup_name"] = "legacy"
        data["groups"] = {"accounts": data["groups"]["accounts"]}
        data["groups"]["accounts"]["databases"] = [{"name": db.name, "target": f"{db.name}-legacy"}]

    e2e.create_target(f"{db.name}-legacy", db.target_credentials.user)
    config, work = e2e.config("legacy", change), e2e.work("legacy")
    run = e2e.sqdb(config, work, "restore", "--yes")
    assert run.returncode == 0, run.output
    assert "has no manifest" in run.output
    run = e2e.sqdb(config, work, "verify")
    assert run.returncode == 0, run.output


def test_failed_dump_leaves_no_object(e2e):
    """pg_dump fails after streaming data: the multipart upload must be aborted."""
    db = DATABASES["chain-a-accounts"]
    name = "rls-broken"
    with e2e.connect("old", "postgres") as admin:
        admin.execute(
            sql.SQL("CREATE DATABASE {} OWNER {}").format(
                sql.Identifier(name), sql.Identifier(db.source_credentials.user)
            )
        )
    from tests.harness.seed import fixture_sql

    with psycopg.connect(
        host=e2e.harness.old_host,
        dbname=name,
        user=db.source_credentials.user,
        password=db.source_credentials.password,
        sslmode="disable",
        autocommit=True,
    ) as conn:
        conn.execute(fixture_sql(conn, "app", e2e.harness.rows * 20))
    with e2e.connect("old", name) as admin:
        # Owned by another role with row-level security: pg_dump fails on its
        # data, which comes after the other tables' data in the archive.
        admin.execute(f"""
            CREATE TABLE app.zzz_secret (id int);
            INSERT INTO app.zzz_secret VALUES (1);
            ALTER TABLE app.zzz_secret ENABLE ROW LEVEL SECURITY;
            CREATE POLICY hide ON app.zzz_secret USING (false);
            GRANT SELECT ON app.zzz_secret TO "{db.source_credentials.user}";
        """)

    def change(data):
        data["backup_name"] = "broken"
        data["groups"] = {"accounts": data["groups"]["accounts"]}
        data["groups"]["accounts"]["databases"] = [name]

    config, work = e2e.config("broken", change), e2e.work("broken")
    run = e2e.sqdb(config, work, "backup", "--yes")
    assert run.returncode == 1
    assert "row-level security" in run.output
    assert "aborted the incomplete upload" in run.output
    client = e2e.harness.s3()
    listed = client.list_objects_v2(Bucket="sqdb-dumps", Prefix=f"{name}/")
    assert listed.get("KeyCount", 0) == 0
    uploads = client.list_multipart_uploads(Bucket="sqdb-dumps", Prefix=f"{name}/")
    assert not uploads.get("Uploads")
    state = json.loads((work / "state" / "broken.json").read_text())["databases"]
    assert state[name]["backup"]["status"] == "failed"


@pytest.mark.parametrize("mode", ["owner", "grants", "group-user"])
def test_create_databases_is_opt_in(e2e, mode):
    """--create-databases: as an admin with OWNER, with grants, or as the group user."""
    db = DATABASES["chain-a-transfers"]
    user = db.target_credentials.user
    target = f"chain-a-transfers-{mode}"

    def change(data):
        data["groups"] = {"transfers": data["groups"]["transfers"]}
        data["groups"]["transfers"]["databases"] = [{"name": db.name, "target": target}]
        if mode != "group-user":
            data["destination"]["admin"] = {"user": ADMIN_USER, "password": ADMIN_PASSWORD}

    with e2e.connect("new", "postgres") as conn:
        if mode == "group-user":
            conn.execute(sql.SQL("ALTER ROLE {} CREATEDB").format(sql.Identifier(user)))
        else:
            create_role(conn, ADMIN_USER, ADMIN_PASSWORD, "CREATEDB", "CREATEROLE")
            if mode == "owner":
                grant_admin_set(conn, user, ADMIN_USER)
    try:
        config, work = e2e.config(f"create-{mode}", change), e2e.work(f"create-{mode}")
        run = e2e.sqdb(config, work, "check", "--no-probe", "--create-databases")
        assert run.returncode == 0, run.output
        assert f"target {target}: missing" in run.output
        run = e2e.sqdb(config, work, "migrate", "--reuse-dumps", "--create-databases", "--yes")
        assert run.returncode == 0, run.output
        assert ("granting privileges instead" in run.output) == (mode == "grants")
        with e2e.connect("new", target) as conn:
            owner = conn.execute(
                "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = %s", (target,)
            ).fetchone()[0]
        assert owner == (ADMIN_USER if mode == "grants" else user)
    finally:
        with e2e.connect("new", "postgres") as conn:
            conn.execute(sql.SQL("ALTER ROLE {} NOCREATEDB").format(sql.Identifier(user)))
            if mode == "owner":
                conn.execute(
                    sql.SQL("REVOKE {} FROM {}").format(
                        sql.Identifier(user), sql.Identifier(ADMIN_USER)
                    )
                )


def test_no_secret_in_outputs_or_files(e2e):
    secrets = e2e.secrets()
    assert secrets
    for output in e2e.outputs:
        for secret in secrets:
            assert secret not in output
    for work in e2e.work_dirs:
        for path in work.rglob("*"):
            if not path.is_file() or "dumps" in path.parts:
                continue
            text = path.read_bytes().decode("utf-8", "replace")
            for secret in secrets:
                assert secret not in text, str(path)
