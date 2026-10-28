# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Regression cases found during the initial implementation review."""

import json
import os
import re
import signal
import subprocess
import sys
import time
from types import SimpleNamespace

import psycopg
import pytest
from psycopg import sql

from tests.harness.seed import create_role, fixture_sql

from .conftest import ROOT, data_digest

pytestmark = pytest.mark.e2e
SOURCE = "chain-b-accounts"
OWNER = "accounts_user"
SCHEMA = "accounts-project"


@pytest.fixture
def review(e2e, request):
    name = "review-" + re.sub("[^a-z0-9-]", "-", request.node.name.lower())[-48:]
    e2e.create_target(name, OWNER)

    def change(data):
        data["backup_name"] = name
        data["storage"] = {"type": "local", "local": {"format": "custom"}}
        data["groups"] = {"accounts": data["groups"]["accounts"]}
        data["groups"]["accounts"]["databases"] = [{"name": SOURCE, "target": name}]
        data["options"] = {"verify_rows": "exact"}
        data["parallel"] = 1

    config = e2e.config(name, change)
    work = e2e.work(name)
    return SimpleNamespace(config=config, work=work, target=name, backup_name=name)


def migrate(e2e, review):
    run = e2e.sqdb(review.config, review.work, "migrate", "--yes")
    assert run.returncode == 0, run.output
    return run


def state(review):
    return json.loads((review.work / "state" / f"{review.backup_name}.json").read_text())[
        "databases"
    ][SOURCE]


def test_target_change_does_not_skip_using_previous_state(e2e, review):
    migrate(e2e, review)
    target = review.target + "-two"
    e2e.create_target(target, OWNER)
    import yaml

    data = yaml.safe_load(review.config.read_text())
    data["groups"]["accounts"]["databases"][0]["target"] = target
    review.config.write_text(yaml.safe_dump(data))
    run = migrate(e2e, review)
    assert "already migrated and verified; skipping" not in run.output
    assert "reusing dump" in run.output
    with e2e.connect("old", SOURCE) as source, e2e.connect("new", target) as dest:
        assert data_digest(source) == data_digest(dest)


def test_recreated_target_does_not_skip_using_previous_state(e2e, review):
    migrate(e2e, review)
    with e2e.connect("new", "postgres") as conn:
        conn.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(review.target)))
        conn.execute(
            sql.SQL("CREATE DATABASE {} OWNER {}").format(
                sql.Identifier(review.target), sql.Identifier(OWNER)
            )
        )
    run = migrate(e2e, review)
    assert "already migrated and verified; skipping" not in run.output


def test_overwrite_replaces_known_dump_and_invalidates_restore(e2e, review):
    migrate(e2e, review)
    first = state(review)["backup"]["dump"]
    run = e2e.sqdb(review.config, review.work, "backup", "--overwrite-dumps", "--yes")
    assert run.returncode == 0, run.output
    saved = state(review)
    assert saved["backup"]["dump"]["backup_id"] != first["backup_id"]
    assert saved["restore"]["status"] == "stale" and "verify" not in saved
    run = e2e.sqdb(review.config, review.work, "restore", "--from-phase", "4", "--yes")
    assert run.returncode == 1 and "dump changed" in run.output
    run = e2e.sqdb(review.config, review.work, "migrate", "--clean-target", "--yes")
    assert run.returncode == 0, run.output


def test_clean_target_rolls_back_cascades_into_unrelated_schemas(e2e, review):
    migrate(e2e, review)
    with e2e.connect("new", review.target) as conn:
        conn.execute("CREATE SCHEMA untouched")
        conn.execute('CREATE VIEW untouched.events AS SELECT * FROM "accounts-project".events')
        conn.execute("CREATE TABLE untouched.t (id int)")
        conn.execute(
            "CREATE TRIGGER protected BEFORE INSERT ON untouched.t "
            'FOR EACH ROW EXECUTE FUNCTION "accounts-project".touch_updated()'
        )
    run = e2e.sqdb(review.config, review.work, "restore", "--clean-target", "--yes")
    assert run.returncode == 1 and "outside the selected schemas; rolled back" in run.output
    with e2e.connect("new", review.target) as conn:
        assert conn.execute("SELECT count(*) FROM untouched.events").fetchone()[0] > 0
        assert conn.execute("SELECT 1 FROM pg_trigger WHERE tgname = 'protected'").fetchone()


def test_target_lock_excludes_another_work_directory(e2e, review):
    migrate(e2e, review)
    with e2e.connect("new", review.target) as conn:
        conn.execute("SELECT pg_advisory_lock(hashtext('sqdb migration'))")
        run = e2e.sqdb(
            review.config, e2e.work(review.target + "-other"), "restore", "--clean-target", "--yes"
        )
        assert run.returncode == 1 and "another sqdb migration" in run.output
        assert conn.execute('SELECT count(*) FROM "accounts-project".events').fetchone()[0] > 0


def test_check_fail_fast_stops_before_the_next_database(e2e, review):
    def change(data):
        data["groups"] = {"accounts": data["groups"]["accounts"]}
        data["groups"]["accounts"]["password"] = "review-wrong-password"

    config = e2e.config("review-check-fail-fast", change)
    run = e2e.sqdb(config, review.work, "check", "--no-probe", "--fail-fast", "--parallel", "1")
    assert run.returncode == 1, run.output
    assert run.output.count("password authentication failed") == 1
    assert f"[{SOURCE}]" in run.output and "check did not complete" in run.output


@pytest.mark.parametrize(
    "statement,category",
    [
        ('ALTER FUNCTION "accounts-project".touch_updated() SECURITY DEFINER', "routines"),
        ('ALTER TABLE "accounts-project"._metadata DISABLE TRIGGER metadata_touch', "triggers"),
        ('ALTER SEQUENCE "accounts-project".batch_seq INCREMENT BY 7', "sequences"),
        ("SELECT setval('\"accounts-project\".batch_seq', 2000, false)", "sequences"),
        ('ALTER DOMAIN "accounts-project".positive_amount SET NOT NULL', "types"),
        ('ALTER TABLE "accounts-project".events ENABLE ROW LEVEL SECURITY', "relations"),
    ],
)
def test_verify_detects_semantic_catalog_damage(e2e, review, statement, category):
    migrate(e2e, review)
    with e2e.connect("new", review.target) as conn:
        if "setval" in statement:
            # Match last_value while changing only is_called.
            value = conn.execute('SELECT last_value FROM "accounts-project".batch_seq').fetchone()[
                0
            ]
            conn.execute("SELECT setval('\"accounts-project\".batch_seq', %s, false)", (value,))
        else:
            conn.execute(statement)
    run = e2e.sqdb(review.config, review.work, "verify")
    assert run.returncode == 1, run.output
    assert f"{category}:" in run.output


def test_failed_restore_invalidates_previous_verification(e2e, review):
    migrate(e2e, review)
    run = e2e.sqdb(review.config, review.work, "restore", "--from-phase", "3", "--yes")
    assert run.returncode == 1
    assert "verify" not in state(review)


def test_enabling_exact_counts_cannot_skip_a_previous_catalog_only_verify(e2e, review):
    import yaml

    data = yaml.safe_load(review.config.read_text())
    data["options"]["verify_rows"] = "none"
    review.config.write_text(yaml.safe_dump(data))
    migrate(e2e, review)
    with e2e.connect("new", review.target) as conn:
        conn.execute('DELETE FROM "accounts-project".events WHERE id = 7')
    data["options"]["verify_rows"] = "exact"
    review.config.write_text(yaml.safe_dump(data))
    run = e2e.sqdb(review.config, review.work, "migrate", "--yes")
    assert run.returncode == 1 and "row_counts:" in run.output


def test_source_alias_cannot_be_cleaned_as_a_target(e2e, review):
    run = e2e.sqdb(review.config, review.work, "backup", "--yes")
    assert run.returncode == 0, run.output
    import yaml

    data = yaml.safe_load(review.config.read_text())
    # Different host strings resolving to the same PostgreSQL server.
    data["destination"] = {
        **data["source"],
        "host": f"{e2e.harness.old_host},{e2e.harness.old_host}",
    }
    data["groups"]["accounts"]["databases"] = [SOURCE]
    review.config.write_text(yaml.safe_dump(data))
    with e2e.connect("old", SOURCE) as conn:
        before = data_digest(conn)
    run = e2e.sqdb(review.config, review.work, "restore", "--clean-target", "--yes")
    assert run.returncode == 1 and "configured source database on the same server" in run.output
    with e2e.connect("old", SOURCE) as conn:
        assert data_digest(conn) == before


def test_names_with_spaces_restore_and_clean_correctly(e2e, review):
    import yaml

    owner, password, source, schema = (
        "review owner",
        "Review-owner-pw",
        "review-spaces-source",
        'a "schema" space',
    )
    with e2e.connect("old", "postgres") as old, e2e.connect("new", "postgres") as new:
        for conn in (old, new):
            create_role(conn, owner, password)
        old.execute(
            sql.SQL("CREATE DATABASE {} OWNER {}").format(
                sql.Identifier(source), sql.Identifier(owner)
            )
        )
        new.execute(
            sql.SQL("ALTER DATABASE {} OWNER TO {}").format(
                sql.Identifier(review.target), sql.Identifier(owner)
            )
        )
    with psycopg.connect(
        host=e2e.harness.old_host, dbname=source, user=owner, password=password, autocommit=True
    ) as conn:
        conn.execute(fixture_sql(conn, schema, 10))
        conn.execute(
            sql.SQL(
                "ALTER TABLE {}.events ADD COLUMN discarded text, ADD COLUMN retained text"
            ).format(sql.Identifier(schema))
        )
        conn.execute(
            sql.SQL("ALTER TABLE {}.events DROP COLUMN discarded").format(sql.Identifier(schema))
        )
        conn.execute(
            sql.SQL("ALTER INDEX {}.rewards_amount RENAME TO {}").format(
                sql.Identifier(schema), sql.Identifier("a parent index")
            )
        )
    data = yaml.safe_load(review.config.read_text())
    data["groups"] = {
        "spaces": {
            "user": owner,
            "password": password,
            "databases": [{"name": source, "target": review.target}],
        }
    }
    review.config.write_text(yaml.safe_dump(data))
    migrate(e2e, review)
    run = e2e.sqdb(review.config, review.work, "migrate", "--clean-target", "--yes")
    assert run.returncode == 0, run.output
    assert password not in run.output
    with e2e.connect("old", source) as src, e2e.connect("new", review.target) as dst:
        assert data_digest(src) == data_digest(dst)


def test_sigint_cancels_a_blocked_verify_query(e2e, review):
    migrate(e2e, review)
    with e2e.connect("new", review.target) as blocker, blocker.transaction():
        blocker.execute('LOCK TABLE "accounts-project".events IN ACCESS EXCLUSIVE MODE')
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "sqdb",
                "--config",
                str(review.config),
                "--work-dir",
                str(review.work),
                "verify",
            ],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env={**os.environ, "PYTHONPATH": str(ROOT)},
        )
        try:
            deadline = time.monotonic() + 15
            with e2e.connect("new", review.target) as observer:
                while not observer.execute(
                    "SELECT 1 FROM pg_stat_activity WHERE datname = %s "
                    "AND application_name = 'sqdb' AND wait_event_type = 'Lock'",
                    (review.target,),
                ).fetchone():
                    assert time.monotonic() < deadline, "verify never reached the blocked query"
                    time.sleep(0.05)
            proc.send_signal(signal.SIGINT)
            output, _ = proc.communicate(timeout=8)
            e2e.outputs.append(output)
            assert proc.returncode == 130, output
            assert state(review)["verify"]["status"] == "interrupted"
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.communicate(timeout=5)


def test_clean_target_needs_a_reachable_source(e2e, review):
    """Without the source, an alias of it cannot be ruled out: never clean blindly."""
    run = e2e.sqdb(review.config, review.work, "backup", "--yes")
    assert run.returncode == 0, run.output
    import yaml

    data = yaml.safe_load(review.config.read_text())
    data["source"]["port"] = 1  # unreachable
    # A target named like a configured source database is what the alias check guards.
    data["groups"]["accounts"]["databases"] = [SOURCE]
    review.config.write_text(yaml.safe_dump(data))
    with e2e.connect("new", "postgres") as conn:
        if not conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (SOURCE,)).fetchone():
            conn.execute(
                sql.SQL("CREATE DATABASE {} OWNER {}").format(
                    sql.Identifier(SOURCE), sql.Identifier(OWNER)
                )
            )
    with e2e.connect("new", SOURCE) as conn:
        conn.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS app AUTHORIZATION {}").format(
                sql.Identifier(OWNER)
            )
        )
        conn.execute("CREATE TABLE IF NOT EXISTS app.keep (id int)")
    run = e2e.sqdb(review.config, review.work, "restore", "--clean-target", "--yes")
    assert run.returncode == 1
    assert "cannot reach the source to confirm" in run.output
    with e2e.connect("new", SOURCE) as conn:
        assert conn.execute("SELECT to_regclass('app.keep')").fetchone()[0] is not None


def test_server_idle_session_timeout_does_not_drop_idle_connections(e2e, review):
    """The target lock and the pg_restore -j leader sit idle for long periods."""
    import yaml

    data = yaml.safe_load(review.config.read_text())
    data["options"]["restore_jobs"] = 2
    review.config.write_text(yaml.safe_dump(data))
    with e2e.connect("new", "postgres") as conn:
        conn.execute(
            sql.SQL("ALTER ROLE {} IN DATABASE {} SET idle_session_timeout = '300ms'").format(
                sql.Identifier(OWNER), sql.Identifier(review.target)
            )
        )
    try:
        run = migrate(e2e, review)
        assert "target lock connection was lost" not in run.output
    finally:
        with e2e.connect("new", "postgres") as conn:
            conn.execute(
                sql.SQL("ALTER ROLE {} IN DATABASE {} RESET idle_session_timeout").format(
                    sql.Identifier(OWNER), sql.Identifier(review.target)
                )
            )


def test_verify_reports_but_ignores_objects_the_restore_did_not_create(e2e, review):
    """A managed target may come with its own objects; verify must not fail on them."""
    with e2e.connect("new", review.target) as conn:
        conn.execute("CREATE SCHEMA service")
        conn.execute("CREATE TABLE service.audit (id int)")
        conn.execute("INSERT INTO service.audit VALUES (1)")
        conn.execute("CREATE TABLE public.service_table (id int)")
        conn.execute("COMMENT ON TABLE public.service_table IS 'created by the service'")
    run = migrate(e2e, review)
    assert "target-only object(s) the restore did not create were not compared" in run.output
    assert "service.audit" in run.output and "public.service_table" in run.output
    assert state(review)["verify"]["status"] == "passed"


def test_clean_target_protects_schemas_whose_names_start_with_pg(e2e, review):
    """LIKE 'pg_%' would also match pgx_*; only system schemas may be skipped."""
    migrate(e2e, review)
    with e2e.connect("new", review.target) as conn:
        conn.execute("CREATE SCHEMA pgx_sidecar")
        conn.execute(f'CREATE VIEW pgx_sidecar.events AS SELECT * FROM "{SCHEMA}".events')
    run = e2e.sqdb(review.config, review.work, "restore", "--clean-target", "--yes")
    assert run.returncode == 1 and "outside the selected schemas; rolled back" in run.output
    with e2e.connect("new", review.target) as conn:
        assert conn.execute("SELECT to_regclass('pgx_sidecar.events')").fetchone()[0]
