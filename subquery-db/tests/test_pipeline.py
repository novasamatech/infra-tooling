# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

import io
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from sqdb.pipeline import Context, DatabaseJob, JobError, RunOptions
from sqdb.state import State
from sqdb.storage import DumpInfo, S3Store
from sqdb.toc import parse_toc


@pytest.fixture
def job(config, s3, tmp_path, monkeypatch):
    ctx = Context(
        config,
        tmp_path,
        RunOptions(),
        State(tmp_path / "state.json"),
        S3Store(config.s3, client=s3),
        17,
        "pg_dump 17",
    )
    job = DatabaseJob(ctx, config.databases[0])
    monkeypatch.setattr(job, "source_conn", MagicMock())
    monkeypatch.setattr("sqdb.pipeline.database_facts", lambda _: SimpleNamespace(size=100))
    monkeypatch.setattr(
        "sqdb.pipeline.server_facts", lambda _: SimpleNamespace(version="16.0", major=16)
    )
    monkeypatch.setattr(job, "read_toc", lambda _: parse_toc("1; 2615 1 SCHEMA - app owner\n"))

    def run(_argv, _label, _env, stdout_consumer, **_kwargs):
        return SimpleNamespace(output=stdout_consumer(io.BytesIO(b"PGDMP-review"), lambda: True))

    monkeypatch.setattr(job, "_run", run)
    return job


def test_overwrite_takes_precedence_over_recorded_dump_and_invalidates_progress(job):
    first = job.backup()
    assert job.backup() == first
    job.ctx.state.update(job.db.name, "restore", status="done")
    job.ctx.state.update(job.db.name, "verify", status="passed")
    job.ctx.options.overwrite_dumps = True
    second = job.backup()
    assert second.manifest["backup_id"] != first.manifest["backup_id"]
    assert job.ctx.state.get(job.db.name, "restore")["status"] == "stale"
    assert not job.ctx.state.get(job.db.name, "verify")


def test_same_sized_replaced_archive_is_not_automatically_reused(job):
    info = job.backup()
    manifest = dict(info.manifest, backup_id="another-generation")
    job.ctx.store.write_manifest(job.db, job.backup_name, manifest)
    with pytest.raises(JobError, match="no record of creating"):
        job.backup()


@pytest.mark.parametrize(
    "field,value", [("source_database", "other"), ("size", 99), ("etag", "other")]
)
def test_manifest_must_match_the_archive(job, field, value):
    manifest = {"source_database": job.db.name, "size": 10, "etag": "etag", field: value}
    with pytest.raises(JobError, match="does not match"):
        job.validate_manifest(DumpInfo("s3://b/k", 10, manifest, "etag"))


def test_failed_verification_cannot_leave_a_previous_pass(job, monkeypatch):
    job.ctx.state.update(job.db.name, "verify", status="passed")

    def fail():
        raise OSError("connection lost")

    monkeypatch.setattr(job, "_verify", fail)
    with pytest.raises(OSError):
        job.verify()
    assert job.ctx.state.get(job.db.name, "verify")["status"] == "failed"
