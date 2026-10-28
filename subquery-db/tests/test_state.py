# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

import json

import pytest

from sqdb.state import State


def test_updates_persist_and_merge(tmp_path):
    path = tmp_path / "state" / "run.json"
    state = State(path)
    state.update("db", "backup", status="running", location="s3://b/k")
    state.update("db", "backup", status="done", size=10)
    assert state.get("db", "backup")["location"] == "s3://b/k"
    state.update("db", "restore", replace=True, phases_done=[1])
    state.update("db", "restore", replace=True, status="running")
    assert "phases_done" not in state.get("db", "restore")
    reloaded = State(path)
    assert reloaded.get("db", "backup")["size"] == 10
    reloaded.clear("db", "restore")
    assert State(path).get("db", "restore") == {}
    assert State(path).get("other", "backup") == {}
    assert set(json.loads(path.read_text())["databases"]["db"]) == {"backup"}


def test_unknown_state_version_is_rejected(tmp_path):
    path = tmp_path / "run.json"
    path.write_text(json.dumps({"version": 99, "databases": {}}))
    with pytest.raises(ValueError, match="unsupported state version"):
        State(path)


def test_state_is_bound_to_source_target_and_storage(tmp_path):
    state = State(tmp_path / "run.json")
    source, target, storage = {"host": "old"}, {"database": "one"}, {"location": "s3://b/k"}
    state.bind("db", source, target, storage)
    for section in ("backup", "restore", "verify"):
        state.update("db", section, status="done")
    assert not state.bind("db", source, target, storage)
    assert state.bind("db", source, {"database": "two"}, storage)
    assert state.get("db", "backup")["status"] == "done"
    assert not state.get("db", "restore") and not state.get("db", "verify")
    assert state.bind("db", {"host": "different"}, target, storage)
    assert not state.get("db", "backup")


def test_work_directory_excludes_concurrent_writers(tmp_path):
    state = State(tmp_path / "state" / "one.json")
    other = State(tmp_path / "state" / "two.json")
    with (
        state.lock_run(),
        pytest.raises(ValueError, match="another sqdb command"),
        other.lock_run(),
    ):
        pytest.fail("second writer acquired the lock")
    with other.lock_run():
        other.update("db", "backup", status="done")


@pytest.mark.parametrize(
    "data",
    [
        [],
        {"version": 1},
        {"version": 1, "databases": {"db": []}},
        {"version": 1, "databases": {"db": {"restore": "done"}}},
    ],
)
def test_malformed_state_has_a_clear_error(tmp_path, data):
    path = tmp_path / "state.json"
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        State(path)
