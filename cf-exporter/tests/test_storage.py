# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""The SQLite storage heals itself: missing, corrupt, outdated, removed and failing
databases are recreated, and the exporter fills them again from Cloudflare."""

import logging
import os
import shutil
import sqlite3
from contextlib import closing

import pytest
from conftest import BUCKET, T0, Clock, analytics, event, run, values

import script

ZONE = {"zone_name": "example.org"}


def result(bucket_end, value=1.0, signature="s"):
    samples = (("cf_requests_5m", '{zone_name="example.org"}', value),)
    return script.BucketResult(bucket_end, samples, signature)


def rows(path, query):
    with closing(sqlite3.connect(path)) as conn:
        return conn.execute(query).fetchall()


def served(storage, budget=100):
    """(zone ID, bucket end) of the buckets a response would carry."""
    return [row[:2] for row in storage.exposition(budget)[0]]


@pytest.fixture
def path(tmp_path):
    return str(tmp_path / "state" / "cf-exporter.sqlite3")


def test_missing_directory_and_file_are_created(path):
    storage = script.Storage(path)
    assert storage.persistent and storage.usable() and storage.resets == 0
    assert os.path.exists(path)
    assert rows(path, "PRAGMA application_id") == [(script.APPLICATION_ID,)]
    assert rows(path, "PRAGMA user_version") == [(script.SCHEMA_VERSION,)]
    assert rows(path, "PRAGMA journal_mode") == [("wal",)]


def test_state_survives_a_restart(path):
    storage = script.Storage(path)
    storage.ensure("z1", T0, T0 + 2 * BUCKET)
    storage.accept("z1", result(T0, 5), now=T0 + 600)
    reopened = script.Storage(path)
    assert reopened.resets == 0
    assert reopened.pending("z1", T0 + 900, T0, T0 + 900, 10) == [
        (T0 + 2 * BUCKET, 0, 0),
        (T0 + BUCKET, 0, 0),
    ]
    picked, samples = reopened.exposition(100)
    assert picked == [("z1", T0, T0 + 600)]
    assert samples == [("cf_requests_5m", '{zone_name="example.org"}', 5.0, T0)]


def own_header(rest: bytes) -> bytes:
    """The first bytes of a database file of the exporter, followed by `rest`."""
    header = bytearray(script.SQLITE_MAGIC + b"\x10\x00\x02\x02" + b"\x00" * 80)
    header[68:72] = script.APPLICATION_ID.to_bytes(4, "big")
    return bytes(header) + rest


@pytest.mark.parametrize(
    "content",
    [own_header(b"\xff" * 4000), b"\x00" * 100, b"\x00" * 70000],
    ids=["own-damaged", "zeros", "many-zeros"],
)
def test_corrupt_database_of_the_exporter_is_replaced(path, content, caplog):
    os.makedirs(os.path.dirname(path))
    with open(path, "wb") as file:
        file.write(content)
    with caplog.at_level(logging.WARNING):
        storage = script.Storage(path)
    assert storage.persistent and storage.resets == 1
    assert "creating a new one" in caplog.text
    assert storage.ensure("z1", T0, T0) == 1
    assert rows(path, "SELECT count(*) FROM buckets") == [(1,)]


@pytest.mark.parametrize(
    "content",
    [
        b"this is not a database",
        b"SQLite format 3\x00" + b"\xff" * 4000,
        b"\x00" * 70000 + b"data at the end",
    ],
    ids=["text", "foreign-damaged", "zeros-then-data"],
)
def test_file_that_is_not_recognizably_the_exporters_is_left_alone(path, content, caplog):
    """A mistyped path must not cost anybody a file: only what is recognizably the
    exporter's is replaced, damaged or not."""
    os.makedirs(os.path.dirname(path))
    with open(path, "wb") as file:
        file.write(content)
    clock = Clock(0)
    with caplog.at_level(logging.ERROR):
        storage = script.Storage(path, monotonic=clock)
    assert not storage.persistent and storage.resets == 0
    assert "not recognizably the exporter's" in caplog.text
    storage.ensure("z1", T0, T0)
    clock.advance(script.STORAGE_RETRY_SECONDS)
    assert storage.pending("z1", T0, T0, T0, 10) == [(T0, 0, 0)]
    with open(path, "rb") as file:
        assert file.read() == content


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads whatever the mode says")
def test_file_that_cannot_be_read_is_not_taken_for_the_exporters(path):
    os.makedirs(os.path.dirname(path))
    with open(path, "wb") as file:
        file.write(b"\x00" * 100)
    os.chmod(path, 0)
    try:
        assert not script.recognizably_ours(path)
    finally:
        os.chmod(path, 0o600)
    assert script.recognizably_ours(path)


def test_new_database_carries_the_exporters_id_in_its_file_header(path):
    """The schema is written before the switch to the write-ahead log, so that the
    file itself shows whose it is, also when its log is lost."""
    script.Storage(path)
    with open(path, "rb") as file:
        header = file.read(100)
    assert header.startswith(script.SQLITE_MAGIC)
    assert int.from_bytes(header[68:72], "big") == script.APPLICATION_ID
    assert script.recognizably_ours(path)


def test_damaged_pages_are_detected_and_the_database_replaced(path):
    storage = script.Storage(path)
    storage.ensure("z1", T0 - 5000 * BUCKET, T0)
    for index in range(200):
        storage.accept("z1", result(T0 - index * BUCKET, index), now=T0)
    storage._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    storage._conn.close()
    size = os.path.getsize(path)
    with open(path, "r+b") as file:
        file.seek(size // 2)
        file.write(b"\xa5" * (size // 4))
    replaced = script.Storage(path)
    assert replaced.persistent and replaced.resets == 1
    assert replaced.summary()["zones"] == {}


@pytest.mark.parametrize("version", [0, script.SCHEMA_VERSION + 1])
def test_database_of_another_schema_version_is_replaced(path, version):
    script.Storage(path)._conn.execute(f"PRAGMA user_version={version}")
    storage = script.Storage(path)
    assert storage.persistent and storage.resets == 1
    assert rows(path, "PRAGMA user_version") == [(script.SCHEMA_VERSION,)]


def test_database_without_the_tables_is_replaced_on_first_use(path):
    storage = script.Storage(path)
    storage._conn.execute("DROP TABLE samples")
    reopened = script.Storage(path)
    reopened.accept("z1", result(T0), now=T0)
    assert reopened.persistent and reopened.resets == 1
    assert served(reopened) == [("z1", T0)]


def test_healthy_database_of_another_application_is_never_touched(path, caplog):
    os.makedirs(os.path.dirname(path))
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute("CREATE TABLE precious (value TEXT)")
        conn.execute("INSERT INTO precious VALUES ('keep me')")
    clock = Clock(0)
    with caplog.at_level(logging.ERROR):
        storage = script.Storage(path, monotonic=clock)
    assert not storage.persistent
    assert "another application" in caplog.text
    # The exporter keeps working from memory.
    storage.accept("z1", result(T0), now=T0)
    assert served(storage) == [("z1", T0)]
    assert rows(path, "SELECT value FROM precious") == [("keep me",)]
    # The problem is logged once, however often the file is tried again.
    caplog.clear()
    clock.advance(script.STORAGE_RETRY_SECONDS)
    assert storage.usable() and not storage.persistent
    assert caplog.text == ""
    assert rows(path, "SELECT value FROM precious") == [("keep me",)]

    # Once the operator moves the foreign file away, the file is used again.
    os.rename(path, path + ".moved")
    clock.advance(script.STORAGE_RETRY_SECONDS)
    assert storage.usable() and storage.persistent
    assert rows(path, "PRAGMA application_id") == [(script.APPLICATION_ID,)]
    assert rows(path + ".moved", "SELECT value FROM precious") == [("keep me",)]


def test_unusable_path_keeps_the_state_in_memory_and_recovers(tmp_path):
    blocker = tmp_path / "blocker"
    blocker.write_text("a file where the directory should be")
    clock = Clock(0)
    storage = script.Storage(str(blocker / "cf-exporter.sqlite3"), monotonic=clock)
    assert not storage.persistent and storage.usable()
    storage.ensure("z1", T0, T0)
    assert storage.pending("z1", T0, T0, T0, 10) == [(T0, 0, 0)]
    os.remove(blocker)
    # Not before the retry interval.
    assert storage.usable() and not storage.persistent
    clock.advance(script.STORAGE_RETRY_SECONDS)
    assert storage.usable() and storage.persistent
    # The memory state is not carried over: the buckets are simply read again.
    assert storage.pending("z1", T0, T0, T0, 10) == []


def test_no_path_means_memory():
    storage = script.Storage(None)
    assert not storage.persistent and storage.usable()


def test_directory_at_the_path_is_left_alone(tmp_path):
    directory = tmp_path / "cf-exporter.sqlite3"
    directory.mkdir()
    (directory / "note.txt").write_text("not ours")
    storage = script.Storage(str(directory))
    assert not storage.persistent and storage.usable()
    assert (directory / "note.txt").read_text() == "not ours"


def test_closed_storage_reports_unusable(path):
    storage = script.Storage(path)
    storage.ensure("z1", T0, T0)
    storage.close()
    assert not storage.usable()
    # Closing folded the write-ahead log into the file.
    assert not os.path.exists(path + "-wal")
    assert rows(path, "SELECT count(*) FROM buckets") == [(1,)]


@pytest.mark.parametrize("suffixes", [("",), ("", "-wal", "-shm")], ids=["file", "all-files"])
def test_database_removed_while_running_is_created_again(path, suffixes):
    storage = script.Storage(path)
    storage.ensure("z1", T0, T0)
    for suffix in suffixes:
        if os.path.exists(path + suffix):
            os.remove(path + suffix)
    assert storage.pending("z1", T0, T0, T0, 10) == []
    assert storage.resets == 1 and storage.persistent
    assert os.path.exists(path)
    assert storage.ensure("z1", T0, T0) == 1
    assert rows(path, "SELECT count(*) FROM buckets") == [(1,)]


def test_removed_directory_is_created_again(path):
    storage = script.Storage(path)
    storage.ensure("z1", T0, T0)
    directory = os.path.dirname(path)
    for name in os.listdir(directory):
        os.remove(os.path.join(directory, name))
    os.rmdir(directory)
    assert storage.ensure("z1", T0, T0) == 1
    assert storage.persistent and storage.resets == 1


def test_database_replaced_while_running_is_opened_again(path):
    storage = script.Storage(path)
    storage.ensure("z1", T0, T0)
    other = script.Storage(path + ".other")
    other.ensure("z2", T0, T0 + BUCKET)
    other._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    other._conn.close()
    os.replace(path + ".other", path)
    assert storage.pending("z2", T0 + 900, T0, T0 + 900, 10) == [(T0 + BUCKET, 0, 0), (T0, 0, 0)]
    assert storage.resets == 1


def failing(times, error):
    """An operation wrapper failing the first `times` calls."""
    calls = []

    def operation(conn):
        calls.append(conn)
        if len(calls) <= times:
            raise error
        return conn.execute("SELECT count(*) FROM buckets").fetchone()[0]

    return operation, calls


def test_failed_operation_recreates_the_database_and_is_retried(path, caplog):
    storage = script.Storage(path)
    storage.ensure("z1", T0, T0)
    operation, calls = failing(1, sqlite3.DatabaseError("database disk image is malformed"))
    with caplog.at_level(logging.ERROR):
        assert storage._run(operation) == 0  # a new, empty database
    assert len(calls) == 2 and storage.resets == 1 and storage.persistent
    assert "history will be read again" in caplog.text
    assert rows(path, "SELECT count(*) FROM buckets") == [(0,)]


def test_database_failing_again_after_recreation_moves_to_memory(path):
    storage = script.Storage(path)
    operation, calls = failing(2, sqlite3.OperationalError("disk I/O error"))
    assert storage._run(operation) == 0
    assert len(calls) == 3 and not storage.persistent
    storage.ensure("z1", T0, T0)
    assert storage.pending("z1", T0, T0, T0, 10) == [(T0, 0, 0)]


def test_error_in_memory_is_raised(caplog):
    storage = script.Storage(None)
    operation, _ = failing(1, sqlite3.OperationalError("no such table: nope"))
    with pytest.raises(sqlite3.OperationalError):
        storage._run(operation)
    assert storage.resets == 0


@pytest.mark.parametrize(
    "error",
    [sqlite3.IntegrityError("constraint failed"), sqlite3.ProgrammingError("bad parameters")],
)
def test_programming_errors_do_not_wipe_the_database(path, error):
    storage = script.Storage(path)
    storage.ensure("z1", T0, T0)
    operation, calls = failing(1, error)
    with pytest.raises(type(error)):
        storage._run(operation)
    assert len(calls) == 1 and storage.resets == 0
    assert rows(path, "SELECT count(*) FROM buckets") == [(1,)]


def test_database_locked_by_another_process_is_not_wiped(path, monkeypatch):
    monkeypatch.setattr(script, "STORAGE_LOCK_SECONDS", 0.05)
    storage = script.Storage(path)
    storage.ensure("z1", T0, T0)
    other = sqlite3.connect(path, isolation_level=None)
    other.execute("BEGIN EXCLUSIVE")
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            storage.ensure("z1", T0, T0 + BUCKET)
    finally:
        other.execute("ROLLBACK")
        other.close()
    assert storage.resets == 0 and storage.persistent
    assert storage.ensure("z1", T0, T0 + BUCKET) == 1
    assert rows(path, "SELECT count(*) FROM buckets") == [(2,)]


def test_extended_lock_codes_are_not_taken_for_a_broken_database(path):
    """SQLite reports extended codes: SQLITE_BUSY_RECOVERY is 261, not 5."""
    storage = script.Storage(path)
    storage.ensure("z1", T0, T0)
    error = sqlite3.OperationalError("database is locked")
    error.sqlite_errorcode = sqlite3.SQLITE_BUSY | (1 << 8)
    operation, calls = failing(1, error)
    with pytest.raises(sqlite3.OperationalError):
        storage._run(operation)
    assert len(calls) == 1 and storage.resets == 0
    assert rows(path, "SELECT count(*) FROM buckets") == [(1,)]
    assert script.storage_busy(error) and not script.storage_busy(sqlite3.OperationalError("x"))


def test_database_file_that_cannot_be_written_is_replaced(path):
    script.Storage(path).close()
    os.chmod(path, 0o400)
    storage = script.Storage(path)
    if os.geteuid() == 0:  # root writes whatever the mode says
        assert storage.persistent and storage.resets == 0
        return
    assert storage.persistent and storage.resets == 1
    assert storage.ensure("z1", T0, T0) == 1
    assert rows(path, "SELECT count(*) FROM buckets") == [(1,)]


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores file permissions")
def test_database_that_can_only_be_read_keeps_the_state_in_memory(path):
    """A file system that became read-only: the database still answers queries,
    so only a write shows that it cannot hold the state. Going back to it every
    retry interval would throw the state in memory away every time."""
    first = script.Storage(path)
    # This connection stays open, so that the write-ahead files exist, as they
    # do after a process was killed.
    first.ensure("z1", T0, T0)
    directory = os.path.dirname(path)
    names = os.listdir(directory)
    assert len(names) == 3
    clock = Clock(0)
    try:
        for name in names:
            os.chmod(os.path.join(directory, name), 0o400)
        os.chmod(directory, 0o500)
        storage = script.Storage(path, monotonic=clock)
        assert not storage.persistent and storage.resets == 0
        storage.ensure("z2", T0, T0)
        for _ in range(3):
            clock.advance(script.STORAGE_RETRY_SECONDS)
            assert storage.pending("z2", T0, T0, T0, 10) == [(T0, 0, 0)]
        assert not storage.persistent and storage.resets == 0
    finally:
        os.chmod(directory, 0o700)
        for name in names:
            os.chmod(os.path.join(directory, name), 0o600)
    # Writable again: the file and what it holds are used.
    clock.advance(script.STORAGE_RETRY_SECONDS)
    assert storage.usable() and storage.persistent
    assert storage.pending("z1", T0, T0, T0, 10) == [(T0, 0, 0)]


def test_file_system_turning_read_only_while_running_moves_to_memory(path, monkeypatch, caplog):
    clock = Clock(0)
    storage = script.Storage(path, monotonic=clock)
    storage.ensure("z1", T0, T0)
    readonly = sqlite3.OperationalError("attempt to write a readonly database")

    def refuse_writes(conn):
        raise readonly

    def refuse_removal(self):
        raise OSError(30, "Read-only file system")

    monkeypatch.setattr(script.Storage, "_probe", staticmethod(refuse_writes))
    monkeypatch.setattr(script.Storage, "_remove_files", refuse_removal)
    operation, calls = failing(1, readonly)
    with caplog.at_level(logging.ERROR):
        assert storage._run(operation) == 0  # answered from memory
    assert len(calls) == 2 and not storage.persistent and storage.resets == 1
    assert "cannot be removed" in caplog.text and "keeping the state in memory" in caplog.text
    # The file is tried again and again; the state in memory survives that,
    # and the lasting problem is not logged every time.
    storage.ensure("z2", T0, T0)
    caplog.clear()
    for _ in range(3):
        clock.advance(script.STORAGE_RETRY_SECONDS)
        assert storage.pending("z2", T0, T0, T0, 10) == [(T0, 0, 0)]
    assert storage.resets == 1 and caplog.text == ""

    # Another reason is reported, once.
    def refuse_differently(self):
        raise OSError(13, "Permission denied")

    monkeypatch.setattr(script.Storage, "_remove_files", refuse_differently)
    for _ in range(2):
        clock.advance(script.STORAGE_RETRY_SECONDS)
        assert storage.usable() and not storage.persistent
    assert caplog.text.count("still cannot be used") == 1


def test_database_that_stays_unusable_after_recreation_is_left_for_memory(path, monkeypatch):
    probe = script.Storage.__dict__["_probe"]

    def refuse_writes(conn):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(script.Storage, "_probe", staticmethod(refuse_writes))
    clock = Clock(0)
    storage = script.Storage(path, monotonic=clock)
    assert not storage.persistent and storage.resets == 1 and storage.usable()
    monkeypatch.setattr(script.Storage, "_probe", probe)
    clock.advance(script.STORAGE_RETRY_SECONDS)
    assert storage.usable() and storage.persistent


def test_database_locked_at_start_is_not_wiped(path, monkeypatch):
    script.Storage(path).ensure("z1", T0, T0)
    monkeypatch.setattr(script, "STORAGE_LOCK_SECONDS", 0.05)
    clock = Clock(0)
    other = sqlite3.connect(path, isolation_level=None)
    other.execute("BEGIN EXCLUSIVE")
    try:
        storage = script.Storage(path, monotonic=clock)
        assert not storage.persistent and storage.resets == 0
    finally:
        other.execute("ROLLBACK")
        other.close()
    clock.advance(script.STORAGE_RETRY_SECONDS)
    assert storage.usable() and storage.persistent
    assert storage.pending("z1", T0, T0, T0, 10) == [(T0, 0, 0)]


def test_write_ahead_log_that_came_with_another_database_is_left_alone(path, tmp_path):
    """Only the exporter's own leftover log is removed when its file was replaced:
    the log of a database moved to the path holds that database's data."""
    storage = script.Storage(path, monotonic=Clock(0))
    storage.ensure("z1", T0, T0)
    foreign = str(tmp_path / "foreign.sqlite3")
    with closing(sqlite3.connect(foreign, isolation_level=None)) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")
        conn.execute("CREATE TABLE precious (value TEXT)")
        conn.execute("INSERT INTO precious VALUES ('keep me')")
        # Copied while open: the table and its row are only in the log.
        shutil.copy(foreign, path + ".new")
        shutil.copy(foreign + "-wal", path + ".new-wal")
    assert os.path.getsize(path + ".new-wal") > 0
    os.replace(path + ".new-wal", path + "-wal")
    os.replace(path + ".new", path)
    assert storage.usable() and not storage.persistent
    assert rows(path, "SELECT value FROM precious") == [("keep me",)]
    assert rows(path, "PRAGMA application_id") == [(0,)]


def test_interrupted_transaction_is_rolled_back(path):
    storage = script.Storage(path)

    def operation(conn):
        with storage._transaction(conn):
            conn.execute("INSERT INTO buckets (zone_id, bucket_end) VALUES ('z1', 1)")
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        storage._run(operation)
    assert storage.summary()["zones"] == {}
    assert storage.ensure("z1", T0, T0) == 1


def test_ensure_adds_the_missing_buckets(path):
    storage = script.Storage(path)
    assert storage.ensure("z1", T0, T0 + 3 * BUCKET) == 4
    assert storage.ensure("z1", T0, T0 + 3 * BUCKET) == 0
    assert storage.ensure("z1", T0 - 2 * BUCKET, T0 + 5 * BUCKET) == 4
    assert storage.ensure("z1", T0 + BUCKET, T0 + 2 * BUCKET) == 0
    assert storage.summary()["zones"]["z1"][0] == 8
    # An empty or inverted range adds nothing.
    assert storage.ensure("z1", T0 + 9 * BUCKET, T0 + 8 * BUCKET) == 0


def test_ensure_fills_gaps_between_existing_buckets(path):
    """Rows lost in the middle, as with a zone that left the listing for a while or
    a database recreated during a read, must not stay unread for good."""
    storage = script.Storage(path)
    storage.ensure("z1", T0, T0 + 5 * BUCKET)
    storage.accept("z1", result(T0 + 2 * BUCKET, 7), now=T0)
    storage._conn.execute(
        "DELETE FROM buckets WHERE bucket_end IN (?, ?)", (T0 + BUCKET, T0 + 4 * BUCKET)
    )
    assert storage.ensure("z1", T0, T0 + 5 * BUCKET) == 2
    assert rows(path, "SELECT count(*), sum(state) FROM buckets") == [(6, 1)]
    # What was there is untouched.
    assert storage.exposition(10)[1][0][2] == 7


def test_failed_reads_are_counted_per_bucket_until_a_read_works(path):
    storage = script.Storage(path)
    storage.ensure("z1", T0, T0 + BUCKET)
    storage.defer("z1", [T0, T0 + BUCKET], T0 + 60)
    storage.defer("z1", [T0], T0 + 120)
    # A rate-limit rejection postpones without blaming the bucket.
    storage.defer("z1", [T0 + BUCKET], T0 + 300, failed=False)
    assert storage.bucket("z1", T0).errors == 2
    assert storage.pending("z1", T0 + 300, T0, T0 + BUCKET, 10) == [
        (T0 + BUCKET, 0, 1),
        (T0, 0, 2),
    ]
    assert storage.pending("z1", T0 + 119, T0, T0 + BUCKET, 10) == []
    # Buckets that wait after failed reads can be made due at once.
    storage.release("z1", T0 + 100)
    assert len(storage.pending("z1", T0 + 100, T0, T0 + BUCKET, 10)) == 2
    # A read that worked but did not verify ends the row of failures.
    storage.reject("z1", result(T0), T0 + 100, T0 + 220)
    row = storage.bucket("z1", T0)
    assert (row.attempts, row.errors) == (1, 0)
    storage.release("z1", T0 + 100)
    assert storage.pending("z1", T0 + 100, T0, T0 + BUCKET, 10) == [(T0 + BUCKET, 0, 1)]


def test_buckets_that_had_their_responses_do_not_keep_others_out(path):
    """A bucket stays for the delivery time after its responses. It must not take
    the room of buckets that no response has carried yet."""
    storage = script.Storage(path)
    for index in range(4):
        storage.accept("z1", result(T0 + index * BUCKET), now=T0 + 2000)
    newest = [("z1", T0 + 3 * BUCKET), ("z1", T0 + 2 * BUCKET)]
    assert served(storage, 2) == newest
    for _ in range(script.DELIVERY_RESPONSES):
        storage.mark_served(storage.exposition(2)[0])
    older = [("z1", T0 + BUCKET), ("z1", T0)]
    assert served(storage, 2) == older
    # With room to spare the waiting ones are repeated, after the others.
    assert served(storage, 3) == [*older, newest[0]]
    for _ in range(script.DELIVERY_RESPONSES):
        storage.mark_served(storage.exposition(2)[0])
    assert served(storage, 2) == newest


def test_response_does_not_count_for_a_bucket_accepted_again_meanwhile(path):
    storage = script.Storage(path)
    storage.accept("z1", result(T0, 1), now=T0 + 600)
    picked, _ = storage.exposition(10)
    # A recheck changes the bucket while the response is on its way.
    storage.accept("z1", result(T0, 2), now=T0 + 4000)
    storage.mark_served(picked)
    assert rows(path, "SELECT served FROM buckets") == [(0,)]
    storage.mark_served(storage.exposition(10)[0])
    assert rows(path, "SELECT served FROM buckets") == [(1,)]


def test_bucket_is_delivered_after_enough_responses_and_time(path):
    storage = script.Storage(path)
    storage.accept("z1", result(T0), now=T0 + 600)
    picked, _ = storage.exposition(10)
    storage.mark_served(picked)
    keep = T0 - 10**9
    # One response is not enough, whatever time has passed.
    storage.prune({}, None, T0 + 10**6, keep)
    assert storage.undelivered_samples() == 1
    storage.mark_served(picked)
    # Enough responses, but accepted too recently.
    storage.prune({}, None, T0 + 600 + script.DELIVERY_SECONDS - 1, keep)
    assert storage.undelivered_samples() == 1
    storage.prune({}, None, T0 + 600 + script.DELIVERY_SECONDS, keep)
    assert storage.undelivered_samples() == 0
    assert storage.exposition(10) == ([], [])
    assert rows(path, "SELECT count(*) FROM samples") == [(0,)]
    # The bucket is remembered as done, so that it is not read again.
    assert rows(path, "SELECT state, delivered FROM buckets") == [(1, 1)]


def test_prune_forgets_what_is_out_of_reach(path):
    storage = script.Storage(path)
    storage.ensure("z1", T0 - BUCKET, T0 + 4 * BUCKET)
    storage.defer("z1", [T0 - BUCKET], T0 + 60)  # its reads failed
    storage.reject("z1", result(T0), T0, T0 + 60)  # read, never verified
    storage.accept("z1", result(T0 + 2 * BUCKET), now=T0)  # accepted, not delivered
    storage.accept("gone", result(T0), now=T0)
    storage.ensure("gone", T0 + BUCKET, T0 + BUCKET)
    horizon = {"z1": T0 + 4 * BUCKET}
    abandoned, expired = storage.prune(horizon, ["z1"], T0, T0 - 10**9)
    # Only buckets that were tried count as abandoned; untried ones just go.
    assert (abandoned, expired) == (2, 0)
    assert rows(path, "SELECT zone_id, bucket_end, state FROM buckets ORDER BY 1, 2") == [
        ("gone", T0, 1),  # still to be delivered
        ("z1", T0 + 2 * BUCKET, 1),  # still to be delivered
        ("z1", T0 + 4 * BUCKET, 0),
    ]
    # Undelivered buckets are finally dropped when nobody scraped them for too long.
    abandoned, expired = storage.prune(horizon, ["z1"], T0, T0 + 3 * BUCKET)
    assert (abandoned, expired) == (0, 2)
    assert rows(path, "SELECT count(*) FROM samples") == [(0,)]
    assert rows(path, "SELECT zone_id, bucket_end FROM buckets") == [("z1", T0 + 4 * BUCKET)]


def test_bucket_older_than_the_keep_time_is_kept_after_its_acceptance(path):
    """A dataset can reach further back than the keep time: its oldest buckets,
    read now, must still get their chance to be scraped."""
    storage = script.Storage(path)
    now = T0 + 10**7
    old = now - script.UNDELIVERED_KEEP_SECONDS - 5 * 86400
    storage.accept("z1", result(old), now=now)
    keep = now - script.UNDELIVERED_KEEP_SECONDS
    assert storage.prune({}, ["z1"], now, keep) == (0, 0)
    assert served(storage) == [("z1", old)]
    later = now + script.UNDELIVERED_KEEP_SECONDS + 1
    assert storage.prune({}, ["z1"], later, later - script.UNDELIVERED_KEEP_SECONDS) == (0, 1)


def test_events_of_accepted_buckets_are_kept_after_delivery(path):
    storage = script.Storage(path)
    storage.ensure("z1", T0, T0 + 4 * BUCKET)
    events = script.BucketResult(T0 + BUCKET, (), "s", requests=5, client_requests=0)
    clients = script.BucketResult(T0 + 3 * BUCKET, (), "s", requests=2, client_requests=2)
    storage.accept("z1", events, now=T0)
    storage.accept("z1", clients, now=T0)
    storage.accept("z1", result(T0 + 4 * BUCKET), now=T0)  # no events
    for _ in range(script.DELIVERY_RESPONSES):
        storage.mark_served(storage.exposition(100)[0])
    storage.prune({}, None, T0 + script.DELIVERY_SECONDS, 0)
    assert storage.undelivered_samples() == 0
    assert storage.has_event("z1", "totals", T0 + BUCKET, T0 + BUCKET)
    assert not storage.has_event("z1", "totals", T0 + 2 * BUCKET, T0 + 2 * BUCKET)
    assert not storage.has_event("z1", "edge_totals", T0, T0 + 2 * BUCKET)
    assert storage.has_event("z1", "edge_totals", T0, T0 + 3 * BUCKET)
    assert not storage.has_event("z2", "totals", 0, T0 + 10**6)
    # The nearest one to a window, inside it if there is one.
    window = (T0 + 4 * BUCKET, T0 + 5 * BUCKET)
    assert storage.nearest_event("z1", "totals", *window, 0, T0 + 10**6) == T0 + 3 * BUCKET
    assert storage.nearest_event("z1", "totals", T0, T0 + BUCKET, 0, T0 + 10**6) == T0 + BUCKET
    assert storage.nearest_event("z1", "edge_totals", T0, T0 + BUCKET, 0, T0) is None
    assert storage.nearest_event("z1", "totals", T0 - BUCKET, T0, T0 + 2 * BUCKET, T0 + 10**6) == (
        T0 + 3 * BUCKET
    )


def test_rechecks_of_buckets_rendered_otherwise_are_cancelled(path):
    storage = script.Storage(path)
    for index, layout in enumerate(("old", "new", None)):
        storage.accept(
            "z1", result(T0 + index * BUCKET), now=T0, recheck_due=T0 + 3600, layout=layout
        )
    storage.accept("z2", result(T0), now=T0, recheck_due=T0 + 3600, layout="old")
    assert storage.skip_rechecks("z1", "new") == 2
    assert storage.rechecks("z1", T0 + 3600, 10) == [T0 + BUCKET]
    assert storage.rechecks("z2", T0 + 3600, 10) == [T0]
    assert storage.bucket("z1", T0).layout == "old"
    assert storage.zone_ids() == {"z1", "z2"}


def test_full_disk_never_wipes_the_database(path, caplog):
    clock = Clock(0)
    storage = script.Storage(path, monotonic=clock)
    storage.ensure("z1", T0, T0)
    error = sqlite3.OperationalError("database or disk is full")
    error.sqlite_errorcode = sqlite3.SQLITE_FULL
    operation, calls = failing(1, error)
    with caplog.at_level(logging.ERROR), pytest.raises(sqlite3.OperationalError):
        storage._run(operation)
    assert len(calls) == 1 and storage.resets == 0 and storage.persistent
    assert storage.full and "disk is full" in caplog.text
    assert rows(path, "SELECT count(*) FROM buckets") == [(1,)]
    clock.advance(script.STORAGE_RETRY_SECONDS)
    assert not storage.full


def test_full_disk_at_start_keeps_the_file_and_uses_memory(path, monkeypatch):
    script.Storage(path).ensure("z1", T0, T0)

    def full(conn):
        error = sqlite3.OperationalError("database or disk is full")
        error.sqlite_errorcode = sqlite3.SQLITE_FULL
        raise error

    monkeypatch.setattr(script.Storage, "_probe", staticmethod(full))
    storage = script.Storage(path, monotonic=Clock(0))
    assert not storage.persistent and storage.resets == 0
    assert rows(path, "SELECT count(*) FROM buckets") == [(1,)]


def test_prune_handles_any_number_of_zones(path):
    storage = script.Storage(path)
    storage.ensure("z7", T0, T0)
    storage.ensure("gone", T0, T0)
    assert storage.prune({}, [f"z{index}" for index in range(40_000)], T0, T0) == (0, 0)
    assert storage.resets == 0
    assert rows(path, "SELECT zone_id FROM buckets") == [("z7",)]


def test_wiped_database_is_filled_again_from_cloudflare(cloudflare, api, clock, path):
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4), event(T0 - 4000, count=9))
    storage = script.Storage(path)
    exporter = script.Exporter(api, storage, clock=clock)
    run(exporter)
    expected = values(analytics(exporter, storage), "cf_zone_requests_5m", **ZONE)
    assert expected[T0 + BUCKET] == 4 and sum(expected.values()) == 13
    for _ in range(script.DELIVERY_RESPONSES):
        analytics(exporter, storage, consume=True)
    clock.advance(script.DELIVERY_SECONDS)
    run(exporter)
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(path + suffix):
            os.remove(path + suffix)
    run(exporter)
    # Everything within reach is exported again with the same timestamps.
    again = values(analytics(exporter, storage), "cf_zone_requests_5m", **ZONE)
    assert again[T0 + BUCKET] == 4 and again[T0 - 4000 // BUCKET * BUCKET] == 9
    assert storage.resets == 1 and storage.persistent


def test_restart_does_not_read_or_serve_delivered_buckets_again(cloudflare, api, clock, path):
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4))
    storage = script.Storage(path)
    exporter = script.Exporter(api, storage, clock=clock)
    run(exporter)
    for _ in range(script.DELIVERY_RESPONSES):
        analytics(exporter, storage, consume=True)
    clock.advance(script.DELIVERY_SECONDS)
    run(exporter)
    before = len(cloudflare.calls)

    restarted_storage = script.Storage(path)
    restarted = script.Exporter(api, restarted_storage, clock=clock)
    run(restarted)
    new_calls = cloudflare.calls[before:]
    # Only buckets that became due meanwhile are read.
    assert all(call["since"] >= T0 + BUCKET for call in new_calls)
    samples = analytics(restarted, restarted_storage)
    assert T0 + BUCKET not in values(samples, "cf_zone_requests_5m")
