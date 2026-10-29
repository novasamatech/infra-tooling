# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

import io
import json
import threading

import pytest

from sqdb.config import MAX_PART_SIZE, MIN_PART_SIZE, LocalSettings
from sqdb.storage import (
    LocalStore,
    MultipartUpload,
    Progress,
    S3Reader,
    S3Store,
    StorageError,
    auto_chunk_size,
    duration,
)

MiB = 1024**2


def upload(
    s3, log, data: bytes, chunk=MIN_PART_SIZE, max_parts=1000, ok=True, concurrency=2, **options
):
    progress = Progress()
    up = MultipartUpload(s3, "b", "k", chunk, max_parts, concurrency, log, progress, **options)
    return up.run(io.BytesIO(data), lambda: ok), progress


def test_auto_chunk_size_fits_the_part_limit():
    assert auto_chunk_size(0, 1000) == 8 * MiB
    size = 200 * 1024**3
    chunk = auto_chunk_size(size, 1000)
    assert chunk % (8 * MiB) == 0 and chunk * 1000 >= size * 1.5
    assert auto_chunk_size(100 * 1024**4, 1000) == MAX_PART_SIZE


def test_multipart_upload_completes_in_order(s3, log):
    data = bytes(range(256)) * (MIN_PART_SIZE * 3 // 256 + 7)
    size, progress = upload(s3, log, data, concurrency=3)
    assert size == len(data) and s3.objects["k"] == data
    assert progress.done == len(data)
    assert not s3.uploads and not s3.aborted


def test_upload_is_aborted_when_the_dump_fails(s3, log):
    with pytest.raises(StorageError, match="did not finish successfully"):
        upload(s3, log, b"x" * (MIN_PART_SIZE + 10), ok=False)
    assert "k" not in s3.objects and s3.aborted == ["k"] and not s3.uploads


def test_upload_is_aborted_on_part_errors_and_empty_streams(s3, log):
    s3.fail_part = 2
    with pytest.raises(Exception, match="InternalError"):
        upload(s3, log, b"y" * (MIN_PART_SIZE * 3))
    with pytest.raises(StorageError, match="no data"):
        upload(s3, log, b"")
    assert s3.aborted == ["k", "k"] and not s3.objects


def test_part_size_grows_before_the_part_limit(s3, log, caplog):
    data = b"z" * (MIN_PART_SIZE * 7)
    size, _ = upload(s3, log, data, max_parts=4)
    assert size == len(data) and s3.objects["k"] == data
    assert "part size raised" in caplog.text


def test_upload_fails_beyond_the_part_limit(s3, log, monkeypatch):
    monkeypatch.setattr("sqdb.storage.MAX_PART_SIZE", MIN_PART_SIZE)
    with pytest.raises(StorageError, match="more than 2 parts"):
        upload(s3, log, b"w" * (MIN_PART_SIZE * 3), max_parts=2)
    assert s3.aborted == ["k"]


def test_reader_resumes_with_range_and_etag(s3, log, monkeypatch):
    monkeypatch.setattr("sqdb.storage.time.sleep", lambda _: None)
    data = bytes(range(256)) * 4096
    s3.objects["k"] = data
    s3.fail_reads = [100_000, 300_000]
    progress = Progress(len(data))
    reader = S3Reader(s3, "b", "k", '"etag"', log, progress=progress)
    monkeypatch.setattr(reader._cancelled, "wait", lambda _: None)
    out = b"".join(iter(lambda: reader.read(65536), b""))
    assert out == data and reader.resumes == 2 and progress.done == len(data)
    assert [call["Range"] for call in s3.get_calls] == [None, "bytes=100000-", "bytes=400000-"]
    assert all(call["IfMatch"] == '"etag"' for call in s3.get_calls)


def test_reader_gives_up_after_repeated_failures(s3, log, monkeypatch):
    monkeypatch.setattr("sqdb.storage.time.sleep", lambda _: None)
    s3.objects["k"] = b"a" * 10
    s3.fail_reads = [0] * 5
    reader = S3Reader(s3, "b", "k", None, log, max_resumes=3)
    monkeypatch.setattr(reader._cancelled, "wait", lambda _: None)
    with pytest.raises(StorageError, match="failed 4 times"):
        reader.read(5)


def test_s3_store_keys_info_and_manifest(s3, config, log):
    store = S3Store(config.s3, client=s3)
    db = config.databases[1]
    assert store.key(db, "run") == "chain-b-accounts/run.dump"
    assert store.location(db, "run") == "s3://dumps/chain-b-accounts/run.dump"
    assert store.info(db, "run") is None
    store.check_access()
    assert s3.aborted[0].startswith(".sqdb-check/") and not s3.uploads
    size = store.upload(
        db, "run", io.BytesIO(b"PGDMP" * 10), lambda: True, MIN_PART_SIZE, log, None
    )
    info = store.info(db, "run")
    assert info.size == size == 50 and info.manifest is None and info.etag
    store.write_manifest(db, "run", {"size": size})
    assert store.info(db, "run").manifest == {"size": 50}
    with store.reader(db, "run", info.etag, log) as reader:
        assert reader.read(5) == b"PGDMP"


def test_local_store_commits_atomically(tmp_path, config):
    store = LocalStore(LocalSettings(path="dumps", format="custom"), tmp_path)
    db = config.databases[0]
    assert store.info(db, "run") is None
    partial = store.prepare(db, "run")
    assert partial.name == "run.dump.partial"
    partial.write_bytes(b"PGDMP-data")
    assert store.commit(db, "run") == 10
    store.write_manifest(db, "run", {"ok": True})
    info = store.info(db, "run")
    assert info.manifest == {"ok": True} and info.size == 10
    assert json.loads(store.manifest_path(db, "run").read_text()) == {"ok": True}
    store.prepare(db, "run")  # a failed new dump must preserve the previous good archive
    assert store.info(db, "run") == info
    store.check_access()
    assert store.free_bytes() > 0


def test_local_directory_dumps(tmp_path, config):
    store = LocalStore(LocalSettings(path=str(tmp_path / "abs"), format="directory"), tmp_path)
    db = config.databases[0]
    partial = store.prepare(db, "run")
    partial.mkdir()
    (partial / "toc.dat").write_bytes(b"1234")
    (partial / "3001.dat").write_bytes(b"56")
    assert store.commit(db, "run") == 6
    assert store.path(db, "run").name == "run.dir"
    store.prepare(db, "run").mkdir()  # pg_dump wrote nothing
    with pytest.raises(StorageError, match="missing"):
        store.commit(db, "run")
    assert store.info(db, "run").size == 6


def test_reader_cancellation_wakes_a_retry_wait(s3, log):
    s3.objects["k"] = b"data"
    s3.fail_reads = [0]
    reader = S3Reader(s3, "b", "k", None, log)
    errors = []

    def read():
        try:
            reader.read(4)
        except StorageError as exc:
            errors.append(exc)

    thread = threading.Thread(target=read, daemon=True)
    thread.start()
    reader.cancel()
    thread.join(timeout=1)
    assert not thread.is_alive() and errors


def test_local_fingerprint_detects_same_size_replacement(tmp_path, config):
    store = LocalStore(LocalSettings("dumps", "custom"), tmp_path)
    db = config.databases[0]
    store.prepare(db, "run").write_bytes(b"one")
    store.commit(db, "run")
    first = store.info(db, "run")
    store.prepare(db, "run").write_bytes(b"two")
    store.commit(db, "run")
    second = store.info(db, "run")
    assert first.size == second.size and first.etag != second.etag


def test_local_directory_replacement_and_interrupted_swap_recovery(tmp_path, config):
    store = LocalStore(LocalSettings("dumps", "directory"), tmp_path)
    db = config.databases[0]
    partial = store.prepare(db, "run")
    partial.mkdir()
    (partial / "toc.dat").write_bytes(b"old")
    store.commit(db, "run")
    path = store.path(db, "run")
    previous = path.with_name(path.name + ".previous")
    path.rename(previous)  # simulate a crash between the two renames
    assert store.info(db, "run").size == 3 and path.is_dir()
    partial = store.prepare(db, "run")
    partial.mkdir()
    (partial / "toc.dat").write_bytes(b"replacement")
    assert store.commit(db, "run") == len(b"replacement")
    assert not previous.exists() and (path / "toc.dat").read_bytes() == b"replacement"


def test_progress_and_duration():
    progress = Progress(total=4 * MiB)
    progress.add(MiB)
    text = progress.describe()
    assert "1.0 MiB" in text and "25% of 4.0 MiB" in text
    assert duration(59) == "0m59s" and duration(3725) == "1h02m"


def test_read_full_handles_short_reads():
    from sqdb.storage import read_full

    class Trickle(io.RawIOBase):
        def __init__(self, data):
            self.data = data

        def read(self, size=-1):
            count = min(3, size)
            chunk, self.data = self.data[:count], self.data[count:]
            return chunk

    assert read_full(Trickle(b"abcdefgh"), 5) == b"abcde"
    assert read_full(Trickle(b"ab"), 5) == b"ab"
    assert read_full(io.BytesIO(b""), 5) == b""


def test_parts_above_the_memory_limit_are_spooled_to_files(s3, log):
    data = bytes(range(256)) * (MIN_PART_SIZE * 2 // 256 + 99)
    size, _ = upload(s3, log, data, max_part_memory=1024**2)
    assert size == len(data) and s3.objects["k"] == data
    assert s3.part_bodies and "bytes" not in s3.part_bodies
    s3.part_bodies.clear()
    upload(s3, log, data)  # default limit: parts stay in memory
    assert set(s3.part_bodies) == {"bytes"}


def test_part_growth_can_be_disabled_for_equal_part_stores(s3, log):
    data = b"z" * (MIN_PART_SIZE * 7)
    with pytest.raises(StorageError, match="more than 4 parts"):
        upload(s3, log, data, max_parts=4, grow_part_size=False)
    assert s3.aborted == ["k"] and "k" not in s3.objects
    size, _ = upload(s3, log, b"y" * (MIN_PART_SIZE * 3 + 7), max_parts=4, grow_part_size=False)
    sizes = [len(part) for part in s3.uploaded_parts["k"]]
    assert size == MIN_PART_SIZE * 3 + 7 and sizes[:-1] == [MIN_PART_SIZE] * 3


def test_reader_budget_counts_only_consecutive_failures(s3, log, monkeypatch):
    monkeypatch.setattr("sqdb.storage.time.sleep", lambda _: None)
    data = bytes(range(256)) * 4096
    s3.objects["k"] = data
    # Ten breaks, each after progress: more than the budget of three in total.
    s3.fail_reads = [50_000] * 10
    reader = S3Reader(s3, "b", "k", None, log, max_resumes=3)
    out = b"".join(iter(lambda: reader.read(65536), b""))
    assert out == data and reader.resumes == 10 and reader.failures == 0
