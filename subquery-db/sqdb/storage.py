# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Dump storage: S3-compatible object storage (streamed) or a local directory.

S3 uploads use the multipart API directly instead of a one-shot "copy from
stdin": the upload completes only after pg_dump has exited successfully and is
aborted otherwise, so a failed dump never leaves a truncated object behind.
Downloads resume with ranged requests pinned to the object's ETag when a long
stream breaks.
"""

import contextlib
import hashlib
import io
import json
import logging
import math
import shutil
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

from .config import MAX_PART_SIZE, MIN_PART_SIZE, Database, LocalSettings, S3Settings, format_size

READ_BLOCK = 1024 * 1024
# Expected dump size relative to pg_database_size(); dumps omit index pages, so
# 1.5 leaves room for text encoding that is larger than the on-disk format.
AUTO_CHUNK_FACTOR = 1.5
AUTO_CHUNK_ALIGN = 8 * 1024**2
AUTO_CHUNK_MIN = 8 * 1024**2


class StorageError(Exception):
    """A dump could not be stored, found or read."""


@dataclass(frozen=True)
class DumpInfo:
    location: str
    size: int | None
    manifest: dict[str, Any] | None
    etag: str | None = None


def auto_chunk_size(database_size: int, max_parts: int) -> int:
    """Smallest aligned part size that keeps the expected dump within max_parts."""
    expected = database_size * AUTO_CHUNK_FACTOR
    chunk = math.ceil(expected / max_parts / AUTO_CHUNK_ALIGN) * AUTO_CHUNK_ALIGN
    return min(max(chunk, AUTO_CHUNK_MIN), MAX_PART_SIZE)


class Progress:
    """Thread-safe byte counter for long transfers; heartbeats report it."""

    def __init__(self, total: int | None = None):
        self.total = total
        self.started = time.monotonic()
        self.done = 0
        self._lock = threading.Lock()

    def add(self, count: int) -> None:
        with self._lock:
            self.done += count

    def describe(self) -> str:
        elapsed = max(time.monotonic() - self.started, 1e-6)
        rate = self.done / elapsed
        text = f"{format_size(self.done)} at {format_size(rate)}/s"
        if self.total:
            share = min(self.done / self.total, 1.0)
            text += f" ({share:.0%} of {format_size(self.total)}"
            if rate > 0 and self.done < self.total:
                text += f", ETA {duration((self.total - self.done) / rate)}"
            text += ")"
        return text


def duration(seconds: float) -> str:
    seconds = int(seconds)
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    return f"{hours}h{minutes:02d}m" if hours else f"{minutes}m{secs:02d}s"


def read_full(stream: BinaryIO, size: int) -> bytes:
    """Read exactly size bytes unless the stream ends first.

    A buffered pipe fills the whole request in one buffer, so a part is held in
    memory once; the loop only matters for streams that return short reads.
    """
    chunks: list[bytes] = []
    remaining = size
    while remaining > 0:
        chunk = stream.read(remaining)
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return chunks[0] if len(chunks) == 1 else b"".join(chunks)


# ------------------------------------------------------------------------ S3


def s3_client(settings: S3Settings):
    import boto3
    from botocore.config import Config as BotoConfig

    config = BotoConfig(
        s3={"addressing_style": "path" if settings.force_path_style else "auto"},
        retries={"max_attempts": settings.max_attempts, "mode": "standard"},
        connect_timeout=30,
        read_timeout=30,
        # S3-compatible providers do not all accept the newer default checksums.
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )
    return boto3.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        region_name=settings.region,
        aws_access_key_id=settings.access_key_id,
        aws_secret_access_key=settings.secret_access_key,
        aws_session_token=settings.session_token,
        verify=settings.verify_ssl,
        config=config,
    )


def _is_not_found(exc: Exception) -> bool:
    from botocore.exceptions import ClientError

    if not isinstance(exc, ClientError):
        return False
    code = str(exc.response.get("Error", {}).get("Code", ""))
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in {"404", "NoSuchKey", "NotFound"} or status == 404


class MultipartUpload:
    """Streams a byte source into one object, completing only when told to."""

    def __init__(
        self,
        client,
        bucket: str,
        key: str,
        chunk_size: int,
        max_parts: int,
        concurrency: int,
        log: logging.LoggerAdapter,
        progress: Progress | None = None,
    ):
        self.client = client
        self.bucket = bucket
        self.key = key
        self.chunk_size = max(chunk_size, MIN_PART_SIZE)
        self.max_parts = max_parts
        self.concurrency = concurrency
        self.log = log
        self.progress = progress
        self.upload_id: str | None = None

    def run(self, stream: BinaryIO, should_complete: Callable[[], bool]) -> int:
        """Upload stream; ``should_complete`` decides at EOF. Returns the object size."""
        response = self.client.create_multipart_upload(Bucket=self.bucket, Key=self.key)
        self.upload_id = response["UploadId"]
        try:
            parts, total = self._upload_parts(stream)
            if not should_complete():
                raise StorageError("the dump did not finish successfully; upload aborted")
            if total == 0:
                raise StorageError("the dump produced no data; upload aborted")
            self.client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self.upload_id,
                MultipartUpload={"Parts": parts},
            )
        except BaseException:
            self.abort()
            raise
        head = self.client.head_object(Bucket=self.bucket, Key=self.key)
        if head["ContentLength"] != total:
            raise StorageError(
                f"uploaded object has {head['ContentLength']} bytes, {total} were sent"
            )
        return total

    def abort(self) -> None:
        if self.upload_id is None:
            return
        try:
            self.client.abort_multipart_upload(
                Bucket=self.bucket, Key=self.key, UploadId=self.upload_id
            )
            self.log.info("aborted the incomplete upload of %s", self.key)
        except Exception as exc:  # best effort; report and keep the original error
            self.log.warning("could not abort multipart upload %s: %s", self.upload_id, exc)
        self.upload_id = None

    def _upload_parts(self, stream: BinaryIO) -> tuple[list[dict], int]:
        parts: list[dict] = []
        pending: set[Future] = set()
        total = 0
        number = 0
        chunk_size = self.chunk_size
        # Parts may differ in size: double the part size whenever half of the
        # remaining part budget is used, so an underestimated dump still fits.
        threshold = self.max_parts // 2
        with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
            try:
                while True:
                    data = read_full(stream, chunk_size)
                    if not data:
                        break
                    number += 1
                    if number > self.max_parts:
                        raise StorageError(
                            f"the dump needs more than {self.max_parts} parts; "
                            "raise storage.s3.chunk_size"
                        )
                    total += len(data)
                    pending.add(pool.submit(self._upload_part, number, data))
                    if number >= threshold and chunk_size < MAX_PART_SIZE:
                        chunk_size = min(chunk_size * 2, MAX_PART_SIZE)
                        threshold += max((self.max_parts - threshold) // 2, 1)
                        self.log.warning(
                            "dump is larger than expected: part size raised to %s after %d parts",
                            format_size(chunk_size),
                            number,
                        )
                    # Bound memory: at most `concurrency` parts in flight.
                    while len(pending) >= self.concurrency:
                        done, pending = wait(pending, return_when=FIRST_COMPLETED)
                        for future in done:
                            parts.append(future.result())
                for future in pending:
                    parts.append(future.result())
            except BaseException:
                for future in pending:
                    future.cancel()
                raise
        parts.sort(key=lambda part: part["PartNumber"])
        return parts, total

    def _upload_part(self, number: int, data: bytes) -> dict:
        response = self.client.upload_part(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            PartNumber=number,
            Body=data,
        )
        if self.progress:
            self.progress.add(len(data))
        return {"PartNumber": number, "ETag": response["ETag"]}


class S3Reader(io.RawIOBase):
    """Readable object stream that resumes from the current offset after errors."""

    def __init__(
        self,
        client,
        bucket: str,
        key: str,
        etag: str | None,
        log: logging.LoggerAdapter,
        max_resumes: int = 20,
        progress: Progress | None = None,
    ):
        super().__init__()
        self.client = client
        self.bucket = bucket
        self.key = key
        self.etag = etag
        self.log = log
        self.max_resumes = max_resumes
        self.progress = progress
        self.position = 0
        self.resumes = 0
        self.body = None
        self._cancelled = threading.Event()

    def cancel(self) -> None:
        """Wake retry waits when the consumer exits or a signal stops the run."""
        self._cancelled.set()
        self._close_body()

    def readable(self) -> bool:
        return True

    def _open(self) -> None:
        kwargs: dict[str, Any] = {"Bucket": self.bucket, "Key": self.key}
        if self.position:
            kwargs["Range"] = f"bytes={self.position}-"
        if self.etag:
            kwargs["IfMatch"] = self.etag
        self.body = self.client.get_object(**kwargs)["Body"]

    def read(self, size: int = -1) -> bytes:
        from botocore.exceptions import BotoCoreError
        from urllib3.exceptions import HTTPError

        size = READ_BLOCK if size is None or size < 0 else size
        if size == 0:
            return b""
        while True:
            if self.closed or self._cancelled.is_set():
                raise StorageError(f"reading {self.key} was cancelled")
            try:
                if self.body is None:
                    self._open()
                data = self.body.read(size)
            except (BotoCoreError, HTTPError, OSError) as exc:
                self._close_body()
                self.resumes += 1
                if self.resumes > self.max_resumes:
                    raise StorageError(
                        f"reading {self.key} failed {self.resumes} times: {exc}"
                    ) from exc
                delay = min(2**self.resumes, 60)
                self.log.warning(
                    "read of %s broke at byte %d (%s); resuming in %ds",
                    self.key,
                    self.position,
                    exc,
                    delay,
                )
                self._cancelled.wait(delay)
                continue
            self.position += len(data)
            if self.progress and data:
                self.progress.add(len(data))
            return data

    def readinto(self, buffer) -> int:
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)

    def _close_body(self) -> None:
        if self.body is not None:
            # Closing a broken stream is best effort.
            with contextlib.suppress(Exception):
                self.body.close()
            self.body = None

    def close(self) -> None:
        self.cancel()
        super().close()


class S3Store:
    kind = "s3"

    def __init__(self, settings: S3Settings, client=None):
        self.settings = settings
        self.client = client if client is not None else s3_client(settings)

    def key(self, db: Database, backup_name: str) -> str:
        return f"{self.settings.prefix}{db.name}/{backup_name}.dump"

    def manifest_key(self, db: Database, backup_name: str) -> str:
        return f"{self.settings.prefix}{db.name}/{backup_name}.manifest.json"

    def location(self, db: Database, backup_name: str) -> str:
        return f"s3://{self.settings.bucket}/{self.key(db, backup_name)}"

    def check_access(self) -> None:
        """Confirm the bucket is reachable and multipart uploads are allowed."""
        bucket = self.settings.bucket
        self.client.head_bucket(Bucket=bucket)
        key = f"{self.settings.prefix}.sqdb-check/{uuid.uuid4().hex}"
        upload = self.client.create_multipart_upload(Bucket=bucket, Key=key)
        self.client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload["UploadId"])

    def info(self, db: Database, backup_name: str) -> DumpInfo | None:
        try:
            head = self.client.head_object(
                Bucket=self.settings.bucket, Key=self.key(db, backup_name)
            )
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise
        manifest = None
        try:
            body = self.client.get_object(
                Bucket=self.settings.bucket, Key=self.manifest_key(db, backup_name)
            )["Body"]
            try:
                manifest = json.loads(body.read())
            finally:
                body.close()
        except Exception as exc:
            if not _is_not_found(exc):
                raise
        if manifest is not None and not isinstance(manifest, dict):
            raise StorageError("dump manifest must be a JSON object")
        return DumpInfo(
            location=self.location(db, backup_name),
            size=head["ContentLength"],
            manifest=manifest,
            etag=head.get("ETag"),
        )

    def upload(
        self,
        db: Database,
        backup_name: str,
        stream: BinaryIO,
        should_complete: Callable[[], bool],
        chunk_size: int,
        log: logging.LoggerAdapter,
        progress: Progress | None,
    ) -> int:
        # The manifest marks a complete dump; drop a stale one before overwriting.
        self.delete_manifest(db, backup_name)
        upload = MultipartUpload(
            self.client,
            self.settings.bucket,
            self.key(db, backup_name),
            chunk_size=chunk_size,
            max_parts=self.settings.max_parts,
            concurrency=self.settings.upload_concurrency,
            log=log,
            progress=progress,
        )
        return upload.run(stream, should_complete)

    def write_manifest(self, db: Database, backup_name: str, manifest: dict) -> None:
        self.client.put_object(
            Bucket=self.settings.bucket,
            Key=self.manifest_key(db, backup_name),
            Body=json.dumps(manifest, indent=2, sort_keys=True).encode(),
            ContentType="application/json",
        )

    def delete_manifest(self, db: Database, backup_name: str) -> None:
        self.client.delete_object(
            Bucket=self.settings.bucket, Key=self.manifest_key(db, backup_name)
        )

    def reader(
        self,
        db: Database,
        backup_name: str,
        etag: str | None,
        log: logging.LoggerAdapter,
        progress: Progress | None = None,
    ) -> S3Reader:
        return S3Reader(
            self.client,
            self.settings.bucket,
            self.key(db, backup_name),
            etag,
            log,
            progress=progress,
        )


# --------------------------------------------------------------------- local


class LocalStore:
    kind = "local"

    def __init__(self, settings: LocalSettings, work_dir: Path):
        self.settings = settings
        base = Path(settings.path)
        self.base = base if base.is_absolute() else work_dir / base

    def path(self, db: Database, backup_name: str) -> Path:
        suffix = ".dir" if self.settings.format == "directory" else ".dump"
        return self.base / db.name / f"{backup_name}{suffix}"

    def partial_path(self, db: Database, backup_name: str) -> Path:
        path = self.path(db, backup_name)
        return path.with_name(path.name + ".partial")

    def manifest_path(self, db: Database, backup_name: str) -> Path:
        return self.base / db.name / f"{backup_name}.manifest.json"

    def location(self, db: Database, backup_name: str) -> str:
        return str(self.path(db, backup_name))

    def check_access(self) -> None:
        self.base.mkdir(parents=True, exist_ok=True)
        probe = self.base / f".sqdb-check-{uuid.uuid4().hex}"
        probe.write_bytes(b"")
        probe.unlink()

    def free_bytes(self) -> int:
        self.base.mkdir(parents=True, exist_ok=True)
        return shutil.disk_usage(self.base).free

    def info(self, db: Database, backup_name: str) -> DumpInfo | None:
        path = self.path(db, backup_name)
        previous = path.with_name(path.name + ".previous")
        if not path.exists() and previous.exists():
            # Recover an interrupted directory swap. Its missing manifest and
            # running/failed state still prevent automatic reuse.
            previous.rename(path)
        if not path.exists():
            return None
        items = (
            sorted(item for item in path.rglob("*") if item.is_file()) if path.is_dir() else [path]
        )
        stats = [
            (str(item.relative_to(path)) if path.is_dir() else item.name, item.stat())
            for item in items
        ]
        size = sum(stat.st_size for _, stat in stats)
        fingerprint = hashlib.sha256(
            repr([(name, s.st_size, s.st_mtime_ns, s.st_ino) for name, s in stats]).encode()
        ).hexdigest()
        manifest_path = self.manifest_path(db, backup_name)
        manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else None
        if manifest is not None and not isinstance(manifest, dict):
            raise StorageError("dump manifest must be a JSON object")
        return DumpInfo(location=str(path), size=size, manifest=manifest, etag=fingerprint)

    def prepare(self, db: Database, backup_name: str) -> Path:
        """Clear any earlier attempt and return the path pg_dump writes to."""
        for path in (self.partial_path(db, backup_name),):
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink(missing_ok=True)
        partial = self.partial_path(db, backup_name)
        partial.parent.mkdir(parents=True, exist_ok=True)
        return partial

    def commit(self, db: Database, backup_name: str) -> int:
        partial = self.partial_path(db, backup_name)
        path = self.path(db, backup_name)
        if (
            not partial.exists()
            or (partial.is_dir() and not (partial / "toc.dat").is_file())
            or (partial.is_file() and not partial.stat().st_size)
        ):
            raise StorageError(f"dump {partial} is missing after pg_dump")
        self.manifest_path(db, backup_name).unlink(missing_ok=True)
        if partial.is_dir() and path.exists():
            previous = path.with_name(path.name + ".previous")
            if previous.exists():
                if previous.is_dir():
                    shutil.rmtree(previous)
                else:
                    previous.unlink()
            path.rename(previous)
            try:
                partial.rename(path)
            except BaseException:
                previous.rename(path)
                raise
            if previous.is_dir():
                shutil.rmtree(previous)
            else:
                previous.unlink()
        else:
            partial.replace(path)
        previous = path.with_name(path.name + ".previous")
        if previous.exists():
            if previous.is_dir():
                shutil.rmtree(previous)
            else:
                previous.unlink()
        info = self.info(db, backup_name)
        if info is None or not info.size:
            raise StorageError(f"dump {self.path(db, backup_name)} is missing after pg_dump")
        return info.size

    def write_manifest(self, db: Database, backup_name: str, manifest: dict) -> None:
        path = self.manifest_path(db, backup_name)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True))
        tmp.replace(path)
