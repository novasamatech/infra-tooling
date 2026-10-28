# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Shared offline fixtures: an in-memory S3 client and config builders."""

import copy
import io
import logging
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from sqdb.config import parse_config
from sqdb.log import db_logger

DATA = Path(__file__).parent / "data"

BASE = {
    "version": 1,
    "backup_name": "20261027-pg17",
    "source": {"host": "old.example", "port": 5432},
    "destination": {"host": "new.example", "admin": {"user": "admin", "password": "admin-pw"}},
    "storage": {
        "type": "s3",
        "s3": {
            "bucket": "dumps",
            "endpoint_url": "https://s3.example",
            "region": "region-1",
            "access_key_id": "AKIA",
            "secret_access_key": "s3-secret",
        },
    },
    "groups": {
        "accounts": {
            "user": "accounts",
            "password": "acc-pw",
            "databases": ["chain-a-accounts", {"name": "chain-b-accounts", "target": "chain-b"}],
        },
        "transfers": {
            "user": "transfers",
            "password": "tr-pw",
            "options": {"verify_rows": "none"},
            "databases": [{"name": "chain-a-transfers", "options": {"probe_ddl": False}}],
        },
    },
}


@pytest.fixture
def raw_config():
    return copy.deepcopy(BASE)


@pytest.fixture
def config(raw_config):
    return parse_config(raw_config)


@pytest.fixture
def log():
    logger = logging.getLogger("sqdb")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    logger.propagate = True  # let caplog see records after a CLI test configured logging
    return db_logger("test")


def client_error(code: str, status: int) -> ClientError:
    return ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}, "op"
    )


class FakeBody(io.BytesIO):
    """Streaming body that can fail once after a number of bytes."""

    def __init__(self, data: bytes, fail_after: int | None = None):
        super().__init__(data)
        self.fail_after = fail_after
        self.served = 0

    def read(self, size=-1):
        if self.fail_after is not None and self.served >= self.fail_after:
            raise ConnectionResetError("connection reset by peer")
        chunk = super().read(
            size if self.fail_after is None else min(size, self.fail_after - self.served)
        )
        self.served += len(chunk)
        return chunk


class FakeS3:
    """The subset of the S3 API sqdb uses, kept in memory."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.uploads: dict[str, dict] = {}
        self.aborted: list[str] = []
        self.get_calls: list[dict] = []
        self.fail_part: int | None = None
        self.fail_reads: list[int] = []  # byte offsets at which successive bodies break
        self.counter = 0

    def head_bucket(self, Bucket):
        return {}

    def create_multipart_upload(self, Bucket, Key):
        self.counter += 1
        upload_id = f"upload-{self.counter}"
        self.uploads[upload_id] = {"key": Key, "parts": {}}
        return {"UploadId": upload_id}

    def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):
        if self.fail_part == PartNumber:
            raise client_error("InternalError", 500)
        self.uploads[UploadId]["parts"][PartNumber] = Body
        return {"ETag": f'"etag-{PartNumber}"'}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        upload = self.uploads.pop(UploadId)
        numbers = [part["PartNumber"] for part in MultipartUpload["Parts"]]
        assert numbers == sorted(upload["parts"])
        self.objects[Key] = b"".join(upload["parts"][n] for n in numbers)

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self.uploads.pop(UploadId, None)
        self.aborted.append(Key)

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise client_error("404", 404)
        return {"ContentLength": len(self.objects[Key]), "ETag": f'"{len(self.objects[Key])}"'}

    def get_object(self, Bucket, Key, Range=None, IfMatch=None):
        self.get_calls.append({"Key": Key, "Range": Range, "IfMatch": IfMatch})
        if Key not in self.objects:
            raise client_error("NoSuchKey", 404)
        data = self.objects[Key]
        if Range:
            data = data[int(Range.removeprefix("bytes=").rstrip("-")) :]
        fail_after = self.fail_reads.pop(0) if self.fail_reads else None
        return {"Body": FakeBody(data, fail_after)}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[Key] = Body

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)


@pytest.fixture
def s3():
    return FakeS3()
