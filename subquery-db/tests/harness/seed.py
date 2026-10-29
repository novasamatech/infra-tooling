# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Seed the local harness from a migration config.

The config's topology is reproduced on the harness servers: every group user
with its password on the old server (source) and the new server (destination),
and every source database filled with the SubQuery-like fixture. Like a managed
database service's API, the seeder creates each empty target database owned by
its group user; group users get no CREATEDB. A destination admin (only used with
--create-databases) is created when the config has one. Everything else on the
harness is reset first. With --write-config the same config is written back
with the harness endpoints, so a real migration config can be rehearsed without
contacting anything outside the harness.

Runs inside the dev image on the harness network (make harness-seed / rehearse).
"""

import argparse
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg
import yaml
from psycopg import sql

from sqdb.config import Config, load_config

FIXTURE = Path(__file__).with_name("fixture.sql")
DEFAULT_PREFIX = "sqdb-harness"


@dataclass(frozen=True)
class Harness:
    old_host: str
    new_host: str
    s3_endpoint: str
    pg_password: str
    s3_access: str
    s3_secret: str
    rows: int

    @classmethod
    def from_env(cls) -> "Harness":
        prefix = os.environ.get("SQDB_HARNESS", DEFAULT_PREFIX)
        return cls(
            old_host=f"{prefix}-old",
            new_host=f"{prefix}-new",
            s3_endpoint=f"http://{prefix}-s3:7070",
            pg_password=os.environ.get("HARNESS_PG_PASSWORD", "harness-superuser"),
            s3_access=os.environ.get("HARNESS_S3_ACCESS", "harness-access-key"),
            s3_secret=os.environ.get("HARNESS_S3_SECRET", "harness-secret-key"),
            rows=int(os.environ.get("HARNESS_ROWS", "300")),
        )

    def superuser(self, host: str, dbname: str = "postgres") -> psycopg.Connection:
        return psycopg.connect(
            host=host,
            port=5432,
            dbname=dbname,
            user="postgres",
            password=self.pg_password,
            sslmode="disable",
            autocommit=True,
        )

    def s3(self):
        import boto3
        from botocore.config import Config as BotoConfig

        return boto3.client(
            "s3",
            endpoint_url=self.s3_endpoint,
            region_name="us-east-1",
            aws_access_key_id=self.s3_access,
            aws_secret_access_key=self.s3_secret,
            config=BotoConfig(
                s3={"addressing_style": "path"},
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )


def reset_server(conn: psycopg.Connection) -> None:
    """Drop every database and role except the bootstrap ones."""
    for (name,) in conn.execute(
        "SELECT datname FROM pg_database "
        "WHERE datname NOT IN ('postgres', 'template0', 'template1')"
    ).fetchall():
        conn.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
    for (name,) in conn.execute(
        "SELECT rolname FROM pg_roles WHERE rolname !~ '^pg_' AND rolname <> 'postgres'"
    ).fetchall():
        conn.execute(sql.SQL("DROP OWNED BY {} CASCADE").format(sql.Identifier(name)))
        conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(name)))


def create_role(conn: psycopg.Connection, user: str, password: str, *attributes: str) -> None:
    exists = conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (user,)).fetchone()
    statement = (
        "ALTER ROLE {} WITH LOGIN PASSWORD {}"
        if exists
        else "CREATE ROLE {} WITH LOGIN PASSWORD {}"
    )
    query = sql.SQL(statement).format(sql.Identifier(user), sql.Literal(password))
    if attributes:
        query = sql.SQL(" ").join([query, *(sql.SQL(attr) for attr in attributes)])
    conn.execute(query)


def grant_admin_set(conn: psycopg.Connection, user: str, admin: str) -> None:
    """Let the admin create databases owned by user (SET ROLE since PostgreSQL 16)."""
    conn.execute(
        sql.SQL("GRANT {} TO {} WITH SET TRUE, INHERIT FALSE").format(
            sql.Identifier(user), sql.Identifier(admin)
        )
    )


def create_target(conn: psycopg.Connection, name: str, owner: str) -> None:
    """An empty target database, as the database service's API creates it."""
    conn.execute(
        sql.SQL("CREATE DATABASE {} OWNER {}").format(sql.Identifier(name), sql.Identifier(owner))
    )


def schema_for(group: str, index: int) -> str:
    """SubQuery's default schema, a dashed custom one and an underscored one."""
    return ("app", f"{group}-project", f"subql_{index}")[index % 3]


def fixture_sql(conn: psycopg.Connection, schema: str, rows: int) -> str:
    text = FIXTURE.read_text(encoding="utf-8")
    return text.replace("__SCHEMA__", sql.Identifier(schema).as_string(conn)).replace(
        "__ROWS__", str(int(rows))
    )


def seed_source_database(harness: Harness, config: Config, db, schema: str, rows: int) -> None:
    with harness.superuser(harness.old_host) as admin:
        admin.execute(
            sql.SQL("CREATE DATABASE {} OWNER {}").format(
                sql.Identifier(db.name), sql.Identifier(db.source_credentials.user)
            )
        )
    with psycopg.connect(
        host=harness.old_host,
        port=5432,
        dbname=db.name,
        user=db.source_credentials.user,
        password=db.source_credentials.password,
        sslmode="disable",
        autocommit=True,
    ) as conn:
        conn.execute(fixture_sql(conn, schema, rows))


def seed(
    config: Config,
    harness: Harness,
    rows: int,
    large_rows: int,
    precreate_targets: bool = True,
) -> dict[str, str]:
    """Reset and seed both servers and the bucket; return the schema per database."""
    if config.s3:
        deadline = time.monotonic() + int(os.environ.get("HARNESS_TIMEOUT", "120"))
        while True:
            try:
                harness.s3().list_buckets()
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)
    with harness.superuser(harness.old_host) as old, harness.superuser(harness.new_host) as new:
        reset_server(old)
        reset_server(new)
        for db in config.databases:
            create_role(old, db.source_credentials.user, db.source_credentials.password)
            create_role(new, db.target_credentials.user, db.target_credentials.password)
        dest = config.destination
        if dest.admin:
            create_role(new, dest.admin.user, dest.admin.password, "CREATEDB", "CREATEROLE")
            for user in {db.target_credentials.user for db in config.databases}:
                grant_admin_set(new, user, dest.admin.user)
        if dest.maintenance_db != "postgres":
            new.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dest.maintenance_db)))
        if precreate_targets:
            for db in config.databases:
                create_target(new, db.target, db.target_credentials.user)

    schemas: dict[str, str] = {}
    for index, db in enumerate(config.databases):
        schemas[db.name] = schema_for(db.group, index)
        count = large_rows if index == 0 else rows
        seed_source_database(harness, config, db, schemas[db.name], count)
        print(f"seeded {db.name} ({db.group}, schema {schemas[db.name]}, {count} rows)")

    if config.s3:
        reset_bucket(harness, config.s3.bucket)
    return schemas


def reset_bucket(harness: Harness, bucket: str) -> None:
    client = harness.s3()
    existing = {item["Name"] for item in client.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)
        return
    for page in client.get_paginator("list_multipart_uploads").paginate(Bucket=bucket):
        for upload in page.get("Uploads", []):
            client.abort_multipart_upload(
                Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"]
            )
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for item in page.get("Contents", []):
            client.delete_object(Bucket=bucket, Key=item["Key"])


def harness_config(raw: dict, harness: Harness) -> dict:
    """The same config pointed at the harness servers and object storage."""
    data = yaml.safe_load(yaml.safe_dump(raw))
    data["source"] = {"host": harness.old_host, "port": 5432, "sslmode": "disable"}
    destination = {
        key: value
        for key, value in data["destination"].items()
        if key in {"admin", "maintenance_db"}
    }
    data["destination"] = {
        "host": harness.new_host,
        "port": 5432,
        "sslmode": "disable",
        **destination,
    }
    storage = data["storage"]
    if storage.get("type", "s3") == "s3":
        s3 = {
            key: value
            for key, value in storage["s3"].items()
            if key in {"bucket", "prefix", "chunk_size", "max_parts", "upload_concurrency"}
        }
        storage["s3"] = {
            **s3,
            "endpoint_url": harness.s3_endpoint,
            "region": "us-east-1",
            "access_key_id": harness.s3_access,
            "secret_access_key": harness.s3_secret,
            "force_path_style": True,
        }
    else:
        storage.setdefault("local", {})["path"] = "dumps"
    return data


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="migration config to replay")
    parser.add_argument("--write-config", help="write the harness variant of the config here")
    parser.add_argument("--rows", type=int, help="rows of the largest fixture table")
    parser.add_argument(
        "--large-rows",
        type=int,
        help="rows for the first database, so its dump spans several parts",
    )
    parser.add_argument(
        "--no-precreate",
        action="store_true",
        help="leave target databases missing (to rehearse --create-databases)",
    )
    args = parser.parse_args(argv)

    harness = Harness.from_env()
    raw = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    config = load_config(args.config)
    target = harness_config(raw, harness)
    rows = args.rows or harness.rows
    seed(config, harness, rows, args.large_rows or rows * 20, not args.no_precreate)
    if args.write_config:
        out = Path(args.write_config)
        out.write_text(
            "# Generated by tests/harness/seed.py for the local harness; contains secrets.\n"
            + yaml.safe_dump(target, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
        out.chmod(0o600)
        print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
