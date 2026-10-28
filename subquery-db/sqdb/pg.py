# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL access: connections, server facts, target preparation and cleanup.

Passwords never appear in connection strings handed to client tools; tools get
them through PGPASSWORD in their own environment.
"""

import logging
import re
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field

import psycopg
from psycopg import errors, sql

from .config import Credentials, Destination, Instance
from .proc import SUPERVISOR

APPLICATION_NAME = "sqdb"
# Detect dead peers during multi-hour COPY streams and idle phases.
KEEPALIVES = {
    "keepalives": "1",
    "keepalives_idle": "30",
    "keepalives_interval": "10",
    "keepalives_count": "6",
}
MISSING_DB_RE = re.compile(r'database ".*" does not exist')
SYSTEM_SCHEMA_FILTER = """
    n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    AND n.nspname NOT LIKE 'pg\\_temp\\_%' AND n.nspname NOT LIKE 'pg\\_toast\\_temp\\_%'
"""
NOT_EXTENSION_MEMBER = """
    NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                WHERE d.classid = '{catalog}'::regclass AND d.objid = {oid} AND d.deptype = 'e')
"""


class DatabaseMissing(Exception):
    """The database does not exist on the server."""


class CleanupError(Exception):
    """Cleanup would affect objects outside the requested schemas."""


def _quote(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def conninfo(instance: Instance, dbname: str, user: str) -> str:
    params = {
        "host": instance.host,
        "port": str(instance.port),
        "dbname": dbname,
        "user": user,
        "sslmode": instance.sslmode,
        "connect_timeout": str(instance.connect_timeout),
        "application_name": APPLICATION_NAME,
        **KEEPALIVES,
    }
    return " ".join(f"{key}={_quote(value)}" for key, value in params.items())


def connect(instance: Instance, dbname: str, creds: Credentials) -> psycopg.Connection:
    """Autocommit connection; DatabaseMissing when dbname does not exist."""
    SUPERVISOR.check()
    try:
        conn = psycopg.connect(
            conninfo(instance, dbname, creds.user), password=creds.password, autocommit=True
        )
    except psycopg.OperationalError as exc:
        if getattr(exc, "sqlstate", None) == "3D000" or MISSING_DB_RE.search(str(exc)):
            raise DatabaseMissing(dbname) from exc
        raise
    try:
        SUPERVISOR.register_connection(conn)
        # Catalog text such as regclass output is then always schema-qualified.
        conn.execute("SET search_path = ''")
        # Some connections stay idle for hours: the target lock, or one side of
        # verify while the other counts rows. A server-side idle_session_timeout
        # (PostgreSQL 14+) must not end them.
        if conn.info.server_version >= 140000:
            conn.execute("SET idle_session_timeout = 0")
    except BaseException:
        conn.close()
        raise
    return conn


def same_cluster(source: psycopg.Connection, target: psycopg.Connection) -> bool:
    """Detect aliases of the same server without requiring superuser functions."""
    marker = f"sqdb-{uuid.uuid4().hex}"
    source.execute("SELECT pg_catalog.set_config('application_name', %s, false)", (marker,))
    return target.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name = %s)",
        (marker,),
    ).fetchone()[0]


@dataclass(frozen=True)
class ServerFacts:
    version_num: int
    version: str

    @property
    def major(self) -> int:
        return self.version_num // 10000


def server_facts(conn: psycopg.Connection) -> ServerFacts:
    row = conn.execute(
        "SELECT current_setting('server_version_num')::int, current_setting('server_version')"
    ).fetchone()
    return ServerFacts(row[0], row[1])


@dataclass(frozen=True)
class DatabaseFacts:
    size: int
    encoding: str
    collate: str
    ctype: str
    settings: tuple[str, ...]


def database_facts(conn: psycopg.Connection) -> DatabaseFacts:
    row = conn.execute(
        """
        SELECT pg_catalog.pg_database_size(d.oid),
               pg_catalog.pg_encoding_to_char(d.encoding), d.datcollate, d.datctype,
               COALESCE((SELECT s.setconfig FROM pg_catalog.pg_db_role_setting s
                         WHERE s.setdatabase = d.oid AND s.setrole = 0), '{}')
        FROM pg_catalog.pg_database d WHERE d.datname = pg_catalog.current_database()
        """
    ).fetchone()
    return DatabaseFacts(row[0], row[1], row[2], row[3], tuple(row[4]))


def user_schemas(conn: psycopg.Connection) -> set[str]:
    """Non-system schemas other than public that no extension owns."""
    rows = conn.execute(
        f"""
        SELECT n.nspname FROM pg_catalog.pg_namespace n
        WHERE {SYSTEM_SCHEMA_FILTER} AND n.nspname <> 'public'
          AND {NOT_EXTENSION_MEMBER.format(catalog="pg_catalog.pg_namespace", oid="n.oid")}
        """
    ).fetchall()
    return {row[0] for row in rows}


# A SubQuery project's default schema. As in the original scripts, a target
# that has it is not empty and must not receive a restore.
GUARD_SCHEMA = "app"


@dataclass
class TargetState:
    """Schemas of a target database that block a restore, and the others."""

    conflicting_schemas: set[str] = field(default_factory=set)
    other_schemas: set[str] = field(default_factory=set)

    @property
    def empty(self) -> bool:
        return not self.conflicting_schemas

    def describe(self) -> str:
        if not self.conflicting_schemas:
            return "empty"
        return f"schema {', '.join(sorted(self.conflicting_schemas))} exists"


def inspect_target(conn: psycopg.Connection, dump_schemas: Iterable[str]) -> TargetState:
    """The target is not empty when it has schema app or a schema the dump creates.

    The second part extends the original app-only check to projects with a
    custom schema name, whose restore would otherwise fail in phase 1.
    """
    blocking = (set(dump_schemas) | {GUARD_SCHEMA}) - {"public"}
    schemas = user_schemas(conn)
    conflicts = {
        row[0]
        for row in conn.execute(
            "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = ANY(%s)",
            (sorted(blocking),),
        ).fetchall()
    }
    return TargetState(conflicting_schemas=conflicts, other_schemas=schemas - blocking)


def clean_target(conn: psycopg.Connection, state: TargetState, log: logging.LoggerAdapter) -> None:
    """Drop the blocking schemas an earlier restore attempt left, in one transaction."""
    with conn.transaction():
        before = _outside_dependencies(conn, state.conflicting_schemas)
        for schema in sorted(state.conflicting_schemas):
            log.info("dropping schema %s", schema)
            conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
        if before - _outside_dependencies(conn, state.conflicting_schemas):
            raise CleanupError(
                "cleanup would drop dependent objects outside the selected schemas; rolled back"
            )


def _outside_dependencies(conn: psycopg.Connection, schemas: set[str]) -> set[tuple]:
    # Identify every dependent object, including columns, constraints, triggers
    # and rewrite rules. CASCADE can otherwise silently remove objects in public
    # or in an unrelated schema even when only the requested schemas are dropped.
    return set(
        conn.execute(
            """SELECT DISTINCT d.classid, d.objid, d.objsubid
           FROM pg_catalog.pg_depend d
           CROSS JOIN LATERAL pg_catalog.pg_identify_object(
             d.classid, d.objid, d.objsubid) o
           CROSS JOIN LATERAL pg_catalog.pg_identify_object_as_address(
             d.classid, d.objid, d.objsubid) i
           WHERE COALESCE(o.schema, pg_catalog.quote_ident(i.object_names[1])) <>
             ALL(ARRAY(SELECT pg_catalog.quote_ident(s) FROM pg_catalog.unnest(%s::text[]) s))
             AND i.object_names[1] NOT LIKE 'pg_%%'
             AND i.object_names[1] <> 'information_schema'""",
            (sorted(schemas),),
        ).fetchall()
    )


@dataclass(frozen=True)
class CreatePrivileges:
    createdb: bool
    can_set_owner: bool


def create_privileges(conn: psycopg.Connection, owner: str) -> CreatePrivileges:
    # CREATE DATABASE ... OWNER needs SET ROLE on the owner since PostgreSQL 16.
    privilege = "SET" if server_facts(conn).major >= 16 else "MEMBER"
    row = conn.execute(
        """
        SELECT r.rolsuper OR r.rolcreatedb,
               r.rolsuper OR r.rolname = %(owner)s OR COALESCE(
                 (SELECT pg_catalog.pg_has_role(r.oid, o.oid, %(privilege)s)
                  FROM pg_catalog.pg_roles o WHERE o.rolname = %(owner)s), false)
        FROM pg_catalog.pg_roles r WHERE r.rolname = current_user
        """,
        {"owner": owner, "privilege": privilege},
    ).fetchone()
    return CreatePrivileges(bool(row[0]), bool(row[1]))


def create_database(
    dest: Destination, owner: Credentials, name: str, log: logging.LoggerAdapter
) -> str:
    """Create the target database and return how ownership was arranged."""
    creator = dest.admin or owner
    db = sql.Identifier(name)
    role = sql.Identifier(owner.user)
    with connect(dest, dest.maintenance_db, creator) as conn:
        if dest.admin is None:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(db))
            return f"created by {owner.user}"
        try:
            conn.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(db, role))
            return f"created by {creator.user}, owner {owner.user}"
        except errors.InsufficientPrivilege as exc:
            log.warning(
                "cannot create %s with owner %s (%s); granting privileges instead",
                name,
                owner.user,
                str(exc).strip(),
            )
        conn.execute(sql.SQL("CREATE DATABASE {}").format(db))
        conn.execute(sql.SQL("GRANT ALL ON DATABASE {} TO {}").format(db, role))
    with connect(dest, name, creator) as conn:
        conn.execute(sql.SQL("GRANT ALL ON SCHEMA public TO {}").format(role))
    return f"created by {creator.user}, ALL granted to {owner.user}"
