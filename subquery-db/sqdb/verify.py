# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Source/target comparison after a restore.

Structure is compared through catalog text that both server versions render
the same way (pg_get_indexdef, pg_get_constraintdef, ...). Extension members are
left out: they belong to the extension version installed on each server.
Exact row counts need a quiescent source; a source that keeps writing will
show up as row mismatches.
"""

import logging
from dataclasses import dataclass, field
from typing import Any

import psycopg
from psycopg import sql

from .pg import NOT_EXTENSION_MEMBER, SYSTEM_SCHEMA_FILTER, database_facts
from .proc import SUPERVISOR

SAMPLE = 20


def _not_ext(catalog: str, oid: str) -> str:
    return NOT_EXTENSION_MEMBER.format(catalog=f"pg_catalog.{catalog}", oid=oid)


CATALOG_QUERIES: dict[str, str] = {
    "schemas": f"""
        SELECT n.nspname, '' FROM pg_catalog.pg_namespace n
        WHERE {SYSTEM_SCHEMA_FILTER} AND {_not_ext("pg_namespace", "n.oid")}""",
    "extensions": "SELECT e.extname, '' FROM pg_catalog.pg_extension e",
    "relations": f"""
        SELECT pg_catalog.format('%I.%I', n.nspname, c.relname),
               pg_catalog.json_build_array(c.relkind, c.relpersistence, c.relreplident,
                 c.relrowsecurity, c.relforcerowsecurity, c.relispopulated,
                 c.reloptions, pg_catalog.pg_get_expr(c.relpartbound, c.oid),
                 CASE WHEN c.relkind = 'p' THEN pg_catalog.pg_get_partkeydef(c.oid) END,
                 (SELECT pg_catalog.string_agg(i.inhparent::regclass::text, ',' ORDER BY i.inhseqno)
                  FROM pg_catalog.pg_inherits i WHERE i.inhrelid = c.oid))::text
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f', 'c', 'i', 'I')
          AND {SYSTEM_SCHEMA_FILTER} AND {_not_ext("pg_class", "c.oid")}""",
    "columns": f"""
        SELECT pg_catalog.format('%I.%I.%I', n.nspname, c.relname, a.attname),
               pg_catalog.concat_ws(' ',
                 (pg_catalog.row_number() OVER (PARTITION BY c.oid ORDER BY a.attnum))::text,
                 pg_catalog.format_type(a.atttypid, a.atttypmod),
                 CASE WHEN a.attnotnull THEN 'not null' END,
                 'default ' || pg_catalog.pg_get_expr(ad.adbin, ad.adrelid),
                 NULLIF(a.attidentity, '') , NULLIF(a.attgenerated, ''),
                 CASE WHEN co.oid IS NOT NULL THEN 'collate ' ||
                   pg_catalog.format('%I.%I', cn.nspname, co.collname) END)
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
        LEFT JOIN pg_catalog.pg_collation co
          ON co.oid = a.attcollation AND co.collname <> 'default'
        LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid = co.collnamespace
        WHERE a.attnum > 0 AND NOT a.attisdropped
          AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'c')
          AND {SYSTEM_SCHEMA_FILTER} AND {_not_ext("pg_class", "c.oid")}""",
    "indexes": f"""
        SELECT pg_catalog.format('%I.%I', n.nspname, c.relname),
               pg_catalog.pg_get_indexdef(i.indexrelid)
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE {SYSTEM_SCHEMA_FILTER} AND {_not_ext("pg_class", "i.indrelid")}""",
    "constraints": f"""
        SELECT pg_catalog.format('%I.%I', n.nspname, con.conname) || ' on '
                 || CASE WHEN con.conrelid <> 0 THEN con.conrelid::pg_catalog.regclass::text
                         ELSE con.contypid::pg_catalog.regtype::text END,
               con.contype::text || ' ' || pg_catalog.pg_get_constraintdef(con.oid)
        FROM pg_catalog.pg_constraint con
        JOIN pg_catalog.pg_namespace n ON n.oid = con.connamespace
        WHERE {SYSTEM_SCHEMA_FILTER} AND {_not_ext("pg_constraint", "con.oid")}""",
    "triggers": f"""
        SELECT t.tgrelid::pg_catalog.regclass::text || ' ' || t.tgname,
               pg_catalog.pg_get_triggerdef(t.oid) || ' enabled=' || t.tgenabled::text
        FROM pg_catalog.pg_trigger t
        JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE NOT t.tgisinternal AND {SYSTEM_SCHEMA_FILTER}
          AND {_not_ext("pg_trigger", "t.oid")}""",
    "routines": f"""
        SELECT p.oid::pg_catalog.regprocedure::text,
               pg_catalog.pg_get_functiondef(p.oid)
        FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE p.prokind <> 'a' AND {SYSTEM_SCHEMA_FILTER}
          AND {_not_ext("pg_proc", "p.oid")}""",
    "aggregates": f"""
        SELECT p.oid::pg_catalog.regprocedure::text,
               pg_catalog.json_build_array(pg_catalog.pg_get_function_result(p.oid),
                 p.proparallel, a.aggkind, a.aggnumdirectargs,
                 a.aggtransfn::regprocedure::text, a.aggfinalfn::regprocedure::text,
                 a.aggcombinefn::regprocedure::text, a.aggserialfn::regprocedure::text,
                 a.aggdeserialfn::regprocedure::text, a.aggmtransfn::regprocedure::text,
                 a.aggminvtransfn::regprocedure::text, a.aggmfinalfn::regprocedure::text,
                 a.aggfinalextra, a.aggmfinalextra, a.aggfinalmodify, a.aggmfinalmodify,
                 a.aggsortop::regoperator::text, a.aggtranstype::regtype::text,
                 a.aggtransspace, a.aggmtranstype::regtype::text, a.aggmtransspace,
                 a.agginitval, a.aggminitval)::text
        FROM pg_catalog.pg_aggregate a JOIN pg_catalog.pg_proc p ON p.oid = a.aggfnoid
        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE {SYSTEM_SCHEMA_FILTER} AND {_not_ext("pg_proc", "p.oid")}""",
    "types": f"""
        SELECT pg_catalog.format('%I.%I', n.nspname, t.typname),
               t.typtype::text || COALESCE(' ' || CASE t.typtype
                 WHEN 'e' THEN (SELECT pg_catalog.array_agg(e.enumlabel
                                                            ORDER BY e.enumsortorder)::text
                                FROM pg_catalog.pg_enum e WHERE e.enumtypid = t.oid)
                 WHEN 'd' THEN pg_catalog.format_type(t.typbasetype, t.typtypmod)
                    || ' notnull=' || t.typnotnull::text
                    || ' default=' || COALESCE(t.typdefault, '') END, '')
        FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typtype IN ('b', 'c', 'd', 'e', 'r', 'm')
          AND (t.typrelid = 0 OR (SELECT c.relkind FROM pg_catalog.pg_class c
                                  WHERE c.oid = t.typrelid) = 'c')
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_type a WHERE a.typarray = t.oid)
          AND {SYSTEM_SCHEMA_FILTER} AND {_not_ext("pg_type", "t.oid")}""",
    "views": f"""
        SELECT pg_catalog.format('%I.%I', n.nspname, c.relname),
               pg_catalog.pg_get_viewdef(c.oid, true)
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('v', 'm') AND {SYSTEM_SCHEMA_FILTER}
          AND {_not_ext("pg_class", "c.oid")}""",
    "sequences": f"""
        SELECT pg_catalog.format('%I.%I', n.nspname, c.relname),
               pg_catalog.json_build_array(s.seqtypid::regtype::text, s.seqstart,
                 s.seqincrement, s.seqmax, s.seqmin, s.seqcache, s.seqcycle,
                 (SELECT pg_catalog.format('%s.%I', a.attrelid::regclass, a.attname)
                  FROM pg_catalog.pg_depend d
                  JOIN pg_catalog.pg_attribute a ON a.attrelid = d.refobjid
                       AND a.attnum = d.refobjsubid
                  WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid
                    AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i')))::text
        FROM pg_catalog.pg_sequence s JOIN pg_catalog.pg_class c ON c.oid = s.seqrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE {SYSTEM_SCHEMA_FILTER} AND {_not_ext("pg_class", "c.oid")}""",
    "policies": f"""
        SELECT pg_catalog.format('%I.%I', n.nspname, c.relname) || ' ' || p.polname,
               pg_catalog.json_build_array(p.polcmd, p.polpermissive,
                 (SELECT pg_catalog.array_agg(CASE WHEN r = 0 THEN 'public'
                            ELSE pg_catalog.pg_get_userbyid(r) END ORDER BY
                              CASE WHEN r = 0 THEN 'public'
                              ELSE pg_catalog.pg_get_userbyid(r) END)
                  FROM pg_catalog.unnest(p.polroles) r),
                 pg_catalog.pg_get_expr(p.polqual, p.polrelid),
                 pg_catalog.pg_get_expr(p.polwithcheck, p.polrelid))::text
        FROM pg_catalog.pg_policy p JOIN pg_catalog.pg_class c ON c.oid = p.polrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE {SYSTEM_SCHEMA_FILTER} AND {_not_ext("pg_class", "c.oid")}""",
    "comments": """
        SELECT pg_catalog.pg_describe_object(d.classoid, d.objoid, d.objsubid), d.description
        FROM pg_catalog.pg_description d
        WHERE d.objoid >= 16384
          AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend dep
                          WHERE dep.classid = d.classoid AND dep.objid = d.objoid
                            AND dep.deptype = 'e')""",
}

COUNT_TABLES = f"""
    SELECT n.nspname, c.relname, pg_catalog.format('%I.%I', n.nspname, c.relname)
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE (c.relkind = 'r' OR (c.relkind = 'm' AND c.relispopulated))
      AND {SYSTEM_SCHEMA_FILTER} AND {_not_ext("pg_class", "c.oid")}
    ORDER BY 1, 2
"""

INVALID_INDEXES = f"""
    SELECT pg_catalog.format('%I.%I', n.nspname, c.relname)
    FROM pg_catalog.pg_index i
    JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE (NOT i.indisvalid OR NOT i.indisready) AND {SYSTEM_SCHEMA_FILTER}
"""


def catalog_snapshot(conn: psycopg.Connection) -> dict[str, dict[str, str]]:
    snapshot: dict[str, dict[str, str]] = {}
    for name, query in CATALOG_QUERIES.items():
        SUPERVISOR.check()
        snapshot[name] = {row[0]: row[1] for row in conn.execute(query).fetchall()}
    for name in list(snapshot["sequences"]):
        SUPERVISOR.check()
        # is_called changes the next generated value even when last_value is
        # identical; pg_sequences hides it (and hides uncalled last_value).
        value, called = conn.execute(
            sql.SQL("SELECT last_value, is_called FROM {}").format(
                sql.Identifier(*_sequence_name(conn, name))
            )
        ).fetchone()
        snapshot["sequences"][name] += f" value={value} called={called}"
    return snapshot


def _sequence_name(conn: psycopg.Connection, name: str) -> tuple[str, str]:
    return conn.execute(
        "SELECT n.nspname, c.relname FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = %s::regclass",
        (name,),
    ).fetchone()


def row_counts(conn: psycopg.Connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    for schema, table, name in conn.execute(COUNT_TABLES).fetchall():
        SUPERVISOR.check()
        query = sql.SQL("SELECT count(*) FROM ONLY {}.{}").format(
            sql.Identifier(schema), sql.Identifier(table)
        )
        counts[name] = conn.execute(query).fetchone()[0]
    return counts


@dataclass
class Difference:
    missing: list[str] = field(default_factory=list)
    extra: list[str] = field(default_factory=list)
    changed: list[dict[str, str]] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.missing or self.extra or self.changed)


def compare_maps(source: dict[str, Any], target: dict[str, Any]) -> Difference:
    diff = Difference()
    diff.missing = sorted(set(source) - set(target))
    diff.extra = sorted(set(target) - set(source))
    for key in sorted(set(source) & set(target)):
        if source[key] != target[key]:
            diff.changed.append(
                {"name": key, "source": str(source[key]), "target": str(target[key])}
            )
    return diff


@dataclass
class Report:
    source_db: str
    target_db: str
    differences: dict[str, Difference] = field(default_factory=dict)
    invalid_indexes: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    rows_checked: bool = False
    tables: int = 0
    total_rows: int = 0

    @property
    def ok(self) -> bool:
        return not self.invalid_indexes and not any(self.differences.values())

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_db": self.source_db,
            "target_db": self.target_db,
            "ok": self.ok,
            "rows_checked": self.rows_checked,
            "tables": self.tables,
            "total_rows": self.total_rows,
            "invalid_indexes": self.invalid_indexes,
            "warnings": self.warnings,
            "differences": {
                name: {"missing": d.missing, "extra": d.extra, "changed": d.changed}
                for name, d in self.differences.items()
                if d
            },
        }

    def summary_lines(self) -> list[str]:
        lines = []
        for name, diff in self.differences.items():
            if not diff:
                continue
            for label, items in (
                ("missing in target", diff.missing),
                ("extra in target", diff.extra),
            ):
                if items:
                    sample = ", ".join(items[:SAMPLE])
                    more = f" (+{len(items) - SAMPLE} more)" if len(items) > SAMPLE else ""
                    lines.append(f"{name}: {len(items)} {label}: {sample}{more}")
            for change in diff.changed[:SAMPLE]:
                lines.append(
                    f"{name}: {change['name']} differs: source={change['source']!r} "
                    f"target={change['target']!r}"
                )
            if len(diff.changed) > SAMPLE:
                lines.append(f"{name}: +{len(diff.changed) - SAMPLE} more differences")
        if self.invalid_indexes:
            lines.append(f"invalid indexes in target: {', '.join(self.invalid_indexes)}")
        return lines


def compare_databases(
    source: psycopg.Connection,
    target: psycopg.Connection,
    source_db: str,
    target_db: str,
    rows: bool,
    log: logging.LoggerAdapter,
) -> Report:
    report = Report(source_db, target_db)
    src_facts, dst_facts = database_facts(source), database_facts(target)
    for attr in ("encoding", "collate", "ctype", "settings"):
        src_value, dst_value = getattr(src_facts, attr), getattr(dst_facts, attr)
        if src_value != dst_value:
            report.warnings.append(
                f"database {attr} differs: source={src_value} target={dst_value}"
            )

    log.info("comparing catalogs")
    src_catalog, dst_catalog = catalog_snapshot(source), catalog_snapshot(target)
    for name in CATALOG_QUERIES:
        if name == "extensions":
            continue
        report.differences[name] = compare_maps(src_catalog[name], dst_catalog[name])
    missing_ext = sorted(set(src_catalog["extensions"]) - set(dst_catalog["extensions"]))
    report.differences["extensions"] = Difference(missing=missing_ext)
    report.invalid_indexes = [row[0] for row in target.execute(INVALID_INDEXES).fetchall()]

    if rows:
        log.info("counting rows in source and target")
        src_rows, dst_rows = row_counts(source), row_counts(target)
        report.differences["row_counts"] = compare_maps(src_rows, dst_rows)
        report.rows_checked = True
        report.tables = len(src_rows)
        report.total_rows = sum(src_rows.values())
    return report
