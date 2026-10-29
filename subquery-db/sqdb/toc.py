# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Archive TOC parsing (``pg_restore -l``) and the restore phase lists.

Phase 2 restores the index entries on empty tables; phase 4 restores the rest
of post-data. The split follows the proven ``grep " INDEX "`` selection (plain
indexes, their partition attachments and comments go to phase 2) with two
corrections that only matter outside plain SubQuery schemas:

* ``INDEX ATTACH`` and ``COMMENT ON INDEX`` entries whose index belongs to a
  constraint (for example the primary key of a partitioned table) stay in
  phase 4, where that constraint is created; in phase 2 they would fail.
* Object names that merely contain "INDEX" do not move an entry.
"""

import re
from collections import Counter
from dataclasses import dataclass, field, replace

# Entry types that contain spaces; the longest match wins.
MULTIWORD_TYPES = sorted(
    (
        "ACCESS METHOD",
        "BLOB METADATA",
        "CHECK CONSTRAINT",
        "DATABASE PROPERTIES",
        "DEFAULT ACL",
        "EVENT TRIGGER",
        "FK CONSTRAINT",
        "FOREIGN DATA WRAPPER",
        "FOREIGN SERVER",
        "FOREIGN TABLE",
        "INDEX ATTACH",
        "LARGE OBJECT",
        "MATERIALIZED VIEW",
        "MATERIALIZED VIEW DATA",
        "OPERATOR CLASS",
        "OPERATOR FAMILY",
        "PROCEDURAL LANGUAGE",
        "PUBLICATION TABLE",
        "PUBLICATION TABLES IN SCHEMA",
        "ROW SECURITY",
        "SEQUENCE OWNED BY",
        "SEQUENCE SET",
        "SHELL TYPE",
        "STATISTICS DATA",
        "SUBSCRIPTION TABLE",
        "TABLE ATTACH",
        "TABLE DATA",
        "TEXT SEARCH CONFIGURATION",
        "TEXT SEARCH DICTIONARY",
        "TEXT SEARCH PARSER",
        "TEXT SEARCH TEMPLATE",
        "USER MAPPING",
    ),
    key=len,
    reverse=True,
)
ENTRY_RE = re.compile(r"^(\d+); (\d+) (\d+) (.+)$")
HEADER_RE = re.compile(r"^;\s+([A-Za-z][A-Za-z ]*?):\s*(.*)$")
DEPENDENCIES_RE = re.compile(r"^;\s*depends on:\s*(.*)$")
SCHEMA_SQL_RE = re.compile(r'^CREATE SCHEMA ("(?:[^"]|"")*"|[^\s;]+);$', re.MULTILINE)


@dataclass(frozen=True)
class Entry:
    line: str
    dump_id: int
    type: str
    namespace: str
    # Tag and owner as printed; the owner is blank for some entry types.
    detail: str
    dependencies: tuple[int, ...] = ()

    @property
    def tag(self) -> str:
        return self.detail.rstrip().rpartition(" ")[0] or self.detail.strip()

    @property
    def commented_index(self) -> str | None:
        """Index name of a ``COMMENT ON INDEX`` entry (printed quoted when needed)."""
        if self.type != "COMMENT" or not self.detail.startswith("INDEX "):
            return None
        return _unquote(self.tag[len("INDEX ") :])


def _unquote(name: str) -> str:
    if len(name) >= 2 and name.startswith('"') and name.endswith('"'):
        return name[1:-1].replace('""', '"')
    return name


@dataclass
class Toc:
    entries: list[Entry]
    header: dict[str, str] = field(default_factory=dict)
    schema_names: set[str] | None = None

    def __post_init__(self) -> None:
        plain = {(e.namespace, e.tag) for e in self.entries if e.type == "INDEX"}
        plain_ids = {e.dump_id for e in self.entries if e.type == "INDEX"}
        self._index_ids = {
            e.dump_id
            for e in self.entries
            if e.type == "INDEX"
            or (
                e.type in {"INDEX ATTACH", "COMMENT"}
                and bool(plain_ids.intersection(e.dependencies))
            )
            or (not e.dependencies and e.type == "INDEX ATTACH" and (e.namespace, e.tag) in plain)
            or (
                not e.dependencies
                and e.commented_index is not None
                and (e.namespace, e.commented_index) in plain
            )
        }

    def is_index(self, entry: Entry) -> bool:
        return entry.dump_id in self._index_ids

    @property
    def index_entries(self) -> list[Entry]:
        return [entry for entry in self.entries if self.is_index(entry)]

    @property
    def other_entries(self) -> list[Entry]:
        return [entry for entry in self.entries if not self.is_index(entry)]

    @property
    def schemas(self) -> set[str]:
        """Schemas the archive creates (``public`` is never dumped as an entry)."""
        if self.schema_names is not None:
            return self.schema_names
        return {entry.tag for entry in self.entries if entry.type == "SCHEMA"}

    def counts(self) -> dict[str, int]:
        return dict(sorted(Counter(entry.type for entry in self.entries).items()))

    def phase_list(self, phase: int) -> str:
        """``--use-list`` content for phase 2 (indexes) or phase 4 (the rest)."""
        if phase == 2:
            selected = self.index_entries
        elif phase == 4:
            selected = self.other_entries
        else:
            raise ValueError(f"phase {phase} restores by section, not by list")
        return "".join(f"{entry.line}\n" for entry in selected)


def _split_type(rest: str) -> tuple[str, str]:
    for candidate in MULTIWORD_TYPES:
        if rest.startswith(candidate + " "):
            return candidate, rest[len(candidate) + 1 :]
    kind, _, remainder = rest.partition(" ")
    return kind, remainder


def parse_toc(text: str) -> Toc:
    entries: list[Entry] = []
    header: dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        if line.startswith(";"):
            dependencies = DEPENDENCIES_RE.match(line)
            if dependencies and entries:
                entries[-1] = replace(
                    entries[-1], dependencies=tuple(map(int, dependencies.group(1).split()))
                )
                continue
            match = HEADER_RE.match(line)
            if match:
                header.setdefault(match.group(1).strip(), match.group(2).strip())
            continue
        match = ENTRY_RE.match(line)
        if not match:
            raise ValueError(f"unexpected TOC line: {line!r}")
        kind, remainder = _split_type(match.group(4))
        namespace, _, detail = remainder.partition(" ")
        entries.append(
            Entry(
                line=line,
                dump_id=int(match.group(1)),
                type=kind,
                namespace=namespace,
                detail=detail,
            )
        )
    if not entries:
        raise ValueError("the archive TOC has no entries")
    return Toc(entries=entries, header=header)


def schema_names(script: str) -> set[str]:
    """Read schema identifiers from pg_restore SQL; TOC names are not escaped."""
    return {_unquote(match.group(1)) for match in SCHEMA_SQL_RE.finditer(script)}
