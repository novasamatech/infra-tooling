# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Migration config: strict YAML loading, validation and per-database resolution.

The config is a one-off file for a single migration and carries plain-text
credentials. Every mistake is reported before anything connects anywhere, and
values YAML would silently coerce (``yes``, ``0123``) are rejected rather than
guessed.
"""

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any

import yaml

CONFIG_VERSION = 1
SSL_MODES = ("disable", "allow", "prefer", "require", "verify-ca", "verify-full")
STORAGE_TYPES = ("s3", "local")
LOCAL_FORMATS = ("directory", "custom")
VERIFY_ROWS = ("exact", "none")
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
# PostgreSQL truncates longer identifiers (NAMEDATALEN - 1 bytes).
MAX_IDENTIFIER_BYTES = 63
# S3 limits: every part but the last must be at least 5 MiB, none above 5 GiB.
MIN_PART_SIZE = 5 * 1024**2
MAX_PART_SIZE = 5 * 1024**3
SIZE_RE = re.compile(r"^\s*(\d+)\s*([KMGT]?)(i?)B?\s*$", re.IGNORECASE)
SIZE_POWERS = {"": 0, "K": 1, "M": 2, "G": 3, "T": 4}


class ConfigError(Exception):
    """The config file is missing, malformed or inconsistent."""


@dataclass(frozen=True)
class Credentials:
    user: str
    password: str

    def __repr__(self) -> str:
        return f"Credentials(user={self.user!r}, password='***')"


@dataclass(frozen=True)
class Instance:
    host: str
    port: int
    sslmode: str
    connect_timeout: int


@dataclass(frozen=True)
class Destination(Instance):
    # Used only with --create-databases; by default target databases exist.
    maintenance_db: str
    admin: Credentials | None


@dataclass(frozen=True)
class S3Settings:
    bucket: str
    prefix: str
    endpoint_url: str | None
    region: str | None
    access_key_id: str
    secret_access_key: str
    session_token: str | None
    force_path_style: bool
    verify_ssl: bool
    chunk_size: int | None  # None: derived from the source database size
    max_parts: int
    upload_concurrency: int
    max_attempts: int
    # Double the part size before the part limit; off for stores that need
    # equal parts (all but the last).
    grow_part_size: bool = True
    # Larger parts are buffered in temporary files instead of memory.
    max_part_memory: int = 1024**3

    def __repr__(self) -> str:
        return f"S3Settings(bucket={self.bucket!r}, prefix={self.prefix!r}, secrets='***')"


@dataclass(frozen=True)
class LocalSettings:
    path: str
    format: str


@dataclass(frozen=True)
class Options:
    """Per-database tunables; groups and databases override the defaults."""

    dump_compress: str = "0"
    dump_jobs: int = 1
    restore_jobs: int = 1
    restore_pgoptions: str = (
        "-c statement_timeout=0 -c maintenance_work_mem=1GB -c max_parallel_maintenance_workers=1"
    )
    probe_ddl: bool = True
    verify_rows: str = "exact"


OPTION_TYPES: dict[str, type] = {f.name: f.type for f in fields(Options)}  # type: ignore[misc]


@dataclass(frozen=True)
class Database:
    group: str
    name: str
    target: str
    source_credentials: Credentials
    target_credentials: Credentials
    options: Options


@dataclass(frozen=True)
class Config:
    path: Path
    backup_name: str
    source: Instance
    destination: Destination
    storage_type: str
    s3: S3Settings | None
    local: LocalSettings | None
    parallel: int
    groups: tuple[str, ...]
    databases: tuple[Database, ...]

    def secrets(self) -> set[str]:
        """Every secret value, for log masking."""
        values: set[str] = set()
        for db in self.databases:
            values.add(db.source_credentials.password)
            values.add(db.target_credentials.password)
        if self.destination.admin:
            values.add(self.destination.admin.password)
        if self.s3:
            values.add(self.s3.access_key_id)
            values.add(self.s3.secret_access_key)
            if self.s3.session_token:
                values.add(self.s3.session_token)
        return {value for value in values if value}

    def select(self, groups: Iterable[str] = (), names: Iterable[str] = ()) -> list[Database]:
        """Databases matching all given filters, in config order."""
        groups, names = set(groups), set(names)
        unknown = groups - set(self.groups)
        if unknown:
            raise ConfigError(f"unknown group(s): {', '.join(sorted(unknown))}")
        known = {db.name for db in self.databases}
        unknown = names - known
        if unknown:
            raise ConfigError(f"unknown source database(s): {', '.join(sorted(unknown))}")
        return [
            db
            for db in self.databases
            if (not groups or db.group in groups) and (not names or db.name in names)
        ]


class _StrictLoader(yaml.SafeLoader):
    """SafeLoader that rejects duplicate mapping keys instead of keeping the last."""

    def construct_mapping(self, node, deep=False):
        seen: set = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, str):
                raise ConfigError(
                    f"mapping keys must be strings at line {key_node.start_mark.line + 1}"
                )
            if key in seen:
                raise ConfigError(f"duplicate key {key!r} at line {key_node.start_mark.line + 1}")
            seen.add(key)
        return super().construct_mapping(node, deep=deep)


def parse_size(value: Any, where: str) -> int:
    """Parse ``64MiB``, ``512MB``, ``1G`` or a byte count; units are binary (MB = MiB)."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    match = SIZE_RE.match(value) if isinstance(value, str) else None
    if not match:
        raise ConfigError(f"{where}: invalid size {value!r} (examples: 64MiB, 512MB, 1GiB)")
    number, unit, _ = match.groups()
    return int(number) * 1024 ** SIZE_POWERS[unit.upper()]


def format_size(size: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(size) < 1024 or unit == "TiB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    raise AssertionError("unreachable")


class _Reader:
    """Typed access to one mapping of the config with path-aware errors."""

    def __init__(self, data: Any, where: str):
        if not isinstance(data, Mapping):
            raise ConfigError(f"{where}: expected a mapping")
        if any(not isinstance(key, str) for key in data):
            raise ConfigError(f"{where}: mapping keys must be strings")
        self.data = data
        self.where = where
        self.used: set[str] = set()

    def _get(self, key: str, required: bool):
        self.used.add(key)
        if key not in self.data or self.data[key] is None:
            if required:
                raise ConfigError(f"{self.where}.{key}: required")
            return None
        return self.data[key]

    def str(self, key: str, default: str | None = None, required: bool = False) -> str | None:
        value = self._get(key, required and default is None)
        if value is None:
            return default
        if not isinstance(value, str):
            raise ConfigError(
                f"{self.where}.{key}: expected a string, got {type(value).__name__} "
                "; quote the value in YAML"
            )
        if "\x00" in value:
            raise ConfigError(f"{self.where}.{key}: must not contain NUL")
        if required and not value:
            raise ConfigError(f"{self.where}.{key}: must not be empty")
        return value

    def int(self, key: str, default: int, minimum: int = 1, maximum: int | None = None) -> int:
        value = self._get(key, False)
        if value is None:
            return default
        if not isinstance(value, int) or isinstance(value, bool):
            raise ConfigError(f"{self.where}.{key}: expected an integer, got {value!r}")
        if value < minimum or (maximum is not None and value > maximum):
            bound = f"between {minimum} and {maximum}" if maximum else f">= {minimum}"
            raise ConfigError(f"{self.where}.{key}: must be {bound}, got {value}")
        return value

    def bool(self, key: str, default: bool) -> bool:
        value = self._get(key, False)
        if value is None:
            return default
        if not isinstance(value, bool):
            raise ConfigError(f"{self.where}.{key}: expected true or false, got {value!r}")
        return value

    def choice(self, key: str, choices: tuple[str, ...], default: str) -> str:
        value = self.str(key, default)
        if value not in choices:
            raise ConfigError(f"{self.where}.{key}: must be one of {', '.join(choices)}")
        return value

    def mapping(self, key: str, required: bool = False) -> "_Reader | None":
        value = self._get(key, required)
        return None if value is None else _Reader(value, f"{self.where}.{key}")

    def finish(self) -> None:
        unknown = set(self.data) - self.used
        if unknown:
            names = ", ".join(sorted(map(str, unknown)))
            raise ConfigError(f"{self.where}: unknown key(s): {names}")


def _credentials(reader: _Reader) -> Credentials:
    user = reader.str("user", required=True)
    password = reader.str("password", required=True)
    reader.finish()
    return Credentials(user, password)


def _instance_fields(reader: _Reader) -> dict:
    return {
        "host": reader.str("host", required=True),
        "port": reader.int("port", 5432, maximum=65535),
        "sslmode": reader.choice("sslmode", SSL_MODES, "prefer"),
        "connect_timeout": reader.int("connect_timeout", 15),
    }


def _options(reader: _Reader | None, base: Options) -> Options:
    if reader is None:
        return base
    changes: dict[str, Any] = {}
    for key in reader.data:
        if key not in OPTION_TYPES:
            continue
        kind = OPTION_TYPES[key]
        if kind is int:
            changes[key] = reader.int(key, getattr(base, key))
        elif kind is bool:
            changes[key] = reader.bool(key, getattr(base, key))
        elif key == "verify_rows":
            changes[key] = reader.choice(key, VERIFY_ROWS, base.verify_rows)
        elif key == "dump_compress":
            value = reader._get(key, False)
            # Accept 0-9 unquoted as well as method specs such as "zstd:3".
            if isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 9:
                changes[key] = str(value)
            else:
                changes[key] = reader.str(key, base.dump_compress, required=True)
        else:
            changes[key] = reader.str(key, getattr(base, key), required=True)
    reader.finish()
    return replace(base, **changes)


def _check_db_name(name: str, where: str) -> None:
    if not name or "/" in name or any(ord(c) < 32 for c in name) or name in {".", ".."}:
        raise ConfigError(f"{where}: invalid database name {name!r}")
    if len(name.encode()) > MAX_IDENTIFIER_BYTES:
        raise ConfigError(f"{where}: database name {name!r} exceeds 63 bytes")


def _s3(reader: _Reader) -> S3Settings:
    chunk = reader._get("chunk_size", False)
    chunk_size = None
    if chunk is not None and chunk != "auto":
        chunk_size = parse_size(chunk, f"{reader.where}.chunk_size")
        if not MIN_PART_SIZE <= chunk_size <= MAX_PART_SIZE:
            raise ConfigError(f"{reader.where}.chunk_size: must be between 5 MiB and 5 GiB")
    prefix = reader.str("prefix", "") or ""
    if prefix.startswith("/"):
        raise ConfigError(f"{reader.where}.prefix: must not start with '/'")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    bucket = reader.str("bucket", required=True)
    memory = reader._get("max_part_memory", False)
    max_part_memory = (
        1024**3 if memory is None else parse_size(memory, f"{reader.where}.max_part_memory")
    )
    settings = S3Settings(
        bucket=bucket,
        prefix=prefix,
        endpoint_url=reader.str("endpoint_url"),
        region=reader.str("region"),
        access_key_id=reader.str("access_key_id", required=True),
        secret_access_key=reader.str("secret_access_key", required=True),
        session_token=reader.str("session_token"),
        force_path_style=reader.bool("force_path_style", False),
        verify_ssl=reader.bool("verify_ssl", True),
        chunk_size=chunk_size,
        max_parts=reader.int("max_parts", 1000, minimum=2, maximum=10000),
        upload_concurrency=reader.int("upload_concurrency", 2, maximum=16),
        max_attempts=reader.int("max_attempts", 10, maximum=50),
        grow_part_size=reader.bool("grow_part_size", True),
        max_part_memory=max_part_memory,
    )
    reader.finish()
    return settings


def _local(reader: _Reader | None) -> LocalSettings:
    if reader is None:
        return LocalSettings(path="dumps", format="directory")
    settings = LocalSettings(
        path=reader.str("path", "dumps") or "dumps",
        format=reader.choice("format", LOCAL_FORMATS, "directory"),
    )
    reader.finish()
    return settings


def _validate_options(db: Database, storage_type: str, local: LocalSettings | None) -> None:
    where = f"groups.{db.group}.databases.{db.name}"
    if not re.fullmatch(
        r"[0-9]|none|gzip(?::[0-9])?|lz4(?::(?:[0-9]|1[0-2]))?|zstd(?::(?:[1-9]|1[0-9]|2[0-2]))?",
        db.options.dump_compress,
    ):
        raise ConfigError(f"{where}: invalid dump_compress specification")
    if storage_type == "s3" and db.options.restore_jobs > 1:
        raise ConfigError(
            f"{where}: restore_jobs > 1 needs local storage; an S3 stream restores with one job"
        )
    if db.options.dump_jobs > 1 and not (storage_type == "local" and local.format == "directory"):
        raise ConfigError(f"{where}: dump_jobs > 1 needs local storage with format: directory")


def load_config(path: str | Path) -> Config:
    path = Path(path)
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeError as exc:
        raise ConfigError(f"{path}: config must be UTF-8") from exc
    except OSError as exc:
        raise ConfigError(f"cannot read {path}: {exc.strerror or exc}") from exc
    try:
        data = yaml.load(text, Loader=_StrictLoader)  # noqa: S506 - SafeLoader subclass
    except yaml.YAMLError as exc:
        # PyYAML's full exception includes source lines, possibly passwords.
        mark = getattr(exc, "problem_mark", None)
        position = f" at line {mark.line + 1}, column {mark.column + 1}" if mark else ""
        raise ConfigError(f"{path}: invalid YAML{position}") from exc
    return parse_config(data, path)


def parse_config(data: Any, path: Path = Path("<config>")) -> Config:
    root = _Reader(data, "config")
    version = root._get("version", True)
    if type(version) is not int or version != CONFIG_VERSION:
        raise ConfigError(f"config.version: expected {CONFIG_VERSION}, got {version!r}")
    backup_name = root.str("backup_name", required=True)
    if not NAME_RE.fullmatch(backup_name):
        raise ConfigError("config.backup_name: use letters, digits, '.', '_' and '-'")

    source_reader = root.mapping("source", required=True)
    source = Instance(**_instance_fields(source_reader))
    source_reader.finish()

    dest_reader = root.mapping("destination", required=True)
    dest_fields = _instance_fields(dest_reader)
    admin_reader = dest_reader.mapping("admin")
    destination = Destination(
        **dest_fields,
        maintenance_db=dest_reader.str("maintenance_db", "postgres"),
        admin=_credentials(admin_reader) if admin_reader else None,
    )
    dest_reader.finish()

    storage_reader = root.mapping("storage", required=True)
    storage_type = storage_reader.choice("type", STORAGE_TYPES, "s3")
    s3_reader = storage_reader.mapping("s3")
    local_reader = storage_reader.mapping("local")
    s3 = None
    if storage_type == "s3":
        if s3_reader is None:
            raise ConfigError("config.storage.s3: required for type s3")
        s3 = _s3(s3_reader)
        if local_reader is not None:
            _local(local_reader)  # Validate inactive settings too (unknown keys are errors).
    elif s3_reader is not None:
        _s3(s3_reader)
    local = _local(local_reader) if storage_type == "local" else None
    storage_reader.finish()

    parallel = root.int("parallel", 1, maximum=64)
    defaults = _options(root.mapping("options"), Options())

    groups_reader = root.mapping("groups", required=True)
    if not groups_reader.data:
        raise ConfigError("config.groups: at least one group is required")
    databases: list[Database] = []
    group_names: list[str] = []
    for group_name in groups_reader.data:
        groups_reader.used.add(group_name)
        if not isinstance(group_name, str) or not NAME_RE.fullmatch(group_name):
            raise ConfigError(
                f"config.groups: invalid group name {group_name!r} "
                "(letters, digits, '.', '_' and '-')"
            )
        group_names.append(group_name)
        group = _Reader(groups_reader.data[group_name], f"groups.{group_name}")
        source_creds = Credentials(
            group.str("user", required=True), group.str("password", required=True)
        )
        dest_override = group.mapping("destination")
        target_creds = _credentials(dest_override) if dest_override else source_creds
        group_options = _options(group.mapping("options"), defaults)
        entries = group._get("databases", True)
        if not isinstance(entries, list) or not entries:
            raise ConfigError(f"groups.{group_name}.databases: expected a non-empty list")
        for index, entry in enumerate(entries):
            where = f"groups.{group_name}.databases[{index}]"
            if isinstance(entry, str):
                name, target, options = entry, entry, group_options
            elif isinstance(entry, Mapping):
                item = _Reader(entry, where)
                name = item.str("name", required=True)
                target = item.str("target", name)
                options = _options(item.mapping("options"), group_options)
                item.finish()
            else:
                raise ConfigError(
                    f"{where}: expected a database name or a mapping, got {entry!r}; "
                    "quote names YAML would read as numbers or booleans"
                )
            _check_db_name(name, where)
            _check_db_name(target, f"{where}.target")
            databases.append(
                Database(group_name, name, target, source_creds, target_creds, options)
            )
        group.finish()
    groups_reader.finish()
    root.finish()

    _check_unique(databases)
    source_names = {db.name for db in databases}
    if (
        source.host == destination.host
        and source.port == destination.port
        and any(db.target in source_names for db in databases)
    ):
        raise ConfigError(
            "source and destination overlap: a target is a configured source database"
        )
    for db in databases:
        _validate_options(db, storage_type, local)
    return Config(
        path=path,
        backup_name=backup_name,
        source=source,
        destination=destination,
        storage_type=storage_type,
        s3=s3,
        local=local,
        parallel=parallel,
        groups=tuple(group_names),
        databases=tuple(databases),
    )


def _check_unique(databases: list[Database]) -> None:
    sources: dict[str, str] = {}
    targets: dict[str, str] = {}
    for db in databases:
        if db.name in sources:
            raise ConfigError(
                f"source database {db.name!r} is listed twice "
                f"(groups {sources[db.name]} and {db.group})"
            )
        sources[db.name] = db.group
        if db.target in targets:
            raise ConfigError(
                f"target database {db.target!r} is used twice "
                f"(groups {targets[db.target]} and {db.group})"
            )
        targets[db.target] = db.group
    for side in ("source_credentials", "target_credentials"):
        passwords: dict[str, tuple[str, str]] = {}
        for db in databases:
            creds: Credentials = getattr(db, side)
            known = passwords.setdefault(creds.user, (creds.password, db.group))
            if known[0] != creds.password:
                raise ConfigError(
                    f"user {creds.user!r} has different passwords in groups "
                    f"{known[1]} and {db.group}"
                )
