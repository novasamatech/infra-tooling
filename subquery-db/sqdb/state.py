# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Run state per backup name: which dumps, restore phases and checks finished.

The file lets a rerun skip finished work and refuse unsafe shortcuts. It is
written atomically after every change, so a crash leaves the last good state.
"""

import fcntl
import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .log import MASKER

STATE_VERSION = 1


def now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class State:
    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        self.data: dict[str, Any] = {"version": STATE_VERSION, "databases": {}}
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            try:
                loaded = json.loads(self.path.read_text(encoding="utf-8"))
            except (ValueError, OSError) as exc:
                raise ValueError(
                    f"{self.path}: cannot read state; "
                    "use a valid state file or a new work directory"
                ) from exc
            if not isinstance(loaded, dict):
                raise ValueError(f"{self.path}: state must be an object")
            if type(loaded.get("version")) is not int or loaded.get("version") != STATE_VERSION:
                raise ValueError(
                    f"{self.path}: unsupported state version {loaded.get('version')!r}"
                )
            databases = loaded.get("databases")
            if not isinstance(databases, dict) or any(
                not isinstance(entry, dict)
                or any(not isinstance(section, dict) for section in entry.values())
                for entry in databases.values()
            ):
                raise ValueError(f"{self.path}: invalid database state")
            self.data = loaded

    @contextmanager
    def lock_run(self) -> Iterator[None]:
        """Exclude concurrent writers, including runs with a different backup name."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with (self.path.parent / ".sqdb.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ValueError("another sqdb command is using this work directory") from exc
            try:
                self._load()  # A writer may have completed since this object was constructed.
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def bind(self, db: str, source: dict, target: dict, storage: dict) -> bool:
        """Discard progress that belongs to other endpoints, users or archives."""
        context = {"source": source, "target": target, "storage": storage}
        with self._lock:
            entry = self.data["databases"].setdefault(db, {})
            previous = entry.get("context", {})
            changed = bool(entry) and previous != context
            if previous.get("source") != source or previous.get("storage") != storage:
                entry.clear()
            elif previous.get("target") != target:
                for section in ("restore", "verify"):
                    entry.pop(section, None)
            entry["context"] = context
            if changed:
                self._save()
            return changed

    def get(self, db: str, section: str) -> dict[str, Any]:
        with self._lock:
            return deepcopy(self.data["databases"].get(db, {}).get(section, {}))

    def all(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return json.loads(json.dumps(self.data["databases"]))

    def update(self, db: str, section: str, replace: bool = False, **values: Any) -> None:
        with self._lock:
            entry = self.data["databases"].setdefault(db, {})
            current = {} if replace else entry.get(section, {})
            if "error" in values:
                values["error"] = MASKER(values["error"])
            current.update(values, updated_at=now())
            entry[section] = current
            self._save()

    def clear(self, db: str, section: str) -> None:
        with self._lock:
            self.data["databases"].get(db, {}).pop(section, None)
            self._save()

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + ".tmp")
        tmp.write_text(json.dumps(self.data, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self.path)
