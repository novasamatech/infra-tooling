# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Child process supervision for pg_dump, pg_restore and psql.

Every tool runs in its own session, so a terminal interrupt reaches only sqdb,
which then stops its children in order (and aborts uploads) instead of racing
them. Tool stderr goes to the database's log file; its tail is kept for error
messages.
"""

import contextlib
import logging
import os
import re
import signal
import subprocess
import threading
import time
import weakref
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO

from .log import MASKER
from .storage import READ_BLOCK, Progress, duration

STDERR_TAIL = 30
TERMINATE_GRACE_SECONDS = 10


class Interrupted(Exception):
    """The run was interrupted by a signal."""


ERROR_LINE_RE = re.compile(r"\b(error|fatal|detail|hint)\b", re.IGNORECASE)


class ToolError(Exception):
    def __init__(self, label: str, returncode: int, tail: Sequence[str]):
        self.label = label
        self.returncode = returncode
        self.tail = list(tail)
        # Verbose tools log progress too; keep the lines that explain the failure.
        self.reason = [line for line in self.tail if ERROR_LINE_RE.search(line)] or self.tail[-5:]
        super().__init__(f"{label} exited with code {returncode}: " + "\n".join(self.reason))

    @property
    def summary(self) -> str:
        return self.reason[0] if self.reason else f"{self.label} exited with {self.returncode}"


class Supervisor:
    """Tracks running children and the global stop request."""

    def __init__(self):
        self.stop = threading.Event()
        self._procs: set[subprocess.Popen] = set()
        self._connections: weakref.WeakSet = weakref.WeakSet()
        self._lock = threading.Lock()

    def spawn(self, argv: Sequence[str], **kwargs: Any) -> subprocess.Popen:
        with self._lock:
            if self.stop.is_set():
                raise Interrupted()
            proc = subprocess.Popen(argv, start_new_session=True, **kwargs)
            self._procs.add(proc)
            return proc

    def release(self, proc: subprocess.Popen) -> None:
        with self._lock:
            self._procs.discard(proc)

    def request_stop(self) -> None:
        self.stop.set()
        with self._lock:
            procs = list(self._procs)
            connections = list(self._connections)
        for proc in procs:
            terminate(proc)
        for conn in connections:
            # Cancellation must not block the signal handler on network I/O.
            threading.Thread(target=self._cancel_connection, args=(conn,), daemon=True).start()

    @staticmethod
    def _cancel_connection(conn) -> None:
        with contextlib.suppress(Exception):
            conn.cancel_safe(timeout=5)

    def register_connection(self, conn) -> None:
        with self._lock:
            self._connections.add(conn)
        self.check()

    def check(self) -> None:
        if self.stop.is_set():
            raise Interrupted()


SUPERVISOR = Supervisor()


def terminate(proc: subprocess.Popen) -> None:
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return


def _kill_after_grace(proc: subprocess.Popen) -> None:
    try:
        proc.wait(timeout=TERMINATE_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
        proc.wait()


@dataclass
class ToolResult:
    returncode: int
    tail: list[str]
    output: Any = None


class _Thread(threading.Thread):
    """Thread that keeps its target's result or exception."""

    def __init__(self, name: str, target: Callable[[], Any]):
        super().__init__(name=name, daemon=True)
        self._target_fn = target
        self.result: Any = None
        self.error: BaseException | None = None

    def run(self) -> None:
        try:
            self.result = self._target_fn()
        except BaseException as exc:
            self.error = exc


def run_tool(
    argv: Sequence[str],
    *,
    label: str,
    env: Mapping[str, str],
    log: logging.LoggerAdapter,
    tool_log: Path,
    stdin_source: BinaryIO | bytes | None = None,
    stdout_consumer: Callable[[BinaryIO, Callable[[], bool]], Any] | None = None,
    progress: Progress | None = None,
    heartbeat: float = 60.0,
    check: bool = True,
    supervisor: Supervisor = SUPERVISOR,
) -> ToolResult:
    """Run one client tool to completion.

    ``stdin_source`` is copied into the tool's stdin; a tool that stops reading
    early (pg_restore after the archive header) is not an error.
    ``stdout_consumer(stream, succeeded)`` reads the tool's stdout in a thread;
    ``succeeded()`` waits for the tool and tells whether it exited with 0.
    A failing feeder or consumer stops the tool.
    """
    tool_log.parent.mkdir(parents=True, exist_ok=True)
    proc = supervisor.spawn(
        list(argv),
        stdin=subprocess.PIPE if stdin_source is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE if stdout_consumer is not None else subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        env=dict(env),
    )
    tail: deque[str] = deque(maxlen=STDERR_TAIL)
    threads: list[_Thread] = []
    # Set when a feeder/consumer failure stopped the tool: that failure is the cause.
    stopped_by_thread = threading.Event()

    def stop_for_thread() -> None:
        # A tool that already exited failed on its own; the stream only noticed.
        if proc.poll() is None:
            stopped_by_thread.set()
            terminate(proc)

    def pump_stderr() -> None:
        try:
            with tool_log.open("a", encoding="utf-8") as sink:
                sink.write(f"--- {datetime.now():%Y-%m-%d %H:%M:%S} {label}\n")
                sink.flush()
                for raw in proc.stderr:
                    line = MASKER(raw.decode("utf-8", "replace").rstrip("\n"))
                    tail.append(line)
                    sink.write(line + "\n")
                    sink.flush()
        except BaseException:
            stop_for_thread()
            raise
        finally:
            proc.stderr.close()

    def feed() -> int:
        fed = 0
        try:
            if isinstance(stdin_source, bytes):
                proc.stdin.write(stdin_source)
                fed = len(stdin_source)
            else:
                while True:
                    if proc.poll() is not None or supervisor.stop.is_set():
                        break
                    block = stdin_source.read(READ_BLOCK)
                    if not block:
                        break
                    proc.stdin.write(block)
                    fed += len(block)
        except BrokenPipeError:
            pass  # the tool has read all it needs
        except BaseException:
            if proc.poll() == 0 and not supervisor.stop.is_set():
                return fed  # A reader cancelled after the consumer's successful early exit.
            stop_for_thread()
            raise
        finally:
            with contextlib.suppress(BrokenPipeError):
                proc.stdin.close()
        return fed

    def consume() -> Any:
        def succeeded() -> bool:
            return proc.wait() == 0 and not supervisor.stop.is_set()

        try:
            return stdout_consumer(proc.stdout, succeeded)
        except BaseException:
            stop_for_thread()
            raise
        finally:
            proc.stdout.close()

    threads.append(_Thread(f"{label}-stderr", pump_stderr))
    if stdin_source is not None:
        threads.append(_Thread(f"{label}-stdin", feed))
    if stdout_consumer is not None:
        threads.append(_Thread(f"{label}-stdout", consume))
    for thread in threads:
        thread.start()

    started = time.monotonic()
    last_beat = started
    try:
        while True:
            try:
                proc.wait(timeout=1)
                break
            except subprocess.TimeoutExpired:
                pass
            if supervisor.stop.is_set() or any(thread.error is not None for thread in threads):
                terminate(proc)
                _kill_after_grace(proc)
                break
            now = time.monotonic()
            if heartbeat > 0 and now - last_beat >= heartbeat:
                last_beat = now
                status = f"{label}: running for {duration(now - started)}"
                if progress is not None:
                    status += f", {progress.describe()}"
                log.info(status)
        # A successful pg_restore can stop after just the archive header. Wake
        # a feeder that was reconnecting to S3 instead of waiting out its retries.
        cancel = getattr(stdin_source, "cancel", None)
        if callable(cancel):
            cancel()
        streams_started = time.monotonic()
        for thread in threads:
            while thread.is_alive():
                thread.join(timeout=0.2)
                if (
                    threads[0].is_alive()
                    and time.monotonic() - streams_started >= TERMINATE_GRACE_SECONDS
                ):
                    # A worker can outlive its leader and keep stderr/stdout
                    # open. Reap the entire process group in that case too.
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(proc.pid, signal.SIGKILL)
                if supervisor.stop.is_set() and callable(cancel):
                    cancel()
                now = time.monotonic()
                if heartbeat > 0 and now - last_beat >= heartbeat:
                    last_beat = now
                    log.info(
                        "%s: finishing streams%s",
                        label,
                        f", {progress.describe()}" if progress is not None else "",
                    )
    finally:
        if proc.poll() is None:
            terminate(proc)
            _kill_after_grace(proc)
        supervisor.release(proc)

    supervisor.check()
    errors = [thread.error for thread in threads if thread.error is not None]
    failed = proc.returncode != 0
    # A tool that failed on its own explains more than the stream it broke.
    if failed and check and not stopped_by_thread.is_set():
        raise ToolError(label, proc.returncode, list(tail))
    if errors:
        raise errors[0]
    output = threads[-1].result if stdout_consumer is not None else None
    if failed and check:
        raise ToolError(label, proc.returncode, list(tail))
    return ToolResult(proc.returncode, list(tail), output)


def collect_stdout(stream: BinaryIO, _succeeded: Callable[[], bool]) -> bytes:
    return stream.read()
