# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

import io
import threading

import pytest

from sqdb.log import MASKER
from sqdb.proc import Interrupted, Supervisor, ToolError, collect_stdout, run_tool


def run(argv, tmp_path, log, **kwargs):
    kwargs.setdefault("supervisor", Supervisor())
    return run_tool(
        argv,
        label="tool",
        env={"PATH": "/usr/bin:/bin"},
        log=log,
        tool_log=tmp_path / "tool.log",
        heartbeat=0,
        **kwargs,
    )


def test_stdout_is_collected_and_stderr_logged_masked(tmp_path, log):
    MASKER.update({"s3cr3t"})
    result = run(
        ["sh", "-c", "echo out; echo 'note s3cr3t' >&2"],
        tmp_path,
        log,
        stdout_consumer=collect_stdout,
    )
    assert result.returncode == 0 and result.output == b"out\n"
    text = (tmp_path / "tool.log").read_text()
    assert "note ***" in text and "s3cr3t" not in text


def test_failure_keeps_error_lines(tmp_path, log):
    script = (
        "for i in 1 2 3 4 5 6 7; do echo progress $i >&2; done; "
        "echo 'tool: error: boom' >&2; echo 'tool: detail: why' >&2; exit 3"
    )
    with pytest.raises(ToolError) as caught:
        run(["sh", "-c", script], tmp_path, log)
    assert caught.value.returncode == 3
    assert caught.value.reason == ["tool: error: boom", "tool: detail: why"]
    assert caught.value.summary == "tool: error: boom"
    result = run(["sh", "-c", "exit 4"], tmp_path, log, check=False)
    assert result.returncode == 4


def test_tool_may_stop_reading_stdin_early(tmp_path, log):
    source = io.BytesIO(b"x" * (8 * 1024 * 1024))
    result = run(
        ["head", "-c", "10"], tmp_path, log, stdin_source=source, stdout_consumer=collect_stdout
    )
    assert result.output == b"x" * 10
    result = run(["cat"], tmp_path, log, stdin_source=b"bytes", stdout_consumer=collect_stdout)
    assert result.output == b"bytes"


def test_stream_failure_stops_the_tool_and_is_reported(tmp_path, log):
    class Broken(io.RawIOBase):
        def read(self, size=-1):
            raise OSError("storage went away")

    with pytest.raises(OSError, match="storage went away"):
        run(["sleep", "30"], tmp_path, log, stdin_source=Broken())

    def consumer(stream, succeeded):
        stream.read(1)
        raise RuntimeError("upload failed")

    with pytest.raises(RuntimeError, match="upload failed"):
        run(["sh", "-c", "echo data; sleep 30"], tmp_path, log, stdout_consumer=consumer)


def test_consumer_sees_whether_the_tool_succeeded(tmp_path, log):
    seen = {}

    def consumer(stream, succeeded):
        stream.read()
        seen["ok"] = succeeded()

    with pytest.raises(ToolError):
        run(["sh", "-c", "echo partial; exit 1"], tmp_path, log, stdout_consumer=consumer)
    assert seen == {"ok": False}


def test_stop_request_interrupts_running_tools(tmp_path, log):
    supervisor = Supervisor()
    timer = threading.Timer(0.5, supervisor.request_stop)
    timer.start()
    with pytest.raises(Interrupted):
        run(["sleep", "30"], tmp_path, log, supervisor=supervisor)
    with pytest.raises(Interrupted):
        run(["true"], tmp_path, log, supervisor=supervisor)


def test_stderr_log_failure_stops_the_process(tmp_path, log):
    (tmp_path / "tool.log").mkdir()
    with pytest.raises(IsADirectoryError):
        run(["sh", "-c", "while true; do echo data >&2; done"], tmp_path, log)


def test_stream_failure_escalates_when_tool_ignores_sigterm(tmp_path, log, monkeypatch):
    monkeypatch.setattr("sqdb.proc.TERMINATE_GRACE_SECONDS", 0.1)

    def consumer(stream, _succeeded):
        stream.read(1)
        raise OSError("stream failed")

    with pytest.raises(OSError, match="stream failed"):
        run(
            ["sh", "-c", 'trap "" TERM; echo x; exec sleep 30'],
            tmp_path,
            log,
            stdout_consumer=consumer,
        )


def test_exited_leader_cannot_leave_workers_holding_the_pipes(tmp_path, log, monkeypatch):
    monkeypatch.setattr("sqdb.proc.TERMINATE_GRACE_SECONDS", 0.1)
    run(
        ["sh", "-c", 'trap "" TERM; sleep 30 & exit 0'],
        tmp_path,
        log,
        stdout_consumer=collect_stdout,
    )
