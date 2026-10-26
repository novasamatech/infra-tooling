# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Container smoke check with the API token sent only over stdin."""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

DAY_FAMILIES = (
    "cf_exporter_snapshot_window_end_seconds",
    "cf_cache_requests",
    "cf_zone_requests",
    "cf_zone_visits",
    "cf_exporter_groups",
    "cf_exporter_group_limit_reached",
)


def _sample_values(body: str, name: str):
    # Unlabelled samples are included, so that they fail the label check.
    pattern = rf"^{re.escape(name)}(?:\{{(.*)\}})? (\S+)$"
    for labels, value in re.findall(pattern, body, re.MULTILINE):
        yield (
            {
                key: json.loads(quoted)
                for key, quoted in re.findall(r'(\w+)=("(?:\\.|[^"\\])*")', labels)
            },
            float(value),
        )


def checked_snapshot_windows(body: str) -> dict[str, tuple[str, float]]:
    """Validate window samples from one exposition, using only the stdlib."""
    expected = {
        labels["zone_name"]
        for labels, value in _sample_values(body, "cf_exporter_zone_scrape_success")
        if value == 1
    }
    windows = list(_sample_values(body, "cf_exporter_snapshot_window_end_seconds"))
    if (
        len(windows) != len(expected)
        or {labels.get("zone_name") for labels, _ in windows} != expected
    ):
        raise ValueError("snapshot windows do not match successful zones")
    result = {}
    for labels, end in windows:
        if set(labels) != {"zone_name", "window_start"}:
            raise ValueError("unexpected snapshot window labels")
        start = datetime.strptime(labels["window_start"], "%Y-%m-%dT%H:%M:%S%z")
        if labels["window_start"] != start.strftime("%Y-%m-%dT00:00:00Z"):
            raise ValueError("snapshot window start is not midnight UTC")
        if not start.timestamp() <= end <= start.timestamp() + 86400 or not end.is_integer():
            raise ValueError("snapshot window end is outside its UTC day or has fractional seconds")
        result[labels["zone_name"]] = (labels["window_start"], end)
    if len(set(result.values())) > 1:
        raise ValueError("zones of one snapshot have different windows")
    return result


def cycle_started_today(body: str) -> bool:
    """Whether the last cycle started on the current UTC day.

    A cycle that started before midnight publishes no day-scoped families once
    the day has changed, so the check must not expect them.
    """
    pattern = r"^(cf_exporter_(?:last_cycle_timestamp|cycle_duration)_seconds) (\S+)$"
    values = dict(re.findall(pattern, body, re.MULTILINE))
    started = float(values["cf_exporter_last_cycle_timestamp_seconds"]) - float(
        values["cf_exporter_cycle_duration_seconds"]
    )
    return time.gmtime(started)[:3] == time.gmtime()[:3]


def main() -> int:
    engine, image, port, timeout = sys.argv[1:]
    token = os.environ["CF_EXPORTER_API_TOKEN"].strip()
    command = shlex.split(engine)
    # Temporary files contain only the container ID and redacted process logs.
    with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent.parent) as directory:
        cidfile = Path(directory) / "container.id"
        with tempfile.TemporaryFile(mode="w+") as log:
            process = subprocess.Popen(
                [
                    *command,
                    "run",
                    "--rm",
                    "-i",
                    "--cidfile",
                    str(cidfile),
                    "-p",
                    f"127.0.0.1:{port}:8000",
                    image,
                    "/bin/sh",
                    "-c",
                    "IFS= read -r CF_EXPORTER_API_TOKEN; "
                    "export CF_EXPORTER_API_TOKEN; exec python script.py",
                ],
                stdin=subprocess.PIPE,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
            )
            try:
                process.stdin.write(token + "\n")
                process.stdin.close()
                metadata_checked = False
                deadline = time.monotonic() + float(timeout)
                while process.poll() is None and time.monotonic() < deadline:
                    if not metadata_checked and cidfile.exists():
                        metadata = subprocess.run(
                            [*command, "inspect", cidfile.read_text().strip()],
                            check=True,
                            capture_output=True,
                            text=True,
                            timeout=10,
                        )
                        leaked = token in metadata.stdout
                        if leaked:
                            print("image smoke test: FAILED - token found in container metadata")
                            return 1
                        metadata_checked = (
                            json.loads(metadata.stdout)[0]["State"]["Running"] is True
                        )
                    try:
                        with urllib.request.urlopen(
                            f"http://127.0.0.1:{port}/metrics", timeout=5
                        ) as response:
                            body = response.read().decode()
                        if (
                            metadata_checked
                            and "cf_exporter_last_cycle_success 1.0" in body.splitlines()
                        ):
                            if (
                                "# TYPE cf_exporter_snapshot_window_end_seconds gauge" in body
                                or cycle_started_today(body)
                            ):
                                for name in DAY_FAMILIES:
                                    if f"# TYPE {name} gauge" not in body:
                                        print(f"image smoke test: FAILED - missing {name}")
                                        return 1
                                try:
                                    checked_snapshot_windows(body)
                                except (KeyError, ValueError) as error:
                                    print(f"image smoke test: FAILED - {error}")
                                    return 1
                            else:
                                print(
                                    "image smoke test: cycle crossed UTC midnight, "
                                    "day-scoped families are omitted by design"
                                )
                            print(f"image smoke test: OK ({image})")
                            return 0
                    except (urllib.error.URLError, ConnectionError):
                        pass
                    time.sleep(1)
                log.seek(0)
                print("image smoke test: FAILED - container logs:")
                print(log.read().replace(token, "[REDACTED]"))
                return 1
            finally:
                if process.poll() is None and cidfile.exists():
                    subprocess.run(
                        [*command, "stop", "-t", "10", cidfile.read_text().strip()],
                        check=False,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        timeout=30,
                    )
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    sys.exit(main())
