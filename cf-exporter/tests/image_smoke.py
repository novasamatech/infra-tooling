# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Container smoke check with the API token sent only over stdin."""

from __future__ import annotations

import json
import math
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

DAY_SECONDS = 24 * 60 * 60
# Mirrors the API range checked in script.py; this driver stays stdlib-only.
UINT16_MAX = (1 << 16) - 1
WINDOW_TYPE_LINE = "# TYPE cf_exporter_snapshot_window_end_seconds gauge"

DAY_FAMILIES = (
    "cf_exporter_snapshot_window_end_seconds",
    "cf_visits",
    "cf_requests",
    "cf_cache_requests",
    "cf_edge_requests",
    "cf_exporter_dataset_available",
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
    """Validate window samples from one exposition, using only the stdlib.

    Raises:
        ValueError: A sample has invalid values, labels or snapshot consistency.
        KeyError: A successful-zone health sample lacks its zone label.
    """
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
        # The end timestamp may equal next midnight for a full historical day;
        # the API interval itself remains half-open [start, end).
        if not start.timestamp() <= end <= start.timestamp() + DAY_SECONDS or not end.is_integer():
            raise ValueError("snapshot window end is outside its UTC day or has fractional seconds")
        result[labels["zone_name"]] = (labels["window_start"], end)
    if len(set(result.values())) > 1:
        raise ValueError("zones of one snapshot have different windows")
    return result


ALWAYS_COLLECTED = ("visits", "requests", "cache")
DATASET_NAMES = (*ALWAYS_COLLECTED, "edge")
EDGE_LABELS = frozenset(
    {
        "zone_name",
        "host_name",
        "method_name",
        "path",
        "edge_response_status",
        "origin_response_status",
    }
)


def _by_zone_and_dataset(body: str, name: str) -> dict[tuple[str, str], float]:
    result = {}
    for labels, value in _sample_values(body, name):
        key = (labels.get("zone_name"), labels.get("dataset"))
        if set(labels) != {"zone_name", "dataset"} or key in result:
            raise ValueError(f"invalid or duplicate {name} sample")
        result[key] = value
    return result


def checked_edge_family(body: str) -> dict[str, int]:
    """Validate cf_edge_requests and dataset health from one exposition.

    Returns the number of edge samples of every zone that collects the dataset.

    Raises:
        ValueError: Edge samples or dataset health are invalid or inconsistent.
        KeyError: A successful-zone health sample lacks its zone label.
    """
    successful = {
        labels["zone_name"]
        for labels, value in _sample_values(body, "cf_exporter_zone_scrape_success")
        if value == 1
    }
    available = _by_zone_and_dataset(body, "cf_exporter_dataset_available")
    if set(available) != {(zone, kind) for zone in successful for kind in DATASET_NAMES}:
        raise ValueError("dataset availability does not match successful zones")
    if any(value not in (0, 1) for value in available.values()):
        raise ValueError("dataset availability is neither 0 nor 1")
    if any(available[(zone, kind)] != 1 for zone in successful for kind in ALWAYS_COLLECTED):
        raise ValueError("an always collected dataset is reported unavailable")
    groups = _by_zone_and_dataset(body, "cf_exporter_groups")
    limited = _by_zone_and_dataset(body, "cf_exporter_group_limit_reached")
    collected = {key for key, value in available.items() if value == 1}
    if set(groups) != collected or set(limited) != collected:
        raise ValueError("dataset health does not match its availability")
    if any(not math.isfinite(v) or v < 0 or not v.is_integer() for v in groups.values()):
        raise ValueError("invalid dataset group count")
    if any(value not in (0, 1) for value in limited.values()):
        raise ValueError("invalid dataset truncation flag")
    counts = dict.fromkeys(successful, 0)
    seen = set()
    for labels, value in _sample_values(body, "cf_edge_requests"):
        if set(labels) != EDGE_LABELS:
            raise ValueError("unexpected edge labels")
        key = tuple(sorted(labels.items()))
        if key in seen:
            raise ValueError("duplicate edge sample")
        seen.add(key)
        if available.get((labels["zone_name"], "edge")) != 1:
            raise ValueError("edge samples for a zone without the edge dataset")
        for label in ("edge_response_status", "origin_response_status"):
            status = labels[label]
            if label == "origin_response_status" and status == "unknown":
                continue
            if (
                not re.fullmatch(r"[0-9]+", status)
                or len(status) > len(str(UINT16_MAX))
                or int(status) > UINT16_MAX
            ):
                raise ValueError("edge sample with an invalid status")
        if not math.isfinite(value) or value <= 0:
            raise ValueError("edge sample without requests")
        counts[labels["zone_name"]] += 1
    for zone in successful:
        if available[(zone, "edge")] == 1 and groups[(zone, "edge")] != counts[zone]:
            raise ValueError("edge samples do not match cf_exporter_groups")
    return {zone: count for zone, count in counts.items() if available[(zone, "edge")] == 1}


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


def readiness_status(port: int) -> int:
    """HTTP status of /readyz; 503 is an answer, not a transport error."""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=5) as response:
            return response.status
    except urllib.error.HTTPError as error:
        error.close()
        return error.code


def expected_readiness(body: str) -> int:
    """The /readyz status implied by the exposition of a successful cycle.

    It is ready only if at least one zone was collected and the snapshot's
    day-scoped families are visible, i.e. its window belongs to the current day.
    """
    collected = any(
        value == 1 for _, value in _sample_values(body, "cf_exporter_zone_scrape_success")
    )
    return 200 if collected and WINDOW_TYPE_LINE in body else 503


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
                        # Read before the metrics: readiness seen first implies
                        # a published cycle in the later metrics response.
                        ready = readiness_status(port)
                        with urllib.request.urlopen(
                            f"http://127.0.0.1:{port}/metrics", timeout=5
                        ) as response:
                            body = response.read().decode()
                        if ready not in (200, 503) or (
                            ready == 200 and "cf_exporter_last_cycle_success" not in body
                        ):
                            print(f"image smoke test: FAILED - /readyz returned {ready} early")
                            return 1
                        if (
                            metadata_checked
                            and "cf_exporter_last_cycle_success 1.0" in body.splitlines()
                        ):
                            ready, expected = readiness_status(port), expected_readiness(body)
                            if ready != expected:
                                print(
                                    f"image smoke test: FAILED - /readyz returned {ready} "
                                    f"after a successful cycle, expected {expected}"
                                )
                                return 1
                            if WINDOW_TYPE_LINE in body or cycle_started_today(body):
                                for name in DAY_FAMILIES:
                                    if f"# TYPE {name} gauge" not in body:
                                        print(f"image smoke test: FAILED - missing {name}")
                                        return 1
                                try:
                                    checked_snapshot_windows(body)
                                    checked_edge_family(body)
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
