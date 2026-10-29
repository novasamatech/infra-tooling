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
                # Prometheus permits literal tabs and carriage returns in labels.
                key: json.loads(quoted, strict=False)
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
# Mirrors script.METRIC_LABELS, the labels of every analytics family by default.
METRIC_LABELS = {
    "cf_visits": (
        "zone_name",
        "host_name",
        "path",
        "client_country_name",
        "client_request_referer",
        "user_agent_browser",
        "user_agent_os",
    ),
    "cf_requests": (
        "zone_name",
        "host_name",
        "method_name",
        "path",
        "query",
        "client_country_name",
        "client_request_referer",
        "user_agent_browser",
        "user_agent_os",
        "cache_status",
        "origin_response_status",
    ),
    "cf_cache_requests": (
        "zone_name",
        "host_name",
        "method_name",
        "path",
        "cache_status",
        "origin_response_status",
    ),
    "cf_edge_requests": (
        "zone_name",
        "host_name",
        "method_name",
        "path",
        "edge_response_status",
        "origin_response_status",
    ),
    "cf_zone_requests": ("zone_name",),
    "cf_zone_visits": ("zone_name",),
}
EDGE_LABELS = frozenset(METRIC_LABELS["cf_edge_requests"])
# Exporter settings forwarded to the container when set. The container port is
# fixed by the port mapping, and the token only travels over stdin.
FORWARDED_SETTINGS = (
    "CF_EXPORTER_REQUEST_TIMEOUT",
    "CF_EXPORTER_SCRAPE_INTERVAL",
    "CF_EXPORTER_LOGLEVEL",
    "CF_EXPORTER_GROUP_LIMIT",
    "CF_EXPORTER_MAX_SLICE_REQUESTS",
    "CF_EXPORTER_MAX_EXTRA_SELECTIONS",
    "CF_EXPORTER_MIN_SLICE_SECONDS",
    "CF_EXPORTER_MAX_RETRY_AFTER_SECONDS",
    "CF_EXPORTER_PAGE_SIZE",
    "CF_EXPORTER_DISABLED_LABELS",
)


def expected_labels(disabled: str = "") -> dict[str, frozenset[str]]:
    """Labels of every analytics family for a CF_EXPORTER_DISABLED_LABELS value.

    The exporter validates the value; this only applies it.
    """
    removed = json.loads(disabled) if disabled.strip() else {}
    return {
        name: frozenset(labels) - frozenset(removed.get(name, ()))
        for name, labels in METRIC_LABELS.items()
    }


def checked_analytics_labels(body: str, expected: dict[str, frozenset[str]]) -> dict[str, int]:
    """Validate labels, values and sample counts against the snapshot's health.

    Returns the number of samples per family.

    Raises:
        ValueError: A sample is invalid, duplicated, missing or inconsistent with health.
    """
    groups = _by_zone_and_dataset(body, "cf_exporter_groups")
    if any(not math.isfinite(v) or v < 0 or not v.is_integer() for v in groups.values()):
        raise ValueError("invalid dataset group count")
    successful = {
        labels["zone_name"]
        for labels, value in _sample_values(body, "cf_exporter_zone_scrape_success")
        if value == 1
    }
    datasets = {
        "cf_visits": "visits",
        "cf_requests": "requests",
        "cf_cache_requests": "cache",
        "cf_edge_requests": "edge",
    }
    counts = {}
    for name, labels in expected.items():
        seen = set()
        by_zone = {}
        for sample_labels, value in _sample_values(body, name):
            if set(sample_labels) != labels:
                raise ValueError(f"unexpected {name} labels")
            key = tuple(sorted(sample_labels.items()))
            if key in seen:
                raise ValueError(f"duplicate {name} sample")
            seen.add(key)
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"invalid {name} value")
            zone = sample_labels.get("zone_name")
            by_zone[zone] = by_zone.get(zone, 0) + 1
        if name in datasets:
            wanted = {
                zone: int(value) for (zone, kind), value in groups.items() if kind == datasets[name]
            }
        else:
            wanted = dict.fromkeys(successful, 1)
        if "zone_name" in labels:
            if set(by_zone) - set(wanted) or any(by_zone.get(z, 0) != n for z, n in wanted.items()):
                raise ValueError(f"{name} samples do not match snapshot health")
        elif not max(wanted.values(), default=0) <= len(seen) <= sum(wanted.values()):
            raise ValueError(f"{name} samples do not match snapshot health")
        counts[name] = len(seen)
    return counts


def _by_zone_and_dataset(body: str, name: str) -> dict[tuple[str, str], float]:
    result = {}
    for labels, value in _sample_values(body, name):
        key = (labels.get("zone_name"), labels.get("dataset"))
        if set(labels) != {"zone_name", "dataset"} or key in result:
            raise ValueError(f"invalid or duplicate {name} sample")
        result[key] = value
    return result


def checked_edge_family(body: str, labels: frozenset[str] = EDGE_LABELS) -> dict[str, int]:
    """Validate cf_edge_requests, exported with `labels`, and dataset health from one
    exposition.

    Returns the number of edge label sets of every zone that collects the dataset:
    its samples, or without zone_name its cf_exporter_groups value.

    Raises:
        ValueError: Edge samples or dataset health are invalid or inconsistent.
        KeyError: A successful-zone health sample lacks its zone label.
    """
    successful = {
        health["zone_name"]
        for health, value in _sample_values(body, "cf_exporter_zone_scrape_success")
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
    edge_zones = {zone for zone in successful if available[(zone, "edge")] == 1}
    counts = dict.fromkeys(successful, 0)
    seen = set()
    for sample_labels, value in _sample_values(body, "cf_edge_requests"):
        if set(sample_labels) != labels:
            raise ValueError("unexpected edge labels")
        key = tuple(sorted(sample_labels.items()))
        if key in seen:
            raise ValueError("duplicate edge sample")
        seen.add(key)
        zone = sample_labels.get("zone_name")
        if (zone is None and not edge_zones) or (zone is not None and zone not in edge_zones):
            raise ValueError("edge samples for a zone without the edge dataset")
        for label in ("edge_response_status", "origin_response_status"):
            status = sample_labels.get(label)
            if status is None or (label == "origin_response_status" and status == "unknown"):
                continue
            if (
                not re.fullmatch(r"[0-9]+", status)
                or len(status) > len(str(UINT16_MAX))
                or int(status) > UINT16_MAX
            ):
                raise ValueError("edge sample with an invalid status")
        if not math.isfinite(value) or value <= 0:
            raise ValueError("edge sample without requests")
        if zone is not None:
            counts[zone] += 1
    per_zone = {zone: int(groups[(zone, "edge")]) for zone in edge_zones}
    if "zone_name" in labels:
        if per_zone != {zone: counts[zone] for zone in edge_zones}:
            raise ValueError("edge samples do not match cf_exporter_groups")
    # Zones combine identical label sets: at least the largest zone's, at most all.
    elif not max(per_zone.values(), default=0) <= len(seen) <= sum(per_zone.values()):
        raise ValueError("edge samples do not match cf_exporter_groups")
    return per_zone


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


# Mirror script.APIS and script.API_ERRORS; this driver stays stdlib-only.
API_NAMES = ("rest", "graphql")
API_ERRORS = (
    "rate_limited",
    "budget_depleted",
    "server_error",
    "client_error",
    "timeout",
    "connection",
    "invalid_response",
    "graphql",
)


def checked_api_counters(body: str) -> dict[tuple[str, str], float]:
    """Validate the Cloudflare API counters after a successful cycle.

    Returns the error counters by (api, error), so callers can report them.

    Raises:
        ValueError: A counter family is missing, a sample is mislabeled, duplicate
            or invalid, or a successful cycle lacks successful API responses.
    """
    for name in ("cf_exporter_api_responses_total", "cf_exporter_api_errors_total"):
        if f"# TYPE {name} counter" not in body:
            raise ValueError(f"missing {name}")
    counters = {}
    for name, label in (("errors", "error"), ("responses", "code")):
        values = counters[name] = {}
        for labels, value in _sample_values(body, f"cf_exporter_api_{name}_total"):
            key = (labels.get("api"), labels.get(label))
            if set(labels) != {"api", label} or key[0] not in API_NAMES or key in values:
                raise ValueError(f"invalid or duplicate API {name} sample")
            if not math.isfinite(value) or value < 0 or not value.is_integer():
                raise ValueError(f"invalid API {name} counter value")
            values[key] = value
    errors, responses = counters["errors"], counters["responses"]
    if set(errors) != {(api, error) for api in API_NAMES for error in API_ERRORS}:
        raise ValueError("API error kinds differ from the fixed set")
    if any(not re.fullmatch(r"[1-5][0-9]{2}", code) for _, code in responses):
        raise ValueError("API response with an invalid status code")
    collected = any(
        value == 1 for _, value in _sample_values(body, "cf_exporter_zone_scrape_success")
    )
    if responses.get(("rest", "200"), 0) < 1 or (
        collected and responses.get(("graphql", "200"), 0) < 1
    ):
        raise ValueError("successful cycle without successful API responses")
    return errors


def readiness_status(port: int) -> int:
    """HTTP status of /readyz; 503 is an answer, not a transport error."""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=5) as response:
            return response.status
    except urllib.error.HTTPError as error:
        error.close()
        return error.code


def expected_readiness(body: str) -> int:
    """The /readyz status implied by the exposition of a fresh successful cycle.

    It is ready if at least one zone was collected, also after UTC midnight,
    when the snapshot's day-scoped families are already omitted.
    """
    collected = any(
        value == 1 for _, value in _sample_values(body, "cf_exporter_zone_scrape_success")
    )
    return 200 if collected else 503


def main() -> int:
    engine, image, port, timeout = sys.argv[1:]
    token = os.environ["CF_EXPORTER_API_TOKEN"].strip()
    command = shlex.split(engine)
    labels = expected_labels(os.environ.get("CF_EXPORTER_DISABLED_LABELS", ""))
    # By name only: the container reads each value from this process's environment.
    settings = [
        flag for name in FORWARDED_SETTINGS if name in os.environ for flag in ("--env", name)
    ]
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
                    *settings,
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
                            try:
                                errors = checked_api_counters(body)
                            except ValueError as error:
                                print(f"image smoke test: FAILED - {error}")
                                return 1
                            failed = {f"{a}/{e}": int(v) for (a, e), v in errors.items() if v}
                            print(f"image smoke test: API errors {failed or 'none'}")
                            if WINDOW_TYPE_LINE in body or cycle_started_today(body):
                                for name in DAY_FAMILIES:
                                    if f"# TYPE {name} gauge" not in body:
                                        print(f"image smoke test: FAILED - missing {name}")
                                        return 1
                                try:
                                    checked_snapshot_windows(body)
                                    checked_edge_family(body, labels["cf_edge_requests"])
                                    series = checked_analytics_labels(body, labels)
                                except (KeyError, ValueError) as error:
                                    print(f"image smoke test: FAILED - {error}")
                                    return 1
                                print(f"image smoke test: series {series}")
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
