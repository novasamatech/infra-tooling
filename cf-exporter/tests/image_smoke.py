# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Container smoke check with the API token sent only over stdin.

Runs the image with its default storage path until every zone has accepted a
recent bucket, then checks readiness, the health metrics, the API counters, the
analytics exposition, delivery and a clean shutdown. Standard library only.
"""

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
from pathlib import Path

BUCKET_SECONDS = 300
# Mirrors the API range checked in script.py; this driver stays stdlib-only.
UINT16_MAX = (1 << 16) - 1
# A zone is collected once its newest accepted bucket ended within this span.
RECENT_SECONDS = 1800
# Nothing older than Cloudflare keeps can be exported.
OLDEST_SECONDS = 32 * 86400
# Mirrors script.METRIC_LABELS, the labels of every analytics family by default.
METRIC_LABELS = {
    "cf_visits_5m": (
        "zone_name",
        "host_name",
        "path",
        "client_country_name",
        "client_request_referer",
        "user_agent_browser",
        "user_agent_os",
    ),
    "cf_requests_5m": (
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
    "cf_cache_requests_5m": (
        "zone_name",
        "host_name",
        "method_name",
        "path",
        "cache_status",
        "origin_response_status",
    ),
    "cf_edge_requests_5m": (
        "zone_name",
        "host_name",
        "method_name",
        "path",
        "edge_response_status",
        "origin_response_status",
    ),
    "cf_zone_requests_5m": ("zone_name",),
    "cf_zone_visits_5m": ("zone_name",),
}
ZONE_TOTALS = ("cf_zone_requests_5m", "cf_zone_visits_5m")
# Families that add up to cf_zone_requests_5m / cf_zone_visits_5m in every bucket.
SUMS = {
    "cf_requests_5m": "cf_zone_requests_5m",
    "cf_cache_requests_5m": "cf_zone_requests_5m",
    "cf_visits_5m": "cf_zone_visits_5m",
}
# Mirror script.APIS, script.API_ERRORS and script.READ_RESULTS.
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
READ_RESULTS = (
    "accepted",
    "accepted_unverified",
    "sampled",
    "inconsistent",
    "recheck_unchanged",
    "recheck_changed",
    "recheck_failed",
    "recheck_skipped",
    "abandoned",
    "expired",
    "error",
)
HEALTH_GAUGES = (
    "cf_exporter_zone_success",
    "cf_exporter_dataset_available",
    "cf_exporter_pending_buckets",
    "cf_exporter_newest_bucket_end_seconds",
    "cf_exporter_oldest_bucket_end_seconds",
    "cf_exporter_undelivered_buckets",
    "cf_exporter_undelivered_samples",
    "cf_exporter_storage_persistent",
    "cf_exporter_storage_full",
    "cf_exporter_loop_timestamp_seconds",
)
HEALTH_COUNTERS = (
    "cf_exporter_bucket_reads_total",
    "cf_exporter_analytics_responses_total",
    "cf_exporter_analytics_samples_total",
    "cf_exporter_storage_resets_total",
)
# Exporter settings forwarded to the container when set. The container port is
# fixed by the port mapping, the storage path is the image's default, and the
# token only travels over stdin.
FORWARDED_SETTINGS = (
    "CF_EXPORTER_REQUEST_TIMEOUT",
    "CF_EXPORTER_LOGLEVEL",
    "CF_EXPORTER_BACKFILL_DAYS",
    "CF_EXPORTER_MAX_RETRY_AFTER_SECONDS",
    "CF_EXPORTER_PAGE_SIZE",
    "CF_EXPORTER_DISABLED_LABELS",
    "CF_EXPORTER_HISTORY_INTERVAL_SECONDS",
    "CF_EXPORTER_RESPONSE_SAMPLES",
)
# The smoke check needs recent buckets only; a set value takes precedence.
DEFAULT_BACKFILL_DAYS = "1"


def _samples(body: str, name: str):
    """(labels, value, timestamp in ms or None) of a family's samples. Unlabelled
    samples are included, so that they fail label checks."""
    pattern = rf"^{re.escape(name)}(?:\{{(.*)\}})? (\S+)(?: (-?\d+))?$"
    for labels, value, timestamp in re.findall(pattern, body, re.MULTILINE):
        yield (
            {
                # Prometheus permits literal tabs and carriage returns in labels.
                key: json.loads(quoted, strict=False)
                for key, quoted in re.findall(r'(\w+)=("(?:\\.|[^"\\])*")', labels)
            },
            float(value),
            int(timestamp) if timestamp else None,
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


def checked_analytics(body: str, expected: dict[str, frozenset[str]], now: float) -> dict[str, int]:
    """Validate an /analytics exposition and return the number of samples per family.

    Raises:
        ValueError: A sample is mislabeled, duplicated, off the bucket grid, out of
            order or range, or a bucket's families do not add up to its zone totals.
    """
    sums: dict[tuple[str, str, int], float] = {}
    counts = {}
    for name, labels in expected.items():
        if f"# TYPE {name} gauge" not in body and re.search(rf"^{name}[{{ ]", body, re.M):
            raise ValueError(f"{name} is not declared a gauge")
        latest: dict[tuple, int] = {}
        counts[name] = 0
        for sample_labels, value, timestamp in _samples(body, name):
            if set(sample_labels) != labels:
                raise ValueError(f"unexpected {name} labels")
            if timestamp is None or timestamp % (BUCKET_SECONDS * 1000):
                raise ValueError(f"{name} sample without a bucket-end timestamp")
            end = timestamp // 1000
            if not now - OLDEST_SECONDS <= end <= now:
                raise ValueError(f"{name} sample outside the possible time range")
            series = tuple(sorted(sample_labels.items()))
            if latest.get(series, -1) >= end:
                raise ValueError(f"duplicate or unordered {name} sample")
            latest[series] = end
            if not math.isfinite(value) or value < 0 or (value == 0 and name not in ZONE_TOTALS):
                raise ValueError(f"invalid {name} value")
            for label in ("edge_response_status", "origin_response_status"):
                status = sample_labels.get(label)
                if name != "cf_edge_requests_5m" or status is None:
                    continue
                if label == "origin_response_status" and status == "unknown":
                    continue
                if not re.fullmatch(r"[0-9]{1,5}", status) or int(status) > UINT16_MAX:
                    raise ValueError("edge sample with an invalid status")
            key = (name, sample_labels["zone_name"], end)
            sums[key] = sums.get(key, 0) + value
            counts[name] += 1
    buckets = {(zone, end) for name, zone, end in sums if name in ZONE_TOTALS}
    for name, zone, end in sums:
        if (zone, end) not in buckets:
            raise ValueError(f"{name} sample of a bucket without zone totals")
    for zone, end in buckets:
        if any((name, zone, end) not in sums for name in ZONE_TOTALS):
            raise ValueError("bucket with only one of its zone totals")
        for name, total in SUMS.items():
            # Disabling labels only merges series: the sums still hold.
            if sums.get((name, zone, end), 0) != sums[(total, zone, end)]:
                raise ValueError(f"{name} does not add up to {total} in a bucket")
        if (
            sums.get(("cf_edge_requests_5m", zone, end), 0)
            > sums[("cf_zone_requests_5m", zone, end)]
        ):
            raise ValueError("more client requests than requests in a bucket")
    return counts


def checked_health(body: str) -> dict[str, dict]:
    """Validate the health metrics and return per-zone success and newest bucket end.

    Raises:
        ValueError: A family is missing or a sample is invalid.
    """
    for name in HEALTH_GAUGES:
        if f"# TYPE {name} gauge" not in body:
            raise ValueError(f"missing {name}")
    for name in HEALTH_COUNTERS:
        if f"# TYPE {name} counter" not in body:
            raise ValueError(f"missing {name}")
    results = {
        labels.get("result"): value for labels, value, _ in _samples(body, HEALTH_COUNTERS[0])
    }
    if set(results) != set(READ_RESULTS):
        raise ValueError("bucket read results differ from the fixed set")
    if any(not math.isfinite(v) or v < 0 or not v.is_integer() for v in results.values()):
        raise ValueError("invalid bucket read counter")
    success = {
        labels["zone_name"]: value
        for labels, value, _ in _samples(body, "cf_exporter_zone_success")
    }
    if any(value not in (0, 1) for value in success.values()):
        raise ValueError("zone success is neither 0 nor 1")
    newest = {
        labels["zone_name"]: value
        for labels, value, _ in _samples(body, "cf_exporter_newest_bucket_end_seconds")
    }
    if any(value % BUCKET_SECONDS for value in newest.values()):
        raise ValueError("newest bucket end off the bucket grid")
    available = {}
    for labels, value, _ in _samples(body, "cf_exporter_dataset_available"):
        if set(labels) != {"zone_name", "dataset"} or value not in (0, 1):
            raise ValueError("invalid dataset availability sample")
        available[(labels["zone_name"], labels["dataset"])] = value
    if any(
        available.get((zone, kind)) != 1
        for zone, _ in available
        for kind in ("visits", "requests", "cache")
    ):
        raise ValueError("an always collected dataset is reported unavailable")
    return {"success": success, "newest": newest, "results": results}


def collected(health: dict, now: float) -> bool:
    """Whether every listed zone succeeded and has a recently ended accepted bucket."""
    success, newest = health["success"], health["newest"]
    return bool(success) and all(
        value == 1 and newest.get(zone, 0) >= now - RECENT_SECONDS
        for zone, value in success.items()
    )


def single_value(body: str, name: str) -> float:
    values = [value for labels, value, _ in _samples(body, name) if not labels]
    if len(values) != 1:
        raise ValueError(f"expected one {name} sample")
    return values[0]


def checked_api_counters(body: str) -> dict[tuple[str, str], float]:
    """Validate the Cloudflare API counters once zones were collected.

    Returns the error counters by (api, error), so callers can report them.

    Raises:
        ValueError: A counter family is missing, a sample is mislabeled, duplicate
            or invalid, or collected zones lack successful API responses.
    """
    for name in ("cf_exporter_api_responses_total", "cf_exporter_api_errors_total"):
        if f"# TYPE {name} counter" not in body:
            raise ValueError(f"missing {name}")
    counters = {}
    for name, label in (("errors", "error"), ("responses", "code")):
        values = counters[name] = {}
        for labels, value, _ in _samples(body, f"cf_exporter_api_{name}_total"):
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
    succeeded = any(value == 1 for _, value, _ in _samples(body, "cf_exporter_zone_success"))
    if responses.get(("rest", "200"), 0) < 1 or (
        succeeded and responses.get(("graphql", "200"), 0) < 1
    ):
        raise ValueError("collected zones without successful API responses")
    return errors


def fetch(port: int, path: str) -> tuple[int, str]:
    """HTTP status and body; an error status is an answer, not a transport error."""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=30) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        with error:
            return error.code, error.read().decode()


def check_container(port: int, labels: dict[str, frozenset[str]]) -> str | None:
    """Check a container whose zones are collected; returns a failure or None."""
    status, body = fetch(port, "/metrics")
    try:
        health = checked_health(body)
        errors = checked_api_counters(body)
        if single_value(body, "cf_exporter_storage_persistent") != 1:
            return "the image's default storage path is not usable"
        failed = {f"{api}/{error}": int(v) for (api, error), v in errors.items() if v}
        print(f"image smoke test: API errors {failed or 'none'}")
        reads = {result: int(value) for result, value in health["results"].items() if value}
        print(f"image smoke test: bucket reads {reads}")
        status, exposition = fetch(port, "/analytics")
        if status != 200:
            return f"/analytics returned {status}"
        counts = checked_analytics(exposition, labels, time.time())
        if not counts["cf_zone_requests_5m"]:
            return "/analytics has no accepted bucket"
        print(f"image smoke test: samples {counts}")
        # Looking at the endpoint consumed nothing; two scrapes are counted.
        before = single_value(body, "cf_exporter_analytics_responses_total")
        for _ in range(2):
            status, consumed = fetch(port, "/analytics?consume=1")
            if status != 200:
                return f"/analytics?consume=1 returned {status}"
            checked_analytics(consumed, labels, time.time())
        after = delivered_responses(port, 2)
        if (before, after) != (0, 2):
            return f"delivery counted {before:g} then {after:g} responses, expected 0 then 2"
    except (KeyError, ValueError) as error:
        return str(error)
    return None


def delivered_responses(port: int, expected: float, timeout: float = 5.0) -> float:
    """The delivery counter, once it reached `expected` or after `timeout` seconds.
    The server counts a response after the client has read all of it, so the
    counter can lag behind the client for a moment."""
    deadline = time.monotonic() + timeout
    while True:
        value = single_value(fetch(port, "/metrics")[1], "cf_exporter_analytics_responses_total")
        if value >= expected or time.monotonic() >= deadline:
            return value
        time.sleep(0.1)


def main() -> int:
    engine, image, port, timeout = sys.argv[1:]
    port = int(port)
    token = os.environ["CF_EXPORTER_API_TOKEN"].strip()
    command = shlex.split(engine)
    labels = expected_labels(os.environ.get("CF_EXPORTER_DISABLED_LABELS", ""))
    # By name only: the container reads each value from this process's environment.
    settings = [
        flag for name in FORWARDED_SETTINGS if name in os.environ for flag in ("--env", name)
    ]
    if "CF_EXPORTER_BACKFILL_DAYS" not in os.environ:
        settings += ["--env", f"CF_EXPORTER_BACKFILL_DAYS={DEFAULT_BACKFILL_DAYS}"]
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

            def failed(reason: str) -> int:
                log.seek(0)
                print(f"image smoke test: FAILED - {reason}; container logs:")
                print(log.read().replace(token, "[REDACTED]"))
                return 1

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
                        if token in metadata.stdout:
                            return failed("token found in container metadata")
                        metadata_checked = (
                            json.loads(metadata.stdout)[0]["State"]["Running"] is True
                        )
                    try:
                        # Metrics first: zones seen there were listed before, so
                        # the readiness read afterwards must already be 200.
                        status, body = fetch(port, "/metrics")
                        ready, _ = fetch(port, "/readyz")
                    except (urllib.error.URLError, ConnectionError):
                        time.sleep(1)
                        continue
                    # Health metrics are served whether ready or not.
                    if ready not in (200, 503) or status != 200:
                        return failed(f"/readyz returned {ready}, /metrics {status}")
                    listed = "cf_exporter_zone_success{" in body
                    if ready == 503 and listed:
                        return failed("/readyz is not ready although zones are listed")
                    if metadata_checked and ready == 200:
                        try:
                            done = collected(checked_health(body), time.time())
                        except (KeyError, ValueError) as error:
                            return failed(str(error))
                        if done:
                            failure = check_container(port, labels)
                            if failure:
                                return failed(failure)
                            subprocess.run(
                                [*command, "stop", "-t", "10", cidfile.read_text().strip()],
                                check=False,
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL,
                                timeout=30,
                            )
                            code = process.wait(timeout=15)
                            if code != 0:
                                return failed(f"the container exited with {code} on SIGTERM")
                            print(f"image smoke test: OK ({image})")
                            return 0
                    time.sleep(1)
                return failed("the zones were not collected in time")
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
