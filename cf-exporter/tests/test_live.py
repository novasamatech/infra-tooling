# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""
Live tests against the real Cloudflare API.

Run with `make test-live`. They need CF_EXPORTER_API_TOKEN in the environment
(permissions: Zone Read + Analytics Read) and are skipped without it. The
token is only ever read from the environment; tests never print it.
"""

import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import pytest
from prometheus_client import generate_latest

import script

API_TOKEN = os.environ.get("CF_EXPORTER_API_TOKEN", "").strip()
SCRIPT = Path(__file__).resolve().parent.parent / "script.py"

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(not API_TOKEN, reason="CF_EXPORTER_API_TOKEN is not set"),
]

TYPE_QUERY = """
query Type($name: String!) {
  __type(name: $name) {
    fields(includeDeprecated: true) {
      name
      isDeprecated
      deprecationReason
      type { ...TypeRef }
      args { name type { ...TypeRef } }
    }
    enumValues(includeDeprecated: true) { name isDeprecated deprecationReason }
  }
}
fragment TypeRef on __Type {
  name
  ofType { name ofType { name ofType { name } } }
}
"""


@pytest.fixture(scope="module")
def api():
    return script.CloudflareAPI(API_TOKEN, request_timeout=60)


@pytest.fixture(scope="module")
def zones(api):
    zones = api.list_zones()
    assert zones, "the API token must have access to at least one zone"
    return zones


def introspect(api, type_name):
    data = api.graphql(TYPE_QUERY, {"name": type_name})
    assert data["__type"], f"GraphQL type {type_name} does not exist"
    return data["__type"]


def type_name(type_ref):
    while type_ref.get("ofType"):
        type_ref = type_ref["ofType"]
    return type_ref["name"]


def current(fields, name):
    """Return the named field/enum value, asserting it exists and is not deprecated."""
    by_name = {f["name"]: f for f in fields}
    assert name in by_name, f"{name} no longer exists in the schema"
    entry = by_name[name]
    assert not entry["isDeprecated"], f"{name} is deprecated: {entry['deprecationReason']}"
    return entry


def test_api_token_is_active(api):
    assert api._request("GET", "/user/tokens/verify")["result"]["status"] == "active"


def test_list_zones_returns_ids_and_names(zones):
    for zone in zones:
        assert len(zone["id"]) == 32
        assert zone["name"]


def test_queried_graphql_schema_is_current(api):
    """The dataset, its fields, dimensions and orderBy values must not be deprecated."""
    zone_fields = introspect(api, "zone")["fields"]
    dataset = current(zone_fields, "httpRequestsAdaptiveGroups")
    settings = introspect(api, type_name(current(zone_fields, "settings")["type"]))["fields"]
    dataset_settings = current(settings, "httpRequestsAdaptiveGroups")
    settings_fields = introspect(api, type_name(dataset_settings["type"]))["fields"]
    for setting in ("enabled", "availableFields", "maxPageSize"):
        current(settings_fields, setting)
    for arg in ("limit", "filter", "orderBy"):
        assert arg in {a["name"] for a in dataset["args"]}

    order_by = next(a for a in dataset["args"] if a["name"] == "orderBy")
    order_values = introspect(api, type_name(order_by["type"]))["enumValues"]
    current(order_values, "count_DESC")
    current(order_values, "sum_visits_DESC")

    groups = introspect(api, type_name(dataset["type"]))["fields"]
    current(groups, "count")
    sums = introspect(api, type_name(current(groups, "sum")["type"]))["fields"]
    current(sums, "visits")
    dimensions = introspect(api, type_name(current(groups, "dimensions")["type"]))["fields"]
    for dimension in {*script.VISITS_DIMENSIONS.values(), *script.REQUESTS_DIMENSIONS.values()}:
        current(dimensions, dimension)


def test_every_zone_is_collected(api, zones):
    since, until = script.day_window(datetime.now(UTC))
    failures = {}
    for zone in zones:
        try:
            result = script.collect_zone(api, zone["id"], zone["name"], since, until)
        except script.CloudflareAPIError as e:
            failures[zone["name"]] = str(e)
            continue
        for labels, value in result.visits.items():
            assert len(labels) == len(script.VISITS_LABELS)
            assert value > 0
        for labels, value in result.requests.items():
            assert len(labels) == len(script.REQUESTS_LABELS)
            assert value > 0
    assert not failures


def test_free_plan_zones_are_collected(api, zones):
    """Regression: Free zones lack some dimensions and used to export nothing."""
    free = [z for z in zones if (z.get("plan") or {}).get("name", "").lower().startswith("free")]
    if not free:
        pytest.skip("the API token sees no Free-plan zone")
    since, until = script.day_window(datetime.now(UTC))
    for zone in free:
        fields = script.fetch_available_fields(api, zone["id"])
        result = script.collect_zone(api, zone["id"], zone["name"], since, until)
        if "dimensions_clientRequestQuery" not in fields:
            query_index = script.REQUESTS_LABELS.index("query")
            assert all(labels[query_index] == "unknown" for labels in result.requests)


def test_full_cycle_renders_metrics(api, zones, registry):
    snapshot = script.collect_once(api)
    assert snapshot.zone_success == {z["name"]: 1 for z in zones}
    assert snapshot.cycle_success == 1
    collector = script.CloudflareCollector()
    registry.register(collector)
    collector.update(snapshot)
    assert registry.get_sample_value("cf_exporter_last_cycle_success") == 1
    text = generate_latest(registry).decode()
    assert "# TYPE cf_requests gauge" in text
    assert "# TYPE cf_visits gauge" in text


def test_invalid_api_token_is_reported_not_fatal():
    bad = script.CloudflareAPI("not-a-valid-cloudflare-credential", request_timeout=60)
    with pytest.raises(script.CloudflareAPIError, match=r"GET /zones: HTTP 4\d\d: "):
        bad.list_zones()
    snapshot = script.collect_once(bad)
    assert snapshot.cycle_success == 0
    assert snapshot.zone_success == {}


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def scrape(port):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
            return response.read().decode()
    except (urllib.error.URLError, ConnectionError):
        return None


def test_exporter_process_end_to_end(zones):
    """Start script.py as a real process, scrape it, stop it with SIGTERM."""
    port = free_port()
    env = {
        **os.environ,
        "CF_EXPORTER_METRICS_PORT": str(port),
        "CF_EXPORTER_SCRAPE_INTERVAL": "3600",
        "CF_EXPORTER_LOGLEVEL": "DEBUG",
        "PYTHONUNBUFFERED": "1",
    }
    process = subprocess.Popen(
        [sys.executable, str(SCRIPT)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    lines = []

    def read_logs():
        lines.extend(process.stdout)

    reader = threading.Thread(target=read_logs, daemon=True)
    reader.start()
    try:
        body = None
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            assert process.poll() is None, "exporter exited before completing a cycle"
            body = scrape(port)
            if body and "cf_exporter_last_cycle_success" in body:
                break
            time.sleep(1)
        assert body and "cf_exporter_last_cycle_success 1.0" in body
        for zone in zones:
            assert f'cf_exporter_zone_scrape_success{{zone_name="{zone["name"]}"}} 1.0' in body
        assert "# TYPE cf_requests gauge" in body

        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=15) == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        reader.join(timeout=10)
        process.stdout.close()

    logs = "".join(lines)
    assert "Signal SIGTERM received, shutting down." in logs
    # Evaluate first, so a failure can never echo the token in the report.
    leaked = API_TOKEN in logs
    assert not leaked, "the API token appeared in the exporter logs"
