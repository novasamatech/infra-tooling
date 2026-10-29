# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""
Live tests against the real Cloudflare API.

Run with `make test-live`. They need CF_EXPORTER_API_TOKEN in the environment
(permissions: Zone Read + Analytics Read) and are skipped without it. The
token is only ever read from the environment; tests never print it.
"""

import dataclasses
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from image_smoke import (
    WINDOW_TYPE_LINE,
    checked_analytics_labels,
    checked_api_counters,
    checked_edge_family,
    checked_snapshot_windows,
    expected_readiness,
    readiness_status,
)
from profile_live import (
    expected_totals,
    request_sources,
    source_filter_valid,
    source_value_present,
)
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
    inputFields { name type { ...TypeRef } }
  }
}
fragment TypeRef on __Type {
  name
  ofType { name ofType { name ofType { name } } }
}
"""


@pytest.fixture(scope="module")
def config():
    """The configuration the exporter process would use, including its options."""
    return script.parse_env()


@pytest.fixture(scope="module")
def api(config):
    client = script.create_api(config)
    try:
        yield client
    finally:
        client.session.close()


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


DAY_FAMILIES = (
    "cf_exporter_snapshot_window_end_seconds",
    "cf_visits",
    "cf_requests",
    "cf_cache_requests",
    "cf_edge_requests",
    "cf_zone_requests",
    "cf_zone_visits",
    "cf_exporter_groups",
    "cf_exporter_group_limit_reached",
    "cf_exporter_dataset_available",
)


def expected_labels(schema):
    return {name: frozenset(schema.labels(name)) for name in script.METRIC_LABELS}


def assert_day_families(text, cycle_start, schema):
    """Require the day-scoped families unless the UTC day changed after the cycle
    started; the exporter omits them then by design."""
    window_present = WINDOW_TYPE_LINE in text
    if window_present or cycle_start.date() == datetime.now(UTC).date():
        for name in DAY_FAMILIES:
            assert f"# TYPE {name} gauge" in text
        checked_snapshot_windows(text)
        labels = expected_labels(schema)
        checked_edge_family(text, labels["cf_edge_requests"])
        checked_analytics_labels(text, labels)
    else:
        assert all(f"# TYPE {name} gauge" not in text for name in DAY_FAMILIES)


def scraped_cycle_start(body):
    pattern = r"^(cf_exporter_(?:last_cycle_timestamp|cycle_duration)_seconds) (\S+)$"
    values = {name: float(value) for name, value in re.findall(pattern, body, re.MULTILINE)}
    return datetime.fromtimestamp(
        values["cf_exporter_last_cycle_timestamp_seconds"]
        - values["cf_exporter_cycle_duration_seconds"],
        UTC,
    )


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

    # Cloudflare exposes input-field names but not their deprecation status.
    filter_arg = next(a for a in dataset["args"] if a["name"] == "filter")
    filter_fields = introspect(api, type_name(filter_arg["type"]))["inputFields"]
    assert {"datetime_geq", "datetime_lt", "requestSource"} <= {f["name"] for f in filter_fields}
    assert (
        type_name(next(f for f in filter_fields if f["name"] == "requestSource")["type"])
        == "string"
    )

    order_by = next(a for a in dataset["args"] if a["name"] == "orderBy")
    order_values = introspect(api, type_name(order_by["type"]))["enumValues"]
    current(order_values, "count_DESC")
    current(order_values, "sum_visits_DESC")

    groups = introspect(api, type_name(dataset["type"]))["fields"]
    current(groups, "count")
    sums = introspect(api, type_name(current(groups, "sum")["type"]))["fields"]
    current(sums, "visits")
    dimensions = introspect(api, type_name(current(groups, "dimensions")["type"]))["fields"]
    for dimension in {d for mapping in script.DATASETS.values() for d in mapping.values()}:
        current(dimensions, dimension)
    # The eyeball filter compares against this dimension's values.
    assert type_name(current(dimensions, "requestSource")["type"]) == "string"
    for status in ("edgeResponseStatus", "originResponseStatus"):
        assert type_name(current(dimensions, status)["type"]) == "uint16"


def completed_day():
    # Use a completed day for exact sums: fresh analytics can still be ingested
    # while separate slice requests are being served.
    until = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    return until - timedelta(days=1), until


def assert_sums_match_totals(api, zone, result, since, until):
    """Every complete dataset sums to its zone total; edge to the eyeball total.

    Returns the zone's request total and raw sources for the cross-zone check, if
    the zone supports the edge dataset.
    """
    eyeball, checked = 0, None
    if result.available["edge"]:
        eyeball, sources = request_sources(api.graphql, zone["id"], since, until)
        # Validate against independently grouped sources, including worker-only zones.
        assert source_filter_valid(eyeball, sources, result.total_requests, completed_day=True), (
            zone["name"],
            eyeball,
            sources,
        )
        checked = result.total_requests, sources
    else:
        assert not result.edge
    # Edge groups are compared with the eyeball total, never the unfiltered one.
    expected = expected_totals(result.total_visits, result.total_requests, eyeball)
    for kind in script.DATASETS:
        if result.available[kind] and not result.limited[kind]:
            actual = sum(getattr(result, kind).values())
            assert actual == expected[kind], (zone["name"], kind, actual, expected[kind])
    return checked


def test_every_zone_is_collected(api, zones):
    since, until = completed_day()
    failures = {}
    checked = []
    for zone in zones:
        try:
            result = script.collect_zone(api, zone["id"], zone["name"], since, until)
        except script.CloudflareAPIError as e:
            failures[zone["name"]] = str(e)
            continue
        for kind in script.DATASETS:
            label_names = api.schema.labels(script.METRICS[kind])
            for labels, value in getattr(result, kind).items():
                assert len(labels) == len(label_names)
                assert value > 0
        if source := assert_sums_match_totals(api, zone, result, since, until):
            checked.append(source)
    assert not failures
    # A renamed filter value would leave every zone without client requests.
    assert source_value_present(checked), checked


def test_disabled_labels_are_aggregated_by_the_api(config, zones):
    """Disabled dimensions are left out of the queries, including an aggregate-only
    selection and an edge selection without its status; sums stay exact."""
    schema = script.Schema(
        {
            "cf_requests": frozenset({"query", "client_request_referer"}),
            "cf_cache_requests": frozenset(script.CACHE_LABELS),
            "cf_edge_requests": frozenset({"edge_response_status"}),
        }
    )
    api = script.create_api(dataclasses.replace(config, schema=schema))
    since, until = completed_day()
    zone = next(
        (
            zone
            for zone in zones
            if "dimensions_clientRequestQuery" in script.fetch_available_fields(api, zone["id"])
        ),
        zones[0],
    )
    result = script.collect_zone(api, zone["id"], zone["name"], since, until)
    assert len(result.cache) <= 1
    assert all(labels == () for labels in result.cache)
    assert all(len(labels) == len(schema.labels("cf_requests")) for labels in result.requests)
    assert all(len(labels) == len(schema.labels("cf_edge_requests")) for labels in result.edge)
    assert_sums_match_totals(api, zone, result, since, until)


def test_dimensionless_page_size_one_is_complete(config, zones):
    """An aggregate-only response cannot hide a second grouping key."""
    schema = script.Schema(
        {name: frozenset(labels) for name, labels in script.METRIC_LABELS.items()}
    )
    api = script.create_api(dataclasses.replace(config, schema=schema, page_size=1))
    zone = zones[0]
    since, until = completed_day()
    try:
        result = script.collect_zone(api, zone["id"], zone["name"], since, until)
        assert not any(result.limited.values())
        for kind in script.DATASETS:
            assert len(getattr(result, kind)) <= 1
            assert all(labels == () for labels in getattr(result, kind))
        assert_sums_match_totals(api, zone, result, since, until)
    finally:
        api.session.close()


def test_free_plan_zones_are_collected(api, zones):
    """Regression: Free zones lack some dimensions and used to export nothing."""
    free = [z for z in zones if (z.get("plan") or {}).get("name", "").lower().startswith("free")]
    if not free:
        pytest.skip("the API token sees no Free-plan zone")
    since, until = script.day_window(datetime.now(UTC))
    for zone in free:
        fields = script.fetch_available_fields(api, zone["id"])
        result = script.collect_zone(api, zone["id"], zone["name"], since, until)
        labels = api.schema.labels("cf_requests")
        if "dimensions_clientRequestQuery" not in fields and "query" in labels:
            query_index = labels.index("query")
            assert all(values[query_index] == "unknown" for values in result.requests)


def test_full_cycle_renders_metrics(api, zones, registry):
    snapshot = script.collect_once(api)
    assert snapshot.zone_success == {z["name"]: 1 for z in zones}
    assert snapshot.cycle_success == 1
    collector = script.CloudflareCollector()
    registry.register(collector)
    registry.register(api.stats)
    collector.update(snapshot)
    assert registry.get_sample_value("cf_exporter_last_cycle_success") == 1
    text = generate_latest(registry).decode()
    checked_api_counters(text)
    assert_day_families(text, snapshot.window_start, api.schema)
    if WINDOW_TYPE_LINE in text:
        assert checked_snapshot_windows(text) == {
            zone["name"]: (script.rfc3339(snapshot.window_start), snapshot.window_end.timestamp())
            for zone in zones
        }


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


def readiness(port):
    try:
        return readiness_status(port)
    except (urllib.error.URLError, ConnectionError):
        return None


def test_exporter_process_end_to_end(zones, config):
    """Start script.py as a real process, check readiness and metrics, stop it with SIGTERM."""
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
            # Readiness first: a ready answer implies a cycle in the later scrape.
            ready, body = readiness(port), scrape(port)
            if body and "cf_exporter_last_cycle_success" in body:
                break
            assert ready != 200, "ready before the first collection cycle"
            time.sleep(1)
        assert body and "cf_exporter_last_cycle_success 1.0" in body
        for zone in zones:
            assert f'cf_exporter_zone_scrape_success{{zone_name="{zone["name"]}"}} 1.0' in body
        assert_day_families(body, scraped_cycle_start(body), config.schema)
        checked_api_counters(body)
        assert readiness(port) == expected_readiness(body)

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
