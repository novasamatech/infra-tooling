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
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import image_smoke
import profile_live
import pytest
from prometheus_client.parser import text_string_to_metric_families

import script

API_TOKEN = os.environ.get("CF_EXPORTER_API_TOKEN", "").strip()
SCRIPT = Path(__file__).resolve().parent.parent / "script.py"
HOUR = 3600

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


@pytest.fixture(scope="module")
def window():
    """The last settled hour."""
    return profile_live.parse_args(["--hours", "1"], time.time())


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
    """The dataset, its settings, fields, dimensions, filters and orderBy values
    must exist and not be deprecated."""
    zone_fields = introspect(api, "zone")["fields"]
    dataset = current(zone_fields, "httpRequestsAdaptiveGroups")
    settings = introspect(api, type_name(current(zone_fields, "settings")["type"]))["fields"]
    dataset_settings = current(settings, "httpRequestsAdaptiveGroups")
    settings_fields = introspect(api, type_name(dataset_settings["type"]))["fields"]
    for setting in ("enabled", "availableFields", "maxPageSize", "maxDuration", "notOlderThan"):
        current(settings_fields, setting)
    for arg in ("limit", "filter", "orderBy"):
        assert arg in {a["name"] for a in dataset["args"]}

    # Cloudflare exposes input-field names but not their deprecation status.
    filter_arg = next(a for a in dataset["args"] if a["name"] == "filter")
    filter_fields = introspect(api, type_name(filter_arg["type"]))["inputFields"]
    queried = {d for mapping in script.DATASETS.values() for d in mapping.values()}
    assert {
        "datetime_geq",
        "datetime_lt",
        "requestSource",
        "AND",
        "OR",
        *queried,
        *(f"{d}_gt" for d in queried),
    } <= {f["name"] for f in filter_fields}
    assert type_name(filter_arg["type"]) == "ZoneHttpRequestsAdaptiveGroupsFilter_InputObject"
    source = next(f for f in filter_fields if f["name"] == "requestSource")
    assert type_name(source["type"]) == "string"

    order_by = next(a for a in dataset["args"] if a["name"] == "orderBy")
    order_values = introspect(api, type_name(order_by["type"]))["enumValues"]
    for dimension in (script.BUCKET_DIMENSION, *queried):
        current(order_values, f"{dimension}_ASC")

    groups = introspect(api, type_name(dataset["type"]))["fields"]
    current(groups, "count")
    sums = introspect(api, type_name(current(groups, "sum")["type"]))["fields"]
    current(sums, "visits")
    averages = introspect(api, type_name(current(groups, "avg")["type"]))["fields"]
    current(averages, "sampleInterval")
    dimensions = introspect(api, type_name(current(groups, "dimensions")["type"]))["fields"]
    assert type_name(current(dimensions, script.BUCKET_DIMENSION)["type"]) == "Time"
    for dimension in queried:
        current(dimensions, dimension)
    # The client filter compares against this dimension's values.
    assert type_name(current(dimensions, "requestSource")["type"]) == "string"
    for status in ("edgeResponseStatus", "originResponseStatus"):
        assert type_name(current(dimensions, status)["type"]) == "uint16"


def test_every_zone_has_usable_settings(api, zones, config):
    for zone in zones:
        settings = script.fetch_settings(api, zone["id"])
        plan = script.make_plan(zone["id"], zone["name"], settings, config.schema)
        assert plan.page_size >= 1 and plan.max_window_buckets >= 1
        # Enough history to recover from a lost database and a long outage.
        assert plan.reach_seconds >= 86400, zone["name"]
        assert settings.fields >= script.REQUIRED_FIELDS


def test_closed_hour_matches_independent_totals(api, zones, config, window):
    """The exporter's buckets of the last settled hour add up to dimensionless
    whole-hour totals, for all requests, visits and client requests."""
    report = profile_live.validate(api, config.schema, *window)
    verdicts = {name: zone["verdict"] for name, zone in report["zones"].items()}
    assert not report["zones_without_plan"]
    assert set(verdicts) == {zone["name"] for zone in zones}
    mismatches = {name: v for name, v in verdicts.items() if v.startswith("MISMATCH")}
    assert not mismatches
    # Reduced-resolution episodes may leave single zones undecided, never all.
    judged = [zone for zone in report["zones"].values() if zone["verdict"] == "ok"]
    assert judged, verdicts
    # A renamed filter value would leave every zone without client requests.
    with_traffic = [zone for zone in judged if zone["reference"][0]]
    if with_traffic:
        assert any(zone["reference"][2] for zone in with_traffic), verdicts
    assert report["results"].get("accepted", 0) >= 12 * len(judged)


def read_zone_rows(api, schema, zone, window):
    storage = script.Storage(None)
    exporter = script.Exporter(api, storage, schema)
    # One zone, as if just listed: only its settings are requested.
    now = time.time()
    exporter.zones = {zone["id"]: zone["name"]}
    exporter.zones_listed, exporter.zones_at = True, now
    exporter.refresh(now)
    plan = exporter.plans[zone["id"]]
    unaccepted = profile_live.read_zone(exporter, plan, *window)
    return plan, unaccepted, storage.exposition(10**9)[1]


def busiest(api, zones, window):
    """The zone with the most requests in the window, with its reference totals."""
    best = None
    for zone in zones:
        settings = script.fetch_settings(api, zone["id"])
        plan = script.make_plan(zone["id"], zone["name"], settings, script.Schema())
        reference = profile_live.fetch_reference(api, plan, *window, time.time())
        if reference and (best is None or reference[0] > best[1][0]):
            best = (zone, reference)
    if best is None or not best[1][0]:
        pytest.skip("no zone with traffic and a full-resolution reference in the last hour")
    return best


def test_disabled_labels_are_aggregated_by_the_api(api, zones, window):
    """Disabled dimensions are left out of the queries, including a family without
    any label and an edge family without its status; sums stay exact."""
    disabled = {
        "cf_requests_5m": ["query", "client_request_referer"],
        "cf_cache_requests_5m": list(script.CACHE_DIMENSIONS),
        "cf_edge_requests_5m": ["edge_response_status"],
    }
    schema = script.Schema({name: frozenset(labels) for name, labels in disabled.items()})
    zone, reference = busiest(api, zones, window)
    plan, unaccepted, rows = read_zone_rows(api, schema, zone, window)
    if unaccepted:
        pytest.skip(f"{unaccepted} buckets were not accepted during a reduced-resolution episode")
    assert "clientRequestQuery" not in plan.dimensions["groups"]
    text = script.render_analytics(rows).decode()
    expected = {name: frozenset(schema.labels(name)) for name in script.METRIC_LABELS}
    counts = image_smoke.checked_analytics(text, expected, time.time())
    assert counts["cf_zone_requests_5m"] == 12
    # Without labels, a family has one series per bucket with traffic.
    assert counts["cf_cache_requests_5m"] <= 12
    sums = profile_live.zone_sums(rows, *window)[zone["name"]]
    assert profile_live.compare(sums, reference) == []


@pytest.fixture(scope="module")
def busy_bucket(api, zones, window):
    """(plan, bucket end, its groups) of the bucket with the most requests in the last
    hour, read in one page from Cloudflare's full data."""
    start, end = window
    best = None
    for zone in zones:
        settings = script.fetch_settings(api, zone["id"])
        plan = script.make_plan(zone["id"], zone["name"], settings, script.Schema())
        totals, _ = script.read_window(api, plan, start, end, ["totals"])
        for bucket_end, data in totals.items():
            count = script._count(data.totals) if data.totals else 0
            if best is None or count > best[0]:
                best = (count, bucket_end, plan)
    if best is None or best[0] < 200:
        pytest.skip("no bucket with enough requests to be read in parts")
    _, bucket_end, plan = best
    since = bucket_end - script.BUCKET_SECONDS
    for attempt in range(3):
        buckets, stats = script.read_window(
            api, plan, since, bucket_end, ["groups", "totals"], variant=attempt
        )
        if stats.states == {"groups": "full", "totals": "full"} and not stats.split:
            return plan, bucket_end, buckets[bucket_end]
    pytest.skip("Cloudflare answered at a reduced resolution three times")


def by_dimensions(groups):
    """Dimension values -> (requests, visits), summed over the parts of a read."""
    sums = {}
    for group in groups:
        key = tuple(sorted((name, str(value)) for name, value in group["dimensions"].items()))
        count, visits = sums.get(key, (0, 0))
        sums[key] = (count + script._count(group), visits + script._visits(group))
    return sums


def test_full_page_is_read_in_parts_without_losing_groups(api, busy_bucket):
    """A page size of a third of the bucket's groups forces the split in time. The
    parts are large, so that a part from a reduced table shows as one; pages of
    a few groups would come back empty under load, which nothing can tell from
    a quiet part."""
    plan, bucket_end, whole = busy_bucket
    if len(whole.groups) < 9:
        pytest.skip("the busiest bucket has too few groups to be read in parts")
    since = bucket_end - script.BUCKET_SECONDS
    small = dataclasses.replace(plan, page_size=len(whole.groups) // 3)
    for attempt in range(3):
        buckets, stats = script.read_window(
            api, small, since, bucket_end, ["groups"], variant=attempt
        )
        if stats.states["groups"] == "split" and not stats.reduced:
            break
    else:
        pytest.skip("Cloudflare answered a part at a reduced resolution three times")
    assert stats.split and stats.requests >= 3
    parts = buckets[bucket_end].groups
    assert sum(map(script._count, parts)) == script._count(whole.totals)
    assert sum(map(script._visits, parts)) == script._visits(whole.totals)
    assert by_dimensions(parts) == by_dimensions(whole.groups)


def test_dimension_pagination_is_accepted_and_advances(api, busy_bucket):
    """The query that reads a full second page by page: sorted by the complete
    dimension tuple, each page filtered to continue after the last group."""
    plan, bucket_end, whole = busy_bucket
    dimensions = plan.dimensions["groups"]
    query = script.build_query({"groups": dimensions}, paging=True)
    since = bucket_end - script.BUCKET_SECONDS
    variables = {
        "zoneTag": plan.zone_id,
        "since": script.rfc3339(since),
        "until": script.rfc3339(bucket_end),
        "limit": 3,
        "after": {"datetime_geq": script.rfc3339(since)},
    }
    keys, states = [], []
    for _ in range(3):
        node = script.zone_node(api.graphql(query, variables), plan.zone_id)
        page = script.group_list("groups", node.get("groups"))
        assert len(page) <= 3
        states.append(script.answer_state(page))
        for group in page:
            script.validate_group("groups", group, (script.BUCKET_DIMENSION, *dimensions))
            keys.append(tuple(group["dimensions"][dimension] for dimension in dimensions))
        if len(page) < 3:
            break
        variables["after"] = script.page_cursor(dimensions, page[-1]["dimensions"])
    # Every page continues strictly after the one before, in the order the
    # exporter compares the tuples in.
    assert keys and keys == sorted(set(keys))
    if set(states) == {"full"}:
        assert len(keys) == min(9, len(whole.groups))


def test_plan_without_a_dimension_exports_unknown(api, zones, window):
    """Regression: Free zones lack some dimensions and used to export nothing."""
    for zone in zones:
        settings = script.fetch_settings(api, zone["id"])
        if "dimensions_clientRequestQuery" in settings.fields:
            continue
        plan, _, rows = read_zone_rows(api, script.Schema(), zone, window)
        assert "clientRequestQuery" not in plan.dimensions["groups"]
        requests_ = [labels for family, labels, _, _ in rows if family == "cf_requests_5m"]
        if requests_:
            assert all('query="unknown"' in labels for labels in requests_)
            return
    pytest.skip("no zone without the query dimension had requests in the last hour")


def test_invalid_api_token_is_reported_not_fatal():
    bad = script.CloudflareAPI("not-a-valid-cloudflare-credential", request_timeout=60)
    with pytest.raises(script.CloudflareAPIError, match=r"GET /zones: HTTP 4\d\d: "):
        bad.list_zones()
    exporter = script.Exporter(bad, script.Storage(None))
    assert exporter.tick(threading.Event()) is False
    assert exporter.readiness_error() == "zones have not been listed yet"
    bad.session.close()


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class ExporterProcess:
    """script.py as a real process with its logs collected."""

    def __init__(self, db_path, **env):
        self.port = free_port()
        self.process = subprocess.Popen(
            [sys.executable, str(SCRIPT)],
            env={
                **os.environ,
                "CF_EXPORTER_METRICS_PORT": str(self.port),
                "CF_EXPORTER_DB_PATH": str(db_path),
                "CF_EXPORTER_BACKFILL_DAYS": "1",
                "PYTHONUNBUFFERED": "1",
                **env,
            },
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.lines = []
        self.reader = threading.Thread(target=lambda: self.lines.extend(self.process.stdout))
        self.reader.daemon = True
        self.reader.start()

    def get(self, path):
        try:
            return image_smoke.fetch(self.port, path)
        except (urllib.error.URLError, ConnectionError):
            return None, None

    def health(self):
        status, body = self.get("/metrics")
        return image_smoke.checked_health(body) if status == 200 else None

    def value(self, name):
        return image_smoke.single_value(self.get("/metrics")[1], name)

    def wait_collected(self, timeout=300):
        """Wait until every zone has a recent accepted bucket; returns the metrics."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            assert self.process.poll() is None, "exporter exited: " + self.logs()[-2000:]
            status, body = self.get("/metrics")
            if status == 200 and "cf_exporter_zone_success{" in body:
                # Zones were listed before they appear here.
                assert self.get("/readyz")[0] == 200
                if image_smoke.collected(image_smoke.checked_health(body), time.time()):
                    return body
            time.sleep(1)
        raise AssertionError("zones were not collected in time: " + self.logs()[-2000:])

    def logs(self):
        return "".join(self.lines).replace(API_TOKEN, "[REDACTED]")

    def stop(self):
        self.process.send_signal(signal.SIGTERM)
        code = self.process.wait(timeout=15)
        self.reader.join(timeout=10)
        self.process.stdout.close()
        return code

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait()


def test_exporter_process_end_to_end_and_restart(zones, config, tmp_path):
    """Start script.py as a real process, check readiness, health, analytics and
    delivery, stop it with SIGTERM, and start it again on the same database."""
    db_path = tmp_path / "state" / "cf-exporter.sqlite3"
    labels = {name: frozenset(config.schema.labels(name)) for name in script.METRIC_LABELS}
    exporter = ExporterProcess(db_path)
    try:
        body = exporter.wait_collected()
        assert image_smoke.checked_health(body)["success"] == {zone["name"]: 1 for zone in zones}
        image_smoke.checked_api_counters(body)
        assert exporter.value("cf_exporter_storage_persistent") == 1
        assert exporter.value("cf_exporter_storage_resets_total") == 0
        status, exposition = exporter.get("/analytics")
        assert status == 200
        counts = image_smoke.checked_analytics(exposition, labels, time.time())
        assert counts["cf_zone_requests_5m"] >= len(zones)
        # Reading the endpoint consumed nothing; scrapes are counted.
        assert exporter.value("cf_exporter_analytics_responses_total") == 0
        for _ in range(2):
            status, consumed = exporter.get("/analytics?consume=1")
            assert status == 200
            image_smoke.checked_analytics(consumed, labels, time.time())
        # Counted after the client has read the whole body: it may lag a moment.
        assert image_smoke.delivered_responses(exporter.port, 2) == 2
        undelivered = exporter.value("cf_exporter_undelivered_buckets")
        assert undelivered > 0
        assert exporter.stop() == 0
    finally:
        exporter.close()
    logs = exporter.logs()
    assert "Signal SIGTERM received, shutting down." in logs
    # Evaluate first, so a failure can never echo the token in the report.
    leaked = API_TOKEN in "".join(exporter.lines)
    assert not leaked, "the API token appeared in the exporter logs"
    assert db_path.exists()

    restarted = ExporterProcess(db_path)
    try:
        body = restarted.wait_collected()
        health = image_smoke.checked_health(body)
        assert restarted.value("cf_exporter_storage_resets_total") == 0
        # The state came from the file: nearly nothing had to be read again.
        assert health["results"]["accepted"] < undelivered / 2
        assert restarted.value("cf_exporter_undelivered_buckets") >= undelivered * 0.9
        assert restarted.stop() == 0
    finally:
        restarted.close()


def test_database_wiped_under_the_running_exporter_is_filled_again(zones, tmp_path):
    """Delete the database files while the exporter runs: it must create a new
    database on its own and export the buckets again."""
    db_path = tmp_path / "cf-exporter.sqlite3"
    exporter = ExporterProcess(db_path)
    try:
        exporter.wait_collected()
        assert exporter.value("cf_exporter_storage_resets_total") == 0
        for suffix in ("", "-wal", "-shm"):
            Path(str(db_path) + suffix).unlink(missing_ok=True)
        deadline = time.monotonic() + 60
        while exporter.value("cf_exporter_storage_resets_total") != 1:
            assert time.monotonic() < deadline, "the wipe was not noticed"
            time.sleep(1)
        assert exporter.value("cf_exporter_storage_persistent") == 1
        body = exporter.wait_collected()
        assert image_smoke.checked_health(body)["success"] == {zone["name"]: 1 for zone in zones}
        assert db_path.exists()
        status, exposition = exporter.get("/analytics")
        assert status == 200
        families = {family.name for family in text_string_to_metric_families(exposition)}
        assert "cf_zone_requests_5m" in families
        assert exporter.get("/readyz")[0] == 200
        assert exporter.stop() == 0
    finally:
        exporter.close()
    assert "was removed or replaced; opening it again" in exporter.logs()
