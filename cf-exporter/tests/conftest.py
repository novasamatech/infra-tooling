# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Shared fixtures: an in-memory fake of the Cloudflare REST + GraphQL APIs.

The fake stores single events and answers the exporter's bucket queries from
them the way Cloudflare does: grouped by 5-minute bucket and the requested
dimensions, filtered, ordered, cut at the page size. Tests bend single answers
with `transforms` to imitate reduced-resolution and incomplete responses.
"""

import gzip
import json
import re
import threading
from datetime import UTC, datetime
from wsgiref.util import setup_testing_defaults

import pytest
import responses
from prometheus_client import CollectorRegistry
from prometheus_client.parser import text_string_to_metric_families

import script

BASE_URL = "https://cf.test/client/v4"
BUCKET = script.BUCKET_SECONDS
# A UTC hour boundary; tests place buckets relative to it.
T0 = 1_800_000_000
MAX_PAGE_SIZE = 10000
ALL_DIMENSIONS = sorted(
    {
        *script.VISITS_DIMENSIONS.values(),
        *script.REQUESTS_DIMENSIONS.values(),
        *script.EDGE_DIMENSIONS.values(),
    }
)
PRO_FIELDS = [
    "count",
    "sum_visits",
    "avg_sampleInterval",
    "dimensions_datetimeFiveMinutes",
    "dimensions_requestSource",
    *(f"dimensions_{d}" for d in ALL_DIMENSIONS),
]
# Free plans lack the query and the referer, as Cloudflare currently reports.
FREE_FIELDS = [
    f
    for f in PRO_FIELDS
    if f not in ("dimensions_clientRequestQuery", "dimensions_clientRequestReferer")
]
DEFAULT_DIMENSIONS = {
    "clientRequestHTTPHost": "example.org",
    "clientRequestHTTPMethodName": "GET",
    "clientRequestPath": "/",
    "clientRequestQuery": "",
    "clientCountryName": "DE",
    "clientRequestReferer": "",
    "userAgentBrowser": "Chrome",
    "userAgentOS": "Linux",
    "cacheStatus": "hit",
    "originResponseStatus": 200,
    "edgeResponseStatus": 200,
}
SELECTION = re.compile(
    r"(\w+): httpRequestsAdaptiveGroups\(\s*limit: \$(?P<limit>\w+)\s*"
    r"filter: \{datetime_geq: \$(?P<since>\w+), datetime_lt: \$(?P<until>\w+)(?P<filter>[^}]*)\}"
    r"\s*orderBy: \[(?P<order>[^\]]*)\]\s*\)\s*"
    r"\{ (?P<values>.*?) dimensions \{ (?P<dimensions>[^}]*) \} \}"
)
HOUR = 3600


def iso(moment: float) -> str:
    return datetime.fromtimestamp(moment, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def unix(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


def event(at, count=1, visits=0, source="eyeball", interval=1, **dimensions) -> dict:
    """`count` requests at Unix second `at`; `interval` is their sample interval."""
    unknown = set(dimensions) - set(DEFAULT_DIMENSIONS)
    assert not unknown, unknown
    return {
        "at": at,
        "count": count,
        "visits": visits,
        "source": source,
        "interval": interval,
        **DEFAULT_DIMENSIONS,
        **dimensions,
    }


def json_response(status, payload, headers=None):
    return status, headers or {}, json.dumps(payload)


def graphql_error(message, code="authz"):
    return json_response(
        200, {"data": None, "errors": [{"message": message, "extensions": {"code": code}}]}
    )


class FakeZone:
    def __init__(self, zone_id, name, fields, page_size, max_duration, not_older_than):
        self.id = zone_id
        self.name = name
        self.fields = list(fields)
        self.page_size = page_size
        self.max_duration = max_duration
        self.not_older_than = not_older_than
        self.enabled = True
        self.events: list[dict] = []
        # A GraphQL error message for every analytics query of the zone.
        self.error: str | None = None
        self.settings_error: str | None = None

    def add(self, *events) -> "FakeZone":
        self.events.extend(events)
        return self


class FakeCloudflare:
    """Answers GET /zones and POST /graphql from the registered zones."""

    def __init__(self, rsps):
        self.zones: dict[str, FakeZone] = {}
        # Analytics requests: zone, since, until, limit, aliases, after.
        self.calls: list[dict] = []
        self.settings_calls: list[str] = []
        self.zone_calls = 0
        # Each gets (call, alias, groups) and returns the groups to answer with.
        self.transforms: list = []
        # Whole responses returned instead of the next analytics answers.
        self.replies: list = []
        # The same for the next settings answers and zone listings.
        self.settings_replies: list = []
        self.zones_replies: list = []
        # Each gets the call and returns a GraphQL error message to fail it with.
        self.failures: list = []
        self.zones_error: str | None = None
        # Dimensionless whole-window queries of the live validation tool, and how
        # many of the next ones are answered at a reduced resolution.
        self.reference_calls: list[dict] = []
        self.reference_reduced = 0
        rsps.add_callback(responses.GET, f"{BASE_URL}/zones", callback=self._zones)
        rsps.add_callback(responses.POST, f"{BASE_URL}/graphql", callback=self._graphql)

    def add_zone(
        self,
        zone_id,
        name,
        *events,
        fields=PRO_FIELDS,
        page_size=MAX_PAGE_SIZE,
        max_duration=30 * 86400,
        # Two hours of reach after the exporter's margin keep tests small;
        # Cloudflare reports 31 days.
        not_older_than=3 * 3600,
    ) -> FakeZone:
        zone = FakeZone(zone_id, name, fields, page_size, max_duration, not_older_than)
        self.zones[zone_id] = zone.add(*events)
        return zone

    # REST

    def _zones(self, request):
        self.zone_calls += 1
        if self.zones_replies:
            return self.zones_replies.pop(0)
        if self.zones_error:
            return json_response(
                403, {"success": False, "errors": [{"message": self.zones_error, "code": 9109}]}
            )
        page = int(request.params.get("page", 1))
        per_page = int(request.params["per_page"])
        zones = sorted(self.zones.values(), key=lambda zone: zone.name)
        chunk = zones[(page - 1) * per_page : page * per_page]
        return json_response(
            200,
            {
                "success": True,
                "result": [{"id": zone.id, "name": zone.name} for zone in chunk],
                "result_info": {
                    "page": page,
                    "total_pages": -(-len(zones) // per_page),
                },
            },
        )

    # GraphQL

    def _graphql(self, request):
        body = json.loads(request.body)
        query, variables = body["query"], body["variables"]
        zone = self.zones.get(variables["zoneTag"])
        if zone is None:
            return json_response(200, {"data": {"viewer": {"zones": []}}, "errors": None})
        if "settings" in query:
            self.settings_calls.append(zone.id)
            if self.settings_replies:
                return self.settings_replies.pop(0)
            if zone.settings_error:
                return graphql_error(zone.settings_error)
            settings = {
                "enabled": zone.enabled,
                "availableFields": zone.fields,
                "maxPageSize": zone.page_size,
                "maxDuration": zone.max_duration,
                "notOlderThan": zone.not_older_than,
            }
            node = {"settings": {"httpRequestsAdaptiveGroups": settings}}
            return json_response(200, {"data": {"viewer": {"zones": [node]}}, "errors": None})
        if "query Reference" in query:
            return self._reference(zone, variables, "clients:" in query)
        selections = list(SELECTION.finditer(query))
        assert selections, f"unexpected query: {query}"
        # GraphQL rejects unused and undeclared variables alike.
        header, body_ = query.split("{", 1)
        declared = set(re.findall(r"\$(\w+)", header))
        assert declared == set(re.findall(r"\$(\w+)", body_)) == set(variables), query
        ranges, limits = {}, {}
        for match in selections:
            alias = match.group(1)
            since, until = variables[match.group("since")], variables[match.group("until")]
            ranges[alias] = (unix(since), unix(until))
            limits[alias] = variables[match.group("limit")]
        with_dimensions = [alias for alias in ranges if alias in script.GROUP_SELECTIONS]
        main = with_dimensions[0] if with_dimensions else next(iter(ranges))
        call = {
            "zone": zone.id,
            # The window of the selections with dimensions; of a request for
            # totals only, their range.
            "since": ranges[main][0],
            "until": ranges[main][1],
            "limit": limits[main],
            "aliases": tuple(ranges),
            # Per selection: totals have their own, wider range and page size.
            "ranges": ranges,
            "limits": limits,
            "after": variables.get("after"),
            "index": len(self.calls),
        }
        self.calls.append(call)
        if self.replies:
            return self.replies.pop(0)
        if zone.error:
            return graphql_error(zone.error)
        for failure in self.failures:
            message = failure(call)
            if message:
                return graphql_error(message)
        node = {}
        for match in selections:
            alias = match.group(1)
            groups = self._groups(zone, call, match)
            for transform in self.transforms:
                groups = transform(call, alias, groups)
            node[alias] = groups
        return json_response(200, {"data": {"viewer": {"zones": [node]}}, "errors": None})

    def _reference(self, zone, variables, clients: bool):
        """The validation tool's own query: hourly totals over its proof range."""
        since, until = unix(variables["since"]), unix(variables["until"])
        self.reference_calls.append(
            {"zone": zone.id, "limit": variables["limit"], "since": since, "until": until}
        )
        assert variables["limit"] <= zone.page_size, "limit above the plan's maxPageSize"
        assert until - since <= zone.max_duration, "range above maxDuration"
        assert (until - since) // HOUR < variables["limit"], "hours beyond one page"
        reduced = self.reference_reduced > 0
        self.reference_reduced -= reduced
        node = {}
        for alias in ("all", "clients") if clients else ("all",):
            hours: dict[int, list] = {}
            for item in zone.events:
                if not since <= item["at"] < until:
                    continue
                if alias == "clients" and item["source"] != variables["source"]:
                    continue
                row = hours.setdefault(item["at"] // HOUR * HOUR, [0, 0, 0.0])
                row[0] += item["count"]
                row[1] += item["visits"]
                row[2] += item["count"] / item["interval"]
            groups = []
            for hour, (count, visits, stored) in sorted(hours.items()):
                group = {
                    "count": count,
                    "avg": {"sampleInterval": count / stored},
                    "dimensions": {"datetimeHour": iso(hour)},
                }
                if alias == "all":
                    group["sum"] = {"visits": visits}
                groups.append(group)
            node[alias] = sampled({alias})(None, alias, groups) if reduced else groups
        return json_response(200, {"data": {"viewer": {"zones": [node]}}, "errors": None})

    def _groups(self, zone, call, match) -> list[dict]:
        alias = match.group(1)
        since, until = call["ranges"][alias]
        limit = call["limits"][alias]
        assert limit <= zone.page_size, "limit above the plan's maxPageSize"
        assert until - since <= zone.max_duration, "range above maxDuration"
        filter_, values = match.group("filter"), match.group("values")
        dimensions = match.group("dimensions").split()
        assert dimensions[0] == script.BUCKET_DIMENSION, dimensions
        fields = dimensions[1:]
        for field in fields:
            assert f"dimensions_{field}" in zone.fields, f"{field} is not offered by the plan"
        order = [item.removesuffix("_ASC") for item in match.group("order").split(", ")]
        paging = "AND: [$after]" in filter_
        assert order == (fields if paging else [script.BUCKET_DIMENSION]), order
        assert paging == (call["after"] is not None)
        eyeball = 'requestSource: "eyeball"' in filter_
        rows: dict[tuple, list] = {}
        for item in zone.events:
            if not since <= item["at"] < until:
                continue
            if eyeball and item["source"] != "eyeball":
                continue
            if paging and not self._after(call["after"], item):
                continue
            key = (item["at"] // BUCKET * BUCKET, *(item[field] for field in fields))
            row = rows.setdefault(key, [0, 0, 0.0])
            row[0] += item["count"]
            row[1] += item["visits"]
            row[2] += item["count"] / item["interval"]
        ordered = sorted(rows.items(), key=lambda entry: entry[0][1:] if paging else entry[0])
        groups = []
        for key, (count, visits, stored) in ordered[:limit]:
            group = {
                "count": count,
                "avg": {"sampleInterval": count / stored},
                "dimensions": {
                    script.BUCKET_DIMENSION: iso(key[0]),
                    **dict(zip(fields, key[1:], strict=True)),
                },
            }
            if "sum { visits }" in values:
                group["sum"] = {"visits": visits}
            groups.append(group)
        return groups

    @staticmethod
    def _after(after: dict, item: dict) -> bool:
        if "OR" not in after:
            assert set(after) == {"datetime_geq"}, after
            return True
        for branch in after["OR"]:
            if all(
                item[key.removesuffix("_gt")] > value if key.endswith("_gt") else item[key] == value
                for key, value in branch.items()
            ):
                return True
        return False


def sampled(alias_names, factor=10):
    """A transform answering the given selections from a reduced-resolution table,
    as Cloudflare does under load: a group with fewer than `factor` events has
    no row there and vanishes; the others come in multiples of `factor` with
    that sample interval."""

    def transform(call, alias, groups):
        if alias not in alias_names:
            return groups
        result = []
        for group in groups:
            if group["count"] < factor:
                continue
            count = round(group["count"] / factor) * factor
            scaled = {**group, "count": count, "avg": {"sampleInterval": float(factor)}}
            if "sum" in group:
                scaled["sum"] = {"visits": round(group["sum"]["visits"] / factor) * factor}
            result.append(scaled)
        return result

    return transform


def first_calls(count, transform):
    """Apply a transform only to the first `count` analytics requests."""
    return lambda call, alias, groups: (
        transform(call, alias, groups) if call["index"] < count else groups
    )


class Clock:
    """A settable wall clock."""

    def __init__(self, now: float):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> float:
        self.now += seconds
        return self.now


@pytest.fixture
def rsps():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        yield mock


@pytest.fixture
def api(request):
    client = script.CloudflareAPI(
        "dummy-credential", base_url=BASE_URL, **getattr(request, "param", {})
    )
    yield client
    client.session.close()


@pytest.fixture
def cloudflare(rsps):
    return FakeCloudflare(rsps)


@pytest.fixture
def clock():
    # Ten minutes into an hour: the buckets ending up to T0 + 300 have settled.
    return Clock(T0 + 600)


@pytest.fixture
def storage():
    return script.Storage(None)


@pytest.fixture(autouse=True)
def close_databases(monkeypatch):
    """Close every database a test opened, including those a storage replaced."""
    opened = []
    connect = script.Storage._connect

    def tracking(target):
        connection = connect(target)
        opened.append(connection)
        return connection

    monkeypatch.setattr(script.Storage, "_connect", staticmethod(tracking))
    yield
    for connection in opened:
        connection.close()


@pytest.fixture(autouse=True)
def no_history_pacing(monkeypatch):
    monkeypatch.setattr(script, "HISTORY_INTERVAL_SECONDS", 0)


@pytest.fixture
def make_exporter(api, storage, clock):
    def make(schema=None, **options):
        exporter = script.Exporter(api, storage, schema, clock=clock, **options)
        # Page-size variants start from zero, whatever this host is called.
        exporter._reads = -1
        return exporter

    return make


@pytest.fixture
def exporter(make_exporter):
    return make_exporter()


def run(exporter, rounds=1):
    """Run collection until nothing is due, as the main loop does."""
    stop = threading.Event()
    for _ in range(rounds):
        while exporter.tick(stop):
            pass


def request(app, path, method="GET", query="", headers=None):
    """Call the WSGI app and read the whole body, as a completed HTTP exchange."""
    environ = {}
    setup_testing_defaults(environ)
    environ.update({"REQUEST_METHOD": method, "PATH_INFO": path, "QUERY_STRING": query})
    for name, value in (headers or {}).items():
        environ["HTTP_" + name.upper().replace("-", "_")] = value
    captured = {}

    def start_response(status, response_headers, exc_info=None):
        captured["status"] = int(status.split()[0])
        captured["headers"] = dict(response_headers)

    body = b"".join(app(environ, start_response))
    if captured["headers"].get("Content-Encoding") == "gzip":
        body = gzip.decompress(body)
    return captured["status"], captured["headers"], body.decode()


def parse(text: str) -> list:
    """Samples of an exposition as (name, labels, value, timestamp seconds or None)."""
    return [
        (sample.name, dict(sample.labels), sample.value, sample.timestamp)
        for family in text_string_to_metric_families(text)
        for sample in family.samples
    ]


def analytics(exporter, storage, consume=False) -> list:
    app = script.make_app(exporter, storage, CollectorRegistry())
    status, _, body = request(app, "/analytics", query="consume=1" if consume else "")
    assert status == 200
    return parse(body)


def values(samples, name, **labels) -> dict[int, float]:
    """bucket end -> summed value of the samples of a family matching the labels."""
    result: dict[int, float] = {}
    for sample_name, sample_labels, value, timestamp in samples:
        if sample_name == name and all(sample_labels.get(k) == v for k, v in labels.items()):
            result[int(timestamp)] = result.get(int(timestamp), 0) + value
    return result


def health(exporter, storage) -> dict:
    """Health samples as {(name, sorted label items): value}."""
    registry = CollectorRegistry()
    registry.register(script.HealthCollector(exporter, storage))
    registry.register(exporter.api.stats)
    result = {}
    for metric in registry.collect():
        for sample in metric.samples:
            result[(sample.name, tuple(sorted(sample.labels.items())))] = sample.value
    return result


def plan_for(api, zone, schema=None, **options) -> script.ZonePlan:
    """The read plan the exporter derives for a fake zone."""
    settings = script.fetch_settings(api, zone.id)
    return script.make_plan(zone.id, zone.name, settings, schema or script.Schema(), **options)


def counts(buckets, alias) -> dict[int, float]:
    """bucket end -> summed count of a selection, for buckets that have groups."""
    result = {}
    for bucket_end, data in buckets.items():
        value = getattr(data, alias)
        groups = value if isinstance(value, list) else ([value] if value else [])
        if groups:
            result[bucket_end] = sum(group["count"] for group in groups)
    return result
