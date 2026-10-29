# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Shared fixtures: an in-memory fake of the Cloudflare REST + GraphQL APIs."""

import json
import re
from datetime import UTC, datetime, timedelta

import pytest
import responses
from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.parser import text_string_to_metric_families

import script

BASE_URL = "https://cf.test/client/v4"
# The fake API's default settings.maxPageSize, as Cloudflare currently reports.
MAX_PAGE_SIZE = 10000
NOW = datetime(2026, 10, 13, 7, 40, 12, 345678, tzinfo=UTC)
# The UTC day of the exposition tests; their clock starts at its noon.
DAY = datetime(2026, 10, 26, tzinfo=UTC)

ALL_FIELDS = frozenset(
    {
        "count",
        "sum_visits",
        "dimensions_requestSource",
        *(f"dimensions_{d}" for mapping in script.DATASETS.values() for d in mapping.values()),
    }
)
# What a Free-plan zone offers (verified against the live API).
FREE_FIELDS = ALL_FIELDS - {"dimensions_clientRequestQuery", "dimensions_clientRequestReferer"}


def visits_group(visits, **dimensions):
    return {"sum": {"visits": visits}, "dimensions": dimensions}


def requests_group(count, **dimensions):
    return {"count": count, "dimensions": dimensions}


def json_response(status, payload):
    return status, {"Content-Type": "application/json"}, json.dumps(payload)


def graphql_error(message, code="authz"):
    return json_response(
        200, {"data": None, "errors": [{"message": message, "extensions": {"code": code}}]}
    )


# An aliased selection and the content of its filter object.
SELECTION = re.compile(r"(\w+): httpRequestsAdaptiveGroups\(\s*limit: \S+\s*filter: \{([^}]*)\}")


def eyeball_only(filters, group):
    """Whether the group passes the selection's filter. Unmarked groups are eyeball traffic."""
    source = group.get("dimensions", {}).get("requestSource", "eyeball")
    selected = re.search(r'requestSource:\s*"([^"\\]*)"', filters)
    return selected is None or source == selected[1]


def selections(query):
    """(alias, filter, dimensions) of every aliased selection of an analytics query."""
    parts = re.split(r"(\w+): httpRequestsAdaptiveGroups\(", query)[1:]
    for alias, body in zip(parts[::2], parts[1::2], strict=True):
        dimensions = re.search(r"dimensions \{ ([^}]*) \}", body)
        filters = re.search(r"filter: \{([^}]*)\}", body)[1]
        yield alias, filters, dimensions[1].split() if dimensions else []


def is_check(call, dimensions):
    """A dimensionless read of one group: the check of an empty page."""
    return not dimensions and call["variables"].get("limit") == 1


def reads(cloudflare, kind, checks=False):
    """Page reads of one dataset in all analytics requests so far, or its checks."""
    return sum(
        alias == kind and is_check(call, dimensions) == checks
        for call in cloudflare.data_calls()
        for alias, _, dimensions in selections(call["query"])
    )


def page_calls(cloudflare):
    """Analytics requests other than checks of empty pages."""
    return [
        call
        for call in cloudflare.data_calls()
        if not all(is_check(call, dimensions) for _, _, dimensions in selections(call["query"]))
    ]


# Dimensions of an event that sets no others; every dataset groups by some of them.
EVENT_DIMENSIONS = {
    "clientRequestHTTPHost": "example.org",
    "clientRequestHTTPMethodName": "GET",
    "clientRequestPath": "/",
    "clientRequestQuery": "",
    "clientCountryName": "DE",
    "clientRequestReferer": "",
    "userAgentBrowser": "Firefox",
    "userAgentOS": "Linux",
    "cacheStatus": "hit",
    "originResponseStatus": 200,
    "edgeResponseStatus": 200,
}


def event(second, count=1, **dimensions):
    """Requests at `second` after the origin; each one is also a visit."""
    return second, count, EVENT_DIMENSIONS | dimensions


def aggregating_responder(events, origin):
    """Analytics as Cloudflare computes them: events of the half-open window that
    pass the selection's filter are grouped by the requested dimensions only,
    ordered largest first, and only then cut to the page size."""

    def respond(body):
        variables = body["variables"]
        start = datetime.fromisoformat(variables["since"])
        end = datetime.fromisoformat(variables["until"])
        node = {}
        for kind, filters, dimensions in selections(body["query"]):
            groups = {}
            for second, count, values in events:
                if start <= origin + timedelta(seconds=second) < end and eyeball_only(
                    filters, {"dimensions": values}
                ):
                    key = tuple(values.get(dimension) for dimension in dimensions)
                    groups[key] = groups.get(key, 0) + count
            if kind == "totals":
                total = sum(groups.values())
                node[kind] = [{"count": total, "sum": {"visits": total}}] if total else []
                continue
            make = visits_group if kind == "visits" else requests_group
            ranked = sorted(groups.items(), key=lambda item: item[1], reverse=True)
            node[kind] = [
                make(value, **dict(zip(dimensions, key, strict=True)))
                for key, value in ranked[: variables["limit"]]
            ]
        return json_response(200, {"data": {"viewer": {"zones": [node]}}})

    return respond


def without_source(group):
    """The exporter never selects requestSource, so the API would not return it."""
    dimensions = {k: v for k, v in group.get("dimensions", {}).items() if k != "requestSource"}
    return {**group, "dimensions": dimensions} if "dimensions" in group else group


class FakeCloudflare:
    """Serves GET /zones and POST /graphql from in-memory zone definitions."""

    def __init__(self, rsps):
        self.zones = {}
        self.zones_error = None
        self.graphql_calls = []
        rsps.add_callback(responses.GET, f"{BASE_URL}/zones", callback=self._zones)
        rsps.add_callback(responses.POST, f"{BASE_URL}/graphql", callback=self._graphql)

    def add_zone(
        self,
        zone_id,
        name,
        fields=ALL_FIELDS,
        visits=(),
        requests=(),
        error=None,
        enabled=True,
        max_page_size=MAX_PAGE_SIZE,
        cache=(),
        totals=None,
        responder=None,
        edge=(),
    ):
        self.zones[zone_id] = {
            "name": name,
            "fields": fields,
            "visits": list(visits),
            "requests": list(requests),
            "cache": list(cache),
            "edge": list(edge),
            "totals": totals,
            "responder": responder,
            "error": error,
            "enabled": enabled,
            "max_page_size": max_page_size,
        }

    def data_calls(self):
        return [c for c in self.graphql_calls if "HttpRequests" in c["query"]]

    def _zones(self, request):
        if self.zones_error:
            return json_response(
                400, {"success": False, "errors": [self.zones_error], "result": None}
            )
        result = [{"id": zone_id, "name": z["name"]} for zone_id, z in self.zones.items()]
        return json_response(
            200,
            {
                "success": True,
                "errors": [],
                "result": result,
                "result_info": {"page": 1, "total_pages": 1},
            },
        )

    def _graphql(self, request):
        body = json.loads(request.body)
        self.graphql_calls.append(body)
        zone = self.zones.get(body["variables"]["zoneTag"])
        if zone is None:
            return graphql_error("Zone not found")
        if "AvailableFields" in body["query"]:
            settings = {
                "enabled": zone["enabled"],
                "availableFields": sorted(zone["fields"]),
                "maxPageSize": zone["max_page_size"],
            }
            node = {"settings": {"httpRequestsAdaptiveGroups": settings}}
        elif zone["error"]:
            return graphql_error(zone["error"])
        else:
            if zone["responder"]:
                return zone["responder"](body)
            node = {}
            for kind, filters in SELECTION.findall(body["query"]):
                if kind == "totals":
                    node[kind] = (
                        zone["totals"]
                        if zone["totals"] is not None
                        else [
                            {
                                "count": sum(g["count"] for g in zone["requests"]),
                                "sum": {"visits": sum(g["sum"]["visits"] for g in zone["visits"])},
                            }
                        ]
                    )
                else:
                    groups = [g for g in zone[kind] if eyeball_only(filters, g)]
                    node[kind] = [without_source(g) for g in groups][: body["variables"]["limit"]]
        return json_response(200, {"data": {"viewer": {"zones": [node]}}, "errors": None})


@pytest.fixture
def rsps():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        yield mock


@pytest.fixture
def api():
    return script.CloudflareAPI("dummy-credential", request_timeout=5, base_url=BASE_URL)


@pytest.fixture
def cloudflare(rsps):
    return FakeCloudflare(rsps)


@pytest.fixture
def registry():
    """A private registry, so tests never touch the global one."""
    return CollectorRegistry()


@pytest.fixture
def clock(monkeypatch):
    """The exporter's UTC clock; assign `clock.value` to move it."""

    class Clock(datetime):
        value = DAY + timedelta(hours=12)

        @classmethod
        def now(cls, tz=None):
            return cls.value

    monkeypatch.setattr(script, "datetime", Clock)
    return Clock


def exposition(collector):
    registry = CollectorRegistry()
    registry.register(collector)
    return generate_latest(registry).decode()


def samples(collector, name):
    """Samples of one family, parsed from the actual Prometheus exposition."""
    return [
        sample
        for family in text_string_to_metric_families(exposition(collector))
        for sample in family.samples
        if sample.name == name
    ]


def publish(api, clock, moment, collector=None):
    """Run a cycle at `moment` and publish its snapshot, as the main loop does."""
    clock.value = moment
    if collector is None:
        collector = script.CloudflareCollector()
    collector.update(script.collect_once(api, now=moment))
    return collector
