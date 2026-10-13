# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Shared fixtures: an in-memory fake of the Cloudflare REST + GraphQL APIs."""

import json
from datetime import UTC, datetime

import pytest
import responses
from prometheus_client import CollectorRegistry

import script

BASE_URL = "https://cf.test/client/v4"
NOW = datetime(2026, 10, 13, 7, 40, 12, 345678, tzinfo=UTC)

ALL_FIELDS = frozenset(
    {
        "count",
        "sum_visits",
        *(
            f"dimensions_{d}"
            for d in (*script.VISITS_DIMENSIONS.values(), *script.REQUESTS_DIMENSIONS.values())
        ),
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
        max_page_size=script.GROUPS_LIMIT,
    ):
        self.zones[zone_id] = {
            "name": name,
            "fields": fields,
            "visits": list(visits),
            "requests": list(requests),
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
            node = {"visits": zone["visits"], "requests": zone["requests"]}
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
