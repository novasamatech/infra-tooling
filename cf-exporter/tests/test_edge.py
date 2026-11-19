# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""The client-facing edge dataset: its request-source filter, plan support and
strict status types."""

import json
import logging

import pytest
from conftest import BUCKET, PRO_FIELDS, T0, analytics, event, health, plan_for, run, values

import script

END = T0 + BUCKET
ZONE = {"zone_name": "example.org"}


def without(*fields):
    return [field for field in PRO_FIELDS if field not in fields]


def selections(rsps) -> dict[str, str]:
    """alias -> filter text of the last analytics query sent."""
    query = json.loads(rsps.calls[-1].request.body)["query"]
    result = {}
    for part in query.split("httpRequestsAdaptiveGroups(")[1:]:
        alias = query[: query.index(part)].rsplit(":", 1)[0].split()[-1]
        result[alias] = part[part.index("filter: {") : part.index("}")]
    return result


def test_only_edge_selections_filter_by_request_source(cloudflare, api, rsps):
    zone = cloudflare.add_zone("z1", "example.org")
    script.read_window(api, plan_for(api, zone), T0, END)
    filters = selections(rsps)
    assert set(filters) == {"groups", "totals", "edge", "edge_totals"}
    for alias in ("edge", "edge_totals"):
        assert 'requestSource: "eyeball"' in filters[alias]
    for alias in ("groups", "totals"):
        assert "requestSource" not in filters[alias]
    assert script.EYEBALL_FILTER == 'requestSource: "eyeball"'


def test_edge_family_counts_client_requests_and_the_others_every_source(
    cloudflare, exporter, storage
):
    cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 1, count=5, edgeResponseStatus=200),
        event(T0 + 2, count=2, edgeResponseStatus=503, originResponseStatus=502),
        event(T0 + 3, count=1, edgeResponseStatus=403, originResponseStatus=0),
        event(T0 + 4, count=40, source="edgeWorkerFetch"),
    )
    run(exporter)
    samples = analytics(exporter, storage)
    edge = {
        (labels["edge_response_status"], labels["origin_response_status"]): value
        for name, labels, value, _ in samples
        if name == "cf_edge_requests_5m"
    }
    # Both statuses are kept; an origin that was never asked stays 0.
    assert edge == {("200", "200"): 5, ("503", "502"): 2, ("403", "0"): 1}
    assert values(samples, "cf_requests_5m", **ZONE) == {END: 48}
    assert values(samples, "cf_cache_requests_5m", **ZONE) == {END: 48}
    assert values(samples, "cf_zone_requests_5m", **ZONE)[END] == 48
    assert exporter.results["inconsistent"] == 0


def test_zone_with_only_subrequests_has_no_edge_series(cloudflare, exporter, storage):
    cloudflare.add_zone("z1", "example.org", event(T0 + 4, count=40, source="edgeWorkerFetch"))
    run(exporter)
    samples = analytics(exporter, storage)
    assert values(samples, "cf_edge_requests_5m") == {}
    assert values(samples, "cf_requests_5m", **ZONE) == {END: 40}


@pytest.mark.parametrize("missing", ["dimensions_requestSource", "dimensions_edgeResponseStatus"])
def test_plan_that_cannot_serve_the_edge_dataset_skips_only_it(
    cloudflare, exporter, storage, api, caplog, missing
):
    zone = cloudflare.add_zone(
        "z1", "example.org", event(T0 + 1, count=5, visits=1), fields=without(missing)
    )
    with caplog.at_level(logging.WARNING):
        plan = plan_for(api, zone)
    assert f"plan does not offer {missing}; the edge dataset is not collected" in caplog.text
    assert not plan.edge and set(plan.dimensions) == {"groups", "totals"}
    run(exporter)
    assert {call["aliases"] for call in cloudflare.calls} == {("groups", "totals")}
    samples = analytics(exporter, storage)
    assert values(samples, "cf_edge_requests_5m") == {}
    assert values(samples, "cf_requests_5m", **ZONE) == {END: 5}
    assert values(samples, "cf_visits_5m", **ZONE) == {END: 1}
    metrics = health(exporter, storage)
    available = {
        dict(labels)["dataset"]: value
        for (name, labels), value in metrics.items()
        if name == "cf_exporter_dataset_available"
    }
    assert available == {"visits": 1, "requests": 1, "cache": 1, "edge": 0}
    # The zone itself is fine.
    assert metrics[("cf_exporter_zone_success", (("zone_name", "example.org"),))] == 1


def test_edge_status_is_required_only_while_its_label_is_exported(cloudflare, api):
    zone = cloudflare.add_zone("z1", "example.org", fields=without("dimensions_edgeResponseStatus"))
    schema = script.Schema({"cf_edge_requests_5m": frozenset({"edge_response_status"})})
    plan = plan_for(api, zone, schema)
    assert plan.edge and "edgeResponseStatus" not in plan.dimensions["edge"]


def test_error_is_never_taken_as_missing_support(cloudflare, exporter, storage):
    zone = cloudflare.add_zone("z1", "example.org", event(T0 + 1, count=5))
    zone.settings_error = "internal error"
    run(exporter)
    metrics = health(exporter, storage)
    assert not [name for name, _ in metrics if name == "cf_exporter_dataset_available"]
    assert metrics[("cf_exporter_zone_success", (("zone_name", "example.org"),))] == 0


def set_edge(**changes):
    def transform(call, alias, groups):
        if alias == "edge":
            for group in groups:
                group["dimensions"].update(changes)
        return groups

    return transform


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"edgeResponseStatus": None}, "group without edgeResponseStatus"),
        ({"edgeResponseStatus": "200"}, "invalid edgeResponseStatus"),
        ({"edgeResponseStatus": True}, "invalid edgeResponseStatus"),
        ({"edgeResponseStatus": 200.0}, "invalid edgeResponseStatus"),
        ({"edgeResponseStatus": -1}, "invalid edgeResponseStatus"),
        ({"edgeResponseStatus": 65536}, "invalid edgeResponseStatus"),
        ({"originResponseStatus": "502"}, "invalid originResponseStatus"),
        ({"clientRequestHTTPHost": 7}, "invalid clientRequestHTTPHost"),
        ({"clientRequestPath": ["/"]}, "invalid clientRequestPath"),
    ],
)
def test_edge_dimensions_are_type_checked(cloudflare, api, changes, message):
    zone = cloudflare.add_zone("z1", "example.org", event(T0 + 1, count=5))
    plan = plan_for(api, zone)
    cloudflare.transforms.append(set_edge(**changes))
    with pytest.raises(script.CloudflareAPIError, match=message):
        script.read_window(api, plan, T0, END)


def test_missing_optional_edge_dimension_is_unknown(cloudflare, exporter, storage):
    cloudflare.add_zone("z1", "example.org", event(T0 + 1, count=5))
    cloudflare.transforms.append(set_edge(originResponseStatus=None))
    run(exporter)
    (labels,) = [
        labels
        for name, labels, _, _ in analytics(exporter, storage)
        if name == "cf_edge_requests_5m"
    ]
    assert labels["origin_response_status"] == "unknown" and labels["edge_response_status"] == "200"


def test_other_families_keep_their_lenient_conversion(cloudflare, exporter, storage):
    cloudflare.add_zone("z1", "example.org", event(T0 + 1, count=5))

    def stringly(call, alias, groups):
        if alias == "groups":
            for group in groups:
                group["dimensions"]["originResponseStatus"] = "200"
        return groups

    cloudflare.transforms.append(stringly)
    run(exporter)
    samples = analytics(exporter, storage)
    assert values(samples, "cf_cache_requests_5m", origin_response_status="200") == {END: 5}
