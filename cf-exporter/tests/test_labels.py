# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Labels: aggregation per family from one selection, disabled labels, plan
dimensions that are not offered, and value conversion."""

import pytest
from conftest import BUCKET, FREE_FIELDS, T0, analytics, event, plan_for, run, values

import script

END = T0 + BUCKET


def series(samples, name) -> dict[tuple, float]:
    """Label values (without the zone) -> value of a family's samples."""
    return {
        tuple(value for key, value in labels.items() if key != "zone_name"): value
        for sample, labels, value, _ in samples
        if sample == name
    }


def test_each_family_aggregates_the_groups_over_its_own_labels(cloudflare, exporter, storage):
    cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 1, count=3, visits=1, clientRequestQuery="?a=1", cacheStatus="hit"),
        event(T0 + 2, count=2, visits=1, clientRequestQuery="?a=2", cacheStatus="hit"),
        event(T0 + 3, count=4, visits=0, clientRequestQuery="?a=2", cacheStatus="miss"),
        event(T0 + 4, count=5, visits=2, clientCountryName="FR", userAgentOS="Android"),
    )
    run(exporter)
    samples = analytics(exporter, storage)
    # cf_requests keeps every dimension apart.
    assert sorted(series(samples, "cf_requests_5m").values()) == [2, 3, 4, 5]
    # cf_visits has no query, method or cache labels: those groups are summed.
    assert series(samples, "cf_visits_5m") == {
        ("example.org", "/", "DE", "direct", "Chrome", "Linux"): 2,
        ("example.org", "/", "FR", "direct", "Chrome", "Android"): 2,
    }
    assert series(samples, "cf_cache_requests_5m") == {
        ("example.org", "GET", "/", "hit", "200"): 10,
        ("example.org", "GET", "/", "miss", "200"): 4,
    }
    assert series(samples, "cf_edge_requests_5m") == {("example.org", "GET", "/", "200", "200"): 14}
    # Whatever the labels, every family adds up to the zone total.
    for name in ("cf_requests_5m", "cf_cache_requests_5m", "cf_edge_requests_5m"):
        assert sum(series(samples, name).values()) == 14
    assert sum(series(samples, "cf_visits_5m").values()) == 4
    assert values(samples, "cf_zone_requests_5m")[END] == 14
    assert values(samples, "cf_zone_visits_5m")[END] == 4


def test_groups_without_visits_export_no_visit_series(cloudflare, exporter, storage):
    cloudflare.add_zone("z1", "example.org", event(T0 + 1, count=3, visits=0))
    run(exporter)
    samples = analytics(exporter, storage)
    assert series(samples, "cf_visits_5m") == {}
    assert values(samples, "cf_zone_visits_5m")[END] == 0


def test_empty_referer_is_direct_and_values_are_strings(cloudflare, exporter, storage):
    cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 1, visits=1, clientRequestReferer=""),
        event(T0 + 2, visits=1, clientRequestReferer="https://search.example/"),
        event(T0 + 3, originResponseStatus=0, cacheStatus="none"),
    )
    run(exporter)
    samples = analytics(exporter, storage)
    referers = {labels[3] for labels in series(samples, "cf_visits_5m")}
    assert referers == {"direct", "https://search.example/"}
    assert ("example.org", "GET", "/", "none", "0") in series(samples, "cf_cache_requests_5m")


def test_dimensions_the_plan_does_not_offer_are_unknown_and_not_queried(
    cloudflare, exporter, storage, api
):
    zone = cloudflare.add_zone(
        "z1",
        "free.example",
        event(T0 + 1, count=3, visits=1, clientRequestQuery="?x", clientRequestReferer="r"),
        fields=FREE_FIELDS,
    )
    plan = plan_for(api, zone)
    assert "clientRequestQuery" not in plan.dimensions["groups"]
    assert "clientRequestReferer" not in plan.dimensions["groups"]
    run(exporter)
    samples = analytics(exporter, storage)
    (labels,) = [labels for name, labels, _, _ in samples if name == "cf_requests_5m"]
    assert labels["query"] == "unknown" and labels["client_request_referer"] == "unknown"
    (labels,) = [labels for name, labels, _, _ in samples if name == "cf_visits_5m"]
    assert labels["client_request_referer"] == "unknown"


def test_group_selection_queries_the_union_of_exported_dimensions(cloudflare, api):
    zone = cloudflare.add_zone("z1", "example.org")
    assert plan_for(api, zone).dimensions["groups"] == (
        "clientRequestHTTPHost",
        "clientRequestPath",
        "clientCountryName",
        "clientRequestReferer",
        "userAgentBrowser",
        "userAgentOS",
        "clientRequestHTTPMethodName",
        "clientRequestQuery",
        "cacheStatus",
        "originResponseStatus",
    )


def test_disabled_label_leaves_the_exposition_and_the_query(
    cloudflare, make_exporter, storage, api
):
    schema = script.parse_disabled_labels(
        '{"cf_requests_5m": ["query", "client_request_referer"], '
        '"cf_edge_requests_5m": ["path", "origin_response_status"]}'
    )
    zone = cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 1, count=3, visits=1, clientRequestQuery="?a=1", clientRequestPath="/a"),
        event(T0 + 2, count=2, visits=1, clientRequestQuery="?a=2", clientRequestPath="/b"),
        event(T0 + 3, count=4, clientRequestQuery="?a=3", clientRequestPath="/b"),
    )
    plan = plan_for(api, zone, schema)
    # No family exports the query any more, so Cloudflare aggregates over it.
    assert "clientRequestQuery" not in plan.dimensions["groups"]
    # The referer is still needed by cf_visits.
    assert "clientRequestReferer" in plan.dimensions["groups"]
    assert plan.dimensions["edge"] == (
        "clientRequestHTTPHost",
        "clientRequestHTTPMethodName",
        "edgeResponseStatus",
    )
    exporter = make_exporter(schema)
    run(exporter)
    samples = analytics(exporter, storage)
    requests_ = [labels for name, labels, _, _ in samples if name == "cf_requests_5m"]
    assert all(
        "query" not in labels and "client_request_referer" not in labels for labels in requests_
    )
    assert series(samples, "cf_requests_5m") == {
        ("example.org", "GET", "/a", "DE", "Chrome", "Linux", "hit", "200"): 3,
        ("example.org", "GET", "/b", "DE", "Chrome", "Linux", "hit", "200"): 6,
    }
    assert series(samples, "cf_edge_requests_5m") == {("example.org", "GET", "200"): 9}
    # Other families keep all their labels.
    assert len(series(samples, "cf_visits_5m")) == 2
    assert all(
        "client_request_referer" in labels
        for name, labels, _, _ in samples
        if name == "cf_visits_5m"
    )


def test_all_labels_of_a_family_can_be_disabled_except_the_zone(cloudflare, make_exporter, storage):
    schema = script.Schema({"cf_cache_requests_5m": frozenset(script.CACHE_DIMENSIONS)})
    cloudflare.add_zone(
        "z1", "example.org", event(T0 + 1, count=3), event(T0 + 2, count=2, cacheStatus="miss")
    )
    exporter = make_exporter(schema)
    run(exporter)
    samples = analytics(exporter, storage)
    assert [
        (labels, value) for name, labels, value, _ in samples if name == "cf_cache_requests_5m"
    ] == [({"zone_name": "example.org"}, 5)]


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ("[]", "must be a JSON object"),
        ("{", "must be valid JSON"),
        ('{"cf_requests": ["query"]}', "unknown metric 'cf_requests'"),
        ('{"cf_requests_5m": "query"}', "must map to a list"),
        ('{"cf_requests_5m": [1]}', "must map to a list"),
        ('{"cf_requests_5m": ["nope"]}', "has no label 'nope'"),
        ('{"cf_visits_5m": ["query"]}', "has no label 'query'"),
        ('{"cf_requests_5m": ["query"], "cf_requests_5m": []}', "must not repeat a key"),
        ('{"cf_requests_5m": ["zone_name"]}', "zone_name cannot be disabled"),
        ('{"cf_zone_requests_5m": ["zone_name"]}', "zone_name cannot be disabled"),
    ],
)
def test_invalid_disabled_labels_fail_at_startup(raw, message):
    with pytest.raises(script.ConfigError, match=message):
        script.parse_disabled_labels(raw)


def test_disabled_labels_parsing():
    assert script.parse_disabled_labels("").disabled == {}
    assert script.parse_disabled_labels('{"cf_requests_5m": []}').disabled == {}
    schema = script.parse_disabled_labels('{"cf_requests_5m": ["query", "path"]}')
    assert schema.labels("cf_requests_5m") == (
        "zone_name",
        "host_name",
        "method_name",
        "client_country_name",
        "client_request_referer",
        "user_agent_browser",
        "user_agent_os",
        "cache_status",
        "origin_response_status",
    )
    assert "path" in schema.labels("cf_visits_5m")
    assert "clientRequestPath" not in schema.dimensions("requests").values()


def test_schema_does_not_follow_later_changes_of_its_input():
    disabled = {"cf_requests_5m": {"query"}}
    schema = script.Schema(disabled)
    disabled["cf_requests_5m"].add("path")
    disabled["cf_visits_5m"] = {"path"}
    assert schema.disabled == {"cf_requests_5m": frozenset({"query"})}
    with pytest.raises(TypeError):
        schema.disabled["cf_visits_5m"] = frozenset()


def test_catalog_matches_the_documented_label_order():
    assert script.METRIC_LABELS["cf_visits_5m"] == (
        "zone_name",
        "host_name",
        "path",
        "client_country_name",
        "client_request_referer",
        "user_agent_browser",
        "user_agent_os",
    )
    assert script.METRIC_LABELS["cf_edge_requests_5m"] == (
        "zone_name",
        "host_name",
        "method_name",
        "path",
        "edge_response_status",
        "origin_response_status",
    )
    assert script.METRIC_LABELS["cf_zone_requests_5m"] == ("zone_name",)
    assert set(script.DESCRIPTIONS) == set(script.METRIC_LABELS)
