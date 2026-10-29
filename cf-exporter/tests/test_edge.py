# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""The client-facing edge dataset, checked through the real Prometheus exposition."""

import json
from datetime import timedelta

import image_smoke
import profile_live
import pytest
from conftest import (
    ALL_FIELDS,
    DAY,
    SELECTION,
    exposition,
    publish,
    requests_group,
    samples,
    visits_group,
)
from image_smoke import checked_edge_family
from profile_live import (
    expected_totals,
    request_sources,
    source_filter_valid,
    source_value_present,
)

import script

EDGE = "cf_edge_requests"
AVAILABLE = "cf_exporter_dataset_available"
WINDOW = "cf_exporter_snapshot_window_end_seconds"
EYEBALL = 'requestSource: "eyeball"'

# (edge, origin) pairs: an origin status never stands in for the client's status.
STATUSES = [
    (101, 101),
    (200, 200),
    (200, 0),  # served from cache
    (202, 202),
    (204, 204),
    (301, 0),
    (304, 304),
    (401, 401),
    (404, 404),
    (499, 0),
    (503, 503),
    (520, 200),
    (521, 0),
    (522, 0),
    (523, 0),
    (524, 0),
    (525, 0),
    (530, 0),
    (0, 0),
]


def edge_group(
    count, edge=200, origin=200, method="GET", path="/a", host="example.org", source=None
):
    dimensions = {
        "clientRequestHTTPHost": host,
        "clientRequestHTTPMethodName": method,
        "clientRequestPath": path,
        "edgeResponseStatus": edge,
        "originResponseStatus": origin,
    }
    if source:
        dimensions["requestSource"] = source
    return requests_group(count, **dimensions)


def by_labels(collector, name, *labels):
    return {tuple(s.labels[label] for label in labels): s.value for s in samples(collector, name)}


def availability(collector):
    return by_labels(collector, AVAILABLE, "zone_name", "dataset")


def test_edge_family_keeps_label_order_and_every_client_status(cloudflare, api, clock):
    cloudflare.add_zone(
        "z",
        "example.org",
        edge=[edge_group(i + 1, edge, origin) for i, (edge, origin) in enumerate(STATUSES)],
    )
    collector = publish(api, clock, clock.value)
    assert script.EDGE_LABELS == (
        "zone_name",
        "host_name",
        "method_name",
        "path",
        "edge_response_status",
        "origin_response_status",
    )
    family = next(family for family in collector.collect() if family.name == EDGE)
    assert all(tuple(sample.labels) == script.EDGE_LABELS for sample in family.samples)
    assert by_labels(collector, EDGE, "edge_response_status", "origin_response_status") == {
        (str(edge), str(origin)): i + 1 for i, (edge, origin) in enumerate(STATUSES)
    }
    assert checked_edge_family(exposition(collector)) == {"example.org": len(STATUSES)}


def test_edge_counts_client_requests_only_and_other_datasets_keep_every_source(
    cloudflare, api, clock
):
    cloudflare.add_zone(
        "z",
        "example.org",
        edge=[
            edge_group(5, 200, 0),
            edge_group(4, 522, 0, method="POST", path="/private/upload"),
            edge_group(3, 204, 204, method="OPTIONS", path="/unmatched"),
            edge_group(7, 200, 200, source="edgeWorkerFetch"),
            edge_group(2, 200, 0, source="earlyHintsCache"),
        ],
        cache=[
            requests_group(
                6,
                clientRequestHTTPHost="example.org",
                clientRequestHTTPMethodName="GET",
                clientRequestPath="/internal",
                cacheStatus="dynamic",
                originResponseStatus=200,
                requestSource="edgeWorkerFetch",
            )
        ],
    )
    collector = publish(api, clock, clock.value)
    assert by_labels(collector, EDGE, "method_name", "path", "edge_response_status") == {
        ("GET", "/a", "200"): 5,
        ("POST", "/private/upload", "522"): 4,
        ("OPTIONS", "/unmatched", "204"): 3,
    }
    # The existing datasets keep their traffic scope, including subrequests.
    assert by_labels(collector, "cf_cache_requests", "path") == {("/internal",): 6}
    selections = [
        (kind, filters)
        for call in cloudflare.data_calls()
        for kind, filters in SELECTION.findall(call["query"])
    ]
    assert {kind for kind, _ in selections} == {*script.DATASETS, "totals"}
    assert all((EYEBALL in filters) == (kind == "edge") for kind, filters in selections)
    assert all("requestSource" not in filters for kind, filters in selections if kind != "edge")


@pytest.mark.parametrize("missing", ["dimensions_edgeResponseStatus", "dimensions_requestSource"])
def test_unsupported_plan_reports_edge_unavailable_and_keeps_the_other_datasets(
    cloudflare, api, clock, caplog, missing
):
    cloudflare.add_zone(
        "z",
        "example.org",
        fields=ALL_FIELDS - {missing},
        requests=[requests_group(4, clientRequestPath="/a")],
        edge=[edge_group(5)],
    )
    cloudflare.add_zone("y", "example.net", edge=[edge_group(2, host="example.net")])
    collector = publish(api, clock, clock.value)
    unsupported = [c for c in cloudflare.data_calls() if c["variables"]["zoneTag"] == "z"]
    assert unsupported and all("edge:" not in call["query"] for call in unsupported)
    assert availability(collector) == {
        **{("example.org", kind): 1 for kind in ("visits", "requests", "cache")},
        ("example.org", "edge"): 0,
        **{("example.net", kind): 1 for kind in script.DATASETS},
    }
    assert set(by_labels(collector, EDGE, "zone_name")) == {("example.net",)}
    for name in ("cf_exporter_groups", "cf_exporter_group_limit_reached"):
        assert ("example.org", "edge") not in by_labels(collector, name, "zone_name", "dataset")
    assert by_labels(collector, "cf_requests", "zone_name") == {("example.org",): 4}
    assert set(by_labels(collector, "cf_exporter_zone_scrape_success", "zone_name").values()) == {1}
    assert samples(collector, "cf_exporter_last_cycle_success")[0].value == 1
    assert f"Zone example.org: plan does not offer {missing}" in caplog.text
    assert checked_edge_family(exposition(collector)) == {"example.net": 1}


@pytest.mark.parametrize("failure", ["analytics", "settings"])
def test_api_error_is_not_reported_as_missing_support(cloudflare, api, clock, failure):
    cloudflare.add_zone("z", "example.org", edge=[edge_group(5)])
    collector = publish(api, clock, clock.value)
    assert availability(collector)[("example.org", "edge")] == 1
    if failure == "analytics":
        cloudflare.zones["z"]["error"] = "unavailable"
    else:
        cloudflare.zones["z"]["enabled"] = False
    publish(api, clock, clock.value + timedelta(minutes=5), collector)
    assert not samples(collector, AVAILABLE)
    assert not samples(collector, EDGE)
    assert not samples(collector, WINDOW)
    assert samples(collector, "cf_exporter_zone_scrape_success")[0].value == 0
    cloudflare.zones["z"].update(error=None, enabled=True)
    cloudflare.zones["z"]["edge"] = [edge_group(6)]
    publish(api, clock, clock.value + timedelta(minutes=30), collector)
    assert availability(collector)[("example.org", "edge")] == 1
    assert [sample.value for sample in samples(collector, EDGE)] == [6]


@pytest.mark.parametrize("count", [0, 5])
@pytest.mark.parametrize("status", ["absent", None, True, False, 200.5, "200", -1, 65536])
def test_invalid_edge_status_fails_the_zone(cloudflare, api, clock, status, count):
    group = edge_group(count)
    if status == "absent":
        del group["dimensions"]["edgeResponseStatus"]
    else:
        group["dimensions"]["edgeResponseStatus"] = status
    cloudflare.add_zone("z", "example.org", edge=[group])
    collector = publish(api, clock, clock.value)
    assert samples(collector, "cf_exporter_zone_scrape_success")[0].value == 0
    assert not samples(collector, EDGE)
    assert not samples(collector, AVAILABLE)


@pytest.mark.parametrize("location", ["parent", "probe"])
def test_invalid_status_in_discarded_group_still_fails_the_zone(cloudflare, api, clock, location):
    group = edge_group(1, path="/b")
    del group["dimensions"]["edgeResponseStatus"]
    cloudflare.add_zone("z", "example.org", edge=[edge_group(5), group], max_page_size=2)
    if location == "probe":
        api.group_limit = 1
    collector = publish(api, clock, clock.value)
    assert samples(collector, "cf_exporter_zone_scrape_success")[0].value == 0
    assert not samples(collector, EDGE)
    assert not samples(collector, WINDOW)
    assert len(cloudflare.data_calls()) == 1


@pytest.mark.parametrize("status", [True, 200.5, "200", -1, 65536])
def test_malformed_origin_status_in_edge_data_fails_the_zone(cloudflare, api, clock, status):
    cloudflare.add_zone("z", "example.org", edge=[edge_group(3, origin=status)])
    collector = publish(api, clock, clock.value)
    assert samples(collector, "cf_exporter_zone_scrape_success")[0].value == 0
    assert not samples(collector, EDGE)


def test_strict_edge_types_do_not_change_legacy_label_conversion(cloudflare, api, clock):
    dimensions = {
        "clientRequestHTTPHost": 42,
        "clientRequestHTTPMethodName": True,
        "clientRequestPath": False,
        "originResponseStatus": "200",
    }
    cloudflare.add_zone(
        "z",
        "example.org",
        visits=[visits_group(1, **dimensions)],
        requests=[requests_group(2, **dimensions)],
        cache=[requests_group(3, **dimensions)],
    )
    collector = publish(api, clock, clock.value)
    assert samples(collector, "cf_exporter_zone_scrape_success")[0].value == 1
    for name in ("cf_visits", "cf_requests", "cf_cache_requests"):
        (sample,) = samples(collector, name)
        assert sample.labels["host_name"] == "42"
        assert sample.labels["path"] == "False"
        if name != "cf_visits":
            assert sample.labels["method_name"] == "True"
            assert sample.labels["origin_response_status"] == "200"


@pytest.mark.parametrize("defect", ["missing", "type"])
def test_dimension_validation_uses_the_dataset_policy(cloudflare, api, clock, monkeypatch, defect):
    monkeypatch.setitem(script.DIMENSION_TYPES, "cache", {"originResponseStatus": int})
    monkeypatch.setitem(script.REQUIRED_DIMENSIONS, "cache", frozenset({"originResponseStatus"}))
    group = requests_group(3, **({} if defect == "missing" else {"originResponseStatus": "200"}))
    cloudflare.add_zone("z", "example.org", cache=[group])
    collector = publish(api, clock, clock.value)
    assert samples(collector, "cf_exporter_zone_scrape_success")[0].value == 0
    assert not samples(collector, "cf_cache_requests")


@pytest.mark.parametrize("cap", [0, 1])
def test_edge_groups_are_validated_once_before_aggregation(
    cloudflare, api, clock, monkeypatch, cap
):
    cloudflare.add_zone("z", "example.org", edge=[edge_group(3), edge_group(2, 404, 0)])
    api.group_limit = cap
    value_checks, dimension_checks = [], []
    original_value, original_dimensions = script.checked_value, script._group_dimensions

    def checked_value(kind, group, value_of):
        value_checks.append(kind)
        return original_value(kind, group, value_of)

    def checked_dimensions(kind, group, available):
        dimension_checks.append(kind)
        return original_dimensions(kind, group, available)

    monkeypatch.setattr(script, "checked_value", checked_value)
    monkeypatch.setattr(script, "_group_dimensions", checked_dimensions)
    collector = publish(api, clock, clock.value)
    assert len(samples(collector, EDGE)) == (cap or 2)
    assert value_checks.count("edge") == dimension_checks.count("edge") == 2


def test_uint16_upper_bound_is_preserved_in_rendering_and_smoke(cloudflare, api, clock):
    cloudflare.add_zone("z", "example.org", edge=[edge_group(3, edge=65535, origin=65535)])
    collector = publish(api, clock, clock.value)
    (sample,) = samples(collector, EDGE)
    assert (
        sample.labels["edge_response_status"] == sample.labels["origin_response_status"] == "65535"
    )
    assert script.UINT16_MAX == image_smoke.UINT16_MAX == 65535
    assert checked_edge_family(exposition(collector)) == {"example.org": 1}


def test_always_collected_dataset_validation_survives_reordering(
    cloudflare, api, clock, monkeypatch
):
    cloudflare.add_zone("z", "example.org", fields=ALL_FIELDS - {"dimensions_requestSource"})
    collector = publish(api, clock, clock.value)
    monkeypatch.setattr(image_smoke, "DATASET_NAMES", tuple(reversed(image_smoke.DATASET_NAMES)))
    assert checked_edge_family(exposition(collector)) == {}
    body = exposition(collector)
    flag = next(
        line
        for line in body.splitlines()
        if line.startswith(AVAILABLE + "{") and 'dataset="visits"' in line
    )
    body = body.replace(flag, flag.rsplit(" ", 1)[0] + " 0.0")
    with pytest.raises(ValueError, match="always collected"):
        checked_edge_family(body)


@pytest.mark.parametrize(
    "dimension", ["clientRequestHTTPHost", "clientRequestHTTPMethodName", "clientRequestPath"]
)
@pytest.mark.parametrize("value", [True, 42, [], {}])
def test_malformed_edge_string_dimensions_fail_the_zone(cloudflare, api, clock, dimension, value):
    group = edge_group(3)
    group["dimensions"][dimension] = value
    cloudflare.add_zone("z", "example.org", edge=[group])
    collector = publish(api, clock, clock.value)
    assert samples(collector, "cf_exporter_zone_scrape_success")[0].value == 0
    assert not samples(collector, EDGE)


@pytest.mark.parametrize("supported", [True, False])
def test_optional_edge_dimensions_remain_unknown(cloudflare, api, clock, supported):
    optional = {
        "clientRequestHTTPHost",
        "clientRequestHTTPMethodName",
        "clientRequestPath",
        "originResponseStatus",
    }
    fields = ALL_FIELDS if supported else ALL_FIELDS - {f"dimensions_{d}" for d in optional}
    cloudflare.add_zone(
        "z",
        "example.org",
        fields=fields,
        edge=[requests_group(3, edgeResponseStatus=0, **{d: None for d in optional})],
    )
    collector = publish(api, clock, clock.value)
    (sample,) = samples(collector, EDGE)
    assert sample.value == 3
    assert sample.labels == dict(
        zip(
            script.EDGE_LABELS,
            ("example.org", "unknown", "unknown", "unknown", "0", "unknown"),
            strict=True,
        )
    )
    edge_selection = cloudflare.data_calls()[0]["query"].split("edge:", 1)[1]
    assert all((dimension in edge_selection) == supported for dimension in optional)
    assert checked_edge_family(exposition(collector)) == {"example.org": 1}


def test_edge_groups_keep_hosts_methods_paths_and_both_statuses_separate(cloudflare, api, clock):
    groups = [
        edge_group(2, 200, 0, host="one.example.org"),
        edge_group(3, 200, 200, host="one.example.org"),
        edge_group(5, 522, 0, host="one.example.org"),
        edge_group(7, 200, 0, host="two.example.org"),
        edge_group(11, 200, 0, host="one.example.org", method="POST"),
        edge_group(13, 200, 0, host="one.example.org", path="/other"),
    ]
    cloudflare.add_zone("z", "example.org", edge=groups)
    collector = publish(api, clock, clock.value)
    assert by_labels(collector, EDGE, *script.EDGE_LABELS) == {
        (
            "example.org",
            *(str(group["dimensions"][dimension]) for dimension in script.EDGE_DIMENSIONS.values()),
        ): group["count"]
        for group in groups
    }
    assert checked_edge_family(exposition(collector)) == {"example.org": 6}


def test_edge_support_can_disappear_and_recover_without_stale_series(
    cloudflare, api, clock, caplog
):
    cloudflare.add_zone("z", "example.org", edge=[edge_group(3)])
    collector = publish(api, clock, clock.value)
    cloudflare.zones["z"]["fields"] = ALL_FIELDS - {"dimensions_requestSource"}
    caplog.clear()
    for _ in range(2):
        publish(api, clock, clock.value + timedelta(minutes=5), collector)
        assert availability(collector)[("example.org", "edge")] == 0
        assert not samples(collector, EDGE)
        assert samples(collector, WINDOW)
        assert checked_edge_family(exposition(collector)) == {}
    assert caplog.text.count("the edge dataset is not collected") == 2
    cloudflare.zones["z"]["fields"] = ALL_FIELDS
    cloudflare.zones["z"]["edge"] = [edge_group(4, 522, 0)]
    publish(api, clock, clock.value + timedelta(minutes=5), collector)
    assert checked_edge_family(exposition(collector)) == {"example.org": 1}
    assert [sample.value for sample in samples(collector, EDGE)] == [4]


def test_successful_empty_zone_reports_zero_groups_not_missing_data(cloudflare, api, clock):
    cloudflare.add_zone("z", "example.org")
    collector = None
    # Startup during a day without traffic, then the empty window at midnight.
    for moment in (clock.value, DAY + timedelta(days=1)):
        collector = publish(api, clock, moment, collector)
        assert not samples(collector, EDGE)
        for name, expected in (
            (AVAILABLE, 1),
            ("cf_exporter_groups", 0),
            ("cf_exporter_group_limit_reached", 0),
        ):
            values = by_labels(collector, name, "zone_name", "dataset")
            assert values[("example.org", "edge")] == expected
        (window,) = samples(collector, WINDOW)
        assert window.labels["window_start"] == script.rfc3339(moment.replace(hour=0))
    assert checked_edge_family(exposition(collector)) == {"example.org": 0}


def test_edge_family_follows_the_snapshot_window(cloudflare, api, clock):
    cloudflare.add_zone("z", "example.org", edge=[edge_group(3)])
    midnight = DAY + timedelta(days=1)
    collector = publish(api, clock, midnight - timedelta(seconds=40))
    other = publish(api, clock, midnight - timedelta(seconds=30))
    # Independent instances report the same daily estimate, not process counts.
    assert samples(collector, EDGE)[0].value == samples(other, EDGE)[0].value == 3
    assert samples(collector, WINDOW)[0].labels == samples(other, WINDOW)[0].labels
    cloudflare.zones["z"]["edge"] = [edge_group(2)]
    publish(api, clock, midnight - timedelta(seconds=20), collector)
    assert [sample.value for sample in samples(collector, EDGE)] == [2]
    clock.value = midnight + timedelta(seconds=1)
    assert not samples(collector, EDGE)
    assert not samples(collector, AVAILABLE)
    assert samples(collector, "cf_exporter_zone_scrape_success")[0].value == 1
    # A new day starting at a higher value is told apart by its window alone.
    cloudflare.zones["z"]["edge"] = [edge_group(5)]
    publish(api, clock, midnight + timedelta(minutes=2), collector)
    assert samples(collector, WINDOW)[0].labels["window_start"] == script.rfc3339(midnight)
    assert [sample.value for sample in samples(collector, EDGE)] == [5]


def test_edge_and_its_dataset_health_come_from_one_snapshot(cloudflare, api, clock):
    cloudflare.add_zone("z", "example.org", edge=[edge_group(3)])
    collector = publish(api, clock, clock.value)
    families = collector.collect()
    window = next(family for family in families if family.name == WINDOW)
    cloudflare.zones["z"]["error"] = "unavailable"
    publish(api, clock, clock.value + timedelta(minutes=5), collector)
    rest = {family.name: family for family in families}
    assert window.samples[0].labels["zone_name"] == "example.org"
    assert [sample.value for sample in rest[EDGE].samples] == [3]
    assert {s.labels["dataset"]: s.value for s in rest[AVAILABLE].samples}["edge"] == 1
    assert not samples(collector, EDGE)


@pytest.mark.parametrize(
    "defect",
    [
        "labels",
        "status",
        "range",
        "nan",
        "inf",
        "duplicate",
        "unavailable",
        "missing",
        "always",
        "groups",
        "fractional_groups",
        "truncation",
        "missing_legacy_health",
        "health",
    ],
)
def test_smoke_validation_checks_edge_samples_and_dataset_health(cloudflare, api, clock, defect):
    cloudflare.add_zone("z", "example.org", edge=[edge_group(3), edge_group(1, 404, 404)])
    cloudflare.add_zone("y", "example.net", fields=ALL_FIELDS - {"dimensions_requestSource"})
    collector = publish(api, clock, clock.value)
    body = exposition(collector)
    assert checked_edge_family(body) == {"example.org": 2}
    lines = body.splitlines()

    def line(prefix, *parts):
        return next(
            item for item in lines if item.startswith(prefix) and all(p in item for p in parts)
        )

    edge = line(EDGE + "{", 'edge_response_status="404"')
    if defect == "labels":
        body = body.replace(edge, edge.replace("method_name", "method"))
    elif defect in ("status", "range"):
        invalid_status = "x" if defect == "status" else "65536"
        body = body.replace(
            edge,
            edge.replace('edge_response_status="404"', f'edge_response_status="{invalid_status}"'),
        )
    elif defect in ("nan", "inf"):
        body = body.replace(edge, edge.rsplit(" ", 1)[0] + (" NaN" if defect == "nan" else " +Inf"))
    elif defect == "duplicate":
        body += edge + "\n"
    elif defect == "unavailable":
        flag = line(AVAILABLE + "{", 'dataset="edge"', 'zone_name="example.org"')
        body = body.replace(flag, flag.rsplit(" ", 1)[0] + " 0.0")
    elif defect == "missing":
        body = body.replace(line(AVAILABLE + "{", 'dataset="edge"', "example.net") + "\n", "")
    elif defect == "always":
        flag = line(AVAILABLE + "{", 'dataset="cache"', "example.net")
        body = body.replace(flag, flag.rsplit(" ", 1)[0] + " 0.0")
    elif defect == "groups":
        count = line("cf_exporter_groups{", 'dataset="edge"', "example.org")
        body = body.replace(count, count.rsplit(" ", 1)[0] + " 5.0")
    elif defect == "fractional_groups":
        count = line("cf_exporter_groups{", 'dataset="cache"', "example.org")
        body = body.replace(count, count.rsplit(" ", 1)[0] + " 0.5")
    elif defect == "truncation":
        flag = line("cf_exporter_group_limit_reached{", 'dataset="edge"', "example.org")
        body = body.replace(flag, flag.rsplit(" ", 1)[0] + " 2.0")
    elif defect == "missing_legacy_health":
        body = body.replace(
            line("cf_exporter_groups{", 'dataset="cache"', "example.org") + "\n", ""
        )
    else:
        # A zero count for an unsupported dataset would look like zero traffic.
        body += 'cf_exporter_groups{dataset="edge",zone_name="example.net"} 0.0\n'
    with pytest.raises(ValueError):
        checked_edge_family(body)


def test_reference_source_filter_is_independent_and_preserves_sources(monkeypatch):
    monkeypatch.setitem(script.FILTERED_DATASETS, "edge", 'requestSource: "incorrect"')
    calls = []

    def graphql(query, variables):
        calls.append((query, variables))
        return {
            "viewer": {
                "zones": [
                    {
                        "eyeball": [{"count": 5}],
                        "sources": [
                            {"count": 5, "dimensions": {"requestSource": "eyeball"}},
                            {"count": 2, "dimensions": {"requestSource": "edgeWorkerFetch"}},
                        ],
                    }
                ]
            }
        }

    assert request_sources(graphql, "z", DAY, DAY + timedelta(hours=1)) == (
        5,
        {"eyeball": 5, "edgeWorkerFetch": 2},
    )
    assert EYEBALL in calls[0][0] and "incorrect" not in calls[0][0]
    assert calls[0][1] == {
        "zoneTag": "z",
        "since": script.rfc3339(DAY),
        "until": script.rfc3339(DAY + timedelta(hours=1)),
    }
    assert request_sources(graphql, "z", DAY, DAY) == (0, {})
    assert len(calls) == 1


@pytest.mark.parametrize(
    "node",
    [
        {},
        {"eyeball": None, "sources": []},
        {"eyeball": [{"count": 0}, {"count": 0}], "sources": []},
        {"eyeball": [{"count": True}], "sources": []},
        {"eyeball": [], "sources": None},
        {"eyeball": [], "sources": [{"count": -1, "dimensions": {"requestSource": "eyeball"}}]},
        {"eyeball": [], "sources": [{"count": 1, "dimensions": []}]},
        {"eyeball": [], "sources": [{"count": 1, "dimensions": {"requestSource": ""}}]},
        {"eyeball": [], "sources": [{"count": 1, "dimensions": {"requestSource": "eyeball"}}] * 2},
    ],
)
def test_reference_source_validation_rejects_malformed_responses(node):
    with pytest.raises(script.CloudflareAPIError):
        request_sources(
            lambda *args: {"viewer": {"zones": [node]}}, "z", DAY, DAY + timedelta(hours=1)
        )


@pytest.mark.parametrize(
    "visits,requests,eyeball",
    [
        (2, 7, 5),
        ({"example.org": 2}, {"example.org": 7}, {"example.org": 5}),
    ],
)
def test_expected_totals_keep_each_dataset_traffic_scope(visits, requests, eyeball):
    assert expected_totals(visits, requests, eyeball) == {
        "visits": visits,
        "requests": requests,
        "cache": requests,
        "edge": eyeball,
    }


@pytest.mark.parametrize(
    "eyeball,sources,requests,completed_day,expected",
    [
        (0, {}, 0, True, True),
        (5, {"eyeball": 5, "edgeWorkerFetch": 2}, 7, True, True),
        (0, {"edgeWorkerFetch": 7}, 7, True, True),
        (0, {}, 7, True, False),
        (5, {"renamed": 5}, 7, True, False),
        # Valid for one zone; a renamed value is caught across zones below.
        (0, {"renamed": 7}, 7, True, True),
        (4, {"eyeball": 5}, 7, True, False),
        (5, {"eyeball": 5}, 4, True, False),
        (4, {"eyeball": 5}, 7, False, True),
        (5, {"eyeball": 5}, 4, False, True),
        (0, {"edgeWorkerFetch": 7}, 7, False, True),
        (0, {}, 7, False, False),
    ],
)
def test_source_validation_distinguishes_completed_days_from_ingestion_drift(
    eyeball, sources, requests, completed_day, expected
):
    assert source_filter_valid(eyeball, sources, requests, completed_day=completed_day) is expected


@pytest.mark.parametrize(
    "checked,expected",
    [
        ([], True),
        ([(0, {})], True),
        ([(7, {"eyeball": 5, "edgeWorkerFetch": 2})], True),
        # One zone with only subrequests among zones with client requests.
        ([(7, {"edgeWorkerFetch": 7}), (5, {"eyeball": 5})], True),
        ([(7, {"edgeWorkerFetch": 7}), (0, {})], False),
        # A renamed value leaves the filter and its reference empty everywhere.
        ([(7, {"renamed": 7}), (5, {"renamed": 5})], False),
    ],
)
def test_source_value_must_occur_in_some_zone_with_traffic(checked, expected):
    assert source_value_present(checked) is expected
    assert source_value_present(iter(checked)) is expected


def test_profiler_tracks_new_zones_and_skips_unsupported_source_queries(
    cloudflare, api, clock, monkeypatch, capsys
):
    def add_zone(zone_id, name, count, fields=ALL_FIELDS):
        cloudflare.add_zone(
            zone_id,
            name,
            fields=fields,
            requests=[requests_group(count)],
            cache=[requests_group(count)],
            edge=[edge_group(count, host=name)],
        )

    add_zone("z", "example.org", 3)
    add_zone("unsupported", "example.net", 2, ALL_FIELDS - {"dimensions_requestSource"})
    calls = []

    def reference(graphql, zone_id, since, until):
        calls.append(zone_id)
        assert zone_id != "unsupported"
        if len(calls) == 1:
            # Discovery during the next cycle must supply this new zone's ID.
            add_zone("new", "example.com", 4)
        count = 4 if zone_id == "new" else 3
        return count, {"eyeball": count}

    monkeypatch.setattr(script, "CloudflareAPI", lambda *args, **kwargs: api)
    monkeypatch.setattr(script, "parse_env", lambda: script.Config("dummy", 5, 300, 8000))
    monkeypatch.setattr(profile_live, "request_sources", reference)
    monkeypatch.setattr("sys.argv", ["profile_live.py", "--cycles", "2"])
    assert profile_live.main() == 0
    assert calls == ["z", "z", "new"]
    reports = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [report["zones"] for report in reports] == [2, 3]
    assert all(report["edge_unavailable_zones"] == 1 for report in reports)
    assert all(report["totals_match"] and report["source_filter_valid"] for report in reports)
