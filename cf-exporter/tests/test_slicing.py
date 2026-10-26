# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Adaptive fetching against a fake API with actual half-open time filtering."""

import json
import re
from datetime import UTC, datetime, timedelta

import pytest
from conftest import (
    ALL_FIELDS,
    SELECTION,
    eyeball_only,
    graphql_error,
    json_response,
    requests_group,
    visits_group,
)
from prometheus_client import generate_latest

import script

START = datetime(2026, 10, 26, tzinfo=UTC)
END = START + timedelta(minutes=4)


@pytest.fixture
def traffic(cloudflare):
    # Boundary events at 60/120/180s must be counted exactly once. Path /a
    # repeats in distinct slices and must be summed, rather than overwritten.
    events = [(0, "/a", 2), (60, "/b", 3), (120, "/a", 5), (180, "/c", 4), (240, "/excluded", 99)]

    def respond(body):
        start = datetime.fromisoformat(body["variables"]["since"])
        end = datetime.fromisoformat(body["variables"]["until"])
        node = {}
        # Every selection applies its own filter; an event may name its request source.
        for kind, filters in SELECTION.findall(body["query"]):
            counts = {}
            for second, path, count, *source in cloudflare.zones["z"]["events"]:
                group = {"dimensions": {"requestSource": source[0]}} if source else {}
                if start <= START + timedelta(seconds=second) < end and eyeball_only(
                    filters, group
                ):
                    counts[path] = counts.get(path, 0) + count
            ranked = sorted(counts.items(), key=lambda pair: pair[1], reverse=True)
            if kind == "totals":
                visits = sum(counts.values()) if cloudflare.zones["z"].get("visits_enabled") else 0
                node[kind] = [{"count": sum(counts.values()), "sum": {"visits": visits}}]
            elif kind == "visits":
                node[kind] = (
                    [visits_group(count, clientRequestPath=path) for path, count in ranked][
                        : body["variables"]["limit"]
                    ]
                    if cloudflare.zones["z"].get("visits_enabled")
                    else []
                )
            else:
                status = {"edgeResponseStatus": 200} if kind == "edge" else {}
                node[kind] = [
                    requests_group(count, clientRequestPath=path, **status)
                    for path, count in ranked
                ][: body["variables"]["limit"]]
        return json_response(200, {"data": {"viewer": {"zones": [node]}}})

    cloudflare.add_zone("z", "example.org", max_page_size=2, responder=respond)
    cloudflare.zones["z"]["events"] = events
    return cloudflare


def paths(samples, label_names):
    return {key[label_names.index("path")]: value for key, value in samples.items()}


def test_split_merge_and_half_open_boundaries(traffic, api):
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert paths(result.requests, script.REQUESTS_LABELS) == {"/a": 7, "/b": 3, "/c": 4}
    assert paths(result.cache, script.CACHE_LABELS) == {"/a": 7, "/b": 3, "/c": 4}
    assert paths(result.edge, script.EDGE_LABELS) == {"/a": 7, "/b": 3, "/c": 4}
    assert result.total_requests == 14
    assert result.total_visits == 0
    assert result.limited == {"visits": 0, "requests": 0, "cache": 0, "edge": 0}
    first, *retries = traffic.data_calls()
    assert all(f"{kind}:" in first["query"] for kind in (*script.DATASETS, "totals"))
    assert retries
    assert all(
        "visits:" not in call["query"] and "totals:" not in call["query"] for call in retries
    )
    assert all(call["variables"]["limit"] == 2 for call in traffic.data_calls())
    assert all("datetime_geq: $since, datetime_lt: $until" in call["query"] for call in retries)


def test_hint_avoids_full_day_retry_and_resets_at_midnight(traffic, api):
    script.collect_zone(api, "z", "example.org", START, END)
    assert api.slice_hints[("z", "requests")] == 4
    assert api.slice_hints[("z", "edge")] == 4
    traffic.graphql_calls.clear()
    script.collect_zone(api, "z", "example.org", START, END)
    assert len(traffic.data_calls()) == 5
    assert "requests:" not in traffic.data_calls()[0]["query"]
    assert "cache:" not in traffic.data_calls()[0]["query"]
    assert "edge:" not in traffic.data_calls()[0]["query"]
    traffic.graphql_calls.clear()
    script.collect_zone(api, "z", "example.org", START + timedelta(days=1), END + timedelta(days=1))
    assert len(traffic.data_calls()) == 1
    assert api.slice_hints[("z", "requests")] == 1
    assert api.slice_hints[("z", "edge")] == 1


def test_eyeball_filter_is_applied_to_every_edge_slice(traffic, api):
    # Subrequests stay in the existing datasets but never reach the edge dataset.
    traffic.zones["z"]["events"] = [
        (0, "/a", 2),
        (30, "/a", 50, "edgeWorkerFetch"),
        (60, "/b", 3),
        (120, "/a", 5),
        (150, "/w", 9, "earlyHintsCache"),
        (180, "/c", 4),
    ]
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert paths(result.edge, script.EDGE_LABELS) == {"/a": 7, "/b": 3, "/c": 4}
    assert paths(result.cache, script.CACHE_LABELS) == {"/a": 57, "/b": 3, "/c": 4, "/w": 9}
    assert sum(result.cache.values()) == result.total_requests == 73
    assert result.limited["edge"] == 0
    edge = [
        (call["variables"]["since"], filters)
        for call in traffic.data_calls()
        for kind, filters in SELECTION.findall(call["query"])
        if kind == "edge"
    ]
    assert len({since for since, _ in edge}) > 1
    assert all('requestSource: "eyeball"' in filters for _, filters in edge)
    assert api.slice_hints[("z", "edge")] > 1
    # A hinted cycle starts with slices that keep the filter as well.
    traffic.graphql_calls.clear()
    assert script.collect_zone(api, "z", "example.org", START, END).edge == result.edge
    assert all(
        'requestSource: "eyeball"' in filters
        for call in traffic.data_calls()
        for kind, filters in SELECTION.findall(call["query"])
        if kind == "edge"
    )


def test_positive_visits_are_split_and_summed(traffic, api):
    traffic.zones["z"]["visits_enabled"] = True
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert paths(result.visits, script.VISITS_LABELS) == {"/a": 7, "/b": 3, "/c": 4}
    assert result.total_visits == 14
    assert result.limited["visits"] == 0


def test_recursive_full_slice_stops_at_one_minute(traffic, api):
    traffic.zones["z"]["events"] = [(0, "/a", 5), (10, "/b", 3), (20, "/c", 1)]
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert paths(result.requests, script.REQUESTS_LABELS) == {"/a": 5, "/b": 3}
    assert result.total_requests == 9
    assert result.limited["requests"] == result.limited["cache"] == 1
    assert paths(result.edge, script.EDGE_LABELS) == {"/a": 5, "/b": 3}
    assert result.limited["edge"] == 1
    durations = [
        (
            datetime.fromisoformat(call["variables"]["until"])
            - datetime.fromisoformat(call["variables"]["since"])
        ).total_seconds()
        for call in traffic.data_calls()
    ]
    assert min(durations) == 60
    assert 120 in durations and 240 in durations


def requests_calls(cloudflare):
    return [call for call in cloudflare.data_calls() if "requests:" in call["query"]]


def test_slice_budget_bounds_requests_and_reports_truncation(traffic, api, monkeypatch, caplog):
    # /x is lost once the budget forbids splitting the full first half.
    monkeypatch.setattr(script, "MAX_SLICE_REQUESTS", 3)
    traffic.zones["z"]["events"] = [
        (0, "/a", 2),
        (10, "/x", 1),
        (60, "/b", 3),
        (120, "/a", 5),
        (180, "/c", 4),
    ]
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert len(requests_calls(traffic)) == 3
    assert len([call for call in traffic.data_calls() if "edge:" in call["query"]]) == 3
    assert paths(result.requests, script.REQUESTS_LABELS) == {"/a": 7, "/b": 3, "/c": 4}
    assert result.total_requests == 15
    assert result.limited["requests"] == result.limited["cache"] == 1
    assert result.limited["edge"] == 1
    assert "slice budget" in caplog.text
    assert api.slice_hints[("z", "requests")] == 2
    assert api.slice_hints[("z", "edge")] == 2


def test_slice_hint_is_capped_by_budget(traffic, api, monkeypatch):
    monkeypatch.setattr(script, "MAX_SLICE_REQUESTS", 3)
    api.hint_day = START
    api.slice_hints[("z", "requests")] = 100
    script.collect_zone(api, "z", "example.org", START, END)
    assert len(requests_calls(traffic)) == 3


def test_partial_minute_tail_is_not_lost(traffic, api):
    result = script.collect_zone(api, "z", "example.org", START, START + timedelta(seconds=61))
    assert paths(result.requests, script.REQUESTS_LABELS) == {"/a": 2, "/b": 3}
    assert result.total_requests == 5
    assert result.limited["requests"] == 0
    assert paths(result.edge, script.EDGE_LABELS) == {"/a": 2, "/b": 3}
    assert result.limited["edge"] == 0


@pytest.mark.parametrize("value", [None, "", "0", " 0 ", "15000"])
def test_group_limit_configuration(value):
    env = {"CF_EXPORTER_API_TOKEN": "dummy"}
    if value is not None:
        env["CF_EXPORTER_GROUP_LIMIT"] = value
    assert script.parse_env(env).group_limit == (15000 if value == "15000" else 0)


def test_limit_below_page_size_uses_one_query(traffic, api, caplog):
    api.group_limit = 1
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert paths(result.requests, script.REQUESTS_LABELS) == {"/a": 7}
    assert paths(result.edge, script.EDGE_LABELS) == {"/a": 7}
    assert result.limited == {"visits": 0, "requests": 1, "cache": 1, "edge": 1}
    assert len(traffic.data_calls()) == 1
    # One group beyond the cap shows whether the cap cut anything.
    assert traffic.data_calls()[0]["variables"]["limit"] == 2
    assert "configured cap" in caplog.text


@pytest.mark.parametrize(
    "limit, expected, limited",
    [(3, {"/a": 7, "/c": 4, "/b": 3}, 0), (4, {"/a": 7, "/c": 4, "/b": 3}, 0)],
)
def test_limit_above_page_size_slices_then_selects_top(traffic, api, limit, expected, limited):
    # The globally largest group /a is the sum of values from separate slices.
    result = script.collect_zone(api, "z", "example.org", START, END, limit=limit)
    assert paths(result.requests, script.REQUESTS_LABELS) == expected
    assert result.limited["requests"] == limited
    assert len(traffic.data_calls()) > 1


def test_limit_above_page_size_trims_merged_groups(traffic, api):
    # Include the event at 240s, so there are four distinct label sets.
    result = script.collect_zone(
        api, "z", "example.org", START, END + timedelta(seconds=60), limit=3
    )
    assert paths(result.requests, script.REQUESTS_LABELS) == {"/excluded": 99, "/a": 7, "/c": 4}
    assert list(result.requests.values()) == [99, 7, 4]
    assert paths(result.edge, script.EDGE_LABELS) == {"/excluded": 99, "/a": 7, "/c": 4}
    assert list(result.edge.values()) == [99, 7, 4]
    assert result.limited["requests"] == result.limited["cache"] == 1


def test_zero_visits_tail_is_complete_and_not_split(cloudflare, api, caplog):
    cloudflare.add_zone(
        "z",
        "example.org",
        max_page_size=2,
        visits=[visits_group(5, clientRequestPath="/a"), visits_group(0, clientRequestPath="/b")],
    )
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert list(result.visits.values()) == [5]
    assert len(cloudflare.data_calls()) == 1
    assert result.limited["visits"] == 0
    assert not caplog.text


def test_full_minute_retains_page_and_reports_truncation(cloudflare, api, caplog):
    cloudflare.add_zone(
        "z",
        "example.org",
        max_page_size=2,
        requests=[
            requests_group(5, clientRequestPath="/a"),
            requests_group(3, clientRequestPath="/b"),
            requests_group(1, clientRequestPath="/c"),
        ],
    )
    result = script.collect_zone(api, "z", "example.org", START, START + timedelta(seconds=60))
    assert paths(result.requests, script.REQUESTS_LABELS) == {"/a": 5, "/b": 3}
    assert result.limited["requests"] == 1
    assert len(cloudflare.data_calls()) == 1
    assert "60s slice" in caplog.text


@pytest.mark.parametrize(
    "requests, visits, limited",
    [
        # Exactly the cap: the probe group is absent, nothing was cut.
        ([requests_group(5, clientRequestPath="/a")], [], {"requests": 0, "visits": 0}),
        # A nonzero group beyond the cap was cut.
        (
            [requests_group(5, clientRequestPath="/a"), requests_group(3, clientRequestPath="/b")],
            [],
            {"requests": 1, "visits": 0},
        ),
        # Only zero visits lie beyond the cap: complete.
        (
            [],
            [visits_group(5, clientRequestPath="/a"), visits_group(0, clientRequestPath="/b")],
            {"requests": 0, "visits": 0},
        ),
    ],
)
def test_configured_cap_is_checked_with_one_extra_group(cloudflare, api, requests, visits, limited):
    # Totals deliberately disagree: the decision must not depend on them.
    cloudflare.add_zone(
        "z",
        "example.org",
        max_page_size=10,
        requests=requests,
        visits=visits,
        totals=[{"count": 1000, "sum": {"visits": 1000}}],
    )
    result = script.collect_zone(api, "z", "example.org", START, END, limit=1)
    assert {kind: result.limited[kind] for kind in limited} == limited
    assert len(result.requests) == min(len(requests), 1)
    assert cloudflare.data_calls()[0]["variables"]["limit"] == 2


def test_cap_equal_to_the_page_size_is_reported_conservatively(cloudflare, api, caplog):
    # No room for a probe group: a full page may hide more groups.
    cloudflare.add_zone("z", "example.org", max_page_size=1, requests=[requests_group(5)])
    result = script.collect_zone(api, "z", "example.org", START, END, limit=1)
    assert result.limited["requests"] == 1
    assert "configured cap" in caplog.text


def test_current_day_total_drift_does_not_clear_a_real_cut(traffic, api, monkeypatch):
    # A later total that happens to equal the cut sum must not hide the cut.
    monkeypatch.setattr(script, "MAX_SLICE_REQUESTS", 3)
    traffic.zones["z"]["events"] = [(0, "/a", 2), (10, "/x", 1), (60, "/b", 3), (120, "/a", 5)]
    original = traffic.zones["z"]["responder"]

    def stale_totals(body):
        status, headers, payload = original(body)
        data = json.loads(payload)
        node = data["data"]["viewer"]["zones"][0]
        if "totals" in node:
            node["totals"][0]["count"] = 10  # Equals the exported sum 2 + 3 + 5.
        return status, headers, json.dumps(data)

    traffic.zones["z"]["responder"] = stale_totals
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert sum(result.requests.values()) == result.total_requests == 10
    assert result.limited["requests"] == 1


def test_cycle_budget_is_shared_by_all_zones(traffic, api, caplog):
    traffic.add_zone("y", "example.net", max_page_size=2, responder=traffic.zones["z"]["responder"])
    api.query_budget = 2 * script.BASELINE_SELECTIONS + 2
    snapshot = script.collect_once(api, now=END)
    selections = sum(
        len(re.findall(r"httpRequestsAdaptiveGroups\(", call["query"]))
        + ("settings" in call["query"])
        for call in traffic.graphql_calls
    )
    assert selections <= api.query_budget
    assert snapshot.zone_success == {"example.org": 1, "example.net": 1}
    # The second zone found no budget left for slicing and reports the cut.
    assert snapshot.limited[("example.net", "requests")] == 1
    assert snapshot.limited[("example.net", "edge")] == 1
    assert "slice budget" in caplog.text


def test_cycle_query_budget_scales_with_the_interval():
    assert script.cycle_query_budget(300) == 200
    assert script.cycle_query_budget(3600) == 200
    assert script.cycle_query_budget(60) == 40


def test_failure_in_child_discards_entire_zone(traffic, api):
    original = traffic.zones["z"]["responder"]

    def fail_child(body):
        if body["variables"]["since"] != script.rfc3339(START):
            return graphql_error("slice failed")
        return original(body)

    traffic.zones["z"]["responder"] = fail_child
    traffic.add_zone("ok", "ok.org")
    snapshot = script.collect_once(api, now=END)
    assert snapshot.zone_success == {"example.org": 0, "ok.org": 1}
    assert snapshot.cycle_success == 0
    assert snapshot.requests == snapshot.cache == {}
    assert snapshot.zone_requests == {"ok.org": 0}
    assert set(snapshot.groups) == {("ok.org", kind) for kind in script.DATASETS}
    assert ("z", "requests") not in api.slice_hints


def test_failure_in_edge_only_slice_discards_the_zone_not_other_zones(traffic, api):
    original = traffic.zones["z"]["responder"]

    def edge_only(body):
        if body["variables"]["since"] != script.rfc3339(START):
            assert {kind for kind, _ in SELECTION.findall(body["query"])} == {"edge"}
            return graphql_error("edge slice failed")
        status, headers, payload = original(body)
        data = json.loads(payload)
        node = data["data"]["viewer"]["zones"][0]
        for kind in ("visits", "requests", "cache"):
            if kind in node:
                node[kind] = []
        return status, headers, json.dumps(data)

    traffic.zones["z"]["responder"] = edge_only
    traffic.add_zone("ok", "example.net")
    snapshot = script.collect_once(api, now=END)
    assert snapshot.zone_success == {"example.org": 0, "example.net": 1}
    assert not snapshot.edge
    assert set(snapshot.available) == {("example.net", kind) for kind in script.DATASETS}
    assert ("z", "edge") not in api.slice_hints


@pytest.mark.parametrize("origin_supported", [True, False])
def test_cache_labels_unknown_dimensions_zero_filter_and_totals(cloudflare, api, origin_supported):
    fields = ALL_FIELDS if origin_supported else ALL_FIELDS - {"dimensions_originResponseStatus"}
    cloudflare.add_zone(
        "z",
        "example.org",
        fields=fields,
        cache=[
            requests_group(
                7,
                clientRequestHTTPHost="any.example.org",
                clientRequestHTTPMethodName="GET",
                clientRequestPath="/route",
                cacheStatus="bypass",
                originResponseStatus=200,
            ),
            requests_group(0),
        ],
        totals=[{"count": 100, "sum": {"visits": 20}}],
    )
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert result.cache == {
        (
            "example.org",
            "any.example.org",
            "GET",
            "/route",
            "bypass",
            "200" if origin_supported else "unknown",
        ): 7
    }
    assert result.total_requests == 100
    assert result.total_visits == 20
    query = cloudflare.data_calls()[0]["query"]
    assert ("originResponseStatus" in query) == origin_supported
    assert "clientRequestHTTPHost_in" not in query


@pytest.mark.parametrize(
    "totals",
    [
        None,
        {},
        [None],
        [{"count": 1}],
        [{"sum": {"visits": 2}}],
        [{"count": True, "sum": {"visits": 0}}],
        [{"count": 0, "sum": {"visits": 0}}] * 2,
    ],
)
def test_malformed_totals_fail_zone(cloudflare, api, totals):
    def malformed(body):
        return json_response(
            200,
            {
                "data": {
                    "viewer": {
                        "zones": [{"visits": [], "requests": [], "cache": [], "totals": totals}]
                    }
                }
            },
        )

    cloudflare.add_zone("z", "example.org", responder=malformed)
    with pytest.raises(script.CloudflareAPIError, match="totals"):
        script.collect_zone(api, "z", "example.org", START, END)


def test_new_metrics_and_midnight_omission(traffic, api, registry, monkeypatch):
    class Clock(datetime):
        now_value = END

        @classmethod
        def now(cls, tz=None):
            return cls.now_value

    monkeypatch.setattr(script, "datetime", Clock)
    collector = script.CloudflareCollector()
    registry.register(collector)
    collector.update(script.collect_once(api, now=END))
    assert collector.snapshot.window_end == END
    assert (
        registry.get_sample_value(
            "cf_exporter_snapshot_window_end_seconds",
            {"zone_name": "example.org", "window_start": script.rfc3339(START)},
        )
        == END.timestamp()
    )
    assert registry.get_sample_value("cf_zone_requests", {"zone_name": "example.org"}) == 14
    assert registry.get_sample_value("cf_zone_visits", {"zone_name": "example.org"}) == 0
    assert (
        registry.get_sample_value(
            "cf_exporter_groups", {"zone_name": "example.org", "dataset": "cache"}
        )
        == 3
    )
    assert (
        registry.get_sample_value(
            "cf_exporter_group_limit_reached", {"zone_name": "example.org", "dataset": "requests"}
        )
        == 0
    )
    assert "# TYPE cf_cache_requests gauge" in generate_latest(registry).decode()
    Clock.now_value = START + timedelta(days=1)
    body = generate_latest(registry).decode()
    for family in (
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
    ):
        assert family not in body
    assert 'cf_exporter_zone_scrape_success{zone_name="example.org"} 1.0' in body
    assert "cf_exporter_last_cycle_success 1.0" in body
    collector.update(script.collect_once(api, now=Clock.now_value))
    assert "# TYPE cf_cache_requests gauge" in generate_latest(registry).decode()


def test_totals_only_query_has_no_unused_limit():
    query = script.build_analytics_query([], [], [], datasets=("totals",))
    assert "$limit" not in query
    assert "limit: 1" in query
    assert "dimensions" not in query
