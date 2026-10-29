# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Adaptive fetching against a fake API with actual half-open time filtering."""

import json
from datetime import UTC, datetime, timedelta
from itertools import pairwise

import pytest
from conftest import (
    ALL_FIELDS,
    SELECTION,
    aggregating_responder,
    event,
    eyeball_only,
    graphql_error,
    json_response,
    page_calls,
    reads,
    requests_group,
    selections,
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
                # Like the API, requests without visits are groups with zero visits.
                visits = cloudflare.zones["z"].get("visits_enabled")
                node[kind] = [
                    visits_group(count if visits else 0, clientRequestPath=path)
                    for path, count in ranked
                ][: body["variables"]["limit"]]
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


def hinted_leaves(api, zone_id, kind):
    """Leaf slices of the last successful collection, remembered for the next cycle."""
    _, starts = api.slice_hints[(zone_id, kind)]
    return len(starts) + 1


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
    assert hinted_leaves(api, "z", "requests") == 4
    assert hinted_leaves(api, "z", "edge") == 4
    traffic.graphql_calls.clear()
    script.collect_zone(api, "z", "example.org", START, END)
    assert len(traffic.data_calls()) == 5
    assert "requests:" not in traffic.data_calls()[0]["query"]
    assert "cache:" not in traffic.data_calls()[0]["query"]
    assert "edge:" not in traffic.data_calls()[0]["query"]
    traffic.graphql_calls.clear()
    script.collect_zone(api, "z", "example.org", START + timedelta(days=1), END + timedelta(days=1))
    assert len(traffic.data_calls()) == 1
    assert hinted_leaves(api, "z", "requests") == 1
    assert hinted_leaves(api, "z", "edge") == 1


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
    assert hinted_leaves(api, "z", "edge") > 1
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


def test_configured_minimum_slice_stops_splitting(traffic, api, caplog):
    api.min_slice_seconds = 60
    traffic.zones["z"]["events"] = [(0, "/a", 5), (10, "/b", 3), (20, "/c", 1)]
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert paths(result.requests, script.REQUESTS_LABELS) == {"/a": 5, "/b": 3}
    assert result.total_requests == 9
    assert result.limited["requests"] == result.limited["cache"] == 1
    assert paths(result.edge, script.EDGE_LABELS) == {"/a": 5, "/b": 3}
    assert result.limited["edge"] == 1
    assert "configured minimum slice of 60s" in caplog.text
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


def test_slice_budget_bounds_requests_and_reports_truncation(traffic, api, caplog):
    # /x is lost once the budget forbids splitting the full first half.
    api.max_slice_requests = 3
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
    assert "dataset slice budget; dataset reads left: 0" in caplog.text
    assert hinted_leaves(api, "z", "requests") == 2
    assert hinted_leaves(api, "z", "edge") == 2


def test_slice_hint_is_capped_by_budget_keeping_its_boundaries(traffic, api):
    traffic.zones["z"]["events"] = [(second, f"/{second}", 1) for second in range(0, 240, 10)]
    script.collect_zone(api, "z", "example.org", START, END)
    _, starts = api.slice_hints[("z", "requests")]
    assert len(starts) > 3
    traffic.graphql_calls.clear()
    api.max_slice_requests = 3
    script.collect_zone(api, "z", "example.org", START, END)
    calls = requests_calls(traffic)
    assert len(calls) == 3
    # Coarser slices merge neighbouring leaves rather than cutting anew.
    bounds = {START, END, *starts}
    for call in calls:
        assert datetime.fromisoformat(call["variables"]["since"]) in bounds
        assert datetime.fromisoformat(call["variables"]["until"]) in bounds


def test_partial_minute_tail_is_not_lost(traffic, api):
    result = script.collect_zone(api, "z", "example.org", START, START + timedelta(seconds=61))
    assert paths(result.requests, script.REQUESTS_LABELS) == {"/a": 2, "/b": 3}
    assert result.total_requests == 5
    assert result.limited["requests"] == 0
    assert paths(result.edge, script.EDGE_LABELS) == {"/a": 2, "/b": 3}
    assert result.limited["edge"] == 0


@pytest.mark.parametrize("field, name", script.LIMITS.items())
@pytest.mark.parametrize("value", [None, "", "0", " 0 ", "15000"])
def test_limit_configuration(field, name, value):
    env = {"CF_EXPORTER_API_TOKEN": "dummy"}
    if value is not None:
        env[name] = value
    config = script.parse_env(env)
    expected = 15000 if value == "15000" else 0
    assert getattr(config, field) == expected
    # Only the configured option is set; zero is no limit, not a small one.
    assert all(getattr(config, other) == 0 for other in script.LIMITS if other != field)
    api = script.create_api(config)
    if field == "max_retry_after_seconds":
        retry = api.session.get_adapter("https://api.cloudflare.com").max_retries
        assert retry.max_retry_after == (expected or None)
        assert retry.new().max_retry_after == (expected or None)
    else:
        assert getattr(api, field) == expected
    shared = expected or None if field == "max_extra_selections" else None
    assert api.slice_allowance == shared


def test_defaults_impose_no_limit_of_the_exporter():
    api = script.create_api(script.parse_env({"CF_EXPORTER_API_TOKEN": "dummy"}))
    retry = api.session.get_adapter("https://api.cloudflare.com").max_retries
    assert retry.max_retry_after is None
    assert api.slice_allowance is None
    assert (api.group_limit, api.max_slice_requests, api.min_slice_seconds, api.page_size) == (
        0,
        0,
        0,
        0,
    )
    api.start_cycle()
    assert api.slice_allowance is None


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


def test_full_minimum_slice_retains_page_and_reports_truncation(cloudflare, api, caplog):
    api.min_slice_seconds = 60
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
    assert len(page_calls(cloudflare)) == 1
    assert "configured minimum slice of 60s" in caplog.text


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


def test_current_day_total_drift_does_not_clear_a_real_cut(traffic, api):
    # A later total that happens to equal the cut sum must not hide the cut.
    api.max_slice_requests = 3
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


def extra_selections(cloudflare):
    """Dataset selections beyond one per zone and dataset; settings and totals excluded."""
    reads = [
        (call["variables"]["zoneTag"], kind)
        for call in cloudflare.data_calls()
        for kind, _ in SELECTION.findall(call["query"])
        if kind != "totals"
    ]
    return len(reads) - len(set(reads))


def test_shared_extra_selections_are_a_per_cycle_budget_of_all_zones(traffic, api, caplog):
    traffic.add_zone("y", "example.net", max_page_size=2, responder=traffic.zones["z"]["responder"])
    # Exactly what the first zone needs: three splits of requests, cache and edge.
    api.max_extra_selections = 18
    snapshot = script.collect_once(api, now=END)
    assert extra_selections(traffic) == 18
    assert snapshot.zone_success == {"example.org": 1, "example.net": 1}
    assert {kind: snapshot.limited[("example.org", kind)] for kind in script.DATASETS} == {
        kind: 0 for kind in script.DATASETS
    }
    # The second zone found no budget left for slicing and reports the cut.
    assert snapshot.limited[("example.net", "requests")] == 1
    assert snapshot.limited[("example.net", "edge")] == 1
    assert snapshot.limited[("example.net", "visits")] == 0
    assert (
        "shared slice budget; dataset reads left: unlimited, shared extra selections left: 0"
        in (caplog.text)
    )
    # The budget is renewed every cycle; hinted slices use less of it than splits.
    traffic.graphql_calls.clear()
    snapshot = script.collect_once(api, now=END)
    assert extra_selections(traffic) <= 18
    assert snapshot.limited[("example.org", "requests")] == 0


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


@pytest.mark.parametrize("kind", script.DATASETS)
@pytest.mark.parametrize("location", ["parent", "probe"])
def test_malformed_dimensions_cannot_hide_in_discarded_pages(cloudflare, api, kind, location):
    def respond(body):
        node = {dataset: [] for dataset in script.DATASETS}
        node["totals"] = []
        window = body["variables"]
        if window["since"] == script.rfc3339(START) and window["until"] == script.rfc3339(END):
            good = (
                visits_group(5) if kind == "visits" else requests_group(5, edgeResponseStatus=200)
            )
            bad = visits_group(1) if kind == "visits" else requests_group(1)
            bad["dimensions"] = []
            node[kind] = [good, bad]
        return json_response(200, {"data": {"viewer": {"zones": [node]}}})

    cloudflare.add_zone("z", "example.org", max_page_size=2, responder=respond)
    if location == "probe":
        api.group_limit = 1
    snapshot = script.collect_once(api, now=END)
    assert snapshot.zone_success == {"example.org": 0}
    assert not snapshot.available
    assert not snapshot.groups
    assert len(cloudflare.data_calls()) == 1


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
    query = script.build_analytics_query({}, datasets=("totals",))
    assert "$limit" not in query
    assert "limit: 1" in query
    assert "dimensions" not in query


# ------------------------------------------- slices, budgets and hints ------


@pytest.mark.parametrize("seconds", [1, 2, 3, 59, 60, 61, 119, 3601, 86400])
@pytest.mark.parametrize("count", [1, 2, 3, 7, 1000, 100000])
@pytest.mark.parametrize("step", [1, 60])
def test_windows_are_disjoint_whole_seconds_covering_the_window(seconds, count, step):
    windows = script._windows(START, START + timedelta(seconds=seconds), count, step)
    assert 1 <= len(windows) <= min(count, -(-seconds // step))
    assert windows[0][0] == START
    assert windows[-1][1] == START + timedelta(seconds=seconds)
    for (start, end), following in zip(windows, [*windows[1:], None], strict=True):
        assert start < end
        assert (end - start).total_seconds().is_integer()
        if following is not None:
            assert following[0] == end
            # Only the trailing slice may be shorter than the step.
            assert (end - START).total_seconds() % step == 0


def test_empty_window_has_no_slices():
    assert script._windows(START, START, 5) == []


def test_coarsened_slices_keep_existing_boundaries():
    edges = [START + timedelta(seconds=second) for second in (0, 10, 20, 30, 40, 50)]
    assert script._coarsen(edges, 2) == [(edges[0], edges[2]), (edges[2], edges[5])]
    assert script._coarsen(edges, 9) == list(pairwise(edges))
    assert script._coarsen(edges, 1) == [(edges[0], edges[5])]


def test_default_slices_reach_one_second_without_truncation(cloudflare, api, caplog):
    # Four groups within four seconds, two per page: the old 60s minimum cut them.
    events = [event(second, 1, clientRequestPath=f"/{second}") for second in range(4)]
    cloudflare.add_zone(
        "z", "example.org", max_page_size=2, responder=aggregating_responder(events, START)
    )
    result = script.collect_zone(api, "z", "example.org", START, START + timedelta(minutes=4))
    assert len(result.requests) == 4
    assert sum(result.requests.values()) == result.total_requests == 4
    assert result.limited == dict.fromkeys(script.DATASETS, 0)
    assert "full page" not in caplog.text


def test_full_one_second_slice_is_reported_and_not_split_again(cloudflare, api, caplog):
    # Three query strings in one second; only the requests dataset groups by them.
    events = [event(5, 1, clientRequestQuery=f"?{query}") for query in range(3)]
    cloudflare.add_zone(
        "z", "example.org", max_page_size=2, responder=aggregating_responder(events, START)
    )
    result = script.collect_zone(api, "z", "example.org", START, START + timedelta(minutes=4))
    assert result.limited == {"visits": 0, "requests": 1, "cache": 0, "edge": 0}
    assert len(result.requests) == 2
    # Halving 240 s reaches one second after eight levels: two reads per level,
    # and one check of the empty half.
    assert reads(cloudflare, "requests") <= 1 + 2 * 8
    assert reads(cloudflare, "requests", checks=True) <= 8
    assert "returned a full page of 2 groups that was not split" in caplog.text
    assert "(1s slice at the time resolution; dataset reads left: unlimited" in caplog.text


# A burst of distinct paths in ten minutes of an otherwise quiet day.
BURST = [event(3600 + second, 1, clientRequestPath=f"/{second}") for second in range(0, 600, 3)]
BURST_END = START + timedelta(hours=2)


@pytest.fixture
def burst(cloudflare):
    cloudflare.add_zone(
        "z", "example.org", max_page_size=4, responder=aggregating_responder(BURST, START)
    )
    return cloudflare


def test_burst_needing_many_reads_is_complete_without_own_limits(burst, api, caplog):
    result = script.collect_zone(api, "z", "example.org", START, BURST_END)
    assert reads(burst, "requests") > 32
    assert len(result.requests) == len(BURST)
    assert sum(result.requests.values()) == result.total_requests == len(BURST)
    assert result.limited == dict.fromkeys(script.DATASETS, 0)
    assert "group limit" not in caplog.text and "full page" not in caplog.text


@pytest.mark.parametrize("budgeted", [False, True])
def test_repeated_burst_window_reuses_its_partition(burst, api, caplog, budgeted):
    """Regression: hints kept only the number of leaves and re-cut the day evenly,
    so an unchanged burst window first completed and then became truncated."""
    script.collect_zone(api, "z", "example.org", START, BURST_END)
    first = reads(burst, "requests")
    if budgeted:
        # Exactly what the cold cycle needed, parent pages included.
        api.max_slice_requests = first
    counts = []
    for _ in range(3):
        burst.graphql_calls.clear()
        result = script.collect_zone(api, "z", "example.org", START, BURST_END)
        counts.append(reads(burst, "requests"))
        assert result.limited == dict.fromkeys(script.DATASETS, 0)
        assert len(result.requests) == len(BURST)
    # Warm cycles read each remembered leaf once, without parents or new splits.
    assert counts == [hinted_leaves(api, "z", "requests")] * 3
    assert reads(burst, "requests", checks=True) == reads(burst, "cache", checks=True)
    assert counts[0] < first
    assert "full page" not in caplog.text


def test_growing_window_extends_the_last_slice_and_refetches_old_ones(burst, api):
    script.collect_zone(api, "z", "example.org", START, BURST_END)
    _, starts = api.slice_hints[("z", "requests")]
    for minutes in (5, 10, 15):
        burst.graphql_calls.clear()
        result = script.collect_zone(
            api, "z", "example.org", START, BURST_END + timedelta(minutes=minutes)
        )
        assert result.limited == dict.fromkeys(script.DATASETS, 0)
        # The same leaves, with only the last one longer: no extra tail slice.
        assert api.slice_hints[("z", "requests")][1] == starts
        assert reads(burst, "requests") == len(starts) + 1
        windows = {
            (call["variables"]["since"], call["variables"]["until"])
            for call in burst.data_calls()
            if "requests:" in call["query"]
        }
        assert (script.rfc3339(START), script.rfc3339(starts[0])) in windows
        assert max(until for _, until in windows) == script.rfc3339(
            BURST_END + timedelta(minutes=minutes)
        )


def test_partition_is_not_reused_for_other_dimensions(burst, api):
    script.collect_zone(api, "z", "example.org", START, BURST_END)
    burst.graphql_calls.clear()
    api.schema = script.Schema({"cf_requests": frozenset({"path"})})
    result = script.collect_zone(api, "z", "example.org", START, BURST_END)
    assert reads(burst, "requests") == 1
    assert sum(result.requests.values()) == len(BURST)
    assert api.slice_hints[("z", "requests")][1] == ()


@pytest.mark.parametrize(
    ("option", "value", "reason"),
    [
        ("max_slice_requests", 3, "dataset slice budget; dataset reads left: 0"),
        ("max_extra_selections", 2, "shared slice budget; dataset reads left: unlimited"),
        ("min_slice_seconds", 3600, "configured minimum slice of 3600s"),
        ("group_limit", 3, "hit the 3 group limit (configured cap)"),
    ],
)
def test_explicit_limits_report_their_reason_only_for_the_cut_dataset(
    cloudflare, api, caplog, option, value, reason
):
    # Only the requests dataset groups by query, so only it has many groups.
    events = [event(3600 + second, 1, clientRequestQuery=f"?{second}") for second in range(20)]
    cloudflare.add_zone(
        "z", "example.org", max_page_size=4, responder=aggregating_responder(events, START)
    )
    setattr(api, option, value)
    api.start_cycle()
    result = script.collect_zone(api, "z", "example.org", START, BURST_END)
    assert result.limited == {"visits": 0, "requests": 1, "cache": 0, "edge": 0}
    assert sum(result.cache.values()) == result.total_requests == 20
    assert sum(result.requests.values()) < 20
    assert reason in caplog.text
    assert caplog.text.count("Zone example.org:") == 1


@pytest.mark.parametrize(("configured", "expected"), [(0, 4), (3, 3), (50, 4)])
def test_page_size_option_never_exceeds_the_plan(burst, api, configured, expected):
    api.page_size = configured
    script.collect_zone(api, "z", "example.org", START, BURST_END)
    assert {call["variables"]["limit"] for call in page_calls(burst)} == {expected}


def blind(events, windows, kind="requests"):
    """Like the API at times: an empty page for a window with traffic, repeated
    for that window and the dataset's dimensions, while the dimensionless read
    of the same window and its halves are correct."""
    respond = aggregating_responder(events, START)

    def blinded(body):
        status, headers, payload = respond(body)
        data = json.loads(payload)
        window = (body["variables"]["since"], body["variables"]["until"])
        for alias, _, dimensions in selections(body["query"]):
            if alias == kind and dimensions and window in windows:
                data["data"]["viewer"]["zones"][0][alias] = []
        return status, headers, json.dumps(data)

    return blinded


def dimensionless_reads(cloudflare, kind="requests"):
    return [
        (call["variables"]["since"], call["variables"]["until"])
        for call in cloudflare.data_calls()
        for alias, _, dimensions in selections(call["query"])
        if alias == kind and not dimensions
    ]


def window(start, end):
    return script.rfc3339(START + timedelta(seconds=start)), script.rfc3339(
        START + timedelta(seconds=end)
    )


EVENTS = [event(second, 1, clientRequestPath=f"/{second % 7}") for second in range(0, 240, 5)]


def test_empty_page_hiding_traffic_is_split_and_recovered(cloudflare, api, caplog):
    cloudflare.add_zone("z", "example.org", responder=blind(EVENTS, {window(0, 240)}))
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert sum(result.requests.values()) == result.total_requests == len(EVENTS)
    assert paths(result.requests, script.REQUESTS_LABELS) == paths(
        result.cache, script.CACHE_LABELS
    )
    assert result.limited == dict.fromkeys(script.DATASETS, 0)
    # Check this same window independently: the day total can differ from pages.
    assert dimensionless_reads(cloudflare) == [window(0, 240)]
    assert hinted_leaves(api, "z", "requests") == 2
    # The next cycle reads the halves directly: the blind window is not reused.
    cloudflare.graphql_calls.clear()
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert sum(result.requests.values()) == len(EVENTS)
    assert not dimensionless_reads(cloudflare)
    assert "empty page" not in caplog.text


def test_unsplittable_empty_page_with_traffic_is_reported(cloudflare, api, caplog):
    events = [event(5, 3)]
    blinded = {window(0, 240), window(0, 120), window(0, 60), window(0, 30), window(0, 15)}
    blinded |= {window(0, 7), window(3, 7), window(5, 7), window(5, 6)}
    cloudflare.add_zone("z", "example.org", responder=blind(events, blinded))
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert result.requests == {}
    assert result.limited == {"visits": 0, "requests": 1, "cache": 0, "edge": 0}
    assert sum(result.cache.values()) == result.total_requests == 3
    assert (
        "requests returned an empty page for a window with traffic that was not split "
        "(1s slice at the time resolution" in caplog.text
    )


def test_empty_page_under_a_configured_cap_is_reported(cloudflare, api, caplog):
    cloudflare.add_zone("z", "example.org", responder=blind(EVENTS, {window(0, 240)}))
    api.group_limit = 100
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert result.limited["requests"] == 1
    assert "returned an empty page for a window with traffic (configured cap, one query)" in (
        caplog.text
    )


def test_genuinely_empty_slices_cost_one_dimensionless_request_each(cloudflare, api):
    # Traffic only in three seconds: every split leaves an empty half.
    events = [event(120 + second, 1, clientRequestPath=f"/{second}") for second in range(3)]
    cloudflare.add_zone(
        "z", "example.org", max_page_size=2, responder=aggregating_responder(events, START)
    )
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert result.limited == dict.fromkeys(script.DATASETS, 0)
    assert sum(result.requests.values()) == 3
    checks = [call for call in cloudflare.data_calls() if call not in page_calls(cloudflare)]
    windows = [(call["variables"]["since"], call["variables"]["until"]) for call in checks]
    assert checks and len(set(windows)) == len(windows)
    # The datasets of an empty window are checked together in one request.
    for call in checks:
        assert {alias for alias, _, _ in selections(call["query"])} == set(script.DATASETS)
    _, starts = api.slice_hints[("z", "requests")]
    assert len(windows) < len(starts) + 1


def test_zone_without_traffic_needs_no_check(cloudflare, api):
    cloudflare.add_zone("z", "example.org", responder=aggregating_responder([], START))
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert result.limited == dict.fromkeys(script.DATASETS, 0)
    assert len(cloudflare.data_calls()) == 1


def test_check_of_empty_pages_counts_against_explicit_budgets(cloudflare, api):
    # The edge filter's scope differs from the already-fetched unfiltered totals.
    cloudflare.add_zone("z", "example.org", responder=blind(EVENTS, {window(0, 240)}, "edge"))
    api.max_slice_requests = 1
    result = script.collect_zone(api, "z", "example.org", START, END)
    # Without budget for the check, completeness cannot be established.
    assert not dimensionless_reads(cloudflare, "edge")
    assert result.edge == {}
    assert result.limited["edge"] == 1
    cloudflare.graphql_calls.clear()
    api.max_slice_requests = 2
    result = script.collect_zone(api, "z", "example.org", START, END)
    # The check fits, the split does not: the hidden traffic is reported.
    assert dimensionless_reads(cloudflare, "edge") == [window(0, 240)]
    assert result.limited["edge"] == 1


def test_exhausted_shared_budget_cannot_certify_an_empty_page(cloudflare, api, caplog):
    cloudflare.add_zone("z", "example.org", responder=blind(EVENTS, {window(0, 240)}, "edge"))
    # A previous dataset or zone spent this cycle's optional extra budget.
    api.slice_allowance = 0
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert not dimensionless_reads(cloudflare, "edge")
    assert result.edge == {}
    assert result.limited == {"visits": 0, "requests": 0, "cache": 0, "edge": 1}
    assert "empty page" in caplog.text
    assert "shared" in caplog.text


def test_zero_visits_total_does_not_need_a_check_or_report_a_cut(cloudflare, api):
    cloudflare.add_zone(
        "z",
        "example.org",
        requests=[requests_group(7)],
        cache=[requests_group(7)],
        edge=[requests_group(7, edgeResponseStatus=200)],
        totals=[{"count": 7, "sum": {"visits": 0}}],
    )
    api.max_slice_requests = 1
    result = script.collect_zone(api, "z", "example.org", START, END)
    assert result.visits == {}
    assert not dimensionless_reads(cloudflare, "visits")
    assert result.limited == dict.fromkeys(script.DATASETS, 0)
