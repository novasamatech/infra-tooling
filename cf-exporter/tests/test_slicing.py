# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Reading a window: one request, the proof range of the totals, splitting full
pages down to one second, dimension pagination, page-size variants and response
validation."""

import copy

import pytest
from conftest import (
    BUCKET,
    T0,
    counts,
    event,
    graphql_error,
    iso,
    json_response,
    plan_for,
    sampled,
)

import script

ALIASES = ("groups", "totals", "edge", "edge_totals")


def test_one_request_reads_every_selection_of_the_window(cloudflare, api):
    zone = cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 5, count=3, visits=1),
        event(T0 + 310, count=2, source="edgeWorkerFetch"),
        event(T0 + 899, count=1),
    )
    plan = plan_for(api, zone)
    buckets, stats = script.read_window(api, plan, T0, T0 + 3 * BUCKET)
    assert [call["aliases"] for call in cloudflare.calls] == [ALIASES]
    assert (cloudflare.calls[0]["since"], cloudflare.calls[0]["until"]) == (T0, T0 + 900)
    assert cloudflare.calls[0]["limit"] == plan.page_size == 10000
    assert stats.requests == 1 and not stats.split
    assert list(buckets) == [T0 + 300, T0 + 600, T0 + 900]
    assert counts(buckets, "groups") == {T0 + 300: 3, T0 + 600: 2, T0 + 900: 1}
    assert counts(buckets, "totals") == {T0 + 300: 3, T0 + 600: 2, T0 + 900: 1}
    # Only client requests reach the edge selections.
    assert counts(buckets, "edge") == {T0 + 300: 3, T0 + 900: 1}
    assert counts(buckets, "edge_totals") == {T0 + 300: 3, T0 + 900: 1}
    assert buckets[T0 + 300].totals["sum"]["visits"] == 1


def test_only_the_named_selections_are_read(cloudflare, api):
    zone = cloudflare.add_zone("z1", "example.org", event(T0 + 5, count=3))
    buckets, _ = script.read_window(api, plan_for(api, zone), T0, T0 + BUCKET, ["totals"])
    assert [call["aliases"] for call in cloudflare.calls] == [("totals",)]
    assert buckets[T0 + BUCKET].totals["count"] == 3
    assert buckets[T0 + BUCKET].groups == []


# ------------------------------------------------------- proof of the totals


def test_totals_are_read_over_their_proof_range_and_only_the_window_is_returned(cloudflare, api):
    zone = cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 5, count=3),
        event(T0 - 7200, count=8, source="edgeWorkerFetch"),
    )
    proof = {"totals": (T0 - 86400, T0 + BUCKET), "edge_totals": (T0 - 3600, T0 + 2 * BUCKET)}
    buckets, stats = script.read_window(api, plan_for(api, zone), T0, T0 + BUCKET, proof=proof)
    assert cloudflare.calls[0]["ranges"] == {
        "groups": (T0, T0 + BUCKET),
        "totals": (T0 - 86400, T0 + BUCKET),
        "edge": (T0, T0 + BUCKET),
        "edge_totals": (T0 - 3600, T0 + 2 * BUCKET),
    }
    assert stats.requests == 1
    # Groups outside the window are witnesses of how the selection was answered.
    assert list(buckets) == [T0 + BUCKET]
    assert counts(buckets, "totals") == counts(buckets, "edge_totals") == {T0 + BUCKET: 3}
    assert stats.groups == {"totals": 2, "edge_totals": 1}
    assert stats.states == dict.fromkeys(ALIASES, "full")


def test_group_outside_the_window_proves_an_empty_window(cloudflare, api):
    zone = cloudflare.add_zone("z1", "example.org", event(T0 - 7200, count=5))
    plan = plan_for(api, zone)
    window_only, alone = script.read_window(api, plan, T0, T0 + BUCKET)
    assert alone.states == dict.fromkeys(ALIASES, "empty")
    proof = dict.fromkeys(("totals", "edge_totals"), (T0 - 86400, T0 + BUCKET))
    buckets, stats = script.read_window(api, plan, T0, T0 + BUCKET, proof=proof)
    assert stats.states == {
        "groups": "empty",
        "totals": "full",
        "edge": "empty",
        "edge_totals": "full",
    }
    assert buckets[T0 + BUCKET] == window_only[T0 + BUCKET] == script.BucketData()


def test_answer_from_a_reduced_table_is_recognized_by_its_groups(cloudflare, api):
    zone = cloudflare.add_zone(
        "z1", "example.org", event(T0 + 5, count=6), event(T0 - 7200, count=500)
    )
    cloudflare.transforms.append(sampled({"totals", "groups"}))
    proof = dict.fromkeys(("totals", "edge_totals"), (T0 - 86400, T0 + BUCKET))
    buckets, stats = script.read_window(api, plan_for(api, zone), T0, T0 + BUCKET, proof=proof)
    # The six requests are in no row of the reduced table. The totals show that
    # by the busy bucket before the window; the groups alone look merely empty.
    assert stats.states["totals"] == "sampled" and stats.states["groups"] == "empty"
    assert stats.states["edge_totals"] == stats.states["edge"] == "full"
    assert buckets[T0 + BUCKET].totals is None and buckets[T0 + BUCKET].groups == []
    assert counts(buckets, "edge_totals") == {T0 + BUCKET: 6}


@pytest.mark.parametrize(
    ("options", "proof", "expected"),
    [
        # A page of 20 holds 19 buckets: the window, then the time before it.
        ({"page_size": 20}, (T0 - 86400, T0 + 86400), (T0 + 900 - 19 * 300, T0 + 900)),
        # Nothing before the window: the time after it.
        ({"page_size": 20}, (T0, T0 + 86400), (T0, T0 + 19 * 300)),
        # One query covers at most maxDuration.
        ({"max_duration": 7200}, (T0 - 86400, T0 + 900), (T0 + 900 - 7200, T0 + 900)),
        # The range always contains the window.
        ({}, (T0 + 300, T0 + 600), (T0, T0 + 900)),
        ({"page_size": 3}, (T0 - 86400, T0 + 86400), (T0, T0 + 900)),
    ],
)
def test_proof_range_is_cut_to_one_page_and_one_query(cloudflare, api, options, proof, expected):
    zone = cloudflare.add_zone("z1", "example.org", **options)
    script.read_window(api, plan_for(api, zone), T0, T0 + 3 * BUCKET, proof={"totals": proof})
    assert cloudflare.calls[0]["ranges"]["totals"] == expected
    # A selection without a proof range of its own is read over the window.
    assert cloudflare.calls[0]["ranges"]["edge_totals"] == (T0, T0 + 3 * BUCKET)


def test_configured_page_size_does_not_shorten_the_proof_of_the_totals(cloudflare, api):
    zone = cloudflare.add_zone("z1", "example.org")
    plan = plan_for(api, zone, page_size=5)
    proof = {"totals": (T0 - 86400, T0 + BUCKET)}
    script.read_window(api, plan, T0, T0 + BUCKET, proof=proof, variant=2)
    call = cloudflare.calls[0]
    assert call["ranges"]["totals"] == (T0 - 86400, T0 + BUCKET)
    # A page of five has two variants, so that it is never cut below half.
    assert call["limits"] == {"groups": 5, "totals": 9998, "edge": 5, "edge_totals": 9998}


def test_totals_in_several_answers_prove_nothing(cloudflare, api):
    zone = cloudflare.add_zone(
        "z1", "example.org", *(event(T0 + index * BUCKET) for index in range(3)), page_size=2
    )
    buckets, stats = script.read_window(api, plan_for(api, zone), T0, T0 + 3 * BUCKET)
    assert counts(buckets, "totals") == {T0 + 300: 1, T0 + 600: 1, T0 + 900: 1}
    assert stats.states["totals"] == stats.states["edge_totals"] == "split"


@pytest.mark.parametrize(
    ("variant", "expected"), [(0, 100), (3, 97), (13, 87), (49, 51), (50, 100), (53, 97)]
)
def test_variant_changes_the_page_size_slightly(cloudflare, api, variant, expected):
    zone = cloudflare.add_zone("z1", "example.org", page_size=100)
    script.read_window(api, plan_for(api, zone), T0, T0 + BUCKET, variant=variant)
    assert cloudflare.calls[0]["limit"] == expected


@pytest.mark.parametrize(
    ("size", "variants"), [(10000, 50), (100, 50), (40, 20), (5, 2), (2, 1), (1, 1)]
)
def test_page_size_variants_never_go_below_half_a_page(size, variants):
    """Fifty sizes in a row, so that a request repeated minutes later rarely meets
    Cloudflare's cached answer of an earlier one."""
    assert script.page_variants(size) == variants
    sizes = {script.page_variant(size, variant) for variant in range(200)}
    assert len(sizes) == variants and min(sizes) >= max(1, size // 2) and max(sizes) == size


def test_full_page_is_split_on_bucket_boundaries_for_that_selection_only(cloudflare, api):
    # Two countries in each of three buckets: six groups against a page of four.
    # Countries are no edge dimension, so the edge selection stays below a page.
    zone = cloudflare.add_zone(
        "z1",
        "example.org",
        *(
            event(T0 + bucket * BUCKET + 7, count=1 + bucket, clientCountryName=country)
            for bucket in range(3)
            for country in ("DE", "FR")
        ),
        page_size=4,
    )
    buckets, stats = script.read_window(api, plan_for(api, zone), T0, T0 + 3 * BUCKET)
    assert [(c["aliases"], c["since"] - T0, c["until"] - T0) for c in cloudflare.calls] == [
        (ALIASES, 0, 900),
        (("groups",), 0, 300),
        (("groups",), 300, 900),  # full again
        (("groups",), 300, 600),
        (("groups",), 600, 900),
    ]
    assert stats.split and stats.requests == 5
    assert counts(buckets, "groups") == {T0 + 300: 2, T0 + 600: 4, T0 + 900: 6}
    assert [len(data.groups) for data in buckets.values()] == [2, 2, 2]
    assert counts(buckets, "totals") == counts(buckets, "groups")


def test_full_bucket_is_split_in_time_and_a_full_second_is_paginated(cloudflare, api):
    paths = ["/a", "/b", "/c", "/d", "/e"]
    zone = cloudflare.add_zone(
        "z1",
        "example.org",
        *(event(T0 + 100, count=index + 1, clientRequestPath=p) for index, p in enumerate(paths)),
        event(T0 + 250, count=7, clientRequestPath="/later"),
        page_size=2,
    )
    plan = plan_for(api, zone)
    buckets, stats = script.read_window(api, plan, T0, T0 + BUCKET, ["groups"])
    data = buckets[T0 + BUCKET]
    assert sorted((g["dimensions"]["clientRequestPath"], g["count"]) for g in data.groups) == [
        ("/a", 1),
        ("/b", 2),
        ("/c", 3),
        ("/d", 4),
        ("/e", 5),
        ("/later", 7),
    ]
    # Every slice stays inside the bucket and is at least one second long.
    assert all(T0 <= c["since"] < c["until"] <= T0 + BUCKET for c in cloudflare.calls)
    paged = [c for c in cloudflare.calls if c["after"] is not None]
    assert [(c["since"], c["until"]) for c in paged] == [(T0 + 100, T0 + 101)] * 3
    # The first page repeats the time bound; later ones continue after the last tuple.
    assert paged[0]["after"] == {"datetime_geq": iso(T0 + 100)}
    cursor = paged[1]["after"]["OR"]
    dimensions = list(plan.dimensions["groups"])
    assert [list(branch) for branch in cursor] == [
        [*dimensions[:index], f"{dimension}_gt"] for index, dimension in enumerate(dimensions)
    ]
    assert cursor[-1]["clientRequestPath"] == "/b"
    assert stats.split


def test_groups_split_inside_a_bucket_still_add_up(cloudflare, api):
    zone = cloudflare.add_zone(
        "z1",
        "example.org",
        *(event(T0 + second, count=1, clientRequestPath=f"/{second % 3}") for second in range(60)),
        page_size=3,
    )
    buckets, _ = script.read_window(api, plan_for(api, zone), T0, T0 + BUCKET)
    assert counts(buckets, "groups") == {T0 + BUCKET: 60}
    assert counts(buckets, "edge") == {T0 + BUCKET: 60}
    assert counts(buckets, "totals") == {T0 + BUCKET: 60}


def test_reduced_part_of_a_split_read_is_asked_for_again_at_once(cloudflare, api):
    """A split read has many parts. Reading all of them again because one came
    from the reduced table would rarely end, so that part alone is repeated."""
    zone = cloudflare.add_zone(
        "z1",
        "example.org",
        *(
            event(T0 + at, count=10, clientRequestPath=path)
            for at, path in ((10, "/a"), (20, "/b"), (200, "/c"), (210, "/d"))
        ),
        page_size=3,
    )
    second_half = (T0 + 150, T0 + BUCKET)
    reduced = []

    def reduced_once(call, alias, groups):
        if (call["since"], call["until"]) == second_half and not reduced:
            reduced.append(call["index"])
            return sampled({"groups"})(call, alias, groups)
        return groups

    cloudflare.transforms.append(reduced_once)
    buckets, stats = script.read_window(api, plan_for(api, zone), T0, T0 + BUCKET, ["groups"])
    halves = [c for c in cloudflare.calls if (c["since"], c["until"]) == second_half]
    # Another page size: Cloudflare caches an answer by query and variables.
    assert [c["limit"] for c in halves] == [3, 2]
    assert stats.states["groups"] == "split" and stats.reduced == {}
    assert counts(buckets, "groups") == {T0 + BUCKET: 40}
    assert stats.requests == len(cloudflare.calls)


def test_bucket_with_a_part_from_the_reduced_table_is_not_verified(cloudflare, api):
    """Among the full parts of a split bucket the sample interval of a reduced part
    goes unnoticed, and its rounded counts can even add up to the totals."""
    zone = cloudflare.add_zone(
        "z1",
        "example.org",
        *(event(T0 + 10 + index, clientRequestPath=f"/{index}") for index in range(3)),
        event(T0 + 200, count=10, clientRequestPath="/busy"),
        page_size=4,
    )
    plan = plan_for(api, zone)

    def second_half_reduced(call, alias, groups):
        if alias == "groups" and call["since"] >= T0 + 150:
            return sampled({"groups"})(call, alias, groups)
        return groups

    cloudflare.transforms.append(second_half_reduced)
    buckets, stats = script.read_window(api, plan, T0, T0 + BUCKET)
    end = T0 + BUCKET
    data = buckets[end]
    asked = [c for c in cloudflare.calls if c["aliases"] == ("groups",) and c["since"] > T0]
    assert len(asked) == script.PART_READS
    assert len({c["limit"] for c in asked}) == script.PART_READS
    assert stats.states["groups"] == "sampled" and stats.reduced == {"groups": {end}}
    # Thirteen requests in four rows: far below the interval of a reduced answer.
    assert counts(buckets, "groups") == counts(buckets, "totals") == {end: 13}
    assert script.effective_interval(data.groups) < script.SAMPLED_INTERVAL
    assert script.evaluate(plan, script.Schema(), end, data, stats.states).problem is None
    result = script.evaluate(plan, script.Schema(), end, data, stats.states, stats.reduced)
    assert result.problem == "sampled"
    assert result.reason == "groups partly at a reduced resolution"


def test_page_size_of_one_cannot_truncate_selections_without_dimensions(cloudflare, api):
    zone = cloudflare.add_zone(
        "z1", "example.org", event(T0 + 5, count=3), event(T0 + 305, count=4), page_size=1
    )
    buckets, _ = script.read_window(api, plan_for(api, zone), T0, T0 + 2 * BUCKET)
    for alias in ALIASES:
        assert counts(buckets, alias) == {T0 + 300: 3, T0 + 600: 4}, alias


def test_pagination_that_does_not_advance_fails(cloudflare, api):
    zone = cloudflare.add_zone(
        "z1",
        "example.org",
        *(event(T0 + 100, clientRequestPath=path) for path in ("/a", "/b", "/c")),
        page_size=2,
    )
    plan = plan_for(api, zone)
    stuck = []

    def repeat_first_page(call, alias, groups):
        if call["after"] is None:
            return groups
        if not stuck:
            stuck.extend(copy.deepcopy(groups))
        return copy.deepcopy(stuck)

    cloudflare.transforms.append(repeat_first_page)
    with pytest.raises(script.CloudflareAPIError, match="did not advance"):
        script.read_window(api, plan, T0, T0 + BUCKET, ["groups"])


def test_pagination_needs_every_dimension(cloudflare, api):
    zone = cloudflare.add_zone(
        "z1",
        "example.org",
        *(event(T0 + 100, clientRequestPath=path) for path in ("/a", "/b", "/c")),
        page_size=2,
    )

    def drop_country(call, alias, groups):
        for group in groups:
            if call["after"] is not None:
                group["dimensions"]["clientCountryName"] = None
        return groups

    cloudflare.transforms.append(drop_country)
    with pytest.raises(script.CloudflareAPIError, match="lacks a dimension"):
        script.read_window(api, plan_for(api, zone), T0, T0 + BUCKET, ["groups"])


def mutate(alias, change):
    def transform(call, name, groups):
        if name == alias and groups:
            change(groups[0])
        return groups

    return transform


@pytest.mark.parametrize(
    ("alias", "change", "message"),
    [
        ("groups", lambda g: g.update(count=-1), "invalid number"),
        ("groups", lambda g: g.update(count=True), "invalid number"),
        ("groups", lambda g: g.update(count="3"), "invalid number"),
        ("groups", lambda g: g.update(count=float("nan")), "invalid number"),
        ("groups", lambda g: g.update(sum=None), "invalid number"),
        ("groups", lambda g: g.update(sum={"visits": -2}), "invalid number"),
        ("totals", lambda g: g.update(avg=None), "invalid number"),
        ("totals", lambda g: g.update(avg={"sampleInterval": 0}), "invalid sample interval"),
        ("groups", lambda g: g.update(dimensions=None), "invalid dimensions"),
        ("groups", lambda g: g["dimensions"].pop("datetimeFiveMinutes"), "invalid bucket time"),
        (
            "groups",
            lambda g: g["dimensions"].update(datetimeFiveMinutes="yesterday"),
            "invalid bucket time",
        ),
        (
            "groups",
            lambda g: g["dimensions"].update(datetimeFiveMinutes="2027-01-15T08:00:00"),
            "without a time zone",
        ),
        (
            "groups",
            lambda g: g["dimensions"].update(datetimeFiveMinutes=iso(T0 + 7)),
            "unaligned bucket",
        ),
        (
            "totals",
            lambda g: g["dimensions"].update(datetimeFiveMinutes=iso(T0 + 600)),
            "outside the window",
        ),
        (
            "edge",
            lambda g: g["dimensions"].update(datetimeFiveMinutes=iso(T0 - 300)),
            "outside the window",
        ),
    ],
)
def test_malformed_groups_fail_the_read(cloudflare, api, alias, change, message):
    zone = cloudflare.add_zone("z1", "example.org", event(T0 + 5, count=3, visits=1))
    plan = plan_for(api, zone)
    cloudflare.transforms.append(mutate(alias, change))
    with pytest.raises(script.CloudflareAPIError, match=message):
        script.read_window(api, plan, T0, T0 + BUCKET)


@pytest.mark.parametrize(
    ("node", "message"),
    [
        ({"groups": None}, "no valid group list"),
        ({"groups": ["oops"]}, "invalid group"),
        ({"groups": []}, "totals contains no valid group list"),
    ],
)
def test_malformed_selections_fail_the_read(cloudflare, api, node, message):
    zone = cloudflare.add_zone("z1", "example.org")
    plan = plan_for(api, zone)
    cloudflare.replies.append(
        json_response(200, {"data": {"viewer": {"zones": [node]}}, "errors": None})
    )
    with pytest.raises(script.CloudflareAPIError, match=message):
        script.read_window(api, plan, T0, T0 + BUCKET)


def test_repeated_bucket_in_totals_fails_the_read(cloudflare, api):
    zone = cloudflare.add_zone("z1", "example.org", event(T0 + 5, count=3))
    plan = plan_for(api, zone)
    cloudflare.transforms.append(
        lambda call, alias, groups: groups * 2 if alias == "totals" else groups
    )
    with pytest.raises(script.CloudflareAPIError, match="repeats a bucket"):
        script.read_window(api, plan, T0, T0 + BUCKET)


@pytest.mark.parametrize(
    "reply",
    [
        graphql_error("unable to execute task", "internal"),
        json_response(200, {"data": {"viewer": {"zones": []}}, "errors": None}),
        json_response(200, {"data": {"viewer": None}, "errors": None}),
        json_response(200, {"data": None, "errors": None}),
    ],
)
def test_failed_or_empty_answers_fail_the_read(cloudflare, api, reply):
    zone = cloudflare.add_zone("z1", "example.org")
    plan = plan_for(api, zone)
    cloudflare.replies.append(reply)
    with pytest.raises(script.CloudflareAPIError):
        script.read_window(api, plan, T0, T0 + BUCKET)
