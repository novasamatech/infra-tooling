# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Bucket acceptance: proof of full-resolution totals, reduced and empty answers,
repairs, consistency checks and rechecks."""

import pytest
from conftest import (
    BUCKET,
    PRO_FIELDS,
    T0,
    analytics,
    event,
    first_calls,
    iso,
    plan_for,
    request,
    run,
    sampled,
    values,
)
from prometheus_client import CollectorRegistry

import script

ZONE = {"zone_name": "example.org"}
ALL = ("groups", "totals", "edge", "edge_totals")
TOTALS = ("totals", "edge_totals")
END = T0 + BUCKET  # the newest settled bucket at the fixture's clock


def deliver(exporter, storage, clock):
    """Scrape twice and let the delivery time pass, as a scraper would."""
    for _ in range(script.DELIVERY_RESPONSES):
        analytics(exporter, storage, consume=True)
    clock.advance(script.DELIVERY_SECONDS)
    exporter.prune(clock.now)


def test_buckets_carry_their_events_at_the_bucket_end(cloudflare, exporter, storage):
    cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 10, count=3, visits=1),
        event(T0 + 299, count=2, visits=1),
        event(T0 - 1, count=5),
        # Not settled yet: its bucket ends at T0 + 600, the current time.
        event(T0 + 300, count=100),
    )
    run(exporter)
    samples = analytics(exporter, storage)
    assert values(samples, "cf_requests_5m", **ZONE) == {T0: 5, END: 5}
    assert values(samples, "cf_visits_5m", **ZONE) == {END: 2}
    assert values(samples, "cf_cache_requests_5m", **ZONE) == {T0: 5, END: 5}
    assert values(samples, "cf_edge_requests_5m", **ZONE) == {T0: 5, END: 5}
    # Zone totals exist for every accepted bucket, also without traffic.
    totals = values(samples, "cf_zone_requests_5m", **ZONE)
    assert totals[T0] == 5 and totals[END] == 5 and totals[T0 - BUCKET] == 0
    assert max(totals) == END
    assert len(totals) == 23  # every bucket within the zone's two hours of reach
    assert values(samples, "cf_zone_visits_5m", **ZONE)[END] == 2
    assert exporter.results["accepted"] == 23
    assert sum(exporter.results.values()) == 23
    # One request per window: the totals carried their own proof.
    assert [call["aliases"] for call in cloudflare.calls] == [ALL, ALL]


# ------------------------------------------------- reduced and empty answers


def test_quiet_hour_missing_from_a_reduced_answer_is_not_exported_as_empty(
    cloudflare, exporter, storage, clock
):
    """Under load Cloudflare answers from every tenth event: the six requests of a
    quiet hour are in none of its selections, which then agree on nothing."""
    busy = T0 - 2 * 3600
    cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 10, count=6, visits=2),
        event(busy, count=500),
        not_older_than=5 * 3600,
    )
    cloudflare.transforms.append(first_calls(4, sampled(set(ALL))))
    run(exporter)
    first = cloudflare.calls[0]
    # The totals were read with the hours before the window, where the busy
    # bucket shows that this answer is not from the full data.
    assert first["ranges"]["totals"][0] <= busy < first["ranges"]["groups"][0]
    assert [call["aliases"] for call in cloudflare.calls[:4]] == [ALL, TOTALS, TOTALS, TOTALS]
    samples = analytics(exporter, storage)
    assert not any(end > T0 - 3300 for end in values(samples, "cf_zone_requests_5m"))
    assert values(samples, "cf_requests_5m", **ZONE) == {busy + BUCKET: 500}
    assert exporter.results["sampled"] == 12

    clock.advance(script.RETRY_DELAYS[0])
    run(exporter)
    samples = analytics(exporter, storage)
    assert values(samples, "cf_requests_5m", **ZONE) == {busy + BUCKET: 500, END: 6}
    assert values(samples, "cf_visits_5m", **ZONE) == {END: 2}
    assert values(samples, "cf_zone_requests_5m", **ZONE)[END] == 6


def test_empty_totals_are_read_over_a_week_before_they_are_believed(
    cloudflare, exporter, storage, clock
):
    cloudflare.add_zone(
        "z1", "example.org", event(T0 + 10, count=6), not_older_than=10 * 86400 + 3600
    )
    # Nothing of this zone is in the reduced table: the first answer is empty.
    cloudflare.transforms.append(first_calls(1, sampled(set(ALL))))
    run(exporter)
    first, again, repair = cloudflare.calls[:3]
    assert first["aliases"] == ALL
    assert first["ranges"]["totals"] == (END - script.PROOF_SECONDS, END)
    # An answer without groups proves nothing: the totals are read again over
    # a week. Never over a month, which Cloudflare always answers reduced.
    assert again["aliases"] == TOTALS
    week = (END - script.LONG_PROOF_SECONDS, END)
    assert again["ranges"]["totals"] == again["ranges"]["edge_totals"] == week
    assert script.LONG_PROOF_SECONDS == 7 * 86400
    # They show the requests the groups lack, so everything is read once more.
    assert repair["aliases"] == ALL
    assert values(analytics(exporter, storage), "cf_requests_5m", **ZONE) == {END: 6}
    assert exporter.results["sampled"] == exporter.results["inconsistent"] == 0


def test_idle_zone_is_believed_after_three_empty_answers_over_the_long_range(
    cloudflare, exporter, storage, clock
):
    zone = cloudflare.add_zone("z1", "example.org")
    run(exporter)
    assert [call["aliases"] for call in cloudflare.calls] == [
        # The fresh hour: empty over the usual range, then three times over the
        # long one.
        ALL,
        TOTALS,
        TOTALS,
        TOTALS,
        # The history: the zone is known to be quiet, the first answer counts.
        ALL,
        TOTALS,
        TOTALS,
    ]
    assert script.EMPTY_LOOKS == 3
    # Every answer is computed anew: another page size each time.
    assert len({call["limits"]["totals"] for call in cloudflare.calls}) == 7
    totals = values(analytics(exporter, storage), "cf_zone_requests_5m", **ZONE)
    assert len(totals) == 23 and set(totals.values()) == {0}
    assert exporter.results["accepted"] == 23

    # With traffic the totals carry their proof again: one request.
    zone.add(event(T0 + 310, count=3))
    clock.advance(BUCKET)
    before = len(cloudflare.calls)
    run(exporter)
    assert [call["aliases"] for call in cloudflare.calls[before:]] == [ALL]
    assert values(analytics(exporter, storage), "cf_requests_5m", **ZONE) == {END + BUCKET: 3}


def test_two_empty_answers_are_not_enough(cloudflare, exporter, storage, clock, monkeypatch):
    monkeypatch.setattr(script, "REPAIR_ROUNDS", 2)
    cloudflare.add_zone("z1", "example.org")
    run(exporter)
    # The fresh hour: the usual range, then twice the long one. One look short.
    totals = values(analytics(exporter, storage), "cf_zone_requests_5m")
    assert max(totals) == T0 - 3300 and exporter.results["sampled"] == 12
    # The history started with the long range and got its three looks; so does
    # the retry of the fresh hour.
    assert exporter.results["accepted"] == 11
    clock.advance(script.RETRY_DELAYS[0])
    run(exporter)
    assert exporter.results["accepted"] == 23


def test_empty_answers_count_only_in_a_row(cloudflare, exporter, clock):
    cloudflare.add_zone("z1", "example.org")
    exporter.refresh(clock.now)
    plan = exporter.plans["z1"]
    exporter.zone_states["z1"].long_proof.add("totals")
    stats = script.ReadStats(states={"totals": "empty"}, ranges={"totals": (T0 - 86400, T0)})
    assert exporter._settle_proof(plan, stats) == {"totals"}
    assert exporter._settle_proof(plan, stats) == {"totals"}
    # An answer with groups between them: the zone is not idle.
    stats.states["totals"] = "sampled"
    assert exporter._settle_proof(plan, stats) == set() and stats.looks == {}
    stats.states["totals"] = "empty"
    assert exporter._settle_proof(plan, stats) == {"totals"}
    assert stats.looks == {"totals": 1} and stats.states["totals"] == "empty"
    exporter._settle_proof(plan, stats)
    exporter._settle_proof(plan, stats)
    assert stats.states["totals"] == "idle"
    # Being idle is a verdict, not another look to be counted or forgotten.
    assert exporter._settle_proof(plan, stats) == set() and stats.states["totals"] == "idle"


def test_reduced_answer_with_groups_between_empty_ones_keeps_the_zone_from_being_idle(
    cloudflare, exporter, storage, clock
):
    """The long range answered empty, from every tenth event, empty, empty: three
    empty answers, but not in a row, and the second one shows traffic."""
    busy = T0 - 2 * 86400
    cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 10, count=6),
        event(busy, count=50),
        not_older_than=10 * 86400 + 3600,
    )
    newest = exporter.plan(clock.now)
    exporter.zone_states["z1"].long_proof = {"totals", "edge_totals"}
    factors = [100, 10, 100, 100]

    def reduced(call, alias, groups):
        if call["index"] >= len(factors):
            return groups
        return sampled(set(ALL), factors[call["index"]])(call, alias, groups)

    cloudflare.transforms.append(reduced)
    exporter.read("fresh", "z1", newest - 3600, newest)
    assert [call["aliases"] for call in cloudflare.calls] == [ALL, TOTALS, TOTALS, TOTALS]
    assert exporter.results["accepted"] == 0 and exporter.results["sampled"] == 12
    assert analytics(exporter, storage) == []
    clock.advance(script.RETRY_DELAYS[0])
    run(exporter)
    assert values(analytics(exporter, storage), "cf_requests_5m", **ZONE)[END] == 6


def test_empty_answer_over_a_range_with_known_events_is_not_believed(
    cloudflare, exporter, storage, clock
):
    """A zone with a few requests now and then. Every answer from a reduced table
    is empty for it, however often it is asked: that must not look idle."""
    zone = cloudflare.add_zone("z1", "example.org", event(T0 - 3000, count=3))
    run(exporter)
    assert exporter.zone_states["z1"].last_event["totals"] == T0 - 2700
    zone.add(event(T0 + 310, count=3))
    cloudflare.transforms.append(sampled(set(ALL)))
    clock.advance(BUCKET)
    before = len(cloudflare.calls)
    run(exporter)
    # The earlier requests are known to lie in the range: an answer without
    # them is a reduced one, read again, and never a look at an idle zone.
    assert [call["aliases"] for call in cloudflare.calls[before:]] == [ALL, TOTALS, TOTALS, TOTALS]
    assert exporter.zone_states["z1"].long_proof == set()
    assert exporter.results["sampled"] == 1
    assert END + BUCKET not in values(analytics(exporter, storage), "cf_zone_requests_5m")

    cloudflare.transforms.clear()
    clock.advance(script.RETRY_DELAYS[0])
    run(exporter)
    samples = analytics(exporter, storage)
    assert values(samples, "cf_requests_5m", **ZONE) == {T0 - 2700: 3, END + BUCKET: 3}


def test_long_proof_range_is_left_when_the_zone_has_traffic_again(cloudflare, exporter, clock):
    zone = cloudflare.add_zone("z1", "example.org", not_older_than=10 * 86400 + 3600)
    run(exporter)
    assert exporter.zone_states["z1"].long_proof == {"totals", "edge_totals"}
    zone.add(
        *(
            event(T0 + 310 + index * BUCKET, source="edgeWorkerFetch")
            for index in range(script.LONG_PROOF_GROUPS)
        )
    )
    clock.advance(script.LONG_PROOF_GROUPS * BUCKET + BUCKET)
    newest = exporter.plan(clock.now)
    exporter.read("fresh", "z1", newest - 3600, newest)
    # Only the selection that has plenty of groups again; clients are still absent.
    assert exporter.zone_states["z1"].long_proof == {"edge_totals"}
    assert exporter._proof(exporter.plans["z1"], newest - 3600, newest) == {
        "totals": (newest - script.PROOF_SECONDS, newest),
        "edge_totals": (newest - script.LONG_PROOF_SECONDS, newest),
    }


@pytest.mark.parametrize("selection", ["groups", "totals", "edge", "edge_totals"])
def test_repair_reads_only_the_reduced_selection_again_with_another_page_size(
    cloudflare, exporter, storage, selection
):
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=57))
    cloudflare.transforms.append(first_calls(1, sampled({selection})))
    run(exporter)
    first, repair = cloudflare.calls[:2]
    assert repair["aliases"] == (selection,)
    assert repair["ranges"][selection] == first["ranges"][selection]
    # Cloudflare caches answers by query and variables: the repair must differ.
    assert repair["limits"][selection] == first["limits"][selection] - 1
    assert values(analytics(exporter, storage), "cf_requests_5m", **ZONE) == {END: 57}
    assert exporter.results["sampled"] == 0


def test_reduced_groups_of_a_bucket_never_pass_for_its_totals(cloudflare, exporter, storage):
    """Ten events in the full data and one row of the reduced table look the same."""
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=10))
    cloudflare.transforms.append(sampled({"groups", "edge"}))
    run(exporter)
    assert values(analytics(exporter, storage), "cf_requests_5m") == {}
    assert exporter.results["sampled"] == 1
    assert exporter.results["accepted"] == 22


# ------------------------------------------------------ consistency checks


def test_groups_that_do_not_add_up_to_the_totals_are_not_exported(
    cloudflare, exporter, storage, clock
):
    cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 10, count=4, clientRequestPath="/a"),
        event(T0 + 20, count=3, clientRequestPath="/b"),
    )

    def lose_a_group(call, alias, groups):
        return groups[:-1] if alias == "groups" else groups

    cloudflare.transforms.append(first_calls(4, lose_a_group))
    run(exporter)
    # Without a reduced selection to blame, every selection is read again.
    assert [call["aliases"] for call in cloudflare.calls[:4]] == [ALL] * 4
    assert len({call["limit"] for call in cloudflare.calls[:4]}) == 4
    assert values(analytics(exporter, storage), "cf_requests_5m") == {}
    assert exporter.results["inconsistent"] == 1
    # The other buckets of the window are consistent and accepted.
    assert exporter.results["accepted"] == 22

    clock.advance(script.RETRY_DELAYS[0])
    run(exporter)
    samples = analytics(exporter, storage)
    assert values(samples, "cf_requests_5m", path="/a") == {END: 4}
    assert values(samples, "cf_requests_5m", path="/b") == {END: 3}


def test_edge_groups_are_checked_against_client_totals(cloudflare, exporter, storage, clock):
    cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 10, count=4),
        event(T0 + 20, count=9, source="edgeWorkerFetch"),
    )

    def lose_edge(call, alias, groups):
        return [] if alias == "edge" else groups

    cloudflare.transforms.append(first_calls(4, lose_edge))
    run(exporter)
    assert exporter.results["inconsistent"] == 1
    clock.advance(script.RETRY_DELAYS[0])
    run(exporter)
    samples = analytics(exporter, storage)
    # Subrequests count everywhere but in the client-facing edge family.
    assert values(samples, "cf_edge_requests_5m", **ZONE) == {END: 4}
    assert values(samples, "cf_requests_5m", **ZONE) == {END: 13}
    assert values(samples, "cf_zone_requests_5m", **ZONE)[END] == 13


def test_more_client_requests_than_requests_is_a_contradiction(
    cloudflare, exporter, storage, clock
):
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4), event(T0 - 290, count=3))

    def lose_the_bucket(call, alias, groups):
        if alias not in ("groups", "totals"):
            return groups
        return [g for g in groups if g["dimensions"]["datetimeFiveMinutes"] != iso(T0)]

    # Groups and totals agree on an empty bucket, and the totals are proven by
    # their other bucket; only the client selections know better.
    cloudflare.transforms.append(first_calls(4, lose_the_bucket))
    run(exporter)
    assert exporter.results["inconsistent"] == 1
    samples = analytics(exporter, storage)
    assert END not in values(samples, "cf_zone_requests_5m")
    clock.advance(script.RETRY_DELAYS[0])
    run(exporter)
    assert values(analytics(exporter, storage), "cf_requests_5m", **ZONE) == {T0: 3, END: 4}


def test_ingestion_sampling_is_accepted_when_the_answer_is_proven_full(
    cloudflare, exporter, storage
):
    # A busy path that Cloudflare itself stores every tenth request of, next to
    # ordinary traffic in another bucket.
    cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 10, count=50, interval=10, clientRequestPath="/busy"),
        event(T0 - 290, count=5),
    )
    run(exporter)
    samples = analytics(exporter, storage)
    assert values(samples, "cf_requests_5m", **ZONE) == {T0: 5, END: 50}
    assert exporter.results["sampled"] == 0


def test_zone_stored_only_at_a_high_sample_interval_is_accepted_after_the_hold(
    cloudflare, exporter, storage, clock
):
    # No answer can ever prove the full data for this zone.
    cloudflare.add_zone(
        "z1", "example.org", event(T0 + 10, count=50, interval=10), not_older_than=13 * 3600
    )
    run(exporter)
    assert values(analytics(exporter, storage), "cf_zone_requests_5m") == {}
    accepted_at = None
    for _ in range(30):
        clock.advance(1800)
        run(exporter)
        if exporter.results["accepted_unverified"]:
            accepted_at = clock.now
            break
    assert accepted_at is not None
    assert accepted_at - (T0 + 600) >= script.UNVERIFIED_HOLD_SECONDS
    assert values(analytics(exporter, storage), "cf_requests_5m", **ZONE) == {END: 50}
    # Nothing of such a zone counts as verified.
    assert exporter.results["accepted"] == 0


def test_changing_reduced_answers_are_never_exported(cloudflare, exporter, storage, clock):
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=57), not_older_than=5 * 3600)

    def unstable(call, alias, groups):
        if alias != "totals":
            return groups
        factor = 10 * (1 + call["index"] % 5)
        return sampled({alias}, factor)(call, alias, groups)

    cloudflare.transforms.append(unstable)
    for _ in range(12):
        run(exporter)
        clock.advance(1800)
    samples = analytics(exporter, storage)
    assert values(samples, "cf_requests_5m") == {}
    assert END not in values(samples, "cf_zone_requests_5m")
    assert exporter.results["accepted_unverified"] == 0
    # It left the zone's reach unverified and is reported as lost.
    assert exporter.results["abandoned"] >= 1


def test_unconsumed_requests_do_not_count_as_deliveries(cloudflare, exporter, storage, clock):
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4))
    run(exporter)
    app = script.make_app(exporter, storage, CollectorRegistry())
    for _ in range(5):
        assert request(app, "/analytics")[0] == 200
    clock.advance(script.DELIVERY_SECONDS)
    exporter.prune(clock.now)
    assert values(analytics(exporter, storage), "cf_requests_5m", **ZONE) == {END: 4}


# ------------------------------------------------------------- unit checks


def group(count, interval=1.0, visits=0, **dimensions):
    return {
        "count": count,
        "sum": {"visits": visits},
        "avg": {"sampleInterval": interval},
        "dimensions": dimensions,
    }


@pytest.mark.parametrize(
    ("groups", "expected"),
    [
        ([], "empty"),
        ([group(0, 0)], "empty"),
        ([group(5)], "full"),
        ([group(50, 10)], "sampled"),
        ([group(500, 100), group(20, 10)], "sampled"),
        # One group from the full data proves the whole answer.
        ([group(50, 10), group(3, 1.5)], "full"),
        ([group(40, 4.99)], "full"),
        ([group(40, 5.0)], "sampled"),
    ],
)
def test_answer_state(groups, expected):
    assert script.answer_state(groups) == expected


FULL = dict.fromkeys(ALL, "full")


@pytest.mark.parametrize(
    ("states", "problem", "reason"),
    [
        (FULL, None, ""),
        ({**FULL, "totals": "idle", "edge_totals": "idle"}, None, ""),
        ({**FULL, "groups": "empty", "edge": "split"}, None, ""),
        ({**FULL, "totals": "sampled"}, "sampled", "totals at a reduced resolution"),
        ({**FULL, "totals": "empty"}, "sampled", "totals not proven complete"),
        ({**FULL, "edge_totals": "split"}, "sampled", "edge_totals not proven complete"),
        ({"groups": "full", "edge": "full"}, "sampled", "totals not proven complete"),
    ],
)
def test_bucket_needs_proven_totals(cloudflare, api, states, problem, reason):
    plan = plan_for(api, cloudflare.add_zone("z1", "example.org"))
    data = script.BucketData()
    result = script.evaluate(plan, script.Schema(), END, data, states)
    assert result.problem == problem
    assert reason in result.reason
    assert [sample[2] for sample in result.samples] == [0, 0]


@pytest.mark.parametrize(
    ("states", "problem"),
    [
        # The groups of this bucket look reduced: fine if their answer has a
        # group from the full data elsewhere, not if nothing proves it.
        (FULL, None),
        ({**FULL, "groups": "split"}, "sampled"),
        ({**FULL, "groups": "sampled"}, "sampled"),
    ],
)
def test_groups_at_a_high_sample_interval_need_a_proven_answer(cloudflare, api, states, problem):
    plan = plan_for(api, cloudflare.add_zone("z1", "example.org"))
    dimensions = {"clientRequestHTTPHost": "example.org", "edgeResponseStatus": 200}
    data = script.BucketData(
        groups=[group(50, 10, **dimensions)],
        edge=[group(50, 1, **dimensions)],
        totals=group(50, 10),
        edge_totals=group(50, 1),
    )
    result = script.evaluate(plan, script.Schema(), END, data, states)
    assert result.problem == problem
    if problem:
        assert "groups sample interval 10.00" in result.reason


@pytest.mark.parametrize(
    ("totals", "states", "holdable"),
    [
        # Cloudflare stores the zone at a high interval: groups, and they add up.
        (50, {**FULL, "totals": "sampled", "groups": "sampled"}, True),
        # No group for this bucket: a reduced answer may have left it out.
        (0, {**FULL, "totals": "sampled", "groups": "sampled"}, False),
        (50, {**FULL, "totals": "missing"}, False),
        (50, {**FULL, "totals": "empty"}, False),
        (50, {**FULL, "totals": "split"}, False),
        # Groups that do not add up never stand.
        (40, {**FULL, "totals": "sampled", "groups": "sampled"}, False),
    ],
)
def test_only_a_consistent_answer_with_the_bucket_may_be_held(
    cloudflare, api, totals, states, holdable
):
    plan = plan_for(api, cloudflare.add_zone("z1", "example.org"))
    dimensions = {"clientRequestHTTPHost": "example.org", "edgeResponseStatus": 200}
    data = script.BucketData(
        groups=[group(50, 10, **dimensions)] if totals else [],
        edge=[group(50, 10, **dimensions)] if totals else [],
        totals=group(totals, 10) if totals else None,
        edge_totals=group(50, 10) if totals else None,
    )
    result = script.evaluate(plan, script.Schema(), END, data, states)
    assert result.problem == "sampled"
    assert result.holdable is holdable


def test_bucket_without_the_edge_dataset_needs_no_client_totals(cloudflare, api):
    fields = [f for f in PRO_FIELDS if f != "dimensions_requestSource"]
    plan = plan_for(api, cloudflare.add_zone("z1", "example.org", fields=fields))
    data = script.BucketData(groups=[group(4, clientRequestHTTPHost="h")], totals=group(4))
    states = {"groups": "full", "totals": "full"}
    result = script.evaluate(plan, script.Schema(), END, data, states)
    assert result.problem is None
    assert not any(sample[0] == "cf_edge_requests_5m" for sample in result.samples)


@pytest.mark.parametrize(
    ("groups", "expected"),
    [
        ([], 0.0),
        ([group(5)], 1.0),
        ([group(50, 10)], 10.0),
        # 20 stored rows for 110 events.
        ([group(10), group(100, 10)], 5.5),
        ([group(0, 0)], 0.0),
    ],
)
def test_effective_interval(groups, expected):
    assert script.effective_interval(groups) == pytest.approx(expected)


def test_proof_range_is_a_day_before_the_window_or_after_it_at_the_edge(
    cloudflare, exporter, clock
):
    cloudflare.add_zone("z1", "example.org", not_older_than=31 * 86400)
    exporter.refresh(clock.now)
    plan = exporter.plans["z1"]
    day = script.PROOF_SECONDS
    assert exporter._proof(plan, END - 3600, END) == {
        "totals": (END - day, END),
        "edge_totals": (END - day, END),
    }
    # The oldest hour within reach has nothing before it: the day after serves.
    oldest = script.bucket_floor(clock.now - plan.reach_seconds)
    assert exporter._proof(plan, oldest, oldest + 3600)["totals"] == (oldest, oldest + day)
    # In between, whatever is before, and the rest after.
    start = oldest + 7200
    assert exporter._proof(plan, start, start + 3600)["totals"] == (oldest, oldest + day)
    # A quiet zone's selection is read over a week, and no further.
    exporter.zone_states["z1"].long_proof.add("edge_totals")
    ranges = exporter._proof(plan, END - 3600, END)
    assert ranges["totals"] == (END - day, END)
    assert ranges["edge_totals"] == (END - 7 * day, END)
    assert plan.long_proof_seconds == 7 * day


# ------------------------------------------------------------------ rechecks


def test_recheck_time_groups_the_buckets_of_an_hour():
    assert script.recheck_time(T0) == T0 + 3600
    assert script.recheck_time(T0 + BUCKET) == T0 + 2 * 3600
    assert script.recheck_time(T0 + 3600) == T0 + 2 * 3600


def test_recheck_reads_an_hour_once_and_keeps_unchanged_buckets(
    cloudflare, exporter, storage, clock
):
    cloudflare.add_zone("z1", "example.org", event(T0 - 10, count=4))
    run(exporter)
    deliver(exporter, storage, clock)
    before = len(cloudflare.calls)
    clock.now = T0 + 3600 - 1
    run(exporter)
    recheck_calls = [c for c in cloudflare.calls[before:] if c["until"] <= T0]
    assert recheck_calls == []
    clock.now = T0 + 3600
    run(exporter)
    recheck_calls = [c for c in cloudflare.calls[before:] if c["until"] <= T0]
    # The ten buckets of the hour that were accepted young, in one request.
    assert [(c["since"], c["until"]) for c in recheck_calls] == [(T0 - 10 * BUCKET, T0)]
    assert exporter.results["recheck_unchanged"] == 10
    assert exporter.results["recheck_changed"] == 0
    # Nothing is served twice.
    assert T0 not in values(analytics(exporter, storage), "cf_zone_requests_5m")


def test_late_events_found_by_the_recheck_are_served_again(cloudflare, exporter, storage, clock):
    zone = cloudflare.add_zone("z1", "example.org", event(T0 - 10, count=4))
    run(exporter)
    assert values(analytics(exporter, storage), "cf_requests_5m", **ZONE) == {T0: 4}
    deliver(exporter, storage, clock)
    assert values(analytics(exporter, storage), "cf_requests_5m") == {}
    zone.add(event(T0 - 20, count=3))
    clock.now = T0 + 3600
    run(exporter)
    assert exporter.results["recheck_changed"] == 1
    samples = analytics(exporter, storage)
    # Same timestamp, new value.
    assert values(samples, "cf_requests_5m", **ZONE) == {T0: 7}
    assert values(samples, "cf_zone_requests_5m", **ZONE)[T0] == 7


def test_recheck_that_does_not_verify_changes_nothing_and_is_given_up(
    cloudflare, exporter, storage, clock
):
    cloudflare.add_zone("z1", "example.org", event(T0 - 10, count=40))
    run(exporter)
    deliver(exporter, storage, clock)
    cloudflare.transforms.append(
        lambda call, alias, groups: (
            sampled({"groups", "totals"})(call, alias, groups)
            if call["ranges"].get("groups", (0, 0))[1] <= T0 or "groups" not in call["ranges"]
            else groups
        )
    )
    clock.now = T0 + 3600
    run(exporter)
    assert exporter.results["recheck_failed"] == 10
    assert exporter.results["recheck_changed"] == 0
    before = len(cloudflare.calls)
    clock.advance(script.RECHECK_RETRY_SECONDS - 1)
    run(exporter)
    assert not [c for c in cloudflare.calls[before:] if c["until"] <= T0]
    # Retried until six hours after the bucket, then left as accepted.
    for _ in range(30):
        clock.advance(script.RECHECK_RETRY_SECONDS)
        run(exporter)
    before = len(cloudflare.calls)
    clock.advance(script.RECHECK_RETRY_SECONDS)
    run(exporter)
    assert not [c for c in cloudflare.calls[before:] if c["until"] <= T0]
    assert exporter.results["recheck_changed"] == 0
    assert values(analytics(exporter, storage), "cf_requests_5m") == {}


def test_recheck_never_replaces_a_bucket_with_events_by_an_empty_answer(
    cloudflare, exporter, storage, clock
):
    """A quiet zone answered from the reduced table at the recheck: every answer is
    empty. The bucket is known to have events, so the answers left them out; they
    must not pass for an idle zone."""
    cloudflare.add_zone("z1", "example.org", event(T0 - 10, count=4), event(T0 + 10, count=3))
    run(exporter)
    deliver(exporter, storage, clock)
    cloudflare.transforms.append(sampled(set(ALL)))
    clock.now = T0 + 3600
    run(exporter)
    assert exporter.results["recheck_changed"] == 0
    assert exporter.results["recheck_failed"] >= 1
    # Nothing is served again, and no new bucket is taken for an idle one.
    assert values(analytics(exporter, storage), "cf_zone_requests_5m") == {}
    assert exporter.results["accepted"] == 23


def test_history_after_a_lost_database_is_not_taken_for_an_idle_zone(
    cloudflare, api, storage, clock
):
    """The database is gone; the fresh hour is read from the full data, the history
    behind it only from the reduced table, which has no row of this quiet zone."""
    cloudflare.add_zone(
        "z1",
        "example.org",
        *(event(at, count=3) for at in range(T0 - 3 * 3600, T0 + BUCKET, BUCKET)),
        not_older_than=4 * 3600,
    )
    cloudflare.transforms.append(
        lambda call, alias, groups: (
            groups if call["index"] == 0 else sampled(set(ALL))(call, alias, groups)
        )
    )
    exporter = script.Exporter(api, storage, clock=clock)
    run(exporter)
    # Only the fresh hour, read from the full data: twelve buckets.
    oldest = T0 + BUCKET - 11 * BUCKET
    totals = values(analytics(exporter, storage), "cf_zone_requests_5m")
    assert totals == dict.fromkeys(range(oldest, T0 + 2 * BUCKET, BUCKET), 3)
    # Every proof range of the history reached the known events of the fresh hour.
    history = [c for c in cloudflare.calls[1:] if "totals" in c["ranges"]]
    assert history and all(c["ranges"]["totals"][1] >= oldest for c in history)
    assert exporter.results["accepted"] == len(totals)


def test_proof_range_is_widened_to_the_nearest_known_event(cloudflare, exporter, storage, clock):
    cloudflare.add_zone("z1", "example.org", not_older_than=10 * 86400 + 3600)
    exporter.refresh(clock.now)
    plan = exporter.plans["z1"]
    window = (T0 - 2 * 86400 - 3600, T0 - 2 * 86400)
    events = script.BucketResult(T0 - 86400 + BUCKET, (), "s", requests=2, client_requests=2)
    storage.accept("z1", events, clock.now)
    ranges = exporter._proof(plan, *window)
    # A day before the window, and up to the known event after it.
    assert ranges["totals"] == (window[1] - script.PROOF_SECONDS, T0 - 86400 + BUCKET)
    # Never beyond a week from the window.
    far = (T0 - 9 * 86400, T0 - 9 * 86400 + 3600)
    assert exporter._proof(plan, *far)["totals"] == (far[1] - script.PROOF_SECONDS, far[1])


def test_empty_answer_with_known_events_is_never_accepted_unverified(
    cloudflare, exporter, storage, clock
):
    """Six hours of the same answer accept a zone that Cloudflare stores at a high
    sample interval. An answer without the events known to be there is no such
    answer, however often it comes."""
    zone = cloudflare.add_zone(
        "z1", "example.org", event(T0 - 10, count=4), not_older_than=13 * 3600
    )
    run(exporter)
    zone.add(event(T0 + 310, count=3))
    cloudflare.transforms.append(sampled(set(ALL)))
    for _ in range(20):
        clock.advance(1800)
        run(exporter)
    assert exporter.results["accepted_unverified"] == 0
    assert END + BUCKET not in values(analytics(exporter, storage), "cf_zone_requests_5m")


def test_contradicting_answers_are_never_accepted_unverified(cloudflare, exporter, storage, clock):
    cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 10, count=4),
        event(T0 + 20, count=5, clientRequestHTTPMethodName="POST"),
        not_older_than=13 * 3600,
    )

    def lose_post(call, alias, groups):
        if alias != "groups":
            return groups
        return [g for g in groups if g["dimensions"].get("clientRequestHTTPMethodName") != "POST"]

    cloudflare.transforms.append(lose_post)
    for _ in range(20):
        run(exporter)
        clock.advance(1800)
    assert exporter.results["accepted_unverified"] == 0
    assert exporter.results["inconsistent"] >= 10
    assert END not in values(analytics(exporter, storage), "cf_zone_requests_5m")


def test_changed_labels_do_not_serve_a_bucket_twice(
    cloudflare, exporter, make_exporter, storage, clock
):
    """A bucket already served with its labels: after a restart with other labels a
    recheck would add a second series for the same events, and a sum over labels
    would count them twice."""
    zone = cloudflare.add_zone("z1", "example.org", event(T0 - 10, count=4))
    run(exporter)
    deliver(exporter, storage, clock)
    zone.add(event(T0 - 20, count=3))  # late events the recheck would find
    disabled = script.Schema({"cf_requests_5m": frozenset({"query"})})
    restarted = make_exporter(schema=disabled)
    clock.now = T0 + 3600
    run(restarted)
    assert restarted.results["recheck_skipped"] > 0
    assert restarted.results["recheck_changed"] == 0
    assert T0 not in values(analytics(restarted, storage), "cf_zone_requests_5m")
    # Buckets read from now on carry the new labels and are rechecked as usual.
    clock.now = T0 + 2 * 3600
    run(restarted)
    assert restarted.results["recheck_unchanged"] > 0


def test_recheck_of_a_bucket_rendered_otherwise_serves_nothing(
    cloudflare, exporter, storage, clock, api
):
    """The layout can change between the settings refresh that cancels rechecks and
    the read itself; the recheck still serves nothing."""
    zone = cloudflare.add_zone("z1", "example.org")
    plan = plan_for(api, zone)
    storage.accept("z1", script.BucketResult(T0, (), "old"), clock.now, layout="other")
    deliver(exporter, storage, clock)
    changed = script.BucketResult(T0, (("cf_zone_requests_5m", "{}", 1.0),), "new")
    exporter._recheck(plan, changed, clock.now, exporter.layout(plan))
    assert exporter.results["recheck_skipped"] == 1
    assert storage.exposition(10) == ([], [])


def test_bucket_first_read_long_after_its_end_is_not_rechecked(cloudflare, exporter, clock):
    cloudflare.add_zone("z1", "example.org", event(T0 - 4000, count=4))
    run(exporter)
    before = len(cloudflare.calls)
    clock.now = T0 + 3 * 3600
    run(exporter)
    # Only the buckets accepted within an hour of their end are read again.
    old = [c for c in cloudflare.calls[before:] if c["until"] <= T0 - 3000]
    assert old == []


def test_recheck_of_a_bucket_the_storage_no_longer_knows_changes_nothing(
    cloudflare, exporter, storage, clock, api
):
    zone = cloudflare.add_zone("z1", "example.org")
    plan = plan_for(api, zone)
    result = script.BucketResult(T0, (("cf_zone_requests_5m", "{}", 1.0),), "s")
    # For example after the database was recreated while the read was under way.
    exporter._recheck(plan, result, clock.now, exporter.layout(plan))
    assert storage.exposition(10) == ([], [])
    assert not any(exporter.results.values())
