# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Which buckets are read when: settling, priorities, history, pacing, retries,
failures and zone changes."""

import json
import logging
import sqlite3
import threading
from itertools import pairwise

import pytest
from conftest import BUCKET, PRO_FIELDS, T0, analytics, event, health, run, sampled, values

import script

E = T0 + 3000  # some bucket end


def first_requests(cloudflare, start=0):
    """The first request of every read: the one asking for all selections."""
    return [c for c in cloudflare.calls[start:] if len(c["aliases"]) == 4 and c["after"] is None]


def buckets_of(call) -> int:
    return (call["until"] - call["since"]) // BUCKET


class Stop:
    """A stop event whose waits pass on the test clock."""

    def __init__(self, clock):
        self.clock = clock
        self.waits = []
        self.stopped = False

    def is_set(self):
        return self.stopped

    def wait(self, timeout):
        self.waits.append(timeout)
        self.clock.advance(timeout)


@pytest.mark.parametrize(
    ("candidates", "limit", "expected"),
    [
        ([], 12, None),
        ([(E, 0, 0), (E - 300, 0, 0), (E - 600, 0, 0)], 12, (E - 900, E)),
        ([(E, 0, 0), (E - 300, 0, 0), (E - 600, 0, 0)], 2, (E - 600, E)),
        ([(E, 0, 0), (E - 600, 0, 0)], 12, (E - 300, E)),
        ([(E - 300 * i, 0, 0) for i in range(80)], 72, (E - 72 * 300, E)),
        # Buckets that failed verification before are retried in small windows.
        ([(E - 300 * i, 1, 0) for i in range(80)], 72, (E - 12 * 300, E)),
        ([(E - 300 * i, 9, 0) for i in range(80)], 72, (E - 12 * 300, E)),
        ([(E - 300 * i, 1, 0) for i in range(20)], 4, (E - 4 * 300, E)),
        ([(E - 300 * i, int(i == 20), 0) for i in range(80)], 72, (E - 20 * 300, E)),
        ([(E - 300 * i, int(i == 5), 0) for i in range(80)], 72, (E - 12 * 300, E)),
        # A bucket whose read failed is read alone and joins no other window.
        ([(E - 300 * i, 0, 1) for i in range(80)], 72, (E - 300, E)),
        ([(E, 0, 3), (E - 300, 0, 0)], 12, (E - 300, E)),
        ([(E - 300 * i, 0, int(i == 4)) for i in range(80)], 72, (E - 4 * 300, E)),
        ([(E - 300 * i, 2, int(i == 4)) for i in range(80)], 72, (E - 4 * 300, E)),
    ],
)
def test_window_of_due_buckets(candidates, limit, expected):
    assert script.Exporter._window(candidates, limit) == expected


def test_bucket_is_read_only_after_it_has_settled(cloudflare, exporter, storage, clock):
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4))
    clock.now = T0 + BUCKET + script.SETTLE_SECONDS - 1
    run(exporter)
    assert max(c["until"] for c in cloudflare.calls) == T0
    assert values(analytics(exporter, storage), "cf_requests_5m") == {}
    clock.advance(1)
    run(exporter)
    assert (cloudflare.calls[-1]["since"], cloudflare.calls[-1]["until"]) == (T0, T0 + BUCKET)
    assert values(analytics(exporter, storage), "cf_requests_5m") == {T0 + BUCKET: 4}


def test_fresh_buckets_of_every_zone_come_before_history(cloudflare, exporter):
    cloudflare.add_zone("za", "a.example", not_older_than=6 * 3600)
    cloudflare.add_zone("zb", "b.example", not_older_than=6 * 3600)
    run(exporter)
    reads = first_requests(cloudflare)
    newest = T0 + BUCKET
    assert [(c["zone"], c["since"], c["until"]) for c in reads[:2]] == [
        ("za", newest - 12 * BUCKET, newest),
        ("zb", newest - 12 * BUCKET, newest),
    ]
    history = reads[2:]
    assert all(c["until"] <= newest - 12 * BUCKET for c in history)
    # History moves back in time evenly, the zone with the newest gap first.
    assert [c["zone"] for c in history[:4]] == ["za", "zb", "za", "zb"]
    assert [c["until"] for c in history] == sorted((c["until"] for c in history), reverse=True)


def test_history_reaches_the_dataset_limit_and_not_beyond(cloudflare, exporter, clock, storage):
    cloudflare.add_zone("z1", "example.org", not_older_than=5 * 3600)
    run(exporter)
    # The exporter keeps an hour away from the edge of notOlderThan.
    horizon = script.bucket_floor(clock.now - 4 * 3600) + BUCKET
    assert min(c["since"] for c in cloudflare.calls) == horizon - BUCKET
    totals = values(analytics(exporter, storage), "cf_zone_requests_5m")
    assert min(totals) == horizon and max(totals) == T0 + BUCKET
    assert len(totals) == (T0 + BUCKET - horizon) // BUCKET + 1


def test_backfill_days_shorten_the_history_but_not_the_proof(cloudflare, make_exporter, clock):
    cloudflare.add_zone("z1", "example.org", not_older_than=31 * 86400)
    run(make_exporter(backfill_days=1))
    windows = [c["ranges"]["groups"] for c in cloudflare.calls if "groups" in c["ranges"]]
    assert min(since for since, _ in windows) == script.bucket_floor(clock.now - 86400)
    # A quiet zone is still judged over a week, not over the shorter history.
    proofs = [c["ranges"]["totals"] for c in cloudflare.calls if "totals" in c["ranges"]]
    assert max(until - since for since, until in proofs) == script.LONG_PROOF_SECONDS


def test_history_windows_grow_while_pages_stay_small(cloudflare, exporter, clock):
    cloudflare.add_zone("z1", "example.org", not_older_than=2 * 86400 + 3600)
    run(exporter)
    history = [buckets_of(c) for c in first_requests(cloudflare)[1:]]
    assert history[:5] == [12, 24, 48, 72, 72]
    assert max(history) == script.MAX_WINDOW_BUCKETS
    # Every bucket that lies completely within two days was read exactly once.
    horizon = script.bucket_floor(clock.now - 2 * 86400) + BUCKET
    assert sum(history) + 12 == (T0 + BUCKET - horizon) // BUCKET + 1 == 575


def test_history_windows_respect_the_dataset_max_duration(cloudflare, exporter):
    cloudflare.add_zone("z1", "example.org", not_older_than=86400, max_duration=2 * 3600)
    run(exporter)
    assert max(buckets_of(c) for c in cloudflare.calls) == 24


@pytest.mark.parametrize("api", [{"page_size": 20}], indirect=True)
def test_history_windows_shrink_after_a_full_page(cloudflare, exporter, clock):
    # Two countries in every bucket: 24 groups per 12 buckets against a
    # configured page of 20.
    start = script.bucket_floor(clock.now - 12 * 3600)
    cloudflare.add_zone(
        "z1",
        "example.org",
        *(
            event(at, clientCountryName=country)
            for at in range(start, T0, BUCKET)
            for country in ("DE", "FR")
        ),
        not_older_than=10 * 3600,
    )
    run(exporter)
    history = [buckets_of(c) for c in first_requests(cloudflare)[1:]]
    assert history[0] == 12 and set(history[1:6]) == {6}
    assert exporter.results["accepted"] == sum(history) + 12


def test_history_waits_while_too_many_samples_are_not_scraped(
    cloudflare, exporter, storage, clock, monkeypatch
):
    monkeypatch.setattr(script, "BACKLOG_SAMPLE_LIMIT", 20)
    cloudflare.add_zone("z1", "example.org")
    run(exporter)
    # Twelve fresh buckets with two zone totals each are waiting.
    assert storage.undelivered_samples() == 24
    assert len(first_requests(cloudflare)) == 1
    for _ in range(script.DELIVERY_RESPONSES):
        analytics(exporter, storage, consume=True)
    clock.advance(script.DELIVERY_SECONDS)
    run(exporter)
    # The two buckets that became due, then the history that is still in reach.
    reads = first_requests(cloudflare)
    assert [buckets_of(c) for c in reads] == [12, 2, 9]
    assert exporter.results["accepted"] == 23


def test_new_bucket_interrupts_history(cloudflare, exporter, clock):
    cloudflare.add_zone("z1", "example.org", not_older_than=2 * 86400)

    def slow(call, alias, groups):
        clock.advance(25)  # four selections: every request takes 100 seconds
        return groups

    cloudflare.transforms.append(slow)
    stop = threading.Event()
    assert exporter.tick(stop) is True
    interrupted = len(cloudflare.calls)
    # Far from all of the two days of history was read.
    assert interrupted < 10
    exporter.tick(stop)
    # The bucket that became due is read before more history.
    resumed = cloudflare.calls[interrupted]
    assert (resumed["since"], resumed["until"]) == (T0 + BUCKET, T0 + 2 * BUCKET)


def test_history_reads_are_paced_and_fresh_reads_are_not(cloudflare, exporter, clock, monkeypatch):
    monkeypatch.setattr(script, "HISTORY_INTERVAL_SECONDS", 5)
    cloudflare.add_zone("z1", "example.org", not_older_than=5 * 3600)
    stop = Stop(clock)
    while exporter.tick(stop):
        pass
    history_reads = len(first_requests(cloudflare)) - 1
    assert history_reads >= 2
    assert stop.waits == [5] * (history_reads - 1)


def test_history_interval_can_be_configured(cloudflare, api, storage, clock):
    """Two instances fit Cloudflare's rate limit at the default pace; more
    instances, or other users of the token, need a longer interval."""
    cloudflare.add_zone("z1", "example.org", not_older_than=5 * 3600)
    exporter = script.Exporter(api, storage, clock=clock, history_interval=40)
    stop = Stop(clock)
    while exporter.tick(stop):
        pass
    history_reads = len(first_requests(cloudflare)) - 1
    assert history_reads >= 2 and exporter.history_pace() == 40
    # Waited in pieces, never longer than the loop interval at once.
    assert sum(stop.waits) >= 40 * (history_reads - 1)
    assert max(stop.waits) <= script.LOOP_SECONDS


def test_history_pace_never_holds_back_a_new_bucket(cloudflare, exporter, clock, monkeypatch):
    """A long pause between history reads is waited in pieces: the next bucket is
    read when it settles, not when the pause is over."""
    monkeypatch.setattr(script, "HISTORY_INTERVAL_SECONDS", 240)
    cloudflare.add_zone("z1", "example.org", not_older_than=2 * 86400)
    stop = Stop(clock)
    settles = T0 + 2 * BUCKET + script.SETTLE_SECONDS
    # History keeps the loop busy; it stops exactly when the next bucket settles.
    assert exporter.tick(stop) is True
    assert clock.now == settles
    assert max(stop.waits) <= script.LOOP_SECONDS
    before = len(cloudflare.calls)
    exporter.tick(stop)
    fresh = cloudflare.calls[before]
    assert (fresh["since"], fresh["until"]) == (T0 + BUCKET, T0 + 2 * BUCKET)


def test_full_disk_pauses_reads_and_loses_nothing(cloudflare, api, clock, monkeypatch, caplog):
    """What is read while the disk is full cannot be stored: reads wait, and the
    database is neither wiped nor read again from the start."""
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4))
    storage = script.Storage(None, monotonic=clock)
    exporter = script.Exporter(api, storage, clock=clock)
    accept = storage.accept

    def full(*args, **kwargs):
        def operation(conn):
            error = sqlite3.OperationalError("database or disk is full")
            error.sqlite_errorcode = sqlite3.SQLITE_FULL
            raise error

        storage._run(operation)

    monkeypatch.setattr(storage, "accept", full)

    class StopAt(Stop):
        def wait(self, timeout):
            super().wait(timeout)
            self.stopped = self.clock.now >= T0 + 600 + 150

    with caplog.at_level(logging.ERROR):
        exporter.run(StopAt(clock))
    assert "disk is full" in caplog.text and "storage operation failed" in caplog.text
    # One read, then a minute without any, then one more.
    starts = [call["index"] for call in first_requests(cloudflare)]
    assert len(starts) == 3 and storage.resets == 0
    assert health(exporter, storage)[("cf_exporter_storage_full", ())] == 1
    monkeypatch.setattr(storage, "accept", accept)
    clock.advance(script.STORAGE_RETRY_SECONDS)
    run(exporter)
    assert values(analytics(exporter, storage), "cf_requests_5m") == {T0 + BUCKET: 4}
    assert health(exporter, storage)[("cf_exporter_storage_full", ())] == 0


def test_fresh_and_recheck_windows_respect_the_dataset_max_duration(cloudflare, exporter, clock):
    cloudflare.add_zone("z1", "example.org", event(T0 - 10, count=4), max_duration=1800)
    run(exporter)
    clock.now = T0 + 3600
    run(exporter)
    assert exporter.results["error"] == 0
    assert exporter.results["recheck_unchanged"] > 0
    assert max(buckets_of(c) for c in cloudflare.calls) == 6


def test_small_failed_history_read_leaves_the_window_size_alone(cloudflare, exporter, clock):
    cloudflare.add_zone("z1", "example.org", not_older_than=2 * 86400)
    exporter.refresh(clock.now)
    zone = exporter.zone_states["z1"]
    zone.window = 48
    ends = [T0 - 20 * BUCKET + index * BUCKET for index in range(3)]
    exporter._postpone("history", zone, ends, False)
    assert (zone.window, zone.window_ceiling) == (48, None)
    full = [T0 - 100 * BUCKET + index * BUCKET for index in range(48)]
    exporter._postpone("history", zone, full, False)
    assert (zone.window, zone.window_ceiling) == (24, 24)


def test_history_is_read_more_slowly_after_the_rate_limit_was_reached(
    cloudflare, exporter, clock, monkeypatch, caplog
):
    """The limit is shared with other instances and users of the token. A request
    that meets it waits for minutes, with the fresh buckets behind it."""
    monkeypatch.setattr(script, "HISTORY_INTERVAL_SECONDS", 5)
    cloudflare.add_zone("z1", "example.org", not_older_than=5 * 3600)
    exporter.api.stats.record("graphql", status=429, error="rate_limited")
    stop = Stop(clock)
    with caplog.at_level(logging.WARNING):
        while exporter.tick(stop):
            pass
    assert "history is read every 10s" in caplog.text
    assert stop.waits and set(stop.waits) == {10}

    # Every further rejection doubles the interval, up to a maximum.
    for _ in range(10):
        exporter.api.stats.record("graphql", error="budget_depleted")
        exporter._watch_rate_limit(clock.now)
    assert exporter._slowdown == script.HISTORY_SLOWDOWN_MAX
    # Without rejections it halves again, hour by hour.
    clock.advance(script.HISTORY_RECOVER_SECONDS - 1)
    exporter._watch_rate_limit(clock.now)
    assert exporter._slowdown == script.HISTORY_SLOWDOWN_MAX
    factors = []
    for _ in range(6):
        clock.advance(script.HISTORY_RECOVER_SECONDS)
        exporter._watch_rate_limit(clock.now)
        factors.append(exporter._slowdown)
    assert factors == [8, 4, 2, 1, 1, 1]


def test_buckets_that_fail_verification_are_retried_with_growing_delays(
    cloudflare, exporter, clock
):
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=57), not_older_than=6 * 3600)
    reads = []

    def reduced(call, alias, groups):
        window = call["ranges"].get("groups")
        if alias == "totals" and window and window[0] <= T0 + 10 < window[1]:
            reads.append((clock.now, (window[1] - window[0]) // BUCKET))
        return sampled({"totals"})(call, alias, groups)

    cloudflare.transforms.append(reduced)
    for _ in range(100):
        run(exporter)
        clock.advance(60)
    times = [at for at, _ in reads]
    assert [later - earlier for earlier, later in pairwise(times)] == [
        120,
        240,
        480,
        900,
        1800,
        1800,
    ]
    # A retry stays a small window, whatever else is due.
    assert max(size for _, size in reads) <= script.RETRY_WINDOW_BUCKETS
    pending = health(exporter, exporter.storage)[
        ("cf_exporter_pending_buckets", (("zone_name", "example.org"),))
    ]
    assert pending > 0


def test_failed_read_is_retried_without_counting_as_an_attempt(
    cloudflare, exporter, storage, clock
):
    zone = cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4))
    run(exporter)  # settings are fetched; the fresh read follows
    zone.error = "unable to execute task"
    clock.advance(BUCKET)
    before = len(cloudflare.calls)
    run(exporter)
    assert len(cloudflare.calls) == before + 1
    metrics = health(exporter, storage)
    assert metrics[("cf_exporter_zone_success", (("zone_name", "example.org"),))] == 0
    assert metrics[("cf_exporter_bucket_reads_total", (("result", "error"),))] == 1
    # Not again before the retry delay.
    clock.advance(script.ERROR_RETRY_SECONDS - 1)
    run(exporter)
    assert len(cloudflare.calls) == before + 1
    zone.error = None
    clock.advance(1)
    run(exporter)
    assert len(cloudflare.calls) == before + 2
    # A failed request is no failed verification of the bucket.
    assert storage.bucket("z1", T0 + 2 * BUCKET).attempts == 0
    assert (
        health(exporter, storage)[("cf_exporter_zone_success", (("zone_name", "example.org"),))]
        == 1
    )


def test_bucket_that_cannot_be_read_does_not_hold_back_its_zone(
    cloudflare, exporter, storage, clock
):
    """Every read of one bucket fails. The other buckets of its window and the
    history behind it are read all the same, and it is tried ever more rarely."""
    bad = T0 - 4 * BUCKET
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4), event(bad - 10, count=9))
    attempts = []

    def poison(call):
        if "groups" in call["aliases"] and call["since"] < bad <= call["until"]:
            attempts.append((clock.now, buckets_of(call)))
            return "unable to execute task"
        return None

    cloudflare.failures.append(poison)
    start = clock.now
    for _ in range(31):
        run(exporter)
        clock.advance(60)
    # The window of the fresh hour, once more as most failures pass, then bucket
    # by bucket: only the bad one fails, with growing delays.
    assert [(at - start, size) for at, size in attempts] == [
        (0, 12),
        (60, 12),
        (120, 1),
        (240, 1),
        (480, 1),
        (960, 1),
    ]
    assert exporter.results["error"] == 6
    assert storage.bucket("z1", bad).errors == 5
    # Everything else was read: the rest of the hour, the history, new buckets.
    totals = values(analytics(exporter, storage), "cf_zone_requests_5m")
    assert sorted(totals) == [end for end in range(T0 - 6300, T0 + 2100 + 1, BUCKET) if end != bad]
    assert totals[T0 + BUCKET] == 4
    pending = health(exporter, storage)[
        ("cf_exporter_pending_buckets", (("zone_name", "example.org"),))
    ]
    assert pending == 1
    # A bucket that was never read counts as abandoned when it leaves the reach.
    cloudflare.failures.clear()
    exporter.prune(bad + 3 * 3600)
    assert exporter.results["abandoned"] == 1


def test_buckets_do_not_keep_waiting_after_cloudflare_recovers(
    cloudflare, exporter, storage, clock
):
    """While every read fails, the buckets tried wait longer and longer. The first
    read that works shows that Cloudflare was failing, not they."""
    zone = cloudflare.add_zone("z1", "example.org", event(T0 + 310, count=4))
    run(exporter)
    zone.error = "unable to execute task"
    clock.advance(BUCKET)
    for _ in range(8):
        run(exporter)
        clock.advance(60)
    # The bucket with the requests failed four times and waits for minutes more.
    assert storage.bucket("z1", T0 + 2 * BUCKET).errors == 4
    assert storage.pending("z1", clock.now, 0, clock.now, 10) == [(T0 + 3 * BUCKET, 0, 2)]
    zone.error = None
    run(exporter)
    assert values(analytics(exporter, storage), "cf_requests_5m") == {T0 + 2 * BUCKET: 4}
    pending = health(exporter, storage)[
        ("cf_exporter_pending_buckets", (("zone_name", "example.org"),))
    ]
    assert pending == 0


def test_bucket_that_cannot_be_read_is_found_in_a_history_window(cloudflare, exporter, clock):
    """History windows that fail are halved down to the one bucket that fails alone;
    everything around it is read."""
    bad = T0 - 30 * BUCKET
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4), not_older_than=7 * 3600)
    sizes = []

    def poison(call):
        if "groups" in call["aliases"] and call["since"] < bad <= call["until"]:
            sizes.append(buckets_of(call))
            return "unable to execute task"
        return None

    cloudflare.failures.append(poison)
    for _ in range(20):
        run(exporter)
        clock.advance(script.ERROR_RETRY_SECONDS)
    # Ever smaller windows around the bucket, and in the end the bucket alone.
    assert sizes[0] > 1 and sizes[-3:] == [1, 1, 1] and len(sizes) < 15
    assert exporter.storage.bucket("z1", bad).errors >= 3
    pending = health(exporter, exporter.storage)[
        ("cf_exporter_pending_buckets", (("zone_name", "example.org"),))
    ]
    assert pending == 1


def test_history_window_that_fails_is_not_grown_back_at_once(cloudflare, exporter, clock):
    """A window too large for one request is halved, and the next windows stay at
    that size until the settings are fetched again."""
    cloudflare.add_zone("z1", "example.org", not_older_than=2 * 86400 + 3600)
    cloudflare.failures.append(
        lambda call: (
            "query timed out" if buckets_of(call) > 24 and "groups" in call["aliases"] else None
        )
    )
    for _ in range(3):
        run(exporter)
        clock.advance(script.ERROR_RETRY_SECONDS)
    history = [buckets_of(c) for c in first_requests(cloudflare)[1:]]
    # 12 and 24 buckets work, 48 fail; from then on 24 at a time, never 48 again.
    assert history[:3] == [12, 24, 48]
    assert set(history[3:-1]) == {24} and len(history) > 20
    assert sum(history) - 48 + 12 == 575  # every bucket of the two days exactly once
    assert exporter.zone_states["z1"].window_ceiling == 24
    assert exporter.results["error"] == 1
    # Fetching the settings again lifts the ceiling.
    clock.advance(script.SETTINGS_REFRESH_SECONDS)
    exporter.refresh(clock.now)
    assert exporter.zone_states["z1"].window_ceiling is None


def test_failing_zone_does_not_hold_back_the_others(cloudflare, exporter, storage):
    cloudflare.add_zone("za", "a.example", event(T0 + 10, count=4)).error = "not authorized"
    cloudflare.add_zone("zb", "b.example", event(T0 + 10, count=6))
    run(exporter)
    samples = analytics(exporter, storage)
    assert values(samples, "cf_requests_5m", zone_name="b.example") == {T0 + BUCKET: 6}
    assert values(samples, "cf_zone_requests_5m", zone_name="a.example") == {}
    assert exporter.snapshot().success == {"a.example": 0, "b.example": 1}


def test_unexpected_error_in_one_read_is_contained(
    cloudflare, exporter, storage, clock, monkeypatch, caplog
):
    cloudflare.add_zone("za", "a.example", event(T0 + 10, count=4))
    cloudflare.add_zone("zb", "b.example", event(T0 + 10, count=6))
    original = script.evaluate

    def broken(plan, *args, **kwargs):
        if plan.zone_name == "a.example":
            raise RuntimeError("bug")
        return original(plan, *args, **kwargs)

    monkeypatch.setattr(script, "evaluate", broken)
    with caplog.at_level(logging.ERROR):
        run(exporter)
    assert "failed unexpectedly" in caplog.text
    samples = analytics(exporter, storage)
    assert values(samples, "cf_requests_5m", zone_name="b.example") == {T0 + BUCKET: 6}
    monkeypatch.setattr(script, "evaluate", original)
    clock.advance(script.ERROR_RETRY_SECONDS)
    run(exporter)
    samples = analytics(exporter, storage)
    assert values(samples, "cf_requests_5m", zone_name="a.example") == {T0 + BUCKET: 4}


@pytest.mark.parametrize("api", [{"max_retry_after_seconds": 1}], indirect=True)
def test_explicit_rate_limit_maximum_pauses_all_reads(cloudflare, exporter, clock, caplog):
    cloudflare.add_zone("za", "a.example")
    cloudflare.add_zone("zb", "b.example")
    limited = (429, {"Retry-After": "100"}, json.dumps({"errors": [{"message": "slow down"}]}))
    cloudflare.replies.append(limited)
    stop = threading.Event()
    with caplog.at_level(logging.ERROR):
        assert exporter.tick(stop) is False
    assert "pausing reads" in caplog.text
    # Every further request would be rejected too: nothing is tried, for any
    # zone, until the cooldown has passed.
    assert len(cloudflare.calls) == 1
    clock.advance(script.RATE_LIMIT_RETRY_SECONDS - 1)
    run(exporter)
    assert len(cloudflare.calls) == 1
    clock.advance(1)
    run(exporter)
    assert exporter.results["error"] == 1
    assert {c["zone"] for c in cloudflare.calls[1:]} == {"za", "zb"}
    assert exporter.snapshot().success == {"a.example": 1, "b.example": 1}


@pytest.mark.parametrize("api", [{"max_retry_after_seconds": 1}], indirect=True)
def test_rate_limit_pause_covers_settings_requests(cloudflare, exporter, clock, caplog):
    for name in ("a", "b", "c"):
        cloudflare.add_zone(f"z{name}", f"{name}.example")
    limited = (429, {"Retry-After": "100"}, json.dumps({"errors": [{"message": "slow down"}]}))
    cloudflare.settings_replies.append(limited)
    with caplog.at_level(logging.ERROR):
        run(exporter)
    assert "pausing reads" in caplog.text
    # The settings of the other zones would be rejected as well: not asked for.
    assert len(cloudflare.settings_calls) == 1 and cloudflare.calls == []
    clock.advance(script.RATE_LIMIT_RETRY_SECONDS - 1)
    run(exporter)
    assert len(cloudflare.settings_calls) == 1 and cloudflare.zone_calls == 1
    clock.advance(1)
    run(exporter)
    assert sorted(cloudflare.settings_calls[1:]) == ["za", "zb", "zc"]
    assert {call["zone"] for call in cloudflare.calls} == {"za", "zb", "zc"}


@pytest.mark.parametrize("api", [{"max_retry_after_seconds": 1}], indirect=True)
def test_rate_limited_zone_listing_pauses_everything(cloudflare, exporter, clock):
    cloudflare.add_zone("z1", "example.org")
    limited = (429, {"Retry-After": "100"}, json.dumps({"errors": [{"message": "slow down"}]}))
    cloudflare.zones_replies.append(limited)
    run(exporter)
    assert exporter.readiness_error() == "zones have not been listed yet"
    clock.advance(script.RATE_LIMIT_RETRY_SECONDS - 1)
    run(exporter)
    assert cloudflare.zone_calls == 1 and cloudflare.settings_calls == []
    clock.advance(1)
    run(exporter)
    assert cloudflare.zone_calls == 2 and exporter.readiness_error() is None
    assert cloudflare.calls


def test_settings_failure_is_retried_and_leaves_other_zones_alone(
    cloudflare, exporter, storage, clock
):
    bad = cloudflare.add_zone("za", "a.example", event(T0 + 10, count=4))
    bad.settings_error = "not authorized"
    cloudflare.add_zone("zb", "b.example", event(T0 + 10, count=6))
    run(exporter)
    assert exporter.snapshot().success == {"a.example": 0, "b.example": 1}
    assert {c["zone"] for c in cloudflare.calls} == {"zb"}
    bad.settings_error = None
    clock.advance(script.ERROR_RETRY_SECONDS)
    run(exporter)
    assert exporter.snapshot().success == {"a.example": 1, "b.example": 1}
    samples = analytics(exporter, storage)
    assert values(samples, "cf_requests_5m", zone_name="a.example") == {T0 + BUCKET: 4}


@pytest.mark.parametrize(
    "unusable",
    [
        {"enabled": False},
        {"fields": [f for f in PRO_FIELDS if f != "count"]},
        {"max_duration": 120},
    ],
    ids=["not-enabled", "missing-field", "too-short"],
)
def test_zone_whose_plan_cannot_be_read_is_asked_again_hourly(
    cloudflare, exporter, clock, caplog, unusable
):
    """Asking again a minute later changes nothing about a plan, and with many
    such zones it would cost a good part of the shared rate limit."""
    enabled = unusable.pop("enabled", True)
    zone = cloudflare.add_zone("za", "a.example", **unusable)
    zone.enabled = enabled
    cloudflare.add_zone("zb", "b.example")
    with caplog.at_level(logging.ERROR):
        run(exporter)
    assert "a.example: cannot be read" in caplog.text
    assert exporter.snapshot().success == {"a.example": 0, "b.example": 1}
    for _ in range(59):
        clock.advance(60)
        run(exporter)
    assert cloudflare.settings_calls.count("za") == 1
    clock.advance(60)
    run(exporter)
    assert cloudflare.settings_calls.count("za") == 2


def test_settings_are_fetched_once_and_refreshed_hourly(cloudflare, exporter, clock):
    cloudflare.add_zone("z1", "example.org")
    for _ in range(11):
        run(exporter)
        clock.advance(BUCKET)
    assert cloudflare.settings_calls == ["z1"]
    for _ in range(2):
        run(exporter)
        clock.advance(BUCKET)
    assert cloudflare.settings_calls == ["z1", "z1"]


def test_zone_listing_failure_keeps_the_known_zones(cloudflare, exporter, storage, clock, caplog):
    zone = cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4))
    run(exporter)
    assert cloudflare.zone_calls == 1
    cloudflare.zones_error = "Unauthorized to access requested resource"
    zone.add(event(T0 + 310, count=2))
    clock.advance(script.ZONES_REFRESH_SECONDS)
    with caplog.at_level(logging.ERROR):
        run(exporter)
    assert "Failed to list zones" in caplog.text
    samples = analytics(exporter, storage)
    assert values(samples, "cf_requests_5m") == {T0 + BUCKET: 4, T0 + 2 * BUCKET: 2}
    # The listing is tried again after a short delay, not on every loop.
    calls = cloudflare.zone_calls
    run(exporter)
    assert cloudflare.zone_calls == calls
    clock.advance(script.ERROR_RETRY_SECONDS)
    run(exporter)
    assert cloudflare.zone_calls == calls + 1


def test_listing_failure_at_startup_is_not_ready_and_recovers(cloudflare, exporter, clock):
    cloudflare.add_zone("z1", "example.org")
    cloudflare.zones_error = "Invalid API Token"
    run(exporter)
    assert exporter.readiness_error() == "zones have not been listed yet"
    assert cloudflare.calls == []
    cloudflare.zones_error = None
    clock.advance(script.ERROR_RETRY_SECONDS)
    run(exporter)
    assert exporter.readiness_error() is None
    assert cloudflare.calls


def test_new_and_removed_zones_are_followed(cloudflare, exporter, storage, clock):
    cloudflare.add_zone("za", "a.example", event(T0 + 10, count=4))
    run(exporter)
    cloudflare.add_zone("zb", "b.example", event(T0 + 10, count=6))
    del cloudflare.zones["za"]
    clock.advance(script.ZONES_REFRESH_SECONDS)
    before = len(cloudflare.calls)
    run(exporter)
    assert {c["zone"] for c in cloudflare.calls[before:]} == {"zb"}
    # The removed zone is not read, but its plan waits for the keep time.
    assert list(exporter.listed_plans()) == ["zb"] and set(exporter.plans) == {"za", "zb"}
    samples = analytics(exporter, storage)
    # What was accepted for the removed zone is still delivered.
    assert values(samples, "cf_requests_5m", zone_name="a.example") == {T0 + BUCKET: 4}
    assert values(samples, "cf_requests_5m", zone_name="b.example") == {T0 + BUCKET: 6}
    metrics = health(exporter, storage)
    # Health shows the listed zones only.
    for family in ("cf_exporter_pending_buckets", "cf_exporter_zone_success"):
        assert {labels for name, labels in metrics if name == family} == {
            (("zone_name", "b.example"),)
        }
    assert {labels for name, labels in metrics if name == "cf_exporter_dataset_available"} == {
        (("dataset", kind), ("zone_name", "b.example")) for kind in script.DATASETS
    }


def test_zone_missing_from_one_listing_keeps_its_state(cloudflare, exporter, storage, clock):
    """A listing can lack a zone that is not gone. Dropping its state at once would
    make the exporter read and serve its whole history again."""

    def remembered(zone_id):
        return storage._conn.execute(
            "SELECT count(*) FROM buckets WHERE zone_id = ? AND delivered = 1", (zone_id,)
        ).fetchone()[0]

    cloudflare.add_zone("za", "a.example", event(T0 + 10, count=4))
    cloudflare.add_zone("zb", "b.example", event(T0 + 10, count=6))
    run(exporter)
    for _ in range(script.DELIVERY_RESPONSES):
        analytics(exporter, storage, consume=True)
    clock.advance(script.DELIVERY_SECONDS)
    run(exporter)
    # The delivered buckets still within reach are remembered as done.
    delivered = remembered("za")
    assert delivered == 21

    zone = cloudflare.zones.pop("za")
    clock.advance(script.ZONES_REFRESH_SECONDS)
    before = len(cloudflare.calls)
    run(exporter)
    assert {call["zone"] for call in cloudflare.calls[before:]} == {"zb"}
    assert list(exporter.listed_plans()) == ["zb"] and remembered("za") == delivered
    settings = len(cloudflare.settings_calls)
    cloudflare.zones["za"] = zone
    clock.advance(script.ZONES_REFRESH_SECONDS)
    before = len(cloudflare.calls)
    run(exporter)
    again = [call for call in cloudflare.calls[before:] if call["zone"] == "za"]
    # Only the buckets that became due meanwhile are read, with the plan kept.
    assert again and min(call["since"] for call in again) == T0 + 3 * BUCKET
    assert len(cloudflare.settings_calls) == settings

    # A zone that stays away is forgotten after the keep time.
    del cloudflare.zones["za"]
    clock.advance(script.ZONES_REFRESH_SECONDS)
    run(exporter)
    assert remembered("za") > 0 and "za" in exporter.plans
    clock.advance(script.ZONE_KEEP_SECONDS)
    run(exporter)
    assert remembered("za") == 0 and exporter.gone_zones() == []
    assert "za" not in exporter.zone_states


def test_incomplete_first_listing_after_a_restart_keeps_a_zones_state(
    cloudflare, api, clock, tmp_path
):
    """Only the database knows the zones after a restart; a zone missing from the
    first listing must keep its state as well."""
    path = str(tmp_path / "cf-exporter.sqlite3")
    cloudflare.add_zone("za", "a.example", event(T0 + 10, count=4))
    storage = script.Storage(path)
    first = script.Exporter(api, storage, clock=clock)
    run(first)
    stored = storage.summary()["zones"]["za"]
    storage.close()

    zone = cloudflare.zones.pop("za")
    restarted = script.Storage(path)
    second = script.Exporter(api, restarted, clock=clock)
    run(second)
    assert second.gone_zones() == ["za"]
    assert restarted.summary()["zones"]["za"] == stored
    cloudflare.zones["za"] = zone
    clock.advance(script.ZONES_REFRESH_SECONDS)
    before = len(cloudflare.calls)
    run(second)
    # Nothing that was accepted before the restart is read again.
    assert all(call["since"] >= T0 + BUCKET for call in cloudflare.calls[before:])
    restarted.close()


def test_account_without_zones_is_ready_and_idle(cloudflare, exporter, storage):
    run(exporter)
    assert exporter.readiness_error() is None
    assert analytics(exporter, storage) == []


def test_loop_survives_errors_and_waits_only_when_idle(exporter, clock, monkeypatch, caplog):
    outcomes = iter([RuntimeError("bug"), True, False])
    stop = Stop(clock)

    def tick(stop_event):
        outcome = next(outcomes, None)
        if outcome is None:
            stop.stopped = True
            return False
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(exporter, "tick", tick)
    with caplog.at_level(logging.ERROR):
        exporter.run(stop)
    assert "Collection loop failed unexpectedly" in caplog.text
    # After the error, after the idle tick and after the last one; not after
    # the tick that was interrupted by a new bucket.
    assert stop.waits == [script.LOOP_SECONDS] * 3
    assert exporter.heartbeat == clock.now - script.LOOP_SECONDS


def test_stop_ends_the_tick_before_any_read(cloudflare, exporter):
    cloudflare.add_zone("z1", "example.org")
    stop = threading.Event()
    stop.set()
    assert exporter.tick(stop) is False
    assert cloudflare.calls == []


def test_buckets_nobody_scrapes_expire_after_the_keep_time(cloudflare, exporter, storage, clock):
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4))
    run(exporter)
    accepted = clock.now
    assert values(analytics(exporter, storage), "cf_requests_5m") == {T0 + BUCKET: 4}
    # Nothing is dropped within the keep time after its acceptance.
    clock.now = accepted + script.UNDELIVERED_KEEP_SECONDS
    exporter.prune(clock.now)
    assert exporter.results["expired"] == 0
    assert values(analytics(exporter, storage), "cf_requests_5m") == {T0 + BUCKET: 4}
    clock.advance(1)
    exporter.prune(clock.now)
    assert exporter.results["expired"] == 23
    assert analytics(exporter, storage) == []


def test_recheck_window_covers_only_adjacent_buckets(cloudflare, exporter, storage, clock):
    cloudflare.add_zone("z1", "example.org")
    exporter.refresh(clock.now)
    for end in (T0, T0 - BUCKET, T0 - 3 * BUCKET):
        storage.accept("z1", script.BucketResult(end, (), "s"), clock.now, recheck_due=clock.now)
    assert exporter.next_read(clock.now) == ("recheck", "z1", T0 - 2 * BUCKET, T0)


def test_failed_recheck_read_is_tried_again_later(cloudflare, exporter, clock):
    cloudflare.add_zone("z1", "example.org", event(T0 - 10, count=4))
    run(exporter)
    # Only the recheck of the past hour fails; fresh buckets are read as usual.
    cloudflare.failures.append(
        lambda call: "unable to execute task" if call["until"] <= T0 else None
    )
    clock.now = T0 + 3600
    run(exporter)
    assert exporter.results["error"] == 1
    assert exporter.results["recheck_unchanged"] == 0
    cloudflare.failures.clear()
    before = len(cloudflare.calls)
    clock.advance(script.RECHECK_RETRY_SECONDS - 1)
    run(exporter)
    assert not [c for c in cloudflare.calls[before:] if c["until"] <= T0]
    clock.advance(1)
    run(exporter)
    assert exporter.results["recheck_unchanged"] == 10


def test_recheck_whose_reads_keep_failing_is_given_up(cloudflare, exporter, clock):
    cloudflare.add_zone("z1", "example.org", event(T0 - 10, count=4))
    run(exporter)
    failed = []

    def broken(call):
        if "groups" in call["aliases"] and call["until"] <= T0:
            failed.append(clock.now)
            return "unable to execute task"
        return None

    cloudflare.failures.append(broken)
    clock.now = T0 + 3600
    while clock.now < T0 + script.RECHECK_GIVE_UP_SECONDS + 2 * 3600:
        run(exporter)
        clock.advance(script.RECHECK_RETRY_SECONDS)
    # Each of the ten buckets was given up six hours after its end, the last
    # ones with the bucket ending at T0; nothing is tried after that.
    assert exporter.results["recheck_failed"] == 10
    assert exporter.results["recheck_unchanged"] > 0
    assert max(failed) == T0 + script.RECHECK_GIVE_UP_SECONDS
    assert exporter.results["error"] == len(failed)
