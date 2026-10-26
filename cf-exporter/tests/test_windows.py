# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""The snapshot contract, checked through the real Prometheus exposition."""

from datetime import timedelta

import pytest
from conftest import DAY, exposition, json_response, publish, requests_group, samples
from image_smoke import checked_snapshot_windows
from profile_live import HistoricalCollector
from prometheus_client.core import GaugeMetricFamily

import script

START = DAY
WINDOW = "cf_exporter_snapshot_window_end_seconds"


def cache_group(count, status="hit", origin=200):
    return requests_group(
        count,
        clientRequestHTTPHost="example.org",
        clientRequestHTTPMethodName="GET",
        clientRequestPath="/a",
        cacheStatus=status,
        originResponseStatus=origin,
    )


def test_window_describes_api_bounds_not_finish_time(cloudflare, api, clock, monkeypatch):
    cloudflare.add_zone("z", "example.org", cache=[cache_group(3)])
    moment = clock.value + timedelta(microseconds=123456)
    finished = moment.timestamp() + 45
    monkeypatch.setattr(script.time, "time", lambda: finished)
    collector = publish(api, clock, moment)
    (window,) = samples(collector, WINDOW)
    assert window.labels == {"zone_name": "example.org", "window_start": script.rfc3339(START)}
    assert window.value == moment.replace(microsecond=0).timestamp()
    assert collector.snapshot.cycle_timestamp == finished > window.value
    assert {call["variables"]["until"] for call in cloudflare.data_calls()} == {
        script.rfc3339(moment)
    }
    (cache,) = samples(collector, "cf_cache_requests")
    assert cache.value == 3
    assert cache.labels == {
        "zone_name": "example.org",
        "host_name": "example.org",
        "method_name": "GET",
        "path": "/a",
        "cache_status": "hit",
        "origin_response_status": "200",
    }


def test_failed_zone_has_no_window_and_recovery_gets_new_bounds(cloudflare, api, clock):
    cloudflare.add_zone("good", "good.example", cache=[cache_group(7)])
    cloudflare.add_zone("bad", "bad.example", cache=[cache_group(3)])
    collector = publish(api, clock, clock.value)
    assert len(samples(collector, WINDOW)) == 2
    cloudflare.zones["bad"]["error"] = "unavailable"
    publish(api, clock, clock.value + timedelta(minutes=5), collector)
    assert [sample.labels["zone_name"] for sample in samples(collector, WINDOW)] == ["good.example"]
    (cache,) = samples(collector, "cf_cache_requests")
    assert cache.labels["zone_name"] == "good.example"
    assert cache.value == 7
    health = {
        sample.labels["zone_name"]: sample.value
        for sample in samples(collector, "cf_exporter_zone_scrape_success")
    }
    assert health == {"good.example": 1, "bad.example": 0}
    cloudflare.zones["bad"]["error"] = None
    recovered = clock.value + timedelta(minutes=25)
    publish(api, clock, recovered, collector)
    assert {sample.labels["zone_name"] for sample in samples(collector, WINDOW)} == {
        "good.example",
        "bad.example",
    }
    assert all(sample.value == recovered.timestamp() for sample in samples(collector, WINDOW))
    assert {
        sample.labels["zone_name"]: sample.value
        for sample in samples(collector, "cf_cache_requests")
    } == {"good.example": 7, "bad.example": 3}


@pytest.mark.parametrize("before,after", [(100, 2), (3, 5), (3, 3)])
def test_midnight_changes_window_even_when_value_does_not_fall(
    cloudflare, api, clock, before, after
):
    cloudflare.add_zone("z", "example.org", cache=[cache_group(before)])
    midnight = START + timedelta(days=1)
    collector = publish(api, clock, midnight - timedelta(seconds=30))
    (old_window,) = samples(collector, WINDOW)
    assert samples(collector, "cf_cache_requests")[0].value == before
    # There is no scrape of zero at midnight.
    cloudflare.zones["z"]["cache"] = [cache_group(after)]
    publish(api, clock, midnight + timedelta(minutes=2), collector)
    (new_window,) = samples(collector, WINDOW)
    assert new_window.labels["window_start"] != old_window.labels["window_start"]
    assert new_window.labels["window_start"] == script.rfc3339(midnight)
    assert samples(collector, "cf_cache_requests")[0].value == after


def test_window_is_present_for_empty_zone_and_new_group_keeps_first_value(cloudflare, api, clock):
    cloudflare.add_zone("z", "example.org")
    collector = publish(api, clock, clock.value)
    (old_window,) = samples(collector, WINDOW)
    assert not samples(collector, "cf_cache_requests")
    cloudflare.zones["z"]["cache"] = [cache_group(3, status="none", origin=0)]
    later = clock.value + timedelta(minutes=5)
    publish(api, clock, later, collector)
    (window,) = samples(collector, WINDOW)
    assert window.labels == old_window.labels
    assert window.value > old_window.value
    (cache,) = samples(collector, "cf_cache_requests")
    assert cache.value == 3
    assert cache.labels["cache_status"] == "none"
    assert cache.labels["origin_response_status"] == "0"


def test_independent_instances_export_overlapping_windows_without_process_counts(
    cloudflare, api, clock
):
    cloudflare.add_zone("z", "example.org", cache=[cache_group(100)])
    first = publish(api, clock, clock.value)
    second = publish(api, clock, clock.value + timedelta(minutes=5))
    assert samples(first, WINDOW)[0].labels == samples(second, WINDOW)[0].labels
    assert samples(first, WINDOW)[0].value < samples(second, WINDOW)[0].value
    assert samples(first, "cf_cache_requests")[0].value == 100
    assert samples(second, "cf_cache_requests")[0].value == 100


def test_downward_revisions_and_late_data_are_preserved(cloudflare, api, clock):
    cloudflare.add_zone("z", "example.org", cache=[cache_group(100)])
    collector = publish(api, clock, clock.value)
    identity = samples(collector, WINDOW)[0].labels
    for count in (95, 98, 0):
        cloudflare.zones["z"]["cache"] = [cache_group(count)] if count else []
        publish(api, clock, clock.value + timedelta(minutes=5), collector)
        assert samples(collector, WINDOW)[0].labels == identity
        values = samples(collector, "cf_cache_requests")
        assert [sample.value for sample in values] == ([count] if count else [])


@pytest.mark.parametrize("failure", [False, True])
def test_metadata_and_analytics_share_an_atomic_snapshot(cloudflare, api, clock, failure):
    cloudflare.add_zone("z", "example.org", cache=[cache_group(3)])
    collector = publish(api, clock, clock.value)
    old_end = collector.snapshot.window_end
    families = collector.collect()
    window = next(family for family in families if family.name == WINDOW)
    cloudflare.zones["z"]["cache"] = [cache_group(5)]
    if failure:
        cloudflare.zones["z"]["error"] = "unavailable"
    publish(api, clock, clock.value + timedelta(minutes=5), collector)
    assert window.samples[0].value == old_end.timestamp()
    rest = {family.name: family for family in families}
    cache = rest["cf_cache_requests"]
    assert cache.samples[0].value == 3
    assert rest["cf_exporter_zone_scrape_success"].samples[0].value == 1
    assert [sample.value for sample in samples(collector, "cf_cache_requests")] == (
        [] if failure else [5]
    )
    assert bool(samples(collector, WINDOW)) is not failure


def test_truncated_success_still_describes_its_window(cloudflare, api, clock):
    other = cache_group(2)
    other["dimensions"]["clientRequestPath"] = "/b"
    cloudflare.add_zone("z", "example.org", cache=[cache_group(3), other])
    api.group_limit = 1
    collector = publish(api, clock, clock.value)
    assert samples(collector, WINDOW)[0].value == clock.value.timestamp()
    assert samples(collector, "cf_cache_requests")[0].value == 3
    limited = [
        sample
        for sample in samples(collector, "cf_exporter_group_limit_reached")
        if sample.labels["dataset"] == "cache"
    ]
    assert len(limited) == 1 and limited[0].value == 1
    assert samples(collector, "cf_exporter_zone_scrape_success")[0].value == 1


def test_empty_midnight_and_multiple_days_have_distinct_windows(cloudflare, api, clock):
    cloudflare.add_zone("z", "example.org")
    collector = None
    for day in range(3):
        moment = START + timedelta(days=day)
        collector = publish(api, clock, moment, collector)
        (window,) = samples(collector, WINDOW)
        assert window.labels["window_start"] == script.rfc3339(moment)
        assert window.value == moment.timestamp()
        assert not samples(collector, "cf_cache_requests")
    assert not cloudflare.data_calls()


def test_old_instance_cannot_expose_a_new_instances_window(cloudflare, api, clock):
    cloudflare.add_zone("z", "example.org", cache=[cache_group(3)])
    midnight = START + timedelta(days=1)
    old = publish(api, clock, midnight - timedelta(seconds=30))
    cloudflare.zones["z"]["cache"] = [cache_group(5)]
    new = publish(api, clock, midnight + timedelta(minutes=1))
    assert not samples(old, WINDOW)
    assert not samples(old, "cf_cache_requests")
    assert samples(old, "cf_exporter_zone_scrape_success")[0].value == 1
    assert samples(new, WINDOW)[0].labels["window_start"] == script.rfc3339(midnight)
    assert samples(new, "cf_cache_requests")[0].value == 5


def test_window_is_removed_after_zone_discovery_fails(cloudflare, api, clock):
    cloudflare.add_zone("z", "example.org", cache=[cache_group(3)])
    collector = publish(api, clock, clock.value)
    assert samples(collector, WINDOW)
    cloudflare.zones_error = {"message": "unavailable"}
    publish(api, clock, clock.value + timedelta(minutes=5), collector)
    assert not samples(collector, WINDOW)
    assert not samples(collector, "cf_cache_requests")
    assert not samples(collector, "cf_exporter_zone_scrape_success")
    assert samples(collector, "cf_exporter_last_cycle_success")[0].value == 0


def test_cycle_that_finishes_after_midnight_keeps_its_original_window(cloudflare, api, clock):
    midnight = START + timedelta(days=1)

    def finish_after_midnight(body):
        clock.value = midnight + timedelta(seconds=10)
        node = {
            "visits": [],
            "requests": [],
            "cache": [cache_group(3)],
            "edge": [],
            "totals": [{"count": 3, "sum": {"visits": 0}}],
        }
        return json_response(200, {"data": {"viewer": {"zones": [node]}}})

    cloudflare.add_zone("z", "example.org", responder=finish_after_midnight)
    collector = publish(api, clock, midnight - timedelta(seconds=10))
    assert collector.snapshot.window_start == START
    assert collector.snapshot.window_end == midnight - timedelta(seconds=10)
    assert collector.snapshot.cache
    assert not samples(collector, WINDOW)
    assert not samples(collector, "cf_cache_requests")
    assert samples(collector, "cf_exporter_zone_scrape_success")[0].value == 1


def test_scrape_that_spans_midnight_never_mixes_windows(cloudflare, api, clock):
    cloudflare.add_zone("z", "example.org", cache=[cache_group(3)])
    collector = publish(api, clock, START + timedelta(days=1, seconds=-10))
    families = collector.collect()
    window = next(family for family in families if family.name == WINDOW)
    clock.value = START + timedelta(days=1, seconds=10)
    cache = next(family for family in families if family.name == "cf_cache_requests")
    assert window.samples[0].labels["window_start"] == script.rfc3339(START)
    assert cache.samples[0].value == 3
    assert not samples(collector, WINDOW)
    assert not samples(collector, "cf_cache_requests")


def test_historical_profiling_preserves_the_real_window(cloudflare, api, clock, monkeypatch):
    cloudflare.add_zone("z", "example.org", cache=[cache_group(3)])
    until = START + timedelta(days=1)
    monkeypatch.setattr(script, "day_window", lambda moment: (START, until))
    clock.value = until + timedelta(days=1)
    snapshot = script.collect_once(api)
    historical = HistoricalCollector()
    historical.update(snapshot)
    (window,) = samples(historical, WINDOW)
    assert window.labels["window_start"] == script.rfc3339(START)
    assert window.value == until.timestamp()
    assert samples(historical, "cf_cache_requests")[0].value == 3
    assert samples(historical, "cf_exporter_last_cycle_success")[0].value == 1
    assert checked_snapshot_windows(exposition(historical)) == {
        "example.org": (script.rfc3339(START), until.timestamp())
    }
    production = script.CloudflareCollector()
    production.update(snapshot)
    assert not samples(production, WINDOW)
    assert not samples(production, "cf_cache_requests")
    assert samples(production, "cf_exporter_last_cycle_success")[0].value == 1


def test_historical_profiling_inherits_the_production_collection_path(
    cloudflare, api, clock, monkeypatch
):
    original = script.CloudflareCollector.collect

    def with_extra_family(self):
        yield from original(self)
        yield GaugeMetricFamily("cf_test_collection_path", "Test additional family", value=1)

    monkeypatch.setattr(script.CloudflareCollector, "collect", with_extra_family)
    cloudflare.add_zone("z", "example.org", cache=[cache_group(3)])
    historical = publish(api, clock, clock.value, HistoricalCollector())
    clock.value += timedelta(days=2)
    assert samples(historical, "cf_test_collection_path")[0].value == 1
    assert samples(historical, "cf_cache_requests")[0].value == 3
    assert samples(historical, "cf_exporter_last_cycle_success")[0].value == 1


@pytest.mark.parametrize(
    "defect",
    [
        "missing",
        "duplicate",
        "unlabelled",
        "zone",
        "start",
        "mixed",
        "end",
        "future",
        "fractional",
        "nan",
    ],
)
def test_smoke_validation_checks_window_values_not_only_type(cloudflare, api, clock, defect):
    cloudflare.add_zone("z", "example.org")
    cloudflare.add_zone("y", "example.net")
    collector = publish(api, clock, clock.value)
    body = exposition(collector)
    window = (script.rfc3339(START), clock.value.timestamp())
    assert checked_snapshot_windows(body) == {"example.org": window, "example.net": window}
    line = next(
        line
        for line in body.splitlines()
        if line.startswith(WINDOW + "{") and 'zone_name="example.org"' in line
    )
    if defect == "missing":
        body = body.replace(line, "")
    elif defect == "duplicate":
        body += line + "\n"
    elif defect == "unlabelled":
        body += f"{WINDOW} {clock.value.timestamp()}\n"
    elif defect == "zone":
        body = body.replace(line, line.replace("example.org", "other.example"))
    elif defect == "start":
        body = body.replace(line, line.replace("T00:00:00Z", "T01:00:00Z"))
    else:
        end = {
            # Within its UTC day, but not the other zone's window.
            "mixed": clock.value.timestamp() - 60,
            "end": START.timestamp() - 1,
            "future": START.timestamp() + 86401,
            "fractional": clock.value.timestamp() + 0.5,
            "nan": float("nan"),
        }[defect]
        body = body.replace(line, line.rsplit(" ", 1)[0] + f" {end}")
    with pytest.raises(ValueError):
        checked_snapshot_windows(body)
