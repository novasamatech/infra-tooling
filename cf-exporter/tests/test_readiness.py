# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Readiness and metrics through the real HTTP server, without Cloudflare calls."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from types import SimpleNamespace

import image_smoke
import pytest
import requests
from conftest import DAY, publish, requests_group, samples
from prometheus_client import CollectorRegistry

import script

MIDNIGHT = DAY + timedelta(days=1)
NO_WINDOW = "the last snapshot has no API window"


@pytest.fixture
def endpoint(monkeypatch):
    collector = script.CloudflareCollector()
    registry = CollectorRegistry(auto_describe=True)
    registry.register(collector)
    monkeypatch.setattr(script, "REGISTRY", registry)
    server, thread = script.start_http_server(0, collector, 900)
    try:
        yield collector, f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.fixture
def fake_cycles(rsps, clock, monkeypatch):
    """Real cycles against the fake API, finishing at the exporter's clock."""
    rsps.add_passthru("http://127.0.0.1")
    monkeypatch.setattr(
        script,
        "time",
        SimpleNamespace(time=lambda: clock.value.timestamp(), monotonic=time.monotonic),
    )


def snapshot(clock, **changes):
    values = {
        "cycle_success": 1,
        "cycle_timestamp": clock.value.timestamp(),
        "window_start": clock.value.replace(hour=0, minute=0, second=0, microsecond=0),
        "window_end": clock.value,
        "zone_success": {"example.org": 1},
    }
    return script.Snapshot(**(values | changes))


def ready(url):
    response = requests.get(url + "/readyz", timeout=2)
    return response.status_code, response.text


def test_real_cycles_drive_readiness_without_hiding_metrics(
    endpoint, cloudflare, api, clock, fake_cycles
):
    collector, url = endpoint
    cloudflare.add_zone("a", "good.example", requests=[requests_group(4)])
    cloudflare.add_zone("b", "bad.example", requests=[requests_group(2)])
    assert ready(url) == (503, "no collection cycle has finished\n")
    assert requests.get(url + "/metrics", timeout=2).status_code == 200

    publish(api, clock, clock.value, collector)
    response = requests.get(url + "/readyz", timeout=2)
    assert (response.status_code, response.text) == (200, "ready\n")
    assert response.headers["Cache-Control"] == "no-store"

    cloudflare.zones["b"]["error"] = "unavailable"
    publish(api, clock, clock.value + timedelta(minutes=5), collector)
    assert ready(url) == (503, "1 of 2 zones failed in the last cycle\n")
    metrics = requests.get(url + "/metrics", timeout=2)
    assert metrics.status_code == 200
    assert "cf_exporter_last_cycle_success 0.0" in metrics.text
    assert 'cf_exporter_zone_scrape_success{zone_name="bad.example"} 0.0' in metrics.text
    assert 'cf_zone_requests{zone_name="good.example"} 4.0' in metrics.text

    cloudflare.zones_error = {"message": "unavailable"}
    publish(api, clock, clock.value + timedelta(minutes=5), collector)
    assert ready(url) == (503, "the last collection cycle failed\n")
    assert "cf_exporter_last_cycle_success 0.0" in requests.get(url + "/metrics", timeout=2).text

    cloudflare.zones_error = None
    cloudflare.zones["b"]["error"] = None
    publish(api, clock, clock.value + timedelta(minutes=5), collector)
    assert ready(url) == (200, "ready\n")


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"cycle_timestamp": None}, "no collection cycle has finished"),
        ({"cycle_success": 0}, "the last collection cycle failed"),
        ({"zone_success": {}}, "no zones were discovered"),
        (
            {"zone_success": {"good.example": 1, "bad.example": 0}},
            "1 of 2 zones failed in the last cycle",
        ),
        ({"window_start": None}, NO_WINDOW),
        ({"window_end": None}, NO_WINDOW),
    ],
)
def test_missing_or_partial_snapshot_is_not_ready(endpoint, clock, changes, reason):
    collector, url = endpoint
    collector.update(snapshot(clock, **changes))
    assert ready(url) == (503, reason + "\n")


@pytest.mark.parametrize("field", ["cycle_timestamp", "window_end"])
@pytest.mark.parametrize("age", [-1, 899, 900])
def test_readiness_checks_both_collection_time_and_api_window(endpoint, clock, field, age):
    collector, url = endpoint
    value = clock.value - timedelta(seconds=age)
    if field == "cycle_timestamp":
        value = value.timestamp()
    collector.update(snapshot(clock, **{field: value}))
    status, text = ready(url)
    if age == 899:
        assert (status, text) == (200, "ready\n")
    else:
        assert status == 503 and text.startswith("snapshot is stale or in the future: ")
        assert text.endswith(", limit 900s\n")


def test_midnight_refill_keeps_a_fresh_previous_day_snapshot_ready(
    endpoint, cloudflare, api, clock, fake_cycles
):
    """Readiness ignores the UTC day: after midnight the previous day's families are
    omitted, but a fresh snapshot stays ready while the new day is collected."""
    collector, url = endpoint
    cloudflare.add_zone("z", "example.org", requests=[requests_group(4)])
    closing = MIDNIGHT - timedelta(seconds=30)
    publish(api, clock, closing, collector)

    clock.value = MIDNIGHT + timedelta(minutes=4)
    assert ready(url) == (200, "ready\n")
    metrics = requests.get(url + "/metrics", timeout=2).text
    assert "cf_zone_requests" not in metrics
    assert "cf_exporter_snapshot_window_end_seconds" not in metrics
    assert "cf_exporter_last_cycle_success 1.0" in metrics

    # Freshness still applies across midnight.
    clock.value = closing + timedelta(seconds=900)
    status, text = ready(url)
    assert status == 503 and text.startswith("snapshot is stale or in the future: ")

    publish(api, clock, clock.value, collector)
    assert ready(url) == (200, "ready\n")
    [window] = samples(collector, "cf_exporter_snapshot_window_end_seconds")
    assert window.labels == {"zone_name": "example.org", "window_start": script.rfc3339(MIDNIGHT)}


def test_readiness_stays_ready_across_midnight_with_the_unchanged_schedule(
    cloudflare, api, clock, fake_cycles
):
    """Regression: the midnight refill must not make the exporter unready."""
    collector = script.CloudflareCollector()
    cloudflare.add_zone("z", "example.org", requests=[requests_group(4)])
    clock.value = MIDNIGHT - timedelta(minutes=3)
    observed = []

    class AdvancingStop:
        remaining = 3

        def is_set(self):
            return self.remaining <= 0

        def wait(self, timeout):
            # What a probe sees just before the next cycle starts.
            clock.value += timedelta(seconds=timeout)
            observed.append((clock.value, collector.readiness_error(900)))
            self.remaining -= 1

    script.run(api, collector, 300, AdvancingStop())
    assert [error for _, error in observed] == [None, None, None]
    # The day-close schedule is unchanged: the cycle closing the day starts 30 s
    # before midnight, and the new day starts one regular interval after it.
    assert MIDNIGHT + timedelta(seconds=269) < observed[1][0] <= MIDNIGHT + timedelta(seconds=270)
    assert collector.snapshot.window_start == MIDNIGHT


def test_unsupported_dataset_and_truncation_do_not_hide_a_healthy_snapshot(endpoint, clock):
    collector, url = endpoint
    collector.update(
        snapshot(
            clock,
            available={("example.org", "edge"): 0},
            limited={("example.org", "cache"): 1},
        )
    )
    assert ready(url) == (200, "ready\n")
    metrics = requests.get(url + "/metrics", timeout=2).text
    assert 'cf_exporter_dataset_available{dataset="edge",zone_name="example.org"} 0.0' in metrics
    assert 'cf_exporter_group_limit_reached{dataset="cache",zone_name="example.org"} 1.0' in metrics


def test_readiness_does_not_encode_metrics_or_wait_for_a_large_scrape(endpoint, clock, monkeypatch):
    collector, url = endpoint
    collector.update(snapshot(clock))
    entered, release = threading.Event(), threading.Event()
    original = collector.collect

    def blocked_collect():
        entered.set()
        assert release.wait(timeout=5), "metrics request was not released"
        yield from original()

    monkeypatch.setattr(collector, "collect", blocked_collect)
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(requests.get, url + "/metrics", timeout=10)
        try:
            assert entered.wait(timeout=2)
            assert requests.get(url + "/readyz", timeout=2).status_code == 200
        finally:
            release.set()
        assert pending.result(timeout=2).status_code == 200


def test_prometheus_compression_filtering_and_methods_are_preserved(endpoint, clock):
    collector, url = endpoint
    collector.update(snapshot(clock, zone_requests={"example.org": 4}))
    metrics = requests.get(url + "/metrics", headers={"Accept-Encoding": "gzip"}, timeout=2)
    assert metrics.status_code == 200 and metrics.headers["Content-Encoding"] == "gzip"
    assert 'cf_zone_requests{zone_name="example.org"} 4.0' in metrics.text
    filtered = requests.get(
        url + "/metrics", params={"name[]": "cf_exporter_snapshot_window_end_seconds"}, timeout=2
    )
    assert 'zone_name="example.org"' in filtered.text
    assert "cf_zone_requests" not in filtered.text
    assert requests.options(url + "/metrics", timeout=2).status_code == 200
    assert requests.post(url + "/metrics", timeout=2).status_code == 405


def test_readiness_head_and_rejected_methods(endpoint, clock):
    collector, url = endpoint
    response = requests.head(url + "/readyz", timeout=2)
    assert response.status_code == 503 and not response.content
    collector.update(snapshot(clock))
    response = requests.head(url + "/readyz", timeout=2)
    assert response.status_code == 200 and not response.content
    response = requests.post(url + "/readyz", timeout=2)
    assert response.status_code == 405 and response.headers["Allow"] == "GET, HEAD"


@pytest.mark.parametrize(
    ("after_midnight", "changes", "status"),
    [
        (False, {}, 200),
        (False, {"zone_success": {}}, 503),
        (True, {}, 200),
    ],
)
def test_smoke_readiness_expectation_matches_the_endpoint(
    endpoint, clock, after_midnight, changes, status
):
    """The image and live checks predict /readyz from a successful cycle's exposition."""
    collector, url = endpoint
    clock.value = MIDNIGHT - timedelta(seconds=10)
    collector.update(snapshot(clock, **changes))
    if after_midnight:
        clock.value = MIDNIGHT + timedelta(seconds=5)
    body = requests.get(url + "/metrics", timeout=2).text
    port = int(url.rsplit(":", 1)[1])
    assert image_smoke.expected_readiness(body) == image_smoke.readiness_status(port) == status
