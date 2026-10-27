# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Readiness and metrics through the real HTTP server, without Cloudflare calls."""

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
import requests
from prometheus_client import CollectorRegistry

import script


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


def snapshot(clock, **changes):
    values = {
        "cycle_success": 1,
        "cycle_timestamp": clock.value.timestamp(),
        "window_start": clock.value.replace(hour=0, minute=0, second=0, microsecond=0),
        "window_end": clock.value,
        "zone_success": {"example.org": 1},
    }
    return script.Snapshot(**(values | changes))


def test_startup_failure_and_recovery_without_hiding_metrics(endpoint, clock):
    collector, url = endpoint
    assert requests.get(url + "/readyz", timeout=2).status_code == 503
    assert requests.get(url + "/metrics", timeout=2).status_code == 200

    collector.update(snapshot(clock))
    ready = requests.get(url + "/readyz", timeout=2)
    assert ready.status_code == 200 and ready.text == "ready\n"
    assert ready.headers["Cache-Control"] == "no-store"

    collector.update(snapshot(clock, cycle_success=0, zone_success={"example.org": 0}))
    assert requests.get(url + "/readyz", timeout=2).status_code == 503
    metrics = requests.get(url + "/metrics", timeout=2)
    assert metrics.status_code == 200
    assert "cf_exporter_last_cycle_success 0.0" in metrics.text
    assert 'cf_exporter_zone_scrape_success{zone_name="example.org"} 0.0' in metrics.text

    collector.update(snapshot(clock))
    assert requests.get(url + "/readyz", timeout=2).status_code == 200


@pytest.mark.parametrize(
    "changes",
    [
        {"zone_success": {}},
        {"zone_success": {"good.example": 1, "bad.example": 0}},
        {"window_start": None},
        {"window_end": None},
        {"cycle_timestamp": None},
    ],
)
def test_missing_or_partial_snapshot_is_not_ready(endpoint, clock, changes):
    collector, url = endpoint
    collector.update(snapshot(clock, **changes))
    assert requests.get(url + "/readyz", timeout=2).status_code == 503


@pytest.mark.parametrize("field", ["cycle_timestamp", "window_end"])
@pytest.mark.parametrize("age", [-1, 899, 900])
def test_readiness_checks_both_collection_time_and_api_window(endpoint, clock, field, age):
    collector, url = endpoint
    value = clock.value - timedelta(seconds=age)
    if field == "cycle_timestamp":
        value = value.timestamp()
    collector.update(snapshot(clock, **{field: value}))
    expected = 200 if age == 899 else 503
    assert requests.get(url + "/readyz", timeout=2).status_code == expected


def test_midnight_expires_a_fresh_previous_day_and_accepts_an_empty_new_day(endpoint, clock):
    collector, url = endpoint
    midnight = clock.value.replace(hour=0, minute=0, second=0) + timedelta(days=1)
    clock.value = midnight - timedelta(seconds=10)
    collector.update(snapshot(clock))
    assert requests.get(url + "/readyz", timeout=2).status_code == 200

    clock.value = midnight
    assert requests.get(url + "/readyz", timeout=2).status_code == 503
    collector.update(snapshot(clock))
    assert not collector.snapshot.cache and not collector.snapshot.edge
    assert requests.get(url + "/readyz", timeout=2).status_code == 200


def test_unsupported_dataset_and_truncation_do_not_hide_a_healthy_snapshot(endpoint, clock):
    collector, url = endpoint
    collector.update(
        snapshot(
            clock,
            available={("example.org", "edge"): 0},
            limited={("example.org", "cache"): 1},
        )
    )
    assert requests.get(url + "/readyz", timeout=2).status_code == 200
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
