# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""The HTTP endpoints: /analytics exposition and delivery, /readyz and health metrics."""

import gzip
import threading
import urllib.error
import urllib.request

import pytest
from conftest import BUCKET, T0, analytics, event, health, parse, request, run, values
from prometheus_client import CollectorRegistry

import script

ZONE = {"zone_name": "example.org"}
ANALYTICS = set(script.METRIC_LABELS)


@pytest.fixture
def app(exporter, storage):
    registry = CollectorRegistry()
    registry.register(script.HealthCollector(exporter, storage))
    registry.register(exporter.api.stats)
    return script.make_app(exporter, storage, registry)


def consume(app):
    status, _, body = request(app, "/analytics", query="consume=1")
    assert status == 200
    return parse(body)


def test_analytics_exposition_format(cloudflare, exporter, app):
    cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 10, count=4, visits=1, clientRequestPath="/app"),
        event(T0 - 10, count=2, clientRequestPath="/app"),
    )
    run(exporter)
    status, headers, body = request(app, "/analytics")
    assert status == 200
    assert headers["Content-Type"] == "text/plain; version=0.0.4; charset=utf-8"
    lines = body.splitlines()
    # One HELP and TYPE per family, every family a gauge.
    families = [line.split()[2] for line in lines if line.startswith("# TYPE")]
    assert sorted(families) == sorted(ANALYTICS) and len(set(families)) == len(families)
    assert all(line.endswith(" gauge") for line in lines if line.startswith("# TYPE"))
    assert sum(line.startswith("# HELP") for line in lines) == len(families)
    # Catalog label order, integer values and the bucket end in milliseconds;
    # the samples of a series follow each other in time order.
    series = (
        'cf_requests_5m{zone_name="example.org",host_name="example.org",method_name="GET",'
        'path="/app",query="",client_country_name="DE",client_request_referer="direct",'
        'user_agent_browser="Chrome",user_agent_os="Linux",cache_status="hit",'
        'origin_response_status="200"}'
    )
    assert [line for line in lines if line.startswith("cf_requests_5m")] == [
        f"{series} 2 {T0 * 1000}",
        f"{series} 4 {(T0 + BUCKET) * 1000}",
    ]
    visits = (
        'cf_visits_5m{zone_name="example.org",host_name="example.org",path="/app",'
        'client_country_name="DE",client_request_referer="direct",user_agent_browser="Chrome",'
        'user_agent_os="Linux"}'
    )
    assert f"{visits} 1 {(T0 + BUCKET) * 1000}" in lines
    edge = (
        'cf_edge_requests_5m{zone_name="example.org",host_name="example.org",method_name="GET",'
        'path="/app",edge_response_status="200",origin_response_status="200"}'
    )
    assert f"{edge} 4 {(T0 + BUCKET) * 1000}" in lines
    cache = (
        'cf_cache_requests_5m{zone_name="example.org",host_name="example.org",method_name="GET",'
        'path="/app",cache_status="hit",origin_response_status="200"}'
    )
    assert f"{cache} 4 {(T0 + BUCKET) * 1000}" in lines
    assert f'cf_zone_requests_5m{{zone_name="example.org"}} 0 {(T0 - BUCKET) * 1000}' in lines
    # Every sample line carries a timestamp on a bucket boundary.
    for name, _, _, timestamp in parse(body):
        assert name in ANALYTICS and timestamp % BUCKET == 0


def test_analytics_is_compressed_on_request(cloudflare, exporter, app):
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4))
    run(exporter)
    plain = request(app, "/analytics")[2]
    environ_headers = {"Accept-Encoding": "gzip, deflate"}
    status, headers, body = request(app, "/analytics", headers=environ_headers)
    assert status == 200
    assert headers["Content-Encoding"] == "gzip" and body == plain
    # The helper decompressed it; the length header is that of the compressed body.
    assert int(headers["Content-Length"]) < len(plain.encode())
    assert int(headers["Content-Length"]) == len(gzip.compress(plain.encode(), compresslevel=6))


def test_only_consuming_responses_deliver(cloudflare, exporter, storage, clock, app):
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4))
    run(exporter)
    for _ in range(5):
        assert values(parse(request(app, "/analytics")[2]), "cf_requests_5m") == {T0 + BUCKET: 4}
    # HEAD carries no body, so it delivers nothing either.
    status, headers, body = request(app, "/analytics", method="HEAD", query="consume=1")
    assert (status, body) == (200, "") and int(headers["Content-Length"]) > 0
    clock.advance(script.DELIVERY_SECONDS)
    exporter.prune(clock.now)
    assert storage.undelivered_samples() > 0
    assert exporter.analytics_responses == 0

    assert values(consume(app), "cf_requests_5m") == {T0 + BUCKET: 4}
    exporter.prune(clock.now)
    assert storage.undelivered_samples() > 0  # one response is not enough
    consume(app)
    exporter.prune(clock.now)
    assert storage.undelivered_samples() == 0
    assert request(app, "/analytics", query="consume=1")[2] == ""
    assert exporter.analytics_responses == 3


def test_bucket_stays_for_the_delivery_time_even_if_scraped_often(
    cloudflare, exporter, storage, clock, app
):
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4))
    run(exporter)
    for _ in range(10):
        consume(app)
    clock.advance(script.DELIVERY_SECONDS - 1)
    exporter.prune(clock.now)
    assert values(consume(app), "cf_requests_5m") == {T0 + BUCKET: 4}
    clock.advance(1)
    exporter.prune(clock.now)
    assert consume(app) == []


def test_response_that_was_not_sent_completely_does_not_deliver(
    cloudflare, exporter, storage, clock
):
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4))
    run(exporter)
    app = script.make_app(exporter, storage, CollectorRegistry())
    for _ in range(3):
        environ = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": "/analytics",
            "QUERY_STRING": "consume=1",
        }
        body = app(environ, lambda status, headers: None)
        assert next(iter(body))  # the server took the body and then lost the client
        body.close()
    clock.advance(script.DELIVERY_SECONDS)
    exporter.prune(clock.now)
    assert exporter.analytics_responses == 0
    assert values(analytics(exporter, storage), "cf_requests_5m") == {T0 + BUCKET: 4}


def test_bucket_accepted_between_scrapes_needs_its_own_responses(
    cloudflare, exporter, storage, clock, app
):
    zone = cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4))
    run(exporter)
    consume(app)
    zone.add(event(T0 + 310, count=6))
    clock.advance(BUCKET)
    run(exporter)
    assert values(consume(app), "cf_requests_5m") == {T0 + BUCKET: 4, T0 + 2 * BUCKET: 6}
    clock.advance(script.DELIVERY_SECONDS)
    exporter.prune(clock.now)
    # The older bucket had two responses, the newer one only one.
    assert values(consume(app), "cf_requests_5m") == {T0 + 2 * BUCKET: 6}
    exporter.prune(clock.now)
    assert consume(app) == []


def test_response_budget_serves_the_newest_buckets_first(
    cloudflare, exporter, storage, clock, app, monkeypatch
):
    cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 10, count=1),
        event(T0 - 290, count=2),
        event(T0 - 590, count=3),
    )
    run(exporter)
    # A bucket with requests has five samples here: three families and two totals.
    monkeypatch.setattr(script, "RESPONSE_SAMPLE_BUDGET", 13)
    served = []
    for _ in range(20):
        buckets = sorted({int(timestamp) for _, _, _, timestamp in consume(app)})
        if not buckets:
            break
        served.append(buckets)
        consume(app)
        clock.advance(script.DELIVERY_SECONDS)
        exporter.prune(clock.now)
    # Whole buckets only, the newest first, older ones in later responses.
    assert served[0] == [T0, T0 + BUCKET]
    assert served[1][-1] == T0 - BUCKET
    assert sorted(b for response in served for b in response) == sorted(
        range(T0 - 21 * BUCKET, T0 + 2 * BUCKET, BUCKET)
    )
    monkeypatch.setattr(script, "RESPONSE_SAMPLE_BUDGET", 1)
    zone_events = [event(T0 + 910, count=9)]
    cloudflare.zones["z1"].add(*zone_events)
    run(exporter)
    # A budget below one bucket still serves one bucket at a time.
    assert {int(t) for _, _, _, t in consume(app)} == {clock.now // BUCKET * BUCKET - BUCKET}


def test_buckets_waiting_for_the_delivery_time_do_not_block_older_ones(
    cloudflare, exporter, storage, clock, app, monkeypatch
):
    """The newest buckets stay on the endpoint for the delivery time. When they
    fill the budget, the older ones must still get their responses meanwhile."""
    cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 10, count=1),
        event(T0 - 290, count=2),
        event(T0 - 590, count=3),
    )
    run(exporter)
    monkeypatch.setattr(script, "RESPONSE_SAMPLE_BUDGET", 13)
    responses = [sorted({int(timestamp) for _, _, _, timestamp in consume(app)}) for _ in range(12)]
    # No time passes: nothing is delivered yet, and the two newest buckets
    # alone fill a response.
    assert responses[0] == responses[1] == [T0, T0 + BUCKET]
    assert responses[2][-1] == T0 - BUCKET
    carried = [bucket for response in responses for bucket in response]
    for bucket in range(T0 - 21 * BUCKET, T0 + 2 * BUCKET, BUCKET):
        assert carried.count(bucket) >= script.DELIVERY_RESPONSES, bucket
    exporter.prune(clock.now)
    assert storage.summary()["undelivered_buckets"] == 23
    clock.advance(script.DELIVERY_SECONDS)
    exporter.prune(clock.now)
    assert storage.summary()["undelivered_buckets"] == 0


def test_readiness_needs_zones_and_storage_but_no_fresh_data(
    cloudflare, exporter, storage, clock, app, monkeypatch
):
    cloudflare.add_zone("z1", "example.org")
    status, headers, body = request(app, "/readyz")
    assert (status, body) == (503, "zones have not been listed yet\n")
    assert headers["Cache-Control"] == "no-store"
    run(exporter)
    assert request(app, "/readyz")[::2] == (200, "ready\n")
    assert request(app, "/readyz", method="HEAD")[::2] == (200, "")
    # Neither old data nor a failing Cloudflare makes the process unready.
    cloudflare.zones["z1"].error = "unable to execute task"
    cloudflare.zones_error = "Unauthorized"
    clock.advance(3 * 86400)
    run(exporter)
    assert request(app, "/readyz")[0] == 200
    monkeypatch.setattr(storage, "usable", lambda: False)
    assert request(app, "/readyz")[::2] == (503, "storage is not usable\n")


@pytest.mark.parametrize("path", ["/analytics", "/readyz"])
def test_other_methods_are_rejected(app, path):
    status, headers, body = request(app, path, method="POST")
    assert status == 405 and headers["Allow"] == "GET, HEAD" and body == "use GET or HEAD\n"


@pytest.mark.parametrize("path", ["/metrics", "/", "/anything"])
def test_health_metrics_are_served_on_every_other_path(cloudflare, exporter, clock, app, path):
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4))
    cloudflare.add_zone("z2", "free.example", fields=["count"])
    run(exporter)
    status, _, body = request(app, path)
    assert status == 200
    samples = {
        (name, tuple(sorted(labels.items()))): value for name, labels, value, _ in parse(body)
    }
    zone = (("zone_name", "example.org"),)
    assert samples[("cf_exporter_zone_success", zone)] == 1
    assert samples[("cf_exporter_zone_success", (("zone_name", "free.example"),))] == 0
    assert samples[("cf_exporter_pending_buckets", zone)] == 0
    assert samples[("cf_exporter_newest_bucket_end_seconds", zone)] == T0 + BUCKET
    assert samples[("cf_exporter_oldest_bucket_end_seconds", zone)] == T0 - 21 * BUCKET
    assert samples[("cf_exporter_undelivered_buckets", ())] == 23
    # Two zone totals per bucket, plus three families for the bucket with requests.
    assert samples[("cf_exporter_undelivered_samples", ())] == 23 * 2 + 3
    assert samples[("cf_exporter_storage_persistent", ())] == 0
    assert samples[("cf_exporter_storage_resets_total", ())] == 0
    assert samples[("cf_exporter_analytics_responses_total", ())] == 0
    assert samples[("cf_exporter_analytics_samples_total", ())] == 0
    assert samples[("cf_exporter_loop_timestamp_seconds", ())] == clock.now
    for dataset in script.DATASETS:
        labels = (("dataset", dataset), ("zone_name", "example.org"))
        assert samples[("cf_exporter_dataset_available", labels)] == 1
    reads = {
        dict(labels)["result"]: value
        for (name, labels), value in samples.items()
        if name == "cf_exporter_bucket_reads_total"
    }
    assert set(reads) == set(script.READ_RESULTS)
    assert reads["accepted"] == 23 and sum(reads.values()) == 23
    assert samples[("cf_exporter_api_responses_total", (("api", "graphql"), ("code", "200")))] == 4
    # Analytics never appear among the health metrics, and nothing has a timestamp.
    assert all(name not in ANALYTICS and timestamp is None for name, _, _, timestamp in parse(body))


def test_health_before_the_first_loop(exporter, storage):
    metrics = health(exporter, storage)
    assert ("cf_exporter_loop_timestamp_seconds", ()) not in metrics
    assert metrics[("cf_exporter_undelivered_samples", ())] == 0
    assert metrics[("cf_exporter_bucket_reads_total", (("result", "accepted"),))] == 0


def test_ready_process_already_reports_its_loop(cloudflare, exporter, storage, clock, monkeypatch):
    """Readiness follows the zone listing, and the settings of many zones take a
    while after it: whoever sees a ready process must find its loop timestamp."""
    cloudflare.add_zone("z1", "example.org")
    seen = []
    fetch_settings = script.fetch_settings

    def watching(api, zone_id):
        assert exporter.readiness_error() is None
        seen.append(health(exporter, storage).get(("cf_exporter_loop_timestamp_seconds", ())))
        return fetch_settings(api, zone_id)

    monkeypatch.setattr(script, "fetch_settings", watching)
    run(exporter)
    assert seen == [clock.now]


def test_delivery_counters(cloudflare, exporter, storage, app):
    cloudflare.add_zone("z1", "example.org", event(T0 + 10, count=4))
    run(exporter)
    consume(app)
    consume(app)
    metrics = health(exporter, storage)
    assert metrics[("cf_exporter_analytics_responses_total", ())] == 2
    assert metrics[("cf_exporter_analytics_samples_total", ())] == 2 * (23 * 2 + 3)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (4.0, "4"),
        (0.0, "0"),
        (2.5, "2.5"),
        (1e15, "1000000000000000"),
        (0.1 + 0.2, "0.30000000000000004"),
    ],
)
def test_values_are_rendered_exactly(value, expected):
    assert script.format_value(value) == expected


@pytest.mark.parametrize(
    "value", ['say "hi"', "back\\slash", "line\nbreak", "tab\there", "ünï©ode", ""]
)
def test_label_values_survive_the_exposition(value):
    labels = script.render_labels(("zone_name", "path"), ("example.org", value))
    text = script.render_analytics([("cf_requests_5m", labels, 3.0, T0)]).decode()
    assert parse(text) == [("cf_requests_5m", {"zone_name": "example.org", "path": value}, 3.0, T0)]
    assert script.render_labels((), ()) == ""


def test_server_answers_concurrent_scrapes_probes_and_health(cloudflare, exporter, storage):
    cloudflare.add_zone(
        "z1",
        "example.org",
        *(event(T0 + 10, count=1, clientRequestPath=f"/{index}") for index in range(300)),
    )
    run(exporter)
    registry = CollectorRegistry()
    registry.register(script.HealthCollector(exporter, storage))
    server, thread = script.start_http_server(0, exporter, storage, registry)
    port = server.server_port
    results, errors = [], []

    def get(path):
        try:
            url = f"http://127.0.0.1:{port}{path}"
            with urllib.request.urlopen(url, timeout=10) as response:
                results.append((path, response.status, len(response.read())))
        except Exception as e:  # collected for the assertion below
            errors.append((path, e))

    paths = ["/analytics?consume=1", "/analytics", "/metrics", "/readyz"] * 6
    workers = [threading.Thread(target=get, args=(path,)) for path in paths]
    try:
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        with pytest.raises(urllib.error.HTTPError) as rejected:
            urllib.request.urlopen(
                urllib.request.Request(f"http://127.0.0.1:{port}/readyz", method="DELETE"),
                timeout=10,
            )
        assert rejected.value.code == 405
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    assert errors == []
    assert all(status == 200 for _, status, _ in results) and len(results) == len(paths)
    sizes = {size for path, _, size in results if path.startswith("/analytics")}
    assert len(sizes) == 1 and sizes.pop() > 10000
    assert exporter.analytics_responses == 6
