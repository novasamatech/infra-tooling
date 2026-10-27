# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Cloudflare API response and error counters, including transport retries."""

import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import image_smoke
import pytest
import requests
import responses
from conftest import BASE_URL, NOW, requests_group
from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.parser import text_string_to_metric_families

import script

ZONES = {"success": True, "result": [], "result_info": {"total_pages": 1}}
SUCCESSFUL_ZONE = 'cf_exporter_zone_scrape_success{zone_name="example.org"} 1.0\n'


def exposition(api):
    registry = CollectorRegistry()
    registry.register(api.stats)
    return generate_latest(registry).decode()


def counters(api):
    """Nonzero counters as {(family, api, code or error): value}, from the exposition."""
    result = {}
    for family in text_string_to_metric_families(exposition(api)):
        for sample in family.samples:
            if sample.value:
                kind = sample.labels.get("code") or sample.labels["error"]
                name = sample.name.removeprefix("cf_exporter_api_").removesuffix("_total")
                result[(name, sample.labels["api"], kind)] = sample.value
    return result


def test_error_kinds_are_exported_from_zero(api):
    text = exposition(api)
    assert "# TYPE cf_exporter_api_responses_total counter" in text
    assert "# TYPE cf_exporter_api_errors_total counter" in text
    for name in script.APIS:
        for error in script.API_ERRORS:
            assert f'cf_exporter_api_errors_total{{api="{name}",error="{error}"}} 0.0' in text
    assert "cf_exporter_api_responses_total{" not in text
    assert counters(api) == {}
    # The stdlib-only image check mirrors the fixed label sets.
    assert (image_smoke.API_NAMES, image_smoke.API_ERRORS) == (script.APIS, script.API_ERRORS)


def zones_call(api):
    api.list_zones()


def graphql_call(api):
    api.graphql("query { viewer { zones { x } } }", {})


@pytest.mark.parametrize(
    ("call", "mock", "expected"),
    [
        (zones_call, {"json": ZONES}, {}),
        (zones_call, {"status": 429, "body": "Too Many Requests"}, {"rate_limited": 1}),
        (zones_call, {"status": 403, "json": {"errors": [{"code": 9109}]}}, {"client_error": 1}),
        (zones_call, {"status": 502, "body": "<html>Bad gateway</html>"}, {"server_error": 1}),
        (zones_call, {"body": "<html>OK</html>"}, {"invalid_response": 1}),
        (zones_call, {"json": {"success": False, "errors": []}}, {"invalid_response": 1}),
        (zones_call, {"json": ["unexpected"]}, {"invalid_response": 1}),
        (
            zones_call,
            {"json": {**ZONES, "result_info": {"total_pages": "1"}}},
            {"invalid_response": 1},
        ),
        (graphql_call, {"json": {"data": {"viewer": {}}}}, {}),
        (
            graphql_call,
            {
                "json": {
                    "data": None,
                    "errors": [{"message": "no", "extensions": {"code": "authz"}}],
                }
            },
            {"graphql": 1},
        ),
        (
            graphql_call,
            {"json": {"errors": [{"message": "rate limiter budget depleted"}]}},
            {"budget_depleted": 1},
        ),
        (graphql_call, {"json": {"data": None}}, {"invalid_response": 1}),
        (graphql_call, {"status": 429, "json": {"errors": []}}, {"rate_limited": 1}),
        (graphql_call, {"status": 504, "json": {}}, {"server_error": 1}),
    ],
)
def test_every_attempt_counts_once(rsps, api, call, mock, expected):
    """Each response counts once, with at most one error kind. The transport retries
    429 and 5xx responses three times; every attempt is counted, none twice."""
    name = "graphql" if call is graphql_call else "rest"
    method = responses.POST if name == "graphql" else responses.GET
    rsps.add(method, f"{BASE_URL}/{'graphql' if name == 'graphql' else 'zones'}", **mock)
    try:
        call(api)
    except script.CloudflareAPIError:
        assert expected, "a successful call raised"
    else:
        assert not expected, "a counted error did not fail the call"
    status = mock.get("status", 200)
    attempts = 4 if status in (429, 500, 502, 503, 504) else 1
    assert len(rsps.calls) == attempts
    assert counters(api) == {
        ("responses", name, str(status)): attempts,
        **{("errors", name, error): count * attempts for error, count in expected.items()},
    }


@pytest.mark.parametrize(
    ("exception", "error"),
    [
        (requests.ReadTimeout("read"), "timeout"),
        (requests.ConnectTimeout("connect"), "timeout"),
        (requests.ConnectionError("refused"), "connection"),
        (requests.exceptions.SSLError("tls"), "connection"),
        (requests.exceptions.ChunkedEncodingError("body"), "connection"),
    ],
)
def test_transport_failure_counts_without_a_response(rsps, api, exception, error):
    rsps.add(responses.GET, f"{BASE_URL}/zones", body=exception)
    with pytest.raises(script.CloudflareAPIError):
        api.list_zones()
    assert counters(api) == {("errors", "rest", error): 1}


def test_cycle_counts_listing_settings_and_a_failed_zone(cloudflare, api):
    cloudflare.add_zone("a", "good.example", requests=[requests_group(4)])
    cloudflare.add_zone("b", "bad.example", error="not authorized")
    snapshot = script.collect_once(api, now=NOW)
    assert snapshot.zone_success == {"good.example": 1, "bad.example": 0}
    # One listing; settings for both zones, analytics for both (one is rejected).
    assert counters(api) == {
        ("responses", "rest", "200"): 1,
        ("responses", "graphql", "200"): 4,
        ("errors", "graphql", "graphql"): 1,
    }


@pytest.fixture
def no_backoff(monkeypatch):
    sleeps = []
    monkeypatch.setattr("urllib3.util.retry.time.sleep", sleeps.append)
    return sleeps


@pytest.fixture
def server():
    """A local API whose responses tests script; transport retries are real."""
    replies, release = [], threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            status, headers = replies.pop(0) if len(replies) > 1 else replies[0]
            if status is None:  # Never answer within the client's timeout.
                release.wait(timeout=5)
                return
            body = {"data": {"viewer": {}}} if status == 200 else {"errors": []}
            self.send_response(status)
            for header in headers.items():
                self.send_header(*header)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as httpd:
        worker = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.01})
        worker.start()
        client = script.CloudflareAPI(
            "dummy-credential",
            request_timeout=0.5,
            base_url=f"http://127.0.0.1:{httpd.server_port}",
        )
        client.session.trust_env = False
        try:
            yield client, replies
        finally:
            release.set()
            client.session.close()
            httpd.shutdown()
            worker.join()


def test_retried_attempts_are_counted_with_the_final_one(server, no_backoff):
    client, replies = server
    replies.extend([(503, {}), (429, {"Retry-After": "1"}), (200, {})])
    assert client.graphql("q", {}) == {"viewer": {}}
    assert no_backoff == [1]  # The 429's Retry-After; the first retry has no backoff.
    assert counters(client) == {
        ("responses", "graphql", "503"): 1,
        ("responses", "graphql", "429"): 1,
        ("responses", "graphql", "200"): 1,
        ("errors", "graphql", "server_error"): 1,
        ("errors", "graphql", "rate_limited"): 1,
    }


def test_long_retry_after_counts_its_single_rate_limited_attempt(server, no_backoff):
    client, replies = server
    replies.append((429, {"Retry-After": "300"}))
    with pytest.raises(script.RateLimitedError):
        client.graphql("q", {})
    assert no_backoff == []
    assert counters(client) == {
        ("responses", "graphql", "429"): 1,
        ("errors", "graphql", "rate_limited"): 1,
    }


def test_exhausted_server_errors_count_every_attempt(server, no_backoff):
    client, replies = server
    replies.append((502, {}))
    with pytest.raises(script.CloudflareAPIError, match="HTTP 502"):
        client.graphql("q", {})
    assert counters(client) == {
        ("responses", "graphql", "502"): 4,
        ("errors", "graphql", "server_error"): 4,
    }


def test_exhausted_read_timeouts_are_timeouts(server, no_backoff):
    """requests reports exhausted read timeouts as a ConnectionError."""
    client, replies = server
    replies.append((None, {}))
    with pytest.raises(script.CloudflareAPIError):
        client.graphql("q", {})
    assert counters(client) == {("errors", "graphql", "timeout"): 4}


def test_refused_connections_are_connection_errors(no_backoff):
    """urllib3 derives a refused connection from its connect timeout error."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    client = script.CloudflareAPI("dummy-credential", base_url=f"http://127.0.0.1:{port}")
    client.session.trust_env = False
    with pytest.raises(script.CloudflareAPIError):
        client.list_zones()
    client.session.close()
    assert counters(client) == {("errors", "rest", "connection"): 4}


def test_image_check_validates_api_counters(api):
    api.stats.record("rest", 200)
    api.stats.record("graphql", 200)
    api.stats.record("graphql", 429, "rate_limited")
    body = SUCCESSFUL_ZONE + exposition(api)
    errors = image_smoke.checked_api_counters(body)
    assert errors[("graphql", "rate_limited")] == 1
    assert sum(errors.values()) == 1
    invalid = [
        body.replace("# TYPE cf_exporter_api_errors_total counter", ""),
        body.replace('error="graphql"', 'error="other"'),
        body + 'cf_exporter_api_errors_total{api="rest",error="timeout"} 0.0\n',
        body + 'cf_exporter_api_responses_total{api="rest",code="abc"} 1.0\n',
        body + 'cf_exporter_api_responses_total{api="soap",code="200"} 1.0\n',
        body.replace('code="429"} 1.0', 'code="429"} 0.5'),
        body.replace('{api="graphql",code="200"} 1.0', '{api="graphql",code="204"} 1.0'),
    ]
    for text in invalid:
        with pytest.raises(ValueError):
            image_smoke.checked_api_counters(text)
