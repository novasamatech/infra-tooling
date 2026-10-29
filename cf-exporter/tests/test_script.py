# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Offline tests: external HTTP traffic is mocked; retry checks use localhost."""

import json
import logging
import signal
import sys
import threading
import time
from datetime import UTC, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
import requests
import responses
from conftest import (
    ALL_FIELDS,
    BASE_URL,
    FREE_FIELDS,
    MAX_PAGE_SIZE,
    NOW,
    json_response,
    requests_group,
    visits_group,
)
from image_smoke import cycle_started_today
from prometheus_client import generate_latest
from responses import matchers

import script

SINCE, UNTIL = script.day_window(NOW)

# ------------------------------------------------------------- config ------


def test_parse_env_defaults():
    config = script.parse_env({"CF_EXPORTER_API_TOKEN": "abc"})
    assert config == script.Config(
        api_token="abc", request_timeout=30, scrape_interval=300, metrics_port=8000
    )


def test_parse_env_custom_values():
    config = script.parse_env(
        {
            "CF_EXPORTER_API_TOKEN": "abc",
            "CF_EXPORTER_REQUEST_TIMEOUT": "60",
            "CF_EXPORTER_SCRAPE_INTERVAL": " 600 ",
            "CF_EXPORTER_METRICS_PORT": "9000",
        }
    )
    assert (config.request_timeout, config.scrape_interval, config.metrics_port) == (60, 600, 9000)


def test_parse_env_strips_trailing_newline_from_secret():
    assert script.parse_env({"CF_EXPORTER_API_TOKEN": "  abc\n"}).api_token == "abc"


@pytest.mark.parametrize("token", ["abc\ndef", "abc def", "abc\tdef", "abc\x00def", "abc\x7fdef"])
def test_invalid_token_is_rejected_without_echoing_it(token):
    with pytest.raises(script.ConfigError) as error:
        script.parse_env({"CF_EXPORTER_API_TOKEN": token})
    assert token not in str(error.value)
    with pytest.raises(script.ConfigError):
        script.CloudflareAPI(token)


def test_config_repr_excludes_credentials():
    assert "dummy-credential" not in repr(
        script.parse_env({"CF_EXPORTER_API_TOKEN": "dummy-credential"})
    )


def test_log_formatter_redacts_credentials_in_messages_and_tracebacks():
    token = "dummy-credential"
    formatter = script.RedactingFormatter(token)
    try:
        raise RuntimeError(token)
    except RuntimeError:
        record = logging.LogRecord(
            "test", logging.ERROR, __file__, 1, "failed: %s", (token,), sys.exc_info()
        )
    output = formatter.format(record)
    assert token not in output
    assert "[REDACTED]" in output
    assert "RuntimeError" in output


@pytest.mark.parametrize("value", [None, "", "   \n"])
def test_parse_env_requires_api_token(value):
    environ = {} if value is None else {"CF_EXPORTER_API_TOKEN": value}
    with pytest.raises(script.ConfigError, match="CF_EXPORTER_API_TOKEN must be set"):
        script.parse_env(environ)


def test_parse_env_empty_values_use_defaults():
    config = script.parse_env({"CF_EXPORTER_API_TOKEN": "abc", "CF_EXPORTER_METRICS_PORT": ""})
    assert config.metrics_port == 8000


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("CF_EXPORTER_REQUEST_TIMEOUT", "abc", "must be an integer"),
        ("CF_EXPORTER_REQUEST_TIMEOUT", "0", "must be >= 1"),
        ("CF_EXPORTER_SCRAPE_INTERVAL", "1.5", "must be an integer"),
        ("CF_EXPORTER_SCRAPE_INTERVAL", "-5", "must be >= 1"),
        ("CF_EXPORTER_METRICS_PORT", "0", "must be between 1 and 65535"),
        ("CF_EXPORTER_METRICS_PORT", "70000", "must be between 1 and 65535"),
        *(
            case
            for name in script.LIMITS.values()
            for case in ((name, "-1", "must be >= 0"), (name, "1.5", "must be an integer"))
        ),
    ],
)
def test_parse_env_rejects_invalid_values(name, value, message):
    with pytest.raises(script.ConfigError, match=f"{name} {message}"):
        script.parse_env({"CF_EXPORTER_API_TOKEN": "abc", name: value})


@pytest.fixture
def basic_config(monkeypatch):
    calls = []
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: calls.append(kwargs))
    return calls


def test_configure_logging_valid_level(basic_config, caplog):
    caplog.set_level(logging.INFO)
    script.configure_logging({"CF_EXPORTER_LOGLEVEL": "debug"})
    assert basic_config[0]["level"] == logging.DEBUG
    assert "Log level set to DEBUG." in caplog.text


def test_configure_logging_defaults_to_info(basic_config):
    script.configure_logging({})
    assert basic_config[0]["level"] == logging.INFO


def test_configure_logging_invalid_level_warns(basic_config, caplog):
    script.configure_logging({"CF_EXPORTER_LOGLEVEL": "verbose"})
    assert basic_config[0]["level"] == logging.INFO
    assert "Invalid CF_EXPORTER_LOGLEVEL 'VERBOSE', using INFO." in caplog.text


# --------------------------------------------------------- API client ------


def test_session_sends_bearer_auth_and_retries_transient_errors(api):
    assert api.session.headers["Authorization"] == "Bearer dummy-credential"
    retry = api.session.get_adapter("https://api.cloudflare.com").max_retries
    assert retry.total == 3
    assert {429, 500, 502, 503, 504} <= set(retry.status_forcelist)
    assert {"GET", "POST"} <= set(retry.allowed_methods)


def test_list_zones_follows_pagination(rsps, api):
    for page, total in ((1, 2), (2, 2)):
        rsps.add(
            responses.GET,
            f"{BASE_URL}/zones",
            match=[matchers.query_param_matcher({"page": str(page)}, strict_match=False)],
            json={
                "success": True,
                "result": [{"id": f"id{page}", "name": f"zone{page}.example"}],
                "result_info": {"page": page, "total_pages": total},
            },
        )
    zones = api.list_zones()
    assert [z["name"] for z in zones] == ["zone1.example", "zone2.example"]
    assert "per_page=50" in rsps.calls[0].request.url


def test_list_zones_reports_cloudflare_error_message(rsps, api):
    rsps.add(
        responses.GET,
        f"{BASE_URL}/zones",
        status=400,
        json={
            "success": False,
            "errors": [{"code": 6003, "message": "Invalid request headers"}],
            "result": None,
        },
    )
    with pytest.raises(
        script.CloudflareAPIError, match=r"HTTP 400: Invalid request headers \(6003\)"
    ):
        api.list_zones()


def test_list_zones_fails_on_success_false_even_with_http_200(rsps, api):
    rsps.add(
        responses.GET,
        f"{BASE_URL}/zones",
        json={"success": False, "errors": [{"code": 1000, "message": "boom"}]},
    )
    with pytest.raises(script.CloudflareAPIError, match=r"boom \(1000\)"):
        api.list_zones()


def test_partial_pagination_failure_is_not_silently_truncated(rsps, api):
    rsps.add(
        responses.GET,
        f"{BASE_URL}/zones",
        match=[matchers.query_param_matcher({"page": "1"}, strict_match=False)],
        json={
            "success": True,
            "result": [{"id": "a", "name": "a"}],
            "result_info": {"total_pages": 2},
        },
    )
    rsps.add(
        responses.GET,
        f"{BASE_URL}/zones",
        match=[matchers.query_param_matcher({"page": "2"}, strict_match=False)],
        status=500,
        json={"success": False, "errors": [{"code": 1, "message": "internal"}]},
    )
    with pytest.raises(script.CloudflareAPIError, match="HTTP 500"):
        api.list_zones()


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"success": True, "result": None, "result_info": {"total_pages": 1}},
        {"success": True, "result": [], "result_info": None},
        {"success": True, "result": [], "result_info": {}},
        {"success": True, "result": [], "result_info": {"total_pages": "1"}},
        {"success": True, "result": [], "result_info": {"total_pages": True}},
        {"success": True, "result": [], "result_info": {"total_pages": -1}},
        {"success": True, "result": [], "result_info": {"total_pages": 2}},
        {"success": True, "result": [], "result_info": {"total_pages": 1, "page": 2}},
        {"success": True, "result": [None], "result_info": {"total_pages": 1}},
        {"success": True, "result": [{"id": "x"}], "result_info": {"total_pages": 1}},
    ],
)
def test_invalid_zone_listing_reports_failure(rsps, api, payload):
    rsps.add(responses.GET, f"{BASE_URL}/zones", json=payload)
    assert script.collect_once(api, now=NOW).cycle_success == 0


@pytest.mark.parametrize("total_pages", [0, 1])
def test_valid_empty_zone_listing(rsps, api, total_pages):
    rsps.add(
        responses.GET,
        f"{BASE_URL}/zones",
        json={"success": True, "result": [], "result_info": {"total_pages": total_pages}},
    )
    assert api.list_zones() == []


@pytest.mark.parametrize("kind", ["rest", "graphql", "network"])
def test_api_errors_redact_credentials(rsps, api, kind):
    token = "dummy-credential"
    if kind == "graphql":
        rsps.add(responses.POST, f"{BASE_URL}/graphql", json={"errors": [{"message": token}]})

        def call():
            return api.graphql("q", {})
    else:
        if kind == "network":
            rsps.add(responses.GET, f"{BASE_URL}/zones", body=requests.ConnectionError(token))
        else:
            rsps.add(
                responses.GET,
                f"{BASE_URL}/zones",
                status=403,
                json={"errors": [{"message": token}]},
            )
        call = api.list_zones
    with pytest.raises(script.CloudflareAPIError) as error:
        call()
    assert token not in str(error.value)
    assert "[REDACTED]" in str(error.value)


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
@pytest.mark.parametrize("method", ["GET", "POST"])
def test_transient_http_errors_are_retried_with_backoff(monkeypatch, status, method):
    sleeps = []
    monkeypatch.setattr("urllib3.util.retry.time.sleep", sleeps.append)
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(self.command)
            self.send_response(status if len(calls) < 3 else 200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"success": true}')

        do_POST = do_GET

        def log_message(self, *args):
            pass

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        worker.start()
        client = script.CloudflareAPI(
            "dummy-credential", base_url=f"http://127.0.0.1:{server.server_port}"
        )
        client.session.trust_env = False
        try:
            assert client._request(method, "/probe") == {"success": True}
            assert calls == [method] * 3
            assert sleeps == [4.0]
        finally:
            client.session.close()
            server.shutdown()
            worker.join()


@pytest.mark.parametrize(
    "maximum, retry_after, attempts, sleeps_expected",
    [(0, "300", 2, [300]), (30, "300", 1, []), (30, "2", 2, [2])],
)
def test_retry_after_is_waited_for_up_to_a_configured_maximum(
    monkeypatch, maximum, retry_after, attempts, sleeps_expected
):
    """Without a configured maximum any Retry-After is waited for; with one, a longer
    Retry-After fails the request at once instead of stalling the cycle."""
    sleeps = []
    monkeypatch.setattr("urllib3.util.retry.time.sleep", sleeps.append)
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            calls.append(self.command)
            limited = len(calls) == 1
            self.send_response(429 if limited else 200)
            if limited:
                self.send_header("Retry-After", retry_after)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            body = {"errors": [{"message": "budget depleted"}]} if limited else {"success": True}
            self.wfile.write(json.dumps(body).encode())

        def log_message(self, *args):
            pass

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        worker.start()
        client = script.CloudflareAPI(
            "dummy-credential",
            base_url=f"http://127.0.0.1:{server.server_port}",
            max_retry_after_seconds=maximum,
        )
        client.session.trust_env = False
        try:
            if attempts == 1:
                with pytest.raises(script.RateLimitedError, match="HTTP 429: budget depleted"):
                    client._request("POST", "/graphql")
            else:
                assert client._request("POST", "/graphql") == {"success": True}
            assert len(calls) == attempts
            assert sleeps == sleeps_expected
        finally:
            client.session.close()
            server.shutdown()
            worker.join()


def test_rate_limit_without_json_body_is_reported(rsps, api):
    rsps.add(responses.GET, f"{BASE_URL}/zones", status=429, body="Too Many Requests")
    with pytest.raises(script.RateLimitedError, match="GET /zones: HTTP 429: rate limited"):
        api.list_zones()


def test_non_json_response_is_reported(rsps, api):
    rsps.add(responses.GET, f"{BASE_URL}/zones", status=502, body="<html>Bad gateway</html>")
    with pytest.raises(script.CloudflareAPIError, match="HTTP 502, response is not JSON"):
        api.list_zones()


def test_non_object_json_response_is_reported(rsps, api):
    rsps.add(responses.GET, f"{BASE_URL}/zones", json=["unexpected"])
    with pytest.raises(script.CloudflareAPIError, match="unexpected response"):
        api.list_zones()


def test_network_error_is_wrapped(rsps, api):
    rsps.add(responses.GET, f"{BASE_URL}/zones", body=requests.ConnectionError("connection reset"))
    with pytest.raises(script.CloudflareAPIError, match="GET /zones: connection reset"):
        api.list_zones()


def test_graphql_sends_query_and_variables(rsps, api):
    rsps.add(
        responses.POST,
        f"{BASE_URL}/graphql",
        match=[matchers.json_params_matcher({"query": "q", "variables": {"a": 1}})],
        json={"data": {"viewer": {}}, "errors": None},
    )
    assert api.graphql("q", {"a": 1}) == {"viewer": {}}


def test_graphql_errors_raise_with_messages(rsps, api):
    rsps.add(
        responses.POST,
        f"{BASE_URL}/graphql",
        json={
            "data": None,
            "errors": [
                {"message": "first", "extensions": {"code": "authz"}},
                {"message": "second"},
            ],
        },
    )
    with pytest.raises(script.CloudflareAPIError, match=r"GraphQL: first \(authz\); second") as e:
        api.graphql("q", {})
    assert not isinstance(e.value, script.RateLimitedError)


@pytest.mark.parametrize(
    "error",
    [
        {"message": "rate limiter budget depleted, try again after 5 minutes"},
        {"message": "limit reached", "extensions": {"code": "budget"}},
    ],
)
def test_graphql_budget_errors_without_http_429_are_rate_limits(rsps, api, error):
    rsps.add(responses.POST, f"{BASE_URL}/graphql", json={"data": None, "errors": [error]})
    with pytest.raises(script.RateLimitedError):
        api.graphql("q", {})


def test_graphql_without_data_raises(rsps, api):
    rsps.add(responses.POST, f"{BASE_URL}/graphql", json={"data": None, "errors": None})
    with pytest.raises(script.CloudflareAPIError, match="contains no data"):
        api.graphql("q", {})


@pytest.mark.parametrize(
    ("errors", "expected"),
    [
        (None, "unknown error"),
        ([], "unknown error"),
        (["plain"], "plain"),
        ([{"code": 10000, "message": "Authentication error"}], "Authentication error (10000)"),
        ([{"message": "x", "extensions": {"code": "budget"}}], "x (budget)"),
        ([{}], "unknown error"),
        ({"message": "x", "extensions": []}, "x"),
        ([{"message": 123, "extensions": "invalid"}], "123"),
    ],
)
def test_format_errors(errors, expected):
    assert script.format_errors(errors) == expected


# ------------------------------------------------------ query building -----


def test_day_window_starts_at_midnight_utc_and_drops_microseconds():
    since, until = script.day_window(NOW)
    assert since == datetime(2026, 10, 13, tzinfo=UTC)
    assert until == datetime(2026, 10, 13, 7, 40, 12, tzinfo=UTC)
    assert script.rfc3339(since) == "2026-10-13T00:00:00Z"
    assert script.rfc3339(until) == "2026-10-13T07:40:12Z"


def test_day_window_converts_other_timezones_to_utc():
    local = datetime(2026, 10, 13, 1, 30, tzinfo=timezone(timedelta(hours=3)))
    since, until = script.day_window(local)
    assert since == datetime(2026, 10, 12, tzinfo=UTC)
    assert until == datetime(2026, 10, 12, 22, 30, tzinfo=UTC)


def test_build_analytics_query_uses_variables_order_and_given_dimensions():
    query = script.build_analytics_query(
        {"visits": ["clientRequestPath"], "requests": ["cacheStatus", "userAgentOS"]}
    )
    assert "$zoneTag: string!" in query
    assert "zoneTag: $zoneTag" in query
    assert "datetime_geq: $since, datetime_lt: $until" in query
    assert "visits: httpRequestsAdaptiveGroups(" in query
    assert "requests: httpRequestsAdaptiveGroups(" in query
    assert "orderBy: [sum_visits_DESC]" in query
    assert "orderBy: [count_DESC]" in query
    assert "cacheStatus userAgentOS" in query
    assert "clientRequestQuery" not in query


def test_query_omits_dimensions_when_none_are_available(cloudflare, api):
    cloudflare.add_zone(
        "z1",
        "example.com",
        fields=script.REQUIRED_FIELDS,
        requests=[{"count": 2}],
        visits=[{"sum": {"visits": 1}}],
    )
    result = script.collect_zone(api, "z1", "example.com", SINCE, UNTIL)
    assert "dimensions" not in cloudflare.data_calls()[0]["query"]
    assert list(result.requests.items()) == [
        (("example.com", *(["unknown"] * len(script.REQUESTS_DIMENSIONS))), 2)
    ]


def test_collect_zone_respects_plan_page_size(cloudflare, api, caplog):
    cloudflare.add_zone(
        "z1", "example.com", requests=[requests_group(2), requests_group(1)], max_page_size=1
    )
    script.collect_zone(api, "z1", "example.com", SINCE, UNTIL, limit=1)
    assert cloudflare.data_calls()[0]["variables"]["limit"] == 1
    assert "hit the 1 group limit" in caplog.text


def test_collect_zone_at_midnight_avoids_empty_range_query(cloudflare, api):
    cloudflare.add_zone("z1", "example.com")
    assert script.collect_zone(api, "z1", "example.com", SINCE, SINCE) == script.ZoneMetrics(
        {},
        {},
        limited=dict.fromkeys(script.DATASETS, 0),
        available=dict.fromkeys(script.DATASETS, 1),
    )
    assert cloudflare.data_calls() == []


@pytest.mark.parametrize("page_size", [None, "10000", 0, -1, True])
def test_collect_zone_rejects_invalid_plan_page_size(cloudflare, api, page_size):
    cloudflare.add_zone("z1", "example.com", max_page_size=page_size)
    assert script.collect_once(api, now=NOW).zone_success == {"example.com": 0}


@pytest.mark.parametrize(
    "node",
    [
        None,
        {},
        {"visits": [], "requests": None},
        {"visits": {}, "requests": []},
        {"visits": [None], "requests": []},
        {"visits": [], "requests": [{"count": None}]},
        {"visits": [], "requests": [{"count": "1"}]},
        {"visits": [], "requests": [{"count": -1}]},
        {"visits": [], "requests": [{"count": True}]},
        {"visits": [], "requests": [{"count": float("inf")}]},
        {"visits": [], "requests": [{"count": 1, "dimensions": []}]},
        {"visits": [{"sum": "invalid"}], "requests": []},
    ],
)
def test_partial_or_invalid_analytics_is_a_zone_failure(rsps, api, node):
    def callback(request):
        query = json.loads(request.body)["query"]
        zone = (
            {
                "settings": {
                    "httpRequestsAdaptiveGroups": {
                        "enabled": True,
                        "availableFields": sorted(ALL_FIELDS),
                        "maxPageSize": 10000,
                    }
                }
            }
            if "AvailableFields" in query
            else ({"cache": [], "totals": [], **node} if isinstance(node, dict) else node)
        )
        return (
            200,
            {"Content-Type": "application/json"},
            json.dumps({"data": {"viewer": {"zones": [zone]}}}),
        )

    rsps.add_callback(responses.POST, f"{BASE_URL}/graphql", callback=callback)
    with pytest.raises(script.CloudflareAPIError):
        script.collect_zone(api, "z1", "example.com", SINCE, UNTIL)


@pytest.mark.parametrize("fields", [None, "count", [123]])
def test_invalid_available_fields_is_an_api_error(rsps, api, fields):
    rsps.add(
        responses.POST,
        f"{BASE_URL}/graphql",
        json={
            "data": {
                "viewer": {
                    "zones": [
                        {
                            "settings": {
                                "httpRequestsAdaptiveGroups": {
                                    "enabled": True,
                                    "availableFields": fields,
                                    "maxPageSize": 10000,
                                }
                            }
                        }
                    ]
                }
            }
        },
    )
    with pytest.raises(script.CloudflareAPIError, match="invalid availableFields"):
        script.fetch_available_fields(api, "z1")


# ----------------------------------------------------- zone collection -----


def test_collect_zone_maps_dimensions_to_labels(cloudflare, api):
    cloudflare.add_zone(
        "z1",
        "example.com",
        visits=[
            visits_group(
                5,
                clientRequestHTTPHost="www.example.com",
                clientRequestPath="/",
                clientCountryName="DE",
                clientRequestReferer="",
                userAgentBrowser="Chrome",
                userAgentOS="Linux",
            ),
            visits_group(0, clientRequestHTTPHost="skipped"),
        ],
        requests=[
            requests_group(
                7,
                clientRequestHTTPHost="www.example.com",
                clientRequestHTTPMethodName="GET",
                clientRequestPath="/a",
                clientRequestQuery="?x=1",
                clientCountryName="US",
                clientRequestReferer="https://ref.example/",
                userAgentBrowser="Firefox",
                userAgentOS="Windows",
                cacheStatus="hit",
                originResponseStatus=200,
            ),
            requests_group(0, clientRequestHTTPHost="skipped"),
        ],
    )
    result = script.collect_zone(api, "z1", "example.com", SINCE, UNTIL)
    assert result.visits == {
        ("example.com", "www.example.com", "/", "DE", "direct", "Chrome", "Linux"): 5
    }
    assert result.requests == {
        (
            "example.com",
            "www.example.com",
            "GET",
            "/a",
            "?x=1",
            "US",
            "https://ref.example/",
            "Firefox",
            "Windows",
            "hit",
            "200",
        ): 7
    }
    variables = cloudflare.data_calls()[0]["variables"]
    assert variables == {
        "zoneTag": "z1",
        "since": "2026-10-13T00:00:00Z",
        "until": "2026-10-13T07:40:12Z",
        "limit": MAX_PAGE_SIZE,
    }


def test_collect_zone_free_plan_skips_unavailable_dimensions(cloudflare, api):
    """Regression: Free zones used to fail entirely and export nothing."""
    cloudflare.add_zone(
        "z1",
        "free.example",
        fields=FREE_FIELDS,
        visits=[visits_group(1, clientRequestHTTPHost="free.example", clientRequestPath="/")],
        requests=[requests_group(2, clientRequestHTTPHost="free.example", cacheStatus="miss")],
    )
    result = script.collect_zone(api, "z1", "free.example", SINCE, UNTIL)
    query = cloudflare.data_calls()[0]["query"]
    assert "clientRequestQuery" not in query
    assert "clientRequestReferer" not in query
    (visit_labels,) = result.visits
    assert dict(zip(script.VISITS_LABELS, visit_labels, strict=True))["client_request_referer"] == (
        "unknown"
    )
    (request_labels,) = result.requests
    labels = dict(zip(script.REQUESTS_LABELS, request_labels, strict=True))
    assert labels["query"] == "unknown"
    assert labels["client_request_referer"] == "unknown"
    assert labels["cache_status"] == "miss"
    # Dimensions missing from a group fall back to "unknown" as well.
    assert labels["method_name"] == "unknown"


def test_collect_zone_aggregates_duplicate_label_sets(cloudflare, api):
    cloudflare.add_zone(
        "z1",
        "example.com",
        requests=[
            requests_group(3, clientRequestHTTPHost="a", originResponseStatus=200),
            requests_group(4, clientRequestHTTPHost="a", originResponseStatus="200"),
        ],
    )
    result = script.collect_zone(api, "z1", "example.com", SINCE, UNTIL)
    assert list(result.requests.values()) == [7]


def test_collect_zone_warns_when_group_limit_is_hit(cloudflare, api, caplog):
    cloudflare.add_zone(
        "z1",
        "big.example",
        requests=[
            requests_group(2, clientRequestPath="/a"),
            requests_group(1, clientRequestPath="/b"),
            requests_group(1, clientRequestPath="/c"),
        ],
    )
    script.collect_zone(api, "z1", "big.example", SINCE, UNTIL, limit=2)
    assert "Zone big.example: requests hit the 2 group limit" in caplog.text
    assert "visits hit" not in caplog.text


def test_collect_zone_dataset_disabled(cloudflare, api):
    cloudflare.add_zone("z1", "example.com", enabled=False)
    with pytest.raises(script.CloudflareAPIError, match="not enabled"):
        script.collect_zone(api, "z1", "example.com", SINCE, UNTIL)


def test_collect_zone_required_fields_missing(cloudflare, api):
    cloudflare.add_zone("z1", "example.com", fields=FREE_FIELDS - {"sum_visits"})
    with pytest.raises(script.CloudflareAPIError, match="sum_visits"):
        script.collect_zone(api, "z1", "example.com", SINCE, UNTIL)


def test_collect_zone_graphql_error(cloudflare, api):
    cloudflare.add_zone("z1", "example.com", error="does not have access to the field")
    with pytest.raises(script.CloudflareAPIError, match="does not have access"):
        script.collect_zone(api, "z1", "example.com", SINCE, UNTIL)


def test_collect_zone_without_zone_node(rsps, api):
    rsps.add(responses.POST, f"{BASE_URL}/graphql", json={"data": {"viewer": {"zones": []}}})
    with pytest.raises(script.CloudflareAPIError, match="no data returned for zone z1"):
        script.collect_zone(api, "z1", "example.com", SINCE, UNTIL)


# ----------------------------------------------------- collection cycle ----


def test_collect_once_isolates_failing_zones(cloudflare, api, caplog):
    cloudflare.add_zone("ok", "ok.example", requests=[requests_group(1, clientRequestPath="/")])
    cloudflare.add_zone("bad", "bad.example", error="quota exceeded")
    snapshot = script.collect_once(api, now=NOW)
    assert snapshot.zone_success == {"ok.example": 1, "bad.example": 0}
    assert snapshot.cycle_success == 0
    assert [labels[0] for labels in snapshot.requests] == ["ok.example"]
    assert "Zone bad.example: collection failed: GraphQL: quota exceeded" in caplog.text


def test_rate_limit_skips_remaining_zones_until_next_cycle(cloudflare, api, caplog):
    cloudflare.add_zone(
        "a",
        "a.example",
        responder=lambda body: json_response(429, {"errors": [{"message": "budget depleted"}]}),
    )
    cloudflare.add_zone("b", "b.example")
    cloudflare.add_zone("c", "c.example")
    snapshot = script.collect_once(api, now=NOW)
    assert snapshot.zone_success == {"a.example": 0, "b.example": 0, "c.example": 0}
    assert snapshot.cycle_success == 0
    assert {call["variables"]["zoneTag"] for call in cloudflare.graphql_calls} == {"a"}
    assert "skipping the remaining zones this cycle" in caplog.text
    # The next cycle tries every zone again.
    cloudflare.zones["a"]["responder"] = None
    snapshot = script.collect_once(api, now=NOW)
    assert snapshot.zone_success == {"a.example": 1, "b.example": 1, "c.example": 1}


def test_collect_once_all_zones_ok(cloudflare, api):
    cloudflare.add_zone("a", "a.example", visits=[visits_group(2, clientRequestPath="/")])
    cloudflare.add_zone("b", "b.example", fields=FREE_FIELDS)
    snapshot = script.collect_once(api, now=NOW)
    assert snapshot.cycle_success == 1
    assert snapshot.zone_success == {"a.example": 1, "b.example": 1}
    assert snapshot.cycle_timestamp is not None
    assert snapshot.cycle_duration >= 0


def test_collect_once_survives_zone_listing_failure(cloudflare, api, caplog):
    """Regression: a failed zone listing used to terminate the process."""
    cloudflare.zones_error = {"code": 10000, "message": "Authentication error"}
    snapshot = script.collect_once(api, now=NOW)
    assert snapshot.cycle_success == 0
    assert snapshot.visits == snapshot.requests == snapshot.zone_success == {}
    assert "Failed to list zones" in caplog.text
    assert "Authentication error (10000)" in caplog.text


def test_collect_once_skips_zones_without_id_or_name(rsps, api, caplog):
    rsps.add(
        responses.GET,
        f"{BASE_URL}/zones",
        json={
            "success": True,
            "result": [{"id": "x"}, {"name": "y"}],
            "result_info": {"total_pages": 1},
        },
    )
    snapshot = script.collect_once(api, now=NOW)
    assert snapshot.zone_success == {}
    assert snapshot.cycle_success == 0
    assert "zone without a valid id or name" in caplog.text


def test_collect_once_isolates_unexpected_errors(cloudflare, api, monkeypatch, caplog):
    cloudflare.add_zone("a", "a.example")
    cloudflare.add_zone("b", "b.example")
    original = script.collect_zone

    def flaky(api, zone_id, *args, **kwargs):
        if zone_id == "a":
            raise KeyError("bug")
        return original(api, zone_id, *args, **kwargs)

    monkeypatch.setattr(script, "collect_zone", flaky)
    snapshot = script.collect_once(api, now=NOW)
    assert snapshot.zone_success == {"a.example": 0, "b.example": 1}
    assert "Zone a.example: unexpected error" in caplog.text


def test_collect_once_uses_one_time_window_for_all_zones(cloudflare, api):
    cloudflare.add_zone("a", "a.example")
    cloudflare.add_zone("b", "b.example")
    script.collect_once(api, now=NOW)
    windows = {(c["variables"]["since"], c["variables"]["until"]) for c in cloudflare.data_calls()}
    assert windows == {("2026-10-13T00:00:00Z", "2026-10-13T07:40:12Z")}


# ------------------------------------------------------------ collector ----

VISIT = ("z.example", "h", "/", "DE", "direct", "Chrome", "Linux")
REQUEST = ("z.example", "h", "GET", "/", "", "DE", "direct", "Chrome", "Linux", "hit", "200")


def snapshot(**overrides):
    values = {
        "visits": {VISIT: 3},
        "requests": {REQUEST: 9},
        "zone_success": {"z.example": 1},
        "cycle_success": 1,
        "cycle_timestamp": 1_700_000_000.0,
        "cycle_duration": 1.5,
    }
    values.update(overrides)
    return script.Snapshot(**values)


def labels(names, values):
    return dict(zip(names, values, strict=True))


def test_collector_exposes_snapshot(registry):
    collector = script.CloudflareCollector()
    registry.register(collector)
    collector.update(snapshot())
    sample = registry.get_sample_value
    assert sample("cf_visits", labels(script.VISITS_LABELS, VISIT)) == 3
    assert sample("cf_requests", labels(script.REQUESTS_LABELS, REQUEST)) == 9
    assert sample("cf_exporter_zone_scrape_success", {"zone_name": "z.example"}) == 1
    assert sample("cf_exporter_last_cycle_success") == 1
    assert sample("cf_exporter_last_cycle_timestamp_seconds") == 1_700_000_000.0
    assert sample("cf_exporter_cycle_duration_seconds") == 1.5
    text = generate_latest(registry).decode()
    assert "# HELP cf_visits Total visits since midnight UTC" in text
    assert "# TYPE cf_requests gauge" in text


def test_collector_before_first_cycle_has_no_cycle_metrics(registry):
    registry.register(script.CloudflareCollector())
    text = generate_latest(registry).decode()
    assert "# TYPE cf_visits gauge" in text
    assert "cf_exporter_last_cycle_success" not in text


def test_smoke_check_detects_cycles_that_crossed_midnight(registry):
    collector = script.CloudflareCollector()
    registry.register(collector)
    now = time.time()
    collector.update(script.Snapshot(cycle_success=1, cycle_timestamp=now, cycle_duration=0.0))
    assert cycle_started_today(generate_latest(registry).decode())
    since_midnight = now % 86400
    collector.update(
        script.Snapshot(cycle_success=1, cycle_timestamp=now, cycle_duration=since_midnight + 60)
    )
    assert not cycle_started_today(generate_latest(registry).decode())


def test_collector_drops_series_missing_from_new_snapshot(registry):
    """Regression: cf_requests series used to live forever."""
    collector = script.CloudflareCollector()
    registry.register(collector)
    collector.update(snapshot())
    collector.update(snapshot(visits={}, requests={}, zone_success={"z.example": 0}))
    assert registry.get_sample_value("cf_visits", labels(script.VISITS_LABELS, VISIT)) is None
    assert registry.get_sample_value("cf_requests", labels(script.REQUESTS_LABELS, REQUEST)) is None
    assert (
        registry.get_sample_value("cf_exporter_zone_scrape_success", {"zone_name": "z.example"})
        == 0
    )


def test_collector_scrape_sees_one_consistent_snapshot():
    """An update during a scrape must not mix two cycles in one response."""
    collector = script.CloudflareCollector()
    collector.update(snapshot())
    families = collector.collect()
    visits = next(family for family in families if family.name == "cf_visits")
    collector.update(snapshot(visits={}, requests={}))
    requests_ = next(family for family in families if family.name == "cf_requests")
    assert len(visits.samples) == 1
    assert len(requests_.samples) == 1


# ----------------------------------------------------------- main loop -----


class StopAfter:
    """Event stand-in that stops the loop after N waits and records them."""

    def __init__(self, waits):
        self.remaining = waits
        self.timeouts = []

    def is_set(self):
        return self.remaining <= 0

    def wait(self, timeout):
        self.timeouts.append(timeout)
        self.remaining -= 1


def test_run_survives_unexpected_errors_and_keeps_going(monkeypatch, caplog):
    """Regression: any exception used to end the main loop (and the process)."""
    results = iter([RuntimeError("bug"), snapshot()])

    def fake_collect_once(api):
        result = next(results)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(script, "collect_once", fake_collect_once)
    collector = script.CloudflareCollector()
    stop = StopAfter(1)
    original_update = collector.update
    states = []

    def recording_update(snap):
        states.append(snap)
        original_update(snap)

    collector.update = recording_update
    script.run(None, collector, 300, stop)
    assert states[0].cycle_success == 0
    assert states[0].cycle_duration >= 0
    assert "Collection cycle failed unexpectedly" in caplog.text
    stop.remaining = 1
    script.run(None, collector, 300, stop)
    assert collector.snapshot.cycle_success == 1


def fixed_clock(monkeypatch, moment):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return moment

    monkeypatch.setattr(script, "datetime", Clock)


def test_run_sleeps_for_the_rest_of_the_interval(monkeypatch):
    fixed_clock(monkeypatch, datetime(2026, 10, 26, 12, 0, tzinfo=UTC))
    monkeypatch.setattr(script, "collect_once", lambda api: snapshot())
    stop = StopAfter(2)
    script.run(None, script.CloudflareCollector(), 300, stop)
    assert len(stop.timeouts) == 2
    assert all(299 < t <= 300 for t in stop.timeouts)


def test_run_moves_the_last_cycle_of_the_day_before_midnight(monkeypatch):
    """Regression: the traffic between the last cycle and midnight was never exported."""
    fixed_clock(monkeypatch, datetime(2026, 10, 26, 23, 57, tzinfo=UTC))
    monkeypatch.setattr(script, "collect_once", lambda api: snapshot())
    stop = StopAfter(2)
    script.run(None, script.CloudflareCollector(), 300, stop)
    # 180 s to midnight minus the margin; then the regular interval, once a day.
    assert 149 < stop.timeouts[0] <= 150
    assert 299 < stop.timeouts[1] <= 300


def test_run_stops_when_event_is_set(monkeypatch):
    calls = []
    monkeypatch.setattr(script, "collect_once", lambda api: calls.append(1) or snapshot())
    event = threading.Event()
    event.set()
    script.run(None, script.CloudflareCollector(), 300, event)
    assert calls == []


@pytest.fixture
def quiet_logging(monkeypatch):
    monkeypatch.setattr(script, "configure_logging", lambda: None)


def test_main_exits_with_error_on_invalid_config(monkeypatch, quiet_logging, caplog):
    monkeypatch.delenv("CF_EXPORTER_API_TOKEN", raising=False)
    assert script.main() == 1
    assert "CF_EXPORTER_API_TOKEN must be set." in caplog.text


def test_main_wires_everything_together(monkeypatch, quiet_logging, registry):
    monkeypatch.setenv("CF_EXPORTER_API_TOKEN", "abc\n")
    monkeypatch.setenv("CF_EXPORTER_METRICS_PORT", "9123")
    monkeypatch.setenv("CF_EXPORTER_SCRAPE_INTERVAL", "42")
    monkeypatch.setenv("CF_EXPORTER_MAX_EXTRA_SELECTIONS", "152")
    monkeypatch.setenv("CF_EXPORTER_DISABLED_LABELS", '{"cf_requests": ["query"]}')
    handlers, servers, runs = {}, [], []
    monkeypatch.setattr(script, "REGISTRY", registry)
    monkeypatch.setattr(
        script.signal, "signal", lambda sig, handler: handlers.update({sig: handler})
    )
    monkeypatch.setattr(script, "start_http_server", lambda *args: servers.append(args))
    monkeypatch.setattr(script, "run", lambda *args: runs.append(args))

    assert script.main() == 0

    assert set(handlers) == {signal.SIGTERM, signal.SIGINT}
    api, collector, interval, event = runs[0]
    assert servers == [(9123, collector, 3 * 42)]
    assert api.session.headers["Authorization"] == "Bearer abc"
    # Configured options reach the client; nothing is derived from the interval.
    assert (api.max_extra_selections, api.slice_allowance) == (152, 152)
    assert (api.group_limit, api.max_slice_requests, api.min_slice_seconds, api.page_size) == (
        0,
        0,
        0,
        0,
    )
    assert "query" not in api.schema.labels("cf_requests")
    assert isinstance(collector, script.CloudflareCollector)
    assert interval == 42
    assert not event.is_set()
    text = generate_latest(registry).decode()
    assert "cf_visits" in text
    assert 'cf_exporter_api_errors_total{api="rest",error="rate_limited"} 0.0' in text


def test_signal_handler_exits_cleanly(caplog):
    caplog.set_level(logging.INFO)
    with pytest.raises(SystemExit) as exc:
        script._handle_signal(signal.SIGTERM, None)
    assert exc.value.code == 0
    assert "Signal SIGTERM received, shutting down." in caplog.text


def test_module_import_has_no_side_effects():
    """Signal handlers and metrics must only be installed by main()."""
    assert signal.getsignal(signal.SIGTERM) is not script._handle_signal
