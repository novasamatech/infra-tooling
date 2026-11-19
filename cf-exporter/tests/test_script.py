# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Configuration, logging, the API client, zone settings and plans, queries and
main(). External HTTP traffic is mocked; retry checks use localhost."""

import json
import logging
import signal
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
import requests
import responses
from conftest import BASE_URL, BUCKET, FREE_FIELDS, PRO_FIELDS, T0, json_response, plan_for
from prometheus_client import REGISTRY, CollectorRegistry, generate_latest
from responses import matchers

import script

# ------------------------------------------------------------- config ------


def test_parse_env_defaults():
    config = script.parse_env({"CF_EXPORTER_API_TOKEN": "abc"})
    assert config == script.Config(
        api_token="abc",
        request_timeout=30,
        metrics_port=8000,
        db_path="/var/lib/cf-exporter/cf-exporter.sqlite3",
        backfill_days=0,
        max_retry_after_seconds=0,
        page_size=0,
        schema=script.Schema(),
    )


def test_parse_env_custom_values():
    config = script.parse_env(
        {
            "CF_EXPORTER_API_TOKEN": "abc",
            "CF_EXPORTER_REQUEST_TIMEOUT": "60",
            "CF_EXPORTER_METRICS_PORT": " 9000 ",
            "CF_EXPORTER_DB_PATH": " /data/state.sqlite3 ",
            "CF_EXPORTER_BACKFILL_DAYS": "7",
            "CF_EXPORTER_MAX_RETRY_AFTER_SECONDS": "30",
            "CF_EXPORTER_PAGE_SIZE": "500",
            "CF_EXPORTER_DISABLED_LABELS": '{"cf_requests_5m": ["query"]}',
        }
    )
    assert (config.request_timeout, config.metrics_port) == (60, 9000)
    assert config.db_path == "/data/state.sqlite3"
    assert (config.backfill_days, config.max_retry_after_seconds, config.page_size) == (7, 30, 500)
    assert "query" not in config.schema.labels("cf_requests_5m")
    api = script.create_api(config)
    assert (api.max_retry_after_seconds, api.page_size, api.request_timeout) == (30, 500, 60)
    api.session.close()


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
    config = script.parse_env(
        {"CF_EXPORTER_API_TOKEN": "abc", "CF_EXPORTER_METRICS_PORT": "", "CF_EXPORTER_DB_PATH": " "}
    )
    assert config.metrics_port == 8000 and config.db_path == script.DEFAULT_DB_PATH


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("CF_EXPORTER_REQUEST_TIMEOUT", "abc", "must be an integer"),
        ("CF_EXPORTER_REQUEST_TIMEOUT", "0", "must be >= 1"),
        ("CF_EXPORTER_METRICS_PORT", "0", "must be between 1 and 65535"),
        ("CF_EXPORTER_METRICS_PORT", "70000", "must be between 1 and 65535"),
        ("CF_EXPORTER_BACKFILL_DAYS", "-1", "must be >= 0"),
        ("CF_EXPORTER_BACKFILL_DAYS", "1.5", "must be an integer"),
        ("CF_EXPORTER_MAX_RETRY_AFTER_SECONDS", "-1", "must be >= 0"),
        ("CF_EXPORTER_PAGE_SIZE", "many", "must be an integer"),
    ],
)
def test_parse_env_rejects_invalid_values(name, value, message):
    with pytest.raises(script.ConfigError, match=f"{name} {message}"):
        script.parse_env({"CF_EXPORTER_API_TOKEN": "abc", name: value})


def test_describe_lists_storage_limits_and_disabled_labels():
    config = script.parse_env({"CF_EXPORTER_API_TOKEN": "abc"})
    assert script.describe(config) == (
        "storage /var/lib/cf-exporter/cf-exporter.sqlite3; optional limits: none; "
        "disabled labels: none"
    )
    config = script.parse_env(
        {
            "CF_EXPORTER_API_TOKEN": "abc",
            "CF_EXPORTER_DB_PATH": "/data/x.sqlite3",
            "CF_EXPORTER_BACKFILL_DAYS": "3",
            "CF_EXPORTER_PAGE_SIZE": "100",
            "CF_EXPORTER_DISABLED_LABELS": (
                '{"cf_visits_5m": ["path"], "cf_requests_5m": ["user_agent_os", "query"]}'
            ),
        }
    )
    assert script.describe(config) == (
        "storage /data/x.sqlite3; optional limits: CF_EXPORTER_BACKFILL_DAYS=3, "
        "CF_EXPORTER_PAGE_SIZE=100; disabled labels: cf_visits_5m: path; "
        "cf_requests_5m: query, user_agent_os"
    )


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
    with pytest.raises(script.CloudflareAPIError):
        api.list_zones()


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
            assert sleeps == ([300, 300] if status == 429 else [4.0])
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


@pytest.mark.parametrize("api", [{"max_retry_after_seconds": 30}], indirect=True)
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
@pytest.mark.parametrize("api", [{"max_retry_after_seconds": 30}], indirect=True)
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


def test_list_zones_reads_every_page_of_the_account(cloudflare, api):
    for index in range(120):
        cloudflare.add_zone(f"id{index:03}", f"zone{index:03}.example")
    zones = api.list_zones()
    assert [zone["name"] for zone in zones] == [f"zone{index:03}.example" for index in range(120)]
    assert cloudflare.zone_calls == 3


def test_graphql_budget_error_waits_and_repeats_the_same_request(rsps, api, monkeypatch):
    sleeps = []
    monkeypatch.setattr(script.time, "sleep", sleeps.append)
    budget = {"data": None, "errors": [{"message": "rate limiter budget depleted"}]}
    rsps.add(responses.POST, f"{BASE_URL}/graphql", json=budget)
    rsps.add(responses.POST, f"{BASE_URL}/graphql", json={"data": {"viewer": {}}})
    assert api.graphql("q", {"a": 1}) == {"viewer": {}}
    assert sleeps == [script.RATE_LIMIT_RETRY_SECONDS]
    assert [json.loads(call.request.body) for call in rsps.calls] == [
        {"query": "q", "variables": {"a": 1}}
    ] * 2


# ------------------------------------------------------------ helpers ------


def test_time_helpers():
    assert script.rfc3339(T0) == "2027-01-15T08:00:00Z"
    assert script.rfc3339(T0 + 0.9) == "2027-01-15T08:00:00Z"
    assert script.parse_rfc3339("2027-01-15T08:00:00Z") == T0
    assert script.parse_rfc3339("2027-01-15T09:00:00+01:00") == T0
    assert script.bucket_floor(T0) == T0
    assert script.bucket_floor(T0 + 299.9) == T0
    assert script.bucket_floor(T0 - 1) == T0 - BUCKET
    # Buckets are aligned to UTC days and hours.
    assert T0 % 3600 == 0 and script.BUCKET_SECONDS == 300 and 86400 % script.BUCKET_SECONDS == 0


@pytest.mark.parametrize("value", [None, 5, "", "08:00", "2027-01-15T08:00:00"])
def test_invalid_bucket_times_are_api_errors(value):
    with pytest.raises(script.CloudflareAPIError):
        script.parse_rfc3339(value)


def test_retry_delays_back_off_to_a_maximum():
    assert [script.retry_delay(attempt) for attempt in (1, 2, 3, 4, 5, 6, 50)] == [
        120,
        240,
        480,
        900,
        1800,
        1800,
        1800,
    ]


# ------------------------------------------------- settings and plans ------


def settings_reply(**overrides):
    settings = {
        "enabled": True,
        "availableFields": PRO_FIELDS,
        "maxPageSize": 10000,
        "maxDuration": 2592000,
        "notOlderThan": 2678400,
        **overrides,
    }
    node = {"settings": {"httpRequestsAdaptiveGroups": settings}}
    return {"data": {"viewer": {"zones": [node]}}, "errors": None}


def test_settings_are_read_with_one_query(rsps, api):
    rsps.add(responses.POST, f"{BASE_URL}/graphql", json=settings_reply())
    settings = script.fetch_settings(api, "z1")
    assert settings == script.ZoneSettings(frozenset(PRO_FIELDS), 10000, 2592000, 2678400)
    sent = json.loads(rsps.calls[0].request.body)
    assert sent["variables"] == {"zoneTag": "z1"}
    for field in ("enabled", "availableFields", "maxPageSize", "maxDuration", "notOlderThan"):
        assert field in sent["query"]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"enabled": False}, "is not enabled"),
        ({"enabled": None}, "is not enabled"),
        ({"availableFields": None}, "invalid availableFields"),
        ({"availableFields": "count"}, "invalid availableFields"),
        ({"availableFields": [123]}, "invalid availableFields"),
        ({"maxPageSize": None}, "invalid maxPageSize"),
        ({"maxPageSize": "10000"}, "invalid maxPageSize"),
        ({"maxPageSize": 0}, "invalid maxPageSize"),
        ({"maxPageSize": True}, "invalid maxPageSize"),
        ({"maxDuration": -1}, "invalid maxDuration"),
        ({"notOlderThan": 1.5}, "invalid notOlderThan"),
        ({"maxDuration": 299}, "does not cover a 5-minute bucket"),
        ({"notOlderThan": 599}, "does not cover a 5-minute bucket"),
    ],
)
def test_invalid_settings_are_api_errors(rsps, api, overrides, message):
    rsps.add(responses.POST, f"{BASE_URL}/graphql", json=settings_reply(**overrides))
    with pytest.raises(script.CloudflareAPIError, match=message):
        script.fetch_settings(api, "z1")


@pytest.mark.parametrize(
    "payload",
    [
        {"data": {"viewer": {"zones": []}}},
        {"data": {"viewer": {"zones": [{}, {}]}}},
        {"data": {"viewer": {"zones": [{"settings": None}]}}},
        {"data": {"viewer": {"zones": [{"settings": {}}]}}},
        {"data": {"viewer": None}},
    ],
)
def test_settings_without_the_zone_are_api_errors(rsps, api, payload):
    rsps.add(responses.POST, f"{BASE_URL}/graphql", json=payload)
    with pytest.raises(script.CloudflareAPIError):
        script.fetch_settings(api, "z1")


def test_plan_follows_the_zone_settings(cloudflare, api):
    zone = cloudflare.add_zone(
        "z1", "example.org", page_size=5000, max_duration=7200, not_older_than=8 * 86400
    )
    plan = plan_for(api, zone)
    assert (plan.zone_id, plan.zone_name, plan.page_size) == ("z1", "example.org", 5000)
    assert plan.max_window_buckets == 24
    # An hour of margin to the edge of what Cloudflare still answers.
    assert plan.reach_seconds == 8 * 86400 - 3600
    assert plan.supported == {"visits": 1, "requests": 1, "cache": 1, "edge": 1}
    assert plan.edge and list(plan.dimensions) == ["groups", "totals", "edge", "edge_totals"]
    assert plan.dimensions["totals"] == plan.dimensions["edge_totals"] == ()


@pytest.mark.parametrize(
    ("configured", "expected"), [(0, 5000), (100, 100), (5000, 5000), (20000, 5000)]
)
def test_configured_page_size_never_exceeds_the_plans(cloudflare, api, configured, expected):
    zone = cloudflare.add_zone("z1", "example.org", page_size=5000)
    assert plan_for(api, zone, page_size=configured).page_size == expected


@pytest.mark.parametrize(
    ("days", "expected"),
    [(0, 31 * 86400 - 3600), (1, 86400), (31, 31 * 86400 - 3600), (90, 31 * 86400 - 3600)],
)
def test_backfill_days_only_shorten_the_history(cloudflare, api, days, expected):
    zone = cloudflare.add_zone("z1", "example.org", not_older_than=31 * 86400)
    plan = plan_for(api, zone, backfill_days=days)
    assert plan.history_seconds == expected
    # What can be queried, the proof ranges included, stays the dataset's reach.
    assert plan.reach_seconds == 31 * 86400 - 3600
    assert plan.long_proof_seconds == script.LONG_PROOF_SECONDS


def test_window_never_exceeds_the_built_in_maximum(cloudflare, api):
    zone = cloudflare.add_zone("z1", "example.org", max_duration=30 * 86400)
    assert plan_for(api, zone).max_window_buckets == script.MAX_WINDOW_BUCKETS


@pytest.mark.parametrize(
    "missing",
    ["count", "sum_visits", "avg_sampleInterval", "dimensions_datetimeFiveMinutes"],
)
def test_plan_without_a_required_field_fails_the_zone(cloudflare, api, missing):
    zone = cloudflare.add_zone(
        "z1", "example.org", fields=[field for field in PRO_FIELDS if field != missing]
    )
    with pytest.raises(script.CloudflareAPIError, match=f"fields not available.*{missing}"):
        plan_for(api, zone)


def test_free_plan_reads_only_what_it_offers(cloudflare, api):
    zone = cloudflare.add_zone("z1", "example.org", fields=FREE_FIELDS)
    plan = plan_for(api, zone)
    assert plan.edge
    assert "clientRequestQuery" not in plan.available
    assert set(plan.dimensions["groups"]) == {
        "clientRequestHTTPHost",
        "clientRequestPath",
        "clientCountryName",
        "userAgentBrowser",
        "userAgentOS",
        "clientRequestHTTPMethodName",
        "cacheStatus",
        "originResponseStatus",
    }


# ------------------------------------------------------ query building -----


def test_query_uses_variables_and_orders_by_bucket():
    query = script.build_query({"groups": ("clientRequestPath",), "edge_totals": ()})
    # Totals have their own range and page size; only used variables are declared.
    assert (
        "query Buckets($zoneTag: string!, $since: Time!, $until: Time!, $limit: uint64!, "
        "$edge_totalsSince: Time!, $edge_totalsUntil: Time!, $totalsLimit: uint64!)" in query
    )
    assert "zones(filter: {zoneTag: $zoneTag})" in query
    assert query.count("httpRequestsAdaptiveGroups(") == 2
    assert query.count("limit: $limit") == 1 and query.count("limit: $totalsLimit") == 1
    assert query.count("orderBy: [datetimeFiveMinutes_ASC]") == 2
    assert "filter: {datetime_geq: $since, datetime_lt: $until}" in query
    assert (
        "filter: {datetime_geq: $edge_totalsSince, datetime_lt: $edge_totalsUntil, "
        'requestSource: "eyeball"}' in query
    )
    assert "groups: httpRequestsAdaptiveGroups" in query
    assert (
        "{ count sum { visits } avg { sampleInterval } "
        "dimensions { datetimeFiveMinutes clientRequestPath } }" in query
    )
    assert "{ count avg { sampleInterval } dimensions { datetimeFiveMinutes } }" in query
    assert "$after" not in query


def test_query_declares_only_the_variables_it_uses():
    totals = script.build_query({"totals": (), "edge_totals": ()})
    assert (
        "query Buckets($zoneTag: string!, $totalsSince: Time!, $totalsUntil: Time!, "
        "$totalsLimit: uint64!, $edge_totalsSince: Time!, $edge_totalsUntil: Time!)" in totals
    )
    groups = script.build_query({"groups": (), "edge": ("clientRequestPath",)})
    assert (
        "query Buckets($zoneTag: string!, $since: Time!, $until: Time!, $limit: uint64!)" in groups
    )


def test_paging_query_orders_by_the_dimensions_and_keeps_the_filters():
    query = script.build_query(
        {"edge": ("clientRequestHTTPHost", "edgeResponseStatus")}, paging=True
    )
    assert "$after: ZoneHttpRequestsAdaptiveGroupsFilter_InputObject!" in query
    assert (
        'filter: {datetime_geq: $since, datetime_lt: $until, requestSource: "eyeball", '
        "AND: [$after]}" in query
    )
    assert "orderBy: [clientRequestHTTPHost_ASC, edgeResponseStatus_ASC]" in query
    assert "dimensions { datetimeFiveMinutes clientRequestHTTPHost edgeResponseStatus }" in query


def test_no_value_is_interpolated_into_queries(cloudflare, api, rsps):
    zone = cloudflare.add_zone("z1", "example.org")
    script.read_window(api, plan_for(api, zone), T0, T0 + BUCKET)
    sent = json.loads(rsps.calls[-1].request.body)
    window = {"Since": "2027-01-15T08:00:00Z", "Until": "2027-01-15T08:05:00Z"}
    assert sent["variables"] == {
        "zoneTag": "z1",
        "since": window["Since"],
        "until": window["Until"],
        "limit": 10000,
        **{
            f"{alias}{bound}": window[bound]
            for alias in ("totals", "edge_totals")
            for bound in window
        },
        "totalsLimit": 10000,
    }
    assert "z1" not in sent["query"] and "2027" not in sent["query"]


def test_missing_zone_in_an_analytics_answer_is_an_api_error(cloudflare, api):
    zone = cloudflare.add_zone("z1", "example.org")
    plan = plan_for(api, zone)
    cloudflare.replies.append(json_response(200, {"data": {"viewer": {"zones": [{}, {}]}}}))
    with pytest.raises(script.CloudflareAPIError, match="no data returned for zone z1"):
        script.read_window(api, plan, T0, T0 + BUCKET)


# --------------------------------------------------------------- main ------


@pytest.fixture
def quiet_logging(monkeypatch):
    monkeypatch.setattr(script, "configure_logging", lambda: None)


def test_main_exits_with_error_on_invalid_config(monkeypatch, quiet_logging, caplog):
    monkeypatch.delenv("CF_EXPORTER_API_TOKEN", raising=False)
    assert script.main() == 1
    assert "CF_EXPORTER_API_TOKEN must be set." in caplog.text


def test_main_wires_everything_together(monkeypatch, quiet_logging, tmp_path, caplog):
    path = tmp_path / "state" / "cf.sqlite3"
    monkeypatch.setenv("CF_EXPORTER_API_TOKEN", "abc\n")
    monkeypatch.setenv("CF_EXPORTER_METRICS_PORT", "9123")
    monkeypatch.setenv("CF_EXPORTER_DB_PATH", str(path))
    monkeypatch.setenv("CF_EXPORTER_BACKFILL_DAYS", "2")
    monkeypatch.setenv("CF_EXPORTER_PAGE_SIZE", "777")
    monkeypatch.setenv("CF_EXPORTER_DISABLED_LABELS", '{"cf_requests_5m": ["query"]}')
    # Settings of 1.x that no longer exist are reported, not silently ignored.
    monkeypatch.setenv("CF_EXPORTER_SCRAPE_INTERVAL", "300")
    monkeypatch.setenv("CF_EXPORTER_GROUP_LIMIT", "")
    registry = CollectorRegistry()
    handlers, servers, runs = {}, [], []
    monkeypatch.setattr(script, "REGISTRY", registry)
    monkeypatch.setattr(
        script.signal, "signal", lambda sig, handler: handlers.update({sig: handler})
    )
    monkeypatch.setattr(script, "start_http_server", lambda *args: servers.append(args))
    # While the loop runs, the registered collectors serve the health metrics.
    monkeypatch.setattr(
        script.Exporter,
        "run",
        lambda self, stop: runs.append((self, stop, generate_latest(registry).decode())),
    )
    caplog.set_level(logging.INFO)

    assert script.main() == 0

    assert set(handlers) == {signal.SIGTERM, signal.SIGINT}
    exporter, stop, text = runs[0]
    assert servers == [(9123, exporter, exporter.storage)]
    assert not stop.is_set()
    assert exporter.api.session.headers["Authorization"] == "Bearer abc"
    assert (exporter.backfill_days, exporter.api.page_size) == (2, 777)
    assert "query" not in exporter.schema.labels("cf_requests_5m")
    assert exporter.storage.persistent and path.exists()
    assert "CF_EXPORTER_SCRAPE_INTERVAL was removed in 2.0.0 and is ignored" in caplog.text
    assert "CF_EXPORTER_GROUP_LIMIT" not in caplog.text
    assert f"storage {path}" in caplog.text
    assert "cf_exporter_storage_persistent 1.0" in text
    assert 'cf_exporter_api_errors_total{api="rest",error="rate_limited"} 0.0' in text
    assert "cf_requests_5m" not in text
    # The database is closed when the loop ends, also by a signal.
    assert not exporter.storage.usable()
    exporter.api.session.close()


def test_database_is_closed_when_a_signal_ends_the_loop(monkeypatch, quiet_logging, tmp_path):
    monkeypatch.setenv("CF_EXPORTER_API_TOKEN", "abc")
    monkeypatch.setenv("CF_EXPORTER_DB_PATH", str(tmp_path / "cf.sqlite3"))
    storages = []
    monkeypatch.setattr(script, "REGISTRY", CollectorRegistry())
    monkeypatch.setattr(script.signal, "signal", lambda sig, handler: None)
    monkeypatch.setattr(script, "start_http_server", lambda *args: storages.append(args[2]))

    def interrupted(self, stop):
        script._handle_signal(signal.SIGTERM, None)

    monkeypatch.setattr(script.Exporter, "run", interrupted)
    with pytest.raises(SystemExit) as exit_:
        script.main()
    assert exit_.value.code == 0
    assert not storages[0].usable()


def test_signal_handler_exits_cleanly(caplog):
    caplog.set_level(logging.INFO)
    with pytest.raises(SystemExit) as exc:
        script._handle_signal(signal.SIGTERM, None)
    assert exc.value.code == 0
    assert "Signal SIGTERM received, shutting down." in caplog.text


def test_module_import_has_no_side_effects():
    """Signal handlers and metrics must only be installed by main()."""
    assert signal.getsignal(signal.SIGTERM) is not script._handle_signal
    assert "cf_exporter" not in generate_latest(REGISTRY).decode()
