# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""The live validation tool and the image smoke checks, tested offline: both
judge real exports, so their own verdicts must be right."""

import re

import image_smoke
import profile_live
import pytest
from conftest import BUCKET, PRO_FIELDS, T0, event, first_calls, request, run
from prometheus_client import CollectorRegistry

import script

HOUR = 3600
NOW = T0 + 1000  # the hour ending at T0 has settled


# ----------------------------------------------------- live validation -----


def test_default_window_is_the_last_settled_hours():
    assert profile_live.parse_args([], NOW) == (T0 - 2 * HOUR, T0)
    assert profile_live.parse_args(["--hours", "5"], NOW) == (T0 - 5 * HOUR, T0)
    # Not before the newest hour has settled.
    assert profile_live.parse_args([], T0 + profile_live.SETTLED_SECONDS - 1) == (
        T0 - 3 * HOUR,
        T0 - HOUR,
    )
    assert profile_live.parse_args(["--end", "2027-01-15T06:00:00Z", "--hours", "1"], NOW) == (
        T0 - 3 * HOUR,
        T0 - 2 * HOUR,
    )
    assert profile_live.parse_args(["--end", "2027-01-15T09:00:00+01:00"], NOW)[1] == T0


@pytest.mark.parametrize(
    "argv",
    [
        ["--hours", "0"],
        ["--hours", "-2"],
        ["--hours", "two"],
        ["--end", "yesterday"],
        ["--end", "2027-01-15T08:00:00"],
        ["--end", "2027-01-15T07:30:00Z"],
        ["--end", "2027-01-15T09:00:00Z"],  # not over yet
    ],
)
def test_invalid_arguments_are_rejected(argv, capsys):
    with pytest.raises(SystemExit) as error:
        profile_live.parse_args(argv, NOW)
    assert error.value.code == 2
    assert "error" in capsys.readouterr().err


@pytest.fixture
def traffic(cloudflare):
    cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 - 3000, count=7, visits=2),
        event(T0 - 600, count=5, visits=1, clientRequestPath="/app"),
        event(T0 - 500, count=40, source="edgeWorkerFetch"),
        event(T0 - 2 * HOUR - 5, count=1000),  # before the window
        event(T0 + 5, count=1000),  # after it
        not_older_than=31 * 86400,
    )
    cloudflare.add_zone("z2", "quiet.example", not_older_than=31 * 86400)
    cloudflare.add_zone(
        "z3",
        "free.example",
        event(T0 - 100, count=30, visits=3),
        fields=[f for f in PRO_FIELDS if f != "dimensions_requestSource"],
        not_older_than=31 * 86400,
    )
    return cloudflare


def validate(api, schema=None):
    return profile_live.validate(api, schema or script.Schema(), T0 - 2 * HOUR, T0, NOW)


def test_matching_zones_are_ok(traffic, api):
    report = validate(api)
    zones = report["zones"]
    assert {name: zone["verdict"] for name, zone in zones.items()} == {
        "example.org": "ok",
        "quiet.example": "ok",
        "free.example": "ok",
    }
    assert zones["example.org"]["reference"] == (52, 3, 12)
    assert zones["example.org"]["sums"]["cf_edge_requests_5m"] == 12
    # Without the request source the plan has no client total to compare.
    assert zones["free.example"]["reference"] == (30, 3, None)
    assert zones["quiet.example"]["reference"] == (0, 0, 0)
    assert report["results"] == {"accepted": 3 * 24}
    assert report["samples"]["cf_zone_requests_5m"] == 3 * 24
    assert report["exposition_bytes"] > 0 and report["series"] > 0
    assert profile_live.exit_code(report) == 0
    # The reference is hourly and reaches a week back, to carry its own proof.
    assert {call["zone"] for call in traffic.reference_calls} == {"z1", "z2", "z3"}
    week = profile_live.REFERENCE_PROOF_SECONDS
    assert {(call["since"], call["until"]) for call in traffic.reference_calls} == {
        (T0 - 2 * HOUR - week, T0)
    }
    # A zone without any group is asked every time before its silence is believed.
    quiet = [call for call in traffic.reference_calls if call["zone"] == "z2"]
    assert len(quiet) == profile_live.ROUNDS


def test_wrong_export_is_a_mismatch(traffic, api, monkeypatch):
    original = script.evaluate

    def lossy(plan, schema, bucket_end, data, *proof):
        result = original(plan, schema, bucket_end, data, *proof)
        kept = tuple(s for s in result.samples if 'path="/app"' not in s[1])
        return script.BucketResult(
            bucket_end, kept, result.signature, result.problem, result.reason
        )

    monkeypatch.setattr(script, "evaluate", lossy)
    report = validate(api)
    verdict = report["zones"]["example.org"]["verdict"]
    assert verdict.startswith("MISMATCH")
    for family in ("cf_requests_5m 47 != 52", "cf_visits_5m 2 != 3", "cf_edge_requests_5m 7 != 12"):
        assert family in verdict
    assert "cf_zone_requests_5m" not in verdict
    assert report["zones"]["quiet.example"]["verdict"] == "ok"
    assert profile_live.exit_code(report) == 1


def test_reduced_reference_is_read_again_with_another_page_size(traffic, api):
    traffic.reference_reduced = 2
    report = validate(api)
    assert report["zones"]["example.org"]["verdict"] == "ok"
    limits = [call["limit"] for call in traffic.reference_calls if call["zone"] == "z1"]
    first = profile_live.REFERENCE_PAGE_SIZE - profile_live.ROUNDS
    assert limits == [first, first + 1, first + 2]


@pytest.mark.parametrize(
    ("options", "hours"),
    [({"max_duration": 2 * 86400}, 48), ({"page_size": 100}, 100 - profile_live.ROUNDS - 1)],
)
def test_reference_fits_the_plans_longest_range_and_page(cloudflare, api, options, hours):
    cloudflare.add_zone(
        "z1", "example.org", event(T0 - 100, count=3), not_older_than=31 * 86400, **options
    )
    report = validate(api)
    assert report["zones"]["example.org"]["verdict"] == "ok"
    assert {(call["since"], call["until"]) for call in cloudflare.reference_calls} == {
        (T0 - hours * HOUR, T0)
    }


def test_reference_that_stays_reduced_is_inconclusive(traffic, api):
    traffic.reference_reduced = 10**6
    report = validate(api)
    for zone in ("example.org", "free.example"):
        assert report["zones"][zone]["verdict"].startswith("inconclusive")
    # A zone without traffic has nothing that could be reduced.
    assert report["zones"]["quiet.example"]["verdict"] == "ok"
    assert profile_live.exit_code(report) == 0
    del report["zones"]["quiet.example"]
    assert profile_live.exit_code(report) == 2


def test_reference_without_groups_is_not_believed_while_another_answer_has_some(cloudflare, api):
    """A reduced answer has no group for a few requests: it looks like silence."""
    cloudflare.add_zone(
        "z1", "example.org", event(T0 - 100, count=3, visits=1), not_older_than=31 * 86400
    )
    cloudflare.reference_reduced = 2
    report = validate(api)
    assert report["zones"]["example.org"]["reference"] == (3, 1, 3)
    assert report["zones"]["example.org"]["verdict"] == "ok"
    assert len(cloudflare.reference_calls) == 3


def test_rejected_buckets_are_read_again_and_reported_if_they_stay(traffic, api):
    def lose_group(call, alias, groups):
        return groups[:-1] if alias == "groups" and call["zone"] == "z1" else groups

    traffic.transforms.append(first_calls(4, lose_group))
    report = validate(api)
    assert report["zones"]["example.org"]["verdict"] == "ok"
    assert report["results"]["inconsistent"] >= 1

    traffic.transforms[:] = [lose_group]
    report = validate(api)
    assert report["zones"]["example.org"]["verdict"].startswith("incomplete: ")
    assert profile_live.exit_code(report) == 0


def test_zone_without_settings_fails_the_validation(traffic, api):
    traffic.zones["z2"].settings_error = "not authorized"
    report = validate(api)
    assert report["zones_without_plan"] == ["quiet.example"]
    assert profile_live.exit_code(report) == 1


def test_validation_honours_disabled_labels(traffic, api):
    schema = script.parse_disabled_labels('{"cf_requests_5m": ["query", "path"]}')
    report = validate(api, schema)
    assert report["zones"]["example.org"]["verdict"] == "ok"


def test_oracle_is_independent_of_the_production_queries():
    query = profile_live.reference_query(clients=True)
    assert script.EYEBALL_FILTER not in query and "$source" in query
    # Hours, not the exporter's buckets.
    assert "datetimeHour" in query and script.BUCKET_DIMENSION not in query
    assert "clients:" not in profile_live.reference_query(clients=False)


# ------------------------------------------------- image smoke checks ------


def test_smoke_driver_mirrors_the_exporter():
    assert image_smoke.METRIC_LABELS == script.METRIC_LABELS
    assert image_smoke.READ_RESULTS == script.READ_RESULTS
    assert (image_smoke.API_NAMES, image_smoke.API_ERRORS) == (script.APIS, script.API_ERRORS)
    assert image_smoke.BUCKET_SECONDS == script.BUCKET_SECONDS
    assert image_smoke.UINT16_MAX == script.UINT16_MAX


@pytest.fixture
def exported(cloudflare, exporter, storage, clock):
    cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 10, count=4, visits=1),
        event(T0 + 20, count=2, edgeResponseStatus=503, originResponseStatus=0),
        event(T0 + 30, count=9, source="edgeWorkerFetch"),
        event(T0 - 10, count=3, visits=3, clientRequestPath='/quote"d'),
    )
    run(exporter)
    registry = CollectorRegistry()
    registry.register(script.HealthCollector(exporter, storage))
    registry.register(exporter.api.stats)
    app = script.make_app(exporter, storage, registry)
    return request(app, "/analytics")[2], request(app, "/metrics")[2], clock.now


def test_real_exposition_passes_the_smoke_checks(exported):
    body, metrics, now = exported
    counts = image_smoke.checked_analytics(body, image_smoke.expected_labels(), now)
    assert counts == {
        "cf_visits_5m": 2,
        # Subrequests share the labels of the client requests they merge with.
        "cf_requests_5m": 3,
        "cf_cache_requests_5m": 3,
        "cf_edge_requests_5m": 3,
        "cf_zone_requests_5m": 23,
        "cf_zone_visits_5m": 23,
    }
    health = image_smoke.checked_health(metrics)
    assert health["success"] == {"example.org": 1}
    assert health["newest"] == {"example.org": T0 + BUCKET}
    assert image_smoke.collected(health, now)
    assert not image_smoke.collected(health, now + image_smoke.RECENT_SECONDS + BUCKET + 1)
    assert sum(image_smoke.checked_api_counters(metrics).values()) == 0
    assert image_smoke.single_value(metrics, "cf_exporter_storage_persistent") == 0


def test_disabled_labels_are_expected_by_the_smoke_checks(
    cloudflare, make_exporter, storage, clock
):
    disabled = (
        '{"cf_requests_5m": ["query", "path"], "cf_edge_requests_5m": ["edge_response_status"]}'
    )
    cloudflare.add_zone(
        "z1",
        "example.org",
        event(T0 + 10, count=4, clientRequestPath="/a"),
        event(T0 + 20, count=2, clientRequestPath="/b", edgeResponseStatus=404),
    )
    exporter = make_exporter(script.parse_disabled_labels(disabled))
    run(exporter)
    body = request(script.make_app(exporter, storage, CollectorRegistry()), "/analytics")[2]
    counts = image_smoke.checked_analytics(body, image_smoke.expected_labels(disabled), clock.now)
    assert counts["cf_requests_5m"] == 1 and counts["cf_edge_requests_5m"] == 2
    with pytest.raises(ValueError, match="unexpected cf_requests_5m labels"):
        image_smoke.checked_analytics(body, image_smoke.expected_labels(), clock.now)


def replaced(body, old, new, count=1):
    assert body.count(old) >= count, old
    return body.replace(old, new, count)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (
            lambda b: replaced(b, f" 4 {(T0 + BUCKET) * 1000}", " 4"),
            "without a bucket-end timestamp",
        ),
        (
            lambda b: replaced(b, f" 4 {(T0 + BUCKET) * 1000}", f" 4 {(T0 + BUCKET) * 1000 + 1}"),
            "without a bucket-end timestamp",
        ),
        (
            lambda b: replaced(b, f" 4 {(T0 + BUCKET) * 1000}", f" 4 {(T0 + 10 * BUCKET) * 1000}"),
            "outside the possible time range",
        ),
        (
            lambda b: replaced(b, f" 4 {(T0 + BUCKET) * 1000}", f" 4 {(T0 - 40 * 86400) * 1000}"),
            "outside the possible time range",
        ),
        (
            lambda b: replaced(b, f" 13 {(T0 + BUCKET) * 1000}", f" 14 {(T0 + BUCKET) * 1000}"),
            "does not add up",
        ),
        (
            lambda b: replaced(b, f" 4 {(T0 + BUCKET) * 1000}", f" -4 {(T0 + BUCKET) * 1000}"),
            "invalid",
        ),
        (
            lambda b: replaced(b, f" 4 {(T0 + BUCKET) * 1000}", f" 0 {(T0 + BUCKET) * 1000}"),
            "invalid",
        ),
        (
            lambda b: replaced(b, f" 4 {(T0 + BUCKET) * 1000}", f" NaN {(T0 + BUCKET) * 1000}"),
            "invalid",
        ),
        (
            lambda b: replaced(b, 'cf_visits_5m{zone_name="example.org",', "cf_visits_5m{"),
            "unexpected cf_visits_5m labels",
        ),
        (
            lambda b: replaced(b, 'edge_response_status="503"', 'edge_response_status="abc"'),
            "invalid status",
        ),
        (
            lambda b: replaced(b, 'edge_response_status="503"', 'edge_response_status="70000"'),
            "invalid status",
        ),
        (
            lambda b: replaced(b, "# TYPE cf_requests_5m gauge", "# TYPE cf_requests_5m counter"),
            "not declared a gauge",
        ),
        (
            lambda b: (
                b + f'cf_zone_requests_5m{{zone_name="example.org"}} 15 {(T0 + BUCKET) * 1000}\n'
            ),
            "duplicate or unordered",
        ),
        (
            lambda b: (
                b
                + f'cf_zone_requests_5m{{zone_name="example.org"}} 0 {(T0 - 30 * BUCKET) * 1000}\n'
            ),
            "duplicate or unordered",
        ),
        (
            lambda b: b + f'cf_zone_requests_5m{{zone_name="other.example"}} 0 {T0 * 1000}\n',
            "only one of its zone totals",
        ),
        (
            lambda b: (
                b + 'cf_edge_requests_5m{zone_name="other.example",host_name="h",method_name="GET",'
                'path="/",edge_response_status="200",origin_response_status="200"} 1 '
                f"{T0 * 1000}\n"
            ),
            "bucket without zone totals",
        ),
    ],
)
def test_smoke_checks_reject_broken_expositions(exported, change, message):
    body, _, now = exported
    with pytest.raises(ValueError, match=message):
        image_smoke.checked_analytics(change(body), image_smoke.expected_labels(), now)


def test_smoke_checks_reject_more_client_requests_than_requests(exported):
    body, _, now = exported
    line = (
        'cf_edge_requests_5m{zone_name="example.org",host_name="x",method_name="GET",path="/",'
        f'edge_response_status="200",origin_response_status="200"}} 100 {(T0 + BUCKET) * 1000}\n'
    )
    with pytest.raises(ValueError, match="more client requests than requests"):
        image_smoke.checked_analytics(body + line, image_smoke.expected_labels(), now)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (
            lambda m: replaced(m, "# TYPE cf_exporter_pending_buckets gauge", ""),
            "missing cf_exporter_pending_buckets",
        ),
        (
            lambda m: replaced(m, "# TYPE cf_exporter_storage_resets_total counter", ""),
            "missing cf_exporter_storage_resets_total",
        ),
        (lambda m: replaced(m, 'result="expired"', 'result="other"'), "differ from the fixed set"),
        (
            lambda m: replaced(m, 'result="accepted"} 23.0', 'result="accepted"} 2.5'),
            "invalid bucket read counter",
        ),
        (
            lambda m: replaced(
                m,
                'cf_exporter_zone_success{zone_name="example.org"} 1.0',
                'cf_exporter_zone_success{zone_name="example.org"} 2.0',
            ),
            "neither 0 nor 1",
        ),
        (
            lambda m: re.sub(
                r"(cf_exporter_newest_bucket_end_seconds\{[^}]*\}) \S+", rf"\1 {T0 + 1}.0", m
            ),
            "off the bucket grid",
        ),
        (
            lambda m: re.sub(
                r'(cf_exporter_dataset_available\{[^}]*dataset="cache"[^}]*\}) 1\.0', r"\1 0.0", m
            ),
            "always collected dataset",
        ),
    ],
)
def test_smoke_checks_reject_broken_health(exported, change, message):
    _, metrics, _ = exported
    with pytest.raises(ValueError, match=message):
        image_smoke.checked_health(change(metrics))


def test_zone_that_failed_or_has_no_recent_bucket_is_not_collected(exported):
    _, metrics, now = exported
    health = image_smoke.checked_health(metrics)
    assert not image_smoke.collected({**health, "success": {"example.org": 0}}, now)
    assert not image_smoke.collected({**health, "newest": {}}, now)
    assert not image_smoke.collected({**health, "success": {}}, now)
    with pytest.raises(ValueError, match="expected one"):
        image_smoke.single_value(metrics, "cf_exporter_no_such_metric")


def test_expected_labels():
    assert image_smoke.expected_labels()["cf_zone_visits_5m"] == {"zone_name"}
    labels = image_smoke.expected_labels('{"cf_visits_5m": ["path", "user_agent_os"]}')
    assert labels["cf_visits_5m"] == set(script.METRIC_LABELS["cf_visits_5m"]) - {
        "path",
        "user_agent_os",
    }
    assert labels["cf_requests_5m"] == set(script.METRIC_LABELS["cf_requests_5m"])
