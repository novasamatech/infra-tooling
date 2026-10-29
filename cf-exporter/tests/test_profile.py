# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Profiling must not report a successful check without collecting valid data."""

import json
from datetime import timedelta
from weakref import ref

import profile_live
import pytest
from conftest import DAY, requests_group, visits_group

import script


@pytest.mark.parametrize(
    "arguments",
    [
        ["--cycles", "0"],
        ["--cycles", "-1"],
        ["--day", "2026-10-25T12:00:00"],
        ["--day", "2026-10-26"],
        ["--day", "2026-10-27"],
    ],
)
def test_invalid_profile_arguments_fail_before_api_creation(monkeypatch, clock, arguments):
    calls = []
    monkeypatch.setattr(profile_live, "datetime", clock)
    monkeypatch.setattr(script, "parse_env", lambda: calls.append(True))
    monkeypatch.setattr("sys.argv", ["profile_live.py", *arguments])
    with pytest.raises(SystemExit) as error:
        profile_live.main()
    assert error.value.code == 2
    assert not calls


@pytest.mark.parametrize("historical, expected_status", [(True, 1), (False, 0)])
def test_until_checks_past_day_sums_as_strictly_as_day(
    cloudflare, api, clock, monkeypatch, capsys, historical, expected_status
):
    cloudflare.add_zone(
        "z",
        "example.org",
        visits=[visits_group(2)],
        requests=[requests_group(1)],
        cache=[requests_group(2)],
        edge=[requests_group(2, edgeResponseStatus=200)],
        totals=[{"count": 2, "sum": {"visits": 2}}],
    )
    until = DAY + timedelta(hours=1) - timedelta(days=int(historical))
    monkeypatch.setattr(profile_live, "datetime", clock)
    monkeypatch.setattr(script, "parse_env", lambda: script.Config("dummy", 5, 300, 8000))
    monkeypatch.setattr(script, "create_api", lambda config: api)
    monkeypatch.setattr(profile_live, "request_sources", lambda *args: (2, {"eyeball": 2}))
    monkeypatch.setattr(
        "sys.argv", ["profile_live.py", "--until", script.rfc3339(until), "--cycles", "1"]
    )
    assert profile_live.main() == expected_status
    report = json.loads(capsys.readouterr().out)
    assert report["mismatched_datasets"] == ["requests"]
    assert report["strict_totals"] is historical
    assert report["window"] == [script.rfc3339(until.replace(hour=0)), script.rfc3339(until)]


@pytest.mark.parametrize(
    "references, expected_status",
    [
        # Only subrequests in one zone: the other zone still has client requests.
        ({"a": (0, {"edgeWorkerFetch": 1}), "b": (1, {"eyeball": 1})}, 0),
        # A renamed source value: neither the filter nor its reference finds anything.
        ({"a": (0, {"renamed": 1}), "b": (0, {"renamed": 1})}, 1),
    ],
)
def test_profile_requires_the_source_value_in_some_zone(
    cloudflare, api, clock, monkeypatch, capsys, references, expected_status
):
    for zone in references:
        edge = [requests_group(1, edgeResponseStatus=200)] if references[zone][0] else []
        cloudflare.add_zone(
            zone,
            f"{zone}.example",
            visits=[visits_group(1)],
            requests=[requests_group(1)],
            cache=[requests_group(1)],
            edge=edge,
        )
    monkeypatch.setattr(script, "parse_env", lambda: script.Config("dummy", 5, 300, 8000))
    monkeypatch.setattr(script, "create_api", lambda config: api)
    monkeypatch.setattr(
        profile_live, "request_sources", lambda graphql, zone_id, *args: references[zone_id]
    )
    monkeypatch.setattr("sys.argv", ["profile_live.py", "--cycles", "1"])
    assert profile_live.main() == expected_status
    report = json.loads(capsys.readouterr().out)
    assert report["totals_match"] is True
    assert report["source_filter_valid"] is (expected_status == 0)


def test_profile_does_not_retain_zone_series_while_rendering(
    cloudflare, api, clock, monkeypatch, capsys
):
    for zone in ("a", "b"):
        cloudflare.add_zone(
            zone,
            f"{zone}.example",
            visits=[visits_group(1)],
            requests=[requests_group(1)],
            cache=[requests_group(1)],
            edge=[requests_group(1, edgeResponseStatus=200)],
        )
    original_collect, original_render = script.collect_zone, profile_live.generate_latest
    references = []

    def recorded(*args, **kwargs):
        result = original_collect(*args, **kwargs)
        references.append(ref(result))
        return result

    def rendered(registry):
        assert len(references) == 2
        assert all(reference() is None for reference in references)
        return original_render(registry)

    monkeypatch.setattr(script, "collect_zone", recorded)
    monkeypatch.setattr(profile_live, "generate_latest", rendered)
    monkeypatch.setattr(script, "parse_env", lambda: script.Config("dummy", 5, 300, 8000))
    monkeypatch.setattr(script, "create_api", lambda config: api)
    monkeypatch.setattr(profile_live, "request_sources", lambda *args: (1, {"eyeball": 1}))
    monkeypatch.setattr("sys.argv", ["profile_live.py", "--cycles", "1"])
    assert profile_live.main() == 0
    assert json.loads(capsys.readouterr().out)["totals_match"] is True
