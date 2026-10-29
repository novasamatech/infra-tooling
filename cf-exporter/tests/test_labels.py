# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Per-metric label configuration, checked in the queries and the real exposition."""

import json
import re
from datetime import timedelta
from pathlib import Path

import image_smoke
import pytest
from conftest import (
    ALL_FIELDS,
    BASE_URL,
    DAY,
    FREE_FIELDS,
    aggregating_responder,
    event,
    exposition,
    publish,
    reads,
    requests_group,
    samples,
    selections,
    visits_group,
)
from image_smoke import checked_analytics_labels, checked_edge_family, expected_labels

import script

TOKEN = {"CF_EXPORTER_API_TOKEN": "dummy-credential"}
EVENTS = [
    event(0, 3, clientRequestPath="/a", clientRequestQuery="?x=1", clientRequestReferer=""),
    event(10, 2, clientRequestPath="/a", clientRequestQuery="?x=2", clientRequestReferer="r"),
    event(20, 4, clientRequestPath="/b", cacheStatus="miss", edgeResponseStatus=404),
]


def schema(**disabled):
    return script.Schema({metric: frozenset(labels) for metric, labels in disabled.items()})


def families(collector):
    return {family.name: family for family in collector.collect()}


def dimensions_queried(call):
    return {kind: set(dimensions) for kind, _, dimensions in selections(call["query"])}


# ---------------------------------------------------------- configuration ---


@pytest.mark.parametrize("raw", [None, "", "  \n", "{}", '{"cf_requests": []}'])
def test_unset_or_empty_configuration_disables_nothing(raw):
    environ = TOKEN if raw is None else {**TOKEN, "CF_EXPORTER_DISABLED_LABELS": raw}
    config = script.parse_env(environ)
    assert config.schema == script.Schema()
    assert {name: config.schema.labels(name) for name in script.METRIC_LABELS} == (
        script.METRIC_LABELS
    )


def test_configuration_disables_labels_per_metric_in_catalog_order():
    config = script.parse_env(
        {
            **TOKEN,
            "CF_EXPORTER_DISABLED_LABELS": json.dumps(
                {
                    "cf_requests": ["client_request_referer", "query", "query"],
                    "cf_zone_visits": ["zone_name"],
                }
            ),
        }
    )
    assert config.schema.labels("cf_requests") == tuple(
        label
        for label in script.REQUESTS_LABELS
        if label not in ("query", "client_request_referer")
    )
    # The same label of another family stays exported.
    assert config.schema.labels("cf_visits") == script.VISITS_LABELS
    assert config.schema.labels("cf_zone_visits") == ()
    assert config.schema.labels("cf_zone_requests") == ("zone_name",)
    assert "clientRequestQuery" not in config.schema.dimensions("requests").values()
    assert script.describe(config) == (
        "optional limits: none; disabled labels: "
        "cf_requests: query, client_request_referer; cf_zone_visits: zone_name"
    )


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ("{", "must be valid JSON"),
        ('["query"]', "must be a JSON object"),
        ('"cf_requests"', "must be a JSON object"),
        ('{"cf_request": ["query"]}', "unknown metric 'cf_request'"),
        ('{"cf_exporter_groups": ["dataset"]}', "unknown metric 'cf_exporter_groups'"),
        ('{"cf_requests": "query"}', "cf_requests must map to a list of label names"),
        ('{"cf_requests": null}', "cf_requests must map to a list of label names"),
        ('{"cf_requests": [1]}', "cf_requests must map to a list of label names"),
        ('{"cf_requests": ["edge_response_status"]}', "has no label 'edge_response_status'"),
        ('{"cf_cache_requests": ["query"]}', "has no label 'query'"),
        ('{"cf_zone_requests": ["host_name"]}', "has no label 'host_name'"),
        ('{"cf_requests": ["query"], "cf_requests": []}', "must not repeat a key"),
    ],
)
def test_invalid_configuration_fails_startup(raw, message):
    with pytest.raises(script.ConfigError, match=re.escape(message)):
        script.parse_env({**TOKEN, "CF_EXPORTER_DISABLED_LABELS": raw})


def test_settings_reach_the_make_targets_and_the_image_check():
    settings = {
        *script.LIMITS.values(),
        script.DISABLED_LABELS,
        "CF_EXPORTER_REQUEST_TIMEOUT",
        "CF_EXPORTER_SCRAPE_INTERVAL",
        "CF_EXPORTER_LOGLEVEL",
    }
    assert set(image_smoke.FORWARDED_SETTINGS) == settings
    makefile = (Path(script.__file__).parent / "Makefile").read_text()
    block = makefile.split("CONFIG_ENV =", 1)[1].split("TOKEN_RUN =", 1)[0]
    assert set(re.findall(r"--env (CF_EXPORTER_\w+)", block)) == {
        *settings,
        "CF_EXPORTER_METRICS_PORT",
    }
    # The stdlib-only image check mirrors the label catalog.
    assert image_smoke.METRIC_LABELS == script.METRIC_LABELS
    assert expected_labels('{"cf_requests": ["query"]}')["cf_requests"] == (
        frozenset(script.REQUESTS_LABELS) - {"query"}
    )


# ------------------------------------------------- queries and exposition ---


@pytest.mark.parametrize(
    "disabled",
    [
        {"cf_requests": ["query"]},
        {"cf_requests": ["client_request_referer"]},
        {"cf_requests": ["query", "client_request_referer"]},
        {"cf_visits": ["client_request_referer"]},
    ],
)
def test_disabled_labels_leave_the_query_and_the_exposition_of_their_metric_only(
    cloudflare, api, clock, disabled
):
    cloudflare.add_zone("z", "example.org", responder=aggregating_responder(EVENTS, DAY))
    api.schema = schema(**disabled)
    collector = publish(api, clock, clock.value)
    (call,) = cloudflare.data_calls()
    queried = dimensions_queried(call)
    rendered = families(collector)
    for kind, metric in script.METRICS.items():
        labels = api.schema.labels(metric)
        assert queried[kind] == {script.DATASETS[kind][label] for label in labels[1:]}
        assert all(tuple(sample.labels) == labels for sample in rendered[metric].samples)
        assert sum(sample.value for sample in rendered[metric].samples) == 9
    if "cf_requests" not in disabled:
        assert labels_of(collector, "cf_requests") == set(script.REQUESTS_LABELS)
    body = exposition(collector)
    checked_analytics_labels(body, expected_labels(json.dumps(disabled)))
    checked_edge_family(body)


def labels_of(collector, metric):
    return {label for sample in samples(collector, metric) for label in sample.labels}


def test_api_aggregates_disabled_dimensions_before_the_page_size(cloudflare, clock):
    """Cloudflare groups by the requested dimensions before the limit, so leaving out
    a dimension avoids its slicing, rather than fetching and dropping it."""
    events = [event(second, clientRequestQuery=f"?v={second}") for second in range(0, 600, 10)]
    cloudflare.add_zone(
        "z", "example.org", max_page_size=8, responder=aggregating_responder(events, DAY)
    )
    moment = DAY + timedelta(minutes=10)
    full = script.CloudflareAPI("dummy-credential", base_url=BASE_URL)
    collector = publish(full, clock, moment)
    assert reads(cloudflare, "requests") > 8
    assert len(samples(collector, "cf_requests")) == 60
    cloudflare.graphql_calls.clear()
    reduced = script.CloudflareAPI(
        "dummy-credential", base_url=BASE_URL, schema=schema(cf_requests=["query"])
    )
    collector = publish(reduced, clock, moment)
    assert reads(cloudflare, "requests") == 1
    (sample,) = samples(collector, "cf_requests")
    assert sample.value == 60
    assert "query" not in sample.labels
    assert all(
        sample.value == 0
        for sample in samples(collector, "cf_exporter_group_limit_reached")
        if sample.labels["dataset"] == "requests"
    )


def test_metrics_without_labels_render_one_series_summing_all_zones(cloudflare, api, clock):
    cloudflare.add_zone("a", "a.example", responder=aggregating_responder(EVENTS, DAY))
    cloudflare.add_zone("b", "b.example", responder=aggregating_responder(EVENTS[:1], DAY))
    api.schema = schema(
        cf_requests=script.REQUESTS_LABELS,
        cf_cache_requests=["zone_name"],
        cf_zone_requests=["zone_name"],
    )
    collector = publish(api, clock, clock.value)
    # An aggregate-only selection, without an empty dimensions block.
    for call in cloudflare.data_calls():
        assert dimensions_queried(call)["requests"] == set()
        assert "dimensions {  }" not in call["query"]
    (requests,) = samples(collector, "cf_requests")
    assert (requests.labels, requests.value) == ({}, 12)
    (total,) = samples(collector, "cf_zone_requests")
    assert (total.labels, total.value) == ({}, 12)
    assert {s.labels["zone_name"]: s.value for s in samples(collector, "cf_zone_visits")} == {
        "a.example": 9,
        "b.example": 3,
    }
    # Identical label sets of both zones are summed; nothing is duplicated.
    cache = {s.labels["path"]: s.value for s in samples(collector, "cf_cache_requests")}
    assert cache == {"/a": 8, "/b": 4}
    assert labels_of(collector, "cf_cache_requests") == set(script.CACHE_LABELS) - {"zone_name"}
    groups = {
        (s.labels["zone_name"], s.labels["dataset"]): s.value
        for s in samples(collector, "cf_exporter_groups")
    }
    assert groups[("a.example", "cache")] == 2
    assert groups[("b.example", "cache")] == 1
    body = exposition(collector)
    checked_analytics_labels(body, expected_labels(json.dumps(serialized(api.schema))))
    checked_edge_family(body)


def serialized(schema):
    return {metric: sorted(labels) for metric, labels in schema.disabled.items()}


def test_group_cap_applies_per_zone_before_zones_are_combined(cloudflare, api, clock):
    cloudflare.add_zone(
        "a",
        "a.example",
        responder=aggregating_responder([event(0, 5, clientRequestPath="/x"), event(1, 1)], DAY),
    )
    cloudflare.add_zone(
        "b",
        "b.example",
        responder=aggregating_responder([event(0, 4, clientRequestPath="/y"), event(1, 2)], DAY),
    )
    api.group_limit = 1
    api.schema = schema(cf_cache_requests=["zone_name"])
    collector = publish(api, clock, clock.value)
    cache = {s.labels["path"]: s.value for s in samples(collector, "cf_cache_requests")}
    assert cache == {"/x": 5, "/y": 4}
    limited = {
        s.labels["zone_name"]: s.value
        for s in samples(collector, "cf_exporter_group_limit_reached")
        if s.labels["dataset"] == "cache"
    }
    assert limited == {"a.example": 1, "b.example": 1}


def test_schema_belongs_to_its_instance_and_snapshot(cloudflare, clock):
    catalog = {name: tuple(labels) for name, labels in script.METRIC_LABELS.items()}
    datasets = {kind: dict(mapping) for kind, mapping in script.DATASETS.items()}
    cloudflare.add_zone("z", "example.org", responder=aggregating_responder(EVENTS, DAY))
    full = script.CloudflareAPI("dummy-credential", base_url=BASE_URL)
    reduced = script.CloudflareAPI(
        "dummy-credential", base_url=BASE_URL, schema=schema(cf_requests=["query", "path"])
    )
    old = publish(full, clock, clock.value)
    new = publish(reduced, clock, clock.value)
    assert labels_of(old, "cf_requests") == set(script.REQUESTS_LABELS)
    assert labels_of(new, "cf_requests") == set(script.REQUESTS_LABELS) - {"query", "path"}
    assert full.schema == script.Schema()
    assert (catalog, datasets) == (script.METRIC_LABELS, script.DATASETS)
    # A scrape that started before an update keeps rendering its own snapshot.
    started = old.collect()
    next(started)
    old.update(new.snapshot)
    rest = {family.name: family for family in started}
    assert {label for s in rest["cf_requests"].samples for label in s.labels} == set(
        script.REQUESTS_LABELS
    )
    assert labels_of(old, "cf_requests") == set(script.REQUESTS_LABELS) - {"query", "path"}


def test_schema_copies_and_freezes_the_supplied_mapping(cloudflare, api, clock):
    removed = {"cf_requests": {"query"}}
    configured = script.Schema(removed)
    api.schema = configured
    cloudflare.add_zone("z", "example.org", responder=aggregating_responder(EVENTS, DAY))
    collector = publish(api, clock, clock.value)
    expected = set(script.REQUESTS_LABELS) - {"query"}
    removed["cf_requests"].add("path")
    removed["cf_visits"] = {"path"}
    assert set(configured.labels("cf_requests")) == expected
    assert labels_of(collector, "cf_requests") == expected
    assert configured.labels("cf_visits") == script.VISITS_LABELS
    with pytest.raises(TypeError):
        configured.disabled["cf_requests"] = frozenset({"path"})


@pytest.mark.parametrize("group_limit", [0, 1])
@pytest.mark.parametrize("keep_zone", [False, True])
def test_dimensionless_single_group_page_is_complete(
    cloudflare, api, clock, group_limit, keep_zone
):
    api.page_size = 1
    api.group_limit = group_limit
    api.schema = schema(
        **{
            metric: [
                label
                for label in script.METRIC_LABELS[metric]
                if not keep_zone or label != "zone_name"
            ]
            for metric in script.METRICS.values()
        }
    )
    cloudflare.add_zone("z", "example.org", responder=aggregating_responder(EVENTS, DAY))
    collector = publish(api, clock, DAY + timedelta(seconds=1))
    assert len(cloudflare.data_calls()) == 1
    assert all(s.value == 0 for s in samples(collector, "cf_exporter_group_limit_reached"))
    for metric in script.METRICS.values():
        (sample,) = samples(collector, metric)
        assert sample.value == 3
        assert sample.labels == ({"zone_name": "example.org"} if keep_zone else {})


@pytest.mark.parametrize("referer", ["", "https://ref.example/"])
def test_plan_without_a_dimension_keeps_enabled_labels_unknown_and_disabled_ones_absent(
    cloudflare, api, clock, referer
):
    cloudflare.add_zone(
        "z",
        "free.example",
        fields=FREE_FIELDS,
        visits=[visits_group(2, clientRequestPath="/")],
        requests=[requests_group(3, clientRequestPath="/")],
    )
    cloudflare.add_zone(
        "p",
        "paid.example",
        requests=[requests_group(4, clientRequestPath="/", clientRequestReferer=referer)],
    )
    api.schema = schema(cf_requests=["query"])
    collector = publish(api, clock, clock.value)
    free = [c for c in cloudflare.data_calls() if c["variables"]["zoneTag"] == "z"]
    assert all(
        "clientRequestQuery" not in c["query"] and "clientRequestReferer" not in c["query"]
        for c in free
    )
    requests = {s.labels["zone_name"]: s.labels for s in samples(collector, "cf_requests")}
    assert all("query" not in labels for labels in requests.values())
    assert requests["free.example"]["client_request_referer"] == "unknown"
    assert requests["paid.example"]["client_request_referer"] == (referer or "direct")
    (visit,) = samples(collector, "cf_visits")
    assert visit.labels["client_request_referer"] == "unknown"
    assert {s.value for s in samples(collector, "cf_exporter_dataset_available")} == {1}


# ------------------------------------------------------------------ edge ----


@pytest.mark.parametrize(
    ("disabled", "missing", "collected"),
    [
        ([], "dimensions_edgeResponseStatus", False),
        (["edge_response_status"], "dimensions_edgeResponseStatus", True),
        (["edge_response_status"], "dimensions_requestSource", False),
    ],
)
def test_edge_status_label_decides_its_required_field_but_never_the_filter(
    cloudflare, api, clock, caplog, disabled, missing, collected
):
    cloudflare.add_zone(
        "z",
        "example.org",
        fields=ALL_FIELDS - {missing},
        edge=[
            requests_group(3, clientRequestPath="/a", originResponseStatus=0),
            requests_group(2, clientRequestPath="/w", requestSource="edgeWorkerFetch"),
        ],
    )
    api.schema = schema(cf_edge_requests=disabled)
    collector = publish(api, clock, clock.value)
    available = {
        s.labels["dataset"]: s.value for s in samples(collector, "cf_exporter_dataset_available")
    }
    assert available["edge"] == int(collected)
    edge = [
        filters
        for call in cloudflare.data_calls()
        for kind, filters, _ in selections(call["query"])
        if kind == "edge"
    ]
    if collected:
        assert edge and all('requestSource: "eyeball"' in filters for filters in edge)
        (sample,) = samples(collector, "cf_edge_requests")
        assert sample.value == 3
        assert "edge_response_status" not in sample.labels
        assert sample.labels["origin_response_status"] == "0"
        checked_edge_family(
            exposition(collector), frozenset(script.EDGE_LABELS) - {"edge_response_status"}
        )
    else:
        assert not edge
        assert not samples(collector, "cf_edge_requests")
        assert f"plan does not offer {missing}" in caplog.text


@pytest.mark.parametrize("disabled", [[], ["edge_response_status"]])
def test_exported_edge_statuses_stay_strictly_validated(cloudflare, api, clock, disabled):
    cloudflare.add_zone(
        "z",
        "example.org",
        edge=[requests_group(3, edgeResponseStatus=200, originResponseStatus="200")],
    )
    api.schema = schema(cf_edge_requests=disabled)
    collector = publish(api, clock, clock.value)
    assert samples(collector, "cf_exporter_zone_scrape_success")[0].value == 0
    assert not samples(collector, "cf_edge_requests")


def test_disabled_edge_status_is_not_required_in_groups(cloudflare, api, clock):
    cloudflare.add_zone("z", "example.org", edge=[requests_group(3, originResponseStatus=0)])
    api.schema = schema(cf_edge_requests=["edge_response_status"])
    collector = publish(api, clock, clock.value)
    assert [s.value for s in samples(collector, "cf_edge_requests")] == [3]
    api.schema = script.Schema()
    collector = publish(api, clock, clock.value + timedelta(minutes=5))
    assert samples(collector, "cf_exporter_zone_scrape_success")[0].value == 0


# ----------------------------------------------------- exposition checks ----


@pytest.mark.parametrize("defect", ["extra", "missing", "duplicate", "negative"])
def test_analytics_label_check_rejects_defects(cloudflare, api, clock, defect):
    cloudflare.add_zone("z", "example.org", responder=aggregating_responder(EVENTS, DAY))
    api.schema = schema(cf_requests=["query"])
    collector = publish(api, clock, clock.value)
    body = exposition(collector)
    expected = expected_labels('{"cf_requests": ["query"]}')
    assert checked_analytics_labels(body, expected)["cf_requests"] == 3
    line = next(item for item in body.splitlines() if item.startswith("cf_requests{"))
    if defect == "extra":
        body = body.replace(line, line.replace("{", '{query="?x=1",', 1))
    elif defect == "missing":
        body = body.replace(line, re.sub(r'path="[^"]*",', "", line))
    elif defect == "duplicate":
        body += line + "\n"
    else:
        body = body.replace(line, line.rsplit(" ", 1)[0] + " -1.0")
    with pytest.raises(ValueError):
        checked_analytics_labels(body, expected)


@pytest.mark.parametrize("metric", script.METRIC_LABELS)
def test_analytics_check_rejects_missing_series_despite_valid_labels(
    cloudflare, api, clock, metric
):
    cloudflare.add_zone("z", "example.org", responder=aggregating_responder(EVENTS, DAY))
    collector = publish(api, clock, clock.value)
    body = (
        "\n".join(
            line
            for line in exposition(collector).splitlines()
            if not line.startswith((metric + "{", metric + " "))
        )
        + "\n"
    )
    with pytest.raises(ValueError, match=metric):
        checked_analytics_labels(body, expected_labels())


@pytest.mark.parametrize("value", ["tab\there", "carriage\rreturn", 'quote"slash\\newline\n'])
def test_smoke_accepts_label_values_rendered_by_the_prometheus_client(
    cloudflare, api, clock, value
):
    cloudflare.add_zone(
        "z",
        "example.org",
        responder=aggregating_responder([event(0, clientRequestQuery=value)], DAY),
    )
    collector = publish(api, clock, clock.value)
    (sample,) = samples(collector, "cf_requests")
    assert sample.labels["query"] == value
    assert checked_analytics_labels(exposition(collector), expected_labels())["cf_requests"] == 1


def test_edge_check_without_zone_name_bounds_combined_series(cloudflare, api, clock):
    cloudflare.add_zone("a", "a.example", responder=aggregating_responder(EVENTS, DAY))
    cloudflare.add_zone("b", "b.example", responder=aggregating_responder(EVENTS, DAY))
    api.schema = schema(cf_edge_requests=["zone_name"])
    collector = publish(api, clock, clock.value)
    labels = frozenset(script.EDGE_LABELS) - {"zone_name"}
    body = exposition(collector)
    # Both zones have the same two label sets, combined into two series.
    assert len(samples(collector, "cf_edge_requests")) == 2
    assert checked_edge_family(body, labels) == {"a.example": 2, "b.example": 2}
    extra = 'cf_edge_requests{edge_response_status="500",host_name="x",method_name="GET",'
    extra += 'origin_response_status="0",path="/z"} 1.0\n'
    with pytest.raises(ValueError, match="do not match"):
        checked_edge_family(
            body + extra + extra.replace("/z", "/y") + extra.replace("/z", "/v"), labels
        )
