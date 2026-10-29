# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Measure live cycles and exposition in the Python 3.13 dev container.

Examples (token already delivered via the Makefile's stdin wrapper):
    python tests/profile_live.py --day 2026-10-25 --cycles 2
    python tests/profile_live.py --until 2026-10-25T12:00:00Z
The exporter's settings, including its optional limits and disabled labels, come
from the environment as for the service. Only aggregate measurements are
printed: no credentials or label values, except the zone of a mismatched sum.
"""

import argparse
import io
import json
import re
import resource
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from prometheus_client import CollectorRegistry, generate_latest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import script

SOURCES_QUERY = """
query RequestSources($zoneTag: string!, $since: Time!, $until: Time!) {
  viewer {
    zones(filter: {zoneTag: $zoneTag}) {
      eyeball: httpRequestsAdaptiveGroups(
        limit: 1
        filter: {datetime_geq: $since, datetime_lt: $until, requestSource: "eyeball"}
      ) { count }
      sources: httpRequestsAdaptiveGroups(
        limit: 100
        filter: {datetime_geq: $since, datetime_lt: $until}
        orderBy: [count_DESC]
      ) { count dimensions { requestSource } }
    }
  }
}
"""


def request_sources(graphql, zone_id, since, until):
    """Independently fetch the eyeball total and all requests by source, for validation.

    The exporter fetches no unconditional eyeball total. This independent query
    checks its edge sums, including when empty-page recovery used a client total.
    """
    if since == until:
        return 0, {}
    # Deliberately do not reuse the production filter: this is its independent oracle.
    variables = {"zoneTag": zone_id, "since": script.rfc3339(since), "until": script.rfc3339(until)}
    node = script.zone_node(graphql(SOURCES_QUERY, variables), zone_id)
    totals = script.group_list("eyeball", node.get("eyeball"))
    if len(totals) > 1:
        raise script.CloudflareAPIError("GraphQL: multiple dimensionless eyeball totals")
    eyeball = script.checked_value("eyeball", totals[0], lambda g: g.get("count")) if totals else 0
    sources = {}
    for group in script.group_list("sources", node.get("sources")):
        count = script.checked_value("sources", group, lambda g: g.get("count"))
        dimensions = group.get("dimensions")
        source = dimensions.get("requestSource") if isinstance(dimensions, dict) else None
        if not isinstance(source, str) or not source or source in sources:
            raise script.CloudflareAPIError("GraphQL: invalid or duplicate request source")
        sources[source] = count
    return eyeball, sources


def expected_totals(visits, requests, eyeball):
    """Map datasets to matching-scope reference totals (scalars or per-zone maps)."""
    return {"visits": visits, "requests": requests, "cache": requests, "edge": eyeball}


def source_filter_valid(eyeball, sources, requests, *, completed_day):
    """Check the raw source value, requiring exact comparable totals only for a past day."""
    raw_eyeball = sources.get("eyeball", 0)
    # A zone can contain only subrequests. An absent eyeball source is valid;
    # a completely empty source breakdown despite traffic is not a valid oracle.
    return (not requests or bool(sources)) and (
        not completed_day or eyeball == raw_eyeball <= requests
    )


def source_value_present(checked) -> bool:
    """Whether the filter's source value still occurs in the data.

    `checked` holds the request total and raw sources of every zone checked in
    one window. A zone with only subrequests is valid on its own, but a renamed
    value empties the filter and its reference in every zone at once.
    """
    checked = list(checked)
    return not any(requests for requests, _ in checked) or any(
        sources.get("eyeball", 0) > 0 for _, sources in checked
    )


class HistoricalCollector(script.CloudflareCollector):
    """Render historical snapshots for profiling without changing their window."""

    @staticmethod
    def _day_visible(snapshot: script.Snapshot) -> bool:
        return True


@dataclass(frozen=True)
class ZoneSummary:
    """Validation scalars; retaining every zone's raw series would inflate RSS."""

    zone_id: str
    total_requests: float
    total_visits: float
    available: dict[str, int]
    limited: dict[str, int]
    sums: dict[str, float]
    groups: dict[str, int]


def pinned_window(args) -> tuple[datetime, datetime] | None:
    """The fixed window of --day or --until, or None for the current cycle."""
    if args.day:
        since = datetime.strptime(args.day, "%Y-%m-%d").replace(tzinfo=UTC)
        if args.day != since.strftime("%Y-%m-%d"):
            raise ValueError("--day must be a UTC date in YYYY-MM-DD format")
        if since.date() >= datetime.now(UTC).date():
            raise ValueError("--day must be a completed UTC day")
        return since, since + timedelta(days=1)
    if args.until:
        until = datetime.strptime(args.until, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        if until > datetime.now(UTC):
            raise ValueError("--until must not be in the future")
        return until.replace(hour=0, minute=0, second=0), until
    return None


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    window = parser.add_mutually_exclusive_group()
    window.add_argument("--day", help="Completed UTC date, YYYY-MM-DD; default: current cycle")
    window.add_argument(
        "--until",
        help="Fixed exclusive window end, YYYY-MM-DDTHH:MM:SSZ; starts at its UTC midnight",
    )
    parser.add_argument("--cycles", type=int, default=2)
    args = parser.parse_args()
    if args.cycles < 1:
        parser.error("--cycles must be positive")
    try:
        pinned = pinned_window(args)
    except ValueError as error:
        parser.error(str(error))
    config = script.parse_env()
    api = script.create_api(config)
    graphql = api.graphql
    counters = {}

    def measured(query, variables):
        selections = len(re.findall(r": httpRequestsAdaptiveGroups\(", query))
        counters["graphql_requests"] += 1
        counters["analytics_requests" if selections else "settings_requests"] += 1
        counters["analytics_selections"] += selections
        return graphql(query, variables)

    api.graphql = measured
    # Keep only summaries, not an extra copy of every zone's series, to validate
    # per-zone sums even when the exposition combines zones without zone_name.
    results = {}
    collect_zone, day_window = script.collect_zone, script.day_window

    def recorded(api, zone_id, zone_name, *args, **kwargs):
        result = collect_zone(api, zone_id, zone_name, *args, **kwargs)
        results[zone_name] = ZoneSummary(
            zone_id=zone_id,
            total_requests=result.total_requests,
            total_visits=result.total_visits,
            available=dict(result.available),
            limited=dict(result.limited),
            sums={kind: sum(getattr(result, kind).values()) for kind in script.DATASETS},
            groups={kind: len(getattr(result, kind)) for kind in script.DATASETS},
        )
        return result

    registry = CollectorRegistry()
    collector = HistoricalCollector() if pinned else script.CloudflareCollector()
    registry.register(collector)
    script.collect_zone = recorded
    if pinned:
        # Pin the window itself; only its rendering bypasses the day check.
        script.day_window = lambda moment: pinned
    try:
        for cycle in range(args.cycles):
            counters.update(
                graphql_requests=0,
                settings_requests=0,
                analytics_requests=0,
                analytics_selections=0,
            )
            results.clear()
            completed_day = pinned is not None and pinned[0].date() < datetime.now(UTC).date()
            report = measure_cycle(api, graphql, results, collector, registry, completed_day)
            report = {"cycle": cycle + 1, **report, "settings": script.describe(config)}
            report.update(counters)
            print(json.dumps(report), flush=True)
            # Current-day data can still be ingested between the requests of a cycle
            # and the later validation, so only a completed day must match exactly.
            if (
                report["success"] != 1
                or not report["source_filter_valid"]
                or (completed_day and report["mismatches"])
                or report["truncated_selections"]
            ):
                return 1
    finally:
        script.collect_zone, script.day_window = collect_zone, day_window
        api.graphql = graphql
        api.session.close()
    return 0


def measure_cycle(api, graphql, results, collector, registry, completed_day) -> dict:
    """Run, render and validate one cycle; `results` receives per-zone summaries."""
    completed_day = bool(completed_day)
    snapshot = script.collect_once(api)
    collector.update(snapshot)
    started = time.monotonic()
    body = generate_latest(registry)
    exposition_seconds = time.monotonic() - started
    # Fetched outside the measured counters; validation is not part of a cycle.
    references = {
        zone: request_sources(graphql, result.zone_id, snapshot.window_start, snapshot.window_end)
        for zone, result in results.items()
        if result.available["edge"]
    }
    sources_valid = all(
        source_filter_valid(
            total, sources, results[zone].total_requests, completed_day=completed_day
        )
        for zone, (total, sources) in references.items()
    ) and source_value_present(
        (results[zone].total_requests, sources) for zone, (_, sources) in references.items()
    )
    mismatches = []
    for zone, result in results.items():
        # Edge groups are compared with the eyeball total, never cf_zone_requests.
        expected = expected_totals(
            result.total_visits, result.total_requests, references.get(zone, (0, {}))[0]
        )
        for kind in script.DATASETS:
            exported = result.sums[kind]
            if result.available[kind] and exported != expected[kind]:
                _, starts = api.slice_hints.get((result.zone_id, kind), (None, ()))
                mismatches.append(
                    {
                        "zone": zone,
                        "dataset": kind,
                        "exported": exported,
                        "total": expected[kind],
                        "groups": result.groups[kind],
                        "leaves": len(starts) + 1,
                        "truncated": result.limited[kind],
                    }
                )
    window = snapshot.window_start and [
        script.rfc3339(snapshot.window_start),
        script.rfc3339(snapshot.window_end),
    ]
    return {
        "window": window,
        "success": snapshot.cycle_success,
        "zones": len(snapshot.zone_success),
        "cycle_seconds": round(snapshot.cycle_duration, 3),
        "groups": {
            kind: int(sum(v for (_, k), v in snapshot.groups.items() if k == kind))
            for kind in script.DATASETS
        },
        "series": {script.METRICS[kind]: len(getattr(snapshot, kind)) for kind in script.DATASETS},
        "truncated_selections": sum(snapshot.limited.values()),
        "edge_unavailable_zones": sum(
            1 for (_, kind), ok in snapshot.available.items() if kind == "edge" and not ok
        ),
        "totals_match": not mismatches,
        "strict_totals": completed_day,
        "source_filter_valid": sources_valid,
        "mismatched_datasets": sorted({mismatch["dataset"] for mismatch in mismatches}),
        "mismatches": mismatches,
        "metrics_bytes": len(body),
        "samples": sum(1 for line in io.BytesIO(body) if not line.startswith(b"#")),
        "exposition_seconds": round(exposition_seconds, 3),
        "peak_rss_mib": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2),
    }


if __name__ == "__main__":
    sys.exit(main())
