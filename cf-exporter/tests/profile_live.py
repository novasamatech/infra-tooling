# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Measure live cycles and exposition in the Python 3.13 dev container.

Example (token already delivered via the Makefile's stdin wrapper):
    python tests/profile_live.py --day 2026-10-25 --cycles 2
Only aggregate measurements are printed; no credentials or label values.
"""

import argparse
import io
import json
import re
import resource
import sys
import time
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

    The exporter itself fetches no eyeball total: its edge groups are checked against this.
    """
    if since == until:
        return 0, {}
    # Deliberately do not reuse the production filter: this is its independent oracle.
    variables = {"zoneTag": zone_id, "since": script.rfc3339(since), "until": script.rfc3339(until)}
    node = script._zone_node(graphql(SOURCES_QUERY, variables), zone_id)
    totals = script._group_list("eyeball", node.get("eyeball"))
    if len(totals) > 1:
        raise script.CloudflareAPIError("GraphQL: multiple dimensionless eyeball totals")
    eyeball = script._checked_value("eyeball", totals[0], lambda g: g.get("count")) if totals else 0
    sources = {}
    for group in script._group_list("sources", node.get("sources")):
        count = script._checked_value("sources", group, lambda g: g.get("count"))
        dimensions = group.get("dimensions")
        source = dimensions.get("requestSource") if isinstance(dimensions, dict) else None
        if not isinstance(source, str) or not source or source in sources:
            raise script.CloudflareAPIError("GraphQL: invalid or duplicate request source")
        sources[source] = count
    return eyeball, sources


class HistoricalCollector(script.CloudflareCollector):
    """Render historical snapshots for profiling without changing their window."""

    def collect(self):
        snapshot = self.snapshot
        yield from self._day_metrics(snapshot)
        yield from self._health_metrics(snapshot)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", help="Completed UTC date, YYYY-MM-DD; default: current cycle")
    parser.add_argument("--cycles", type=int, default=2)
    args = parser.parse_args()
    now = (
        datetime.fromisoformat(args.day).replace(tzinfo=UTC) + timedelta(days=1)
        if args.day
        else None
    )
    config = script.parse_env()
    api = script.CloudflareAPI(
        config.api_token, config.request_timeout, group_limit=config.group_limit
    )
    graphql = api.graphql
    counters = {}

    def measured(query, variables):
        counters["graphql_requests"] += 1
        counters["analytics_selections"] += len(
            re.findall(r": httpRequestsAdaptiveGroups\(", query)
        )
        return graphql(query, variables)

    api.graphql = measured
    list_zones = api.list_zones
    zone_ids = {}

    def listed():
        zones = list_zones()
        zone_ids.clear()
        zone_ids.update((zone["name"], zone["id"]) for zone in zones)
        return zones

    api.list_zones = listed
    registry = CollectorRegistry()
    collector = HistoricalCollector() if args.day else script.CloudflareCollector()
    registry.register(collector)
    if args.day:
        # Pin the full historical window rather than the empty following day.
        script.day_window = lambda moment: (now - timedelta(days=1), now)
    for cycle in range(args.cycles):
        counters.update(graphql_requests=0, analytics_selections=0)
        snapshot = script.collect_once(api, now=now)
        collector.update(snapshot)
        started = time.monotonic()
        body = generate_latest(registry)
        exposition_seconds = time.monotonic() - started
        edge_zones = [
            zone for (zone, kind), ok in snapshot.available.items() if kind == "edge" and ok
        ]
        # Fetched outside the measured counters; validation is not part of a cycle.
        references = {
            zone: request_sources(
                graphql, zone_ids[zone], snapshot.window_start, snapshot.window_end
            )
            for zone in edge_zones
        }
        edge_totals = {zone: total for zone, (total, _) in references.items()}
        source_filter_valid = all(
            (not snapshot.zone_requests[zone] or sources.get("eyeball", 0) > 0)
            and (not args.day or total == sources.get("eyeball", 0) <= snapshot.zone_requests[zone])
            for zone, (total, sources) in references.items()
        )
        # Edge groups are compared with the eyeball total, never cf_zone_requests.
        expected = {
            "visits": snapshot.zone_visits,
            "requests": snapshot.zone_requests,
            "cache": snapshot.zone_requests,
            "edge": edge_totals,
        }
        mismatched = sorted(
            {
                kind
                for kind in script.DATASETS
                for zone, total in expected[kind].items()
                if sum(v for labels, v in getattr(snapshot, kind).items() if labels[0] == zone)
                != total
            }
        )
        report = {
            "cycle": cycle + 1,
            "day": args.day or "today",
            "success": snapshot.cycle_success,
            "zones": len(snapshot.zone_success),
            "cycle_seconds": round(snapshot.cycle_duration, 3),
            **counters,
            "groups": {kind: len(getattr(snapshot, kind)) for kind in script.DATASETS},
            "truncated_selections": sum(snapshot.limited.values()),
            "edge_unavailable_zones": sum(
                1 for (_, kind), ok in snapshot.available.items() if kind == "edge" and not ok
            ),
            "totals_match": not mismatched,
            "source_filter_valid": source_filter_valid,
            "mismatched_datasets": mismatched,
            "metrics_bytes": len(body),
            "samples": sum(1 for line in io.BytesIO(body) if not line.startswith(b"#")),
            "exposition_seconds": round(exposition_seconds, 3),
            "peak_rss_mib": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2),
        }
        print(json.dumps(report), flush=True)
        del body
        # Current-day data can still be ingested between the requests of a cycle
        # and the later validation, so only a completed day must match exactly.
        if (
            snapshot.cycle_success != 1
            or not source_filter_valid
            or (args.day and mismatched)
            or report["truncated_selections"]
        ):
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
