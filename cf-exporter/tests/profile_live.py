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
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from prometheus_client import CollectorRegistry, generate_latest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import script


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
    registry = CollectorRegistry()
    collector = script.CloudflareCollector()
    registry.register(collector)
    if args.day:
        # Pin the full historical window rather than the empty following day.
        script.day_window = lambda moment: (now - timedelta(days=1), now)
    for cycle in range(args.cycles):
        counters.update(graphql_requests=0, analytics_selections=0)
        snapshot = script.collect_once(api, now=now)
        # Only the profiling collector renders historical data after midnight.
        # Production collect() continues to suppress stale day-scoped metrics.
        collector.update(
            replace(snapshot, window_start=datetime.now(UTC)) if args.day else snapshot
        )
        started = time.monotonic()
        body = generate_latest(registry)
        exposition_seconds = time.monotonic() - started
        report = {
            "cycle": cycle + 1,
            "day": args.day or "today",
            "success": snapshot.cycle_success,
            "zones": len(snapshot.zone_success),
            "cycle_seconds": round(snapshot.cycle_duration, 3),
            **counters,
            "groups": {kind: len(getattr(snapshot, kind)) for kind in script.DATASETS},
            "truncated_selections": sum(snapshot.limited.values()),
            "totals_match": all(
                sum(v for labels, v in getattr(snapshot, kind).items() if labels[0] == zone)
                == total
                for kind in script.DATASETS
                for zone, total in (
                    snapshot.zone_visits if kind == "visits" else snapshot.zone_requests
                ).items()
            ),
            "metrics_bytes": len(body),
            "samples": sum(1 for line in io.BytesIO(body) if not line.startswith(b"#")),
            "exposition_seconds": round(exposition_seconds, 3),
            "peak_rss_mib": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2),
        }
        print(json.dumps(report), flush=True)
        del body
        if (
            snapshot.cycle_success != 1
            or not report["totals_match"]
            or report["truncated_selections"]
        ):
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
