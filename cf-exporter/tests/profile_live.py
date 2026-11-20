# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Live validation and resource measurements against the real Cloudflare API.

Reads the buckets of a few closed hours of every zone with the exporter's own
code, renders them as /analytics would, and compares each zone's sums with
hourly totals fetched independently. Reports API requests, duration, memory and
the size of the exposition.

    CF_EXPORTER_API_TOKEN=... python tests/profile_live.py [--hours N] [--end TIME]
"""

from __future__ import annotations

import argparse
import math
import resource
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import script

HOUR = 3600
# The newest validated hour ended at least this long ago, so that its late
# events have arrived and exporter and reference see the same data.
SETTLED_SECONDS = 900
# Times a zone's rejected buckets and a reduced-resolution reference are read again.
ROUNDS = 6
# Deliberately not script.EYEBALL_FILTER: the oracle must not share the
# production filter constant it checks.
CLIENT_SOURCE = "eyeball"
# Cloudflare answers from a table of every tenth or hundredth event under load.
# Its groups have a sample interval of 10 or more, and quiet hours have no group
# at all. A reference is trusted only with a group below this value, which
# proves the full data; an answer without any group proves nothing by itself.
REFERENCE_INTERVAL_LIMIT = 5.0
# The reference is read with this much time before the window, to contain such
# a group. Without any group in all of it the zone had no traffic. The plan's
# longest query range and page size may shorten it.
REFERENCE_PROOF_SECONDS = 7 * 86400
# Hourly groups per reference page; a week and the window need about 180.
REFERENCE_PAGE_SIZE = 1000
# Families whose buckets add up to all requests, to all visits and to client requests.
REQUEST_FAMILIES = ("cf_zone_requests_5m", "cf_requests_5m", "cf_cache_requests_5m")
VISIT_FAMILIES = ("cf_zone_visits_5m", "cf_visits_5m")
EDGE_FAMILY = "cf_edge_requests_5m"


def reference_query(clients: bool) -> str:
    """Hourly totals: another shape than the exporter's 5-minute buckets."""
    selection = """
      clients: httpRequestsAdaptiveGroups(
        limit: $limit
        filter: {datetime_geq: $since, datetime_lt: $until, requestSource: $source}
        orderBy: [datetimeHour_ASC]
      ) { count avg { sampleInterval } dimensions { datetimeHour } }"""
    return f"""
query Reference($zoneTag: string!, $since: Time!, $until: Time!, $limit: uint64!
    {", $source: string!" if clients else ""}) {{
  viewer {{
    zones(filter: {{zoneTag: $zoneTag}}) {{
      all: httpRequestsAdaptiveGroups(
        limit: $limit
        filter: {{datetime_geq: $since, datetime_lt: $until}}
        orderBy: [datetimeHour_ASC]
      ) {{ count sum {{ visits }} avg {{ sampleInterval }} dimensions {{ datetimeHour }} }}{
        selection if clients else ""
    }
    }}
  }}
}}
"""


def parse_time(value: str) -> int:
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise argparse.ArgumentTypeError(f"not an RFC3339 time: {value!r}") from None
    if moment.tzinfo is None:
        raise argparse.ArgumentTypeError(f"time without a zone: {value!r}")
    return int(moment.astimezone(UTC).timestamp())


def positive(value: str) -> int:
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not an integer: {value!r}") from None
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def parse_args(argv: list[str], now: float) -> tuple[int, int]:
    """[start, end) of the hours to validate; exits on invalid arguments."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--hours", type=positive, default=2, help="closed hours to validate")
    parser.add_argument(
        "--end", type=parse_time, help="end of the last hour, a past UTC hour boundary"
    )
    args = parser.parse_args(argv)
    latest = int(now - SETTLED_SECONDS) // HOUR * HOUR
    end = latest if args.end is None else args.end
    if end % HOUR:
        parser.error("--end must be a full UTC hour")
    if end > latest:
        parser.error(f"--end must not be after {script.rfc3339(latest)}: the hour has to settle")
    return end - args.hours * HOUR, end


def window_totals(groups: list, start: int, end: int) -> tuple[float, float] | None:
    """(count, visits) of the hours in [start, end) from an answer proven to be at
    full resolution; None for an answer that is reduced or has no group."""
    if not any(group["avg"]["sampleInterval"] < REFERENCE_INTERVAL_LIMIT for group in groups):
        return None
    count = visits = 0
    for group in groups:
        hour = parse_time(group["dimensions"]["datetimeHour"])
        if start <= hour < end:
            count += group["count"]
            visits += (group.get("sum") or {}).get("visits", 0)
    return count, visits


def fetch_reference(
    api: script.CloudflareAPI, plan: script.ZonePlan, start: int, end: int, now: float
):
    """(requests, visits, client requests or None) of the window, or None if it
    cannot be told.

    Each selection is answered on its own, so each keeps its first proven
    answer. A selection whose every answer was without groups had no traffic
    in the whole range and counts as zero.
    """
    oldest = (int(now - plan.reach_seconds) // HOUR + 1) * HOUR
    # One page of hours and one query of the plan's longest range must hold it all.
    page = min(REFERENCE_PAGE_SIZE, plan.totals_page_size) - ROUNDS
    longest = min(plan.max_duration, (page - 1) * HOUR) // HOUR * HOUR
    since = min(start, max(start - REFERENCE_PROOF_SECONDS, oldest, end - longest))
    aliases = ("all", "clients") if plan.edge else ("all",)
    proven: dict[str, tuple[float, float]] = {}
    seen_groups = set()
    for attempt in range(ROUNDS):
        variables = {
            "zoneTag": plan.zone_id,
            "since": script.rfc3339(since),
            "until": script.rfc3339(end),
            # Another page size each time: Cloudflare caches by query and variables.
            "limit": page + attempt,
        }
        if plan.edge:
            variables["source"] = CLIENT_SOURCE
        data = api.graphql(reference_query(plan.edge), variables)
        node = script.zone_node(data, plan.zone_id)
        for alias in aliases:
            groups = node.get(alias) or []
            if groups:
                seen_groups.add(alias)
            totals = window_totals(groups, start, end)
            if totals is not None:
                proven.setdefault(alias, totals)
        # An empty answer may be a reduced one: only all attempts together tell.
        if all(alias in proven for alias in aliases):
            break
    for alias in aliases:
        if alias not in proven:
            if alias in seen_groups:
                return None
            proven[alias] = (0, 0)
    return (*proven["all"], proven["clients"][0] if plan.edge else None)


def read_zone(exporter: script.Exporter, plan: script.ZonePlan, start: int, end: int) -> int:
    """Read every bucket of [start, end) as the exporter would; returns how many
    buckets are still not accepted after the retries."""
    first, last = start + script.BUCKET_SECONDS, end
    exporter.storage.ensure(plan.zone_id, first, last)
    pending = []
    for _ in range(ROUNDS):
        # Rejected buckets are due at once here; the service would wait first.
        pending = exporter.storage.pending(plan.zone_id, math.inf, first, last, 10**6)
        while pending:
            window = exporter._window(pending, script.INITIAL_WINDOW_BUCKETS)
            exporter.read("history", plan.zone_id, *window)
            pending = [item for item in pending if item[0] <= window[0]]
        pending = exporter.storage.pending(plan.zone_id, math.inf, first, last, 10**6)
        if not pending:
            break
    return len(pending)


def zone_sums(rows, start: int, end: int) -> dict[str, dict[str, float]]:
    """zone name -> family -> sum over the buckets ending in (start, end]."""
    sums: dict[str, dict[str, float]] = {}
    for family, labels, value, bucket_end in rows:
        if not start < bucket_end <= end:
            continue
        zone = labels.split('zone_name="', 1)[1].split('"', 1)[0]
        families = sums.setdefault(zone, {})
        families[family] = families.get(family, 0) + value
    return sums


def compare(sums: dict[str, float], reference: tuple) -> list[str]:
    """Differences between a zone's exported sums and its reference totals."""
    requests_, visits, clients = reference
    problems = []
    expected = {
        **dict.fromkeys(REQUEST_FAMILIES, requests_),
        **dict.fromkeys(VISIT_FAMILIES, visits),
    }
    if clients is not None:
        expected[EDGE_FAMILY] = clients
    for family, value in expected.items():
        exported = sums.get(family, 0)
        if exported != value:
            problems.append(f"{family} {exported:g} != {value:g}")
    return problems


def validate(
    api: script.CloudflareAPI,
    schema: script.Schema,
    start: int,
    end: int,
    now: float | None = None,
) -> dict:
    """Read and check every zone; returns the report. `now` is the current time."""
    started = time.monotonic()
    now = time.time() if now is None else now
    storage = script.Storage(None)
    exporter = script.Exporter(api, storage, schema, clock=lambda: now + time.monotonic() - started)
    try:
        exporter.refresh(now)
        zones = {}
        for plan in exporter.plans.values():
            unaccepted = read_zone(exporter, plan, start, end)
            reference = fetch_reference(api, plan, start, end, now)
            zones[plan.zone_name] = {"unaccepted": unaccepted, "reference": reference}
        rows = storage.exposition(10**9)[1]
    finally:
        storage.close()
    sums = zone_sums(rows, start, end)
    for name, zone in zones.items():
        zone["sums"] = sums.get(name, {})
        if zone["unaccepted"]:
            zone["verdict"] = f"incomplete: {zone['unaccepted']} buckets not accepted"
        elif zone["reference"] is None:
            zone["verdict"] = "inconclusive: the reference stayed at a reduced resolution"
        else:
            problems = compare(zone["sums"], zone["reference"])
            zone["verdict"] = "MISMATCH: " + "; ".join(problems) if problems else "ok"
    failed_zones = [name for name in exporter.zones.values() if name not in zones]
    families: dict[str, int] = {}
    series = set()
    for family, labels, _, _ in rows:
        families[family] = families.get(family, 0) + 1
        series.add((family, labels))
    responses = {key: value for key, value in api.stats._responses.items()}
    return {
        "zones": zones,
        "zones_without_plan": failed_zones,
        "results": {key: value for key, value in exporter.results.items() if value},
        "samples": families,
        "series": len(series),
        "exposition_bytes": len(script.render_analytics(rows)),
        "http_responses": responses,
        "seconds": time.monotonic() - started,
        "max_rss_mib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
    }


def exit_code(report: dict) -> int:
    """1 for a wrong or unreadable zone, 2 if nothing could be judged, else 0."""
    verdicts = [zone["verdict"] for zone in report["zones"].values()]
    if report["zones_without_plan"] or any(v.startswith("MISMATCH") for v in verdicts):
        return 1
    if verdicts and not any(verdict == "ok" for verdict in verdicts):
        return 2
    return 0


def main(argv: list[str] | None = None) -> int:
    start, end = parse_args(sys.argv[1:] if argv is None else argv, time.time())
    script.configure_logging()
    config = script.parse_env()
    api = script.create_api(config)
    print(f"window {script.rfc3339(start)} to {script.rfc3339(end)}; {script.describe(config)}")
    report = validate(api, config.schema, start, end)
    for name, zone in sorted(report["zones"].items()):
        sums = zone["sums"]
        print(
            f"{name}: requests {sums.get('cf_zone_requests_5m', 0):g}, "
            f"visits {sums.get('cf_zone_visits_5m', 0):g}, "
            f"client requests {sums.get(EDGE_FAMILY, 0):g}, reference {zone['reference']}: "
            f"{zone['verdict']}"
        )
    for name in report["zones_without_plan"]:
        print(f"{name}: FAILED - no settings")
    print(f"bucket reads: {report['results']}")
    print(f"samples: {report['samples']}; series: {report['series']}")
    print(f"exposition: {report['exposition_bytes']} bytes")
    print(f"HTTP responses: {report['http_responses']}")
    print(f"duration: {report['seconds']:.1f}s; max RSS: {report['max_rss_mib']:.1f} MiB")
    return exit_code(report)


if __name__ == "__main__":
    sys.exit(main())
