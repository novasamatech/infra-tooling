#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

import argparse
import datetime as dt
import decimal
import json
import os
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import requests

API_BASE_URL = "https://api.scaleway.com"
PAGE_SIZE = 100
NANOS_IN_UNIT = decimal.Decimal("1000000000")


class ScalewayBillingClient:
    def __init__(self, api_token: str, base_url: str = API_BASE_URL, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-Auth-Token": api_token,
                "Content-Type": "application/json",
            }
        )

    def get_consumptions_page(
        self,
        billing_period: str,
        page: int,
        project_id: Optional[str] = None,
        organization_id: Optional[str] = None,
        category_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "billing_period": billing_period,
            "page": page,
            "page_size": PAGE_SIZE,
        }
        if project_id:
            params["project_id"] = project_id
        if organization_id:
            params["organization_id"] = organization_id
        if category_name:
            params["category_name"] = category_name

        response = self.session.get(
            f"{self.base_url}/billing/v2beta1/consumptions",
            params=params,
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise RuntimeError(self._format_error(response))

        try:
            return response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Invalid JSON response for billing_period={billing_period}, page={page}"
            ) from exc

    def fetch_month_consumptions(
        self,
        billing_period: str,
        project_id: Optional[str] = None,
        organization_id: Optional[str] = None,
        category_name: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        all_items: List[Dict[str, Any]] = []
        page = 1
        updated_at: Optional[str] = None

        while True:
            payload = self.get_consumptions_page(
                billing_period=billing_period,
                page=page,
                project_id=project_id,
                organization_id=organization_id,
                category_name=category_name,
            )

            if updated_at is None:
                updated_at = payload.get("updated_at")

            items = payload.get("consumptions") or []
            if not isinstance(items, list):
                raise RuntimeError(
                    f"Unexpected API payload: 'consumptions' must be a list, got {type(items)}"
                )

            all_items.extend(items)
            total_count = payload.get("total_count")

            if not items:
                break
            if isinstance(total_count, int) and len(all_items) >= total_count:
                break
            if len(items) < PAGE_SIZE:
                break

            page += 1

        return all_items, updated_at

    @staticmethod
    def _format_error(response: requests.Response) -> str:
        details = response.text.strip()
        try:
            details = json.dumps(response.json(), ensure_ascii=False)
        except ValueError:
            pass
        return (
            f"Scaleway API error {response.status_code} for "
            f"{response.request.method} {response.url}: {details}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Import Scaleway billing data and calculate monthly costs for a "
            "specific project or all projects over a month range or over the last N months."
        )
    )
    parser.add_argument(
        "--api-token",
        default=os.environ.get("SCW_SECRET_KEY"),
        help="Scaleway API token (default: SCW_SECRET_KEY).",
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("SCW_PROJECT_ID"),
        help="Target Scaleway project ID (default: SCW_PROJECT_ID).",
    )
    parser.add_argument(
        "--organization-id",
        default=os.environ.get("SCW_ORGANIZATION_ID"),
        help=(
            "Target Scaleway organization ID for all-project aggregation "
            "(default: SCW_ORGANIZATION_ID)."
        ),
    )
    parser.add_argument(
        "--months",
        type=int,
        default=int(os.environ.get("SCW_BILLING_MONTHS", "3")),
        help=(
            "Number of recent months to include, current month included (default: 3). "
            "Ignored when --start-period and --end-period are set."
        ),
    )
    parser.add_argument(
        "--start-period",
        default=os.environ.get("SCW_BILLING_START_PERIOD"),
        help="Start billing period in YYYY-MM format (inclusive).",
    )
    parser.add_argument(
        "--end-period",
        default=os.environ.get("SCW_BILLING_END_PERIOD"),
        help="End billing period in YYYY-MM format (inclusive).",
    )
    parser.add_argument(
        "--category-name",
        default=os.environ.get("SCW_CATEGORY_NAME"),
        help="Optional Scaleway category filter (e.g. Compute, Network).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=int(os.environ.get("SCW_REQUEST_TIMEOUT", "30")),
        help="HTTP request timeout in seconds (default: 30).",
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("SCW_API_URL", API_BASE_URL),
        help=f"Scaleway API base URL (default: {API_BASE_URL}).",
    )
    parser.add_argument(
        "--format",
        choices=("table", "json"),
        default=os.environ.get("SCW_OUTPUT_FORMAT", "table"),
        help="Output format (default: table).",
    )
    parser.add_argument(
        "--output-file",
        help="Optional path to save the resulting output.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if not args.api_token:
        raise ValueError("Missing API token. Use --api-token or SCW_SECRET_KEY.")
    if args.project_id and args.organization_id:
        raise ValueError("Use either --project-id or --organization-id, not both.")
    if not args.project_id and not args.organization_id:
        raise ValueError(
            "Provide --project-id (single project) or --organization-id (all projects)."
        )
    if args.months <= 0:
        raise ValueError("--months must be a positive integer.")
    if args.timeout <= 0:
        raise ValueError("--timeout must be a positive integer.")

    has_start = bool(args.start_period)
    has_end = bool(args.end_period)
    if has_start != has_end:
        raise ValueError("--start-period and --end-period must be provided together.")
    if has_start and has_end:
        _ = parse_billing_period(args.start_period)
        _ = parse_billing_period(args.end_period)


def get_last_n_billing_periods(
    months: int, today: Optional[dt.date] = None
) -> List[str]:
    reference = today or dt.datetime.now(dt.timezone.utc).date()
    year = reference.year
    month = reference.month
    periods: List[str] = []

    for _ in range(months):
        periods.append(f"{year:04d}-{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1

    periods.reverse()
    return periods


def parse_billing_period(period: str) -> Tuple[int, int]:
    try:
        parsed = dt.datetime.strptime(period, "%Y-%m")
    except ValueError as exc:
        raise ValueError(
            f"Invalid billing period '{period}'. Expected format YYYY-MM."
        ) from exc
    return parsed.year, parsed.month


def get_billing_periods_from_range(start_period: str, end_period: str) -> List[str]:
    start_year, start_month = parse_billing_period(start_period)
    end_year, end_month = parse_billing_period(end_period)

    current_year, current_month = start_year, start_month
    periods: List[str] = []

    while (current_year < end_year) or (
        current_year == end_year and current_month <= end_month
    ):
        periods.append(f"{current_year:04d}-{current_month:02d}")
        current_month += 1
        if current_month == 13:
            current_month = 1
            current_year += 1

    if not periods:
        raise ValueError(
            f"Invalid range: start_period '{start_period}' is after end_period '{end_period}'."
        )

    return periods


def money_to_decimal(money: Dict[str, Any]) -> decimal.Decimal:
    units = decimal.Decimal(str(money.get("units", 0)))
    nanos = decimal.Decimal(str(money.get("nanos", 0)))
    return units + (nanos / NANOS_IN_UNIT)


def format_decimal(value: decimal.Decimal) -> str:
    normalized = value.quantize(decimal.Decimal("0.000000001"))
    text = format(normalized, "f").rstrip("0").rstrip(".")
    if text in {"", "-0"}:
        return "0"
    return text


def aggregate_costs(
    consumptions: List[Dict[str, Any]],
    project_id: Optional[str] = None,
) -> Tuple[Dict[str, decimal.Decimal], int]:
    totals: Dict[str, decimal.Decimal] = defaultdict(decimal.Decimal)
    items_count = 0

    for item in consumptions:
        if project_id and item.get("project_id") != project_id:
            continue

        value = item.get("value")
        if not isinstance(value, dict):
            continue

        currency = value.get("currency_code") or "UNKNOWN"
        totals[currency] += money_to_decimal(value)
        items_count += 1

    return dict(totals), items_count


def totals_to_string_map(totals: Dict[str, decimal.Decimal]) -> Dict[str, str]:
    return {
        currency: format_decimal(amount) for currency, amount in sorted(totals.items())
    }


def totals_for_table(totals: Dict[str, decimal.Decimal]) -> str:
    if not totals:
        return "0"
    parts = [
        f"{format_decimal(amount)} {currency}"
        for currency, amount in sorted(totals.items())
    ]
    return ", ".join(parts)


def collect_monthly_data(
    client: ScalewayBillingClient,
    project_id: Optional[str],
    organization_id: Optional[str],
    billing_periods: List[str],
    category_name: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, decimal.Decimal]]:
    monthly_rows: List[Dict[str, Any]] = []
    grand_total: Dict[str, decimal.Decimal] = defaultdict(decimal.Decimal)

    for billing_period in billing_periods:
        consumptions, updated_at = client.fetch_month_consumptions(
            billing_period=billing_period,
            project_id=project_id,
            organization_id=organization_id,
            category_name=category_name,
        )
        month_totals, items_count = aggregate_costs(consumptions, project_id=project_id)

        for currency, amount in month_totals.items():
            grand_total[currency] += amount

        monthly_rows.append(
            {
                "billing_period": billing_period,
                "totals": month_totals,
                "items_count": items_count,
                "updated_at": updated_at,
            }
        )

    return monthly_rows, dict(grand_total)


def render_table(
    rows: List[Dict[str, Any]], grand_total: Dict[str, decimal.Decimal]
) -> str:
    period_values = [row["billing_period"] for row in rows]
    total_values = [totals_for_table(row["totals"]) for row in rows]
    item_values = [str(row["items_count"]) for row in rows]

    period_width = max(
        [len("billing_period")] + [len(value) for value in period_values]
    )
    total_width = max(
        [len("total")]
        + [len(value) for value in total_values]
        + [len(totals_for_table(grand_total))]
    )
    items_width = max([len("items")] + [len(value) for value in item_values])

    header = (
        f"{'billing_period':<{period_width}}  "
        f"{'total':<{total_width}}  "
        f"{'items':>{items_width}}"
    )
    separator = "-" * len(header)

    lines = [header, separator]
    for row in rows:
        lines.append(
            f"{row['billing_period']:<{period_width}}  "
            f"{totals_for_table(row['totals']):<{total_width}}  "
            f"{row['items_count']:>{items_width}}"
        )

    lines.append(separator)
    lines.append(
        f"{'TOTAL':<{period_width}}  "
        f"{totals_for_table(grand_total):<{total_width}}  "
        f"{sum(row['items_count'] for row in rows):>{items_width}}"
    )
    return "\n".join(lines)


def to_json_payload(
    project_id: Optional[str],
    organization_id: Optional[str],
    months_requested: Optional[int],
    start_period: Optional[str],
    end_period: Optional[str],
    category_name: Optional[str],
    rows: List[Dict[str, Any]],
    grand_total: Dict[str, decimal.Decimal],
) -> Dict[str, Any]:
    return {
        "project_id": project_id,
        "organization_id": organization_id,
        "scope": "project" if project_id else "organization",
        "months_requested": months_requested,
        "start_period": start_period,
        "end_period": end_period,
        "category_name": category_name,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "monthly_breakdown": [
            {
                "billing_period": row["billing_period"],
                "totals": totals_to_string_map(row["totals"]),
                "items_count": row["items_count"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ],
        "grand_total": totals_to_string_map(grand_total),
    }


def main() -> int:
    args = parse_args()

    try:
        validate_args(args)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    decimal.getcontext().prec = 28

    client = ScalewayBillingClient(
        api_token=args.api_token,
        base_url=args.base_url,
        timeout=args.timeout,
    )

    try:
        if args.start_period and args.end_period:
            billing_periods = get_billing_periods_from_range(
                args.start_period, args.end_period
            )
            months_requested: Optional[int] = None
        else:
            billing_periods = get_last_n_billing_periods(args.months)
            months_requested = args.months
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    try:
        rows, grand_total = collect_monthly_data(
            client=client,
            project_id=args.project_id,
            organization_id=args.organization_id,
            billing_periods=billing_periods,
            category_name=args.category_name,
        )
    except Exception as exc:
        print(f"Error while fetching billing data: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        output = json.dumps(
            to_json_payload(
                project_id=args.project_id,
                organization_id=args.organization_id,
                months_requested=months_requested,
                start_period=args.start_period,
                end_period=args.end_period,
                category_name=args.category_name,
                rows=rows,
                grand_total=grand_total,
            ),
            ensure_ascii=False,
            indent=2,
        )
    else:
        output = render_table(rows, grand_total)

    if args.output_file:
        with open(args.output_file, "w", encoding="utf-8") as file_handle:
            file_handle.write(output)
            file_handle.write("\n")

    print(output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
