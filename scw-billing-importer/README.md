# Scaleway Billing Importer

This script fetches billing consumption from Scaleway and calculates monthly costs:

- for a fixed range of months (`--start-period` ... `--end-period`)
- or for the last `N` months (`--months`)
- and can aggregate either a single project (`--project-id`) or all projects in an organization (`--organization-id`)

It calls the Billing API endpoint:

- `GET /billing/v2beta1/consumptions`

For each month (`billing_period=YYYY-MM`) it:

1. Fetches all pages of consumptions.
2. Filters line items by `project_id`.
3. Sums `value` (Money fields: `units` + `nanos`) by currency.
4. Returns a monthly breakdown and a grand total.

## Requirements

- Python 3.9+
- Scaleway API token with billing read permissions

Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

From this directory:

```bash
python3 import_monthly_project_costs.py \
  --api-token "$SCW_SECRET_KEY" \
  --project-id "$SCW_PROJECT_ID" \
  --months 6
```

Range mode (inclusive):

```bash
python3 import_monthly_project_costs.py \
  --api-token "$SCW_SECRET_KEY" \
  --project-id "$SCW_PROJECT_ID" \
  --start-period 2024-09 \
  --end-period 2025-02
```

All projects in an organization (no `--project-id`):

```bash
python3 import_monthly_project_costs.py \
  --api-token "$SCW_SECRET_KEY" \
  --organization-id "$SCW_ORGANIZATION_ID" \
  --start-period 2025-02 \
  --end-period 2025-07
```

Output formats:

```bash
# Human-readable table (default)
python3 import_monthly_project_costs.py --project-id "$SCW_PROJECT_ID" --months 6

# JSON
python3 import_monthly_project_costs.py --project-id "$SCW_PROJECT_ID" --months 6 --format json
```

Write output to a file:

```bash
python3 import_monthly_project_costs.py \
  --project-id "$SCW_PROJECT_ID" \
  --months 6 \
  --format json \
  --output-file billing-project-costs.json
```

## Environment Variables

- `SCW_SECRET_KEY`: API token
- `SCW_PROJECT_ID`: project ID
- `SCW_ORGANIZATION_ID`: organization ID for all-project mode
- `SCW_BILLING_MONTHS`: number of recent months (default `3`)
- `SCW_BILLING_START_PERIOD`: start period in `YYYY-MM` (requires `SCW_BILLING_END_PERIOD`)
- `SCW_BILLING_END_PERIOD`: end period in `YYYY-MM` (requires `SCW_BILLING_START_PERIOD`)
- `SCW_CATEGORY_NAME`: optional category filter
- `SCW_REQUEST_TIMEOUT`: request timeout in seconds (default `30`)
- `SCW_API_URL`: API base URL (default `https://api.scaleway.com`)
- `SCW_OUTPUT_FORMAT`: `table` or `json` (default `table`)

## Notes

- `--start-period`/`--end-period` are inclusive boundaries.
- If range is not provided, the script includes the current month in `--months`.
- Use either `--project-id` or `--organization-id`.
- If no consumptions are found for the selected scope in a month, that month is shown with `0`.
- Official API docs:
  - https://www.scaleway.com/en/developers/api/billing/#path-consumption-get-monthly-consumption
