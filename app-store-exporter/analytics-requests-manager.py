#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ASC Analytics Requests Manager (ONGOING only)

Description:
  Utility to manage App Store Connect Analytics *ONGOING* report requests for a set of apps.
  - Accepts a comma-separated list of bundle IDs.
  - Can CREATE (default), DELETE, or LIST existing ONGOING report requests and available reports.
  - Prints a snapshot of current requests at the START and END of execution
    (for --list it prints requests and available reports).

CLI options:
  --issuer      <ISSUER_ID>      Required. Issuer ID from App Store Connect → Users and Access → Integrations.
  --key-id      <KEY_ID>         Required. Key ID for your .p8 key (shown next to the generated key).
  --p8          <PATH>           Required. Path to the private key file, e.g. AuthKey_XXXXXX.p8.
  --bundles     <LIST>           Required. Comma-separated bundle IDs, e.g. com.app.one,com.app.two.
  --create                      Optional. Create ONGOING requests for each bundle (DEFAULT action if no flag specified).
  --delete                      Optional. Delete existing ONGOING requests for each bundle.
  --list                        Optional. List existing ONGOING requests and available reports for each bundle and exit.

Permissions:
- Requires an API key with Admin (or Account Holder) privileges to create/delete requests!

Security features:
  - Validates bundle ID format (alphanumeric, dots, hyphens only)
  - Limits maximum number of bundle IDs to 50 per execution
  - Checks private key file permissions
  - Implements rate limiting between API requests
  - Includes retry mechanism with exponential backoff for temporary failures
  - Sanitizes error messages to avoid leaking sensitive information

Behavior notes:
  - If none of --create/--delete/--list is specified, the script defaults to --create.
  - Requires an API key with Admin (or Account Holder) privileges to create/delete requests.
  - Handles "already exists" and "nothing to delete" gracefully with informative messages.
  - Works ONLY with access type ONGOING. Snapshots are not managed by this tool.
  - The --list option shows both report requests and available analytics reports.

Dependencies:
  pip install requests pyjwt cryptography python-dateutil
"""

import argparse
import os
import sys
import time
from typing import List, Tuple, Iterator

import jwt
import re
import requests

BASE = "https://api.appstoreconnect.apple.com"


# -------------------- JWT --------------------
def make_token(issuer: str, key_id: str, p8_path: str, ttl: int = 1200) -> str:
    """Create short-lived JWT for App Store Connect API (ES256)."""
    try:
        # Check file permissions (should not be world-readable)
        if os.stat(p8_path).st_mode & 0o077:
            print(f"[WARNING] Private key file {p8_path} has overly permissive permissions", file=sys.stderr)

        with open(p8_path, "r", encoding='utf-8') as f:
            private_key = f.read()
    except FileNotFoundError:
        print(f"[ERROR] Private key file not found: {p8_path}", file=sys.stderr)
        sys.exit(2)
    except PermissionError:
        print(f"[ERROR] Permission denied reading private key file: {p8_path}", file=sys.stderr)
        sys.exit(2)
    now = int(time.time())
    return jwt.encode(
        {"iss": issuer, "iat": now, "exp": now + ttl, "aud": "appstoreconnect-v1"},
        private_key,
        algorithm="ES256",
        headers={"kid": key_id, "typ": "JWT"},
    )


# -------------------- HTTP helpers --------------------
def asc_get(path: str, token: str, params: dict | None = None, max_retries: int = 3) -> dict:
    for attempt in range(max_retries):
        try:
            r = requests.get(f"{BASE}{path}", headers={"Authorization": f"Bearer {token}"}, params=params or {}, timeout=60)
            if r.status_code == 403:
                print(f"[ERROR] 403 Forbidden for GET {path}. Check API key role (Admin needed for create/delete), app access, and team.", file=sys.stderr)
                sys.exit(3)
            r.raise_for_status()
            return r.json()
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"[WARNING] GET {path} failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise
    # This should never be reached due to the raise in the else clause, but for type safety
    raise RuntimeError(f"Failed to GET {path} after {max_retries} attempts")


def asc_post(path: str, token: str, payload: dict, max_retries: int = 3) -> dict:
    for attempt in range(max_retries):
        try:
            r = requests.post(
                f"{BASE}{path}",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=payload,
                timeout=60,
            )
            if r.status_code == 403:
                print(f"[ERROR] 403 Forbidden for POST {path}. Admin (or Account Holder) role required to create requests.", file=sys.stderr)
                sys.exit(3)
            r.raise_for_status()
            return r.json()
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"[WARNING] POST {path} failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise
    # This should never be reached due to the raise in the else clause, but for type safety
    raise RuntimeError(f"Failed to POST {path} after {max_retries} attempts")


def asc_delete(path: str, token: str, max_retries: int = 3) -> None:
    for attempt in range(max_retries):
        try:
            r = requests.delete(f"{BASE}{path}", headers={"Authorization": f"Bearer {token}"}, timeout=60)
            if r.status_code == 403:
                print(f"[ERROR] 403 Forbidden for DELETE {path}. Admin (or Account Holder) role required to delete requests.", file=sys.stderr)
                sys.exit(3)
            if r.status_code not in (200, 202, 204):
                r.raise_for_status()
            return
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"[WARNING] DELETE {path} failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise


# -------------------- Core helpers --------------------
def get_app(bundle_id: str, token: str) -> Tuple[str, str]:
    """Return (app_id, app_name) by bundleId."""
    j = asc_get("/v1/apps", token, params={"filter[bundleId]": bundle_id, "limit": 1})
    data = j.get("data") or []
    if not data:
        raise RuntimeError(f"App with bundleId '{bundle_id}' not found")
    item = data[0]
    app_id = item["id"]
    app_name = (item.get("attributes") or {}).get("name", "")
    return app_id, app_name


def list_requests_for_app(app_id: str, token: str) -> list[dict]:
    """List ONGOING analytics report requests for the given app."""
    params = {"filter[accessType]": "ONGOING", "limit": 200}
    j = asc_get(f"/v1/apps/{app_id}/analyticsReportRequests", token, params=params)
    return j.get("data", [])


def list_available_reports_for_app(app_id: str, token: str) -> Iterator[dict]:
    """Yield available analytics reports for the given app with detailed request and file information."""
    # First get report requests with full details
    report_requests = list_requests_for_app(app_id, token)

    for request in report_requests:
        request_id = request["id"]
        request_attrs = request.get("attributes") or {}
        # Store request attributes for later use
        request_info = {
            "created_date": request_attrs.get("createdDate", ""),
            "stopped": request_attrs.get("stoppedDueToInactivity", False),
            "access_type": request_attrs.get("accessType", "")
        }
        try:
            # Get reports for this request
            reports_response = asc_get(f"/v1/analyticsReportRequests/{request_id}/reports",
                                     token, params={"limit": 200})
            for report in reports_response.get("data", []):
                report_attrs = report.get("attributes") or {}

                # Get report files/instances for this report
                report_files = []
                try:
                    # According to Apple documentation, files are accessed through analyticsReportInstances
                    # First get the instances for this report
                    instances_response = asc_get(f"/v1/analyticsReports/{report['id']}/instances",
                                               token, params={"limit": 100})

                    # For each instance, get the files
                    for instance_data in instances_response.get("data", []):
                        instance_id = instance_data["id"]
                        try:
                            files_response = asc_get(f"/v1/analyticsReportInstances/{instance_id}/files",
                                                   token, params={"limit": 100})

                            # Process files for this instance
                            for file_data in files_response.get("data", []):
                                file_attrs = file_data.get("attributes") or {}
                                report_files.append({
                                    "file_id": file_data["id"],
                                    "name": file_attrs.get("fileName", ""),
                                    "url": file_attrs.get("downloadUrl", ""),
                                    "size": file_attrs.get("fileSize", 0),
                                    "created_date": file_attrs.get("createdDate", ""),
                                    "start_date": file_attrs.get("startDate", ""),
                                    "end_date": file_attrs.get("endDate", ""),
                                    "instance_id": instance_id
                                })
                        except Exception as e:
                            # Log instance-level errors but continue
                            print(f"[INFO] Could not access files for instance {instance_id}: {e}")

                except Exception as e:
                    # Log any errors but continue processing other reports
                    print(f"[INFO] Could not access instances for report {report['id']}: {e}")

                report_info = {
                    "request_id": request_id,
                    "report_id": report["id"],
                    "name": report_attrs.get("name", ""),
                    "category": report_attrs.get("category", ""),
                    "report_type": report_attrs.get("reportType", report_attrs.get("granularity", report_attrs.get("frequency", ""))),
                    "request_created_date": request_info.get("created_date", ""),
                    "request_stopped": request_info.get("stopped", False),
                    "request_access_type": request_info.get("access_type", ""),
                    "files": report_files
                }
                yield report_info
        except Exception as e:
            print(f"[WARNING] Failed to get reports for request {request_id}: {e}")


def create_request_for_app(app_id: str, token: str) -> str:
    """Create ONGOING analytics report request for the given app."""
    payload = {
        "data": {
            "type": "analyticsReportRequests",
            "attributes": {"accessType": "ONGOING"},
            "relationships": {"app": {"data": {"type": "apps", "id": app_id}}},
        }
    }
    j = asc_post("/v1/analyticsReportRequests", token, payload)
    return j["data"]["id"]


def delete_request(request_id: str, token: str) -> None:
    """Delete analytics report request by id."""
    asc_delete(f"/v1/analyticsReportRequests/{request_id}", token)


# -------------------- Validation --------------------
def validate_bundle_id(bundle_id: str) -> bool:
    """Validate bundle ID format according to Apple conventions."""
    # Apple bundle IDs typically follow: com.company.appname or similar
    # They should contain only alphanumeric characters, dots, and hyphens
    if not bundle_id or len(bundle_id) > 155:  # Apple's maximum length
        return False
    pattern = r'^[a-zA-Z0-9.-]+$'
    return bool(re.match(pattern, bundle_id))


# -------------------- Configuration --------------------
MAX_BUNDLES = 10  # Maximum number of bundle IDs to process at once


# -------------------- Pretty print --------------------
def print_requests_table(title: str, rows: List[Tuple[str, str, str, str]]) -> None:
    """
    rows: list of tuples (bundleId, appName, requestId, note)
    """
    print(f"\n{title}")
    if not rows:
        print("(no requests)")
        return
    headers = ("bundleId", "appName", "requestId", "note")
    colw = [max(len(x[i]) for x in rows + [headers]) for i in range(4)]
    print(" | ".join(headers[i].ljust(colw[i]) for i in range(4)))
    print("-+-".join("-" * colw[i] for i in range(4)))
    for r in rows:
        print(" | ".join(r[i].ljust(colw[i]) for i in range(4)))


def print_reports_table(title: str, reports_iter: Iterator[dict]) -> None:
    """Print available reports one at a time with complete information including files."""
    print(f"\n{title}")
    print("=" * 80)

    current_app = None
    current_request = None
    report_count = 0

    for report in reports_iter:
        # Print app header when it changes
        if current_app != (report.get("bundle_id"), report.get("app_name")):
            current_app = (report.get("bundle_id"), report.get("app_name"))
            print(f"\n📱 App: {current_app[1]} ({current_app[0]})")
            print("-" * 60)

        # Print request header when it changes
        if current_request != report.get("request_id"):
            current_request = report.get("request_id")
            print(f"\n  🔗 Request: {current_request}")
            print(f"    Created: {report.get('request_created_date', 'Not available')}")
            print(f"    Access Type: {report.get('request_access_type', '')}")
            print(f"    Stopped: {'Yes' if report.get('request_stopped') else 'No'}")

        # Print report details
        print(f"\n    📊 Report: {report.get('name', 'Unknown')}")
        print(f"      Category: {report.get('category', '')}")
        report_type = report.get('report_type', '')
        if report_type:
            print(f"      Type: {report_type}")
        print(f"      Report ID: {report.get('report_id', '')}")

        # Print files information
        files = report.get("files", [])
        if files:
            # Group files by date for better organization
            files_by_date = {}
            for file_info in files:
                date_key = file_info.get("start_date", "") or file_info.get("created_date", "unknown_date")
                if date_key not in files_by_date:
                    files_by_date[date_key] = []
                files_by_date[date_key].append(file_info)

            print(f"      📁 Files:")
            for date_key, files in sorted(files_by_date.items()):
                if date_key != "unknown_date":
                    print(f"        📅 {date_key}:")
                else:
                    print(f"        📅 Unknown date:")

                for file_info in files:
                    file_size_mb = file_info.get("size", 0) / (1024 * 1024)
                    print(f"          • {file_info.get('name', 'unknown')}")
                    print(f"            ID: {file_info.get('file_id', '')}")
                    print(f"            Size: {file_size_mb:.2f} MB")
                    if file_info.get("start_date") and file_info.get("end_date"):
                        print(f"            Period: {file_info.get('start_date')} - {file_info.get('end_date')}")
        else:
            print(f"      📭 No files available (report may be processing)")

        report_count += 1
        print(f"\n    {'─' * 40}")

    print(f"\n📈 Total reports processed: {report_count}")
    print("=" * 80)


def collect_requests_snapshot(bundles: List[str], token: str) -> List[Tuple[str, str, str, str]]:
    snapshot = []
    for b in bundles:
        try:
            app_id, app_name = get_app(b, token)
            reqs = list_requests_for_app(app_id, token)
            if not reqs:
                snapshot.append((b, app_name, "-", "no ONGOING requests"))
            else:
                for r in reqs:
                    attrs = r.get("attributes") or {}
                    rid = r["id"]
                    note = []
                    if attrs.get("stoppedDueToInactivity"):
                        note.append("stoppedDueToInactivity")
                    if attrs.get("createdDate"):
                        note.append(f"created:{attrs.get('createdDate')}")
                    snapshot.append((b, app_name, rid, ", ".join(note) if note else ""))
        except Exception as e:
            # Sanitize error message to avoid leaking sensitive information
            error_msg = str(e)
            if any(sensitive in error_msg.lower() for sensitive in ["token", "key", "secret", "auth"]):
                error_msg = "Authentication error"
            snapshot.append((b, "-", "-", f"ERROR: {error_msg}"))
    return snapshot


def collect_reports_snapshot(bundles: List[str], token: str) -> Iterator[dict]:
    """Yield available reports for all bundles progressively."""
    for b in bundles:
        try:
            app_id, app_name = get_app(b, token)
            for report in list_available_reports_for_app(app_id, token):
                report["bundle_id"] = b
                report["app_name"] = app_name
                yield report
        except Exception as e:
            # Skip errors for individual apps, continue with others
            print(f"[WARNING] Failed to get reports for {b}: {e}")
            # Yield an error report to maintain structure
            yield {
                "bundle_id": b,
                "app_name": "ERROR",
                "request_id": "ERROR",
                "report_id": "ERROR",
                "name": f"Error: {str(e)[:100]}",
                "category": "ERROR",
                "report_type": "",
                "files": []
            }


# -------------------- Main --------------------
def main():
    ap = argparse.ArgumentParser(
        description="Create/Delete/List App Store Connect Analytics ONGOING report requests for multiple apps."
    )
    ap.add_argument("--issuer", required=True, help="Issuer ID (Users and Access → Integrations)")
    ap.add_argument("--key-id", required=True, help="Key ID for the .p8")
    ap.add_argument("--p8", required=True, help="Path to AuthKey_XXXXXX.p8")
    ap.add_argument(
        "--bundles",
        required=True,
        help="Comma-separated bundle IDs, e.g. com.app.one,com.app.two",
    )
    ap.add_argument("--create", action="store_true", help="Create ONGOING requests (default action)")
    ap.add_argument("--delete", action="store_true", help="Delete existing ONGOING requests")
    ap.add_argument("--list", action="store_true", help="List existing ONGOING requests and exit")
    ap.add_argument("--reports", action="store_true", help="List available reports for each app")
    args = ap.parse_args()

    # Decide action (default to --create if nothing specified)
    action_create = args.create
    action_delete = args.delete
    action_list   = args.list
    if not any([action_create, action_delete, action_list]):
        action_create = True

    # Parse and validate bundles
    bundles = [b.strip() for b in args.bundles.split(",") if b.strip()]
    if not bundles:
        print("[ERROR] --bundles must contain at least one bundleId", file=sys.stderr)
        sys.exit(1)

    # Check maximum bundle limit
    if len(bundles) > MAX_BUNDLES:
        print(f"[ERROR] Too many bundle IDs ({len(bundles)}). Maximum allowed is {MAX_BUNDLES}", file=sys.stderr)
        sys.exit(1)

    # Validate bundle IDs
    invalid_bundles = [b for b in bundles if not validate_bundle_id(b)]
    if invalid_bundles:
        print(f"[ERROR] Invalid bundle ID format: {invalid_bundles}", file=sys.stderr)
        print("Bundle IDs should contain only alphanumeric characters, dots, and hyphens", file=sys.stderr)
        sys.exit(1)

    # Build token
    try:
        token = make_token(args.issuer, args.key_id, args.p8)
    except Exception as e:
        print(f"[ERROR] Cannot create JWT: {e}", file=sys.stderr)
        sys.exit(2)

    # Snapshot BEFORE
    before = collect_requests_snapshot(bundles, token)
    print_requests_table("Current ONGOING requests (BEFORE):", before)

    if args.reports:
        # List available reports and exit
        reports = collect_reports_snapshot(bundles, token)
        print_reports_table("Available Analytics Reports:", reports)
        return

    if action_list:
        # List available reports and exit
        reports = collect_reports_snapshot(bundles, token)
        print_reports_table("Available Analytics Reports:", reports)
        return

    # Perform actions with rate limiting
    for i, b in enumerate(bundles):
        print(f"\n==> {b}")

        # Add rate limiting (0.5 seconds between requests)
        if i > 0:
            time.sleep(0.5)

        try:
            app_id, app_name = get_app(b, token)
        except Exception as e:
            print(f"[ERROR] {b}: cannot resolve app: {e}")
            continue

        try:
            if action_create:
                existing = list_requests_for_app(app_id, token)
                if existing:
                    ids = ", ".join(r["id"] for r in existing)
                    print(f"[INFO] {b}: ONGOING request already exists: {ids} — skipping create")
                else:
                    rid = create_request_for_app(app_id, token)
                    print(f"[OK]   {b}: created ONGOING request: {rid}")

            if action_delete:
                existing = list_requests_for_app(app_id, token)
                if not existing:
                    print(f"[INFO] {b}: no ONGOING requests to delete — skipping")
                else:
                    for r in existing:
                        rid = r["id"]
                        try:
                            delete_request(rid, token)
                            print(f"[OK]   {b}: deleted ONGOING request {rid}")
                            # Add small delay after delete operation
                            time.sleep(0.2)
                        except requests.HTTPError as he:
                            status = he.response.status_code if he.response is not None else "?"
                            # Avoid exposing sensitive information in error messages
                            error_msg = str(he)
                            if "detail" in error_msg.lower() or "token" in error_msg.lower():
                                error_msg = "API error occurred (details hidden for security)"
                            print(f"[ERROR] {b}: failed to delete {rid} (HTTP {status}): {error_msg}")

        except requests.HTTPError as he:
            status = he.response.status_code if he.response is not None else "?"
            # Avoid exposing sensitive information in error messages
            error_msg = str(he)
            if "detail" in error_msg.lower() or "token" in error_msg.lower():
                error_msg = "API error occurred (details hidden for security)"
            print(f"[ERROR] {b}: HTTP {status}: {error_msg}")
        except Exception as e:
            # Sanitize exception messages to avoid leaking sensitive info
            error_msg = str(e)
            if any(sensitive in error_msg.lower() for sensitive in ["token", "key", "secret", "auth"]):
                error_msg = "Authentication error occurred"
            print(f"[ERROR] {b}: {error_msg}")

    # Snapshot AFTER
    after = collect_requests_snapshot(bundles, token)
    print_requests_table("Current ONGOING requests (AFTER):", after)


if __name__ == "__main__":
    main()
