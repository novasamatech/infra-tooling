#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""
ASC Analytics Requests Manager (ONGOING only)

Description:
  Utility to manage App Store Connect Analytics *ONGOING* report requests for a set of apps.
  - Accepts a comma-separated list of bundle IDs.
  - Can CREATE (default), DELETE, or LIST existing ONGOING report requests and available reports.
  - Prints a snapshot of current requests at the START and END of execution
    (for --list it prints requests and available reports).

CLI options:
  --issuer           <ISSUER_ID>      Required. Issuer ID from App Store Connect → Users and Access → Integrations.
                                       Can also be set via APPSTORE_EXPORTER_ISSUER_ID environment variable.
  --key-id           <KEY_ID>         Required. Key ID for your .p8 key (shown next to the generated key).
                                       Can also be set via APPSTORE_EXPORTER_KEY_ID environment variable.
  --p8               <PATH>           Required. Path to the private key file, e.g. AuthKey_XXXXXX.p8.
                                       Can also be set via APPSTORE_EXPORTER_PRIVATE_KEY environment variable.
  --bundles          <LIST>           Required. Comma-separated bundle IDs, e.g. com.app.one,com.app.two.
                                       Can also be set via APPSTORE_EXPORTER_BUNDLE_IDS environment variable.
  --create                          Optional. Create ONGOING requests for each bundle (DEFAULT action if no flag specified).
  --delete                          Optional. Delete existing ONGOING requests for each bundle.
  --list                            Optional. List existing ONGOING requests and available reports for each bundle and exit.
  --start/--from     <YYYY-MM-DD>     Optional. Start date (inclusive) for filtering report instances. If start == end, filters by processingDate; otherwise uses startDate/endDate.
  --end/--to         <YYYY-MM-DD>     Optional. End date (inclusive) for filtering report instances.
  --debug                           Optional. Enable debug logging and iterate reports until the first report with non-empty instances/segments, then stop.
  --diagnose                        Optional. Run comprehensive API diagnostics to check token validity, permissions, and access levels, then exit.
                                    Shows detailed error information for any 403 errors and provides specific recommendations to fix issues.

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
import base64
import json
import os
import sys
import time
from typing import List, Tuple, Iterator

import jwt
import re
import requests
import logging
from datetime import datetime, timedelta, timezone

BASE = "https://api.appstoreconnect.apple.com"


# -------------------- Debug Helpers --------------------
def decode_jwt_without_verification(token: str) -> dict:
    """Decode JWT token without signature verification for debugging purposes."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return {"error": "Invalid JWT format"}

        # Decode header
        header_padding = parts[0] + "=" * (4 - len(parts[0]) % 4)
        header = json.loads(base64.urlsafe_b64decode(header_padding))

        # Decode payload
        payload_padding = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_padding))

        # Convert timestamps to readable format
        if "iat" in payload:
            payload["iat_readable"] = datetime.fromtimestamp(
                payload["iat"], tz=timezone.utc
            ).isoformat()
        if "exp" in payload:
            payload["exp_readable"] = datetime.fromtimestamp(
                payload["exp"], tz=timezone.utc
            ).isoformat()
            if payload["exp"] < time.time():
                payload["is_expired"] = True
                payload["expired_since"] = (
                    f"{int(time.time() - payload['exp'])} seconds ago"
                )
            else:
                payload["is_expired"] = False
                payload["expires_in"] = f"{int(payload['exp'] - time.time())} seconds"

        return {"header": header, "payload": payload}
    except Exception as e:
        return {"error": f"Failed to decode JWT: {str(e)}"}


def check_api_permissions(token: str) -> dict:
    """Check API key permissions by attempting to get user info."""
    try:
        r = requests.get(
            f"{BASE}/v1/userInvitations",
            headers={"Authorization": f"Bearer {token}"},
            params={"limit": 1},
            timeout=60,
        )

        logging.debug(f"User invitations response: {r.status_code}")
        logging.debug(f"Response headers: {dict(r.headers)}")

        if r.status_code == 200:
            return {"can_read_users": True, "status_code": r.status_code}
        elif r.status_code == 403:
            return {
                "can_read_users": False,
                "status_code": r.status_code,
                "error": "Forbidden - insufficient permissions",
            }
        else:
            return {
                "can_read_users": False,
                "status_code": r.status_code,
                "error": r.text,
            }

    except Exception as e:
        return {"error": f"Failed to check permissions: {str(e)}"}


def test_api_key_role(token: str) -> dict:
    """Test various endpoints to determine API key role and permissions."""
    results = {"likely_role": "Unknown", "permissions": {}, "endpoints_tested": []}

    # Define endpoints that require different permission levels
    endpoints = [
        # Basic read access (all roles)
        {
            "path": "/v1/apps",
            "method": "GET",
            "params": {"limit": 1},
            "permission": "Read Apps",
            "required_role": "Any",
        },
        # User management (Admin/Account Holder)
        {
            "path": "/v1/users",
            "method": "GET",
            "params": {"limit": 1},
            "permission": "Manage Users",
            "required_role": "Admin/Account Holder",
        },
        {
            "path": "/v1/userInvitations",
            "method": "GET",
            "params": {"limit": 1},
            "permission": "User Invitations",
            "required_role": "Admin/Account Holder",
        },
        # Financial data (Finance/Admin/Account Holder)
        {
            "path": "/v1/financeReports",
            "method": "GET",
            "params": {"limit": 1},
            "permission": "Finance Reports",
            "required_role": "Finance/Admin/Account Holder",
        },
        {
            "path": "/v1/salesReports",
            "method": "GET",
            "params": {"limit": 1},
            "permission": "Sales Reports",
            "required_role": "Sales/Finance/Admin/Account Holder",
        },
        # Analytics (specific permissions needed for create/delete)
        {
            "path": "/v1/analyticsReportRequests",
            "method": "GET",
            "params": {"limit": 1},
            "permission": "List Analytics Requests",
            "required_role": "Any with app access",
        },
    ]

    for endpoint in endpoints:
        try:
            r = requests.request(
                endpoint["method"],
                f"{BASE}{endpoint['path']}",
                headers={"Authorization": f"Bearer {token}"},
                params=endpoint.get("params", {}),
                timeout=30,
            )

            status = r.status_code
            endpoint_result = {
                "status": status,
                "accessible": status == 200,
                "permission": endpoint["permission"],
                "required_role": endpoint["required_role"],
            }

            if status == 403:
                try:
                    error_data = r.json()
                    if "errors" in error_data and error_data["errors"]:
                        endpoint_result["error"] = error_data["errors"][0].get(
                            "detail", "Access denied"
                        )
                except:
                    endpoint_result["error"] = "Access denied"

            results["endpoints_tested"].append(endpoint_result)
            results["permissions"][endpoint["permission"]] = endpoint_result[
                "accessible"
            ]

        except Exception as e:
            results["endpoints_tested"].append(
                {
                    "permission": endpoint["permission"],
                    "accessible": False,
                    "error": str(e),
                }
            )
            results["permissions"][endpoint["permission"]] = False

    # Determine likely role based on permissions
    can_manage_users = results["permissions"].get("Manage Users", False)
    can_read_finance = results["permissions"].get("Finance Reports", False)
    can_read_sales = results["permissions"].get("Sales Reports", False)
    can_read_apps = results["permissions"].get("Read Apps", False)

    if can_manage_users:
        results["likely_role"] = "Admin or Account Holder"
    elif can_read_finance and can_read_sales:
        results["likely_role"] = "Finance"
    elif can_read_sales:
        results["likely_role"] = "Sales"
    elif can_read_apps:
        results["likely_role"] = "Developer or App Manager"
    else:
        results["likely_role"] = "Unknown or Invalid"

    return results


def log_token_info(token: str, logger: logging.Logger) -> None:
    """Log debug information about the JWT token."""
    decoded = decode_jwt_without_verification(token)

    if "error" in decoded:
        logger.error(f"Token decode error: {decoded['error']}")
    else:
        logger.info(
            f"Token Header: {json.dumps(decoded['header'], separators=(',', ':'))}"
        )
        logger.info(
            f"Token Payload: {json.dumps(decoded['payload'], separators=(',', ':'))}"
        )


# -------------------- JWT --------------------
def make_token(issuer: str, key_id: str, p8_path: str, ttl: int = 1200) -> str:
    """Create short-lived JWT for App Store Connect API (ES256)."""
    try:
        # Check file permissions (should not be world-readable)
        if os.stat(p8_path).st_mode & 0o077:
            print(
                f"[WARNING] Private key file {p8_path} has overly permissive permissions",
                file=sys.stderr,
            )

        with open(p8_path, "r", encoding="utf-8") as f:
            private_key = f.read()
    except FileNotFoundError:
        print(f"[ERROR] Private key file not found: {p8_path}", file=sys.stderr)
        sys.exit(2)
    except PermissionError:
        print(
            f"[ERROR] Permission denied reading private key file: {p8_path}",
            file=sys.stderr,
        )
        sys.exit(2)
    now = int(time.time())
    return jwt.encode(
        {"iss": issuer, "iat": now, "exp": now + ttl, "aud": "appstoreconnect-v1"},
        private_key,
        algorithm="ES256",
        headers={"kid": key_id, "typ": "JWT"},
    )


# -------------------- HTTP helpers --------------------
def asc_get(
    path: str, token: str, params: dict | None = None, max_retries: int = 3
) -> dict:
    for attempt in range(max_retries):
        try:
            r = requests.get(
                f"{BASE}{path}",
                headers={"Authorization": f"Bearer {token}"},
                params=params or {},
                timeout=60,
            )
            if r.status_code == 403:
                print(
                    f"[ERROR] 403 Forbidden for GET {path}. Check API key role (Admin needed for create/delete), app access, and team.",
                    file=sys.stderr,
                )
                sys.exit(3)
            r.raise_for_status()
            logging.debug("GET %s params=%s -> %s", path, params or {}, r.status_code)
            logging.debug("Response body: %s", r.text)
            return r.json()
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt < max_retries - 1:
                wait_time = 2**attempt  # Exponential backoff
                print(
                    f"[WARNING] GET {path} failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s..."
                )
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
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=60,
            )
            if r.status_code == 403:
                print(
                    f"[ERROR] 403 Forbidden for POST {path}. Admin (or Account Holder) role required to create requests.",
                    file=sys.stderr,
                )
                sys.exit(3)
            r.raise_for_status()
            logging.debug("POST %s payload=%s -> %s", path, payload, r.status_code)
            logging.debug("Response body: %s", r.text)
            return r.json()
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt < max_retries - 1:
                wait_time = 2**attempt  # Exponential backoff
                print(
                    f"[WARNING] POST {path} failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s..."
                )
                time.sleep(wait_time)
            else:
                raise
    # This should never be reached due to the raise in the else clause, but for type safety
    raise RuntimeError(f"Failed to POST {path} after {max_retries} attempts")


def asc_delete(path: str, token: str, max_retries: int = 3) -> None:
    for attempt in range(max_retries):
        try:
            r = requests.delete(
                f"{BASE}{path}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=60,
            )
            if r.status_code == 403:
                print(
                    f"[ERROR] 403 Forbidden for DELETE {path}. Admin (or Account Holder) role required to delete requests.",
                    file=sys.stderr,
                )
                sys.exit(3)
            if r.status_code not in (200, 202, 204):
                r.raise_for_status()
            logging.debug("DELETE %s -> %s", path, r.status_code)
            logging.debug("Response body: %s", r.text)
            return
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt < max_retries - 1:
                wait_time = 2**attempt  # Exponential backoff
                print(
                    f"[WARNING] DELETE {path} failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s..."
                )
                time.sleep(wait_time)
            else:
                raise


# -------------------- Extra helpers (pagination, links, debug) --------------------
IS_DEBUG = False


def dbg(msg: str) -> None:
    logging.debug(msg)


def asc_get_any(
    url_or_path: str, token: str, params: dict | None = None, max_retries: int = 3
) -> dict:
    """GET that accepts either relative API path ('/v1/...') or absolute URL (links.next)."""
    if url_or_path.startswith("http"):
        for attempt in range(max_retries):
            try:
                r = requests.get(
                    url_or_path,
                    headers={"Authorization": f"Bearer {token}"},
                    params=params or {},
                    timeout=60,
                )
                if r.status_code == 403:
                    print(
                        f"[ERROR] 403 Forbidden for GET {url_or_path}. Check API key role (Admin needed for create/delete), app access, and team.",
                        file=sys.stderr,
                    )
                    sys.exit(3)
                r.raise_for_status()
                logging.debug(
                    "GET %s params=%s -> %s", url_or_path, params or {}, r.status_code
                )
                logging.debug("Response body: %s", r.text)
                return r.json()
            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt < max_retries - 1:
                    wait_time = 2**attempt  # Exponential backoff
                    print(
                        f"[WARNING] GET {url_or_path} failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    raise
        raise RuntimeError(f"Failed to GET {url_or_path} after {max_retries} attempts")
    else:
        return asc_get(url_or_path, token, params=params, max_retries=max_retries)


def fetch_all(url_or_path: str, token: str, params: dict | None = None) -> list[dict]:
    """Fetch all pages following links.next, accumulating 'data' list."""
    items: list[dict] = []
    next_url = url_or_path
    next_params = params or {}
    while True:
        dbg(f"GET {next_url} params={next_params}")
        resp = asc_get_any(next_url, token, params=next_params)
        data = resp.get("data") or []
        items.extend(data)
        links = resp.get("links") or {}
        next_link = links.get("next")
        if not next_link:
            break
        dbg(f"Following next: {next_link}")
        next_url = next_link
        next_params = None  # 'next' is a fully qualified URL with params
    return items


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


def list_available_reports_for_app(
    app_id: str, token: str, date_from: str | None = None, date_to: str | None = None
) -> Iterator[dict]:
    """Yield available analytics reports for the given app with detailed request and segment information."""
    # First get report requests with full details
    report_requests = list_requests_for_app(app_id, token)

    for request in report_requests:
        request_id = request["id"]
        request_attrs = request.get("attributes") or {}
        # Store request attributes for later use
        request_info = {
            "created_date": request_attrs.get("createdDate", ""),
            "stopped": request_attrs.get("stoppedDueToInactivity", False),
            "access_type": request_attrs.get("accessType", ""),
        }
        try:
            # Get reports for this request via relationship link (fallback to fixed path)
            rel = (request.get("relationships") or {}).get("reports") or {}
            dbg(
                f"Request relationships keys: {list((request.get('relationships') or {}).keys())}"
            )
            dbg(f"Reports rel keys: {list((rel or {}).keys())}")
            dbg(
                f"Reports rel.links keys: {list(((rel or {}).get('links') or {}).keys())}"
            )
            related_url = (rel.get("links") or {}).get(
                "related"
            ) or f"/v1/analyticsReportRequests/{request_id}/reports"
            dbg(f"Reports related URL for request {request_id}: {related_url}")
            reports = fetch_all(related_url, token, params={"limit": 200})
            report_iter_count = 0
            for report in reports:
                report_attrs = report.get("attributes") or {}

                # Get report instances for this report
                report_segments: list[dict] = []
                try:
                    # Get the instances for this report via relationship link (fallback to fixed path)
                    rep_rels = report.get("relationships") or {}
                    dbg(
                        f"Report {report.get('id', '?')} relationships keys: {list(rep_rels.keys())}"
                    )
                    rrel = rep_rels.get("instances") or {}
                    dbg(f"Report instances rel keys: {list((rrel or {}).keys())}")
                    dbg(
                        f"Report instances rel.links keys: {list(((rrel or {}).get('links') or {}).keys())}"
                    )
                    instances_url = (rrel.get("links") or {}).get(
                        "related"
                    ) or f"/v1/analyticsReports/{report['id']}/instances"
                    dbg(
                        f"Instances URL for report {report.get('id', '?')}: {instances_url}"
                    )
                    # Apply API-side date filters only if provided via CLI; otherwise, request without date filters.
                    # If start == end, prefer filtering by processingDate for exact-day match.
                    params = {"limit": 200}
                    if date_from or date_to:
                        if date_from and date_to and date_from == date_to:
                            params["filter[processingDate]"] = date_from
                        else:
                            if date_from:
                                params["filter[startDate]"] = date_from
                            if date_to:
                                params["filter[endDate]"] = date_to
                    try:
                        instances = fetch_all(instances_url, token, params=params)
                    except requests.HTTPError as he:
                        status = (
                            he.response.status_code if he.response is not None else None
                        )
                        # On 400 for processingDate, fall back to startDate/endDate; on 400 for start/end, retry without filters.
                        if status == 400 and params.get("filter[processingDate]"):
                            dbg(
                                "400 from instances with processingDate filter; retrying with startDate/endDate"
                            )
                            params.pop("filter[processingDate]", None)
                            params["filter[startDate]"] = date_from
                            params["filter[endDate]"] = date_to or date_from
                            instances = fetch_all(instances_url, token, params=params)
                        elif status == 400 and (date_from or date_to):
                            dbg(
                                "400 from instances with start/end filters; retrying without filters"
                            )
                            instances = fetch_all(
                                instances_url, token, params={"limit": 100}
                            )
                        else:
                            raise

                    # In debug mode, skip empty-instance reports after filtering; stop after first non-empty
                    if IS_DEBUG and not instances:
                        dbg(
                            "Debug: report has empty instances after date filter, continue searching..."
                        )
                        continue

                    # For each instance, get the segments
                    instance_iter_count = 0
                    for instance_data in instances:
                        instance_id = instance_data["id"]
                        instance_attrs = instance_data.get("attributes") or {}
                        try:
                            inst_rels = instance_data.get("relationships") or {}
                            dbg(
                                f"Instance {instance_id} relationships keys: {list(inst_rels.keys())}"
                            )
                            srel = inst_rels.get("segments") or {}
                            dbg(
                                f"Instance segments rel keys: {list((srel or {}).keys())}"
                            )
                            dbg(
                                f"Instance segments rel.links keys: {list(((srel or {}).get('links') or {}).keys())}"
                            )
                            segments_url = (srel.get("links") or {}).get(
                                "related"
                            ) or f"/v1/analyticsReportInstances/{instance_id}/segments"
                            dbg(
                                f"Segments URL for instance {instance_id}: {segments_url}"
                            )
                            segments = fetch_all(
                                segments_url, token, params={"limit": 100}
                            )

                            # Process segments for this instance
                            for segment_data in segments:
                                seg_attrs = segment_data.get("attributes") or {}
                                report_segments.append(
                                    {
                                        "segment_id": segment_data["id"],
                                        "start_date": seg_attrs.get("startDate", ""),
                                        "end_date": seg_attrs.get("endDate", ""),
                                        "instance_id": instance_id,
                                        "instance_processing_date": instance_attrs.get(
                                            "processingDate", ""
                                        ),
                                        "instance_granularity": instance_attrs.get(
                                            "granularity", ""
                                        ),
                                        "attributes": seg_attrs,
                                    }
                                )
                            instance_iter_count += 1
                        except Exception as e:
                            # Log instance-level errors but continue
                            print(
                                f"[INFO] Could not access segments for instance {instance_id}: {e}"
                            )

                except Exception as e:
                    # Log any errors but continue processing other reports
                    print(
                        f"[INFO] Could not access instances for report {report.get('id', '?')}: {e}"
                    )

                report_info = {
                    "request_id": request_id,
                    "report_id": report["id"],
                    "name": report_attrs.get("name", ""),
                    "category": report_attrs.get("category", ""),
                    "report_type": report_attrs.get(
                        "reportType",
                        report_attrs.get(
                            "granularity", report_attrs.get("frequency", "")
                        ),
                    ),
                    "request_created_date": request_info.get("created_date", ""),
                    "request_stopped": request_info.get("stopped", False),
                    "request_access_type": request_info.get("access_type", ""),
                    "segments": report_segments,
                }

                # In debug mode: stop processing as soon as we encounter a report with non-empty instances (segments collected)
                if IS_DEBUG:
                    if report_segments or instances:
                        yield report_info
                        return
                    # If both instances and segments ended up empty, skip yielding in debug
                    continue

                # Normal mode: yield every report (even if no segments found yet)
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
    pattern = r"^[a-zA-Z0-9.-]+$"
    return bool(re.match(pattern, bundle_id))


# -------------------- Configuration --------------------
MAX_BUNDLES = 50  # Maximum number of bundle IDs to process at once


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
    """Print available reports one at a time with complete information including segments."""
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
        report_type = report.get("report_type", "")
        if report_type:
            print(f"      Type: {report_type}")
        print(f"      Report ID: {report.get('report_id', '')}")

        # Print segments information grouped by instance
        segments = report.get("segments", [])
        if segments:
            # Group segments by instance
            instances_map = {}
            for seg in segments:
                iid = seg.get("instance_id", "")
                inst_entry = instances_map.setdefault(
                    iid,
                    {
                        "processing_date": seg.get("instance_processing_date", ""),
                        "granularity": seg.get("instance_granularity", ""),
                        "segments": [],
                    },
                )
                inst_entry["segments"].append(seg)

            print(f"      🧩 Instances:")
            for iid, inst in instances_map.items():
                print(f"        ▸ Instance {iid}")
                if inst.get("processing_date"):
                    print(f"          Processing Date: {inst.get('processing_date')}")
                if inst.get("granularity"):
                    print(f"          Granularity: {inst.get('granularity')}")
                segs = inst.get("segments") or []
                if segs:
                    print(f"          📑 Segments:")
                    for s in segs:
                        print(f"            • Segment")
                        print(f"              ID: {s.get('segment_id', '')}")
                else:
                    print(f"          📭 No segments for this instance")
        else:
            print(f"      📭 No segments available (report may be processing)")

        report_count += 1
        print(f"\n    {'─' * 40}")

    print(f"\n📈 Total reports processed: {report_count}")
    print("=" * 80)


def collect_requests_snapshot(
    bundles: List[str], token: str
) -> List[Tuple[str, str, str, str]]:
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
            if any(
                sensitive in error_msg.lower()
                for sensitive in ["token", "key", "secret", "auth"]
            ):
                error_msg = "Authentication error"
            snapshot.append((b, "-", "-", f"ERROR: {error_msg}"))
    return snapshot


def collect_reports_snapshot(
    bundles: List[str],
    token: str,
    date_from: str | None = None,
    date_to: str | None = None,
) -> Iterator[dict]:
    """Yield available reports for all bundles progressively."""
    for b in bundles:
        try:
            app_id, app_name = get_app(b, token)
            for report in list_available_reports_for_app(
                app_id, token, date_from, date_to
            ):
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
                "segments": [],
            }


def extract_403_error_details(response_text: str) -> dict:
    """Extract detailed error information from a 403 response."""
    try:
        error_json = json.loads(response_text)
        if "errors" in error_json:
            errors = []
            for error in error_json["errors"]:
                errors.append(
                    {
                        "code": error.get("code", "Unknown"),
                        "title": error.get("title", "Unknown"),
                        "detail": error.get("detail", "No details"),
                        "source": error.get("source", {}),
                    }
                )
            return {"errors": errors}
    except:
        pass
    return {"raw_response": response_text[:500]}


def run_api_diagnostics(token: str, bundles: list[str]) -> None:
    """Run API diagnostics and display results."""
    # Create a dedicated logger for diagnostics - avoid duplicates
    diag_logger = logging.getLogger("diagnostics")
    diag_logger.handlers.clear()  # Clear any existing handlers to avoid duplicates
    diag_logger.setLevel(logging.INFO)
    diag_logger.propagate = False  # Don't propagate to root logger

    # Create console handler with custom format
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    diag_logger.addHandler(handler)

    diag_logger.info("🔧 App Store Connect API Diagnostics")
    diag_logger.info("=" * 60)

    # Track diagnostic results for summary
    diagnostic_results = {
        "token_valid": False,
        "token_expired": False,
        "api_accessible": False,
        "role": "Unknown",
        "can_create_analytics": False,
        "bundles_accessible": [],
        "bundles_inaccessible": [],
        "issues": [],
    }

    # Analyze JWT token
    diag_logger.info("📋 JWT Token Analysis:")
    decoded = decode_jwt_without_verification(token)
    if "error" not in decoded:
        diagnostic_results["token_valid"] = True
        if decoded["payload"].get("is_expired"):
            diagnostic_results["token_expired"] = True
            diagnostic_results["issues"].append("JWT token is expired")
        # Log token info inline
        diag_logger.info(
            f"Token Header: {json.dumps(decoded['header'], separators=(',', ':'))}"
        )
        diag_logger.info(
            f"Token Payload: {json.dumps(decoded['payload'], separators=(',', ':'))}"
        )
    else:
        diagnostic_results["issues"].append(f"Invalid JWT token: {decoded['error']}")
        diag_logger.error(f"Token decode error: {decoded['error']}")

    # Check permissions
    diag_logger.info("🔑 Checking API Permissions:")
    permissions = check_api_permissions(token)
    diag_logger.info(
        f"Permission check result: {json.dumps(permissions, separators=(',', ':'))}"
    )

    # Test API key role
    diag_logger.info("👤 Testing API Key Role:")
    role_test = test_api_key_role(token)
    diagnostic_results["role"] = role_test["likely_role"]
    diag_logger.info(f"Likely API key role: {role_test['likely_role']}")
    for test in role_test["endpoints_tested"]:
        status_symbol = "✅" if test["accessible"] else "❌"
        diag_logger.info(
            f"{status_symbol} {test['permission']}: {'Accessible' if test['accessible'] else test.get('error', 'Not accessible')}"
        )

    if (
        "Admin" in role_test["likely_role"]
        or "Account Holder" in role_test["likely_role"]
    ):
        diagnostic_results["can_create_analytics"] = True
    else:
        diagnostic_results["issues"].append("API key lacks Admin/Account Holder role")
        diag_logger.warning(
            "⚠️ Your API key does not appear to have Admin or Account Holder role - you will NOT be able to create/delete analytics requests"
        )

    # Test access to apps
    diag_logger.info("📱 Testing App Access:")
    try:
        # Make raw request to catch 403 details
        r = requests.get(
            f"{BASE}/v1/apps",
            headers={"Authorization": f"Bearer {token}"},
            params={"limit": 5},
            timeout=60,
        )
        if r.status_code == 403:
            diag_logger.error("❌ 403 Forbidden when accessing /v1/apps")
            error_details = extract_403_error_details(r.text)
            if "errors" in error_details:
                for err in error_details["errors"]:
                    diag_logger.error(
                        f"Error [{err['code']}] {err['title']}: {err['detail']}"
                    )
            else:
                diag_logger.error(
                    f"Response: {error_details.get('raw_response', r.text[:500])}"
                )
            diag_logger.error(
                "This usually means: API key revoked/invalid, no app access, or wrong team/organization"
            )
        elif r.status_code == 200:
            diagnostic_results["api_accessible"] = True
            test_response = r.json()
            if test_response.get("data"):
                diag_logger.info(
                    f"✅ Successfully accessed API. Found {len(test_response['data'])} apps"
                )
                for app in test_response["data"]:
                    attrs = app.get("attributes", {})
                    diag_logger.info(
                        f"  • {attrs.get('name', 'Unknown')} ({attrs.get('bundleId', 'Unknown')})"
                    )
        else:
            diag_logger.error(
                f"Unexpected status code {r.status_code} when accessing /v1/apps"
            )
            diag_logger.error(f"Response: {r.text[:500]}")
    except requests.RequestException as e:
        diag_logger.error(f"Failed to list apps: {e}")
    except Exception as e:
        diag_logger.error(f"Unexpected error: {e}")

    # Test specific bundles
    if bundles:
        diag_logger.info("🔍 Testing Access to Specified Bundles:")
        for bundle in bundles[:3]:  # Test first 3 bundles
            try:
                # Make raw request to catch 403 details
                r = requests.get(
                    f"{BASE}/v1/apps",
                    headers={"Authorization": f"Bearer {token}"},
                    params={"filter[bundleId]": bundle, "limit": 1},
                    timeout=60,
                )
                if r.status_code == 403:
                    diagnostic_results["bundles_inaccessible"].append(bundle)
                    error_details = extract_403_error_details(r.text)
                    if "errors" in error_details:
                        err_msg = " | ".join(
                            [err["detail"] for err in error_details["errors"]]
                        )
                        diag_logger.error(f"❌ {bundle}: 403 Forbidden - {err_msg}")
                    else:
                        diag_logger.error(f"❌ {bundle}: 403 Forbidden")
                elif r.status_code == 200:
                    data = r.json()
                    apps = data.get("data", [])
                    if apps:
                        app = apps[0]
                        app_id = app["id"]
                        app_name = app.get("attributes", {}).get("name", "Unknown")
                        diagnostic_results["bundles_accessible"].append(bundle)
                        diag_logger.info(
                            f"✅ {bundle}: Found app '{app_name}' (ID: {app_id})"
                        )

                        # Check if we can list analytics requests
                        try:
                            r2 = requests.get(
                                f"{BASE}/v1/apps/{app_id}/analyticsReportRequests",
                                headers={"Authorization": f"Bearer {token}"},
                                params={"filter[accessType]": "ONGOING", "limit": 200},
                                timeout=60,
                            )
                            if r2.status_code == 403:
                                error_details = extract_403_error_details(r2.text)
                                if "errors" in error_details:
                                    err_msg = " | ".join(
                                        [
                                            err["detail"]
                                            for err in error_details["errors"]
                                        ]
                                    )
                                    diag_logger.warning(
                                        f"  ⚠️ Cannot list analytics requests: {err_msg} (need Admin role)"
                                    )
                                else:
                                    diag_logger.warning(
                                        f"  ⚠️ Cannot list analytics requests: 403 Forbidden (need Admin role)"
                                    )
                            elif r2.status_code == 200:
                                reqs = r2.json().get("data", [])
                                diag_logger.info(
                                    f"  ✅ Can list analytics requests: YES ({len(reqs)} existing)"
                                )
                            else:
                                diag_logger.warning(
                                    f"     Unexpected status {r2.status_code} for analytics requests"
                                )
                        except Exception as e:
                            diag_logger.warning(
                                f"     Cannot check analytics requests: {e}"
                            )
                    else:
                        diagnostic_results["bundles_inaccessible"].append(bundle)
                        diag_logger.warning(
                            f"⚠️ {bundle}: No app found with this bundle ID"
                        )
                else:
                    diag_logger.error(
                        f"  ❌ {bundle}: Unexpected status code {r.status_code}"
                    )
            except requests.RequestException as e:
                diag_logger.error(f"  ❌ {bundle}: Connection error: {e}")
            except Exception as e:
                diag_logger.error(f"  ❌ {bundle}: {e}")

    # Generate comprehensive summary and recommendations
    diag_logger.info("=" * 60)
    diag_logger.info("📊 DIAGNOSTICS SUMMARY")

    # Overall status
    if diagnostic_results["token_expired"]:
        diag_logger.error("❌ CRITICAL: JWT token is expired")
    elif not diagnostic_results["token_valid"]:
        diag_logger.error("❌ CRITICAL: JWT token is invalid")
    elif not diagnostic_results["api_accessible"]:
        diag_logger.error("❌ CRITICAL: Cannot access App Store Connect API")
    elif (
        not diagnostic_results["can_create_analytics"]
        and not diagnostic_results["bundles_accessible"]
    ):
        diag_logger.error("❌ FAILED: No access to apps and insufficient permissions")
    elif not diagnostic_results["can_create_analytics"]:
        diag_logger.warning(
            "⚠️  WARNING: Limited access - cannot manage analytics requests"
        )
    else:
        diag_logger.info("✅ SUCCESS: API access is properly configured")

    # Details
    diag_logger.info("📋 Status Details:")
    diag_logger.info(
        f"Token Status: {'✅ Valid' if diagnostic_results['token_valid'] else '❌ Invalid'} | API Access: {'✅ Working' if diagnostic_results['api_accessible'] else '❌ Not working'} | Role: {diagnostic_results['role']} | Can Create/Delete: {'✅ Yes' if diagnostic_results['can_create_analytics'] else '❌ No'}"
    )

    if (
        diagnostic_results["bundles_accessible"]
        or diagnostic_results["bundles_inaccessible"]
    ):
        diag_logger.info("📱 Bundle Access:")
        if diagnostic_results["bundles_accessible"]:
            diag_logger.info(
                f"✅ Accessible: {', '.join(diagnostic_results['bundles_accessible'])}"
            )
        if diagnostic_results["bundles_inaccessible"]:
            diag_logger.info(
                f"❌ Not accessible: {', '.join(diagnostic_results['bundles_inaccessible'])}"
            )

    # Recommendations
    if diagnostic_results["issues"]:
        diag_logger.info("💡 RECOMMENDATIONS TO FIX ISSUES:")

        if diagnostic_results["token_expired"]:
            diag_logger.info(
                "1. Your token has expired - simply re-run the command to regenerate"
            )

        elif not diagnostic_results["token_valid"]:
            diag_logger.info(
                "1. Check credentials: Issuer ID format (UUID), Key ID (10 chars), .p8 file exists and valid"
            )

        elif not diagnostic_results["api_accessible"]:
            diag_logger.info(
                "1. Verify API key is active in App Store Connect → Users and Access → Keys"
            )
            diag_logger.info("2. Ensure correct team/organization and key not revoked")

        if "API key lacks Admin/Account Holder role" in diagnostic_results["issues"]:
            diag_logger.info(
                "📝 For analytics management: Create new API key with 'Admin' role in App Store Connect"
            )

        if diagnostic_results["bundles_inaccessible"]:
            diag_logger.info(
                "📱 For inaccessible bundles: Verify bundle IDs, check API key access, confirm team/organization"
            )
    else:
        diag_logger.info(
            "✅ No issues detected. Your configuration appears to be correct!"
        )
        if not diagnostic_results["can_create_analytics"]:
            diag_logger.info(
                "Note: Read-only access - cannot create/delete analytics requests (fine for --list flag)"
            )

    diag_logger.info("=" * 60)


# -------------------- Main --------------------
def main():
    ap = argparse.ArgumentParser(
        description="Create/Delete/List App Store Connect Analytics ONGOING report requests for multiple apps."
    )
    ap.add_argument(
        "--issuer",
        default=os.environ.get("APPSTORE_EXPORTER_ISSUER_ID"),
        required=not os.environ.get("APPSTORE_EXPORTER_ISSUER_ID"),
        help="Issuer ID (Users and Access → Integrations). Default: APPSTORE_EXPORTER_ISSUER_ID env var",
    )
    ap.add_argument(
        "--key-id",
        default=os.environ.get("APPSTORE_EXPORTER_KEY_ID"),
        required=not os.environ.get("APPSTORE_EXPORTER_KEY_ID"),
        help="Key ID for the .p8. Default: APPSTORE_EXPORTER_KEY_ID env var",
    )
    ap.add_argument(
        "--p8",
        default=os.environ.get("APPSTORE_EXPORTER_PRIVATE_KEY"),
        required=not os.environ.get("APPSTORE_EXPORTER_PRIVATE_KEY"),
        help="Path to AuthKey_XXXXXX.p8. Default: APPSTORE_EXPORTER_PRIVATE_KEY env var",
    )
    ap.add_argument(
        "--bundles",
        default=os.environ.get("APPSTORE_EXPORTER_BUNDLE_IDS"),
        required=not os.environ.get("APPSTORE_EXPORTER_BUNDLE_IDS"),
        help="Comma-separated bundle IDs, e.g. com.app.one,com.app.two. Default: APPSTORE_EXPORTER_BUNDLE_IDS env var",
    )
    ap.add_argument(
        "--create", action="store_true", help="Create ONGOING requests (default action)"
    )
    ap.add_argument(
        "--delete", action="store_true", help="Delete existing ONGOING requests"
    )
    ap.add_argument(
        "--list", action="store_true", help="List existing ONGOING requests and exit"
    )

    ap.add_argument(
        "--start",
        "--from",
        dest="date_from",
        help="Start date (YYYY-MM-DD) filter for report instances (uses filter[processingDate] when equal to --end)",
    )
    ap.add_argument(
        "--end",
        "--to",
        dest="date_to",
        help="End date (YYYY-MM-DD) filter for report instances (uses filter[processingDate] when equal to --start)",
    )
    ap.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging and iterate reports until the first report with non-empty instances/segments, then stop.",
    )
    ap.add_argument(
        "--diagnose",
        action="store_true",
        help="Run API diagnostics to check permissions and access levels, then exit.",
    )
    args = ap.parse_args()

    # Decide action (default to --create if nothing specified)
    action_create = args.create
    action_delete = args.delete
    action_list = args.list
    if not any([action_create, action_delete, action_list]):
        action_create = True

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Configure debug mode (in debug mode we iterate until first report with non-empty instances/segments and then stop)
    global IS_DEBUG
    IS_DEBUG = args.debug

    # Parse and validate bundles
    bundles = [b.strip() for b in args.bundles.split(",") if b.strip()]
    if not bundles:
        print("[ERROR] --bundles must contain at least one bundleId", file=sys.stderr)
        sys.exit(1)

    # Check maximum bundle limit
    if len(bundles) > MAX_BUNDLES:
        print(
            f"[ERROR] Too many bundle IDs ({len(bundles)}). Maximum allowed is {MAX_BUNDLES}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Validate bundle IDs
    invalid_bundles = [b for b in bundles if not validate_bundle_id(b)]
    if invalid_bundles:
        print(f"[ERROR] Invalid bundle ID format: {invalid_bundles}", file=sys.stderr)
        print(
            "Bundle IDs should contain only alphanumeric characters, dots, and hyphens",
            file=sys.stderr,
        )
        sys.exit(1)

    # Build token
    try:
        token = make_token(args.issuer, args.key_id, args.p8)
    except Exception as e:
        print(f"[ERROR] Cannot create JWT: {e}", file=sys.stderr)
        sys.exit(2)

    # If diagnose flag is set, run diagnostics and exit
    if args.diagnose:
        run_api_diagnostics(token, bundles)
        sys.exit(0)

    if action_list:
        # Show current requests first, then available reports
        print("\n📋 Current ONGOING requests:")
        requests_snapshot = collect_requests_snapshot(bundles, token)
        print_requests_table("", requests_snapshot)

        # List available reports and exit
        reports = collect_reports_snapshot(bundles, token, args.date_from, args.date_to)
        print_reports_table("Available Analytics Reports:", reports)
        return

    # Snapshot BEFORE only for create/delete operations
    if action_create or action_delete:
        print("\n⚠️  Showing current ONGOING requests BEFORE operation:")
        before = collect_requests_snapshot(bundles, token)
        print_requests_table("Current ONGOING requests (BEFORE):", before)

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
                    print(
                        f"[INFO] {b}: ONGOING request already exists: {ids} — skipping create"
                    )
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
                            status = (
                                he.response.status_code
                                if he.response is not None
                                else "?"
                            )
                            # Avoid exposing sensitive information in error messages
                            error_msg = str(he)
                            if (
                                "detail" in error_msg.lower()
                                or "token" in error_msg.lower()
                            ):
                                error_msg = (
                                    "API error occurred (details hidden for security)"
                                )
                            print(
                                f"[ERROR] {b}: failed to delete {rid} (HTTP {status}): {error_msg}"
                            )

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
            if any(
                sensitive in error_msg.lower()
                for sensitive in ["token", "key", "secret", "auth"]
            ):
                error_msg = "Authentication error occurred"
            print(f"[ERROR] {b}: {error_msg}")

    # Snapshot AFTER only for create/delete operations
    if action_create or action_delete:
        print("\n⚠️  Showing current ONGOING requests AFTER operation:")
        after = collect_requests_snapshot(bundles, token)
        print_requests_table("Current ONGOING requests (AFTER):", after)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user (Ctrl+C)", file=sys.stderr)
        sys.exit(130)
