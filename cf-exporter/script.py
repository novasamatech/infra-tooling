#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

import logging
import traceback
import io
import os
import time
import sys
import signal
from datetime import datetime, timezone
from typing import List, Dict, Optional
import requests
from prometheus_client import start_http_server, Gauge

# Prometheus metric definition
metrics = {
    'visits_counter': Gauge(
        'cf_visits',
        'Total visits since midnight UTC',
        [
            'zone_name',
            'host_name',
            'client_country_name',
            'client_request_referer',
            'user_agent_browser',
            'user_agent_os'
        ]
    ),
    'requests_counter': Gauge(
        'cf_requests',
        'Total requests since midnight UTC',
        [
            'zone_name',
            'host_name',
            'method_name',
            "path",
            "query",
            'client_country_name',
            'client_request_referer',
            'user_agent_browser',
            'user_agent_os',
            'cache_status',
            'origin_response_status'
        ]
    )
}

def handle_exceptions(func):
    '''Decorator that handles all exceptions.'''

    def wrap(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f'{func.__name__} function raised the exception, error: "{e}"')
            tb_output = io.StringIO()
            traceback.print_tb(e.__traceback__, file=tb_output)
            logging.debug(f'{func.__name__} function raised the exception, '
                          f'traceback:\n{tb_output.getvalue()}')
            tb_output.close()
            return None
    return wrap

class CloudflareAPI:
    """
    Manages Cloudflare API sessions and queries.
    """
    API_BASE_URL = "https://api.cloudflare.com/client/v4"
    GRAPHQL_URL = f"{API_BASE_URL}/graphql"

    def __init__(self, api_token: str, request_timeout=30):
        # Initialize HTTP session with headers
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        })
        self.request_timeout = request_timeout

    @handle_exceptions
    def graphql_query(self, query: str) -> Optional[Dict]:
        """Execute a GraphQL query via HTTP POST."""
        response = self.session.post(
            self.GRAPHQL_URL,
            json={'query': query},
            timeout=self.request_timeout
        )
        response.raise_for_status()
        return response.json()

    @handle_exceptions
    def list_zones(self) -> List[Dict]:
        """List all zones (paginated) using the REST API."""
        zones = []
        page = 1
        per_page = 50

        while True:

            response = self.session.get(
                f"{self.API_BASE_URL}/zones",
                params={
                    'page': page,
                    'per_page': per_page,
                    'order': 'name',
                    'direction': 'asc'
                },
                timeout=self.request_timeout
            )
            response.raise_for_status()
            data = response.json()

            if not data.get('success', False):
                logging.error(f"API error: {data.get('errors', 'Unknown error')}")
                break

            zones.extend(data.get('result', []))

            result_info = data.get('result_info', {})
            current_page = result_info.get('page', page)
            total_pages = result_info.get('total_pages', 1)

            if current_page >= total_pages:
                break
            page = current_page + 1

        return zones

@handle_exceptions
def get_visits_for_zone(api: CloudflareAPI, zone_id: str, zone_name: str) -> None:
    """
    Retrieve and record visit metrics for a specific zone since midnight.
    """
    now = datetime.now(timezone.utc)
    start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
    datetime_filter = {
        "geq": start_time.isoformat(),
        "lt": now.isoformat()
    }

    query = f"""
    query {{
      viewer {{
        zones(filter: {{ zoneTag: "{zone_id}" }}) {{
          httpRequestsAdaptiveGroups(
            limit: 10000,
            filter: {{
              datetime_geq: "{datetime_filter['geq']}",
              datetime_lt: "{datetime_filter['lt']}",
              clientRequestPath: "/"
            }}
          ) {{
            sum {{
              visits
            }}
            dimensions {{
              clientRequestHTTPHost,
              clientCountryName,
              clientRequestReferer,
              userAgentBrowser,
              userAgentOS
            }}
          }}
        }}
      }}
    }}
    """

    result = api.graphql_query(query)
    if not result:
        return

    errors = result.get('errors')
    if errors:
        error_messages = [
            f"{e.get('message')}"
            for e in errors
            if isinstance(e, dict)
        ]
        logging.error(f"GraphQL errors for {zone_name}: {', '.join(error_messages)}")
        return

    zones_data = result.get('data', {}).get('viewer', {}).get('zones', [])
    if not zones_data:
        logging.info(f"No data found for zone {zone_name}")
        return

    total_updates = 0

    for zone_data in zones_data:
        zone_groups = zone_data.get('httpRequestsAdaptiveGroups', [])
        for group in zone_groups:
            sum_data = group.get('sum', {})
            visits = sum_data.get('visits', 0)
            if visits == 0:
                continue
            dimensions = group.get('dimensions', {})
            labels = {}
            labels.update({"zone_name": zone_name})
            labels.update({"host_name": dimensions.get('clientRequestHTTPHost', 'unknown')})
            labels.update({"client_country_name": dimensions.get('clientCountryName', 'unknown')})
            labels.update({"client_request_referer": dimensions.get('clientRequestReferer', 'unknown')})
            labels.update({"user_agent_browser": dimensions.get('userAgentBrowser', 'unknown')})
            labels.update({"user_agent_os": dimensions.get('userAgentOS', 'unknown')})
            if labels['client_request_referer'] == '':
                labels['client_request_referer'] = 'direct'
            metrics['visits_counter'].labels(**labels).set(visits)
            total_updates += 1

    logging.info(
        "Zone %s processed visits: %d metrics, time range: %s to %s",
        zone_name,
        total_updates,
        datetime_filter['geq'],
        datetime_filter['lt']
    )

@handle_exceptions
def get_requests_for_zone(api: CloudflareAPI, zone_id: str, zone_name: str) -> None:
    """
    Retrieve and record requests metrics for a specific zone since midnight.
    """
    now = datetime.now(timezone.utc)
    start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
    datetime_filter = {
        "geq": start_time.isoformat(),
        "lt": now.isoformat()
    }

    query = f"""
    query {{
      viewer {{
        zones(filter: {{ zoneTag: "{zone_id}" }}) {{
          httpRequestsAdaptiveGroups(
            limit: 10000,
            filter: {{
              datetime_geq: "{datetime_filter['geq']}",
              datetime_lt: "{datetime_filter['lt']}"
            }}
          ) {{
            count
            dimensions {{
              clientRequestHTTPHost,
              clientRequestHTTPMethodName,
              clientRequestPath,
              clientRequestQuery,
              clientCountryName,
              clientRequestReferer,
              userAgentBrowser,
              userAgentOS,
              cacheStatus,
              originResponseStatus
            }}
          }}
        }}
      }}
    }}
    """

    result = api.graphql_query(query)
    if not result:
        return

    errors = result.get('errors')
    if errors:
        error_messages = [
            f"{e.get('message')}"
            for e in errors
            if isinstance(e, dict)
        ]
        logging.error(f"GraphQL errors for {zone_name}: {', '.join(error_messages)}")
        return

    zones_data = result.get('data', {}).get('viewer', {}).get('zones', [])
    if not zones_data:
        logging.info(f"No data found for zone {zone_name}")
        return

    total_updates = 0

    for zone_data in zones_data:
        zone_groups = zone_data.get('httpRequestsAdaptiveGroups', [])
        for group in zone_groups:
            requests = group.get('count', 0)
            if requests == 0:
                continue
            dimensions = group.get('dimensions', {})
            labels = {}
            labels.update({"zone_name": zone_name})
            labels.update({"host_name": dimensions.get('clientRequestHTTPHost', 'unknown')})
            labels.update({"method_name": dimensions.get('clientRequestHTTPMethodName', 'unknown')})
            labels.update({"path": dimensions.get('clientRequestPath', 'unknown')})
            labels.update({"query": dimensions.get('clientRequestQuery', 'unknown')})
            labels.update({"client_country_name": dimensions.get('clientCountryName', 'unknown')})
            labels.update({"client_request_referer": dimensions.get('clientRequestReferer', 'unknown')})
            labels.update({"user_agent_browser": dimensions.get('userAgentBrowser', 'unknown')})
            labels.update({"user_agent_os": dimensions.get('userAgentOS', 'unknown')})
            labels.update({"cache_status": dimensions.get('cacheStatus', 'unknown')})
            labels.update({"origin_response_status": dimensions.get('originResponseStatus', 'unknown')})
            if labels['client_request_referer'] == '':
                labels['client_request_referer'] = 'direct'
            metrics['requests_counter'].labels(**labels).set(requests)
            total_updates += 1

    logging.info(
        "Zone %s processed requests: %d metrics, time range: %s to %s",
        zone_name,
        total_updates,
        datetime_filter['geq'],
        datetime_filter['lt']
    )


def configure_logging():
    """
    Set up logging with LOGLEVEL from env or default to INFO.
    """
    log_level_str = os.environ.get("CF_EXPORTER_LOGLEVEL", "INFO").upper()
    valid_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    log_level = valid_levels.get(log_level_str, logging.INFO)

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logging.info(f"Log level set to {log_level_str}.")

def parse_env():
    """
    Read essential env variables or exit if not found.
    """
    api_token = os.environ.get("CF_EXPORTER_API_TOKEN")
    if not api_token:
        logging.error("CF_EXPORTER_API_TOKEN must be set.")
        sys.exit(1)

    request_timeout_str = os.environ.get("CF_EXPORTER_REQUEST_TIMEOUT", "30")
    scrape_interval_str = os.environ.get("CF_EXPORTER_SCRAPE_INTERVAL", "300")
    metrics_port_str = os.environ.get("CF_EXPORTER_METRICS_PORT", "8000")

    try:
        request_timeout = int(request_timeout_str)
    except ValueError:
        logging.error(f"Invalid CF_EXPORTER_REQUEST_TIMEOUT: {request_timeout_str}.")
        sys.exit(1)

    try:
        scrape_interval = int(scrape_interval_str)
    except ValueError:
        logging.error(f"Invalid CF_EXPORTER_SCRAPE_INTERVAL: {scrape_interval_str}.")
        sys.exit(1)

    try:
        metrics_port = int(metrics_port_str)
    except ValueError:
        logging.error(f"Invalid CF_EXPORTER_METRICS_PORT: {metrics_port_str}.")
        sys.exit(1)

    return {
        "api_token": api_token,
        "request_timeout": request_timeout,
        "scrape_interval": scrape_interval,
        "metrics_port": metrics_port
    }

@handle_exceptions
def collect_metrics(config):
    """
    Main loop: fetch zones, clear metrics, update with new data.
    """
    api = CloudflareAPI(
        api_token=config['api_token'],
        request_timeout=config['request_timeout']
    )

    while True:
        start_time = time.time()

        zones = api.list_zones()
        logging.info(f"Discovered {len(zones)} zones")
        metrics['visits_counter'].clear()

        for zone in zones:
            zone_id = zone.get('id')
            zone_name = zone.get('name')
            if not zone_id or not zone_name:
                continue
            get_visits_for_zone(api, zone_id, zone_name)
            get_requests_for_zone(api, zone_id, zone_name)

        elapsed = time.time() - start_time
        logging.debug(f"Metrics collection finished in {elapsed:.2f} seconds")
        time.sleep(max(0, config['scrape_interval'] - elapsed))

def signal_handler(sig, frame):
    """
    Handle termination signals and exit cleanly.
    """
    logging.info(f"Signal {signal.Signals(sig).name} received, shutting down.")
    sys.exit(0)

# Signal hooks for graceful shutdown
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

if __name__ == '__main__':
    configure_logging()
    config = parse_env()
    start_http_server(config['metrics_port'])
    logging.info(f"Exporter running on port {config['metrics_port']}")
    collect_metrics(config)
