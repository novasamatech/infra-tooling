#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import time
import sys
import signal
from datetime import datetime, timezone
from typing import List, Dict, Optional
import requests
from prometheus_client import start_http_server, Gauge

# Prometheus metrics
metrics = {
    'visits_counter': Gauge('cf_visits',
                            'Total visits since midnight UTC',
                            ['zone_name', 'host_name', 'client_country_name', 'client_request_referer', 'user_agent_browser', 'user_agent_os']
                           )
}
class CloudflareAPI:
    API_BASE_URL = "https://api.cloudflare.com/client/v4"
    GRAPHQL_URL = f"{API_BASE_URL}/graphql"
    
    def __init__(self, api_token: str, request_timeout=30):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        })
        self.request_timeout = request_timeout

    def graphql_query(self, query: str) -> Optional[Dict]:
        """Execute GraphQL query"""
        try:
            response = self.session.post(
                self.GRAPHQL_URL,
                json={'query': query},
                timeout=self.request_timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"GraphQL request failed: {str(e)}")
            return None

    def list_zones(self) -> List[Dict]:
        """List all zones using REST API with pagination"""
        zones = []
        page = 1
        per_page = 50
        
        while True:
            try:
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
                
            except Exception as e:
                logging.error(f"Zone listing failed: {str(e)}")
                break
                
        return zones

def get_visits_for_zone(api: CloudflareAPI, zone_id: str, zone_name: str) -> None:
    """Fetch and update visits metrics for a zone using adaptive groups"""
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

    # Improved error handling
    errors = result.get('errors')
    if errors:
        error_messages = [
            f"{e.get('message')}" 
            for e in errors 
            if isinstance(e, dict)
        ]
        logging.error(
            f"GraphQL errors for {zone_name}: {', '.join(error_messages)}"
        )
        return

    try:
        total_updates = 0
        zones_data = result.get('data', {}).get('viewer', {}).get('zones', [])
        
        if not zones_data:
            logging.info(f"No data found for zone {zone_name}")
            return

        for zone_data in zones_data:
            zone_groups = zone_data.get('httpRequestsAdaptiveGroups', [])
            
            for group in zone_groups:
                dimensions = group.get('dimensions', {})
                sum_data = group.get('sum', {})
                
                host = dimensions.get('clientRequestHTTPHost', 'unknown')
                client_country_name = dimensions.get('clientCountryName', 'unknown')
                client_request_referer = dimensions.get('clientRequestReferer', 'unknown')
                user_agent_browser = dimensions.get('userAgentBrowser', 'unknown')
                user_agent_os = dimensions.get('userAgentOS', 'unknown')
                visits = sum_data.get('visits', 0)
                
                if client_request_referer == '':
                    client_request_referer = 'direct'
                
                logging.debug(
                    "Updating metric: zone=%s host=%s client_country_name=%s client_request_referer=%s user_agent_browser=%s user_agent_os=%s visits=%d",
                    zone_name,
                    host,
                    client_country_name,
                    client_request_referer,
                    user_agent_browser,
                    user_agent_os,
                    visits
                )
                
                metrics['visits_counter'].labels(
                    zone_name=zone_name,
                    host_name=host,
                    client_country_name = client_country_name,
                    client_request_referer = client_request_referer,
                    user_agent_browser = user_agent_browser,
                    user_agent_os = user_agent_os
                ).set(visits)
                
                total_updates += 1
        
        logging.info(
            "Zone %s processed: %d metrics, time range: %s to %s",
            zone_name,
            total_updates,
            datetime_filter['geq'],
            datetime_filter['lt']
        )
                
    except Exception as e:
        logging.error(f"Error processing response for {zone_name}: {str(e)}")



def configure_logging():
    """
    Configures the logging level based on the CF_EXPORTER_LOGLEVEL environment variable.
    Defaults to INFO if not set or invalid.
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
    Reads and validates the necessary environment variables.
    Returns a dictionary of configuration parameters or terminates the script if invalid.
    """
    api_token = os.environ.get("CF_EXPORTER_API_TOKEN")

    if not all([api_token]):
        logging.error(
            "Environment variables CF_EXPORTER_API_TOKEN must be set."
        )
        sys.exit(1)

    request_timeout_str = os.environ.get("CF_EXPORTER_REQUEST_TIMEOUT", "30")
    scrape_interval_str = os.environ.get("CF_EXPORTER_SCRAPE_INTERVAL", "300")

    try:
        request_timeout = int(request_timeout_str)
    except ValueError:
        logging.error(f"Invalid value for CF_EXPORTER_REQUEST_TIMEOUT: {request_timeout_str}.")
        sys.exit(1)

    try:
        scrape_interval = int(scrape_interval_str)
    except ValueError:
        logging.error(f"Invalid value for CF_EXPORTER_SCRAPE_INTERVAL: {scrape_interval_str}.")
        sys.exit(1)

    metrics_port_str = os.environ.get("CF_EXPORTER_METRICS_PORT", "8000")
    try:
        metrics_port = int(metrics_port_str)
    except ValueError:
        logging.error(f"Invalid value for CF_EXPORTER_METRICS_PORT: {metrics_port_str}.")
        sys.exit(1)

    return {
        "api_token": api_token,
        "request_timeout": request_timeout,
        "scrape_interval": scrape_interval,
        "metrics_port": metrics_port
    }

def collect_metrics(config):
    """Main metrics collection loop"""
    api = CloudflareAPI(api_token=config['api_token'], request_timeout=config['request_timeout'])
    
    while True:
        start_time = time.time()
        try:
            zones = api.list_zones()
            logging.info(f"Discovered {len(zones)} zones")
            
            metrics['visits_counter'].clear()
            
            for zone in zones:
                if not (zone_id := zone.get('id')):
                    continue
                if not (zone_name := zone.get('name')):
                    continue
                
                get_visits_for_zone(api, zone_id, zone_name)
                
        except Exception as e:
            logging.error(f"Metrics collection failed: {str(e)}")
        
        elapsed = time.time() - start_time
        logging.debug(f"Metrics collection completed in {elapsed:.2f} seconds")
        sleep_time = max(0, config['scrape_interval'] - elapsed)
        time.sleep(sleep_time)

def signal_handler(sig, frame):
    logging.info(f"Signal {signal.Signals(sig).name} detected, exiting...")
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

if __name__ == '__main__':
    configure_logging()
    config = parse_env()
    start_http_server(config['metrics_port'])
    logging.info(f"Exporter started on port {config['metrics_port']}")
    collect_metrics(config)
