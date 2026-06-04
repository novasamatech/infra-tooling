#!/usr/bin/env python3

# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""
Prometheus exporter for CoTURN servers.

Periodically runs STUN / TURN / relay-only WebRTC checks (via the shared
``turntest`` library) against an arbitrary list of servers and exposes the
results as Prometheus metrics over HTTP at ``/metrics``.

The WebRTC check is an **adaptive capacity probe**: instead of a fixed target
rate and duration, it ramps the send rate up step by step until the SCTP DATA
retransmission ratio crosses a configurable threshold (``retransmit_threshold_percent``,
default 0.5 %). The reported ``coturn_webrtc_capacity_bits_per_second`` is the highest rate
sustained below that threshold — i.e. the relay's usable throughput "knee". The
ramp stops as soon as loss appears, so it stays gentle on the servers.

Configuration is a TOML file (see ``coturn-exporter.example.toml``). Every server
is probed once per ``interval`` (default 300 s = 5 min).

Usage:
    ./exporter.py -c coturn-exporter.toml
    ./exporter.py -c coturn-exporter.toml --listen-port 9686 --oneshot

Metrics:
    coturn_probe_success{server,transport,test}            1 if the sub-test passed, else 0
    coturn_probe_duration_seconds{server,transport,test}   sub-test latency (stun/turn only)
    coturn_webrtc_capacity_bits_per_second{server,transport}   max rate sustained < threshold
    coturn_webrtc_threshold_reached{server,transport}      1 if a knee was found (else hit the cap)
    coturn_cycle_duration_seconds                          duration of the whole probe cycle
    coturn_cycles_total                                    number of completed cycles
    coturn_exporter_build_info{version}                    exporter version (value always 1)
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
import time
import tomllib
from dataclasses import dataclass, field
from typing import List, Optional

from prometheus_client import CollectorRegistry, Counter, Gauge, Info, start_http_server

import turntest

LOG = logging.getLogger("coturn-exporter")


def _read_version():
    """Read the .version file next to this script; fall back to 'unknown'."""
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, ".version"), encoding="utf-8") as fh:
            return fh.read().strip() or "unknown"
    except OSError:
        return "unknown"


EXPORTER_VERSION = _read_version()

# Sub-test identifiers used as the `test` metric label.
TEST_STUN = "stun"
TEST_TURN = "turn"
TEST_WEBRTC = "webrtc"

VALID_TRANSPORTS = ("udp", "tcp", "tls")
VALID_TESTS = (TEST_STUN, TEST_TURN, TEST_WEBRTC)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass
class ServerConfig:
    """A single monitored CoTURN server (with per-server overrides applied)."""

    name: str
    host: str
    port: int = 3478
    tls_port: int = 5349
    transports: List[str] = field(default_factory=lambda: list(VALID_TRANSPORTS))
    tests: List[str] = field(default_factory=lambda: list(VALID_TESTS))
    # Authentication: either static username/password or a REST static-auth-secret.
    username: Optional[str] = None
    password: Optional[str] = None
    auth_secret: Optional[str] = None
    auth_user: Optional[str] = None
    auth_ttl: int = 3600
    timeout: int = 30
    insecure: bool = False
    # Adaptive WebRTC capacity probe. The threshold is the primary knob; the ramp
    # shape has sane defaults so neither a rate nor a duration must be configured.
    threshold_percent: float = 0.5
    ramp_start_mbps: float = 1.0
    ramp_factor: float = 1.5
    ramp_step_duration: float = 3.0
    ramp_warmup_duration: float = 2.0
    ramp_max_mbps: float = 100.0
    ramp_max_duration: float = 60.0

    def credentials(self):
        """Return (username, password), minting fresh REST creds if needed."""
        if self.auth_secret:
            return turntest.make_rest_credentials(
                self.auth_secret, self.auth_ttl, self.auth_user
            )
        return self.username, self.password

    def port_for(self, transport):
        return self.tls_port if transport == "tls" else self.port


@dataclass
class ExporterConfig:
    listen_address: str = "0.0.0.0"
    listen_port: int = 9686
    interval: int = 300
    servers: List[ServerConfig] = field(default_factory=list)


def _as_float(value, name):
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a number, got {value!r}")


def _validate_list(values, allowed, name):
    if not isinstance(values, list) or not values:
        raise ValueError(f"{name} must be a non-empty list")
    bad = [v for v in values if v not in allowed]
    if bad:
        raise ValueError(f"{name} contains unknown values {bad}; allowed: {list(allowed)}")
    return list(values)


def _validate_ramp(s, name):
    """Sanity-check a server's ramp/threshold parameters."""
    if not 0 < s.threshold_percent <= 100:
        raise ValueError(f"{name}: retransmit_threshold_percent must be in (0, 100]")
    if s.ramp_factor <= 1.0:
        raise ValueError(f"{name}: ramp_factor must be > 1.0")
    if s.ramp_start_mbps <= 0:
        raise ValueError(f"{name}: ramp_start_mbps must be > 0")
    if s.ramp_step_duration <= 0:
        raise ValueError(f"{name}: ramp_step_duration must be > 0")
    if s.ramp_warmup_duration < 0:
        raise ValueError(f"{name}: ramp_warmup_duration must be >= 0")
    if s.ramp_max_mbps < s.ramp_start_mbps:
        raise ValueError(f"{name}: ramp_max_mbps must be >= ramp_start_mbps")
    if s.ramp_max_duration <= 0:
        raise ValueError(f"{name}: ramp_max_duration must be > 0")


def load_config(path):
    """Parse and validate the TOML config into an ExporterConfig."""
    with open(path, "rb") as fh:
        raw = tomllib.load(fh)

    # Global defaults that individual servers inherit unless they override them.
    g_interval = int(raw.get("interval", 300))
    g_timeout = int(raw.get("timeout", 30))
    g_insecure = bool(raw.get("insecure", False))
    g_auth_ttl = int(raw.get("auth_ttl", 3600))
    g_transports = _validate_list(
        raw.get("transports", list(VALID_TRANSPORTS)), VALID_TRANSPORTS, "transports"
    )
    g_tests = _validate_list(raw.get("tests", list(VALID_TESTS)), VALID_TESTS, "tests")
    g_threshold = _as_float(
        raw.get("retransmit_threshold_percent", 0.5), "retransmit_threshold_percent"
    )
    g_ramp_start = _as_float(raw.get("ramp_start_mbps", 1.0), "ramp_start_mbps")
    g_ramp_factor = _as_float(raw.get("ramp_factor", 1.5), "ramp_factor")
    g_ramp_step = _as_float(raw.get("ramp_step_duration", 3.0), "ramp_step_duration")
    g_ramp_warmup = _as_float(raw.get("ramp_warmup_duration", 2.0), "ramp_warmup_duration")
    g_ramp_max = _as_float(raw.get("ramp_max_mbps", 100.0), "ramp_max_mbps")
    g_ramp_maxdur = _as_float(raw.get("ramp_max_duration", 60.0), "ramp_max_duration")

    if g_interval <= 0:
        raise ValueError("interval must be a positive number of seconds")

    servers_raw = raw.get("servers") or raw.get("server") or []
    if not isinstance(servers_raw, list) or not servers_raw:
        raise ValueError("config must define at least one [[servers]] entry")

    servers = []
    seen_names = set()
    for idx, s in enumerate(servers_raw):
        name = s.get("name") or s.get("host")
        if not name:
            raise ValueError(f"servers[{idx}] needs a 'name' or 'host'")
        if "host" not in s:
            raise ValueError(f"server {name!r} is missing 'host'")
        if name in seen_names:
            raise ValueError(f"duplicate server name {name!r}")
        seen_names.add(name)

        has_static = bool(s.get("username") and s.get("password"))
        has_secret = bool(s.get("auth_secret"))
        if has_static and has_secret:
            raise ValueError(
                f"server {name!r}: 'auth_secret' is mutually exclusive with "
                "'username'/'password'"
            )
        if not has_static and not has_secret:
            raise ValueError(
                f"server {name!r}: provide either username+password or auth_secret"
            )

        transports = _validate_list(
            s.get("transports", g_transports), VALID_TRANSPORTS, f"server {name!r}.transports"
        )
        tests = _validate_list(
            s.get("tests", g_tests), VALID_TESTS, f"server {name!r}.tests"
        )

        server = ServerConfig(
            name=name,
            host=s["host"],
            port=int(s.get("port", 3478)),
            tls_port=int(s.get("tls_port", 5349)),
            transports=transports,
            tests=tests,
            username=s.get("username"),
            password=s.get("password"),
            auth_secret=s.get("auth_secret"),
            auth_user=s.get("auth_user"),
            auth_ttl=int(s.get("auth_ttl", g_auth_ttl)),
            timeout=int(s.get("timeout", g_timeout)),
            insecure=bool(s.get("insecure", g_insecure)),
            threshold_percent=_as_float(
                s.get("retransmit_threshold_percent", g_threshold),
                f"server {name!r}.retransmit_threshold_percent",
            ),
            ramp_start_mbps=_as_float(
                s.get("ramp_start_mbps", g_ramp_start), f"server {name!r}.ramp_start_mbps"
            ),
            ramp_factor=_as_float(
                s.get("ramp_factor", g_ramp_factor), f"server {name!r}.ramp_factor"
            ),
            ramp_step_duration=_as_float(
                s.get("ramp_step_duration", g_ramp_step),
                f"server {name!r}.ramp_step_duration",
            ),
            ramp_warmup_duration=_as_float(
                s.get("ramp_warmup_duration", g_ramp_warmup),
                f"server {name!r}.ramp_warmup_duration",
            ),
            ramp_max_mbps=_as_float(
                s.get("ramp_max_mbps", g_ramp_max), f"server {name!r}.ramp_max_mbps"
            ),
            ramp_max_duration=_as_float(
                s.get("ramp_max_duration", g_ramp_maxdur),
                f"server {name!r}.ramp_max_duration",
            ),
        )
        _validate_ramp(server, f"server {name!r}")
        servers.append(server)

    return ExporterConfig(
        listen_address=str(raw.get("listen_address", "0.0.0.0")),
        listen_port=int(raw.get("listen_port", 9686)),
        interval=g_interval,
        servers=servers,
    )


# --------------------------------------------------------------------------- #
# Metrics
# --------------------------------------------------------------------------- #
class Metrics:
    """Holds the Prometheus metric objects and updates them per probe."""

    def __init__(self, registry=None):
        reg = registry
        probe_labels = ["server", "transport", "test"]
        wr_labels = ["server", "transport"]
        # Health per server × transport × sub-test (stun/turn/webrtc) — tells you
        # exactly which protocol and transport is failing.
        self.probe_success = Gauge(
            "coturn_probe_success",
            "1 if the sub-test passed, else 0 (per server, transport, test)",
            probe_labels,
            registry=reg,
        )
        # Latency of the sub-test. Only meaningful for stun/turn (the webrtc value
        # would be the ramp wall-time, an artifact), so it is recorded for those two.
        self.duration = Gauge(
            "coturn_probe_duration_seconds",
            "Sub-test latency in seconds (stun/turn only)",
            probe_labels,
            registry=reg,
        )
        # Base unit (bits/second), per Prometheus convention — e.g. 16 Mbit/s = 1.6e7.
        self.capacity_bits_per_second = Gauge(
            "coturn_webrtc_capacity_bits_per_second",
            "Highest WebRTC send rate sustained below the retransmit threshold, in bits/second",
            wr_labels,
            registry=reg,
        )
        self.threshold_reached = Gauge(
            "coturn_webrtc_threshold_reached",
            "1 if the ramp found a knee (crossed the threshold); 0 if it hit the rate/time cap",
            wr_labels,
            registry=reg,
        )
        self.cycle_duration = Gauge(
            "coturn_cycle_duration_seconds",
            "Duration of the most recent probe cycle in seconds",
            registry=reg,
        )
        self.cycles_total = Counter(
            "coturn_cycles_total", "Number of completed probe cycles", registry=reg
        )
        self.build_info = Info(
            "coturn_exporter_build", "CoTURN exporter build information", registry=reg
        )
        self.build_info.info({"version": EXPORTER_VERSION})

    def record_stun(self, server, transport, result):
        lbl = (server, transport, TEST_STUN)
        self.probe_success.labels(*lbl).set(1 if result.ok else 0)
        self.duration.labels(*lbl).set(result.duration)

    def record_turn(self, server, transport, result):
        lbl = (server, transport, TEST_TURN)
        self.probe_success.labels(*lbl).set(1 if result.ok else 0)
        self.duration.labels(*lbl).set(result.duration)

    def record_capacity(self, server, transport, result):
        self.probe_success.labels(server, transport, TEST_WEBRTC).set(
            1 if result.ok else 0
        )
        self.capacity_bits_per_second.labels(server, transport).set(
            result.capacity_mbps * 1_000_000
        )
        self.threshold_reached.labels(server, transport).set(
            1 if result.stopped_on_threshold else 0
        )


# --------------------------------------------------------------------------- #
# Probe cycle
# --------------------------------------------------------------------------- #
async def probe_server(server, metrics):
    """Run every configured sub-test for one server, updating the metrics."""
    username, password = server.credentials()

    for transport in server.transports:
        port = server.port_for(transport)
        LOG.info("probing %s via %s (%s:%s)", server.name, transport, server.host, port)

        if TEST_STUN in server.tests:
            try:
                res = await turntest.test_stun_binding(
                    server.host, port, server.timeout, transport, server.insecure
                )
            except Exception as exc:  # defensive: never let one probe kill the cycle
                LOG.exception("stun probe crashed for %s/%s: %s", server.name, transport, exc)
                res = turntest.StunResult(ok=False)
            metrics.record_stun(server.name, transport, res)

        if TEST_TURN in server.tests:
            try:
                res = await turntest.test_turn_allocation(
                    server.host, port, username, password,
                    server.timeout, transport, server.insecure,
                )
            except Exception as exc:
                LOG.exception("turn probe crashed for %s/%s: %s", server.name, transport, exc)
                res = turntest.TurnResult(ok=False)
            metrics.record_turn(server.name, transport, res)

        if TEST_WEBRTC in server.tests:
            try:
                res = await turntest.webrtc_capacity_probe(
                    server.host, port, username, password,
                    server.timeout, transport,
                    threshold_ratio=server.threshold_percent / 100.0,
                    start_mbps=server.ramp_start_mbps,
                    ramp_factor=server.ramp_factor,
                    step_duration=server.ramp_step_duration,
                    warmup_duration=server.ramp_warmup_duration,
                    max_mbps=server.ramp_max_mbps,
                    max_duration=server.ramp_max_duration,
                    insecure=server.insecure,
                )
            except Exception as exc:
                LOG.exception("webrtc probe crashed for %s/%s: %s", server.name, transport, exc)
                res = turntest.WebRtcCapacityResult(ok=False)
            metrics.record_capacity(server.name, transport, res)


async def run_cycle(config, metrics):
    """Probe every server once, sequentially, then publish cycle-level metrics."""
    start = time.monotonic()
    LOG.info("starting probe cycle over %d server(s)", len(config.servers))
    for server in config.servers:
        try:
            await probe_server(server, metrics)
        except Exception as exc:
            LOG.exception("server %s crashed the cycle: %s", server.name, exc)
    elapsed = time.monotonic() - start
    metrics.cycle_duration.set(elapsed)
    metrics.cycles_total.inc()
    LOG.info("probe cycle finished in %.1fs", elapsed)
    return elapsed


async def main_loop(config, metrics, oneshot=False):
    """Run probe cycles forever, pacing them to `interval`, until cancelled."""
    while True:
        elapsed = await run_cycle(config, metrics)
        if oneshot:
            return
        sleep_for = max(0.0, config.interval - elapsed)
        if sleep_for == 0:
            LOG.warning(
                "cycle took %.1fs, longer than interval %ds; starting next immediately",
                elapsed, config.interval,
            )
        await asyncio.sleep(sleep_for)


def main():
    parser = argparse.ArgumentParser(
        description="Prometheus exporter that probes CoTURN servers (STUN/TURN/WebRTC)",
    )
    parser.add_argument(
        "-c", "--config", required=True, help="Path to the TOML configuration file"
    )
    parser.add_argument(
        "--listen-address", default=None, help="Override the metrics bind address"
    )
    parser.add_argument(
        "--listen-port", type=int, default=None, help="Override the metrics port"
    )
    parser.add_argument(
        "--oneshot",
        action="store_true",
        help="Run a single probe cycle, serve metrics, and exit (for testing)",
    )
    parser.add_argument(
        "--log-level", default="INFO", help="Logging level (default: INFO)"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    if not turntest.AIORTC_AVAILABLE:
        LOG.error("aiortc not installed; install with: pip install -r requirements.txt")
        sys.exit(1)

    try:
        config = load_config(args.config)
    except (OSError, ValueError, tomllib.TOMLDecodeError) as exc:
        LOG.error("failed to load config %s: %s", args.config, exc)
        sys.exit(2)

    if args.listen_address is not None:
        config.listen_address = args.listen_address
    if args.listen_port is not None:
        config.listen_port = args.listen_port

    # A dedicated registry holds exactly the metrics we define, and is what
    # /metrics serves — no process/GC collectors from the global default.
    registry = CollectorRegistry()
    metrics = Metrics(registry=registry)
    start_http_server(
        config.listen_port, addr=config.listen_address, registry=registry
    )
    LOG.info(
        "serving metrics on http://%s:%d/metrics; interval=%ds, servers=%d",
        config.listen_address, config.listen_port, config.interval, len(config.servers),
    )

    stop = asyncio.Event()

    async def runner():
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except (NotImplementedError, RuntimeError):
                pass  # not available on every platform
        loop_task = asyncio.ensure_future(main_loop(config, metrics, args.oneshot))
        stop_task = asyncio.ensure_future(stop.wait())
        done, pending = await asyncio.wait(
            {loop_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        # Surface an exception from the probe loop if it crashed.
        if loop_task in done:
            loop_task.result()

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        pass
    LOG.info("exporter stopped")


if __name__ == "__main__":
    main()
