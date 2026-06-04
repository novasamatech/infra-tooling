<!--
Copyright © 2026 Novasama Technologies GmbH
SPDX-License-Identifier: Apache-2.0
-->

# Changelog

All notable changes to `turn-testing` are documented here.
This project adheres to [Semantic Versioning](https://semver.org/).

## [3.0.0] — 2026-06-04

### Added
- Alternative **TURN REST authentication** via coturn `static-auth-secret`
  (`--auth-secret` / `--auth-user` / `--auth-ttl`), deriving time-limited
  credentials; mutually exclusive with the existing static `-u`/`-p`. The
  Makefile selects it via the new `TURN_SECRET` env var (mutually exclusive
  with `TURN_USER`/`TURN_PASS`).
- **Prometheus exporter** (`exporter.py`): periodically probes an arbitrary list
  of CoTURN servers (TOML config) and exposes the results over HTTP at
  `/metrics`. Configurable probe `interval` (default 300 s) and per-server
  `timeout` / `transports` / `tests`; supports both static and `auth_secret`
  (TURN REST) credentials per server. The WebRTC check is an **adaptive capacity
  probe** (`webrtc_capacity_probe`): instead of a fixed rate/duration it ramps the
  send rate up (after a discarded warm-up that absorbs SCTP slow-start) until the
  retransmission ratio crosses `retransmit_threshold_percent` (default 0.5 %),
  reporting the highest rate sustained below the threshold. It stops at the first
  sign of loss, so it stays gentle on the relay. Metrics follow Prometheus naming
  conventions and base units: `coturn_probe_success{server,transport,test}`,
  `coturn_probe_duration_seconds` (stun/turn), `coturn_webrtc_capacity_bits_per_second`,
  `coturn_webrtc_threshold_reached`, `coturn_cycle_duration_seconds`,
  `coturn_cycles_total`, and `coturn_exporter_build_info{version}`. An
  unreachable host or rejected credentials just set `coturn_probe_success=0` for
  the affected sub-tests — one bad target never crashes the cycle. Shuts down
  cleanly on SIGTERM/SIGINT. See `coturn-exporter.example.toml`.
- A `make test-exporter` no-network smoke target (modules compile + example
  config parses).
- `prometheus_client` dependency.

### Changed
- `-u`/`-p` are no longer required — exactly one authentication method (static
  or `--auth-secret`) must be provided.
- The shared STUN / TURN / WebRTC test logic moved from `app.py` into a reusable
  library module (`turntest.py`), consumed by both `app.py` and `exporter.py`.
  The `app.py` CLI behaviour (output, flags, exit codes) is **unchanged**.
- The `test_*` coroutines now return result dataclasses (`StunResult` /
  `TurnResult` / `WebRtcResult`) carrying the measured metrics; `.ok` is the
  pass/fail flag the CLI uses.
- A single `Dockerfile` now ships both entry points: `ENTRYPOINT ["python"]`
  with a default `CMD` that runs the exporter, and the CLI reached by overriding
  the `CMD` (`docker run <image> app.py …`). The previous `app.py`-only
  `ENTRYPOINT` is replaced.

## [2.0.0] — 2026-06-02

### Added
- STUN and TURN tests now support **TCP and TLS** transports (not just UDP).
- `--insecure` flag to skip TLS verification (STUN/TURN tests only).
- Input validation for `--port`, `--timeout`, `--duration`, `--rate-mbps`.
- **SCTP retransmission metric** for the WebRTC stream — channel-loss % printed
  even on throughput failure.
- Backpressure on the data-channel send buffer (~4 MiB cap) to bound memory.
- `README.md`, `AGENTS.md`, and a `Makefile` test runner (creates the venv,
  runs the full UDP/TCP/TLS + guards + speed-mode matrix; server/ports/creds via
  env vars).

### Fixed
- Pacing no longer has a sleep cap that made rates below ~2.6 Mbps (incl. the
  default 1 Mbps) finish early; `--rate-mbps`/`--duration` are now honored.
- STUN/TURN tests honor `--transport` instead of always using UDP.
- Throughput is measured over the send window, not the ACK wait.
- Errors/warnings go to stderr; removed dead `mapped_address` code.

### Changed
- `--stun-only`/`--turn-only`/`--webrtc-only` are now mutually exclusive.
- `Dockerfile` uses `ENTRYPOINT` so CLI args append to `docker run`.
- `requirements.txt` re-pinned to the verified dependency set.

## [1.0.0]
- Initial release: STUN binding, TURN allocation, relay-only WebRTC data-channel
  test (UDP only).
