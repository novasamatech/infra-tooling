<!--
Copyright © 2026 Novasama Technologies GmbH
SPDX-License-Identifier: Apache-2.0
-->

# Changelog

All notable changes to `turn-testing` are documented here.
This project adheres to [Semantic Versioning](https://semver.org/).

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
