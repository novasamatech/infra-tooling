<!--
Copyright © 2025 Novasama Technologies GmbH
SPDX-License-Identifier: Apache-2.0
-->

# AGENTS.md — turn-testing

Guidance for AI agents and contributors working on this directory. Read
[README.md](README.md) first for user-facing behavior; this file covers the
*how* and the *gotchas*.

## Scope

Tooling that tests a CoTURN server: STUN binding, TURN allocation, and a
relay-only WebRTC data-channel throughput/integrity check. The shared logic
lives in a small library (`turntest.py`) with two thin front-ends: the
single-shot CLI (`app.py`) and a Prometheus exporter (`exporter.py`). Keep it
flat — a library module plus entry points, not a package.

## Files

| File | Purpose |
| --- | --- |
| `turntest.py` | The shared library: STUN/TURN/WebRTC tests, the exporter-only `webrtc_capacity_probe`, helpers, and the `StunResult`/`TurnResult`/`WebRtcResult`/`WebRtcCapacityResult` dataclasses. No `main()`. |
| `app.py` | Single-shot CLI front-end. Executable, `#!/usr/bin/env python3`; imports from `turntest`. |
| `exporter.py` | Prometheus exporter front-end: probes many servers on a timer, serves `/metrics`. Imports from `turntest`. |
| `coturn-exporter.example.toml` | Placeholder exporter config (committed). The **live** config with real creds is never committed. |
| `requirements.txt` | **Pinned** deps. The code relies on private internals of these exact versions. |
| `Dockerfile` | Single image, both entry points. Alpine + Python 3.13, non-root, `EXPOSE 9686`. `ENTRYPOINT ["python"]`, `CMD` runs the exporter by default; override the `CMD` with `app.py …` for the CLI. |
| `Makefile` | Test runner: creates `venv/` on demand and runs the full scenario matrix. `make help`. |
| `.version` | Image/release version tag (bump on behavioral changes). |
| `venv/` | Local virtualenv (gitignored). Not committed. |

## Running & testing

This is Python, not a JS project — run it directly through the venv (the global
"never run the JS toolchain on the host" rule does **not** apply here):

```sh
venv/bin/python -m py_compile turntest.py app.py exporter.py   # quick syntax check
```

There are **no unit tests**; validation is done against a live CoTURN server via
the [`Makefile`](Makefile). It creates `./venv` on demand (installing
`requirements.txt`) and runs the full scenario matrix — the no-network exporter
smoke (`test-exporter`: modules compile + example config parses), UDP/TCP/TLS ×
stun/turn/webrtc/all-three, the `--insecure` smoke, the argument guards (each
must exit 2), and the low/moderate/high speed modes (which print the SCTP
retransmission metric).

Server, ports and credentials come from **environment variables with no
defaults**:

```sh
export TURN_HOST=...        # hostname (required for the TLS WebRTC cert check)
export TURN_PORT=3478       # UDP/TCP STUN/TURN port
export TURN_TLS_PORT=5349   # TLS (turns:) port
# then EITHER static auth …
export TURN_USER=... TURN_PASS=...
# … OR a TURN REST secret (mutually exclusive with TURN_USER/TURN_PASS)
export TURN_SECRET=...       # coturn static-auth-secret
make test                   # full run; `make help` lists individual targets
                            # (test-udp / test-tcp / test-tls / test-guards / test-speed)
```

A change is "verified" only after `make test` passes against a real server. Two
behaviours are by design and worth remembering:

* For `--transport tls`, set `TURN_HOST` to the **hostname** (not an IP): the
  WebRTC `turns:` path always verifies the certificate and the cert CN is the
  hostname. An unroutable AAAA record is fine — `create_connection` falls back
  to IPv4. (`--insecure` only relaxes the STUN/TURN TLS contexts — see gotcha #4.)
* The high-rate capacity probe in `test-speed` may fail on a constrained path;
  it is marked informational (`-` in the recipe) and does **not** fail
  `make test`. Read its `SCTP retransmissions … (P%)` line to judge the channel.

## Architecture / invariants

* **Library split.** All test logic lives in `turntest.py`; `app.py` and
  `exporter.py` only import from it. The three `test_*` coroutines **return**
  result dataclasses (`StunResult`/`TurnResult`/`WebRtcResult`) *and* keep every
  original stdout/stderr progress line — so the CLI output is byte-for-byte
  unchanged while the exporter can read the measured metrics. `.ok` is the
  pass/fail flag. When editing a `test_*` function, every `return` must hand back
  the right dataclass (the WebRTC path funnels them through the local `_wr()`
  helper, which folds in the live SCTP retransmit counts). Do **not** move prints
  to stderr or drop them — the CLI contract depends on them. A fourth coroutine,
  `webrtc_capacity_probe` (→ `WebRtcCapacityResult`), is exporter-only and the CLI
  never calls it.
* **`app.py main()`** parses args, picks which sub-tests to run (the three
  `--*-only` flags are a mutually exclusive group), then drives them inside one
  `asyncio.run`, reading `.ok` from each result. Exit `0` = all selected tests
  passed, `1` = a failure, `2` = bad args.
* **`exporter.py`** loads a TOML config (`load_config`), then loops
  `run_cycle` → sleep(`interval`) forever, probing every server sequentially
  (`probe_server`) and updating the Prometheus metrics (`Metrics` — gauges plus
  the `coturn_cycles_total` counter and the `coturn_exporter_build_info` info).
  Sequential by design — concurrent WebRTC probes would skew each other's
  throughput and load the relay. The HTTP server runs in `prometheus_client`'s
  background thread; the asyncio loop and a `SIGTERM`/`SIGINT` handler share an
  `asyncio.Event` for clean shutdown (exit `0`). The probe coroutines already
  swallow their own errors, but `probe_server` wraps each in a try/except so one
  crash never kills the cycle — an unreachable host or bad credentials just set
  `coturn_probe_success=0` for the affected sub-tests and the loop keeps cycling.
  `coturn_exporter_build_info{version}` is read from `.version` next to the script
  (copied into the image by the Dockerfile; falls back to `unknown`).
* **Exporter WebRTC = capacity ramp, not a fixed rate.** The exporter does **not**
  call `test_webrtc_datachannel`; it calls `turntest.webrtc_capacity_probe`, which
  ramps the send rate (`ramp_start_mbps` × `ramp_factor` per step, `ramp_step_duration`
  each) and stops at the first step whose *incremental* SCTP retransmit ratio
  reaches the threshold (`retransmit_threshold_percent`, default 0.5 %).
  `capacity_mbps` is the last rate **below** the threshold (the over-capacity step
  that trips the threshold is excluded — we `break` before recording it), and
  `stopped_on_threshold` distinguishes a real knee from hitting
  `ramp_max_mbps`/`ramp_max_duration`. The exporter exposes only
  `coturn_webrtc_capacity_bits_per_second` (the dataclass `capacity_mbps` × 1e6 —
  base units per Prometheus convention) + `coturn_webrtc_threshold_reached`; the
  per-test pass/fail is `coturn_probe_success{server,transport,test}`. **No
  retransmit-ratio metric** (the ramp drives *to* the threshold, so loss at
  capacity is sub-threshold ~0 by construction) and **no relay-confirmed metric**
  (relay-only is already enforced; `coturn_probe_success` implies it).
  `WebRtcCapacityResult` still
  computes `retransmit_ratio`/`relay_confirmed` for the log line and library
  callers, but they are not published. `duration` is recorded for stun/turn only
  (the webrtc value is the ramp wall-time, an artifact).
  Each step is measured by per-step counter **deltas**; the cumulative SCTP counter
  is never exposed, so the over-driven step can't pollute earlier steps.
  A discarded **warm-up** (`ramp_warmup_duration` at `start_mbps`) runs before the
  ramp so SCTP slow-start / initial-RTO retransmits don't contaminate the first
  measured step — without it the first step reads a spurious double-digit ratio and
  capacity is underreported. Each
  step snapshots the cumulative SCTP counters, sends a paced burst, then **settles**
  (drains `bufferedAmount`, then sleeps ~0.5 s) before reading the counters again so
  late retransmits are attributed to the right step. If `instrument_sctp_retransmits`
  returns `None` the probe fails fast — without the counter the ramp has no stop
  signal. The CLI's `test_webrtc_datachannel` (fixed `--rate-mbps`/`--duration`) is
  unchanged and untouched by this path.
* **Authentication** is resolved in `main()` to a `username`/`password` pair that
  the rest of the code uses unchanged. Two mutually exclusive modes: static
  (`-u`/`-p`) or TURN REST (`--auth-secret`, coturn `static-auth-secret`).
  `make_rest_credentials()` derives `username = <expiry>[:userid]` and
  `password = base64(HMAC-SHA1(secret, username))` — the secret is the raw HMAC
  key, and each process run mints a fresh credential (`--auth-ttl`, default 1h).
* **stdout vs stderr:** progress (`[STUN]`/`[TURN]`/`[WEBRTC] …`) goes to
  stdout; errors/warnings go through `eprint()` to stderr. Keep that split.
* **Transport handling:** `--transport` must flow into *all three* sub-tests.
  - STUN: UDP uses a datagram endpoint; TCP/TLS uses `asyncio.open_connection`
    with manual RFC 5389 framing (read 20-byte header, then the length in bytes
    2–3).
  - TURN: `turn.create_turn_endpoint(..., transport=, ssl=)`.
  - WebRTC: `build_turn_url()` emits `turn:`/`turns:` + `?transport=`.
* **Relay-only enforcement (WebRTC):** aiortc has no
  `iceTransportPolicy:"relay"`. We enforce it in two places that must stay in
  sync — `filter_sdp_for_relay_only()` (drops non-`typ relay` candidate lines
  from the SDP exchanged with the peer) **and** `prune_local_candidates_to_relay()`
  (drops non-relay candidates from the live aioice connection *before*
  connectivity checks start). Pruning must happen after gathering completes and
  before `setRemoteDescription` on that peer.
* **Integrity:** sender and receiver each SHA‑256 the byte stream; the channel
  is `ordered=True`/reliable so hashes must match regardless of message
  boundaries. The `__END__` / `__ACK__` strings are control messages on the
  same channel — do not feed them into the hashers (the receiver checks
  `isinstance(message, bytes)` first).

## Known gotchas (do not regress)

1. **Pacing.** The send loop sleeps *exactly* enough to stay on schedule
   (`(bytes_sent - expected_bytes) / bytes_per_sec`). A previous version capped
   the per-iteration sleep at `0.05 s`, which combined with the 16 KiB chunk
   imposed a ~2.6 Mbps floor — rates below that (including the default 1 Mbps)
   silently ran too fast and finished early. **Do not reintroduce a sleep cap.**
2. **Backpressure.** The loop waits while `dc1.bufferedAmount` exceeds ~4 MiB so
   a slow relay can't grow the SCTP queue without bound. Keep this — high
   `--rate-mbps` over a slow path otherwise blows up memory.
3. **Private library attributes.** These break on dependency upgrades; re-verify
   against the new versions and update if needed:
   - `turn_transport._TurnTransport__relayed_address`
   - `pc.sctp.transport.transport.iceGatherer._connection`
   - aioice `Connection._local_candidates`, `Connection._nominated`
   - `CandidatePair.local_candidate` / `.remote_candidate`, `Candidate.type`
   - `pc.sctp._send_chunk` (wrapped) + `DataChunk._sent_count` from
     `aiortc.rtcsctptransport` — the SCTP retransmission metric. aiortc
     increments `_sent_count` *before* calling `_send_chunk`, so a `DataChunk`
     with `_sent_count > 1` is a retransmission. If the transmit path is
     refactored, re-check that all (re)transmits still funnel through
     `_send_chunk`.
   aioice's TURN client has **no** `mapped_address` attribute — don't try to
   read one (an earlier version did, and it was dead code).
4. **TLS verification.** `--insecure` only reaches the STUN and standalone TURN
   contexts. aiortc builds the WebRTC `turns:` TLS context itself and always
   verifies — don't claim `--insecure` covers WebRTC.
5. **Port defaults follow transport** (`5349` for tls, else `3478`). Because all
   sub-tests now honor `--transport`, this is consistent; if you decouple ports
   per-test, re-check this logic.

## Dependency bumps

If you change `requirements.txt`, recreate the venv (`make clean && make venv`)
and run `make test` against a real server. Confirm the private-attribute access
points still resolve: relay pruning logs "kept N relay", "Relay-only path
confirmed" prints, and the "SCTP retransmissions" line appears. The exporter's
`webrtc_capacity_probe` leans on the same internals — also run the exporter
against a real server and confirm its `[CAPACITY] step …` lines show non-zero
DATA-chunk counts and `[CAPACITY] … Relay-only path confirmed` prints (if
`instrument_sctp_retransmits` breaks, the probe logs "could not instrument SCTP
retransmissions" and fails). Update `.version` accordingly.
