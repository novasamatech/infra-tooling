<!--
Copyright © 2025 Novasama Technologies GmbH
SPDX-License-Identifier: Apache-2.0
-->

# AGENTS.md — turn-testing

Guidance for AI agents and contributors working on this directory. Read
[README.md](README.md) first for user-facing behavior; this file covers the
*how* and the *gotchas*.

## Scope

A single-file Python CLI (`app.py`) that tests a CoTURN server: STUN binding,
TURN allocation, and a relay-only WebRTC data-channel throughput/integrity
check. No package, no submodules — keep it a self-contained script.

## Files

| File | Purpose |
| --- | --- |
| `app.py` | The whole tool. Executable, `#!/usr/bin/env python3`. |
| `requirements.txt` | **Pinned** deps. The code relies on private internals of these exact versions. |
| `Dockerfile` | Alpine + Python 3.13, non-root user, `ENTRYPOINT ["python","app.py"]`. |
| `Makefile` | Test runner: creates `venv/` on demand and runs the full scenario matrix. `make help`. |
| `.version` | Image/release version tag (bump on behavioral changes). |
| `venv/` | Local virtualenv (gitignored). Not committed. |

## Running & testing

This is Python, not a JS project — run it directly through the venv (the global
"never run the JS toolchain on the host" rule does **not** apply here):

```sh
venv/bin/python -m py_compile app.py     # quick syntax check
```

There are **no unit tests**; validation is done against a live CoTURN server via
the [`Makefile`](Makefile). It creates `./venv` on demand (installing
`requirements.txt`) and runs the full scenario matrix — UDP/TCP/TLS ×
stun/turn/webrtc/all-three, the `--insecure` smoke, the argument guards (each
must exit 2), and the low/moderate/high speed modes (which print the SCTP
retransmission metric).

Server, ports and credentials come from **environment variables with no
defaults**:

```sh
export TURN_HOST=...        # hostname (required for the TLS WebRTC cert check)
export TURN_PORT=3478       # UDP/TCP STUN/TURN port
export TURN_TLS_PORT=5349   # TLS (turns:) port
export TURN_USER=...
export TURN_PASS=...
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

* **`main()`** parses args, picks which sub-tests to run (the three `--*-only`
  flags are a mutually exclusive group), then drives them inside one
  `asyncio.run`. Exit `0` = all selected tests passed, `1` = a failure, `2` =
  bad args.
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
confirmed" prints, and the "SCTP retransmissions" line appears. Update
`.version` accordingly.
