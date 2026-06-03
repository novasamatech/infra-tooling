<!--
Copyright © 2025 Novasama Technologies GmbH
SPDX-License-Identifier: Apache-2.0
-->

# turn-testing

A command-line tool that verifies a [CoTURN](https://github.com/coturn/coturn)
(STUN/TURN) server is reachable and actually relays traffic. It runs three
independent checks and exits non-zero if any of them fail, so it can be used
both interactively and as a health probe in CI.

## What it checks

| Test | What it does | Pass criteria |
| --- | --- | --- |
| **STUN Binding** | Sends a STUN binding request and reads back the server-reflexive (public) address. | A valid binding response is received. |
| **TURN Allocation** | Authenticates and asks the server to allocate a relayed transport address. | The server returns a relayed address (`XOR-RELAYED-ADDRESS`). |
| **WebRTC Data Channel** | Spins up two peer connections in one process, forces a **relay-only** path through the TURN server, opens an SCTP data channel, and streams a paced pseudorandom payload in both directions. | The connection is established **relay↔relay**, the achieved throughput is within 80 % of the target, every byte arrives (`bytes_sent == bytes_received`), and the SHA‑256 of the stream matches end to end. |

All three honour the `--transport` flag (`udp` / `tcp` / `tls`).

## Requirements

Python 3.11+ and the pinned dependencies in [`requirements.txt`](requirements.txt)
(`aiortc`, `aioice`, and their native deps).

```sh
# Use the bundled virtualenv
source venv/bin/activate

# …or create your own
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

## Usage

```sh
./app.py -H <host> -P <port> -u <username> -p <password> [options]
```

### Options

| Flag | Description | Default |
| --- | --- | --- |
| `-H, --host` | TURN server address (hostname or IP). **Required.** | — |
| `-P, --port` | TURN server port. | `3478` for udp/tcp, `5349` for tls |
| `-u, --username` | Username for authentication. **Required.** | — |
| `-p, --password` | Password for authentication. **Required.** | — |
| `-t, --timeout` | Per-operation timeout in seconds. | `30` |
| `--stun-only` | Run only the STUN binding test. | — |
| `--turn-only` | Run only the TURN allocation test. | — |
| `--webrtc-only` | Run only the WebRTC data channel test (alias: `--webrtc-test`). | — |
| `--transport` | Transport for **every** sub-test: `udp`, `tcp`, or `tls`. | `udp` |
| `--duration` | WebRTC data-stream duration in seconds. | `10` |
| `--rate-mbps` | Target WebRTC data-stream rate in Mbps. | `1` |
| `--insecure` | Skip TLS certificate verification (TLS STUN/TURN only — see caveats). | off |

`--stun-only`, `--turn-only`, and `--webrtc-only` are mutually exclusive; with
none of them set, all three tests run.

### Examples

```sh
# All three tests over UDP
./app.py -H turn.example.com -P 3478 -u myuser -p mypass

# Force TCP for every sub-test
./app.py -H turn.example.com -P 3478 -u myuser -p mypass --transport tcp

# TLS (turns:) — uses port 5349 by default
./app.py -H turn.example.com -u myuser -p mypass --transport tls --webrtc-only

# STUN / TURN only
./app.py -H turn.example.com -P 3478 -u myuser -p mypass --stun-only
./app.py -H turn.example.com -P 3478 -u myuser -p mypass --turn-only

# Throughput soak: 25 Mbps for 30 s through the relay
./app.py -H turn.example.com -P 3478 -u myuser -p mypass \
    --webrtc-only --rate-mbps 25 --duration 30
```

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | All selected tests passed. |
| `1` | At least one selected test failed. |
| `2` | Invalid arguments (argparse). |

Progress lines go to **stdout**; errors and warnings go to **stderr**.

## Docker

The image runs `app.py` via `ENTRYPOINT`, so CLI arguments are appended to
`docker run`:

```sh
docker build -t turn-testing .

docker run --rm turn-testing \
  -H turn.example.com -P 3478 -u myuser -p mypassword --transport tcp
```

> **UDP note:** the relay path between the two peers uses UDP. When the test
> runs inside Docker, make sure the container can reach the server's relay port
> range (CoTURN's `min-port`–`max-port`, e.g. `49152-65535`) and that NAT
> hair-pinning / egress is not blocked, otherwise the WebRTC test will time out
> even though STUN/TURN succeed.

## Test suite

A [`Makefile`](Makefile) runs the full scenario matrix against a live server:
UDP/TCP/TLS × STUN/TURN/WebRTC, argument-validation guards, and low/moderate/high
speed modes. It creates the local `venv/` on demand and installs
[`requirements.txt`](requirements.txt) into it. Server, ports and credentials are
taken from **environment variables with no defaults**:

```sh
export TURN_HOST=turn.example.com   # hostname (required for the TLS WebRTC cert check)
export TURN_PORT=3478               # UDP/TCP STUN/TURN port
export TURN_TLS_PORT=5349           # TLS (turns:) port
export TURN_USER=myuser
export TURN_PASS=mypass

make test          # full run
make help          # list individual targets (test-udp, test-tcp, test-tls, …)
```

The high-rate probe in `make test-speed` may fail by design on a bandwidth-limited
path; it is informational and does not fail `make test`. Contributors should also
read [AGENTS.md](AGENTS.md).

## How the WebRTC test works

* aiortc has no `iceTransportPolicy: "relay"`, so a relay-only path is enforced
  manually: after ICE gathering, non-relay (`host`/`srflx`) candidates are
  stripped from both the SDP and the underlying ICE connection. If the server
  offers no relay candidate, the test fails fast.
* The payload is a deterministic pseudorandom stream (fixed seed) so the
  receiver can verify integrity with a SHA‑256 over the whole stream.
* The sender is **paced** to `--rate-mbps` and applies **backpressure** when the
  SCTP send buffer grows beyond ~4 MiB, so a slow relay degrades gracefully
  instead of exhausting memory. A run that cannot sustain the target rate is
  reported as a throughput failure (this is intentional).
* The tool reports the **SCTP retransmission rate** for the stream, e.g.:

  ```
  [WEBRTC] SCTP retransmissions PC1->PC2 (stream): 64/920 DATA chunks (6.96%, high loss)
  ```

  Because the data channel is **reliable and ordered**, the application never
  observes loss — every byte is eventually delivered. The retransmission
  percentage is therefore the only window into how lossy the underlying
  path/relay is. It is printed even when the run fails its throughput target,
  so you can tell *why* throughput was low (loss/congestion vs. a hard cap).
  Rough guide: `0%` none, `<1%` good, `1–5%` moderate, `>5%` high loss.
  The metric is measured on the sender (the `PC1 → PC2` stream direction).
* After the transfer, the tool prints the **selected candidate pair** and
  confirms both ends are of type `relay`.

> The relayed ICE candidate is always advertised as `udp` even when
> `--transport tcp`/`tls` is used — `--transport` controls the **client → TURN**
> leg, not the relayed leg.

## Caveats

* **TLS verification:** `--insecure` only affects the STUN and standalone TURN
  allocation tests (whose TLS context this tool controls). The WebRTC
  `turns:` connection is established by aiortc/aioice internally and **always**
  verifies the server certificate against the system trust store. Use a
  certificate trusted by the host (or a hostname, not a bare IP) for the
  `--transport tls` WebRTC test.
* **Library internals:** relay-only enforcement and the candidate-pair
  inspection depend on private attributes of `aiortc`/`aioice`. They are
  validated against the **pinned** versions in `requirements.txt` and may need
  updating after a dependency bump. See [AGENTS.md](AGENTS.md).

## License

Apache-2.0 — see [`../LICENSE`](../LICENSE).
