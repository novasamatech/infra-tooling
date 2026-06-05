<!--
Copyright © 2025 Novasama Technologies GmbH
SPDX-License-Identifier: Apache-2.0
-->

# turn-testing

A command-line tool that verifies a [CoTURN](https://github.com/coturn/coturn)
(STUN/TURN) server is reachable and actually relays traffic. It runs three
independent checks and exits non-zero if any of them fail, so it can be used
both interactively and as a health probe in CI.

The same checks are also available as a **Prometheus exporter** (`exporter.py`)
that probes any number of servers on a timer — see
[Prometheus exporter](#prometheus-exporter). The shared test logic lives in the
`turntest.py` library, consumed by both the CLI and the exporter.

## What it checks

| Test | What it does | Pass criteria |
| --- | --- | --- |
| **STUN Binding** | Sends a STUN binding request and reads back the server-reflexive (public) address. | A valid binding response is received. |
| **TURN Allocation** | Authenticates and asks the server to allocate a relayed transport address. | The server returns a relayed address (`XOR-RELAYED-ADDRESS`). |
| **WebRTC Data Channel** | Spins up two peer connections in one process, forces a **relay-only** path through the TURN server, opens an SCTP data channel, and streams a paced pseudorandom payload in both directions. | The connection is established **relay↔relay**, the achieved throughput is within 80 % of the target, every byte arrives (`bytes_sent == bytes_received`), and the SHA‑256 of the stream matches end to end. |

All three honour the `--transport` flag (`udp` / `tcp` / `tls`).

## Requirements

Python 3.11+ (the exporter reads TOML via the stdlib `tomllib`) and the pinned
dependencies in [`requirements.txt`](requirements.txt) (`aiortc`, `aioice`,
their native deps, and `prometheus_client` for the exporter).

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
| `-u, --username` | Username for static authentication. | — |
| `-p, --password` | Password for static authentication. | — |
| `--auth-secret` | coturn `static-auth-secret` for TURN REST (time-limited) credentials; alternative to `-u`/`-p`. | — |
| `--auth-user` | Optional user id embedded in the REST username (`timestamp:userid`). | — |
| `--auth-ttl` | Lifetime of derived REST credentials, seconds. | `3600` |
| `-t, --timeout` | Per-operation timeout in seconds. | `30` |
| `--stun-only` | Run only the STUN binding test. | — |
| `--turn-only` | Run only the TURN allocation test. | — |
| `--webrtc-only` | Run only the WebRTC data channel test (alias: `--webrtc-test`). | — |
| `--transport` | Transport for **every** sub-test: `udp`, `tcp`, or `tls`. | `udp` |
| `--duration` | WebRTC data-stream duration in seconds. | `10` |
| `--rate-mbps` | Target WebRTC data-stream rate in Mbps. | `1` |
| `--insecure` | Skip TLS certificate verification for all TLS sub-tests (STUN, TURN, and the WebRTC `turns:` relay — see caveats). | off |

`--stun-only`, `--turn-only`, and `--webrtc-only` are mutually exclusive; with
none of them set, all three tests run.

Authentication is either **static** (`-u` + `-p`) or **TURN REST** via
`--auth-secret` (coturn's `static-auth-secret`) — provide exactly one. In REST
mode the tool derives time-limited credentials: `username = <expiry>[:userid]`,
`password = base64(HMAC-SHA1(secret, username))`.

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

A **single image** ships both entry points. `ENTRYPOINT` is the Python
interpreter and the `CMD` is the script to run, so the default is the exporter
and the CLI is reached by overriding the `CMD`.

```sh
docker build -t turn-testing .
```

**Exporter (default).** Mount your config at `/config/coturn-exporter.toml`:

```sh
docker run --rm -p 9686:9686 \
  -v "$PWD/coturn-exporter.toml:/config/coturn-exporter.toml:ro" \
  turn-testing
# → serving metrics on http://0.0.0.0:9686/metrics
```

To point at a different path, override the `CMD`:
`docker run ... turn-testing exporter.py -c /config/other.toml`.

**One-shot CLI.** Override the `CMD` with `app.py` and its flags:

```sh
docker run --rm turn-testing \
  app.py -H turn.example.com -P 3478 -u myuser -p mypassword --transport tcp
```

> **UDP note:** the relay path between the two peers uses UDP. When the test
> runs inside Docker, make sure the container can reach the server's relay port
> range (CoTURN's `min-port`–`max-port`, e.g. `49152-65535`) and that NAT
> hair-pinning / egress is not blocked, otherwise the WebRTC test will time out
> even though STUN/TURN succeed.

## Prometheus exporter

`exporter.py` runs the same STUN / TURN / relay-only WebRTC checks against an
arbitrary list of servers on a schedule and exposes the results as Prometheus
metrics at `/metrics`. Servers and tunables come from a TOML config — copy
[`coturn-exporter.example.toml`](coturn-exporter.example.toml) and fill in real
values:

```sh
./exporter.py -c coturn-exporter.toml
# → serving metrics on http://0.0.0.0:9686/metrics
```

* Each server is probed once per `interval` (**default 300 s = 5 min**).
* The WebRTC check is an **adaptive capacity probe** — there is no fixed rate or
  duration to configure. It ramps the send rate up step by step
  (`ramp_start_mbps` × `ramp_factor` each step) and stops at the first step whose
  SCTP retransmission ratio reaches `retransmit_threshold_percent`
  (**default 0.5 %**). `turn_testing_webrtc_capacity_bits_per_second` is then the
  highest rate sustained below that threshold — the relay's usable throughput
  "knee". Because
  it stops at the first sign of loss, it stays gentle on the relay (no fixed
  high-rate soak). Safety caps `ramp_max_mbps` / `ramp_max_duration` bound a
  perfectly clean path.
* A short `ramp_warmup_duration` at the start rate runs first and is discarded,
  so SCTP slow-start / RTO transients don't make the first step read a spuriously
  high retransmit ratio.
* The threshold is the one knob you normally set; the ramp shape
  (`ramp_start_mbps`, `ramp_factor`, `ramp_step_duration`, `ramp_warmup_duration`,
  `ramp_max_mbps`, `ramp_max_duration`) has sane defaults.
* Per-server authentication is either static (`username`/`password`) or TURN REST
  (`auth_secret`), mirroring the CLI.
* **Robust to bad targets.** If a server is unreachable, times out, or rejects
  credentials, the affected sub-tests report `turn_testing_probe_success = 0` (per
  `transport` and `test`) and `turn_testing_webrtc_capacity_bits_per_second = 0`; the
  exporter logs the error and keeps cycling — one bad target never crashes it.
  (STUN binding needs no auth, so on a wrong credential STUN can still report `1`
  while TURN/WebRTC report `0` — telling you it's an auth problem, not reachability.)
* `--oneshot` runs a single cycle (useful for tests); the exporter shuts down
  cleanly on `SIGTERM`/`SIGINT`.

> **Do not commit a config with real credentials.** Only the `*.example.toml`
> placeholder is tracked; keep the live config out of the repo (mount it as a
> secret in production).

### Metrics

`turn_testing_probe_success` and `turn_testing_probe_duration_seconds` carry the labels
`server`, `transport`, and `test` (`stun` / `turn` / `webrtc`) — so you can see
exactly **which protocol on which transport** is failing; the `turn_testing_webrtc_*`
gauges carry `server` and `transport`:

| Metric | Meaning |
| --- | --- |
| `turn_testing_probe_success` | `1` if the sub-test passed, else `0`. Labelled per `server` × `transport` × `test`. |
| `turn_testing_probe_duration_seconds` | Sub-test latency, seconds. Recorded for `stun`/`turn` only (the webrtc value would be the ramp wall-time, not a latency). |
| `turn_testing_webrtc_capacity_bits_per_second` | Highest send rate sustained **below** the retransmit threshold, in **bits/second** (e.g. `1.6e7` = 16 Mbit/s) — the headline result. |
| `turn_testing_webrtc_threshold_reached` | `1` if a knee was found; `0` if the ramp hit the rate/time cap (capacity is then a floor, the real value may be higher). |
| `turn_testing_cycle_duration_seconds` | Duration of the most recent probe cycle (no labels). |
| `turn_testing_cycles_total` | Completed probe cycles (counter); `increase(turn_testing_cycles_total[N]) == 0` means the exporter stopped probing. |
| `turn_testing_exporter_build_info` | Exporter version carried in the `version` label; value is always `1`. |

Running the exporter in Docker is covered by the unified image in
[Docker](#docker) above (it is the default `CMD`).

### Alerting rules

Ready-to-use **Prometheus alerting rules** (load into Prometheus; Alertmanager
routes them by the `severity` label). They are designed to avoid false positives:

* **Layered reachability.** STUN needs no auth, so a STUN failure is pure
  reachability; TURN failing *while STUN works* is an auth/allocation problem;
  WebRTC failing *while TURN works* is a relay-path problem. Each rule is guarded
  by "the layer below is OK", so exactly **one** fires and names the broken layer
  instead of three firing at once.
* **`for:` spans several probe cycles** (the values below assume the default
  `interval: 300` → `15m` ≈ 3 cycles), so a single dropped UDP packet or a
  momentary relay blip never pages.
* **Exporter-liveness meta-alerts** fire if the exporter is unscrapeable or
  stops cycling — otherwise the probe gauges would freeze at their last value and
  the target alerts would go *silently* stale (a false negative).
* **Capacity is treated as noisy** (it legitimately reads `0` on a healthy server
  for a single cycle): it is averaged over an hour, compared to a conservative
  floor, gated on the path being up, and kept `warning`.

```yaml
groups:
  - name: coturn-exporter-liveness
    rules:
      - alert: CoturnExporterDown
        expr: up{job="coturn-exporter"} == 0          # adjust job to your scrape config
        for: 5m
        labels: { severity: critical }
        annotations:
          summary: "CoTURN exporter not scrapeable ({{ $labels.instance }})"
          description: "All turn_testing_* metrics are stale; the target alerts are blind until this clears."

      - alert: CoturnExporterStalled
        # The window MUST be larger than the exporter's `interval` (default 300s).
        expr: increase(turn_testing_cycles_total[20m]) < 1
        for: 5m
        labels: { severity: critical }
        annotations:
          summary: "CoTURN exporter stopped probing"
          description: "Exporter is up but completed no probe cycle in 20m — metrics are frozen."

  - name: coturn-targets
    rules:
      # STUN is unauthenticated → a STUN failure is pure reachability (host/port/DNS/net).
      - alert: CoturnServerUnreachable
        expr: turn_testing_probe_success{test="stun"} == 0
        for: 15m
        labels: { severity: critical }
        annotations:
          summary: "CoTURN unreachable: {{ $labels.server }} via {{ $labels.transport }}"
          description: "STUN binding has failed for 15m (≥3 cycles) — the server/port is unreachable on this transport."

      # STUN OK but TURN not → auth / allocation problem, not the network.
      - alert: CoturnTurnAllocationFailing
        expr: |
          turn_testing_probe_success{test="turn"} == 0
          and on (server, transport) turn_testing_probe_success{test="stun"} == 1
        for: 15m
        labels: { severity: critical }
        annotations:
          summary: "CoTURN TURN allocation failing: {{ $labels.server }} via {{ $labels.transport }}"
          description: "STUN is fine but TURN allocation failed for 15m — likely credentials / static-auth-secret / realm."

      # TURN allocates but the relayed data path won't come up → relay ports / NAT / egress.
      - alert: CoturnRelayPathFailing
        expr: |
          turn_testing_probe_success{test="webrtc"} == 0
          and on (server, transport) turn_testing_probe_success{test="turn"} == 1
        for: 20m
        labels: { severity: critical }
        annotations:
          summary: "CoTURN relay path failing: {{ $labels.server }} via {{ $labels.transport }}"
          description: "TURN allocates but no relay↔relay channel for 20m — check the relay port range (min-port–max-port) and egress/NAT."

      # TLS path fails while a non-TLS transport on the SAME server works → cert/TLS-specific.
      - alert: CoturnTlsProbeFailing
        expr: |
          turn_testing_probe_success{transport="tls", test="turn"} == 0
          and on (server) max by (server) (turn_testing_probe_success{transport!="tls", test="turn"}) == 1
        for: 15m
        labels: { severity: warning }
        annotations:
          summary: "CoTURN TLS path failing: {{ $labels.server }}"
          description: "turns: (TLS) fails while UDP/TCP work on the same server — most often an expired/untrusted/mismatched certificate."

      # Relay reachable but slow/lossy. Capacity is noisy → average over 1h, floor well
      # below your relays' normal knee, gated on the path being up. Tune 2e6 to your env.
      - alert: CoturnCapacityDegraded
        expr: |
          avg_over_time(turn_testing_webrtc_capacity_bits_per_second[1h]) < 2e6
          and on (server, transport) turn_testing_probe_success{test="webrtc"} == 1
        for: 30m
        labels: { severity: warning }
        annotations:
          summary: "CoTURN relay throughput degraded: {{ $labels.server }} via {{ $labels.transport }}"
          description: "1h-average relay capacity below 2 Mbit/s while the path is up — reachable but slow/lossy."
```

**Tuning notes**

* Set `job="coturn-exporter"` to your scrape job name.
* Scale every `for:` to **2–3× your `interval`**, and keep the
  `CoturnExporterStalled` window **larger than `interval`**.
* The layered `and on(...)` guards assume `tests = ["stun", "turn", "webrtc"]`
  (the default). If you run a reduced `tests` list, drop the guard and alert on
  `turn_testing_probe_success{test="…"} == 0` directly.
* `CoturnCapacityDegraded`'s `2e6` floor is a placeholder — capacity varies, so
  pick a value well under your relays' typical knee and keep it `warning`.

## Test suite

A [`Makefile`](Makefile) runs the full scenario matrix against a live server: a
no-network exporter smoke (`make test-exporter`), UDP/TCP/TLS × STUN/TURN/WebRTC,
argument-validation guards, and low/moderate/high speed modes. It creates the
local `venv/` on demand and installs
[`requirements.txt`](requirements.txt) into it. Server, ports and credentials are
taken from **environment variables with no defaults**:

```sh
export TURN_HOST=turn.example.com   # hostname (required for the TLS WebRTC cert check)
export TURN_PORT=3478               # UDP/TCP STUN/TURN port
export TURN_TLS_PORT=5349           # TLS (turns:) port

# then EITHER static auth …
export TURN_USER=myuser
export TURN_PASS=mypass
# … OR a TURN REST secret (mutually exclusive with TURN_USER/TURN_PASS)
export TURN_SECRET=<static-auth-secret>

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

* **TLS verification:** off by default — every TLS sub-test verifies the server
  certificate against the system trust store. `--insecure` (exporter: `insecure`)
  disables verification for **all** TLS sub-tests: STUN, standalone TURN, *and*
  the WebRTC `turns:` relay. aiortc/aioice build the WebRTC `turns:` TLS context
  internally with no public knob to relax it, so the tool reaches into the aioice
  connection and swaps in a non-verifying context before ICE gathering (a
  private-attribute access — see *Library internals*). For a verifying run use a
  certificate trusted by the host and a hostname (not a bare IP) for the
  `--transport tls` WebRTC test.
* **Library internals:** relay-only enforcement and the candidate-pair
  inspection depend on private attributes of `aiortc`/`aioice`. They are
  validated against the **pinned** versions in `requirements.txt` and may need
  updating after a dependency bump. See [AGENTS.md](AGENTS.md).

## License

Apache-2.0 — see [`../LICENSE`](../LICENSE).
