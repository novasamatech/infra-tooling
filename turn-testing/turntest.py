#!/usr/bin/env python3

# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""
Shared CoTURN connectivity-test library.

This module holds the reusable building blocks for testing a CoTURN server:
STUN binding, TURN allocation, and a relay-only WebRTC data-channel
throughput/integrity check. It is consumed by two entry points:

* ``app.py``     — the single-shot CLI (unchanged user-facing behaviour);
* ``exporter.py`` — a Prometheus exporter that probes many servers on a timer.

The three ``test_*`` coroutines preserve the exact stdout/stderr progress
output of the original script but additionally **return** a small result
dataclass (``StunResult`` / ``TurnResult`` / ``WebRtcResult``) so callers can
read the measured metrics (durations, throughput, SCTP retransmissions, …)
instead of only a pass/fail boolean. ``.ok`` is the pass/fail flag the CLI uses.

Note on internals:
    The relay-only enforcement and the candidate-pair inspection rely on
    private attributes of aiortc/aioice (the WebRTC API has no
    iceTransportPolicy="relay" in aiortc). They are validated against the
    pinned versions in requirements.txt and may break on a library upgrade.
"""

import asyncio
import base64
import hashlib
import hmac
import random
import ssl
import sys
import time
from dataclasses import dataclass
from typing import Optional, Tuple

# Check for aiortc
try:
    from aioice import stun, turn
    from aiortc import (
        RTCConfiguration,
        RTCIceServer,
        RTCPeerConnection,
        RTCSessionDescription,
    )

    AIORTC_AVAILABLE = True
except ImportError:
    AIORTC_AVAILABLE = False


# --------------------------------------------------------------------------- #
# Result types
# --------------------------------------------------------------------------- #
@dataclass
class StunResult:
    """Outcome of a STUN binding test."""

    ok: bool
    duration: float = 0.0
    mapped_address: Optional[Tuple[str, int]] = None
    software: Optional[str] = None


@dataclass
class TurnResult:
    """Outcome of a TURN allocation test."""

    ok: bool
    duration: float = 0.0
    relayed_address: Optional[Tuple[str, int]] = None


@dataclass
class WebRtcResult:
    """Outcome of a relay-only WebRTC data-channel test."""

    ok: bool
    duration: float = 0.0
    bytes_sent: int = 0
    bytes_received: int = 0
    send_mbps: float = 0.0
    recv_mbps: float = 0.0
    retransmit_sent: int = 0
    retransmit_count: int = 0
    relay_confirmed: bool = False

    @property
    def retransmit_ratio(self) -> Optional[float]:
        """Fraction of DATA chunks retransmitted, or None if nothing was sent."""
        if self.retransmit_sent == 0:
            return None
        return self.retransmit_count / self.retransmit_sent


@dataclass
class WebRtcCapacityResult:
    """Outcome of a ramp-up WebRTC capacity probe.

    The probe raises the send rate step by step until the per-step SCTP DATA
    retransmission ratio reaches ``threshold_ratio``. ``capacity_mbps`` is the
    highest rate sustained strictly *below* that threshold.
    """

    ok: bool
    duration: float = 0.0
    capacity_mbps: float = 0.0
    retransmit_ratio: float = 0.0  # ratio at the deciding (last) step
    threshold_ratio: float = 0.0  # the configured setpoint, echoed back
    stopped_on_threshold: bool = False  # True = found a knee; False = hit a cap
    relay_confirmed: bool = False
    peak_attempted_mbps: float = 0.0  # highest rate actually sent at


def eprint(*args, **kwargs):
    """Print diagnostics/errors to stderr so they don't pollute stdout."""
    kwargs.setdefault("file", sys.stderr)
    print(*args, **kwargs)


def make_ssl_context(insecure):
    """Build a client SSL context, optionally skipping verification."""
    ctx = ssl.create_default_context()
    if insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def make_rest_credentials(secret, ttl, userid=None):
    """Derive time-limited TURN REST credentials from a static-auth-secret.

    Mirrors coturn `use-auth-secret`: the username is an expiry UNIX timestamp
    (optionally ``timestamp:userid``) and the password is
    ``base64(HMAC-SHA1(secret, username))``. The secret is used as the raw HMAC
    key, exactly as coturn treats `static-auth-secret`.
    """
    expiry = int(time.time()) + ttl
    username = f"{expiry}:{userid}" if userid else str(expiry)
    digest = hmac.new(secret.encode(), username.encode(), hashlib.sha1).digest()
    password = base64.b64encode(digest).decode("ascii")
    return username, password


def build_turn_url(host, port, transport):
    """Compose TURN/TURNS URL with explicit transport parameter."""
    scheme = "turns" if transport == "tls" else "turn"
    transport_param = "tcp" if transport in ("tcp", "tls") else "udp"
    return f"{scheme}:{host}:{port}?transport={transport_param}"


async def wait_for_ice_gathering_complete(pc, timeout=10):
    """Wait until ICE gathering completes for the given peer connection."""
    if pc.iceGatheringState == "complete":
        return

    loop = asyncio.get_running_loop()
    future = loop.create_future()

    @pc.on("icegatheringstatechange")
    def on_state_change():
        if pc.iceGatheringState == "complete" and not future.done():
            future.set_result(True)

    await asyncio.wait_for(future, timeout=timeout)


def filter_sdp_for_relay_only(description, label):
    """Return a new RTCSessionDescription with only relay candidates."""
    lines = description.sdp.split("\r\n")
    filtered = []
    relay = 0
    dropped = 0

    for line in lines:
        if line.startswith("a=candidate:") and " typ relay " not in line:
            dropped += 1
            continue
        if line.startswith("a=candidate:"):
            relay += 1
        filtered.append(line)

    print(
        f"[WEBRTC] {label}: SDP filtered to relay-only (kept {relay}, removed {dropped})"
    )
    return RTCSessionDescription(sdp="\r\n".join(filtered), type=description.type)


def prune_local_candidates_to_relay(pc, label):
    """Remove non-relay candidates from the local ICE gatherer.

    aiortc has no iceTransportPolicy="relay", so we drop host/srflx
    candidates from the underlying aioice connection before connectivity
    checks start. Relies on private attributes (see module docstring).
    """
    try:
        ice_gatherer = pc.sctp.transport.transport.iceGatherer  # type: ignore[attr-defined]
        conn = ice_gatherer._connection  # type: ignore[attr-defined]
        before = len(conn._local_candidates)  # type: ignore[attr-defined]
        conn._local_candidates = [  # type: ignore[attr-defined]
            c
            for c in conn._local_candidates
            if c.type == "relay"  # type: ignore[attr-defined]
        ]
        after = len(conn._local_candidates)  # type: ignore[attr-defined]
        print(
            f"[WEBRTC] {label}: local candidates pruned to relay-only (kept {after}, removed {before - after})"
        )
        return after
    except Exception as e:
        eprint(f"[WEBRTC] WARNING: Could not prune local candidates for {label}: {e}")
        return None


def apply_insecure_turns_tls(pc, transport, insecure, label, tag="[WEBRTC]"):
    """Relax TLS verification on the WebRTC ``turns:`` relay connection.

    aiortc builds the ``turns:`` TLS context itself: it hands aioice
    ``turn_ssl=True``, which becomes a *verifying* default ``SSLContext``, and
    exposes no public knob to relax it — so ``insecure`` otherwise never reaches
    the WebRTC path (STUN/TURN honor it, WebRTC does not). We reach into the
    aioice ``Connection`` and swap that ``turn_ssl`` flag for a non-verifying
    ``SSLContext`` *before* candidate gathering starts (``create_turn_endpoint``
    accepts an ``SSLContext`` in place of the bool). Relies on private attributes
    (see module docstring).

    No-op unless ``transport == 'tls'`` and ``insecure``. The ``conn.turn_ssl``
    guard ensures we only relax an existing ``turns:`` context and never
    accidentally enable TLS on a plaintext ``turn:`` connection. Must run after
    the peer's SCTP transport exists (offerer: after ``createDataChannel``;
    answerer: after ``setRemoteDescription``) and before its gathering.
    """
    if transport != "tls" or not insecure:
        return
    try:
        conn = pc.sctp.transport.transport.iceGatherer._connection  # type: ignore[attr-defined]
        if conn.turn_ssl:  # only relax an actual turns: context
            conn.turn_ssl = make_ssl_context(insecure=True)  # type: ignore[attr-defined]
            print(f"{tag} {label}: turns: TLS verification disabled (insecure)")
    except Exception as e:
        eprint(
            f"{tag} WARNING: could not apply insecure TLS to turns: for {label}: {e}"
        )


def instrument_sctp_retransmits(pc):
    """Wrap a peer's SCTP transport to count DATA chunk (re)transmissions.

    A reliable/ordered data channel hides channel loss from the application
    (every byte is eventually delivered), so the SCTP retransmission rate is
    the only window into how lossy the underlying path actually is. We wrap
    ``_send_chunk``: aiortc increments ``chunk._sent_count`` before sending, so
    a DataChunk arriving here with ``_sent_count > 1`` is a retransmission.
    Relies on aiortc internals (see module docstring); degrades gracefully.
    """
    try:
        from aiortc.rtcsctptransport import DataChunk
    except Exception:
        return None

    sctp = getattr(pc, "sctp", None)
    if sctp is None or not hasattr(sctp, "_send_chunk"):
        return None

    stats = {"data_sent": 0, "data_retx": 0}
    original = sctp._send_chunk

    async def counting_send_chunk(chunk):
        if isinstance(chunk, DataChunk):
            stats["data_sent"] += 1
            if getattr(chunk, "_sent_count", 1) > 1:
                stats["data_retx"] += 1
        return await original(chunk)

    try:
        sctp._send_chunk = counting_send_chunk
    except Exception:
        return None
    return stats


def format_retransmit_rate(stats, direction):
    """Render an SCTP retransmission summary line, or None if no DATA was sent."""
    if not stats or stats["data_sent"] == 0:
        return None
    total = stats["data_sent"]
    retx = stats["data_retx"]
    pct = retx / total * 100
    if pct == 0:
        verdict = "no loss"
    elif pct < 1:
        verdict = "good"
    elif pct < 5:
        verdict = "moderate loss"
    else:
        verdict = "high loss"
    return (
        f"[WEBRTC] SCTP retransmissions {direction}: "
        f"{retx}/{total} DATA chunks ({pct:.2f}%, {verdict})"
    )


def report_stun_response(response):
    """Print a STUN binding response; return (ok, mapped_address, software)."""
    if response and response.message_class == stun.Class.RESPONSE:
        print("[STUN] Binding successful!")

        mapped_addr = response.attributes.get("XOR-MAPPED-ADDRESS")
        if mapped_addr:
            print(f"[STUN] Your public address: {mapped_addr[0]}:{mapped_addr[1]}")

        software = response.attributes.get("SOFTWARE")
        if software:
            print(f"[STUN] Server software: {software}")

        return True, mapped_addr, software

    eprint(f"[STUN] Unexpected response: {response}")
    return False, None, None


class StunProtocol(asyncio.DatagramProtocol):
    """Simple STUN protocol handler for UDP binding requests."""

    def __init__(self):
        self.transport = None
        self.response_future = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        try:
            message = stun.parse_message(data)
            if self.response_future and not self.response_future.done():
                self.response_future.set_result(message)
        except Exception as e:
            if self.response_future and not self.response_future.done():
                self.response_future.set_exception(e)

    def arm(self):
        """Create the response future before sending, to avoid a race."""
        self.response_future = asyncio.get_running_loop().create_future()

    def send_stun(self, message):
        data = bytes(message)
        self.transport.sendto(data)


class TurnTestProtocol(asyncio.DatagramProtocol):
    """Simple protocol for TURN allocation test."""

    def __init__(self):
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        pass


def build_binding_request():
    """Create a STUN binding request (constructor already sets a random id)."""
    return stun.Message(
        message_method=stun.Method.BINDING,
        message_class=stun.Class.REQUEST,
    )


async def _stun_binding_udp(host, port, timeout):
    """STUN binding request over UDP."""
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: StunProtocol(), remote_addr=(host, port)
    )
    try:
        protocol.arm()
        protocol.send_stun(build_binding_request())
        response = await asyncio.wait_for(protocol.response_future, timeout=timeout)
        return report_stun_response(response)
    finally:
        transport.close()


async def _stun_binding_stream(host, port, timeout, use_tls, insecure):
    """STUN binding request over TCP (RFC 5389 framing) or TLS."""
    ssl_ctx = make_ssl_context(insecure) if use_tls else None
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port, ssl=ssl_ctx), timeout=timeout
    )
    try:
        writer.write(bytes(build_binding_request()))
        await writer.drain()

        # The STUN header is 20 bytes; bytes 2-3 hold the attribute length.
        header = await asyncio.wait_for(reader.readexactly(20), timeout=timeout)
        length = int.from_bytes(header[2:4], "big")
        body = b""
        if length:
            body = await asyncio.wait_for(
                reader.readexactly(length), timeout=timeout
            )
        response = stun.parse_message(header + body)
        return report_stun_response(response)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def test_stun_binding(host, port, timeout=30, transport="udp", insecure=False):
    """Test a STUN binding request over the selected transport.

    Returns a ``StunResult`` (``.ok`` is the pass/fail flag).
    """
    print(
        f"\n[STUN] Testing binding request to {host}:{port} via {transport.upper()}..."
    )

    start = time.monotonic()
    try:
        if transport == "udp":
            ok, mapped, software = await _stun_binding_udp(host, port, timeout)
        else:
            ok, mapped, software = await _stun_binding_stream(
                host, port, timeout, use_tls=(transport == "tls"), insecure=insecure
            )
        return StunResult(
            ok=ok,
            duration=time.monotonic() - start,
            mapped_address=mapped,
            software=software,
        )
    except asyncio.TimeoutError:
        eprint(f"[STUN] Timeout after {timeout}s")
        return StunResult(ok=False, duration=time.monotonic() - start)
    except Exception as e:
        eprint(f"[STUN] Error: {e}")
        import traceback

        traceback.print_exc()
        return StunResult(ok=False, duration=time.monotonic() - start)


async def test_turn_allocation(
    host,
    port,
    username,
    password,
    timeout=30,
    transport="udp",
    insecure=False,
):
    """Test TURN allocation over the selected transport.

    Returns a ``TurnResult`` (``.ok`` is the pass/fail flag).
    """
    print(
        f"\n[TURN] Testing allocation to {host}:{port} via {transport.upper()} "
        f"with user '{username}'..."
    )

    start = time.monotonic()
    endpoint_kwargs = {}
    if transport in ("tcp", "tls"):
        endpoint_kwargs["transport"] = "tcp"
        if transport == "tls":
            endpoint_kwargs["ssl"] = make_ssl_context(insecure)
    else:
        endpoint_kwargs["transport"] = "udp"

    try:
        # Create TURN endpoint using aioice
        turn_transport, _protocol = await asyncio.wait_for(
            turn.create_turn_endpoint(
                TurnTestProtocol,
                server_addr=(host, port),
                username=username,
                password=password,
                **endpoint_kwargs,
            ),
            timeout=timeout,
        )

        try:
            # Get allocation info from TurnTransport (private attr, see docstring)
            relayed_address = getattr(
                turn_transport, "_TurnTransport__relayed_address", None
            )

            if relayed_address:
                print("[TURN] Allocation successful!")
                print(
                    f"[TURN] Relayed address: {relayed_address[0]}:{relayed_address[1]}"
                )
                return TurnResult(
                    ok=True,
                    duration=time.monotonic() - start,
                    relayed_address=relayed_address,
                )

            eprint("[TURN] No relayed address received")
            return TurnResult(ok=False, duration=time.monotonic() - start)

        finally:
            turn_transport.close()

    except asyncio.TimeoutError:
        eprint(f"[TURN] Timeout after {timeout}s")
        return TurnResult(ok=False, duration=time.monotonic() - start)
    except Exception as e:
        eprint(f"[TURN] Error: {e}")
        import traceback

        traceback.print_exc()
        return TurnResult(ok=False, duration=time.monotonic() - start)


async def test_webrtc_datachannel(
    host,
    port,
    username,
    password,
    timeout=30,
    transport="udp",
    stream_duration=10,
    rate_mbps=1.0,
    insecure=False,
):
    """
    Full WebRTC Data Channel test through TURN relay.

    Creates two peer connections in one process, forces relay-only candidates,
    establishes a Data Channel, and verifies bidirectional communication with a
    paced pseudorandom stream (throughput + SHA-256 integrity).

    Returns a ``WebRtcResult`` (``.ok`` is the pass/fail flag; the other fields
    carry the measured throughput / byte counts / SCTP retransmissions).
    """
    print(
        f"\n[WEBRTC] Testing Data Channel through TURN relay {host}:{port} via {transport.upper()}..."
    )
    print(
        f"[WEBRTC] Target stream: {rate_mbps} Mbps for {stream_duration}s (data channel)"
    )

    test_start = time.monotonic()

    # Configure TURN server (transport can be udp/tcp/tls)
    turn_url = build_turn_url(host, port, transport)
    ice_servers = [
        RTCIceServer(
            urls=[turn_url],
            username=username,
            credential=password,
        )
    ]
    config = RTCConfiguration(iceServers=ice_servers)

    # Create peer connections
    pc1 = RTCPeerConnection(configuration=config)
    pc2 = RTCPeerConnection(configuration=config)

    # State tracking
    connection_complete = asyncio.Event()
    received_messages = []
    dc1_open = asyncio.Event()
    ack_received = asyncio.Event()
    recv_stats = {
        "bytes": 0,
        "hasher": hashlib.sha256(),
        "end": asyncio.Event(),
    }
    sender_hasher = hashlib.sha256()
    rng = random.Random(0xC0FFEE)
    target_bps = int(rate_mbps * 1_000_000)
    target_bytes = target_bps * stream_duration // 8
    chunk_size = 16384
    # Cap the SCTP send queue so a slow relay can't blow up memory.
    max_buffered_bytes = 4 * 1024 * 1024
    sctp_stats_tx = None

    def _wr(
        ok,
        *,
        bytes_sent=0,
        bytes_received=0,
        send_mbps=0.0,
        recv_mbps=0.0,
        relay_confirmed=False,
    ):
        """Build a WebRtcResult, folding in the latest SCTP retransmit counts."""
        sent = sctp_stats_tx["data_sent"] if sctp_stats_tx else 0
        retx = sctp_stats_tx["data_retx"] if sctp_stats_tx else 0
        return WebRtcResult(
            ok=ok,
            duration=time.monotonic() - test_start,
            bytes_sent=bytes_sent,
            bytes_received=bytes_received,
            send_mbps=send_mbps,
            recv_mbps=recv_mbps,
            retransmit_sent=sent,
            retransmit_count=retx,
            relay_confirmed=relay_confirmed,
        )

    # Connection state handlers
    @pc1.on("connectionstatechange")
    def on_pc1_connection_state():
        print(f"[WEBRTC] PC1 state: {pc1.connectionState}")
        if pc1.connectionState in ("connected", "failed"):
            connection_complete.set()

    @pc2.on("connectionstatechange")
    def on_pc2_connection_state():
        print(f"[WEBRTC] PC2 state: {pc2.connectionState}")

    # Data channel handler for PC2
    @pc2.on("datachannel")
    def on_datachannel(channel):
        print(f"[WEBRTC] PC2 received data channel: {channel.label}")

        @channel.on("message")
        def on_message(message):
            if isinstance(message, bytes):
                recv_stats["bytes"] += len(message)
                recv_stats["hasher"].update(message)
                received_messages.append(("pc2-bytes", len(message)))
            elif message == "__END__":
                print("[WEBRTC] PC2 received end-of-stream marker")
                recv_stats["end"].set()
                channel.send("__ACK__")
            else:
                received_messages.append(("pc2", message))

    try:
        # Step 1: Create data channel on PC1
        print("[WEBRTC] Creating data channel...")
        dc1 = pc1.createDataChannel("test-channel", ordered=True)

        @dc1.on("open")
        def on_dc1_open():
            print("[WEBRTC] PC1 data channel opened")
            dc1_open.set()

        @dc1.on("message")
        def on_dc1_message(message):
            if message == "__ACK__":
                ack_received.set()
            else:
                received_messages.append(("pc1", message))

        @dc1.on("error")
        def on_dc1_error(error):
            eprint(f"[WEBRTC] PC1 data channel error: {error}")

        # Honor insecure on the turns: relay connection (must run before PC1
        # gathers, i.e. before setLocalDescription). No-op unless tls + insecure.
        apply_insecure_turns_tls(pc1, transport, insecure, "PC1")

        # Step 2: Create and exchange offers (relay-only ICE policy)
        print("[WEBRTC] Creating offer...")
        offer = await pc1.createOffer()
        await pc1.setLocalDescription(offer)

        print("[WEBRTC] Gathering ICE candidates for PC1...")
        await wait_for_ice_gathering_complete(pc1, timeout=timeout)

        if "typ relay" not in pc1.localDescription.sdp:
            eprint("[WEBRTC] ERROR: No relay candidates in offer (PC1)")
            eprint("[WEBRTC] TURN server may not be providing relay candidates")
            return _wr(False)

        prune_local_candidates_to_relay(pc1, "PC1")

        filtered_offer = filter_sdp_for_relay_only(pc1.localDescription, "PC1")

        # Step 3: PC2 processes offer and creates answer
        print("[WEBRTC] PC2 processing offer...")
        await pc2.setRemoteDescription(filtered_offer)

        # PC2 also reaches the relay over turns:; relax its TLS context too,
        # before it gathers (setLocalDescription below). No-op unless tls + insecure.
        apply_insecure_turns_tls(pc2, transport, insecure, "PC2")

        answer = await pc2.createAnswer()
        await pc2.setLocalDescription(answer)

        print("[WEBRTC] Gathering ICE candidates for PC2...")
        await wait_for_ice_gathering_complete(pc2, timeout=timeout)

        if "typ relay" not in pc2.localDescription.sdp:
            eprint("[WEBRTC] ERROR: No relay candidates in answer (PC2)")
            eprint("[WEBRTC] TURN server may not be providing relay candidates")
            return _wr(False)

        prune_local_candidates_to_relay(pc2, "PC2")

        filtered_answer = filter_sdp_for_relay_only(pc2.localDescription, "PC2")

        # Step 4: PC1 processes answer
        print("[WEBRTC] PC1 processing answer...")
        await pc1.setRemoteDescription(filtered_answer)

        # Step 5: Wait for connection
        print("[WEBRTC] Waiting for connection through TURN relay...")
        try:
            await asyncio.wait_for(connection_complete.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            eprint(f"[WEBRTC] Connection timeout after {timeout}s")
            eprint(f"[WEBRTC] PC1: {pc1.connectionState}, PC2: {pc2.connectionState}")
            return _wr(False)

        if pc1.connectionState != "connected":
            eprint(f"[WEBRTC] Connection failed: {pc1.connectionState}")
            return _wr(False)

        print("[WEBRTC] Connection established through TURN relay!")

        # Step 6: Wait for data channel to open
        print("[WEBRTC] Waiting for data channel...")
        try:
            await asyncio.wait_for(dc1_open.wait(), timeout=10)
        except asyncio.TimeoutError:
            eprint("[WEBRTC] Data channel open timeout")
            return _wr(False)

        # Count SCTP DATA retransmissions on the sender for the bulk stream
        # (the DCEP open handshake is already done, so this measures the stream
        # only). Reveals path loss that the reliable channel hides at app level.
        sctp_stats_tx = instrument_sctp_retransmits(pc1)

        # Step 7: High-rate pseudorandom stream
        print(
            f"[WEBRTC] Sending pseudorandom stream for {stream_duration}s at ~{rate_mbps} Mbps"
        )
        start = time.monotonic()
        deadline = start + stream_duration
        bytes_per_sec = target_bps / 8.0
        bytes_sent = 0

        while bytes_sent < target_bytes and time.monotonic() < deadline:
            remaining = target_bytes - bytes_sent
            chunk_len = min(chunk_size, remaining)
            chunk = rng.randbytes(chunk_len)
            sender_hasher.update(chunk)
            dc1.send(chunk)
            bytes_sent += chunk_len

            # Backpressure: if the relay can't drain the queue fast enough,
            # wait instead of letting bufferedAmount grow without bound.
            while (
                dc1.bufferedAmount > max_buffered_bytes
                and time.monotonic() < deadline
            ):
                await asyncio.sleep(0.01)

            # Pace to the target rate: sleep exactly enough to stay on schedule.
            if bytes_per_sec > 0:
                elapsed = time.monotonic() - start
                expected_bytes = bytes_per_sec * elapsed
                if bytes_sent > expected_bytes:
                    await asyncio.sleep((bytes_sent - expected_bytes) / bytes_per_sec)

        # Capture the moment the send window closed (before the ACK wait), so
        # the reported send rate reflects the paced stream and not the drain.
        send_done = time.monotonic()

        # Signal end of stream
        dc1.send("__END__")

        # Step 8: Wait for receiver to confirm end
        ack_timeout = max(timeout * 4, stream_duration * 10, 120)
        print(f"[WEBRTC] Waiting for receiver ACK (timeout {ack_timeout}s)...")
        try:
            await asyncio.wait_for(ack_received.wait(), timeout=ack_timeout)
        except asyncio.TimeoutError:
            buffered = getattr(dc1, "bufferedAmount", None)
            send_elapsed = send_done - start
            recv_elapsed = time.monotonic() - start
            bytes_received = recv_stats["bytes"]
            send_mbps = (
                (bytes_sent * 8 / 1_000_000) / send_elapsed if send_elapsed else 0
            )
            recv_mbps = (
                (bytes_received * 8 / 1_000_000) / recv_elapsed if recv_elapsed else 0
            )
            eprint(
                f"[WEBRTC] Progress before timeout: sent {bytes_sent} bytes ({send_mbps:.2f} Mbps paced), received {bytes_received} bytes ({recv_mbps:.2f} Mbps)"
            )
            if buffered is not None:
                eprint(
                    f"[WEBRTC] Did not receive ACK from receiver (bufferedAmount={buffered} bytes)"
                )
            else:
                eprint("[WEBRTC] Did not receive ACK from receiver")
            retx_line = format_retransmit_rate(sctp_stats_tx, "PC1->PC2 (stream)")
            if retx_line:
                eprint(retx_line)
            return _wr(
                False,
                bytes_sent=bytes_sent,
                bytes_received=bytes_received,
                send_mbps=send_mbps,
                recv_mbps=recv_mbps,
            )

        # Step 9: Verify throughput and integrity
        recv_done = time.monotonic()
        await recv_stats["end"].wait()
        send_elapsed = send_done - start
        recv_elapsed = recv_done - start
        bytes_received = recv_stats["bytes"]
        send_mbps = (bytes_sent * 8 / 1_000_000) / send_elapsed if send_elapsed else 0
        recv_mbps = (
            (bytes_received * 8 / 1_000_000) / recv_elapsed if recv_elapsed else 0
        )

        expected_hash = sender_hasher.hexdigest()
        received_hash = recv_stats["hasher"].hexdigest()

        metrics = dict(
            bytes_sent=bytes_sent,
            bytes_received=bytes_received,
            send_mbps=send_mbps,
            recv_mbps=recv_mbps,
        )

        print(
            f"[WEBRTC] Sent {bytes_sent} bytes in {send_elapsed:.2f}s ({send_mbps:.2f} Mbps)"
        )
        print(
            f"[WEBRTC] Received {bytes_received} bytes in {recv_elapsed:.2f}s ({recv_mbps:.2f} Mbps)"
        )

        # Channel-quality metric: SCTP retransmission rate on the sender. Since
        # the channel is reliable, the app sees no loss; this is the only signal
        # of how lossy the underlying path/relay is. Print it before the
        # pass/fail checks so it is visible even when throughput is below target.
        retx_line = format_retransmit_rate(sctp_stats_tx, "PC1->PC2 (stream)")
        if retx_line:
            print(retx_line)

        if bytes_received != bytes_sent:
            eprint(
                f"[WEBRTC] Byte count mismatch (sent {bytes_sent}, received {bytes_received})"
            )
            return _wr(False, **metrics)

        if bytes_received < target_bytes * 0.9:
            eprint(
                f"[WEBRTC] Throughput below target (received {bytes_received} < 90% of {target_bytes})"
            )
            return _wr(False, **metrics)

        if send_mbps < rate_mbps * 0.8 or recv_mbps < rate_mbps * 0.8:
            eprint(
                f"[WEBRTC] Throughput below expectation (send {send_mbps:.2f} Mbps, recv {recv_mbps:.2f} Mbps, target {rate_mbps} Mbps)"
            )
            return _wr(False, **metrics)

        if expected_hash != received_hash:
            eprint(
                f"[WEBRTC] Hash mismatch: sent {expected_hash}, received {received_hash}"
            )
            return _wr(False, **metrics)

        print("[WEBRTC] Payload integrity verified (SHA-256 match)")

        # Inspect the selected candidate pair (private attrs, see docstring)
        pair = None
        try:
            ice_transport = pc1.sctp.transport.transport  # type: ignore[attr-defined]
            nominated = getattr(ice_transport._connection, "_nominated", {})  # type: ignore[attr-defined]
            pair = next(iter(nominated.values()), None)
        except Exception:
            pair = None

        if not pair:
            eprint("[WEBRTC] WARNING: Could not determine selected candidate pair")
            return _wr(True, **metrics, relay_confirmed=False)

        local_candidate = pair.local_candidate
        remote_candidate = pair.remote_candidate
        local_type = getattr(local_candidate, "type", None)
        remote_type = getattr(remote_candidate, "type", None)
        print(
            f"[WEBRTC] Selected candidate pair: local={local_candidate}, remote={remote_candidate}"
        )

        if local_type == "relay" and remote_type == "relay":
            print("[WEBRTC] Relay-only path confirmed")
            print("[WEBRTC] Data Channel test PASSED!")
            return _wr(True, **metrics, relay_confirmed=True)

        eprint(
            f"[WEBRTC] Unexpected candidate types (local={local_type}, remote={remote_type})"
        )
        return _wr(False, **metrics, relay_confirmed=False)

    except Exception as e:
        eprint(f"[WEBRTC] Error: {e}")
        import traceback

        traceback.print_exc()
        return _wr(False)
    finally:
        await pc1.close()
        await pc2.close()
        print("[WEBRTC] Connections closed")


def _confirm_relay_pair(pc, tag="[WEBRTC]"):
    """Return True if the selected ICE pair is relay<->relay (best-effort).

    Inspects the nominated candidate pair via private aiortc/aioice attributes
    (see module docstring). Prints the pair and verdict; never raises.
    """
    try:
        ice_transport = pc.sctp.transport.transport  # type: ignore[attr-defined]
        nominated = getattr(ice_transport._connection, "_nominated", {})  # type: ignore[attr-defined]
        pair = next(iter(nominated.values()), None)
    except Exception:
        pair = None

    if not pair:
        eprint(f"{tag} WARNING: Could not determine selected candidate pair")
        return False

    local_type = getattr(pair.local_candidate, "type", None)
    remote_type = getattr(pair.remote_candidate, "type", None)
    print(
        f"{tag} Selected candidate pair: local={pair.local_candidate}, remote={pair.remote_candidate}"
    )
    if local_type == "relay" and remote_type == "relay":
        print(f"{tag} Relay-only path confirmed")
        return True
    eprint(
        f"{tag} Unexpected candidate types (local={local_type}, remote={remote_type})"
    )
    return False


async def webrtc_capacity_probe(
    host,
    port,
    username,
    password,
    timeout=30,
    transport="udp",
    *,
    threshold_ratio=0.005,
    start_mbps=1.0,
    ramp_factor=1.5,
    step_duration=3.0,
    warmup_duration=2.0,
    max_mbps=100.0,
    max_duration=60.0,
    insecure=False,
):
    """Find a relay's throughput "knee" by ramping the send rate.

    Establishes a relay-only WebRTC data channel once, then sends in steps of
    increasing rate (``start_mbps``, then ``×ramp_factor`` each step). For every
    step it measures the *incremental* SCTP DATA retransmission ratio on the
    sender (path loss the reliable channel otherwise hides). The ramp stops at the
    first step whose ratio ``>= threshold_ratio`` — the knee — or when it reaches
    ``max_mbps`` / ``max_duration``. ``capacity_mbps`` is the highest rate
    sustained strictly below the threshold. A short ``warmup_duration`` at
    ``start_mbps`` runs first (and is discarded) so SCTP slow-start / RTO
    transients don't contaminate the first measured step.

    Unlike ``test_webrtc_datachannel`` there is no fixed target rate or duration:
    the rate is discovered. Reuses the same relay-only enforcement and SCTP
    instrumentation (see module docstring for the private-attribute caveats).
    Returns a ``WebRtcCapacityResult``.
    """
    print(
        f"\n[CAPACITY] Ramping WebRTC throughput through TURN relay {host}:{port} via "
        f"{transport.upper()} until retransmits >= {threshold_ratio * 100:.2f}%..."
    )

    test_start = time.monotonic()

    turn_url = build_turn_url(host, port, transport)
    config = RTCConfiguration(
        iceServers=[
            RTCIceServer(urls=[turn_url], username=username, credential=password)
        ]
    )
    pc1 = RTCPeerConnection(configuration=config)
    pc2 = RTCPeerConnection(configuration=config)

    connection_complete = asyncio.Event()
    dc1_open = asyncio.Event()
    recv_bytes = {"n": 0}
    rng = random.Random(0xC0FFEE)
    chunk_size = 16384
    max_buffered_bytes = 4 * 1024 * 1024

    def _result(
        ok,
        *,
        capacity_mbps=0.0,
        retransmit_ratio=0.0,
        stopped_on_threshold=False,
        relay_confirmed=False,
        peak_attempted_mbps=0.0,
    ):
        return WebRtcCapacityResult(
            ok=ok,
            duration=time.monotonic() - test_start,
            capacity_mbps=capacity_mbps,
            retransmit_ratio=retransmit_ratio,
            threshold_ratio=threshold_ratio,
            stopped_on_threshold=stopped_on_threshold,
            relay_confirmed=relay_confirmed,
            peak_attempted_mbps=peak_attempted_mbps,
        )

    @pc1.on("connectionstatechange")
    def _on_pc1_state():
        print(f"[CAPACITY] PC1 state: {pc1.connectionState}")
        if pc1.connectionState in ("connected", "failed"):
            connection_complete.set()

    @pc2.on("datachannel")
    def _on_dc(channel):
        @channel.on("message")
        def _on_msg(message):
            # Drain the receiver so SCTP rwnd stays open; we don't hash here.
            if isinstance(message, bytes):
                recv_bytes["n"] += len(message)

    try:
        dc1 = pc1.createDataChannel("capacity-channel", ordered=True)

        @dc1.on("open")
        def _on_open():
            dc1_open.set()

        # Honor insecure on the turns: relay connection before PC1 gathers.
        # No-op unless tls + insecure (see apply_insecure_turns_tls).
        apply_insecure_turns_tls(pc1, transport, insecure, "PC1", tag="[CAPACITY]")

        # Offer / answer with relay-only enforcement (same dance as the CLI test).
        offer = await pc1.createOffer()
        await pc1.setLocalDescription(offer)
        await wait_for_ice_gathering_complete(pc1, timeout=timeout)
        if "typ relay" not in pc1.localDescription.sdp:
            eprint("[CAPACITY] ERROR: No relay candidates in offer (PC1)")
            return _result(False)
        prune_local_candidates_to_relay(pc1, "PC1")
        filtered_offer = filter_sdp_for_relay_only(pc1.localDescription, "PC1")

        await pc2.setRemoteDescription(filtered_offer)
        apply_insecure_turns_tls(pc2, transport, insecure, "PC2", tag="[CAPACITY]")
        answer = await pc2.createAnswer()
        await pc2.setLocalDescription(answer)
        await wait_for_ice_gathering_complete(pc2, timeout=timeout)
        if "typ relay" not in pc2.localDescription.sdp:
            eprint("[CAPACITY] ERROR: No relay candidates in answer (PC2)")
            return _result(False)
        prune_local_candidates_to_relay(pc2, "PC2")
        filtered_answer = filter_sdp_for_relay_only(pc2.localDescription, "PC2")

        await pc1.setRemoteDescription(filtered_answer)

        try:
            await asyncio.wait_for(connection_complete.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            eprint(f"[CAPACITY] Connection timeout after {timeout}s")
            return _result(False)
        if pc1.connectionState != "connected":
            eprint(f"[CAPACITY] Connection failed: {pc1.connectionState}")
            return _result(False)
        try:
            await asyncio.wait_for(dc1_open.wait(), timeout=10)
        except asyncio.TimeoutError:
            eprint("[CAPACITY] Data channel open timeout")
            return _result(False)

        # The retransmit counter is the ramp's only stop signal — bail if we
        # can't instrument it rather than ramp blindly to the cap.
        stats = instrument_sctp_retransmits(pc1)
        if stats is None:
            eprint("[CAPACITY] ERROR: could not instrument SCTP retransmissions")
            return _result(False, relay_confirmed=_confirm_relay_pair(pc1, "[CAPACITY]"))

        # Warm-up: send at the start rate so SCTP slow-start and RTO calibration
        # settle before we measure. Retransmits during connection startup are not
        # path loss — without this the first ramp step reads a spuriously high
        # ratio and the probe underreports capacity. We discard warm-up by only
        # snapshotting the per-step counters *after* it (plus a grace so its
        # retransmits land first).
        if warmup_duration > 0:
            wu_bps = start_mbps * 1_000_000 / 8.0
            wu_start = time.monotonic()
            wu_deadline = wu_start + warmup_duration
            wu_sent = 0
            while time.monotonic() < wu_deadline:
                dc1.send(rng.randbytes(chunk_size))
                wu_sent += chunk_size
                while (
                    dc1.bufferedAmount > max_buffered_bytes
                    and time.monotonic() < wu_deadline
                ):
                    await asyncio.sleep(0.01)
                if wu_bps > 0:
                    elapsed = time.monotonic() - wu_start
                    expected = wu_bps * elapsed
                    if wu_sent > expected:
                        await asyncio.sleep((wu_sent - expected) / wu_bps)
            settle_deadline = time.monotonic() + 1.0
            while dc1.bufferedAmount > 0 and time.monotonic() < settle_deadline:
                await asyncio.sleep(0.02)
            await asyncio.sleep(0.5)
            print(
                f"[CAPACITY] warm-up complete ({warmup_duration:.1f}s at "
                f"{start_mbps:.2f} Mbps); starting ramp"
            )

        overall_deadline = time.monotonic() + max_duration
        rate = float(start_mbps)
        capacity = 0.0
        capacity_ratio = 0.0  # retransmit ratio at the last sustained step
        deciding_ratio = 0.0  # ratio at the step that crossed (over-driven)
        have_sustained = False
        peak = 0.0
        stopped_on_threshold = False

        while rate <= max_mbps and time.monotonic() < overall_deadline:
            sent0 = stats["data_sent"]
            retx0 = stats["data_retx"]
            bytes_per_sec = rate * 1_000_000 / 8.0
            step_start = time.monotonic()
            step_deadline = min(step_start + step_duration, overall_deadline)
            bytes_sent = 0

            while time.monotonic() < step_deadline:
                dc1.send(rng.randbytes(chunk_size))
                bytes_sent += chunk_size
                # Backpressure: don't let a saturated relay grow the queue.
                while (
                    dc1.bufferedAmount > max_buffered_bytes
                    and time.monotonic() < step_deadline
                ):
                    await asyncio.sleep(0.01)
                # Pace to the step's target rate (no sleep cap — see gotcha #1).
                if bytes_per_sec > 0:
                    elapsed = time.monotonic() - step_start
                    expected = bytes_per_sec * elapsed
                    if bytes_sent > expected:
                        await asyncio.sleep((bytes_sent - expected) / bytes_per_sec)

            # Settle: drain the send queue (cap 1 s) then a short grace so SCTP
            # retransmissions for this burst land before we read the counters.
            settle_deadline = time.monotonic() + 1.0
            while dc1.bufferedAmount > 0 and time.monotonic() < settle_deadline:
                await asyncio.sleep(0.02)
            await asyncio.sleep(0.5)

            step_sent = stats["data_sent"] - sent0
            step_retx = stats["data_retx"] - retx0
            ratio = (step_retx / step_sent) if step_sent > 0 else 0.0
            peak = rate
            print(
                f"[CAPACITY] step {rate:.2f} Mbps: {step_retx}/{step_sent} DATA chunks "
                f"retransmitted ({ratio * 100:.2f}%)"
            )
            deciding_ratio = ratio
            if ratio >= threshold_ratio:
                stopped_on_threshold = True
                break
            capacity = rate
            capacity_ratio = ratio
            have_sustained = True
            rate *= ramp_factor

        # Report the retransmit ratio measured at the *sustainable* rate, not the
        # deliberately over-driven step that tripped the threshold (that one is
        # an artifact of the ramp overshoot and would inflate the metric). If even
        # the start rate was already over the threshold (no sustained step), fall
        # back to that first measurement so the number isn't a misleading 0.
        reported_ratio = capacity_ratio if have_sustained else deciding_ratio

        relay_confirmed = _confirm_relay_pair(pc1, "[CAPACITY]")
        if stopped_on_threshold:
            print(
                f"[CAPACITY] knee found: sustained {capacity:.2f} Mbps at "
                f"{capacity_ratio * 100:.2f}% retransmits (< {threshold_ratio * 100:.2f}%); "
                f"crossed at {peak:.2f} Mbps ({deciding_ratio * 100:.2f}%)"
            )
        else:
            print(
                f"[CAPACITY] reached cap {peak:.2f} Mbps at {capacity_ratio * 100:.2f}% "
                f"without crossing {threshold_ratio * 100:.2f}% (capacity is at least {capacity:.2f} Mbps)"
            )
        return _result(
            True,
            capacity_mbps=capacity,
            retransmit_ratio=reported_ratio,
            stopped_on_threshold=stopped_on_threshold,
            relay_confirmed=relay_confirmed,
            peak_attempted_mbps=peak,
        )

    except Exception as e:
        eprint(f"[CAPACITY] Error: {e}")
        import traceback

        traceback.print_exc()
        return _result(False)
    finally:
        await pc1.close()
        await pc2.close()
        print("[CAPACITY] Connections closed")
