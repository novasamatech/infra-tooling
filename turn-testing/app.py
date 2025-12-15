#!/usr/bin/env python3

# Copyright © 2025 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""
CoTURN server connectivity test script.

Tests STUN binding, TURN allocation, and WebRTC Data Channel
through the TURN server using aiortc library.

Usage:
    # Activate venv first:
    source scripts/.venv/bin/activate

    # Basic test (STUN + TURN allocation)
    ./coturn-test.py -H <host> -P <port> -u <username> -p <password>

    # Full WebRTC Data Channel test through TURN relay
    ./coturn-test.py -H <host> -P <port> -u <username> -p <password>
    # Force TCP or TLS transport for TURN
    ./coturn-test.py -H <host> -P <port> -u <username> -p <password> --transport tcp

    # STUN only
    ./coturn-test.py -H <host> -P <port> -u <username> -p <password> --stun-only

    # TURN allocation only
    ./coturn-test.py -H <host> -P <port> -u <username> -p <password> --turn-only

CLI Parameters:
    -H, --host       TURN server address (hostname or IP) [required]
    -P, --port       TURN server port [default: 3478 for udp/tcp, 5349 for tls]
    -u, --username   Username for authentication [required]
    -p, --password   Password for authentication [required]
    -t, --timeout    Timeout in seconds [default: 30]
    --stun-only      Only test STUN binding
    --turn-only      Only test TURN allocation
    --webrtc-only    Only test WebRTC Data Channel (default is all tests)
    --transport     TURN transport for WebRTC test (udp|tcp|tls) [default: udp]
    --duration      Duration in seconds for WebRTC data stream [default: 10]
    --rate-mbps     Target Mbps for WebRTC data stream [default: 1]
    --transport     TURN transport for WebRTC test (udp|tcp|tls) [default: udp]

Requirements:
    pip install aiortc
    Or use bundled venv: source scripts/.venv/bin/activate
"""

import argparse
import asyncio
import hashlib
import random
import sys
import time

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


def build_turn_url(host, port, transport):
    """Compose TURN/TURNS URL with explicit transport parameter."""
    scheme = "turns" if transport == "tls" else "turn"
    transport_param = "tcp" if transport in ("tcp", "tls") else "udp"
    return f"{scheme}:{host}:{port}?transport={transport_param}"


async def wait_for_ice_gathering_complete(pc, timeout=10):
    """Wait until ICE gathering completes for the given peer connection."""
    if pc.iceGatheringState == "complete":
        return

    loop = asyncio.get_event_loop()
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
    """Remove non-relay candidates from the local ICE gatherer."""
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
        print(f"[WEBRTC] WARNING: Could not prune local candidates for {label}: {e}")
        return None


async def test_stun_binding(host, port, timeout=30):
    """Test STUN binding request."""
    print(f"\n[STUN] Testing binding request to {host}:{port}...")

    try:
        # Create UDP socket
        loop = asyncio.get_event_loop()
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: StunProtocol(), remote_addr=(host, port)
        )

        try:
            # Create and send STUN binding request
            request = stun.Message(
                message_method=stun.Method.BINDING,
                message_class=stun.Class.REQUEST,
            )
            request.transaction_id = stun.random_transaction_id()

            protocol.send_stun(request)

            # Wait for response
            response = await asyncio.wait_for(protocol.wait_response(), timeout=timeout)

            if response and response.message_class == stun.Class.RESPONSE:
                print("[STUN] Binding successful!")

                # Extract mapped address
                mapped_addr = response.attributes.get("XOR-MAPPED-ADDRESS")
                if mapped_addr:
                    print(
                        f"[STUN] Your public address: {mapped_addr[0]}:{mapped_addr[1]}"
                    )

                software = response.attributes.get("SOFTWARE")
                if software:
                    print(f"[STUN] Server software: {software}")

                return True
            else:
                print(f"[STUN] Unexpected response: {response}")
                return False

        finally:
            transport.close()

    except asyncio.TimeoutError:
        print(f"[STUN] Timeout after {timeout}s")
        return False
    except Exception as e:
        print(f"[STUN] Error: {e}")
        import traceback

        traceback.print_exc()
        return False


class StunProtocol(asyncio.DatagramProtocol):
    """Simple STUN protocol handler."""

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

    def send_stun(self, message):
        data = bytes(message)
        self.transport.sendto(data)

    async def wait_response(self):
        self.response_future = asyncio.get_event_loop().create_future()
        return await self.response_future


class TurnTestProtocol(asyncio.DatagramProtocol):
    """Simple protocol for TURN allocation test."""

    def __init__(self):
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        pass


async def test_turn_allocation(host, port, username, password, timeout=30):
    """Test TURN allocation."""
    print(f"\n[TURN] Testing allocation to {host}:{port} with user '{username}'...")

    try:
        # Create TURN endpoint using aioice
        transport, protocol = await asyncio.wait_for(
            turn.create_turn_endpoint(
                TurnTestProtocol,
                server_addr=(host, port),
                username=username,
                password=password,
            ),
            timeout=timeout,
        )

        try:
            # Get allocation info from TurnTransport (access private attrs)
            relayed_address = getattr(
                transport, "_TurnTransport__relayed_address", None
            )
            # Get mapped address from inner protocol
            inner = getattr(transport, "_TurnTransport__inner_protocol", None)
            mapped_address = getattr(inner, "mapped_address", None) if inner else None

            if relayed_address:
                print("[TURN] Allocation successful!")
                print(
                    f"[TURN] Relayed address: {relayed_address[0]}:{relayed_address[1]}"
                )
            else:
                print("[TURN] No relayed address received")
                return False

            if mapped_address:
                print(f"[TURN] Mapped address: {mapped_address[0]}:{mapped_address[1]}")

            return True

        finally:
            transport.close()

    except asyncio.TimeoutError:
        print(f"[TURN] Timeout after {timeout}s")
        return False
    except Exception as e:
        print(f"[TURN] Error: {e}")
        import traceback

        traceback.print_exc()
        return False


async def test_webrtc_datachannel(
    host,
    port,
    username,
    password,
    timeout=30,
    transport="udp",
    stream_duration=10,
    rate_mbps=25,
):
    """
    Full WebRTC Data Channel test through TURN relay.

    Creates two peer connections in one process, forces relay-only candidates
    via ICE policy, establishes a Data Channel, and verifies bidirectional
    communication.
    """
    print(
        f"\n[WEBRTC] Testing Data Channel through TURN relay {host}:{port} via {transport.upper()}..."
    )
    print(
        f"[WEBRTC] Target stream: {rate_mbps} Mbps for {stream_duration}s (data channel)"
    )

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
            print(f"[WEBRTC] PC1 data channel error: {error}")

        # Step 2: Create and exchange offers (relay-only ICE policy)
        print("[WEBRTC] Creating offer...")
        offer = await pc1.createOffer()
        await pc1.setLocalDescription(offer)

        print("[WEBRTC] Gathering ICE candidates for PC1...")
        await wait_for_ice_gathering_complete(pc1, timeout=timeout)

        if "typ relay" not in pc1.localDescription.sdp:
            print("[WEBRTC] ERROR: No relay candidates in offer (PC1)")
            print("[WEBRTC] TURN server may not be providing relay candidates")
            return False

        prune_local_candidates_to_relay(pc1, "PC1")

        filtered_offer = filter_sdp_for_relay_only(pc1.localDescription, "PC1")

        # Step 3: PC2 processes offer and creates answer
        print("[WEBRTC] PC2 processing offer...")
        await pc2.setRemoteDescription(filtered_offer)

        answer = await pc2.createAnswer()
        await pc2.setLocalDescription(answer)

        print("[WEBRTC] Gathering ICE candidates for PC2...")
        await wait_for_ice_gathering_complete(pc2, timeout=timeout)

        if "typ relay" not in pc2.localDescription.sdp:
            print("[WEBRTC] ERROR: No relay candidates in answer (PC2)")
            print("[WEBRTC] TURN server may not be providing relay candidates")
            return False

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
            print(f"[WEBRTC] Connection timeout after {timeout}s")
            print(f"[WEBRTC] PC1: {pc1.connectionState}, PC2: {pc2.connectionState}")
            return False

        if pc1.connectionState != "connected":
            print(f"[WEBRTC] Connection failed: {pc1.connectionState}")
            return False

        print("[WEBRTC] Connection established through TURN relay!")

        # Step 6: Wait for data channel to open
        print("[WEBRTC] Waiting for data channel...")
        try:
            await asyncio.wait_for(dc1_open.wait(), timeout=10)
        except asyncio.TimeoutError:
            print("[WEBRTC] Data channel open timeout")
            return False

        # Step 7: High-rate pseudorandom stream
        print(
            f"[WEBRTC] Sending pseudorandom stream for {stream_duration}s at ~{rate_mbps} Mbps"
        )
        start = time.monotonic()
        deadline = start + stream_duration
        target_bps_float = float(target_bps)
        bytes_per_sec = target_bps_float / 8.0
        bytes_sent = 0

        while bytes_sent < target_bytes and time.monotonic() < deadline:
            remaining = target_bytes - bytes_sent
            if remaining <= 0:
                break
            chunk_len = min(chunk_size, remaining)
            chunk = rng.randbytes(chunk_len)
            sender_hasher.update(chunk)
            dc1.send(chunk)
            bytes_sent += chunk_len

            # pace to target rate
            elapsed = time.monotonic() - start
            if elapsed > 0:
                expected_bytes = min(target_bytes, bytes_per_sec * elapsed)
                if bytes_sent > expected_bytes:
                    sleep_time = min(
                        0.05, (bytes_sent - expected_bytes) / bytes_per_sec
                    )
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)

        # Signal end of stream
        dc1.send("__END__")

        # Step 8: Wait for receiver to confirm end
        ack_timeout = max(timeout * 4, stream_duration * 10, 120)
        print(f"[WEBRTC] Waiting for receiver ACK (timeout {ack_timeout}s)...")
        try:
            await asyncio.wait_for(ack_received.wait(), timeout=ack_timeout)
        except asyncio.TimeoutError:
            buffered = getattr(dc1, "bufferedAmount", None)
            send_elapsed = time.monotonic() - start
            bytes_received = recv_stats["bytes"]
            send_mbps = (
                (bytes_sent * 8 / 1_000_000) / send_elapsed if send_elapsed else 0
            )
            recv_mbps = (
                (bytes_received * 8 / 1_000_000) / send_elapsed if send_elapsed else 0
            )
            print(
                f"[WEBRTC] Progress before timeout: sent {bytes_sent} bytes ({send_mbps:.2f} Mbps paced), received {bytes_received} bytes ({recv_mbps:.2f} Mbps)"
            )
            if buffered is not None:
                print(
                    f"[WEBRTC] Did not receive ACK from receiver (bufferedAmount={buffered} bytes)"
                )
            else:
                print("[WEBRTC] Did not receive ACK from receiver")
            return False

        # Step 9: Verify throughput and integrity
        send_elapsed = time.monotonic() - start
        await recv_stats["end"].wait()
        bytes_received = recv_stats["bytes"]
        send_mbps = (bytes_sent * 8 / 1_000_000) / send_elapsed if send_elapsed else 0
        recv_mbps = (
            (bytes_received * 8 / 1_000_000) / send_elapsed if send_elapsed else 0
        )

        expected_hash = sender_hasher.hexdigest()
        received_hash = recv_stats["hasher"].hexdigest()

        print(
            f"[WEBRTC] Sent {bytes_sent} bytes in {send_elapsed:.2f}s ({send_mbps:.2f} Mbps)"
        )
        print(
            f"[WEBRTC] Received {bytes_received} bytes in {send_elapsed:.2f}s ({recv_mbps:.2f} Mbps)"
        )

        if bytes_received != bytes_sent:
            print(
                f"[WEBRTC] Byte count mismatch (sent {bytes_sent}, received {bytes_received})"
            )
            return False

        if bytes_received < target_bytes * 0.9:
            print(
                f"[WEBRTC] Throughput below target (received {bytes_received} < 90% of {target_bytes})"
            )
            return False

        if send_mbps < rate_mbps * 0.8 or recv_mbps < rate_mbps * 0.8:
            print(
                f"[WEBRTC] Throughput below expectation (send {send_mbps:.2f} Mbps, recv {recv_mbps:.2f} Mbps, target {rate_mbps} Mbps)"
            )
            return False

        if expected_hash != received_hash:
            print(
                f"[WEBRTC] Hash mismatch: sent {expected_hash}, received {received_hash}"
            )
            return False

        print("[WEBRTC] Payload integrity verified (SHA-256 match)")

        pair = None
        try:
            ice_transport = pc1.sctp.transport.transport  # type: ignore[attr-defined]
            nominated = getattr(ice_transport._connection, "_nominated", {})  # type: ignore[attr-defined]
            pair = next(iter(nominated.values()), None)
        except Exception:
            pair = None

        if not pair:
            print("[WEBRTC] WARNING: Could not determine selected candidate pair")
            return True

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
            return True

        print(
            f"[WEBRTC] Unexpected candidate types (local={local_type}, remote={remote_type})"
        )
        return False

    except Exception as e:
        print(f"[WEBRTC] Error: {e}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        await pc1.close()
        await pc2.close()
        print("[WEBRTC] Connections closed")


def main():
    parser = argparse.ArgumentParser(
        description="Test CoTURN server connectivity with authentication",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -H turn.example.com -P 3478 -u myuser -p mypassword
  %(prog)s -H turn.example.com -P 3478 -u myuser -p mypassword --transport tcp
  %(prog)s -H 192.168.1.100 -P 3478 -u user -p pass --stun-only
  %(prog)s -H turn.example.com -P 5349 -u myuser -p mypassword --transport tls --webrtc-only
        """,
    )

    parser.add_argument(
        "-H", "--host", required=True, help="TURN server address (hostname or IP)"
    )
    parser.add_argument(
        "-P",
        "--port",
        type=int,
        default=None,
        help="TURN server port (default: 3478 for udp/tcp, 5349 for tls)",
    )
    parser.add_argument(
        "-u", "--username", required=True, help="Username for authentication"
    )
    parser.add_argument(
        "-p", "--password", required=True, help="Password for authentication"
    )
    parser.add_argument(
        "-t", "--timeout", type=int, default=30, help="Timeout in seconds (default: 30)"
    )
    parser.add_argument(
        "--stun-only", action="store_true", help="Only test STUN binding"
    )
    parser.add_argument(
        "--turn-only", action="store_true", help="Only test TURN allocation"
    )
    parser.add_argument(
        "--webrtc-only",
        "--webrtc-test",
        action="store_true",
        help="Only run WebRTC Data Channel test (default runs all tests)",
    )
    parser.add_argument(
        "--transport",
        choices=["udp", "tcp", "tls"],
        default="udp",
        help="TURN transport for WebRTC test (default: udp; tls uses turns:)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=10,
        help="Duration in seconds for WebRTC data stream (default: 10)",
    )
    parser.add_argument(
        "--rate-mbps",
        type=float,
        default=1.0,
        help="Target Mbps for WebRTC data stream (default: 1)",
    )

    args = parser.parse_args()

    # Default ports: 3478 for UDP/TCP, 5349 for TLS when not specified
    if args.port is None:
        args.port = 5349 if args.transport == "tls" else 3478

    # Check dependencies
    if not AIORTC_AVAILABLE:
        print("ERROR: aiortc not installed")
        print("Install with: pip install aiortc")
        print("Or activate venv: source scripts/.venv/bin/activate")
        sys.exit(1)

    print("CoTURN Server Test")
    print("=" * 50)
    print(f"Server: {args.host}:{args.port}")
    print(f"Username: {args.username}")
    print(f"Transport: {args.transport.upper()}")
    print(f"Timeout: {args.timeout}s")
    print(f"Data stream: {args.rate_mbps} Mbps for {args.duration}s (WebRTC)")

    # Determine which tests to run
    run_stun = True
    run_turn = True
    run_webrtc = True

    if args.stun_only:
        run_turn = False
        run_webrtc = False
    elif args.turn_only:
        run_stun = False
        run_webrtc = False
    elif args.webrtc_only:
        run_stun = False
        run_turn = False

    # Run tests
    results = {}

    async def run_tests():
        if run_stun:
            results["stun"] = await test_stun_binding(
                args.host, args.port, args.timeout
            )
        if run_turn:
            results["turn"] = await test_turn_allocation(
                args.host, args.port, args.username, args.password, args.timeout
            )
        if run_webrtc:
            results["webrtc"] = await test_webrtc_datachannel(
                args.host,
                args.port,
                args.username,
                args.password,
                args.timeout,
                args.transport,
                args.duration,
                args.rate_mbps,
            )

    asyncio.run(run_tests())

    # Summary
    print(f"\n{'=' * 50}")
    print("Summary:")

    all_ok = True
    for test_name, result in results.items():
        status = "OK" if result else "FAILED"
        label = {
            "stun": "STUN Binding",
            "turn": "TURN Allocation",
            "webrtc": "WebRTC Data Channel",
        }.get(test_name, test_name)
        print(f"  {label}: {status}")
        if not result:
            all_ok = False

    if all_ok:
        print("\nAll tests passed!")
        sys.exit(0)
    else:
        print("\nSome tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
