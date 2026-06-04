#!/usr/bin/env python3

# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""
CoTURN server connectivity test script.

Tests STUN binding, TURN allocation, and a WebRTC Data Channel through the
TURN server (relay-only) using the aiortc / aioice libraries.

The reusable test logic lives in ``turntest.py``; this file is the single-shot
command-line front-end. A Prometheus exporter built on the same library lives
in ``exporter.py``.

Usage:
    # Activate the bundled venv first:
    source venv/bin/activate

    # Basic test (STUN + TURN allocation + WebRTC over UDP)
    ./app.py -H <host> -P <port> -u <username> -p <password>

    # Force TCP or TLS transport for every sub-test
    ./app.py -H <host> -P <port> -u <username> -p <password> --transport tcp
    ./app.py -H <host> -P <port> -u <username> -p <password> --transport tls

    # STUN only
    ./app.py -H <host> -P <port> -u <username> -p <password> --stun-only

    # TURN allocation only
    ./app.py -H <host> -P <port> -u <username> -p <password> --turn-only

    # WebRTC Data Channel only, 25 Mbps for 30s
    ./app.py -H <host> -P <port> -u <username> -p <password> \\
        --webrtc-only --rate-mbps 25 --duration 30

CLI Parameters:
    -H, --host       TURN server address (hostname or IP) [required]
    -P, --port       TURN server port [default: 3478 for udp/tcp, 5349 for tls]
    -u, --username   Username for static authentication (or use --auth-secret)
    -p, --password   Password for static authentication (or use --auth-secret)
    --auth-secret    static-auth-secret for TURN REST credentials (alt to -u/-p)
    --auth-user      Optional user id embedded in REST username (timestamp:userid)
    --auth-ttl       Lifetime of derived REST credentials in seconds [default: 3600]
    -t, --timeout    Timeout in seconds [default: 30]
    --stun-only      Only test STUN binding
    --turn-only      Only test TURN allocation
    --webrtc-only    Only test WebRTC Data Channel (default is all tests)
    --transport      TURN transport for every sub-test (udp|tcp|tls) [default: udp]
    --duration       Duration in seconds for the WebRTC data stream [default: 10]
    --rate-mbps      Target Mbps for the WebRTC data stream [default: 1]
    --insecure       Skip TLS certificate verification (TLS STUN/TURN only)

Requirements:
    pip install -r requirements.txt
    Or use the bundled venv: source venv/bin/activate

Note on internals:
    The relay-only enforcement and the candidate-pair inspection rely on
    private attributes of aiortc/aioice (the WebRTC API has no
    iceTransportPolicy="relay" in aiortc). They are validated against the
    pinned versions in requirements.txt and may break on a library upgrade.
"""

import argparse
import asyncio
import sys

from turntest import (
    AIORTC_AVAILABLE,
    eprint,
    make_rest_credentials,
    test_stun_binding,
    test_turn_allocation,
    test_webrtc_datachannel,
)


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
  %(prog)s -H turn.example.com -P 3478 --auth-secret <static-auth-secret>
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
        "-u", "--username", help="Username for static authentication (or use --auth-secret)"
    )
    parser.add_argument(
        "-p", "--password", help="Password for static authentication (or use --auth-secret)"
    )
    parser.add_argument(
        "--auth-secret",
        help="static-auth-secret for TURN REST (time-limited) credentials; "
        "alternative to -u/-p and mutually exclusive with them",
    )
    parser.add_argument(
        "--auth-user",
        default=None,
        help="Optional user id embedded in the REST username (timestamp:userid)",
    )
    parser.add_argument(
        "--auth-ttl",
        type=int,
        default=3600,
        help="Lifetime of derived REST credentials in seconds (default: 3600)",
    )
    parser.add_argument(
        "-t", "--timeout", type=int, default=30, help="Timeout in seconds (default: 30)"
    )

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--stun-only", action="store_true", help="Only test STUN binding"
    )
    mode.add_argument(
        "--turn-only", action="store_true", help="Only test TURN allocation"
    )
    mode.add_argument(
        "--webrtc-only",
        "--webrtc-test",
        action="store_true",
        help="Only run WebRTC Data Channel test (default runs all tests)",
    )

    parser.add_argument(
        "--transport",
        choices=["udp", "tcp", "tls"],
        default="udp",
        help="TURN transport for every sub-test (default: udp; tls uses turns:)",
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
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Skip TLS certificate verification (applies to TLS STUN/TURN tests only)",
    )

    args = parser.parse_args()

    # Default ports: 3478 for UDP/TCP, 5349 for TLS when not specified
    if args.port is None:
        args.port = 5349 if args.transport == "tls" else 3478

    # Validate numeric inputs (argparse exits with code 2 on parser.error)
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if args.timeout <= 0:
        parser.error("--timeout must be a positive number of seconds")
    if args.duration <= 0:
        parser.error("--duration must be a positive number of seconds")
    if args.rate_mbps <= 0:
        parser.error("--rate-mbps must be greater than 0")

    # Resolve authentication: either a static user/password, or a TURN REST
    # secret from which time-limited credentials are derived. The two are
    # mutually exclusive; exactly one must be provided.
    if args.auth_secret:
        if args.username or args.password:
            parser.error(
                "--auth-secret is mutually exclusive with -u/--username and -p/--password"
            )
        if args.auth_ttl <= 0:
            parser.error("--auth-ttl must be a positive number of seconds")
        args.username, args.password = make_rest_credentials(
            args.auth_secret, args.auth_ttl, args.auth_user
        )
        auth_mode = f"TURN REST secret (ttl={args.auth_ttl}s)"
    else:
        if not (args.username and args.password):
            parser.error(
                "provide either -u/--username and -p/--password, or --auth-secret"
            )
        auth_mode = "static user"

    # Check dependencies
    if not AIORTC_AVAILABLE:
        eprint("ERROR: aiortc not installed")
        eprint("Install with: pip install -r requirements.txt")
        eprint("Or activate venv: source venv/bin/activate")
        sys.exit(1)

    print("CoTURN Server Test")
    print("=" * 50)
    print(f"Server: {args.host}:{args.port}")
    print(f"Auth: {auth_mode}")
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
            results["stun"] = (
                await test_stun_binding(
                    args.host, args.port, args.timeout, args.transport, args.insecure
                )
            ).ok
        if run_turn:
            results["turn"] = (
                await test_turn_allocation(
                    args.host,
                    args.port,
                    args.username,
                    args.password,
                    args.timeout,
                    args.transport,
                    args.insecure,
                )
            ).ok
        if run_webrtc:
            results["webrtc"] = (
                await test_webrtc_datachannel(
                    args.host,
                    args.port,
                    args.username,
                    args.password,
                    args.timeout,
                    args.transport,
                    args.duration,
                    args.rate_mbps,
                    args.insecure,
                )
            ).ok

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
