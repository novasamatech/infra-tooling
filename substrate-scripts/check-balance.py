#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright © 2025 kogeler
# SPDX-License-Identifier: Apache-2.0

"""
Substrate balance poller: query an account balance multiple times via WSS.

Default behavior:
  - Opens ONE persistent connection to the given WSS endpoint (auto_reconnect on)
  - Queries in a loop with a delay between attempts
  - Prints Free, Reserved, and Total (where Total = Free + Reserved)
  - Counts how many responses had TOTAL == 0 (including "no account data")

CLI options:
  --url / --ws / --wss  (REQUIRED)
      WSS RPC endpoint. Example: wss://rpc.polkadot.io

  -a / --address        (REQUIRED)
      SS58 address to query.

  -n / --count          (default: 10)
      Number of attempts (iterations).

  -d / --delay          (default: 1.0)
      Delay between attempts in seconds.

  --asset-id            (optional)
      If set, read balance from the Assets pallet for this asset ID instead of
      the native token (System.Account). Reserved is treated as 0 for Assets.

  -v / --verbose        (flag)
      Print extra info (chain, node name/version) on connect.

  --unique-connection   (flag)
      Switch to "one-connection-per-iteration" mode (reconnect each attempt).
      This is the previous behavior. By default the script uses ONE connection.

Notes:
  - Zero check uses TOTAL balance (Free + Reserved). "No account data" counts as zero.
  - Output is line-buffered; suitable for piping/logging.

Examples:
  python check-balance.py --url wss://rpc.polkadot.io -a 12ab...xyz
  python check-balance.py --url wss://rpc.polkadot.io -a 12ab...xyz -n 25 -d 0.5
  python check-balance.py --url wss://statemint-rpc.polkadot.io -a 12ab...xyz --asset-id 1984
  python check-balance.py --url wss://rpc.polkadot.io -a 12ab...xyz --unique-connection -v
"""

import argparse
import sys
import time
from datetime import datetime, timezone

from substrateinterface import SubstrateInterface

# Exception import that works across library versions
try:
    from substrateinterface.exceptions import SubstrateRequestException
except Exception:
    class SubstrateRequestException(Exception):
        pass


def human_amount(raw: int, decimals: int) -> float:
    """Convert integer planck-like value into human units using decimals."""
    try:
        return raw / (10 ** decimals) if decimals else float(raw)
    except Exception:
        return float(raw)


def read_native_balance(sub: SubstrateInterface, address: str):
    """Query native balance from System.Account."""
    acc = sub.query("System", "Account", [address]).value
    if not acc:
        return None
    data = acc["data"] if isinstance(acc, dict) else {}
    raw_free = int(data.get("free", 0))
    raw_reserved = int(data.get("reserved", 0))

    # Try to get token symbol/decimals (may be list or scalar)
    symbol = getattr(sub, "token_symbol", None)
    decimals = getattr(sub, "token_decimals", None)
    if symbol is None or decimals is None:
        props = getattr(sub, "properties", {}) or {}
        symbol = props.get("tokenSymbol")
        decimals = props.get("tokenDecimals")

    if isinstance(symbol, list):
        symbol = symbol[0] if symbol else ""
    if isinstance(decimals, list):
        decimals = int(decimals[0]) if decimals else 0
    decimals = int(decimals) if decimals is not None else 0

    return {"symbol": symbol or "", "decimals": decimals, "free": raw_free, "reserved": raw_reserved}


def read_assets_balance(sub: SubstrateInterface, address: str, asset_id: int):
    """Query balance from Assets pallet for a given asset_id."""
    acc = sub.query("Assets", "Account", [asset_id, address]).value or {}
    raw = int(acc.get("balance", 0))
    meta = sub.query("Assets", "Metadata", [asset_id]).value or {}
    symbol = meta.get("symbol") or f"Asset#{asset_id}"
    decimals = int(meta.get("decimals", 0))
    # Assets pallet doesn't expose reserved here; treat as 0
    return {"symbol": symbol, "decimals": decimals, "free": raw, "reserved": 0}


def fetch_chain_info(sub: SubstrateInterface):
    """Return (chain, node_name, node_ver) with safe fallbacks."""
    try:
        chain = sub.rpc_request("system_chain", [])["result"]
    except Exception:
        chain = getattr(sub, "chain", "UnknownChain")
    try:
        node_name = sub.rpc_request("system_name", [])["result"]
    except Exception:
        node_name = "UnknownNode"
    try:
        node_ver = sub.rpc_request("system_version", [])["result"]
    except Exception:
        node_ver = "UnknownVersion"
    return chain, node_name, node_ver


def print_flush(msg: str):
    print(msg, flush=True)


def connect(url: str, auto_reconnect: bool, verbose: bool):
    """Create a SubstrateInterface and print connection info if requested."""
    sub = SubstrateInterface(
        url=url,
        ss58_format=None,           # infer from chain properties
        type_registry_preset=None,  # fetch metadata remotely
        use_remote_preset=True,
        auto_reconnect=auto_reconnect,
    )
    chain, node_name, node_ver = fetch_chain_info(sub)
    if verbose:
        print_flush(f"  Connected: chain={chain}, node={node_name} {node_ver}")
    return sub, chain


def print_balances_line(chain: str, url: str, address: str, info: dict):
    """Pretty-print Free/Reserved/Total balances."""
    raw_free = int(info.get("free", 0))
    raw_reserved = int(info.get("reserved", 0))
    raw_total = raw_free + raw_reserved

    human_free = human_amount(raw_free, info["decimals"])
    human_reserved = human_amount(raw_reserved, info["decimals"])
    human_total = human_amount(raw_total, info["decimals"])

    now = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    print_flush(f"[{now}] {chain} @ {url}")
    print_flush(f"  Address:  {address}")
    print_flush(f"  Token:    {info['symbol']} (decimals={info['decimals']})")
    print_flush(f"  Free:     {human_free} {info['symbol']}  [raw={raw_free}]")
    print_flush(f"  Reserved: {human_reserved} {info['symbol']}  [raw={raw_reserved}]")
    print_flush(f"  Total:    {human_total} {info['symbol']}  [raw={raw_total}]")
    print_flush("-" * 60)
    return raw_total


def main():
    parser = argparse.ArgumentParser(description="Query a Substrate account balance multiple times via WSS.")
    parser.add_argument("--url", "--ws", "--wss", dest="url", required=True,
                        help="WSS RPC endpoint, e.g. wss://rpc.polkadot.io")
    parser.add_argument("-a", "--address", required=True, help="Account SS58 address to check")
    parser.add_argument("-n", "--count", type=int, default=10, help="How many times to query (default: 10)")
    parser.add_argument("-d", "--delay", type=float, default=1.0, help="Delay between queries in seconds (default: 1)")
    parser.add_argument("--asset-id", type=int, default=None,
                        help="If set, query Assets pallet balance for this asset ID instead of native token")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logs (connection, node info)")
    parser.add_argument("--unique-connection", action="store_true",
                        help="Reconnect on every iteration (instead of keeping one persistent connection)")
    args = parser.parse_args()

    total_attempts = 0
    zero_total_count = 0

    if not args.unique_connection:
        # ONE persistent connection for the whole run
        print_flush(f"Connecting once to {args.url} …")
        sub = None
        chain = "UnknownChain"
        try:
            sub, chain = connect(args.url, auto_reconnect=True, verbose=args.verbose)
            for i in range(args.count):
                total_attempts += 1
                try:
                    if args.asset_id is not None:
                        info = read_assets_balance(sub, args.address, args.asset_id)
                    else:
                        info = read_native_balance(sub, args.address)

                    if not info:
                        zero_total_count += 1  # treat missing account as total=0
                        now = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
                        print_flush(f"[{now}] {chain}: no account data for {args.address} → treat as TOTAL=0.")
                        print_flush("-" * 60)
                    else:
                        raw_total = print_balances_line(chain, args.url, args.address, info)
                        if raw_total == 0:
                            zero_total_count += 1

                except SubstrateRequestException as e:
                    print_flush(f"Request error: {e}")
                    print_flush("-" * 60)
                except KeyboardInterrupt:
                    print_flush("Interrupted by user.")
                    break
                except Exception as e:
                    print_flush(f"Error: {e}")
                    print_flush("-" * 60)

                if i < args.count - 1:
                    try:
                        time.sleep(args.delay)
                    except KeyboardInterrupt:
                        print_flush("Interrupted by user.")
                        break
        finally:
            try:
                if sub:
                    sub.close()
            except Exception:
                pass

    else:
        # Reconnect on every iteration (previous behavior)
        for i in range(args.count):
            total_attempts += 1
            sub = None
            try:
                print_flush(f"[{i+1}/{args.count}] Connecting to {args.url} …")
                sub, chain = connect(args.url, auto_reconnect=False, verbose=args.verbose)

                if args.asset_id is not None:
                    info = read_assets_balance(sub, args.address, args.asset_id)
                else:
                    info = read_native_balance(sub, args.address)

                if not info:
                    zero_total_count += 1  # treat missing account as total=0
                    now = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
                    print_flush(f"[{now}] {chain}: no account data for {args.address} → treat as TOTAL=0.")
                    print_flush("-" * 60)
                else:
                    raw_total = print_balances_line(chain, args.url, args.address, info)
                    if raw_total == 0:
                        zero_total_count += 1

            except SubstrateRequestException as e:
                print_flush(f"Request error: {e}")
                print_flush("-" * 60)
            except KeyboardInterrupt:
                print_flush("Interrupted by user.")
                break
            except Exception as e:
                print_flush(f"Error: {e}")
                print_flush("-" * 60)
            finally:
                try:
                    if sub:
                        sub.close()
                except Exception:
                    pass

            if i < args.count - 1:
                try:
                    time.sleep(args.delay)
                except KeyboardInterrupt:
                    print_flush("Interrupted by user.")
                    break

    # Summary
    if total_attempts > 0:
        pct = (zero_total_count / total_attempts) * 100.0
        print_flush("== Summary ==")
        print_flush(f"Zero-TOTAL responses: {zero_total_count} / {total_attempts} ({pct:.2f}%)")
    else:
        print_flush("No attempts executed.")


if __name__ == "__main__":
    # Ensure line-buffered stdout if possible
    if hasattr(sys, "stdout"):
        try:
            sys.stdout.reconfigure(line_buffering=True)
        except Exception:
            pass
    main()
