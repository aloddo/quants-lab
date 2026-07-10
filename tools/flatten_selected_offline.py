#!/usr/bin/env python3
"""Selected-position flatten for live HL parent account.

Safety:
  - Requires both pause markers so the live bot cannot race or re-enter.
  - Dry-run by default; --execute is required to place orders.
  - Only closes coins explicitly passed on the command line.
"""
from __future__ import annotations

import os
import sys
import time

import eth_account
import requests
from dotenv import load_dotenv
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

HL_API = "https://api.hyperliquid.xyz"
BUILDER_DEXES = ["xyz", "flx"]
PARENT = os.environ.get("HL_QUERY_ADDRESS", "0x11ca20aeb7cd014cf8406560ae405b12601994b4")
PAUSE_MARKERS = ("/tmp/v12_pause", "/Users/hermes/quants-lab/.HALT_COPY")


def retry(label: str, fn, attempts: int = 5):
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt == attempts:
                break
            wait = min(8.0, 1.5 * attempt)
            print(f"{label}: attempt {attempt}/{attempts} failed ({exc}); retrying in {wait:.1f}s")
            time.sleep(wait)
    raise last_exc


def open_positions() -> list[tuple[str, str, float]]:
    out = []
    for dex_name in [""] + BUILDER_DEXES:
        payload = {"type": "clearinghouseState", "user": PARENT}
        if dex_name:
            payload["dex"] = dex_name
        data = retry(
            f"open_positions[{dex_name or 'main'}]",
            lambda: requests.post(f"{HL_API}/info", json=payload, timeout=8).json(),
        )
        if not isinstance(data, dict) or "assetPositions" not in data:
            raise ValueError(f"clearinghouseState missing assetPositions (dex={dex_name or 'main'})")
        for ap in data.get("assetPositions", []):
            pos = ap.get("position", {})
            coin = pos.get("coin")
            szi = float(pos.get("szi", 0) or 0)
            if coin and abs(szi) >= 1e-12:
                out.append((dex_name or "main", coin, szi))
    return out


def main() -> int:
    execute = "--execute" in sys.argv
    coins = [arg for arg in sys.argv[1:] if not arg.startswith("--")]
    if not coins:
        print("Usage: tools/flatten_selected_offline.py [--execute] COIN [COIN ...]")
        return 2

    missing = [marker for marker in PAUSE_MARKERS if not os.path.exists(marker)]
    if missing:
        print(f"ABORT: pause marker(s) missing: {', '.join(missing)}")
        return 2

    load_dotenv("/Users/hermes/quants-lab/.env")
    acct = eth_account.Account.from_key(os.environ["HL_PRIVATE_KEY"])
    retry("Info metadata warmup", lambda: Info(HL_API, skip_ws=True, perp_dexs=[""] + BUILDER_DEXES))
    ex = retry(
        "Exchange init",
        lambda: Exchange(acct, HL_API, account_address=PARENT, perp_dexs=[""] + BUILDER_DEXES),
    )

    wanted = set(coins)
    current = [pos for pos in open_positions() if pos[1] in wanted]
    print(f"=== SELECTED OPEN POSITIONS: {len(current)} / requested {sorted(wanted)} ===")
    for dex, coin, szi in current:
        side = "BUY-to-close" if szi < 0 else "SELL-to-close"
        print(f"  [{dex:4}] {coin:14} szi={szi:+.8f} -> {side} reduce-only market")
    missing_open = sorted(wanted - {coin for _, coin, _ in current})
    if missing_open:
        print(f"  not open now: {missing_open}")
    if not current:
        print("Nothing selected is open.")
        return 0
    if not execute:
        print("\nDRY-RUN: no orders placed. Re-run with --execute to close selected positions.")
        return 0

    for attempt in range(1, 5):
        current = [pos for pos in open_positions() if pos[1] in wanted]
        if not current:
            break
        print(f"\n--- SELECTED FLATTEN pass {attempt}: {len(current)} open ---")
        for _dex, coin, szi in current:
            try:
                result = retry(f"market_close[{coin}]", lambda: ex.market_close(coin), attempts=3)
                print(f"  market_close {coin} (szi={szi:+.8f}) -> {str(result)[:260]}")
            except Exception as exc:
                print(f"  FAILED market_close {coin}: {exc}")
            time.sleep(1.0)
        time.sleep(2.0)

    left = [pos for pos in open_positions() if pos[1] in wanted]
    if left:
        print(f"\nWARNING: still open after selected flatten: {left}")
        return 1
    print("\nSelected positions flat confirmed across main+xyz+flx.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
