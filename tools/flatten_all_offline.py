#!/usr/bin/env python3
"""One-off ALL-DEX flatten for the 6h server-offline window (Alberto TG 10319 "Flatten", 2026-06-30).

Replicates the engine's TESTED _emergency_flatten path EXACTLY (hl_copy_trader_v17.py L1953):
  - signer = HL_PRIVATE_KEY (agent key), account_address = HL_ADDRESS (agent), perp_dexs = ["","xyz","flx"]
  - reads EXCHANGE TRUTH clearinghouseState for the PARENT across main+xyz+flx dexes
  - exchange.market_close(coin) reduce-only market for every open position (no book dependency)
  - idempotent: re-runs passes until 0 open; verifies flatness on-exchange afterwards (Rule 8)

SAFETY:
  - ABORTS unless BOTH pause markers present (/tmp/v12_pause + .HALT_COPY) so the bot cannot re-enter.
  - DRY-RUN by default: prints what it WOULD close, places NO orders. Pass --execute to fire.
"""
from __future__ import annotations
import os, sys, time
import requests
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
import eth_account

HL_API = "https://api.hyperliquid.xyz"
BUILDER_DEXES = ["xyz", "flx"]
PARENT = os.environ.get("HL_QUERY_ADDRESS", "0x11ca20aeb7cd014cf8406560ae405b12601994b4")


def open_positions():
    """Exchange-truth open positions across main + builder dexes -> list of (dex, coin, szi)."""
    out = []
    for dex_name in [""] + BUILDER_DEXES:
        payload = {"type": "clearinghouseState", "user": PARENT}
        if dex_name:
            payload["dex"] = dex_name
        data = requests.post(f"{HL_API}/info", json=payload, timeout=5).json()
        if not isinstance(data, dict) or "assetPositions" not in data:
            raise ValueError(f"clearinghouseState missing assetPositions (dex={dex_name or 'main'})")
        for ap in data.get("assetPositions", []):
            pos = ap.get("position", {})
            coin = pos.get("coin")
            szi = float(pos.get("szi", 0) or 0)
            if coin and abs(szi) >= 1e-12:
                out.append((dex_name or "main", coin, szi))
    return out


def main():
    execute = "--execute" in sys.argv
    for marker in ("/tmp/v12_pause", "/Users/hermes/quants-lab/.HALT_COPY"):
        if not os.path.exists(marker):
            print(f"ABORT: pause marker missing: {marker} (pause the bot first)")
            sys.exit(2)

    acct = eth_account.Account.from_key(os.environ["HL_PRIVATE_KEY"])
    Info(HL_API, skip_ws=True, perp_dexs=[""] + BUILDER_DEXES)  # warms dex meta
    # account_address MUST be the PARENT (where positions live) so the SDK's market_close
    # position lookup (info.user_state(account_address)) finds them. The agent key signs.
    # (HL_ADDRESS/agent holds no positions -> market_close returns None and closes nothing.)
    ex = Exchange(acct, HL_API, account_address=PARENT,
                  perp_dexs=[""] + BUILDER_DEXES)

    poss = open_positions()
    print(f"=== OPEN POSITIONS (exchange truth, parent={PARENT[:10]}...): {len(poss)} ===")
    for dex, coin, szi in poss:
        side = "BUY-to-close" if szi < 0 else "SELL-to-close"
        print(f"  [{dex:4}] {coin:14} szi={szi:+.6f} -> {side} reduce-only market")
    if not poss:
        print("Already FLAT. Nothing to do.")
        return
    if not execute:
        print("\nDRY-RUN (no orders placed). Re-run with --execute to flatten.")
        return

    # Up to 4 idempotent passes.
    for attempt in range(1, 5):
        poss = open_positions()
        if not poss:
            break
        print(f"\n--- FLATTEN pass {attempt}: {len(poss)} open ---")
        for dex, coin, szi in poss:
            try:
                r = ex.market_close(coin)
                print(f"  market_close {coin} (szi={szi:+.6f}) -> {r}")
            except Exception as e:
                print(f"  FAILED market_close {coin}: {e}")
            time.sleep(1)
        time.sleep(2)

    left = open_positions()
    if left:
        print(f"\nWARNING: STILL OPEN after 4 passes: {[(c, s) for _, c, s in left]}")
        sys.exit(1)
    print("\nFLAT confirmed across main+xyz+flx (exchange truth). 0 open.")


if __name__ == "__main__":
    main()
