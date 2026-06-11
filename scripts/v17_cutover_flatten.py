#!/usr/bin/env python3
"""V17 cutover step 1: flatten V16's open legs (reduce-only IOC at market) for a clean V17 epoch.

Run ONLY with V16 paused (/tmp/v12_pause + .HALT_COPY present) so it cannot re-enter.
Verifies on-exchange flatness afterwards (rule 8: exchange = truth).
"""
from __future__ import annotations
import os, sys, time

from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
import eth_account

HL_API = "https://api.hyperliquid.xyz"
PARENT = "0x11ca20aeb7cd014cf8406560ae405b12601994b4"


def main():
    for marker in ("/tmp/v12_pause", "/Users/hermes/quants-lab/.HALT_COPY"):
        if not os.path.exists(marker):
            print(f"ABORT: pause marker missing: {marker} (pause V16 first)")
            sys.exit(2)
    pk = os.environ["HL_PRIVATE_KEY"]
    acct = eth_account.Account.from_key(pk)
    info = Info(HL_API, skip_ws=True)
    ex = Exchange(acct, HL_API, account_address=PARENT)

    st = info.user_state(PARENT)
    poss = [p["position"] for p in st.get("assetPositions", []) if abs(float(p["position"]["szi"])) > 1e-12]
    if not poss:
        print("Already flat. Nothing to do.")
        return
    for pos in poss:
        coin = pos["coin"]
        szi = float(pos["szi"])
        is_buy = szi < 0           # closing a short = buy
        sz = abs(szi)
        print(f"FLATTEN {coin}: szi={szi} -> {'BUY' if is_buy else 'SELL'} {sz} reduce-only IOC")
        r = ex.market_close(coin)
        print(f"  -> {r}")
        time.sleep(1)

    time.sleep(2)
    st2 = info.user_state(PARENT)
    left = [p["position"] for p in st2.get("assetPositions", []) if abs(float(p["position"]["szi"])) > 1e-12]
    if left:
        print(f"WARNING: still open after close attempts: {[(p['coin'], p['szi']) for p in left]}")
        sys.exit(1)
    eq = st2.get("marginSummary", {}).get("accountValue")
    print(f"FLAT confirmed. Account value: ${float(eq):,.2f}" if eq else "FLAT confirmed.")


if __name__ == "__main__":
    main()
