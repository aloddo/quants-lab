#!/usr/bin/env python3
"""Canonical portfolio snapshot for HB reports.

HARD RULE (Alberto correction msg 7126, 2026-05-25, repeated 10000+ times):
  HL EQUITY = SPOT USDC ONLY.
  Perp account values (main, xyz, flx dexes) are LOCKED MARGIN backing open
  positions and DOUBLE-COUNT if added to equity. They are NOT equity.

This script enforces that rule. Use it from every heartbeat instead of writing
ad-hoc inline python that re-implements HL queries from memory and gets the
rule wrong every time.

Usage:
    python /Users/hermes/quants-lab/scripts/portfolio_snapshot.py

Output (single line, machine-parseable):
    HL_EQ=<spot_usdc> | BYBIT_EQ=<bybit_total> | OPTIONS_UPNL=<bybit_opt_upnl> N_OPT=<n_legs> | COMBINED=<hl+bybit> | HL_POS_MAIN=<n>+XYZ=<n>+FLX=<n>+TOTAL=<n>

Notes:
- HL positions are reported across dexes as INFORMATIONAL ONLY (not added to equity).
- The COMBINED line is HL_EQ + BYBIT_EQ (spot + Bybit).
- If you see perp acct_value anywhere in this output, IT IS A BUG.
"""
import os
import sys
import time
from pathlib import Path

import requests
from pybit.unified_trading import HTTP

# Auto-load .env so BYBIT_API_KEY/SECRET are always present (HB-safe).
# Without this, the script silently reports BYBIT_EQ=$0 when invoked from a
# shell that hasn't sourced .env (caused false "Bybit $0" status on 2026-05-26
# 16:43 HB — the snapshot showed $0 while the real balance was $477.32).
try:
    from dotenv import load_dotenv
    _ENV = Path(__file__).resolve().parent.parent / ".env"
    if _ENV.exists():
        load_dotenv(_ENV)
except ImportError:
    pass


HL_PARENT = "0x11ca20aeb7cd014cf8406560ae405b12601994b4"
HL_API = "https://api.hyperliquid.xyz/info"
DEXES = [None, "xyz", "flx"]  # main + builder dexes for position COUNT only


def _retry(fn, n=3, sleep=1.0):
    last = None
    for attempt in range(n):
        try:
            return fn()
        except Exception as e:
            last = e
            time.sleep(sleep)
    raise last if last else RuntimeError("retry failed")


def hl_spot_usdc() -> float:
    """HL spot USDC balance = HL EQUITY per Alberto rule."""
    def _go():
        r = requests.post(HL_API, json={
            "type": "spotClearinghouseState",
            "user": HL_PARENT,
        }, timeout=10)
        d = r.json()
        return sum(float(b["total"]) for b in d.get("balances", []) if b["coin"] == "USDC")
    return float(_retry(_go))


def hl_perp_positions_by_dex() -> dict:
    """Return {dex_name: n_positions} across main, xyz, flx. INFORMATIONAL ONLY.
    Do NOT add the perp acct_values to equity.
    """
    out = {}
    for dex in DEXES:
        def _go(dex=dex):
            payload = {"type": "clearinghouseState", "user": HL_PARENT}
            if dex:
                payload["dex"] = dex
            r = requests.post(HL_API, json=payload, timeout=10)
            d = r.json()
            if not d or "assetPositions" not in d:
                return 0
            return len(d.get("assetPositions", []))
        name = dex if dex else "main"
        try:
            out[name] = _retry(_go)
        except Exception:
            out[name] = -1  # signal "query failed" instead of silent 0
    return out


def bybit_snapshot() -> tuple[float, float, int]:
    """Return (total_equity, options_upnl, n_option_legs) for Bybit unified.

    Raises RuntimeError if creds are missing — silent $0 returns caused a
    status integrity bug on 2026-05-26 16:43 HB. Fail loud so the caller sees
    BYBIT_EQ=ERR instead of misreporting balance as zero.
    """
    if not (os.environ.get("BYBIT_API_KEY") and os.environ.get("BYBIT_API_SECRET")):
        raise RuntimeError(
            "BYBIT_API_KEY/SECRET missing in env; source .env or run via "
            "`set -a && source .env && set +a` first"
        )
    s = HTTP(api_key=os.environ["BYBIT_API_KEY"], api_secret=os.environ["BYBIT_API_SECRET"])
    r = s.get_wallet_balance(accountType="UNIFIED")
    by_total = float(r["result"]["list"][0]["totalEquity"])
    opnl = 0.0
    nopt = 0
    for coin in ("BTC", "ETH"):
        rr = s.get_positions(category="option", baseCoin=coin)
        for p in rr["result"]["list"]:
            if float(p.get("size", 0)) != 0:
                opnl += float(p["unrealisedPnl"])
                nopt += 1
    return (by_total, opnl, nopt)


def main():
    # HL equity (spot USDC only — Alberto rule)
    try:
        hl_eq = hl_spot_usdc()
    except Exception as e:
        print(f"HL_EQ=ERR ({e}); continuing", file=sys.stderr)
        hl_eq = float("nan")

    # HL position counts (informational only, NOT equity)
    pos = hl_perp_positions_by_dex()
    pos_main = pos.get("main", -1)
    pos_xyz = pos.get("xyz", -1)
    pos_flx = pos.get("flx", -1)
    pos_total = sum(v for v in (pos_main, pos_xyz, pos_flx) if v >= 0)

    # Bybit
    try:
        by_eq, opt_upnl, n_opt = bybit_snapshot()
    except Exception as e:
        print(f"BYBIT_EQ=ERR ({e}); continuing", file=sys.stderr)
        by_eq, opt_upnl, n_opt = (float("nan"), 0.0, 0)

    combined = hl_eq + by_eq

    print(f"HL_EQ=${hl_eq:.2f} | BYBIT_EQ=${by_eq:.2f} | "
          f"OPTIONS_UPNL=${opt_upnl:+.2f} N_OPT={n_opt} | "
          f"COMBINED=${combined:.2f} | "
          f"HL_POS_MAIN={pos_main}+XYZ={pos_xyz}+FLX={pos_flx}+TOTAL={pos_total}")


if __name__ == "__main__":
    main()
