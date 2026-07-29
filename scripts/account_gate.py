#!/usr/bin/env python3
"""EXCHANGE-TRUTH ACCOUNT GATE -- the last check before a roster gets capital.

WHY THIS EXISTS (2026-07-29). Two independently-built cohorts were about to be traded and both were
unfit, for reasons no amount of backtesting could see:

  config/copy_trader_totalreturn5_20260726.json  -- 2 of 4 were LIFETIME-NEGATIVE perp traders with
      accounts of $2,658 and $969. Our own book was $937.
  app/data/v15/census20k_20260728/go_live_final.csv -- 10 of 11 held ~$0 perp equity, 9 of 11 were
      lifetime-negative, 5 had not filled in 30 days, and nine of eleven ran above the 10x leverage cap.

  findings/quant/2026-07-29-live-roster-fails-own-validator
  findings/quant/2026-07-29-m05-copyability-lane-disabled-account-gates

The V15 funnel cannot catch this: the account-quality gates (equity floor, LEVERAGE_CAP, ROE, max_dd,
days_green) are all computed from M1, and M1 is out of scope (Alberto 2026-07-17). This gate does NOT
reinstate M1 -- it asks Hyperliquid directly, for the handful of wallets that actually reach go-live,
instead of reconstructing history for 20,378.

DELIBERATE LIMITATION -- READ THIS BEFORE REUSING IT. This reads the account as it is TODAY. It is a
DOOR GATE, not a selection gate. Using current state to judge a wallet inside a 2026-02 fold would be
look-ahead. It stops us trading a blown-up wallet; it does NOT improve who the funnel surfaces.

Exit 0 = every wallet passed. Exit 1 = at least one failed. Exit 2 = could not decide (fail closed).

  python scripts/account_gate.py --config config/copy_trader_totalreturn5_20260726.json
  python scripts/account_gate.py --wallets 0xab5e...,0x1d11...
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone

INFO = "https://api.hyperliquid.xyz/info"

# --- thresholds. Each one is here because it would have caught a specific real failure today. ---
MIN_PERP_EQUITY_USD = 10_000.0   # kills the $0 / $969 / $2,658 accounts (9 of 11 + 3 of 4)
MIN_LIFETIME_PERP_PNL = 0.0      # kills lifetime-negative traders (-$23,637 / -$19,920 / -$881 / -$190)
MAX_STALE_DAYS = 7.0             # kills the dormant (5 of 11 had no fill in 30d; one never filled)
MAX_ACCOUNT_LEVERAGE = 10.0      # Alberto copy spec 2026-06-04, same constant as m05 LEVERAGE_CAP


_LAST_CALL = [0.0]
MIN_CALL_INTERVAL_S = 0.35     # HL info returned HTTP 429 at 3 unpaced calls/wallet on 2026-07-29


def post(body: dict, tries: int = 5):
    """Paced + backing-off. A 429 must never be the reason a good wallet is rejected: fail-closed is
    correct, but a gate that randomly fails is a gate operators learn to bypass."""
    last = None
    for i in range(tries):
        wait = MIN_CALL_INTERVAL_S - (time.time() - _LAST_CALL[0])
        if wait > 0:
            time.sleep(wait)
        try:
            req = urllib.request.Request(
                INFO, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}
            )
            r = json.load(urllib.request.urlopen(req, timeout=30))
            _LAST_CALL[0] = time.time()
            return r
        except Exception as e:  # noqa: BLE001
            last = e
            _LAST_CALL[0] = time.time()
            # 429 is a penalty state, not a blip: HL keeps returning it for a while after a burst.
            # Back off much harder for rate limits than for a transient network error.
            rate_limited = "429" in str(e)
            time.sleep((5 * 2 ** i) if rate_limited else (2 ** i))
    raise RuntimeError(f"HL info {body.get('type')} failed after {tries} tries: {last}")


def check(wallet: str) -> dict:
    """Returns {'wallet','pass','fails':[...],'metrics':{...}}. Any UNKNOWN metric fails CLOSED."""
    fails: list[str] = []
    m: dict = {}

    st = post({"type": "clearinghouseState", "user": wallet})
    ms = st.get("marginSummary") or {}
    eq = float(ms.get("accountValue", "nan"))
    ntl = float(ms.get("totalNtlPos", 0.0) or 0.0)
    m["perp_equity"] = eq
    m["leverage"] = (ntl / eq) if eq > 0 else float("inf")

    if not (eq == eq):                                  # NaN -> no evidence -> fail closed
        fails.append("perp_equity_unknown")
    elif eq < MIN_PERP_EQUITY_USD:
        fails.append(f"perp_equity<${MIN_PERP_EQUITY_USD:,.0f} (${eq:,.0f})")

    if m["leverage"] > MAX_ACCOUNT_LEVERAGE:
        fails.append(f"leverage>{MAX_ACCOUNT_LEVERAGE:.0f}x ({m['leverage']:.1f}x)")

    pf = post({"type": "portfolio", "user": wallet})
    hist = dict(pf) if not isinstance(pf, dict) else pf
    pnl_hist = (hist.get("perpAllTime") or {}).get("pnlHistory") or []
    life = (float(pnl_hist[-1][1]) - float(pnl_hist[0][1])) if pnl_hist else float("nan")
    m["lifetime_perp_pnl"] = life
    if not (life == life):
        fails.append("lifetime_perp_pnl_unknown")
    elif life <= MIN_LIFETIME_PERP_PNL:
        fails.append(f"lifetime_perp_pnl<=0 (${life:,.0f})")

    fills = post({"type": "userFills", "user": wallet})
    last_ms = max((int(f["time"]) for f in fills), default=None) if isinstance(fills, list) else None
    if last_ms is None:
        m["days_since_fill"] = float("inf")
        fails.append("no_fills_on_record")
    else:
        days = (time.time() * 1000 - last_ms) / 86_400_000
        m["days_since_fill"] = days
        m["last_fill"] = datetime.fromtimestamp(last_ms / 1000, timezone.utc).strftime("%Y-%m-%d")
        if days > MAX_STALE_DAYS:
            fails.append(f"stale>{MAX_STALE_DAYS:.0f}d ({days:.1f}d)")

    return {"wallet": wallet, "pass": not fails, "fails": fails, "metrics": m}


def load_wallets(args) -> list[str]:
    if args.wallets:
        return [w.strip() for w in args.wallets.split(",") if w.strip()]
    cfg = json.load(open(args.config))
    w = cfg.get("wallets", cfg)
    return [k for k in w if isinstance(k, str) and k.startswith("0x")]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", help="copy_trader roster JSON")
    ap.add_argument("--wallets", help="comma-separated wallets (overrides --config)")
    ap.add_argument("--json-out", help="write full results here")
    args = ap.parse_args()
    if not args.config and not args.wallets:
        print("ERROR: need --config or --wallets", file=sys.stderr)
        return 2

    wallets = load_wallets(args)
    if not wallets:
        print("ERROR: no wallets resolved -- failing closed", file=sys.stderr)
        return 2

    print(f"ACCOUNT GATE  equity>=${MIN_PERP_EQUITY_USD:,.0f} | lifetime perp PnL>0 | "
          f"fill<={MAX_STALE_DAYS:.0f}d | leverage<={MAX_ACCOUNT_LEVERAGE:.0f}x")
    print(f"{'wallet':14s} {'perp_eq':>12s} {'lifetime':>13s} {'lev':>6s} {'stale_d':>8s}  verdict")
    results = []
    for w in wallets:
        try:
            r = check(w)
        except Exception as e:  # noqa: BLE001 -- unreachable exchange must never read as PASS
            # UNKNOWN is not the same as BAD. Both block arming, but an operator must be able to tell
            # "this wallet is broke" from "I could not reach Hyperliquid" -- otherwise a rate-limit
            # blip reads as a damning verdict, and the next person learns to --force past the gate.
            r = {"wallet": w, "pass": False, "unknown": True,
                 "fails": [f"UNKNOWN (not a verdict on the wallet): {e}"], "metrics": {}}
        results.append(r)
        m = r["metrics"]
        print(f"{w[:14]:14s} {m.get('perp_equity', float('nan')):>12,.0f} "
              f"{m.get('lifetime_perp_pnl', float('nan')):>13,.0f} "
              f"{m.get('leverage', float('nan')):>6.1f} {m.get('days_since_fill', float('nan')):>8.1f}  "
              f"{'PASS' if r['pass'] else 'FAIL: ' + '; '.join(r['fails'])}")

    if args.json_out:
        json.dump(results, open(args.json_out, "w"), indent=1)

    n_pass = sum(1 for r in results if r["pass"])
    n_unknown = sum(1 for r in results if r.get("unknown"))
    print(f"\n{n_pass}/{len(results)} wallets PASS" + (f"  ({n_unknown} UNDECIDED)" if n_unknown else ""))
    if n_unknown:
        print("UNDECIDED means the exchange could not be reached (often HTTP 429 after a burst of "
              "runs), NOT that the wallet is bad. Wait a minute and re-run before concluding anything.")
        return 2                      # distinct from a real FAIL, still blocking
    return 0 if n_pass == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
