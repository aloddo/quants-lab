#!/usr/bin/env python3
"""V16 BOOK SIM -- what does the live BOOK look like at $X/trade, uncapped vs capped?

coin_concurrency.py showed the cohort HERDS (median ~8 same-coin open at entry; any per-coin cap
blocks 50-88% of flow). So per-coin caps are off the table (Alberto + data). The remaining question:
at $/trade = 50/75/100, what is the portfolio's gross exposure path, daily PnL, max drawdown -- and
would the -8% latched global stop or the 6x gross backstop have tripped during the 75d window?

Method: cohort round-trips (liquid, faithful taker nets via marks+2s+execution_model, 500bps clip,
48h hold cap) become book positions of $SIZE each at their real entry/exit times. No per-coin caps.
Daily marks: position PnL accrues at exit (per-trade net x SIZE) -- plus intraday gross tracked at
1h resolution for exposure stats. Margin-util gate optionally applied (util cap blocks entries when
gross/10 > util_cap x equity).

Run: python research/v16/book_sim.py
"""
from __future__ import annotations
import json, sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parent.parent
sys.path.insert(0, str(_REPO / "research" / "v15"))
sys.path.insert(0, str(_HERE))

import leadlag_clean_rank_sim as S
from fidelity_replay import roundtrips
from execution_model import fee_rt, apply_entry, apply_exit, set_latency_ms
from _streaming_io import install_memory_guard
from select_cohort import load_wallet_fills, LIQUID, CAP

import json as _json
_CFG = _json.load(open(_REPO / "config" / "copy_trader_wallets_v16.json"))
FEE_T = fee_rt(maker=False)
LAT = 2_000
# codex gate finding #1: params from the SHIPPED config, never hardcoded
MAX_HOLD_MS = int(_CFG["defaults"]["max_hold_s"]) * 1000
EQUITY0 = 486.0
SIZES = [50.0, 75.0, 100.0]
UTIL_CAP = float(_CFG["global"]["max_margin_util"])
LEV = float(_CFG["global"]["max_leverage_cap"])
STOP_PCT = float(_CFG["global"]["global_stop_pct"])
BACKSTOP_X = float(_CFG["global"]["gross_backstop_x"])
HOUR = 3_600_000
# NOTE: superseded by engine_replay.py (the codex-required end-to-end proof). Kept for sizing scans.


def main():
    install_memory_guard(soft_gb=8.0, label="v16_book")
    set_latency_ms(LAT)
    cfg = json.load(open(_REPO / "config" / "copy_trader_wallets_v16.json"))
    cohort = set(cfg["wallets"].keys())
    end_ms = int(pd.Timestamp(cfg["global"]["cohort_asof"]).timestamp() * 1000)
    start_ms = end_ms - cfg["global"]["cohort_window_days"] * 86_400_000

    wf = load_wallet_fills(cohort, start_ms, end_ms)
    trades = []   # (entry_ts, exit_ts, coin, net_frac)
    for w, fl in wf.items():
        fl.sort(key=lambda x: x[0])
        for c, dir_, ets, xts, *_ in roundtrips(fl):
            if c not in LIQUID or not (start_ms <= ets < end_ms):
                continue
            xts_c = min(xts, ets + MAX_HOLD_MS)
            m0 = S.mark_at(c, ets + LAT); m1 = S.mark_at(c, xts_c + LAT)
            if m0 is None or m1 is None or m0 <= 0:
                continue
            e = apply_entry(c, m0, dir_ > 0); x = apply_exit(c, m1, dir_ > 0)
            g = max(-CAP, min(CAP, dir_ * (x - e) / e))
            trades.append((ets, xts_c, c, g - FEE_T))
    trades.sort()
    print(f"{len(trades)} book trades over {(end_ms-start_ms)/86_400_000:.0f}d "
          f"({len(trades)/((end_ms-start_ms)/86_400_000):.1f}/day)")

    for SIZE in SIZES:
        for gated in (False, True):
            # event sim: entries blocked if util gate on and projected util > cap, or backstop latched
            open_pos = []     # (exit_ts, net, entry_ts)
            realized = 0.0
            equity = EQUITY0
            blocked = 0
            stop_hit_day = None
            gross_samples = []
            daily_pnl = defaultdict(float)
            peak_eq = equity
            max_dd = 0.0
            stopped = False
            for ets, xts, c, net in trades:
                # settle exits before this entry
                open_pos = [p for p in open_pos if not (p[0] <= ets and (
                    _settle(p, daily_pnl, SIZE) or True))]
                gross = len(open_pos) * SIZE
                gross_samples.append(gross)
                if stopped:
                    blocked += 1
                    continue
                if gated:
                    if (gross + SIZE) / LEV > UTIL_CAP * EQUITY0:
                        blocked += 1
                        continue
                    if (gross + SIZE) > BACKSTOP_X * EQUITY0:
                        blocked += 1
                        continue
                open_pos.append((xts, net, ets))
            for p in open_pos:
                _settle(p, daily_pnl, SIZE)
            # equity path daily
            days = sorted(daily_pnl)
            eq = EQUITY0; peak = eq; max_dd = 0.0; stop_day = None
            for d in days:
                eq += daily_pnl[d]
                peak = max(peak, eq)
                max_dd = max(max_dd, peak - eq)
                if stop_day is None and (eq - EQUITY0) <= -STOP_PCT * EQUITY0:
                    stop_day = d
            gs = np.array(gross_samples)
            dp = np.array([daily_pnl[d] for d in days])
            tag = "GATED(util60/bs6x)" if gated else "UNCAPPED"
            print(f"\n  ${SIZE:.0f}/trade {tag}: blocked {blocked} ({blocked/len(trades)*100:.0f}%)")
            print(f"    gross at entries: p50 ${np.percentile(gs,50):.0f} p90 ${np.percentile(gs,90):.0f} "
                  f"max ${gs.max():.0f} ({gs.max()/EQUITY0:.1f}x equity)")
            print(f"    PnL: total ${dp.sum():+.0f} | daily mean ${dp.mean():+.2f} std ${dp.std():.2f} "
                  f"| worst day ${dp.min():+.2f} | best ${dp.max():+.2f}")
            print(f"    max drawdown ${max_dd:.0f} ({max_dd/EQUITY0*100:.1f}%) | "
                  f"-8% stop ({-STOP_PCT*EQUITY0:.0f}$) hit: {pd.Timestamp(stop_day, unit='ms').date() if stop_day else 'NO'}")


def _settle(p, daily_pnl, size):
    xts, net, ets = p
    day = (xts // 86_400_000) * 86_400_000
    daily_pnl[day] += net * size
    return True


if __name__ == "__main__":
    main()
