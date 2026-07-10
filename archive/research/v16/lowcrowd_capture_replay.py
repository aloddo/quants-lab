#!/usr/bin/env python
"""
lowcrowd_capture_replay.py -- validate the MRR path (research/2026-06-14-frequency-funnel-mrr-reachable).

Question: if we capture the LOW-CROWD (k_opp<=K) entries with multi-leader stacking depth D per coin,
held to their natural exit, under the live account caps -- what is the REALIZED trips/day, does the
per-trade edge survive (vs the theoretical band mean), how many concurrent positions, and what is the
max drawdown? In-sample (sprint_trades) + OOS (forward_trades).

Inputs already priced through research/v15/execution_model.py (ov_bps = realized overlay-net edge).
This is a portfolio occupancy replay, NOT a new pricing model -- it answers CAPTUREABILITY under caps.

Caps (live): order_size $150; equity ~$486; max_coin_notional_pct 0.50 -> ~3 stacked $150/coin;
position slots from margin-util 0.48 (10x proxy) -> ~15 concurrent; gross_backstop 5.0x.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/lowcrowd_capture_replay.py
"""
import sys
import numpy as np
import pandas as pd

BASE = 150.0
EQUITY = 486.0
CONC_CAP = 0.50
MAX_SLOTS = 15            # ~ what 0.48 util allows at $150 on $486
PER_COIN_MAX = int(CONC_CAP * EQUITY / BASE)   # ~3 stacked $150 / coin


def replay(df, k_max, stack_depth, max_slots=MAX_SLOTS):
    """Step entries in time; open if (per-coin held < min(stack_depth, PER_COIN_MAX)) and (slots free).
    Each opened position realizes its ov_bps at exit. Returns dict of metrics."""
    d = df[df["k_opp"] <= k_max].copy()
    d["entry_s"] = d["entry_ts"].astype("float64") / 1000.0
    d["exit_s"] = d["exit_ts"].astype("float64") / 1000.0
    d = d.sort_values("entry_s").reset_index(drop=True)
    ndays = (d["entry_s"].max() - d["entry_s"].min()) / 86400.0

    # event stream: (time, +1 open-attempt idx) and we close by exit_s via a heap-free sweep
    open_by_coin = {}                 # coin -> count currently held
    open_positions = []               # list of (exit_s, coin, ov_bps)
    accepted, rejected_coin, rejected_slot = 0, 0, 0
    realized = []                     # (exit_s, pnl_usd)
    per_coin_cap = min(stack_depth, PER_COIN_MAX)
    max_concurrent = 0

    # to close positions as time advances, keep open_positions sorted by exit; sweep on each entry
    def close_due(now):
        nonlocal open_positions
        still = []
        for ex, c, ov in open_positions:
            if ex <= now:
                open_by_coin[c] -= 1
                realized.append((ex, ov / 1e4 * BASE))
            else:
                still.append((ex, c, ov))
        open_positions = still

    for _, r in d.iterrows():
        now = r["entry_s"]
        close_due(now)
        coin = r["coin"]
        held_coin = open_by_coin.get(coin, 0)
        slots_used = len(open_positions)
        if held_coin >= per_coin_cap:
            rejected_coin += 1
            continue
        if slots_used >= max_slots:
            rejected_slot += 1
            continue
        # OPEN
        open_by_coin[coin] = held_coin + 1
        open_positions.append((r["exit_s"], coin, r["ov_bps"]))
        accepted += 1
        max_concurrent = max(max_concurrent, len(open_positions))
    # close any remaining
    for ex, c, ov in open_positions:
        realized.append((ex, ov / 1e4 * BASE))

    rl = pd.DataFrame(realized, columns=["exit_s", "pnl"]).sort_values("exit_s")
    total = rl["pnl"].sum()
    eq = EQUITY + rl["pnl"].cumsum().to_numpy()
    peak = np.maximum.accumulate(eq) if len(eq) else np.array([EQUITY])
    maxdd = float((peak - eq).max()) if len(eq) else 0.0
    trips_day = accepted / ndays
    return {
        "k_max": k_max, "stack": stack_depth, "n_band": len(d),
        "accepted": accepted, "trips_day": trips_day,
        "rej_coin": rejected_coin, "rej_slot": rejected_slot,
        "edge_acc_bps": (total / (accepted * BASE) * 1e4) if accepted else 0.0,
        "usd_day": total / ndays, "usd_mo": total / ndays * 30.4,
        "max_concurrent": max_concurrent, "maxdd": maxdd,
        "ndays": ndays,
    }


def show(tag, df):
    print(f"\n================= {tag} =================")
    print(f"{'k<=':>4}{'stack':>6}{'band_n':>8}{'accept':>8}{'trips/d':>9}"
          f"{'edge_bps':>9}{'$/day':>8}{'$/mo':>8}{'conc':>6}{'maxDD':>8}{'rejC/rejS':>12}")
    for k_max in [3, 6, 99]:
        for stack in [1, 2, 3]:
            m = replay(df, k_max, stack)
            print(f"{m['k_max']:>4}{m['stack']:>6}{m['n_band']:>8}{m['accepted']:>8}"
                  f"{m['trips_day']:>9.1f}{m['edge_acc_bps']:>9.1f}{m['usd_day']:>8.1f}"
                  f"{m['usd_mo']:>8.0f}{m['max_concurrent']:>6}{m['maxdd']:>8.1f}"
                  f"{str(m['rej_coin'])+'/'+str(m['rej_slot']):>12}")


def main():
    ins = pd.read_parquet("app/data/v16/sprint_trades_enriched.parquet")
    oos = pd.read_parquet("app/data/v16/forward_trades.parquet")
    print(f"PER_COIN_MAX={PER_COIN_MAX} (0.50 conc / $150), MAX_SLOTS={MAX_SLOTS} (0.48 util)")
    show("IN-SAMPLE (sprint_trades, 10 majors 2 folds)", ins)
    show("OOS (forward_trades)", oos)
    print("\nTarget: $500/mo = $16.45/day. Read trips/d (realized capture), edge_bps (survival),"
          " conc (<=15 fits), maxDD (correlated-DD risk).")


if __name__ == "__main__":
    main()
