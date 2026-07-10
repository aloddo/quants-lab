"""Copy A shadow harness -- replays roster leader fills through the LIVE controller code.

Uses the SAME decide_on_leader_fill / risk_exit as the live loop (controller.py) so shadow behaviour
is a valid proxy of live. Fills are priced through execution_model (per-coin calibrated slippage on
entry AND exit; NO double-spread) + real HL fees. Produces per-pair + total FOLLOWER realized PnL so
we can confirm the executor LOGIC reproduces the CV backtest before any live order.

Realism still pending (flagged for the live shadow, not this offline replay): true WS latency,
partial/no fills vs L2 depth, funding accrual. Those need the live feed + testnet; this offline
replay validates the decision+exit logic end to end.

Run: python -m copy_a.shadow  (from strategies/live, or via the wrapper in scripts)
"""
from __future__ import annotations
import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, "research/v15")
import execution_model as EM  # noqa: E402
from copy_a.controller import (  # noqa: E402
    decide_net_mirror, risk_exit, FollowerPos, ControllerConfig,
)

FILLS_DIR = "app/data/hl_s3_fills_v2_by_wallet"
CONFIG = "config/copy_a_probe.json"


def load_cfg():
    c = json.load(open(CONFIG))
    g = c["global"]
    pairs = c["pairs"]
    allowed = frozenset((p["wallet"].lower(), p["coin"]) for p in pairs.values())
    ccfg = ControllerConfig(
        allowed_pairs=allowed, order_size_usd=g["order_size_usd"], cooldown_s=g["cooldown_s"],
        stop_frac=g["stop_frac"], trail_frac=g["trail_frac"], trail_arm_frac=g["trail_arm_frac"],
        max_hold_s=g["max_hold_s"],
    )
    return c, pairs, ccfg


def replay_pair(wallet, coin, size_usd, ccfg):
    """Replay one (wallet,coin) leader fill stream as a follower; return realized follower PnL ($)."""
    path = f"{FILLS_DIR}/{wallet}.parquet"
    if not os.path.exists(path):
        return None
    df = pd.read_parquet(path, columns=["coin", "time", "dir", "price", "startPosition", "signed_sz"])
    df = df[df["coin"] == coin].sort_values("time")
    if df.empty:
        return None
    df = df.rename(columns={"price": "px"})
    df["sp"] = pd.to_numeric(df["startPosition"], errors="coerce").fillna(0.0)
    df["ss"] = pd.to_numeric(df["signed_sz"], errors="coerce").fillna(0.0)
    df["leader_after"] = df["sp"] + df["ss"]
    fee_rt = EM.fee_rt(coin=coin)
    slip = EM.slip_oneway(coin)
    foll: FollowerPos | None = None
    last_entry_ts = -1e18
    realized = 0.0
    n_entries = n_exits = 0
    pcfg = ControllerConfig(frozenset({(wallet.lower(), coin)}), size_usd, ccfg.cooldown_s,
                            ccfg.stop_frac, ccfg.trail_frac, ccfg.trail_arm_frac, ccfg.max_hold_s)
    for _, row in df.iterrows():
        mark = float(row["px"])
        now = float(row["time"]) / 1000.0
        leader_before = float(row["sp"])
        leader_after = float(row["leader_after"])
        # risk exit check first (on this mark)
        if foll is not None and foll.signed_sz != 0:
            rexit, foll = risk_exit(foll, mark, now, pcfg)
            if rexit is not None:
                exit_px = mark * (1 - slip) if foll.signed_sz > 0 else mark * (1 + slip)
                gain = (exit_px - foll.entry_px) / foll.entry_px * (1 if foll.signed_sz > 0 else -1)
                realized += size_usd * gain - size_usd * fee_rt
                foll = None
                n_exits += 1
                continue
        intent = decide_net_mirror(leader_before, leader_after, wallet, coin, mark, foll, pcfg,
                                   now, last_entry_ts)
        if intent is None:
            continue
        if not intent.reduce_only:
            # entry: pay entry slippage; open follower at fixed size
            entry_px = mark * (1 + slip) if intent.is_buy else mark * (1 - slip)
            foll = FollowerPos(wallet.lower(), coin,
                               signed_sz=(size_usd / entry_px) * (1 if intent.is_buy else -1),
                               entry_px=entry_px, peak_gain_frac=0.0, opened_ts=now)
            last_entry_ts = now
            n_entries += 1
        else:
            # mirror leader close: exit at slippage
            if foll is not None:
                exit_px = mark * (1 - slip) if foll.signed_sz > 0 else mark * (1 + slip)
                gain = (exit_px - foll.entry_px) / foll.entry_px * (1 if foll.signed_sz > 0 else -1)
                realized += size_usd * gain - size_usd * fee_rt
                foll = None
                n_exits += 1
    return {"wallet": wallet, "coin": coin, "realized_usd": round(realized, 2),
            "entries": n_entries, "exits": n_exits}


def main():
    c, pairs, ccfg = load_cfg()
    print(f"shadow replay: {len(pairs)} pairs from {CONFIG}\n")
    tot = 0.0
    rows = []
    for key, p in pairs.items():
        r = replay_pair(p["wallet"], p["coin"], p["order_size_usd"], ccfg)
        if r is None:
            continue
        rows.append(r)
        tot += r["realized_usd"]
    rows.sort(key=lambda x: x["realized_usd"], reverse=True)
    print(f"{'coin':>5} {'wallet':>12} {'realized$':>10} {'entries':>8} {'exits':>6}")
    for r in rows:
        print(f"{r['coin']:>5} {r['wallet'][:10]:>12} {r['realized_usd']:>10.2f} {r['entries']:>8} {r['exits']:>6}")
    span_mo = 7.0  # data window ~Dec-Jun
    print(f"\nTOTAL follower realized (full window): ${tot:.0f}  (~${tot/span_mo:.0f}/mo across {len(rows)} pairs)")
    print("NOTE: offline replay validates controller LOGIC + exec costs; live shadow adds latency/"
          "partial-fill/funding realism before deploy.")


if __name__ == "__main__":
    main()
