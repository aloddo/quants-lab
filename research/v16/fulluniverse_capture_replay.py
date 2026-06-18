#!/usr/bin/env python
"""
fulluniverse_capture_replay.py -- THE question: does COIN-UNIVERSE BREADTH alone reach $500/mo?

Builds faithful-copy journeys (leader ENTRY -> matching EXIT) for the FULL cohort across all coins from
m02_cohort_slice, prices OUR $150 copy of each through research/v15/execution_model.py (per-coin slip +
RT taker fee), then runs the live-cap occupancy replay (15 slots, 1 position/coin at $150+0.50 conc)
across coin-universe tiers: 10 majors -> +liquid alts -> all-liquid -> all coins.

calibrated_share() is reported: non-major coins use the DEFAULT slip, so their edge is slip-uncertain.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/fulluniverse_capture_replay.py
"""
import sys
import numpy as np
import pandas as pd

sys.path.insert(0, "research/v15")
import execution_model as EM  # noqa: E402

BASE = 150.0
EQUITY = 486.0
MAX_SLOTS = 15
CONC_CAP = 0.50

MAJORS = ["ADA", "AVAX", "BNB", "BTC", "CRV", "DOGE", "ETH", "HYPE", "LINK", "SOL"]
# liquid HL alts (real perp markets, decent depth) -- the realistic expansion set
LIQUID_ALTS = ["XRP", "NEAR", "ENA", "TON", "WLD", "ONDO", "SUI", "ARB", "AAVE", "TAO", "ICP",
               "UNI", "BCH", "DOT", "APT", "SEI", "LTC", "ZEC", "AVAX", "OP", "INJ", "ATOM",
               "FIL", "RUNE", "TIA", "PYTH", "JUP", "STX", "AAVE"]


def build_journeys():
    cols = ["coin", "ts", "action_type", "signed_size", "price", "wallet",
            "opening_journey_id", "closing_journey_id", "is_liquidation"]
    sl = pd.read_parquet("app/data/v16/m02_cohort_slice.parquet", columns=cols)
    ent = sl[sl.action_type == "ENTRY"].copy()
    exi = sl[sl.action_type == "EXIT"].copy()
    # journey_id is reused across coins per wallet -> key MUST include coin
    ekey = ["wallet", "coin", "opening_journey_id"]
    xkey = ["wallet", "coin", "closing_journey_id"]
    e = (ent.sort_values("ts").groupby(ekey).first()[["ts", "price", "signed_size"]]
         .rename(columns={"ts": "entry_t", "price": "entry_px"}))
    x = (exi.sort_values("ts").groupby(xkey).first()[["ts", "price"]]
         .rename(columns={"ts": "exit_t", "price": "exit_px"}))
    e.index = e.index.rename(["wallet", "coin", "jid"])
    x.index = x.index.rename(["wallet", "coin", "jid"])
    j = e.join(x, how="inner").reset_index()
    j["is_long"] = j["signed_size"] > 0
    j["entry_s"] = j["entry_t"].astype("float64") / 1000.0
    j["exit_s"] = j["exit_t"].astype("float64") / 1000.0
    j = j[(j.exit_s > j.entry_s) & (j.entry_px > 0) & (j.exit_px > 0)].copy()
    return j.reset_index(drop=True)


def load_alt_calib():
    """Load agentC's measured per-coin one-way slip for the expanded universe into execution_model."""
    import json
    import os
    p = "/tmp/agentC_l2_calib_expanded.json"
    if not os.path.exists(p):
        print("  (no expanded calib found -- alts ride DEFAULT slip)")
        return 0
    d = json.load(open(p))
    mapping = {c: v["one_way_1k_bps"] / 1e4 for c, v in d.items()
               if isinstance(v, dict) and v.get("one_way_1k_bps") is not None}
    EM.load_extra_calib(mapping, allow_override=False)
    print(f"  loaded agentC calib for {len(mapping)} coins (one_way_1k_bps -> execution_model)")
    return len(mapping)


def price_edge(j):
    EM.reset_hits()
    net = np.empty(len(j))
    coin = j["coin"].to_numpy()
    epx = j["entry_px"].to_numpy()
    xpx = j["exit_px"].to_numpy()
    lng = j["is_long"].to_numpy()
    for i in range(len(j)):
        e = EM.apply_entry(coin[i], epx[i], bool(lng[i]))
        x = EM.apply_exit(coin[i], xpx[i], bool(lng[i]))
        net[i] = EM.gross_return(e, x, bool(lng[i])) - EM.fee_rt()
    j = j.copy()
    j["net_edge"] = net
    return j


def occupancy_replay(j, universe, max_slots=MAX_SLOTS):
    d = j[j.coin.isin(universe)].sort_values("entry_s").reset_index(drop=True)
    if not len(d):
        return None
    ndays = (d.entry_s.max() - d.entry_s.min()) / 86400.0
    open_pos = []        # (exit_s, coin, pnl_usd)
    held = {}
    accepted = 0
    realized = []
    maxc = 0
    per_coin_max = max(1, int(CONC_CAP * EQUITY / BASE))  # =1 at $150
    for _, r in d.iterrows():
        now = r.entry_s
        keep = []
        for ex, c, p in open_pos:
            if ex <= now:
                held[c] -= 1
                realized.append((ex, p))
            else:
                keep.append((ex, c, p))
        open_pos = keep
        c = r.coin
        if held.get(c, 0) >= per_coin_max:
            continue
        if len(open_pos) >= max_slots:
            continue
        held[c] = held.get(c, 0) + 1
        open_pos.append((r.exit_s, c, r.net_edge / 1.0 * BASE))  # net_edge is fraction
        accepted += 1
        maxc = max(maxc, len(open_pos))
    for ex, c, p in open_pos:
        realized.append((ex, p))
    rl = pd.DataFrame(realized, columns=["t", "pnl"]).sort_values("t")
    total = rl.pnl.sum()
    eq = EQUITY + rl.pnl.cumsum().to_numpy()
    peak = np.maximum.accumulate(eq) if len(eq) else np.array([EQUITY])
    maxdd = float((peak - eq).max()) if len(eq) else 0.0
    return dict(coins=len(set(d.coin)), accepted=accepted, trips_day=accepted / ndays,
                edge_bps=(total / (accepted * BASE) * 1e4) if accepted else 0,
                usd_day=total / ndays, usd_mo=total / ndays * 30.4,
                maxc=maxc, maxdd=maxdd, ndays=ndays)


def main():
    print("Building journeys from m02_cohort_slice...")
    j = build_journeys()
    print(f"  journeys (entry->exit matched): {len(j)} across {j.coin.nunique()} coins")
    load_alt_calib()
    j = price_edge(j)
    share, ncal, ndef = EM.calibrated_share()
    # universe tiers
    allcoins = set(j.coin.unique())
    liquid = set(MAJORS) | (set(LIQUID_ALTS) & allcoins)
    liquid_nonxyz = {c for c in allcoins if not c.startswith("xyz:")}
    tiers = [("10 majors", set(MAJORS)),
             ("majors + liquid alts", liquid),
             ("all non-xyz coins", liquid_nonxyz),
             ("ALL coins (incl xyz)", allcoins)]
    print(f"\nslip calibrated_share over priced journeys = {share:.1f}% "
          f"(calib={ncal}, default={ndef}) -- non-major edges ride the DEFAULT slip, flag.")
    print(f"\n{'universe':<24}{'coins':>6}{'trips/d':>9}{'edge_bps':>9}{'$/day':>8}{'$/mo':>8}{'conc':>6}{'maxDD':>9}")
    for name, u in tiers:
        m = occupancy_replay(j, u)
        if m:
            print(f"{name:<24}{m['coins']:>6}{m['trips_day']:>9.1f}{m['edge_bps']:>9.1f}"
                  f"{m['usd_day']:>8.1f}{m['usd_mo']:>8.0f}{m['maxc']:>6}{m['maxdd']:>9.1f}")
    print(f"\nTarget $500/mo = $16.45/day | window ~{m['ndays']:.0f}d | 15-slot cap, 1 pos/coin @ $150")
    print("READ: does $/mo climb with coin breadth toward $500, or plateau (=> account is the ceiling)?")


if __name__ == "__main__":
    main()
