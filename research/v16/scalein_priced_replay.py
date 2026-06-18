#!/usr/bin/env python
"""
scalein_priced_replay.py -- the EXECUTION-PRICED scale-in tradeoff curve (Alberto asked, 2026-06-14).

Question: if we copy the leaders' SCALE-IN (not just their first open), up to a per-coin cap C, what is the
NET $/mo, trips/day, and concentration -- priced through research/v15/execution_model.py on EVERY fill?
This turns the fork (activity vs concentration) into a data curve.

Mechanics (faithful to live V17 + the cap):
- Each journey: leader's ordered build fills = ENTRY then ADDON(s) by time; exit = first EXIT/TRIM fill.
- OUR copy: add $150 per leader build-fill, in order, until our position notional reaches cap C; ignore
  further adds. Each of our fills is priced apply_entry (per-coin slip). Exit the whole position at the
  leader's first close, priced apply_exit. Fee = fee_rt on the round-tripped notional.
- net_pnl(journey) = (gross_return(avg_entry_slipped, exit_slipped, is_long) - fee_rt) * our_notional.
- trips counted = our fills (each add is a taker order = real activity + real fee).
- Concentration = C by construction (per coin); we also report the realized equity-DD and the worst-tail
  (scale-into-loser) journeys -- the martingale-risk check.

agentC measured slip loaded (calibrated_share reported). Sweep C in multiples of equity.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/scalein_priced_replay.py
"""
import sys
import json
import numpy as np
import pandas as pd

sys.path.insert(0, "research/v15")
import execution_model as EM  # noqa: E402

BASE = 150.0
EQUITY = 486.0
FEE_RT = None  # from EM
CAPS = [0.50, 0.75, 1.00, 1.50, 2.00, 3.00]   # per-coin cap as multiple of equity


def load_universe():
    d = json.load(open("config/copy_trader_wallets_v17_expansion.json"))
    return set(d["global"]["coin_whitelist"]) | set(d["global"]["expansion"]["coins"])


def load_alt_calib():
    import os
    p = "/tmp/agentC_l2_calib_expanded.json"
    if not os.path.exists(p):
        return 0
    d = json.load(open(p))
    mapping = {c: v["one_way_1k_bps"] / 1e4 for c, v in d.items()
               if isinstance(v, dict) and v.get("one_way_1k_bps") is not None}
    EM.load_extra_calib(mapping, allow_override=False)
    return len(mapping)


def build_journey_fills(universe):
    """Per (wallet, coin, journey_id): ordered build-fill prices/ts + first-close price/ts + direction."""
    cols = ["coin", "ts", "action_type", "signed_size", "price", "wallet",
            "opening_journey_id", "closing_journey_id"]
    sl = pd.read_parquet("app/data/v16/m02_cohort_slice.parquet", columns=cols)
    sl = sl[sl.coin.isin(universe)].copy()
    sl["t"] = sl["ts"].astype("float64") / 1000.0

    builds = sl[sl.action_type.isin(["ENTRY", "ADDON"])].copy()
    builds["jid"] = builds["opening_journey_id"]
    closes = sl[sl.action_type.isin(["EXIT", "TRIM", "REVERSE"])].copy()
    closes["jid"] = closes["closing_journey_id"]

    # build-fill lists per journey (sorted by time)
    builds = builds.sort_values("t")
    bg = builds.groupby(["wallet", "coin", "jid"])
    open_px = bg["price"].apply(list)
    open_t = bg["t"].apply(list)
    sign0 = bg["signed_size"].first()
    # first close per journey
    closes = closes.sort_values("t")
    cg = closes.groupby(["wallet", "coin", "jid"]).first()[["price", "t"]]

    j = pd.DataFrame({"px_list": open_px, "t_list": open_t, "sign0": sign0}).join(
        cg.rename(columns={"price": "exit_px", "t": "exit_t"}), how="inner")
    j = j.reset_index()
    j = j[(j.exit_px > 0) & (j.sign0 != 0)].copy()
    j["is_long"] = j["sign0"] > 0
    j["entry_t"] = j["t_list"].apply(lambda l: l[0])
    return j


def replay_cap(j, cap_mult, max_slots=15):
    """Occupancy replay: 15 position slots, 1 active journey per coin at a time. Step journeys by entry
    time; skip if coin already held or slots full. This bounds the copy to the real account."""
    C = cap_mult * EQUITY
    n_fills_cap = max(1, int(round(C / BASE)))   # how many $150 fills fit in the cap
    d = j.sort_values("entry_t").reset_index(drop=True)
    ndays = (d.entry_t.max() - d.entry_t.min()) / 86400.0
    realized = []   # (exit_t, net_pnl)
    total_fills = 0
    tail = []
    open_pos = []     # (exit_t, coin)
    held = set()
    for _, r in d.iterrows():
        now = r.entry_t
        if open_pos:
            keep = []
            for ex, c in open_pos:
                if ex <= now:
                    held.discard(c)
                else:
                    keep.append((ex, c))
            open_pos = keep
        coin = r.coin
        if coin in held or len(open_pos) >= max_slots:
            continue                              # slot/coin occupancy block (the real account limit)
        is_long = bool(r.is_long)
        pxs = r.px_list[:n_fills_cap]            # we take the first n_fills_cap build fills
        if not pxs:
            continue
        ent_fills = [EM.apply_entry(coin, p, is_long) for p in pxs if p > 0]
        if not ent_fills:
            continue
        avg_entry = float(np.mean(ent_fills))    # equal $150 per fill
        our_notional = BASE * len(ent_fills)
        exit_px = EM.apply_exit(coin, r.exit_px, is_long)
        ret = EM.gross_return(avg_entry, exit_px, is_long)
        net = (ret - EM.fee_rt()) * our_notional
        realized.append((r.exit_t, net))
        total_fills += len(ent_fills)
        tail.append(net)
        open_pos.append((r.exit_t, coin))
        held.add(coin)
    rl = pd.DataFrame(realized, columns=["t", "pnl"]).sort_values("t")
    total = rl.pnl.sum()
    eq = EQUITY + rl.pnl.cumsum().to_numpy()
    peak = np.maximum.accumulate(eq) if len(eq) else np.array([EQUITY])
    maxdd = float((peak - eq).max()) if len(eq) else 0.0
    tail = np.array(tail)
    return dict(cap=cap_mult, percoin_usd=C, fills_per_journey=n_fills_cap,
                trips_day=total_fills / ndays, usd_day=total / ndays, usd_mo=total / ndays * 30.4,
                maxdd=maxdd, maxdd_pct=maxdd / EQUITY * 100,
                worst1pct=float(np.percentile(tail, 1)) if len(tail) else 0.0,
                n_journeys=len(rl), ndays=ndays)


def main():
    universe = load_universe()
    ncal = load_alt_calib()
    EM.reset_hits()
    print(f"Loading journey fills (universe={len(universe)} coins, agentC calib {ncal} coins)...")
    j = build_journey_fills(universe)
    print(f"  journeys: {len(j)} | median build-fills/journey: {int(j.px_list.apply(len).median())}")
    share, nc, nd = EM.calibrated_share()
    print(f"\n{'cap':>5}{'$/coin':>8}{'fills/jrny':>11}{'trips/d':>9}{'$/day':>8}{'$/mo':>8}"
          f"{'maxDD%':>8}{'worst1%$':>10}")
    rows = []
    for c in CAPS:
        m = replay_cap(j, c)
        rows.append(m)
        print(f"{m['cap']:>5.2f}{m['percoin_usd']:>8.0f}{m['fills_per_journey']:>11}{m['trips_day']:>9.1f}"
              f"{m['usd_day']:>8.1f}{m['usd_mo']:>8.0f}{m['maxdd_pct']:>8.1f}{m['worst1pct']:>10.2f}")
    share, nc, nd = EM.calibrated_share()
    print(f"\ncalibrated_share = {share:.1f}% (calib={nc}, default={nd}) | window ~{m['ndays']:.0f}d "
          f"| $500/mo = $16.45/day")
    print("cap 0.50 = current live (1 pos/coin, no scale-in). Higher cap = copy more scale-in = more")
    print("activity + more concentration. maxDD% + worst1% are the scale-into-loser (martingale) risk.")
    print("NOTE: net of per-coin slip + RT taker fee on EVERY fill. Equal-$150-per-fill approximation.")


if __name__ == "__main__":
    main()
