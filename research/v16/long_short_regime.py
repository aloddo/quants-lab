#!/usr/bin/env python
"""
long_short_regime.py -- STRUCTURAL vs REGIME test for the skill cohort's short-side weakness.

Live (n=13 shorts) shows SHORT -516bps vs LONG +453bps, but that is ONE rally sample. The structure
page (projects/quant/research/2026-06-15-copy-edge-structure-long-vs-short) flagged the open question:
are shorts structurally negative for these leaders, or just squeezed in a rising regime? This answers it
on the LARGE historical journey sample (NOT the thin live one), conditioning on BTC regime at entry.

Method: skill-cohort journeys (memory-safe pyarrow filtered scan) -> per-journey return = net_realized_pnl
/ max_position_notional minus a flat RT cost. Classify each journey by BTC trailing-7d return at its entry
(UP / FLAT / DOWN terciles). Compare LONG vs SHORT net edge WITHIN each regime.

READ: if SHORT is negative in ALL regimes -> structural -> propose long-only/short-deprioritized copy
filter (codex + Alberto gate). If SHORT is positive in DOWN regime -> regime/squeeze -> keep both-sided.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/long_short_regime.py
"""
import json
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.compute as pc

RT_BPS = 11.0
JOURNEYS = "app/data/v15/m02_journeys.parquet"
BTC_1H = "app/data/cache/candles/binance_spot|BTC-USDT|1h.parquet"


def main():
    sk = list(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    calib = set(json.load(open("/tmp/agentC_l2_calib_expanded.json")).keys())

    # memory-safe filtered scan: only skill-cohort rows + needed columns
    dset = ds.dataset(JOURNEYS, format="parquet")
    cols = ["wallet", "coin", "side", "entry_ts", "max_position_notional", "net_realized_pnl"]
    tbl = dset.to_table(columns=cols, filter=pc.field("wallet").isin(sk))
    j = tbl.to_pandas()
    j = j[(j.coin.isin(calib)) & (j.max_position_notional > 10)].copy()
    j["ret"] = j["net_realized_pnl"] / j["max_position_notional"]
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["edge_bps"] = (j["ret"] - RT_BPS / 1e4) * 1e4
    j["side_n"] = j["side"].str.lower().map(lambda s: "LONG" if "long" in str(s) else ("SHORT" if "short" in str(s) else "?"))
    j["entry_ms"] = j["entry_ts"].astype("int64")
    print(f"skill-cohort copyable journeys: {len(j)} | LONG {int((j.side_n=='LONG').sum())} / SHORT {int((j.side_n=='SHORT').sum())}")

    # BTC trailing-7d regime at each entry
    btc = pd.read_parquet(BTC_1H)
    tcol = "timestamp_utc" if "timestamp_utc" in btc.columns else ("timestamp" if "timestamp" in btc.columns else btc.columns[0])
    btc = btc[[tcol, "close"]].dropna().sort_values(tcol).reset_index(drop=True)
    btc["ms"] = btc[tcol].astype("int64")
    if btc["ms"].iloc[0] < 1e12:   # seconds -> ms
        btc["ms"] *= 1000
    btc["close_7d_ago"] = btc["close"].shift(168)
    btc["btc_7d_ret"] = btc["close"] / btc["close_7d_ago"] - 1.0
    btc = btc.dropna(subset=["btc_7d_ret"])

    # asof-join journey entry -> most recent BTC bar
    j = j.sort_values("entry_ms")
    merged = pd.merge_asof(j, btc[["ms", "btc_7d_ret"]].sort_values("ms"),
                           left_on="entry_ms", right_on="ms", direction="backward")
    merged = merged.dropna(subset=["btc_7d_ret"])
    print(f"journeys with BTC regime: {len(merged)} | BTC 7d-ret range {merged.btc_7d_ret.min():+.1%}..{merged.btc_7d_ret.max():+.1%}")

    # regime terciles by BTC 7d return
    q1, q2 = merged.btc_7d_ret.quantile([0.3333, 0.6667])
    def regime(r):
        return "DOWN" if r <= q1 else ("FLAT" if r <= q2 else "UP")
    merged["regime"] = merged.btc_7d_ret.map(regime)
    print(f"regime cuts: DOWN<= {q1:+.1%} < FLAT <= {q2:+.1%} < UP\n")

    def stat(d):
        if not len(d):
            return "       n=0"
        return (f"n={len(d):>5} edge={d.edge_bps.mean():+7.0f}bps win={(d.edge_bps>0).mean()*100:>3.0f}% "
                f"sum_ret={d.ret.sum():+.1f}")

    print(f"{'':<8}{'LONG':>40}{'SHORT':>40}")
    for reg in ["DOWN", "FLAT", "UP"]:
        sub = merged[merged.regime == reg]
        L = sub[sub.side_n == "LONG"]; S = sub[sub.side_n == "SHORT"]
        print(f"{reg:<8}{stat(L):>40}{stat(S):>40}")
    print(f"{'ALL':<8}{stat(merged[merged.side_n=='LONG']):>40}{stat(merged[merged.side_n=='SHORT']):>40}")

    # verdict
    short_by_reg = {reg: merged[(merged.regime == reg) & (merged.side_n == "SHORT")].edge_bps.mean()
                    for reg in ["DOWN", "FLAT", "UP"]}
    print("\nSHORT edge by regime:", {k: f"{v:+.0f}bps" for k, v in short_by_reg.items()})
    neg_all = all(v < 0 for v in short_by_reg.values() if v == v)
    down_pos = short_by_reg.get("DOWN", float("nan")) > 0
    if neg_all:
        print("VERDICT: STRUCTURAL -- shorts negative in ALL regimes. Propose long-only/short-deprioritized "
              "copy filter (codex + Alberto gate).")
    elif down_pos:
        print("VERDICT: REGIME -- shorts POSITIVE in DOWN markets, negative only in the rally. Keep both-sided; "
              "the live -516bps is the rally squeeze, not a broken edge.")
    else:
        print("VERDICT: MIXED -- shorts weak but not uniformly negative; size down shorts, do not kill.")


if __name__ == "__main__":
    main()
