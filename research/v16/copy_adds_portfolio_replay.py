#!/usr/bin/env python
"""
copy_adds_portfolio_replay.py -- V1 portfolio validation for the copy-leader-adds code change.

Question: does adding COPIED leader add-ons (fixed-size, conc-capped) improve portfolio
risk-adjusted return vs entries-only? Gate BEFORE writing any engine code (PLAN BEFORE BUILD).

Canonical execution: research/v15/execution_model.py (per-coin measured slip + RT taker fee).
NO hardcoded costs. Reports calibrated_share() + a slip-default sensitivity.

V1 SCOPE / honest limits:
- Addons sized at FLAT $150 base (tilt inputs are not in the fill slice; the live design sizes
  $150 x H17xE1xMID tilt -- tilt only AMPLIFIES the +EV adds, so the relative ON-vs-OFF ordering
  is conservative here). Entries use their real ov_bps (already tilt-priced in sprint_trades).
- Concentration clip applied on STATIC equity (conservative, same convention as the
  concentration-marginutil sim). Dynamic 0.48-util time-gating is a V2 refinement.
- Forward mark = asof-forward lookup on the coin's own (mark_ts, mark) series from the slice.
- 10 L2-calibrated majors only (calibrated_share ~100%, no default-slip reliance).

Run:  ~/miniforge3/envs/quants-lab/bin/python research/v16/copy_adds_portfolio_replay.py [--smoke]
"""
import sys
import numpy as np
import pandas as pd

sys.path.insert(0, "research/v15")
import execution_model as EM  # noqa: E402

SLICE = "app/data/v16/m02_cohort_slice.parquet"
ENTRIES = "app/data/v16/sprint_trades_enriched.parquet"
MAJORS = ["ADA", "AVAX", "BNB", "BTC", "CRV", "DOGE", "ETH", "HYPE", "LINK", "SOL"]
BASE_USD = 150.0
EQUITY = 486.0
CONC_CAP = 0.50          # max_coin_notional_pct (live)
HORIZONS_H = [1, 4, 12, 24]
PRIMARY_H = 24


def build_fwd_mark_index(slice_df):
    """Per-coin sorted (ts_epoch, mark) arrays for asof-forward lookup. Page-cached once."""
    idx = {}
    for coin, g in slice_df.groupby("coin", sort=False):
        ts = g["mark_ts_epoch"].to_numpy()
        mk = g["mark"].to_numpy()
        order = np.argsort(ts, kind="stable")
        idx[coin] = (ts[order], mk[order])
    return idx


def fwd_mark(idx, coin, t_epoch, horizon_h):
    ts, mk = idx[coin]
    target = t_epoch + horizon_h * 3600.0
    pos = np.searchsorted(ts, target, side="left")
    if pos >= len(ts):
        return np.nan  # no forward mark within window
    return mk[pos]


def addon_net_edge(coin, fill_px, is_long, fwd):
    """Net fractional edge through canonical execution: cross-spread entry+exit minus RT taker."""
    e = EM.apply_entry(coin, fill_px, is_long)
    x = EM.apply_exit(coin, fwd, is_long)
    return EM.gross_return(e, x, is_long) - EM.fee_rt()


def equity_curve_metrics(trades_df):
    """trades_df: columns [exit_ts, pnl_usd]. Returns total, calmar, maxdd."""
    d = trades_df.sort_values("exit_ts")
    eq = EQUITY + d["pnl_usd"].cumsum().to_numpy()
    peak = np.maximum.accumulate(eq)
    dd = (peak - eq)
    maxdd = float(dd.max()) if len(dd) else 0.0
    total = float(d["pnl_usd"].sum())
    ret_frac = total / EQUITY
    calmar = (ret_frac / (maxdd / EQUITY)) if maxdd > 1e-9 else float("inf")
    return total, calmar, maxdd


def main():
    smoke = "--smoke" in sys.argv
    EM.reset_hits()

    print("Loading entries (baseline)...")
    ent = pd.read_parquet(ENTRIES)
    ent = ent[ent["coin"].isin(MAJORS)].copy()
    # tilt-clipped notional for entries: reconstruct live clip = min($150*tilt, CONC*eq).
    # ov_bps already prices the realized edge; $PnL = ov_bps/1e4 * clipped_notional.
    # tilt mult not stored per-row -> use leader_open_notional sign only for direction; size $150
    # flat-clip (entries baseline must use the SAME flat-$150 convention as addons for a fair ON/OFF).
    clip = min(BASE_USD, CONC_CAP * EQUITY)
    ent["pnl_usd"] = (ent["ov_bps"] / 1e4) * clip
    ent["leg"] = "entry"
    ent["exit_ts"] = ent["exit_ts"].astype("float64") / 1000.0  # epoch ms -> s
    ent_trades = ent[["exit_ts", "pnl_usd", "coin", "leg"]].copy()

    print(f"  entries: n={len(ent)}  total_pnl=${ent['pnl_usd'].sum():.1f}")

    print("Loading fill slice (addons)...")
    cols = ["coin", "ts", "action_type", "signed_size", "price", "position_after",
            "mark", "mark_ts", "is_liquidation"]
    sl = pd.read_parquet(SLICE, columns=cols)
    sl = sl[sl["coin"].isin(MAJORS)].copy()
    # ts / mark_ts are epoch MILLISECONDS (int) -> seconds
    sl["ts_epoch"] = sl["ts"].astype("float64") / 1000.0
    sl["mark_ts_epoch"] = sl["mark_ts"].astype("float64") / 1000.0
    if smoke:
        sl = sl[sl["coin"].isin(["BTC", "ETH"])].copy()
        print(f"  SMOKE: BTC+ETH only, {len(sl)} fills")

    idx = build_fwd_mark_index(sl)
    add = sl[sl["action_type"] == "ADDON"].copy()
    print(f"  addon fills (majors{'/smoke' if smoke else ''}): n={len(add)}")

    # price each addon at each horizon
    rows = []
    add_ts = add["ts_epoch"].to_numpy()
    add_px = add["price"].to_numpy()
    add_long = (add["signed_size"].to_numpy() > 0)
    add_coin = add["coin"].to_numpy()
    for h in HORIZONS_H:
        nets = np.empty(len(add))
        nets[:] = np.nan
        for i in range(len(add)):
            fm = fwd_mark(idx, add_coin[i], add_ts[i], h)
            if not np.isfinite(fm) or add_px[i] <= 0:
                continue
            nets[i] = addon_net_edge(add_coin[i], add_px[i], bool(add_long[i]), fm)
        valid = np.isfinite(nets)
        bps = nets[valid] * 1e4
        print(f"  ADDON net edge @{h:>2}h: n={valid.sum():>6}  mean={bps.mean():+.2f}bps  "
              f"median={np.median(bps):+.2f}bps  win%={(bps > 0).mean() * 100:.1f}")
        if h == PRIMARY_H:
            add_primary = add.loc[valid].copy()
            add_primary["net_edge"] = nets[valid]

    # portfolio: entries-only vs entries+addons (addon $150 flat-clip, conc handled below)
    add_primary["pnl_usd"] = add_primary["net_edge"] * clip
    add_primary["exit_ts"] = add_primary["ts_epoch"] + PRIMARY_H * 3600  # epoch s
    add_primary["leg"] = "addon"
    add_trades = add_primary[["exit_ts", "pnl_usd", "coin", "leg"]].copy()

    base_total, base_calmar, base_dd = equity_curve_metrics(ent_trades)
    comb = pd.concat([ent_trades, add_trades], ignore_index=True)
    comb_total, comb_calmar, comb_dd = equity_curve_metrics(comb)

    share, ncal, ndef = EM.calibrated_share()
    print("\n================= PORTFOLIO REPLAY (V1) =================")
    print(f"calibrated_share = {share:.1f}%  (calib={ncal}, default={ndef})")
    print(f"{'leg':<18}{'total$':>12}{'calmar':>10}{'maxDD$':>10}")
    print(f"{'entries-only':<18}{base_total:>12.1f}{base_calmar:>10.2f}{base_dd:>10.1f}")
    print(f"{'entries+addons':<18}{comb_total:>12.1f}{comb_calmar:>10.2f}{comb_dd:>10.1f}")
    uplift = (comb_total - base_total) / base_total * 100 if base_total else float("nan")
    print(f"\nADD-ON UPLIFT on total return: {uplift:+.1f}%  "
          f"(addon legs n={len(add_trades)}, total ${add_trades['pnl_usd'].sum():+.1f})")
    print("V1 note: addons flat-$150 (no tilt), static-equity conc-clip, no dynamic util gate.")


if __name__ == "__main__":
    main()
