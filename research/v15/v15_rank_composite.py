"""V15 rank-not-gate composite (Alberto 2026-06-05): no hard thresholds. Compute a metric matrix
per copyable wallet from REAL m02 journey PnL (NOT reconstructed equity), percentile-rank each metric
across the pool, composite by mean rank, surface wallets highest ACROSS ALL. Shows per-metric ranks
so it's not a black box. Type filter only (M4-copyable: no wash/MM/carry), no performance gate."""
import sys, json
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _streaming_io import install_memory_guard

D = Path("app/data/v15")
install_memory_guard(soft_gb=10, label="rank_composite")

jr = pd.read_parquet(D / "m02_journeys.parquet",
                     columns=["wallet","coin","entry_ts","exit_ts","duration_h","net_realized_pnl",
                              "fees","realized_pnl","max_position_notional",
                              "n_entry_fills","n_addon_fills","n_trim_fills","n_exit_fills","n_reverse_fills"])
m04 = pd.read_parquet(D / "m04_authenticity.parquet")
copyable = set(m04[(m04["copyable"] == True) & (m04["tier"] != "KILL")]["wallet"].str.lower())
jr["w"] = jr["wallet"].str.lower()
jr = jr[jr["w"].isin(copyable)].copy()
jr["entry_dt"] = pd.to_datetime(jr["entry_ts"].astype("int64"), unit="ms", utc=True)
jr["week"] = jr["entry_dt"].dt.strftime("%G-W%V")
jr["fills"] = (jr[["n_entry_fills","n_addon_fills","n_trim_fills","n_exit_fills","n_reverse_fills"]]
               .fillna(0).sum(axis=1))
jr["notional"] = jr["max_position_notional"].astype(float).abs()

rows = []
for w, g in jr.groupby("w"):
    g = g.sort_values("entry_dt")
    npj = len(g)
    if npj < 15:   # need enough positions to compute meaningful stats (NOT a performance gate)
        continue
    span_days = max(1.0, (g["entry_dt"].iloc[-1] - g["entry_dt"].iloc[0]).total_seconds() / 86400)
    wk = g.groupby("week")["net_realized_pnl"].sum()
    n_weeks = len(wk)
    if n_weeks < 6:
        continue
    net = float(g["net_realized_pnl"].sum())
    notional = float(g["notional"].sum())
    cumpnl = g["net_realized_pnl"].cumsum().values
    peak = np.maximum.accumulate(cumpnl)
    dd = (peak - cumpnl)
    maxdd_abs = float(dd.max())
    maxdd_frac = float(maxdd_abs / peak.max()) if peak.max() > 0 else 1.0  # giveback vs best cum PnL
    # --- MARTINGALE / averaging-down screen (codex 2026-06-05) ---------------------------------- #
    # A high win-rate is a TRAP: hold losers, size up after losses, realize only winners -> looks
    # perfect until a rare loss cluster. Detect from journeys; HARD-veto the time bombs.
    pnl = g["net_realized_pnl"].values
    notl = g["notional"].values
    hold = g["duration_h"].values
    win = pnl > 0; loss = pnl < 0
    n_loss = int(loss.sum())
    nxt_after_loss = [notl[i+1] for i in range(len(g)-1) if pnl[i] < 0]
    nxt_after_win  = [notl[i+1] for i in range(len(g)-1) if pnl[i] > 0]
    size_up = (float(np.mean(nxt_after_loss)) / float(np.mean(nxt_after_win))
               if nxt_after_loss and nxt_after_win and np.mean(nxt_after_win) > 0 else np.nan)
    hold_asym = (float(np.median(hold[loss])) / float(np.median(hold[win]))
                 if n_loss > 0 and win.any() and np.median(hold[win]) > 0 else np.nan)
    wl_mag = (float(np.mean(pnl[win])) / float(np.mean(np.abs(pnl[loss])))
              if n_loss > 0 and win.any() and np.mean(np.abs(pnl[loss])) > 0 else np.inf)
    # hidden-loser flag: near-zero realized losses over a large sample = carrying losers (no real
    # trader wins ~99% of hundreds of trades). codex's core martingale tell.
    hides_losers = (npj >= 50 and n_loss < max(5, int(0.02 * npj)))
    extreme = ((size_up == size_up and size_up > 3.0) or (hold_asym == hold_asym and hold_asym > 5.0)
               or (wl_mag < 0.3) or hides_losers)   # any one extreme flag = veto
    mild = int((size_up == size_up and size_up > 1.3) + (hold_asym == hold_asym and hold_asym > 2.5)
               + (wl_mag < 0.6))
    martingale = bool(extreme or mild >= 2)            # veto: 1 extreme OR >=2 mild flags
    rows.append({
        "martingale": martingale, "mart_size_up": size_up, "mart_hold_asym": hold_asym,
        "mart_wl_mag": wl_mag, "n_loss": n_loss,
        "wallet": g["wallet"].iloc[0],
        "n_pos": npj,
        "n_weeks": n_weeks,
        "pct_weeks_green": float((wk > 0).mean()),
        "net_pnl": net,
        "edge_bps": float(net / notional * 1e4) if notional > 0 else 0.0,
        "win_rate": float((g["net_realized_pnl"] > 0).mean()),
        "pos_per_day": float(npj / span_days),
        "median_hold_min": float(g["duration_h"].median() * 60.0),
        "fills_per_pos": float(g["fills"].sum() / npj),
        "maxdd_frac": maxdd_frac,
        "last_exit_days_ago": float((pd.Timestamp.utcnow() - pd.to_datetime(g["exit_ts"].dropna().astype("int64"), unit="ms", utc=True).max()).days)
                              if g["exit_ts"].notna().any() else 9999.0,
    })

df_all = pd.DataFrame(rows)
n_mart = int(df_all["martingale"].sum())
df = df_all[~df_all["martingale"]].copy()   # HARD martingale veto before ranking
print(f"candidate pool (copyable, >=15 pos, >=6 weeks): {len(df_all)} | martingale-vetoed: {n_mart} | clean: {len(df)}")

# percentile-rank each metric; higher rank = better. invert the lower-is-better ones.
higher_better = ["pct_weeks_green","net_pnl","edge_bps","win_rate","pos_per_day"]
lower_better  = ["maxdd_frac","fills_per_pos","last_exit_days_ago"]
for m in higher_better:
    df[f"r_{m}"] = df[m].rank(pct=True)
for m in lower_better:
    df[f"r_{m}"] = (-df[m]).rank(pct=True)
rank_cols = [f"r_{m}" for m in higher_better + lower_better]
df["composite"] = df[rank_cols].mean(axis=1)
df["weakest"] = df[rank_cols].min(axis=1)   # well-roundedness: worst single dimension
df = df.sort_values("composite", ascending=False)
df.to_parquet("/tmp/v15_rank_composite.parquet", index=False)

pd.set_option("display.width", 240); pd.set_option("display.max_columns", 30)
top = df.head(15)
print("\n=== TOP 15 by composite rank (across all metrics) ===")
for i, (_, r) in enumerate(top.iterrows(), 1):
    print(f"{i:2d}. {r['wallet'][:10]} comp={r['composite']:.2f} weakest={r['weakest']:.2f} | "
          f"green={r['pct_weeks_green']:.0%} net=${r['net_pnl']:,.0f} edge={r['edge_bps']:.0f}bps "
          f"win={r['win_rate']:.0%} pos/d={r['pos_per_day']:.1f} hold={r['median_hold_min']:.0f}m "
          f"f/pos={r['fills_per_pos']:.1f} DD={r['maxdd_frac']:.0%} last={r['last_exit_days_ago']:.0f}d ago")
print("\nsaved /tmp/v15_rank_composite.parquet")
