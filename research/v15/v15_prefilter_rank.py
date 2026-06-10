"""V15 copy PRE-FILTER ranker (Alberto/GPT spec 2026-06-05). CHEAP features from m02 real-fill
journeys -> hard filters -> 35/25/20/10/10 z-score composite -> bucket by hold -> top candidates per
bucket fed to the M7 copy sim (the final truth). Job: avoid wasting sim time on garbage, NOT be the
final ranking. MFE/MAE + liq-distance computed later on survivors only (expensive)."""
import sys
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _streaming_io import install_memory_guard

D = Path("app/data/v15")
install_memory_guard(soft_gb=10, label="prefilter")
RT_FEE_BPS = 9.0          # HL round-trip taker ~9bps (4.5 one-way); our copy cost floor
P95_LAT_S = 15.0          # our P95 copy latency; PnL realized faster than this is not copyable

jr = pd.read_parquet(D / "m02_journeys.parquet",
    columns=["wallet","entry_ts","exit_ts","duration_h","net_realized_pnl","fees","realized_pnl",
             "max_position_notional","liq_closed","n_entry_fills","n_addon_fills","n_trim_fills",
             "n_exit_fills","n_reverse_fills"])
m04 = pd.read_parquet(D / "m04_authenticity.parquet")
copyable = set(m04[(m04["copyable"] == True) & (m04["tier"] != "KILL")]["wallet"].str.lower())
jr["w"] = jr["wallet"].str.lower()
jr = jr[jr["w"].isin(copyable)].copy()
jr["entry_dt"] = pd.to_datetime(jr["entry_ts"].astype("int64"), unit="ms", utc=True)
jr["day"] = jr["entry_dt"].dt.strftime("%Y-%m-%d")
jr["week"] = jr["entry_dt"].dt.strftime("%G-W%V")
jr["hold_s"] = jr["duration_h"].astype(float) * 3600.0
jr["fills"] = jr[["n_entry_fills","n_addon_fills","n_trim_fills","n_exit_fills","n_reverse_fills"]].fillna(0).sum(axis=1)
jr["notional"] = jr["max_position_notional"].astype(float).abs()
now = pd.Timestamp.utcnow()

rows = []
for w, g in jr.groupby("w"):
    g = g.sort_values("entry_dt")
    npos = len(g)
    active_days = g["day"].nunique()
    if npos < 50 or active_days < 14:        # HARD FILTER: activity
        continue
    pnl = g["net_realized_pnl"].values
    notl = g["notional"].values; holds = g["hold_s"].values
    net = float(pnl.sum()); notional = float(g["notional"].sum())
    if notional <= 0:
        continue
    edge_bps = net / notional * 1e4
    fee_adj_edge = edge_bps - RT_FEE_BPS
    # copyability: share of (positive) PnL from trades held longer than our latency
    pos = pnl > 0
    tot_pos = float(pnl[pos].sum()) if pos.any() else 0.0
    pnl_gt_lat = float(pnl[pos & (holds >= P95_LAT_S)].sum()) if pos.any() else 0.0
    copyability = (pnl_gt_lat / tot_pos) if tot_pos > 0 else 0.0
    pnl_share_under_15s = 1.0 - copyability
    # consistency
    dwk = g.groupby("day")["net_realized_pnl"].sum()
    prof_day_ratio = float((dwk > 0).mean())
    wwk = g.groupby("week")["net_realized_pnl"].sum()
    pct_weeks_green = float((wwk > 0).mean())
    top1_day_share = float(dwk.max() / net) if net > 0 else 1.0
    consistency = prof_day_ratio * pct_weeks_green * (1.0 - max(0.0, top1_day_share - 0.40))
    # risk discipline: add-to-loser + liquidation events (higher score = safer)
    win = pnl > 0; loss = pnl < 0
    nal = [notl[i+1] for i in range(len(g)-1) if pnl[i] < 0]
    naw = [notl[i+1] for i in range(len(g)-1) if pnl[i] > 0]
    size_up = (np.mean(nal)/np.mean(naw)) if nal and naw and np.mean(naw) > 0 else 1.0
    hold_asym = (np.median(holds[loss])/np.median(holds[win])) if loss.any() and win.any() and np.median(holds[win])>0 else 1.0
    liq_rate = float(g["liq_closed"].fillna(False).astype(bool).mean())
    risk_score = 1.0 - liq_rate - 0.5*max(0.0, size_up-1.0) - 0.1*max(0.0, hold_asym-1.0)
    # recent momentum: recency-weighted per-notional edge
    def win_edge(days_lo, days_hi):
        m = (g["entry_dt"] >= now - pd.Timedelta(days=days_hi)) & (g["entry_dt"] < now - pd.Timedelta(days=days_lo))
        sub = g[m]; n = sub["notional"].sum()
        return float(sub["net_realized_pnl"].sum()/n*1e4) if n > 0 else 0.0
    recent_mom = 0.5*win_edge(0,14) + 0.3*win_edge(14,44) + 0.2*win_edge(44,9999)
    med_hold_min = float(np.median(holds)/60.0)
    bucket = "scalp" if med_hold_min < 10 else ("intraday" if med_hold_min < 720 else "swing")
    rows.append({"wallet": g["wallet"].iloc[0], "n_pos": npos, "active_days": active_days,
        "net_pnl": net, "edge_bps": edge_bps, "fee_adj_edge": fee_adj_edge,
        "profit_per_fill": net/float(g["fills"].sum()) if g["fills"].sum()>0 else 0.0,
        "consistency": consistency, "pct_weeks_green": pct_weeks_green, "prof_day_ratio": prof_day_ratio,
        "top1_day_share": top1_day_share, "copyability": copyability, "pnl_share_under_15s": pnl_share_under_15s,
        "risk_score": risk_score, "liq_rate": liq_rate, "size_up": size_up, "hold_asym": hold_asym,
        "recent_mom": recent_mom, "med_hold_min": med_hold_min, "bucket": bucket})

df = pd.DataFrame(rows)
print(f"after activity filter (>=50 pos, >=14 days): {len(df)}")
# HARD FILTERS: edge + fast-PnL
pre = len(df)
df = df[(df["fee_adj_edge"] > 10.0) & (df["pnl_share_under_15s"] < 0.30)].copy()
print(f"after edge>10bps(fee-adj) + <30% PnL-under-15s: {len(df)} (dropped {pre-len(df)})")

def z(s):
    sd = s.std(ddof=0)
    return (s - s.mean())/sd if sd > 0 else s*0.0
df["rank_score"] = (0.35*z(df["fee_adj_edge"]) + 0.25*z(df["consistency"]) + 0.20*z(df["copyability"])
                    + 0.10*z(df["risk_score"]) + 0.10*z(df["recent_mom"]))
df = df.sort_values("rank_score", ascending=False)
df.to_parquet("/tmp/v15_prefilter.parquet", index=False)
print("\nbucket counts:", df["bucket"].value_counts().to_dict())
print("\n=== TOP 8 OVERALL ===")
for i,(_,r) in enumerate(df.head(8).iterrows(),1):
    print(f"{i}. {r['wallet'][:10]} score={r['rank_score']:.2f} edge={r['edge_bps']:.0f}(fa{r['fee_adj_edge']:.0f}) cons={r['consistency']:.2f} copyab={r['copyability']:.0%} risk={r['risk_score']:.2f} recent={r['recent_mom']:.0f}bps hold={r['med_hold_min']:.0f}m liq={r['liq_rate']:.0%} [{r['bucket']}]")
for b in ["scalp","intraday","swing"]:
    sub = df[df["bucket"]==b].head(5)
    print(f"\n=== TOP 5 {b.upper()} ===")
    for i,(_,r) in enumerate(sub.iterrows(),1):
        print(f"{i}. {r['wallet'][:10]} score={r['rank_score']:.2f} edge={r['edge_bps']:.0f}bps copyab={r['copyability']:.0%} hold={r['med_hold_min']:.0f}m recent={r['recent_mom']:.0f}bps")
