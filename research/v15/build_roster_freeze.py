#!/usr/bin/env python
"""ROSTER FREEZE builder — merges both pre-registered FDR families through the THREE-TIER gates
and emits the frozen-roster ladder for LP review. CORPUS-ONLY (<= 2026-07-13): no input touches
the validation window (corrections/quant-engineer/2026-08-07-touched-validation-window-with-scorer).

Three tiers (findings/quant/2026-08-07-replica-space-attributes-neuter-behavior-vetoes):
  LEADER tier  — martingale/behavior vetoes on LEADER-side attributes (July panel; journeys-derived
                 fallback for panel gaps: addon-rate + liq_closed + hold asymmetry).
  REPLICA tier — what OUR book experiences (latency/median-hold from the parity panel; two-sided).
  EXCHANGE tier — lifetime PnL / recency / (equity diagnostic) via HL API, fail-closed on NaN.

Ladder levels (pre-registered in the locked decision's sweep clause):
  L0 locked | L1 recency<=14d | L2 +liq<=1.5% | L3 +uw-add<=0.30 | L4 +two-sided 20-80.
"""
import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from post_m06b_hard_gates import api_gates  # reuse the API tier (rate-limited, fail-closed)

LADDER = [
    ("L0", dict(recency=7.0, liq=0.005, uw=0.20, lo=0.25, hi=0.75)),
    ("L1", dict(recency=14.0, liq=0.005, uw=0.20, lo=0.25, hi=0.75)),
    ("L2", dict(recency=14.0, liq=0.015, uw=0.20, lo=0.25, hi=0.75)),
    ("L3", dict(recency=14.0, liq=0.015, uw=0.30, lo=0.25, hi=0.75)),
    ("L4", dict(recency=14.0, liq=0.015, uw=0.30, lo=0.20, hi=0.80)),
]
MAE_LEADER_MAX = 0.15
LATENCY_MAX = 0.02
COPY_LATENCY_S = 4.0


def wmean(d, col, wcol="n_pos"):
    v = pd.to_numeric(d[col], errors="coerce")
    w = d[wcol].clip(lower=1).astype(float)
    m = v.notna()
    return float(np.average(v[m], weights=w[m])) if m.any() else np.nan


def leader_tier(july: pd.DataFrame, journeys: pd.DataFrame, w: str) -> dict:
    d = july[july["primary_wallet"].str.lower() == w]
    if len(d):
        return {"leader_src": "july_panel", "uw_add": wmean(d, "mean_underwater_add"),
                "leader_mae_p90": wmean(d, "mae_p90"), "leader_liq": wmean(d, "liq_rate"),
                "leader_long": wmean(d, "frac_long")}
    j = journeys[journeys["wallet"] == w]
    if not len(j):
        return {"leader_src": "NONE", "uw_add": np.nan, "leader_mae_p90": np.nan,
                "leader_liq": np.nan, "leader_long": np.nan}
    # Journeys fallback: addon-rate as the martingale-act proxy (adds per journey on LOSING
    # journeys vs winners is unavailable without marks; plain addon-rate is CONSERVATIVE-NEUTRAL —
    # flagged in the report as proxy-sourced), liq from liq_closed, long share from side.
    ret = pd.to_numeric(j["net_realized_pnl"], errors="coerce")
    losers = j[ret < 0]
    addon_rate_losers = float((pd.to_numeric(losers["n_addon_fills"], errors="coerce") > 0).mean()) \
        if len(losers) else 0.0
    return {"leader_src": "journeys_proxy",
            "uw_add": addon_rate_losers,     # adds-on-losing-journeys share (mark-free proxy)
            "leader_mae_p90": np.nan,        # not derivable mark-free -> NA (reported, not gated)
            "leader_liq": float(j["liq_closed"].mean()),
            "leader_long": float((j["side"].astype(str).str.lower().isin(["long", "buy", "b"])).mean())}


def replica_tier(fresh: pd.DataFrame, w: str) -> dict:
    d = fresh[fresh["primary_wallet"].str.lower() == w]
    if not len(d):
        return {"replica_hold_h": np.nan, "latency_ratio": np.nan, "replica_long": np.nan}
    hold = wmean(d, "median_hold_h")
    return {"replica_hold_h": hold,
            "latency_ratio": COPY_LATENCY_S / max(hold * 3600.0, 1e-9) if hold == hold else np.nan,
            "replica_long": wmean(d, "frac_long")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--july-panel", required=True)
    ap.add_argument("--fresh-panel", required=True)
    ap.add_argument("--journeys", required=True)
    ap.add_argument("--skip-api", action="store_true")
    args = ap.parse_args()
    run = Path(args.run_dir)

    locked = pd.read_parquet(run / "m06b_confirmed.parquet")
    relaxed = pd.read_parquet(run / "m06b_relaxed" / "m06b_confirmed.parquet")
    fam = {}
    for _, r in locked.iterrows():
        fam.setdefault(r.primary_wallet.lower(), set()).add("locked")
    for _, r in relaxed.iterrows():
        fam.setdefault(r.primary_wallet.lower(), set()).add("relaxed")
    stats = pd.concat([locked, relaxed]).drop_duplicates("primary_wallet", keep="first")
    stats["w"] = stats.primary_wallet.str.lower()
    stats = stats.set_index("w")
    wallets = sorted(fam)
    print(f"union: {len(wallets)} (locked {len(locked)}, relaxed {len(relaxed)})")

    july = pd.read_parquet(args.july_panel)
    fresh = pd.read_parquet(args.fresh_panel)
    journeys = pd.read_parquet(args.journeys, columns=[
        "wallet", "side", "n_addon_fills", "liq_closed", "net_realized_pnl"])
    journeys["wallet"] = journeys["wallet"].str.lower()

    rows = []
    for w in wallets:
        row = {"wallet": w, "families": "+".join(sorted(fam[w]))}
        row.update(leader_tier(july, journeys, w))
        row.update(replica_tier(fresh, w))
        if not args.skip_api:
            row.update(api_gates(w))
        for c in ("oos_mean_r", "oos_n", "oos_folds", "oos_frac_folds_pos", "p_boot"):
            row[c] = stats.loc[w, c] if w in stats.index else np.nan
        rows.append(row)
    df = pd.DataFrame(rows).set_index("wallet")
    df["oos_bps"] = (df["oos_mean_r"] * 1e4).round(1)
    # two-sided judged on LEADER long-share where present, replica otherwise (identical universes)
    df["long_share"] = df["leader_long"].fillna(df["replica_long"])

    def passes(r, lv):
        checks = [
            r.uw_add <= lv["uw"],
            (r.leader_mae_p90 <= MAE_LEADER_MAX) or (r.leader_mae_p90 != r.leader_mae_p90
                                                     and r.leader_src == "journeys_proxy"),
            r.leader_liq <= lv["liq"],
            lv["lo"] <= r.long_share <= lv["hi"],
            r.latency_ratio <= LATENCY_MAX,
            r.lifetime_pnl > 0,
            r.days_since_fill <= lv["recency"],
        ]
        return all(bool(c) and c == c for c in checks)

    for name, lv in LADDER:
        df[name] = df.apply(lambda r: passes(r, lv), axis=1)
    out_report = run / "roster_freeze_report.csv"
    df.to_csv(out_report)
    prov = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "code_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "corpus_only": True,
        "families": {"locked": "q=0.20 j>=50 p>=25", "relaxed": "q=0.20 j>=30 p>=15"},
        "tiers": {"leader": str(args.july_panel) + " + journeys proxy",
                  "replica": str(args.fresh_panel), "exchange": "HL API live"},
        "ladder_counts": {name: int(df[name].sum()) for name, _ in LADDER},
        "mae_proxy_note": "journeys_proxy wallets have NA leader MAE (reported, not gated)",
    }
    (run / "roster_freeze_provenance.json").write_text(json.dumps(prov, indent=1))
    print(json.dumps(prov["ladder_counts"]))
    for name, _ in LADDER:
        sel = df[df[name]]
        print(f"\n{name} ({len(sel)}):")
        print(sel[["families", "oos_bps", "oos_n", "leader_src", "uw_add", "long_share",
                   "replica_hold_h", "lifetime_pnl", "days_since_fill"]].round(3)
              .sort_values("oos_bps", ascending=False).to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
