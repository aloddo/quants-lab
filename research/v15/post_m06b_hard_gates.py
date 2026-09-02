#!/usr/bin/env python
"""POST-m06b HARD behavior gates — the locked decision's transition clause.

projects/quant/decisions/2026-08-06-selection-principles-and-dials-locked: until the bag-risk /
two-sidedness / latency-ratio / lifetime-PnL gates are native m06b knobs, they run HERE, on the
confirmed set, recorded in provenance. Each gate is a MECHANISM-KILLER from a documented loss
(martingales: econ20/0x1efb, 06-14 skill cohort; corpses/lifetime-losers: recent9 8-of-9;
latency-infeasible: totalreturn5). Components stay SEPARATE (audit: never one opaque score).

Behavior attributes come from the existing PROFILE PANEL (copy_wallet_profile.py output — the
TG-12006 profiling machinery; GATE-0 reuse, not recomputation): per (entity_id, fold_id) cells
with primary_wallet. Aggregation per wallet = n_pos-weighted mean across its fold cells
(worst-fold mae_p90 also reported). Exchange-truth gates (lifetime PnL, equity, recency) query
the HL API live. Unknown/NaN on a required gate FAILS CLOSED.

Usage:
  python research/v15/post_m06b_hard_gates.py \
      --run-dir app/data/v15/experiments/<run> \
      --profile-panel app/data/v15/census20k_20260728/profile_panel.parquet [--skip-api]
Outputs (run dir): hard_gates_report.csv, hard_gates_survivors.csv, hard_gates_provenance.json.
"""
import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

TH = {
    "uw_add_max": 0.20,
    "mae_p90_max": 0.15,
    "closure_min": 0.90,
    "liq_rate_max": 0.005,
    "long_share_lo": 0.25, "long_share_hi": 0.75,
    "latency_ratio_max": 0.02,
    "copy_latency_s": 4.0,
    "giveback_max": None,          # reported, not gated (no locked threshold)
    "lifetime_pnl_min": 0.0,
    "equity_floor_usd": 1000.0,
    "recency_max_days": 7.0,
}


def _j(url, payload):
    import requests
    r = requests.post(url, json=payload, timeout=10)
    r.raise_for_status()
    return r.json()


def panel_gates(panel: pd.DataFrame, w: str) -> dict:
    d = panel[panel["primary_wallet"].str.lower() == w]
    out = {"wallet": w, "n_fold_cells": int(len(d)), "n_pos_total": int(d["n_pos"].sum()) if len(d) else 0}
    if not len(d):
        for k in ("uw_add", "mae_p90_wmean", "mae_p90_worst", "closure", "liq_rate",
                  "long_share", "median_hold_h", "latency_ratio", "giveback", "time_underwater"):
            out[k] = np.nan
        return out
    wgt = d["n_pos"].clip(lower=1).astype(float)

    def wmean(col):
        v = pd.to_numeric(d[col], errors="coerce")
        m = v.notna()
        return float(np.average(v[m], weights=wgt[m])) if m.any() else np.nan

    hold = wmean("median_hold_h")
    out.update(
        uw_add=wmean("mean_underwater_add"),
        mae_p90_wmean=wmean("mae_p90"),
        mae_p90_worst=float(pd.to_numeric(d["mae_p90"], errors="coerce").max()),
        closure=wmean("clean_close_rate"),
        liq_rate=wmean("liq_rate"),
        long_share=wmean("frac_long"),
        median_hold_h=hold,
        latency_ratio=float(TH["copy_latency_s"] / max(hold * 3600.0, 1e-9)) if hold == hold else np.nan,
        giveback=wmean("mean_giveback"),
        time_underwater=wmean("mean_time_underwater"),
    )
    return out


def api_gates(w: str) -> dict:
    url = "https://api.hyperliquid.xyz/info"
    out = {"wallet": w}
    try:
        pf = _j(url, {"type": "portfolio", "user": w})
        allt = dict(pf).get("allTime") if isinstance(pf, list) else None
        out["lifetime_pnl"] = float(allt["pnlHistory"][-1][1]) if allt and allt.get("pnlHistory") else np.nan
    except Exception as e:
        out["lifetime_pnl"] = np.nan
        out["api_error_portfolio"] = str(e)[:80]
    try:
        st = _j(url, {"type": "clearinghouseState", "user": w})
        out["equity_usd"] = float(st.get("marginSummary", {}).get("accountValue", "nan"))
    except Exception as e:
        out["equity_usd"] = np.nan
        out["api_error_state"] = str(e)[:80]
    try:
        fills = _j(url, {"type": "userFills", "user": w})
        last_ms = max((f.get("time", 0) for f in fills), default=0)
        out["days_since_fill"] = (time.time() * 1000 - last_ms) / 86_400_000 if last_ms else np.nan
    except Exception as e:
        out["days_since_fill"] = np.nan
        out["api_error_fills"] = str(e)[:80]
    time.sleep(0.4)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--profile-panel", required=True)
    ap.add_argument("--skip-api", action="store_true")
    args = ap.parse_args()
    run = Path(args.run_dir)
    conf = pd.read_parquet(run / "m06b_confirmed.parquet")
    wallets = conf["primary_wallet"].str.lower().tolist()
    print(f"confirmed set: {len(wallets)} wallets")

    panel = pd.read_parquet(args.profile_panel)
    rows = [panel_gates(panel, w) for w in wallets]
    jdf = pd.DataFrame(rows).set_index("wallet")

    if not args.skip_api:
        adf = pd.DataFrame([api_gates(w) for w in wallets]).set_index("wallet")
        jdf = jdf.join(adf, how="left")

    g = pd.DataFrame(index=jdf.index)
    # NaN on a REQUIRED behavior attribute fails closed (unmeasurable = not admissible).
    g["uw_add_ok"] = jdf["uw_add"] <= TH["uw_add_max"]
    g["mae_p90_ok"] = jdf["mae_p90_wmean"] <= TH["mae_p90_max"]
    g["liq_ok"] = jdf["liq_rate"] <= TH["liq_rate_max"]
    g["two_sided_ok"] = jdf["long_share"].between(TH["long_share_lo"], TH["long_share_hi"])
    g["latency_ok"] = jdf["latency_ratio"] <= TH["latency_ratio_max"]
    if "lifetime_pnl" in jdf.columns:
        g["lifetime_pnl_ok"] = jdf["lifetime_pnl"] > TH["lifetime_pnl_min"]
        g["recency_ok"] = jdf["days_since_fill"] <= TH["recency_max_days"]
    # DIAGNOSTIC-ONLY (2026-08-07 first-run findings, NOT gated):
    #  - equity: clearinghouseState accountValue reads ~0 for UNIFIED accounts holding spot USDC
    #    (Rule 16 — the exact trap our own account hit 2026-07-31). Needs a spot-inclusive read
    #    before it can gate; until then it is a report column.
    #  - closure: the panel's clean_close_rate is a CLEAN-EXIT share (excludes liq/reverse/window
    #    closes), NOT the audit's "closure fraction" (closed-vs-open-at-boundary). Wrong semantic
    #    to gate at 0.90; report-only pending the proper column from marked m07 outputs.
    report_only = ["closure", "equity_usd"]
    g["ALL_PASS"] = g.fillna(False).all(axis=1)

    report = jdf.join(g)
    report.to_csv(run / "hard_gates_report.csv")
    surv = report[report["ALL_PASS"]]
    surv.to_csv(run / "hard_gates_survivors.csv")
    prov = {
        "thresholds": {k: v for k, v in TH.items() if v is not None},
        "decision": "projects/quant/decisions/2026-08-06-selection-principles-and-dials-locked",
        "attributes_source": str(args.profile_panel),
        "aggregation": "n_pos-weighted mean across fold cells; worst-fold mae_p90 reported",
        "nan_policy": "required gate NaN fails closed",
        "code_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "api_used": not args.skip_api,
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "n_confirmed": len(wallets), "n_survivors": int(g["ALL_PASS"].sum()),
        "gate_fail_counts": {c: int((~g[c].fillna(False)).sum()) for c in g.columns if c != "ALL_PASS"},
    }
    (run / "hard_gates_provenance.json").write_text(json.dumps(prov, indent=1))
    print(json.dumps(prov["gate_fail_counts"]))
    print(f"SURVIVORS: {prov['n_survivors']} / {len(wallets)}")
    for w in surv.index:
        print(" ", w)
    return 0


if __name__ == "__main__":
    sys.exit(main())
