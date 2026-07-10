#!/usr/bin/env python3
"""Copy A -- OUT-OF-SAMPLE walk-forward on the fee-surviving roster.

The decisive gate. Selecting the best fee-survivors from 20k wallets in-sample is
the overfitting trap that killed copy-selection before. Real copyable edge must
PERSIST out-of-sample: a wallet chosen on the first months must keep paying a
follower in the later months.

Method (per wallet, LOCAL data, clean in-window round-trips via copyA_roster):
  - split months into IN (first N_IN) and OUT (rest)
  - follower size S_w = min(TARGET_S, leader median entry notional)  # never mirror
    bigger than the leader traded (a $40-notional strategy can't take our $200)
  - IN gate (selection): >= MIN_IN_MONTHS active in IN, IN pos_frac >= 0.75,
    IN follower $ > 0   (net of round-trip slippage + fees)
  - OOS measure: follower $/mo and pos_frac in OUT
Report: of IN-selected wallets, how many stay OOS-positive, and the OOS $/mo
distribution. A roster only ships if OOS survival is materially better than the
~50% coin-flip you'd get from pure selection luck.
"""
from __future__ import annotations
import sys, os, json
import numpy as np
import pandas as pd
sys.path.insert(0, "research/v15")
import execution_model as EM
from copyA_roster import is_perp_major, positions_from_fills, screen_survivors, FILLS_DIR

OUT = "app/data/copyA/oos.jsonl"
TARGET_S = float(os.environ.get("FOLLOWER_S", "200"))
N_IN = int(os.environ.get("N_IN", "4"))          # first N_IN calendar months = selection
MIN_IN_MONTHS = int(os.environ.get("MIN_IN_MONTHS", "3"))
IN_POSFRAC = 0.75


def wallet_oos(wallet):
    path = f"{FILLS_DIR}/{wallet}.parquet"
    if not os.path.exists(path):
        return {"wallet": wallet, "status": "no_local"}
    df = pd.read_parquet(path, columns=["coin", "size", "price", "time", "closedPnl",
                                        "startPosition", "signed_sz"])
    df = df[df["coin"].map(is_perp_major)].copy()
    if df.empty:
        return {"wallet": wallet, "status": "no_perp"}
    pos = list(positions_from_fills(df))
    if not pos:
        return {"wallet": wallet, "status": "no_positions"}
    p = pd.DataFrame(pos, columns=["m", "ret", "entry_notl"])
    # follower size capped at leader's typical size
    S_w = min(TARGET_S, float(p["entry_notl"].median()))
    p["foll_usd"] = p["ret"] * S_w
    months = sorted(p["m"].unique())
    in_months = months[:N_IN]
    out_months = months[N_IN:]
    if not out_months:
        return {"wallet": wallet, "status": "no_oos_window"}
    pin = p[p["m"].isin(in_months)]
    pout = p[p["m"].isin(out_months)]
    gin = pin.groupby("m")["foll_usd"].sum()
    gout = pout.groupby("m")["foll_usd"].sum()
    in_active = len(gin)
    in_posf = float((gin > 0).mean()) if in_active else 0.0
    in_usd = float(gin.sum())
    out_active = len(gout)
    out_posf = float((gout > 0).mean()) if out_active else 0.0
    out_usd_mo = float(gout.sum()) / out_active if out_active else 0.0
    selected = (in_active >= MIN_IN_MONTHS and in_posf >= IN_POSFRAC and in_usd > 0)
    return {
        "wallet": wallet, "status": "ok",
        "follower_size": round(S_w, 0),
        "in_months": in_active, "in_posf": round(in_posf, 3), "in_usd": round(in_usd, 2),
        "out_months": out_active, "out_posf": round(out_posf, 3),
        "out_usd_per_month": round(out_usd_mo, 2),
        "selected_in": bool(selected),
    }


def main():
    ws = screen_survivors()
    print(f"{len(ws)} consistency survivors -> OOS walk-forward "
          f"(IN=first {N_IN} mo, cap S=${TARGET_S:.0f})", flush=True)
    rows = []
    with open(OUT, "w") as f:
        for i, w in enumerate(ws):
            try:
                r = wallet_oos(w)
            except Exception as e:
                r = {"wallet": w, "status": f"err:{type(e).__name__}"}
            f.write(json.dumps(r) + "\n"); f.flush()
            rows.append(r)
            if (i + 1) % 200 == 0:
                print(f"  {i+1}/{len(ws)}", flush=True)
    ok = [r for r in rows if r.get("status") == "ok"]
    sel = [r for r in ok if r["selected_in"] and r["out_months"] >= 2]
    survivors = [r for r in sel if r["out_usd_per_month"] > 0]
    persist = [r for r in sel if r["out_posf"] >= 0.5 and r["out_usd_per_month"] > 0]
    print(f"\nok={len(ok)}  IN-selected(>=2 OOS months)={len(sel)}", flush=True)
    if sel:
        frac_pos = len(survivors) / len(sel)
        print(f"OOS-positive $/mo: {len(survivors)}/{len(sel)} = {frac_pos:.1%} "
              f"(pure luck would be ~50%)", flush=True)
        print(f"OOS-persistent (posf>=.5 AND $>0): {len(persist)}/{len(sel)} = "
              f"{len(persist)/len(sel):.1%}", flush=True)
        arr = np.array([r["out_usd_per_month"] for r in sel])
        print(f"OOS $/mo across selected: median={np.median(arr):.2f} "
              f"mean={arr.mean():.2f} p90={np.percentile(arr,90):.2f}", flush=True)
    persist.sort(key=lambda r: (r["out_posf"], r["out_usd_per_month"]), reverse=True)
    hdr = (f"{'wallet':<44} {'S':>6} {'inMo':>4} {'inPf':>5} {'outMo':>5} "
           f"{'outPf':>5} {'OOS$/mo':>8}")
    print("\n" + hdr); print("-" * len(hdr))
    for r in persist[:40]:
        print(f"{r['wallet']:<44} {r['follower_size']:>6.0f} {r['in_months']:>4} "
              f"{r['in_posf']:>5.2f} {r['out_months']:>5} {r['out_posf']:>5.2f} "
              f"{r['out_usd_per_month']:>8.2f}")


if __name__ == "__main__":
    main()
