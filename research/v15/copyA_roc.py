#!/usr/bin/env python3
"""Copy A -- follower return-on-capital (ROC) layer over the consistency survivors.

Rule 18 (capital-math): absolute leader PnL is NOT the ship criterion. What
transfers to a copier at OUR ~$924 equity is the leader's RETURN PROFILE. A
follower who mirrors each of the leader's trades at a FIXED notional S earns, per
closing trade i:  follower_pnl_i = (closedPnl_i / notional_i) * S  minus execution
drag. So the copyable signal is the sum of per-trade percentage returns
(unlevered, conservative) -- independent of how big the leader traded.

For each survivor (from copyA_rank gate, read LOCAL parquet, zero network):
  roc_pct        = sum over perp closing fills of closedPnl_i/notl_i  (net of drag)
  monthly roc    -> active_months, pos_frac (consistency of the ROC itself)
  worst_month_roc, best_month_roc
  est_$_per_month @ S = (roc_pct / active_months) * S      # follower profit at fixed size S
  n_trades, avg_notl (leader's typical size -> capacity sanity)

Ranks by consistency-of-ROC then est monthly $ at S. This is the roster gate:
a wallet only ships if a fixed-size follower at our capital actually makes money
after real execution, consistently, month over month.
"""
from __future__ import annotations
import sys, os, json, glob
import pandas as pd
sys.path.insert(0, "research/v15")
import execution_model as EM

FILLS_DIR = "app/data/hl_s3_fills_v2_by_wallet"
SURVIVORS = "app/data/copyA/local_screen.jsonl"
OUT = "app/data/copyA/roc_results.jsonl"
FOLLOWER_S = float(os.environ.get("FOLLOWER_S", "200"))  # $ notional per mirrored trade

# survivor gate (mirror copyA_rank)
PERP_MIN, MAKER_MAX, MONTHS_MIN, POSFRAC_MIN, LIQ_MIN = 0.50, 0.60, 4, 0.60, 0.70


def is_perp_major(coin: str) -> bool:
    return bool(coin) and not any(c in coin for c in ("@", ":", "/", "#"))


def survivors():
    out = []
    for line in open(SURVIVORS):
        try:
            r = json.loads(line)
        except Exception:
            continue
        if r.get("status") != "ok":
            continue
        if (r.get("perp_share", 0) >= PERP_MIN and r.get("maker_share", 0) <= MAKER_MAX
                and r.get("active_months", 0) >= MONTHS_MIN and r.get("pos_frac", 0) >= POSFRAC_MIN
                and r.get("total_real_k", 0) > 0 and r.get("liquid_share", 0) >= LIQ_MIN):
            out.append(r["wallet"])
    return out


def roc_one(wallet):
    path = f"{FILLS_DIR}/{wallet}.parquet"
    if not os.path.exists(path):
        return {"wallet": wallet, "status": "no_local"}
    df = pd.read_parquet(path, columns=["coin", "size", "price", "time", "closedPnl"])
    df = df[df["coin"].map(is_perp_major)].copy()
    if df.empty:
        return {"wallet": wallet, "status": "no_perp"}
    df["cp"] = pd.to_numeric(df["closedPnl"], errors="coerce").fillna(0.0)
    df["px"] = pd.to_numeric(df["price"], errors="coerce")
    df["sz"] = pd.to_numeric(df["size"], errors="coerce")
    df["notl"] = (df["px"] * df["sz"]).abs()
    # closing fills only carry realized closedPnl; per-trade % on notional traded
    clo = df[(df["cp"] != 0) & (df["notl"] > 0)].copy()
    if clo.empty:
        return {"wallet": wallet, "status": "no_closes"}
    # per-trade fractional return, minus round-trip execution drag as a fraction of notional
    drag_frac = clo["coin"].map(lambda c: EM.slip_oneway(c) * 2 + EM.fee_rt(coin=c))
    clo["pct"] = clo["cp"] / clo["notl"] - drag_frac
    clo["m"] = pd.to_datetime(clo["time"], unit="ms").dt.strftime("%Y-%m")
    g = clo.groupby("m")["pct"].sum()
    active = len(g)
    pos = int((g > 0).sum())
    roc_pct = float(clo["pct"].sum())
    est_month_usd = (roc_pct / active) * FOLLOWER_S if active else 0.0
    return {
        "wallet": wallet, "status": "ok",
        "n_trades": int(len(clo)),
        "avg_notl": round(float(clo["notl"].mean()), 1),
        "roc_pct": round(roc_pct, 4),
        "roc_active_months": active, "roc_pos_months": pos,
        "roc_pos_frac": round(pos / active, 3) if active else 0.0,
        "worst_month_roc": round(float(g.min()), 4),
        "best_month_roc": round(float(g.max()), 4),
        f"est_usd_per_month_at_{int(FOLLOWER_S)}": round(est_month_usd, 2),
    }


def main():
    ws = survivors()
    print(f"{len(ws)} survivors -> ROC (follower size ${FOLLOWER_S:.0f}/trade)", flush=True)
    rows = []
    with open(OUT, "w") as f:
        for i, w in enumerate(ws):
            try:
                r = roc_one(w)
            except Exception as e:
                r = {"wallet": w, "status": f"err:{type(e).__name__}"}
            f.write(json.dumps(r) + "\n"); f.flush()
            rows.append(r)
            if (i + 1) % 200 == 0:
                print(f"  {i+1}/{len(ws)}", flush=True)
    ok = [r for r in rows if r.get("status") == "ok"]
    key = f"est_usd_per_month_at_{int(FOLLOWER_S)}"
    # roster gate: ROC consistent (>=4 months, >=0.75 positive) AND positive est $
    roster = [r for r in ok if r["roc_active_months"] >= 4 and r["roc_pos_frac"] >= 0.75 and r[key] > 0]
    roster.sort(key=lambda r: (r["roc_pos_frac"], r[key]), reverse=True)
    print(f"\nok={len(ok)} roster(>=4mo, posfrac>=.75, est$>0)={len(roster)}\n", flush=True)
    hdr = f"{'wallet':<44} {'trades':>6} {'avgNotl':>9} {'rocMo':>5} {'posf':>5} {'roc%':>8} {'wMroc':>8} {'$/mo@'+str(int(FOLLOWER_S)):>9}"
    print(hdr); print("-" * len(hdr))
    for r in roster[:40]:
        print(f"{r['wallet']:<44} {r['n_trades']:>6} {r['avg_notl']:>9.0f} "
              f"{r['roc_active_months']:>5} {r['roc_pos_frac']:>5.2f} "
              f"{r['roc_pct']*100:>7.1f}% {r['worst_month_roc']*100:>7.1f}% {r[key]:>9.2f}")


if __name__ == "__main__":
    main()
