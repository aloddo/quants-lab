#!/usr/bin/env python3
"""Copy A -- ROSTER build: position-level follower ROC over copyable-frequency survivors.

Fixes the scale-out double-count in copyA_roc.py. A follower mirrors the LEADER's
positions (not fills) at a FIXED notional S. So we aggregate fills to POSITIONS
(via startPosition zero-crossings) and compute ONE return per position:

  position_realized = sum(closedPnl over the position's fills)      # clean, leader $
  entry_notional    = sum(|notional| of opening/adding fills)       # what leader risked
  pos_return        = position_realized / entry_notional            # return on notional
  follower_$/pos    = S * pos_return  -  S * drag_frac_roundtrip     # follower at size S

Sum per month -> follower monthly $ at our capital. Gates:
  - copyable frequency: <= MAX_FPD fills/day (HFT uncopyable with follower latency)
  - consistency: >= MIN_MONTHS active, >= MIN_POSFRAC months net-positive for follower
  - est monthly $ > 0

S defaults to $200 (about a quarter of the ~$924 book -> ~4 concurrent mirrors).
"""
from __future__ import annotations
import sys, os, json, glob
import numpy as np
import pandas as pd
sys.path.insert(0, "research/v15")
import execution_model as EM

FILLS_DIR = "app/data/hl_s3_fills_v2_by_wallet"
SCREEN = "app/data/copyA/local_screen.jsonl"
OUT = "app/data/copyA/roster.jsonl"
S = float(os.environ.get("FOLLOWER_S", "200"))
MAX_FPD = float(os.environ.get("MAX_FPD", "8"))
MIN_MONTHS = 4
MIN_POSFRAC = 0.60


def is_perp_major(coin: str) -> bool:
    return bool(coin) and not any(c in coin for c in ("@", ":", "/", "#"))


def screen_survivors():
    out = []
    for line in open(SCREEN):
        try:
            r = json.loads(line)
        except Exception:
            continue
        if r.get("status") != "ok":
            continue
        if (r.get("perp_share", 0) >= 0.5 and r.get("maker_share", 0) <= 0.6
                and r.get("active_months", 0) >= MIN_MONTHS and r.get("pos_frac", 0) >= MIN_POSFRAC
                and r.get("total_real_k", 0) > 0 and r.get("liquid_share", 0) >= 0.7):
            out.append(r["wallet"])
    return out


def positions_from_fills(df):
    """Yield (month, pos_return, entry_notl) per CLEAN in-window round-trip, per coin.

    A clean round-trip both OPENS from flat and CLOSES to flat inside the data
    window. Positions carried in from before the window (first fill has
    startPosition != 0) are skipped entirely -- their entry notional is not
    observable in-window, so realized/entry would explode (the L3 carried-position
    bug). Positions still open at window end are also dropped (no close).
    """
    EPS = 1e-9
    df = df.sort_values("time")
    for coin, sub in df.groupby("coin", sort=False):
        sub = sub.reset_index(drop=True)
        start = pd.to_numeric(sub["startPosition"], errors="coerce").fillna(0.0).to_numpy()
        ssz = pd.to_numeric(sub["signed_sz"], errors="coerce").fillna(0.0).to_numpy()
        cp = pd.to_numeric(sub["closedPnl"], errors="coerce").fillna(0.0).to_numpy()
        notl = (pd.to_numeric(sub["price"], errors="coerce")
                * pd.to_numeric(sub["size"], errors="coerce")).abs().fillna(0.0).to_numpy()
        times = sub["time"].to_numpy()
        after = start + ssz
        drag_frac = EM.slip_oneway(coin) * 2 + EM.fee_rt(coin=coin)
        tracking = False   # inside a clean position opened-from-flat in-window
        skipping = False   # inside a carried-in position (ignore until it closes)
        realized = 0.0
        entry = 0.0
        for i in range(len(sub)):
            prev = start[i]
            post = after[i]
            if not tracking and not skipping:
                if abs(prev) < EPS and abs(post) > EPS:
                    tracking = True            # opened from flat -> clean
                elif abs(prev) > EPS:
                    skipping = True            # first sight already open -> carried, skip
            if skipping:
                if abs(post) < EPS:            # carried position finally flat -> reset, ready
                    skipping = False
                continue
            if tracking:
                if abs(post) > abs(prev) + 1e-12:
                    entry += notl[i]
                realized += cp[i]
                if abs(post) < EPS:            # clean close
                    if entry > 0:
                        m = pd.to_datetime(times[i], unit="ms").strftime("%Y-%m")
                        yield (m, realized / entry - drag_frac, entry)
                    realized = 0.0
                    entry = 0.0
                    tracking = False


def roster_one(wallet):
    path = f"{FILLS_DIR}/{wallet}.parquet"
    if not os.path.exists(path):
        return {"wallet": wallet, "status": "no_local"}
    df = pd.read_parquet(path, columns=["coin", "size", "price", "time", "closedPnl",
                                        "startPosition", "signed_sz"])
    df = df[df["coin"].map(is_perp_major)].copy()
    if df.empty:
        return {"wallet": wallet, "status": "no_perp"}
    days = max((df["time"].max() - df["time"].min()) / 86400000.0, 1.0)
    fpd = len(df) / days
    if fpd > MAX_FPD:
        return {"wallet": wallet, "status": "hft", "fills_per_day": round(fpd, 1)}
    pos = list(positions_from_fills(df))
    if not pos:
        return {"wallet": wallet, "status": "no_positions", "fills_per_day": round(fpd, 1)}
    pdf = pd.DataFrame(pos, columns=["m", "ret", "entry_notl"])
    # follower $ per position at fixed size S
    pdf["foll_usd"] = pdf["ret"] * S
    g = pdf.groupby("m")["foll_usd"].sum()
    active = len(g)
    pos_m = int((g > 0).sum())
    n_pos = len(pdf)
    return {
        "wallet": wallet, "status": "ok",
        "fills_per_day": round(fpd, 1),
        "n_positions": n_pos,
        "pos_per_month": round(n_pos / active, 1) if active else 0.0,
        "median_entry_notl": round(float(pdf["entry_notl"].median()), 0),
        "active_months": active, "pos_months": pos_m,
        "pos_frac": round(pos_m / active, 3) if active else 0.0,
        "foll_total_usd": round(float(g.sum()), 2),
        "foll_per_month_usd": round(float(g.sum()) / active, 2) if active else 0.0,
        "worst_month_usd": round(float(g.min()), 2),
        "best_month_usd": round(float(g.max()), 2),
    }


def main():
    ws = screen_survivors()
    print(f"{len(ws)} consistency survivors -> roster (S=${S:.0f}, max {MAX_FPD} fills/day)", flush=True)
    rows = []
    with open(OUT, "w") as f:
        for i, w in enumerate(ws):
            try:
                r = roster_one(w)
            except Exception as e:
                r = {"wallet": w, "status": f"err:{type(e).__name__}"}
            f.write(json.dumps(r) + "\n"); f.flush()
            rows.append(r)
            if (i + 1) % 200 == 0:
                print(f"  {i+1}/{len(ws)}", flush=True)
    ok = [r for r in rows if r.get("status") == "ok"]
    roster = [r for r in ok if r["active_months"] >= MIN_MONTHS and r["pos_frac"] >= 0.75
              and r["foll_per_month_usd"] > 0]
    roster.sort(key=lambda r: (r["pos_frac"], r["foll_per_month_usd"]), reverse=True)
    print(f"\nok={len(ok)} roster(>=4mo, posfrac>=.75, $>0)={len(roster)}\n", flush=True)
    hdr = (f"{'wallet':<44} {'fpd':>5} {'nPos':>5} {'pos/mo':>6} {'medNotl':>8} "
           f"{'mo':>3} {'posf':>5} {'$/mo':>8} {'wMo$':>8} {'tot$':>9}")
    print(hdr); print("-" * len(hdr))
    for r in roster[:40]:
        print(f"{r['wallet']:<44} {r['fills_per_day']:>5.1f} {r['n_positions']:>5} "
              f"{r['pos_per_month']:>6.1f} {r['median_entry_notl']:>8.0f} "
              f"{r['active_months']:>3} {r['pos_frac']:>5.2f} {r['foll_per_month_usd']:>8.2f} "
              f"{r['worst_month_usd']:>8.2f} {r['foll_total_usd']:>9.2f}")


if __name__ == "__main__":
    main()
