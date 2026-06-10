"""Forward-persistence test: does train-period ROE persist forward, by leverage band?

Per wallet, flow-adjusted ROE from perpAllTime anchors + ledger (GROUND TRUTH) in two windows:
  train  = [2025-12-01, SPLIT)
  forward= [SPLIT, 2026-05-23]
SPLIT default 2026-03-04 (the drift-break date). Light: anchors (cached) + ledger only, no fills.
Merged downstream with m01_drift_profit_scan.parquet for leverage/activity dims.
"""
import sys
sys.path.insert(0, "research/v15")
import argparse
import multiprocessing as mp
import numpy as np
import pandas as pd
import v15_m01_equity_reconstruct as m01

S = int(pd.Timestamp("2025-12-01", tz="UTC").timestamp() * 1000)
SPLIT = int(pd.Timestamp("2026-03-04", tz="UTC").timestamp() * 1000)
E = int(pd.Timestamp("2026-05-23", tz="UTC").timestamp() * 1000 + 86_399_999)
_AN = None


def _init():
    global _AN
    _AN = pd.read_parquet(m01.ANCHOR_PARQUET)


def roe_window(anchors, ld, wl, lo, hi):
    """flow-adjusted ROE in [lo,hi): (last_eq - first_eq - net_ext_flow)/mean_eq."""
    wa = [(t, v) for t, v in anchors if v > 0.01 and lo <= t < hi]
    if len(wa) < 2:
        return np.nan, len(wa)
    eqs = [v for _, v in wa]
    net = sum(m01.ledger_cash_delta(e, wl).ext_flow for e in ld
              if lo <= int(e["time"]) < hi)
    mean_eq = float(np.mean(eqs))
    if mean_eq <= 1:
        return np.nan, len(wa)
    return ((eqs[-1] - eqs[0]) - net) / mean_eq * 100, len(wa)


def scan(w):
    try:
        wl = w.lower()
        anchor = m01.load_wallet_anchor(w, _AN)
        if anchor is None:
            return None
        avh = m01.get_portfolio_perp(w)
        anchors = [(t, v) for t, v in avh if S <= t <= E]
        ld = m01.load_wallet_ledger(w, S, E)
        tr, ntr = roe_window(anchors, ld, wl, S, SPLIT)
        fw, nfw = roe_window(anchors, ld, wl, SPLIT, E)
        return {"wallet": w, "train_roe": tr, "fwd_roe": fw,
                "n_train_anchors": ntr, "n_fwd_anchors": nfw}
    except Exception:  # noqa: BLE001
        return {"wallet": w, "train_roe": np.nan, "fwd_roe": np.nan}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets", default="app/data/v15/m01_nonerroring_wallets.txt")
    ap.add_argument("--out", default="app/data/v15/m01_fwd_persistence.parquet")
    ap.add_argument("--procs", type=int, default=4)
    args = ap.parse_args()
    wallets = [l.strip() for l in open(args.wallets) if l.strip().startswith("0x")]
    print(f"fwd-persistence scan {len(wallets)} wallets, split=2026-03-04, {args.procs} procs", flush=True)
    rows = []
    with mp.Pool(args.procs, initializer=_init) as pool:
        for i, r in enumerate(pool.imap_unordered(scan, wallets, chunksize=32)):
            if r:
                rows.append(r)
            if (i + 1) % 2000 == 0:
                print(f"  {i+1}/{len(wallets)}", flush=True)
    df = pd.DataFrame(rows)
    df.to_parquet(args.out, index=False, compression="snappy")
    print(f"DONE {len(df)} rows -> {args.out}", flush=True)
