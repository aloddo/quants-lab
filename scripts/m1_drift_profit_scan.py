"""Full-universe error distribution for the cent-recon GO/NO-GO call.

Per wallet (burst-aware seed = current M1), compute:
  - max/median inter-anchor DRIFT % (|recon_equity(anchor) - perpAllTime_anchor| / anchor),
    i.e. "drift at any anchor check over the 6 months" (the >5% question).
  - PROFITABILITY from perpAllTime anchors + ledger flows ONLY (ground truth, independent
    of our reconstruction): flow_adj_pnl = (last_eq - first_eq) - net_external_flow;
    roe = flow_adj_pnl / mean(anchor equity).
  - Dimensions: n_fills (activity), exotic? (any ':' coin), n_dexes, n_sentinel_zeros,
    max gross leverage proxy (max posval/equity over anchors), mean equity.
Streams per-wallet rows to a parquet via multiprocessing. Memory-safe (one wallet at a time).
"""
import sys
sys.path.insert(0, "research/v15")
import argparse
import multiprocessing as mp
import numpy as np
import pandas as pd
import v15_m01_equity_reconstruct as m01

S = int(pd.Timestamp("2025-12-01", tz="UTC").timestamp() * 1000)
E = int(pd.Timestamp("2026-05-23", tz="UTC").timestamp() * 1000 + 86_399_999)
_ANCHORS = None


def scan(w):
    try:
        wl = w.lower()
        anchor = m01.load_wallet_anchor(w, _ANCHORS)
        if anchor is None:
            return {"wallet": w, "err": "no_anchor"}
        avh = m01.get_portfolio_perp(w)
        wa = sorted((t, v) for t, v in avh if v > 0.01 and S <= t <= E)
        n_sentinel = sum(1 for t, v in avh if v == 0.0 and S <= t <= E)
        if len(wa) < 2:
            return {"wallet": w, "err": "lt2_anchors", "n_sentinel": n_sentinel}
        we = min(E, anchor.fetched_ms)
        le = max(we, int(anchor.fetched_ms))
        fills = m01.load_wallet_fills(w, S, le)
        fu = m01.load_wallet_funding(w, S, le)
        ld = m01.load_wallet_ledger(w, S, le)
        stream = ([(f["time"], "fill", f) for f in fills]
                  + [(int(x["time"]), "ledger", x) for x in ld]
                  + [(int(x["time"]), "funding", x) for x in fu])
        stream.sort(key=lambda x: x[0])
        wa = [(t, v) for t, v in wa if S <= t <= we]
        drifts, levs, errs_usd, incomplete = [], [], [], 0
        for i in range(1, len(wa)):
            a_t, a_v = wa[i - 1]
            b_t, b_v = wa[i]
            if b_t <= a_t:
                continue
            wr = m01.compute_eq_at(stream, fills, anchor, wl, b_t, a_t, a_v)
            if wr.recon_incomplete:
                incomplete += 1
                continue
            if abs(b_v) > 0.01:
                drifts.append(abs(wr.equity - b_v) / abs(b_v))
                errs_usd.append(abs(wr.equity - b_v))
                if wr.equity > 1:
                    levs.append(abs(wr.position_value) / max(abs(wr.equity), 1.0))
        # profitability from anchors + ledger (ground truth)
        net_flow = sum(m01.ledger_cash_delta(e, wl).ext_flow for e in ld
                       if S <= int(e["time"]) <= we)
        eqs = [v for _, v in wa]
        first_eq, last_eq = eqs[0], eqs[-1]
        mean_eq = float(np.mean(eqs))
        pnl = (last_eq - first_eq) - net_flow
        roe = pnl / mean_eq if mean_eq > 1 else float("nan")
        exotic = any(":" in f["coin"] for f in fills)
        dexes = sorted({m01.coin_dex(f["coin"]) for f in fills})
        darr = np.array(drifts)
        return {
            "wallet": w, "err": "",
            "max_drift_pct": float(np.max(drifts)) * 100 if drifts else float("nan"),
            "median_drift_pct": float(np.median(drifts)) * 100 if drifts else float("nan"),
            "p90_drift_pct": float(np.percentile(darr, 90)) * 100 if drifts else float("nan"),
            "n_over_5pct": int((darr > 0.05).sum()) if drifts else 0,
            "frac_over_5pct": float((darr > 0.05).mean()) if drifts else float("nan"),
            "max_err_usd": float(np.max(errs_usd)) if errs_usd else float("nan"),
            "n_checks": len(drifts), "n_incomplete": incomplete,
            "n_fills": len(fills), "n_anchors": len(wa), "n_sentinel": n_sentinel,
            "exotic": exotic, "n_dexes": len(dexes), "dexes": ",".join(dexes),
            "max_leverage": float(np.max(levs)) if levs else float("nan"),
            "mean_equity": mean_eq, "first_eq": first_eq, "last_eq": last_eq,
            "net_flow": net_flow, "flow_adj_pnl": pnl, "roe_pct": roe * 100 if roe == roe else float("nan"),
        }
    except Exception as e:  # noqa: BLE001
        return {"wallet": w, "err": f"exc:{type(e).__name__}"}


def _init():
    global _ANCHORS
    _ANCHORS = pd.read_parquet(m01.ANCHOR_PARQUET)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets", default="app/data/v15/m01_nonerroring_wallets.txt")
    ap.add_argument("--out", default="app/data/v15/m01_drift_profit_scan.parquet")
    ap.add_argument("--procs", type=int, default=8)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    wallets = [l.strip() for l in open(args.wallets) if l.strip().startswith("0x")]
    if args.limit:
        wallets = wallets[:args.limit]
    print(f"scanning {len(wallets)} wallets with {args.procs} procs", flush=True)
    rows = []
    with mp.Pool(args.procs, initializer=_init) as pool:
        for i, r in enumerate(pool.imap_unordered(scan, wallets, chunksize=16)):
            rows.append(r)
            if (i + 1) % 500 == 0:
                print(f"  {i+1}/{len(wallets)}", flush=True)
    df = pd.DataFrame(rows)
    df.to_parquet(args.out, index=False, compression="snappy")
    ok = df[df["err"] == ""]
    print(f"DONE {len(df)} rows -> {args.out}; scanned_ok={len(ok)}", flush=True)
