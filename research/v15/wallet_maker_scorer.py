#!/usr/bin/env python3
"""Full-universe per-wallet MAKER copy-edge scorer (drop-or-prove the wallet list).

Alberto 2026-06-10: the V11 wallet list was agent-picked with no justification -> find better wallets
data-drivenly, or prove V11's are the best. This scores EVERY wallet's fixed-hold maker copy edge over
an OOS window, ranks them, and places the V11 hand-picked wallets in that distribution.

Artifact-free by design: FIXED hold (each copied open exits at entry+hold), maker pricing (no spread
cross, 2.88bps RT), causal entry mark at fill+latency. No 5.5-month force-close (the bug that inflated
the earlier inline eval), no FIFO ambiguity. This is a SELECTION score (entry quality), not a full
strategy sim. Per-decision harness remains the strategy validator.

Run: python research/v15/wallet_maker_scorer.py --start 2025-12-01 --end 2026-05-17 --hold-min 60
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from execution_model import fee_rt, set_latency_ms

V11_CURATED = "/tmp/v11_curated_wallets.txt"


def score_wallet(we, start_ms, end_ms, hold_ms, lat_ms, fee):
    """Fixed-hold maker copy edge: each open in [start, end-hold] -> exit at entry+hold. Maker (no slip)."""
    ev = we.slice_dicts(start_ms, end_ms)
    nets = []
    for f in ev:
        if not f["is_open"]:
            continue
        et = f["ts"] + lat_ms
        if et + hold_ms > end_ms:
            continue
        ent = S.mark_at(f["coin"], et)
        ex = S.mark_at(f["coin"], et + hold_ms)
        if ent is None or ex is None or ent <= 0:
            continue
        g = (ex - ent) / ent if f["is_long"] else (ent - ex) / ent   # maker: no spread cross
        nets.append(g - fee)
    if not nets:
        return None
    a = np.array(nets)
    return {"n": len(a), "mean_bps": float(a.mean() * 1e4), "win": float((a > 0).mean() * 100)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-12-01")
    ap.add_argument("--end", default="2026-05-17")   # pre-V11-launch (~May17) = OOS for the curated set
    ap.add_argument("--hold-min", type=int, default=60)
    ap.add_argument("--latency-s", type=int, default=2)
    ap.add_argument("--min-trades", type=int, default=30)
    ap.add_argument("--universe-file", default="app/data/v15/m01_universe_20k_wallets.txt")
    ap.add_argument("--out", default="app/data/v15/wallet_maker_scores.parquet")
    args = ap.parse_args()
    set_latency_ms(args.latency_s * 1000)
    start = int(pd.Timestamp(args.start, tz="UTC").timestamp() * 1000)
    end = int(pd.Timestamp(args.end, tz="UTC").timestamp() * 1000)
    hold_ms, lat_ms, fee = args.hold_min * 60_000, args.latency_s * 1000, fee_rt(maker=True)

    uni = [l.strip().lower() for l in open(args.universe_file) if l.strip() and not l.startswith("#")]
    print(f"scoring {len(uni)} universe wallets (maker, fixed {args.hold_min}m hold, {args.start}..{args.end}) ...")
    wf = S.load_events_from_m02(set(uni), start - hold_ms, end)
    rows = []
    for w, we in wf.items():
        r = score_wallet(we, start, end, hold_ms, lat_ms, fee)
        if r and r["n"] >= args.min_trades:
            rows.append({"wallet": w, **r})
    df = pd.DataFrame(rows).sort_values("mean_bps", ascending=False).reset_index(drop=True)
    df["pctile"] = (1 - df.index / len(df)) * 100
    df.to_parquet(args.out)
    print(f"\n=== universe maker copy-edge ({len(df)} wallets, n>={args.min_trades}) ===")
    print(f"top {df.mean_bps.iloc[0]:.1f}bps | p90 {df.mean_bps.quantile(.9):.1f} | median {df.mean_bps.median():.1f} | "
          f"frac>0 {(df.mean_bps>0).mean()*100:.0f}%")
    print("\nTOP 10 data-driven wallets:")
    print(df.head(10)[["wallet", "n", "mean_bps", "win", "pctile"]].to_string(index=False))

    # locate the V11 hand-picked wallets (score via API for any not in m02)
    cur = [l.strip().lower() for l in open(V11_CURATED) if l.strip()]
    print(f"\n=== V11 hand-picked wallets in the maker-edge ranking ===")
    for c in cur:
        r = df[df.wallet == c]
        if len(r):
            print(f"  {c[:12]} rank {r.index[0]+1}/{len(df)} pctile {r.pctile.iloc[0]:.0f}% mean {r.mean_bps.iloc[0]:.1f}bps n={int(r.n.iloc[0])}")
        else:
            try:
                f = S.load_wallet_opens_closes(c, start - hold_ms, end)
                we = S.WalletEvents.from_tuples([(d["ts"], S._coin_id(d["coin"]), d["is_open"], d["is_long"]) for d in f])
                sr = score_wallet(we, start, end, hold_ms, lat_ms, fee)
                if sr and sr["n"] >= args.min_trades:
                    pct = (df.mean_bps < sr["mean_bps"]).mean() * 100
                    print(f"  {c[:12]} (API) mean {sr['mean_bps']:.1f}bps n={sr['n']} -> ~pctile {pct:.0f}% of universe")
                else:
                    print(f"  {c[:12]} (API) too few trades ({sr['n'] if sr else 0})")
            except Exception as e:
                print(f"  {c[:12]} API fail: {str(e)[:40]}")


if __name__ == "__main__":
    main()
