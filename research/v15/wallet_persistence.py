#!/usr/bin/env python3
"""Wallet-selection PERSISTENCE test (is the maker copy edge real or in-sample luck?).

Rank wallets by maker copy edge on a TRAIN window, then measure the SAME wallets' edge on a later
TEST window. If train-top stays high in test (and beats random/train-bottom), selection is a real,
deployable WHO. If train and test edges are uncorrelated, the wide score distribution was luck.

Run: python research/v15/wallet_persistence.py --start 2025-12-01 --split 2026-03-15 --end 2026-05-17
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from wallet_maker_scorer import score_wallet
from execution_model import fee_rt, set_latency_ms


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-12-01")
    ap.add_argument("--split", default="2026-03-15")
    ap.add_argument("--end", default="2026-05-17")
    ap.add_argument("--hold-min", type=int, default=60)
    ap.add_argument("--latency-s", type=int, default=2)
    ap.add_argument("--min-trades", type=int, default=30)
    ap.add_argument("--universe-file", default="app/data/v15/m01_nonerroring_wallets.txt")
    args = ap.parse_args()
    set_latency_ms(args.latency_s * 1000)
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    start, split, end = ms(args.start), ms(args.split), ms(args.end)
    hold_ms, lat_ms, fee = args.hold_min * 60_000, args.latency_s * 1000, fee_rt(maker=True)

    uni = [l.strip().lower() for l in open(args.universe_file) if l.strip() and not l.startswith("#")]
    print(f"loading {len(uni)} wallets {args.start}..{args.end} ...")
    wf = S.load_events_from_m02(set(uni), start - hold_ms, end)
    rows = []
    for w, we in wf.items():
        tr = score_wallet(we, start, split, hold_ms, lat_ms, fee)
        te = score_wallet(we, split, end, hold_ms, lat_ms, fee)
        if tr and te and tr["n"] >= args.min_trades and te["n"] >= args.min_trades:
            rows.append({"wallet": w, "train_bps": tr["mean_bps"], "train_n": tr["n"],
                         "test_bps": te["mean_bps"], "test_n": te["n"]})
    df = pd.DataFrame(rows)
    print(f"\n=== persistence ({len(df)} wallets with >={args.min_trades} trades in BOTH windows) ===")
    print(f"corr(train_bps, test_bps): pearson {df.train_bps.corr(df.test_bps):.3f} | "
          f"spearman {df.train_bps.corr(df.test_bps, method='spearman'):.3f}")
    df = df.sort_values("train_bps", ascending=False).reset_index(drop=True)
    n = len(df)
    dec = max(5, n // 10)
    top = df.head(dec); bot = df.tail(dec)
    print(f"\ntrain-TOP decile ({dec} wallets): train {top.train_bps.mean():.1f}bps -> TEST {top.test_bps.mean():.1f}bps "
          f"(test win-of-wallets>0: {(top.test_bps>0).mean()*100:.0f}%)")
    print(f"train-BOTTOM decile: train {bot.train_bps.mean():.1f}bps -> TEST {bot.test_bps.mean():.1f}bps")
    print(f"ALL wallets test mean (random baseline): {df.test_bps.mean():.1f}bps")
    print(f"\nVERDICT: selection {'PERSISTS (top-decile test >> random)' if top.test_bps.mean() > df.test_bps.mean()+5 else 'does NOT persist (in-sample luck)'}")
    # where do V11's 3 proven wallets land
    proven = ["0x53b63a30a688beb53b5dc7bd731c661d678c555c","0x9e897322ae0e75b1eb3d86668d34f2271260b706","0xbbf7d7a9d0eaeab4115f022a6863450296112422"]
    print("\nV11 'proven' 3 (train -> test):")
    for p in proven:
        r = df[df.wallet == p]
        if len(r): print(f"  {p[:12]} train {r.train_bps.iloc[0]:.1f} -> test {r.test_bps.iloc[0]:.1f}bps")
        else: print(f"  {p[:12]} not in both-window set (insufficient m02 trades)")
    df.to_parquet("app/data/v15/wallet_persistence.parquet")


if __name__ == "__main__":
    main()
