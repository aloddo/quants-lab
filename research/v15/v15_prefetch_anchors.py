#!/usr/bin/env python3
"""Prefetch + disk-cache HL perpAllTime weekly anchors for a wallet list.

Populates app/data/v15/perp_anchor_cache/{wallet}.json via m01.get_portfolio_perp
(which now caches). Run sharded for speed; after this, every downstream module that
needs weekly anchors reads the cache with ZERO API calls.

Usage: python v15_prefetch_anchors.py --wallets-file W.txt
"""
import argparse
import sys
import time

sys.path.insert(0, "/Users/hermes/quants-lab/research/v15")
import v15_m01_equity_reconstruct as m  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets-file", required=True)
    args = ap.parse_args()
    wallets = [w.strip().lower() for w in open(args.wallets_file)
               if w.strip() and not w.startswith("#")]
    t0 = time.time()
    n_cached = n_empty = 0
    for i, w in enumerate(wallets, 1):
        avh = m.get_portfolio_perp(w)  # writes cache as a side effect
        if avh:
            n_cached += 1
        else:
            n_empty += 1
        if i % 200 == 0:
            print(f"  [{i}/{len(wallets)}] cached={n_cached} empty={n_empty} "
                  f"({(time.time()-t0)/60:.1f}min)", flush=True)
    print(f"DONE {len(wallets)} wallets: cached={n_cached} empty={n_empty} "
          f"in {(time.time()-t0)/60:.1f}min", flush=True)


if __name__ == "__main__":
    main()
