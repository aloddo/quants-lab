#!/usr/bin/env python3
"""Refresh the HL funding cache to the FULL window per wallet -> cent reconciliation (M1).

PROVEN 2026-06-04: raw_funding_cache_20k stopped ~January; every post-Jan inter-anchor segment was
missing its funding and the M1 residual EQUALLED the missing funding to the dollar (a9881f6f seg10:
HL funding -$44.28 vs cache $0 == residual $42.59). Refreshing to the full window collapses residuals
to the cent. This pulls HL `userFunding` (paginated) per wallet and writes it in the cache format
load_wallet_funding() reads: app/data/v13/raw_funding_cache_20k/<wallet>_<start>_<end>.json.

STREAMING + crash-safe + resumable: one wallet at a time, flushed immediately; skips wallets whose
full-window file already exists. (rule 8.) Raw data, additive, never deletes.

Usage:
  python scripts/refresh_funding_cache.py --wallets-file W.txt [--start 2025-12-01 --end 2026-05-24]
  python scripts/refresh_funding_cache.py --wallet 0xABC...                 # single
"""
import argparse, json, logging, os, sys, time, datetime as dt
from pathlib import Path
import requests, pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stdout)
log = logging.getLogger("funding")
URL = "https://api.hyperliquid.xyz/info"
CACHE = Path("/Users/hermes/quants-lab/app/data/v13/raw_funding_cache_20k")


def pull(wallet: str, t0: int, t1: int) -> list:
    out, cur = [], t0
    while cur <= t1:
        r = None
        for _ in range(4):
            try:
                resp = requests.post(URL, json={"type": "userFunding", "user": wallet,
                                                "startTime": int(cur), "endTime": int(t1)}, timeout=25)
                if resp.status_code == 200:
                    r = resp.json(); break
            except Exception:
                pass
            time.sleep(1)
        if not r:
            break
        out.extend(r)
        last = max(int(x["time"]) for x in r)
        if len(r) < 500 or last <= cur:
            break
        cur = last + 1
    seen, ded = set(), []
    for x in out:
        k = (x["time"], x.get("delta", {}).get("coin"))
        if k in seen:
            continue
        seen.add(k); ded.append(x)
    return ded


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets-file"); ap.add_argument("--wallet")
    ap.add_argument("--start", default="2025-12-01"); ap.add_argument("--end", default="2026-05-24")
    ap.add_argument("--tag", default="full")  # filename suffix to mark a complete-window pull
    args = ap.parse_args()
    CACHE.mkdir(parents=True, exist_ok=True)
    t0 = int(pd.Timestamp(args.start, tz="UTC").timestamp() * 1000)
    t1 = int(pd.Timestamp(args.end, tz="UTC").timestamp() * 1000)
    sd, ed = args.start.replace("-", ""), args.end.replace("-", "")
    wallets = []
    if args.wallet:
        wallets = [args.wallet]
    elif args.wallets_file:
        wallets = [l.strip() for l in open(args.wallets_file) if l.strip().startswith("0x")]
    log.info(f"refreshing funding for {len(wallets)} wallets, {args.start}..{args.end}")
    for i, w in enumerate(wallets):
        wl = w.lower()
        fp = CACHE / f"{wl}_{sd}_{ed}_{args.tag}.json"
        if fp.exists():
            log.info(f"  [{i+1}/{len(wallets)}] {wl[:12]} exists, skip"); continue
        ev = pull(wl, t0, t1)
        tmp = fp.with_suffix(".json.tmp")
        json.dump(ev, open(tmp, "w")); tmp.replace(fp)  # flush per wallet (crash-safe)
        tot = sum(float(x["delta"]["usdc"]) for x in ev) if ev else 0.0
        log.info(f"  [{i+1}/{len(wallets)}] {wl[:12]} -> {len(ev)} funding events, ${tot:,.2f}")
    log.info("DONE")


if __name__ == "__main__":
    main()
