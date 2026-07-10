"""Materialize m01's funding + ledger JSON caches from on-disk parquet sources.

WHY (2026-07-10): m01's load_wallet_funding/load_wallet_ledger read per-wallet JSON from
app/data/v13/raw_{funding,ledger}_cache_20k/ (written by v13_s3_ledger_downloader). Those dirs
were empty, so m01 was reconstructing equity with ZERO funding + ZERO ledger events. The raw
data still exists on disk as parquet (v13/funding_cache per-wallet + raw/ledger/ledger_20k
monolithic). This tool rebuilds the exact JSON contract m01 expects from that parquet, so the
audited m01 code runs UNCHANGED on its intended input. Faithful to v13_s3_ledger_downloader's
schema: [{'time': ms, 'hash': str, 'delta': {<raw HL delta dict>}}], token stored under delta['token'].

Usage: python tools/materialize_m01_funding_ledger_cache.py --wallets-file <f> [--start ... --end ...]
NON-DESTRUCTIVE: writes new JSON files; never deletes raw parquet.
"""
import argparse
import json
import math
from pathlib import Path

import pandas as pd

FUNDING_CACHE = Path("/Users/hermes/quants-lab/app/data/v13/funding_cache")
LEDGER_20K = Path("/Users/hermes/quants-lab/app/data/raw/ledger/ledger_20k.parquet")
FUNDING_OUT = Path("/Users/hermes/quants-lab/app/data/v13/raw_funding_cache_20k")
LEDGER_OUT = Path("/Users/hermes/quants-lab/app/data/v13/raw_ledger_cache_20k")

# Delta fields ledger_cash_delta() reads. token is materialized from the parquet 'coin' column.
_LEDGER_DELTA_FIELDS = [
    "usdc", "usdcValue", "amount", "fee", "user", "destination",
    "sourceDex", "destinationDex", "toPerp", "netWithdrawnUsd", "operation",
]


def _clean(v):
    """Drop NaN/None; keep everything else as-is (strings stay strings)."""
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


def materialize_funding(wallet: str, s_ms: int, e_ms: int) -> int:
    files = sorted(FUNDING_CACHE.glob(f"{wallet}_*.parquet"))
    recs = []
    for f in files:
        d = pd.read_parquet(f)
        for r in d.itertuples(index=False):
            t = int(r.time_ms)
            if t < s_ms or t > e_ms:
                continue
            recs.append({
                "time": t,
                "hash": "",
                "delta": {"type": "funding", "coin": str(r.coin), "usdc": str(r.usdc_signed)},
            })
    recs.sort(key=lambda x: x["time"])
    FUNDING_OUT.mkdir(parents=True, exist_ok=True)
    out = FUNDING_OUT / f"{wallet}_{s_ms}_{e_ms}.json"
    tmp = out.with_suffix(".json.tmp")
    with open(tmp, "w") as fh:
        json.dump(recs, fh)
    tmp.replace(out)
    return len(recs)


def materialize_ledger(ledger_df: pd.DataFrame, wallet: str, s_ms: int, e_ms: int) -> int:
    sub = ledger_df[ledger_df["wallet"] == wallet]
    recs = []
    for r in sub.itertuples(index=False):
        t = int(r.time)
        if t < s_ms or t > e_ms:
            continue
        delta = {"type": str(r.type)}
        # token lives in the parquet 'coin' column; ledger_cash_delta reads delta['token'].
        coin = _clean(getattr(r, "coin", None))
        if coin is not None and str(coin) != "":
            delta["token"] = str(coin)
        for fld in _LEDGER_DELTA_FIELDS:
            v = _clean(getattr(r, fld, None))
            if v is not None:
                delta[fld] = v
        recs.append({"time": t, "hash": _clean(getattr(r, "hash", "")) or "", "delta": delta})
    recs.sort(key=lambda x: x["time"])
    LEDGER_OUT.mkdir(parents=True, exist_ok=True)
    out = LEDGER_OUT / f"{wallet}_{s_ms}_{e_ms}.json"
    tmp = out.with_suffix(".json.tmp")
    with open(tmp, "w") as fh:
        json.dump(recs, fh)
    tmp.replace(out)
    return len(recs)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets-file", required=True)
    ap.add_argument("--start", default="2025-12-01")
    ap.add_argument("--end", default="2026-05-23")
    args = ap.parse_args()

    s_ms = int(pd.Timestamp(args.start, tz="UTC").timestamp() * 1000)
    e_ms = int((pd.Timestamp(args.end, tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1000 - 1)

    with open(args.wallets_file) as f:
        wallets = [ln.strip().lower() for ln in f if ln.strip() and not ln.startswith("#")]

    print(f"Loading ledger_20k ({LEDGER_20K}) ...")
    lg = pd.read_parquet(LEDGER_20K)
    print(f"  {len(lg):,} ledger rows, {lg['wallet'].nunique():,} wallets")

    for w in wallets:
        nf = materialize_funding(w, s_ms, e_ms)
        nl = materialize_ledger(lg, w, s_ms, e_ms)
        print(f"  {w[:12]}: funding={nf}  ledger={nl}")
    print("DONE")


if __name__ == "__main__":
    main()
