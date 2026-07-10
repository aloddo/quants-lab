#!/usr/bin/env python3
"""COPYABILITY: notional-weighted calibrated-slip share per wallet (durable version of the 2026-07-10 inline read).

WHY: raw-bps ranking over-selects execution-fragile wallets whose edge concentrates in default-slip / exotic
(xyz HIP-3) coins where our slippage is a class-default (possibly underpriced). A trustworthy-copyable wallet
earns its edge in the CALIBRATED set (l2_calib_10coin.json: measured L2 slippage). This scores, per wallet, the
notional-weighted share of activity in each execution class over a window.

Classes (per fill, notional = size*price):
  - calibrated : coin in l2_calib_10coin.json keys (BTC/ETH/SOL/BNB/HYPE/DOGE/ADA/AVAX/LINK/CRV) -> measured slip
  - exotic     : coin contains ':' (xyz / HIP-3 prefixed market) -> class-default slip, execution-fragile
  - default    : everything else (majors/midcaps absent from l2_calib) -> class-default slip

Reads the SAME hot dailies + side->signed convention as forward_oos_hot / s3_taker_verify (stage consistency).
Read-only. Rank by calib_share DESC to get the best-copyable sub-cohort. Run:
  python research/v15/copyability_calib_share.py --universe-file <csv> --lo 2026-07-01 --hi 2026-07-08 --out <csv>
"""
import sys, glob, json
from pathlib import Path
import pandas as pd, pyarrow.parquet as pq

REPO = Path(__file__).resolve().parent.parent.parent
HOT = REPO / "app" / "data" / "hl_s3_fills_v2_hot"
CALIB = set(json.load(open(REPO / "app" / "data" / "v15" / "l2_calib_10coin.json")).keys())


def klass(coin: str) -> str:
    if ":" in coin:
        return "exotic"
    if coin in CALIB:
        return "calibrated"
    return "default"


def compute(wallets, lo_ms, hi_ms):
    want = set(w.lower() for w in wallets)
    notion = {w: {"calibrated": 0.0, "default": 0.0, "exotic": 0.0} for w in want}
    for fp in sorted(glob.glob(str(HOT / "2026*.parquet"))):
        day = Path(fp).stem
        d_ms = int(pd.Timestamp(f"{day[:4]}-{day[4:6]}-{day[6:]}", tz="UTC").timestamp() * 1000)
        if d_ms + 86_400_000 < lo_ms or d_ms >= hi_ms:
            continue
        t = pq.read_table(fp, columns=["wallet", "coin", "size", "price", "time"]).to_pydict()
        w_, c_, sz_, px_, tm_ = t["wallet"], t["coin"], t["size"], t["price"], t["time"]
        for i in range(len(tm_)):
            w = w_[i]
            if w not in want:
                continue
            ts = int(tm_[i])
            if ts < lo_ms or ts >= hi_ms:
                continue
            c = c_[i]
            if not c or sz_[i] is None or px_[i] is None:
                continue
            try:
                s = float(sz_[i]); p = float(px_[i])
            except (TypeError, ValueError):
                continue
            if s <= 0 or p <= 0:
                continue
            notion[w][klass(c)] += s * p
    rows = []
    for w in wallets:
        d = notion.get(w.lower(), {"calibrated": 0.0, "default": 0.0, "exotic": 0.0})
        tot = d["calibrated"] + d["default"] + d["exotic"]
        rows.append({"wallet": w,
                     "calib_share_fresh": (d["calibrated"] / tot) if tot else 0.0,
                     "exotic_share_fresh": (d["exotic"] / tot) if tot else 0.0,
                     "notional": tot})
    return pd.DataFrame(rows)


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe-file", required=True)
    ap.add_argument("--lo", default="2026-07-01")
    ap.add_argument("--hi", default="2026-07-08")
    ap.add_argument("--out", default="/tmp/copyability_calib.csv")
    args = ap.parse_args()
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    wallets = [l.strip().lower().split(",")[0] for l in open(args.universe_file)
               if l.strip() and not l.lower().startswith("wallet") and not l.startswith("#")]
    df = compute(wallets, ms(args.lo), ms(args.hi))
    df = df.sort_values("calib_share_fresh", ascending=False)
    df.to_csv(args.out, index=False)
    print(f"calib set: {sorted(CALIB)}")
    print(f"scored {len(df)} wallets over {args.lo}..{args.hi} -> {args.out}")
    print(f"calib>=50%: {(df.calib_share_fresh>=0.5).sum()} | exotic>=30%: {(df.exotic_share_fresh>=0.3).sum()}")


if __name__ == "__main__":
    main()
