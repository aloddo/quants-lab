#!/usr/bin/env python3
"""
X11 Hyperliquid 1s Microstructure EDA

Consumes:
  app/data/cache/hyperliquid_micro_1s/l2|PAIR|YYYYMMDD.parquet
  app/data/cache/hyperliquid_micro_1s/trades|PAIR|YYYYMMDD.parquet

Produces:
  app/data/cache/x11_hyperliquid/<stamp>_micro_summary.csv
  app/data/cache/x11_hyperliquid/<stamp>_micro_quantiles.csv
"""

from __future__ import annotations

import argparse
import re
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Microstructure EDA on Hyperliquid 1s snapshots")
    p.add_argument("--micro-dir", default="app/data/cache/hyperliquid_micro_1s")
    p.add_argument("--out-dir", default="app/data/cache/x11_hyperliquid")
    p.add_argument("--date", default=None, help="YYYYMMDD; default latest available")
    return p.parse_args()


def latest_date(micro_dir: Path) -> str:
    dates = set()
    for p in micro_dir.glob("l2|*|*.parquet"):
        m = re.search(r"\|(\d{8})(?:_\d{6})?\.parquet$", p.name)
        if m:
            dates.add(m.group(1))
    for p in micro_dir.glob("chunks/*/l2|*|*.parquet"):
        m = re.search(r"\|(\d{8})(?:_\d{6})?\.parquet$", p.name)
        if m:
            dates.add(m.group(1))
    if not dates:
        raise SystemExit("No l2 parquet files found in micro-dir.")
    return sorted(dates)[-1]


def safe_corr(x: pd.Series, y: pd.Series) -> float:
    idx = x.notna() & y.notna()
    if idx.sum() < 20:
        return float("nan")
    return float(x[idx].corr(y[idx]))


def _pairs_for_day(micro_dir: Path, day: str) -> List[str]:
    pairs = set()
    for p in micro_dir.glob(f"l2|*|{day}.parquet"):
        pairs.add(p.name.split("|")[1])
    for p in micro_dir.glob(f"chunks/{day}/l2|*|{day}_*.parquet"):
        pairs.add(p.name.split("|")[1])
    return sorted(pairs)


def _collect_pair_files(micro_dir: Path, day: str, pair: str, kind: str) -> List[Path]:
    out = []
    out.extend(sorted(micro_dir.glob(f"{kind}|{pair}|{day}.parquet")))
    out.extend(sorted(micro_dir.glob(f"chunks/{day}/{kind}|{pair}|{day}_*.parquet")))
    return out


def main() -> None:
    args = parse_args()
    micro_dir = Path(args.micro_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    day = args.date or latest_date(micro_dir)
    pairs = _pairs_for_day(micro_dir, day)
    if not pairs:
        raise SystemExit(f"No l2 files for date={day}")

    summary_rows = []
    q_rows = []

    for pair in pairs:
        l2_files = _collect_pair_files(micro_dir, day, pair, "l2")
        tr_files = _collect_pair_files(micro_dir, day, pair, "trades")
        if not l2_files or not tr_files:
            continue

        l2 = pd.concat([pd.read_parquet(p) for p in l2_files], ignore_index=True).sort_values("timestamp_utc")
        tr = pd.concat([pd.read_parquet(p) for p in tr_files], ignore_index=True).sort_values("time")
        if len(l2) < 50:
            continue

        l2["sec"] = (l2["timestamp_utc"] // 1000).astype("int64")
        l2 = l2.drop_duplicates(subset=["sec"], keep="last")
        l2["ret_1s_bps"] = (l2["mid_px"].shift(-1) / l2["mid_px"] - 1.0) * 1e4
        l2["ret_5s_bps"] = (l2["mid_px"].shift(-5) / l2["mid_px"] - 1.0) * 1e4
        l2["ret_10s_bps"] = (l2["mid_px"].shift(-10) / l2["mid_px"] - 1.0) * 1e4

        # Trade flow imbalance per second
        tr["sec"] = (tr["time"] // 1000).astype("int64")
        tr["notional"] = tr["px"] * tr["sz"]
        tr["sign"] = tr["side"].map({"B": 1.0, "A": -1.0}).fillna(0.0)
        agg = tr.groupby("sec", as_index=False).agg(
            flow_notional=("notional", "sum"),
            signed_notional=("notional", lambda s: float((s * tr.loc[s.index, "sign"]).sum())),
            n_trades=("tid", "count"),
        )
        agg["flow_imb"] = agg["signed_notional"] / agg["flow_notional"].replace(0, np.nan)

        m = l2.merge(agg[["sec", "flow_imb", "n_trades"]], on="sec", how="left")
        m["flow_imb"] = m["flow_imb"].fillna(0.0)
        m["n_trades"] = m["n_trades"].fillna(0).astype(int)

        summary_rows.append(
            {
                "pair": pair,
                "n_snapshots": int(len(m)),
                "n_trade_rows": int(len(tr)),
                "spread_bps_median": float(m["spread_bps"].median()),
                "spread_bps_p90": float(m["spread_bps"].quantile(0.9)),
                "imb_ic_1s": safe_corr(m["imbalance_topn"], m["ret_1s_bps"]),
                "imb_ic_5s": safe_corr(m["imbalance_topn"], m["ret_5s_bps"]),
                "imb_ic_10s": safe_corr(m["imbalance_topn"], m["ret_10s_bps"]),
                "flow_ic_1s": safe_corr(m["flow_imb"], m["ret_1s_bps"]),
                "flow_ic_5s": safe_corr(m["flow_imb"], m["ret_5s_bps"]),
                "flow_ic_10s": safe_corr(m["flow_imb"], m["ret_10s_bps"]),
                "imbalance_std": float(m["imbalance_topn"].std()),
                "flow_imb_std": float(m["flow_imb"].std()),
            }
        )

        # Quantile markouts (imbalance + flow)
        for signal_col in ["imbalance_topn", "flow_imb"]:
            valid = m[[signal_col, "ret_1s_bps", "ret_5s_bps"]].dropna()
            if len(valid) < 80:
                continue
            q = pd.qcut(valid[signal_col], q=5, labels=False, duplicates="drop")
            grp = (
                valid.assign(bucket=q.astype(int))
                .groupby("bucket", as_index=False)
                .agg(mean_1s_bps=("ret_1s_bps", "mean"), mean_5s_bps=("ret_5s_bps", "mean"), n=("ret_1s_bps", "size"))
            )
            grp["pair"] = pair
            grp["signal"] = signal_col
            q_rows.append(grp)

    summary = pd.DataFrame(summary_rows).sort_values("pair").reset_index(drop=True)
    quant = pd.concat(q_rows, ignore_index=True) if q_rows else pd.DataFrame()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    summary_path = out_dir / f"{stamp}_micro_summary.csv"
    quant_path = out_dir / f"{stamp}_micro_quantiles.csv"
    summary.to_csv(summary_path, index=False)
    quant.to_csv(quant_path, index=False)

    print("=== Micro EDA Summary ===")
    print(summary.to_string(index=False))
    print("\nArtifacts:")
    print(summary_path)
    print(quant_path)


if __name__ == "__main__":
    main()
