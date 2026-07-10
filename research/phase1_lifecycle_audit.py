#!/usr/bin/env python3
"""Audit equity-independent Hyperliquid perp fill lifecycle semantics.

The audit works wallet-by-wallet to remain memory bounded. It verifies the
ordering used by M1/M2 (time, tid), startPosition continuity, and direction
labels without requiring reconstructed leader equity.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
WALLET_DIR = ROOT / "app/data/hl_s3_fills_v2_by_wallet"
COLS = ["coin", "side", "size", "time", "tid", "startPosition", "dir"]


def _is_perp(coin: str) -> bool:
    return not (coin.startswith(("@", "#")) or "/" in coin or coin == "USDC")


def _close(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    return np.isclose(a, b, rtol=1e-9, atol=1e-9, equal_nan=False)


def _direction_valid(direction: str, start: float, end: float) -> bool | None:
    eps = 1e-9
    if direction == "Open Long":
        return start >= -eps and end > start + eps
    if direction == "Open Short":
        return start <= eps and end < start - eps
    if direction == "Close Long":
        return start > eps and end >= -eps and end < start - eps
    if direction == "Close Short":
        return start < -eps and end <= eps and end > start + eps
    if direction == "Long > Short":
        return start > eps and end < -eps
    if direction == "Short > Long":
        return start < -eps and end > eps
    if direction.startswith(("Liquidated ", "Backstop ", "Partial ")) or direction == "Auto-Deleveraging":
        return None
    return None


def select_wallet_files(limit: int, largest: int) -> list[Path]:
    files = list(WALLET_DIR.glob("*.parquet"))
    largest_files = sorted(files, key=lambda p: (p.stat().st_size, p.name), reverse=True)[:largest]
    selected = {p for p in largest_files}
    remaining = sorted(
        (p for p in files if p not in selected),
        key=lambda p: hashlib.sha256(p.name.encode()).digest(),
    )
    selected.update(remaining[: max(0, limit - len(selected))])
    return sorted(selected)


def audit_files(files: list[Path]) -> dict:
    totals = Counter()
    direction_counts = Counter()
    direction_bad = Counter()
    worst_wallets: list[dict] = []

    for path in files:
        df = pd.read_parquet(path, columns=COLS)
        df = df[df["coin"].astype(str).map(_is_perp)].copy()
        if df.empty:
            totals["wallets_without_perps"] += 1
            continue
        df["_row"] = np.arange(len(df), dtype="int64")
        for c in ("size", "startPosition"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["tid"] = pd.to_numeric(df["tid"], errors="coerce").fillna(0).astype("int64")
        df["signed_size"] = np.where(df["side"].eq("B"), df["size"], -df["size"])
        df["end_position"] = df["startPosition"] + df["signed_size"]

        ordered = df.sort_values(["coin", "time", "tid", "_row"], kind="mergesort")
        grouped = ordered.groupby("coin", sort=False)
        prev_end = grouped["end_position"].shift()
        prev_time = grouped["time"].shift()
        comparable = prev_end.notna()
        mismatch = comparable & ~_close(
            ordered["startPosition"].to_numpy(float), prev_end.fillna(0).to_numpy(float)
        )
        same_ms = comparable & ordered["time"].eq(prev_time)
        cross_ms = comparable & ~same_ms

        n_bad = int(mismatch.sum())
        totals.update(
            wallets=1,
            fills=len(ordered),
            tid_zero=int(ordered["tid"].eq(0).sum()),
            comparable_transitions=int(comparable.sum()),
            continuity_mismatches=n_bad,
            same_ms_transitions=int(same_ms.sum()),
            same_ms_mismatches=int((mismatch & same_ms).sum()),
            cross_ms_transitions=int(cross_ms.sum()),
            cross_ms_mismatches=int((mismatch & cross_ms).sum()),
            multi_fill_same_ms_rows=int(ordered.duplicated(["coin", "time"], keep=False).sum()),
        )
        if n_bad:
            worst_wallets.append({"wallet": path.stem, "mismatches": n_bad, "fills": len(ordered)})

        for row in ordered[["dir", "startPosition", "end_position"]].itertuples(index=False):
            direction = str(row.dir or "")
            direction_counts[direction] += 1
            ok = _direction_valid(direction, float(row.startPosition), float(row.end_position))
            if ok is False:
                direction_bad[direction] += 1

    worst_wallets.sort(key=lambda x: (x["mismatches"], x["fills"]), reverse=True)
    out = dict(totals)
    for scope in ("same_ms", "cross_ms"):
        denom = out.get(f"{scope}_transitions", 0)
        out[f"{scope}_mismatch_rate"] = out.get(f"{scope}_mismatches", 0) / denom if denom else 0.0
    denom = out.get("comparable_transitions", 0)
    out["continuity_mismatch_rate"] = out.get("continuity_mismatches", 0) / denom if denom else 0.0
    return {
        "summary": out,
        "direction_counts": dict(direction_counts.most_common()),
        "direction_mismatches": dict(direction_bad.most_common()),
        "worst_wallets": worst_wallets[:25],
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--wallet-limit", type=int, default=500)
    ap.add_argument("--largest-wallets", type=int, default=100)
    ap.add_argument("--output", type=Path)
    args = ap.parse_args()
    files = select_wallet_files(args.wallet_limit, min(args.largest_wallets, args.wallet_limit))
    report = {
        "selection": {"wallet_files": len(files), "largest_wallets": min(args.largest_wallets, len(files))},
        **audit_files(files),
    }
    rendered = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered)
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
