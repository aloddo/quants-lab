#!/usr/bin/env python3
"""Build a low-overlap Gate-2 roster from accepted leaders.

Uses local v27 entries to penalize wallets that fire the same coin/side/day
signals. No Hyperliquid REST calls.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
V27 = REPO / "app" / "data" / "research" / "v27"
V28 = REPO / "app" / "data" / "research" / "v28"
MS_DAY = 86_400_000


def cosine(a: dict, b: dict) -> float:
    if not a or not b:
        return 0.0
    if len(a) > len(b):
        a, b = b, a
    dot = sum(v * b.get(k, 0.0) for k, v in a.items())
    na = sum(v * v for v in a.values()) ** 0.5
    nb = sum(v * v for v in b.values()) ** 0.5
    if na == 0 or nb == 0:
        return 0.0
    return float(dot / (na * nb))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--screen", default=str(V28 / "gate2_leader_screen.csv"))
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--max-overlap", type=float, default=0.35)
    ap.add_argument("--candidate-status", default="accept")
    ap.add_argument("--out", default=str(V28 / "gate2_roster_top10.csv"))
    args = ap.parse_args()

    screen = pd.read_csv(args.screen)
    cand = (
        screen[screen["gate2_status"].eq(args.candidate_status)]
        .sort_values("gate2_score", ascending=False)
        .copy()
    )
    wallets = cand["wallet"].tolist()
    if not wallets:
        raise SystemExit("no candidate wallets")

    entries = pd.read_parquet(
        V27 / "entries_full.parquet",
        columns=["wallet", "coin", "side", "ts"],
        filters=[("wallet", "in", wallets)],
    )
    entries["day"] = (entries["ts"] // MS_DAY).astype("int64")
    entries["key"] = (
        entries["day"].astype(str)
        + "|"
        + entries["coin"].astype(str)
        + "|"
        + entries["side"].astype(str)
    )
    vecs = {}
    for w, g in entries.groupby("wallet", sort=False):
        vc = g["key"].value_counts()
        vecs[w] = vc.astype(float).to_dict()

    selected = []
    rows = []
    for r in cand.itertuples(index=False):
        overlaps = [cosine(vecs.get(r.wallet, {}), vecs.get(w, {})) for w in selected]
        max_ov = max(overlaps) if overlaps else 0.0
        avg_ov = float(np.mean(overlaps)) if overlaps else 0.0
        if len(selected) < args.top and max_ov <= args.max_overlap:
            selected.append(r.wallet)
            rows.append(
                {
                    "rank": len(selected),
                    "wallet": r.wallet,
                    "gate2_score": r.gate2_score,
                    "n_journeys": r.n_journeys,
                    "latest_lcb_bps": r.latest_lcb_bps,
                    "net_pnl": r.net_pnl,
                    "win_rate": r.win_rate,
                    "loss_win_avg_hold_ratio": r.loss_win_avg_hold_ratio,
                    "big_loss_rate_20pct": r.big_loss_rate_20pct,
                    "max_overlap_to_prior": max_ov,
                    "avg_overlap_to_prior": avg_ov,
                }
            )
        if len(selected) >= args.top:
            break

    roster = pd.DataFrame(rows)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    roster.to_csv(out, index=False)

    print(f"selected {len(roster)} wallets from {len(cand)} {args.candidate_status} candidates")
    print(f"max_overlap threshold {args.max_overlap}")
    print(f"wrote {out}")
    if not roster.empty:
        print(roster.to_string(index=False))


if __name__ == "__main__":
    main()
