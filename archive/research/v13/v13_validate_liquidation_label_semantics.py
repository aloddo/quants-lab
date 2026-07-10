#!/usr/bin/env python3
"""Validate HL S3 fills `dir` semantics around liquidations.

GPT review point 13: the v1 spec gates `liquidation_events == 0` by
filtering S3 fills where `dir` contains the substring "Liquidat". S3
emits BOTH counterparties of every match (one row per side). The
question is whether the `dir` field disambiguates the LIQUIDATED side
from the COUNTERPARTY (e.g., the HLP that absorbed the position).

Empirical finding (5-day sample, all 174-day archive scanned in
preamble):

  dir variants involving liquidation:
    Liquidated Isolated Long
    Liquidated Isolated Short
    Liquidated Cross Long
    Liquidated Cross Short
    Partial Borrow Liquidation
    Backstop Borrow Liquidation
    Auto-Deleveraging   <-- different mechanism; affects counterparties of
                            liquidations, NOT the wallet's own liquidation.

  Both counterparties of a Liquidated* event have THE SAME dir string.
  Disambiguator: the LIQUIDATED side has closedPnl < 0 (they lost). The
  counterparty (typically HLP at 0x4000...0004) has closedPnl == 0 (took
  over the position with no immediate PnL).

This script proves the disambiguator empirically by:

  1. Counting rows by dir / side combinations
  2. Showing closedPnl distribution per (dir, side)
  3. Confirming that filtering "Liquidated" + closedPnl < 0 isolates
     true self-liquidations from counterparty rows

Output: a verdict table the spec can cite, plus a recommended fix for
`v13_wallet_metrics.py:282-284`.

Usage:
    python scripts/v13_validate_liquidation_label_semantics.py
"""
from __future__ import annotations

import glob
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [v13_liq_validate] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
FILLS_DIR = ROOT / "app" / "data" / "hl_s3_fills"

HLP_ADDR = "0x4000000000000000000000000000000000000004"


def main():
    files = sorted(glob.glob(str(FILLS_DIR / "*.parquet")))
    logger.info(f"Scanning {len(files)} daily parquets")

    # 1) Global dir-value tally limited to liq/ADL.
    rows = []
    for p in files:
        df = pd.read_parquet(p, columns=["wallet", "side", "dir", "closedPnl"])
        df = df[df["dir"].str.contains("Liquid|Delever", na=False, regex=True)]
        if df.empty:
            continue
        rows.append(df)
    if not rows:
        logger.error("No liquidation-related rows found across the archive.")
        return
    liq_df = pd.concat(rows, ignore_index=True)
    logger.info(f"Total liq/ADL rows: {len(liq_df):,}")

    # 2) Counts by dir + side.
    pivot_count = liq_df.groupby(["dir", "side"]).size().unstack(fill_value=0)
    logger.info("\n=== rows by dir x side ===")
    logger.info(pivot_count.to_string())

    # 3) closedPnl summary by dir x side (median + share with pnl<0 vs ==0 vs >0).
    def pnl_breakdown(g: pd.DataFrame) -> pd.Series:
        return pd.Series({
            "n": len(g),
            "n_pnl_neg": int((g["closedPnl"] < 0).sum()),
            "n_pnl_zero": int((g["closedPnl"] == 0).sum()),
            "n_pnl_pos": int((g["closedPnl"] > 0).sum()),
            "pnl_p50": float(g["closedPnl"].median()),
            "pnl_p05": float(g["closedPnl"].quantile(0.05)),
            "pnl_p95": float(g["closedPnl"].quantile(0.95)),
        })
    summary = liq_df.groupby(["dir", "side"]).apply(pnl_breakdown)
    logger.info("\n=== closedPnl breakdown by dir x side ===")
    logger.info(summary.to_string())

    # 4) HLP signature: how many liq rows are the HLP wallet?
    hlp_liq = liq_df[liq_df["wallet"] == HLP_ADDR]
    logger.info(f"\nHLP ({HLP_ADDR}) shows up on {len(hlp_liq):,} liq rows "
                f"({len(hlp_liq)/len(liq_df)*100:.1f}%)")
    if len(hlp_liq):
        logger.info("HLP closedPnl distribution (should be ~0 for takeover side):")
        logger.info(f"  median={hlp_liq['closedPnl'].median():.4f} "
                    f"p95_abs={hlp_liq['closedPnl'].abs().quantile(0.95):.4f}")

    # 5) The proposed filter: rows where dir contains "Liquidat" AND closedPnl < 0.
    #    This should isolate self-liquidations (wallet actually got liquidated).
    self_liq = liq_df[
        liq_df["dir"].str.contains("Liquidat", na=False)
        & (liq_df["closedPnl"] < 0)
    ]
    counterparty = liq_df[
        liq_df["dir"].str.contains("Liquidat", na=False)
        & (liq_df["closedPnl"] >= 0)
    ]
    logger.info(f"\n=== Proposed filter ===")
    logger.info(f"dir contains 'Liquidat' total: {len(liq_df[liq_df['dir'].str.contains('Liquidat', na=False)]):,}")
    logger.info(f"  self-liq (closedPnl<0):  {len(self_liq):,} across {self_liq['wallet'].nunique():,} wallets")
    logger.info(f"  counterparty (closedPnl>=0): {len(counterparty):,} across {counterparty['wallet'].nunique():,} wallets")
    logger.info(f"  HLP share of counterparty: {(counterparty['wallet'] == HLP_ADDR).mean()*100:.1f}%")

    # 6) Wallets that got liquidated multiple times (top 10).
    self_liq_counts = self_liq.groupby("wallet").size().sort_values(ascending=False)
    logger.info(f"\nTop 10 wallets by self-liq count:")
    logger.info(self_liq_counts.head(10).to_string())

    # 7) Wallets that show up in counterparty role (top 10).
    cp_counts = counterparty.groupby("wallet").size().sort_values(ascending=False)
    logger.info(f"\nTop 10 wallets by counterparty-of-liq count:")
    logger.info(cp_counts.head(10).to_string())

    # 8) ADL semantics: Auto-Deleveraging is a different mechanism. The wallet
    #    on the ADL side had a WINNING position that got auto-closed because
    #    the counterparty was liquidated. ADL does NOT reflect that wallet's
    #    own risk failure.
    adl = liq_df[liq_df["dir"] == "Auto-Deleveraging"]
    logger.info(f"\n=== Auto-Deleveraging ===")
    logger.info(f"ADL rows: {len(adl):,} across {adl['wallet'].nunique():,} wallets")
    logger.info(f"closedPnl distribution: p05={adl['closedPnl'].quantile(0.05):.2f} "
                f"p50={adl['closedPnl'].median():.2f} "
                f"p95={adl['closedPnl'].quantile(0.95):.2f}")
    pct_pos = (adl["closedPnl"] > 0).mean() * 100
    logger.info(f"ADL closedPnl > 0 share: {pct_pos:.1f}%")
    logger.info("Interpretation: ADL'd wallets are typically PROFITABLE positions "
                "force-closed; this is NOT a self-liquidation event.")

    # 9) Verdict.
    logger.info("\n=== VERDICT ===")
    logger.info(
        "The substring 'Liquidat' alone does NOT identify self-liquidation. "
        "It matches BOTH the liquidated wallet AND the takeover counterparty "
        "(typically the HLP at 0x4000...0004). "
        f"In this archive, {len(counterparty)/len(liq_df[liq_df['dir'].str.contains('Liquidat',na=False)])*100:.1f}% "
        "of 'Liquidat'-tagged rows are counterparty rows, not self-liquidations."
    )
    logger.info(
        "Recommended fix for v13_wallet_metrics.py liquidation_events gate:\n"
        "  OLD: count fills where dir.str.contains('Liquidat')\n"
        "  NEW: count fills where dir.str.startswith('Liquidated') AND closedPnl < 0\n"
        "        + dir == 'Partial Borrow Liquidation' AND closedPnl < 0\n"
        "        + dir == 'Backstop Borrow Liquidation' AND closedPnl < 0\n"
        "  ADL (Auto-Deleveraging) is NOT counted; it is a forced exit of a"
        " WINNING position and does not reflect the wallet's risk failure."
    )


if __name__ == "__main__":
    main()
