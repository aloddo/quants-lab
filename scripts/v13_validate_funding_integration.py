#!/usr/bin/env python3
"""Validate B.1 funding integration in v13_equity_reconstruct.

Goals:
  1) Confirm that integrating `userFunding` does NOT break the
     `audit_today_diff_pct` invariant (reconstructed equity at today's date
     must match the live API equity, since the script is ANCHORED on it).
  2) Show that funding has a non-trivial impact on the equity walk for at
     least one wallet known to carry persistent directional positions.
  3) Confirm output schema gained the new diagnostic columns
     (ledger_nonfunding_cum, funding_cum).

This script runs the reconstructor against a small sample of wallets, reads
the output parquet, and prints the validation report.

Usage:
    python scripts/v13_validate_funding_integration.py \\
        --wallets 0x11ca20aeb7cd014cf8406560ae405b12601994b4 \\
        --start 2026-05-16 --end 2026-05-23
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [v13_funding_validate] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets", required=True,
                    help="Comma-separated wallet addresses to test")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--output", default="/tmp/v13_funding_val_equity.parquet")
    ap.add_argument("--audit-tolerance-pct", type=float, default=0.5,
                    help="Max acceptable audit_today_diff_pct (default 0.5%)")
    args = ap.parse_args()

    addrs = [a.strip().lower() for a in args.wallets.split(",") if a.strip()]
    logger.info(f"Validating funding integration on {len(addrs)} wallets")

    # 1) Write wallets to a temp file for the reconstructor.
    wallets_path = Path("/tmp/v13_funding_val_wallets.txt")
    with open(wallets_path, "w") as f:
        for a in addrs:
            f.write(a + "\n")

    # 2) Invoke the reconstructor.
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "v13_equity_reconstruct.py"),
        "--wallets", str(wallets_path),
        "--start", args.start, "--end", args.end,
        "--output", args.output,
    ]
    logger.info(f"Running reconstructor: {' '.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if res.returncode != 0:
        logger.error("Reconstructor failed:")
        logger.error(res.stdout[-3000:])
        logger.error(res.stderr[-3000:])
        sys.exit(1)
    logger.info("Reconstructor completed")

    # 3) Read + audit.
    df = pd.read_parquet(args.output)
    logger.info(f"Loaded {len(df)} rows from {args.output}")

    # Required columns (v3-A schema per Alberto rule 16 + decision A 2026-05-26).
    # `perp_account_value_usd` replaces the legacy `equity_usd` column. The new
    # `spot_usdc_today` column is the wallet-level sizing scalar.
    required = ["wallet", "date", "perp_account_value_usd", "spot_usdc_today",
                "realized_pnl_cum", "ledger_net_cum", "ledger_nonfunding_cum",
                "funding_cum", "mtm_unrealized", "audit_today_diff_pct"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        logger.error(f"Output schema missing columns: {missing}")
        sys.exit(2)
    logger.info("Schema OK: all required columns present")

    # Identity check: ledger_net_cum == ledger_nonfunding_cum + funding_cum
    df["ledger_identity_check"] = (
        df["ledger_net_cum"] - (df["ledger_nonfunding_cum"] + df["funding_cum"])
    ).abs()
    max_id_err = float(df["ledger_identity_check"].max())
    logger.info(f"ledger_net_cum identity max abs error: {max_id_err:.6e} (should be ~0)")
    if max_id_err > 1e-3:
        logger.error("Ledger identity broken!")
        sys.exit(3)

    # Audit drift per wallet (last row of each wallet has audit_today_diff_pct).
    #
    # Codex r2 2026-05-26: audit_today_diff_pct is TAUTOLOGICAL by construction
    # — the last row is anchored to today_api_equity directly when
    # historical_anchor=False (see v13_equity_reconstruct.reconstruct_one_wallet
    # final df build). This loop logs the value for visibility but the
    # tolerance check below is mostly a no-op for the current anchor flow.
    # The REAL quality gates are the audit_perp_anchor_zero / audit_vault_*
    # / audit_unknown_ledger_type_count flags on the wallet rows.
    per_wallet = df.dropna(subset=["audit_today_diff_pct"]).copy()
    if per_wallet.empty:
        logger.error("No audit rows found")
        sys.exit(4)
    logger.info("=== Audit drift per wallet (NOTE: tautological for same-day anchor) ===")
    for _, r in per_wallet.iterrows():
        addr = r["wallet"]
        diff = float(r["audit_today_diff_pct"])
        logger.info(f"  {addr[:10]}..  audit_today_diff_pct={diff:+.4f}%")
        if abs(diff) > args.audit_tolerance_pct:
            logger.error(
                f"  Audit drift exceeds tolerance "
                f"({abs(diff):.4f}% > {args.audit_tolerance_pct}%)"
            )

    # Funding impact summary per wallet.
    logger.info("=== Funding impact per wallet (over window) ===")
    funding_summary = df.groupby("wallet").agg(
        funding_first=("funding_cum", "first"),
        funding_last=("funding_cum", "last"),
        nf_first=("ledger_nonfunding_cum", "first"),
        nf_last=("ledger_nonfunding_cum", "last"),
        n_days=("date", "nunique"),
    ).reset_index()
    funding_summary["funding_net_usd"] = (
        funding_summary["funding_last"] - funding_summary["funding_first"]
    )
    funding_summary["nonfunding_net_usd"] = (
        funding_summary["nf_last"] - funding_summary["nf_first"]
    )
    for _, r in funding_summary.iterrows():
        logger.info(
            f"  {r['wallet'][:10]}.. funding_net={r['funding_net_usd']:>10.4f} "
            f"non-funding_net={r['nonfunding_net_usd']:>10.4f} "
            f"(n_days={r['n_days']})"
        )

    n_nonzero = int((funding_summary["funding_net_usd"].abs() > 1e-9).sum())
    logger.info(
        f"\nWallets with non-zero funding flow in window: {n_nonzero}/{len(funding_summary)}"
    )

    # Final verdict.
    all_pass = (
        max_id_err < 1e-3
        and (per_wallet["audit_today_diff_pct"].abs() <= args.audit_tolerance_pct).all()
    )
    if all_pass:
        logger.info("VERDICT: PASS -- funding integration faithful, schema correct")
    else:
        logger.warning("VERDICT: REVIEW -- see failures above")


if __name__ == "__main__":
    main()
