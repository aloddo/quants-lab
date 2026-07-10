#!/usr/bin/env python3
"""Validate a completed V15 M1 series/audit pair and fail on hard invariants."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def validate(series: Path, audit: Path, wallets_file: Path | None = None) -> dict:
    con = duckdb.connect()
    series_path = str(series).replace("'", "''")
    audit_path = str(audit).replace("'", "''")
    series_sql = f"read_parquet('{series_path}')"
    audit_sql = f"read_parquet('{audit_path}')"

    s = con.execute(
        f"""
        SELECT count(*) AS n_rows, count(DISTINCT wallet) AS wallets,
               count(*) - count(DISTINCT (wallet, date)) duplicate_wallet_days,
               sum((NOT recon_incomplete AND NOT isfinite(equity_usd))::INT) nonfinite_complete_equity,
               sum((gross_position_notional_usd + 1e-8 < abs(position_value_usd))::INT) gross_lt_abs_net,
               sum(recon_incomplete::INT) incomplete_rows,
               min(date) first_date, max(date) last_date
        FROM {series_sql}
        """
    ).fetchdf().iloc[0].to_dict()

    a = con.execute(
        f"""
        WITH x AS (
          SELECT *,
            coalesce(array_length(unknown_ledger_types), 0) > 0
              OR (isfinite(median_inter_anchor_drift_pct) AND median_inter_anchor_drift_pct > 0.10)
              OR (isfinite(max_inter_anchor_drift_pct) AND max_inter_anchor_drift_pct > 0.50)
              OR (isfinite(frac_incomplete_rows) AND frac_incomplete_rows > 0.10)
              OR n_inter_anchor_checks < 2
              AS expected_quarantine
          FROM {audit_sql}
        )
        SELECT count(*) AS n_rows, count(DISTINCT wallet) AS wallets,
               count(*) - count(DISTINCT wallet) duplicate_wallets,
               sum(quarantined::INT) quarantined_wallets,
               sum((quarantined != expected_quarantine)::INT) quarantine_rule_mismatches,
               sum((n_incomplete_rows > 0)::INT) wallets_with_incomplete_rows,
               sum((n_inter_anchor_checks < 2)::INT) insufficient_anchor_validation,
               quantile_cont(median_inter_anchor_drift_pct, [0.5, 0.9, 0.99]) median_drift_quantiles,
               quantile_cont(max_inter_anchor_drift_pct, [0.5, 0.9, 0.99]) max_drift_quantiles
        FROM x
        """
    ).fetchdf().iloc[0].to_dict()

    expected_wallets = None
    missing_from_audit = None
    if wallets_file is not None:
        wallets = {
            line.strip().lower()
            for line in wallets_file.read_text().splitlines()
            if line.strip() and not line.startswith("#")
        }
        expected_wallets = len(wallets)
        observed = {row[0].lower() for row in con.execute(f"SELECT wallet FROM {audit_sql}").fetchall()}
        missing_from_audit = len(wallets - observed)
    con.close()

    failures = {
        "duplicate_wallet_days": int(s["duplicate_wallet_days"]),
        "nonfinite_complete_equity": int(s["nonfinite_complete_equity"]),
        "gross_lt_abs_net": int(s["gross_lt_abs_net"]),
        "duplicate_audit_wallets": int(a["duplicate_wallets"]),
        "quarantine_rule_mismatches": int(a["quarantine_rule_mismatches"]),
    }
    hard_fail = any(failures.values())
    return {
        "hard_fail": hard_fail,
        "failures": failures,
        "series": s,
        "audit": a,
        "expected_wallets": expected_wallets,
        # Missing audit wallets are reported, not a hard failure: M1 deliberately
        # emits no audit row for no-anchor/error wallets and logs their reason.
        "missing_from_audit": missing_from_audit,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--series", type=Path, required=True)
    ap.add_argument("--audit", type=Path, required=True)
    ap.add_argument("--wallets-file", type=Path)
    ap.add_argument("--output", type=Path)
    args = ap.parse_args()
    report = validate(args.series, args.audit, args.wallets_file)
    rendered = json.dumps(report, indent=2, default=str) + "\n"
    if args.output:
        args.output.write_text(rendered)
    else:
        print(rendered, end="")
    raise SystemExit(1 if report["hard_fail"] else 0)


if __name__ == "__main__":
    main()
