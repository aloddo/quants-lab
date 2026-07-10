#!/usr/bin/env python3
"""Audit the local data used by Gate-2 leader selection.

No network calls. This checks whether the v27 historical inputs are fresh,
internally consistent, and complete enough to justify live-capital promotion.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[2]
V27 = REPO / "app" / "data" / "research" / "v27"
V28 = REPO / "app" / "data" / "research" / "v28"
DOCS = REPO / "docs" / "research"

GRID_START_MS = 1765670400000  # 2025-12-14 00:00 UTC
CUTOFF_MS = 1781222400000      # 2026-06-12 00:00 UTC
MS_DAY = 86_400_000


def iso(ms: float | int | None) -> str:
    if ms is None or pd.isna(ms):
        return ""
    return datetime.fromtimestamp(float(ms) / 1000, timezone.utc).isoformat()


def parquet_rows(path: Path) -> int:
    return pq.ParquetFile(path).metadata.num_rows


def file_row(name: str) -> dict:
    path = V27 / name
    return {
        "file": str(path.relative_to(REPO)),
        "size_mb": path.stat().st_size / 1_048_576,
        "mtime_utc": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(),
        "rows": parquet_rows(path),
    }


def main() -> int:
    DOCS.mkdir(parents=True, exist_ok=True)
    V28.mkdir(parents=True, exist_ok=True)

    files = [file_row(n) for n in ["journeys_full.parquet", "entries_full.parquet", "gates_full.parquet", "r2trips_full.parquet", "lcb_table.parquet"]]

    journeys = pd.read_parquet(
        V27 / "journeys_full.parquet",
        columns=["wallet", "coin", "side", "entry_ts", "exit_ts", "duration_h", "net_realized_pnl", "max_notional", "open_size", "liq_closed", "n_actions"],
    )
    entries = pd.read_parquet(V27 / "entries_full.parquet", columns=["wallet", "coin", "side", "ts"])
    gates = pd.read_parquet(V27 / "gates_full.parquet")
    r2 = pd.read_parquet(V27 / "r2trips_full.parquet", columns=["wallet", "entry_fill_ts", "exit_fill_ts_last", "net_bps", "terminal"])
    lcb = pd.read_parquet(V27 / "lcb_table.parquet")

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    closed = journeys[journeys["exit_ts"].notna()].copy()

    duplicate_keys = ["wallet", "coin", "side", "entry_ts", "exit_ts", "max_notional"]
    journey_dupes = int(closed.duplicated(duplicate_keys).sum())
    entry_dupes = int(entries.duplicated(["wallet", "coin", "side", "ts"]).sum())

    lcb_latest_k = int(lcb["boundary_k"].max()) if len(lcb) else None
    latest_lcb_wallets = int(lcb[lcb["boundary_k"].eq(lcb_latest_k)]["wallet"].nunique()) if lcb_latest_k is not None else 0

    audit = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "grid_start_ms": GRID_START_MS,
        "grid_start_utc": iso(GRID_START_MS),
        "cutoff_ms": CUTOFF_MS,
        "cutoff_utc": iso(CUTOFF_MS),
        "staleness_days_vs_now": (now_ms - CUTOFF_MS) / MS_DAY,
        "files": files,
        "journeys": {
            "rows": int(len(journeys)),
            "closed_rows": int(len(closed)),
            "wallets": int(journeys["wallet"].nunique()),
            "closed_wallets": int(closed["wallet"].nunique()),
            "entry_min_utc": iso(journeys["entry_ts"].min()),
            "entry_max_utc": iso(journeys["entry_ts"].max()),
            "exit_min_utc": iso(closed["exit_ts"].min()),
            "exit_max_utc": iso(closed["exit_ts"].max()),
            "entries_after_cutoff": int((journeys["entry_ts"] > CUTOFF_MS).sum()),
            "exits_after_cutoff": int((closed["exit_ts"] > CUTOFF_MS).sum()),
            "entries_before_grid": int((journeys["entry_ts"] < GRID_START_MS).sum()),
            "negative_duration_rows": int((closed["duration_h"] < 0).sum()),
            "nonpositive_notional_rows": int((journeys["max_notional"] <= 0).sum()),
            "duplicate_closed_key_rows": journey_dupes,
            "null_net_realized_closed": int(closed["net_realized_pnl"].isna().sum()),
        },
        "entries": {
            "rows": int(len(entries)),
            "wallets": int(entries["wallet"].nunique()),
            "ts_min_utc": iso(entries["ts"].min()),
            "ts_max_utc": iso(entries["ts"].max()),
            "after_initial_boundary_only_expected": "entries_full is only initial-boundary clustering input, not full-window entries",
            "duplicate_key_rows": entry_dupes,
        },
        "gates": {
            "rows": int(len(gates)),
            "wallets": int(gates["wallet"].nunique()),
            "eligible": int(gates["eligible"].sum()) if "eligible" in gates else None,
            "duplicate_wallet_rows": int(gates.duplicated(["wallet"]).sum()) if "wallet" in gates else None,
        },
        "r2trips": {
            "rows": int(len(r2)),
            "nonterminal_rows": int((~r2["terminal"].astype(bool)).sum()),
            "wallets": int(r2["wallet"].nunique()),
            "entry_min_utc": iso(r2["entry_fill_ts"].min()),
            "entry_max_utc": iso(r2["entry_fill_ts"].max()),
            "exit_min_utc": iso(r2.loc[~r2["terminal"].astype(bool), "exit_fill_ts_last"].min()),
            "exit_max_utc": iso(r2.loc[~r2["terminal"].astype(bool), "exit_fill_ts_last"].max()),
            "terminal_rows": int(r2["terminal"].astype(bool).sum()),
        },
        "lcb": {
            "rows": int(len(lcb)),
            "wallets": int(lcb["wallet"].nunique()) if "wallet" in lcb else 0,
            "boundary_min": int(lcb["boundary_k"].min()) if len(lcb) else None,
            "boundary_max": lcb_latest_k,
            "latest_boundary_wallets": latest_lcb_wallets,
            "nan_lcb_rows": int(lcb["lcb_bps"].isna().sum()) if "lcb_bps" in lcb else None,
        },
    }

    hard_notes = []
    if audit["staleness_days_vs_now"] > 7:
        hard_notes.append(f"historical window is stale by {audit['staleness_days_vs_now']:.1f} days")
    if audit["journeys"]["exits_after_cutoff"] > 0 or audit["journeys"]["entries_after_cutoff"] > 0:
        hard_notes.append("journey rows exceed declared cutoff")
    if audit["journeys"]["duplicate_closed_key_rows"] > 0:
        hard_notes.append("duplicate closed journey keys present")
    if audit["journeys"]["negative_duration_rows"] > 0:
        hard_notes.append("negative journey durations present")
    if latest_lcb_wallets == 0:
        hard_notes.append("latest LCB boundary has no wallets")

    audit["verdict"] = "not_live_sufficient" if hard_notes else "internally_ok_but_still_needs_recent_refresh"
    audit["hard_notes"] = hard_notes

    out_json = V28 / "gate2_data_audit.json"
    out_json.write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n")

    report = DOCS / f"gate2_data_audit_{datetime.now(timezone.utc):%Y%m%d}.md"
    lines = [
        "# Gate-2 Data Audit",
        "",
        f"Generated: {audit['generated_utc']}",
        "",
        f"Verdict: **{audit['verdict']}**",
        "",
        "## Critical Notes",
        "",
    ]
    if hard_notes:
        lines.extend([f"- {n}" for n in hard_notes])
    else:
        lines.append("- no hard internal consistency failures found")
    lines.extend(
        [
            "",
            "## Coverage",
            "",
            f"- grid start: {audit['grid_start_utc']}",
            f"- declared cutoff: {audit['cutoff_utc']}",
            f"- staleness vs now: {audit['staleness_days_vs_now']:.1f} days",
            f"- closed journeys: {audit['journeys']['closed_rows']:,}",
            f"- journey wallets: {audit['journeys']['wallets']:,}",
            f"- eligible gates: {audit['gates']['eligible']:,}",
            f"- r2 nonterminal trips: {audit['r2trips']['nonterminal_rows']:,}",
            f"- latest LCB boundary: {audit['lcb']['boundary_max']}",
            f"- latest LCB wallets: {audit['lcb']['latest_boundary_wallets']:,}",
            "",
            "## Consistency",
            "",
            f"- duplicate closed journey keys: {audit['journeys']['duplicate_closed_key_rows']:,}",
            f"- duplicate entry keys: {audit['entries']['duplicate_key_rows']:,}",
            f"- negative durations: {audit['journeys']['negative_duration_rows']:,}",
            f"- nonpositive notionals: {audit['journeys']['nonpositive_notional_rows']:,}",
            f"- exits after cutoff: {audit['journeys']['exits_after_cutoff']:,}",
            f"- entries after cutoff: {audit['journeys']['entries_after_cutoff']:,}",
            "",
            f"JSON: `{out_json.relative_to(REPO)}`",
        ]
    )
    report.write_text("\n".join(lines) + "\n")

    print(json.dumps({k: audit[k] for k in ["verdict", "hard_notes", "staleness_days_vs_now"]}, indent=2))
    print(f"wrote {out_json}")
    print(f"wrote {report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
