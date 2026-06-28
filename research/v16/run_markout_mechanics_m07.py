"""Mechanics pass (probation): markout cohort + per-seat DD stop (follower_trail). Prereg
copy-rebuild/2026-06-28-mechanics-pass-prereg. Loss-stop pre-registered at -8% (follower_trail=0.08)."""
import sys; from pathlib import Path
sys.path.insert(0, "research/v15")
from v15_m07_engine import run_shortlist
WS = Path("app/data/v15/weekly_spike")
run_shortlist(
    actions_path=Path("app/data/v15/m02_actions.parquet"),
    shortlist_path=WS/"markout_cohort_shortlist.parquet",
    folds_path=WS/"m03_folds.parquet",
    out_dir=WS/"m07_markout_mech_t08",
    band="base", start_equity=10_000.0, require_cache=True, window="test",
    slip_calib_path="app/data/v15/rebuild_chain/slippage_calib_v11.json",
    copy_latency_ms=1860,
    follower_trail=0.08,   # per-seat drawdown stop (pre-registered -8%)
)
print("MARKOUT MECHANICS (trail08) M07 DONE")
