"""Decisive codex test: portfolio engine on the TRAIN-KNOWN markout cohort."""
import sys; from pathlib import Path
sys.path.insert(0, "research/v15")
from v15_m07_engine import run_shortlist
WS = Path("app/data/v15/weekly_spike")
run_shortlist(
    actions_path=Path("app/data/v15/m02_actions.parquet"),
    shortlist_path=WS/"markout_cohort_shortlist.parquet",
    folds_path=WS/"m03_folds.parquet",
    out_dir=WS/"m07_markout_cohort",
    band="base", start_equity=10_000.0, require_cache=True, window="test",
    slip_calib_path="app/data/v15/rebuild_chain/slippage_calib_v11.json",
    copy_latency_ms=1860,
)
print("MARKOUT COHORT M07 DONE")
