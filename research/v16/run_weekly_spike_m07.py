"""Launch the weekly M7 spike engine run (costed). Per copy-rebuild/2026-06-28-weekly-m7-spike-prereg."""
import sys; from pathlib import Path
sys.path.insert(0, "research/v15")
from v15_m07_engine import run_shortlist
WS = Path("app/data/v15/weekly_spike")
run_shortlist(
    actions_path=Path("app/data/v15/m02_actions.parquet"),
    shortlist_path=WS/"weekly_shortlist.parquet",
    folds_path=WS/"m03_folds.parquet",
    out_dir=WS/"m07_test",
    band="base",
    start_equity=10_000.0,
    require_cache=True,
    window="test",
    slip_calib_path="app/data/v15/rebuild_chain/slippage_calib_v11.json",
    copy_latency_ms=1860,   # measured median copy latency
)
print("WEEKLY M7 SPIKE DONE")
