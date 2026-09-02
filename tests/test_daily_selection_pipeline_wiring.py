from pathlib import Path


SCRIPT = (Path(__file__).parents[1] / "scripts" / "daily_selection_pipeline.sh").read_text()


def test_pre_stateful_migration_falls_back_to_incremental_m2():
    assert "running canonical incremental fallback" in SCRIPT
    assert "$PY data_pipeline/m02_journeys_daily.py\n" in SCRIPT


def test_both_m2_paths_use_memory_backstop():
    assert "--label m02-stateful" in SCRIPT
    assert "--label m02-incremental" in SCRIPT
