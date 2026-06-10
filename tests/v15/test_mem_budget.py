"""V15 aggregate memory-budget tests.

Proves plan_memory_budget caps worker count from ACTUAL free RAM so a parallel fan-out cannot blow
past physical RAM (the 2026-06-10 OOM that jetsam-killed gbrain-postgres), and ABORTS before any work
when even main + one worker will not fit (codex review: no 'run serial and hope the guard fires').
free_gb is injected so the test needs no real RAM.

Run: /Users/hermes/miniforge3/envs/quants-lab/bin/python -m pytest tests/v15/test_mem_budget.py -q
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
from _streaming_io import plan_memory_budget, MemoryBudgetError  # noqa: E402

# Common knobs used across cases.
HR = 6.0          # headroom
PW = 2.0          # per_worker_gb (margined -> 2.5)
MR = 1.5          # main_reserve_gb
MARGIN = 1.25
WORKER_PLANNED = PW * MARGIN  # 2.5


def _b(requested, free, **kw):
    return plan_memory_budget(requested_procs=requested, per_worker_gb=PW, headroom_gb=HR,
                              free_gb=free, main_reserve_gb=MR, worker_margin=MARGIN, **kw)


def test_ample_budget_honors_request():
    # free 30 - 6 hr - 1.5 main = 22.5 usable; floor(22.5/2.5)=9 >= requested 8 -> uncapped.
    b = _b(8, 30.0)
    assert b.procs == 8


def test_intermediate_budget_partial_cap():
    # free 16 - 6 - 1.5 = 8.5 usable; floor(8.5/2.5)=3 -> capped below requested 8.
    b = _b(8, 16.0)
    assert b.procs == 3


def test_invariant_holds_when_capped():
    # The enforced invariant: procs*worker_planned + main_reserve + headroom <= free.
    b = _b(8, 16.0)
    assert b.procs * b.worker_planned_gb + b.main_reserve_gb + b.headroom_gb <= b.free_gb + 1e-9


def test_infeasible_aborts_by_default():
    # free 8 - 6 - 1.5 = 0.5 usable < one worker 2.5 -> MUST abort before any work.
    with pytest.raises(MemoryBudgetError):
        _b(8, 8.0)


def test_infeasible_negative_budget_aborts():
    # the exact live-box case: free 3.2 - 6 headroom < 0 -> abort, never run.
    with pytest.raises(MemoryBudgetError):
        _b(8, 3.2)


def test_main_soft_never_above_grantable():
    # diagnostic tripwire must never exceed (free - headroom); huge cap still clamps to grantable.
    b = _b(2, 10.0, main_soft_cap=100.0)
    assert b.main_soft_gb <= b.free_gb - b.headroom_gb + 1e-9


def test_main_soft_capped_when_ram_huge():
    # plenty of RAM -> main guard capped at main_soft_cap, not the full grantable.
    b = _b(4, 128.0, main_soft_cap=12.0)
    assert b.main_soft_gb == 12.0


def test_worker_guard_tracks_footprint():
    # worker tripwire = per_worker * margin, far below the old loose 12GB.
    b = _b(8, 30.0)
    assert b.worker_soft_gb == pytest.approx(2.5)


def test_requested_is_ceiling_not_floor():
    # ample RAM but small request -> respect the smaller request.
    b = _b(2, 64.0)
    assert b.procs == 2


def test_requested_procs_zero_raises():
    with pytest.raises(ValueError):
        _b(0, 64.0)


def test_invalid_per_worker_raises():
    with pytest.raises(ValueError):
        plan_memory_budget(requested_procs=4, per_worker_gb=0.0, free_gb=64.0)


def test_invalid_worker_margin_raises():
    with pytest.raises(ValueError):
        plan_memory_budget(requested_procs=4, per_worker_gb=2.0, free_gb=64.0, worker_margin=0.0)


def test_negative_headroom_raises():
    with pytest.raises(ValueError):
        plan_memory_budget(requested_procs=4, per_worker_gb=2.0, free_gb=64.0, headroom_gb=-1.0)


def test_single_process_model_main_reserve_zero():
    # m01 pattern: process IS the worker (main_reserve=0). free 10 - 6 = 4 usable; floor(4/2.5)=1.
    b = plan_memory_budget(requested_procs=1, per_worker_gb=PW, headroom_gb=HR, free_gb=10.0,
                           main_reserve_gb=0.0, worker_margin=MARGIN)
    assert b.procs == 1
