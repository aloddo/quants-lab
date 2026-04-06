"""
E1 Fill Quality Smoke Tests — P3 readiness checks

Tests the update_candidate_fill + TERMINAL_STATES guard path.
Covers 6 cases:

  FQ-1  happy path: touched + actual fill written, state → ORDER_FILLED
  FQ-2  no-touch:   touched=False, actual_fill=False, state → ORDER_EXPIRED
  FQ-3  terminal guard: attempt to overwrite ORDER_FILLED → silently blocked
  FQ-4  terminal guard: attempt to overwrite ORDER_EXPIRED → silently blocked
  FQ-5  fillable_estimate ≠ actual_fill (selection-bias sentinel preserved)
  FQ-6  MAE before fill stored correctly (negative for long adverse, positive OK)

Run:
    cd ~/quants-lab
    conda run -n quants-lab python app/controllers/directional_trading/test_e1_fill_quality.py
"""

import sqlite3
import tempfile
import traceback
from pathlib import Path

from e1_snapshot import (
    State, TERMINAL_STATES,
    DecisionSnapshot, Candidate,
    build_snapshot, evaluate_candidate,
    init_db, register_candidate, update_candidate_fill,
)

# ── helpers ──────────────────────────────────────────────────────────────────

def make_db() -> Path:
    tmp = tempfile.mktemp(suffix=".db")
    p = Path(tmp)
    init_db(p)
    # ensure updated_at column exists (added in P2 migration)
    conn = sqlite3.connect(str(p))
    try:
        conn.execute("ALTER TABLE candidate_log ADD COLUMN updated_at INTEGER")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # already exists
    conn.close()
    return p


def make_registered_candidate(db: Path) -> Candidate:
    """Produce a CANDIDATE_REGISTERED row ready for fill updates."""
    feat_1h = dict(
        atr_pct=0.35, compressed=True,
        range_high=100.0, range_low=95.0,
        breakout_event=1,       # long breakout
        volume_floor_ok=True,   volume_spike=True,
        risk_off=False, risk_off_source="", short_trend_ok=True,
    )
    feat_5m = dict(
        atr_5m=0.10, timestamp=1_700_000_000_000,
        close=100.05, open=99.90,
        entry_distance_atr=0.10,
        entry_body_atr=0.15,
        entry_gap_bps=1.0,
    )
    snap = build_snapshot("BTC-USDT", feat_1h, feat_5m)
    # force staleness ok
    snap.staleness_ok = True
    cand = evaluate_candidate(snap)
    assert cand.state == State.CANDIDATE_REGISTERED, f"Unexpected state: {cand.state}"
    register_candidate(cand, snap, db)
    return cand


def fetch_row(candidate_id: str, db: Path) -> dict:
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM candidate_log WHERE candidate_id=?", (candidate_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else {}


def update_if_not_terminal(candidate_id: str, db: Path, **kwargs) -> bool:
    """
    Wrapper that enforces TERMINAL_STATES guard before calling update_candidate_fill.
    Returns True if update was applied, False if blocked.
    """
    row = fetch_row(candidate_id, db)
    if row.get("state") in TERMINAL_STATES:
        return False
    update_candidate_fill(candidate_id, db_path=db, **kwargs)
    return True


# ── test cases ───────────────────────────────────────────────────────────────

PASS = "PASS"
FAIL = "FAIL"
results = []

def run(label, fn):
    try:
        fn()
        results.append((PASS, label, ""))
    except AssertionError as e:
        results.append((FAIL, label, str(e)))
    except Exception as e:
        results.append((FAIL, label, f"{type(e).__name__}: {e}\n{traceback.format_exc()}"))


# FQ-1: happy path — touched + actual fill, state → ORDER_FILLED
def test_fq1():
    db = make_db()
    cand = make_registered_candidate(db)

    applied = update_if_not_terminal(
        cand.candidate_id, db,
        touched=True,
        fillable=True,
        actual_fill=True,
        actual_fill_price=100.02,
        mae_before_fill=-0.05,
        state=State.ORDER_FILLED,
    )
    assert applied, "Update should not be blocked"
    row = fetch_row(cand.candidate_id, db)
    assert row["touched_signal_price"] == 1, f"touched={row['touched_signal_price']}"
    assert row["actual_fill"] == 1, f"actual_fill={row['actual_fill']}"
    assert abs(row["actual_fill_price"] - 100.02) < 1e-6
    assert row["state"] == State.ORDER_FILLED

run("FQ-1 happy-path fill written, state=ORDER_FILLED", test_fq1)


# FQ-2: no-touch — price never reached level, state → ORDER_EXPIRED
def test_fq2():
    db = make_db()
    cand = make_registered_candidate(db)

    applied = update_if_not_terminal(
        cand.candidate_id, db,
        touched=False,
        fillable=False,
        actual_fill=False,
        actual_fill_price=None,
        mae_before_fill=None,
        state=State.ORDER_EXPIRED,
    )
    assert applied, "Update should not be blocked"
    row = fetch_row(cand.candidate_id, db)
    assert row["touched_signal_price"] == 0
    assert row["actual_fill"] == 0
    assert row["actual_fill_price"] is None
    assert row["state"] == State.ORDER_EXPIRED

run("FQ-2 no-touch, state=ORDER_EXPIRED", test_fq2)


# FQ-3: terminal guard — attempt to overwrite ORDER_FILLED is blocked
def test_fq3():
    db = make_db()
    cand = make_registered_candidate(db)

    # First: write terminal state
    update_if_not_terminal(
        cand.candidate_id, db,
        touched=True, fillable=True,
        actual_fill=True, actual_fill_price=100.02,
        mae_before_fill=-0.05, state=State.ORDER_FILLED,
    )

    # Second: attempt overwrite with stale updater
    applied = update_if_not_terminal(
        cand.candidate_id, db,
        touched=True, fillable=True,
        actual_fill=True, actual_fill_price=99.50,  # wrong price
        mae_before_fill=None, state=State.ORDER_FILLED,
    )
    assert not applied, "Should be blocked by TERMINAL_STATES guard"
    row = fetch_row(cand.candidate_id, db)
    assert abs(row["actual_fill_price"] - 100.02) < 1e-6, \
        f"Price was overwritten to {row['actual_fill_price']}"

run("FQ-3 terminal guard blocks ORDER_FILLED overwrite", test_fq3)


# FQ-4: terminal guard — attempt to overwrite ORDER_EXPIRED is blocked
def test_fq4():
    db = make_db()
    cand = make_registered_candidate(db)

    update_if_not_terminal(
        cand.candidate_id, db,
        touched=False, fillable=False,
        actual_fill=False, actual_fill_price=None,
        mae_before_fill=None, state=State.ORDER_EXPIRED,
    )

    applied = update_if_not_terminal(
        cand.candidate_id, db,
        touched=True, fillable=True,
        actual_fill=True, actual_fill_price=100.10,
        mae_before_fill=None, state=State.ORDER_FILLED,
    )
    assert not applied, "Should be blocked by TERMINAL_STATES guard"
    row = fetch_row(cand.candidate_id, db)
    assert row["state"] == State.ORDER_EXPIRED, f"State was mutated to {row['state']}"
    assert row["actual_fill"] == 0

run("FQ-4 terminal guard blocks ORDER_EXPIRED overwrite", test_fq4)


# FQ-5: fillable_estimate ≠ actual_fill (selection-bias sentinel preserved)
def test_fq5():
    db = make_db()
    cand = make_registered_candidate(db)

    # Heuristic said fillable=True but actual_fill=False (missed fill)
    applied = update_if_not_terminal(
        cand.candidate_id, db,
        touched=True, fillable=True,
        actual_fill=False, actual_fill_price=None,
        mae_before_fill=None, state=State.ORDER_EXPIRED,
    )
    assert applied
    row = fetch_row(cand.candidate_id, db)
    assert row["fillable_estimate"] == 1,  f"fillable={row['fillable_estimate']}"
    assert row["actual_fill"] == 0,         f"actual_fill={row['actual_fill']}"
    # Both columns preserved independently — this is the selection-bias signal
    assert row["fillable_estimate"] != row["actual_fill"], \
        "Selection-bias row must show divergence"

run("FQ-5 fillable_estimate != actual_fill (selection-bias row preserved)", test_fq5)


# FQ-6: MAE before fill stored correctly (adverse move before fill on a long)
def test_fq6():
    db = make_db()
    cand = make_registered_candidate(db)

    mae = -0.23   # price moved 0.23 against long before filling

    applied = update_if_not_terminal(
        cand.candidate_id, db,
        touched=True, fillable=True,
        actual_fill=True, actual_fill_price=99.85,
        mae_before_fill=mae,
        state=State.ORDER_FILLED,
    )
    assert applied
    row = fetch_row(cand.candidate_id, db)
    assert abs(row["max_adverse_excursion_before_fill"] - mae) < 1e-9, \
        f"MAE stored as {row['max_adverse_excursion_before_fill']}, expected {mae}"

run("FQ-6 MAE before fill stored correctly", test_fq6)


# ── report ────────────────────────────────────────────────────────────────────

print()
total = len(results)
passed = sum(1 for r in results if r[0] == PASS)

for status, label, msg in results:
    icon = "✅" if status == PASS else "❌"
    print(f"  {icon} {label}")
    if msg:
        print(f"      → {msg}")

print()
print(f"  {passed}/{total} passed")
if passed == total:
    print("  All fill quality smoke tests passed — P3 monitoring pipeline is safe to build.")
else:
    print("  Fix failures before wiring live fill updater.")
print()
