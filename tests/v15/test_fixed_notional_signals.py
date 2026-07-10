import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "research" / "v15"))
import v15_fixed_notional_signals as sig  # noqa: E402


def _fill(ts, side, size, start, tid, price=100.0):
    return {
        "coin": "BTC", "side": side, "size": float(size), "price": float(price),
        "time": ts, "tid": tid, "startPosition": float(start),
        "signed_sz": float(size) if side == "B" else -float(size),
        "fee": 0.0, "builderFee": 0.0, "deployerFee": 0.0,
    }


def test_tracks_adds_and_exits_only_after_close_fraction():
    fills = [
        _fill(1000, "B", 1.0, 0.0, 1),       # open; accumulated $100
        _fill(2000, "B", 1.0, 1.0, 2),       # add; accumulated $200
        _fill(3000, "A", 1.6, 2.0, 3),       # reverse flow $160 = 80%
        _fill(4000, "A", 0.2, 0.4, 4),       # cumulative $180 = 90% -> close
    ]
    rows, audit = sig.derive_copy_lifecycles("0xABC", fills, close_fraction=0.85)
    assert len(rows) == 1
    assert rows[0]["entry_ts"] == 1000
    assert rows[0]["exit_ts"] == 4000
    assert rows[0]["leader_accumulated_notional"] == pytest.approx(200.0)
    assert rows[0]["reverse_fraction"] == pytest.approx(0.9)
    assert rows[0]["lifecycle_valid"] is True
    assert audit["n_valid_closes"] == 1


def test_stream_gap_invalidates_open_copy_lifecycle():
    fills = [
        _fill(1000, "B", 1.0, 0.0, 1),
        _fill(2000, "A", 1.0, 5.0, 2),
    ]
    rows, audit = sig.derive_copy_lifecycles("0xabc", fills)
    assert len(rows) == 1
    assert rows[0]["exit_reason"] == "stream_gap"
    assert rows[0]["lifecycle_valid"] is False
    assert audit["n_invalidated"] == 1


def test_same_ms_causal_chain_preserves_open_add_exit_contract():
    fills = [
        _fill(1000, "A", 2.0, 3.0, 30),
        _fill(1000, "B", 1.0, 0.0, 20),
        _fill(1000, "B", 2.0, 1.0, 10),
        _fill(1000, "A", 1.0, 1.0, 40),
    ]
    rows, _ = sig.derive_copy_lifecycles("0xabc", fills)
    assert len(rows) == 1
    assert rows[0]["entry_ts"] == 1000
    assert rows[0]["exit_ts"] == 1000
    assert rows[0]["n_leader_fills"] == 4
    assert rows[0]["lifecycle_valid"] is True


def test_reverse_on_uncopied_position_never_becomes_entry():
    rows, audit = sig.derive_copy_lifecycles(
        "0xabc", [_fill(1000, "A", 2.0, 1.0, 1)]
    )
    assert rows == []
    assert audit["n_entries"] == 0


def test_full_quantity_reverse_below_dollar_threshold_stays_unresolved():
    # Accumulated at $100, then the price halves. Closing/reversing all leader
    # quantity is only 50% by the runtime's dollar-notional denominator.
    fills = [
        _fill(1000, "B", 1.0, 0.0, 1, price=100.0),
        _fill(2000, "A", 2.0, 1.0, 2, price=50.0),
        _fill(3000, "A", 1.0, -1.0, 3, price=50.0),
    ]
    rows, audit = sig.derive_copy_lifecycles("0xabc", fills, close_fraction=0.85)
    assert len(rows) == 1
    assert rows[0]["exit_reason"] == "leader_side_diverged"
    assert rows[0]["exit_ts"] is None
    assert rows[0]["reverse_fraction"] == pytest.approx(0.5)
    assert rows[0]["leader_side_divergence_ts"] == 2000
    assert rows[0]["lifecycle_valid"] is True
    assert audit["n_leader_side_diverged"] == 1
