import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "research" / "v15"))
import v15_equity_independent_actions as ei  # noqa: E402


def _fill(ts, side, size, start, tid):
    return {
        "coin": "BTC", "side": side, "size": float(size), "price": 100.0,
        "time": ts, "tid": tid, "startPosition": float(start),
        "signed_sz": float(size) if side == "B" else -float(size),
        "fee": 0.1, "builderFee": 0.0, "deployerFee": 0.0,
    }


def test_burst_action_uses_position_chain_without_equity():
    fills = [
        _fill(1000, "A", 855, -573, 10),
        _fill(1000, "A", 573, -3011, 20),
        _fill(1000, "A", 1583, -1428, 30),
    ]
    rows = ei.build_burst_actions("0xABC", fills)
    assert len(rows) == 1
    row = rows[0]
    assert row["action_type"] == "ADDON"
    assert row["position_before"] == -573.0
    assert row["position_after"] == -3584.0
    assert row["n_source_fills"] == 3
    assert row["copy_signal_valid"] is True
    assert row["live_open_candidate"] is False


def test_cross_burst_gap_resync_is_explicit_but_current_signal_valid():
    fills = [_fill(1000, "B", 1, 0, 1), _fill(2000, "A", 1, 5, 2)]
    rows = ei.build_burst_actions("0xabc", fills)
    assert rows[0]["action_type"] == "ENTRY"
    assert rows[1]["action_type"] == "TRIM"
    assert rows[1]["continuity_ok"] is False
    assert rows[1]["state_resync"] is True
    # The current transition is directly known from startPosition, but a live
    # raw-trade tracker could not repair the missing state: fail closed.
    assert rows[1]["transition_valid"] is True
    assert rows[1]["copy_signal_valid"] is False


def test_live_open_candidate_uses_one_dollar_dust_boundary():
    rows = ei.build_burst_actions("0xabc", [_fill(1000, "B", 1, 0.005, 1)])
    assert rows[0]["action_type"] == "ADDON"  # mathematically nonzero before
    assert rows[0]["live_open_candidate"] is True  # V17 treats $0.50 as flat dust


def test_atomic_actions_preserve_intraburst_entry_add_and_exit():
    fills = [
        _fill(1000, "B", 1, 0, 30),
        _fill(1000, "B", 2, 1, 10),
        _fill(1000, "A", 3, 3, 20),
    ]
    rows = ei.build_atomic_actions("0xabc", fills)
    assert [r["action_type"] for r in rows] == ["ENTRY", "ADDON", "EXIT"]
    assert [r["fill_seq"] for r in rows] == [0, 1, 2]
    assert [r["copy_signal_valid"] for r in rows] == [True, True, True]

    # Burst netting erases the whole live-observable lifecycle, which is why
    # burst output is explicitly diagnostic-only.
    burst = ei.build_burst_actions("0xabc", fills)
    assert len(burst) == 1
    assert burst[0]["action_type"] == "FLAT"


def test_atomic_gap_fails_one_signal_then_reseeds():
    rows = ei.build_atomic_actions(
        "0xabc",
        [
            _fill(1000, "B", 1, 0, 1),
            _fill(2000, "A", 1, 5, 2),
            _fill(3000, "A", 1, 4, 3),
        ],
    )
    assert [r["copy_signal_valid"] for r in rows] == [True, False, True]
    assert rows[1]["state_resync"] is True
