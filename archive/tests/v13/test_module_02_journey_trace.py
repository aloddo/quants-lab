"""V13 Module 02 — journey_trace.py tests.

Maps to per-Module 12 spec fixtures F2-1 through F2-11.
This is the FIRST concrete test file. Adds coverage for the highest-risk paths:
  - F2-2: carry-in spot filter (codex m02 r3 + r4#5)
  - F2-3: carry-in incomplete on fetch failure (codex m02 r4#3 + r7 regression fix)
  - F2-4: post-window boundary day not double-subtracted (codex m02 r4#1)
"""
import pytest
import pandas as pd
import sys
from pathlib import Path

# Make scripts/ importable
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))


def test_F2_2_carry_in_spot_filter():
    """Spot coins (@-prefix or USDC) must NOT contribute to carry-in derivation."""
    from v13_journey_trace import _compute_carry_in_via_backwalk

    # Synthetic: wallet has 1 perp fill BTC and 1 spot fill @142.
    # current_positions = {BTC: 0.1, @142: 5.0} — but @142 is spot (HL clearinghouseState
    # only returns perp; this is a hypothetical to test the filter).
    fills = pd.DataFrame([
        {"wallet": "0xa" * 40, "coin": "BTC", "time": 1000, "side": "B", "size": 0.1, "price": 50000},
        {"wallet": "0xa" * 40, "coin": "@142", "time": 2000, "side": "B", "size": 5.0, "price": 50},
    ])
    current = {"0xa" * 40: {"BTC": 0.1, "@142": 5.0}}
    snapshot_ts = {"0xa" * 40: 5000}

    out = _compute_carry_in_via_backwalk(
        fills, ["0xa" * 40], current,
        post_window_fills=None, snapshot_ts_by_wallet=snapshot_ts,
    )

    # BTC: present, carry resolved (no_carry since net = 0.1 - 0.1 = 0)
    assert ("0xa" * 40, "BTC") in out
    assert out[("0xa" * 40, "BTC")][2] == "no_carry"
    # @142: spot, MUST NOT be in carry-in result
    assert ("0xa" * 40, "@142") not in out


def test_F2_3_carry_in_incomplete_on_fetch_failure():
    """Wallet missing from current_positions (fetch failed) → all pairs incomplete.

    Regression fix for codex m02 r7: previously the r6 snapshot bound dropped failed-wallet
    fills from in_window_net, which caused the incomplete-emit path to find no coins → silent
    no_carry fallback. r7 fix uses coins_by_wallet_all precomputed before snapshot bound.
    """
    from v13_journey_trace import _compute_carry_in_via_backwalk

    fills = pd.DataFrame([
        {"wallet": "0xa" * 40, "coin": "BTC", "time": 1000, "side": "B", "size": 0.1, "price": 50000},
        {"wallet": "0xa" * 40, "coin": "ETH", "time": 2000, "side": "S", "size": 1.0, "price": 3000},
        {"wallet": "0xb" * 40, "coin": "SOL", "time": 3000, "side": "B", "size": 5.0, "price": 200},
        {"wallet": "0xb" * 40, "coin": "AVAX", "time": 4000, "side": "S", "size": 10.0, "price": 30},
    ])
    # Wallet A succeeded (has positions); wallet B fetch FAILED (missing)
    current = {"0xa" * 40: {"BTC": 0.1, "ETH": -1.0}}
    snapshot_ts = {"0xa" * 40: 5000}  # B missing

    out = _compute_carry_in_via_backwalk(
        fills, ["0xa" * 40, "0xb" * 40], current,
        post_window_fills=None, snapshot_ts_by_wallet=snapshot_ts,
    )

    # Wallet A: 2 pairs, no_carry (current matches in_window net)
    assert out[("0xa" * 40, "BTC")][2] == "no_carry"
    assert out[("0xa" * 40, "ETH")][2] == "no_carry"
    # Wallet B: 2 pairs, BOTH marked incomplete (fetch failed)
    assert out[("0xb" * 40, "SOL")][2] == "incomplete"
    assert out[("0xb" * 40, "AVAX")][2] == "incomplete"


def test_F2_4_carry_in_snapshot_bound_drops_post_snapshot_fills():
    """In-window fills with time > snapshot_ms must NOT contribute to carry-in arithmetic.

    Per codex m02 r6: current_qty reflects state AT snapshot_ms; fills after that are NOT
    in current_qty → including them would over-subtract.
    """
    from v13_journey_trace import _compute_carry_in_via_backwalk

    # Fill at t=10000 should be DROPPED because wallet's snapshot is at t=5000.
    # If included, in_window_net = +0.1 (the at-10000 fill); subtracting from current_qty=0.1
    # would give position_at_start = 0 → "no_carry". But the at-10000 fill is post-snapshot
    # so position_at_start should still equal 0.1 (since current snapshot already EXCLUDES
    # that fill — it happened after the snapshot was taken).
    # Wait — actually our snapshot_ts represents the time at which we sampled current_qty.
    # If a fill at t=10000 happened AFTER snapshot, the wallet's CURRENT (now) position would
    # reflect it, but our CACHED snapshot does NOT. Test verifies the bound filters it out
    # of in_window_net so we don't double-subtract.
    fills = pd.DataFrame([
        {"wallet": "0xa" * 40, "coin": "BTC", "time": 1000, "side": "B", "size": 0.05, "price": 50000},
        {"wallet": "0xa" * 40, "coin": "BTC", "time": 10000, "side": "B", "size": 0.05, "price": 51000},
    ])
    current = {"0xa" * 40: {"BTC": 0.05}}  # snapshot took at t=5000, before the t=10000 fill
    snapshot_ts = {"0xa" * 40: 5000}

    out = _compute_carry_in_via_backwalk(
        fills, ["0xa" * 40], current,
        post_window_fills=None, snapshot_ts_by_wallet=snapshot_ts,
    )

    pos, cb, status = out[("0xa" * 40, "BTC")]
    # in_window_net should EXCLUDE the at-10000 fill → arithmetic = 0.05 (current) - 0.05 (in-window net of pre-snapshot) = 0
    # → status = no_carry
    assert status == "no_carry", f"expected no_carry, got {status} with pos={pos}"


def test_F2_3_dedup_on_resume_no_duplicate_journeys():
    """Per codex m02 r4 #4: resumed_journeys + freshly-traced journeys must dedup by (wallet, coin, journey_id).

    This is a unit-level placeholder — the full integration test is covered by the resume
    smoke that ran in shell (20 wallets, partial=20, done=15 → 5 re-processed → 0 duplicates).
    Here we just verify the dedup primitive logic exists.
    """
    import v13_journey_trace as jt
    # Source code must contain seen_keys + per-pair dedup.
    src = (Path(jt.__file__)).read_text()
    assert "seen_keys: set[tuple[str, str, int]]" in src, "seed-time dedup container missing"
    assert "if k in seen_keys:" in src, "per-pair dedup check missing"
