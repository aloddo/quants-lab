"""V13 Module 07 — CopyLedger tests.

Maps to per-Module 12 spec fixtures F7-1 through F7-6:
  - F7-1 BUY: cash -50K, position +1
  - F7-2 SELL: cash +52K, position -1
  - F7-3 Funding long pays positive rate: cash decreases
  - F7-4 Funding short pays positive rate: cash INCREASES
  - F7-5 Equity at t: cash + sum(qty × mark)
  - F7-6 Multi-hour funding: 11:00 + 12:00 boundaries each accrue
"""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))


def test_F7_1_buy_outflow_position_increase():
    from v13_portfolio_ledger import CopyLedger
    led = CopyLedger(cash_usd=100_000.0)
    led.on_leg_executed("BTC", side=+1, qty=1.0, executable_px=50_000.0, fee_usd=22.0)
    assert led.cash_usd == 100_000.0 - 50_000.0 - 22.0
    assert led.position_qty["BTC"] == 1.0


def test_F7_2_sell_inflow_position_decrease():
    from v13_portfolio_ledger import CopyLedger
    led = CopyLedger(cash_usd=100_000.0)
    led.on_leg_executed("BTC", side=-1, qty=1.0, executable_px=52_000.0, fee_usd=22.0)
    assert led.cash_usd == 100_000.0 + 52_000.0 - 22.0
    assert led.position_qty["BTC"] == -1.0


def test_F7_3_funding_long_pays_positive_rate():
    """Long 1 BTC @ $50K, positive hourly rate 0.0001 → we pay $5 (long pays positive)."""
    from v13_portfolio_ledger import CopyLedger
    led = CopyLedger(cash_usd=10_000.0, position_qty={"BTC": 1.0})
    led.on_funding_hour_boundary(hour_ts=1, marks={"BTC": 50_000.0}, hourly_rates={"BTC": 0.0001})
    # signed_notional = 1 × 50000 = 50000
    # funding_cashflow = -50000 × 0.0001 = -5
    assert led.cash_usd == 10_000.0 - 5.0


def test_F7_4_funding_short_receives_positive_rate():
    """Short 1 BTC @ $50K, positive rate 0.0001 → we receive $5."""
    from v13_portfolio_ledger import CopyLedger
    led = CopyLedger(cash_usd=10_000.0, position_qty={"BTC": -1.0})
    led.on_funding_hour_boundary(hour_ts=1, marks={"BTC": 50_000.0}, hourly_rates={"BTC": 0.0001})
    # signed_notional = -1 × 50000 = -50000
    # funding_cashflow = -(-50000) × 0.0001 = +5
    assert led.cash_usd == 10_000.0 + 5.0


def test_F7_5_equity_at_t_with_marks():
    """cash $1000, long 1 BTC, mark $50K → equity $51K. Marks missing → coin contributes 0."""
    from v13_portfolio_ledger import CopyLedger
    led = CopyLedger(cash_usd=1_000.0, position_qty={"BTC": 1.0, "ETH": 2.0})
    marks = {"BTC": 50_000.0, "ETH": 3_000.0}
    eq = led.equity_usd_at(t=0, candle_close_at_fn=lambda c, t: marks.get(c))
    # 1000 + 1*50000 + 2*3000 = 57000
    assert eq == 57_000.0

    # Missing mark → coin contributes 0
    eq_missing = led.equity_usd_at(t=0, candle_close_at_fn=lambda c, t: marks.get(c) if c == "BTC" else None)
    # 1000 + 1*50000 + 0 = 51000
    assert eq_missing == 51_000.0


def test_F7_6_multi_hour_funding_accruals():
    """Hold 1 BTC long from poll N to poll N+1 spanning 2 hour boundaries → 2 funding events."""
    from v13_portfolio_ledger import CopyLedger
    led = CopyLedger(cash_usd=10_000.0, position_qty={"BTC": 1.0})
    # Two boundaries, both at different marks/rates
    led.on_funding_hour_boundary(hour_ts=11_00_00_000, marks={"BTC": 50_000.0}, hourly_rates={"BTC": 0.0001})
    # cash -5
    led.on_funding_hour_boundary(hour_ts=12_00_00_000, marks={"BTC": 51_000.0}, hourly_rates={"BTC": 0.0002})
    # cash -1*51000*0.0002 = -10.20
    assert led.cash_usd == pytest.approx(10_000.0 - 5.0 - 10.2)


def test_F7_position_zero_cleanup():
    """Flat position should NOT appear in position_qty dict."""
    from v13_portfolio_ledger import CopyLedger
    led = CopyLedger(cash_usd=100_000.0)
    led.on_leg_executed("BTC", side=+1, qty=1.0, executable_px=50_000.0, fee_usd=22.0)
    assert "BTC" in led.position_qty
    # Sell exactly the same → flat
    led.on_leg_executed("BTC", side=-1, qty=1.0, executable_px=51_000.0, fee_usd=22.0)
    assert "BTC" not in led.position_qty, "flat position should be deleted from dict"


def test_F7_signed_notional_helper():
    from v13_portfolio_ledger import CopyLedger
    led = CopyLedger(position_qty={"BTC": 0.5, "ETH": -2.0})
    assert led.signed_notional_usd("BTC", 50_000.0) == 25_000.0
    assert led.signed_notional_usd("ETH", 3_000.0) == -6_000.0
    assert led.signed_notional_usd("SOL", 200.0) == 0.0  # not in position_qty


def test_F7_input_validation_on_leg_executed():
    """codex m07 r1 fix: invalid inputs MUST raise ValueError (not silently corrupt ledger)."""
    from v13_portfolio_ledger import CopyLedger
    led = CopyLedger(cash_usd=10_000.0)

    # Bad side
    with pytest.raises(ValueError, match="side"):
        led.on_leg_executed("BTC", side=0, qty=1.0, executable_px=50_000.0, fee_usd=10.0)
    with pytest.raises(ValueError, match="side"):
        led.on_leg_executed("BTC", side=2, qty=1.0, executable_px=50_000.0, fee_usd=10.0)
    # Negative qty
    with pytest.raises(ValueError, match="qty"):
        led.on_leg_executed("BTC", side=1, qty=-1.0, executable_px=50_000.0, fee_usd=10.0)
    # NaN qty
    with pytest.raises(ValueError, match="qty"):
        led.on_leg_executed("BTC", side=1, qty=float('nan'), executable_px=50_000.0, fee_usd=10.0)
    # Non-positive px
    with pytest.raises(ValueError, match="executable_px"):
        led.on_leg_executed("BTC", side=1, qty=1.0, executable_px=0.0, fee_usd=10.0)
    with pytest.raises(ValueError, match="executable_px"):
        led.on_leg_executed("BTC", side=1, qty=1.0, executable_px=-50.0, fee_usd=10.0)
    # NaN px
    with pytest.raises(ValueError, match="executable_px"):
        led.on_leg_executed("BTC", side=1, qty=1.0, executable_px=float('nan'), fee_usd=10.0)
    # Negative fee
    with pytest.raises(ValueError, match="fee_usd"):
        led.on_leg_executed("BTC", side=1, qty=1.0, executable_px=50_000.0, fee_usd=-1.0)


def test_F7_funding_rejects_nan_marks_and_rates():
    """codex m07 r1 fix: NaN/Inf marks or rates MUST NOT poison cash."""
    from v13_portfolio_ledger import CopyLedger
    led = CopyLedger(cash_usd=10_000.0, position_qty={"BTC": 1.0, "ETH": 2.0})

    # NaN mark on BTC → BTC skipped, ETH accrues
    led.on_funding_hour_boundary(
        hour_ts=1,
        marks={"BTC": float('nan'), "ETH": 3_000.0},
        hourly_rates={"BTC": 0.0001, "ETH": 0.0001},
    )
    # Only ETH: 2 × 3000 × 0.0001 = 0.6 → cash -0.6
    assert led.cash_usd == pytest.approx(10_000.0 - 0.6)
    # NaN check: cash must be finite
    import math
    assert math.isfinite(led.cash_usd)

    # NaN rate on next call → coin skipped
    led2 = CopyLedger(cash_usd=10_000.0, position_qty={"BTC": 1.0})
    led2.on_funding_hour_boundary(
        hour_ts=1,
        marks={"BTC": 50_000.0},
        hourly_rates={"BTC": float('nan')},
    )
    assert led2.cash_usd == 10_000.0  # no change


def test_F7_equity_rejects_invalid_marks():
    """codex m07 r1 fix: NaN/Inf/non-positive marks contribute 0 (not poison equity)."""
    from v13_portfolio_ledger import CopyLedger
    import math
    led = CopyLedger(cash_usd=1_000.0, position_qty={"BTC": 1.0, "ETH": 2.0, "SOL": 5.0})

    bad_marks = {"BTC": float('nan'), "ETH": -100.0, "SOL": float('inf')}
    eq = led.equity_usd_at(t=0, candle_close_at_fn=lambda c, t: bad_marks.get(c))
    assert math.isfinite(eq), "equity must be finite even with bad marks"
    assert eq == 1_000.0, "all coins skipped due to bad marks → equity = cash only"


def test_F7_signed_notional_raises_on_overflow():
    """codex m07 r5 fix: signed_notional_usd RAISES on overflow (was returning 0; that masked
    real exposure → could treat enormous position as flat for sizing)."""
    from v13_portfolio_ledger import CopyLedger
    led = CopyLedger(cash_usd=0.0, position_qty={"BTC": 1e300})
    with pytest.raises(ValueError, match="overflow"):
        led.signed_notional_usd("BTC", 1e10)  # would be 1e310 (Inf)
    # Sanity: small numbers still work
    led_small = CopyLedger(position_qty={"BTC": 0.5})
    assert led_small.signed_notional_usd("BTC", 50_000.0) == 25_000.0
    # Invalid mark → 0 (no market data, not overflow)
    assert led_small.signed_notional_usd("BTC", float('nan')) == 0.0


def test_F7_equity_overflow_raises():
    """codex m07 r4+r5 fix: equity_usd_at raises ValueError on ANY contribution overflow
    (was silently skipping coin → falsely-finite equity that hides exposure)."""
    from v13_portfolio_ledger import CopyLedger
    # Single-coin overflow → raise (was skip+return cash before r5)
    led_single = CopyLedger(cash_usd=1000.0, position_qty={"BTC": 1e300})
    with pytest.raises(ValueError, match="overflow"):
        led_single.equity_usd_at(t=0, candle_close_at_fn=lambda c, t: 1e10)

    # Accumulator overflow on multi-coin sum
    led = CopyLedger(cash_usd=0.0, position_qty={"BTC": 1e154, "ETH": 1e154})
    marks = {"BTC": 1e154, "ETH": 1e154}
    with pytest.raises(ValueError, match="overflow"):
        led.equity_usd_at(t=0, candle_close_at_fn=lambda c, t: marks.get(c))


def test_F7_dust_qty_with_fee_rejected():
    """codex m07 r4 fix: dust qty with nonzero fee is caller bug → raise."""
    from v13_portfolio_ledger import CopyLedger
    led = CopyLedger(cash_usd=10_000.0)
    with pytest.raises(ValueError, match="dust qty"):
        led.on_leg_executed("BTC", side=1, qty=1e-12, executable_px=50_000.0, fee_usd=0.5)
    # Dust qty with ZERO fee → no-op (silent)
    led.on_leg_executed("BTC", side=1, qty=1e-12, executable_px=50_000.0, fee_usd=0.0)
    assert "BTC" not in led.position_qty
    assert led.cash_usd == 10_000.0


def test_F7_overflow_atomic_commit_leg():
    """codex m07 r3 fix: leg execution that would produce Inf cash/position raises ValueError
    BEFORE mutating ledger state (atomic commit)."""
    from v13_portfolio_ledger import CopyLedger
    led = CopyLedger(cash_usd=1e308)  # near float max
    # Adding another 1e308 outflow would overflow
    with pytest.raises(ValueError, match="cash_usd not finite"):
        led.on_leg_executed("BTC", side=-1, qty=1.0, executable_px=1e308, fee_usd=0.0)
    # cash unchanged after the failed commit
    assert led.cash_usd == 1e308


def test_F7_overflow_funding_raises():
    """codex m07 r6 fix: funding overflow from finite inputs now RAISES (was silent skip).
    Consistency with equity_usd_at + signed_notional_usd: overflow is a real signal."""
    from v13_portfolio_ledger import CopyLedger
    # Position so large that funding × rate overflows
    led = CopyLedger(cash_usd=0.0, position_qty={"BTC": 1e300})
    with pytest.raises(ValueError, match="overflow"):
        led.on_funding_hour_boundary(
            hour_ts=1,
            marks={"BTC": 1e10},
            hourly_rates={"BTC": 1e10},
        )
    # Cash unchanged
    assert led.cash_usd == 0.0


def test_F7_funding_atomic_no_partial_mutation():
    """codex m07 r7 fix: funding must be ATOMIC — if any coin overflows, NO cash mutated
    (was partially applied earlier coins before raising on later overflow)."""
    from v13_portfolio_ledger import CopyLedger
    # BTC funding is small + valid; ETH overflows. Per r7, BTC must NOT be applied.
    led = CopyLedger(cash_usd=1000.0, position_qty={"BTC": 1.0, "ETH": 1e300})
    with pytest.raises(ValueError, match="overflow"):
        led.on_funding_hour_boundary(
            hour_ts=1,
            marks={"BTC": 50_000.0, "ETH": 1e10},
            hourly_rates={"BTC": 0.0001, "ETH": 1e10},
        )
    # Cash unchanged from initial — BTC's would-be -5 not applied
    assert led.cash_usd == 1000.0, "atomic: NO mutation on partial overflow"


def test_F7_overflow_cash_event_raises():
    """codex m07 r3 fix: on_cash_event with overflow result raises ValueError."""
    from v13_portfolio_ledger import CopyLedger
    led = CopyLedger(cash_usd=1e308)
    with pytest.raises(ValueError, match="cash_usd not finite"):
        led.on_cash_event(1e308)
    # State unchanged
    assert led.cash_usd == 1e308


def test_F7_huge_int_raises_value_error_not_overflow_error():
    """codex m07 r6 fix: huge Python int (>float max as int) must raise ValueError per
    documented contract, NOT leak raw OverflowError."""
    from v13_portfolio_ledger import CopyLedger
    huge_int = 10 ** 10000

    with pytest.raises(ValueError):  # not OverflowError
        CopyLedger(cash_usd=huge_int)
    with pytest.raises(ValueError):
        CopyLedger(cash_usd=1000.0, position_qty={"BTC": huge_int})
    led = CopyLedger(cash_usd=10_000.0)
    with pytest.raises(ValueError):
        led.on_leg_executed("BTC", side=1, qty=huge_int, executable_px=50_000.0, fee_usd=0.0)
    with pytest.raises(ValueError):
        led.on_cash_event(huge_int)


def test_F7_bool_rejected_in_validation():
    """codex m07 r3 fix: bool MUST NOT pass numeric validation (bool subclasses int)."""
    from v13_portfolio_ledger import CopyLedger
    # Init with bool cash
    with pytest.raises(ValueError, match="cash_usd"):
        CopyLedger(cash_usd=True)
    # Init with bool position
    with pytest.raises(ValueError, match="position_qty"):
        CopyLedger(cash_usd=1000.0, position_qty={"BTC": True})
    # Leg execution with bool inputs
    led = CopyLedger(cash_usd=10_000.0)
    with pytest.raises(ValueError, match="side"):
        led.on_leg_executed("BTC", side=True, qty=1.0, executable_px=50_000.0, fee_usd=10.0)
    with pytest.raises(ValueError, match="qty"):
        led.on_leg_executed("BTC", side=1, qty=True, executable_px=50_000.0, fee_usd=10.0)


def test_F7_init_rejects_invalid_cash_or_positions():
    """codex m07 r2 fix: __post_init__ rejects NaN/Inf on cash or position qty."""
    from v13_portfolio_ledger import CopyLedger

    # NaN cash
    with pytest.raises(ValueError, match="cash_usd"):
        CopyLedger(cash_usd=float('nan'))
    # Inf cash
    with pytest.raises(ValueError, match="cash_usd"):
        CopyLedger(cash_usd=float('inf'))
    # NaN position qty
    with pytest.raises(ValueError, match="position_qty"):
        CopyLedger(cash_usd=1000.0, position_qty={"BTC": float('nan')})
    # Inf position qty
    with pytest.raises(ValueError, match="position_qty"):
        CopyLedger(cash_usd=1000.0, position_qty={"BTC": float('inf')})
    # Valid construction
    led = CopyLedger(cash_usd=1000.0, position_qty={"BTC": 0.5})
    assert led.cash_usd == 1000.0


def test_F7_on_cash_event_rejects_nan_inf():
    """codex m07 r2 fix: on_cash_event must validate amount_usd."""
    from v13_portfolio_ledger import CopyLedger
    led = CopyLedger(cash_usd=1000.0)

    with pytest.raises(ValueError, match="amount_usd"):
        led.on_cash_event(float('nan'))
    with pytest.raises(ValueError, match="amount_usd"):
        led.on_cash_event(float('inf'))
    # Negative (withdrawal) allowed
    led.on_cash_event(-100.0, event_type="withdrawal")
    assert led.cash_usd == 900.0
    # Positive (deposit) allowed
    led.on_cash_event(50.0)
    assert led.cash_usd == 950.0


def test_F7_funding_skips_missing_rate_or_mark():
    """Coin in position but missing from rates dict → no funding accrual."""
    from v13_portfolio_ledger import CopyLedger
    led = CopyLedger(cash_usd=10_000.0, position_qty={"BTC": 1.0, "ETH": 2.0})
    # Only BTC rate provided
    led.on_funding_hour_boundary(hour_ts=1, marks={"BTC": 50_000.0, "ETH": 3_000.0}, hourly_rates={"BTC": 0.0001})
    # Only BTC accrues: -5
    assert led.cash_usd == 10_000.0 - 5.0
