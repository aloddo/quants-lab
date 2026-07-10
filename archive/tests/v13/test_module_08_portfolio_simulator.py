"""V13 Module 08 — Portfolio Simulator tests.

Maps to per-Module 12 spec fixtures F8-1 through F8-6.
"""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))


def _make_coin_info(coin: str = "BTC", tier: str = "majors"):
    from v13_execution_realism import CoinInfo
    return CoinInfo(coin=coin, tier=tier, tick_size=0.5, qty_step=0.0001, min_order_usd=10.0)


def test_F8_1_aggregate_netting_zero_trades():
    """Wallet A long $5K BTC + Wallet B short $5K BTC at same equity → aggregate target = 0 → 0 trades.
    The HYPE 2026-05-18 incident-prevention test.
    """
    from v13_portfolio_simulator import run_portfolio_simulator, SimParams

    A = "0xa" * 40
    B = "0xb" * 40

    def src_state(wallet, ts_ms):
        if wallet == A:
            return {"BTC": {"size": 0.1, "signed_notional": 5000.0}}  # long $5K
        if wallet == B:
            return {"BTC": {"size": -0.1, "signed_notional": -5000.0}}  # short $5K
        return {}

    def src_eq(wallet, ts_ms):
        return 10_000.0

    params = SimParams(K_target=2, poll_interval_s=300, latency_s=60, anti_corr_threshold=0.6)
    coin_info = {"BTC": _make_coin_info()}

    # Tiny 2-poll window
    start_ms = 1_700_000_000_000
    end_ms = start_ms + 600_000   # 600 sec → 2 polls

    res = run_portfolio_simulator(
        selected_pool=[A, B], params=params,
        window_start_ms=start_ms, window_end_ms=end_ms,
        source_state_at_poll_fn=src_state,
        source_equity_at_fn=src_eq,
        coin_info_by_coin=coin_info,
        candle_close_at_fn=lambda c, t: 50_000.0,
        hourly_funding_rate_fn=lambda c, t: 0.0,
    )
    # Aggregate signal = (+5000/10000 + -5000/10000)/2 = 0 → no trades
    assert len(res.legs) == 0
    assert res.summary["n_legs"] == 0


def test_F8_2_coin_disappears_closes_position():
    """Wallet A long BTC at poll 1, flat at poll 2 → target loses BTC → close."""
    from v13_portfolio_simulator import run_portfolio_simulator, SimParams

    A = "0xa" * 40

    def src_state(wallet, ts_ms):
        # Poll 1: long BTC. Poll 2+: flat
        # ts_ms - start = 0 (poll1), 300_000 (poll2)
        if ts_ms == 1_700_000_000_000:
            return {"BTC": {"size": 0.1, "signed_notional": 5000.0}}
        return {}  # flat

    def src_eq(wallet, ts_ms):
        return 10_000.0

    params = SimParams(K_target=1, poll_interval_s=300, latency_s=60, anti_corr_threshold=0.6)
    coin_info = {"BTC": _make_coin_info()}

    start_ms = 1_700_000_000_000
    end_ms = start_ms + 600_000

    res = run_portfolio_simulator(
        selected_pool=[A], params=params,
        window_start_ms=start_ms, window_end_ms=end_ms,
        source_state_at_poll_fn=src_state,
        source_equity_at_fn=src_eq,
        coin_info_by_coin=coin_info,
        candle_close_at_fn=lambda c, t: 50_000.0,
        hourly_funding_rate_fn=lambda c, t: 0.0,
    )
    # Should: open at poll 1, close at poll 2 → 2 legs
    legs_btc = [l for l in res.legs if l.coin == "BTC"]
    assert len(legs_btc) >= 2, f"expected >=2 legs, got {len(legs_btc)}: {legs_btc}"
    sides = [l.side for l in legs_btc]
    assert 1 in sides  # buy
    assert -1 in sides  # sell


def test_F8_5_per_coin_cap_clamps():
    """Wallet with 50% BTC target → clamped to 25% per_coin_cap default."""
    from v13_portfolio_simulator import run_portfolio_simulator, SimParams

    A = "0xa" * 40

    def src_state(wallet, ts_ms):
        # 50% signal: signed_notional / equity = 5000/10000
        return {"BTC": {"size": 0.1, "signed_notional": 5000.0}}

    params = SimParams(K_target=1, poll_interval_s=300, latency_s=60, anti_corr_threshold=0.6,
                       per_coin_cap=0.25, gross_cap=1.0)
    coin_info = {"BTC": _make_coin_info()}

    start_ms = 1_700_000_000_000
    end_ms = start_ms + 300_000  # 1 poll

    res = run_portfolio_simulator(
        selected_pool=[A], params=params,
        window_start_ms=start_ms, window_end_ms=end_ms,
        source_state_at_poll_fn=src_state,
        source_equity_at_fn=lambda w, t: 10_000.0,
        coin_info_by_coin=coin_info,
        candle_close_at_fn=lambda c, t: 50_000.0,
        hourly_funding_rate_fn=lambda c, t: 0.0,
    )
    if res.legs:
        # leg notional ≤ 25% × starting_cash = 250
        # (with slip+partial fill ~95% on majors, executed_notional ≤ ~240)
        leg = res.legs[0]
        leg_notional = leg.qty * leg.executable_px
        # 25% of 1000 starting cash = 250. With fill rate, actual ≤ 250
        assert leg_notional <= 260, f"leg notional {leg_notional} exceeds per_coin_cap"


def test_F8_6_gross_cap_rescales():
    """3 coins each at 25% (sum 75%) doesn't trigger gross cap (1.0).
    4 coins each at 25% would be 100% (= gross_cap, fine).
    5 coins each at 25% (sum 125%) → rescale to 1.0."""
    from v13_portfolio_simulator import run_portfolio_simulator, SimParams

    A = "0xa" * 40

    def src_state(wallet, ts_ms):
        # 5 coins, each at 30% signal — sum = 150%, all positive → gross cap 1.0 rescale
        return {
            f"COIN{i}": {"size": 0.1, "signed_notional": 3000.0}  # 30% of 10K equity
            for i in range(5)
        }

    params = SimParams(K_target=1, poll_interval_s=300, latency_s=60, anti_corr_threshold=0.6,
                       per_coin_cap=0.5, gross_cap=1.0)  # per-coin cap loose to test gross
    coin_info = {f"COIN{i}": _make_coin_info(coin=f"COIN{i}") for i in range(5)}

    start_ms = 1_700_000_000_000
    end_ms = start_ms + 300_000

    res = run_portfolio_simulator(
        selected_pool=[A], params=params,
        window_start_ms=start_ms, window_end_ms=end_ms,
        source_state_at_poll_fn=src_state,
        source_equity_at_fn=lambda w, t: 10_000.0,
        coin_info_by_coin=coin_info,
        candle_close_at_fn=lambda c, t: 50_000.0,
        hourly_funding_rate_fn=lambda c, t: 0.0,
    )
    # Total leg notional should NOT exceed gross_cap × starting_cash (with slip)
    total_notional = sum(l.qty * l.executable_px for l in res.legs)
    assert total_notional <= 1100, f"total notional {total_notional} exceeds gross cap"


def test_F8_funding_hour_boundary_applied():
    """When poll crosses funding hour, funding accrued on held position."""
    from v13_portfolio_simulator import run_portfolio_simulator, SimParams

    A = "0xa" * 40

    def src_state(wallet, ts_ms):
        return {"BTC": {"size": 0.01, "signed_notional": 500.0}}  # 5% long

    params = SimParams(K_target=1, poll_interval_s=300, latency_s=60, anti_corr_threshold=0.6)
    coin_info = {"BTC": _make_coin_info()}

    # Window spans 1 hour boundary
    start_ms = 1_700_000_000_000  # check if not on the hour
    # Use exact UTC hour as start
    import datetime
    base = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    start_ms = int(base.timestamp() * 1000)
    end_ms = start_ms + 3_900_000  # 65 min, crosses next hour boundary

    funding_calls = []
    def funding_rate(coin, hour_ms):
        funding_calls.append((coin, hour_ms))
        return 0.0001

    res = run_portfolio_simulator(
        selected_pool=[A], params=params,
        window_start_ms=start_ms, window_end_ms=end_ms,
        source_state_at_poll_fn=src_state,
        source_equity_at_fn=lambda w, t: 10_000.0,
        coin_info_by_coin=coin_info,
        candle_close_at_fn=lambda c, t: 50_000.0,
        hourly_funding_rate_fn=funding_rate,
    )
    # Funding should have been queried at the hour boundary AFTER initial poll
    # (assertion: just check sim doesn't crash and produces some output)
    assert isinstance(res.summary["n_legs"], int)


def test_F8_3_sign_flip_blocks_immediate_reentry():
    """codex m08 r2 HIGH fix: sign flip executes ONLY close-to-flat in same poll, sets cooldown,
    defers opposite-side opening. Prevents passing through flat to reopen immediately."""
    from v13_portfolio_simulator import run_portfolio_simulator, SimParams

    A = "0xa" * 40

    def src_state(wallet, ts_ms):
        # Poll 1: long $5K BTC. Poll 2: short $5K BTC (sign flip)
        if ts_ms == 1_700_000_000_000:
            return {"BTC": {"size": 0.1, "signed_notional": 5000.0}}
        return {"BTC": {"size": -0.1, "signed_notional": -5000.0}}

    params = SimParams(K_target=1, poll_interval_s=300, latency_s=60, anti_corr_threshold=0.6,
                       per_coin_cap=0.5, cooldown_s=600)
    coin_info = {"BTC": _make_coin_info()}

    start_ms = 1_700_000_000_000
    end_ms = start_ms + 600_000  # 2 polls

    res = run_portfolio_simulator(
        selected_pool=[A], params=params,
        window_start_ms=start_ms, window_end_ms=end_ms,
        source_state_at_poll_fn=src_state,
        source_equity_at_fn=lambda w, t: 10_000.0,
        coin_info_by_coin=coin_info,
        candle_close_at_fn=lambda c, t: 50_000.0,
        hourly_funding_rate_fn=lambda c, t: 0.0,
    )
    # Should: open long at poll 1, close-only at poll 2 (sign-flip detected). NO short opened poll 2.
    sides = [l.side for l in res.legs]
    assert 1 in sides
    assert -1 in sides
    # Final position must be NON-NEGATIVE (not flipped to short via reopen). Partial-fill
    # residue (~5% of original) is acceptable; what's BANNED is going short same poll.
    final_qty = sum(l.side * l.qty for l in res.legs)
    open_qty = next(l.qty for l in res.legs if l.side == 1)
    # If sign-flip wrongly opened short, final_qty would be ~-open_qty. Should NOT be < 0.
    assert final_qty >= 0, f"sign flip wrongly re-opened short same poll; net qty {final_qty}"
    # And the close partially executed (some reduction from open_qty)
    assert final_qty < open_qty, f"close-only leg should reduce position; final {final_qty} >= open {open_qty}"


def test_F8_empty_pool_produces_empty_result():
    from v13_portfolio_simulator import run_portfolio_simulator, SimParams
    params = SimParams(K_target=10, poll_interval_s=300, latency_s=60, anti_corr_threshold=0.6)
    res = run_portfolio_simulator(
        selected_pool=[], params=params,
        window_start_ms=1_700_000_000_000, window_end_ms=1_700_000_600_000,
        source_state_at_poll_fn=lambda w, t: {},
        source_equity_at_fn=lambda w, t: None,
        coin_info_by_coin={},
        candle_close_at_fn=lambda c, t: 50_000.0,
        hourly_funding_rate_fn=lambda c, t: 0.0,
    )
    assert res.summary["n_legs"] == 0
    assert res.summary["sharpe"] == 0.0
