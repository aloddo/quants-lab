"""v25 sim core unit tests: 2s next-mark repricing (never prior bar), window-boundary
mark isolation, FIRST_CLOSE 85% cumulative-reverse semantics, REVERSE-never-opens,
terminal MTM, event-level MTM DD, dropout determinism, coalescing, caps.
Synthetic data only."""
import numpy as np
import pandas as pd
import pytest

from v25_common import (INITIAL_EQUITY, MS_DAY, MS_MIN, ExecScenario, MarksIndex,
                        event_dropout, scenario_base, scenario_worst)
from v25_portfolio_sim import CopySim, simulate_portfolio, simulate_wallet_trips

T0 = pd.Timestamp("2026-03-01").value // 10**6      # UTC midnight, on a minute boundary
END = T0 + 7 * MS_DAY


def zero_cost():
    return ExecScenario("TEST", 0.0, 0.0, 0.0, False)


def actions(rows, wallet="0xw"):
    cols = ["coin", "ts", "action_type", "signed_size", "price", "position_after",
            "journey_id", "is_liquidation"]
    df = pd.DataFrame(rows, columns=cols)
    df["wallet"] = wallet
    return df


class TestRepricing:
    def test_next_close_never_prior_bar(self, marks_dir):
        # bars open at 0/60/120s (closes at 60/120/180s), closes 100/110/120
        marks_dir("BTC", [T0, T0 + MS_MIN, T0 + 2 * MS_MIN], [100.0, 110.0, 120.0])
        m = MarksIndex(cache_dir=marks_dir.dir)
        # signal at +30s -> target +32s -> first bar close at +60s (price 100)
        ts, px = m.next_mark("BTC", T0 + 30_000)
        assert ts == T0 + MS_MIN and px == 100.0
        # signal at +59s -> target +61s -> the +60s close is PRIOR (< target): must skip
        ts, px = m.next_mark("BTC", T0 + 59_000)
        assert ts == T0 + 2 * MS_MIN and px == 110.0
        assert ts >= T0 + 59_000 + 2000

    def test_drop_when_no_mark_within_60s(self, marks_dir):
        marks_dir("BTC", [T0, T0 + 10 * MS_MIN], [100.0, 105.0])
        m = MarksIndex(cache_dir=marks_dir.dir)
        # signal at +70s: next close is +660s, 588s past target -> outside 60s window
        ts, px = m.next_mark("BTC", T0 + 70_000)
        assert ts is None and px is None
        # unbounded (exit fallback path) finds it
        ts, px = m.next_mark("BTC", T0 + 70_000, window_ms=None)
        assert ts == T0 + 11 * MS_MIN and px == 105.0

    def test_cap_ms_isolates_window_boundary(self, marks_dir):
        # a mark EXISTS beyond the cap; with cap_ms it must never be readable
        marks_dir("BTC", [T0, T0 + 10 * MS_MIN], [100.0, 105.0])
        m = MarksIndex(cache_dir=marks_dir.dir)
        cap = T0 + 5 * MS_MIN
        assert m.next_mark("BTC", T0 + 70_000, window_ms=None, cap_ms=cap) == (None, None)
        # without the cap the same call returns the future mark (control)
        assert m.next_mark("BTC", T0 + 70_000, window_ms=None)[1] == 105.0

    def test_no_future_mark_at_all(self, marks_dir):
        marks_dir("BTC", [T0], [100.0])
        m = MarksIndex(cache_dir=marks_dir.dir)
        assert m.next_mark("BTC", T0 + 2 * MS_MIN) == (None, None)

    def test_missing_series_fail_closed(self, marks_dir):
        m = MarksIndex(cache_dir=marks_dir.dir)
        assert m.next_mark("NOPE", T0) == (None, None)
        assert m.asof_mark("NOPE", T0) is None

    def test_entry_dropped_and_counted(self, marks_dir):
        marks_dir("BTC", [T0], [100.0])          # no mark after the signal minute
        m = MarksIndex(cache_dir=marks_dir.dir)
        a = actions([("BTC", T0 + 5 * MS_MIN, "ENTRY", 1.0, 100.0, 1.0, 1, False)])
        res = simulate_wallet_trips(a, zero_cost(), m, T0, END)
        assert res["counters"]["entries_dropped_no_mark"] == 1
        assert res["counters"]["entries"] == 0
        assert len(res["trips"]) == 0


def _flat_marks(marks_dir, coin="BTC", px=100.0, n_min=10 * 24 * 60, step_px=None,
                skip=None):
    mins, closes = [], []
    for i in range(n_min):
        if skip is not None and skip(i):
            continue
        mins.append(T0 + i * MS_MIN)
        closes.append(px if step_px is None else step_px(i))
    marks_dir(coin, mins, closes)


class TestFirstClose:
    def test_full_round_trip_zero_cost(self, marks_dir):
        _flat_marks(marks_dir, px=100.0)
        m = MarksIndex(cache_dir=marks_dir.dir)
        a = actions([
            ("BTC", T0 + 10 * MS_MIN, "ENTRY", 2.0, 100.0, 2.0, 1, False),
            ("BTC", T0 + 100 * MS_MIN, "EXIT", -2.0, 100.0, 0.0, 1, False),
        ])
        res = simulate_wallet_trips(a, zero_cost(), m, T0, END)
        t = res["trips"]
        assert len(t) == 1
        assert not t.iloc[0]["terminal"]
        assert t.iloc[0]["net_pnl"] == pytest.approx(0.0)     # flat marks, zero cost
        assert t.iloc[0]["entry_notional"] == pytest.approx(150.0)

    def test_costs_paid_both_sides(self, marks_dir):
        _flat_marks(marks_dir, px=100.0)
        m = MarksIndex(cache_dir=marks_dir.dir)
        sc = scenario_worst()      # flat 7bps slip one-way, 4.5bps fee one-way (main)
        a = actions([
            ("BTC", T0 + 10 * MS_MIN, "ENTRY", 2.0, 100.0, 2.0, 1, False),
            ("BTC", T0 + 100 * MS_MIN, "EXIT", -2.0, 100.0, 0.0, 1, False),
        ])
        res = simulate_wallet_trips(a, sc, m, T0, END)
        bps = res["trips"].iloc[0]["net_bps"]
        # RT cost ~= 2x7 slip + 2x4.5 fee = 23 bps (small second-order effects)
        assert -24.5 < bps < -21.5

    def test_trim_below_85pct_holds_then_full_close(self, marks_dir):
        # leader opens $1000, trims $300 (30% cumulative < 85% -> HOLD, no partial
        # exit exists), then exits the rest (100% -> full close): ONE exit fill
        _flat_marks(marks_dir, px=100.0)
        m = MarksIndex(cache_dir=marks_dir.dir)
        a = actions([
            ("BTC", T0 + 10 * MS_MIN, "ENTRY", 10.0, 100.0, 10.0, 1, False),
            ("BTC", T0 + 50 * MS_MIN, "TRIM", -3.0, 100.0, 7.0, 1, False),
            ("BTC", T0 + 90 * MS_MIN, "EXIT", -7.0, 100.0, 0.0, 1, False),
        ])
        res = simulate_wallet_trips(a, zero_cost(), m, T0, END)
        t = res["trips"]
        assert len(t) == 1
        assert t.iloc[0]["n_exit_fills"] == 1      # FULL close only, never partial
        assert not t.iloc[0]["terminal"]

    def test_cumulative_trims_cross_85pct(self, marks_dir):
        # trims of 50% then 40% -> cumulative 90% >= 85% -> full close on the SECOND trim
        _flat_marks(marks_dir, px=100.0)
        m = MarksIndex(cache_dir=marks_dir.dir)
        a = actions([
            ("BTC", T0 + 10 * MS_MIN, "ENTRY", 10.0, 100.0, 10.0, 1, False),
            ("BTC", T0 + 50 * MS_MIN, "TRIM", -5.0, 100.0, 5.0, 1, False),
            ("BTC", T0 + 90 * MS_MIN, "TRIM", -4.0, 100.0, 1.0, 1, False),
        ])
        res = simulate_wallet_trips(a, zero_cost(), m, T0, END)
        t = res["trips"]
        assert len(t) == 1 and not t.iloc[0]["terminal"]
        # closed at the second trim, before window end
        assert t.iloc[0]["exit_fill_ts_last"] < T0 + 100 * MS_MIN

    def test_addon_grows_denominator_only(self, marks_dir):
        # open $1000 + addon $1000 -> denominator $2000. A $900 trim (90% of the open
        # but 45% cumulative) must NOT close; a further $900 (90% cumulative) closes.
        _flat_marks(marks_dir, px=100.0)
        m = MarksIndex(cache_dir=marks_dir.dir)
        a = actions([
            ("BTC", T0 + 10 * MS_MIN, "ENTRY", 10.0, 100.0, 10.0, 1, False),
            ("BTC", T0 + 20 * MS_MIN, "ADDON", 10.0, 100.0, 20.0, 1, False),
            ("BTC", T0 + 50 * MS_MIN, "TRIM", -9.0, 100.0, 11.0, 1, False),
            ("BTC", T0 + 90 * MS_MIN, "TRIM", -9.0, 100.0, 2.0, 1, False),
        ])
        res = simulate_wallet_trips(a, zero_cost(), m, T0, END)
        t = res["trips"]
        assert res["counters"]["entries"] == 1     # addon NOT copied
        assert len(t) == 1 and not t.iloc[0]["terminal"]
        assert t.iloc[0]["exit_fill_ts_last"] >= T0 + 90 * MS_MIN

    def test_leader_dust_residual_closes(self, marks_dir):
        # dust rule: cumulative reverse 3.2/4.0 = 80% < 85%, but the leader residual
        # ($0.80) is below the $1 dust threshold -> counts as fully closed
        _flat_marks(marks_dir, px=1.0)
        m = MarksIndex(cache_dir=marks_dir.dir)
        a = actions([
            ("BTC", T0 + 10 * MS_MIN, "ENTRY", 2.0, 1.0, 2.0, 1, False),
            ("BTC", T0 + 20 * MS_MIN, "ADDON", 2.0, 1.0, 4.0, 1, False),
            ("BTC", T0 + 50 * MS_MIN, "TRIM", -3.2, 1.0, 0.8, 1, False),
        ])
        res = simulate_wallet_trips(a, zero_cost(), m, T0, END)
        t = res["trips"]
        assert len(t) == 1 and not t.iloc[0]["terminal"]
        assert res["counters"]["trips_realized"] == 1

    def test_entry_below_dust_skipped(self, marks_dir):
        _flat_marks(marks_dir, px=1.0)
        m = MarksIndex(cache_dir=marks_dir.dir)
        a = actions([("BTC", T0 + 10 * MS_MIN, "ENTRY", 0.5, 1.0, 0.5, 1, False)])
        res = simulate_wallet_trips(a, zero_cost(), m, T0, END)
        assert res["counters"]["entries_dust_skipped"] == 1
        assert res["counters"]["entries"] == 0

    def test_terminal_mtm_included(self, marks_dir):
        # price steps from 100 to 110 halfway; leader never closes
        _flat_marks(marks_dir, step_px=lambda i: 100.0 if i < 3 * 24 * 60 else 110.0)
        m = MarksIndex(cache_dir=marks_dir.dir)
        a = actions([("BTC", T0 + 10 * MS_MIN, "ENTRY", 1.0, 100.0, 1.0, 1, False)])
        res = simulate_wallet_trips(a, zero_cost(), m, T0, END)
        t = res["trips"]
        assert len(t) == 1 and bool(t.iloc[0]["terminal"])
        # size = 150/100 = 1.5 ; terminal at 110 -> +$15
        assert t.iloc[0]["net_pnl"] == pytest.approx(15.0)
        assert res["total_pnl"] == pytest.approx(15.0)

    def test_cold_start_ignores_pre_window_journey(self, marks_dir):
        _flat_marks(marks_dir)
        m = MarksIndex(cache_dir=marks_dir.dir)
        a = actions([
            ("BTC", T0 - 5 * MS_MIN, "ENTRY", 1.0, 100.0, 1.0, 1, False),
            ("BTC", T0 + 100 * MS_MIN, "EXIT", -1.0, 100.0, 0.0, 1, False),
        ])
        res = simulate_wallet_trips(a, zero_cost(), m, T0, END)
        assert res["counters"]["entries"] == 0
        assert res["counters"]["leader_reduce_ignored_no_lot"] == 1
        assert len(res["trips"]) == 0


class TestReverseSemantics:
    def test_reverse_closes_and_never_opens(self, marks_dir):
        """Frozen (gate-b blocker #5): a leader REVERSAL closes our copy and NEVER
        opens a new one (v15_fixed_notional_signals.py canonical lifecycle)."""
        _flat_marks(marks_dir, px=100.0)
        m = MarksIndex(cache_dir=marks_dir.dir)
        a = actions([
            ("BTC", T0 + 10 * MS_MIN, "ENTRY", 1.0, 100.0, 1.0, 1, False),
            # leader flips long 1 -> short 1 (journey 2 opens for the LEADER only)
            ("BTC", T0 + 50 * MS_MIN, "REVERSE", -2.0, 100.0, -1.0, 2, False),
            # leader later exits the short: we must have NO lot
            ("BTC", T0 + 90 * MS_MIN, "EXIT", 1.0, 100.0, 0.0, 2, False),
        ])
        res = simulate_wallet_trips(a, zero_cost(), m, T0, END)
        assert res["counters"]["entries"] == 1              # only the original ENTRY
        assert res["counters"]["reverse_closes"] == 1
        assert res["counters"]["leader_reduce_ignored_no_lot"] == 1
        t = res["trips"]
        assert len(t) == 1 and not t.iloc[0]["terminal"]    # closed BY the reversal
        assert t.iloc[0]["side"] == 1
        assert res["counters"]["trips_terminal"] == 0       # nothing rides the flip

    def test_reverse_on_uncopied_position_no_entry(self, marks_dir):
        # pre-window open + in-window REVERSE: tracking-only, never an entry
        _flat_marks(marks_dir, px=100.0)
        m = MarksIndex(cache_dir=marks_dir.dir)
        a = actions([
            ("BTC", T0 - 5 * MS_MIN, "ENTRY", 1.0, 100.0, 1.0, 1, False),
            ("BTC", T0 + 50 * MS_MIN, "REVERSE", -2.0, 100.0, -1.0, 2, False),
        ])
        res = simulate_wallet_trips(a, zero_cost(), m, T0, END)
        assert res["counters"]["entries"] == 0
        assert len(res["trips"]) == 0


class TestExitBoundary:
    def test_late_exit_before_window_end(self, marks_dir):
        # marks gap: no mark within 60s of the exit signal, but one exists later and
        # BEFORE the window end -> exit late at that mark (counted)
        gap_lo, gap_hi = 100, 200        # minutes 100..199 missing
        _flat_marks(marks_dir, px=100.0, skip=lambda i: gap_lo <= i < gap_hi)
        m = MarksIndex(cache_dir=marks_dir.dir)
        a = actions([
            ("BTC", T0 + 10 * MS_MIN, "ENTRY", 1.0, 100.0, 1.0, 1, False),
            ("BTC", T0 + 100 * MS_MIN, "EXIT", -1.0, 100.0, 0.0, 1, False),
        ])
        res = simulate_wallet_trips(a, zero_cost(), m, T0, END)
        assert res["counters"]["exits_late"] == 1
        t = res["trips"]
        assert len(t) == 1 and not t.iloc[0]["terminal"]
        assert bool(t.iloc[0]["any_late_exit"])
        assert t.iloc[0]["exit_fill_ts_last"] == T0 + (gap_hi + 1) * MS_MIN

    def test_no_mark_before_end_falls_to_terminal(self, marks_dir):
        # marks stop before the exit signal; a future mark EXISTS but only BEYOND the
        # window end -> the exit is unpriced and the position falls to terminal MTM at
        # the window end (no mark beyond the boundary is ever read)
        last_mark_min = 3 * 24 * 60
        _flat_marks(marks_dir, px=100.0,
                    skip=lambda i: last_mark_min <= i < 8 * 24 * 60)
        m = MarksIndex(cache_dir=marks_dir.dir)     # next mark after gap is PAST END
        a = actions([
            ("BTC", T0 + 10 * MS_MIN, "ENTRY", 1.0, 100.0, 1.0, 1, False),
            ("BTC", T0 + (last_mark_min + 100) * MS_MIN, "EXIT", -1.0, 100.0, 0.0, 1,
             False),
        ])
        res = simulate_wallet_trips(a, zero_cost(), m, T0, END)
        assert res["counters"]["exits_unpriced_to_terminal"] == 1
        assert res["counters"]["exits_late"] == 0
        t = res["trips"]
        assert len(t) == 1 and bool(t.iloc[0]["terminal"])
        # terminal fill is stamped at the window end with the last causal mark
        assert t.iloc[0]["exit_fill_ts_last"] == END

    def test_entry_near_end_never_reads_past_boundary(self, marks_dir):
        # marks exist beyond END in the cache, but the bar closing exactly at END is
        # missing: an entry signal 30s before END whose first eligible mark closes
        # AFTER END must be DROPPED, not filled from beyond the boundary
        _flat_marks(marks_dir, px=100.0, n_min=9 * 24 * 60,
                    skip=lambda i: i == 7 * 24 * 60 - 1)
        m = MarksIndex(cache_dir=marks_dir.dir)
        a = actions([("BTC", END - 30_000, "ENTRY", 1.0, 100.0, 1.0, 1, False)])
        res = simulate_wallet_trips(a, zero_cost(), m, T0, END)
        assert res["counters"]["entries"] == 0
        assert res["counters"]["entries_dropped_no_mark"] == 1


class TestPortfolio:
    def test_duplicate_signal_coalesced(self, marks_dir):
        _flat_marks(marks_dir)
        m = MarksIndex(cache_dir=marks_dir.dir)
        a = actions([
            ("BTC", T0 + 10 * MS_MIN, "ENTRY", 1.0, 100.0, 1.0, 1, False),
            ("BTC", T0 + 20 * MS_MIN, "ENTRY", 1.0, 100.0, 2.0, 2, False),
        ])
        res = simulate_portfolio(a, zero_cost(), m, T0, END)
        assert res["counters"]["entries"] == 1
        assert res["counters"]["dup_coalesced"] == 1

    def test_cross_wallet_stacking_and_coin_side_cap(self, marks_dir):
        _flat_marks(marks_dir)
        m = MarksIndex(cache_dir=marks_dir.dir)
        # 8 wallets signal the same coin+side; coin-side cap = 2x$500 = $1000 -> 6 lots
        frames = [actions([("BTC", T0 + (10 + i) * MS_MIN, "ENTRY", 1.0, 100.0, 1.0, 1,
                            False)], wallet=f"0x{i:040x}") for i in range(8)]
        a = pd.concat(frames, ignore_index=True)
        res = simulate_portfolio(a, zero_cost(), m, T0, END)
        assert res["counters"]["entries"] == 6
        assert res["counters"]["entries_blocked_coin_side"] == 2

    def test_gross_cap(self, marks_dir):
        _flat_marks(marks_dir, coin="BTC")
        for i in range(12):
            _flat_marks(marks_dir, coin=f"C{i}")
        m = MarksIndex(cache_dir=marks_dir.dir)
        # 12 wallets on 12 different coins: gross cap 2.5x$500=$1250 -> 8 lots of $150
        frames = [actions([(f"C{i}", T0 + (10 + i) * MS_MIN, "ENTRY", 1.0, 100.0, 1.0, 1,
                            False)], wallet=f"0x{i:040x}") for i in range(12)]
        a = pd.concat(frames, ignore_index=True)
        res = simulate_portfolio(a, zero_cost(), m, T0, END)
        assert res["counters"]["entries"] == 8
        assert res["counters"]["entries_blocked_gross"] == 4

    def test_daily_series_and_final_equity(self, marks_dir):
        _flat_marks(marks_dir, step_px=lambda i: 100.0 if i < 3 * 24 * 60 else 110.0)
        m = MarksIndex(cache_dir=marks_dir.dir)
        a = actions([("BTC", T0 + 10 * MS_MIN, "ENTRY", 1.0, 100.0, 1.0, 1, False)])
        res = simulate_portfolio(a, zero_cost(), m, T0, END)
        d = res["daily"]
        assert len(d) == 7                                    # 7 test days
        assert d["daily_pnl"].sum() == pytest.approx(res["total_pnl"])
        assert res["final_equity"] == pytest.approx(INITIAL_EQUITY + res["total_pnl"])
        assert res["total_pnl"] == pytest.approx(15.0)


class TestEventLevelDD:
    def test_intraday_dip_caught_by_event_series(self, marks_dir):
        """Frozen (gate-b blocker #6): the MTM DD equity series starts at $500 and
        updates at EVERY simulated event. An intraday dip visible at an event but
        recovered by the daily endpoint MUST register in max_mtm_dd_frac."""
        # BTC dips 100 -> 90 during minutes 300..330 of day 1, recovers after
        _flat_marks(marks_dir, coin="BTC",
                    step_px=lambda i: 90.0 if 300 <= i < 330 else 100.0)
        _flat_marks(marks_dir, coin="ETH", px=50.0)
        m = MarksIndex(cache_dir=marks_dir.dir)
        a = pd.concat([
            actions([("BTC", T0 + 10 * MS_MIN, "ENTRY", 1.0, 100.0, 1.0, 1, False)],
                    wallet="0xa"),
            # second wallet's entry event lands DURING the dip -> equity sampled there
            actions([("ETH", T0 + 310 * MS_MIN, "ENTRY", 1.0, 50.0, 1.0, 1, False)],
                    wallet="0xb"),
        ], ignore_index=True)
        res = simulate_portfolio(a, zero_cost(), m, T0, END)
        # at the dip event: BTC lot = 1.5 units x (90 - 100) = -$15 -> equity 485
        # peak 500 -> dd 3%; daily endpoints alone would show ~0
        assert res["max_mtm_dd_frac"] == pytest.approx(15.0 / 500.0, rel=1e-6)
        d = res["daily"]
        endpoint_dd = (d["equity"].cummax() - d["equity"]).max() / 500.0
        assert endpoint_dd < 0.001                # daily endpoints missed the dip

    def test_dd_starts_at_initial_equity(self, marks_dir):
        # immediate loss below $500 registers against the INITIAL equity peak
        _flat_marks(marks_dir, step_px=lambda i: 100.0 if i < 60 else 80.0)
        m = MarksIndex(cache_dir=marks_dir.dir)
        a = actions([
            ("BTC", T0 + 10 * MS_MIN, "ENTRY", 1.0, 100.0, 1.0, 1, False),
            ("BTC", T0 + 120 * MS_MIN, "EXIT", -1.0, 80.0, 0.0, 1, False),
        ])
        res = simulate_portfolio(a, zero_cost(), m, T0, END)
        # loss = 1.5 x 20 = $30 -> dd vs initial 500 = 6%
        assert res["max_mtm_dd_frac"] == pytest.approx(30.0 / 500.0, rel=1e-6)


class TestDropout:
    def _mk(self, marks_dir, n=40):
        _flat_marks(marks_dir)
        rows = []
        for i in range(n):
            rows.append(("BTC", T0 + (10 + 20 * i) * MS_MIN, "ENTRY", 1.0, 100.0, 1.0,
                         i + 1, False))
            rows.append(("BTC", T0 + (20 + 20 * i) * MS_MIN, "EXIT", -1.0, 100.0, 0.0,
                         i + 1, False))
        return actions(rows), MarksIndex(cache_dir=marks_dir.dir)

    def test_deterministic_per_seed(self, marks_dir):
        a, m = self._mk(marks_dir)
        r1 = simulate_portfolio(a, zero_cost(), m, T0, END, dropout_seed=17)
        r2 = simulate_portfolio(a, zero_cost(), m, T0, END, dropout_seed=17)
        assert r1["counters"] == r2["counters"]
        pd.testing.assert_frame_equal(r1["trips"], r2["trips"])

    def test_seeds_differ_and_exits_ignored(self, marks_dir):
        a, m = self._mk(marks_dir)
        outs = {s: simulate_portfolio(a, zero_cost(), m, T0, END, dropout_seed=s)
                for s in (17, 42, 137)}
        dropped = {s: o["counters"]["dropout_dropped"] for s, o in outs.items()}
        for s, o in outs.items():
            # every non-dropped journey round-trips; dropped journeys' exits are ignored
            assert o["counters"]["entries"] == 40 - dropped[s]
            assert o["counters"]["trips_realized"] == 40 - dropped[s]
            assert o["counters"]["leader_reduce_ignored_no_lot"] == 0
            assert dropped[s] > 0
        # event-hash: at least two seeds must disagree on the dropped set size or trips
        vals = {(dropped[s], outs[s]["total_pnl"]) for s in outs}
        assert len({d for d, _ in vals}) > 1 or len(vals) > 1

    def test_event_hash_order_independent(self):
        assert event_dropout(17, "0xa", "BTC", 1) == event_dropout(17, "0xa", "BTC", 1)
        marks = [event_dropout(42, f"0x{i}", "BTC", i) for i in range(200)]
        frac = sum(marks) / len(marks)
        assert 0.25 < frac < 0.55                    # p=0.4, loose deterministic bound


class TestScenarios:
    def test_frozen_constants(self):
        b = scenario_base()
        w = scenario_worst()
        assert b.fee_oneway("BTC") == pytest.approx(4.32e-4)
        assert b.fee_oneway("xyz:FOO") == pytest.approx(8.64e-4)
        assert w.fee_oneway("BTC") == pytest.approx(4.5e-4)
        assert w.fee_oneway("xyz:FOO") == pytest.approx(9.0e-4)
        assert w.slip_oneway("BTC") == pytest.approx(7.0e-4)   # flat everywhere in WORST
        assert b.slip_oneway("UNCALIBRATED_COIN") == pytest.approx(4.7e-4)
        assert b.slip_oneway("BTC") < 1e-4                     # measured L2, ~0.13bps
