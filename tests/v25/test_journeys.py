"""v25 journey reconstruction tests: opening_journey_id / closing_journey_id semantics
(gate-b blocker #4). A REVERSE row carries TWO journey ids; grouping by back-compat
journey_id corrupts both legs -- these tests pin the correct split."""
import numpy as np
import pandas as pd
import pytest

from v25_common import LEADER_FEE_RATE, build_journeys

T0 = pd.Timestamp("2026-01-05").value // 10**6


def actions(rows):
    cols = ["coin", "ts", "action_type", "signed_size", "price", "position_after",
            "mark", "is_liquidation", "journey_id", "opening_journey_id",
            "closing_journey_id"]
    df = pd.DataFrame(rows, columns=cols)
    df["wallet"] = "0xw"
    return df


class TestReverseSplit:
    def _reverse_actions(self):
        # ENTRY long 1 @100 (j1); REVERSE -2 @110 -> short 1 (closes j1, opens j2;
        # back-compat journey_id = OPENING side = 2); EXIT +1 @105 (closes j2).
        return actions([
            ("BTC", T0, "ENTRY", 1.0, 100.0, 1.0, 100.0, False, 1, 1, np.nan),
            ("BTC", T0 + 3_600_000, "REVERSE", -2.0, 110.0, -1.0, 110.0, False, 2, 2, 1.0),
            ("BTC", T0 + 7_200_000, "EXIT", 1.0, 105.0, 0.0, 105.0, False, 2, 2, 2.0),
        ])

    def test_two_journeys_correct_legs(self):
        j = build_journeys(self._reverse_actions()).sort_values("journey_id")
        assert len(j) == 2
        j1, j2 = j.iloc[0], j.iloc[1]
        # closing leg j1: long 1 from 100 closed at 110 by the REVERSE -> +10
        assert j1["journey_id"] == 1
        assert j1["side"] == 1
        assert j1["realized_pnl"] == pytest.approx(10.0)
        assert j1["exit_ts"] == T0 + 3_600_000
        # opening leg j2: short 1 from 110 (basis = REVERSE price), closed at 105 -> +5
        assert j2["journey_id"] == 2
        assert j2["side"] == -1
        assert j2["entry_ts"] == T0 + 3_600_000
        assert j2["realized_pnl"] == pytest.approx(5.0)
        assert j2["exit_ts"] == T0 + 7_200_000

    def test_reverse_fee_split_and_net(self):
        j = build_journeys(self._reverse_actions()).sort_values("journey_id")
        j1, j2 = j.iloc[0], j.iloc[1]
        # canonical fee model: 4.32bps per fill side; REVERSE fee (2 units @110) splits
        # proportionally: 1 unit closing / 1 unit opening
        rev_fee_half = 1.0 * 110.0 * LEADER_FEE_RATE
        fee_j1 = 1.0 * 100.0 * LEADER_FEE_RATE + rev_fee_half
        fee_j2 = rev_fee_half + 1.0 * 105.0 * LEADER_FEE_RATE
        assert j1["fees_paid"] == pytest.approx(fee_j1)
        assert j2["fees_paid"] == pytest.approx(fee_j2)
        assert j1["net_realized_pnl"] == pytest.approx(10.0 - fee_j1)
        assert j2["net_realized_pnl"] == pytest.approx(5.0 - fee_j2)

    def test_backcompat_grouping_would_corrupt(self):
        # sanity: naive groupby journey_id assigns the REVERSE row wholly to j2 and
        # leaves j1 without a close -- the reconstruction must NOT do that
        a = self._reverse_actions()
        j = build_journeys(a)
        closed = j[j["exit_ts"].notna()]
        assert len(closed) == 2          # BOTH legs closed (naive grouping closes only 1)


class TestPlainJourneys:
    def test_entry_trim_exit(self):
        a = actions([
            ("ETH", T0, "ENTRY", 2.0, 100.0, 2.0, 100.0, False, 1, 1, np.nan),
            ("ETH", T0 + 3_600_000, "TRIM", -1.0, 110.0, 1.0, 110.0, False, 1, 1, 1.0),
            ("ETH", T0 + 7_200_000, "EXIT", -1.0, 120.0, 0.0, 120.0, False, 1, 1, 1.0),
        ])
        j = build_journeys(a)
        assert len(j) == 1
        r = j.iloc[0]
        assert r["realized_pnl"] == pytest.approx(10.0 + 20.0)
        # pre-fill peak capture (m02 parity): 2 units marked at the TRIM price 110
        assert r["max_notional"] == pytest.approx(220.0)
        assert r["duration_h"] == pytest.approx(2.0)

    def test_addon_average_cost(self):
        a = actions([
            ("ETH", T0, "ENTRY", 1.0, 100.0, 1.0, 100.0, False, 1, 1, np.nan),
            ("ETH", T0 + 60_000, "ADDON", 1.0, 120.0, 2.0, 120.0, False, 1, 1, 1.0),
            ("ETH", T0 + 3_600_000, "EXIT", -2.0, 130.0, 0.0, 130.0, False, 1, 1, 1.0),
        ])
        j = build_journeys(a)
        assert j.iloc[0]["realized_pnl"] == pytest.approx(2 * (130 - 110))  # basis 110

    def test_carried_in_leg_skipped_fail_closed(self):
        # TRIM/EXIT with no observed opening ENTRY: no journey is fabricated
        a = actions([
            ("SOL", T0, "TRIM", -1.0, 50.0, 1.0, 50.0, False, 1, 1, 1.0),
            ("SOL", T0 + 60_000, "EXIT", -1.0, 55.0, 0.0, 55.0, False, 1, 1, 1.0),
        ])
        assert len(build_journeys(a)) == 0

    def test_open_journey_and_unmarkable_flag(self):
        a = actions([
            ("SOL", T0, "ENTRY", 1.0, 50.0, 1.0, np.nan, False, 1, 1, np.nan),
        ])
        j = build_journeys(a)
        r = j.iloc[0]
        assert np.isnan(r["exit_ts"]) and r["open_size"] == pytest.approx(1.0)
        assert bool(r["unmarkable"])     # NaN mark on its only action

    def test_spot_excluded(self):
        a = actions([
            ("@107", T0, "ENTRY", 1.0, 50.0, 1.0, 50.0, False, 1, 1, np.nan),
        ])
        assert len(build_journeys(a)) == 0
