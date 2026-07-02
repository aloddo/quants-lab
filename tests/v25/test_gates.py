"""v25 gate + entity-clustering unit tests (synthetic data only)."""
import numpy as np
import pandas as pd
import pytest

from v25_common import MS_DAY, MS_MIN, MarksIndex
from v25_gates import (cluster_entities, exclusion_summary, one_to_one_matched,
                       wallet_gate_row)

ASOF = pd.Timestamp("2026-03-01").value // 10**6      # F1 asof (frozen)


def entries_df(rows):
    return pd.DataFrame(rows, columns=["coin", "side", "ts", "wallet"])


class TestOneToOneMatcher:
    def test_codex_regression_10v10(self):
        """Codex gate-b blocker #3 synthetic case: 10-vs-10 where the rolling-window
        count gave matched=4 (one A event reused against 4 later B events) but strict
        one-to-one greedy gives 1 -- and therefore NO edge."""
        t0 = ASOF - 20 * MS_DAY
        rows = [("BTC", 1, t0, "0xaaa")]
        # 4 B events inside 60s of A's single clustered event
        for i in range(1, 5):
            rows.append(("BTC", 1, t0 + i * 10_000, "0xbbb"))
        # remaining 9 A and 6 B events far apart from everything (> 60s gaps)
        for i in range(9):
            rows.append(("BTC", 1, t0 + (i + 1) * 100_000_000, "0xaaa"))
        for i in range(6):
            rows.append(("BTC", 1, t0 + (i + 1) * 100_000_000 + 5_000_000, "0xbbb"))
        mapping, edges = cluster_entities(entries_df(rows), {"0xaaa", "0xbbb"})
        e = edges.iloc[0]
        assert e["matched"] == 1                       # NOT 4
        assert e["min_entries"] == 10
        assert e["overlap_frac"] == pytest.approx(0.1)
        assert not e["edge"]
        assert mapping["0xaaa"] == "0xaaa" and mapping["0xbbb"] == "0xbbb"

    def test_greedy_pairs_consume_both(self):
        # perfectly paired: each event matches exactly once
        a = np.array([0, 100_000, 200_000], dtype="int64")
        b = a + 30_000
        assert one_to_one_matched(a, b, 60_000) == 3
        # one B inside the window of two As: only ONE match total
        assert one_to_one_matched(np.array([0, 40_000]), np.array([20_000]), 60_000) == 1
        assert one_to_one_matched(np.array([0]), np.array([70_000]), 60_000) == 0


class TestClustering:
    def test_known_components(self):
        # A and B enter the same coin, same side, within 60s, every time (100% overlap).
        # C enters the same coin but hours away. Expected components: {A,B}, {C}.
        rows = []
        t0 = ASOF - 20 * MS_DAY
        for i in range(10):
            rows.append(("BTC", 1, t0 + i * 3_600_000, "0xbbb"))
            rows.append(("BTC", 1, t0 + i * 3_600_000 + 30_000, "0xaaa"))
            rows.append(("BTC", 1, t0 + i * 3_600_000 + 1_800_000, "0xccc"))
        mapping, edges = cluster_entities(entries_df(rows), {"0xaaa", "0xbbb", "0xccc"})
        assert mapping["0xaaa"] == "0xaaa"
        assert mapping["0xbbb"] == "0xaaa"          # lexicographically smallest rep
        assert mapping["0xccc"] == "0xccc"
        e = edges[edges["edge"]]
        assert len(e) == 1
        assert {e.iloc[0]["wallet_a"], e.iloc[0]["wallet_b"]} == {"0xaaa", "0xbbb"}
        assert e.iloc[0]["matched"] == 10           # one-to-one perfect pairing

    def test_overlap_threshold_strict_gt(self):
        # exactly 30% overlap must NOT create an edge (spec: >30%)
        rows = []
        t0 = ASOF - 20 * MS_DAY
        for i in range(10):
            rows.append(("ETH", 1, t0 + i * 3_600_000, "0xaaa"))
        for i in range(3):   # 3 of B's 10 entries co-occur one-to-one -> 3/min(10,10)=30%
            rows.append(("ETH", 1, t0 + i * 3_600_000 + 10_000, "0xbbb"))
        for i in range(7):
            rows.append(("ETH", 1, t0 + 100 * MS_DAY // 10 + i * 3_600_000, "0xbbb"))
        mapping, edges = cluster_entities(entries_df(rows), {"0xaaa", "0xbbb"})
        assert edges.iloc[0]["matched"] == 3
        assert mapping["0xaaa"] == "0xaaa" and mapping["0xbbb"] == "0xbbb"
        assert not edges["edge"].any()

    def test_opposite_side_no_edge(self):
        rows = []
        t0 = ASOF - 20 * MS_DAY
        for i in range(10):
            rows.append(("SOL", 1, t0 + i * 3_600_000, "0xaaa"))
            rows.append(("SOL", -1, t0 + i * 3_600_000 + 5_000, "0xbbb"))
        mapping, edges = cluster_entities(entries_df(rows), {"0xaaa", "0xbbb"})
        assert mapping["0xbbb"] == "0xbbb"

    def test_transitive_component_single_rep(self):
        # A-B edge and B-C edge -> one component rep=A even without an A-C edge
        rows = []
        t0 = ASOF - 20 * MS_DAY
        for i in range(10):
            rows.append(("BTC", 1, t0 + i * 3_600_000, "0xaaa"))
            rows.append(("BTC", 1, t0 + i * 3_600_000 + 20_000, "0xbbb"))
            rows.append(("ETH", 1, t0 + i * 3_600_000, "0xbbb"))
            rows.append(("ETH", 1, t0 + i * 3_600_000 + 20_000, "0xccc"))
        mapping, _ = cluster_entities(entries_df(rows),
                                      {"0xaaa", "0xbbb", "0xccc"})
        assert mapping["0xaaa"] == mapping["0xbbb"] == mapping["0xccc"] == "0xaaa"


def _actions(rows):
    cols = ["coin", "ts", "action_type", "signed_size", "price", "position_after",
            "mark", "is_liquidation", "journey_id"]
    df = pd.DataFrame(rows, columns=cols)
    df["wallet"] = "0xw"
    return df


def _base_wallet_actions(open_pos=False, n_days=25, last_gap_days=2):
    """n_days daily closed BTC journeys (+$4 realized each), the last one closing
    last_gap_days before asof. Passes the frozen activity gate (>= 20 distinct active
    days, last fill <= 7d before asof) and coverage (all marks valid)."""
    rows = []
    for d in range(n_days):
        t_open = ASOF - (last_gap_days + n_days - 1 - d) * MS_DAY
        rows.append(("BTC", t_open, "ENTRY", 1.0, 100.0, 1.0, 100.0, False, d + 1))
        rows.append(("BTC", t_open + 4 * 3_600_000, "EXIT", -1.0, 104.0, 0.0, 104.0,
                     False, d + 1))
    if open_pos:
        rows.append(("OPN", ASOF - MS_DAY, "ENTRY", 1.0, 100.0, 1.0, 100.0, False, 1))
    return _actions(rows)


class TestActivityGate:
    def test_frozen_thresholds_pass(self, marks_dir):
        marks = MarksIndex(cache_dir=marks_dir.dir)
        row = wallet_gate_row("0xw", _base_wallet_actions(), ASOF, marks)
        assert row["n_active_days"] >= 20
        assert row["pass_activity"] is True and row["eligible"]

    def test_stale_last_fill_8d_fails(self, marks_dir):
        # 25 active days but last fill 8d before asof: FAILS the frozen 7d rule
        # (the vetoed 14d build would have passed this wallet)
        marks = MarksIndex(cache_dir=marks_dir.dir)
        row = wallet_gate_row("0xw", _base_wallet_actions(last_gap_days=8), ASOF, marks)
        assert row["pass_activity"] is False and not row["eligible"]

    def test_too_few_active_days_fails(self, marks_dir):
        # recent but only 10 distinct active days: FAILS the frozen >= 20 rule
        marks = MarksIndex(cache_dir=marks_dir.dir)
        row = wallet_gate_row("0xw", _base_wallet_actions(n_days=10), ASOF, marks)
        assert row["n_active_days"] == 10
        assert row["pass_activity"] is False and not row["eligible"]


class TestCoverageGate:
    def test_journey_level_over_5pct_fails(self, marks_dir):
        # 2 of 25 journeys carry an unmarked action -> 8% > 5% -> EXCLUDED
        marks = MarksIndex(cache_dir=marks_dir.dir)
        wdf = _base_wallet_actions()
        idx = wdf.index[wdf["journey_id"].isin([3, 7]) & (wdf["action_type"] == "ENTRY")]
        wdf.loc[idx, "mark"] = np.nan
        row = wallet_gate_row("0xw", wdf, ASOF, marks)
        assert row["unmarkable_frac"] == pytest.approx(2 / 25)
        assert row["pass_coverage"] is False and not row["eligible"]

    def test_journey_level_under_5pct_passes(self, marks_dir):
        # 1 of 25 journeys unmarkable -> 4% <= 5% -> passes (journey-level, NOT the old
        # action-level 90% rule)
        marks = MarksIndex(cache_dir=marks_dir.dir)
        wdf = _base_wallet_actions()
        idx = wdf.index[(wdf["journey_id"] == 3) & (wdf["coin"] == "BTC")]
        wdf.loc[idx, "mark"] = np.nan     # BOTH actions of journey 3 unmarked: 1 journey
        row = wallet_gate_row("0xw", wdf, ASOF, marks)
        assert row["unmarkable_frac"] == pytest.approx(1 / 25)
        assert row["pass_coverage"] is True and row["eligible"]

    def test_closed_journeys_only(self, marks_dir):
        """Gate-b round-2 residual #1: the unmarkable fraction is computed over CLOSED
        train journeys only (exit_ts <= asof) -- an OPEN unmarkable journey must not
        enter the numerator or the denominator."""
        m0 = (ASOF // MS_MIN - 10) * MS_MIN
        marks_dir("OPN", [m0], [100.0])           # open-bag gate still passes (flat)
        marks = MarksIndex(cache_dir=marks_dir.dir)
        wdf = _base_wallet_actions(open_pos=True)  # 25 closed BTC + 1 OPEN OPN journey
        idx = wdf.index[wdf["coin"] == "OPN"]
        wdf.loc[idx, "mark"] = np.nan              # the OPEN journey is unmarkable
        row = wallet_gate_row("0xw", wdf, ASOF, marks)
        assert row["n_closed_journeys"] == 25
        assert row["unmarkable_frac"] == pytest.approx(0.0)   # NOT 1/26
        assert row["pass_coverage"] is True


class TestOpenBagGate:
    def test_zero_open_passes(self, marks_dir):
        marks = MarksIndex(cache_dir=marks_dir.dir)
        row = wallet_gate_row("0xw", _base_wallet_actions(open_pos=False), ASOF, marks)
        assert row["pass_open_bag"] is True
        assert row["open_mtm_usd"] == 0.0
        assert row["eligible"]

    def test_exactly_minus_10pct_passes_gte(self, marks_dir):
        # trailing 30d |PnL| = 100; open MTM exactly -10 -> >= -10% PASSES (frozen >=)
        m0 = (ASOF // MS_MIN - 10) * MS_MIN
        marks_dir("OPN", [m0], [90.0])         # basis 100, mark 90 -> mtm -10
        marks = MarksIndex(cache_dir=marks_dir.dir)
        row = wallet_gate_row("0xw", _base_wallet_actions(open_pos=True), ASOF, marks)
        assert row["trail30_abs_pnl"] == pytest.approx(100.0)
        assert row["open_mtm_usd"] == pytest.approx(-10.0)
        assert row["pass_open_bag"] is True

    def test_below_minus_10pct_fails(self, marks_dir):
        m0 = (ASOF // MS_MIN - 10) * MS_MIN
        marks_dir("OPN", [m0], [89.9])
        marks = MarksIndex(cache_dir=marks_dir.dir)
        row = wallet_gate_row("0xw", _base_wallet_actions(open_pos=True), ASOF, marks)
        assert row["pass_open_bag"] is False
        assert not row["eligible"]

    def test_missing_mark_fail_closed(self, marks_dir):
        marks = MarksIndex(cache_dir=marks_dir.dir)   # no OPN series at all
        row = wallet_gate_row("0xw", _base_wallet_actions(open_pos=True), ASOF, marks)
        assert row["pass_open_bag"] is False
        assert row["fail_open_bag_missing_mark"] is True


class TestOtherGates:
    def test_liquidation_excludes(self, marks_dir):
        marks = MarksIndex(cache_dir=marks_dir.dir)
        wdf = _base_wallet_actions()
        wdf.loc[wdf.index[-1], "is_liquidation"] = True
        row = wallet_gate_row("0xw", wdf, ASOF, marks)
        assert row["pass_liquidation"] is False and not row["eligible"]

    def test_exclusion_counts(self, marks_dir):
        marks = MarksIndex(cache_dir=marks_dir.dir)
        rows = [wallet_gate_row("0xw", _base_wallet_actions(), ASOF, marks)]
        wdf = _base_wallet_actions()
        wdf.loc[wdf.index[-1], "is_liquidation"] = True
        r2 = wallet_gate_row("0xliq", wdf, ASOF, marks)
        r2["wallet"] = "0xliq"
        rows.append(r2)
        rep = pd.DataFrame(rows)
        summ = exclusion_summary(rep)
        assert summ["n_wallets"] == 2
        assert summ["n_eligible"] == 1
        assert summ["fail_liquidation"] == 1
