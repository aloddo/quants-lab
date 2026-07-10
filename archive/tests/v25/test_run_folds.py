"""v25 orchestrator unit tests: frozen fold windows, pass criteria (terminal-$
concentration, zero denominators, no coin trip-count gate), Bonferroni fallback
trigger + manifest ordering, automated winner selection. Synthetic data only."""
import json

import numpy as np
import pandas as pd
import pytest

from v25_common import DROPOUT_SEEDS, folds
from v25_run_folds import RULES, _boot_with_fallback, _criteria, evaluate


class TestFoldWindows:
    def test_exact_frozen_bounds(self):
        f = folds()
        assert len(f) == 5
        def ms(s):
            return int(pd.Timestamp(s).value // 10**6)
        # train_k = [2025-12-01, test_start_k); asof_k = test_start_k (gate-b blocker #2)
        assert all(x["train_start_ms"] == ms("2025-12-01") for x in f)
        expected = [("2026-03-01", "2026-03-22", 21),
                    ("2026-03-22", "2026-04-12", 21),
                    ("2026-04-12", "2026-05-03", 21),
                    ("2026-05-03", "2026-05-24", 21),
                    ("2026-05-24", "2026-06-11", 18)]
        for x, (s, e, d) in zip(f, expected):
            assert x["asof_ms"] == ms(s)
            assert x["test_end_ms"] == ms(e)
            assert x["test_days"] == d
        assert sum(x["test_days"] for x in f) == 102


def _rule_report(fold_list, dd=0.01, worst=10.0, stress=5.0, pnl=50.0):
    return {
        "folds": {f["fold"]: {
            "total_pnl_incl_terminal": pnl,
            "worst_total_pnl": worst,
            "stress_total_pnl_by_seed": {str(s): stress for s in DROPOUT_SEEDS},
            "max_mtm_dd_frac": dd,
        } for f in fold_list},
        "total_realized_trips": 200,
        "total_test_days": 102,
    }


def _trips(rows):
    return pd.DataFrame(rows, columns=["entity", "coin", "net_pnl", "terminal", "side",
                                       "net_bps"])


class TestCriteria:
    def test_terminal_dollars_included_in_concentration(self):
        fold_list = folds()
        r = _rule_report(fold_list)
        # 10 realized trips $1 each on 10 coins/entities; ONE terminal row with $100 on
        # coin K. Terminal-$ INCLUSION makes coin K 100/110 > 40% -> FAIL. Excluding
        # terminal rows (the old bug) would have passed 10 x 10%.
        rows = [(f"e{i}", f"C{i}", 1.0, False, 1, 10.0) for i in range(10)]
        rows.append(("e0", "K", 100.0, True, 1, 10.0))
        c = _criteria(r, _trips(rows), fold_list)
        assert c["coin_pnl_conc_le_40pct"] is False
        assert c["entity_pnl_conc_le_40pct"] is False    # e0 carries 101/110 too
        assert c["entity_trip_conc_le_30pct"] is True    # trip count = realized only

    def test_coin_trip_count_gate_not_registered(self):
        fold_list = folds()
        r = _rule_report(fold_list)
        # ALL realized trips on one coin, balanced $: the unregistered coin trip-count
        # gate must NOT exist (spec: no other concentration tests exist)
        rows = [(f"e{i}", "BTC", 1.0 if i % 2 else -1.0, False, 1, 10.0)
                for i in range(20)]
        c = _criteria(r, _trips(rows), fold_list)
        assert "coin_trip_conc_le_30pct" not in c
        assert "coin_pnl_conc_le_40pct" in c

    def test_zero_denominators_trivially_pass(self):
        fold_list = folds()
        r = _rule_report(fold_list)
        # zero trips at all: every concentration denominator is exactly 0 -> pass
        c = _criteria(r, pd.DataFrame(), fold_list)
        assert c["entity_trip_conc_le_30pct"] is True
        assert c["entity_pnl_conc_le_40pct"] is True
        assert c["coin_pnl_conc_le_40pct"] is True
        assert c["min_entities_15"] is False             # but entity minimum still fails
        # net $ that sums to |0| per group: $ denominators 0 -> trivially pass
        rows = [(f"e{i}", f"C{i}", 0.0, False, 1, 0.0) for i in range(20)]
        c2 = _criteria(r, _trips(rows), fold_list)
        assert c2["entity_pnl_conc_le_40pct"] is True
        assert c2["coin_pnl_conc_le_40pct"] is True

    def test_dd_gate_uses_event_level_value(self):
        fold_list = folds()
        r = _rule_report(fold_list, dd=0.051)
        c = _criteria(r, pd.DataFrame(), fold_list)
        assert c["no_fold_mtm_dd_gt_5pct"] is False


class TestBootFallback:
    def test_joint_path_no_trigger(self, tmp_path):
        rng = np.random.default_rng(3)
        segs = {"R1": [rng.normal(1, 1, 21)], "R2": [rng.normal(1, 1, 21)]}
        b7, b14, method = _boot_with_fallback(segs, tmp_path)
        assert method == "joint_maxstat"
        assert not (tmp_path / "manifest.json").exists() or \
            "bonferroni_trigger" not in json.load(open(tmp_path / "manifest.json"))

    def test_exception_triggers_fallback_and_writes_manifest(self, tmp_path):
        # mismatched segment lengths make the joint bootstrap raise -> frozen trigger
        segs = {"R1": [np.ones(21)], "R2": [np.ones(19)]}
        b7, b14, method = _boot_with_fallback(segs, tmp_path)
        assert method == "bonferroni_fallback"
        man = json.load(open(tmp_path / "manifest.json"))
        trig = man["bonferroni_trigger"]
        assert trig["type"] == "exception"
        assert trig["fallback"] == "bonferroni_97.5_one_sided"
        assert np.isfinite(b7["rules"]["R1"]["lcb_maxstat"])

    def test_nonfinite_lcb_triggers_fallback(self, tmp_path):
        segs = {"R1": [], "R2": []}
        b7, b14, method = _boot_with_fallback(segs, tmp_path)
        assert method == "bonferroni_fallback"
        man = json.load(open(tmp_path / "manifest.json"))
        assert man["bonferroni_trigger"]["type"] == "nonfinite_lcb"


def _fake_results(fold_list, mean_daily):
    """Synthetic results dict for evaluate(): per rule x scenario x fold x seed."""
    rng = np.random.default_rng(11)
    results = {}
    for rule, mu in mean_daily.items():
        for f in fold_list:
            k, nd = f["fold"], f["test_days"]
            daily = pd.DataFrame({"daily_pnl": rng.normal(mu, 0.2, nd)})
            trips = pd.DataFrame({
                "entity": [f"e{i % 20}" for i in range(40)],
                "coin": [f"C{i % 12}" for i in range(40)],
                "wallet": [f"0x{i % 20}" for i in range(40)],
                "net_pnl": np.full(40, mu),
                "net_bps": np.full(40, mu * 10),
                "side": [1 if i % 2 else -1 for i in range(40)],
                "terminal": [False] * 38 + [True] * 2,
            })
            base = {"trips": trips, "daily": daily, "total_pnl": float(mu * nd),
                    "final_equity": 500.0 + mu * nd, "max_mtm_dd_frac": 0.01,
                    "counters": {}}
            results[(rule, "BASE", k, None)] = base
            results[(rule, "WORST", k, None)] = {**base, "total_pnl": float(mu * nd * 0.5)}
            for s in DROPOUT_SEEDS:
                results[(rule, "BASE", k, s)] = {**base, "total_pnl": float(mu * nd * 0.6)}
    return results


class TestEvaluateWinner:
    def test_winner_is_higher_lcb_among_passers(self, tmp_path):
        fold_list = folds()
        results = _fake_results(fold_list, {"R1": 2.0, "R2": -1.0})
        verdict = evaluate(fold_list, results, tmp_path)
        assert verdict["per_rule"]["R1"]["PASS"] is True
        assert verdict["per_rule"]["R2"]["PASS"] is False
        assert verdict["winner"]["rule"] == "R1"
        assert (tmp_path / "verdict.json").exists()
        # required reporting (gate-b blocker #7): per rule-fold activity profile
        fold1 = verdict["per_rule"]["R1"]["folds"][1]
        for key in ("realized_trip_mean_net_bps", "realized_trip_mean_net_usd",
                    "trips_per_day", "n_trips_long", "n_trips_short"):
            assert key in fold1
        assert fold1["n_trips_long"] + fold1["n_trips_short"] == fold1["n_trips_realized"]

    def test_no_passers_is_kill(self, tmp_path):
        fold_list = folds()
        results = _fake_results(fold_list, {"R1": -1.0, "R2": -2.0})
        verdict = evaluate(fold_list, results, tmp_path)
        assert verdict["winner"] is None
        assert verdict["recommendation"] == "KILL"
