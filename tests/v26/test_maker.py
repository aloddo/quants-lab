"""Maker close-only fill model: both sides, timeout, cancellation, exit fallback
anchored at expiry, fill-rate dual eval (worse variant wins). Synthetic marks."""
import numpy as np
import pandas as pd

from v25_common import MS_DAY, MS_MIN, MarksIndex
from v26_maker import maker_entry, maker_exit

T0 = pd.Timestamp("2026-03-01").value // 10**6
END = T0 + 2 * MS_DAY


def idx(marks_dir, closes, coin="BTC", minutes=None):
    minutes = minutes or [T0 + i * MS_MIN for i in range(len(closes))]
    marks_dir(coin, minutes, closes)
    return MarksIndex(cache_dir=marks_dir.dir)


class TestMakerEntry:
    def test_buy_fills_on_close_at_or_below_post(self, marks_dir):
        m = idx(marks_dir, [100, 99.5, 101])
        r = maker_entry(m, "BTC", T0, +1, END)
        # post = mark at signal+2s = close 100 @ T0+60s; next close 99.5 <= 100
        assert r["filled"] and r["post_px"] == 100.0
        assert r["fill_ts"] == T0 + 2 * MS_MIN and r["fill_px"] == 100.0

    def test_sell_fills_on_close_at_or_above_post(self, marks_dir):
        m = idx(marks_dir, [100, 100.5, 99])
        r = maker_entry(m, "BTC", T0, -1, END)
        assert r["filled"] and r["fill_px"] == 100.0

    def test_no_cross_is_missed_not_slipped(self, marks_dir):
        m = idx(marks_dir, [100, 100.4, 101])       # price runs away from our BUY
        r = maker_entry(m, "BTC", T0, +1, END)
        assert not r["filled"] and r["reason"] == "maker_no_cross"

    def test_timeout_no_close_within_60s(self, marks_dir):
        m = idx(marks_dir, [100, 99.0], minutes=[T0, T0 + 10 * MS_MIN])
        r = maker_entry(m, "BTC", T0, +1, END)      # next close 9min past post
        assert not r["filled"] and r["reason"] == "maker_no_cross"

    def test_leader_exit_pre_fill_cancels(self, marks_dir):
        m = idx(marks_dir, [100, 99.5, 101])
        r = maker_entry(m, "BTC", T0, +1, END, mirror_ts=T0 + 90_000)
        assert not r["filled"] and r["reason"] == "maker_cancelled"

    def test_no_post_mark(self, marks_dir):
        m = idx(marks_dir, [100.0], minutes=[T0 + 30 * MS_MIN])   # nothing within 60s
        r = maker_entry(m, "BTC", T0, +1, END)
        assert not r["filled"] and r["reason"] == "maker_no_post"


class TestMakerExit:
    def test_maker_exit_fills_at_post_no_slip(self, marks_dir):
        m = idx(marks_dir, [100, 100.6, 99])
        r = maker_exit(m, "BTC", T0, +1, END)       # long exit posts SELL @100
        assert r["is_maker"] and not r["fallback"]
        assert r["fill_px_mark"] == 100.0 and r["fill_ts"] == T0 + 2 * MS_MIN

    def test_timeout_falls_back_to_taker_anchored_at_expiry(self, marks_dir):
        # post @100 (T0+60s); next close 99 never crosses a SELL; expiry = T0+120s;
        # v25 chain anchored at expiry: first close >= T0+122s is T0+180s
        m = idx(marks_dir, [100, 99, 99, 99])
        r = maker_exit(m, "BTC", T0, +1, END)
        assert not r["is_maker"] and r["fallback"]
        assert r["fill_ts"] == T0 + 3 * MS_MIN and r["fill_px_mark"] == 99.0

    def test_short_exit_posts_buy(self, marks_dir):
        m = idx(marks_dir, [100, 99.4, 101])
        r = maker_exit(m, "BTC", T0, -1, END)       # short exit BUY fills on <= post
        assert r["is_maker"] and r["fill_px_mark"] == 100.0


class TestFillRateDualEval:
    def _variant(self, mean_pnl, n_days=40, fills=10, missed=30):
        c = {k: 0 for k in ["maker_fills", "maker_missed", "trips_realized"]}
        c["maker_fills"], c["maker_missed"] = fills, missed
        c["trips_realized"] = 200
        return {"daily_pnl": np.full(n_days, mean_pnl),
                "admitted": np.full(n_days, 150.0),
                "avg_gross": np.full(n_days, 300.0),
                "total_pnl": mean_pnl * n_days, "max_dd": 0.01,
                "n_realized": 200, "n_terminal": 0, "n_long": 100, "n_short": 100,
                "mean_net_bps": 5.0, "counters": c,
                "entity_trips": {f"e{i}": 10 for i in range(20)},
                "entity_net": {f"e{i}": mean_pnl for i in range(20)},
                "coin_net": {"BTC": mean_pnl, "ETH": mean_pnl}}

    def _agg(self, fills, missed, fb_worse=True, worst_fb_worse=True):
        from v26_run_grid import _agg_config
        base = self._variant(+1.0, fills=fills, missed=missed)
        fb = self._variant(-1.0 if fb_worse else +2.0, fills=fills, missed=missed)
        worst_fb = self._variant(-2.0 if worst_fb_worse else +3.0,
                                 fills=fills, missed=missed)
        variants = {"BASE": base, "BASE_FB": fb, "WORST": base,
                    "WORST_FB": worst_fb}
        for s in (17, 42, 137):
            variants[f"BASE_seed{s}"] = base
            variants[f"BASE_seed{s}_FB"] = fb
        per_fold = [{"config_id": "c", "fold": 1, "k_real": 20,
                     "variants": variants}]
        cfg = {"config_id": "c", "K": 25, "execution": "maker_entry",
               "family": "F3", "hold_band": "any", "exit_style": "E2",
               "gross_cap": 2.5, "sizing": "150"}
        fold_list = [{"fold": 1, "test_days": 40}]
        return _agg_config(per_fold, fold_list, cfg)

    def test_low_fill_rate_uses_worse_variant(self):
        a = self._agg(fills=10, missed=30)          # 25% < 50%
        assert abs(a["maker_fill_rate"] - 0.25) < 1e-12
        assert a["variant_used"] == "BASE_FB"
        assert np.nanmean(a["excess_series"]) < 0

    def test_low_fill_rate_keeps_primary_if_fallback_better(self):
        a = self._agg(fills=10, missed=30, fb_worse=False)
        assert a["variant_used"] == "BASE"          # criteria use the WORSE = primary

    def test_high_fill_rate_no_dual_eval(self):
        a = self._agg(fills=30, missed=10)
        assert a["maker_fill_rate"] == 0.75 and a["variant_used"] == "BASE"

    def test_dual_eval_applies_to_worst_and_stress(self):
        # codex code-gate #6: the <50% dual evaluation also applies to WORST and the
        # dropout-seed stress runs; criteria use the worse, BOTH are persisted
        a = self._agg(fills=10, missed=30)          # 25% < 50%
        assert a["dual_eval_applied"]
        assert a["worst_pooled_primary"] == 40.0    # +1.0 x 40 days
        assert a["worst_pooled_fb"] == -80.0        # -2.0 x 40 days
        assert a["worst_pooled"] == -80.0           # criteria use the worse
        assert not a["criteria"]["worst_pooled_positive"]
        for s in ("17", "42", "137"):
            assert a["stress_by_seed_primary"][s] == 40.0
            assert a["stress_by_seed_fb"][s] == -40.0
            assert a["stress_by_seed"][s] == -40.0  # worse per seed
        assert not a["criteria"]["stress_median_positive"]

    def test_high_fill_rate_worst_stays_primary_both_persisted(self):
        a = self._agg(fills=30, missed=10)          # 75% >= 50%: no dual eval
        assert not a["dual_eval_applied"]
        assert a["worst_pooled"] == a["worst_pooled_primary"] == 40.0
        assert a["worst_pooled_fb"] == -80.0        # still persisted for the report
        assert a["stress_by_seed"]["17"] == 40.0
        assert a["criteria"]["worst_pooled_positive"]


class TestWorstMakerSlippage:
    def _stream(self, marks_dir, scen_name):
        from v26_common import FoldMarks, scenario_base, scenario_worst
        from v26_overlays import build_trip_stream
        closes = [100, 99.5, 100.6, 101, 101, 101, 101, 101]
        m = idx(marks_dir, closes)
        fm = FoldMarks(m, T0, T0 + MS_DAY)
        j = pd.DataFrame([{"wallet": "0xw", "coin": "BTC", "journey_id": 1,
                           "side": 1, "entry_ts": T0, "leader_notional": 1000.0,
                           "mirror_ts": float(T0 + 150_000)}])
        sc = scenario_base() if scen_name == "BASE" else scenario_worst()
        return build_trip_stream(j, "E1", "maker_both", sc,
                                 lambda c: 0.0, lambda c: 0.0, m, fm, T0, T0 + MS_DAY)

    def test_base_maker_fills_have_no_slippage(self, marks_dir):
        r = self._stream(marks_dir, "BASE").iloc[0]
        assert r["status"] == "ok" and r["entry_is_maker"]
        assert r["entry_px"] == 100.0               # at post, no slippage (BASE)
        assert r["exit_is_maker"] and r["exit_px"] == 100.6

    def test_worst_maker_fills_pay_frozen_7bps_slip(self, marks_dir):
        # codex code-gate #6: WORST maker fills include the frozen 7.0bps slip default
        # on entry AND exit
        r = self._stream(marks_dir, "WORST").iloc[0]
        assert r["status"] == "ok" and r["entry_is_maker"]
        assert abs(r["entry_px"] - 100.0 * 1.0007) < 1e-9
        assert r["exit_is_maker"]
        assert abs(r["exit_px"] - 100.6 * 0.9993) < 1e-9
