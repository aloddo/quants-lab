"""Causal trailing-14d tier engine: yesterday's volume moves today's fee, same-day
volume never does, fold reset, WORST keeps causal tier evolution with base-rate floor
(codex code-gate #5), rate-departure assertion fails loud, referral discount,
HIP-3 multiplier."""
from v26_common import MS_DAY
from v26_fees import FeeEngine

D0 = 20_000 * MS_DAY                     # an arbitrary UTC day boundary


class TestCausalTiers:
    def test_volume_yesterday_moves_todays_fee(self, tier_snapshot):
        e = FeeEngine(tier_snapshot, "BASE")
        assert e.rate(D0, "BTC", maker=False) == 0.001            # base tier, cold
        e.record_volume(D0 + 1000, 2000.0)                        # day 0 volume
        assert e.rate(D0 + 50_000, "BTC", maker=False) == 0.001   # same day: unchanged
        assert e.rate(D0 + MS_DAY, "BTC", maker=False) == 0.0005  # day 1: tier applies
        assert e.rate(D0 + MS_DAY, "BTC", maker=True) == 0.0001

    def test_trailing_window_expires(self, tier_snapshot):
        e = FeeEngine(tier_snapshot, "BASE")
        e.record_volume(D0, 2000.0)
        assert e.rate(D0 + 14 * MS_DAY, "BTC", False) == 0.0005   # day 0 still inside
        assert e.rate(D0 + 15 * MS_DAY, "BTC", False) == 0.001    # expired

    def test_fold_reset_is_cold_start(self, tier_snapshot):
        e = FeeEngine(tier_snapshot, "BASE")
        e.record_volume(D0, 2000.0)
        assert e.rate(D0 + MS_DAY, "BTC", False) == 0.0005
        e.reset()
        assert e.rate(D0 + MS_DAY, "BTC", False) == 0.001
        assert e.tier_departed_base == 0

    def test_worst_rates_floored_but_tiers_still_evolve(self, tier_snapshot):
        # codex code-gate #5: WORST = base-tier-no-discount RATES as the FLOOR; the
        # causal tier engine stays active (departures counted) but an improved tier
        # can never reduce the charged WORST rate
        e = FeeEngine(tier_snapshot, "WORST")
        e.record_volume(D0, 1e9)
        assert e.rate(D0 + MS_DAY, "BTC", False) == 0.001     # floored at base
        assert e.rate(D0 + MS_DAY, "BTC", True) == 0.0002
        assert e.tier_departed_base > 0                       # mechanism active
        assert e.rate_departed_base == 0                # charged rate never left base

    def test_departure_counters(self, tier_snapshot):
        e = FeeEngine(tier_snapshot, "BASE")
        e.record_volume(D0, 2000.0)
        e.rate(D0 + MS_DAY, "BTC", False)
        assert e.tier_departed_base == 1
        assert e.rate_departed_base == 1          # BASE: charged rate departed base

    def test_assembly_fails_loud_on_rate_departure(self, marks_dir, tier_snapshot):
        # decision D5 assertion (codex code-gate #5): a config whose charged rate
        # departs the base-tier schedule must raise (-> runtime_failure, fail closed),
        # never silently keep base-tier trigger fees
        import pandas as pd
        import pytest
        from v25_common import MS_MIN, MarksIndex
        from v26_common import FoldMarks
        from v26_overlays import TRIP_COLS
        from v26_run_grid import assemble_config_fold
        T0 = pd.Timestamp("2026-03-01").value // 10**6
        n_min = 1440
        marks_dir("BTC", [T0 + i * MS_MIN for i in range(n_min)], [100.0] * n_min)
        fm = FoldMarks(MarksIndex(cache_dir=marks_dir.dir), T0, T0 + MS_DAY)
        trip = dict(zip(TRIP_COLS, [
            "0xw", "BTC", 1, 1, 1000.0, T0, T0 + MS_MIN, 100.0, False,
            "MIRROR", T0 + 60 * MS_MIN, T0 + 61 * MS_MIN, 100.0,
            False, False, False, "ok", "", float(T0 + 60 * MS_MIN)]))
        fee = FeeEngine(tier_snapshot, "BASE")
        fee.record_volume(T0 - MS_DAY, 2000.0)   # prior-day volume crosses the cutoff
        with pytest.raises(RuntimeError, match="tier-departure assertion"):
            assemble_config_fold(pd.DataFrame([trip]), fm, fee, 2.5, "150", {})


class TestRateMapping:
    def test_referral_discount_base_only(self, tier_snapshot):
        tier_snapshot["data"]["activeReferralDiscount"] = "0.04"
        b = FeeEngine(tier_snapshot, "BASE")
        assert abs(b.rate(D0, "BTC", False) - 0.001 * 0.96) < 1e-15
        w = FeeEngine(tier_snapshot, "WORST")
        assert w.rate(D0, "BTC", False) == 0.001                  # no discount in WORST

    def test_hip3_multiplier(self, tier_snapshot):
        e = FeeEngine(tier_snapshot, "BASE")
        assert e.rate(D0, "xyz:GOLD", False) == 2 * e.rate(D0, "BTC", False)
        assert e.base_taker_rate("xyz:GOLD") == 2 * e.base_taker_rate("BTC")

    def test_real_snapshot_reproduces_v25_base(self):
        from v26_fees import load_snapshot
        e = FeeEngine(load_snapshot(), "BASE")
        assert abs(e.base_taker_rate("BTC") - 4.32e-4) < 1e-12    # 4.5 x 0.96 bps
        assert abs(e.base_taker_rate("xyz:X") - 8.64e-4) < 1e-12
        w = FeeEngine(load_snapshot(), "WORST")
        assert abs(w.base_taker_rate("BTC") - 4.5e-4) < 1e-12
