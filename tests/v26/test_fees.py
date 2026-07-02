"""Causal trailing-14d tier engine: yesterday's volume moves today's fee, same-day
volume never does, fold reset, WORST mode has no tier evolution, referral discount,
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

    def test_worst_mode_never_upgrades(self, tier_snapshot):
        e = FeeEngine(tier_snapshot, "WORST")
        e.record_volume(D0, 1e9)
        assert e.rate(D0 + MS_DAY, "BTC", False) == 0.001
        assert e.tier_departed_base == 0

    def test_departure_counter(self, tier_snapshot):
        e = FeeEngine(tier_snapshot, "BASE")
        e.record_volume(D0, 2000.0)
        e.rate(D0 + MS_DAY, "BTC", False)
        assert e.tier_departed_base == 1


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
