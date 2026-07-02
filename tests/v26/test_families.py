"""F3 tail edge cases (frozen formula) + band stats. Synthetic arrays only."""
import numpy as np
import pandas as pd

from v26_common import MS_DAY
from v26_families import (band_pass_wallets, f3_wallet_row, wallet_band_stats)


def bps_array(n=100, n_losses=25, big_runs=3, r_unit=10.0):
    """n journeys: n_losses at -r_unit*... p25 of losses = -r_unit is arranged by making
    ALL losses equal -r_unit; big_runs journeys at exactly 10*r_unit; rest small wins."""
    losses = np.full(n_losses, -r_unit)
    bigs = np.full(big_runs, 10.0 * r_unit)
    rest = np.full(n - n_losses - big_runs, 1.0)
    return np.concatenate([losses, bigs, rest])


class TestF3:
    def test_accepts_valid_wallet(self):
        r = f3_wallet_row(bps_array())
        assert r is not None and "fail" not in r
        assert r["r_unit"] == 10.0 and r["big_runs"] == 3

    def test_zero_r_unit_fails(self):
        # >= 20 losing journeys but all exactly 0 net bps: R_unit = |p25| = 0 => fail
        bps = np.concatenate([np.zeros(25), np.full(75, 5.0)])
        r = f3_wallet_row(bps)
        assert r == {"fail": "zero_r_unit"}

    def test_big_run_threshold_max3_or_1pct(self):
        assert f3_wallet_row(bps_array(big_runs=2)) is None       # 2 < max(3, 1)
        assert f3_wallet_row(bps_array(big_runs=3)) is not None
        # n = 400: 1% of n = 4 > 3 => 3 big runs no longer enough
        assert f3_wallet_row(bps_array(n=400, n_losses=50, big_runs=3)) is None
        assert f3_wallet_row(bps_array(n=400, n_losses=50, big_runs=4)) is not None

    def test_single_tail_observation_rejected(self):
        assert f3_wallet_row(bps_array(big_runs=1)) is None

    def test_sample_minima(self):
        assert f3_wallet_row(bps_array(n=99, n_losses=25)) is None    # < 100 journeys
        assert f3_wallet_row(bps_array(n=100, n_losses=19)) is None   # < 20 losses


class TestBandStats:
    def test_median_and_cadence_and_min_closed(self):
        rows = []
        for i in range(40):                       # 40 closed journeys over 10 days
            rows.append({"wallet": "0xa", "duration_h": 1.0 + (i % 3),
                         "exit_ts": float((i % 10) * MS_DAY + 1000)})
        for i in range(10):                       # too few closed for a band filter
            rows.append({"wallet": "0xb", "duration_h": 5.0,
                         "exit_ts": float(i * MS_DAY + 1000)})
        st = wallet_band_stats(pd.DataFrame(rows))
        assert st.loc["0xa", "n_closed"] == 40
        assert st.loc["0xa", "cadence"] == 4.0            # 40 / 10 distinct exit days
        assert st.loc["0xa", "median_hold_ms"] == 2.0 * 3.6e6
        ok, n_failed = band_pass_wallets(st, (3.6e6, 3 * 3.6e6))
        assert ok == {"0xa"} and n_failed == 1            # 0xb fails the 30-closed min
        ok_any, n2 = band_pass_wallets(st, None)
        assert ok_any == {"0xa", "0xb"} and n2 == 0       # 'any': no band filter
