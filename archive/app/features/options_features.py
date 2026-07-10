"""
Options-derived features from Deribit options surface data.

Computes from MongoDB `deribit_options_surface` snapshots:
- 25-delta risk reversal (skew) + z-score
- Term structure slope (front-week vs front-month ATM IV)
- Gamma Exposure (GEX) + GEX flip point
- Max pain for upcoming expiry
- Put/call OI ratio + flow imbalance
- DVol proxy (30-day constant-maturity ATM IV)

These are BTC+ETH only but serve as portfolio-level features for all alts.
"""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from core.features.feature_base import FeatureBase, FeatureConfig
from core.features.models import Feature
from core.data_structures.candles import Candles
from app.features.decorators import screening_feature


class OptionsConfig(FeatureConfig):
    name: str = "options"
    skew_zscore_window: int = 30  # 30 snapshots for z-score (~7.5 hours at 15min)


@screening_feature
class OptionsFeature(FeatureBase[OptionsConfig]):
    """
    Options feature — reads from MongoDB deribit_options_surface.

    For FeatureComputationTask: data is assembled by querying MongoDB and passed
    in via create_feature_from_data(). Requires multiple snapshots for z-scores.
    """

    HISTORY_DEPTH = 100  # snapshots to request (at 15min = ~25 hours)

    def __init__(self, feature_config: Optional[OptionsConfig] = None):
        super().__init__(feature_config or OptionsConfig())

    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        return data

    def create_feature(self, candles: Candles) -> Feature:
        return Feature(
            timestamp=datetime.now(timezone.utc),
            feature_name=self.config.name,
            trading_pair=candles.trading_pair,
            connector_name=candles.connector_name,
            value={},
        )

    def create_feature_from_data(
        self,
        currency: str,
        snapshots: List[Dict],
    ) -> Feature:
        """Build options features from Deribit snapshots.

        snapshots: list of dicts from deribit_options_surface, sorted by
        timestamp_utc DESC (newest first). Should contain multiple expiries
        and strikes per timestamp.
        """
        value: Dict[str, Any] = {}

        if not snapshots:
            return self._empty_feature(currency)

        # Group by timestamp to get distinct snapshots
        df = pd.DataFrame(snapshots)
        latest_ts = df["timestamp_utc"].max()
        latest = df[df["timestamp_utc"] == latest_ts]

        if latest.empty:
            return self._empty_feature(currency)

        underlying_price = latest["underlying_price"].iloc[0]

        # ── 25-Delta Skew ────────────────────────────────────
        skew = self._compute_25d_skew(latest, underlying_price)
        if skew is not None:
            value["skew_25d"] = skew

        # Skew z-score over historical snapshots
        if len(df["timestamp_utc"].unique()) >= self.config.skew_zscore_window:
            skew_history = self._compute_skew_history(df, underlying_price)
            if len(skew_history) >= self.config.skew_zscore_window:
                mean = np.mean(skew_history[-self.config.skew_zscore_window:])
                std = np.std(skew_history[-self.config.skew_zscore_window:])
                if std > 0:
                    value["skew_25d_zscore"] = float((skew_history[-1] - mean) / std)

        # ── Term Structure ───────────────────────────────────
        term_slope = self._compute_term_structure(latest, underlying_price)
        if term_slope is not None:
            value["term_structure_slope"] = term_slope

        # ── GEX (Gamma Exposure) ─────────────────────────────
        gex, gex_flip = self._compute_gex(latest, underlying_price)
        if gex is not None:
            value["net_gex"] = gex
        if gex_flip is not None:
            value["gex_flip_distance"] = (gex_flip - underlying_price) / underlying_price

        # ── Max Pain ─────────────────────────────────────────
        max_pain = self._compute_max_pain(latest, underlying_price)
        if max_pain is not None:
            value["max_pain"] = max_pain
            value["max_pain_distance"] = (max_pain - underlying_price) / underlying_price

        # ── Put/Call Ratio ───────────────────────────────────
        pc_ratio = self._compute_put_call_ratio(latest)
        if pc_ratio is not None:
            value["put_call_oi_ratio"] = pc_ratio

        # ── DVol Proxy ───────────────────────────────────────
        dvol = self._compute_dvol_proxy(latest, underlying_price)
        if dvol is not None:
            value["dvol_proxy"] = dvol

        return Feature(
            timestamp=datetime.now(timezone.utc),
            feature_name=self.config.name,
            trading_pair=f"{currency}-USDT",
            connector_name="deribit",
            value=value,
        )

    def _empty_feature(self, currency: str) -> Feature:
        return Feature(
            timestamp=datetime.now(timezone.utc),
            feature_name=self.config.name,
            trading_pair=f"{currency}-USDT",
            connector_name="deribit",
            value={},
        )

    def _compute_25d_skew(
        self, snapshot: pd.DataFrame, spot: float
    ) -> Optional[float]:
        """25-delta skew: IV(25d put) - IV(25d call) for nearest expiry.

        Approximation: 25-delta ~ strike at 0.93 * spot (put) and 1.07 * spot (call)
        for typical crypto vol. We find the nearest strike to these levels.
        """
        # Use the nearest expiry with sufficient strikes
        expiries = sorted(snapshot["expiry"].unique())
        if not expiries:
            return None

        for expiry in expiries:
            exp_data = snapshot[snapshot["expiry"] == expiry]
            calls = exp_data[exp_data["type"] == "call"]
            puts = exp_data[exp_data["type"] == "put"]

            if len(calls) < 3 or len(puts) < 3:
                continue

            # Find 25-delta approximations
            put_target = spot * 0.93
            call_target = spot * 1.07

            put_row = puts.iloc[(puts["strike"] - put_target).abs().argsort()[:1]]
            call_row = calls.iloc[(calls["strike"] - call_target).abs().argsort()[:1]]

            if put_row.empty or call_row.empty:
                continue

            put_iv = put_row["mark_iv"].iloc[0]
            call_iv = call_row["mark_iv"].iloc[0]

            if put_iv > 0 and call_iv > 0:
                return float(put_iv - call_iv)

        return None

    def _compute_skew_history(
        self, df: pd.DataFrame, current_spot: float
    ) -> List[float]:
        """Compute skew for each historical snapshot."""
        skews = []
        for ts in sorted(df["timestamp_utc"].unique()):
            snap = df[df["timestamp_utc"] == ts]
            spot = snap["underlying_price"].iloc[0] if not snap.empty else current_spot
            skew = self._compute_25d_skew(snap, spot)
            if skew is not None:
                skews.append(skew)
        return skews

    def _compute_term_structure(
        self, snapshot: pd.DataFrame, spot: float
    ) -> Optional[float]:
        """Term structure slope: ATM IV of longest expiry minus shortest.

        Positive = contango (calm), Negative = backwardation (near-term fear).
        """
        expiries = sorted(snapshot["expiry"].unique())
        if len(expiries) < 2:
            return None

        def _atm_iv(expiry: str) -> Optional[float]:
            exp_data = snapshot[snapshot["expiry"] == expiry]
            calls = exp_data[exp_data["type"] == "call"]
            if calls.empty:
                return None
            atm = calls.iloc[(calls["strike"] - spot).abs().argsort()[:1]]
            return float(atm["mark_iv"].iloc[0]) if not atm.empty else None

        front_iv = _atm_iv(expiries[0])
        back_iv = _atm_iv(expiries[-1])

        if front_iv is not None and back_iv is not None:
            return float(back_iv - front_iv)
        return None

    def _compute_gex(
        self, snapshot: pd.DataFrame, spot: float
    ) -> tuple:
        """Net Gamma Exposure and GEX flip point.

        GEX per strike = gamma_approx * OI * 100
        Positive GEX → market makers dampen moves
        Negative GEX → market makers amplify moves
        """
        if "open_interest" not in snapshot.columns:
            return None, None

        strikes = sorted(snapshot["strike"].unique())
        if len(strikes) < 5:
            return None, None

        gex_by_strike = {}
        for strike in strikes:
            strike_data = snapshot[snapshot["strike"] == strike]
            call_oi = strike_data[strike_data["type"] == "call"]["open_interest"].sum()
            put_oi = strike_data[strike_data["type"] == "put"]["open_interest"].sum()

            # Approximate gamma: higher near ATM, lower far OTM
            moneyness = abs(strike - spot) / spot
            gamma_approx = max(0, 1 - moneyness * 5) * 0.01  # rough approximation

            # Calls have positive gamma, puts have negative gamma for dealers
            call_gex = gamma_approx * call_oi * 100
            put_gex = -gamma_approx * put_oi * 100
            gex_by_strike[strike] = call_gex + put_gex

        net_gex = sum(gex_by_strike.values())

        # GEX flip: find where cumulative GEX changes sign
        gex_flip = None
        sorted_strikes = sorted(gex_by_strike.keys())
        cumulative = 0
        for s in sorted_strikes:
            prev = cumulative
            cumulative += gex_by_strike[s]
            if prev <= 0 < cumulative or prev >= 0 > cumulative:
                gex_flip = s
                break

        return net_gex, gex_flip

    def _compute_max_pain(
        self, snapshot: pd.DataFrame, spot: float
    ) -> Optional[float]:
        """Max pain: strike that minimizes total option holder payoff."""
        # Use nearest expiry
        expiries = sorted(snapshot["expiry"].unique())
        if not expiries:
            return None

        exp_data = snapshot[snapshot["expiry"] == expiries[0]]
        strikes = sorted(exp_data["strike"].unique())
        if len(strikes) < 3:
            return None

        min_pain = float("inf")
        max_pain_strike = None

        for test_strike in strikes:
            total_pain = 0
            for _, row in exp_data.iterrows():
                oi = row.get("open_interest", 0)
                if oi <= 0:
                    continue
                if row["type"] == "call":
                    intrinsic = max(0, test_strike - row["strike"])
                else:
                    intrinsic = max(0, row["strike"] - test_strike)
                total_pain += intrinsic * oi

            if total_pain < min_pain:
                min_pain = total_pain
                max_pain_strike = test_strike

        return float(max_pain_strike) if max_pain_strike else None

    def _compute_put_call_ratio(self, snapshot: pd.DataFrame) -> Optional[float]:
        """Put/Call OI ratio across all expiries."""
        puts = snapshot[snapshot["type"] == "put"]
        calls = snapshot[snapshot["type"] == "call"]
        put_oi = puts["open_interest"].sum()
        call_oi = calls["open_interest"].sum()
        if call_oi > 0:
            return float(put_oi / call_oi)
        return None

    def _compute_dvol_proxy(
        self, snapshot: pd.DataFrame, spot: float
    ) -> Optional[float]:
        """DVol proxy: weighted ATM IV across expiries (approximation)."""
        expiries = sorted(snapshot["expiry"].unique())
        if not expiries:
            return None

        ivs = []
        for expiry in expiries:
            exp_data = snapshot[snapshot["expiry"] == expiry]
            calls = exp_data[exp_data["type"] == "call"]
            if calls.empty:
                continue
            atm = calls.iloc[(calls["strike"] - spot).abs().argsort()[:1]]
            if not atm.empty:
                ivs.append(float(atm["mark_iv"].iloc[0]))

        if ivs:
            return float(np.mean(ivs))
        return None
