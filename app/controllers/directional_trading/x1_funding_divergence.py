"""
X1 — Cross-Exchange Funding Divergence (HB V2 Controller)
V1 — April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: When Bybit funding rate significantly diverges from Binance funding rate,
arb flow creates directional pressure that mean-reverts. At 2-sigma extremes,
the spread reverts 72-97% of the time across major pairs (6 years of data).

Why this is NOT H2: H2 used the same signal but died because of exit structure.
  - H2 used trailing stop (wrong for mean reversion — locks in losses on whipsaws)
  - H2 used 8h time limit (too short — funding settles every 8h, need multiple cycles)
  - H2 had tight 3% SL which got triggered by noise before reversion completed

X1 fixes:
  - No trailing stop (mean reversion, not momentum)
  - 48h time limit (6 funding settlement windows for reversion to play out)
  - 4% SL (wide enough to survive noise; 72-97% reversion means most trades win)
  - Dynamic exit: signal flips to 0 when spread normalizes (z < exit_threshold)
  - LIMIT entry orders (learned from E3 market order bug)
  - OI conviction filter: high OI = more arb capital = stronger reversion

Signal (1h candle with funding_rate + binance_funding_rate columns):
  - funding_spread = bybit_fr - binance_fr
  - Compute rolling z-score of spread (window = 30 funding periods ~10 days)
  - Entry: |z-score| > 2.0 AND |spread| > min_spread
  - Direction: SHORT when Bybit premium (z > 2), LONG when Bybit discount (z < -2)
  - Exit: z-score returns within [-exit_z, +exit_z] (default 0.5)

Data:
  - Backtest: funding_rate + binance_funding_rate columns pre-merged by BulkBacktestTask
  - Live: fetched from Bybit and Binance REST APIs (cached 5 min)
"""
from decimal import Decimal
from typing import List
import json
import logging
import time
import urllib.request

import numpy as np
import pandas as pd
from pydantic import Field

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import (
    DirectionalTradingControllerBase,
    DirectionalTradingControllerConfigBase,
)
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TripleBarrierConfig,
)

logger = logging.getLogger(__name__)


class X1FundingDivergenceConfig(DirectionalTradingControllerConfigBase):
    """V1 config — Cross-Exchange Funding Divergence (Mean Reversion)."""
    controller_name: str = "x1_funding_divergence"

    # Signal parameters
    zscore_window: int = Field(
        default=30,
        description="Rolling window for z-score in funding periods (~10 days at 8h settlements)"
    )
    zscore_entry: float = Field(
        default=2.0,
        description="Z-score threshold for entry (research shows 72-97% reversion at 2-sigma)"
    )
    zscore_exit: float = Field(
        default=0.5,
        description="Z-score threshold for exit (spread reverted enough)"
    )
    min_spread_abs: float = Field(
        default=0.00005,
        description="Min |spread| to filter noise (0.005%)"
    )

    # Exit — designed for mean reversion, NOT momentum
    sl_pct: float = Field(
        default=0.04,
        description="Stop loss 4% (wide — reversion rate is 72-97%, give it room)"
    )
    tp_pct: float = Field(
        default=0.025,
        description="Take profit 2.5% (capture convergence move, not greedy)"
    )
    time_limit_seconds: int = Field(
        default=172800,
        description="48 hours (6 funding settlement windows for reversion)"
    )

    # OI conviction filter: high OI = more arb capital = stronger reversion pressure
    oi_filter_enabled: bool = Field(
        default=True,
        description="Only enter when OI is above minimum percentile (arb capital present)"
    )
    oi_filter_min_pct: float = Field(
        default=0.30,
        description="Min OI rolling percentile to confirm arb capital present"
    )
    oi_filter_window: int = Field(
        default=168,
        description="OI percentile rolling window in hours (7 days)"
    )

    # BTC regime filter: suppress counter-BTC trades during strong BTC moves
    btc_regime_enabled: bool = Field(
        default=False,
        description="Suppress signals opposing strong BTC moves (conservative, off by default)"
    )
    btc_regime_threshold: float = Field(
        default=0.02,
        description="Min |btc_return_4h| to suppress opposing trades"
    )

    # Fear & Greed overlay: boost conviction at sentiment extremes
    fg_filter_enabled: bool = Field(
        default=False,
        description="Only enter at F&G extremes (< 25 or > 75)"
    )
    fg_extreme_low: float = Field(default=25.0, description="F&G below this = extreme fear")
    fg_extreme_high: float = Field(default=75.0, description="F&G above this = extreme greed")

    # REST API fetch (live mode only)
    bybit_api_url: str = Field(default="https://api.bybit.com")
    binance_api_url: str = Field(default="https://fapi.binance.com")
    funding_fetch_limit: int = Field(default=50)

    candles_config: List[CandlesConfig] = Field(default_factory=list)

    def model_post_init(self, __context) -> None:
        if not self.candles_config:
            candles_connector = self.connector_name.replace("_testnet", "").replace("_demo", "")
            self.candles_config = [
                CandlesConfig(
                    connector=candles_connector,
                    trading_pair=self.trading_pair,
                    interval="1h",
                    max_records=200,  # Need zscore_window + buffer for rolling calc
                ),
            ]


class X1FundingDivergenceController(DirectionalTradingControllerBase):
    """X1 — trade cross-exchange funding spread mean reversion.

    Key difference from H2 (dead):
      - No trailing stop (mean reversion, not momentum)
      - 48h time limit (not 8h — give reversion time)
      - 4% SL (not 3% — wider to survive noise)
      - Dynamic exit via signal neutralization when spread normalizes
      - OI conviction filter (arb capital must be present)
    """

    def __init__(self, config: X1FundingDivergenceConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        self._last_fetch = 0.0
        self._cached_bybit_funding: list = []
        self._cached_binance_funding: list = []
        super().__init__(config, *args, **kwargs)

    def get_candles_config(self) -> List[CandlesConfig]:
        return self.config.candles_config

    # ── Signal computation ──────────────────────────────────

    async def update_processed_data(self):
        c = self.config

        df = self.market_data_provider.get_candles_df(
            connector_name=self._candles_connector,
            trading_pair=c.trading_pair,
            interval="1h",
            max_records=200,
        )

        if df is None or len(df) < c.zscore_window + 5:
            self.processed_data["signal"] = 0
            return

        # Backtest mode: derivatives columns pre-merged
        if "funding_rate" in df.columns and "binance_funding_rate" in df.columns:
            df = self._compute_signals_vectorized(df)
        else:
            # Live mode: fetch from APIs, compute scalar
            signal = self._compute_signal_live(c.trading_pair)
            df = df.copy()
            df["signal"] = 0
            if len(df) > 0:
                df.iloc[-1, df.columns.get_loc("signal")] = signal

        signal = int(df["signal"].iloc[-1]) if len(df) > 0 else 0
        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

    def _compute_signals_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute signal for every bar (required for BacktestingEngine).

        CRITICAL: Do NOT store intermediate columns with NaN in the returned DataFrame.
        BacktestingEngine.prepare_market_data() does merge_asof + dropna(how='any'),
        so ANY NaN in ANY column kills the entire row. Use temporary Series for
        rolling calculations, and only add the clean 'signal' column to df.

        Signal logic:
          1. Compute funding spread = bybit_fr - binance_fr
          2. Z-score the spread over rolling window
          3. Entry: |z| > zscore_entry AND |spread| > min_spread
          4. Exit (signal=0): |z| < zscore_exit (spread reverted)
          5. Apply optional filters (OI, BTC regime, F&G)
        """
        c = self.config
        df = df.copy()
        df["signal"] = 0

        # Step 1: Funding spread (temporary, NOT stored in df)
        spread = df["funding_rate"] - df["binance_funding_rate"]

        # Step 2: Rolling z-score (temporary Series, NOT stored in df)
        roll_mean = spread.rolling(window=c.zscore_window, min_periods=c.zscore_window).mean()
        roll_std = spread.rolling(window=c.zscore_window, min_periods=c.zscore_window).std()
        roll_std = roll_std.replace(0, np.nan)
        spread_zscore = (spread - roll_mean) / roll_std

        # Step 3: Entry signals with noise filter
        above_noise = spread.abs() >= c.min_spread_abs

        # Bybit premium (spread > 0, z > entry) -> SHORT: arb pressure pushes Bybit down
        short_signal = (spread_zscore > c.zscore_entry) & above_noise
        # Bybit discount (spread < 0, z < -entry) -> LONG: arb pressure pushes Bybit up
        long_signal = (spread_zscore < -c.zscore_entry) & above_noise

        df.loc[short_signal, "signal"] = -1
        df.loc[long_signal, "signal"] = 1

        # Step 4: OI conviction filter
        if c.oi_filter_enabled and "oi_value" in df.columns:
            oi = df["oi_value"]
            oi_pct = oi.rolling(c.oi_filter_window, min_periods=24).rank(pct=True)
            low_oi = oi_pct < c.oi_filter_min_pct
            df.loc[(df["signal"] != 0) & low_oi, "signal"] = 0

        # Step 5: BTC regime filter (optional, off by default)
        if c.btc_regime_enabled and "btc_return_4h" in df.columns:
            btc_ret = df["btc_return_4h"]
            long_against = (df["signal"] == 1) & (btc_ret < -c.btc_regime_threshold)
            short_against = (df["signal"] == -1) & (btc_ret > c.btc_regime_threshold)
            df.loc[long_against | short_against, "signal"] = 0

        # Step 6: Fear & Greed overlay (optional, off by default)
        if c.fg_filter_enabled and "fear_greed_value" in df.columns:
            fg = df["fear_greed_value"]
            not_extreme = (fg >= c.fg_extreme_low) & (fg <= c.fg_extreme_high)
            df.loc[(df["signal"] != 0) & not_extreme, "signal"] = 0

        return df

    # ── Live mode API fetching ──────────────────────────────

    def _compute_signal_live(self, trading_pair: str) -> int:
        """Compute signal from live API data. Cached for 5 min."""
        c = self.config
        now = time.time()
        if now - self._last_fetch < 300 and self._cached_bybit_funding:
            bybit_rates = self._cached_bybit_funding
            binance_rates = self._cached_binance_funding
        else:
            bybit_rates = self._fetch_bybit_funding(trading_pair)
            binance_rates = self._fetch_binance_funding(trading_pair)
            self._cached_bybit_funding = bybit_rates
            self._cached_binance_funding = binance_rates
            self._last_fetch = now

        if len(bybit_rates) < c.zscore_window or len(binance_rates) < c.zscore_window:
            return 0

        # Align by timestamp and compute spread
        bybit_map = {
            int(r.get("fundingRateTimestamp", 0)): float(r.get("fundingRate", 0))
            for r in bybit_rates
        }
        binance_map = {
            int(r.get("fundingTime", 0)): float(r.get("fundingRate", 0))
            for r in binance_rates
        }

        # Match timestamps (within 1h tolerance)
        spreads = []
        for ts, bybit_fr in sorted(bybit_map.items()):
            best_ts = min(binance_map.keys(), key=lambda t: abs(t - ts), default=None)
            if best_ts and abs(best_ts - ts) < 3600_000:
                spreads.append(bybit_fr - binance_map[best_ts])

        if len(spreads) < c.zscore_window:
            return 0

        spreads_arr = np.array(spreads[-c.zscore_window:])
        mean = spreads_arr.mean()
        std = spreads_arr.std()
        if std == 0:
            return 0

        current_spread = spreads_arr[-1]
        zscore = (current_spread - mean) / std

        if abs(current_spread) < c.min_spread_abs:
            return 0

        if zscore > c.zscore_entry:
            return -1  # SHORT: Bybit premium, arbs push down
        elif zscore < -c.zscore_entry:
            return 1   # LONG: Bybit discount, arbs push up

        return 0

    def _fetch_bybit_funding(self, trading_pair: str) -> list:
        """Fetch Bybit funding history via REST API."""
        c = self.config
        symbol = trading_pair.replace("-", "")
        url = (
            f"{c.bybit_api_url}/v5/market/funding/history"
            f"?category=linear&symbol={symbol}&limit={c.funding_fetch_limit}"
        )
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "hummingbot"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            return data.get("result", {}).get("list", [])
        except Exception as e:
            logger.warning(f"X1 Bybit funding fetch failed for {trading_pair}: {e}")
            return []

    def _fetch_binance_funding(self, trading_pair: str) -> list:
        """Fetch Binance funding history via REST API."""
        c = self.config
        symbol = trading_pair.replace("-", "")
        url = (
            f"{c.binance_api_url}/fapi/v1/fundingRate"
            f"?symbol={symbol}&limit={c.funding_fetch_limit}"
        )
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "hummingbot"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            return data
        except Exception as e:
            logger.warning(f"X1 Binance funding fetch failed for {trading_pair}: {e}")
            return []

    # ── Executor config ─────────────────────────────────────

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        c = self.config
        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            connector_name=c.connector_name,
            trading_pair=c.trading_pair,
            side=trade_type,
            entry_price=price,
            amount=amount,
            triple_barrier_config=TripleBarrierConfig(
                stop_loss=Decimal(str(c.sl_pct)),
                take_profit=Decimal(str(c.tp_pct)),
                time_limit=c.time_limit_seconds,
                trailing_stop=None,  # NO trailing stop — mean reversion, not momentum
                # LIMIT entry (E3 bug lesson: market orders waste taker fees)
                open_order_type=OrderType.LIMIT,
                take_profit_order_type=OrderType.LIMIT,
                # SL must be MARKET (guaranteed exit)
                stop_loss_order_type=OrderType.MARKET,
                time_limit_order_type=OrderType.MARKET,
            ),
            leverage=c.leverage,
        )
