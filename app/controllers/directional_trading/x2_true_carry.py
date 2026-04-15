"""
X2 — True Carry (HB V2 Controller)
V1 — April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: Funding rate IS the edge, not a signal for direction. When funding is
persistently skewed (longs paying shorts or vice versa), position on the
RECEIVING side and HOLD to collect funding over multiple settlement cycles.
Price movement is noise; funding income is the signal.

Why this is NOT E3: E3 trades funding as a directional signal (fade the crowd).
X2 collects funding as the primary source of return:
  - E3: 3% TP cuts winners short → loses remaining carry income
  - X2: NO effective TP → let winners accumulate carry
  - E3: 5-day time limit → misses 6+ settlement windows
  - X2: 7-day time limit → 21 settlement windows per trade
  - E3: enters on streak count (binary) → ignores funding magnitude
  - X2: enters on rolling average rate (continuous) → captures SIZE of carry
  - X2: volatility filter → calm markets where carry/noise ratio is highest

Signal (1h candle with funding_rate column):
  - Compute rolling average funding rate over lookback window
  - Entry: avg_rate > threshold AND streak >= min AND (optional) low volatility
  - Direction: opposite to payers (positive avg → SHORT to receive)
  - Exit: SL only (emergency), time limit (refresh cycle), or funding regime flip

Data:
  - Backtest: funding_rate column pre-merged by BulkBacktestTask
  - Live: fetched from Bybit REST API (cached 5 min)
  - Funding PnL: computed post-hoc by BulkBacktestTask._compute_funding_pnl()

Performance target: 10-15% annualized carry on majors (80%+ positive rate).
At 0.01% avg rate per 8h settlement × 3/day × 365 = ~11% base carry.
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


class X2TrueCarryConfig(DirectionalTradingControllerConfigBase):
    """V1 config — True Carry (funding collection)."""
    controller_name: str = "x2_true_carry"

    # ── Funding signal parameters ──
    funding_lookback: int = Field(
        default=6,
        description="Rolling window for avg funding rate (in funding periods = 8h each, 6 = 48h)"
    )
    funding_min_rate: float = Field(
        default=0.00005,
        description="Min |avg_funding_rate| to enter (0.005% per 8h = ~6.7% annualized)"
    )
    funding_min_streak: int = Field(
        default=3,
        description="Min consecutive same-sign funding periods before entry"
    )
    funding_zscore_boost: float = Field(
        default=2.0,
        description="Z-score threshold for extreme funding (enters with higher confidence)"
    )
    funding_zscore_window: int = Field(
        default=90,
        description="Rolling window for funding z-score (90 periods = ~30 days at 8h)"
    )

    # ── Carry-optimized exits ──
    sl_pct: float = Field(
        default=0.06,
        description="Emergency stop loss 6% (wide — carry absorbs moderate drawdowns)"
    )
    tp_pct: float = Field(
        default=0.10,
        description="TP 10% (effectively unreachable safety net — carry strategy never cuts winners)"
    )
    time_limit_seconds: int = Field(
        default=604800,
        description="7 days = 21 funding settlements. After TL, re-enter if conditions hold."
    )

    # ── Volatility filter: calm markets → higher carry/noise ratio ──
    vol_filter_enabled: bool = Field(
        default=True,
        description="Only enter when volatility is below threshold (calm markets)"
    )
    vol_atr_period: int = Field(
        default=24,
        description="ATR period in hours for volatility filter"
    )
    vol_max_atr_pct: float = Field(
        default=0.04,
        description="Max ATR/price ratio (4% daily). Calm markets only."
    )

    # ── OI filter: open interest confirms real capital, not just wash trading ──
    oi_filter_enabled: bool = Field(
        default=True,
        description="Require minimum OI percentile (real capital = more stable funding)"
    )
    oi_filter_min_pct: float = Field(
        default=0.25,
        description="Min OI rolling percentile to confirm real capital present"
    )
    oi_filter_window: int = Field(
        default=168,
        description="OI percentile rolling window in hours (7 days)"
    )

    # ── REST API (live mode only) ──
    bybit_api_url: str = Field(default="https://api.bybit.com")
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
                    max_records=200,
                ),
            ]


class X2TrueCarryController(DirectionalTradingControllerBase):
    """X2 — collect funding by positioning on the receiving side.

    Core principle: funding IS the edge. Price movement is noise.
    Wide SL, no effective TP, long hold = maximize settlement collections.
    """

    def __init__(self, config: X2TrueCarryConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        self._last_fetch = 0.0
        self._cached_funding: list = []
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

        if df is None or len(df) < c.vol_atr_period + 5:
            self.processed_data["signal"] = 0
            return

        # Backtest mode: funding_rate column pre-merged
        if "funding_rate" in df.columns:
            df = self._compute_signals_vectorized(df)
        else:
            # Live mode: fetch from API, compute scalar
            signal = self._compute_signal_live(c.trading_pair)
            df = df.copy()
            df["signal"] = 0
            if len(df) > 0:
                df.iloc[-1, df.columns.get_loc("signal")] = signal

        signal = int(df["signal"].iloc[-1]) if len(df) > 0 else 0
        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

    def _compute_signals_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute carry signal for every bar (required for BacktestingEngine).

        CRITICAL: Do NOT store intermediate NaN columns in returned DataFrame.
        BacktestingEngine.prepare_market_data() does dropna(how='any'),
        so ANY NaN in ANY column kills the entire row.

        Carry signal logic:
          1. Rolling average funding rate over lookback window
          2. Consecutive same-sign streak count
          3. Entry: avg_rate > threshold AND streak >= min_streak
          4. Direction: opposite to payers (positive avg → SHORT to receive)
          5. Optional filters: volatility (calm markets), OI (real capital)
        """
        c = self.config
        df = df.copy()
        df["signal"] = 0

        fr = df["funding_rate"]

        # Step 1: Rolling average funding rate (temporary, NOT stored in df)
        # Funding settles every 8h. With 1h candles, funding_rate is forward-filled.
        # The rolling mean captures the average rate over the lookback period.
        avg_fr = fr.rolling(window=c.funding_lookback, min_periods=c.funding_lookback).mean()

        # Step 2: Consecutive same-sign streak (temporary)
        sign = fr.apply(np.sign)
        # Group consecutive same-sign periods and count within each group
        group_id = (sign != sign.shift()).cumsum()
        streak = sign.groupby(group_id).cumcount() + 1

        # Step 3: Entry conditions
        # Positive avg funding → longs paying shorts → SHORT to receive
        short_entry = (avg_fr > c.funding_min_rate) & (streak >= c.funding_min_streak)
        # Negative avg funding → shorts paying longs → LONG to receive
        long_entry = (avg_fr < -c.funding_min_rate) & (streak >= c.funding_min_streak)

        df.loc[short_entry, "signal"] = -1
        df.loc[long_entry, "signal"] = 1

        # Step 4: Z-score boost — at extremes, higher confidence (for logging/analysis)
        # Not used for filtering, just tagged. Kept as temporary.
        if c.funding_zscore_window > 0:
            fr_mean = fr.rolling(c.funding_zscore_window, min_periods=c.funding_zscore_window).mean()
            fr_std = fr.rolling(c.funding_zscore_window, min_periods=c.funding_zscore_window).std()
            fr_std = fr_std.replace(0, np.nan)
            # z-score exists but we don't filter on it — just informational

        # Step 5: Volatility filter (calm markets = higher carry/noise)
        if c.vol_filter_enabled:
            high = df["high"] if "high" in df.columns else df.get("close", fr)
            low = df["low"] if "low" in df.columns else df.get("close", fr)
            close = df["close"] if "close" in df.columns else fr

            # Simple ATR approximation: rolling mean of (high - low)
            tr = high - low
            atr = tr.rolling(window=c.vol_atr_period, min_periods=c.vol_atr_period).mean()
            atr_pct = atr / close

            # Suppress signals in volatile markets
            volatile = atr_pct > c.vol_max_atr_pct
            df.loc[(df["signal"] != 0) & volatile, "signal"] = 0

        # Step 6: OI filter (real capital = more stable funding regime)
        if c.oi_filter_enabled and "oi_value" in df.columns:
            oi = df["oi_value"]
            oi_pct = oi.rolling(c.oi_filter_window, min_periods=24).rank(pct=True)
            low_oi = oi_pct < c.oi_filter_min_pct
            df.loc[(df["signal"] != 0) & low_oi, "signal"] = 0

        return df

    # ── Live mode API fetching ──────────────────────────────

    def _compute_signal_live(self, trading_pair: str) -> int:
        """Compute carry signal from live Bybit funding API. Cached 5 min."""
        c = self.config
        now = time.time()
        if now - self._last_fetch < 300 and self._cached_funding:
            rates = self._cached_funding
        else:
            rates = self._fetch_bybit_funding(trading_pair)
            self._cached_funding = rates
            self._last_fetch = now

        if len(rates) < c.funding_lookback:
            return 0

        # Extract funding rates (most recent first from API, reverse)
        fr_values = [float(r.get("fundingRate", 0)) for r in rates[:c.funding_lookback]]
        fr_values.reverse()  # oldest first

        # Rolling average
        avg_rate = np.mean(fr_values)

        # Streak count (from most recent backward)
        if len(fr_values) == 0:
            return 0
        last_sign = np.sign(fr_values[-1])
        streak = 0
        for val in reversed(fr_values):
            if np.sign(val) == last_sign and last_sign != 0:
                streak += 1
            else:
                break

        if abs(avg_rate) < c.funding_min_rate:
            return 0
        if streak < c.funding_min_streak:
            return 0

        # Positive avg → SHORT to receive from longs
        if avg_rate > c.funding_min_rate:
            return -1
        # Negative avg → LONG to receive from shorts
        elif avg_rate < -c.funding_min_rate:
            return 1

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
            logger.warning(f"X2 Bybit funding fetch failed for {trading_pair}: {e}")
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
                trailing_stop=None,  # NO trailing — carry, not momentum
                # LIMIT entry (minimize taker fees — critical for carry edge)
                open_order_type=OrderType.LIMIT,
                take_profit_order_type=OrderType.LIMIT,
                stop_loss_order_type=OrderType.MARKET,
                time_limit_order_type=OrderType.MARKET,
            ),
            leverage=c.leverage,
        )
