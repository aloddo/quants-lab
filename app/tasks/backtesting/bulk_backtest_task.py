"""
Bulk backtest task — runs backtests across all pairs with parquet data.

Uses the engine registry (app/engines/registry.py) to determine resolution,
candles config, exit params, and config class per engine. No hard-coded
engine-specific logic here.

Records PF, WR, Sharpe, max_dd per pair per engine.
Writes verdicts (ALLOW/WATCH/BLOCK) to MongoDB pair_historical collection.
Stores individual trades to backtest_trades collection for post-hoc analysis.

Memory safety: each pair's BacktestingEngine runs in a subprocess via
_run_pair_subprocess(). The OS reclaims all memory (3-5 GB per pair at 1m)
when the subprocess exits. The parent stays lightweight and handles all
MongoDB writes, trade storage, and verdict computation.

Schedule: manual trigger or weekly cron.
"""
import gc
import json
import logging
import os
import pickle
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from core.backtesting.engine import BacktestingEngine
from core.data_paths import data_paths
from core.tasks import BaseTask, TaskContext

from app.engines.strategy_registry import build_backtest_config

logger = logging.getLogger(__name__)

# Python interpreter for subprocess isolation
_PYTHON = sys.executable
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent.parent)

# Verdict thresholds (multi-criteria — PF alone is not enough)
PF_ALLOW = 1.3
PF_WATCH = 1.0
SHARPE_ALLOW = 1.0
SHARPE_WATCH = 0.0
MAX_DD_ALLOW = -0.15   # -15% max drawdown
MAX_DD_WATCH = -0.20   # -20%
MIN_TRADES = 30        # need at least 30 trades for statistical significance


from app.tasks.notifying_task import NotifyingTaskMixin


class BulkBacktestTask(NotifyingTaskMixin, BaseTask):
    """Run backtests for any registered engine across all pairs and update pair_historical."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.engine_name = task_config.get("engine", "E1")
        self.connector_name = task_config.get("connector_name", "bybit_perpetual")
        self.backtest_days = task_config.get("backtest_days", 365)
        self.trade_cost = task_config.get("trade_cost", 0.000375)

        # Resolution, intervals, and pair source come from the engine registry
        from app.engines.strategy_registry import get_strategy
        engine_meta = get_strategy(self.engine_name)
        self.backtesting_resolution = engine_meta.backtesting_resolution
        self.pair_source = engine_meta.pair_source
        self.pair_allowlist = engine_meta.pair_allowlist

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB required for BulkBacktestTask")

    async def _merge_derivatives_into_candles(
        self, shared_candles: dict, pairs: List[str]
    ) -> int:
        """Merge derivatives data (funding, OI, LS ratio) from MongoDB into candle DataFrames.

        For each pair's 1h candle feed, queries MongoDB for historical derivatives
        data and adds funding_rate, oi_value, buy_ratio columns aligned by timestamp.
        Forward-fills gaps (funding is 8h, OI/LS are 15min-1h), then back-fills
        and fills remaining NaN with neutral values.

        NaN safety: BacktestingEngineBase.prepare_market_data() calls
        dropna(inplace=True) on the merged DataFrame (how='any'). Any NaN
        in ANY column kills the row. We must ensure derivatives columns never
        contain NaN, even for timestamps before MongoDB coverage begins.

        Neutral fill values:
          funding_rate = 0.0   (no funding → no signal)
          oi_value     = bfill then 0.0 (use earliest known, else zero)
          buy_ratio    = 0.5   (balanced → no crowd signal)

        Returns number of pairs enriched.
        """
        db = self.mongodb_client.get_database()
        enriched = 0

        for pair in pairs:
            feed_key = f"{self.connector_name}_{pair}_1h"
            if feed_key not in shared_candles:
                continue

            df = shared_candles[feed_key]
            if df is None or len(df) == 0:
                continue

            query = {"pair": pair}

            try:
                candle_idx = pd.to_datetime(df["timestamp"], unit="s", utc=True)

                # Funding rates
                funding_cursor = db["bybit_funding_rates"].find(query).sort("timestamp_utc", 1)
                funding_docs = await funding_cursor.to_list(length=100000)
                if funding_docs:
                    fdf = pd.DataFrame(funding_docs)
                    if "timestamp_utc" in fdf.columns and "funding_rate" in fdf.columns:
                        fdf["ts"] = pd.to_datetime(fdf["timestamp_utc"], unit="ms", utc=True)
                        fdf = fdf.set_index("ts")[["funding_rate"]].sort_index()
                        fdf = fdf[~fdf.index.duplicated(keep="last")]
                        fdf_reindexed = fdf.reindex(candle_idx, method="ffill")
                        df["funding_rate"] = fdf_reindexed["funding_rate"].fillna(0.0).values
                else:
                    df["funding_rate"] = 0.0

                # Open interest
                oi_cursor = db["bybit_open_interest"].find(query).sort("timestamp_utc", 1)
                oi_docs = await oi_cursor.to_list(length=100000)
                if oi_docs:
                    odf = pd.DataFrame(oi_docs)
                    if "timestamp_utc" in odf.columns and "oi_value" in odf.columns:
                        odf["ts"] = pd.to_datetime(odf["timestamp_utc"], unit="ms", utc=True)
                        odf = odf.set_index("ts")[["oi_value"]].sort_index()
                        odf = odf[~odf.index.duplicated(keep="last")]
                        odf_reindexed = odf.reindex(candle_idx, method="ffill")
                        df["oi_value"] = odf_reindexed["oi_value"].bfill().fillna(0.0).values
                else:
                    df["oi_value"] = 0.0

                # LS ratio
                ls_cursor = db["bybit_ls_ratio"].find(query).sort("timestamp_utc", 1)
                ls_docs = await ls_cursor.to_list(length=100000)
                if ls_docs:
                    ldf = pd.DataFrame(ls_docs)
                    if "timestamp_utc" in ldf.columns and "buy_ratio" in ldf.columns:
                        ldf["ts"] = pd.to_datetime(ldf["timestamp_utc"], unit="ms", utc=True)
                        ldf = ldf.set_index("ts")[["buy_ratio"]].sort_index()
                        ldf = ldf[~ldf.index.duplicated(keep="last")]
                        ldf_reindexed = ldf.reindex(candle_idx, method="ffill")
                        df["buy_ratio"] = ldf_reindexed["buy_ratio"].fillna(0.5).values
                else:
                    df["buy_ratio"] = 0.5

                # Binance funding rates (for cross-exchange spread signals)
                bin_cursor = db["binance_funding_rates"].find(query).sort("timestamp_utc", 1)
                bin_docs = await bin_cursor.to_list(length=100000)
                if bin_docs:
                    bdf = pd.DataFrame(bin_docs)
                    if "timestamp_utc" in bdf.columns and "funding_rate" in bdf.columns:
                        bdf["ts"] = pd.to_datetime(bdf["timestamp_utc"], unit="ms", utc=True)
                        bdf = bdf.set_index("ts")[["funding_rate"]].sort_index()
                        bdf = bdf.rename(columns={"funding_rate": "binance_funding_rate"})
                        bdf = bdf[~bdf.index.duplicated(keep="last")]
                        bdf_reindexed = bdf.reindex(candle_idx, method="ffill")
                        df["binance_funding_rate"] = bdf_reindexed["binance_funding_rate"].fillna(0.0).values
                else:
                    df["binance_funding_rate"] = 0.0

                # Coinalyze liquidations (daily resolution — cross-exchange aggregated)
                # Provides: liq_long_usd, liq_short_usd, liq_total_usd
                # Neutral fill: 0.0 (no liquidation = no signal)
                liq_cursor = db["coinalyze_liquidations"].find(
                    {"pair": pair, "resolution": "daily"}
                ).sort("timestamp_utc", 1)
                liq_docs = await liq_cursor.to_list(length=100000)
                if liq_docs:
                    liqdf = pd.DataFrame(liq_docs)
                    if "timestamp_utc" in liqdf.columns:
                        liqdf["ts"] = pd.to_datetime(liqdf["timestamp_utc"], unit="ms", utc=True)
                        liqdf = liqdf.set_index("ts").sort_index()
                        liqdf = liqdf[~liqdf.index.duplicated(keep="last")]
                        for col, default in [
                            ("long_liquidations_usd", 0.0),
                            ("short_liquidations_usd", 0.0),
                            ("total_liquidations_usd", 0.0),
                        ]:
                            if col in liqdf.columns:
                                col_reindexed = liqdf[[col]].reindex(candle_idx, method="ffill")
                                df[col] = col_reindexed[col].fillna(default).values
                            else:
                                df[col] = default
                else:
                    df["long_liquidations_usd"] = 0.0
                    df["short_liquidations_usd"] = 0.0
                    df["total_liquidations_usd"] = 0.0

                shared_candles[feed_key] = df
                enriched += 1

            except Exception as e:
                logger.warning(f"Failed to merge derivatives for {pair}: {e}")

        # Merge fear_greed_index and BTC regime features (pair-independent, do once)
        await self._merge_sentiment_and_regime(shared_candles, pairs)

        return enriched

    async def _merge_sentiment_and_regime(
        self, shared_candles: dict, pairs: List[str]
    ) -> None:
        """Merge fear_greed_value and BTC regime features into all pair candles.

        These are pair-independent features (same value for all pairs at a given time).
        Loaded once from MongoDB / BTC candle parquet, then merged into each pair's 1h candles.

        NaN safety: all columns filled with neutral defaults after merge.
        """
        db = self.mongodb_client.get_database()

        # 1. Fear & Greed Index
        fg_cursor = db["fear_greed_index"].find(
            {}, {"timestamp_utc": 1, "value": 1, "_id": 0}
        ).sort("timestamp_utc", 1)
        fg_docs = await fg_cursor.to_list(length=100000)
        fg_df = None
        if fg_docs:
            fgd = pd.DataFrame(fg_docs)
            if "timestamp_utc" in fgd.columns and "value" in fgd.columns:
                fgd["ts"] = pd.to_datetime(fgd["timestamp_utc"], unit="ms", utc=True)
                fg_df = fgd.set_index("ts")[["value"]].rename(
                    columns={"value": "fear_greed_value"}
                ).sort_index()
                fg_df = fg_df[~fg_df.index.duplicated(keep="last")]
                logger.info(f"Fear & Greed: {len(fg_df)} entries loaded for merge")

        # 2. BTC regime from parquet
        btc_key = f"{self.connector_name}_BTC-USDT_1h"
        btc_df = shared_candles.get(btc_key)
        regime_df = None
        if btc_df is not None and len(btc_df) > 200:
            btc_idx = pd.to_datetime(btc_df["timestamp"], unit="s", utc=True)
            close = btc_df["close"].values
            close_s = pd.Series(close, index=btc_idx)
            ret_4h = (close_s / close_s.shift(4) - 1) * 100
            regime_df = pd.DataFrame({"btc_return_4h": ret_4h}).dropna()
            logger.info(f"BTC regime: {len(regime_df)} bars for merge")

        # 3. Merge into each pair's candle feed
        for pair in pairs:
            feed_key = f"{self.connector_name}_{pair}_1h"
            df = shared_candles.get(feed_key)
            if df is None or len(df) == 0:
                continue

            candle_idx = pd.to_datetime(df["timestamp"], unit="s", utc=True)

            # Fear & Greed
            if fg_df is not None:
                fg_reindexed = fg_df.reindex(candle_idx, method="ffill")
                df["fear_greed_value"] = fg_reindexed["fear_greed_value"].fillna(50.0).values
            else:
                df["fear_greed_value"] = 50.0

            # BTC regime
            if regime_df is not None:
                reg_reindexed = regime_df.reindex(candle_idx, method="ffill")
                df["btc_return_4h"] = reg_reindexed["btc_return_4h"].fillna(0.0).values
            else:
                df["btc_return_4h"] = 0.0

            shared_candles[feed_key] = df

    def _discover_pairs(self, start_ts: int = 0) -> List[str]:
        """Find pairs with sufficient parquet data covering the backtest window.

        Skips pairs whose backtesting-resolution data starts AFTER the backtest
        start time — those would trigger a live API fetch that hangs.
        """
        import pandas as pd
        candles_dir = data_paths.candles_dir
        resolution = self.backtesting_resolution
        pairs = []
        skipped = []
        for f in candles_dir.glob(f"{self.connector_name}|*|1h.parquet"):
            parts = f.stem.split("|")
            if len(parts) != 3:
                continue
            pair = parts[1]
            # Check resolution data covers backtest start
            if start_ts > 0:
                res_file = candles_dir / f"{self.connector_name}|{pair}|{resolution}.parquet"
                if not res_file.exists():
                    skipped.append(pair)
                    continue
                df = pd.read_parquet(res_file, columns=["timestamp"])
                if df["timestamp"].min() > start_ts:
                    skipped.append(pair)
                    continue
            pairs.append(pair)
        if skipped:
            logger.info(f"Skipped {len(skipped)} pairs (insufficient {resolution} data): {skipped[:5]}...")
        return sorted(pairs)

    def _get_data_end_time(self, pairs: List[str]) -> int:
        """Find the earliest end time across all resolution parquet files.

        Checks BOTH the backtesting resolution (e.g. 1m) AND the 1h feed.
        Uses the minimum of both as the binding constraint. This prevents
        initialize_candles_feed from refetching 1h data (which would overwrite
        derivatives columns merged by _merge_derivatives_into_candles).

        Without this, a 1m feed ending at 11:16 but 1h feed ending at 11:00
        would cause the engine to refetch 1h data from parquet, losing the
        merged funding_rate/oi_value/buy_ratio columns and producing 0 signals.
        """
        import pandas as pd
        candles_dir = data_paths.candles_dir
        min_end = float("inf")

        # Check both resolutions: backtesting (1m) and signal (1h)
        resolutions_to_check = {self.backtesting_resolution}
        # Always include 1h since that's where derivatives are merged
        resolutions_to_check.add("1h")

        for resolution in resolutions_to_check:
            for pair in pairs[:5]:  # sample first 5 pairs (all downloaded together)
                f = candles_dir / f"{self.connector_name}|{pair}|{resolution}.parquet"
                if f.exists():
                    df = pd.read_parquet(f, columns=["timestamp"])
                    end = df["timestamp"].max()
                    if end < min_end:
                        min_end = end

        if min_end == float("inf"):
            logger.warning("No parquet data found for end time — using now()")
            return int(datetime.now(timezone.utc).timestamp())

        logger.info(f"Data end time: {datetime.fromtimestamp(min_end, tz=timezone.utc)} "
                     f"(resolution={self.backtesting_resolution})")
        return int(min_end)

    @staticmethod
    def _compute_max_dd(edf, trade_cost: float = 0.000375) -> float:
        """Compute real peak-to-trough max drawdown from executor DataFrame.

        Builds an equity curve from sequential trade PnLs, tracks the peak,
        and returns the worst drawdown as a negative ratio (e.g. -0.15 = -15%).

        HB's built-in max_drawdown_pct is broken: it computes cumulative PnL
        drawdown, not peak-to-trough equity drawdown, and can exceed -100%
        which is impossible at 1x leverage.

        Returns 0.0 if no trade data available.
        """
        if edf is None or len(edf) == 0:
            return 0.0

        pnl_col = "net_pnl_quote"
        if pnl_col not in edf.columns:
            return 0.0

        # Sort by close time to build chronological equity curve
        df = edf.sort_values("close_timestamp") if "close_timestamp" in edf.columns else edf

        # Starting capital = typical position size (from filled_amount_quote)
        if "filled_amount_quote" in df.columns:
            capital = float(df["filled_amount_quote"].median())
            if capital <= 0:
                capital = 300.0
        else:
            capital = 300.0

        equity = capital
        peak = equity
        max_dd = 0.0

        for pnl in df[pnl_col]:
            pnl_f = float(pnl) if pnl is not None else 0.0
            equity += pnl_f
            if equity > peak:
                peak = equity
            if peak > 0:
                dd = (equity - peak) / peak
                if dd < max_dd:
                    max_dd = dd

        return max_dd

    def _compute_trade_quality(self, executors_df) -> dict:
        """Compute trade-level quality metrics from backtest executors DataFrame.

        Returns dict with monthly_wr, max_loss_streak, side balance PFs,
        and composite sub-scores for the verdict function.
        """
        if executors_df is None or len(executors_df) == 0:
            return {
                "monthly_wr": 0.0, "max_loss_streak": 0,
                "long_pf": 0.0, "short_pf": 0.0,
                "composite_quality": 0.0,
            }

        df = executors_df.copy()

        # Ensure numeric columns are float — subprocess pickle can produce
        # StringDtype (PyArrow) that breaks comparisons like `> 0`.
        for col in ["net_pnl_quote", "net_pnl_pct", "cum_fees_quote",
                     "filled_amount_quote", "timestamp", "close_timestamp"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # --- Monthly consistency ---
        if "timestamp" in df.columns:
            df["_month"] = pd.to_datetime(df["timestamp"], unit="s").dt.to_period("M")
        elif "datetime" in df.columns:
            df["_month"] = pd.to_datetime(df["datetime"]).dt.to_period("M")
        else:
            df["_month"] = 0

        pnl_col = "net_pnl_quote"
        if pnl_col not in df.columns:
            pnl_col = "net_pnl_pct"  # fallback

        monthly_pnl = df.groupby("_month")[pnl_col].sum()
        win_months = (monthly_pnl > 0).sum()
        total_months = max(len(monthly_pnl), 1)
        monthly_wr = win_months / total_months

        # --- Max consecutive losses ---
        is_loss = (df[pnl_col] < 0).astype(int)
        if len(is_loss) > 0:
            streaks = is_loss.groupby((is_loss != is_loss.shift()).cumsum()).sum()
            max_loss_streak = int(streaks.max()) if len(streaks) > 0 else 0
        else:
            max_loss_streak = 0

        # --- Side-specific profit factors ---
        def _side_pf(subset):
            if len(subset) == 0:
                return 0.0
            wins = subset[subset[pnl_col] > 0][pnl_col].sum()
            losses = abs(subset[subset[pnl_col] <= 0][pnl_col].sum())
            return float(wins / losses) if losses > 0 else 0.0

        side_col = "side" if "side" in df.columns else None
        if side_col:
            long_mask = df[side_col].astype(str).str.contains("BUY", na=False)
            short_mask = df[side_col].astype(str).str.contains("SELL", na=False)
            long_pf = _side_pf(df[long_mask])
            short_pf = _side_pf(df[short_mask])
        else:
            long_pf = 0.0
            short_pf = 0.0

        # --- Composite quality sub-score (0-1) ---
        # Mirrors governance weights but uses only bulk-available data
        monthly_score = monthly_wr  # already 0-1
        streak_score = float(np.clip(1.0 - (max_loss_streak - 3) / 15, 0, 1))
        trade_vol_score = float(np.clip(len(df) / 100, 0, 1))

        # Side balance: both sides profitable = bonus, one side dead = penalty
        dominant_pf = max(short_pf, long_pf)
        weak_pf = min(short_pf, long_pf)
        if weak_pf >= 1.0:
            side_score = float(np.clip((dominant_pf - 1.0) / 0.5, 0, 1))
        elif weak_pf >= 0.9:
            side_score = float(np.clip((dominant_pf - 1.0) / 0.5, 0, 1)) * 0.7
        else:
            side_score = float(np.clip((dominant_pf - 1.0) / 0.5, 0, 1)) * 0.4

        composite_quality = (
            0.35 * monthly_score +
            0.25 * streak_score +
            0.20 * trade_vol_score +
            0.20 * side_score
        )

        return {
            "monthly_wr": monthly_wr,
            "max_loss_streak": max_loss_streak,
            "long_pf": long_pf,
            "short_pf": short_pf,
            "composite_quality": composite_quality,
        }

    def _compute_verdict(self, pf: float, trades: int,
                          sharpe: float = 0.0, max_dd: float = 0.0,
                          trade_quality: dict = None) -> tuple:
        """Determine ALLOW/WATCH/BLOCK from backtest results.

        Multi-gate verdict aligned with quant governance framework:

        Gate 1: Hard floors (instant BLOCK)
          - trades < 30
          - PF < 1.05 (not even marginally positive)

        Gate 2: Core metrics (PF, Sharpe, drawdown)
          - Carry strategies use relaxed DD thresholds (dd_gate_relaxed)

        Gate 3: Trade quality (monthly consistency, loss streaks, side balance)
          - composite_quality < 0.20 → demote ALLOW→WATCH
          - monthly_wr < 0.40 → demote ALLOW→WATCH (inconsistent)
          - max_loss_streak > 12 → demote ALLOW→WATCH (fragile)

        Returns (verdict: str, quality_detail: dict).
        """
        from app.engines.strategy_registry import get_strategy
        engine_meta = get_strategy(self.engine_name)
        if engine_meta.dd_gate_relaxed:
            dd_allow = -0.25   # -25% (carry strategies have wider stops)
            dd_watch = -0.35   # -35%
        else:
            dd_allow = MAX_DD_ALLOW
            dd_watch = MAX_DD_WATCH

        quality = trade_quality or {}
        detail = {
            "monthly_wr": quality.get("monthly_wr", 0),
            "max_loss_streak": quality.get("max_loss_streak", 0),
            "long_pf": quality.get("long_pf", 0),
            "short_pf": quality.get("short_pf", 0),
            "composite_quality": quality.get("composite_quality", 0),
            "gate_failures": [],
        }

        # Gate 1: Hard floors
        if trades < MIN_TRADES:
            detail["gate_failures"].append(f"trades={trades} < {MIN_TRADES}")
            return "BLOCK", detail
        if pf < 1.05:
            detail["gate_failures"].append(f"PF={pf:.2f} < 1.05")
            return "BLOCK", detail

        # Gate 2: Core metrics tier
        if (pf >= PF_ALLOW and sharpe >= SHARPE_ALLOW
                and max_dd >= dd_allow):
            verdict = "ALLOW"
        elif (pf >= 1.05 and sharpe >= SHARPE_WATCH
                and max_dd >= dd_watch):
            verdict = "WATCH"
        else:
            # Identify which gate failed for diagnostics
            if pf < 1.05:
                detail["gate_failures"].append(f"PF={pf:.2f} < 1.05")
            if sharpe < SHARPE_WATCH:
                detail["gate_failures"].append(f"Sharpe={sharpe:.2f} < {SHARPE_WATCH}")
            if max_dd < dd_watch:
                detail["gate_failures"].append(f"DD={max_dd:.2f} < {dd_watch}")
            return "BLOCK", detail

        # Gate 3: Trade quality (only applies if we have trade data)
        if verdict == "ALLOW" and quality:
            demote_reasons = []
            if quality.get("composite_quality", 1) < 0.20:
                demote_reasons.append(
                    f"quality={quality['composite_quality']:.2f} < 0.20")
            if quality.get("monthly_wr", 1) < 0.40:
                demote_reasons.append(
                    f"monthly_wr={quality['monthly_wr']:.0%} < 40%")
            if quality.get("max_loss_streak", 0) > 12:
                demote_reasons.append(
                    f"loss_streak={quality['max_loss_streak']} > 12")
            if demote_reasons:
                detail["gate_failures"] = demote_reasons
                verdict = "WATCH"
                logger.info(
                    f"  Demoted ALLOW→WATCH: {', '.join(demote_reasons)}")

        return verdict, detail

    def _compute_funding_pnl(
        self, shared_candles: dict, pair: str, entry_ts: float,
        exit_ts: float, side: str, position_value: float,
    ) -> dict:
        """Compute funding PnL accumulated during a trade's hold period.

        Bybit funding settles every 8h (00:00, 08:00, 16:00 UTC).
        - Positive funding_rate: longs PAY shorts
        - Negative funding_rate: shorts PAY longs
        - Payment = position_value * funding_rate

        If you're SHORT and rate is positive → you RECEIVE funding (positive PnL).
        If you're LONG and rate is positive → you PAY funding (negative PnL).

        Returns dict with funding_pnl, funding_payments (count), avg_rate.
        """
        feed_key = f"{self.connector_name}_{pair}_1h"
        df = shared_candles.get(feed_key)
        if df is None or "funding_rate" not in df.columns:
            return {"funding_pnl": 0.0, "funding_payments": 0, "avg_rate": 0.0}

        # Filter candles within the hold period
        mask = (df["timestamp"] >= entry_ts) & (df["timestamp"] <= exit_ts)
        hold_df = df[mask]

        if len(hold_df) == 0:
            return {"funding_pnl": 0.0, "funding_payments": 0, "avg_rate": 0.0}

        # Funding settles every 8h. With 1h candles, find settlement bars.
        # Settlement hours: 0, 8, 16 UTC
        settlement_mask = hold_df["timestamp"].apply(
            lambda ts: pd.Timestamp(ts, unit="s", tz="UTC").hour in (0, 8, 16)
        )
        settlement_bars = hold_df[settlement_mask]

        if len(settlement_bars) == 0:
            return {"funding_pnl": 0.0, "funding_payments": 0, "avg_rate": 0.0}

        # Side multiplier: short receives when rate positive, long pays
        # funding_payment = position_value * rate * (-1 if long, +1 if short)
        side_mult = -1.0 if side.upper() in ("BUY", "TRADEYPE.BUY") else 1.0

        rates = settlement_bars["funding_rate"].values
        total_funding = float(np.sum(rates)) * position_value * side_mult
        avg_rate = float(np.mean(rates)) if len(rates) > 0 else 0.0

        return {
            "funding_pnl": total_funding,
            "funding_payments": len(settlement_bars),
            "avg_rate": avg_rate,
        }

    async def _store_trades(self, db, bt_result, pair: str, period_label: str,
                            run_id: str, shared_candles: dict = None):
        """Store individual trade records from a backtest result.

        If shared_candles is provided and contains funding_rate data,
        computes and includes funding PnL for each trade (critical for
        carry strategy backtesting).
        """
        if not hasattr(bt_result, "executors") or not bt_result.executors:
            return 0
        try:
            edf = bt_result.executors_df
        except (KeyError, AttributeError):
            return 0
        if len(edf) == 0:
            return 0

        trades_coll = db["backtest_trades"]

        docs = []
        total_funding_pnl = 0.0
        for _, row in edf.iterrows():
            entry_ts = float(row.get("timestamp", 0))
            exit_ts = float(row.get("close_timestamp", 0))
            side = str(row.get("side", ""))
            position_value = float(row["filled_amount_quote"]) if "filled_amount_quote" in row and row["filled_amount_quote"] is not None else 0.0
            net_pnl_quote = float(row["net_pnl_quote"]) if "net_pnl_quote" in row and row["net_pnl_quote"] is not None else 0.0

            # Compute funding PnL during hold period
            funding_info = {"funding_pnl": 0.0, "funding_payments": 0, "avg_rate": 0.0}
            if shared_candles is not None and position_value > 0:
                funding_info = self._compute_funding_pnl(
                    shared_candles, pair, entry_ts, exit_ts, side, position_value
                )
                total_funding_pnl += funding_info["funding_pnl"]

            # Adjusted PnL = price PnL + funding PnL
            adjusted_pnl = net_pnl_quote + funding_info["funding_pnl"]

            doc = {
                "engine": self.engine_name,
                "pair": pair,
                "period": period_label,
                "run_id": run_id,
                "timestamp": entry_ts,
                "close_timestamp": exit_ts,
                "side": side,
                "close_type": str(row.get("close_type", "")),
                "net_pnl_quote": net_pnl_quote,
                "net_pnl_pct": float(row["net_pnl_pct"]) if "net_pnl_pct" in row and row["net_pnl_pct"] is not None else None,
                "cum_fees_quote": float(row["cum_fees_quote"]) if "cum_fees_quote" in row and row["cum_fees_quote"] is not None else None,
                "filled_amount_quote": position_value if position_value else None,
                # Funding PnL fields
                "funding_pnl": funding_info["funding_pnl"],
                "funding_payments": funding_info["funding_payments"],
                "avg_funding_rate": funding_info["avg_rate"],
                "adjusted_pnl_quote": adjusted_pnl,  # price PnL + funding PnL
            }
            # Convert any remaining Decimal values
            for k, v in doc.items():
                if isinstance(v, Decimal):
                    doc[k] = float(v)
            docs.append(doc)

        if docs:
            # Clear previous trades for this engine+pair+period, then insert
            await trades_coll.delete_many({
                "engine": self.engine_name, "pair": pair, "run_id": run_id,
            })
            await trades_coll.insert_many(docs)

        if total_funding_pnl != 0.0:
            logger.info(f"  {pair}: funding PnL = {total_funding_pnl:+.2f} across {len(docs)} trades")

        return len(docs)

    async def _run_pair_subprocess(
        self, pair: str, start_ts: int, end_ts: int,
    ) -> tuple:
        """Run one pair's backtest in a subprocess for memory isolation.

        Returns (results_dict, executors_df) or (None, None) on error.
        The subprocess loads parquet, merges derivatives, runs BacktestingEngine,
        and writes results to a temp pickle file. When the subprocess exits,
        the OS reclaims all memory (3-5 GB per pair at 1m).
        """
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
            output_path = tmp.name

        try:
            cmd = [
                _PYTHON, "-m", "app.tasks.backtesting._backtest_worker",
                "--engine", self.engine_name,
                "--connector", self.connector_name,
                "--pair", pair,
                "--start", str(start_ts),
                "--end", str(end_ts),
                "--resolution", self.backtesting_resolution,
                "--trade-cost", str(self.trade_cost),
                "--output", output_path,
            ]

            proc = subprocess.run(
                cmd, capture_output=True, text=True,
                timeout=600, cwd=_PROJECT_ROOT,
                env={**os.environ},
            )

            if proc.returncode != 0:
                logger.error(f"  {pair}: subprocess failed (exit {proc.returncode}): {proc.stderr[-500:]}")
                return None, None

            with open(output_path, "rb") as f:
                payload = pickle.load(f)

            if payload.get("status") == "error":
                logger.error(f"  {pair}: backtest error: {payload.get('error')}")
                return None, None

            return payload.get("results"), payload.get("executors_df")

        except subprocess.TimeoutExpired:
            logger.error(f"  {pair}: subprocess timed out (600s)")
            return None, None
        except Exception as e:
            logger.error(f"  {pair}: subprocess exception: {e}")
            return None, None
        finally:
            try:
                os.unlink(output_path)
            except OSError:
                pass

    async def _store_trades_from_df(
        self, db, edf, pair: str, period_label: str,
        run_id: str, shared_candles: dict = None,
    ) -> int:
        """Store individual trades from an executor DataFrame returned by subprocess.

        Same logic as _store_trades but works with a raw DataFrame instead of
        a BacktestingResult object.
        """
        if edf is None or len(edf) == 0:
            return 0

        trades_coll = db["backtest_trades"]
        docs = []
        total_funding_pnl = 0.0

        for _, row in edf.iterrows():
            entry_ts = float(row.get("timestamp", 0))
            exit_ts = float(row.get("close_timestamp", 0))
            side = str(row.get("side", ""))
            position_value = float(row["filled_amount_quote"]) if "filled_amount_quote" in row and row["filled_amount_quote"] is not None else 0.0
            net_pnl_quote = float(row["net_pnl_quote"]) if "net_pnl_quote" in row and row["net_pnl_quote"] is not None else 0.0

            # Compute funding PnL during hold period
            funding_info = {"funding_pnl": 0.0, "funding_payments": 0, "avg_rate": 0.0}
            if shared_candles is not None and position_value > 0:
                funding_info = self._compute_funding_pnl(
                    shared_candles, pair, entry_ts, exit_ts, side, position_value
                )
                total_funding_pnl += funding_info["funding_pnl"]

            adjusted_pnl = net_pnl_quote + funding_info["funding_pnl"]

            doc = {
                "engine": self.engine_name,
                "pair": pair,
                "period": period_label,
                "run_id": run_id,
                "timestamp": entry_ts,
                "close_timestamp": exit_ts,
                "side": side,
                "close_type": str(row.get("close_type", "")),
                "net_pnl_quote": net_pnl_quote,
                "net_pnl_pct": float(row["net_pnl_pct"]) if "net_pnl_pct" in row and row["net_pnl_pct"] is not None else None,
                "cum_fees_quote": float(row["cum_fees_quote"]) if "cum_fees_quote" in row and row["cum_fees_quote"] is not None else None,
                "filled_amount_quote": position_value if position_value else None,
                "funding_pnl": funding_info["funding_pnl"],
                "funding_payments": funding_info["funding_payments"],
                "avg_funding_rate": funding_info["avg_rate"],
                "adjusted_pnl_quote": adjusted_pnl,
            }
            for k, v in doc.items():
                if isinstance(v, Decimal):
                    doc[k] = float(v)
            docs.append(doc)

        if docs:
            await trades_coll.delete_many({
                "engine": self.engine_name, "pair": pair, "run_id": run_id,
            })
            await trades_coll.insert_many(docs)

        if total_funding_pnl != 0.0:
            logger.info(f"  {pair}: funding PnL = {total_funding_pnl:+.2f} across {len(docs)} trades")

        return len(docs)

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        # Pre-compute time window so we can filter pairs by data coverage
        if self.pair_source == "explicit" and self.pair_allowlist:
            # Use explicit allowlist (e.g. S6 pair groups)
            pairs = sorted(self.pair_allowlist)
            _end_probe = self._get_data_end_time(pairs)
        else:
            _end_probe = self._get_data_end_time(self._discover_pairs())
            _start_probe = _end_probe - self.backtest_days * 86400
            pairs = self._discover_pairs(start_ts=_start_probe)
        logger.info(
            f"BulkBacktest {self.engine_name}: found {len(pairs)} pairs, "
            f"resolution={self.backtesting_resolution}, days={self.backtest_days}"
        )

        # Use actual parquet data end time — not now() — to avoid triggering
        # live API fetches when data is a few hours stale.
        end_ts = self._get_data_end_time(pairs)
        start_ts = end_ts - self.backtest_days * 86400
        period_label = (
            f"{datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime('%Y-%m-%d')}"
            f"_{datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime('%Y-%m-%d')}"
        )
        run_id = f"{self.engine_name}_{period_label}_{end_ts}"

        # Ensure indexes on backtest_trades for efficient querying
        db = self.mongodb_client.get_database()
        await db["backtest_trades"].create_index([
            ("engine", 1), ("pair", 1), ("run_id", 1),
        ])
        await db["backtest_trades"].create_index([("run_id", 1)])

        stats = {"pairs_tested": 0, "allow": 0, "watch": 0, "block": 0, "errors": 0}
        results = []

        total_trades_stored = 0

        # Load shared candles once for funding PnL computation (used by _store_trades).
        # The actual backtest runs in a subprocess — this is only for post-hoc analysis.
        _shared_cache = BacktestingEngine(load_cached_data=True)
        shared_candles = _shared_cache._bt_engine.backtesting_data_provider.candles_feeds.copy()
        del _shared_cache
        gc.collect()

        # Merge derivatives into shared_candles for funding PnL computation in _store_trades
        from app.engines.strategy_registry import get_strategy
        engine_meta = get_strategy(self.engine_name)
        if "derivatives" in engine_meta.required_features:
            n_enriched = await self._merge_derivatives_into_candles(shared_candles, pairs)
            logger.info(f"Merged derivatives data into {n_enriched}/{len(pairs)} pairs (for funding PnL)")

        for pair in pairs:
            try:
                # Run BacktestingEngine in a subprocess for full memory isolation.
                # Each pair uses 3-5 GB at 1m resolution. The subprocess loads parquet,
                # merges derivatives, runs the engine, and writes results to a temp file.
                # When it exits, the OS reclaims ALL memory.
                r, edf = await self._run_pair_subprocess(
                    pair, start_ts, end_ts,
                )

                if r is None:
                    stats["errors"] += 1
                    continue

                trades = r.get("total_executors", 0)
                pf = r.get("profit_factor", 0) or 0
                wr = (r.get("accuracy_long", 0) or 0) * 100
                sharpe = r.get("sharpe_ratio", None)
                pnl = r.get("net_pnl_quote", 0) or 0
                close_types = r.get("close_types", {})

                # Cast numeric columns — pickle from subprocess can produce
                # string types (from Decimal serialization).
                if edf is not None and len(edf) > 0:
                    for col in ["net_pnl_quote", "net_pnl_pct", "cum_fees_quote",
                                "filled_amount_quote", "timestamp", "close_timestamp"]:
                        if col in edf.columns:
                            edf[col] = pd.to_numeric(edf[col], errors="coerce")

                # Compute REAL peak-to-trough MaxDD from equity curve.
                # HB's max_drawdown_pct is broken (cumulative, not peak-to-trough,
                # can exceed -100% which is impossible at 1x leverage).
                max_dd = self._compute_max_dd(edf, self.trade_cost)

                # Compute trade-level quality metrics from executors
                trade_quality = {}
                if edf is not None and len(edf) > 0:
                    trade_quality = self._compute_trade_quality(edf)

                verdict, quality_detail = self._compute_verdict(
                    pf, trades,
                    sharpe=sharpe or 0.0, max_dd=max_dd or 0.0,
                    trade_quality=trade_quality,
                )

                doc = {
                    "engine": self.engine_name,
                    "pair": pair,
                    "period": period_label,
                    "run_id": run_id,
                    "trades": trades,
                    "profit_factor": pf,
                    "win_rate": wr,
                    "pnl_quote": pnl,
                    "max_dd_pct": max_dd,
                    "sharpe": sharpe,
                    "n_long": r.get("total_long", 0),
                    "n_short": r.get("total_short", 0),
                    "close_types": {str(k): v for k, v in close_types.items()},
                    "verdict": verdict,
                    "funding_pnl_included": "derivatives" in engine_meta.required_features,
                    "created_at": int(datetime.now(timezone.utc).timestamp() * 1000),
                    # Quality metrics (new governance gates)
                    "monthly_wr": quality_detail.get("monthly_wr", 0),
                    "max_loss_streak": quality_detail.get("max_loss_streak", 0),
                    "long_pf": quality_detail.get("long_pf", 0),
                    "short_pf": quality_detail.get("short_pf", 0),
                    "composite_quality": quality_detail.get("composite_quality", 0),
                    "gate_failures": quality_detail.get("gate_failures", []),
                }

                await db["pair_historical"].update_one(
                    {"engine": self.engine_name, "pair": pair},
                    {"$set": doc},
                    upsert=True,
                )

                # Store individual trades for post-hoc analysis (with funding PnL).
                # We reconstruct a lightweight result object from the subprocess data.
                n_stored = await self._store_trades_from_df(
                    db, edf, pair, period_label, run_id,
                    shared_candles=shared_candles,
                )
                total_trades_stored += n_stored

                stats["pairs_tested"] += 1
                stats[verdict.lower()] = stats.get(verdict.lower(), 0) + 1
                results.append({
                    "pair": pair, "pf": pf, "wr": wr, "trades": trades,
                    "verdict": verdict,
                    "monthly_wr": quality_detail.get("monthly_wr", 0),
                    "max_loss_streak": quality_detail.get("max_loss_streak", 0),
                    "composite_quality": quality_detail.get("composite_quality", 0),
                    "gate_failures": quality_detail.get("gate_failures", []),
                })

                gate_str = ""
                if quality_detail.get("gate_failures"):
                    gate_str = f" [{', '.join(quality_detail['gate_failures'])}]"
                logger.info(
                    f"  {pair}: PF={pf:.2f} WR={wr:.1f}% trades={trades} "
                    f"stored={n_stored} "
                    f"MoWR={quality_detail.get('monthly_wr', 0):.0%} "
                    f"streak={quality_detail.get('max_loss_streak', 0)} "
                    f"-> {verdict}{gate_str}"
                )

            except Exception as e:
                stats["errors"] += 1
                import traceback
                logger.error(f"  {pair}: backtest failed -- {e}\n{traceback.format_exc()}")

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(
            f"BulkBacktest {self.engine_name} complete: "
            f"{stats['pairs_tested']} tested, {stats['allow']} ALLOW, "
            f"{stats['watch']} WATCH, {stats['block']} BLOCK, "
            f"{stats['errors']} errors, {total_trades_stored} trades stored "
            f"in {duration:.0f}s"
        )

        return {
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_id": context.execution_id,
            "engine": self.engine_name,
            "period": period_label,
            "run_id": run_id,
            "stats": stats,
            "total_trades_stored": total_trades_stored,
            "results": results,
            "duration_seconds": duration,
        }

    async def on_success(self, context: TaskContext, result) -> None:
        stats = result.result_data.get("stats", {})
        logger.info(
            f"BulkBacktest: {stats['pairs_tested']} pairs -- "
            f"ALLOW={stats['allow']} WATCH={stats['watch']} BLOCK={stats['block']}"
        )

    async def on_failure(self, context: TaskContext, result) -> None:
        logger.error(f"BulkBacktest failed: {result.error_message}")
