"""
Bulk backtest task — runs backtests across all pairs with parquet data.

Uses the engine registry (app/engines/registry.py) to determine resolution,
candles config, exit params, and config class per engine. No hard-coded
engine-specific logic here.

Records PF, WR, Sharpe, max_dd per pair per engine.
Writes verdicts (ALLOW/WATCH/BLOCK) to MongoDB pair_historical collection.
Stores individual trades to backtest_trades collection for post-hoc analysis.
Schedule: manual trigger or weekly cron.
"""
import gc
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from core.backtesting.engine import BacktestingEngine
from core.data_paths import data_paths
from core.tasks import BaseTask, TaskContext

from app.engines.strategy_registry import build_backtest_config

logger = logging.getLogger(__name__)

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

        Uses the backtesting resolution (e.g. 1m) as the binding constraint
        since that's what the BacktestingEngine iterates over. Falls back to
        now() if no parquet files exist.
        """
        import pandas as pd
        candles_dir = data_paths.candles_dir
        min_end = float("inf")
        resolution = self.backtesting_resolution

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
                     f"(resolution={resolution})")
        return int(min_end)

    def _compute_verdict(self, pf: float, trades: int,
                          sharpe: float = 0.0, max_dd: float = 0.0) -> str:
        """Determine ALLOW/WATCH/BLOCK from backtest results.

        Multi-criteria: a pair must pass ALL thresholds for a given tier.
        PF alone is not enough — Sharpe and max drawdown are hard gates.

        Carry strategies (dd_gate_relaxed=True) use relaxed DD thresholds
        because backtests run unlimited concurrent positions, inflating DD.
        Production uses max 3 concurrent at $300 each.
        """
        from app.engines.strategy_registry import get_strategy
        engine_meta = get_strategy(self.engine_name)
        if engine_meta.dd_gate_relaxed:
            dd_allow = -0.50   # -50% (carry backtest concurrency)
            dd_watch = -0.70   # -70%
        else:
            dd_allow = MAX_DD_ALLOW
            dd_watch = MAX_DD_WATCH

        if trades < MIN_TRADES:
            return "BLOCK"
        if (pf >= PF_ALLOW and sharpe >= SHARPE_ALLOW
                and max_dd >= dd_allow):
            return "ALLOW"
        if (pf >= PF_WATCH and sharpe >= SHARPE_WATCH
                and max_dd >= dd_watch):
            return "WATCH"
        return "BLOCK"

    async def _store_trades(self, db, bt_result, pair: str, period_label: str, run_id: str):
        """Store individual trade records from a backtest result."""
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
        for _, row in edf.iterrows():
            doc = {
                "engine": self.engine_name,
                "pair": pair,
                "period": period_label,
                "run_id": run_id,
                "timestamp": float(row.get("timestamp", 0)),
                "close_timestamp": float(row.get("close_timestamp", 0)),
                "side": str(row.get("side", "")),
                "close_type": str(row.get("close_type", "")),
                "net_pnl_quote": float(row["net_pnl_quote"]) if "net_pnl_quote" in row and row["net_pnl_quote"] is not None else None,
                "net_pnl_pct": float(row["net_pnl_pct"]) if "net_pnl_pct" in row and row["net_pnl_pct"] is not None else None,
                "cum_fees_quote": float(row["cum_fees_quote"]) if "cum_fees_quote" in row and row["cum_fees_quote"] is not None else None,
                "filled_amount_quote": float(row["filled_amount_quote"]) if "filled_amount_quote" in row and row["filled_amount_quote"] is not None else None,
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

        # Pre-load parquet cache ONCE — reused across all pairs (saves ~2GB)
        _shared_cache = BacktestingEngine(load_cached_data=True)
        shared_candles = _shared_cache._bt_engine.backtesting_data_provider.candles_feeds.copy()
        del _shared_cache
        gc.collect()

        # Merge derivatives data into candle DataFrames if engine needs it
        from app.engines.strategy_registry import get_strategy
        engine_meta = get_strategy(self.engine_name)
        if "derivatives" in engine_meta.required_features:
            n_enriched = await self._merge_derivatives_into_candles(shared_candles, pairs)
            logger.info(f"Merged derivatives data into {n_enriched}/{len(pairs)} pairs")

        for pair in pairs:
            try:
                # Fresh engine per pair (HB bug: state corrupts on reuse)
                # but inject shared cache to avoid reloading parquet
                bt_engine = BacktestingEngine(load_cached_data=False)
                bt_engine._bt_engine.backtesting_data_provider.candles_feeds = shared_candles

                # Build config via registry — handles candles, exit params, trailing stop
                config_instance = build_backtest_config(
                    engine_name=self.engine_name,
                    connector=self.connector_name,
                    pair=pair,
                )

                bt_result = await bt_engine.run_backtesting(
                    config=config_instance,
                    start=start_ts,
                    end=end_ts,
                    backtesting_resolution=self.backtesting_resolution,
                    trade_cost=self.trade_cost,
                )

                # Workaround: close_types returns int(0) when no trades
                if not isinstance(bt_result.results.get("close_types"), dict):
                    bt_result.results["close_types"] = {}

                r = bt_result.results
                trades = r.get("total_executors", 0)
                pf = r.get("profit_factor", 0) or 0
                wr = (r.get("accuracy_long", 0) or 0) * 100
                sharpe = r.get("sharpe_ratio", None)
                max_dd = r.get("max_drawdown_pct", None)
                pnl = r.get("net_pnl_quote", 0) or 0
                close_types = r.get("close_types", {})

                verdict = self._compute_verdict(pf, trades,
                    sharpe=sharpe or 0.0, max_dd=max_dd or 0.0)

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
                    "created_at": int(datetime.now(timezone.utc).timestamp() * 1000),
                }

                await db["pair_historical"].update_one(
                    {"engine": self.engine_name, "pair": pair},
                    {"$set": doc},
                    upsert=True,
                )

                # Store individual trades for post-hoc analysis
                n_stored = await self._store_trades(db, bt_result, pair, period_label, run_id)
                total_trades_stored += n_stored

                stats["pairs_tested"] += 1
                stats[verdict.lower()] = stats.get(verdict.lower(), 0) + 1
                results.append({"pair": pair, "pf": pf, "wr": wr, "trades": trades, "verdict": verdict})

                logger.info(f"  {pair}: PF={pf:.2f} WR={wr:.1f}% trades={trades} -> {verdict}")

                del bt_engine, bt_result
                gc.collect()

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
