"""
Bulk backtest task — runs E1 or E2 backtests across all pairs with parquet data.

Records PF, WR, Sharpe, max_dd per pair per engine.
Writes verdicts (ALLOW/WATCH/BLOCK) to MongoDB pair_historical collection.
Schedule: manual trigger or weekly cron.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from core.backtesting.engine import BacktestingEngine
from core.data_paths import data_paths
from core.tasks import BaseTask, TaskContext

logger = logging.getLogger(__name__)

# Verdict thresholds
PF_ALLOW = 1.3     # profit factor >= 1.3 → ALLOW
PF_WATCH = 1.0     # 1.0 <= PF < 1.3 → WATCH
MIN_TRADES = 10     # need at least 10 trades for statistical significance


class BulkBacktestTask(BaseTask):
    """Run backtests for E1 or E2 across all pairs and update pair_historical."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.engine_name = task_config.get("engine", "E1")
        self.connector_name = task_config.get("connector_name", "bybit_perpetual")
        self.controller_name = task_config.get("controller_name")
        self.controller_config = task_config.get("controller_config", {})
        self.backtest_days = task_config.get("backtest_days", 180)
        self.backtesting_resolution = task_config.get("resolution", "1h")
        self.trade_cost = task_config.get("trade_cost", 0.000375)  # 0.0375% blended
        self.min_bars = task_config.get("min_bars", 500)

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB required for BulkBacktestTask")

    def _discover_pairs(self) -> List[str]:
        """Find pairs with sufficient 1h parquet data."""
        candles_dir = data_paths.candles_dir
        pairs = []
        for f in candles_dir.glob(f"{self.connector_name}|*|1h.parquet"):
            parts = f.stem.split("|")
            if len(parts) == 3:
                pairs.append(parts[1])
        return sorted(pairs)

    def _make_controller_config(self, pair: str) -> Dict[str, Any]:
        """Build controller config dict for a specific pair."""
        base = {
            "controller_name": self.controller_name,
            "controller_type": "directional_trading",
            "connector_name": self.connector_name,
            "trading_pair": pair,
            "total_amount_quote": 100,
            "max_executors_per_side": 1,
            "cooldown_time": 60,
        }
        base.update(self.controller_config)
        return base

    def _compute_verdict(self, pf: float, trades: int) -> str:
        """Determine ALLOW/WATCH/BLOCK from backtest results."""
        if trades < MIN_TRADES:
            return "BLOCK"
        if pf >= PF_ALLOW:
            return "ALLOW"
        if pf >= PF_WATCH:
            return "WATCH"
        return "BLOCK"

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        pairs = self._discover_pairs()
        logger.info(f"BulkBacktest {self.engine_name}: found {len(pairs)} pairs with 1h data")

        # Load backtesting engine (loads all parquet data)
        bt_engine = BacktestingEngine(load_cached_data=True)

        now_ts = int(datetime.now(timezone.utc).timestamp())
        start_ts = now_ts - self.backtest_days * 86400
        period_label = f"{datetime.utcfromtimestamp(start_ts).strftime('%Y-%m-%d')}_{datetime.utcfromtimestamp(now_ts).strftime('%Y-%m-%d')}"

        stats = {"pairs_tested": 0, "allow": 0, "watch": 0, "block": 0, "errors": 0}
        results = []

        for pair in pairs:
            try:
                config_dict = self._make_controller_config(pair)
                config_instance = bt_engine.get_controller_config_instance_from_dict(config_dict)

                bt_result = await bt_engine.run_backtesting(
                    config=config_instance,
                    start=start_ts,
                    end=now_ts,
                    backtesting_resolution=self.backtesting_resolution,
                    trade_cost=self.trade_cost,
                )

                r = bt_result.results
                trades = r.get("total_executors", 0)
                pf = r.get("profit_factor", 0)
                wr = r.get("win_rate", 0) if r.get("win_rate") else 0
                sharpe = r.get("sharpe_ratio", None)
                max_dd = r.get("max_drawdown_pct", None)
                pnl = r.get("net_pnl_quote", 0)

                verdict = self._compute_verdict(pf, trades)

                doc = {
                    "engine": self.engine_name,
                    "pair": pair,
                    "period": period_label,
                    "trades": trades,
                    "profit_factor": pf,
                    "win_rate": wr,
                    "pnl_quote": pnl,
                    "max_dd_pct": max_dd,
                    "sharpe": sharpe,
                    "n_long": r.get("total_long", 0),
                    "n_short": r.get("total_short", 0),
                    "verdict": verdict,
                    "created_at": int(datetime.now(timezone.utc).timestamp() * 1000),
                }

                # Upsert to MongoDB
                db = self.mongodb_client.get_database()
                await db["pair_historical"].update_one(
                    {"engine": self.engine_name, "pair": pair},
                    {"$set": doc},
                    upsert=True,
                )

                stats["pairs_tested"] += 1
                stats[verdict.lower()] = stats.get(verdict.lower(), 0) + 1
                results.append({"pair": pair, "pf": pf, "wr": wr, "trades": trades, "verdict": verdict})

                logger.info(f"  {pair}: PF={pf:.2f} WR={wr:.1f}% trades={trades} → {verdict}")

            except Exception as e:
                stats["errors"] += 1
                logger.error(f"  {pair}: backtest failed — {e}")

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(
            f"BulkBacktest {self.engine_name} complete: "
            f"{stats['pairs_tested']} tested, {stats['allow']} ALLOW, "
            f"{stats['watch']} WATCH, {stats['block']} BLOCK, "
            f"{stats['errors']} errors in {duration:.0f}s"
        )

        return {
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_id": context.execution_id,
            "engine": self.engine_name,
            "period": period_label,
            "stats": stats,
            "results": results,
            "duration_seconds": duration,
        }

    async def on_success(self, context: TaskContext, result) -> None:
        stats = result.result_data.get("stats", {})
        logger.info(
            f"BulkBacktest: {stats['pairs_tested']} pairs — "
            f"ALLOW={stats['allow']} WATCH={stats['watch']} BLOCK={stats['block']}"
        )

    async def on_failure(self, context: TaskContext, result) -> None:
        logger.error(f"BulkBacktest failed: {result.error_message}")
