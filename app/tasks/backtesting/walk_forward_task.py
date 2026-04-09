"""
Walk-forward backtest task — proper out-of-sample validation.

Splits a history window into rolling train/test folds and runs backtests
on each separately.  Verdicts are computed from TEST-period metrics only.
Flags overfitting when train PF >> test PF.

Results stored to pair_historical with period_type ("train"/"test") and
fold_index for post-hoc analysis.

Schedule: weekly (replaces or supplements BulkBacktestTask).
"""
import gc
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Tuple

from core.backtesting.engine import BacktestingEngine
from core.data_paths import data_paths
from core.tasks import BaseTask, TaskContext

from app.engines.strategy_registry import get_strategy, build_backtest_config
from app.tasks.notifying_task import NotifyingTaskMixin

logger = logging.getLogger(__name__)

# Verdict thresholds (same as BulkBacktestTask)
PF_ALLOW = 1.3
PF_WATCH = 1.0
MIN_TRADES = 10

# Overfitting detection
OVERFIT_RATIO_THRESHOLD = 2.0  # train_pf / test_pf > 2.0 = red flag


class WalkForwardBacktestTask(NotifyingTaskMixin, BaseTask):
    """Run walk-forward backtests with rolling train/test folds."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.engine_name = task_config.get("engine", "E1")
        self.connector_name = task_config.get("connector_name", "bybit_perpetual")
        self.total_days = task_config.get("total_days", 365)
        self.train_days = task_config.get("train_days", 270)
        self.test_days = task_config.get("test_days", 90)
        self.step_days = task_config.get("step_days", 30)
        self.trade_cost = task_config.get("trade_cost", 0.000375)

        meta = get_strategy(self.engine_name)
        self.backtesting_resolution = meta.backtesting_resolution

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB required for WalkForwardBacktestTask")

    def _discover_pairs(self) -> List[str]:
        """Find pairs with sufficient 1h parquet data."""
        candles_dir = data_paths.candles_dir
        pairs = []
        for f in candles_dir.glob(f"{self.connector_name}|*|1h.parquet"):
            parts = f.stem.split("|")
            if len(parts) == 3:
                pairs.append(parts[1])
        return sorted(pairs)

    def _compute_folds(self) -> List[Dict[str, Any]]:
        """Compute rolling train/test fold timestamps.

        Returns list of dicts with: fold_index, train_start, train_end,
        test_start, test_end (all as epoch seconds).
        """
        now_ts = int(datetime.now(timezone.utc).timestamp())
        history_start = now_ts - self.total_days * 86400

        folds = []
        fold_idx = 0
        train_start = history_start

        while True:
            train_end = train_start + self.train_days * 86400
            test_start = train_end
            test_end = test_start + self.test_days * 86400

            if test_end > now_ts:
                # Last fold: extend test to now, but only if there's
                # enough test data (at least half the test window)
                if now_ts - test_start >= self.test_days * 86400 * 0.5:
                    test_end = now_ts
                else:
                    break

            folds.append({
                "fold_index": fold_idx,
                "train_start": train_start,
                "train_end": train_end,
                "test_start": test_start,
                "test_end": test_end,
            })

            train_start += self.step_days * 86400
            fold_idx += 1

            if train_start + self.train_days * 86400 > now_ts:
                break

        return folds

    def _compute_verdict(self, pf: float, trades: int) -> str:
        if trades < MIN_TRADES:
            return "BLOCK"
        if pf >= PF_ALLOW:
            return "ALLOW"
        if pf >= PF_WATCH:
            return "WATCH"
        return "BLOCK"

    async def _run_single_backtest(
        self, pair: str, start_ts: int, end_ts: int,
    ) -> Dict[str, Any]:
        """Run a single backtest and return metrics dict."""
        bt_engine = BacktestingEngine(load_cached_data=True)

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

        if not isinstance(bt_result.results.get("close_types"), dict):
            bt_result.results["close_types"] = {}

        r = bt_result.results
        metrics = {
            "trades": r.get("total_executors", 0),
            "profit_factor": r.get("profit_factor", 0) or 0,
            "win_rate": (r.get("accuracy_long", 0) or 0) * 100,
            "sharpe": r.get("sharpe_ratio", None),
            "max_dd_pct": r.get("max_drawdown_pct", None),
            "pnl_quote": r.get("net_pnl_quote", 0) or 0,
            "n_long": r.get("total_long", 0),
            "n_short": r.get("total_short", 0),
            "close_types": {str(k): v for k, v in r.get("close_types", {}).items()},
        }

        # Store trades
        n_trades_stored = 0
        if hasattr(bt_result, "executors_df") and bt_result.executors_df is not None:
            n_trades_stored = len(bt_result.executors_df)

        del bt_engine, bt_result
        gc.collect()

        return metrics

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start_time = datetime.now(timezone.utc)
        pairs = self._discover_pairs()
        folds = self._compute_folds()

        logger.info(
            f"WalkForward {self.engine_name}: {len(pairs)} pairs, "
            f"{len(folds)} folds (train={self.train_days}d, test={self.test_days}d, "
            f"step={self.step_days}d)"
        )

        if not folds:
            return {
                "status": "completed",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "execution_id": context.execution_id,
                "error": "No valid folds computed (insufficient history?)",
            }

        db = self.mongodb_client.get_database()
        run_id = f"wf_{self.engine_name}_{int(start_time.timestamp())}"

        # Ensure indexes
        await db["walk_forward_results"].create_index([
            ("engine", 1), ("pair", 1), ("run_id", 1), ("fold_index", 1),
        ])

        stats = {
            "pairs_tested": 0,
            "folds_per_pair": len(folds),
            "total_fold_runs": 0,
            "overfit_flags": 0,
            "errors": 0,
        }
        pair_summaries = []
        overfit_alerts = []

        for pair in pairs:
            try:
                fold_results = []

                for fold in folds:
                    fold_idx = fold["fold_index"]

                    # --- Train period ---
                    try:
                        train_metrics = await self._run_single_backtest(
                            pair, fold["train_start"], fold["train_end"],
                        )
                        train_metrics["period_type"] = "train"
                        train_metrics["fold_index"] = fold_idx
                        stats["total_fold_runs"] += 1
                    except Exception as e:
                        logger.warning(f"  {pair} fold {fold_idx} train failed: {e}")
                        train_metrics = None

                    # --- Test period ---
                    try:
                        test_metrics = await self._run_single_backtest(
                            pair, fold["test_start"], fold["test_end"],
                        )
                        test_metrics["period_type"] = "test"
                        test_metrics["fold_index"] = fold_idx
                        stats["total_fold_runs"] += 1
                    except Exception as e:
                        logger.warning(f"  {pair} fold {fold_idx} test failed: {e}")
                        test_metrics = None

                    # Store both to MongoDB
                    for metrics, period_type in [
                        (train_metrics, "train"),
                        (test_metrics, "test"),
                    ]:
                        if metrics is None:
                            continue
                        ts_label = (
                            f"{datetime.fromtimestamp(fold[f'{period_type}_start'], tz=timezone.utc).strftime('%Y-%m-%d')}"
                            f"_{datetime.fromtimestamp(fold[f'{period_type}_end'], tz=timezone.utc).strftime('%Y-%m-%d')}"
                        )
                        doc = {
                            "engine": self.engine_name,
                            "pair": pair,
                            "run_id": run_id,
                            "fold_index": fold_idx,
                            "period_type": period_type,
                            "period": ts_label,
                            **metrics,
                            "created_at": int(datetime.now(timezone.utc).timestamp() * 1000),
                        }
                        await db["walk_forward_results"].insert_one(doc)

                    if train_metrics and test_metrics:
                        fold_results.append({
                            "fold": fold_idx,
                            "train_pf": train_metrics["profit_factor"],
                            "test_pf": test_metrics["profit_factor"],
                            "train_trades": train_metrics["trades"],
                            "test_trades": test_metrics["trades"],
                        })

                # Aggregate test-period metrics for this pair
                test_results = [
                    fr for fr in fold_results if fr["test_trades"] >= MIN_TRADES
                ]

                if test_results:
                    avg_test_pf = sum(fr["test_pf"] for fr in test_results) / len(test_results)
                    avg_train_pf = sum(fr["train_pf"] for fr in test_results) / len(test_results)
                    verdict = self._compute_verdict(avg_test_pf, sum(fr["test_trades"] for fr in test_results))

                    # Overfitting check
                    overfit = False
                    if avg_test_pf > 0:
                        overfit_ratio = avg_train_pf / avg_test_pf
                        if overfit_ratio > OVERFIT_RATIO_THRESHOLD:
                            overfit = True
                            stats["overfit_flags"] += 1
                            overfit_alerts.append(
                                f"{pair}: train PF {avg_train_pf:.2f} vs test PF {avg_test_pf:.2f} "
                                f"(ratio {overfit_ratio:.1f}x)"
                            )
                else:
                    avg_test_pf = 0
                    avg_train_pf = 0
                    verdict = "BLOCK"
                    overfit = False

                # Update pair_historical with walk-forward verdict
                await db["pair_historical"].update_one(
                    {"engine": self.engine_name, "pair": pair},
                    {"$set": {
                        "engine": self.engine_name,
                        "pair": pair,
                        "verdict": verdict,
                        "wf_run_id": run_id,
                        "wf_avg_test_pf": avg_test_pf,
                        "wf_avg_train_pf": avg_train_pf,
                        "wf_folds": len(test_results),
                        "wf_overfit": overfit,
                        "updated_at": int(datetime.now(timezone.utc).timestamp() * 1000),
                    }},
                    upsert=True,
                )

                stats["pairs_tested"] += 1
                pair_summaries.append({
                    "pair": pair,
                    "avg_test_pf": round(avg_test_pf, 3),
                    "avg_train_pf": round(avg_train_pf, 3),
                    "verdict": verdict,
                    "overfit": overfit,
                    "folds": len(test_results),
                })

                logger.info(
                    f"  {pair}: test_PF={avg_test_pf:.2f} train_PF={avg_train_pf:.2f} "
                    f"verdict={verdict} folds={len(test_results)}"
                    f"{' OVERFIT' if overfit else ''}"
                )

            except Exception as e:
                stats["errors"] += 1
                logger.error(f"  {pair}: walk-forward failed — {e}")

        # Send overfitting alerts
        if overfit_alerts and self.notification_manager:
            try:
                from core.notifiers.base import NotificationMessage
                examples = "\n".join(f"  {a}" for a in overfit_alerts[:5])
                if len(overfit_alerts) > 5:
                    examples += f"\n  ... and {len(overfit_alerts) - 5} more"
                await self.notification_manager.send_notification(NotificationMessage(
                    title=f"Walk-Forward: {len(overfit_alerts)} Overfitting Flags",
                    message=(
                        f"<b>{self.engine_name} Walk-Forward Overfitting Alert</b>\n\n"
                        f"{examples}\n\n"
                        f"Train PF >> Test PF suggests overfit parameters.\n"
                        f"Review walk_forward_results for run_id={run_id}"
                    ),
                    level="warning",
                ))
            except Exception as e:
                logger.warning(f"Failed to send overfit alert: {e}")

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        # Summary counts
        verdicts = {}
        for ps in pair_summaries:
            verdicts[ps["verdict"]] = verdicts.get(ps["verdict"], 0) + 1

        logger.info(
            f"WalkForward {self.engine_name} complete: "
            f"{stats['pairs_tested']} pairs, {stats['total_fold_runs']} fold runs, "
            f"ALLOW={verdicts.get('ALLOW', 0)} WATCH={verdicts.get('WATCH', 0)} "
            f"BLOCK={verdicts.get('BLOCK', 0)}, "
            f"{stats['overfit_flags']} overfit flags, "
            f"{stats['errors']} errors in {duration:.0f}s"
        )

        return {
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_id": context.execution_id,
            "engine": self.engine_name,
            "run_id": run_id,
            "folds": len(folds),
            "stats": stats,
            "verdicts": verdicts,
            "pair_summaries": pair_summaries,
            "overfit_alerts": overfit_alerts,
            "duration_seconds": duration,
        }

    async def on_success(self, context: TaskContext, result) -> None:
        stats = result.result_data.get("stats", {})
        verdicts = result.result_data.get("verdicts", {})
        logger.info(
            f"WalkForward: {stats.get('pairs_tested', 0)} pairs — "
            f"ALLOW={verdicts.get('ALLOW', 0)} WATCH={verdicts.get('WATCH', 0)} "
            f"BLOCK={verdicts.get('BLOCK', 0)}"
        )

    async def on_failure(self, context: TaskContext, result) -> None:
        logger.error(f"WalkForward failed: {result.error_message}")
