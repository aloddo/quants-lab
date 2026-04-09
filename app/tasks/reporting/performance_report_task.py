"""
Performance report task — daily rolling metrics + weekly Telegram summary.

Computes rolling 30-day performance metrics per engine from resolved
candidates in MongoDB. Stores daily snapshots to performance_daily
collection. Sends weekly summary via Telegram on Sundays.

Schedule: daily.
"""
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from core.tasks import BaseTask, TaskContext

from app.tasks.notifying_task import NotifyingTaskMixin

logger = logging.getLogger(__name__)


class PerformanceReportTask(NotifyingTaskMixin, BaseTask):
    """Compute and store rolling performance metrics per engine."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.engines = task_config.get("engines", ["E1", "E2"])
        self.rolling_days = task_config.get("rolling_days", 30)
        self.weekly_summary_day = task_config.get("weekly_summary_day", 6)  # 0=Mon, 6=Sun

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB required for PerformanceReportTask")

    async def _compute_engine_metrics(self, engine: str) -> Dict[str, Any]:
        """Compute rolling metrics for one engine from resolved candidates."""
        db = self.mongodb_client.get_database()
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.rolling_days)
        cutoff_ms = int(cutoff.timestamp() * 1000)

        pipeline = [
            {"$match": {
                "engine": engine,
                "disposition": "RESOLVED_TESTNET",
                "testnet_resolved_at": {"$gte": cutoff_ms},
            }},
            {"$group": {
                "_id": None,
                "total_trades": {"$sum": 1},
                "wins": {"$sum": {"$cond": [{"$gt": ["$testnet_pnl", 0]}, 1, 0]}},
                "losses": {"$sum": {"$cond": [{"$lte": ["$testnet_pnl", 0]}, 1, 0]}},
                "total_pnl": {"$sum": "$testnet_pnl"},
                "avg_pnl": {"$avg": "$testnet_pnl"},
                "max_win": {"$max": "$testnet_pnl"},
                "max_loss": {"$min": "$testnet_pnl"},
                "pnl_list": {"$push": "$testnet_pnl"},
            }},
        ]

        results = []
        async for doc in db["candidates"].aggregate(pipeline):
            results.append(doc)

        if not results:
            return {
                "engine": engine,
                "trades": 0,
                "wins": 0,
                "win_rate": 0,
                "total_pnl": 0,
                "avg_pnl": 0,
                "sharpe": None,
                "max_win": 0,
                "max_loss": 0,
            }

        r = results[0]
        trades = r["total_trades"]
        wins = r["wins"]
        win_rate = (wins / trades * 100) if trades > 0 else 0

        # Compute Sharpe (annualized, using daily PnL proxy)
        sharpe = None
        pnl_list = [p for p in r.get("pnl_list", []) if p is not None]
        if len(pnl_list) >= 5:
            import statistics
            mean_pnl = statistics.mean(pnl_list)
            std_pnl = statistics.stdev(pnl_list)
            if std_pnl > 0:
                # Rough annualization: sqrt(trades_per_year)
                trades_per_year = trades * (365 / self.rolling_days)
                sharpe = (mean_pnl / std_pnl) * (trades_per_year ** 0.5)

        # Close type breakdown
        ct_pipeline = [
            {"$match": {
                "engine": engine,
                "disposition": "RESOLVED_TESTNET",
                "testnet_resolved_at": {"$gte": cutoff_ms},
            }},
            {"$group": {
                "_id": "$testnet_close_type",
                "count": {"$sum": 1},
                "avg_pnl": {"$avg": "$testnet_pnl"},
            }},
        ]
        close_types = {}
        async for doc in db["candidates"].aggregate(ct_pipeline):
            ct = doc["_id"] or "UNKNOWN"
            close_types[ct] = {"count": doc["count"], "avg_pnl": doc["avg_pnl"]}

        return {
            "engine": engine,
            "trades": trades,
            "wins": wins,
            "win_rate": round(win_rate, 1),
            "total_pnl": round(r["total_pnl"], 2) if r["total_pnl"] else 0,
            "avg_pnl": round(r["avg_pnl"], 2) if r["avg_pnl"] else 0,
            "sharpe": round(sharpe, 2) if sharpe else None,
            "max_win": round(r["max_win"], 2) if r["max_win"] else 0,
            "max_loss": round(r["max_loss"], 2) if r["max_loss"] else 0,
            "close_types": close_types,
        }

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        today = start.strftime("%Y-%m-%d")

        db = self.mongodb_client.get_database()
        await db["performance_daily"].create_index([
            ("date", 1), ("engine", 1),
        ], unique=True)

        all_metrics = {}
        for engine in self.engines:
            metrics = await self._compute_engine_metrics(engine)
            all_metrics[engine] = metrics

            # Store daily snapshot
            await db["performance_daily"].update_one(
                {"date": today, "engine": engine},
                {"$set": {
                    "date": today,
                    "engine": engine,
                    **metrics,
                    "rolling_days": self.rolling_days,
                    "computed_at": datetime.now(timezone.utc),
                }},
                upsert=True,
            )

            logger.info(
                f"Performance {engine}: {metrics['trades']} trades, "
                f"WR={metrics['win_rate']}%, PnL={metrics['total_pnl']}, "
                f"Sharpe={metrics['sharpe']}"
            )

        # Weekly summary on configured day
        if start.weekday() == self.weekly_summary_day:
            await self._send_weekly_summary(all_metrics)

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        return {
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_id": context.execution_id,
            "date": today,
            "metrics": all_metrics,
            "duration_seconds": duration,
        }

    async def _send_weekly_summary(self, metrics: Dict[str, Dict]) -> None:
        """Send weekly performance summary via Telegram."""
        if not self.notification_manager:
            return

        lines = [f"<b>Weekly Performance Summary ({self.rolling_days}d rolling)</b>\n"]
        for engine, m in metrics.items():
            if m["trades"] == 0:
                lines.append(f"<b>{engine}</b>: No trades")
                continue
            sharpe_s = f"{m['sharpe']:.2f}" if m['sharpe'] else "N/A"
            lines.append(
                f"<b>{engine}</b>: {m['trades']} trades, "
                f"WR={m['win_rate']}%, PnL=${m['total_pnl']:.0f}, "
                f"Sharpe={sharpe_s}"
            )
            if m.get("close_types"):
                ct_parts = []
                for ct_name, ct_data in m["close_types"].items():
                    ct_parts.append(f"{ct_name}={ct_data['count']}")
                lines.append(f"  Close: {', '.join(ct_parts)}")

        try:
            from core.notifiers.base import NotificationMessage
            await self.notification_manager.send_notification(NotificationMessage(
                title="Weekly Performance Summary",
                message="\n".join(lines),
                level="info",
            ))
        except Exception as e:
            logger.warning(f"Failed to send weekly summary: {e}")

    async def on_success(self, context: TaskContext, result) -> None:
        metrics = result.result_data.get("metrics", {})
        for engine, m in metrics.items():
            logger.info(f"Performance {engine}: {m['trades']} trades, PnL={m['total_pnl']}")
