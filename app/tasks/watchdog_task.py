"""
Watchdog task — two jobs:
1. Reap stale task locks (prevents permanent pipeline blockage after crashes).
2. Check data freshness (alerts if candles or features go stale).

Runs every 5 minutes.  Sends Telegram alerts with cooldown (max 1 per hour
for the same issue category).
"""
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from pymongo import MongoClient

from core.tasks import BaseTask, TaskContext, TaskResult, TaskStatus

logger = logging.getLogger(__name__)

# A task lock held longer than this is considered stale
LOCK_STALE_MINUTES = 30
# If no task has executed successfully in this window, alert.
# Must be > the longest normal task interval (candles = hourly) plus
# execution time buffer, otherwise every normal cycle triggers a false alarm.
TASK_SILENCE_MINUTES = 90
# Minimum time between Telegram alerts (prevents spam)
ALERT_COOLDOWN_MINUTES = 60
# Critical tasks that must have recent successful executions
CRITICAL_TASKS = [
    "candles_downloader_bybit",
    "feature_computation",
    "signal_scan",
]


class WatchdogTask(BaseTask):
    """Reap stale task locks and check data pipeline health."""

    async def execute(self, context: TaskContext) -> TaskResult:
        mongo_uri = os.getenv("MONGO_URI")
        mongo_db = os.getenv("MONGO_DATABASE", "quants_lab")
        if not mongo_uri:
            return self._result(context, TaskStatus.COMPLETED, "No MONGO_URI")

        client = MongoClient(mongo_uri)
        db = client[mongo_db]
        issues = []

        # ── 1. Reap stale locks ─────────────────────────────
        reaped = self._reap_stale_locks(db)
        if reaped:
            await self._clear_in_memory_locks(reaped)
            issues.append(f"Reaped stale locks: {', '.join(reaped)}")

        # ── 2. Check task execution recency ──────────────────
        silent = self._check_task_silence(db)
        if silent:
            issues.extend(silent)

        # ── 3. Check candle data freshness ───────────────────
        candle_issues = self._check_candle_freshness()
        if candle_issues:
            issues.extend(candle_issues)

        # ── 4. Check feature freshness ───────────────────────
        feature_issues = self._check_feature_freshness(db)
        if feature_issues:
            issues.extend(feature_issues)

        # ── 5. Alert with cooldown ────────────────────────────
        # Lock reaps always alert (they're actionable and self-healing).
        # Health issues use cooldown to avoid spam.
        if reaped:
            await self._send_lock_alert(reaped)
        if issues:
            logger.warning(f"Watchdog issues: {issues}")
            await self._send_health_alert(db, issues)
        else:
            logger.info("Watchdog: all checks passed")

        client.close()

        if issues or reaped:
            msg = f"{len(issues)} health issue(s), {len(reaped)} lock(s) reaped"
        else:
            msg = "All clear"

        return self._result(
            context, TaskStatus.COMPLETED, msg,
            result_data={"issues": issues, "reaped": reaped},
        )

    # ── Lock reaping ────────────────────────────────────────

    def _reap_stale_locks(self, db) -> list:
        cutoff = datetime.utcnow() - timedelta(minutes=LOCK_STALE_MINUTES)
        stale = list(db.task_schedules.find({
            "is_running": True,
            "$or": [
                {"updated_at": {"$lt": cutoff}},
                {"updated_at": None},
                {"updated_at": {"$exists": False}},
            ],
        }))
        reaped = []
        for doc in stale:
            task_name = doc["task_name"]
            if task_name == self.config.name:
                continue
            db.task_schedules.update_one(
                {"task_name": task_name},
                {"$set": {"is_running": False, "current_execution_id": None}},
            )
            reaped.append(task_name)
            logger.warning(f"Watchdog: reaped stale lock for '{task_name}' "
                           f"(held since {doc.get('updated_at', '?')})")
        return reaped

    async def _clear_in_memory_locks(self, task_names: list):
        try:
            from core.tasks.api import orchestrator as orch
            if orch:
                for name in task_names:
                    orch.running_tasks.discard(name)
                    if name in orch.tasks:
                        orch.tasks[name]._is_running = False
                logger.info(f"Watchdog: cleared in-memory locks for {task_names}")
        except Exception as e:
            logger.debug(f"Watchdog: could not clear in-memory locks: {e}")

    # ── Task silence detection ──────────────────────────────

    def _check_task_silence(self, db) -> list:
        """Alert if critical tasks haven't completed recently."""
        issues = []
        cutoff = datetime.utcnow() - timedelta(minutes=TASK_SILENCE_MINUTES)

        for task_name in CRITICAL_TASKS:
            last = db.task_executions.find_one(
                {"task_name": task_name, "status": "completed"},
                sort=[("started_at", -1)],
            )
            if last is None:
                issues.append(f"Task '{task_name}' has NEVER completed successfully")
            elif last.get("started_at") and last["started_at"] < cutoff:
                age_min = (datetime.utcnow() - last["started_at"]).total_seconds() / 60
                issues.append(
                    f"Task '{task_name}' last succeeded {age_min:.0f}m ago "
                    f"(threshold: {TASK_SILENCE_MINUTES}m)"
                )
        return issues

    # ── Candle freshness ────────────────────────────────────

    def _check_candle_freshness(self) -> list:
        """Check that BTC 1h parquet is recent (proxy for all candle data)."""
        from core.data_paths import data_paths
        issues = []
        btc_path = data_paths.candles_dir / "bybit_perpetual|BTC-USDT|1h.parquet"
        if not btc_path.exists():
            issues.append("BTC-USDT 1h parquet missing entirely")
            return issues

        import pandas as pd
        try:
            df = pd.read_parquet(btc_path)
            if df.empty:
                issues.append("BTC-USDT 1h parquet is empty")
                return issues
            if "timestamp" in df.columns:
                last_ts = pd.Timestamp(df["timestamp"].iloc[-1], unit="s", tz="UTC")
                age = datetime.now(timezone.utc) - last_ts.to_pydatetime()
                if age > timedelta(hours=3):
                    issues.append(
                        f"BTC candle data is {age.total_seconds()/3600:.1f}h old"
                    )
        except Exception as e:
            issues.append(f"Cannot read BTC parquet: {e}")
        return issues

    # ── Feature freshness ───────────────────────────────────

    def _check_feature_freshness(self, db) -> list:
        """Check that BTC features have been computed recently."""
        issues = []
        for fname in ["atr", "range", "volume"]:
            doc = db.features.find_one(
                {"feature_name": fname, "trading_pair": "BTC-USDT"},
                sort=[("timestamp", -1)],
            )
            if doc is None:
                issues.append(f"BTC-USDT feature '{fname}' missing")
                continue
            computed = doc.get("computed_at")
            if computed:
                if computed.tzinfo is None:
                    computed = computed.replace(tzinfo=timezone.utc)
                age = datetime.now(timezone.utc) - computed
                if age > timedelta(hours=3):
                    issues.append(
                        f"BTC feature '{fname}' is {age.total_seconds()/3600:.1f}h old"
                    )
        return issues

    # ── Alerting with cooldown ───────────────────────────────

    def _should_alert(self, db, alert_key: str) -> bool:
        """Check if enough time has passed since the last alert of this type."""
        cooldown_cutoff = datetime.utcnow() - timedelta(minutes=ALERT_COOLDOWN_MINUTES)
        doc = db.watchdog_alerts.find_one({"key": alert_key})
        if doc and doc.get("last_sent_at", datetime.min) > cooldown_cutoff:
            return False
        # Update the timestamp
        db.watchdog_alerts.update_one(
            {"key": alert_key},
            {"$set": {"last_sent_at": datetime.utcnow()}},
            upsert=True,
        )
        return True

    async def _send_lock_alert(self, reaped: list):
        """Lock reaps always alert immediately (they're self-healing actions)."""
        try:
            if hasattr(self, "notification_manager") and self.notification_manager:
                from core.notifiers.base import NotificationMessage
                await self.notification_manager.send_notification(NotificationMessage(
                    title="Watchdog: Locks Reaped",
                    message=(
                        f"<b>Watchdog — Stale Locks Reaped</b>\n\n"
                        f"Cleared: {', '.join(reaped)}\n"
                        f"These tasks will resume on next schedule."
                    ),
                    level="warning",
                ))
        except Exception as e:
            logger.debug(f"Watchdog: failed to send lock alert: {e}")

    async def _send_health_alert(self, db, issues: list):
        """Health issues use cooldown — max 1 Telegram per hour."""
        if not self._should_alert(db, "health_issues"):
            logger.info(f"Watchdog: {len(issues)} issue(s) suppressed (cooldown)")
            return
        try:
            if hasattr(self, "notification_manager") and self.notification_manager:
                from core.notifiers.base import NotificationMessage
                body = "\n".join(f"• {i}" for i in issues)
                await self.notification_manager.send_notification(NotificationMessage(
                    title="Watchdog Health Alert",
                    message=(
                        f"<b>Watchdog — {len(issues)} issue(s)</b>\n\n"
                        f"{body}\n\n"
                        f"Next alert in {ALERT_COOLDOWN_MINUTES}m if unresolved.\n"
                        f"Check: <code>bash scripts/status.sh</code>"
                    ),
                    level="error" if any("never" in i.lower() or "missing" in i.lower()
                                         for i in issues) else "warning",
                ))
        except Exception as e:
            logger.debug(f"Watchdog: failed to send health alert: {e}")

    # ── Helpers ──────────────────────────────────────────────

    def _result(self, context, status, message, result_data=None):
        return TaskResult(
            execution_id=context.execution_id,
            task_name=self.config.name,
            status=status,
            started_at=context.started_at,
            completed_at=datetime.now(timezone.utc),
            result_data=result_data or {},
            error_message=message if status == TaskStatus.FAILED else None,
            metrics={"message": message},
        )
