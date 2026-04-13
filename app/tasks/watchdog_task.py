"""
Watchdog task — health checks + self-healing:
1. Reap stale task locks + auto-restart via QL API.
2. Check task execution recency.
3. Check candle data freshness + auto-trigger downloader.
4. Check feature freshness + auto-trigger computation.
5. Check HB API health + auto-restart Docker container.
6. Detect stuck executors (>2x time_limit) + force-stop.
7. Detect consecutive task failure streaks + alert.

Runs every 5 minutes.  Sends Telegram alerts with cooldown (max 1 per hour
for the same issue category).  Self-healing actions are logged to the
`watchdog_state` MongoDB collection for the /heal Telegram command.
"""
import asyncio
import logging
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiohttp
from pymongo import MongoClient

from core.tasks import BaseTask, TaskContext, TaskResult, TaskStatus

logger = logging.getLogger(__name__)

# A task lock held longer than this is considered stale
LOCK_STALE_MINUTES = 30
# If no task has executed successfully in this window, alert.
TASK_SILENCE_MINUTES = 90
# Minimum time between Telegram alerts (prevents spam)
ALERT_COOLDOWN_MINUTES = 60
# Critical tasks that must have recent successful executions
# E1 runs as HB-native bot — signal_scan only needed for E2 (legacy)
CRITICAL_TASKS = [
    "candles_downloader_bybit",
    "feature_computation",
]

# Self-healing limits
HB_MAX_RESTARTS_PER_6H = 2
HB_CONSECUTIVE_FAILURES_THRESHOLD = 3
DATA_RETRIGGER_COOLDOWN_S = 7200  # 2 hours
STUCK_EXECUTOR_MULTIPLIER = 2  # kill after 2x time_limit
FAILURE_STREAK_THRESHOLD = 5

# QL API for task triggering
QL_API_BASE = "http://localhost:8001"


class WatchdogTask(BaseTask):
    """Reap stale task locks, check pipeline health, and auto-heal."""

    async def execute(self, context: TaskContext) -> TaskResult:
        mongo_uri = os.getenv("MONGO_URI")
        mongo_db = os.getenv("MONGO_DATABASE", "quants_lab")
        if not mongo_uri:
            return self._result(context, TaskStatus.COMPLETED, "No MONGO_URI")

        client = MongoClient(mongo_uri)
        db = client[mongo_db]
        issues = []
        healed = []

        # ── 1. Reap stale locks + auto-restart ────────────────
        reaped = self._reap_stale_locks(db)
        if reaped:
            await self._clear_in_memory_locks(reaped)
            issues.append(f"Reaped stale locks: {', '.join(reaped)}")
            restarted = await self._heal_stale_locks(reaped)
            if restarted:
                healed.append(f"Re-triggered: {', '.join(restarted)}")

        # ── 2. Check task execution recency ───────────────────
        silent = self._check_task_silence(db)
        if silent:
            issues.extend(silent)

        # ── 3. Check candle data freshness ────────────────────
        candle_issues = self._check_candle_freshness()
        if candle_issues:
            issues.extend(candle_issues)

        # ── 4. Check feature freshness ────────────────────────
        feature_issues = self._check_feature_freshness(db)
        if feature_issues:
            issues.extend(feature_issues)

        # ── 3+4 heal: auto-trigger stale data tasks ──────────
        all_data_issues = (candle_issues or []) + (feature_issues or [])
        if all_data_issues:
            data_healed = await self._heal_stale_data(db, all_data_issues)
            if data_healed:
                healed.extend(data_healed)

        # ── 5. Check API health + auto-heal HB ───────────────
        api_issues = await self._check_api_health()
        hb_down = any("HB API" in i for i in api_issues) if api_issues else False
        if api_issues:
            issues.extend(api_issues)
        if hb_down:
            heal_result = await self._heal_hb_api(db)
            if heal_result != "monitoring":
                if "ok" in heal_result:
                    healed.append(f"HB API: {heal_result}")
                else:
                    issues.append(f"HB API heal: {heal_result}")
        else:
            # Reset failure counter on success
            self._reset_hb_state(db)

        # ── 6. Stuck executors ────────────────────────────────
        stuck = await self._check_stuck_executors(db)
        if stuck:
            healed.extend([f"Stopped stuck: {s}" for s in stuck])

        # ── 6b. Exchange reconciliation (3-way) ────────────────
        try:
            from app.tasks.resolution.reconciliation import reconcile_positions
            recon = await reconcile_positions(db)
            if recon.get("ghost_recovered", 0) > 0:
                healed.append(
                    f"Recovered {recon['ghost_recovered']} ghost position(s) from executor history"
                )
            if recon.get("ghost_unresolved", 0) > 0:
                issues.append(
                    f"Reconciliation: {recon['ghost_unresolved']} ghost position(s) "
                    f"could not be recovered — manual check needed"
                )
            if recon.get("exchange_untracked", 0) > 0:
                issues.append(
                    f"Reconciliation: {recon['exchange_untracked']} untracked exchange "
                    f"position(s) — check Bybit UI"
                )
            if recon.get("orphan_executors", 0) > 0:
                issues.append(f"Reconciliation: {recon['orphan_executors']} orphan executor(s)")
            if recon.get("size_mismatches", 0) > 0:
                issues.append(
                    f"Reconciliation: {recon['size_mismatches']} STACKED POSITION(s) — "
                    f"exchange size >> bot volume. Ghost bot likely doubled exposure."
                )
        except Exception as e:
            logger.warning(f"Reconciliation check failed: {e}")

        # ── 7. Failure streaks ────────────────────────────────
        failure_alerts = self._check_failure_streaks(db)
        if failure_alerts:
            issues.extend(failure_alerts)

        # ── Log healing actions to watchdog_state ─────────────
        if healed:
            self._log_heal_actions(db, healed)

        # ── Alert ─────────────────────────────────────────────
        if reaped:
            await self._send_lock_alert(reaped, healed)
        if healed:
            await self._send_heal_alert(healed)
        if issues:
            logger.warning(f"Watchdog issues: {issues}")
            await self._send_health_alert(db, issues)
        else:
            logger.info("Watchdog: all checks passed")
            self._clear_issue_state(db)  # reset escalation timer

        client.close()

        n_issues = len(issues)
        n_reaped = len(reaped) if reaped else 0
        n_healed = len(healed)
        if issues or reaped or healed:
            msg = f"{n_issues} issue(s), {n_reaped} lock(s) reaped, {n_healed} healed"
        else:
            msg = "All clear"

        return self._result(
            context, TaskStatus.COMPLETED, msg,
            result_data={"issues": issues, "reaped": reaped, "healed": healed},
        )

    # ══════════════════════════════════════════════════════════
    # SELF-HEALING METHODS
    # ══════════════════════════════════════════════════════════

    # ── Heal 1: Re-trigger tasks after lock reap ─────────────

    async def _heal_stale_locks(self, reaped_tasks: list) -> list:
        """After reaping locks, trigger tasks to re-run via QL API."""
        restarted = []
        for task_name in reaped_tasks:
            ok = await self._trigger_task_via_api(task_name)
            if ok:
                restarted.append(task_name)
                logger.info(f"Watchdog: auto-restarted '{task_name}' after lock reap")
        return restarted

    # ── Heal 2: HB API auto-restart ──────────────────────────

    async def _heal_hb_api(self, db) -> str:
        """Attempt HB API recovery via docker restart + re-patch."""
        now = datetime.utcnow()
        state = db.watchdog_state.find_one({"_id": "hb_api"}) or {}
        fail_streak = state.get("consecutive_failures", 0)

        # Track consecutive failures
        fail_streak += 1
        db.watchdog_state.update_one(
            {"_id": "hb_api"},
            {"$set": {"consecutive_failures": fail_streak, "updated_at": now}},
            upsert=True,
        )

        if fail_streak < HB_CONSECUTIVE_FAILURES_THRESHOLD:
            logger.info(f"Watchdog: HB API fail streak {fail_streak}/{HB_CONSECUTIVE_FAILURES_THRESHOLD}")
            return "monitoring"

        # Check restart budget (max 2 per 6 hours)
        six_hours_ago = now - timedelta(hours=6)
        recent_restarts = db.watchdog_state.count_documents({
            "_id": {"$regex": "^hb_restart_"},
            "timestamp": {"$gte": six_hours_ago},
        })
        if recent_restarts >= HB_MAX_RESTARTS_PER_6H:
            logger.warning("Watchdog: HB API max restarts reached (2/6h)")
            return "max_restarts_reached — manual intervention needed"

        # Attempt docker restart
        try:
            logger.info("Watchdog: attempting docker restart hummingbot-api")
            result = subprocess.run(
                ["docker", "restart", "hummingbot-api"],
                capture_output=True, timeout=30,
            )
            if result.returncode != 0:
                stderr = result.stderr.decode()[:200]
                logger.warning(f"Watchdog: docker restart failed: {stderr}")
                if "permission denied" in stderr.lower() or "connect" in stderr.lower():
                    return "docker_not_accessible — alerting only"
                return f"restart_failed: {stderr[:100]}"
        except subprocess.TimeoutExpired:
            return "restart_timed_out"
        except (PermissionError, FileNotFoundError) as e:
            return f"docker_not_accessible: {e}"

        # Wait for container boot
        await asyncio.sleep(15)

        # Re-apply Bybit demo patches (CRITICAL)
        patch_script = Path(__file__).resolve().parent.parent.parent / "scripts" / "patch_hb_docker.sh"
        if patch_script.exists():
            try:
                patch_result = subprocess.run(
                    ["bash", str(patch_script)],
                    capture_output=True, timeout=60,
                )
                if patch_result.returncode != 0:
                    stderr = patch_result.stderr.decode()[:200]
                    logger.warning(f"Watchdog: patch script failed: {stderr}")
                    return f"restarted_but_patch_failed: {stderr[:100]}"
            except subprocess.TimeoutExpired:
                return "restarted_but_patch_timed_out"
        else:
            logger.warning("Watchdog: patch_hb_docker.sh not found, skipping patches")

        # Wait for patches to settle
        await asyncio.sleep(10)

        # Verify health
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                auth = aiohttp.BasicAuth(
                    os.getenv("HUMMINGBOT_API_USERNAME", "admin"),
                    os.getenv("HUMMINGBOT_API_PASSWORD", "admin"),
                )
                async with session.get("http://localhost:8000/", auth=auth) as resp:
                    healthy = resp.status < 500
        except Exception:
            healthy = False

        # Record the restart
        restart_id = f"hb_restart_{int(now.timestamp())}"
        db.watchdog_state.update_one(
            {"_id": restart_id},
            {"$set": {"timestamp": now, "healthy_after": healthy}},
            upsert=True,
        )

        if healthy:
            # Reset failure counter
            self._reset_hb_state(db)
            logger.info("Watchdog: HB API restarted + patched successfully")
            return "restarted_and_patched_ok"

        return "restarted_but_still_unhealthy"

    def _reset_hb_state(self, db):
        """Reset HB API failure counter on successful health check."""
        db.watchdog_state.update_one(
            {"_id": "hb_api"},
            {"$set": {"consecutive_failures": 0, "updated_at": datetime.utcnow()}},
            upsert=True,
        )

    # ── Heal 3: Auto-trigger stale data tasks ────────────────

    async def _heal_stale_data(self, db, issues: list) -> list:
        """Auto-trigger candle/feature tasks when data is stale."""
        healed = []
        now = datetime.utcnow()

        has_candle_issue = any("candle" in i.lower() or "parquet" in i.lower() for i in issues)
        has_feature_issue = any("feature" in i.lower() for i in issues)

        if has_candle_issue:
            state = db.watchdog_state.find_one({"_id": "retrigger_candles"})
            last = state.get("last_triggered") if state else None
            if not last or (now - last).total_seconds() >= DATA_RETRIGGER_COOLDOWN_S:
                ok = await self._trigger_task_via_api("candles_downloader_bybit")
                if ok:
                    db.watchdog_state.update_one(
                        {"_id": "retrigger_candles"},
                        {"$set": {"last_triggered": now}},
                        upsert=True,
                    )
                    healed.append("Triggered candles_downloader_bybit")
                    logger.info("Watchdog: auto-triggered candles_downloader_bybit")

        if has_feature_issue and not has_candle_issue:
            # Only trigger features if candles are fresh (features depend on candles)
            state = db.watchdog_state.find_one({"_id": "retrigger_features"})
            last = state.get("last_triggered") if state else None
            if not last or (now - last).total_seconds() >= DATA_RETRIGGER_COOLDOWN_S:
                ok = await self._trigger_task_via_api("feature_computation")
                if ok:
                    db.watchdog_state.update_one(
                        {"_id": "retrigger_features"},
                        {"$set": {"last_triggered": now}},
                        upsert=True,
                    )
                    healed.append("Triggered feature_computation")
                    logger.info("Watchdog: auto-triggered feature_computation")

        return healed

    # ── Heal 4: Stuck executor detection + kill ──────────────

    async def _check_stuck_executors(self, db) -> list:
        """Find and kill executors that exceeded 2x their time limit."""
        stuck = []
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        # Orphan cleanup: TESTNET_ACTIVE/PLACING with null/missing executor_id
        orphans = list(db.candidates.find({
            "disposition": {"$in": ["TESTNET_ACTIVE", "PLACING"]},
            "$or": [{"executor_id": None}, {"executor_id": {"$exists": False}}],
        }))
        for doc in orphans:
            cid = doc.get("candidate_id", "?")
            pair = doc.get("pair", "?")
            engine = doc.get("engine", "?")
            logger.warning(f"Watchdog: orphan candidate {cid} ({engine}/{pair}) — "
                           "TESTNET_ACTIVE with no executor_id, resolving")
            db.candidates.update_one(
                {"candidate_id": cid},
                {"$set": {
                    "disposition": "RESOLVED_TESTNET",
                    "testnet_close_type": "ORPHAN_NULL_EXECUTOR",
                    "testnet_resolved_at": now_ms,
                }},
            )
            stuck.append(f"{engine}/{pair} (orphan)")

        active = list(db.candidates.find({"disposition": "TESTNET_ACTIVE"}))

        for doc in active:
            placed_at = doc.get("testnet_placed_at", 0)
            if not placed_at:
                continue

            time_limit = doc.get("testnet_time_limit") or 86400
            max_age_ms = int(time_limit) * STUCK_EXECUTOR_MULTIPLIER * 1000

            if (now_ms - int(placed_at)) <= max_age_ms:
                continue

            executor_id = doc.get("executor_id")
            pair = doc.get("pair", "?")
            engine = doc.get("engine", "?")
            age_h = (now_ms - int(placed_at)) / 3600000

            stop_succeeded = False
            if executor_id:
                try:
                    from app.services.hb_api_client import HBApiClient
                    hb = HBApiClient()
                    await hb.stop_executor(executor_id)
                    await hb.close()
                    stop_succeeded = True
                    logger.info(f"Watchdog: stopped stuck executor {executor_id} "
                                f"({engine}/{pair}, {age_h:.1f}h old)")
                except Exception as e:
                    logger.warning(f"Watchdog: failed to stop executor {executor_id}: {e}")

            if stop_succeeded or not executor_id:
                db.candidates.update_one(
                    {"candidate_id": doc["candidate_id"]},
                    {"$set": {
                        "disposition": "RESOLVED_TESTNET",
                        "testnet_close_type": "WATCHDOG_STOP",
                        "testnet_resolved_at": now_ms,
                    }},
                )
                stuck.append(f"{engine}/{pair} ({age_h:.1f}h)")
            else:
                db.candidates.update_one(
                    {"candidate_id": doc["candidate_id"]},
                    {"$set": {
                        "watchdog_stop_failed": True,
                        "watchdog_stop_error": "executor stop failed",
                    }},
                )
                logger.warning(f"Watchdog: {engine}/{pair} stop failed — keeping TESTNET_ACTIVE")

        return stuck

    # ── Heal 5: Consecutive failure streak detection ─────────

    def _check_failure_streaks(self, db) -> list:
        """Detect tasks failing 5+ times consecutively."""
        alerts = []
        check_tasks = CRITICAL_TASKS + ["testnet_resolver"]

        for task_name in check_tasks:
            recent = list(db.task_executions.find(
                {"task_name": task_name}
            ).sort("started_at", -1).limit(FAILURE_STREAK_THRESHOLD))

            if (len(recent) >= FAILURE_STREAK_THRESHOLD and
                    all(r.get("status") == "failed" for r in recent)):
                last_error = recent[0].get("error_message", "unknown")[:100]
                alerts.append(
                    f"STREAK: '{task_name}' failed {FAILURE_STREAK_THRESHOLD}x — {last_error}"
                )
                logger.error(f"Watchdog: {task_name} has {FAILURE_STREAK_THRESHOLD} consecutive failures")

        return alerts

    # ══════════════════════════════════════════════════════════
    # EXISTING DETECTION METHODS (unchanged)
    # ══════════════════════════════════════════════════════════

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

    # ── API health checks ──────────────────────────────────

    async def _check_api_health(self) -> list:
        """Check that HB API (:8000) and QL API (:8001) are reachable."""
        issues = []
        checks = [
            ("HB API", "http://localhost:8000/", os.getenv("HUMMINGBOT_API_USERNAME", "admin"),
             os.getenv("HUMMINGBOT_API_PASSWORD", "admin")),
            ("QL API", "http://localhost:8001/health", None, None),
        ]
        timeout = aiohttp.ClientTimeout(total=5)
        for label, url, user, pwd in checks:
            try:
                auth = aiohttp.BasicAuth(user, pwd) if user else None
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url, auth=auth) as resp:
                        if resp.status >= 500:
                            issues.append(f"{label} returned HTTP {resp.status}")
            except aiohttp.ClientError as e:
                issues.append(f"{label} unreachable: {type(e).__name__}")
            except Exception as e:
                issues.append(f"{label} check failed: {e}")
        return issues

    # ══════════════════════════════════════════════════════════
    # ALERTING
    # ══════════════════════════════════════════════════════════

    def _should_alert(self, db, alert_key: str) -> bool:
        """Check if enough time has passed since the last alert of this type."""
        cooldown_cutoff = datetime.utcnow() - timedelta(minutes=ALERT_COOLDOWN_MINUTES)
        doc = db.watchdog_alerts.find_one({"key": alert_key})
        if doc and doc.get("last_sent_at", datetime.min) > cooldown_cutoff:
            return False
        db.watchdog_alerts.update_one(
            {"key": alert_key},
            {"$set": {"last_sent_at": datetime.utcnow()}},
            upsert=True,
        )
        return True

    def _get_issue_level(self, db, issues: list) -> int:
        """
        Return escalation level based on how long issues have been unresolved.

        L1 (< 30 min): first detection
        L2 (30-120 min): unresolved warning
        L3 (>= 120 min): critical with diagnostics

        Tracks first_seen_at per issue fingerprint in watchdog_state.
        Clears resolved issues from state.
        """
        now = datetime.utcnow()
        # Use a stable fingerprint for the current issue set
        issue_key = "active_issues"
        doc = db.watchdog_state.find_one({"_id": issue_key}) or {}
        first_seen = doc.get("first_seen_at")

        if first_seen is None:
            # First detection
            db.watchdog_state.update_one(
                {"_id": issue_key},
                {"$set": {"first_seen_at": now, "issues": issues}},
                upsert=True,
            )
            return 1

        age_min = (now - first_seen).total_seconds() / 60
        if age_min >= 120:
            return 3
        if age_min >= 30:
            return 2
        return 1

    def _clear_issue_state(self, db):
        """Call when watchdog runs clean (no issues) to reset escalation timer."""
        db.watchdog_state.delete_one({"_id": "active_issues"})

    async def _send_lock_alert(self, reaped: list, healed: list = None):
        """Lock reaps always alert immediately (they're self-healing actions)."""
        try:
            if hasattr(self, "notification_manager") and self.notification_manager:
                from core.notifiers.base import NotificationMessage
                heal_line = ""
                if healed:
                    heal_line = f"\nAuto-healed: {', '.join(healed)}"
                await self.notification_manager.send_notification(NotificationMessage(
                    title="Watchdog: Locks Reaped",
                    message=(
                        f"<b>Watchdog — Stale Locks Reaped</b>\n\n"
                        f"Cleared: {', '.join(reaped)}{heal_line}\n"
                        f"Tasks will resume on next schedule."
                    ),
                    level="warning",
                ))
        except Exception as e:
            logger.debug(f"Watchdog: failed to send lock alert: {e}")

    async def _send_heal_alert(self, healed: list):
        """Notify about successful self-healing actions."""
        try:
            if hasattr(self, "notification_manager") and self.notification_manager:
                from core.notifiers.base import NotificationMessage
                body = "\n".join(f"\u2705 {h}" for h in healed)
                await self.notification_manager.send_notification(NotificationMessage(
                    title="Watchdog: Self-Healed",
                    message=(
                        f"<b>Watchdog — Self-Healing</b>\n\n"
                        f"{body}"
                    ),
                    level="info",
                ))
        except Exception as e:
            logger.debug(f"Watchdog: failed to send heal alert: {e}")

    async def _send_health_alert(self, db, issues: list):
        """
        Tiered health alerts.

        L1 (first detection): normal — Watchdog Health Alert
        L2 (>30 min unresolved): ⚠️ UNRESOLVED — with elapsed time
        L3 (>2h unresolved): 🚨 CRITICAL — diagnostic dump
        """
        level = self._get_issue_level(db, issues)

        if not self._should_alert(db, "health_issues"):
            logger.info(f"Watchdog: {len(issues)} issue(s) suppressed (cooldown)")
            return

        try:
            if not (hasattr(self, "notification_manager") and self.notification_manager):
                return

            from core.notifiers.base import NotificationMessage
            body = "\n".join(f"\u2022 {i}" for i in issues)

            if level == 1:
                title = "Watchdog Health Alert"
                header = f"<b>Watchdog — {len(issues)} issue(s)</b>"
                suffix = (
                    f"\n\nNext alert in {ALERT_COOLDOWN_MINUTES}m if unresolved.\n"
                    f"Check: <code>bash scripts/status.sh</code>"
                )
                alert_level = "warning"

            elif level == 2:
                doc = db.watchdog_state.find_one({"_id": "active_issues"}) or {}
                first_seen = doc.get("first_seen_at")
                elapsed = ""
                if first_seen:
                    age_min = (datetime.utcnow() - first_seen).total_seconds() / 60
                    elapsed = f" (unresolved {age_min:.0f}m)"
                title = f"UNRESOLVED: Watchdog{elapsed}"
                header = f"<b>\u26a0\ufe0f UNRESOLVED{elapsed} — {len(issues)} issue(s)</b>"
                suffix = "\n\nCheck: <code>bash scripts/status.sh</code>"
                alert_level = "error"

            else:  # level == 3
                doc = db.watchdog_state.find_one({"_id": "active_issues"}) or {}
                first_seen = doc.get("first_seen_at")
                elapsed = ""
                if first_seen:
                    age_min = (datetime.utcnow() - first_seen).total_seconds() / 60
                    elapsed = f" ({age_min:.0f}m)"
                title = f"CRITICAL: Watchdog{elapsed}"
                header = f"<b>\U0001f6a8 CRITICAL — Unresolved {len(issues)} issue(s){elapsed}</b>"
                # Add diagnostic dump
                try:
                    import subprocess
                    proc = subprocess.run(
                        ["bash", "scripts/status.sh"],
                        capture_output=True, text=True, timeout=10,
                        cwd="/Users/hermes/quants-lab",
                    )
                    diag = proc.stdout[:500] if proc.stdout else "status.sh failed"
                except Exception as ex:
                    diag = f"Diagnostic unavailable: {ex}"
                suffix = f"\n\n<code>{diag}</code>\n\nManual check required."
                alert_level = "error"

            await self.notification_manager.send_notification(NotificationMessage(
                title=title,
                message=f"{header}\n\n{body}{suffix}",
                level=alert_level,
            ))

        except Exception as e:
            logger.debug(f"Watchdog: failed to send health alert: {e}")

    # ══════════════════════════════════════════════════════════
    # HELPERS
    # ══════════════════════════════════════════════════════════

    async def _trigger_task_via_api(self, task_name: str) -> bool:
        """Trigger a task via QL API POST /tasks/{name}/trigger."""
        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    f"{QL_API_BASE}/tasks/{task_name}/trigger",
                    json={"task_name": task_name, "triggered_by": "watchdog"},
                ) as resp:
                    if resp.status in (200, 201, 202):
                        return True
                    logger.warning(f"Watchdog: trigger {task_name} returned {resp.status}")
                    return False
        except Exception as e:
            logger.warning(f"Watchdog: failed to trigger {task_name}: {e}")
            return False

    def _log_heal_actions(self, db, healed: list):
        """Write healing actions to watchdog_state for /heal command."""
        now = datetime.utcnow()
        db.watchdog_state.update_one(
            {"_id": "heal_log"},
            {
                "$push": {
                    "actions": {
                        "$each": [{"action": h, "timestamp": now} for h in healed],
                        "$slice": -50,  # keep last 50 actions
                    }
                },
                "$set": {"updated_at": now},
            },
            upsert=True,
        )

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
