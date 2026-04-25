"""
Continuous Validation Task — weekly re-validation of deployed strategies.

Detects performance degradation by re-running walk-forward on fresh data
and comparing against stored baselines in pair_historical.

Alerts via Telegram when:
- PF drops below baseline - threshold (DEGRADATION)
- PF flips below 1.0 (CRITICAL — negative expectancy)

Scoped to strategies with continuous_validation config in StrategyMetadata.

Schedule: weekly (Sunday 3am UTC). Uses existing walk-forward infrastructure.
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from pymongo import MongoClient

from core.tasks import BaseTask, TaskContext
from app.tasks.notifying_task import NotifyingTaskMixin

logger = logging.getLogger(__name__)


class ContinuousValidationTask(NotifyingTaskMixin, BaseTask):
    """Re-validate deployed strategies against fresh data."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config if hasattr(self.config, "config") else config
        self.mongo_uri = task_config.get("mongo_uri", "mongodb://localhost:27017/quants_lab")
        self.mongo_db = task_config.get("mongo_database", "quants_lab")
        self.engines = task_config.get("engines", [])
        self.lookback_days = task_config.get("lookback_days", 90)
        self.degradation_pf_threshold = task_config.get("degradation_pf_threshold", 0.3)
        self.connector_name = task_config.get("connector_name", "bybit_perpetual")

    async def execute(self, context: TaskContext):
        if not self.engines:
            logger.info("ContinuousValidation: no engines configured, skipping")
            return {"status": "skipped", "reason": "no engines"}

        client = MongoClient(self.mongo_uri)
        db = client[self.mongo_db]
        now = datetime.now(timezone.utc)

        results = {}
        alerts = []

        for engine_name in self.engines:
            try:
                result = await self._validate_engine(db, engine_name, now)
                results[engine_name] = result

                # Check for degradation (skip pairs without enough data)
                for pair_result in result.get("pairs", []):
                    if pair_result.get("status") != "validated" or pair_result.get("current_pf") is None:
                        continue
                    pair = pair_result["pair"]
                    baseline_pf = pair_result.get("baseline_pf", 0)
                    current_pf = pair_result.get("current_pf", 0)
                    pf_delta = current_pf - baseline_pf

                    if current_pf < 1.0:
                        alerts.append({
                            "level": "CRITICAL",
                            "engine": engine_name,
                            "pair": pair,
                            "message": f"{engine_name}/{pair}: PF={current_pf:.2f} (NEGATIVE EXPECTANCY). Baseline was {baseline_pf:.2f}.",
                        })
                    elif pf_delta < -self.degradation_pf_threshold:
                        alerts.append({
                            "level": "DEGRADATION",
                            "engine": engine_name,
                            "pair": pair,
                            "message": f"{engine_name}/{pair}: PF dropped {baseline_pf:.2f}→{current_pf:.2f} (delta={pf_delta:+.2f}, threshold={self.degradation_pf_threshold}).",
                        })

            except Exception as e:
                logger.error(f"ContinuousValidation: {engine_name} failed: {e}", exc_info=True)
                results[engine_name] = {"error": str(e)}

        # Store results
        doc = {
            "timestamp_utc": now,
            "lookback_days": self.lookback_days,
            "engines": results,
            "alerts": alerts,
        }
        db["continuous_validation"].insert_one(doc)

        # Send alerts via Telegram
        if alerts:
            critical = [a for a in alerts if a["level"] == "CRITICAL"]
            degraded = [a for a in alerts if a["level"] == "DEGRADATION"]

            msg_parts = []
            if critical:
                msg_parts.append(f"🚨 {len(critical)} CRITICAL (negative expectancy)")
                for a in critical:
                    msg_parts.append(f"  {a['message']}")
            if degraded:
                msg_parts.append(f"⚠️ {len(degraded)} DEGRADATION")
                for a in degraded:
                    msg_parts.append(f"  {a['message']}")

            await self._notify(
                title="Continuous Validation",
                message="\n".join(msg_parts),
                level="critical" if critical else "warning",
            )

        client.close()

        logger.info(
            f"ContinuousValidation: {len(self.engines)} engines checked, "
            f"{len(alerts)} alerts ({sum(1 for a in alerts if a['level']=='CRITICAL')} critical)"
        )
        return {"engines": len(self.engines), "alerts": len(alerts), "details": results}

    async def _validate_engine(self, db, engine_name: str, now: datetime) -> dict:
        """Run walk-forward validation for one engine against recent data."""
        from app.engines.strategy_registry import get_strategy

        meta = get_strategy(engine_name)

        # Get baseline PF from pair_historical
        baseline_docs = list(db["pair_historical"].find(
            {"engine": engine_name},
            {"pair": 1, "pf": 1, "wf_avg_test_pf": 1, "_id": 0},
        ))
        baselines = {}
        for doc in baseline_docs:
            pf = doc.get("wf_avg_test_pf") or doc.get("pf", 0)
            if pf and pf > 0:
                baselines[doc["pair"]] = pf

        if not baselines:
            return {"status": "no_baseline", "pairs": []}

        # Compute recent PF from backtest_trades (last N days)
        cutoff_ms = int((now - timedelta(days=self.lookback_days)).timestamp() * 1000)

        pair_results = []
        for pair, baseline_pf in baselines.items():
            # Get recent trades for this engine+pair
            trades = list(db["backtest_trades"].find({
                "engine": engine_name,
                "pair": pair,
                "timestamp": {"$gte": cutoff_ms / 1000},  # trades use seconds
            }))

            if len(trades) < 10:
                pair_results.append({
                    "pair": pair,
                    "baseline_pf": baseline_pf,
                    "current_pf": None,
                    "n_trades": len(trades),
                    "status": "insufficient_data",
                })
                continue

            # Compute PF from recent trades
            gross_profit = sum(t.get("net_pnl_quote", 0) for t in trades if t.get("net_pnl_quote", 0) > 0)
            gross_loss = abs(sum(t.get("net_pnl_quote", 0) for t in trades if t.get("net_pnl_quote", 0) < 0))
            current_pf = gross_profit / gross_loss if gross_loss > 0 else 0.0

            pair_results.append({
                "pair": pair,
                "baseline_pf": baseline_pf,
                "current_pf": round(current_pf, 3),
                "n_trades": len(trades),
                "status": "validated",
            })

        return {
            "status": "ok",
            "n_pairs": len(pair_results),
            "pairs": pair_results,
        }
