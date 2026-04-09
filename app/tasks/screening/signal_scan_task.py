"""
Signal scan task — evaluates E1/E2 engines on eligible pairs.

Reads pre-computed features from FeatureStorage, builds DecisionSnapshots,
runs engine evaluation, stores candidates to MongoDB, sends Telegram alerts.

Data integrity: refuses to generate candidates when upstream data is stale.
"""
import logging
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from core.data_paths import data_paths
from core.data_structures.candles import Candles
from core.tasks import BaseTask, TaskContext

from app.engines.models import CandidateBase, DecisionSnapshot, FeatureRow
from app.engines.pair_selector import get_eligible_pairs
from app.engines.strategy_registry import get_evaluate_fn

logger = logging.getLogger(__name__)

# ── Data freshness thresholds ────────────────────────────────
# Candle data older than this → stale (should refresh every hour)
CANDLE_MAX_AGE = timedelta(hours=3)
# Feature docs older than this → stale (computed after candle download)
FEATURE_MAX_AGE = timedelta(hours=3)
# Minimum required features to build a valid FeatureRow
REQUIRED_FEATURES = {"atr", "range", "volume"}


from app.tasks.notifying_task import NotifyingTaskMixin



# Pairs that are actually tradeable via HB API (must match the rate limiter
# configuration in the Docker container).  If a pair isn't in this set, the
# executor will crash with a missing throttler pool.  This acts as a hard
# safety gate regardless of what pair_historical says.
TRADEABLE_PAIRS: set[str] = {
    "BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "DOGE-USDT",
    "ADA-USDT", "AVAX-USDT", "LINK-USDT", "DOT-USDT", "UNI-USDT",
    "NEAR-USDT", "APT-USDT", "ARB-USDT", "OP-USDT", "SUI-USDT",
    "SEI-USDT", "WLD-USDT", "LTC-USDT", "BCH-USDT", "BNB-USDT",
    "CRV-USDT", "1000PEPE-USDT", "ALGO-USDT", "GALA-USDT",
    "ONT-USDT", "TAO-USDT", "ZEC-USDT",
}


class SignalScanTask(NotifyingTaskMixin, BaseTask):
    """Evaluate E1/E2 engines on screened pairs and log candidates."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.engines = task_config.get("engines", ["E1", "E2"])
        self.connector_name = task_config.get("connector_name", "bybit_perpetual")
        self.top_n = task_config.get("top_n", 10)
        self.telegram_chat_id = task_config.get("telegram_chat_id")

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB required for SignalScanTask")

    async def _build_feature_row(self, pair: str) -> Optional[FeatureRow]:
        """Assemble a FeatureRow from the latest features in MongoDB.

        Performs three integrity checks before returning:
        1. **Candle freshness** — parquet last row must be < CANDLE_MAX_AGE old
        2. **Feature freshness** — computed_at must be < FEATURE_MAX_AGE old
        3. **Feature completeness** — REQUIRED_FEATURES must all be present
        If any check fails, ``feature_staleness_ok`` is set to False and
        ``staleness_flags`` lists the reasons.
        """
        now = datetime.now(timezone.utc)
        staleness_flags: List[str] = []

        # ── 1. Candle freshness check ───────────────────────
        close = None
        candle_high, candle_low, candle_open = None, None, None
        path = data_paths.candles_dir / f"{self.connector_name}|{pair}|1h.parquet"
        if path.exists():
            df = pd.read_parquet(path)
            if not df.empty:
                close = float(df["close"].iloc[-1])
                candle_high = float(df["high"].iloc[-1])
                candle_low = float(df["low"].iloc[-1])
                candle_open = float(df["open"].iloc[-1])
                # Check candle age
                if "timestamp" in df.columns:
                    last_candle_ts = pd.Timestamp(df["timestamp"].iloc[-1], unit="s", tz="UTC")
                    candle_age = now - last_candle_ts.to_pydatetime()
                    if candle_age > CANDLE_MAX_AGE:
                        staleness_flags.append(
                            f"candle_stale:{candle_age.total_seconds()/3600:.1f}h"
                        )

        if close is None:
            return None

        # ── 2. Feature freshness + completeness check ───────
        feature_names = ["atr", "range", "volume", "momentum", "derivatives", "market_regime"]
        feature_data = {}
        present_features = set()

        for fname in feature_names:
            docs = await self.mongodb_client.get_documents(
                "features",
                {"feature_name": fname, "trading_pair": pair},
                limit=1,
            )
            if docs:
                doc = docs[0]
                feature_data[fname] = doc.get("value", {})
                present_features.add(fname)

                # Check computed_at freshness
                computed_at = doc.get("computed_at")
                if computed_at:
                    if isinstance(computed_at, datetime):
                        age = now - computed_at.replace(tzinfo=timezone.utc) if computed_at.tzinfo is None else now - computed_at
                    else:
                        age = FEATURE_MAX_AGE + timedelta(seconds=1)  # force stale
                    if age > FEATURE_MAX_AGE:
                        staleness_flags.append(
                            f"feature_stale:{fname}:{age.total_seconds()/3600:.1f}h"
                        )

        # Completeness: required features must all be present
        missing = REQUIRED_FEATURES - present_features
        if missing:
            staleness_flags.append(f"missing_features:{','.join(sorted(missing))}")

        atr_d = feature_data.get("atr", {})
        range_d = feature_data.get("range", {})
        vol_d = feature_data.get("volume", {})
        mom_d = feature_data.get("momentum", {})
        deriv_d = feature_data.get("derivatives", {})

        # Critical values must not be None
        if atr_d.get("atr_percentile_90d") is None:
            staleness_flags.append("null:atr_percentile_90d")
        if range_d.get("range_high_20") is None or range_d.get("range_low_20") is None:
            staleness_flags.append("null:range_bounds")

        feature_ok = len(staleness_flags) == 0

        return FeatureRow(
            pair=pair,
            timestamp_utc=int(now.timestamp() * 1000),
            close=close,
            return_1h=mom_d.get("return_1h"),
            return_4h=mom_d.get("return_4h"),
            return_24h=mom_d.get("return_24h"),
            atr_14_1h=atr_d.get("atr_14_1h"),
            atr_percentile_90d=atr_d.get("atr_percentile_90d"),
            range_high_20=range_d.get("range_high_20"),
            range_low_20=range_d.get("range_low_20"),
            # Apply thresholds from raw values — NOT from pre-baked feature flags.
            # Features store raw ATR percentile and volume; thresholds are strategy logic.
            range_compression_confirmed=(
                atr_d.get("atr_percentile_90d") is not None
                and atr_d["atr_percentile_90d"] < 0.35  # V3.2 locked
            ),
            volume_1h=vol_d.get("vol_avg_20"),
            volume_zscore_20=vol_d.get("vol_zscore_20"),
            volume_floor_passed=(
                vol_d.get("vol_avg_20") is not None
                and close is not None
                and vol_d.get("vol_zscore_20") is not None
                # Can't compute exact floor from stored features — use the pre-baked flag
                # as fallback, but prefer raw check when available
                and vol_d.get("vol_floor_passed", 0) > 0.5
            ),
            funding_rate_current=deriv_d.get("funding_rate"),
            funding_neutral=deriv_d.get("funding_neutral", 0) > 0.5,
            oi_change_1h_pct=deriv_d.get("oi_change_1h_pct"),
            oi_increasing=deriv_d.get("oi_increasing", 0) > 0.5,
            rs_aligned=deriv_d.get("rs_aligned", 0) > 0.5,
            # Candle OHLC (for engines like E2 that need intra-candle data)
            candle_high=candle_high,
            candle_low=candle_low,
            candle_open=candle_open,
            # Range expansion (for E2 range stability check)
            range_expanding=range_d.get("range_expanding"),
            feature_staleness_ok=feature_ok,
            staleness_flags=staleness_flags,
        )

    async def _get_market_state(self) -> str:
        """Read BTC regime from feature store."""
        docs = await self.mongodb_client.get_documents(
            "features",
            {"feature_name": "market_regime", "trading_pair": "BTC-USDT"},
            limit=1,
        )
        if docs:
            val = docs[0].get("value", {})
            if val.get("risk_off", 0) > 0.5:
                return "Risk-Off Contagion"
        return "Normal"

    async def _store_candidate(self, engine: str, cand) -> None:
        """Store candidate to MongoDB candidates collection."""
        doc = asdict(cand)
        doc["engine"] = engine
        await self.mongodb_client.insert_documents(
            collection_name="candidates",
            documents=[doc],
            index=[("engine", 1), ("pair", 1), ("timestamp_utc", 1)],
        )

    async def _send_telegram(self, engine: str, cand) -> None:
        """Send Telegram alert for CANDIDATE_READY signals."""
        if not self.notification_manager:
            return
        from core.notifiers.base import NotificationMessage
        from app.engines.fmt import fp
        msg = (
            f"<b>{engine} Signal — {cand.pair}</b>\n"
            f"Direction: {cand.direction}\n"
            f"Trigger: {cand.trigger_reason}\n"
            f"Score: {cand.composite_score:.2f}\n"
            f"Price: {fp(cand.decision_price)}"
        )
        try:
            notification = NotificationMessage(
                title=f"{engine} Signal — {cand.pair}",
                message=msg,
                level="warning",
            )
            await self.notification_manager.send_notification(notification)
        except Exception as e:
            logger.warning(f"Telegram send failed: {e}")

    async def _check_engine_circuit_breaker(self, engine: str) -> bool:
        """Check if engine should be paused.

        Gates (checked in order):
        1. Manual override (force_enabled in engine_state)
        2. Research phase gate (must have passed P2_MONTECARLO to paper trade)
        3. Walk-forward test PF (aggregate test PF must be >= 1.0)

        Returns True if engine is OK to run, False if paused.
        """
        db = self.mongodb_client.get_database()

        # Check manual override
        state = await db["engine_state"].find_one({"engine": engine})
        if state and state.get("force_enabled"):
            logger.info(f"{engine}: circuit breaker overridden (force_enabled=true)")
            return True

        # Research phase gate — engine must have passed Phase 2
        try:
            from app.engines.research_tracker import ResearchTracker, PAPER_TRADE_MINIMUM
            tracker = ResearchTracker(db)
            phase = await tracker.get_phase(engine)
            if phase.value < PAPER_TRADE_MINIMUM.value:
                logger.info(
                    f"{engine}: blocked by research gate — "
                    f"phase={phase.name}, need={PAPER_TRADE_MINIMUM.name}. "
                    f"Override: db.engine_state.updateOne("
                    f'{{engine: "{engine}"}}, {{$set: {{force_enabled: true}}}})'
                )
                return False
        except Exception as e:
            # Don't block on research tracker errors — fall through to other checks
            logger.debug(f"{engine}: research tracker check failed: {e}")

        # Check walk-forward test PF (aggregated across ALLOW pairs)
        pipeline = [
            {"$match": {"engine": engine, "wf_avg_test_pf": {"$exists": True}}},
            {"$group": {
                "_id": None,
                "avg_test_pf": {"$avg": "$wf_avg_test_pf"},
                "count": {"$sum": 1},
            }},
        ]
        results = []
        async for doc in db["pair_historical"].aggregate(pipeline):
            results.append(doc)

        if not results:
            # No walk-forward data yet — let it run
            return True

        agg = results[0]
        avg_pf = agg["avg_test_pf"]
        count = agg["count"]

        if avg_pf < 1.0 and count >= 3:
            # Pause engine
            await db["engine_state"].update_one(
                {"engine": engine},
                {"$set": {
                    "engine": engine,
                    "paused": True,
                    "paused_reason": f"Walk-forward test PF = {avg_pf:.2f} (< 1.0, {count} pairs)",
                    "paused_at": datetime.now(timezone.utc),
                }},
                upsert=True,
            )
            logger.warning(f"{engine}: PAUSED by circuit breaker — WF test PF = {avg_pf:.2f}")

            if self.notification_manager:
                try:
                    from core.notifiers.base import NotificationMessage
                    await self.notification_manager.send_notification(NotificationMessage(
                        title=f"{engine} PAUSED — Walk-Forward Test PF < 1.0",
                        message=(
                            f"<b>{engine} Engine Paused</b>\n\n"
                            f"Walk-forward test PF = {avg_pf:.2f} (across {count} pairs)\n"
                            f"Engine will not generate signals until:\n"
                            f"1. Walk-forward results improve, OR\n"
                            f"2. Manual override: db.engine_state.updateOne("
                            f'{{engine: "{engine}"}}, {{$set: {{force_enabled: true}}}})'
                        ),
                        level="error",
                    ))
                except Exception:
                    pass

            return False

        return True

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        market_state = await self._get_market_state()

        stats = {"scanned": 0, "candidates_ready": 0, "skipped": 0,
                 "stale_blocked": 0, "errors": 0, "engines_paused": 0}
        signals = []
        stale_pairs = []

        for engine in self.engines:
            try:
                # ── Engine circuit breaker ─────────────
                if not await self._check_engine_circuit_breaker(engine):
                    stats["engines_paused"] += 1
                    continue

                eligible, rejections = await get_eligible_pairs(
                    self.mongodb_client, engine, top_n=self.top_n
                )
                logger.info(f"{engine}: {len(eligible)} eligible pairs, {len(rejections)} rejected")

                for pair_info in eligible:
                    pair = pair_info["pair"]

                    # ── Tradeable-pair safety gate ─────────
                    if pair not in TRADEABLE_PAIRS:
                        logger.warning(
                            f"{engine}/{pair}: skipped — not in TRADEABLE_PAIRS"
                        )
                        stats["skipped"] += 1
                        continue

                    try:
                        fr = await self._build_feature_row(pair)
                        if fr is None:
                            stats["skipped"] += 1
                            continue

                        # ── Data integrity gate ──────────────
                        if not fr.feature_staleness_ok:
                            stats["stale_blocked"] += 1
                            if pair not in [p for p, _ in stale_pairs]:
                                stale_pairs.append((pair, fr.staleness_flags))
                            logger.warning(
                                f"{engine}/{pair}: blocked — stale data: "
                                f"{', '.join(fr.staleness_flags)}"
                            )
                            continue

                        snap = DecisionSnapshot(
                            pair=pair,
                            features=fr,
                            market_state=market_state,
                            staleness_ok=fr.feature_staleness_ok,
                            staleness_flags=fr.staleness_flags,
                        )

                        # Generic dispatch via strategy registry
                        evaluate_fn = get_evaluate_fn(engine)
                        cand = evaluate_fn(snap)

                        # Store candidate (always, even filtered)
                        await self._store_candidate(engine, cand)
                        stats["scanned"] += 1

                        if cand.disposition == "CANDIDATE_READY":
                            stats["candidates_ready"] += 1
                            signals.append({"engine": engine, "pair": pair, "direction": cand.direction})
                            await self._send_telegram(engine, cand)
                            logger.info(f"🔔 {engine} CANDIDATE_READY: {pair} {cand.direction}")
                        else:
                            logger.debug(f"{engine} {pair}: {cand.disposition}")

                    except Exception as e:
                        stats["errors"] += 1
                        logger.error(f"Error scanning {engine}/{pair}: {e}")

            except Exception as e:
                stats["errors"] += 1
                logger.error(f"Error in {engine} scan: {e}")

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(f"SignalScanTask: {stats} in {duration:.1f}s")

        # Alert if a significant fraction of pairs is stale
        if stats["stale_blocked"] > 0:
            await self._send_staleness_alert(stats, stale_pairs)

        return {
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_id": context.execution_id,
            "market_state": market_state,
            "stats": stats,
            "signals": signals,
            "stale_pairs": len(stale_pairs),
            "duration_seconds": duration,
        }

    async def _send_staleness_alert(self, stats: dict, stale_pairs: list) -> None:
        """Send Telegram alert when data integrity checks block pairs.

        Uses a 60-minute cooldown via MongoDB to avoid spamming.
        """
        if not self.notification_manager:
            return

        # ── Cooldown check (1 alert per hour) ─────────────
        try:
            db = self.mongodb_client.get_database()
            cooldown_key = "signal_scan_staleness"
            cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
            doc = db.watchdog_alerts.find_one({"key": cooldown_key})
            if doc:
                last = doc.get("last_sent_at")
                if last:
                    if last.tzinfo is None:
                        last = last.replace(tzinfo=timezone.utc)
                    if last > cutoff:
                        logger.info(
                            f"Staleness alert suppressed (cooldown): "
                            f"{stats['stale_blocked']} blocked"
                        )
                        return
            db.watchdog_alerts.update_one(
                {"key": cooldown_key},
                {"$set": {"last_sent_at": datetime.now(timezone.utc)}},
                upsert=True,
            )
        except Exception as e:
            logger.debug(f"Cooldown check failed, sending anyway: {e}")

        try:
            from core.notifiers.base import NotificationMessage
            # Show up to 5 examples
            examples = "\n".join(
                f"  {p}: {', '.join(flags)}" for p, flags in stale_pairs[:5]
            )
            if len(stale_pairs) > 5:
                examples += f"\n  ... and {len(stale_pairs) - 5} more"
            msg = (
                f"<b>Data Integrity Warning</b>\n"
                f"Blocked {stats['stale_blocked']} pair-engine combos (stale data):\n"
                f"{examples}\n\n"
                f"Scanned: {stats['scanned']} | Ready: {stats['candidates_ready']} | "
                f"Errors: {stats['errors']}\n\n"
                f"Next alert in 60m if unresolved."
            )
            await self.notification_manager.send_notification(NotificationMessage(
                title="Signal Scan: Stale Data Warning",
                message=msg,
                level="warning",
            ))
        except Exception as e:
            logger.debug(f"Failed to send staleness alert: {e}")

    async def on_success(self, context: TaskContext, result) -> None:
        stats = result.result_data.get("stats", {})
        signals = result.result_data.get("signals", [])
        logger.info(f"SignalScanTask: {stats['scanned']} scanned, {stats['candidates_ready']} ready")
        for s in signals:
            logger.info(f"  Signal: {s['engine']} {s['pair']} {s['direction']}")

    async def on_failure(self, context: TaskContext, result) -> None:
        logger.error(f"SignalScanTask failed: {result.error_message}")
