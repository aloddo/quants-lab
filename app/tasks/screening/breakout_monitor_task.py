"""
Breakout Monitor — real-time E1 signal detection via frequent price polling.

Runs every 60 seconds (not chained to the hourly candle pipeline).
Fetches ALL USDT perpetual prices in a single Bybit API call, then checks
each ALLOW pair for compression breakouts using cached features from MongoDB.

Solves the core E1 problem: the hourly signal scan misses breakouts that
happen and resolve within one scan interval.

Features (ATR percentile, range bounds) are loaded from MongoDB and cached
in memory.  The cache is refreshed when features are newer than the cached
version.
"""
import logging
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiohttp

from core.tasks import BaseTask, TaskContext

from app.engines.strategy_registry import get_evaluate_fn, get_strategy
from app.engines.models import DecisionSnapshot, FeatureRow
from app.services.bybit_rest import fetch_all_prices
from app.tasks.notifying_task import NotifyingTaskMixin

logger = logging.getLogger(__name__)

# Debounce: don't re-trigger the same pair within this window
DEBOUNCE_SECONDS = 3600  # 1 hour


class BreakoutMonitorTask(NotifyingTaskMixin, BaseTask):
    """Real-time breakout detection via frequent price polling."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.engine_name = task_config.get("engine", "E1")
        self.connector_name = task_config.get("connector_name", "bybit_perpetual")

        # In-memory caches (refreshed from MongoDB)
        self._feature_cache: Dict[str, dict] = {}  # pair -> feature data
        self._feature_cache_ts: Optional[datetime] = None
        self._allow_pairs: List[str] = []
        self._debounce: Dict[str, datetime] = {}  # pair -> last trigger time

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB required for BreakoutMonitorTask")

    async def _refresh_feature_cache(self) -> None:
        """Load features and ALLOW pairs from MongoDB.

        Only refreshes if features are newer than cached version.
        """
        db = self.mongodb_client.get_database()

        # Check if features have been updated since last cache
        latest = await db["features"].find_one(
            {"feature_name": "atr"},
            sort=[("computed_at", -1)],
        )
        if latest and self._feature_cache_ts:
            computed = latest.get("computed_at")
            if computed and computed <= self._feature_cache_ts:
                return  # cache is still fresh

        # Load ALLOW pairs for this engine
        allow_docs = await self.mongodb_client.get_documents(
            "pair_historical",
            {"engine": self.engine_name, "verdict": "ALLOW"},
        )
        self._allow_pairs = [d["pair"] for d in allow_docs]

        # Load features for each ALLOW pair
        cache = {}
        for pair in self._allow_pairs:
            feat = {}
            for fname in ["atr", "range", "volume", "derivatives"]:
                docs = await self.mongodb_client.get_documents(
                    "features",
                    {"feature_name": fname, "trading_pair": pair},
                    limit=1,
                )
                if docs:
                    feat[fname] = docs[0].get("value", {})
            if feat.get("atr") and feat.get("range"):
                cache[pair] = feat

        self._feature_cache = cache
        self._feature_cache_ts = datetime.now(timezone.utc)
        logger.info(
            f"BreakoutMonitor: cached features for {len(cache)}/{len(self._allow_pairs)} "
            f"ALLOW pairs"
        )

    def _build_feature_row(self, pair: str, live_price: float) -> Optional[FeatureRow]:
        """Build a FeatureRow from cached features + live price."""
        feat = self._feature_cache.get(pair)
        if not feat:
            return None

        atr_d = feat.get("atr", {})
        range_d = feat.get("range", {})
        vol_d = feat.get("volume", {})
        deriv_d = feat.get("derivatives", {})

        atr_pct = atr_d.get("atr_percentile_90d")
        if atr_pct is None:
            return None

        return FeatureRow(
            pair=pair,
            timestamp_utc=int(datetime.now(timezone.utc).timestamp() * 1000),
            close=live_price,
            atr_14_1h=atr_d.get("atr_14_1h"),
            atr_percentile_90d=atr_pct,
            range_high_20=range_d.get("range_high_20"),
            range_low_20=range_d.get("range_low_20"),
            range_compression_confirmed=(atr_pct < 0.35),
            volume_1h=vol_d.get("vol_avg_20"),
            volume_zscore_20=vol_d.get("vol_zscore_20"),
            volume_floor_passed=(vol_d.get("vol_floor_passed", 0) > 0.5),
            funding_rate_current=deriv_d.get("funding_rate"),
            funding_neutral=deriv_d.get("funding_neutral", 0) > 0.5,
            oi_change_1h_pct=deriv_d.get("oi_change_1h_pct"),
            oi_increasing=deriv_d.get("oi_increasing", 0) > 0.5,
            rs_aligned=deriv_d.get("rs_aligned", 0) > 0.5,
            # Candle OHLC not available in real-time — use live price as proxy
            candle_high=live_price,
            candle_low=live_price,
            candle_open=live_price,
            range_expanding=range_d.get("range_expanding"),
            feature_staleness_ok=True,
            staleness_flags=[],
        )

    def _is_debounced(self, pair: str) -> bool:
        """Check if this pair triggered recently."""
        last = self._debounce.get(pair)
        if last is None:
            return False
        return (datetime.now(timezone.utc) - last).total_seconds() < DEBOUNCE_SECONDS

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

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)

        # Refresh feature cache if needed
        await self._refresh_feature_cache()

        if not self._feature_cache:
            return {
                "status": "completed",
                "timestamp": start.isoformat(),
                "execution_id": context.execution_id,
                "stats": {"checked": 0, "note": "no features cached"},
            }

        # Fetch all prices in one API call
        async with aiohttp.ClientSession() as session:
            prices = await fetch_all_prices(session)

        market_state = await self._get_market_state()
        evaluate_fn = get_evaluate_fn(self.engine_name)

        stats = {"checked": 0, "compressed": 0, "breakouts": 0,
                 "candidates_ready": 0, "debounced": 0}

        for pair in self._allow_pairs:
            price = prices.get(pair)
            if not price:
                continue

            fr = self._build_feature_row(pair, price)
            if fr is None:
                continue

            stats["checked"] += 1

            # Quick pre-filter: only check pairs in compression
            if not fr.range_compression_confirmed:
                continue
            stats["compressed"] += 1

            # Check for breakout: price outside range
            if fr.range_high_20 and fr.range_low_20:
                if fr.range_low_20 <= price <= fr.range_high_20:
                    continue  # inside range, no breakout

            stats["breakouts"] += 1

            # Debounce check
            if self._is_debounced(pair):
                stats["debounced"] += 1
                continue

            # Run full evaluation
            snap = DecisionSnapshot(
                pair=pair,
                features=fr,
                market_state=market_state,
                staleness_ok=True,
                staleness_flags=[],
            )
            cand = evaluate_fn(snap)

            if cand.disposition == "CANDIDATE_READY":
                stats["candidates_ready"] += 1
                self._debounce[pair] = datetime.now(timezone.utc)

                # Store candidate
                doc = asdict(cand)
                doc["engine"] = self.engine_name
                doc["source"] = "breakout_monitor"  # distinguish from hourly scan
                await self.mongodb_client.insert_documents(
                    collection_name="candidates",
                    documents=[doc],
                    index=[("engine", 1), ("pair", 1), ("timestamp_utc", 1)],
                )

                logger.info(
                    f"BREAKOUT {self.engine_name}/{pair} {cand.direction} "
                    f"price={price} score={cand.composite_score:.2f}"
                )

                # Telegram alert
                if self.notification_manager:
                    try:
                        from core.notifiers.base import NotificationMessage
                        from app.engines.fmt import fp
                        await self.notification_manager.send_notification(NotificationMessage(
                            title=f"RT Breakout — {self.engine_name}/{pair}",
                            message=(
                                f"<b>Real-Time Breakout Detected</b>\n"
                                f"Engine: {self.engine_name} | Pair: {pair}\n"
                                f"Direction: {cand.direction}\n"
                                f"Price: {fp(price)}\n"
                                f"Trigger: {cand.trigger_reason}\n"
                                f"Score: {cand.composite_score:.2f}"
                            ),
                            level="warning",
                        ))
                    except Exception as e:
                        logger.warning(f"Telegram failed: {e}")

        duration = (datetime.now(timezone.utc) - start).total_seconds()

        if stats["candidates_ready"] > 0:
            logger.info(
                f"BreakoutMonitor: {stats['candidates_ready']} signals from "
                f"{stats['checked']} pairs in {duration:.1f}s"
            )

        return {
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_id": context.execution_id,
            "stats": stats,
            "duration_seconds": duration,
        }
