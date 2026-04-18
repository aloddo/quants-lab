"""
ThinExecutorTask — pipeline task wrapper for ThinExecutor.

Runs signal checks and lifecycle management on a configurable schedule.
Signal check: every minute (configurable via signal_interval_s).
Lifecycle check: every 30s (manages fills, TP/SL, trailing, time limits).
"""

import asyncio
import logging
import os
import time
from typing import Any, Dict

from pymongo import MongoClient

from core.tasks import BaseTask, TaskContext

from app.services.bybit_exchange_client import BybitExchangeClient
from app.services.market_data_shim import MarketDataShim
from app.services.thin_executor import ThinExecutor, StrategySlot

logger = logging.getLogger(__name__)


class ThinExecutorTask(BaseTask):
    """Pipeline task: run thin executor signal + lifecycle checks."""

    _executor: ThinExecutor = None
    _last_signal_check: float = 0
    _last_lifecycle_check: float = 0
    _initialized: bool = False

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        config = self.config.config
        signal_interval = config.get("signal_interval_s", 60)
        lifecycle_interval = config.get("lifecycle_interval_s", 30)

        if not self._initialized:
            self._init_executor(config)
            self._initialized = True

        now = time.time()
        results = {}

        # Signal check
        if now - self._last_signal_check >= signal_interval:
            try:
                await self._executor.signal_check()
                self._last_signal_check = now
                results["signal_check"] = "ok"
            except Exception as e:
                logger.error(f"Signal check failed: {e}")
                results["signal_check"] = str(e)

        # Lifecycle check
        if now - self._last_lifecycle_check >= lifecycle_interval:
            try:
                await self._executor.lifecycle_check()
                self._last_lifecycle_check = now
                results["lifecycle_check"] = "ok"
            except Exception as e:
                logger.error(f"Lifecycle check failed: {e}")
                results["lifecycle_check"] = str(e)

        # Count active executors
        active = self._executor.db.executor_state.count_documents(
            {"state": {"$in": ["PENDING", "ACTIVE"]}}
        )
        results["active_executors"] = active

        return results

    def _init_executor(self, config: dict):
        """Initialize the ThinExecutor with configured strategies."""
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
        mongo_db = os.getenv("MONGO_DATABASE", "quants_lab")
        db = MongoClient(mongo_uri)[mongo_db]

        exchange = BybitExchangeClient()
        shim = MarketDataShim()

        strategy_names = config.get("strategies", [])
        strategies = []

        for engine_name in strategy_names:
            slots = self._build_strategy_slots(engine_name, shim, db)
            strategies.extend(slots)

        self._executor = ThinExecutor(
            strategies=strategies,
            exchange=exchange,
            shim=shim,
            db=db,
        )

        # Reconcile on startup (run synchronously in init)
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._executor.reconcile_on_startup())
        except RuntimeError:
            # No running loop (shouldn't happen in pipeline, but safe fallback)
            asyncio.run(self._executor.reconcile_on_startup())

        logger.info(
            f"ThinExecutor initialized: {len(strategies)} strategy slots "
            f"across {len(strategy_names)} engines"
        )

    def _build_strategy_slots(self, engine_name: str, shim, db) -> list:
        """Build StrategySlot instances for all ALLOW pairs for an engine."""
        from app.engines.strategy_registry import STRATEGY_REGISTRY

        # Find engine in registry (STRATEGY_REGISTRY is a Dict[str, StrategyMetadata])
        engine_key = engine_name.upper()
        engine_meta = STRATEGY_REGISTRY.get(engine_key)

        if not engine_meta:
            logger.warning(f"Engine {engine_key} not in registry, skipping")
            return []

        # Get ALLOW pairs from pair_historical
        allow_pairs = list(db.pair_historical.find(
            {"engine": engine_name.upper(), "verdict": "ALLOW"},
        ))
        pairs = [p["pair"] for p in allow_pairs]

        if not pairs:
            logger.warning(f"No ALLOW pairs for {engine_name}")
            return []

        # Import controller class from registry
        import importlib
        mod = importlib.import_module(engine_meta.controller_module)

        # Config class name is in the registry
        config_class_name = engine_meta.config_class_name
        ConfigClass = getattr(mod, config_class_name)

        # Controller class: derive from config class name (FooConfig -> FooController)
        controller_class_name = config_class_name.replace("Config", "Controller")
        ControllerClass = getattr(mod, controller_class_name)

        slots = []
        queue = asyncio.Queue()  # shared dummy queue

        for pair in pairs:
            try:
                config = ConfigClass(
                    id=f"{engine_name.lower()}_wrapper_{pair}",
                    connector_name="bybit_perpetual_testnet",
                    trading_pair=pair,
                )
                controller = ControllerClass(
                    config,
                    market_data_provider=shim,
                    actions_queue=queue,
                )

                slot = StrategySlot(
                    engine=engine_name.upper(),
                    pair=pair,
                    connector_name="bybit_perpetual_testnet",
                    controller=controller,
                    signal_interval_s=self.config.config.get("signal_interval_s", 60),
                )
                slots.append(slot)
            except Exception as e:
                logger.error(f"Failed to create slot {engine_name}/{pair}: {e}")

        logger.info(f"Built {len(slots)} slots for {engine_name}: {pairs}")
        return slots
