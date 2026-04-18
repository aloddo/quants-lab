"""
RiskStateTask — computes and writes portfolio risk state to MongoDB every 60s.

This is the "adult in the room" for all live trading. Controllers and the thin
executor read portfolio_risk_state from MongoDB to make sizing/admission decisions.

Design (from unified plan v4, Apr 17 2026):
- Reads equity + positions from Bybit via BybitExchangeClient
- Computes portfolio/strategy/pair heat with conservative estimates
- Writes single document to portfolio_risk_state (upserted)
- If Bybit is unreachable for >5 minutes, triggers circuit breaker
- Cleans up expired risk reservations (>5 min old)
"""

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict

import aiohttp
from pymongo import MongoClient

from core.tasks import BaseTask, TaskContext

from app.services.bybit_exchange_client import BybitExchangeClient
from app.services.portfolio_risk_manager import (
    compute_risk_state,
    DEFAULT_RISK_CONFIG,
)

logger = logging.getLogger(__name__)

# How long before we declare risk state stale and trigger circuit breaker
STALENESS_THRESHOLD_SECONDS = 300  # 5 minutes


class RiskStateTask(BaseTask):
    """Pipeline task: compute and write portfolio risk state every 60s."""

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        """Main task execution. Called by TaskOrchestrator."""
        config = self.config.config
        fallback_capital = config.get("fallback_capital", 100000)
        initial_capital = config.get("initial_capital", 100000)

        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
        mongo_db_name = os.getenv("MONGO_DATABASE", "quants_lab")
        db = MongoClient(mongo_uri)[mongo_db_name]

        exchange = BybitExchangeClient()
        risk_config = _load_risk_config(db)

        try:
            async with aiohttp.ClientSession() as session:
                positions = await exchange.fetch_positions(session)
                wallet = await exchange.fetch_wallet_balance(session)

            equity = wallet.get("equity", 0)
            available_margin = wallet.get("available", 0)

            if equity <= 0:
                equity = fallback_capital
                logger.warning(
                    f"Equity query returned {equity}, using fallback {fallback_capital}"
                )

            # Enrich positions with engine attribution
            positions = _enrich_positions(db, positions)

            # Preserve existing reservations
            existing = db.portfolio_risk_state.find_one({"_id": "current"})
            reservations = existing.get("reservations", {}) if existing else {}

            # Clean expired reservations
            reservations = _clean_expired_reservations(reservations)

            # Compute full risk state
            state = compute_risk_state(
                equity=equity,
                available_margin=available_margin,
                initial_capital=initial_capital,
                positions=positions,
                risk_config=risk_config,
                reservations=reservations,
            )

            # Upsert to MongoDB
            db.portfolio_risk_state.replace_one(
                {"_id": "current"}, state, upsert=True
            )

            pos_count = len(positions)
            heat_pct = state["portfolio_heat"]["total_risk_pct"]
            cb = state["circuit_breaker"]["triggered"]

            logger.info(
                f"RiskState: equity=${equity:.0f}, positions={pos_count}, "
                f"heat={heat_pct:.1%}, circuit_breaker={cb}"
            )

            return {
                "equity": equity,
                "positions": pos_count,
                "heat_pct": heat_pct,
                "circuit_breaker": cb,
            }

        except Exception as e:
            logger.error(f"RiskStateTask failed: {e}")
            _check_staleness_breaker(db)
            return {"error": str(e)}


def _load_risk_config(db) -> dict:
    """Load risk config from MongoDB, fall back to defaults."""
    stored = db.portfolio_risk_state.find_one({"_id": "current"})
    if stored and "risk_config" in stored:
        return stored["risk_config"]
    return DEFAULT_RISK_CONFIG.copy()


def _enrich_positions(db, positions: list) -> list:
    """Add engine attribution to positions from executor_state or paper_trades."""
    for pos in positions:
        pair = pos.get("pair", "")

        # Try executor_state first (thin wrapper positions)
        executor = db.executor_state.find_one(
            {"pair": pair, "state": "ACTIVE"},
            sort=[("signal_at", -1)],
        )
        if executor:
            pos["engine"] = executor.get("engine", "untracked")
            continue

        # Try paper_trades (HB bot positions)
        bot = db.paper_trades.find_one(
            {"trading_pair": pair.replace("-", "")},
            sort=[("timestamp", -1)],
        )
        if bot:
            bot_name = bot.get("bot_name", "")
            m = re.match(r"^([a-zA-Z]+\d+)", bot_name)
            pos["engine"] = m.group(1).upper() if m else "untracked"
            continue

        pos["engine"] = "untracked"

    return positions


def _clean_expired_reservations(reservations: dict) -> dict:
    """Remove reservations older than 5 minutes."""
    now = datetime.now(timezone.utc)
    cleaned = {}
    for key, res in reservations.items():
        expires = res.get("expires_at")
        if isinstance(expires, datetime) and expires > now:
            cleaned[key] = res
        elif isinstance(expires, str):
            try:
                exp_dt = datetime.fromisoformat(expires.replace("Z", "+00:00"))
                if exp_dt > now:
                    cleaned[key] = res
            except (ValueError, TypeError):
                pass
    return cleaned


def _check_staleness_breaker(db):
    """If risk state is stale beyond threshold, trigger circuit breaker."""
    existing = db.portfolio_risk_state.find_one({"_id": "current"})
    if not existing:
        return

    updated = existing.get("updated_at")
    if not isinstance(updated, datetime):
        return

    now = datetime.now(timezone.utc)
    age = (now - updated).total_seconds()

    if age > STALENESS_THRESHOLD_SECONDS:
        logger.warning(
            f"Risk state is {age:.0f}s old (>{STALENESS_THRESHOLD_SECONDS}s). "
            "Triggering circuit breaker."
        )
        db.portfolio_risk_state.update_one(
            {"_id": "current"},
            {"$set": {
                "circuit_breaker": {
                    "triggered": True,
                    "reason": f"stale_equity ({age:.0f}s without update)",
                    "triggered_at": now,
                }
            }},
        )
