"""
Shared placement logic for order execution via HB API.

Used by both TestnetResolverTask (hourly signal_scan candidates) and
BreakoutMonitorTask (real-time breakout candidates).

Centralises position sizing, deduplication guards, HB executor creation,
and rich Telegram notifications so both paths behave identically.
"""
import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.engines.fmt import fp, fmt_duration, fmt_pct
from app.engines.strategy_registry import get_strategy
from app.services.hb_api_client import HBApiClient

logger = logging.getLogger(__name__)


# ── Quantization ────────────────────────────────────────────

def quantize_amount(
    pair: str, raw_amount: float, trading_rules: Dict[str, Any],
) -> float:
    """Quantize order amount to the pair's min_base_amount_increment."""
    rule = trading_rules.get(pair)
    if rule:
        step = rule.get("min_base_amount_increment", 0)
        if step and step > 0:
            quantized = math.floor(raw_amount / step) * step
            min_size = rule.get("min_order_size", 0) or 0
            if quantized < min_size:
                return 0.0
            return quantized
    return round(raw_amount, 3)


# ── Deduplication guards ────────────────────────────────────

async def check_duplicate(db, engine: str, pair: str, direction: str) -> Optional[str]:
    """Check for position collisions. Returns skip reason or None if clear.

    Rules (strategy-agnostic):
    - HARD BLOCK: same (engine, pair, direction) already TESTNET_ACTIVE
    - SOFT BLOCK: same (engine, pair) opposite direction already TESTNET_ACTIVE
    - Cross-engine same pair is ALLOWED (different strategies)
    """
    # Exact duplicate
    existing = await db["candidates"].find_one({
        "engine": engine,
        "pair": pair,
        "direction": direction,
        "disposition": "TESTNET_ACTIVE",
    })
    if existing:
        return "duplicate_active"

    # Direction conflict within same engine
    opposite = await db["candidates"].find_one({
        "engine": engine,
        "pair": pair,
        "direction": {"$ne": direction},
        "disposition": "TESTNET_ACTIVE",
    })
    if opposite:
        return "direction_conflict"

    return None


async def check_portfolio_limits(
    db, engine: str, engines: list, max_portfolio: int,
) -> Optional[str]:
    """Check portfolio and per-engine concurrency limits.

    Returns skip reason or None if there's room.
    """
    meta = get_strategy(engine)
    max_concurrent = meta.max_concurrent

    # Per-engine count
    engine_active = await db["candidates"].count_documents(
        {"engine": engine, "disposition": "TESTNET_ACTIVE"},
    )
    if engine_active >= max_concurrent:
        return f"{engine}_concurrent_limit"

    # Portfolio count
    total_active = await db["candidates"].count_documents(
        {"disposition": "TESTNET_ACTIVE"},
    )
    if total_active >= max_portfolio:
        return "portfolio_full"

    return None


async def mark_skipped(db, candidate_id: str, disposition: str, reason: str) -> None:
    """Mark a candidate as skipped with reason for audit trail."""
    await db["candidates"].update_one(
        {"candidate_id": candidate_id},
        {"$set": {
            "disposition": disposition,
            "skipped_reason": reason,
            "skipped_at": int(datetime.now(timezone.utc).timestamp() * 1000),
        }},
    )


# ── Order placement ─────────────────────────────────────────

async def place_order(
    candidate: dict,
    capital: float,
    position_size_pct: float,
    connector: str,
    account: str,
    hb_client: HBApiClient,
    trading_rules: Dict[str, Any],
    db,
    notification_manager=None,
) -> Optional[str]:
    """Place an order via HB API and update MongoDB.

    Returns executor_id on success, None on failure.
    """
    engine = candidate["engine"]
    pair = candidate["pair"]
    direction = candidate["direction"]
    meta = get_strategy(engine)

    amount_usd = capital * position_size_pct
    price = candidate.get("decision_price", 0)
    if price <= 0:
        logger.error(f"Invalid decision_price for {pair}: {price}")
        return None

    raw_amount = amount_usd / price
    amount = quantize_amount(pair, raw_amount, trading_rules)
    if amount <= 0:
        logger.warning(
            f"Order amount too small for {pair}: "
            f"${amount_usd:.2f} / {price} = {raw_amount} (below min)"
        )
        return None

    side = 1 if direction == "LONG" else 2

    # TP/SL: candidate-level prices if available, fall back to registry pcts
    tp_abs = candidate.get("tp_price")
    sl_abs = candidate.get("sl_price")
    if tp_abs and price > 0:
        tp_pct = abs(float(tp_abs) - price) / price
    else:
        tp_pct = float(meta.exit_params.get("take_profit", "0.03"))
    if sl_abs and price > 0:
        sl_pct = abs(float(sl_abs) - price) / price
    else:
        sl_pct = float(meta.exit_params.get("stop_loss", "0.015"))

    time_limit = meta.exit_params.get("time_limit", 86400)

    triple_barrier = {
        "take_profit": str(tp_pct),
        "stop_loss": str(sl_pct),
        "time_limit": time_limit,
    }
    if meta.trailing_stop:
        triple_barrier["trailing_stop"] = {
            "activation_price": str(meta.trailing_stop["activation_price"]),
            "trailing_delta": str(meta.trailing_stop["trailing_delta"]),
        }

    # Pre-register pair so HB connector builds rate limits
    registered = await hb_client.ensure_trading_pair(connector, pair, account)
    if not registered:
        logger.warning(f"Could not pre-register {pair} — executor may fail")

    request_body = {
        "account_name": account,
        "executor_config": {
            "type": "position_executor",
            "connector_name": connector,
            "trading_pair": pair,
            "side": side,
            "amount": str(amount),
            "leverage": 1,
            "triple_barrier_config": triple_barrier,
        },
    }

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    try:
        result = await hb_client.create_executor(request_body)
        executor_id = result.get("executor_id") or result.get("id")
        logger.info(f"Placed demo order for {engine}/{pair}: executor_id={executor_id}")

        # Update candidate in MongoDB
        await db["candidates"].update_one(
            {"candidate_id": candidate["candidate_id"]},
            {"$set": {
                "disposition": "TESTNET_ACTIVE",
                "executor_id": executor_id,
                "testnet_placed_at": now_ms,
                "testnet_amount": amount,
                "testnet_amount_usd": amount_usd,
                "testnet_tp_pct": tp_pct,
                "testnet_sl_pct": sl_pct,
                "testnet_time_limit": time_limit,
                "testnet_trailing_stop": meta.trailing_stop if meta.trailing_stop else None,
            }},
        )

        # Rich notification
        if notification_manager:
            try:
                from core.notifiers.base import NotificationMessage

                # Compute TP/SL display prices
                if direction == "LONG":
                    tp_display = price * (1 + tp_pct)
                    sl_display = price * (1 - sl_pct)
                    tp_sign, sl_sign = "+", "-"
                else:
                    tp_display = price * (1 - tp_pct)
                    sl_display = price * (1 + sl_pct)
                    tp_sign, sl_sign = "-", "+"

                rr = tp_pct / sl_pct if sl_pct > 0 else 0

                await notification_manager.send_notification(NotificationMessage(
                    title=f"Demo Order — {engine}/{pair}",
                    message=(
                        f"<b>Demo Order Placed</b>\n"
                        f"{engine}/{pair} {direction}\n"
                        f"Entry: {fp(price)} | Size: ${amount_usd:.0f} ({amount:.4g} {pair.split('-')[0]})\n"
                        f"TP: {fp(tp_display)} ({tp_sign}{fmt_pct(tp_pct, signed=False)}) | "
                        f"SL: {fp(sl_display)} ({sl_sign}{fmt_pct(sl_pct, signed=False)})\n"
                        f"R:R {rr:.2f} | Time limit: {fmt_duration(time_limit)}"
                    ),
                    level="info",
                ))
            except Exception as e:
                logger.warning(f"Notification failed: {e}")

        return executor_id

    except Exception as e:
        logger.error(f"Failed to place demo order for {engine}/{pair}: {e}")
        await db["candidates"].update_one(
            {"candidate_id": candidate["candidate_id"]},
            {"$set": {"disposition": "TESTNET_FAILED", "testnet_error": str(e)}},
        )
        return None


# ── Capital fetching ─────────────────────────────────────────

async def get_capital(
    hb_client: HBApiClient, account: str, fallback: float,
) -> float:
    """Fetch USDT capital from HB API, with fallback."""
    try:
        portfolio = await hb_client.get_portfolio_state(account)
        if isinstance(portfolio, dict):
            for token_data in portfolio.get("tokens", []):
                if token_data.get("token") == "USDT":
                    return float(token_data.get("balance", fallback))
        return fallback
    except Exception as e:
        logger.warning(f"Could not fetch capital: {e}, using fallback {fallback}")
        return fallback
