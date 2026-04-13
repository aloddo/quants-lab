"""
Exchange-level position reconciliation — exchange is the source of truth.

Comparison paths:
1. Legacy (E2): MongoDB candidates vs HB executors vs exchange positions.
2. HB-native (E3+): Exchange positions vs HB bot orchestration controllers.

Key principle: NEVER trust HB's filled_amount=0 after a crash. HB stores
executor↔order linkage in memory only. On any crash, it reports 0 filled
even when the exchange has an open position.

The exchange is queried DIRECTLY via BybitExchangeClient (not through the
HB API connector, which is blind to bot container positions).

Used by:
- WatchdogTask: periodic reconciliation (every 5 min)
- cli.py startup: exchange audit before pipeline starts
"""
import logging
import re
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from app.services.hb_api_client import HBApiClient

logger = logging.getLogger(__name__)


async def _find_docs(collection, query: dict, projection: dict = None) -> list:
    """Query MongoDB collection, handling both sync (pymongo) and async (motor)."""
    import inspect
    cursor = collection.find(query, projection) if projection else collection.find(query)
    result = cursor.to_list(None)
    return await result if inspect.isawaitable(result) else result


# ── Public API ──────────────────────────────────────────────


async def startup_exchange_audit(
    db, connector: str = "bybit_perpetual_testnet",
    account: str = "master_account",
) -> dict:
    """Run on pipeline startup BEFORE any trading tasks.

    Queries the exchange DIRECTLY for ALL open positions, cross-references
    with both MongoDB candidates (legacy) and HB bot controllers (native).
    """
    hb = HBApiClient()
    stats = {"exchange_positions": 0, "orphan_positions": 0, "stale_candidates": 0}

    try:
        exchange_positions = await _get_exchange_positions()
        stats["exchange_positions"] = len(exchange_positions)

        if not exchange_positions:
            logger.info("Startup audit: no exchange positions")
            await hb.close()
            return stats

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        # Gather tracked pairs from BOTH legacy and HB-native paths
        tracked_pairs = set()

        # Legacy: candidates collection
        for doc in await _find_docs(
            db.candidates,
            {"disposition": {"$in": ["TESTNET_ACTIVE", "PLACING"]}},
            {"pair": 1},
        ):
            tracked_pairs.add(doc.get("pair"))

        # HB-native: active bot controllers
        bot_controller_pairs, _ = await _get_bot_controller_info(hb)
        tracked_pairs.update(bot_controller_pairs)

        # Check each exchange position
        for pos in exchange_positions:
            pair = pos["pair"]
            side = pos["side"]
            qty = pos.get("qty", 0)

            if pair in tracked_pairs:
                logger.info(f"Startup audit: {pair} {side} — tracked")
            else:
                stats["orphan_positions"] += 1
                logger.error(
                    f"Startup audit: ORPHAN EXCHANGE POSITION {pair} {side} "
                    f"qty={qty} entry={pos.get('entry_price')} "
                    f"uPnL={pos.get('unrealised_pnl')} — not tracked by any bot or candidate"
                )

        # Check for stale TESTNET_ACTIVE candidates with no exchange position
        exchange_pairs = {p["pair"] for p in exchange_positions}
        mongo_active = await _find_docs(db.candidates, {
            "disposition": {"$in": ["TESTNET_ACTIVE", "PLACING"]},
        })

        for doc in mongo_active:
            pair = doc.get("pair", "?")
            if pair not in exchange_pairs:
                stats["stale_candidates"] += 1
                engine = doc.get("engine", "?")
                logger.warning(
                    f"Startup audit: {engine}/{pair} is TESTNET_ACTIVE in MongoDB "
                    f"but has no exchange position — attempting recovery"
                )
                eid = doc.get("executor_id")
                if eid:
                    if await hb.health_check():
                        await _recover_from_executor_history(
                            hb, db, doc, eid, now_ms, connector, account
                        )

        if stats["orphan_positions"] == 0 and stats["stale_candidates"] == 0:
            logger.info(
                f"Startup audit: clean — {stats['exchange_positions']} exchange "
                f"position(s), all tracked"
            )

    except Exception as e:
        logger.error(f"Startup audit failed: {e}")
        stats["error"] = str(e)
    finally:
        await hb.close()

    return stats


async def reconcile_positions(
    db, connector: str = "bybit_perpetual_testnet",
    account: str = "master_account",
) -> dict:
    """Three-way reconciliation: exchange vs HB bots vs MongoDB candidates.

    Runs periodically from the watchdog task. Covers BOTH legacy (E2) and
    HB-native (E3+) bot paths.
    """
    stats = {
        "mongo_active": 0, "hb_active": 0, "bot_controller_pairs": 0,
        "exchange_active": 0,
        "ghost_positions": 0, "ghost_recovered": 0, "ghost_unresolved": 0,
        "orphan_executors": 0, "exchange_untracked": 0, "size_mismatches": 0,
    }

    hb = HBApiClient()
    try:
        hb_reachable = await hb.health_check()

        # ── Source 1: Exchange positions (source of truth) ──
        exchange_positions = await _get_exchange_positions()
        stats["exchange_active"] = len(exchange_positions)
        exchange_pairs = {p["pair"] for p in exchange_positions}

        # ── Source 2: MongoDB candidates (legacy tracking) ──
        mongo_active = await _find_docs(db.candidates, {
            "disposition": {"$in": ["TESTNET_ACTIVE", "PLACING"]},
        })
        stats["mongo_active"] = len(mongo_active)
        mongo_by_executor = {
            doc["executor_id"]: doc
            for doc in mongo_active
            if doc.get("executor_id")
        }
        mongo_pairs = {doc.get("pair") for doc in mongo_active}

        # ── Source 3: HB bot controllers (HB-native tracking) ──
        bot_pairs = set()
        bot_volume_by_pair = {}  # pair -> volume_traded by current bot
        hb_active_ids = set()
        if hb_reachable:
            bot_pairs, bot_volume_by_pair = await _get_bot_controller_info(hb)
            stats["bot_controller_pairs"] = len(bot_pairs)

            hb_executors = await hb.get_active_executors()
            stats["hb_active"] = len(hb_executors)
            for ex in hb_executors:
                eid = ex.get("executor_id") or ex.get("id")
                if eid:
                    hb_active_ids.add(eid)
        else:
            logger.warning("Reconciliation: HB API unreachable — skipping executor checks")

        # All tracked pairs from both paths
        tracked_pairs = mongo_pairs | bot_pairs

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        # ── Check 1: Ghost positions (MongoDB active, HB executor gone) ──
        if hb_reachable:
            for eid, doc in mongo_by_executor.items():
                if eid in hb_active_ids:
                    continue

                stats["ghost_positions"] += 1
                pair = doc.get("pair", "?")
                engine = doc.get("engine", "?")
                direction = doc.get("direction", "?")

                logger.warning(
                    f"Reconciliation: ghost {engine}/{pair} {direction} "
                    f"(executor {eid}) — attempting recovery"
                )

                recovered = await _recover_from_executor_history(
                    hb, db, doc, eid, now_ms, connector, account
                )
                if recovered:
                    stats["ghost_recovered"] += 1
                else:
                    stats["ghost_unresolved"] += 1

        # ── Check 2: Exchange positions not tracked anywhere ──
        for pos in exchange_positions:
            pair = pos["pair"]
            if pair not in tracked_pairs:
                stats["exchange_untracked"] += 1
                logger.error(
                    f"Reconciliation: UNTRACKED EXCHANGE POSITION {pair} "
                    f"{pos['side']} qty={pos.get('qty', pos.get('amount'))} "
                    f"entry={pos.get('entry_price')} "
                    f"uPnL={pos.get('unrealised_pnl', pos.get('unrealized_pnl'))} "
                    f"— not tracked by any bot or candidate"
                )

        # ── Check 3: Position size mismatch (exchange value >> bot volume) ──
        # Detects stacked positions from ghost bot containers
        if hb_reachable and bot_volume_by_pair:
            for pos in exchange_positions:
                pair = pos["pair"]
                exchange_value = pos.get("position_value", 0)
                bot_volume = bot_volume_by_pair.get(pair)

                if bot_volume is not None and bot_volume > 0 and exchange_value > 0:
                    ratio = exchange_value / bot_volume
                    if ratio > 1.5:  # >50% larger than what the bot traded
                        stats["size_mismatches"] += 1
                        logger.error(
                            f"Reconciliation: SIZE MISMATCH {pair} — "
                            f"exchange value=${exchange_value:.2f} but bot "
                            f"traded only ${bot_volume:.2f} "
                            f"(ratio={ratio:.1f}x). Likely stacked from ghost bot."
                        )

        # ── Check 4: Orphan executors (HB active, not in MongoDB) ──
        if hb_reachable:
            for eid in hb_active_ids:
                if eid not in mongo_by_executor:
                    stats["orphan_executors"] += 1
                    logger.warning(
                        f"Reconciliation: orphan executor {eid} — "
                        "active in HB but not tracked in MongoDB"
                    )

    except Exception as e:
        logger.error(f"Reconciliation failed: {e}")
        stats["error"] = str(e)
    finally:
        await hb.close()

    clean = (
        stats["ghost_positions"] == 0
        and stats["orphan_executors"] == 0
        and stats["exchange_untracked"] == 0
        and stats["size_mismatches"] == 0
    )
    if clean:
        logger.info(
            f"Reconciliation: clean — {stats['exchange_active']} exchange, "
            f"{stats['mongo_active']} MongoDB, {stats['bot_controller_pairs']} bot controllers"
        )

    return stats


# ── Recovery logic ──────────────────────────────────────────


async def _recover_from_executor_history(
    hb: HBApiClient, db, doc: dict, executor_id: str, now_ms: int,
    connector: str = "bybit_perpetual_testnet",
    account: str = "master_account",
) -> bool:
    """Recover a ghost position from HB executor history.

    CRITICAL: If executor reports filled_amount=0 after a crash
    (SYSTEM_CLEANUP/FAILED), we cross-check with the exchange before
    accepting. HB loses executor↔order state on crash.
    """
    cid = doc.get("candidate_id", "?")
    pair = doc.get("pair", "?")
    engine = doc.get("engine", "?")

    try:
        status = await hb.get_executor_status(executor_id)
    except Exception as e:
        logger.warning(f"Reconciliation: cannot query executor {executor_id}: {e}")
        return False

    exec_status = (status.get("status") or "").lower()
    close_type = status.get("close_type")

    if exec_status not in ("completed", "failed", "stopped", "closed", "terminated"):
        return False

    filled_amount = status.get("filled_amount_quote")
    filled_zero = filled_amount is not None and float(filled_amount) == 0
    is_cleanup = close_type in ("SYSTEM_CLEANUP", "FAILED")

    # THE KEY CHECK: never trust filled_amount=0 from a crashed executor
    if filled_zero and is_cleanup:
        has_exchange_pos = await _check_exchange_position(pair)
        if has_exchange_pos:
            # Exchange has a position but executor says unfilled — MISMATCH
            logger.error(
                f"Reconciliation: EXCHANGE MISMATCH {engine}/{pair} — "
                f"executor reports filled=0 but exchange has open position. "
                f"DO NOT resolve. Manual intervention required."
            )
            db.candidates.update_one(
                {"candidate_id": cid},
                {"$set": {
                    "reconciliation_divergent": True,
                    "reconciliation_checked_at": now_ms,
                    "reconciliation_reason": "exchange_mismatch_filled_zero",
                    "reconciliation_needs_manual": True,
                }},
            )
            return False  # NOT recovered — needs manual handling

        # No exchange position either → genuinely never filled
        logger.info(
            f"Reconciliation: {engine}/{pair} — executor says unfilled, "
            f"exchange confirms no position. Marking NEVER_FILLED."
        )
        resolved_close_type = "NEVER_FILLED"
    else:
        resolved_close_type = close_type or "RECOVERED"

    # Extract outcome data
    custom_info = status.get("custom_info") or {}
    fill_price = (
        status.get("entry_price")
        or status.get("fill_price")
        or custom_info.get("current_position_average_price")
    )
    exit_price = (
        status.get("close_price")
        or status.get("exit_price")
        or custom_info.get("close_price")
    )
    pnl = status.get("pnl") or status.get("net_pnl_quote")

    # Compute slippage
    decision_price = doc.get("decision_price")
    slippage_bps = None
    slippage_bucket = None
    if decision_price and fill_price:
        try:
            dp = float(decision_price)
            fp_val = float(fill_price)
            if dp > 0:
                direction = doc.get("direction", "LONG")
                raw_slip = (fp_val - dp) / dp * 10000
                slippage_bps = raw_slip if direction == "LONG" else -raw_slip
                abs_slip = abs(slippage_bps)
                slippage_bucket = (
                    "safe" if abs_slip < 10
                    else "borderline" if abs_slip < 20
                    else "danger"
                )
        except (ValueError, TypeError):
            pass

    db.candidates.update_one(
        {"candidate_id": cid},
        {"$set": {
            "disposition": "RESOLVED_TESTNET",
            "testnet_resolved_at": now_ms,
            "testnet_fill_price": fill_price,
            "testnet_exit_price": exit_price,
            "testnet_pnl": pnl,
            "testnet_close_type": resolved_close_type,
            "testnet_filled_amount_quote": filled_amount,
            "testnet_status": exec_status,
            "testnet_raw_result": status,
            "testnet_slippage_bps": slippage_bps,
            "testnet_slippage_bucket": slippage_bucket,
            "reconciliation_recovered": True,
            "reconciliation_recovered_at": now_ms,
        }},
    )

    logger.info(
        f"Reconciliation: recovered {engine}/{pair} — "
        f"close_type={resolved_close_type}, pnl={pnl}, filled={filled_amount}"
    )
    return True


# ── Exchange helpers ────────────────────────────────────────


async def _get_exchange_positions() -> list[dict]:
    """Query Bybit directly for all open positions.

    Uses BybitExchangeClient (authenticated REST) — bypasses HB API entirely.
    This sees ALL positions regardless of which bot opened them.
    """
    from app.services.bybit_exchange_client import BybitExchangeClient

    exchange = BybitExchangeClient()
    if not exchange.is_configured():
        logger.warning("Reconciliation: Bybit credentials not configured")
        return []

    try:
        async with aiohttp.ClientSession() as session:
            return await exchange.fetch_positions(session)
    except Exception as e:
        logger.warning(f"Reconciliation: cannot query exchange positions: {e}")
        return []


async def _check_exchange_position(pair: str) -> bool:
    """Check if a specific pair has an open position on the exchange."""
    positions = await _get_exchange_positions()
    return any(p["pair"] == pair for p in positions)


async def _get_bot_controller_info(hb: HBApiClient) -> tuple[set[str], dict[str, float]]:
    """Extract active trading pairs and volume data from HB bot orchestration.

    Returns:
        (pairs, volume_by_pair) where volume_by_pair maps pair -> volume_traded
        by the current bot. If exchange position_value >> volume_traded, there's
        a stacked position from a ghost bot.
    """
    pairs = set()
    volume_by_pair = {}
    try:
        raw = await hb.get_bot_status()
        # HB API wraps in {"status": "success", "data": {...bots...}}
        if isinstance(raw, dict) and "data" in raw:
            bots = raw["data"]
        else:
            bots = raw
        if not isinstance(bots, dict):
            return pairs, volume_by_pair

        for bot_name, bot_info in bots.items():
            if not isinstance(bot_info, dict):
                continue
            bot_status = (bot_info.get("status") or "").lower()
            if bot_status not in ("running", "active"):
                continue

            # Controllers appear as keys in the "performance" dict
            perf = bot_info.get("performance", {})
            if isinstance(perf, dict):
                for ctrl_name, ctrl_data in perf.items():
                    pair = _parse_pair_from_controller(ctrl_name)
                    if pair:
                        pairs.add(pair)
                        # Extract volume traded by this bot's controller
                        p = ctrl_data.get("performance", {}) if isinstance(ctrl_data, dict) else {}
                        vol = float(p.get("volume_traded", 0) or 0)
                        if vol > 0:
                            volume_by_pair[pair] = volume_by_pair.get(pair, 0) + vol

    except Exception as e:
        logger.warning(f"Reconciliation: cannot query bot controllers: {e}")

    return pairs, volume_by_pair


def _parse_pair_from_controller(name: str) -> Optional[str]:
    """Parse a trading pair from a controller name.

    Examples:
        'e3_ada_usdt' -> 'ADA-USDT'
        'e3_1000pepe_usdt' -> '1000PEPE-USDT'
        'e4_crowd_fade_btc_usdt' -> 'BTC-USDT'
    """
    if not name:
        return None
    # Match the trailing <base>_usdt pattern
    m = re.search(r"_([a-z0-9]+)_usdt$", name.lower())
    if m:
        base = m.group(1).upper()
        return f"{base}-USDT"
    return None
