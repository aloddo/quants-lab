"""
V2 ExitFlow — Stateless exit orchestrator.

Sequence: persist EXITING with full leg state -> submit both legs -> wait fills
-> handle partial (persist which leg closed) -> market escalate unfilled
-> transition to CLOSED/PARTIAL_EXIT.

Key hardening from Eng review:
- Full exit leg state persisted BEFORE submit (crash recovery can resolve)
- Direction-aware spread (Codex #6: uses position's reverse spread, not best spread)
- RE-CHECK via get_order() BEFORE market escalation (prevents double-fill)
- Tri-state fill detection (UNKNOWN = retry, never assume unfilled)
- Partial exit persists per-leg state with order IDs for crash recovery

Designed for reuse across any cross-venue dual-leg strategy.
"""
import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

from pymongo import MongoClient
from app.services.arb.position_store import (
    PositionStore, PositionState, LegState as LegStateEnum,
)
from app.services.arb.fill_detector import FillDetector, TrackedLeg
from app.services.arb.order_gateway import OrderGateway

logger = logging.getLogger(__name__)


@dataclass
class ExitResult:
    """Outcome of an exit attempt."""
    outcome: str  # SUCCESS, PARTIAL, FAILED, RISK_PAUSE
    bb_filled_qty: float = 0.0
    bn_filled_qty: float = 0.0
    bb_fill_price: float = 0.0
    bn_fill_price: float = 0.0
    actual_exit_spread_bps: float = 0.0
    latency_ms: float = 0.0
    pnl_net_bps: float = 0.0
    pnl_net_usd: float = 0.0
    fees_usd: float = 0.0
    slippage_bps: float = 0.0


class ExitFlow:
    """
    Stateless exit orchestrator. All state lives in PositionStore (MongoDB).

    Caller must hold the per-symbol Lock before calling execute().
    """

    FILL_TIMEOUT_S = 3.0
    FILL_TIMEOUT_POSTONLY_S = 5.0  # PostOnly needs more time to rest on book
    MARKET_ESCALATION_TIMEOUT_S = 2.0
    MAX_EXIT_ATTEMPTS = 5
    MAX_EXIT_TOTAL_S = 15.0  # Hard budget across all exit attempts

    def __init__(
        self,
        store: PositionStore,
        detector: FillDetector,
        gateway: OrderGateway,
        ws_available: dict[str, bool] | None = None,
        mongo_uri: str = "",
    ):
        self._store = store
        self._detector = detector
        self._gateway = gateway
        self._ws_available = ws_available or {"bybit": True, "binance": False}
        # Fill analytics collection
        self._analytics_coll = None
        if mongo_uri:
            try:
                db_name = mongo_uri.rsplit("/", 1)[-1]
                client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
                self._analytics_coll = client[db_name]["arb_h2_fill_analytics"]
            except Exception as e:
                logger.warning(f"ExitFlow: fill analytics disabled: {e}")

    def _log_fill_analytics(self, symbol: str, outcome: str, bb_leg: TrackedLeg = None, bn_leg: TrackedLeg = None, **extra):
        """Log exit attempt outcome for fill rate analytics."""
        if self._analytics_coll is None:
            return
        try:
            doc = {
                "type": "exit",
                "symbol": symbol,
                "outcome": outcome,
                "timestamp": time.time(),
                "bb_filled": bb_leg.is_filled if bb_leg else None,
                "bb_partial": bb_leg.has_any_fill if bb_leg else None,
                "bb_is_maker": bb_leg.is_maker if bb_leg else None,
                "bn_filled": bn_leg.is_filled if bn_leg else None,
                "bn_partial": bn_leg.has_any_fill if bn_leg else None,
                "bn_is_maker": bn_leg.is_maker if bn_leg else None,
                "had_market_escalation": extra.pop("had_market_escalation", False),
                **extra,
            }
            self._analytics_coll.insert_one(doc)
        except Exception as e:
            logger.debug(f"Fill analytics insert failed: {e}")

    async def execute(
        self,
        position_id: str,
        symbol: str,
        direction: str,
        bb_side: str,
        bn_side: str,
        qty_bb: float,
        qty_bn: float,
        price_bb: float,
        price_bn: float,
        exit_reason: str,
        is_stop_loss: bool = False,
        position_usd: float = 10.0,
        bb_order_mode: str = "aggressive",
    ) -> ExitResult:
        """
        Execute a dual-leg exit. Caller must hold per-symbol Lock.

        Handles: full exit, partial exit (one leg filled), retry of partial,
        and market escalation for unfilled legs.

        Exit ladder (per attempt):
          Attempt 1: PostOnly (if bb_order_mode="passive" and not stop_loss)
          Attempt 2-4: IOC (aggressive)
          Attempt 5+: Market (forced)

        For stop loss: uses market orders from the start.
        """
        t0 = time.time()
        order_type = "market" if is_stop_loss else "limit"

        # 1. Generate client IDs and persist EXITING state BEFORE submit
        bb_client_id = self._gateway.generate_client_id("h2v2x_bb")
        bn_client_id = self._gateway.generate_client_id("h2v2x_bn")

        # Only update exit leg fields for legs that are actually being retried.
        # For legs with qty=0 (already closed from prior partial), preserve their
        # persisted fill data — do NOT overwrite with IDLE.
        exit_updates = {"exit.reason": exit_reason}
        if qty_bb > 0:
            exit_updates.update({
                "exit.bb.client_order_id": bb_client_id,
                "exit.bb.side": bb_side,
                "exit.bb.target_qty": qty_bb,
                "exit.bb.target_price": price_bb,
                "exit.bb.state": LegStateEnum.PENDING_SUBMIT,
            })
        # else: bb leg already closed from prior partial — preserve its persisted fill data

        if qty_bn > 0:
            exit_updates.update({
                "exit.bn.client_order_id": bn_client_id,
                "exit.bn.side": bn_side,
                "exit.bn.target_qty": qty_bn,
                "exit.bn.target_price": price_bn,
                "exit.bn.state": LegStateEnum.PENDING_SUBMIT,
            })
        # else: bn leg already closed from prior partial — preserve its persisted fill data

        # Increment attempt count (for retries of partial exits)
        pos = await self._store.get(position_id)
        if not pos:
            return ExitResult(outcome="POSITION_NOT_FOUND")

        current_state = pos.get("state")
        attempt_count = pos.get("exit", {}).get("attempt_count", 0) + 1
        exit_updates["exit.attempt_count"] = attempt_count

        # LOW FIX (Codex #12): Enforce MAX_EXIT_TOTAL_S hard time budget
        first_exit_time = pos.get("exit", {}).get("first_attempt_time", t0)
        if attempt_count == 1:
            exit_updates["exit.first_attempt_time"] = t0
            first_exit_time = t0
        total_exit_seconds = t0 - first_exit_time
        if total_exit_seconds > self.MAX_EXIT_TOTAL_S and order_type != "market":
            logger.warning(f"Exit time budget exceeded ({total_exit_seconds:.0f}s > {self.MAX_EXIT_TOTAL_S}s) — forcing market")
            order_type = "market"
            is_stop_loss = True

        # Exit ladder: PostOnly (attempt 1) → IOC (2-4) → Market (5+)
        bb_exit_mode = "aggressive"  # default IOC for BB
        if attempt_count >= self.MAX_EXIT_ATTEMPTS and order_type != "market":
            logger.warning(
                f"Exit attempt {attempt_count} for {position_id} — forcing market orders on both legs"
            )
            order_type = "market"
            is_stop_loss = True
        elif attempt_count == 1 and bb_order_mode == "passive" and not is_stop_loss:
            bb_exit_mode = "passive"  # PostOnly on first attempt only
            logger.info(f"Exit attempt 1: using PostOnly for BB on {symbol}")

        # Transition from OPEN or PARTIAL_EXIT to EXITING
        if current_state == PositionState.OPEN:
            if not await self._store.transition(position_id, PositionState.OPEN, PositionState.EXITING, exit_updates):
                return ExitResult(outcome="TRANSITION_FAILED")
        elif current_state == PositionState.PARTIAL_EXIT:
            if not await self._store.transition(position_id, PositionState.PARTIAL_EXIT, PositionState.EXITING, exit_updates):
                return ExitResult(outcome="TRANSITION_FAILED")
        else:
            logger.warning(f"Exit called on position in state {current_state}")
            return ExitResult(outcome="WRONG_STATE")

        # 2. Create tracked legs and register BEFORE submit
        legs_to_track = []
        bb_leg = None
        bn_leg = None

        if qty_bb > 0:
            bb_leg = TrackedLeg(
                venue="bybit", symbol=symbol, side=bb_side,
                client_order_id=bb_client_id,
                target_qty=qty_bb, target_price=price_bb,
            )
            self._detector.register_leg(bb_leg)
            legs_to_track.append(bb_leg)
        if qty_bn > 0:
            bn_leg = TrackedLeg(
                venue="binance", symbol=symbol, side=bn_side,
                client_order_id=bn_client_id,
                target_qty=qty_bn, target_price=price_bn,
            )
            self._detector.register_leg(bn_leg)
            legs_to_track.append(bn_leg)

        if not legs_to_track:
            # Both legs were already closed from prior partial -- compute PnL from stored data
            # HIGH FIX (Opus C3 #3): Don't skip PnL computation for "no legs" path
            entry = pos.get("entry", {})
            exit_stored = pos.get("exit", {})
            bb_ep = float(entry.get("bb", {}).get("avg_fill_price", 0))
            bb_eq = float(entry.get("bb", {}).get("filled_qty", 0))
            bn_ep = float(entry.get("bn", {}).get("avg_fill_price", 0))
            bn_eq = float(entry.get("bn", {}).get("filled_qty", 0))
            bb_xp = float(exit_stored.get("bb", {}).get("avg_fill_price", 0))
            bn_xp = float(exit_stored.get("bn", {}).get("avg_fill_price", 0))
            bb_side_e = entry.get("bb", {}).get("side", "Buy")
            bn_side_e = entry.get("bn", {}).get("side", "Sell")
            bb_pnl = (bb_xp - bb_ep) * bb_eq if bb_side_e == "Buy" else (bb_ep - bb_xp) * bb_eq
            bn_pnl = (bn_ep - bn_xp) * bn_eq if bn_side_e == "Sell" else (bn_xp - bn_ep) * bn_eq
            gross = bb_pnl + bn_pnl
            total_fee = sum(float(entry.get(v, {}).get("fee", 0)) + float(exit_stored.get(v, {}).get("fee", 0)) for v in ("bb", "bn"))
            net = gross - total_fee
            avg_notional = (bb_ep * bb_eq + bn_ep * bn_eq) / 2 if (bb_ep * bb_eq + bn_ep * bn_eq) > 0 else 1
            trade_slippage_bps = abs(float(entry.get("slippage_bps", 0)))
            close_updates = {
                "exit_time": time.time(),
                "hold_seconds": time.time() - pos.get("entry_time", time.time()),
                "pnl.gross_bps": gross / avg_notional * 10000,
                "pnl.fees_bps": total_fee / avg_notional * 10000,
                "pnl.net_bps": (gross - total_fee) / avg_notional * 10000,
                "pnl.net_usd": net,
            }
            if not await self._store.transition(position_id, PositionState.EXITING, PositionState.CLOSED, close_updates):
                logger.critical(f"DB transition FAILED for {position_id} -> CLOSED (no legs)")
                return ExitResult(outcome="RISK_PAUSE")
            return ExitResult(
                outcome="SUCCESS",
                pnl_net_usd=net,
                pnl_net_bps=(gross - total_fee) / avg_notional * 10000,
                fees_usd=total_fee,
                slippage_bps=trade_slippage_bps,
            )

        # 3. Submit exit orders
        bb_oid = None
        bn_oid = None

        async def submit_bb():
            nonlocal bb_oid, bb_client_id
            if not bb_leg:
                return
            try:
                bb_oid = await self._gateway.submit(
                    "bybit", symbol, bb_side, qty_bb, price_bb, order_type, bb_client_id,
                    order_mode=bb_exit_mode,
                )
            except Exception as e:
                # PostOnly rejection on exit: immediately retry as IOC within same attempt
                is_postonly_reject = bb_exit_mode == "passive" and ("170213" in str(e) or "post only" in str(e).lower())
                if is_postonly_reject:
                    logger.info(f"Exit PostOnly rejected for {symbol}, retrying as IOC")
                    try:
                        bb_client_id_ioc = self._gateway.generate_client_id("h2v2x_bb_ioc")
                        bb_oid = await self._gateway.submit(
                            "bybit", symbol, bb_side, qty_bb, price_bb, "limit", bb_client_id_ioc,
                            order_mode="aggressive",
                        )
                        # HIGH FIX (Codex C2 #5): Re-register leg with new client_id
                        # so fill detector can match fills from the IOC order
                        bb_leg.client_order_id = bb_client_id_ioc
                        self._detector.register_leg(bb_leg)
                        # Update persisted client ID
                        await self._store.update_leg(position_id, "exit", "bb", {
                            "client_order_id": bb_client_id_ioc,
                        })
                        bb_client_id = bb_client_id_ioc
                    except Exception as e2:
                        logger.error(f"Exit Bybit IOC fallback also failed: {e2}")
                else:
                    logger.error(f"Exit Bybit submit failed: {e}")

        async def submit_bn():
            nonlocal bn_oid
            if not bn_leg:
                return
            try:
                bn_oid = await self._gateway.submit(
                    "binance", symbol, bn_side, qty_bn, price_bn, order_type, bn_client_id,
                )
            except Exception as e:
                logger.error(f"Exit Binance submit failed: {e}")

        await asyncio.gather(submit_bb(), submit_bn())

        # On submit failure, query by client_order_id before concluding order wasn't placed
        # (submit may have succeeded but HTTP response was lost)
        if not bb_oid and bb_leg:
            result = await self._detector.check_fill_definitive("bybit", symbol, client_order_id=bb_client_id)
            bb_leg.apply_rest_result(result)
            bb_oid = result.get("order_id", "")
            if bb_oid:
                logger.info(f"Exit Bybit submit appeared to fail but order exists: {bb_oid}")
        if not bn_oid and bn_leg:
            result = await self._detector.check_fill_definitive("binance", symbol, client_order_id=bn_client_id)
            bn_leg.apply_rest_result(result)
            bn_oid = result.get("order_id", "")
            if bn_oid:
                logger.info(f"Exit Binance submit appeared to fail but order exists: {bn_oid}")

        # Register exchange IDs
        if bb_oid and bb_leg:
            self._detector.register_exchange_id(bb_client_id, bb_oid)
            await self._store.update_leg(position_id, "exit", "bb", {
                "order_id": bb_oid, "state": LegStateEnum.SUBMITTED, "submitted_at": time.time(),
            })
        if bn_oid and bn_leg:
            self._detector.register_exchange_id(bn_client_id, bn_oid)
            await self._store.update_leg(position_id, "exit", "bn", {
                "order_id": bn_oid, "state": LegStateEnum.SUBMITTED, "submitted_at": time.time(),
            })

        # Replay buffered fills
        self._detector.replay_buffered()

        # 4. Wait for fills (PostOnly gets more time to rest on book)
        if is_stop_loss:
            timeout = self.MARKET_ESCALATION_TIMEOUT_S
        elif bb_exit_mode == "passive":
            timeout = self.FILL_TIMEOUT_POSTONLY_S
        else:
            timeout = self.FILL_TIMEOUT_S
        await self._detector.wait_for_fills(legs_to_track, timeout=timeout, ws_available=self._ws_available)

        # Track if any market escalation failed (for RISK_PAUSE decision)
        _had_escalation_failure = False

        # 5. For unfilled legs: CANCEL then RE-CHECK via get_order() BEFORE market escalation
        # This is the critical double-fill prevention gate (Eng review CRITICAL finding)
        for leg, oid, cid in [(bb_leg, bb_oid, bb_client_id), (bn_leg, bn_oid, bn_client_id)]:
            if not leg or leg.is_filled or not oid:
                continue

            # Cancel the original order first
            await self._gateway.cancel(leg.venue, symbol, oid)
            await asyncio.sleep(0.2)

            # RE-CHECK: did the original order actually fill?
            result = await self._detector.check_fill_definitive(
                leg.venue, symbol, order_id=oid, client_order_id=cid,
            )
            leg.apply_rest_result(result)

            if leg.is_filled:
                logger.info(f"Exit leg {leg.venue} filled after cancel (confirmed via REST)")
                continue

            # Check fill_result -- if UNKNOWN, do NOT escalate (could cause double-fill)
            if result.get("fill_result") == "UNKNOWN":
                logger.warning(
                    f"Exit leg {leg.venue} fill status UNKNOWN after cancel. "
                    f"NOT escalating to market (risk of double-fill). Will retry next cycle."
                )
                continue  # Leave unfilled, retry next cycle

            # Confirmed NOT_FILLED -- safe to escalate to market
            remaining = leg.target_qty - leg.filled_qty
            if remaining > 0:
                logger.warning(f"Exit leg {leg.venue} confirmed unfilled, escalating to market ({remaining:.4f})")
                try:
                    # Generate client_order_id for market escalation and persist to MongoDB
                    venue_key = "bb" if leg.venue == "bybit" else "bn"
                    mkt_client_id = self._gateway.generate_client_id(f"h2v2x_mkt_{venue_key}")
                    mkt_oid = await self._gateway.submit(
                        leg.venue, symbol, leg.side, remaining, 0, "market", mkt_client_id,
                    )
                    # Persist market escalation order ID to MongoDB
                    await self._store.update_leg(position_id, "exit", venue_key, {
                        "market_order_id": mkt_oid,
                        "market_client_order_id": mkt_client_id,
                        "market_submitted_at": time.time(),
                    })
                    mkt_leg = TrackedLeg(
                        venue=leg.venue, symbol=symbol, side=leg.side,
                        order_id=mkt_oid, client_order_id=mkt_client_id,
                        target_qty=remaining,
                        state=LegStateEnum.SUBMITTED, submitted_at=time.time(),
                    )
                    self._detector.register_leg(mkt_leg)
                    await self._detector.wait_for_fills(
                        [mkt_leg], timeout=self.MARKET_ESCALATION_TIMEOUT_S,
                        ws_available=self._ws_available,
                    )

                    if not mkt_leg.is_filled:
                        result = await self._detector.check_fill_definitive(
                            leg.venue, symbol, order_id=mkt_oid, client_order_id=mkt_client_id,
                        )
                        mkt_leg.apply_rest_result(result)

                    if mkt_leg.is_filled or mkt_leg.has_any_fill:
                        # Enrich fee data for Binance market escalation leg
                        if leg.venue == "binance":
                            await self._detector.enrich_leg_fee(mkt_leg)
                            leg._escalation_enriched = True  # Mark to skip overwrite in 5b
                        # Merge market fill into original leg
                        orig_qty = leg.filled_qty
                        orig_value = leg.avg_fill_price * orig_qty if orig_qty > 0 else 0
                        mkt_value = mkt_leg.avg_fill_price * mkt_leg.filled_qty
                        leg.filled_qty = orig_qty + mkt_leg.filled_qty
                        leg.avg_fill_price = (orig_value + mkt_value) / leg.filled_qty if leg.filled_qty > 0 else 0
                        leg.fee += mkt_leg.fee
                        if leg.filled_qty >= leg.target_qty * 0.99:
                            leg.state = LegStateEnum.FILLED
                        else:
                            leg.state = LegStateEnum.PARTIAL
                        leg.fill_event.set()
                        # Compute escalation slippage: market fill vs original limit target
                        escalation_slip_bps = 0.0
                        if leg.target_price > 0 and mkt_leg.avg_fill_price > 0:
                            escalation_slip_bps = (mkt_leg.avg_fill_price - leg.target_price) / leg.target_price * 10000
                        # Persist partial market fill so next retry computes correct remaining
                        await self._store.update_leg(position_id, "exit", venue_key, {
                            "filled_qty": leg.filled_qty,
                            "avg_fill_price": leg.avg_fill_price,
                            "fee": leg.fee,
                            "state": leg.state,
                            "escalation_slippage_bps": escalation_slip_bps,
                            "market_fill_price": mkt_leg.avg_fill_price,
                        })
                    else:
                        # HIGH FIX (Codex #4): Market escalation timeout = fail-closed
                        logger.critical(f"EXIT MARKET TIMEOUT {leg.venue} {symbol} -- NAKED LEG, RISK_PAUSE")
                        _had_escalation_failure = True
                        await self._store.update_leg(position_id, "exit", venue_key, {
                            "market_timeout": True,
                            "naked_leg": True,
                        })

                    self._detector.cleanup_leg(mkt_leg)
                except Exception as e:
                    logger.critical(f"EXIT MARKET FAILED {leg.venue} {symbol}: {e}")
                    _had_escalation_failure = True

        # CRITICAL FIX (Codex C2 #2): If market escalation failed, force RISK_PAUSE
        if _had_escalation_failure:
            logger.critical(f"Market escalation FAILED for {symbol} — forcing RISK_PAUSE")
            if not await self._store.transition(position_id, PositionState.EXITING, PositionState.PARTIAL_EXIT, {
                "exit.escalation_failed": True,
            }):
                pass  # Best effort
            self._cleanup_legs(bb_leg, bn_leg)
            return ExitResult(outcome="RISK_PAUSE", latency_ms=(time.time() - t0) * 1000)

        # 5b. Enrich Binance fee data (REST get_order doesn't return fees)
        # Skip if market escalation already enriched mkt_leg and merged fees into bn_leg
        # (enrich_leg_fee overwrites fee, which would lose the merged escalation fee)
        bn_had_escalation = bn_leg and hasattr(bn_leg, '_escalation_enriched') and bn_leg._escalation_enriched
        if bn_leg and bn_leg.has_any_fill and not bn_had_escalation:
            await self._detector.enrich_leg_fee(bn_leg)

        # 6. Evaluate outcome and persist
        bb_filled = bb_leg.is_filled if bb_leg else True  # True if leg was skipped (IDLE/already closed)
        bn_filled = bn_leg.is_filled if bn_leg else True  # True if leg was skipped (IDLE/already closed)

        if bb_filled and bn_filled:
            # SUCCESS -- both legs closed
            # PnL: compute from ACTUAL fill prices per leg, not spread bps approximation.
            # The spread-bps formula was wrong (5-10x overstatement) because it doesn't
            # account for actual quantities, price differences between venues, or fees.
            entry = pos.get("entry", {})
            exit_stored = pos.get("exit", {})

            # Get all fill prices and quantities
            bb_entry_price = float(entry.get("bb", {}).get("avg_fill_price", 0))
            bb_entry_qty = float(entry.get("bb", {}).get("filled_qty", 0))
            bn_entry_price = float(entry.get("bn", {}).get("avg_fill_price", 0))
            bn_entry_qty = float(entry.get("bn", {}).get("filled_qty", 0))

            bb_exit_price = bb_leg.avg_fill_price if bb_leg else float(exit_stored.get("bb", {}).get("avg_fill_price", 0))
            bb_exit_qty = bb_leg.filled_qty if bb_leg else float(exit_stored.get("bb", {}).get("filled_qty", 0))
            bn_exit_price = bn_leg.avg_fill_price if bn_leg else float(exit_stored.get("bn", {}).get("avg_fill_price", 0))
            bn_exit_qty = bn_leg.filled_qty if bn_leg else float(exit_stored.get("bn", {}).get("filled_qty", 0))

            # Fees — CRITICAL FIX (Opus C3 #1): Only add prior stored fee for legs
            # NOT active in this attempt. Active legs already include all fills+escalation.
            # Adding prior fee on top of active leg = double-counting.
            bb_entry_fee = float(entry.get("bb", {}).get("fee", 0))
            if bb_leg:
                bb_exit_fee = bb_leg.fee  # This attempt's fee (includes escalation merges)
            else:
                bb_exit_fee = float(exit_stored.get("bb", {}).get("fee", 0))  # Prior closed leg
            bn_entry_fee = float(entry.get("bn", {}).get("fee", 0))
            if bn_leg:
                bn_exit_fee = bn_leg.fee
            else:
                bn_exit_fee = float(exit_stored.get("bn", {}).get("fee", 0))
            total_fees_usd = bb_entry_fee + bb_exit_fee + bn_entry_fee + bn_exit_fee

            # CRITICAL FIX (Codex #2 + #8): Use min(entry_qty, exit_qty) for PnL to handle
            # partial exits that span multiple attempts. If exit filled less than entry
            # (shouldn't happen normally but can in partial retries), use the smaller qty.
            bb_pnl_qty = min(bb_entry_qty, bb_exit_qty) if bb_exit_qty > 0 else bb_entry_qty
            bn_pnl_qty = min(bn_entry_qty, bn_exit_qty) if bn_exit_qty > 0 else bn_entry_qty

            # Bybit perp PnL: (exit - entry) * qty for longs, (entry - exit) * qty for shorts
            bb_entry_side = entry.get("bb", {}).get("side", "Buy")
            if bb_entry_side == "Buy":
                bb_pnl = (bb_exit_price - bb_entry_price) * bb_pnl_qty
            else:
                bb_pnl = (bb_entry_price - bb_exit_price) * bb_pnl_qty

            # Binance spot PnL: sold high bought low (or vice versa)
            bn_entry_side = entry.get("bn", {}).get("side", "Sell")
            if bn_entry_side == "Sell":
                bn_pnl = (bn_entry_price - bn_exit_price) * bn_pnl_qty
            else:
                bn_pnl = (bn_exit_price - bn_entry_price) * bn_pnl_qty

            # Combined PnL
            gross_usd = bb_pnl + bn_pnl
            net_usd = gross_usd - total_fees_usd

            # Convert to bps for reporting (using average notional)
            avg_notional = (bb_entry_price * bb_entry_qty + bn_entry_price * bn_entry_qty) / 2
            gross_bps = (gross_usd / avg_notional * 10000) if avg_notional > 0 else 0
            fees_bps = (total_fees_usd / avg_notional * 10000) if avg_notional > 0 else 0
            net_bps = gross_bps - fees_bps

            entry_slippage_bps = abs(float(entry.get("slippage_bps", 0)))
            bb_exit_slip_bps = (
                abs((bb_leg.avg_fill_price - bb_leg.target_price) / bb_leg.target_price * 10000)
                if bb_leg and bb_leg.target_price > 0 and bb_leg.avg_fill_price > 0 else 0.0
            )
            bn_exit_slip_bps = (
                abs((bn_leg.avg_fill_price - bn_leg.target_price) / bn_leg.target_price * 10000)
                if bn_leg and bn_leg.target_price > 0 and bn_leg.avg_fill_price > 0 else 0.0
            )
            trade_slippage_bps = entry_slippage_bps + bb_exit_slip_bps + bn_exit_slip_bps

            actual_spread = self._compute_exit_spread(bb_leg, bn_leg, bb_side)

            close_updates = {
                "exit_time": time.time(),
                "hold_seconds": time.time() - pos.get("entry_time", time.time()),
                "pnl.gross_bps": gross_bps,
                "pnl.fees_bps": fees_bps,
                "pnl.net_bps": net_bps,
                "pnl.net_usd": net_usd,
                "exit.actual_spread_bps": actual_spread,
                "exit.latency_ms": (time.time() - t0) * 1000,
            }
            # Persist fill data for legs that were active in this exit
            if bb_leg:
                close_updates.update({
                    "exit.bb.filled_qty": bb_leg.filled_qty,
                    "exit.bb.avg_fill_price": bb_leg.avg_fill_price,
                    "exit.bb.fee": bb_leg.fee,
                    "exit.bb.fee_asset": bb_leg.fee_asset,
                    "exit.bb.is_maker": bb_leg.is_maker,
                    "exit.bb.exec_ids": bb_leg.exec_ids,
                    "exit.bb.state": LegStateEnum.FILLED,
                    "exit.bb.slippage_bps": ((bb_leg.avg_fill_price - bb_leg.target_price) / bb_leg.target_price * 10000) if bb_leg.target_price > 0 else 0,
                })
            if bn_leg:
                close_updates.update({
                    "exit.bn.filled_qty": bn_leg.filled_qty,
                    "exit.bn.avg_fill_price": bn_leg.avg_fill_price,
                    "exit.bn.fee": bn_leg.fee,
                    "exit.bn.fee_asset": bn_leg.fee_asset,
                    "exit.bn.is_maker": bn_leg.is_maker,
                    "exit.bn.exec_ids": bn_leg.exec_ids,
                    "exit.bn.state": LegStateEnum.FILLED,
                    "exit.bn.slippage_bps": ((bn_leg.avg_fill_price - bn_leg.target_price) / bn_leg.target_price * 10000) if bn_leg.target_price > 0 else 0,
                })

            if not await self._store.transition(position_id, PositionState.EXITING, PositionState.CLOSED, close_updates):
                logger.critical(f"DB transition FAILED for {position_id} -> CLOSED. "
                                f"NOT unregistering position or updating inventory.")
                # Do NOT cleanup or return SUCCESS — position state is inconsistent
                self._cleanup_legs(bb_leg, bn_leg)
                return ExitResult(
                    outcome="RISK_PAUSE",
                    latency_ms=(time.time() - t0) * 1000,
                )
            self._log_fill_analytics(symbol, "SUCCESS", bb_leg, bn_leg,
                                     latency_ms=(time.time() - t0) * 1000,
                                     pnl_net_bps=net_bps, pnl_net_usd=net_usd,
                                     exit_reason=exit_reason)
            self._cleanup_legs(bb_leg, bn_leg)

            return ExitResult(
                outcome="SUCCESS",
                bb_filled_qty=bb_leg.filled_qty if bb_leg else 0,
                bn_filled_qty=bn_leg.filled_qty if bn_leg else 0,
                bb_fill_price=bb_leg.avg_fill_price if bb_leg else 0,
                bn_fill_price=bn_leg.avg_fill_price if bn_leg else 0,
                actual_exit_spread_bps=actual_spread,
                latency_ms=(time.time() - t0) * 1000,
                pnl_net_bps=net_bps,
                pnl_net_usd=net_usd,
                fees_usd=total_fees_usd,
                slippage_bps=trade_slippage_bps,
            )

        # PARTIAL -- one or both legs unfilled
        # CRITICAL FIX (Codex C4 #1): Persist CUMULATIVE filled_qty (prior + current)
        # not just this attempt's local fill. Otherwise remaining qty is miscalculated.
        exit_stored = pos.get("exit", {})
        partial_updates = {"exit.latency_ms": (time.time() - t0) * 1000}
        if bb_leg:
            prior_bb_qty = float(exit_stored.get("bb", {}).get("filled_qty", 0))
            cumulative_bb_qty = prior_bb_qty + bb_leg.filled_qty
            prior_bb_fee = float(exit_stored.get("bb", {}).get("fee", 0))
            partial_updates.update({
                "exit.bb.filled_qty": cumulative_bb_qty,
                "exit.bb.avg_fill_price": bb_leg.avg_fill_price,  # latest fill price
                "exit.bb.fee": prior_bb_fee + bb_leg.fee,  # cumulative fees
                "exit.bb.fee_asset": bb_leg.fee_asset,
                "exit.bb.state": bb_leg.state if bb_leg.is_filled else LegStateEnum.PARTIAL,
            })
        if bn_leg:
            prior_bn_qty = float(exit_stored.get("bn", {}).get("filled_qty", 0))
            cumulative_bn_qty = prior_bn_qty + bn_leg.filled_qty
            prior_bn_fee = float(exit_stored.get("bn", {}).get("fee", 0))
            partial_updates.update({
                "exit.bn.filled_qty": cumulative_bn_qty,
                "exit.bn.avg_fill_price": bn_leg.avg_fill_price,
                "exit.bn.fee": prior_bn_fee + bn_leg.fee,
                "exit.bn.fee_asset": bn_leg.fee_asset,
                "exit.bn.state": bn_leg.state if bn_leg.is_filled else LegStateEnum.PARTIAL,
            })

        # FIX #3: Check for ANY fills (including PARTIAL), not just fully filled legs.
        # If any leg has fills, go to PARTIAL_EXIT — never revert to OPEN with fills.
        bb_has_any = bb_leg and bb_leg.has_any_fill
        bn_has_any = bn_leg and bn_leg.has_any_fill
        if bb_has_any or bn_has_any or (bb_leg and bb_leg.is_filled) or (bn_leg and bn_leg.is_filled):
            # At least one leg has fills (full or partial) -- PARTIAL_EXIT
            if not await self._store.transition(position_id, PositionState.EXITING, PositionState.PARTIAL_EXIT, partial_updates):
                logger.critical(f"DB transition FAILED for {position_id} -> PARTIAL_EXIT. "
                                f"NOT unregistering position or updating inventory.")
                self._cleanup_legs(bb_leg, bn_leg)
                return ExitResult(outcome="RISK_PAUSE", latency_ms=(time.time() - t0) * 1000)
            self._cleanup_legs(bb_leg, bn_leg)
            return ExitResult(
                outcome="PARTIAL",
                bb_filled_qty=bb_leg.filled_qty if bb_leg else 0,
                bn_filled_qty=bn_leg.filled_qty if bn_leg else 0,
                bb_fill_price=bb_leg.avg_fill_price if bb_leg else 0,  # HIGH FIX (Codex C2 #4)
                bn_fill_price=bn_leg.avg_fill_price if bn_leg else 0,  # HIGH FIX (Codex C2 #4)
                latency_ms=(time.time() - t0) * 1000,
            )

        # Neither leg has ANY fills in THIS attempt.
        # But if a prior exit already closed one leg, we must stay in PARTIAL_EXIT, not OPEN.
        # Going back to OPEN when Bybit is already flat would cause the system to re-sell on Bybit.
        prior_bb_filled = pos.get("exit", {}).get("bb", {}).get("state") in (LegStateEnum.FILLED, "FILLED")
        prior_bn_filled = pos.get("exit", {}).get("bn", {}).get("state") in (LegStateEnum.FILLED, "FILLED")
        if prior_bb_filled or prior_bn_filled:
            # Prior exit already closed one leg -- stay in PARTIAL_EXIT
            if not await self._store.transition(position_id, PositionState.EXITING, PositionState.PARTIAL_EXIT, partial_updates):
                logger.critical(f"DB transition FAILED for {position_id} -> PARTIAL_EXIT (prior leg filled)")
                self._cleanup_legs(bb_leg, bn_leg)
                return ExitResult(outcome="RISK_PAUSE", latency_ms=(time.time() - t0) * 1000)
            self._cleanup_legs(bb_leg, bn_leg)
            logger.warning(f"Exit retry had no new fills but prior exit closed one leg. Staying in PARTIAL_EXIT.")
            return ExitResult(outcome="PARTIAL", latency_ms=(time.time() - t0) * 1000)

        # CRITICAL FIX (Codex C2 #1): If any leg has UNKNOWN fill status, do NOT revert to OPEN
        # UNKNOWN means we can't confirm whether fills happened — stay in PARTIAL_EXIT for retry
        # HIGH FIX (Codex C4 #3): IDLE after submit attempt = unknown, not safe.
        # Only FILLED/CANCELLED/REJECTED are definitively resolved states.
        _resolved = (LegStateEnum.FILLED, LegStateEnum.CANCELLED, LegStateEnum.REJECTED)
        bb_unknown = bb_leg and bb_leg.state not in _resolved and (bb_oid or bb_client_id)
        bn_unknown = bn_leg and bn_leg.state not in _resolved and (bn_oid or bn_client_id)
        if bb_unknown or bn_unknown:
            logger.warning(f"Exit has UNKNOWN fill status (bb={bb_leg.state if bb_leg else 'N/A'}, bn={bn_leg.state if bn_leg else 'N/A'}). Staying in PARTIAL_EXIT for retry.")
            if not await self._store.transition(position_id, PositionState.EXITING, PositionState.PARTIAL_EXIT, partial_updates):
                logger.critical(f"DB transition FAILED for {position_id} -> PARTIAL_EXIT (UNKNOWN status)")
                self._cleanup_legs(bb_leg, bn_leg)
                return ExitResult(outcome="RISK_PAUSE", latency_ms=(time.time() - t0) * 1000)
            self._cleanup_legs(bb_leg, bn_leg)
            return ExitResult(outcome="PARTIAL", latency_ms=(time.time() - t0) * 1000)

        # Truly no fills (no prior, no current) -- safe to go back to OPEN
        if not await self._store.transition(position_id, PositionState.EXITING, PositionState.OPEN, partial_updates):
            logger.critical(f"DB transition FAILED for {position_id} -> OPEN (exit failed, no fills). "
                            f"NOT unregistering position or updating inventory.")
            self._cleanup_legs(bb_leg, bn_leg)
            return ExitResult(outcome="RISK_PAUSE", latency_ms=(time.time() - t0) * 1000)
        self._cleanup_legs(bb_leg, bn_leg)
        return ExitResult(
            outcome="FAILED",
            latency_ms=(time.time() - t0) * 1000,
        )

    def _compute_exit_spread(
        self, bb_leg: Optional[TrackedLeg], bn_leg: Optional[TrackedLeg], bb_side: str,
    ) -> float:
        """Compute actual exit spread from fill prices (in bps)."""
        bb_price = bb_leg.avg_fill_price if bb_leg and bb_leg.avg_fill_price > 0 else 0
        bn_price = bn_leg.avg_fill_price if bn_leg and bn_leg.avg_fill_price > 0 else 0
        if bb_price <= 0 or bn_price <= 0:
            return 0.0
        if bb_side == "Sell":
            # Exit of BUY_BB_SELL_BN: sell BB, buy BN
            return (bb_price - bn_price) / bn_price * 10000
        else:
            return (bn_price - bb_price) / bb_price * 10000

    def _cleanup_legs(self, *legs):
        for leg in legs:
            if leg:
                self._detector.cleanup_leg(leg)
