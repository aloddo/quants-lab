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


class ExitFlow:
    """
    Stateless exit orchestrator. All state lives in PositionStore (MongoDB).

    Caller must hold the per-symbol Lock before calling execute().
    """

    FILL_TIMEOUT_S = 3.0
    MARKET_ESCALATION_TIMEOUT_S = 2.0
    MAX_EXIT_ATTEMPTS = 5

    def __init__(
        self,
        store: PositionStore,
        detector: FillDetector,
        gateway: OrderGateway,
        ws_available: dict[str, bool] | None = None,
    ):
        self._store = store
        self._detector = detector
        self._gateway = gateway
        self._ws_available = ws_available or {"bybit": True, "binance": False}

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
    ) -> ExitResult:
        """
        Execute a dual-leg exit. Caller must hold per-symbol Lock.

        Handles: full exit, partial exit (one leg filled), retry of partial,
        and market escalation for unfilled legs.

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

        # After MAX_EXIT_ATTEMPTS failed attempts, force market orders on both legs
        if attempt_count >= self.MAX_EXIT_ATTEMPTS and order_type != "market":
            logger.warning(
                f"Exit attempt {attempt_count} for {position_id} — forcing market orders on both legs"
            )
            order_type = "market"
            is_stop_loss = True  # Use shorter timeout for forced market

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
            # Both legs were already closed from prior partial -- just close
            if not await self._store.transition(position_id, PositionState.EXITING, PositionState.CLOSED, {
                "exit_time": time.time(),
            }):
                logger.critical(f"DB transition FAILED for {position_id} -> CLOSED (no legs)")
                return ExitResult(outcome="RISK_PAUSE")
            return ExitResult(outcome="SUCCESS")

        # 3. Submit exit orders
        bb_oid = None
        bn_oid = None

        async def submit_bb():
            nonlocal bb_oid
            if not bb_leg:
                return
            try:
                bb_oid = await self._gateway.submit(
                    "bybit", symbol, bb_side, qty_bb, price_bb, order_type, bb_client_id,
                )
            except Exception as e:
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

        # 4. Wait for fills
        timeout = self.MARKET_ESCALATION_TIMEOUT_S if is_stop_loss else self.FILL_TIMEOUT_S
        await self._detector.wait_for_fills(legs_to_track, timeout=timeout, ws_available=self._ws_available)

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
                        # Persist partial market fill so next retry computes correct remaining
                        await self._store.update_leg(position_id, "exit", venue_key, {
                            "filled_qty": leg.filled_qty,
                            "avg_fill_price": leg.avg_fill_price,
                            "fee": leg.fee,
                            "state": leg.state,
                        })
                    else:
                        logger.critical(f"EXIT MARKET TIMEOUT {leg.venue} {symbol} -- MANUAL INTERVENTION")

                    self._detector.cleanup_leg(mkt_leg)
                except Exception as e:
                    logger.critical(f"EXIT MARKET FAILED {leg.venue} {symbol}: {e}")

        # 6. Evaluate outcome and persist
        bb_filled = bb_leg.is_filled if bb_leg else True  # True if leg was skipped (IDLE/already closed)
        bn_filled = bn_leg.is_filled if bn_leg else True  # True if leg was skipped (IDLE/already closed)

        if bb_filled and bn_filled:
            # SUCCESS -- both legs closed
            # For PnL, combine current in-memory leg data with any prior partial exit data
            # (if bb_leg is None, the bb exit happened in a prior partial — use stored data)
            exit_stored = pos.get("exit", {})
            bb_exit_price = bb_leg.avg_fill_price if bb_leg else float(exit_stored.get("bb", {}).get("avg_fill_price", 0))
            bn_exit_price = bn_leg.avg_fill_price if bn_leg else float(exit_stored.get("bn", {}).get("avg_fill_price", 0))
            bb_exit_fee = (bb_leg.fee if bb_leg else 0) + float(exit_stored.get("bb", {}).get("fee", 0)) if not bb_leg else bb_leg.fee
            bn_exit_fee = (bn_leg.fee if bn_leg else 0) + float(exit_stored.get("bn", {}).get("fee", 0)) if not bn_leg else bn_leg.fee

            actual_spread = self._compute_exit_spread(bb_leg, bn_leg, bb_side)
            entry = pos.get("entry", {})
            entry_spread = entry.get("actual_spread_bps", 0)
            gross_bps = entry_spread - actual_spread
            # Compute actual fees from ALL legs (entry + exit, current + prior partial)
            entry_bb_fee = float(entry.get("bb", {}).get("fee", 0))
            entry_bn_fee = float(entry.get("bn", {}).get("fee", 0))
            total_fees_usd = entry_bb_fee + entry_bn_fee + bb_exit_fee + bn_exit_fee
            fees_bps = total_fees_usd / (position_usd * 2) * 10000 if position_usd > 0 else 31.0
            net_bps = gross_bps - fees_bps
            net_usd = net_bps / 10000 * position_usd * 2

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
                    "exit.bb.exec_ids": bb_leg.exec_ids,
                    "exit.bb.state": LegStateEnum.FILLED,
                })
            if bn_leg:
                close_updates.update({
                    "exit.bn.filled_qty": bn_leg.filled_qty,
                    "exit.bn.avg_fill_price": bn_leg.avg_fill_price,
                    "exit.bn.fee": bn_leg.fee,
                    "exit.bn.fee_asset": bn_leg.fee_asset,
                    "exit.bn.exec_ids": bn_leg.exec_ids,
                    "exit.bn.state": LegStateEnum.FILLED,
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
            )

        # PARTIAL -- one or both legs unfilled
        partial_updates = {"exit.latency_ms": (time.time() - t0) * 1000}
        if bb_leg:
            partial_updates.update({
                "exit.bb.filled_qty": bb_leg.filled_qty,
                "exit.bb.avg_fill_price": bb_leg.avg_fill_price,
                "exit.bb.state": bb_leg.state,
            })
        if bn_leg:
            partial_updates.update({
                "exit.bn.filled_qty": bn_leg.filled_qty,
                "exit.bn.avg_fill_price": bn_leg.avg_fill_price,
                "exit.bn.state": bn_leg.state,
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
                latency_ms=(time.time() - t0) * 1000,
            )

        # Neither leg has ANY fills -- safe to go back to OPEN for retry
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
