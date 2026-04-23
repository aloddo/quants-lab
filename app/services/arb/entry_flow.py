"""
V2 EntryFlow — Stateless entry orchestrator.

Sequence: pre-check -> persist PENDING -> generate client IDs -> persist ENTERING
-> submit both legs -> wait fills -> transition to OPEN/UNWINDING/FAILED.

Key hardening from Eng review:
- Client IDs persisted to MongoDB BEFORE submit (submit-timeout recovery)
- Tri-state fill detection (UNKNOWN = pause, never assume unfilled)
- Unwind failure triggers RISK_PAUSE (never assume flat)
- Inventory locked atomically with PENDING creation

Designed for reuse across any cross-venue dual-leg strategy.
"""
import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

from pymongo import MongoClient
from app.services.arb.position_store import (
    PositionStore, PositionState, LegState as LegStateEnum, new_position_doc,
)
from app.services.arb.fill_detector import FillDetector, TrackedLeg
from app.services.arb.order_gateway import OrderGateway

logger = logging.getLogger(__name__)


@dataclass
class EntryResult:
    """Outcome of an entry attempt."""
    success: bool
    position_id: str = ""
    outcome: str = ""  # SUCCESS, LEG_FAILURE, BOTH_MISSED, BOTH_REJECTED, RISK_PAUSE
    bb_filled_qty: float = 0.0
    bn_filled_qty: float = 0.0
    bb_fill_price: float = 0.0
    bn_fill_price: float = 0.0
    actual_spread_bps: float = 0.0
    slippage_bps: float = 0.0
    latency_ms: float = 0.0
    unwind_pnl_usd: float = 0.0
    failure_venue: str = ""


class EntryFlow:
    """
    Stateless entry orchestrator. All state lives in PositionStore (MongoDB).

    Caller must hold the per-symbol Lock before calling execute().
    """

    FILL_TIMEOUT_S = 3.0
    UNWIND_MARKET_TIMEOUT_S = 2.0

    def __init__(
        self,
        store: PositionStore,
        detector: FillDetector,
        gateway: OrderGateway,
        ws_available: dict[str, bool] | None = None,
        shadow: bool = False,
        mongo_uri: str = "",
        max_naked_ms: float = 3000.0,
    ):
        self._store = store
        self._detector = detector
        self._gateway = gateway
        self._ws_available = ws_available or {"bybit": True, "binance": False}
        self._shadow = shadow
        self._max_naked_ms = max_naked_ms
        # Fill analytics collection
        self._analytics_coll = None
        if mongo_uri:
            try:
                db_name = mongo_uri.rsplit("/", 1)[-1]
                client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
                self._analytics_coll = client[db_name]["arb_h2_fill_analytics"]
            except Exception as e:
                logger.warning(f"EntryFlow: fill analytics disabled: {e}")

    def _log_fill_analytics(self, symbol: str, outcome: str, bb_leg: TrackedLeg = None, bn_leg: TrackedLeg = None, **extra):
        """Log entry attempt outcome for fill rate analytics."""
        if not self._analytics_coll:
            return
        try:
            doc = {
                "type": "entry",
                "symbol": symbol,
                "outcome": outcome,
                "timestamp": time.time(),
                "bb_filled": bb_leg.is_filled if bb_leg else False,
                "bb_partial": bb_leg.has_any_fill if bb_leg else False,
                "bb_is_maker": bb_leg.is_maker if bb_leg else None,
                "bb_slippage_bps": ((bb_leg.avg_fill_price - bb_leg.target_price) / bb_leg.target_price * 10000) if bb_leg and bb_leg.target_price > 0 and bb_leg.has_any_fill else None,
                "bn_filled": bn_leg.is_filled if bn_leg else False,
                "bn_partial": bn_leg.has_any_fill if bn_leg else False,
                "bn_is_maker": bn_leg.is_maker if bn_leg else None,
                "bn_slippage_bps": ((bn_leg.avg_fill_price - bn_leg.target_price) / bn_leg.target_price * 10000) if bn_leg and bn_leg.target_price > 0 and bn_leg.has_any_fill else None,
                **extra,
            }
            self._analytics_coll.insert_one(doc)
        except Exception as e:
            logger.debug(f"Fill analytics insert failed: {e}")

    async def execute(
        self,
        symbol: str,
        bn_symbol: str,
        direction: str,
        bb_side: str,
        bn_side: str,
        qty_bb: float,
        qty_bn: float,
        price_bb: float,
        price_bn: float,
        signal_spread_bps: float,
        threshold_p90: float,
        threshold_p25: float,
        bb_order_mode: str = "aggressive",
        spread_verify_fn=None,
    ) -> EntryResult:
        """
        Execute a dual-leg entry. Caller must hold per-symbol Lock.

        Returns EntryResult with outcome and fill details.
        """
        t0 = time.time()
        position_id = f"h2v2_{symbol}_{int(t0 * 1000)}"

        # Shadow mode: log the signal but do NOT create MongoDB positions or submit orders
        if self._shadow:
            logger.info(f"[SHADOW] Would enter {symbol} {direction} qty_bb={qty_bb} qty_bn={qty_bn}")
            return EntryResult(success=False, position_id=position_id, outcome="SHADOW_SKIP")

        # 1. Create PENDING position in MongoDB
        doc = new_position_doc(
            position_id=position_id,
            symbol=symbol,
            bn_symbol=bn_symbol,
            direction=direction,
            signal_spread_bps=signal_spread_bps,
            threshold_p90=threshold_p90,
            threshold_p25=threshold_p25,
        )
        if not await self._store.create(doc):
            return EntryResult(success=False, outcome="CREATE_FAILED")

        # 2. Generate client IDs and persist to ENTERING state BEFORE submit
        bb_client_id = self._gateway.generate_client_id("h2v2_bb")
        bn_client_id = self._gateway.generate_client_id("h2v2_bn")

        # Persist client IDs to MongoDB so crash recovery can lookup by them
        entry_updates = {
            "entry.bb.client_order_id": bb_client_id,
            "entry.bb.side": bb_side,
            "entry.bb.target_qty": qty_bb,
            "entry.bb.target_price": price_bb,
            "entry.bb.state": LegStateEnum.PENDING_SUBMIT,
            "entry.bn.client_order_id": bn_client_id,
            "entry.bn.side": bn_side,
            "entry.bn.target_qty": qty_bn,
            "entry.bn.target_price": price_bn,
            "entry.bn.state": LegStateEnum.PENDING_SUBMIT,
            "entry_time": t0,
        }
        if not await self._store.transition(position_id, PositionState.PENDING, PositionState.ENTERING, entry_updates):
            return EntryResult(success=False, position_id=position_id, outcome="TRANSITION_FAILED")

        # 3. Create tracked legs and register with fill detector BEFORE submit
        bb_leg = TrackedLeg(
            venue="bybit", symbol=symbol, side=bb_side,
            client_order_id=bb_client_id,
            target_qty=qty_bb, target_price=price_bb,
        )
        bn_leg = TrackedLeg(
            venue="binance", symbol=symbol, side=bn_side,
            client_order_id=bn_client_id,
            target_qty=qty_bn, target_price=price_bn,
        )
        self._detector.register_leg(bb_leg)
        self._detector.register_leg(bn_leg)

        # 4. Submit legs — sequential (PostOnly) or concurrent (IOC)
        bb_oid = None
        bn_oid = None
        bb_err = None
        bn_err = None

        if bb_order_mode == "passive":
            # SEQUENTIAL: Bybit PostOnly first, wait fill, verify spread, then Binance
            t_bb_submit = time.time()
            try:
                bb_oid = await self._gateway.submit(
                    "bybit", symbol, bb_side, qty_bb, price_bb, "limit", bb_client_id,
                    order_mode="passive",
                )
            except Exception as e:
                bb_err = e
                # PostOnly rejection: Bybit returns error if price would cross spread
                # This is a normal event, not a failure. Log and return cleanly.
                is_postonly_reject = "170213" in str(e) or "post only" in str(e).lower() or "would immediately" in str(e).lower()
                if is_postonly_reject:
                    logger.info(f"PostOnly rejected for {symbol} (price crossed spread)")
                    if not await self._store.transition(position_id, PositionState.ENTERING, PositionState.FAILED, {
                        "entry.bb.state": LegStateEnum.REJECTED,
                        "entry.bn.state": LegStateEnum.CANCELLED,
                        "entry.reject_reason": "POSTONLY_CROSSED",
                    }):
                        logger.critical(f"DB transition FAILED for {position_id} -> FAILED (PostOnly reject)")
                    self._log_fill_analytics(symbol, "POSTONLY_REJECTED", bb_leg, bn_leg,
                                             latency_ms=(time.time() - t0) * 1000)
                    self._cleanup_legs(bb_leg, bn_leg)
                    return EntryResult(success=False, position_id=position_id, outcome="POSTONLY_REJECTED",
                                       latency_ms=(time.time() - t0) * 1000)

            # CRITICAL FIX (Codex #1): If submit threw non-PostOnly error, check if order
            # was actually accepted (HTTP response lost but order placed). Without this,
            # an orphaned Bybit order sits on the exchange with no tracking.
            if bb_err and not bb_oid:
                is_postonly_reject = "170213" in str(bb_err) or "post only" in str(bb_err).lower()
                if not is_postonly_reject:
                    logger.warning(f"BB submit error (may be orphaned): {bb_err}. Checking by client_id...")
                    check = await self._detector.check_fill_definitive(
                        "bybit", symbol, client_order_id=bb_client_id,
                    )
                    if check.get("fill_result") != "NOT_FOUND" and check.get("order_id"):
                        bb_oid = check["order_id"]
                        bb_err = None  # Order exists, clear error
                        logger.info(f"Orphaned BB order found: {bb_oid}")
                        self._detector.register_exchange_id(bb_client_id, bb_oid)
                        bb_leg.apply_rest_result(check)
                    else:
                        # Order truly doesn't exist — safe to abort
                        if not await self._store.transition(position_id, PositionState.ENTERING, PositionState.FAILED, {
                            "entry.bb.state": LegStateEnum.REJECTED,
                            "entry.bn.state": LegStateEnum.CANCELLED,
                        }):
                            logger.critical(f"DB transition FAILED for {position_id} -> FAILED (BB submit error)")
                        self._cleanup_legs(bb_leg, bn_leg)
                        return EntryResult(success=False, position_id=position_id, outcome="BB_SUBMIT_FAILED",
                                           latency_ms=(time.time() - t0) * 1000)

            if bb_oid and not bb_err:
                # Register exchange ID and wait for Bybit fill
                self._detector.register_exchange_id(bb_client_id, bb_oid)
                await self._store.update_leg(position_id, "entry", "bb", {
                    "order_id": bb_oid, "state": LegStateEnum.SUBMITTED, "submitted_at": time.time(),
                })
                self._detector.replay_buffered()

                # Wait for BB fill (1.5s for PostOnly to rest on book)
                await self._detector.wait_for_fills(
                    [bb_leg], timeout=1.5, ws_available=self._ws_available,
                )

                # REST fallback if WS didn't catch it
                if not bb_leg.is_filled and bb_oid:
                    result = await self._detector.check_fill_definitive(
                        "bybit", symbol, order_id=bb_oid, client_order_id=bb_client_id,
                    )
                    bb_leg.apply_rest_result(result)

                if not bb_leg.is_filled:
                    # PostOnly didn't fill in 1.5s — cancel and abort cleanly
                    await self._gateway.cancel("bybit", symbol, bb_oid)
                    await asyncio.sleep(0.2)
                    # Recheck after cancel
                    result = await self._detector.check_fill_definitive(
                        "bybit", symbol, order_id=bb_oid, client_order_id=bb_client_id,
                    )
                    bb_leg.apply_rest_result(result)

                    if not bb_leg.has_any_fill:
                        # No fills at all — clean abort
                        if not await self._store.transition(position_id, PositionState.ENTERING, PositionState.FAILED, {
                            "entry.bb.state": LegStateEnum.CANCELLED,
                            "entry.bn.state": LegStateEnum.CANCELLED,
                        }):
                            logger.critical(f"DB transition FAILED for {position_id} -> FAILED (PostOnly timeout)")
                        self._log_fill_analytics(symbol, "POSTONLY_TIMEOUT", bb_leg, bn_leg,
                                                 latency_ms=(time.time() - t0) * 1000)
                        self._cleanup_legs(bb_leg, bn_leg)
                        return EntryResult(success=False, position_id=position_id, outcome="POSTONLY_TIMEOUT",
                                           latency_ms=(time.time() - t0) * 1000)
                    else:
                        # CRITICAL FIX (Opus C2): Partial fill on BB — must unwind, not continue.
                        # Continuing would submit BN for full hedge_qty while BB only has partial,
                        # creating an unhedged position.
                        logger.warning(f"PostOnly partial fill on BB ({bb_leg.filled_qty}/{bb_leg.target_qty}), unwinding")
                        if not await self._store.transition(position_id, PositionState.ENTERING, PositionState.UNWINDING, {
                            "entry.bb.filled_qty": bb_leg.filled_qty,
                            "entry.bb.avg_fill_price": bb_leg.avg_fill_price,
                            "entry.bb.state": LegStateEnum.PARTIAL,
                        }):
                            logger.critical(f"DB transition FAILED for {position_id} -> UNWINDING (PostOnly partial)")
                        unwind_pnl, unwind_ok = await self._emergency_unwind(bb_leg, symbol, position_id)
                        self._log_fill_analytics(symbol, "POSTONLY_PARTIAL_UNWIND", bb_leg, bn_leg,
                                                 latency_ms=(time.time() - t0) * 1000,
                                                 unwind_pnl_usd=unwind_pnl)
                        self._cleanup_legs(bb_leg, bn_leg)
                        outcome = "RISK_PAUSE" if not unwind_ok else "POSTONLY_PARTIAL"
                        return EntryResult(success=False, position_id=position_id, outcome=outcome,
                                           failure_venue="bybit_partial", unwind_pnl_usd=unwind_pnl,
                                           latency_ms=(time.time() - t0) * 1000)

                # BB fully filled! Persist fill data BEFORE BN submit (crash recovery needs this)
                await self._store.update_leg(position_id, "entry", "bb", {
                    "filled_qty": bb_leg.filled_qty,
                    "avg_fill_price": bb_leg.avg_fill_price,
                    "fee": bb_leg.fee,
                    "state": LegStateEnum.FILLED,
                    "filled_at": bb_leg.filled_at,
                })

                # Naked-leg SLO: check elapsed time since BB fill
                bb_fill_ms = (time.time() - t_bb_submit) * 1000
                if bb_fill_ms > self._max_naked_ms:
                    logger.warning(f"Naked-leg SLO breached: {bb_fill_ms:.0f}ms > {self._max_naked_ms}ms, unwinding BB")
                    if not await self._store.transition(position_id, PositionState.ENTERING, PositionState.UNWINDING, {
                        "entry.bb.filled_qty": bb_leg.filled_qty,
                        "entry.bb.state": LegStateEnum.FILLED,
                    }):
                        pass
                    unwind_pnl, unwind_ok = await self._emergency_unwind(bb_leg, symbol, position_id)
                    self._log_fill_analytics(symbol, "NAKED_LEG_SLO", bb_leg, bn_leg,
                                             latency_ms=(time.time() - t0) * 1000, naked_ms=bb_fill_ms)
                    self._cleanup_legs(bb_leg, bn_leg)
                    outcome = "RISK_PAUSE" if not unwind_ok else "NAKED_LEG_SLO"
                    return EntryResult(success=False, position_id=position_id, outcome=outcome,
                                       failure_venue="naked_leg_timeout", unwind_pnl_usd=unwind_pnl,
                                       latency_ms=(time.time() - t0) * 1000)

                # Verify spread is still alive before submitting BN
                if spread_verify_fn:
                    try:
                        spread_ok = spread_verify_fn(symbol, signal_spread_bps, threshold_p90)
                    except Exception as e:
                        logger.error(f"Spread verify crashed for {symbol}: {e} — treating as collapsed")
                        spread_ok = False
                    if not spread_ok:
                        # Spread collapsed — unwind BB and abort
                        logger.warning(f"Spread collapsed after BB fill for {symbol}, unwinding")
                        if not await self._store.transition(position_id, PositionState.ENTERING, PositionState.UNWINDING, {
                            "entry.bb.filled_qty": bb_leg.filled_qty,
                            "entry.bb.avg_fill_price": bb_leg.avg_fill_price,
                            "entry.bb.state": LegStateEnum.FILLED,
                        }):
                            logger.critical(f"DB transition FAILED for {position_id} -> UNWINDING (spread collapsed)")
                        unwind_pnl, unwind_ok = await self._emergency_unwind(bb_leg, symbol, position_id)
                        self._log_fill_analytics(symbol, "SPREAD_COLLAPSED", bb_leg, bn_leg,
                                                 latency_ms=(time.time() - t0) * 1000,
                                                 unwind_pnl_usd=unwind_pnl)
                        self._cleanup_legs(bb_leg, bn_leg)
                        outcome = "RISK_PAUSE" if not unwind_ok else "SPREAD_COLLAPSED"
                        return EntryResult(success=False, position_id=position_id, outcome=outcome,
                                           failure_venue="bybit_unwind", unwind_pnl_usd=unwind_pnl,
                                           latency_ms=(time.time() - t0) * 1000)

                # Submit BN leg (GTC, aggressive — same as before)
                t_bn_submit = time.time()
                try:
                    bn_oid = await self._gateway.submit("binance", symbol, bn_side, qty_bn, price_bn, "limit", bn_client_id)
                except Exception as e:
                    bn_err = e
                    # BN submit failed after BB filled — must unwind BB
                    logger.error(f"BN submit failed after BB fill: {e}")

                if bn_oid:
                    self._detector.register_exchange_id(bn_client_id, bn_oid)
                    await self._store.update_leg(position_id, "entry", "bn", {
                        "order_id": bn_oid, "state": LegStateEnum.SUBMITTED, "submitted_at": time.time(),
                    })

            # If BB failed (non-PostOnly error), fall through to existing error handling below

        else:
            # CONCURRENT: Original IOC behavior — both legs at once
            async def submit_bb():
                nonlocal bb_oid, bb_err
                try:
                    bb_oid = await self._gateway.submit("bybit", symbol, bb_side, qty_bb, price_bb, "limit", bb_client_id)
                except Exception as e:
                    bb_err = e

            async def submit_bn():
                nonlocal bn_oid, bn_err
                try:
                    bn_oid = await self._gateway.submit("binance", symbol, bn_side, qty_bn, price_bn, "limit", bn_client_id)
                except Exception as e:
                    bn_err = e

            await asyncio.gather(submit_bb(), submit_bn())

        # Handle both submit failures
        if bb_err and bn_err:
            logger.error(f"Both submits failed: bb={bb_err}, bn={bn_err}")
            # Before marking REJECTED, check if orders were actually accepted
            # (submit may have succeeded but HTTP response was lost)
            # Track which orders actually exist on exchange (accepted but possibly unfilled)
            bb_exists = False
            bn_exists = False
            for leg, cid, venue, exists_flag in [
                (bb_leg, bb_client_id, "bybit", "bb"),
                (bn_leg, bn_client_id, "binance", "bn"),
            ]:
                result = await self._detector.check_fill_definitive(venue, symbol, client_order_id=cid)
                leg.apply_rest_result(result)
                # NOT_FOUND = order never existed. Anything else = order exists on exchange.
                order_exists = result.get("fill_result") != "NOT_FOUND" and result.get("order_id", "")
                if exists_flag == "bb":
                    bb_exists = order_exists
                    if order_exists and result.get("order_id"):
                        bb_oid = result["order_id"]
                        self._detector.register_exchange_id(cid, bb_oid)
                else:
                    bn_exists = order_exists
                    if order_exists and result.get("order_id"):
                        bn_oid = result["order_id"]
                        self._detector.register_exchange_id(cid, bn_oid)

            # Cancel any accepted-but-unfilled orders before proceeding
            if bb_exists and not bb_leg.has_any_fill and bb_oid:
                await self._gateway.cancel("bybit", symbol, bb_oid)
            if bn_exists and not bn_leg.has_any_fill and bn_oid:
                await self._gateway.cancel("binance", symbol, bn_oid)

            # If either leg actually got accepted/filled, handle as asymmetric failure
            if bb_leg.has_any_fill and bn_leg.has_any_fill:
                # Both actually filled despite submit errors — treat as success path
                # (fall through to normal fill evaluation below)
                pass
            elif bb_leg.has_any_fill or bn_leg.has_any_fill:
                # One side actually filled — unwind it
                await self._store.transition(position_id, PositionState.ENTERING, PositionState.UNWINDING, {
                    "entry.bb.filled_qty": bb_leg.filled_qty,
                    "entry.bb.state": LegStateEnum.FILLED if bb_leg.is_filled else bb_leg.state,
                    "entry.bn.filled_qty": bn_leg.filled_qty,
                    "entry.bn.state": LegStateEnum.FILLED if bn_leg.is_filled else bn_leg.state,
                })
                filled_leg = bb_leg if bb_leg.has_any_fill else bn_leg
                failure_venue = "binance" if bb_leg.has_any_fill else "bybit"
                unwind_pnl, unwind_ok = await self._emergency_unwind(filled_leg, symbol, position_id)
                self._cleanup_legs(bb_leg, bn_leg)
                if not unwind_ok:
                    return EntryResult(success=False, position_id=position_id, outcome="RISK_PAUSE",
                                       failure_venue=failure_venue, unwind_pnl_usd=unwind_pnl,
                                       latency_ms=(time.time() - t0) * 1000)
                return EntryResult(success=False, position_id=position_id, outcome="LEG_FAILURE",
                                   failure_venue=failure_venue, unwind_pnl_usd=unwind_pnl,
                                   latency_ms=(time.time() - t0) * 1000)
            else:
                # Truly neither accepted — safe to reject
                if not await self._store.transition(position_id, PositionState.ENTERING, PositionState.FAILED, {
                    "entry.bb.state": LegStateEnum.REJECTED,
                    "entry.bn.state": LegStateEnum.REJECTED,
                }):
                    logger.critical(f"DB transition FAILED for {position_id} -> FAILED (both rejected)")
                    self._cleanup_legs(bb_leg, bn_leg)
                    return EntryResult(success=False, position_id=position_id, outcome="DB_FAILURE",
                                       latency_ms=(time.time() - t0) * 1000)
                self._cleanup_legs(bb_leg, bn_leg)
                return EntryResult(success=False, position_id=position_id, outcome="BOTH_REJECTED",
                                   latency_ms=(time.time() - t0) * 1000)

        # Handle asymmetric submit failure
        if bb_err and bn_oid:
            # Check if the "failed" side actually got accepted
            bb_check = await self._detector.check_fill_definitive("bybit", symbol, client_order_id=bb_client_id)
            bb_leg.apply_rest_result(bb_check)
            if bb_leg.has_any_fill:
                # Failed side actually filled — register the success side and continue to fill evaluation
                bb_oid = bb_check.get("order_id", "")
                if bb_oid:
                    self._detector.register_exchange_id(bb_client_id, bb_oid)
                self._detector.register_exchange_id(bn_client_id, bn_oid)
                bn_leg.order_id = bn_oid
                bn_leg.state = LegStateEnum.SUBMITTED
            else:
                return await self._handle_one_submit_failed(
                    position_id, "bybit", bb_leg, bn_leg, bn_oid, bn_client_id,
                    symbol, t0, bb_err,
                )
        if bn_err and bb_oid:
            # Check if the "failed" side actually got accepted
            bn_check = await self._detector.check_fill_definitive("binance", symbol, client_order_id=bn_client_id)
            bn_leg.apply_rest_result(bn_check)
            if bn_leg.has_any_fill:
                # Failed side actually filled — register the success side and continue to fill evaluation
                bn_oid = bn_check.get("order_id", "")
                if bn_oid:
                    self._detector.register_exchange_id(bn_client_id, bn_oid)
                self._detector.register_exchange_id(bb_client_id, bb_oid)
                bb_leg.order_id = bb_oid
                bb_leg.state = LegStateEnum.SUBMITTED
            else:
                return await self._handle_one_submit_failed(
                    position_id, "binance", bn_leg, bb_leg, bb_oid, bb_client_id,
                    symbol, t0, bn_err,
                )

        # Both submits succeeded -- register exchange IDs
        if bb_oid:
            self._detector.register_exchange_id(bb_client_id, bb_oid)
            await self._store.update_leg(position_id, "entry", "bb", {
                "order_id": bb_oid, "state": LegStateEnum.SUBMITTED, "submitted_at": time.time(),
            })
        if bn_oid:
            self._detector.register_exchange_id(bn_client_id, bn_oid)
            await self._store.update_leg(position_id, "entry", "bn", {
                "order_id": bn_oid, "state": LegStateEnum.SUBMITTED, "submitted_at": time.time(),
            })

        # Replay any buffered fills
        self._detector.replay_buffered()

        # 5. Wait for fills (WS + REST as appropriate)
        await self._detector.wait_for_fills(
            [bb_leg, bn_leg],
            timeout=self.FILL_TIMEOUT_S,
            ws_available=self._ws_available,
        )

        # 6. REST fallback for any unfilled legs
        for leg, oid, cid in [(bb_leg, bb_oid, bb_client_id), (bn_leg, bn_oid, bn_client_id)]:
            if not leg.is_filled and oid:
                result = await self._detector.check_fill_definitive(
                    leg.venue, symbol, order_id=oid, client_order_id=cid,
                )
                leg.apply_rest_result(result)

        # 7. Cancel unfilled legs
        if not bb_leg.is_filled and bb_oid:
            await self._gateway.cancel("bybit", symbol, bb_oid)
            await asyncio.sleep(0.2)  # Wait for cancel propagation
        if not bn_leg.is_filled and bn_oid:
            await self._gateway.cancel("binance", symbol, bn_oid)
            await asyncio.sleep(0.2)

        # Re-check after cancel (fill-after-cancel is possible)
        for leg, oid, cid in [(bb_leg, bb_oid, bb_client_id), (bn_leg, bn_oid, bn_client_id)]:
            if not leg.is_filled and oid:
                result = await self._detector.check_fill_definitive(
                    leg.venue, symbol, order_id=oid, client_order_id=cid,
                )
                leg.apply_rest_result(result)

        # 7b. Enrich Binance fee data (REST get_order doesn't return fees)
        if bn_leg.has_any_fill:
            await self._detector.enrich_leg_fee(bn_leg)

        # 8. Evaluate outcome
        bb_filled = bb_leg.is_filled
        bn_filled = bn_leg.is_filled

        if bb_filled and bn_filled:
            # SUCCESS
            actual_spread = self._compute_spread(bb_leg, bn_leg, bb_side)
            if not await self._store.transition(position_id, PositionState.ENTERING, PositionState.OPEN, {
                "entry.bb.filled_qty": bb_leg.filled_qty,
                "entry.bb.avg_fill_price": bb_leg.avg_fill_price,
                "entry.bb.fee": bb_leg.fee,
                "entry.bb.fee_asset": bb_leg.fee_asset,
                "entry.bb.is_maker": bb_leg.is_maker,
                "entry.bb.exec_ids": bb_leg.exec_ids,
                "entry.bb.state": LegStateEnum.FILLED,
                "entry.bb.filled_at": bb_leg.filled_at,
                "entry.bb.slippage_bps": ((bb_leg.avg_fill_price - bb_leg.target_price) / bb_leg.target_price * 10000) if bb_leg.target_price > 0 else 0,
                "entry.bn.filled_qty": bn_leg.filled_qty,
                "entry.bn.avg_fill_price": bn_leg.avg_fill_price,
                "entry.bn.fee": bn_leg.fee,
                "entry.bn.fee_asset": bn_leg.fee_asset,
                "entry.bn.is_maker": bn_leg.is_maker,
                "entry.bn.exec_ids": bn_leg.exec_ids,
                "entry.bn.state": LegStateEnum.FILLED,
                "entry.bn.filled_at": bn_leg.filled_at,
                "entry.bn.slippage_bps": ((bn_leg.avg_fill_price - bn_leg.target_price) / bn_leg.target_price * 10000) if bn_leg.target_price > 0 else 0,
                "entry.actual_spread_bps": actual_spread,
                "entry.slippage_bps": signal_spread_bps - actual_spread,
                "entry.latency_ms": (time.time() - t0) * 1000,
            }):
                logger.critical(f"DB transition FAILED for {position_id} -> OPEN (both filled)")
                self._cleanup_legs(bb_leg, bn_leg)
                return EntryResult(success=False, position_id=position_id, outcome="DB_FAILURE",
                                   latency_ms=(time.time() - t0) * 1000)
            self._log_fill_analytics(symbol, "SUCCESS", bb_leg, bn_leg,
                                     latency_ms=(time.time() - t0) * 1000,
                                     signal_spread_bps=signal_spread_bps,
                                     actual_spread_bps=actual_spread)
            self._cleanup_legs(bb_leg, bn_leg)
            return EntryResult(
                success=True, position_id=position_id, outcome="SUCCESS",
                bb_filled_qty=bb_leg.filled_qty, bn_filled_qty=bn_leg.filled_qty,
                bb_fill_price=bb_leg.avg_fill_price, bn_fill_price=bn_leg.avg_fill_price,
                actual_spread_bps=actual_spread,
                slippage_bps=signal_spread_bps - actual_spread,
                latency_ms=(time.time() - t0) * 1000,
            )

        if bb_leg.has_any_fill or bn_leg.has_any_fill:
            # One or both have fills -- unwind ALL filled legs
            # CRITICAL FIX: Claude review #2 — must unwind BOTH if both have partial fills
            if not await self._store.transition(position_id, PositionState.ENTERING, PositionState.UNWINDING, {
                "entry.bb.filled_qty": bb_leg.filled_qty,
                "entry.bb.state": LegStateEnum.FILLED if bb_leg.is_filled else bb_leg.state,
                "entry.bn.filled_qty": bn_leg.filled_qty,
                "entry.bn.state": LegStateEnum.FILLED if bn_leg.is_filled else bn_leg.state,
            }):
                logger.critical(f"DB transition FAILED for {position_id} -> UNWINDING")
                self._cleanup_legs(bb_leg, bn_leg)
                return EntryResult(success=False, position_id=position_id, outcome="DB_FAILURE",
                                   latency_ms=(time.time() - t0) * 1000)

            unwind_pnl = 0.0
            all_unwinds_ok = True
            failure_venue = ""
            if bb_leg.has_any_fill and bn_leg.has_any_fill:
                # Both have fills — unwind BOTH (not just one!)
                failure_venue = "both_partial"
                pnl1, ok1 = await self._emergency_unwind(bb_leg, symbol, position_id)
                pnl2, ok2 = await self._emergency_unwind(bn_leg, symbol, position_id)
                unwind_pnl = pnl1 + pnl2
                all_unwinds_ok = ok1 and ok2
            elif bb_leg.has_any_fill:
                failure_venue = "binance"
                unwind_pnl, all_unwinds_ok = await self._emergency_unwind(bb_leg, symbol, position_id)
            else:
                failure_venue = "bybit"
                unwind_pnl, all_unwinds_ok = await self._emergency_unwind(bn_leg, symbol, position_id)

            outcome = "RISK_PAUSE" if not all_unwinds_ok else "LEG_FAILURE"
            self._log_fill_analytics(symbol, outcome, bb_leg, bn_leg,
                                     latency_ms=(time.time() - t0) * 1000,
                                     failure_venue=failure_venue,
                                     unwind_pnl_usd=unwind_pnl)
            self._cleanup_legs(bb_leg, bn_leg)
            return EntryResult(
                success=False, position_id=position_id, outcome=outcome,
                failure_venue=failure_venue, unwind_pnl_usd=unwind_pnl,
                latency_ms=(time.time() - t0) * 1000,
            )

        # Neither has any fill
        if not await self._store.transition(position_id, PositionState.ENTERING, PositionState.FAILED, {
            "entry.bb.state": bb_leg.state,
            "entry.bn.state": bn_leg.state,
        }):
            logger.critical(f"DB transition FAILED for {position_id} -> FAILED (both missed)")
            self._cleanup_legs(bb_leg, bn_leg)
            return EntryResult(success=False, position_id=position_id, outcome="DB_FAILURE",
                               latency_ms=(time.time() - t0) * 1000)
        self._log_fill_analytics(symbol, "BOTH_MISSED", bb_leg, bn_leg,
                                 latency_ms=(time.time() - t0) * 1000,
                                 signal_spread_bps=signal_spread_bps)
        self._cleanup_legs(bb_leg, bn_leg)
        return EntryResult(
            success=False, position_id=position_id, outcome="BOTH_MISSED",
            latency_ms=(time.time() - t0) * 1000,
        )

    async def _emergency_unwind(self, leg: TrackedLeg, symbol: str, position_id: str) -> tuple[float, bool]:
        """
        Close a filled leg that has no counterpart.
        Goes straight to market. On failure, triggers RISK_PAUSE (never assume flat).

        Returns (pnl: float, success: bool). On timeout/exception, returns (0.0, False).
        The caller must check success and trigger RISK_PAUSE if False.
        """
        logger.warning(f"EMERGENCY UNWIND {leg.venue} {symbol} {leg.side} qty={leg.filled_qty}")
        unwind_side = "Sell" if leg.side == "Buy" else "Buy"

        try:
            # FIX #2: Generate client ID and persist BEFORE submit so crash recovery can query
            unwind_client_id = self._gateway.generate_client_id("h2v2_unw")
            await self._store.update_leg(position_id, "unwind", "", {
                "client_order_id": unwind_client_id,
                "side": unwind_side,
                "filled_qty": 0,  # Not yet filled
            })

            oid = await self._gateway.submit(
                leg.venue, symbol, unwind_side,
                leg.filled_qty, 0, "market", unwind_client_id,
            )

            # Persist exchange order ID immediately after submit
            await self._store.update_leg(position_id, "unwind", "", {
                "order_id": oid,
            })

            unwind_leg = TrackedLeg(
                venue=leg.venue, symbol=symbol, side=unwind_side,
                order_id=oid, client_order_id=unwind_client_id,
                target_qty=leg.filled_qty,
                state=LegStateEnum.SUBMITTED, submitted_at=time.time(),
            )
            self._detector.register_leg(unwind_leg)

            await self._detector.wait_for_fills(
                [unwind_leg], timeout=self.UNWIND_MARKET_TIMEOUT_S,
                ws_available=self._ws_available,
            )

            if not unwind_leg.is_filled:
                # REST fallback
                result = await self._detector.check_fill_definitive(
                    leg.venue, symbol, order_id=oid,
                )
                unwind_leg.apply_rest_result(result)

            if unwind_leg.is_filled:
                loss = (unwind_leg.avg_fill_price - leg.avg_fill_price) * leg.filled_qty
                if leg.side == "Sell":
                    loss = -loss
                if not await self._store.transition(position_id, PositionState.UNWINDING, PositionState.FAILED, {
                    "unwind.order_id": oid,
                    "unwind.side": unwind_side,
                    "unwind.filled_qty": unwind_leg.filled_qty,
                    "unwind.avg_fill_price": unwind_leg.avg_fill_price,
                    "unwind.pnl_usd": loss,
                }):
                    logger.critical(f"DB transition FAILED for {position_id} -> FAILED (unwind complete)")
                    self._detector.cleanup_leg(unwind_leg)
                    return (loss, False)
                self._detector.cleanup_leg(unwind_leg)
                logger.warning(f"Unwind complete: loss=${loss:.4f}")
                return (loss, True)

            # UNWIND FAILED -- this is the catastrophic case
            # Do NOT assume flat. Do NOT release inventory.
            logger.critical(
                f"EMERGENCY UNWIND FAILED {leg.venue} {symbol} — NAKED LEG OPEN. "
                f"RISK_PAUSE required. Manual intervention needed."
            )
            self._detector.cleanup_leg(unwind_leg)
            return (0.0, False)

        except Exception as e:
            logger.critical(f"EMERGENCY UNWIND EXCEPTION {leg.venue} {symbol}: {e}")
            return (0.0, False)

    async def _handle_one_submit_failed(
        self, position_id, failed_venue, failed_leg, success_leg, success_oid,
        success_client_id, symbol, t0, error,
    ):
        """Handle case where one submit succeeded and the other failed."""
        logger.warning(f"{failed_venue} submit failed ({error}), unwinding {success_leg.venue}")

        self._detector.register_exchange_id(success_client_id, success_oid)
        success_leg.order_id = success_oid
        success_leg.state = LegStateEnum.SUBMITTED

        # Cancel the successful order — HIGH FIX (Codex C2 #3): wrap in try/except
        try:
            await self._gateway.cancel(success_leg.venue, symbol, success_oid)
        except Exception as cancel_err:
            logger.warning(f"Cancel failed for {success_leg.venue} {success_oid}: {cancel_err}")
        await asyncio.sleep(0.2)

        # Check if it filled before cancel
        result = await self._detector.check_fill_definitive(
            success_leg.venue, symbol, order_id=success_oid, client_order_id=success_client_id,
        )
        success_leg.apply_rest_result(result)

        if success_leg.has_any_fill:
            # Transition to UNWINDING
            if not await self._store.transition(position_id, PositionState.ENTERING, PositionState.UNWINDING, {}):
                logger.critical(f"DB transition FAILED for {position_id} -> UNWINDING (one submit failed)")
                self._cleanup_legs(failed_leg, success_leg)
                return EntryResult(success=False, position_id=position_id, outcome="DB_FAILURE",
                                   latency_ms=(time.time() - t0) * 1000)
            unwind_pnl, unwind_ok = await self._emergency_unwind(success_leg, symbol, position_id)
            self._cleanup_legs(failed_leg, success_leg)
            outcome = "RISK_PAUSE" if not unwind_ok else "LEG_FAILURE"
            return EntryResult(
                success=False, position_id=position_id, outcome=outcome,
                failure_venue=failed_venue, unwind_pnl_usd=unwind_pnl,
                latency_ms=(time.time() - t0) * 1000,
            )

        # Success leg didn't fill -- clean exit
        if not await self._store.transition(position_id, PositionState.ENTERING, PositionState.FAILED, {
            f"entry.{'bb' if failed_venue == 'bybit' else 'bn'}.state": LegStateEnum.REJECTED,
        }):
            logger.critical(f"DB transition FAILED for {position_id} -> FAILED (one submit failed, no fill)")
            self._cleanup_legs(failed_leg, success_leg)
            return EntryResult(success=False, position_id=position_id, outcome="DB_FAILURE",
                               latency_ms=(time.time() - t0) * 1000)
        self._cleanup_legs(failed_leg, success_leg)
        return EntryResult(
            success=False, position_id=position_id, outcome="LEG_FAILURE",
            failure_venue=failed_venue,
            latency_ms=(time.time() - t0) * 1000,
        )

    def _compute_spread(self, bb_leg: TrackedLeg, bn_leg: TrackedLeg, bb_side: str) -> float:
        """Compute actual spread from fill prices (in bps)."""
        if bb_leg.avg_fill_price <= 0 or bn_leg.avg_fill_price <= 0:
            return 0.0
        if bb_side == "Buy":
            return (bn_leg.avg_fill_price - bb_leg.avg_fill_price) / bb_leg.avg_fill_price * 10000
        else:
            return (bb_leg.avg_fill_price - bn_leg.avg_fill_price) / bn_leg.avg_fill_price * 10000

    def _cleanup_legs(self, *legs):
        """Remove legs from fill detector tracking."""
        for leg in legs:
            self._detector.cleanup_leg(leg)
