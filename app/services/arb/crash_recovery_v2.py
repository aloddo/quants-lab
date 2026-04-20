"""
V2 CrashRecovery — State-machine-aware startup reconciliation.

Reads all non-terminal positions from PositionStore, queries exchange for
actual order/position state, and resolves each to a terminal or resumable state.

Key improvement over V1: uses PositionStore states + persisted order IDs/client IDs
to query exchange definitively via get_order(). No reliance on in-memory state.

Returns a list of deferred actions (orphan buybacks) that the orchestrator
executes AFTER feeds and instrument rules are loaded (Codex #13 fix).
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field

import aiohttp

from app.services.arb.position_store import PositionStore, PositionState, LegState as LegStateEnum
from app.services.arb.order_gateway import OrderGateway
from app.services.arb.inventory_ledger import InventoryLedger

logger = logging.getLogger(__name__)


@dataclass
class RecoveryReport:
    """Results of V2 crash recovery."""
    positions_resolved: int = 0
    positions_resumed: int = 0
    orphans_closed: int = 0
    orders_cancelled: int = 0
    inventory_reconciled: int = 0
    errors: list = field(default_factory=list)
    deferred_actions: list = field(default_factory=list)  # executed AFTER feeds loaded

    @property
    def clean(self) -> bool:
        return self.orphans_closed == 0 and len(self.errors) == 0


class CrashRecoveryV2:
    """
    State-machine-aware crash recovery.

    For each non-terminal position:
    - PENDING: should never exist (created + immediately transitioned). Mark FAILED.
    - ENTERING: query both entry orders by client_order_id. Resolve to OPEN/FAILED.
    - OPEN: verify Bybit position exists. Resume for exit monitoring.
    - EXITING: query both exit orders. Resolve to CLOSED/PARTIAL_EXIT.
    - PARTIAL_EXIT: query unfilled exit leg. Complete or market-escalate.
    - UNWINDING: query unwind order. Resolve to FAILED.
    """

    def __init__(
        self,
        store: PositionStore,
        gateway: OrderGateway,
        inventory: InventoryLedger | None = None,
    ):
        self._store = store
        self._gateway = gateway
        self._inventory = inventory

    async def recover(self, symbols: list[str]) -> RecoveryReport:
        """Full recovery sequence. Call on startup BEFORE main loop."""
        report = RecoveryReport()
        async with self._store.recovery_lock:
            logger.info("=" * 40)
            logger.info("V2 CRASH RECOVERY starting...")

            # Step 1: Resolve all non-terminal positions
            active = await self._store.get_active()
            logger.info(f"Found {len(active)} non-terminal positions")

            for pos in active:
                try:
                    await self._resolve_position(pos, report)
                except Exception as e:
                    pid = pos.get("position_id", "?")
                    logger.error(f"Recovery failed for {pid}: {e}", exc_info=True)
                    report.errors.append(f"{pid}: {e}")

            # Step 2: Check for orphan Bybit positions (on exchange, not in DB)
            await self._check_orphans(symbols, report)

            # Step 3: Cancel all stale orders on both exchanges
            await self._cancel_stale_orders(symbols, report)

            # Step 4: Reconcile inventory
            if self._inventory:
                self._inventory.release_stale_locks(set())
                report.inventory_reconciled += 1

            logger.info(f"V2 CRASH RECOVERY complete: {report}")
            logger.info("=" * 40)
        return report

    async def _resolve_position(self, pos: dict, report: RecoveryReport):
        """Resolve a single non-terminal position by querying exchange state."""
        pid = pos["position_id"]
        state = pos["state"]
        symbol = pos["symbol"]

        logger.info(f"Resolving {pid}: state={state} symbol={symbol}")

        if state == PositionState.PENDING:
            # Should never exist -- mark FAILED
            await self._store.transition(pid, PositionState.PENDING, PositionState.FAILED, {
                "exit.reason": "RECOVERY_STALE_PENDING",
            })
            report.positions_resolved += 1

        elif state == PositionState.ENTERING:
            await self._resolve_entering(pos, report)

        elif state == PositionState.OPEN:
            await self._resolve_open(pos, report)

        elif state == PositionState.EXITING:
            await self._resolve_exiting(pos, report)

        elif state == PositionState.PARTIAL_EXIT:
            await self._resolve_partial_exit(pos, report)

        elif state == PositionState.UNWINDING:
            await self._resolve_unwinding(pos, report)

    async def _resolve_entering(self, pos: dict, report: RecoveryReport):
        """Resolve ENTERING: query both entry orders, determine if we have fills."""
        pid = pos["position_id"]
        symbol = pos["symbol"]
        entry = pos.get("entry", {})

        bb_info = await self._query_leg(entry.get("bb", {}), "bybit", symbol)
        bn_info = await self._query_leg(entry.get("bn", {}), "binance", symbol)

        # PARTIAL counts as exposure-bearing (same as FILLED for recovery purposes)
        bb_has_fill = bb_info.get("fill_result") in ("FILLED", "PARTIAL")
        bn_has_fill = bn_info.get("fill_result") in ("FILLED", "PARTIAL")

        bb_unknown = bb_info.get("fill_result") == "UNKNOWN"
        bn_unknown = bn_info.get("fill_result") == "UNKNOWN"

        # CRITICAL FIX (both reviews): NEVER make terminal decisions on UNKNOWN state.
        # If either leg is UNKNOWN, we cannot safely determine exchange state.
        if bb_unknown or bn_unknown:
            logger.critical(
                f"ENTERING recovery UNRESOLVED: {pid} bb={bb_info.get('fill_result')} "
                f"bn={bn_info.get('fill_result')} — MANUAL INTERVENTION REQUIRED"
            )
            report.errors.append(
                f"entering_unresolved_{pid}: bb={bb_info.get('fill_result')} bn={bn_info.get('fill_result')}"
            )
            return  # Leave in ENTERING state — do NOT mark FAILED or cancel orders

        if bb_has_fill and bn_has_fill:
            # Both have fills — check if BOTH are fully filled before going to OPEN
            bb_target = float(entry.get("bb", {}).get("target_qty", 0))
            bn_target = float(entry.get("bn", {}).get("target_qty", 0))
            bb_filled_qty = float(bb_info.get("filled_qty", 0))
            bn_filled_qty = float(bn_info.get("filled_qty", 0))
            bb_fully = bb_info.get("fill_result") == "FILLED" and (bb_target == 0 or bb_filled_qty >= bb_target * 0.99)
            bn_fully = bn_info.get("fill_result") == "FILLED" and (bn_target == 0 or bn_filled_qty >= bn_target * 0.99)

            if bb_fully and bn_fully:
                # Both fully filled -- safe to go OPEN
                await self._store.transition(pid, PositionState.ENTERING, PositionState.OPEN, {
                    "entry.bb.filled_qty": bb_filled_qty,
                    "entry.bb.avg_fill_price": float(bb_info.get("avg_price", 0)),
                    "entry.bb.state": LegStateEnum.FILLED,
                    "entry.bn.filled_qty": bn_filled_qty,
                    "entry.bn.avg_fill_price": float(bn_info.get("avg_price", 0)),
                    "entry.bn.state": LegStateEnum.FILLED,
                })
                report.positions_resumed += 1
                logger.info(f"ENTERING -> OPEN: {pid} (both legs fully filled)")
            else:
                # FIX (Round 4 #2): One or both legs are only PARTIAL.
                # Live EntryFlow sends partial-entry to UNWINDING, not OPEN.
                # Unwind BOTH legs to avoid naked exposure from mismatched partials.
                logger.warning(
                    f"ENTERING recovery: {pid} both legs have fills but not fully filled "
                    f"(bb={bb_info.get('fill_result')} {bb_filled_qty}/{bb_target}, "
                    f"bn={bn_info.get('fill_result')} {bn_filled_qty}/{bn_target}). "
                    f"Unwinding both legs."
                )
                await self._store.transition(pid, PositionState.ENTERING, PositionState.UNWINDING, {})
                unwind_errors = []
                for venue, leg_key, filled_qty in [("bybit", "bb", bb_filled_qty), ("binance", "bn", bn_filled_qty)]:
                    if filled_qty <= 0:
                        continue
                    filled_side = entry.get(leg_key, {}).get("side", "Buy")
                    unwind_side = "Sell" if filled_side == "Buy" else "Buy"
                    try:
                        oid = await self._gateway.submit(venue, symbol, unwind_side, filled_qty, 0, "market")
                        await asyncio.sleep(2)
                        result = await self._gateway.get_order_with_retry(venue, symbol, order_id=oid)
                        if result.get("fill_result") != "FILLED":
                            unwind_errors.append(f"{venue}_unwind_unconfirmed")
                            logger.critical(f"Recovery unwind NOT CONFIRMED for {pid} on {venue}")
                    except Exception as e:
                        unwind_errors.append(f"{venue}_unwind: {e}")
                        logger.critical(f"Recovery unwind FAILED for {pid} on {venue}: {e}")

                if unwind_errors:
                    report.errors.extend([f"{pid}_{err}" for err in unwind_errors])
                else:
                    await self._store.transition(pid, PositionState.UNWINDING, PositionState.FAILED, {
                        "exit.reason": "RECOVERY_PARTIAL_ENTRY_UNWOUND",
                    })
                report.positions_resolved += 1

        elif bb_has_fill or bn_has_fill:
            # One has fills, other definitively NOT_FILLED -- unwind the filled leg
            filled_venue = "bybit" if bb_has_fill else "binance"
            filled_info = bb_info if bb_has_fill else bn_info
            filled_qty = float(filled_info.get("filled_qty", 0))
            filled_side = entry.get("bb" if bb_has_fill else "bn", {}).get("side", "Buy")
            unwind_side = "Sell" if filled_side == "Buy" else "Buy"

            logger.warning(f"ENTERING recovery: {pid} {filled_venue} filled, other NOT_FILLED, unwinding")
            await self._store.transition(pid, PositionState.ENTERING, PositionState.UNWINDING, {})

            try:
                oid = await self._gateway.submit(filled_venue, symbol, unwind_side, filled_qty, 0, "market")
                await asyncio.sleep(2)
                result = await self._gateway.get_order_with_retry(filled_venue, symbol, order_id=oid)
                unwind_filled = float(result.get("filled_qty", 0))
                unwind_price = float(result.get("avg_price", 0))

                if result.get("fill_result") == "FILLED":
                    await self._store.transition(pid, PositionState.UNWINDING, PositionState.FAILED, {
                        "unwind.order_id": oid,
                        "unwind.side": unwind_side,
                        "unwind.filled_qty": unwind_filled,
                        "unwind.avg_fill_price": unwind_price,
                    })
                else:
                    # Unwind not confirmed — leave in UNWINDING, do NOT terminalize
                    logger.critical(f"Recovery unwind NOT CONFIRMED for {pid} — MANUAL INTERVENTION")
                    report.errors.append(f"unwind_unconfirmed_{pid}")
            except Exception as e:
                logger.critical(f"Recovery unwind FAILED for {pid}: {e}")
                report.errors.append(f"unwind_{pid}: {e}")

            report.positions_resolved += 1

        else:
            # Both definitively NOT_FILLED — safe to cancel and mark FAILED
            await self._cancel_leg_order(entry.get("bb", {}), "bybit", symbol)
            await self._cancel_leg_order(entry.get("bn", {}), "binance", symbol)
            await self._store.transition(pid, PositionState.ENTERING, PositionState.FAILED, {
                "exit.reason": "RECOVERY_NO_FILLS",
            })
            report.positions_resolved += 1

    async def _resolve_open(self, pos: dict, report: RecoveryReport):
        """Resolve OPEN: verify Bybit position exists, resume for exit monitoring."""
        pid = pos["position_id"]
        symbol = pos["symbol"]

        actual_pos = await self._gateway.check_position("bybit", symbol)

        if actual_pos is None:
            # CRITICAL FIX: check_position failed (API error). Do NOT assume flat.
            logger.critical(f"OPEN check FAILED: {pid} — cannot verify Bybit position. Resuming as OPEN.")
            report.errors.append(f"open_check_failed_{pid}")
            report.positions_resumed += 1  # Keep as OPEN, safer than closing
            return

        if abs(actual_pos) > 0:
            report.positions_resumed += 1
            logger.info(f"OPEN resumed: {pid} (Bybit position verified: {actual_pos})")
        else:
            # Bybit position confirmed gone (actual_pos == 0.0)
            # FIX (Round 4 #5): Check Binance exposure before closing.
            # If Binance had fills, we can't just close — there's inventory on the other side.
            entry = pos.get("entry", {})
            bn_qty = float(entry.get("bn", {}).get("filled_qty", 0))
            if bn_qty > 0:
                # Binance exposure exists — do NOT transition to CLOSED.
                # Leave position in OPEN and defer manual reconciliation.
                logger.critical(
                    f"OPEN recovery: {pid} Bybit FLAT but Binance has {bn_qty} of {symbol}. "
                    f"Cannot auto-close — manual reconciliation required."
                )
                report.errors.append(f"binance_exposure_{pid}: Bybit flat, Binance {bn_qty} units still held")
                report.deferred_actions.append({
                    "action": "reconcile_binance_exposure",
                    "position_id": pid,
                    "symbol": symbol,
                    "bn_qty": bn_qty,
                    "reason": "Bybit position gone but Binance inventory remains",
                })
                report.positions_resumed += 1  # Keep in OPEN for operator attention
            else:
                # No Binance exposure — safe to close
                await self._store.transition(pid, PositionState.OPEN, PositionState.EXITING, {
                    "exit.reason": "RECOVERY_POSITION_GONE",
                })
                await self._store.transition(pid, PositionState.EXITING, PositionState.CLOSED, {
                    "exit_time": time.time(),
                })
                report.positions_resolved += 1
                logger.warning(f"OPEN -> CLOSED: {pid} (Bybit position confirmed gone, no Binance exposure)")

    async def _resolve_exiting(self, pos: dict, report: RecoveryReport):
        """Resolve EXITING: query exit orders, complete or move to partial."""
        pid = pos["position_id"]
        symbol = pos["symbol"]
        exit_data = pos.get("exit", {})

        bb_leg_data = exit_data.get("bb", {})
        bn_leg_data = exit_data.get("bn", {})

        # Skip legs that were IDLE (qty=0, already closed from prior partial)
        bb_info = await self._query_leg(bb_leg_data, "bybit", symbol) if bb_leg_data.get("state") != LegStateEnum.IDLE else {"fill_result": "FILLED", "filled_qty": 0}
        bn_info = await self._query_leg(bn_leg_data, "binance", symbol) if bn_leg_data.get("state") != LegStateEnum.IDLE else {"fill_result": "FILLED", "filled_qty": 0}

        bb_unknown = bb_info.get("fill_result") == "UNKNOWN"
        bn_unknown = bn_info.get("fill_result") == "UNKNOWN"
        bb_has_fill = bb_info.get("fill_result") in ("FILLED", "PARTIAL")
        bn_has_fill = bn_info.get("fill_result") in ("FILLED", "PARTIAL")
        bb_fully = bb_info.get("fill_result") == "FILLED" and float(bb_info.get("filled_qty", 0)) >= float(bb_leg_data.get("target_qty", 1)) * 0.99
        bn_fully = bn_info.get("fill_result") == "FILLED" and float(bn_info.get("filled_qty", 0)) >= float(bn_leg_data.get("target_qty", 1)) * 0.99

        # CRITICAL FIX: UNKNOWN = do NOT change state. Leave in EXITING.
        if bb_unknown or bn_unknown:
            logger.critical(f"EXITING recovery UNRESOLVED: {pid} — leave in EXITING for retry")
            report.errors.append(f"exiting_unresolved_{pid}")
            return

        if bb_fully and bn_fully:
            await self._store.transition(pid, PositionState.EXITING, PositionState.CLOSED, {
                "exit_time": time.time(),
                "exit.bb.filled_qty": float(bb_info.get("filled_qty", 0)),
                "exit.bb.state": LegStateEnum.FILLED,
                "exit.bn.filled_qty": float(bn_info.get("filled_qty", 0)),
                "exit.bn.state": LegStateEnum.FILLED,
            })
            report.positions_resolved += 1
            logger.info(f"EXITING -> CLOSED: {pid}")
        elif bb_has_fill or bn_has_fill:
            # At least one leg has fills (full or partial) — persist partial state
            await self._store.transition(pid, PositionState.EXITING, PositionState.PARTIAL_EXIT, {
                "exit.bb.filled_qty": float(bb_info.get("filled_qty", 0)),
                "exit.bb.state": LegStateEnum.FILLED if bb_fully else (LegStateEnum.PARTIAL if bb_has_fill else LegStateEnum.CANCELLED),
                "exit.bn.filled_qty": float(bn_info.get("filled_qty", 0)),
                "exit.bn.state": LegStateEnum.FILLED if bn_fully else (LegStateEnum.PARTIAL if bn_has_fill else LegStateEnum.CANCELLED),
            })
            report.positions_resolved += 1
            logger.warning(f"EXITING -> PARTIAL_EXIT: {pid} (bb={bb_info.get('fill_result')} bn={bn_info.get('fill_result')})")
        else:
            # Both definitively NOT_FILLED -- go back to OPEN for retry
            await self._store.transition(pid, PositionState.EXITING, PositionState.OPEN, {})
            report.positions_resumed += 1
            logger.info(f"EXITING -> OPEN (retry): {pid}")

    async def _resolve_partial_exit(self, pos: dict, report: RecoveryReport):
        """Resolve PARTIAL_EXIT: check BOTH legs, market-close any non-FILLED with remaining qty.

        FIX (Round 4 #1): Previous code only handled the FIRST non-FILLED leg.
        Both legs could be non-FILLED (both PARTIAL). Now checks each leg independently
        and only transitions to CLOSED after ALL legs are resolved.
        """
        pid = pos["position_id"]
        symbol = pos["symbol"]
        exit_data = pos.get("exit", {})

        bb_leg = exit_data.get("bb", {})
        bn_leg = exit_data.get("bn", {})
        bb_state = bb_leg.get("state", "")
        bn_state = bn_leg.get("state", "")

        # Collect legs that need resolution
        legs_to_resolve = []
        if bb_state != LegStateEnum.FILLED:
            legs_to_resolve.append(("bybit", "bb", bb_leg))
        if bn_state != LegStateEnum.FILLED:
            legs_to_resolve.append(("binance", "bn", bn_leg))

        if not legs_to_resolve:
            # Both actually filled -- close
            await self._store.transition(pid, PositionState.PARTIAL_EXIT, PositionState.CLOSED, {
                "exit_time": time.time(),
            })
            report.positions_resolved += 1
            return

        # Resolve each non-FILLED leg
        all_resolved = True
        close_updates = {"exit_time": time.time()}

        for venue, venue_key, leg in legs_to_resolve:
            info = await self._query_leg(leg, venue, symbol)
            if info.get("fill_result") == "FILLED":
                close_updates[f"exit.{venue_key}.filled_qty"] = float(info.get("filled_qty", 0))
                close_updates[f"exit.{venue_key}.state"] = LegStateEnum.FILLED
                logger.info(f"PARTIAL_EXIT {pid}: {venue} leg now FILLED")
                continue

            # Try market close — use REMAINING qty, not full target
            side = leg.get("side", "Sell")
            target_qty = leg.get("target_qty", 0)
            already_filled = float(leg.get("filled_qty", 0))
            qty = target_qty - already_filled
            if qty <= 0:
                # Nothing to close for this leg
                close_updates[f"exit.{venue_key}.state"] = LegStateEnum.FILLED
                continue

            try:
                oid = await self._gateway.submit(venue, symbol, side, qty, 0, "market")
                await asyncio.sleep(2)
                result = await self._gateway.get_order_with_retry(venue, symbol, order_id=oid)
                if result.get("fill_result") == "FILLED":
                    close_updates[f"exit.{venue_key}.filled_qty"] = already_filled + float(result.get("filled_qty", 0))
                    close_updates[f"exit.{venue_key}.state"] = LegStateEnum.FILLED
                    logger.info(f"PARTIAL_EXIT {pid}: {venue} market close FILLED")
                else:
                    all_resolved = False
                    report.errors.append(f"partial_exit_market_{pid}_{venue}: still unfilled after market")
            except Exception as e:
                all_resolved = False
                report.errors.append(f"partial_exit_{pid}_{venue}: {e}")

        if all_resolved:
            await self._store.transition(pid, PositionState.PARTIAL_EXIT, PositionState.CLOSED, close_updates)
            report.positions_resolved += 1
            logger.info(f"PARTIAL_EXIT -> CLOSED: {pid} (all legs resolved)")
        else:
            logger.warning(f"PARTIAL_EXIT {pid}: not all legs resolved, staying in PARTIAL_EXIT")

    async def _resolve_unwinding(self, pos: dict, report: RecoveryReport):
        """Resolve UNWINDING: query unwind order, complete if filled."""
        pid = pos["position_id"]
        symbol = pos["symbol"]
        unwind = pos.get("unwind", {})
        oid = unwind.get("order_id", "")
        cid = unwind.get("client_order_id", "")

        if oid or cid:
            # Determine venue from the entry data
            entry = pos.get("entry", {})
            bb_filled = float(entry.get("bb", {}).get("filled_qty", 0)) > 0
            venue = "bybit" if bb_filled else "binance"

            info = await self._gateway.get_order_with_retry(venue, symbol, order_id=oid, client_order_id=cid)
            if info.get("fill_result") == "FILLED":
                await self._store.transition(pid, PositionState.UNWINDING, PositionState.FAILED, {
                    "unwind.filled_qty": float(info.get("filled_qty", 0)),
                    "unwind.avg_fill_price": float(info.get("avg_price", 0)),
                })
                report.positions_resolved += 1
                return

        # Unwind order not found or not filled -- manual intervention
        report.errors.append(f"unwind_unresolved_{pid}")
        logger.critical(f"UNWINDING unresolved: {pid} -- MANUAL INTERVENTION NEEDED")

    async def _query_leg(self, leg: dict, venue: str, symbol: str) -> dict:
        """Query a leg's order status using order_id or client_order_id."""
        oid = leg.get("order_id", "")
        cid = leg.get("client_order_id", "")
        if not oid and not cid:
            return {"fill_result": "NOT_FILLED", "filled_qty": 0}

        # If we already know it's filled from stored state, skip the query
        stored_state = leg.get("state", "")
        if stored_state == LegStateEnum.FILLED and float(leg.get("filled_qty", 0)) > 0:
            return {
                "fill_result": "FILLED",
                "filled_qty": float(leg.get("filled_qty", 0)),
                "avg_price": float(leg.get("avg_fill_price", 0)),
            }

        return await self._gateway.get_order_with_retry(venue, symbol, order_id=oid, client_order_id=cid)

    async def _cancel_leg_order(self, leg: dict, venue: str, symbol: str):
        """Cancel a leg's order if it has an order ID."""
        oid = leg.get("order_id", "")
        if oid:
            try:
                await self._gateway.cancel(venue, symbol, oid)
            except Exception:
                pass

    async def _check_orphans(self, symbols: list[str], report: RecoveryReport):
        """Check for Bybit positions not tracked in PositionStore."""
        active_symbols = await self._store.get_open_symbols()

        for sym in symbols:
            actual_pos = await self._gateway.check_position("bybit", sym)
            if actual_pos and abs(actual_pos) > 0 and sym not in active_symbols:
                logger.warning(f"ORPHAN detected: {sym} size={actual_pos}")
                try:
                    close_side = "Sell" if actual_pos > 0 else "Buy"
                    await self._gateway.submit("bybit", sym, close_side, abs(actual_pos), 0, "market")
                    report.orphans_closed += 1

                    # Defer buyback to AFTER feeds loaded (Codex #13)
                    # Side depends on orphan direction:
                    # Long orphan (actual_pos > 0): we closed by selling on Bybit,
                    #   so we need to Buy on Binance to restore inventory
                    # Short orphan (actual_pos < 0): we closed by buying on Bybit,
                    #   so we need to Sell on Binance to reduce inventory
                    buyback_side = "Buy" if actual_pos > 0 else "Sell"
                    report.deferred_actions.append({
                        "action": "orphan_buyback",
                        "symbol": sym,
                        "qty": abs(actual_pos),
                        "side": buyback_side,
                    })
                except Exception as e:
                    report.errors.append(f"orphan_close_{sym}: {e}")

    async def _cancel_stale_orders(self, symbols: list[str], report: RecoveryReport):
        """Cancel open orders on both exchanges, but only for symbols with no active positions.
        Avoids nuking fresh orders that belong to live positions."""
        active_symbols = await self._store.get_open_symbols()
        for sym in symbols:
            if sym in active_symbols:
                logger.debug(f"Skipping stale order cleanup for {sym} — has active position")
                continue
            try:
                orders = await self._gateway.bybit_api.get_open_orders(sym)
                for order in orders:
                    oid = order.get("orderId", "")
                    if oid:
                        await self._gateway.cancel("bybit", sym, oid)
                        report.orders_cancelled += 1
            except Exception as e:
                logger.warning(f"Bybit stale order cleanup failed for {sym}: {e}")

            try:
                bn_sym = self._gateway.map_bn_symbol(sym)
                orders = await self._gateway.binance_api.get_open_orders(bn_sym)
                for order in orders:
                    oid = str(order.get("orderId", ""))
                    if oid:
                        await self._gateway.cancel("binance", sym, oid)
                        report.orders_cancelled += 1
            except Exception as e:
                logger.warning(f"Binance stale order cleanup failed for {sym}: {e}")
