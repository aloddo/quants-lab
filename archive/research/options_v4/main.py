#!/usr/bin/env python3
"""
Options V4 Main Loop -- Regime-adaptive options trading.

Usage:
    python -m scripts.options_v4.main              # Paper mode (default)
    python -m scripts.options_v4.main --live        # Live trading
    python -m scripts.options_v4.main --once        # Single cycle
    python -m scripts.options_v4.main --status      # Show current state
"""
import argparse
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pybit.unified_trading import HTTP

from research.options_v4.regime import (
    Regime, RegimeFeatures, compute_features, classify, should_kill,
)
from research.options_v4.execution import (
    SpreadIntent, create_intent, execute_spread, get_open_intents, update_intent,
    IntentStatus, get_db,
)
from research.options_v4.risk import (
    RiskConfig, AccountState, fetch_account, check_pretrade,
    check_liquidity, check_instrument,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [opts_v4] %(levelname)s: %(message)s")
logger = logging.getLogger("opts_v4")

STRATEGY_INTERVAL_S = 900    # Strategy evaluation every 15 min


def get_session():
    return HTTP(
        api_key=os.environ.get("BYBIT_API_KEY"),
        api_secret=os.environ.get("BYBIT_API_SECRET"),
    )


# ── FIX #3: All options filtered to SAME expiry ─────────────────────────────

def _pick_target_expiry(options: list) -> str | None:
    """Pick the best single expiry (closest to 10 DTE). All legs must use this."""
    if not options:
        return None
    # Group by expiry string (from symbol), pick closest to 10 DTE
    from collections import Counter
    expiries = {}
    for o in options:
        exp = o["symbol"].split("-")[1]
        if exp not in expiries:
            expiries[exp] = o["dte"]
    if not expiries:
        return None
    # Pick expiry closest to 10 days
    return min(expiries, key=lambda e: abs(expiries[e] - 10))


# ── FIX #7: Strike validation (same expiry, different strikes, correct ordering) ──

def _find_spread_legs(options: list, spot: float, short_otm: float, long_otm: float,
                      opt_type: str, expiry: str, direction: str = "otm_put") -> tuple | None:
    """Find two valid spread legs with same expiry and correct ordering.
    Returns (short_leg, long_leg) for credits or (long_leg, short_leg) for debits.
    """
    filtered = [o for o in options if o["type"] == opt_type and o["symbol"].split("-")[1] == expiry]
    if len(filtered) < 2:
        return None

    if opt_type == "P":
        filtered.sort(key=lambda x: -x["strike"])  # Descending for puts
        short_target = spot * (1 - short_otm)
        long_target = spot * (1 - long_otm)
    else:
        filtered.sort(key=lambda x: x["strike"])  # Ascending for calls
        short_target = spot * (1 + short_otm)
        long_target = spot * (1 + long_otm)

    short_leg = min(filtered, key=lambda o: abs(o["strike"] - short_target))
    long_leg = min(filtered, key=lambda o: abs(o["strike"] - long_target))

    # FIX #7: Validate legs are different and correctly ordered
    if short_leg["symbol"] == long_leg["symbol"]:
        return None
    if opt_type == "P" and short_leg["strike"] <= long_leg["strike"]:
        return None  # Short put must be higher strike than long put
    if opt_type == "C" and short_leg["strike"] >= long_leg["strike"]:
        return None  # Short call must be lower strike than long call

    return short_leg, long_leg


UNDERLYING_CONFIG = {
    "BTC": {"qty": 0.01},
    "ETH": {"qty": 0.1},
}


def select_spread(session, regime: Regime, features: RegimeFeatures,
                   skip_underlyings: set = None) -> dict | None:
    """Select best spread across BTC and ETH. All legs MUST share same expiry (Fix #3)."""
    spot = features.spot
    skip = skip_underlyings or set()

    # Try each underlying, return first valid spread
    for underlying, ucfg in UNDERLYING_CONFIG.items():
        if underlying in skip:
            logger.info(f"Skipping {underlying} (already have open position)")
            continue
        result = _select_spread_for(session, regime, spot, underlying, ucfg["qty"])
        if result:
            return result
    return None


def _select_spread_for(session, regime: Regime, spot: float, underlying: str, qty: float) -> dict | None:
    """Select spread for a single underlying."""
    # For ETH, approximate spot from BTC spot (fetch live if possible)
    if underlying == "ETH":
        try:
            from pybit.unified_trading import HTTP
            tickers_spot = session.get_tickers(category="spot", symbol="ETHUSDT")
            spot = float(tickers_spot["result"]["list"][0]["lastPrice"])
        except Exception:
            spot = spot * 0.032  # rough ETH/BTC ratio fallback

    try:
        tickers = session.get_tickers(category="option", baseCoin=underlying)
    except Exception as e:
        logger.error(f"Tickers failed for {underlying}: {e}")
        return None

    options = []
    now = datetime.now(timezone.utc)
    for t in tickers["result"]["list"]:
        bid = float(t.get("bid1Price", 0))
        ask = float(t.get("ask1Price", 0))
        if bid <= 0 and ask <= 0:
            continue
        parts = t["symbol"].split("-")
        if len(parts) < 4:
            continue
        try:
            strike = int(parts[2])
            opt_type = parts[3][0]
        except (ValueError, IndexError):
            continue
        try:
            exp_dt = datetime.strptime(parts[1], "%d%b%y").replace(tzinfo=timezone.utc)
            dte = (exp_dt - now).days
        except ValueError:
            continue
        if dte < 7 or dte > 14:
            continue
        options.append({"symbol": t["symbol"], "strike": strike, "type": opt_type,
                        "bid": bid, "ask": ask, "dte": dte})

    if not options:
        return None

    target_expiry = _pick_target_expiry(options)
    if not target_expiry:
        return None

    spread = None
    if regime in (Regime.NEUTRAL, Regime.HIGH_IV):
        spread = _build_credit_spread(options, spot, qty, target_expiry, side="put",
                                      short_otm=0.08, long_otm=0.11)
    elif regime == Regime.BULL:
        spread = _build_debit_spread(options, spot, qty, target_expiry, side="call",
                                     buy_otm=0.03, sell_otm=0.06)
    elif regime == Regime.CRASH_MOMENTUM:
        spread = _build_debit_spread(options, spot, qty, target_expiry, side="put",
                                     buy_otm=0.05, sell_otm=0.10)
    elif regime == Regime.STRESS:
        spread = _build_credit_spread(options, spot, qty, target_expiry, side="call",
                                      short_otm=0.05, long_otm=0.08)

    if spread:
        spread["underlying"] = underlying
    return spread


def _build_credit_spread(options, spot, qty, expiry, side, short_otm, long_otm) -> dict | None:
    """Build a credit spread (sell short, buy long for protection)."""
    opt_type = "P" if side == "put" else "C"
    legs = _find_spread_legs(options, spot, short_otm, long_otm, opt_type, expiry)
    if not legs:
        return None
    short_leg, long_leg = legs

    if short_leg["bid"] <= 0 or long_leg["ask"] <= 0:
        return None

    credit = (short_leg["bid"] - long_leg["ask"]) * qty
    if opt_type == "P":
        width = (short_leg["strike"] - long_leg["strike"]) * qty
    else:
        width = (long_leg["strike"] - short_leg["strike"]) * qty
    max_loss = width - credit

    if credit <= 0 or max_loss <= 0 or width <= 0:
        return None

    return {
        "strategy": f"{side}_credit",
        "long_symbol": long_leg["symbol"],
        "short_symbol": short_leg["symbol"],
        "qty": qty,
        "credit": credit,
        "max_loss": max_loss,
        "description": f"{side.title()} credit {short_leg['strike']}/{long_leg['strike']}",
    }


def _build_debit_spread(options, spot, qty, expiry, side, buy_otm, sell_otm) -> dict | None:
    """Build a debit spread (buy long for profit, sell short as hedge)."""
    opt_type = "P" if side == "put" else "C"
    # For debit: buy closer to ATM (buy_otm), sell further OTM (sell_otm)
    # Note: buy_otm < sell_otm for puts; buy_otm < sell_otm for calls
    legs = _find_spread_legs(options, spot, buy_otm, sell_otm, opt_type, expiry)
    if not legs:
        return None
    short_leg, long_leg = legs  # short=closer to ATM, long=further OTM

    # For debit spread: we BUY the short_leg (closer) and SELL the long_leg (further)
    buy_leg = short_leg  # Higher strike put (closer to ATM)
    sell_leg = long_leg  # Lower strike put (further OTM)

    if buy_leg["ask"] <= 0 or sell_leg["bid"] <= 0:
        return None

    debit = (buy_leg["ask"] - sell_leg["bid"]) * qty
    if debit <= 0:
        return None

    return {
        "strategy": f"{side}_debit",
        "long_symbol": buy_leg["symbol"],    # Buy first (protection-first still applies)
        "short_symbol": sell_leg["symbol"],   # Sell second
        "qty": qty,
        "credit": -debit,
        "max_loss": debit,
        "description": f"{side.title()} debit {buy_leg['strike']}/{sell_leg['strike']}",
    }


# ── FIX #4: Startup reconciliation ──────────────────────────────────────────

def reconcile_on_startup(session) -> bool:
    """Check for orphaned intents/positions. Returns False if unsafe to trade (Codex #4).
    FIX #2: Orphans BLOCK trading, not just log."""
    db = get_db()
    safe_to_trade = True

    # Find intents in non-terminal, non-filled states (crashed mid-execution)
    stuck = list(db["options_v4_intents"].find({
        "status": {"$in": ["long_pending", "short_pending", "created", "long_filled"]}
    }))
    if stuck:
        logger.warning(f"RECONCILE: {len(stuck)} stuck intent(s) from crash")
        safe_to_trade = False
        for intent in stuck:
            # FIX R4#2: Cancel any live GTC orders from crashed intents
            for oid_field in ["long_order_link_id", "short_order_link_id"]:
                oid = intent.get(oid_field, "")
                sym = intent.get("long_symbol" if "long" in oid_field else "short_symbol", "")
                if oid and sym:
                    try:
                        session.cancel_order(category="option", symbol=sym, orderLinkId=oid)
                        logger.info(f"  Cancelled orphan order {oid} on {sym}")
                    except Exception:
                        pass  # May already be filled/cancelled, that's OK
            update_intent(intent["intent_id"], status="failed_crash",
                          close_reason="crash_recovery_orders_cancelled")
            logger.warning(f"  {intent['intent_id']} ({intent['status']}) -> failed_crash + orders cancelled")

    # Check exchange for untracked option positions
    try:
        pos = session.get_positions(category="option", settleCoin="USDT")
        open_pos = [p for p in pos["result"]["list"] if float(p["size"]) > 0]
        if open_pos:
            filled_intents = list(db["options_v4_intents"].find({"status": "both_filled"}))
            tracked_symbols = set()
            for i in filled_intents:
                tracked_symbols.add(i.get("long_symbol", ""))
                tracked_symbols.add(i.get("short_symbol", ""))
            orphans = [p for p in open_pos if p["symbol"] not in tracked_symbols]
            if orphans:
                safe_to_trade = False
                for p in orphans:
                    logger.error(f"ORPHAN BLOCKS TRADING: {p['symbol']} sz={p['size']} -- close manually before restart")
    except Exception as e:
        logger.error(f"Reconcile failed: {e}")
        safe_to_trade = False

    if not safe_to_trade:
        logger.error("UNSAFE: reconciliation found issues. Trading blocked until resolved.")
    return safe_to_trade


# ── FIX #1: Kill switch actually closes positions ────────────────────────────

def execute_kill(session, open_intents: list, dry_run: bool):
    """Kill switch: close all credit positions. FIX #1: verify fills before marking killed."""
    for intent in open_intents:
        if intent.get("strategy") not in ("put_credit", "call_credit", "iron_condor"):
            continue
        intent_id = intent["intent_id"]
        logger.warning(f"KILL: closing {intent_id} ({intent.get('strategy')})")

        if dry_run:
            update_intent(intent_id, status=IntentStatus.KILLED.value, close_reason="regime_kill")
            continue

        short_sym = intent.get("short_symbol", "")
        long_sym = intent.get("long_symbol", "")
        qty = intent.get("qty", 0.01)
        all_closed = True

        # Buy back short FIRST (remove risk exposure). Verify fill.
        if short_sym:
            try:
                oid = f"kill-S-{uuid.uuid4().hex[:8]}"
                result = session.place_order(
                    category="option", symbol=short_sym, side="Buy",
                    orderType="Market", qty=str(qty), timeInForce="IOC",
                    reduceOnly=True, orderLinkId=oid,
                )
                if result.get("retCode", -1) != 0:
                    logger.error(f"Kill short REJECTED: {result.get('retMsg')}")
                    all_closed = False
                else:
                    # FIX: Verify fill by polling order history
                    time.sleep(2)
                    try:
                        hist = session.get_order_history(category="option", symbol=short_sym, orderLinkId=oid)
                        orders = hist["result"]["list"]
                        if orders and orders[0].get("orderStatus") == "Filled":
                            logger.info(f"Kill short CONFIRMED filled: {short_sym}")
                        else:
                            status = orders[0].get("orderStatus", "unknown") if orders else "no_order"
                            logger.error(f"Kill short NOT CONFIRMED: {short_sym} status={status}")
                            all_closed = False
                    except Exception as ve:
                        logger.error(f"Kill short verify failed: {ve}")
                        all_closed = False
            except Exception as e:
                logger.error(f"Kill short failed: {e}")
                all_closed = False

        # Only sell long AFTER short is confirmed closed
        long_closed = False
        if all_closed and long_sym:
            try:
                oid = f"kill-L-{uuid.uuid4().hex[:8]}"
                result = session.place_order(
                    category="option", symbol=long_sym, side="Sell",
                    orderType="Market", qty=str(qty), timeInForce="IOC",
                    reduceOnly=True, orderLinkId=oid,
                )
                if result.get("retCode", -1) == 0:
                    long_closed = True
                else:
                    logger.warning(f"Kill long sell rejected (keeping protection): {result.get('retMsg')}")
            except Exception as e:
                logger.warning(f"Kill long failed (keeping protection): {e}")

        # FIX R4#3: Only mark killed if BOTH legs handled
        if all_closed and long_closed:
            update_intent(intent_id, status=IntentStatus.KILLED.value, close_reason="regime_kill_both_closed")
        elif all_closed and not long_closed:
            # Short closed but long remains (acceptable: long is protection, no risk)
            update_intent(intent_id, status=IntentStatus.KILLED.value, close_reason="regime_kill_long_remains")
            logger.info(f"Kill {intent_id}: short closed, long remains as protection (no risk)")
        else:
            logger.error(f"KILL INCOMPLETE: {intent_id} -- short NOT confirmed closed. MANUAL REVIEW.")


# ── Main cycle ───────────────────────────────────────────────────────────────

def run_cycle(session, config: RiskConfig, dry_run: bool) -> None:
    """Run one decision cycle."""
    features = compute_features()
    if not features:
        logger.warning("No features, skipping")
        return

    regime = classify(features)
    logger.info(f"REGIME: {regime.value} | DVOL={features.dvol:.1f}% VRP={features.vrp:+.1f}% "
                f"Trend={features.trend_7d:+.1f}% Spot=${features.spot:,.0f}")

    # Kill check (FIX #1: actually closes)
    open_intents = get_open_intents()
    has_credits = any(i.get("strategy") in ("put_credit", "call_credit", "iron_condor") for i in open_intents)
    if should_kill(features, has_credits):
        logger.warning(f"KILL SIGNAL: regime={regime.value}")
        execute_kill(session, open_intents, dry_run)
        return

    # Flat regimes
    if regime in (Regime.CRASH_EXTREME, Regime.UNCLEAR):
        logger.info(f"FLAT regime ({regime.value})")
        return

    # Check capacity (max_concurrent from risk config)
    if len(open_intents) >= config.max_concurrent:
        logger.info(f"Already {len(open_intents)} open, skipping")
        return

    # Skip underlyings that already have an open position
    open_underlyings = {i.get("underlying", "BTC") for i in open_intents}

    # Find spread (skipping underlyings already in use)
    spread = select_spread(session, regime, features, skip_underlyings=open_underlyings)
    if not spread:
        logger.info("No suitable spread")
        return

    logger.info(f"CANDIDATE: {spread['description']} credit=${spread['credit']:.2f} max_loss=${spread['max_loss']:.2f}")

    # Risk checks
    account = fetch_account(session)
    ok, reason = check_pretrade(config, account, spread["max_loss"], max(spread["credit"], 0))
    if not ok:
        logger.warning(f"BLOCKED: {reason}")
        return

    # Liquidity + instrument checks
    for sym in [spread["long_symbol"], spread["short_symbol"]]:
        ok, reason = check_liquidity(session, sym, config)
        if not ok:
            logger.warning(f"LIQUIDITY: {reason}")
            return
        ok, reason = check_instrument(session, sym, spread["qty"])
        if not ok:
            logger.warning(f"INSTRUMENT: {reason}")
            return

    # Execute
    intent = SpreadIntent(
        intent_id=f"v4-{uuid.uuid4().hex[:8]}",
        strategy=spread["strategy"],
        regime=regime.value,
        underlying=spread.get("underlying", "BTC"),
        long_symbol=spread["long_symbol"],
        short_symbol=spread["short_symbol"],
        qty=spread["qty"],
        max_loss=spread["max_loss"],
        expected_credit=spread["credit"],
    )
    create_intent(intent)
    success = execute_spread(session, intent, dry_run=dry_run)
    if success:
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}OPENED: {intent.intent_id}")
    else:
        logger.warning(f"FAILED: {intent.intent_id}")


def show_status(session, config):
    """Show current system state."""
    features = compute_features()
    if not features:
        print("No features available")
        return
    regime = classify(features)
    account = fetch_account(session)
    open_intents = get_open_intents()

    print(f"\n{'='*60}")
    print(f"OPTIONS V4 -- {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")
    print(f"BTC: ${features.spot:,.2f} | DVOL: {features.dvol:.1f}% | VRP: {features.vrp:+.1f}%")
    print(f"Trend: {features.trend_7d:+.1f}% | DD: {features.drawdown_7d:.1f}%")
    print(f"Regime: {regime.value} | Account: ${account.equity:.2f} | Open: {len(open_intents)}")
    print(f"Kill: {should_kill(features, bool(open_intents))}")

    spread = select_spread(session, regime, features)
    if spread:
        print(f"Trade: {spread['description']} | ${spread['credit']:.2f} credit | ${spread['max_loss']:.2f} risk")
        ok, reason = check_pretrade(config, account, spread["max_loss"], max(spread["credit"], 0))
        print(f"Risk: {'PASS' if ok else 'BLOCKED'} ({reason})")
    else:
        print("Trade: none available")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()

    session = get_session()
    config = RiskConfig()
    dry_run = not args.live

    if args.status:
        show_status(session, config)
        return

    # FIX #4: Reconcile on startup. Block if unsafe.
    safe = reconcile_on_startup(session)
    if not safe and not dry_run:
        logger.error("ABORTING: reconciliation failed. Resolve issues before live trading.")
        return

    logger.info(f"=== {'LIVE' if not dry_run else 'PAPER'} MODE ===")
    running = True
    def shutdown(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    last_cycle = 0
    while running:
        now = time.time()
        if now - last_cycle >= STRATEGY_INTERVAL_S:
            try:
                run_cycle(session, config, dry_run)
            except Exception as e:
                # FIX #5: Don't silently continue on errors. Log and halt after 3 consecutive.
                logger.error(f"Cycle error: {e}", exc_info=True)
                if not hasattr(main, '_error_count'):
                    main._error_count = 0
                main._error_count += 1
                if main._error_count >= 3:
                    logger.error("3 consecutive errors, halting")
                    break
            else:
                if hasattr(main, '_error_count'):
                    main._error_count = 0
            last_cycle = now
            if args.once:
                break
        time.sleep(10)

    logger.info("Shutdown")


if __name__ == "__main__":
    main()
