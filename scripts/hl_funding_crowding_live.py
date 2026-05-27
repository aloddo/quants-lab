"""
HL Funding Crowding SHORT — Live Executor for Hyperliquid.

Strategy: SHORT meme coins when hourly funding rate 72h z-score > 2.5.
Exit: TP=3%, SL=8%, Time Limit=8h.
Venue: Hyperliquid perpetuals (maker fees 1.5bp).

Signal: Retail longs crowd into speculative coins → funding spikes →
        crowded longs unwind within 8h → mean reversion.

Validated (2026-05-06 systematic scan of 122 HL coins):
  FARTCOIN: 42 trades, +1.56%, 67%WR, p=0.007 → LIVE
  PUMP:     14 trades, +1.95%, 79%WR, p=0.016 → PAPER (signal-only)
  ZRO/kPEPE/HYPE/XMR/all others: NO EDGE (confirmed dead)

Architecture: Standalone script, HL Python SDK for execution,
              MongoDB for signal data + trade recording, Telegram for alerts.

Env vars required:
    HL_PRIVATE_KEY      — HL wallet private key
    HL_ADDRESS          — HL signing/sub-account address
    HL_QUERY_ADDRESS    — HL parent/query address (optional, defaults to HL_ADDRESS)
    MONGO_URI
    MONGO_DATABASE
    TELEGRAM_BOT_TOKEN  — (optional) for trade notifications
    TELEGRAM_CHAT_ID    — (optional) quant group chat

Usage:
    set -a && source .env && set +a
    /Users/hermes/miniforge3/envs/quants-lab/bin/python scripts/hl_funding_crowding_live.py

    Options:
        --dry-run           Signal scan only, no orders
        --position-size     USD per trade (default: 75)
        --leverage          Leverage multiplier (default: 3)
        --max-positions     Max concurrent (default: 3)
"""

import argparse
import json
import logging
import os
import signal as signal_mod
import sys
import time
import traceback
import urllib.request
from datetime import datetime, timezone
from typing import Optional

import numpy as np
from pymongo import MongoClient

# HL SDK imports
try:
    import eth_account
    from hyperliquid.exchange import Exchange
    from hyperliquid.info import Info
    from hyperliquid.utils import constants
except ImportError:
    print("ERROR: hyperliquid-python-sdk not installed. Run: pip install hyperliquid-python-sdk")
    sys.exit(1)

# ── Config ───────────────────────────────────────────────────────────────────

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("hl_funding_crowding")

# MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
MONGO_DB = os.getenv("MONGO_DATABASE", "quants_lab")

# Telegram
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "-1003576397888")

# HL API
HL_API_URL = "https://api.hyperliquid.xyz"

# ── Strategy Parameters ────────────────���─────────────────────────────────────

COINS = {
    "FARTCOIN": {
        "z_threshold": 2.5,     # upgraded from 2.0: p=0.0069 at z>2.5, fewer but higher-conviction
        "tp_pct": 0.03,         # 3% take profit
        "sl_pct": 0.08,         # 8% stop loss
        "time_limit_h": 8,      # 8 hour time limit
        "cooldown_h": 12,       # min hours between trades on same coin
    },
    "PUMP": {
        "z_threshold": 2.5,     # NEW: 14 trades, +1.95%, 79% WR, p=0.016. Paper-validate.
        "tp_pct": 0.03,
        "sl_pct": 0.08,
        "time_limit_h": 8,
        "cooldown_h": 12,
        "paper_only": True,     # Signal-only mode: log signals but don't trade
    },
    # DEAD (confirmed by 122-coin systematic scan 2026-05-06):
    # "ZRO": 25 trades, +0.07%, p=0.46 — NO EDGE
    # "kPEPE": 9 trades, -0.52%, p=0.65 — DEAD
    # "HYPE": 49 trades, -0.06%, p=0.55 — DEAD
    # "XMR": 43 trades, -0.48%, p=0.95 — DEAD (inverse signal)
}

# Rolling z-score window (72 hours of hourly funding)
Z_WINDOW = 72
Z_MIN_PERIODS = 30  # need at least 30 obs for stable z-score

# Signal check interval (hourly — aligns with HL funding frequency)
SIGNAL_INTERVAL_S = 3600  # 1 hour
LIFECYCLE_INTERVAL_S = 180  # check exits every 3 min (reduces HL API pressure)


# ── Telegram ───────────────���─────────────────────────────────────────────────

def tg_send(msg: str) -> None:
    """Send Telegram notification (non-blocking, fire-and-forget)."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        data = json.dumps({"chat_id": TG_CHAT_ID, "text": msg}).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")


# ── HL SDK Wrapper ───────────────────────────────────────────────────────────

class HLClient:
    """Thin wrapper around hyperliquid-python-sdk for order management."""

    def __init__(self, private_key: str, address: str, query_address: str = ""):
        self.address = address
        self.query_address = query_address or address
        wallet = eth_account.Account.from_key(private_key)
        self.info = Info(HL_API_URL, skip_ws=True)
        self.exchange = Exchange(wallet, HL_API_URL, account_address=self.query_address)
        logger.info(f"HL SDK initialized. Address: {self.address[:10]}...")

    def get_mid_price(self, coin: str, retries: int = 3) -> Optional[float]:
        """Get current mid price for a coin with retry + backoff."""
        for attempt in range(retries):
            try:
                all_mids = self.info.all_mids()
                return float(all_mids.get(coin, 0))
            except Exception as e:
                if attempt < retries - 1:
                    wait = 5 * (2 ** attempt)  # 5s, 10s, 20s
                    logger.warning(f"Mid price retry {attempt+1}/{retries} for {coin}, wait {wait}s")
                    time.sleep(wait)
                else:
                    logger.error(f"Failed to get mid price for {coin} after {retries} retries: {e}")
                    return None

    def get_position(self, coin: str) -> Optional[dict]:
        """Get current position for a coin. Returns position dict or None if flat."""
        try:
            user_state = self.info.user_state(self.query_address)
            for pos in user_state.get("assetPositions", []):
                p = pos.get("position", {})
                if p.get("coin") == coin:
                    szi = float(p.get("szi", "0"))
                    if szi != 0:
                        return p
            return None
        except Exception as e:
            logger.error(f"Failed to get position for {coin}: {e}")
            return None

    def get_account_value(self) -> float:
        """Get available balance from HL unified account.

        HL unified wallet: USDC balance shows under spotClearinghouseState
        but IS usable as perps margin. When no perps positions are open,
        marginSummary.accountValue = 0 even though funds are available.
        Total available = marginSummary.accountValue + spot USDC balance.
        """
        try:
            total = 0.0

            # Perps margin (non-zero when positions are open)
            user_state = self.info.user_state(self.query_address)
            margin = user_state.get("marginSummary", {})
            acct_val = float(margin.get("accountValue", 0))
            total += acct_val

            # Spot USDC (available as perps margin in unified wallet)
            try:
                spot = self.info.spot_user_state(self.query_address)
                for b in spot.get("balances", []):
                    if b.get("coin") == "USDC":
                        spot_usdc = float(b.get("total", 0))
                        total += spot_usdc
                        break
            except Exception:
                pass

            logger.info(f"HL account value: ${total:.2f} (perps=${acct_val:.2f} + spot USDC)")
            return total
        except Exception as e:
            logger.error(f"Failed to get account value: {e}")
            return 0.0

    def market_short(self, coin: str, size_usd: float, leverage: int) -> Optional[dict]:
        """Place a market SHORT order."""
        try:
            mid = self.get_mid_price(coin)
            if not mid or mid <= 0:
                logger.error(f"Invalid mid price for {coin}: {mid}")
                return None

            # Get size decimals for this coin
            sz_decimals = self._get_sz_decimals(coin)

            # Compute size in coin units, rounded to exchange precision
            size_coins = round(size_usd / mid, sz_decimals)
            if size_coins <= 0:
                logger.error(f"Size too small for {coin}: ${size_usd} / ${mid} = {size_coins}")
                return None

            # Set leverage first
            self.exchange.update_leverage(leverage, coin, is_cross=True)

            # Place market sell (SHORT)
            result = self.exchange.market_open(
                name=coin,
                is_buy=False,  # SHORT
                sz=size_coins,
                slippage=0.01,  # 1% max slippage
            )
            logger.info(f"SHORT {coin}: size={size_coins} @ ~${mid:.4f}, result={result}")
            return result
        except Exception as e:
            logger.error(f"Failed to place SHORT on {coin}: {e}")
            return None

    def _get_sz_decimals(self, coin: str) -> int:
        """Get size decimals for a coin from exchange metadata."""
        try:
            meta = self.info.meta()
            for asset in meta.get("universe", []):
                if asset.get("name") == coin:
                    return asset.get("szDecimals", 0)
            return 0  # default to whole units
        except Exception:
            return 0

    def market_close(self, coin: str) -> Optional[dict]:
        """Close position on a coin."""
        try:
            result = self.exchange.market_close(coin=coin)
            logger.info(f"CLOSE {coin}: result={result}")
            return result
        except Exception as e:
            logger.error(f"Failed to close {coin}: {e}")
            return None


# ── Signal Engine ─────────────���──────────────────────────────────────────────

class FundingSignalEngine:
    """Compute funding rate z-scores from MongoDB."""

    def __init__(self, db):
        self.db = db
        self.collection = db["hyperliquid_funding_rates"]

    def get_funding_zscore(self, coin: str) -> Optional[float]:
        """
        Compute current funding rate z-score using last 72 hourly observations.
        Returns z-score or None if insufficient data.
        """
        # Fetch last Z_WINDOW + buffer funding rates for this coin
        docs = list(
            self.collection.find(
                {"coin": coin},
                {"funding_rate": 1, "timestamp_utc": 1}
            ).sort("timestamp_utc", -1).limit(Z_WINDOW + 10)
        )

        if len(docs) < Z_MIN_PERIODS:
            logger.warning(f"{coin}: only {len(docs)} funding docs, need {Z_MIN_PERIODS}")
            return None

        # Extract funding rates (newest first, reverse to chronological)
        rates = np.array([d["funding_rate"] for d in docs[:Z_WINDOW]][::-1])

        if len(rates) < Z_MIN_PERIODS:
            return None

        # Compute z-score of the LATEST rate vs the rolling window
        mean = np.mean(rates[:-1])  # exclude current for unbiased z
        std = np.std(rates[:-1], ddof=1)

        if std < 1e-10:
            return 0.0

        current_rate = rates[-1]
        z = (current_rate - mean) / std
        return z

    def scan_signals(self, coins: dict) -> list:
        """Scan all coins for SHORT signals. Returns list of (coin, z_score, config)."""
        signals = []
        for coin, config in coins.items():
            z = self.get_funding_zscore(coin)
            if z is None:
                logger.debug(f"{coin}: no z-score available")
                continue

            threshold = config["z_threshold"]
            if z > threshold:
                # CRITICAL: also verify funding is actually POSITIVE (longs paying shorts)
                # A high z-score on negative funding means "less negative than usual" — wrong thesis
                latest = self.collection.find_one(
                    {"coin": coin}, sort=[("timestamp_utc", -1)]
                )
                if latest and latest.get("funding_rate", 0) <= 0:
                    logger.info(
                        f"  {coin}: z={z:.2f} but funding={latest['funding_rate']:.6f} (NEGATIVE). "
                        f"Skipping — crowded longs thesis requires positive funding."
                    )
                    continue

                signals.append((coin, z, config))
                logger.info(f"🔴 SIGNAL: {coin} funding z={z:.2f} > {threshold} → SHORT")
            else:
                logger.debug(f"{coin}: z={z:.2f} (below {threshold})")

        return signals


# ── Position Manager ────────────���──────────────────────��─────────────────────

class PositionManager:
    """Track open positions, manage exits (TP/SL/time limit)."""

    def __init__(self, db, hl_client: HLClient):
        self.db = db
        self.hl = hl_client
        self.collection = db["hl_live_executions"]
        self.positions = {}  # coin -> position_doc
        self._restore_open_positions()

    def _restore_open_positions(self):
        """On startup, reconcile MongoDB state with actual exchange positions."""
        open_docs = list(self.collection.find({"status": "OPEN"}))

        # Get actual exchange positions for reconciliation
        exchange_positions = {}
        try:
            user_state = self.hl.info.user_state(self.hl.query_address)
            for pos in user_state.get("assetPositions", []):
                p = pos.get("position", {})
                coin = p.get("coin", "")
                szi = float(p.get("szi", "0"))
                if szi != 0:
                    exchange_positions[coin] = p
        except Exception as e:
            logger.error(f"Failed to fetch exchange positions for reconciliation: {e}")

        # Reconcile: DB says OPEN but exchange is flat → mark as CLOSED (orphan)
        for doc in open_docs:
            coin = doc["coin"]
            if coin not in exchange_positions:
                logger.warning(f"RECONCILE: {coin} OPEN in DB but NOT on exchange. Marking CLOSED.")
                self.collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"status": "CLOSED", "close_type": "RECONCILE_FLAT",
                              "closed_at": datetime.now(timezone.utc)}}
                )
            else:
                self.positions[coin] = doc
                logger.info(f"Restored open position: {coin} SHORT @ ${doc['entry_price']:.4f}")

        # Reconcile: exchange has position but DB doesn't know → log warning
        for coin, pos_data in exchange_positions.items():
            if coin not in self.positions:
                szi = float(pos_data.get("szi", "0"))
                entry = float(pos_data.get("entryPx", "0"))
                logger.warning(
                    f"RECONCILE: {coin} has exchange position (size={szi}, entry=${entry}) "
                    f"but NO matching DB record. Manual intervention needed."
                )
                tg_send(f"⚠️ ORPHAN: {coin} position on HL not tracked by bot. size={szi}")

        if self.positions:
            logger.info(f"Restored {len(self.positions)} reconciled positions")

    def has_position(self, coin: str) -> bool:
        return coin in self.positions

    def in_cooldown(self, coin: str, cooldown_h: float) -> bool:
        """Check if coin is in post-trade cooldown."""
        last_trade = self.collection.find_one(
            {"coin": coin, "status": {"$ne": "OPEN"}},
            sort=[("closed_at", -1)]
        )
        if not last_trade or "closed_at" not in last_trade:
            return False
        closed_at = last_trade["closed_at"]
        if closed_at.tzinfo is None:
            closed_at = closed_at.replace(tzinfo=timezone.utc)
        elapsed = (datetime.now(timezone.utc) - closed_at).total_seconds() / 3600
        return elapsed < cooldown_h

    def open_position(self, coin: str, entry_price: float, size_usd: float,
                      config: dict, z_score: float) -> None:
        """Record a new open position."""
        doc = {
            "coin": coin,
            "direction": "SHORT",
            "entry_price": entry_price,
            "size_usd": size_usd,
            "tp_price": entry_price * (1 - config["tp_pct"]),
            "sl_price": entry_price * (1 + config["sl_pct"]),
            "time_limit_h": config["time_limit_h"],
            "z_score_at_entry": z_score,
            "opened_at": datetime.now(timezone.utc),
            "status": "OPEN",
            "strategy": "HL_FUNDING_CROWDING",
        }
        self.collection.insert_one(doc)
        self.positions[coin] = doc
        logger.info(
            f"Position opened: {coin} SHORT @ ${entry_price:.4f} "
            f"TP=${doc['tp_price']:.4f} SL=${doc['sl_price']:.4f} TL={config['time_limit_h']}h"
        )

    def check_exits(self) -> list:
        """Check all open positions for TP/SL/time limit. Returns list of closed coins."""
        closed = []
        for coin, pos in list(self.positions.items()):
            mid = self.hl.get_mid_price(coin)
            if mid is None:
                continue

            entry = pos["entry_price"]
            close_type = None
            exit_price = mid

            # Check SL (price went UP for a SHORT)
            if mid >= pos["sl_price"]:
                close_type = "SL"
            # Check TP (price went DOWN for a SHORT)
            elif mid <= pos["tp_price"]:
                close_type = "TP"
            # Check time limit
            else:
                opened = pos["opened_at"]
                if opened.tzinfo is None:
                    opened = opened.replace(tzinfo=timezone.utc)
                elapsed_h = (datetime.now(timezone.utc) - opened).total_seconds() / 3600
                if elapsed_h >= pos["time_limit_h"]:
                    close_type = "TIME_LIMIT"

            if close_type:
                # Close on exchange — verify success before updating DB
                result = self.hl.market_close(coin)

                # Verify the close actually worked by checking exchange position
                close_confirmed = False
                try:
                    remaining_pos = self.hl.get_position(coin)
                    close_confirmed = (remaining_pos is None)
                except Exception:
                    pass

                if not close_confirmed:
                    logger.error(
                        f"CLOSE {coin} may have FAILED (position still exists). "
                        f"Result: {result}. Will retry next cycle."
                    )
                    tg_send(f"⚠️ {coin} close may have failed! Check manually.")
                    continue  # Don't mark as closed, retry next cycle

                # Get actual exit price from exchange (not just mid)
                actual_exit = self.hl.get_mid_price(coin) or exit_price

                # Compute PnL (SHORT: profit when price drops)
                pnl_pct = (entry - actual_exit) / entry
                pnl_usd = pnl_pct * pos["size_usd"]

                # Update MongoDB — only after confirmed close
                self.collection.update_one(
                    {"_id": pos["_id"]},
                    {"$set": {
                        "status": "CLOSED",
                        "close_type": close_type,
                        "exit_price": actual_exit,
                        "pnl_pct": pnl_pct,
                        "pnl_usd": pnl_usd,
                        "closed_at": datetime.now(timezone.utc),
                        "close_result": str(result),
                    }}
                )

                del self.positions[coin]
                closed.append((coin, close_type, pnl_pct, pnl_usd))

                emoji = "✅" if pnl_usd > 0 else "❌"
                logger.info(
                    f"{emoji} CLOSED {coin} via {close_type}: "
                    f"entry=${entry:.4f} exit=${exit_price:.4f} PnL={pnl_pct*100:.2f}% (${pnl_usd:.2f})"
                )
                tg_send(
                    f"{emoji} HL {coin} SHORT closed ({close_type})\n"
                    f"Entry: ${entry:.4f} → Exit: ${exit_price:.4f}\n"
                    f"PnL: {pnl_pct*100:.2f}% (${pnl_usd:.2f})"
                )

        return closed


# ── Main Loop ───────────────────���───────────────────────────────��────────────

def parse_args():
    parser = argparse.ArgumentParser(description="HL Funding Crowding SHORT Executor")
    parser.add_argument("--dry-run", action="store_true", help="Signal scan only, no orders")
    parser.add_argument("--position-size", type=float, default=75, help="USD per trade")
    parser.add_argument("--leverage", type=int, default=3, help="Leverage")
    parser.add_argument("--max-positions", type=int, default=3, help="Max concurrent positions")
    return parser.parse_args()


def main():
    args = parse_args()

    # Validate env
    private_key = os.getenv("HL_PRIVATE_KEY", "")
    address = os.getenv("HL_ADDRESS", "")
    # Use the parent/main wallet address for trading (agent key trades on behalf of parent)
    query_address = os.getenv("HL_QUERY_ADDRESS", "") or "0x11ca20aeb7cd014cf8406560ae405b12601994b4"

    if not private_key and not args.dry_run:
        logger.error("HL_PRIVATE_KEY required for live trading")
        sys.exit(1)

    # Connect MongoDB (tz_aware to avoid naive/aware datetime mixing)
    client = MongoClient(MONGO_URI, tz_aware=True)
    db = client[MONGO_DB]

    # Initialize components
    signal_engine = FundingSignalEngine(db)

    hl_client = None
    pos_manager = None
    if not args.dry_run:
        hl_client = HLClient(private_key, address, query_address)
        pos_manager = PositionManager(db, hl_client)
        account_val = hl_client.get_account_value()
        logger.info(f"HL account value: ${account_val:.2f}")
        tg_send(
            f"🟢 HL Funding Crowding executor started\n"
            f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}\n"
            f"Coins: {', '.join(COINS.keys())}\n"
            f"Size: ${args.position_size}/trade, {args.leverage}x\n"
            f"Account: ${account_val:.2f}"
        )
    else:
        logger.info("=== DRY RUN MODE — no orders will be placed ===")
        tg_send("🔵 HL Funding Crowding DRY RUN started")

    # Graceful shutdown
    running = [True]

    def handle_signal(signum, frame):
        logger.info("Shutdown signal received")
        running[0] = False

    signal_mod.signal(signal_mod.SIGINT, handle_signal)
    signal_mod.signal(signal_mod.SIGTERM, handle_signal)

    # ── Main loop ────────────────────────────────────────────────────────────
    last_signal_check = 0
    last_lifecycle_check = 0
    pending_entries = {}  # coin -> {"z_score": float, "config": dict, "execute_after": float}

    logger.info("Entering main loop...")

    while running[0]:
        now = time.time()

        try:
            # ── Execute deferred entries (next-bar alignment) ────────────
            # Signals detected in hour N execute at the top of hour N+1.
            # This matches backtesting (which uses next-bar open) and avoids
            # entering during the same-hour dump that caused the funding spike.
            for coin in list(pending_entries.keys()):
                entry = pending_entries[coin]
                if now < entry["execute_after"]:
                    continue  # not yet time

                z_score = entry["z_score"]
                config = entry["config"]
                del pending_entries[coin]

                # Re-validate: still above threshold? (funding may have normalized)
                z_now = signal_engine.get_funding_zscore(coin)
                if z_now is None or z_now < config["z_threshold"]:
                    logger.info(
                        f"  {coin}: deferred entry CANCELLED — z dropped to "
                        f"{z_now:.2f} (was {z_score:.2f} at signal). Funding normalized."
                    )
                    tg_send(
                        f"⏭️ {coin} entry cancelled: z dropped {z_score:.2f}→{z_now:.2f} "
                        f"before next-bar entry"
                    )
                    continue

                # Re-check position/cooldown (may have changed in the hour)
                if pos_manager and pos_manager.has_position(coin):
                    logger.info(f"  {coin}: deferred entry skipped — already positioned")
                    continue
                if pos_manager and len(pos_manager.positions) >= args.max_positions:
                    logger.info(f"  {coin}: deferred entry skipped — max positions reached")
                    continue

                logger.info(
                    f"  ⏰ DEFERRED ENTRY: {coin} z={z_score:.2f}→{z_now:.2f} "
                    f"(next-bar alignment, executing now)"
                )
                tg_send(
                    f"⏰ Next-bar entry: {coin} SHORT\n"
                    f"Signal z={z_score:.2f}, current z={z_now:.2f}"
                )

                # >>> Fall through to execution below <<<
                # We set z_score to current for the execution block
                z_score = z_now
                goto_execute = True
                break
            else:
                goto_execute = False

            # Signal scan (every SIGNAL_INTERVAL_S)
            if not goto_execute and now - last_signal_check >= SIGNAL_INTERVAL_S:
                last_signal_check = now
                logger.info("Signal scan starting...")

                signals = signal_engine.scan_signals(COINS)

                for coin, z_score, config in signals:
                    # Skip if already positioned
                    if pos_manager and pos_manager.has_position(coin):
                        logger.info(f"  {coin}: signal active but already positioned, skip")
                        continue

                    # Skip if max positions reached
                    if pos_manager and len(pos_manager.positions) >= args.max_positions:
                        logger.info(f"  {coin}: max positions ({args.max_positions}) reached, skip")
                        continue

                    # Skip if in cooldown
                    if pos_manager and pos_manager.in_cooldown(coin, config["cooldown_h"]):
                        logger.info(f"  {coin}: in cooldown, skip")
                        continue

                    if args.dry_run:
                        logger.info(f"  [DRY RUN] Would SHORT {coin} (z={z_score:.2f})")
                        continue

                    # Paper-only mode: log signal + TG alert but don't trade
                    if config.get("paper_only", False):
                        logger.info(f"  [PAPER] {coin} z={z_score:.2f} — signal logged, no trade")
                        tg_send(f"📋 PAPER SIGNAL: {coin} SHORT z={z_score:.2f} (paper_only mode)")
                        continue

                    # ── Defer entry to next hour (next-bar alignment) ────────
                    # Calculate next hour boundary
                    current_hour_start = (int(now) // 3600) * 3600
                    next_hour = current_hour_start + 3600
                    wait_min = (next_hour - now) / 60

                    if coin not in pending_entries:
                        pending_entries[coin] = {
                            "z_score": z_score,
                            "config": config,
                            "execute_after": next_hour,
                        }
                        logger.info(
                            f"  📋 {coin} z={z_score:.2f} — DEFERRED entry "
                            f"to next bar ({wait_min:.0f}min). "
                            f"Avoids same-hour dump entry."
                        )
                        tg_send(
                            f"📋 Signal: {coin} SHORT z={z_score:.2f}\n"
                            f"Entry deferred {wait_min:.0f}min to next bar "
                            f"(backtest alignment)"
                        )
                    continue

                if not signals:
                    logger.info("Signal scan complete (no signals)")

            # ── Execute deferred entry (if ready) ─────────────────────────────
            # coin, z_score, config are set by the deferred entry loop above
            if goto_execute:
                # Pre-flight: check account balance is sufficient
                acct_val = hl_client.get_account_value()
                min_required = args.position_size / args.leverage * 1.1  # margin + 10% buffer
                if acct_val < min_required:
                    logger.warning(
                        f"  {coin}: SKIP entry — available balance ${acct_val:.2f} "
                        f"< required ${min_required:.2f}."
                    )
                    continue

                logger.info(f"  Executing SHORT {coin} (z={z_score:.2f})...")

                # Step 1: Record PENDING state BEFORE placing order
                pending_doc = {
                    "coin": coin, "direction": "SHORT",
                    "size_usd": args.position_size,
                    "z_score_at_entry": z_score,
                    "opened_at": datetime.now(timezone.utc),
                    "status": "PENDING",
                    "strategy": "HL_FUNDING_CROWDING",
                }
                pending_id = pos_manager.collection.insert_one(pending_doc).inserted_id

                # Step 2: Place order
                result = hl_client.market_short(coin, args.position_size, args.leverage)

                # Step 3: Verify fill by checking exchange position
                time.sleep(1)  # brief wait for fill to propagate
                exchange_pos = hl_client.get_position(coin)

                if exchange_pos:
                    # Successfully filled — get actual entry price from exchange
                    entry_price = float(exchange_pos.get("entryPx", "0"))
                    if entry_price <= 0:
                        entry_price = hl_client.get_mid_price(coin) or 0

                    # Update PENDING → OPEN with real fill price
                    pos_manager.collection.update_one(
                        {"_id": pending_id},
                        {"$set": {
                            "status": "OPEN",
                            "entry_price": entry_price,
                            "tp_price": entry_price * (1 - config["tp_pct"]),
                            "sl_price": entry_price * (1 + config["sl_pct"]),
                            "time_limit_h": config["time_limit_h"],
                            "fill_result": str(result),
                        }}
                    )
                    # Update in-memory state
                    doc = pos_manager.collection.find_one({"_id": pending_id})
                    pos_manager.positions[coin] = doc

                    logger.info(
                        f"Position FILLED: {coin} SHORT @ ${entry_price:.4f} "
                        f"TP=${entry_price * (1 - config['tp_pct']):.4f} "
                        f"SL=${entry_price * (1 + config['sl_pct']):.4f}"
                    )
                    tg_send(
                        f"🔴 HL SHORT {coin}\n"
                        f"Z-score: {z_score:.2f}\n"
                        f"Entry: ${entry_price:.4f} (next-bar fill)\n"
                        f"TP: ${entry_price * (1 - config['tp_pct']):.4f} (-3%)\n"
                        f"SL: ${entry_price * (1 + config['sl_pct']):.4f} (+8%)\n"
                        f"TL: {config['time_limit_h']}h"
                    )
                else:
                    # Order failed or didn't fill — clean up PENDING
                    logger.error(f"  SHORT {coin} NOT FILLED. Result: {result}")
                    pos_manager.collection.update_one(
                        {"_id": pending_id},
                        {"$set": {"status": "FAILED", "fail_result": str(result)}}
                    )
                    tg_send(f"⚠️ HL SHORT {coin} FAILED (no fill): {result}")

            # Lifecycle check (every LIFECYCLE_INTERVAL_S)
            if pos_manager and now - last_lifecycle_check >= LIFECYCLE_INTERVAL_S:
                last_lifecycle_check = now
                n_pos = len(pos_manager.positions)
                if n_pos > 0:
                    logger.info(f"Lifecycle check: {n_pos} position(s)")
                closed = pos_manager.check_exits()
                if closed:
                    for coin, close_type, pnl_pct, pnl_usd in closed:
                        logger.info(f"  Closed {coin}: {close_type} PnL={pnl_pct*100:.2f}%")

        except Exception as e:
            logger.error(f"Main loop error: {e}\n{traceback.format_exc()}")
            tg_send(f"⚠️ HL executor error: {e}")
            time.sleep(30)  # back off on error
            continue

        # Sleep
        time.sleep(5)

    # Shutdown
    logger.info("Executor stopped gracefully")
    tg_send("🔴 HL Funding Crowding executor stopped")


if __name__ == "__main__":
    # Restart loop: auto-recover from fatal crashes (DB disconnect, SDK init failure, etc.)
    max_backoff = 300  # 5 min max
    backoff = 15
    while True:
        try:
            main()
            break  # Clean exit (SIGINT/SIGTERM) — don't restart
        except KeyboardInterrupt:
            break
        except SystemExit:
            break
        except Exception as e:
            logger.error(f"FATAL: executor crashed: {e}\n{traceback.format_exc()}")
            tg_send(f"⚠️ Executor crashed, restarting in {backoff}s: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
