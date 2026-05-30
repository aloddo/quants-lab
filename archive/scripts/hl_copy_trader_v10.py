#!/usr/bin/env python3
"""
HL Copy Trader V10 — Momentum wallet copy trader.

Unlike V9 (DCA whale copier), V10 targets momentum wallets with timing alpha:
- Enter on FIRST detected fill (no TWAP accumulation)
- Entry guards: max chase 15bps, spread 20bps, depth $3K
- No add-ons, no DCA
- Hard stop at -150bps
- Max hold 12h

Usage:
    python scripts/hl_copy_trader_v10.py --wallets 0xe65b...,0xbbf7... --coins BTC,ETH,SOL --size 11
"""
import argparse
import asyncio
import json
import logging
import math
import os
import signal
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import eth_account
import requests
import websockets
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("hl_copy")

HL_API = "https://api.hyperliquid.xyz"
HL_WS = "wss://api.hyperliquid.xyz/ws"
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "-1003576397888")
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")

# ── V10 Risk limits ──────────────────────────────────────────────────────────
MAX_MARGIN_UTIL = 0.95       # max 95% of equity used as margin
MAX_COIN_CONCENTRATION = 0.25  # max 25% of equity on any single coin
MAX_LEVERAGE_CAP = 10        # 10x — at $11 positions, liquidation risk is negligible; more room for concurrent positions
MAX_DAILY_LOSS = -15.0       # kill switch threshold (USD)

# ── V10 Entry guards ────────────────────────────────────────────────────────
MAX_CHASE_BPS = 15           # don't enter if price moved >15bps from W's fill
MAX_SPREAD_BPS = 20          # don't enter if spread >20bps
MIN_BOOK_DEPTH_USD = 3000    # don't enter if book depth <$3K on entry side
COOLDOWN_S = 30              # min time between entries on same (wallet, coin)

# ── V10 Exit ─────────────────────────────────────────────────────────────────
HARD_STOP_BPS = -99999       # disabled — follow wallet in AND out, no override
MAX_HOLD_S = 604800          # 7 days — follow wallet out, no time override
EXIT_TWAP_WINDOW_S = 60      # exit TWAP detection window (trade-stream based)
EXIT_TWAP_MIN_NOTIONAL = 50  # lower threshold for momentum wallets (they trade small)

# ── V10 disabled features ───────────────────────────────────────────────────
MAX_ADDON_MULTIPLIER = 1     # V10: single entry only (1x base = no add-ons beyond initial)
EXIT_MIN_TRIM_PCT = 0.05     # 5% reverse flow triggers trim/exit
EXIT_MIN_TRIM_USD = 3.0      # or $3 absolute minimum
TWAP_WINDOW_S = 0            # NO TWAP accumulation. Enter on first fill.
MIN_TWAP_NOTIONAL = 0        # NO notional threshold. Wallet IS the signal.

# Per-wallet exit mechanics: FIRST_CLOSE exits on any Close event, GRADUAL waits for 30%
WALLET_EXIT_TYPE = {
    "0xf6e4e49d2786fb5c284094e39eaac62db263af81": "FIRST_CLOSE",
    "0x2be81c3c28820ad7e97aee68c3dcff55f89cde2b": "FIRST_CLOSE",
    "0xde626f894ff07947a4a73bb34675cc4d9f1b0d4e": "FIRST_CLOSE",
    "0x76eed6343d4c09d06de05ad84de48ec9ae4ff4e9": "FIRST_CLOSE",
    "0xe9b0f5794b008bb1c3a3e5c12c7f90bad4b1e9a3": "FIRST_CLOSE",
    "0xf517639a8872e756ac98d3c65507d2ebc25cc032": "FIRST_CLOSE",
    "0xad227f63d34e7251c1d0ab65e64eeea07aee4e44": "GRADUAL",
    "0xa6ee1ed1ae80b8352603654b39f5e7b9bedd5078": "GRADUAL",
    "0xcbf77db37dce8a92bf1d98fae655f1e8e16825d5": "GRADUAL",
    # Pipeline v1 picks (2026-05-14)
    "0x627f2d53236ff3e4c36ea3f9ca411bc2576a53b8": "FIRST_CLOSE",  # V10: fast scalper, negative slope
    "0x1f9e2e25e2aca439d91807d845df531404937be0": "FIRST_CLOSE",  # V10: momentum, flat slope
}

# Per-wallet exit parameters: hard SL, trailing stop, max hold.
# V10 momentum wallets need TIGHT trailing stops (alpha decays fast).
DEFAULT_EXIT_PARAMS = {"sl_bps": -500, "trail_activate_bps": None, "trail_bps": None, "max_hold_s": MAX_HOLD_S}
WALLET_EXIT_PARAMS = {
    # NEGATIVE slope: tight trailing, short max hold. Trailing stop is PRIMARY exit.
    "0x627f2d53236ff3e4c36ea3f9ca411bc2576a53b8": {"sl_bps": -400, "trail_activate_bps": 15, "trail_bps": 20, "max_hold_s": 600},
    # FLAT slope: moderate trailing, medium max hold.
    "0x1f9e2e25e2aca439d91807d845df531404937be0": {"sl_bps": -500, "trail_activate_bps": 30, "trail_bps": 25, "max_hold_s": 1800},
}


def _tg(msg: str):
    """Send Telegram alert."""
    if not TG_TOKEN:
        logger.warning("TG send skipped: no TELEGRAM_BOT_TOKEN")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": msg}, timeout=5
        )
        if not r.json().get("ok"):
            logger.error(f"TG send failed: {r.json().get('description', r.text)}")
    except Exception as e:
        logger.error(f"TG send error: {e}")


def _tg_with_image(caption: str, image_path: str):
    """Send Telegram photo with caption."""
    if not TG_TOKEN:
        return
    try:
        with open(image_path, "rb") as f:
            r = requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendPhoto",
                data={"chat_id": TG_CHAT_ID, "caption": caption},
                files={"photo": f}, timeout=10
            )
        if not r.json().get("ok"):
            logger.error(f"TG photo failed: {r.json().get('description', r.text)}")
    except Exception as e:
        logger.error(f"TG photo error: {e}")
        _tg(caption)  # fallback to text


class CopyTrader:
    def __init__(self, target_wallets: list, coins: list, order_size_usd: float, shadow: bool = False):
        self.targets = set()
        for w in target_wallets:
            w = w.lower().strip()
            if len(w) < 20:
                raise ValueError(f"Wallet must be full address, got {len(w)} chars: {w}")
            self.targets.add(w)
        # Preserve original case for coins like kSHIB, kBONK, kPEPE
        self.coins = [c.strip() for c in coins]
        self.order_size = order_size_usd
        self._deploy_time = datetime.now(timezone.utc)

        # HL SDK
        self.private_key = os.environ["HL_PRIVATE_KEY"]
        self.agent_address = os.environ["HL_ADDRESS"]
        self.parent_address = os.environ.get(
            "HL_QUERY_ADDRESS", "0x11ca20aeb7cd014cf8406560ae405b12601994b4"
        )
        self.account = eth_account.Account.from_key(self.private_key)

        # Retry SDK init
        for attempt in range(5):
            try:
                self.info = Info(HL_API, skip_ws=True)
                self.exchange = Exchange(self.account, HL_API, account_address=self.agent_address)
                break
            except Exception as e:
                wait = (attempt + 1) * 10
                logger.warning(f"SDK init attempt {attempt+1} failed, waiting {wait}s: {e}")
                time.sleep(wait)

        # Get asset metadata (sz_decimals + per-coin max leverage)
        meta = self.info.meta_and_asset_ctxs()
        self.sz_decimals = {}
        self.max_leverage = {}  # coin → min(hl_max, MAX_LEVERAGE_CAP)
        if meta and len(meta) == 2:
            for u in meta[0]["universe"]:
                self.sz_decimals[u["name"]] = u.get("szDecimals", 2)
                self.max_leverage[u["name"]] = min(u.get("maxLeverage", 3), MAX_LEVERAGE_CAP)

        # MongoDB
        self.db = MongoClient("mongodb://localhost:27017").quants_lab

        # V9: Exchange state cache (source of truth for margin, equity, positions)
        self._equity_cache = None  # None = never fetched (blocks trading until first fetch)
        self._equity_cache_ts = 0
        self._exch_margin_used = 0.0  # from exchange marginSummary.totalMarginUsed
        self._exch_unrealized_pnl = 0.0  # from exchange per-position unrealizedPnl
        self._exch_positions = {}  # coin → {marginUsed, positionValue, unrealizedPnl, szi}
        self._pending_margin = 0.0  # inflight margin for concurrent entry prevention (F1)

        # State
        self.positions = []  # [{coin, side, entry_px, entry_time, size, wallet, notional}]
        self.last_entry = {}  # (wallet, coin) → timestamp (cooldown)
        self.mid_prices = {}
        self.running = True
        # R4-F7: Load lifetime PnL from MongoDB instead of resetting to 0.
        # Kill switch should track from strategy inception, not from last restart.
        v9_epoch = datetime(2026, 5, 9, 23, 0, 0)  # V9 inception: May 10 01:00 CEST
        lifetime_trades = list(self.db["v10_copy_trades"].find(
            {"timestamp": {"$gte": v9_epoch}}
        ))
        self.total_trades = len(lifetime_trades)
        self.total_pnl = sum(t.get("pnl_usd", 0) for t in lifetime_trades)
        if self.total_trades > 0:
            logger.info(f"LOADED lifetime PnL: {self.total_trades} trades, ${self.total_pnl:+.4f}")
        self._last_stats = 0
        self._last_reconcile = 0

        # Position lifecycle tracking: accumulated notional per (wallet, coin)
        # Used for %-based exit: only exit when reverse flow > 30% of accumulated
        self._position_accumulated = {}  # (wallet, coin) → total entry notional

        # TWAP aggregation: accumulate target fills before entering
        # Key: (wallet, coin) → {first_ts, buys_notional, sells_notional, count, last_ts}
        # Codex R3 fix: keyed by (wallet, coin) to prevent multi-wallet signal merging
        self._twap_buffer = {}
        self._twap_entered = set()  # (wallet, coin, first_ts_rounded) already acted on
        self._twap_completed_ts = {}  # (wallet, coin) -> timestamp of last TWAP completion
        self._mid_price_ts = {}  # coin -> timestamp of last mid-price update

        # Exit TWAP tracking: detect reverse flow from target via trade stream
        # Key: (wallet, coin) → {first_ts, reverse_notional, count, last_ts}
        self._exit_twap_buffer = {}

        # L2 book depth cache: coin → {bid_usd, ask_usd, ts}
        self._book_depth = {}

        # Target position tracking: detect opening vs closing
        # Key: target_address → {coin → position_size}
        self.shadow_mode = shadow
        self._target_positions = {}
        self._init_target_positions()

        # Position recovery: pick up orphaned positions from exchange
        # V10: skip in shadow mode (don't claim V9's positions)
        if not self.shadow_mode:
            self._recover_positions()

    def _init_target_positions(self):
        """Snapshot each target wallet's current positions on startup."""
        self._target_init_failed = set()  # track which targets returned NULL/error
        for addr in self.targets:
            self._target_positions[addr] = {}
            try:
                r = requests.post(f"{HL_API}/info", json={
                    "type": "clearinghouseState", "user": addr
                }, timeout=5)
                data = r.json()
                if data is None:
                    logger.warning(f"TARGET INIT: {addr[:14]} returned NULL (agent key)")
                    self._target_init_failed.add(addr)
                    continue
                for p in data.get("assetPositions", []):
                    pos = p["position"]
                    coin = pos["coin"]
                    sz = float(pos["szi"])
                    self._target_positions[addr][coin] = sz
                    logger.info(f"TARGET INIT: {addr[:14]} holds {coin} {sz}")
                time.sleep(0.3)
            except Exception as e:
                logger.warning(f"Can't init positions for {addr[:14]}: {e}")
                self._target_init_failed.add(addr)

        flat = sum(1 for a in self._target_positions if not self._target_positions[a])
        logger.info(f"Target positions initialized: {flat}/{len(self.targets)} flat")

    def _recover_positions(self):
        """On startup, recover orphaned positions from exchange.
        Match each exchange position to a target wallet holding the same coin+direction.
        Auto-close orphans with no matching target."""
        try:
            r = requests.post(f"{HL_API}/info", json={
                "type": "clearinghouseState", "user": self.parent_address
            }, timeout=5)
            data = r.json()
            if not data:
                return

            for ap in data.get("assetPositions", []):
                pos = ap["position"]
                coin = pos["coin"]
                sz = float(pos.get("szi", 0))
                entry_px = float(pos.get("entryPx", 0))
                # R4-F1: Use notional threshold, not raw coin amount.
                # 0.001 BTC = $80+ which is a real position. Skip only true dust (<$1).
                notional = abs(sz) * entry_px
                if notional < 1.0 and abs(sz) < 1e-9:
                    continue
                side = "BUY" if sz > 0 else "SELL"

                # Find matching target wallet
                # For agent-key wallets (clearinghouseState returned NULL),
                # _target_positions may be empty — don't treat as "no match"
                matched_wallet = None
                any_target_unknown = False
                for addr in self.targets:
                    target_data = self._target_positions.get(addr, {})
                    # Check if this target's init failed (agent key → empty dict, no data)
                    if not target_data and addr in self._target_init_failed:
                        any_target_unknown = True
                        # Assume match if we can't verify — safer than orphan-closing
                        if not matched_wallet:
                            matched_wallet = addr
                        continue
                    target_sz = target_data.get(coin, 0)
                    # Same direction? (both long or both short)
                    if (target_sz > 0 and sz > 0) or (target_sz < 0 and sz < 0):
                        matched_wallet = addr
                        break

                if matched_wallet:
                    self.positions.append({
                        'coin': coin, 'side': side, 'entry_px': entry_px,
                        'entry_time': time.time(), 'fill_time': time.time(),
                        'size': abs(sz), 'oid': 0, 'filled': True,
                        'wallet': matched_wallet, 'target_coin': coin,
                        '_recovered': True,
                    })
                    # Codex R6 fix #4: seed accumulated for recovered positions
                    acc_key = (matched_wallet, coin)
                    self._position_accumulated[acc_key] = abs(sz) * entry_px
                    # Codex R8: seed mid-price from entry_px so phantom check
                    # doesn't see coin as "stale" before first WS book update
                    if coin not in self._mid_price_ts:
                        self.mid_prices[coin] = entry_px
                        self._mid_price_ts[coin] = time.time()
                    logger.info(f"RECOVERED: {coin} {side} {abs(sz)} @ {entry_px} → tracking {matched_wallet[:14]}")
                    _tg(f"♻️ RECOVERED: {coin} {side} @ {entry_px} → {matched_wallet[:10]}")
                elif coin.upper() in [c.upper() for c in self.coins]:
                    # R3-F3: Don't auto-close orphans on recovery. Other strategies
                    # (e.g. X9 funding crowding) may hold positions on the same account.
                    # Just warn and let the operator decide.
                    notional = abs(sz) * entry_px
                    logger.warning(
                        f"UNMATCHED: {coin} {side} {abs(sz)} @ {entry_px} (${notional:.2f}) "
                        f"— no target wallet match. NOT closing (may belong to another strategy)."
                    )
                    _tg(f"⚠️ UNMATCHED on restart: {coin} {side} ${notional:.2f} — not closing (manual review needed)")

            logger.info(f"Position recovery: {len(self.positions)} recovered (V9 margin-based sizing)")
        except Exception as e:
            logger.warning(f"Position recovery failed: {e}")

    def _get_book_depth(self, coin: str) -> dict:
        """Get L2 book depth in USD for a coin. WS-only, no REST calls."""
        return self._book_depth.get(coin, {"bid_usd": 0, "ask_usd": 0, "best_bid": 0, "best_ask": 0, "ts": 0})

    def _update_book_depth_from_ws(self, coin: str, levels: list):
        """Update book depth cache from WS l2Book stream.
        Stores: best bid/ask prices, depth in USD, timestamp.
        This is the ONLY source of book data — no REST calls needed."""
        try:
            best_bid = float(levels[0][0]["px"]) if levels[0] else 0
            best_ask = float(levels[1][0]["px"]) if levels[1] else 0
            bid_usd = sum(float(l["sz"]) * float(l["px"]) for l in levels[0][:10]) if levels[0] else 0
            ask_usd = sum(float(l["sz"]) * float(l["px"]) for l in levels[1][:10]) if levels[1] else 0
            self._book_depth[coin] = {
                "best_bid": best_bid, "best_ask": best_ask,
                "bid_usd": bid_usd, "ask_usd": ask_usd,
                "ts": time.time(),
            }
        except:
            pass

    def _query_target_position(self, addr: str, coin: str) -> Optional[float]:
        """Get target's current position size for a coin.
        Returns None if API returns NULL (agent key) or on error.
        Returns 0.0 only if the wallet is queryable but has no position in this coin."""
        try:
            r = requests.post(f"{HL_API}/info", json={
                "type": "clearinghouseState", "user": addr
            }, timeout=5)
            data = r.json()
            if data is None:
                # Agent key — clearinghouseState returns NULL
                return None
            for p in data.get("assetPositions", []):
                pos = p["position"]
                if pos["coin"] == coin:
                    return float(pos["szi"])
            return 0.0
        except:
            return None  # Network error — don't treat as flat

    def _is_opening_trade(self, wallet: str, coin: str, is_buy: bool) -> bool:
        """Determine if the target's TWAP is an opening (increase) or closing (decrease).

        Compare their position before the burst vs direction of trade:
        - Was flat, now trading → OPENING
        - Was long, now buying more → ADDING (treat as opening)
        - Was long, now selling → CLOSING (don't copy)
        - Was short, now selling more → ADDING
        - Was short, now buying → CLOSING (don't copy)
        """
        prev_sz = self._target_positions.get(wallet, {}).get(coin, 0)

        # R4-F1: Use notional threshold for "flat" detection, not raw coin amount.
        # A target holding 0.0005 BTC ($40+) is NOT flat.
        mid = self.mid_prices.get(coin, 0)
        prev_notional = abs(prev_sz) * mid if mid > 0 else abs(prev_sz) * 1
        if prev_notional < 1.0 and abs(prev_sz) < 1e-9:
            # Was flat → any trade is an opening
            return True

        if prev_sz > 0 and is_buy:
            # Long and buying more → adding to position
            return True
        if prev_sz < 0 and not is_buy:
            # Short and selling more → adding to position
            return True

        # Otherwise: reducing/closing
        return False

    # ── V9: Margin-based risk management (exchange as source of truth) ─────

    def _refresh_exchange_state(self) -> bool:
        """Fetch full account state from exchange. Cached for 30s, busted on fills.

        Populates:
          _exch_equity: accountValue + spot USDC
          _exch_margin_used: totalMarginUsed (from exchange, the REAL number)
          _exch_unrealized_pnl: sum of per-position unrealizedPnl
          _exch_positions: {coin: {marginUsed, positionValue, unrealizedPnl, szi}}
        """
        now = time.time()
        if self._equity_cache is not None and now - self._equity_cache_ts < 30:
            return True
        try:
            r1 = requests.post(
                HL_API + "/info",
                json={"type": "clearinghouseState", "user": self.parent_address},
                timeout=5,
            )
            data = r1.json()
            margin = data.get("marginSummary", {})
            acct_val = float(margin.get("accountValue", 0))
            self._exch_margin_used = float(margin.get("totalMarginUsed", 0))

            # Per-position data from exchange
            self._exch_positions = {}
            total_upnl = 0.0
            for p in data.get("assetPositions", []):
                pos = p.get("position", {})
                coin = pos.get("coin", "")
                szi = float(pos.get("szi", 0))
                if abs(szi) > 1e-10:
                    mu = float(pos.get("marginUsed", 0))
                    upnl = float(pos.get("unrealizedPnl", 0))
                    pv = float(pos.get("positionValue", 0))
                    self._exch_positions[coin] = {
                        "marginUsed": mu, "positionValue": pv,
                        "unrealizedPnl": upnl, "szi": szi,
                    }
                    total_upnl += upnl
            self._exch_unrealized_pnl = total_upnl

            # Spot USDC
            r2 = requests.post(
                HL_API + "/info",
                json={"type": "spotClearinghouseState", "user": self.parent_address},
                timeout=5,
            )
            spot = sum(
                float(b.get("total", 0))
                for b in r2.json().get("balances", [])
                if b.get("coin") == "USDC"
            )

            # R4-F3: HL unified account — spot USDC IS total capital.
            # Don't add accountValue + spot (double-counting).
            # Spot USDC is the single source of truth for equity.
            self._equity_cache = spot
            self._equity_cache_ts = now
            return True
        except Exception as e:
            logger.warning(f"Exchange state fetch failed: {e}")
            return self._equity_cache is not None  # OK if we have stale data

    def _get_equity(self) -> Optional[float]:
        """Get account equity (exchange-sourced, cached)."""
        self._refresh_exchange_state()
        return self._equity_cache

    def _get_coin_leverage(self, coin: str) -> int:
        """Get leverage for a coin, capped at MAX_LEVERAGE_CAP."""
        return self.max_leverage.get(coin, 3)

    def _check_margin_budget(self, coin: str, additional_notional: float) -> bool:
        """Check if we can afford this entry. Uses EXCHANGE margin data as source of truth.

        Returns True if:
        - equity is available (not None)
        - total margin utilization stays under MAX_MARGIN_UTIL
        - per-coin concentration stays under MAX_COIN_CONCENTRATION
        - per-coin notional stays under MAX_ADDON_MULTIPLIER * order_size
        """
        if not self._refresh_exchange_state():
            logger.warning(f"Margin check blocked: no exchange data")
            return False

        equity = self._equity_cache
        if equity is None or equity <= 0:
            logger.warning(f"Margin check blocked: equity={equity}")
            return False

        # Exchange-sourced margin + our pending (inflight orders not yet on-chain)
        lev = self._get_coin_leverage(coin)
        additional_margin = additional_notional / lev
        total_margin = self._exch_margin_used + self._pending_margin + additional_margin
        util = total_margin / equity
        if util > MAX_MARGIN_UTIL:
            logger.info(
                f"Margin BLOCKED {coin}: total util {util:.0%} > {MAX_MARGIN_UTIL:.0%} "
                f"(exch_margin=${self._exch_margin_used:.2f} + pending=${self._pending_margin:.2f} "
                f"+ new=${additional_margin:.2f} / equity=${equity:.2f})"
            )
            return False

        # Per-coin concentration: exchange position margin + additional
        coin_data = self._exch_positions.get(coin, {})
        coin_margin = coin_data.get("marginUsed", 0) + additional_margin
        coin_util = coin_margin / equity
        if coin_util > MAX_COIN_CONCENTRATION:
            logger.info(
                f"Margin BLOCKED {coin}: coin concentration {coin_util:.0%} > {MAX_COIN_CONCENTRATION:.0%}"
            )
            return False

        # Per-coin notional cap (max add-ons)
        coin_notional = coin_data.get("positionValue", 0) + additional_notional
        if coin_notional > MAX_ADDON_MULTIPLIER * self.order_size:
            logger.info(
                f"Margin BLOCKED {coin}: notional ${coin_notional:.0f} > {MAX_ADDON_MULTIPLIER}x base ${self.order_size}"
            )
            return False

        return True

    def _compute_unrealized_pnl(self) -> float:
        """Get unrealized PnL from exchange (not estimated locally)."""
        self._refresh_exchange_state()
        return getattr(self, '_exch_unrealized_pnl', 0.0)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _round_price(self, px: float) -> float:
        if px <= 0:
            return 0.0
        mag = math.floor(math.log10(abs(px)))
        decimals = min(4 - mag, 5)
        return round(px, max(decimals, 0))

    def _round_size(self, coin: str, sz: float) -> float:
        dec = self.sz_decimals.get(coin, 2)
        return round(sz, dec)

    async def _enter_position(self, coin: str, is_buy: bool, twap_dedup_key=None, wallet: str = None):
        """Place an order to copy the target wallet's trade. Supports add-ons (V9)."""
        # R4-F9: Block new entries when kill switch is active
        if getattr(self, '_kill_switch_active', False):
            logger.debug(f"Entry blocked (kill switch active): {coin}")
            return

        now = time.time()
        twap_wallet = wallet if wallet else "unknown"

        # Cooldown check — per (wallet, coin) not just coin
        cooldown_key = (twap_wallet, coin)
        if now - self.last_entry.get(cooldown_key, 0) < COOLDOWN_S:
            logger.debug(f"Cooldown active for {coin} from {twap_wallet[:10]}")
            return

        # V9: Check existing position for add-on vs new entry
        existing = None
        for p in self.positions:
            if p.get("filled") and p["coin"] == coin and p.get("wallet") == twap_wallet:
                existing = p
                break

        if existing:
            # Add-on: same wallet, same coin, same direction
            if (existing["side"] == "BUY") != is_buy:
                logger.debug(f"Skipping {coin}: existing {existing['side']}, new {'BUY' if is_buy else 'SELL'}")
                return
            logger.info(f"ADD-ON: {twap_wallet[:10]} {coin} — existing size={existing['size']}")

        # V9: Margin budget check (replaces MAX_POSITIONS)
        # Shadow mode: use simulated margin (exchange positions belong to V9)
        if getattr(self, 'shadow_mode', False):
            shadow_margin = sum(
                abs(p.get('size', 0) * p.get('entry_px', 0)) / self._get_coin_leverage(p['coin'])
                for p in self.positions if p.get('filled')
            )
            equity = self._equity_cache or 35.0  # fallback equity for shadow
            shadow_util = (shadow_margin + self.order_size / self._get_coin_leverage(coin)) / equity
            if shadow_util > MAX_MARGIN_UTIL:
                logger.info(f"SHADOW margin blocked {coin}: {shadow_util:.0%} util (shadow positions only)")
                return
        elif not self._check_margin_budget(coin, self.order_size):
            return

        # Use WS-fed book data — zero REST calls
        book = self._book_depth.get(coin)
        if not book or book.get("ts", 0) == 0 or book.get("best_bid", 0) <= 0:
            logger.debug(f"Entry skipped for {coin}: no WS book data yet")
            return
        best_bid = book["best_bid"]
        best_ask = book["best_ask"]
        mid = (best_bid + best_ask) / 2
        self.mid_prices[coin] = mid
        self._mid_price_ts[coin] = time.time()

        sz = self._round_size(coin, self.order_size / mid)
        if sz <= 0:
            return

        # F1 fix: track pending margin BEFORE await to prevent concurrent over-commitment
        lev = self._get_coin_leverage(coin)
        pending_add = (self.order_size) / lev
        self._pending_margin += pending_add

        # ── V10: Shadow mode — simulate fill without placing order ──
        if getattr(self, 'shadow_mode', False):
            fill_px = mid
            fill_sz = sz
            self.positions.append({
                "coin": coin, "side": "BUY" if is_buy else "SELL",
                "entry_px": fill_px, "entry_time": now, "fill_time": now,
                "size": fill_sz, "oid": 0, "filled": True,
                "wallet": twap_wallet, "target_coin": coin,
                "_shadow": True,
            })
            self.last_entry[cooldown_key] = now
            logger.info(
                f"SHADOW ENTRY: {coin} {'BUY' if is_buy else 'SELL'} {fill_sz:.4f} @ {fill_px:.4f} "
                f"(simulated, no order placed)"
            )
            # Log to MongoDB for analysis
            self.db["v10_shadow_signals"].insert_one({
                "type": "entry", "coin": coin, "side": "BUY" if is_buy else "SELL",
                "sim_fill_px": fill_px, "sim_fill_sz": fill_sz,
                "mid_at_signal": mid, "spread_bps": (best_ask - best_bid) / mid * 10000,
                "wallet": twap_wallet,
                "timestamp": datetime.now(timezone.utc),
            })
            self._pending_margin = max(0, self._pending_margin - pending_add)
            return

        # IOC taker entry
        try:
            if is_buy:
                px = self._round_price(best_ask * 1.003)
            else:
                px = self._round_price(best_bid * 0.997)

            result = await asyncio.to_thread(
                self.exchange.order, coin, is_buy, sz, px, {"limit": {"tif": "Ioc"}}
            )
            statuses = result.get("response", {}).get("data", {}).get("statuses", [{}])

            if statuses and "filled" in statuses[0]:
                fill_px = float(statuses[0]["filled"]["avgPx"])
                fill_sz = float(statuses[0]["filled"].get("totalSz", sz))

                # V10: No add-ons. New position only.
                self.positions.append({
                    "coin": coin, "side": "BUY" if is_buy else "SELL",
                    "entry_px": fill_px, "entry_time": now, "fill_time": now,
                    "size": fill_sz, "oid": 0, "filled": True,
                    "wallet": twap_wallet, "target_coin": coin,
                })
                logger.info(
                    f"ENTRY FILLED (IOC): {coin} {'BUY' if is_buy else 'SELL'} {fill_sz} @ {fill_px}"
                )
                _tg(f"V10 FILL: {coin} {'BUY' if is_buy else 'SELL'} {fill_sz} @ {fill_px}")

                self.last_entry[cooldown_key] = now
                self._equity_cache_ts = 0

            elif statuses and "error" in statuses[0]:
                logger.warning(f"Entry rejected: {statuses[0]['error']}")

        except Exception as e:
            logger.error(f"Entry error: {e}")
        finally:
            self._pending_margin = max(0, self._pending_margin - pending_add)

    async def _check_exits(self):
        """Exit when TARGET exits, not on fixed timer.
        Poll target's position every EXIT_POLL_S. When target reduces >50%, we exit.
        Hard max hold as safety net."""
        now = time.time()
        still_open = []
        exited_ids = set()

        for pos in self.positions:
            if not pos['filled']:
                still_open.append(pos)
                continue

            fill_elapsed = now - pos.get('fill_time', pos['entry_time'])
            wallet = pos.get('wallet', '')
            coin = pos['coin']
            exit_params = WALLET_EXIT_PARAMS.get(wallet, DEFAULT_EXIT_PARAMS)

            # ── Compute current PnL in bps ──
            mid = self.mid_prices.get(coin, 0)
            entry_px = pos.get('entry_px', 0)
            if mid > 0 and entry_px > 0:
                if pos['side'] == 'BUY':
                    current_pnl_bps = (mid - entry_px) / entry_px * 10000
                else:
                    current_pnl_bps = (entry_px - mid) / entry_px * 10000
            else:
                current_pnl_bps = 0.0

            # Track peak PnL for trailing stop
            peak_key = "_peak_pnl_bps"
            pos[peak_key] = max(pos.get(peak_key, current_pnl_bps), current_pnl_bps)
            peak_pnl_bps = pos[peak_key]

            _risk_exit_attempted = False

            # ── EXIT LAYER 1: Hard stop-loss (per-wallet, replaces old HARD_STOP_BPS) ──
            sl_bps = exit_params.get("sl_bps", -500)
            if sl_bps is not None and mid > 0 and entry_px > 0 and current_pnl_bps <= sl_bps:
                _risk_exit_attempted = True
                logger.warning(f"HARD SL: {coin} at {current_pnl_bps:.0f}bps (limit: {sl_bps}bps)")
                _tg(f"🛑 HARD SL: {coin} at {current_pnl_bps:.0f}bps")
                exited = await self._exit_position(pos)
                if exited:
                    acc_key = (wallet, coin)
                    self._position_accumulated.pop(acc_key, None)
                    self._exit_twap_buffer.pop(acc_key, None)
                    exited_ids.add(id(pos))
                    continue

            # ── EXIT LAYER 2: Trailing stop ──
            trail_activate = exit_params.get("trail_activate_bps")
            trail_dist = exit_params.get("trail_bps")
            if not _risk_exit_attempted and trail_activate is not None and trail_dist is not None and mid > 0 and entry_px > 0:
                if peak_pnl_bps >= trail_activate and (peak_pnl_bps - current_pnl_bps) >= trail_dist:
                    _risk_exit_attempted = True
                    logger.warning(
                        f"TRAILING STOP: {coin} peak={peak_pnl_bps:.0f}bps, "
                        f"current={current_pnl_bps:.0f}bps, trail={trail_dist}bps"
                    )
                    _tg(f"📉 TRAILING STOP: {coin} peak={peak_pnl_bps:.0f} now={current_pnl_bps:.0f}bps")
                    exited = await self._exit_position(pos)
                    if exited:
                        acc_key = (wallet, coin)
                        self._position_accumulated.pop(acc_key, None)
                        self._exit_twap_buffer.pop(acc_key, None)
                        exited_ids.add(id(pos))
                        continue

            # ── EXIT LAYER 4: Per-wallet max hold (safety valve) ──
            wallet_max_hold = exit_params.get("max_hold_s", MAX_HOLD_S)
            if fill_elapsed >= wallet_max_hold:
                exit_attempts = pos.get('_exit_attempts', 0) + 1
                pos['_exit_attempts'] = exit_attempts
                if exit_attempts > 10:
                    if not pos.get('_gave_up'):
                        logger.error(f"GIVING UP on {pos['coin']} after {exit_attempts} exit attempts — removing from tracker")
                        _tg(f"🛑 GAVE UP exiting {pos['coin']} after {exit_attempts} attempts — position dropped from tracker")
                        pos['_gave_up'] = True
                    # Codex R6 fix #5: clean accumulated on give-up
                    acc_key = (pos.get('wallet', ''), pos['coin'])
                    self._position_accumulated.pop(acc_key, None)
                    self._exit_twap_buffer.pop(acc_key, None)
                    continue  # drop position, don't add to still_open
                logger.warning(f"MAX HOLD reached for {pos['coin']} — force closing (attempt {exit_attempts})")
                if exit_attempts == 1:
                    _tg(f"⏰ MAX HOLD: {pos['coin']} — force closing after {MAX_HOLD_S}s")
                exited = await self._exit_position(pos)
                if exited:
                    # Codex R6 fix #5: clean accumulated on max-hold exit
                    acc_key = (pos.get('wallet', ''), pos['coin'])
                    self._position_accumulated.pop(acc_key, None)
                    self._exit_twap_buffer.pop(acc_key, None)
                else:
                    still_open.append(pos)
                continue

            # PRIMARY EXIT: trade-stream based reverse TWAP detection
            # Codex R3 fix: don't poll clearinghouseState (returns NULL for agent keys)
            wallet = pos.get('wallet', '')
            coin = pos['coin']
            exit_key = (wallet, coin)

            if exit_key in self._exit_twap_buffer:
                ebuf = self._exit_twap_buffer[exit_key]
                exit_elapsed = now - ebuf['first_ts']

                # R4-F2: PROPORTIONAL EXIT — trim in concert with target wallet.
                # Instead of binary "exit all or nothing", calculate what % the target
                # has reduced and trim our position by the same %.
                wallet_exit = WALLET_EXIT_TYPE.get(wallet, "GRADUAL")
                acc_key = (wallet, coin)
                accumulated = self._position_accumulated.get(acc_key, 0)

                # Calculate trim percentage: how much has target reversed vs our entry?
                reverse_notional = ebuf['reverse_notional']
                # R4-F5: If accumulated is zero/missing (recovered position with bad data),
                # use current position notional as denominator instead of defaulting to 100%.
                if accumulated <= 0:
                    mid = self.mid_prices.get(coin, 0)
                    accumulated = pos['size'] * mid if mid > 0 else pos['size'] * pos.get('entry_px', 1)
                    self._position_accumulated[acc_key] = accumulated  # seed it
                trim_pct = reverse_notional / accumulated if accumulated > 0 else 0.0
                trim_pct = min(trim_pct, 1.0)  # cap at 100%

                # Determine if we should act
                should_trim = False
                if wallet_exit == "FIRST_CLOSE":
                    # FIRST_CLOSE: act on any meaningful reverse (>5% or >$3)
                    should_trim = (trim_pct >= EXIT_MIN_TRIM_PCT
                                   or reverse_notional >= EXIT_MIN_TRIM_USD)
                elif exit_elapsed >= EXIT_TWAP_WINDOW_S:
                    # GRADUAL: wait for window, then trim proportionally if >5%
                    should_trim = (trim_pct >= EXIT_MIN_TRIM_PCT
                                   or reverse_notional >= EXIT_MIN_TRIM_USD)
                # else: still within TWAP window, wait

                if should_trim:
                    # Log the trim
                    trim_pct_display = trim_pct * 100
                    is_full_exit = trim_pct >= 0.90  # treat 90%+ as full exit
                    if not pos.get('_exit_logged') or pos.get('_last_trim_pct', 0) != round(trim_pct, 2):
                        action_str = "FULL EXIT" if is_full_exit else f"TRIM {trim_pct_display:.0f}%"
                        logger.info(
                            f"TARGET {action_str} ({wallet_exit}): {wallet[:10]} {coin} "
                            f"reverse=${reverse_notional:,.0f} = {trim_pct_display:.0f}% of ${accumulated:,.0f}"
                        )
                        _tg(f"{'🚨' if is_full_exit else '✂️'} {action_str}: {coin} — {'closing' if is_full_exit else 'trimming'}")
                        pos['_exit_logged'] = True
                        pos['_last_trim_pct'] = round(trim_pct, 2)

                    # R4-F6: Don't mutate target position BEFORE exit succeeds.
                    # Moved to after successful exit below. Otherwise, failed exits
                    # leave target state incorrect, causing bad entry/add-on decisions.

                    # Cooldown on exit retries
                    last_exit_attempt = pos.get('_last_exit_attempt', 0)
                    if now - last_exit_attempt < 10:
                        still_open.append(pos)
                        continue
                    pos['_last_exit_attempt'] = now

                    if is_full_exit:
                        # Full exit: close entire position
                        exited = await self._exit_position(pos)
                        if exited:
                            # R4-F6: NOW mutate target position (after confirmed exit)
                            pos_sz = self._target_positions.get(wallet, {}).get(coin, 0)
                            self._target_positions.setdefault(wallet, {})[coin] = 0
                            del self._exit_twap_buffer[exit_key]
                            self._position_accumulated.pop(acc_key, None)
                        else:
                            still_open.append(pos)
                    else:
                        # Proportional trim: reduce position by trim_pct
                        trim_size = pos['size'] * trim_pct
                        exited = await self._exit_position(pos, trim_size=trim_size)
                        if exited:
                            # R4-F6: Mutate target position proportionally (after confirmed trim)
                            pos_sz = self._target_positions.get(wallet, {}).get(coin, 0)
                            reverse_sz = reverse_notional / (self.mid_prices.get(coin, 1) or 1)
                            if pos_sz > 0:
                                self._target_positions.setdefault(wallet, {})[coin] = max(0, pos_sz - reverse_sz)
                            else:
                                self._target_positions.setdefault(wallet, {})[coin] = min(0, pos_sz + reverse_sz)
                            # Reduce accumulated
                            self._position_accumulated[acc_key] = max(0, accumulated - reverse_notional)
                            del self._exit_twap_buffer[exit_key]
                            pos['_exit_logged'] = False  # reset for next trim
                            if pos['size'] * (self.mid_prices.get(coin, 1) or 1) < 1.0:
                                # Dust remaining — close fully
                                pass  # don't add to still_open
                            else:
                                still_open.append(pos)
                        else:
                            still_open.append(pos)
                    continue

                elif exit_elapsed >= EXIT_TWAP_WINDOW_S and not should_trim and wallet_exit == "GRADUAL":
                    # Sub-threshold reverse for GRADUAL: reduce accumulated, clear buffer
                    if acc_key in self._position_accumulated:
                        self._position_accumulated[acc_key] = max(0,
                            self._position_accumulated[acc_key] - ebuf['reverse_notional'])
                    del self._exit_twap_buffer[exit_key]

            # FALLBACK EXIT: position polling DISABLED
            # Codex R5: all 3 target wallets are agent keys (clearinghouseState returns NULL).
            # Polling burns API rate limit for zero benefit. Trade-stream exit is primary.

            still_open.append(pos)

        # R4-F5: Merge still_open with any NEW entries that were appended during awaits.
        # Entries added by _enter_position during our iteration are at indices > original length.
        # Use set of IDs to detect which positions were in our iteration vs newly added.
        known_ids = {id(pos) for pos in still_open} | exited_ids
        new_during_exit = [p for p in self.positions if id(p) not in known_ids]
        self.positions = still_open + new_during_exit

    async def _exit_position(self, pos: dict, trim_size: float = None) -> bool:
        """Exit a position (full or partial trim).
        In shadow mode: simulate exit at mid price and log.
        In live mode: try maker first, IOC fallback.
        Returns True if exit order filled, False if not."""
        coin = pos['coin']
        is_buy = pos['side'] == 'SELL'  # reverse direction to exit
        exit_sz = trim_size if trim_size is not None else pos['size']
        sz = self._round_size(coin, exit_sz)
        if sz <= 0:
            logger.warning(f"Exit skip {coin}: size {pos['size']} rounds to 0")
            return False

        # ── V10: Shadow exit ──
        if getattr(self, 'shadow_mode', False):
            mid = self.mid_prices.get(coin, 0)
            if mid <= 0:
                return False
            entry_px = pos.get('entry_px', mid)
            if pos['side'] == 'BUY':
                pnl_bps = (mid - entry_px) / entry_px * 10000
            else:
                pnl_bps = (entry_px - mid) / entry_px * 10000
            hold_s = time.time() - pos.get('fill_time', pos.get('entry_time', time.time()))
            logger.info(
                f"SHADOW EXIT: {coin} {pos['side']} @ {mid:.4f} (entry {entry_px:.4f}) "
                f"PnL={pnl_bps:+.0f}bps hold={hold_s:.0f}s"
            )
            self.db["v10_shadow_signals"].insert_one({
                "type": "exit", "coin": coin, "side": pos['side'],
                "entry_px": entry_px, "exit_px": mid,
                "pnl_bps": pnl_bps, "hold_s": hold_s,
                "wallet": pos.get('wallet', ''),
                "timestamp": datetime.now(timezone.utc),
            })
            self.total_trades += 1
            self.total_pnl += pnl_bps  # track in bps for shadow
            return True

        try:
            # Codex R5: removed per-exit on-chain position check (was causing 429 rate limits).
            # Phantom position protection now handled by the 10-attempt give-up in MAX_HOLD.

            # Use WS-fed book data — zero REST calls
            ws_book = self._book_depth.get(coin)
            if not ws_book or ws_book.get("best_bid", 0) <= 0:
                logger.debug(f"Exit deferred for {coin}: no WS book data")
                return False

            best_bid = ws_book["best_bid"]
            best_ask = ws_book["best_ask"]

            # Try maker exit first (at best bid/ask)
            if not pos.get('_maker_exit_tried'):
                if is_buy:
                    px = self._round_price(best_bid)
                else:
                    px = self._round_price(best_ask)

                result = await asyncio.to_thread(
                    self.exchange.order, coin, is_buy, sz, px,
                    {"limit": {"tif": "Alo"}}, True  # reduce_only
                )
                statuses = result.get("response", {}).get("data", {}).get("statuses", [{}])
                if statuses and "resting" in statuses[0]:
                    pos['exit_oid'] = statuses[0]["resting"]["oid"]
                    pos['_maker_exit_tried'] = True
                    pos['_maker_exit_time'] = time.time()
                    logger.info(f"EXIT MAKER: {coin} {'BUY' if is_buy else 'SELL'} {sz} @ {px}")
                    return False  # not done yet, wait for fill

            # Codex R4 #7: proper maker exit state machine
            # If maker exit pending and < 60s, wait (don't fall through to IOC)
            if pos.get('_maker_exit_tried'):
                elapsed_since_maker = time.time() - pos.get('_maker_exit_time', 0)
                if elapsed_since_maker < 60:
                    return False  # still waiting for maker fill
                # 60s passed — check if maker filled before cancelling
                if pos.get('exit_oid'):
                    # Check order status first: it may have filled while we waited
                    try:
                        status = await asyncio.to_thread(
                            self.info.query_order_by_oid, self.parent_address, int(pos['exit_oid']))
                        order_status = status.get("order", {}).get("status", "")
                        filled_sz_str = status.get("order", {}).get("origSz", "0")
                        if order_status == "filled":
                            # Maker DID fill — record PnL instead of falling through to IOC
                            exit_px = float(status["order"].get("limitPx", best_bid if not is_buy else best_ask))
                            filled_sz = float(filled_sz_str)
                            if pos['side'] == 'BUY':
                                pnl_bps = (exit_px - pos['entry_px']) / pos['entry_px'] * 10000
                            else:
                                pnl_bps = (pos['entry_px'] - exit_px) / pos['entry_px'] * 10000
                            pnl_usd = pnl_bps / 10000 * filled_sz * exit_px
                            self.total_pnl += pnl_usd
                            self.total_trades += 1
                            logger.info(
                                f"EXIT MAKER FILLED (detected at timeout): {coin} {pos['side']} "
                                f"entry={pos['entry_px']} exit={exit_px} sz={filled_sz} "
                                f"pnl={pnl_bps:+.1f}bp (${pnl_usd:+.4f}) total_pnl=${self.total_pnl:+.4f}")
                            self.db["v10_copy_trades"].insert_one({
                                "target_wallet": pos.get('wallet', str(self.targets)),
                                "coin": coin, "side": pos['side'],
                                "entry_px": pos['entry_px'], "exit_px": exit_px,
                                "size": filled_sz, "pnl_bps": pnl_bps, "pnl_usd": pnl_usd,
                                "exit_type": "maker_late_detect",
                                "hold_s": time.time() - pos.get('fill_time', pos['entry_time']),
                                "timestamp": datetime.now(timezone.utc),
                            })
                            self._equity_cache_ts = 0
                            pos.pop('exit_oid', None)
                            pos['_maker_exit_tried'] = False
                            wallet = pos.get('wallet', '')
                            self._target_positions.setdefault(wallet, {})[coin] = 0
                            acc_key = (wallet, coin)
                            self._position_accumulated.pop(acc_key, None)
                            self._exit_twap_buffer.pop(acc_key, None)
                            return True  # fully exited via maker
                    except Exception as e:
                        logger.debug(f"Order status check failed for {coin}: {e}")
                    # Not filled — cancel and fall through to IOC
                    try:
                        await asyncio.to_thread(self.exchange.cancel, coin, int(pos['exit_oid']))
                    except:
                        pass
                pos['_maker_exit_tried'] = False  # reset so next call can retry maker
                pos.pop('exit_oid', None)
                logger.info(f"EXIT MAKER timeout — falling back to IOC for {coin}")

            # IOC fallback — use WS book prices with escalating aggression
            ioc_attempts = pos.get('_ioc_exit_attempts', 0)
            pos['_ioc_exit_attempts'] = ioc_attempts + 1
            # Escalate: 0.3% -> 1% -> 2% slippage on repeated failures
            if ioc_attempts < 2:
                slip = 0.003
            elif ioc_attempts < 4:
                slip = 0.01
            else:
                slip = 0.02
            if is_buy:
                px = self._round_price(best_ask * (1 + slip))
            else:
                px = self._round_price(best_bid * (1 - slip))

            if ioc_attempts > 0:
                logger.info(f"IOC EXIT {coin}: attempt {ioc_attempts + 1}, slip={slip*100:.1f}%, px={px}")

            result = await asyncio.to_thread(
                self.exchange.order, coin, is_buy, sz, px,
                {"limit": {"tif": "Ioc"}}, True  # reduce_only
            )
            statuses = result.get("response", {}).get("data", {}).get("statuses", [{}])

            if statuses and "filled" in statuses[0]:
                pos.pop('_ioc_exit_attempts', None)  # reset on success
                exit_px = float(statuses[0]["filled"]["avgPx"])
                filled_sz = float(statuses[0]["filled"].get("totalSz", sz))

                if pos['side'] == 'BUY':
                    pnl_bps = (exit_px - pos['entry_px']) / pos['entry_px'] * 10000
                else:
                    pnl_bps = (pos['entry_px'] - exit_px) / pos['entry_px'] * 10000

                pnl_usd = pnl_bps / 10000 * filled_sz * exit_px
                self.total_pnl += pnl_usd
                self.total_trades += 1

                # F4: invalidate equity cache after exit fill
                self._equity_cache_ts = 0

                # R2-F1: handle partial fills
                # R4-F2 fix: when trim_size is passed, a fill of trim_size is SUCCESS,
                # not a "partial fill failure". Only treat as partial if the IOC
                # couldn't fill the REQUESTED amount.
                requested_sz = sz  # the rounded trim_size or full size we asked for
                remainder = requested_sz - filled_sz
                is_partial = remainder > 1e-8  # IOC didn't fill what we asked

                # F5: reset target position tracking on FULL exit only
                wallet = pos.get('wallet', '')
                is_full_position_exit = (trim_size is None) and not is_partial
                if is_full_position_exit:
                    self._target_positions.setdefault(wallet, {})[coin] = 0

                logger.info(
                    f"EXIT{'(PARTIAL)' if is_partial else ''}: {coin} {pos['side']} "
                    f"entry={pos['entry_px']} exit={exit_px} filled={filled_sz}/{pos['size']} "
                    f"pnl={pnl_bps:+.1f}bp (${pnl_usd:+.4f}) total_pnl=${self.total_pnl:+.4f}"
                )

                self.db["v10_copy_trades"].insert_one({
                    "target_wallet": pos.get('wallet', str(self.targets)),
                    "coin": coin, "side": pos['side'],
                    "entry_px": pos['entry_px'], "exit_px": exit_px,
                    "size": filled_sz, "pnl_bps": pnl_bps, "pnl_usd": pnl_usd,
                    "exit_type": "ioc" + ("_partial" if is_partial else ""),
                    "hold_s": time.time() - pos.get('fill_time', pos['entry_time']),
                    "timestamp": datetime.now(timezone.utc),
                })

                if is_partial:
                    # R2-F1: IOC couldn't fill requested amount — retry later
                    pos['_maker_exit_tried'] = False
                    logger.warning(f"PARTIAL EXIT: {coin} filled={filled_sz}/{requested_sz} — will retry")
                    return False

                # R4-F2: Successful fill. For trims, reduce pos['size'] here.
                if trim_size is not None:
                    pos['size'] = pos['size'] - filled_sz
                    if pos['size'] * (self.mid_prices.get(coin, 1) or 1) < 1.0:
                        pos['size'] = 0  # dust, treat as fully closed
                return True  # successfully filled (full exit or trim)

            # IOC didn't fill — keep position tracked
            logger.warning(f"EXIT FAILED: {coin} IOC not filled — keeping position tracked")
            return False

        except Exception as e:
            logger.error(f"Exit error for {coin}: {e}")
            return False

    def _on_hl_trade(self, trade: dict):
        """Process HL WS trade — detect target wallet from users field.

        HL trade format: {coin, side, px, sz, time, hash, tid, users: [buyer, seller]}
        users is ALWAYS [buyer, seller] regardless of side field.
        side field indicates taker direction (B=taker bought, A=taker sold).
        Codex finding #1: do NOT flip users based on side.
        """
        coin = trade.get("coin", "")
        if coin not in self.coins:
            return

        users = trade.get("users", [])
        if len(users) < 2:
            return

        # users = [buyer, seller] ALWAYS (HL docs, verified against wallet collector)
        buyer = users[0].lower()
        seller = users[1].lower()

        is_target = False
        is_buy = False
        matched_wallet = ""

        if buyer in self.targets:
            is_target = True
            is_buy = True
            matched_wallet = buyer
        elif seller in self.targets:
            is_target = True
            is_buy = False
            matched_wallet = seller

        if not is_target:
            return

        sz = float(trade.get("sz", 0))
        px = float(trade.get("px", 0))
        notional = sz * px
        tid = trade.get("tid", "")

        # Codex #7: de-dupe by trade ID
        # Codex R3 fix: skip dedup if tid is empty/missing (would drop all no-tid trades)
        if not hasattr(self, '_seen_tids'):
            self._seen_tids = {}
        if tid:  # only dedup if tid is present
            if tid in self._seen_tids:
                return
            self._seen_tids[tid] = time.time()
        # Evict entries older than 5 minutes
        if len(self._seen_tids) > 10000:
            cutoff = time.time() - 300
            self._seen_tids = {k: v for k, v in self._seen_tids.items() if v > cutoff}

        # Store raw target fill for forensics
        self.db["hl_copy_target_fills"].insert_one({
            "wallet": matched_wallet, "coin": coin,
            "side": "BUY" if is_buy else "SELL",
            "price": px, "size": sz, "notional": notional,
            "timestamp": datetime.now(timezone.utc),
            "ts_epoch": time.time(),
        })

        # ── V10: IMMEDIATE ENTRY on first fill (no TWAP accumulation) ──
        now = time.time()

        logger.info(
            f"FILL: {matched_wallet[:10]} {coin} {'BUY' if is_buy else 'SELL'} "
            f"${notional:,.0f} @ {px}"
        )

        # Skip if we already have a position on this coin (no stacking)
        existing = None
        for pos in self.positions:
            if pos['coin'] == coin and pos['filled']:
                existing = pos
                break
        if existing:
            logger.debug(f"V10: skip {coin} — already have position")
            # But still feed exit TWAP buffer below
        else:
            # ── V10 Entry Guards ──
            wallet = matched_wallet
            cooldown_key = (wallet, coin)

            if now - self.last_entry.get(cooldown_key, 0) < COOLDOWN_S:
                logger.debug(f"V10: cooldown active for {coin}")
            elif not self._is_opening_trade(wallet, coin, is_buy):
                logger.debug(f"V10: not an opening trade for {coin}")
            elif not getattr(self, 'shadow_mode', False) and not self._check_margin_budget(coin, self.order_size):
                pass  # logged inside _check_margin_budget (skip in shadow mode)
            else:
                # Guard: chase distance — don't enter if price already moved too far
                mid = self.mid_prices.get(coin, 0)
                if mid > 0:
                    chase_bps = abs(mid - px) / px * 10000
                    if chase_bps > MAX_CHASE_BPS:
                        logger.info(f"V10 SKIP {coin}: chase {chase_bps:.0f}bps > {MAX_CHASE_BPS}bps")
                    else:
                        # Guard: spread
                        book = self._book_depth.get(coin, {})
                        bid = book.get("best_bid", 0)
                        ask = book.get("best_ask", 0)
                        spread_bps = (ask - bid) / mid * 10000 if mid > 0 and bid > 0 and ask > 0 else 999
                        if spread_bps > MAX_SPREAD_BPS:
                            logger.info(f"V10 SKIP {coin}: spread {spread_bps:.0f}bps > {MAX_SPREAD_BPS}bps")
                        else:
                            # Guard: book depth
                            depth = self._get_book_depth(coin)
                            entry_depth = depth["ask_usd"] if is_buy else depth["bid_usd"]
                            if entry_depth < MIN_BOOK_DEPTH_USD:
                                logger.info(f"V10 SKIP {coin}: depth ${entry_depth:.0f} < ${MIN_BOOK_DEPTH_USD}")
                            else:
                                # ALL GUARDS PASS — enter immediately
                                dedup_key = (wallet, coin, int(now))
                                logger.info(
                                    f"V10 ENTRY: {wallet[:10]} {coin} {'BUY' if is_buy else 'SELL'} "
                                    f"${notional:,.0f} — chase={chase_bps:.0f}bps spread={spread_bps:.0f}bps depth=${entry_depth:,.0f}"
                                )
                                _tg(f"V10 ENTRY: {coin} {'BUY' if is_buy else 'SELL'} — {wallet[:10]}")
                                self._twap_entered.add(dedup_key)
                                trade_sz = notional / mid if mid > 0 else 0
                                if not is_buy:
                                    trade_sz = -trade_sz
                                prev = self._target_positions.get(wallet, {}).get(coin, 0)
                                self._target_positions.setdefault(wallet, {})[coin] = prev + trade_sz
                                asyncio.get_event_loop().create_task(
                                    self._enter_position(coin, is_buy, twap_dedup_key=dedup_key, wallet=wallet)
                                )

        # --- EXIT TWAP: detect reverse flow for open positions ---
        # If we have an open position on this coin from this wallet,
        # and the target is now trading in the OPPOSITE direction, track it as exit signal
        for pos in self.positions:
            if pos['coin'] == coin and pos.get('wallet') == matched_wallet and pos['filled']:
                # Check if this trade is opposite to our position
                pos_is_long = pos['side'] == 'BUY'
                is_reverse = (pos_is_long and not is_buy) or (not pos_is_long and is_buy)
                if is_reverse:
                    exit_key = (matched_wallet, coin)
                    if exit_key not in self._exit_twap_buffer:
                        self._exit_twap_buffer[exit_key] = {
                            'first_ts': now, 'last_ts': now,
                            'reverse_notional': 0, 'count': 0,
                        }
                        logger.info(f"EXIT TWAP START: {matched_wallet[:14]} reversing on {coin}")
                    ebuf = self._exit_twap_buffer[exit_key]
                    ebuf['last_ts'] = now
                    ebuf['reverse_notional'] += notional
                    ebuf['count'] += 1

    def _on_order_update(self, updates: list):
        """Detect fills on our entry AND exit orders."""
        for update in updates:
            order = update.get("order", {})
            oid = order.get("oid")
            status = update.get("status")

            if status == "filled" and oid:
                for pos in self.positions:
                    # Entry fill
                    if pos['oid'] == oid and not pos['filled']:
                        pos['filled'] = True
                        pos['entry_px'] = float(order.get("limitPx", pos['entry_px']))
                        pos['fill_time'] = time.time()  # Codex fix #3: track actual fill time
                        logger.info(f"ENTRY FILLED: {pos['coin']} {pos['side']} @ {pos['entry_px']}")
                        _tg(f"✅ ENTRY: {pos['coin']} {pos['side']} @ {pos['entry_px']}")
                        break
                    # Exit fill (maker exit detected via WS)
                    if pos.get('exit_oid') and int(pos['exit_oid']) == oid and not pos.get('exit_filled'):
                        # Codex R3 fix: use avgPx if available, fallback to limitPx
                        exit_px = float(order.get("avgPx", order.get("limitPx", 0)))
                        pos['exit_filled'] = True

                        if pos['side'] == 'BUY':
                            pnl_bps = (exit_px - pos['entry_px']) / pos['entry_px'] * 10000
                        else:
                            pnl_bps = (pos['entry_px'] - exit_px) / pos['entry_px'] * 10000

                        pnl_usd = pnl_bps / 10000 * pos['size'] * exit_px
                        self.total_pnl += pnl_usd
                        self.total_trades += 1

                        logger.info(
                            f"EXIT FILLED (MAKER): {pos['coin']} {pos['side']} "
                            f"entry={pos['entry_px']} exit={exit_px} "
                            f"pnl={pnl_bps:+.1f}bp (${pnl_usd:+.4f}) "
                            f"total_pnl=${self.total_pnl:+.4f}"
                        )

                        _tg(f"💰 EXIT (maker): {pos['coin']} {pos['side']} {pnl_bps:+.1f}bp ${pnl_usd:+.4f} total=${self.total_pnl:+.4f}")

                        self.db["v10_copy_trades"].insert_one({
                            "target_wallet": pos.get('wallet', str(self.targets)),  # BUG 1 fix
                            "coin": pos['coin'], "side": pos['side'],
                            "entry_px": pos['entry_px'], "exit_px": exit_px,
                            "size": pos['size'], "pnl_bps": pnl_bps,
                            "pnl_usd": pnl_usd, "exit_type": "maker",
                            "hold_s": time.time() - pos.get('fill_time', pos['entry_time']),
                            "timestamp": datetime.now(timezone.utc),
                        })
                        # Codex R3 fix: remove position from tracker after maker fill
                        # Codex R6 fix #1: clean accumulated + exit buffer on maker exit
                        acc_key = (pos.get('wallet', ''), pos['coin'])
                        self._position_accumulated.pop(acc_key, None)
                        self._exit_twap_buffer.pop(acc_key, None)
                        self.positions = [p for p in self.positions if p is not pos]
                        break

    async def _check_twap_windows(self):
        """Check if any TWAP aggregation window has expired → enter position.

        Codex fix #1: NET buys vs sells to determine direction.
        Only enter if net notional exceeds minimum and is clearly directional.
        """
        now = time.time()
        expired = []

        for twap_key, buf in list(self._twap_buffer.items()):
            wallet, coin = twap_key
            elapsed = now - buf['first_ts']
            if elapsed < TWAP_WINDOW_S:
                continue

            expired.append(twap_key)

            # Dedup
            dedup_key = (wallet, coin, int(buf['first_ts']))
            if dedup_key in self._twap_entered:
                continue

            # NET direction
            net = buf['buy_notional'] - buf['sell_notional']
            gross = buf['buy_notional'] + buf['sell_notional']
            is_buy = net > 0

            # Both sides enabled for V8 validated wallets

            # Skip if net is too small (target might be closing, not opening)
            abs_net = abs(net)
            if abs_net < MIN_TWAP_NOTIONAL:
                logger.info(
                    f"TWAP SKIP: {wallet[:10]} {coin} net=${net:+,.0f} < ${MIN_TWAP_NOTIONAL} min "
                    f"(buys=${buf['buy_notional']:,.0f} sells=${buf['sell_notional']:,.0f} "
                    f"{buf['count']} fills)"
                )
                continue

            # Skip if direction is ambiguous (buys and sells roughly equal)
            if gross > 0 and abs_net / gross < 0.6:
                logger.info(
                    f"TWAP SKIP: {wallet[:10]} {coin} ambiguous — net/gross={abs_net/gross:.1%} "
                    f"(buys=${buf['buy_notional']:,.0f} sells=${buf['sell_notional']:,.0f})"
                )
                continue

            side_str = 'BUY' if is_buy else 'SELL'

            # Check if this is an OPENING or CLOSING trade
            opening = self._is_opening_trade(wallet, coin, is_buy)

            if not opening:
                logger.info(
                    f"TWAP SKIP (CLOSING): {wallet[:10]} {coin} {side_str} — target reducing position, not opening"
                )
                # Codex R5: skip notifications are noise — log only
                continue

            logger.info(
                f"TWAP COMPLETE: {wallet[:10]} {coin} NET {side_str} ${abs_net:,.0f} "
                f"(buys=${buf['buy_notional']:,.0f} sells=${buf['sell_notional']:,.0f}) "
                f"{buf['count']} fills — OPENING confirmed — ENTERING"
            )
            _tg(
                f"📊 TWAP DONE: {wallet[:10]} {side_str} {coin} "
                f"net=${abs_net:,.0f} ({buf['count']} fills) — opening → copying"
            )

            # Update our snapshot of their position from trade stream
            # (don't rely on clearinghouseState — agent keys return NULL)
            trade_sz = abs_net / (self.mid_prices.get(coin, 1) or 1)
            if not is_buy:
                trade_sz = -trade_sz
            prev = self._target_positions.get(wallet, {}).get(coin, 0)
            self._target_positions.setdefault(wallet, {})[coin] = prev + trade_sz
            logger.info(f"TARGET POS UPDATE (trade-stream): {wallet[:14]} {coin} → {prev + trade_sz:.4f}")

            # Dedup after entry (consistent with mid-TWAP path)
            await self._enter_position(coin, is_buy, twap_dedup_key=dedup_key, wallet=wallet)

        for key in expired:
            self._twap_completed_ts[key] = time.time()  # debounce window starts
            del self._twap_buffer[key]

    def _reconcile_positions(self):
        """Check exchange for actual positions every 60s.
        BUG 2 fix: also clean up cancel-pending entries that have no real position.
        BUG 3 fix: use explicit variable name to avoid shadowing."""
        now = time.time()
        if now - self._last_reconcile < 300:  # every 5min instead of 60s to reduce API load
            return
        self._last_reconcile = now
        try:
            r = requests.post(f"{HL_API}/info", json={
                "type": "clearinghouseState", "user": self.parent_address
            }, timeout=5)
            exchange_positions = {}
            for ap in r.json().get("assetPositions", []):
                ep = ap["position"]
                exchange_positions[ep["coin"]] = float(ep["szi"])

            # Check for untracked positions — auto-close orphans
            # DISABLED: sharing HL account with V9, orphans may belong to other strategy
            if False:  # was: not getattr(self, 'shadow_mode', False)
             if not hasattr(self, '_orphan_attempts'):
                self._orphan_attempts = {}  # coin -> attempt count
             for coin, actual_sz in exchange_positions.items():
                tracked = any(tp['coin'] == coin and tp['filled'] for tp in self.positions)
                # R4-F1: Use notional threshold, not raw coin amount
                mid = self.mid_prices.get(coin, 0)
                actual_notional = abs(actual_sz) * mid if mid > 0 else 0
                if (actual_notional > 1.0 or abs(actual_sz) > 0.001) and not tracked:
                    attempts = self._orphan_attempts.get(coin, 0)
                    if attempts >= 5:
                        continue  # give up after 5 attempts
                    if attempts == 0:
                        logger.warning(f"RECONCILE: untracked {coin} size={actual_sz} — auto-closing")
                        _tg(f"🗑️ AUTO-CLOSE orphan: {coin} size={actual_sz}")
                    try:
                        is_buy = actual_sz < 0
                        mid = self.mid_prices.get(coin, 0)
                        if mid > 0:
                            px = self._round_price(mid * (1.01 if is_buy else 0.99))
                            sz = self._round_size(coin, abs(actual_sz))
                            self.exchange.order(coin, is_buy, sz, px,
                                               {"limit": {"tif": "Ioc"}}, reduce_only=True)
                            logger.info(f"ORPHAN CLOSED: {coin} (attempt {attempts + 1})")
                            self._orphan_attempts[coin] = 99  # mark as done
                        else:
                            self._orphan_attempts[coin] = attempts + 1
                            logger.debug(f"Orphan {coin}: no mid price yet, retry {attempts + 1}/5")
                    except Exception as e:
                        self._orphan_attempts[coin] = attempts + 1
                        logger.error(f"Failed to close orphan {coin} (attempt {attempts + 1}): {e}")

            # BUG 2: Remove cancel-pending entries that have no exchange position
            # R4-F1: Use notional threshold for position existence check
            self.positions = [
                tp for tp in self.positions
                if not tp.get('_cancel_pending')
                or (abs(exchange_positions.get(tp['coin'], 0)) * self.mid_prices.get(tp['coin'], 1) > 1.0)
            ]

            # R2-F2: Phantom position cleanup by (wallet, coin), not just coin
            # V10: skip phantom cleanup in shadow mode (shadow positions don't exist on exchange)
            before = len(self.positions)
            phantom_keys = []
            if not self.shadow_mode:
                now_ts = time.time()
                for tp in self.positions:
                    if not tp.get('filled'):
                        continue
                    grace_s = 300 if tp.get('_recovered') else 60
                    fill_time = tp.get('fill_time', tp.get('entry_time', 0))
                    if now_ts - fill_time < grace_s:
                        continue
                    coin = tp['coin']
                    exch_sz = abs(exchange_positions.get(coin, 0))
                    # CODEX FIX R7: Check mid-price freshness. If stale (>120s)
                    # or zero, use entry_px. If both zero, skip to avoid orphaning.
                    mid = self.mid_prices.get(coin, 0)
                    mid_age = now_ts - self._mid_price_ts.get(coin, 0)
                    if mid <= 0 or mid_age > 120:
                        mid = tp.get('entry_px', 0)
                        if mid_age > 120 and mid > 0:
                            logger.warning(f"PHANTOM CHECK: stale mid for {coin} ({mid_age:.0f}s old), using entry_px={mid}")
                    if mid <= 0:
                        continue  # no price data, skip phantom check
                    exch_notional = exch_sz * mid
                    if exch_notional < 1.0 and exch_sz < 0.001:
                        key = (tp.get('wallet', ''), coin)
                        phantom_keys.append(key)
                        self._position_accumulated.pop(key, None)
                        self._exit_twap_buffer.pop(key, None)
            if phantom_keys:
                phantom_set = set(phantom_keys)
                self.positions = [
                    tp for tp in self.positions
                    if (tp.get('wallet', ''), tp['coin']) not in phantom_set
                ]
                phantom_coins = [k[1] for k in phantom_keys]
                logger.info(f"RECONCILE: removed {len(phantom_keys)} phantom positions: {phantom_coins}")
                _tg(f"🧹 Cleaned {len(phantom_keys)} phantom positions: {', '.join(phantom_coins)}")
        except Exception as e:
            logger.debug(f"Reconcile error: {e}")

    def _log_stats(self):
        now = time.time()
        if now - self._last_stats < 60:
            return
        self._last_stats = now

        # R2-F6: kill switch includes unrealized PnL
        # R4-F9: Kill switch disables NEW ENTRIES but keeps exit monitoring alive.
        # Previously it set self.running = False which stopped everything,
        # causing us to miss wallet exit signals and get stuck with orphan positions.
        total_upnl = self._compute_unrealized_pnl()
        net_pnl = self.total_pnl + total_upnl
        if net_pnl < MAX_DAILY_LOSS:
            if not getattr(self, '_kill_switch_active', False):
                self._kill_switch_active = True
                logger.error(f"KILL SWITCH: net ${net_pnl:.4f} (realized=${self.total_pnl:.4f} + unrealized=${total_upnl:.4f}) < ${MAX_DAILY_LOSS}")
                logger.error("KILL SWITCH: new entries DISABLED. Exit monitoring continues.")
                _tg(f"🛑 KILL SWITCH: net ${net_pnl:.4f} — entries disabled, exits still active")
            # Don't set self.running = False — keep WS + exit loop alive
        elif getattr(self, '_kill_switch_active', False):
            # Recovery: net PnL back above threshold
            self._kill_switch_active = False
            logger.info(f"KILL SWITCH LIFTED: net ${net_pnl:.4f} > ${MAX_DAILY_LOSS}")
            _tg(f"✅ Kill switch lifted: net ${net_pnl:.4f}")

        self._reconcile_positions()

        open_pos = [p for p in self.positions if p['filled']]
        pending = [p for p in self.positions if not p['filled']]
        # Reuse total_upnl from kill switch check above (avoid double-compute)
        open_coins = " ".join(f"{p['coin']}" for p in open_pos) if open_pos else "none"
        # V9: exchange-sourced margin utilization
        equity = self._equity_cache or 0
        margin_used = getattr(self, '_exch_margin_used', 0)
        margin_pct = (margin_used / equity * 100) if equity > 0 else 0
        logger.info(
            f"STATS: wallets={len(self.targets)} trades={self.total_trades} "
            f"realized=${self.total_pnl:+.4f} unrealized=${total_upnl:+.4f} "
            f"net=${self.total_pnl + total_upnl:+.4f} "
            f"open={len(open_pos)}[{open_coins}] margin={margin_pct:.0f}% equity=${equity or 0:.2f}"
        )

        # Detailed TG report every 15 minutes
        if not hasattr(self, '_last_tg_report'):
            self._last_tg_report = now
        if now - self._last_tg_report >= 900:  # 15 min
            self._last_tg_report = now
            self._send_performance_report()

    def _send_performance_report(self):
        """Send detailed performance report to Telegram every 15 min."""
        try:
            # V8 epoch: first V8 deployment with corrected wallets
            v8_epoch = datetime(2026, 5, 9, 23, 26, 0, tzinfo=timezone.utc)
            today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            # Strip tzinfo for MongoDB comparison (stores naive datetimes)
            v8_epoch_naive = v8_epoch.replace(tzinfo=None)
            today_start_naive = today_start.replace(tzinfo=None)
            all_closed = list(self.db["v10_copy_trades"].find(
                {"timestamp": {"$gte": v8_epoch_naive}}
            ).sort("timestamp", 1))
            closed_today = [t for t in all_closed if t.get("timestamp") and t["timestamp"] >= today_start_naive]
            closed = all_closed  # all-time = since V8

            # Account equity — R4-F3: HL unified account, spot USDC is total capital
            acct_val = 0.0
            try:
                _info_url = "https://api.hyperliquid.xyz/info"
                _r2 = requests.post(_info_url, json={"type": "spotClearinghouseState", "user": self.parent_address}, timeout=5)
                for _b in _r2.json().get("balances", []):
                    if _b.get("coin") == "USDC":
                        acct_val = float(_b.get("total", 0))
            except Exception:
                pass

            # Overall stats
            n_trades = len(closed)
            if closed:
                pnls = [t.get("pnl_bps", 0) for t in closed]
                pnl_usd = [t.get("pnl_usd", 0) for t in closed]
                wins = sum(1 for p in pnls if p > 0)
                wr = wins / n_trades * 100 if n_trades > 0 else 0
                total_usd = sum(pnl_usd)
                avg_bps = sum(pnls) / n_trades if n_trades > 0 else 0
                best = max(pnls)
                worst = min(pnls)
            else:
                wr = 0
                total_usd = self.total_pnl
                avg_bps = 0
                best = worst = 0

            # Unrealized PnL from open positions
            total_upnl = 0
            open_lines = []
            for pos in self.positions:
                if not pos.get('filled'):
                    continue
                mid = self.mid_prices.get(pos['coin'], 0)
                if mid > 0:
                    if pos['side'] == 'BUY':
                        upnl_bps = (mid - pos['entry_px']) / pos['entry_px'] * 10000
                    else:
                        upnl_bps = (pos['entry_px'] - mid) / pos['entry_px'] * 10000
                    upnl_usd = upnl_bps / 10000 * pos['size'] * mid
                    total_upnl += upnl_usd
                    wallet_short = str(pos.get('wallet', '?'))[:10]
                    # entry_time may be float (epoch) or datetime
                    et = pos.get('entry_time')
                    if isinstance(et, (int, float)):
                        et = datetime.fromtimestamp(et, tz=timezone.utc)
                    elif isinstance(et, datetime) and et.tzinfo is None:
                        et = et.replace(tzinfo=timezone.utc)
                    hold_min = (datetime.now(timezone.utc) - et).total_seconds() / 60 if et else 0
                    open_lines.append(
                        f"  {pos['coin']} {pos['side']} {upnl_bps:+.0f}bp ${upnl_usd:+.3f} "
                        f"({hold_min:.0f}m, {wallet_short})"
                    )

            # Per-wallet breakdown
            wallet_stats = {}
            for t in closed:
                w = str(t.get("target_wallet", "?"))[:10]
                if w not in wallet_stats:
                    wallet_stats[w] = {"trades": 0, "wins": 0, "pnl": 0, "coins": set()}
                wallet_stats[w]["trades"] += 1
                wallet_stats[w]["pnl"] += t.get("pnl_usd", 0)
                wallet_stats[w]["coins"].add(t.get("coin", "?"))
                if t.get("pnl_bps", 0) > 0:
                    wallet_stats[w]["wins"] += 1

            # Long vs short
            longs = [t for t in closed if t.get("side") == "BUY"]
            shorts = [t for t in closed if t.get("side") == "SELL"]
            long_pnl = sum(t.get("pnl_usd", 0) for t in longs)
            short_pnl = sum(t.get("pnl_usd", 0) for t in shorts)

            # Target wallet activity (how many are trading right now)
            active_targets = set()
            for pos in self.positions:
                if pos.get('filled'):
                    active_targets.add(str(pos.get('wallet', ''))[:10])

            # Uptime
            uptime_h = (datetime.now(timezone.utc) - self._deploy_time).total_seconds() / 3600

            # Today stats
            n_today = len(closed_today)
            today_usd = sum(t.get("pnl_usd", 0) for t in closed_today)
            today_wins = sum(1 for t in closed_today if t.get("pnl_bps", 0) > 0)
            today_wr = today_wins / n_today * 100 if n_today > 0 else 0

            # Build report
            net_pnl = total_usd + total_upnl
            emoji = "🟢" if net_pnl >= 0 else "🔴"
            lines = [f"{emoji} COPY TRADER V10 — {datetime.now(timezone.utc).strftime('%H:%M')} UTC"]
            lines.append(f"Equity: ${acct_val:.2f} | Wallets: {len(self.targets)} tracked, {len(active_targets)} active")
            lines.append("")
            lines.append(f"Today: {n_today}t ${today_usd:+.3f} ({today_wr:.0f}%WR)")
            lines.append(f"All-time: {n_trades}t ${total_usd:+.3f} ({wr:.0f}%WR, avg {avg_bps:+.0f}bp)")
            if n_trades > 0:
                lines.append(f"  Best: {best:+.0f}bp | Worst: {worst:+.0f}bp")
            lines.append(f"Unrealized: {len(open_lines)} pos ${total_upnl:+.3f}")
            lines.append(f"NET: ${net_pnl:+.3f}")

            # Long vs short
            if n_trades > 0:
                lines.append(f"\nL: {len(longs)}t ${long_pnl:+.3f} | S: {len(shorts)}t ${short_pnl:+.3f}")

            # Per wallet (if any closed trades)
            if wallet_stats:
                lines.append("\nPer wallet:")
                for w, s in sorted(wallet_stats.items(), key=lambda x: -x[1]["pnl"]):
                    w_wr = s["wins"] / s["trades"] * 100 if s["trades"] > 0 else 0
                    coins_str = ",".join(sorted(s["coins"]))
                    lines.append(f"  {w} {s['trades']}t {w_wr:.0f}%WR ${s['pnl']:+.3f} [{coins_str}]")

            # Open positions
            if open_lines:
                lines.append(f"\nOpen positions:")
                lines.extend(sorted(open_lines, key=lambda x: x)[:8])
                if len(open_lines) > 8:
                    lines.append(f"  +{len(open_lines)-8} more")

            # Generate equity curve chart
            chart_path = self._generate_equity_chart(closed)

            if chart_path:
                _tg_with_image("\n".join(lines), chart_path)
            else:
                _tg("\n".join(lines))

        except Exception as e:
            logger.error(f"Performance report error: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _generate_equity_chart(self, closed_trades: list) -> Optional[str]:
        """Generate equity curve PNG and return path. Works with 1+ trades."""
        try:
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            import matplotlib.dates as mdates

            if len(closed_trades) < 1:
                return None

            # Build cumulative PnL series — start from 0 at deploy time
            v8_epoch_chart = datetime(2026, 5, 9, 23, 26, 0, tzinfo=timezone.utc)
            timestamps = [v8_epoch_chart]
            cum_pnl = [0.0]
            running = 0.0

            for t in closed_trades:
                ts = t.get("timestamp")
                if isinstance(ts, datetime):
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    timestamps.append(ts)
                else:
                    continue
                running += t.get("pnl_usd", 0)
                cum_pnl.append(running)

            # Add current unrealized as final point
            now_ts = datetime.now(timezone.utc)
            total_upnl = 0
            for pos in self.positions:
                if not pos.get('filled'):
                    continue
                mid = self.mid_prices.get(pos['coin'], 0)
                if mid > 0:
                    if pos['side'] == 'BUY':
                        upnl = (mid - pos['entry_px']) / pos['entry_px'] * pos['size'] * mid
                    else:
                        upnl = (pos['entry_px'] - mid) / pos['entry_px'] * pos['size'] * mid
                    total_upnl += upnl
            timestamps.append(now_ts)
            cum_pnl.append(running + total_upnl)

            # Plot
            fig, ax = plt.subplots(figsize=(8, 4))

            # Realized line (solid)
            ax.plot(timestamps[:-1], cum_pnl[:-1], 'b-', linewidth=1.5, label='Realized')
            # Unrealized extension (dashed)
            ax.plot(timestamps[-2:], cum_pnl[-2:], 'b--', linewidth=1.5, alpha=0.6, label='+ Unrealized')
            ax.fill_between(timestamps, cum_pnl, alpha=0.08, color='blue')
            ax.axhline(y=0, color='gray', linestyle='--', linewidth=0.5)

            # Mark each trade
            for i, t in enumerate(closed_trades):
                ts = t.get("timestamp")
                if isinstance(ts, datetime):
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    pnl_bps = t.get("pnl_bps", 0)
                    color = '#2ecc71' if pnl_bps >= 0 else '#e74c3c'
                    ax.plot(ts, cum_pnl[i + 1], 'o', color=color, markersize=6, zorder=5)

            # Color the endpoint
            final = cum_pnl[-1]
            color = '#2ecc71' if final >= 0 else '#e74c3c'
            ax.plot(timestamps[-1], final, 's', color=color, markersize=10, zorder=5)
            ax.annotate(f'${final:+.3f}', (timestamps[-1], final),
                       textcoords="offset points", xytext=(10, 5),
                       fontsize=11, fontweight='bold', color=color)

            # Open position markers
            n_open = sum(1 for p in self.positions if p.get('filled'))
            if n_open > 0:
                ax.annotate(f'{n_open} open', (timestamps[-1], final),
                           textcoords="offset points", xytext=(10, -12),
                           fontsize=8, color='gray')

            ax.set_title(f'Copy Trader V8 — {len(closed_trades)} closed, {n_open} open', fontsize=12)
            ax.set_ylabel('Cumulative PnL ($)', fontsize=10)

            # Smart date formatting based on time span
            span_hours = (timestamps[-1] - timestamps[0]).total_seconds() / 3600
            if span_hours < 24:
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            else:
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))

            ax.legend(loc='upper left', fontsize=8)
            ax.grid(True, alpha=0.3)
            fig.autofmt_xdate()
            plt.tight_layout()

            path = '/tmp/copy_equity_curve.png'
            fig.savefig(path, dpi=120)
            plt.close(fig)
            return path

        except Exception as e:
            logger.error(f"Chart generation error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    async def run(self):
        logger.info(f"Copy trader starting: target={str(self.targets)[:40]} coins={self.coins} size=${self.order_size}")

        def shutdown(sig, frame):
            self.running = False
        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

        while self.running:
            try:
                async with websockets.connect(HL_WS, ping_interval=20) as ws:
                    # BUG 7 + R2-F7: clear stale TWAP buffers on reconnect
                    self._twap_buffer.clear()
                    self._exit_twap_buffer.clear()

                    # Subscribe to trades for our coins
                    for coin in self.coins:
                        await ws.send(json.dumps({
                            "method": "subscribe",
                            "subscription": {"type": "trades", "coin": coin}
                        }))
                        await asyncio.sleep(0.1)

                    # Subscribe to L2 for mid prices
                    for coin in self.coins:
                        await ws.send(json.dumps({
                            "method": "subscribe",
                            "subscription": {"type": "l2Book", "coin": coin}
                        }))
                        await asyncio.sleep(0.1)

                    # Subscribe to our order updates
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "orderUpdates", "user": self.parent_address}
                    }))

                    logger.info("WS subscribed")

                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=5)
                            data = json.loads(msg)
                            channel = data.get("channel")

                            if channel == "l2Book":
                                coin_data = data.get("data", {})
                                coin = coin_data.get("coin", "")
                                levels = coin_data.get("levels", [])
                                if len(levels) >= 2 and levels[0] and levels[1]:
                                    self.mid_prices[coin] = (
                                        float(levels[0][0]["px"]) + float(levels[1][0]["px"])
                                    ) / 2
                                    self._mid_price_ts[coin] = time.time()
                                    # Codex R4 #8+#9: real-time book depth from WS
                                    self._update_book_depth_from_ws(coin, levels)

                            elif channel == "trades":
                                # HL trade stream — check each trade for target wallet
                                trades_data = data.get("data", [])
                                if isinstance(trades_data, list):
                                    for t in trades_data:
                                        self._on_hl_trade(t)

                            elif channel == "orderUpdates":
                                self._on_order_update(data.get("data", []))

                        except asyncio.TimeoutError:
                            pass

                        # BUG 8 fix: throttle expensive checks to max 1/sec
                        now_check = time.time()
                        if not hasattr(self, '_last_check') or now_check - self._last_check >= 1.0:
                            self._last_check = now_check
                            await self._check_twap_windows()
                            await self._check_exits()
                            self._log_stats()

            except Exception as e:
                logger.error(f"WS error: {e}, reconnecting in 5s...")
                await asyncio.sleep(5)

        # Shutdown: preserve positions (they'll be recovered on restart)
        # Only close pending (unfilled) orders, not filled positions
        logger.info(f"Shutting down -- preserving {len([p for p in self.positions if p['filled']])} positions for recovery")
        for pos in self.positions:
            if not pos['filled']:
                try:
                    self.exchange.cancel(pos['coin'], int(pos['oid']))
                    logger.info(f"Cancelled pending order: {pos['coin']}")
                except:
                    pass
        logger.info(f"FINAL: trades={self.total_trades} pnl=${self.total_pnl:+.4f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--wallets", required=True, help="Comma-separated target wallets (full addresses)")
    parser.add_argument("--size", type=float, default=11.0, help="Order size in USD")
    parser.add_argument("--coins", default="BTC", help="Comma-separated coins to trade")
    parser.add_argument("--shadow", action="store_true", help="Shadow mode: log signals but don't place orders")
    args = parser.parse_args()

    wallets = [w.strip() for w in args.wallets.split(",")]
    coins = [c.strip() for c in args.coins.split(",")]
    trader = CopyTrader(wallets, coins, args.size, shadow=args.shadow)
    if args.shadow:
        logger.info("SHADOW MODE: logging signals only, no orders placed")
    asyncio.run(trader.run())
