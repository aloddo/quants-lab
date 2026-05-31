#!/usr/bin/env python3
"""
HL Copy Trader V11 -- Unified engine merging V9 (DCA/TWAP) and V10 (momentum/instant).

Per-wallet configuration from JSON: entry mode, add-on behavior, exit params, entry guards.
Dynamic coin subscription: trades for ALL perp coins, l2Book only for active coins.

Usage:
    python scripts/hl_copy_trader_v11.py [--config config/copy_trader_wallets.json] [--size 11] [--shadow]
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
from pathlib import Path
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
logger = logging.getLogger("hl_copy_v11")

HL_API = "https://api.hyperliquid.xyz"
HL_WS = "wss://api.hyperliquid.xyz/ws"
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "-1003576397888")
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")

# Builder-deployed perp dexes (HIP-3) to subscribe to
BUILDER_DEXES = ["xyz", "flx"]


def _get_market_type(coin: str) -> str:
    """Classify coin into market type for independent PnL tracking."""
    if ":" in coin:
        return coin.split(":")[0]  # "xyz", "flx", "vntl", etc.
    elif coin.startswith("@"):
        return "spot"
    return "perp"

# ── Fixed constants (not per-wallet) ────────────────────────────────────────
EXIT_TWAP_WINDOW_S = 60      # exit TWAP detection window (trade-stream based)
EXIT_POSITION_PCT = 0.30     # GRADUAL: exit trigger when reverse flow > 30% of accumulated

DB_COLLECTION = "unified_copy_trades"
DB_SHADOW_COLLECTION = "unified_shadow_signals"
DB_FILLS_COLLECTION = "hl_copy_target_fills"
DB_OPEN_POSITIONS = "v11_open_positions"  # persistent position state (per-wallet)
DB_EXCHANGE_FILLS = "v11_exchange_fills"  # exchange fills (source of truth for PnL)
DB_ORDER_IDS = "v11_order_ids"  # every oid V11 generates (for fill attribution)


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
    """Send Telegram photo + report as two separate messages."""
    if not TG_TOKEN:
        return
    # Send photo without caption
    try:
        with open(image_path, "rb") as f:
            r = requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendPhoto",
                data={"chat_id": TG_CHAT_ID},
                files={"photo": f}, timeout=10
            )
        if not r.json().get("ok"):
            logger.error(f"TG photo failed: {r.json().get('description', r.text)}")
    except Exception as e:
        logger.error(f"TG photo error: {e}")
    # Send report as separate text message
    _tg(caption)


class CopyTrader:
    def __init__(self, config_path: str, order_size_override: float = None, shadow: bool = False):
        # ── Load config ─────────────────────────────────────────────────────
        with open(config_path) as f:
            config = json.load(f)

        # Fix #12: validate required config sections
        for key in ("global", "defaults", "wallets"):
            if key not in config:
                raise ValueError(f"Config missing required section: {key}")
        for key in ("max_margin_util", "max_daily_loss", "order_size_usd"):
            if key not in config["global"]:
                raise ValueError(f"Config global missing required key: {key}")

        self.global_config = config["global"]
        self.default_config = config["defaults"]
        self.wallet_configs = {}  # address -> per-wallet overrides (sparse)
        self.wallet_groups = {}   # address -> group name

        for addr, wc in config["wallets"].items():
            addr = addr.lower().strip()
            if len(addr) < 20:
                raise ValueError(f"Wallet must be full address, got {len(addr)} chars: {addr}")
            self.wallet_configs[addr] = wc
            self.wallet_groups[addr] = wc.get("group", self.default_config.get("group", "default"))

        self.target_set = set(self.wallet_configs.keys())  # O(1) lookup in trade handler
        self.leader_to_vault = {}  # vault leader address -> vault address (for WS matching)
        self.order_size = order_size_override or self.global_config["order_size_usd"]
        self.shadow_mode = shadow
        self._deploy_time = datetime.now(timezone.utc)

        # Resolve vault leaders: vaults trade under their leader address on WS
        self._resolve_vault_leaders()

        logger.info(f"V11: loaded {len(self.target_set)} wallets ({len(self.leader_to_vault)} vaults) from {config_path}")
        for addr in sorted(self.wallet_configs.keys()):
            wc = self._wallet_config(addr)
            logger.info(
                f"  {addr[:14]} group={self.wallet_groups[addr]} "
                f"mode={wc['entry_mode']} addon={wc['max_addon_multiplier']}x "
                f"sl={wc['sl_bps']} trail={wc.get('trail_activate_bps')}/{wc.get('trail_bps')} "
                f"hold={wc['max_hold_s']}s exit={wc['exit_type']}"
            )

        # ── HL SDK ──────────────────────────────────────────────────────────
        self.private_key = os.environ["HL_PRIVATE_KEY"]
        self.agent_address = os.environ["HL_ADDRESS"]
        self.parent_address = os.environ.get(
            "HL_QUERY_ADDRESS", "0x11ca20aeb7cd014cf8406560ae405b12601994b4"
        )
        self.account = eth_account.Account.from_key(self.private_key)

        # Retry SDK init (include builder dexes for xyz:/flx: coins)
        all_dexes = [""] + BUILDER_DEXES
        for attempt in range(5):
            try:
                self.info = Info(HL_API, skip_ws=True, perp_dexs=all_dexes)
                self.exchange = Exchange(
                    self.account, HL_API, account_address=self.agent_address,
                    perp_dexs=all_dexes,
                )
                break
            except Exception as e:
                wait = (attempt + 1) * 10
                logger.warning(f"SDK init attempt {attempt+1} failed, waiting {wait}s: {e}")
                time.sleep(wait)
        else:
            raise RuntimeError("SDK init failed after 5 attempts. Cannot start.")

        # Get asset metadata (sz_decimals + per-coin max leverage) for ALL dexes
        self.sz_decimals = {}
        self.max_leverage = {}
        self.all_perp_coins = []       # regular perp coins (no prefix)
        self.all_builder_coins = []    # builder-deployed coins (xyz:X, flx:Y, etc.)
        max_lev_cap = self.global_config["max_leverage_cap"]

        # Regular perps
        meta = self.info.meta_and_asset_ctxs()
        if meta and len(meta) == 2:
            for u in meta[0]["universe"]:
                name = u["name"]
                self.sz_decimals[name] = u.get("szDecimals", 2)
                self.max_leverage[name] = min(u.get("maxLeverage", 3), max_lev_cap)
                self.all_perp_coins.append(name)

        # Builder-deployed dexes (xyz, flx, etc.)
        for dex_name in BUILDER_DEXES:
            try:
                dex_meta = self.info.meta(dex=dex_name)
                for u in dex_meta.get("universe", []):
                    name = u["name"]  # already prefixed, e.g. "xyz:TSLA"
                    self.sz_decimals[name] = u.get("szDecimals", 2)
                    self.max_leverage[name] = min(u.get("maxLeverage", 3), max_lev_cap)
                    self.all_builder_coins.append(name)
                logger.info(f"V11: loaded {dex_name} dex: {len([c for c in self.all_builder_coins if c.startswith(dex_name + ':')])} coins")
            except Exception as e:
                logger.warning(f"V11: failed to load {dex_name} dex meta: {e}")

        logger.info(
            f"V11: {len(self.all_perp_coins)} perp + {len(self.all_builder_coins)} builder coins available"
        )

        # MongoDB
        self.db = MongoClient("mongodb://localhost:27017").quants_lab

        # Exchange state cache (source of truth for margin, equity, positions)
        self._equity_cache = None
        self._equity_cache_ts = 0
        self._exch_margin_used = 0.0
        self._exch_unrealized_pnl = 0.0
        self._exch_positions = {}
        self._pending_margin = 0.0

        # State
        self.positions = []
        self.last_entry = {}
        self.mid_prices = {}
        self.running = True
        self._last_fill_sync = 0  # force immediate sync on first stats
        self.pnl_by_market = {}  # per-market-type breakdown (informational only)
        # 2026-05-22: dedup drift TG alerts to 1/hour/coin (was spamming every 5min cycle)
        self._last_drift_tg = {}  # coin -> unix ts of last TG alert
        self._kill_switch_active = False
        self._kill_reasons = {}  # {"stale": True, "loss": True} -- separate tracking

        # Exchange PnL cache (READ-ONLY, refreshed by _sync_exchange_fills)
        # V11 NEVER computes PnL internally. This cache is the ONLY source.
        self._exch_pnl = {
            "account_net": 0.0,    # all fills: closedPnl - fees
            "v11_net": 0.0,        # V11-attributed fills only
            "v11_closes": 0,       # V11 closing fill count
            "account_closes": 0,   # all closing fills
            "fees": 0.0,           # total fees
            "last_sync": 0.0,      # timestamp of last successful sync
        }
        self._last_successful_sync = 0

        # Initial sync from exchange (blocking, must succeed before trading)
        try:
            self._do_exchange_fill_sync()
            logger.info(
                f"LOADED from EXCHANGE: {self._exch_pnl['account_closes']} closes, "
                f"account_net=${self._exch_pnl['account_net']:+.4f} "
                f"v11_net=${self._exch_pnl['v11_net']:+.4f} "
                f"fees=${self._exch_pnl['fees']:.4f}"
            )
        except Exception as e:
            logger.error(f"CRITICAL: Failed to load exchange PnL on startup: {e}")
            # Set to 0, will retry on first stats cycle
        self._last_stats = 0
        self._last_reconcile = 0

        # Position lifecycle tracking
        self._position_accumulated = {}
        self._twap_buffer = {}
        self._twap_entered = set()
        self._twap_completed_ts = {}
        self._mid_price_ts = {}
        self._exit_twap_buffer = {}
        self._book_depth = {}
        self._seen_tids = {}
        self._post_exit_cooldown = {}  # (wallet, coin) -> timestamp of last exit

        # Dynamic l2Book subscriptions: coins we currently need book data for
        self._l2_subscribed = set()

        # Target position tracking
        self._target_positions = {}
        self._init_target_positions()

        # Position recovery
        if not self.shadow_mode:
            self._recover_positions()

    def _resolve_vault_leaders(self):
        """Check if any target wallets are vaults. If so, add their leader address
        to target_set so we detect trades on WS (vaults trade under leader address)."""
        for addr in list(self.wallet_configs.keys()):
            try:
                resp = requests.post(
                    f"{HL_API}/info",
                    json={"type": "vaultDetails", "user": addr, "vaultAddress": addr},
                    timeout=5,
                )
                data = resp.json()
                if data and isinstance(data, dict) and data.get("name"):
                    leader = data["leader"].lower()
                    self.leader_to_vault[leader] = addr
                    self.target_set.add(leader)
                    logger.info(f"VAULT: {addr[:14]} = \"{data['name']}\" leader={leader[:14]}")
            except Exception as e:
                logger.warning(f"Vault check failed for {addr[:14]}: {e}")

    def _wallet_config(self, wallet: str) -> dict:
        """Get merged config for a wallet: defaults + per-wallet overrides."""
        wc = self.wallet_configs.get(wallet, {})
        merged = {**self.default_config, **wc}
        return merged

    def _init_target_positions(self):
        """Snapshot each target wallet's current positions on startup."""
        self._target_init_failed = set()
        for addr in self.target_set:
            self._target_positions[addr] = {}
            try:
                # Query main perp positions + builder dex positions
                all_dexes = [""] + BUILDER_DEXES
                for dex_name in all_dexes:
                    payload = {"type": "clearinghouseState", "user": addr}
                    if dex_name:
                        payload["dex"] = dex_name
                    r = requests.post(f"{HL_API}/info", json=payload, timeout=5)
                    data = r.json()
                    if data is None:
                        if dex_name == "":
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
        logger.info(f"Target positions initialized: {flat}/{len(self.target_set)} flat")

    # ── Persistent position state ─────────────────────────────────────────────

    def _persist_position(self, pos: dict):
        """Save/update an open position to MongoDB. Keyed by (wallet, coin)."""
        if self.shadow_mode:
            return
        key = {"wallet": pos.get("wallet", ""), "coin": pos["coin"]}
        doc = {
            "wallet": pos.get("wallet", ""),
            "coin": pos["coin"],
            "side": pos["side"],
            "entry_px": pos.get("entry_px", 0),
            "size": pos.get("size", 0),
            "entry_time": pos.get("entry_time", time.time()),
            "fill_time": pos.get("fill_time", time.time()),
            "target_coin": pos.get("target_coin", pos["coin"]),
            "updated_at": datetime.now(timezone.utc),
        }
        try:
            self.db[DB_OPEN_POSITIONS].update_one(key, {"$set": doc}, upsert=True)
        except Exception as e:
            logger.warning(f"Failed to persist position {pos['coin']}: {e}")

    def _record_oid(self, oid, coin: str, side: str, action: str, wallet: str = ""):
        """Record every order ID V11 generates for fill attribution."""
        if self.shadow_mode or not oid:
            return
        try:
            doc = {
                "oid": int(oid), "coin": coin, "side": side,
                "action": action,  # "entry" or "exit"
                "timestamp": datetime.now(timezone.utc),
            }
            if wallet:
                doc["wallet"] = wallet
                doc["wallet_group"] = self.wallet_groups.get(wallet, "unknown")
            self.db[DB_ORDER_IDS].update_one(
                {"oid": int(oid)},
                {"$set": doc},
                upsert=True,
            )
        except Exception as e:
            logger.debug(f"Failed to record oid {oid}: {e}")

    def _remove_persisted_position(self, wallet: str, coin: str):
        """Remove a closed position from persistent state."""
        if self.shadow_mode:
            return
        try:
            self.db[DB_OPEN_POSITIONS].delete_one({"wallet": wallet, "coin": coin})
        except Exception as e:
            logger.warning(f"Failed to remove persisted position {coin}: {e}")

    def _load_persisted_positions(self) -> list:
        """Load all open positions from MongoDB. Returns list of position dicts."""
        try:
            docs = list(self.db[DB_OPEN_POSITIONS].find())
            positions = []
            for doc in docs:
                pos = {
                    "coin": doc["coin"],
                    "side": doc["side"],
                    "entry_px": doc.get("entry_px", 0),
                    "entry_time": doc.get("entry_time", time.time()),
                    "fill_time": doc.get("fill_time", time.time()),
                    "size": doc.get("size", 0),
                    "oid": 0,
                    "filled": True,
                    "wallet": doc.get("wallet", ""),
                    "target_coin": doc.get("target_coin", doc["coin"]),
                    "_recovered": True,
                }
                # 2026-05-23: propagate _force_exit flag for orphan management.
                # Previously dropped on load, leaving __orphan__ rows tracked but never closed.
                if doc.get("_force_exit"):
                    pos["_force_exit"] = True
                positions.append(pos)
            return positions
        except Exception as e:
            logger.warning(f"Failed to load persisted positions: {e}")
            return []

    def _recover_positions(self):
        """On startup, recover positions. DB-FIRST: load persisted state from MongoDB.
        Each wallet's position is tracked independently (no netting loss).
        Exchange state is used only for validation, not as source of truth."""

        # Step 1: Try loading from persistent DB state
        db_positions = self._load_persisted_positions()

        if db_positions:
            # DB has state: use it as truth
            self.positions = db_positions
            for pos in self.positions:
                acc_key = (pos.get('wallet', ''), pos['coin'])
                self._position_accumulated[acc_key] = pos['size'] * pos['entry_px']
                if pos['coin'] not in self._mid_price_ts:
                    self.mid_prices[pos['coin']] = pos['entry_px']
                    self._mid_price_ts[pos['coin']] = time.time()
            logger.info(f"DB RECOVERY: loaded {len(self.positions)} positions from persistent state")

            # Validate against exchange: sum tracked per coin vs exchange net
            self._validate_against_exchange()
            return

        # Step 2: Fallback for first run (no DB state yet) -- old exchange-based recovery
        logger.info("No persistent state found, falling back to exchange-based recovery")
        self._recover_from_exchange()

    def _validate_against_exchange(self):
        """Compare DB-loaded positions against exchange. Warn on drift, don't overwrite."""
        try:
            from collections import defaultdict

            r = requests.post(f"{HL_API}/info", json={
                "type": "clearinghouseState", "user": self.parent_address
            }, timeout=5)
            data = r.json()
            if not data:
                return

            exchange_positions = {}
            exchange_entry_prices = {}  # coin -> entry_px (for all dexes)
            for ap in data.get("assetPositions", []):
                ep = ap["position"]
                exchange_positions[ep["coin"]] = float(ep["szi"])
                exchange_entry_prices[ep["coin"]] = float(ep.get("entryPx", 0))

            for dex_name in BUILDER_DEXES:
                try:
                    rd = requests.post(f"{HL_API}/info", json={
                        "type": "clearinghouseState", "user": self.parent_address,
                        "dex": dex_name,
                    }, timeout=5)
                    for ap in rd.json().get("assetPositions", []):
                        ep = ap["position"]
                        exchange_positions[ep["coin"]] = float(ep["szi"])
                        exchange_entry_prices[ep["coin"]] = float(ep.get("entryPx", 0))
                except Exception:
                    pass

            # Sum tracked positions per coin (signed)
            tracked_by_coin = defaultdict(float)
            for pos in self.positions:
                sign = 1 if pos['side'] == 'BUY' else -1
                tracked_by_coin[pos['coin']] += pos['size'] * sign

            # Check for exchange positions we don't track at all
            tracked_coins = set(tracked_by_coin.keys())
            for coin, exch_sz in exchange_positions.items():
                if abs(exch_sz) < 1e-10:
                    continue
                entry_px = exchange_entry_prices.get(coin, 0)
                mid = entry_px if entry_px > 0 else self.mid_prices.get(coin, 1)
                notional = abs(exch_sz) * mid
                if coin not in tracked_coins and notional >= 5.0:
                    # Orphan on exchange not in our DB: auto-close it
                    side = "BUY" if exch_sz > 0 else "SELL"
                    logger.warning(
                        f"ORPHAN ON EXCHANGE: {coin} {side} sz={abs(exch_sz)} (${notional:.2f}) "
                        f"not in DB state -- queuing auto-close"
                    )
                    _tg(f"ORPHAN AUTO-CLOSE: {coin} {side} ${notional:.2f}")
                    self.positions.append({
                        'coin': coin, 'side': side, 'entry_px': entry_px,
                        'entry_time': time.time(), 'fill_time': time.time(),
                        'size': abs(exch_sz), 'oid': 0, 'filled': True,
                        'wallet': '__orphan__', 'target_coin': coin,
                        '_recovered': True, '_force_exit': True,
                    })

            # Check for significant drift between tracked net and exchange
            for coin, tracked_net in tracked_by_coin.items():
                exch_sz = exchange_positions.get(coin, 0)
                diff = abs(exch_sz - tracked_net)
                mid = self.mid_prices.get(coin, 0) or 1
                diff_notional = diff * mid
                if diff_notional > 5.0:
                    logger.warning(
                        f"DRIFT: {coin} DB_net={tracked_net:.4f} exchange={exch_sz:.4f} "
                        f"diff=${diff_notional:.2f} -- investigate"
                    )
                    # 2026-05-22: 1h per-coin TG cooldown — persistent drifts spam otherwise
                    _now = time.time()
                    if _now - self._last_drift_tg.get(coin, 0) > 3600:
                        _tg(f"POSITION DRIFT: {coin} tracked={tracked_net:.4f} vs exchange={exch_sz:.4f} (${diff_notional:.0f})")
                        self._last_drift_tg[coin] = _now

        except Exception as e:
            logger.warning(f"Exchange validation failed: {e}")

    def _recover_from_exchange(self):
        """Legacy fallback: recover from exchange state + persist to DB for future startups."""
        try:
            r = requests.post(f"{HL_API}/info", json={
                "type": "clearinghouseState", "user": self.parent_address
            }, timeout=5)
            data = r.json()
            if not data:
                return

            all_asset_positions = list(data.get("assetPositions", []))
            for dex_name in BUILDER_DEXES:
                try:
                    rd = requests.post(f"{HL_API}/info", json={
                        "type": "clearinghouseState", "user": self.parent_address,
                        "dex": dex_name,
                    }, timeout=5)
                    all_asset_positions.extend(rd.json().get("assetPositions", []))
                except Exception:
                    pass

            claimed_coins = set()

            for ap in all_asset_positions:
                pos = ap["position"]
                coin = pos["coin"]
                sz = float(pos.get("szi", 0))
                entry_px = float(pos.get("entryPx", 0))
                notional = abs(sz) * entry_px
                if notional < 1.0 and abs(sz) < 1e-9:
                    continue
                if coin in claimed_coins:
                    continue
                side = "BUY" if sz > 0 else "SELL"

                matched_wallet = None
                weak_matched_wallets = getattr(self, '_weak_matched_wallets', set())
                for addr in self.target_set:
                    target_data = self._target_positions.get(addr, {})
                    if not target_data and addr in self._target_init_failed:
                        # Weak match: only use if not already weak-matched to another coin
                        if not matched_wallet and addr not in weak_matched_wallets:
                            matched_wallet = addr
                        continue
                    target_sz = target_data.get(coin, 0)
                    if (target_sz > 0 and sz > 0) or (target_sz < 0 and sz < 0):
                        matched_wallet = addr
                        break
                if matched_wallet and matched_wallet in self._target_init_failed:
                    weak_matched_wallets.add(matched_wallet)
                    self._weak_matched_wallets = weak_matched_wallets

                if matched_wallet:
                    new_pos = {
                        'coin': coin, 'side': side, 'entry_px': entry_px,
                        'entry_time': time.time(), 'fill_time': time.time(),
                        'size': abs(sz), 'oid': 0, 'filled': True,
                        'wallet': matched_wallet, 'target_coin': coin,
                        '_recovered': True,
                    }
                    self.positions.append(new_pos)
                    self._persist_position(new_pos)  # Save to DB for future restarts
                    claimed_coins.add(coin)
                    acc_key = (matched_wallet, coin)
                    self._position_accumulated[acc_key] = abs(sz) * entry_px
                    if coin not in self._mid_price_ts:
                        self.mid_prices[coin] = entry_px
                        self._mid_price_ts[coin] = time.time()
                    logger.info(f"RECOVERED: {coin} {side} {abs(sz)} @ {entry_px} -> tracking {matched_wallet[:14]}")
                    _tg(f"RECOVERED: {coin} {side} @ {entry_px} -> {matched_wallet[:10]}")
                else:
                    # Unmatched: auto-close if significant
                    UNMATCHED_CLOSE_THRESHOLD = 5.0
                    if notional >= UNMATCHED_CLOSE_THRESHOLD:
                        logger.warning(
                            f"UNMATCHED: {coin} {side} {abs(sz)} @ {entry_px} (${notional:.2f}) "
                            f"-- AUTO-CLOSING"
                        )
                        _tg(f"UNMATCHED AUTO-CLOSE: {coin} {side} ${notional:.2f}")
                        self.positions.append({
                            'coin': coin, 'side': side, 'entry_px': entry_px,
                            'entry_time': time.time(), 'fill_time': time.time(),
                            'size': abs(sz), 'oid': 0, 'filled': True,
                            'wallet': '__unmatched__', 'target_coin': coin,
                            '_recovered': True, '_force_exit': True,
                        })
                        claimed_coins.add(coin)
                    elif notional >= 1.0:
                        logger.warning(
                            f"UNMATCHED: {coin} {side} {abs(sz)} @ {entry_px} (${notional:.2f}) "
                            f"-- below threshold, treating as dust"
                        )

            logger.info(f"Position recovery (exchange fallback): {len(self.positions)} recovered")
        except Exception as e:
            logger.warning(f"Position recovery failed: {e}")

    # ── Book depth ───────────────────────────────────────────────────────────

    def _get_book_depth(self, coin: str) -> dict:
        """Get L2 book depth in USD for a coin. WS-only, no REST calls."""
        return self._book_depth.get(coin, {"bid_usd": 0, "ask_usd": 0, "best_bid": 0, "best_ask": 0, "ts": 0})

    def _update_book_depth_from_ws(self, coin: str, levels: list):
        """Update book depth cache from WS l2Book stream."""
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
        except Exception:
            pass

    def _query_target_position(self, addr: str, coin: str) -> Optional[float]:
        """Get target's current position size for a coin."""
        dex = _get_market_type(coin) if ":" in coin else ""
        # For regular perps, dex="" works. For builder coins, use the prefix.
        if dex in ("perp", "spot"):
            dex = ""
        try:
            payload = {"type": "clearinghouseState", "user": addr}
            if dex:
                payload["dex"] = dex
            r = requests.post(f"{HL_API}/info", json=payload, timeout=5)
            data = r.json()
            if data is None:
                return None
            for p in data.get("assetPositions", []):
                pos = p["position"]
                if pos["coin"] == coin:
                    return float(pos["szi"])
            return 0.0
        except Exception:
            return None

    def _refresh_target_position(self, wallet: str, coin: str):
        """After exiting OUR position, refresh the target's actual exchange position
        instead of resetting to 0. Prevents misclassifying subsequent fills as new
        entries when the target still holds their position."""
        actual = self._query_target_position(wallet, coin)
        if actual is not None:
            self._target_positions.setdefault(wallet, {})[coin] = actual
            logger.debug(f"TARGET POS REFRESH: {wallet[:14]} {coin} -> {actual:.4f}")
        else:
            # API failed, keep the last known value rather than zeroing out
            logger.warning(f"TARGET POS REFRESH FAILED: {wallet[:14]} {coin} -- keeping current tracker value")

    def _is_opening_trade(self, wallet: str, coin: str, is_buy: bool) -> bool:
        """Determine if the target's TWAP is an opening (increase) or closing (decrease).
        Post-exit cooldown prevents re-entering same wallet+coin for 300s after exit."""
        # Post-exit cooldown: prevent re-entry after we just exited this wallet+coin
        cooldown_key = (wallet, coin)
        last_exit = self._post_exit_cooldown.get(cooldown_key, 0)
        if last_exit > 0 and time.time() - last_exit < 300:
            return False  # recently exited, do not re-enter

        prev_sz = self._target_positions.get(wallet, {}).get(coin, 0)
        mid = self.mid_prices.get(coin, 0)
        prev_notional = abs(prev_sz) * mid if mid > 0 else abs(prev_sz) * 1
        if prev_notional < 1.0 and abs(prev_sz) < 1e-9:
            return True
        if prev_sz > 0 and is_buy:
            return True
        if prev_sz < 0 and not is_buy:
            return True
        return False

    # ── Margin-based risk management ─────────────────────────────────────────

    def _refresh_exchange_state(self) -> bool:
        """Fetch full account state from exchange. Cached for 30s."""
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
            self._exch_margin_used = float(margin.get("totalMarginUsed", 0))

            self._exch_positions = {}
            total_upnl = 0.0
            # Parse positions from default dex
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

            # Also fetch positions from builder dexes (xyz, flx, etc.)
            for dex_name in BUILDER_DEXES:
                try:
                    rd = requests.post(
                        HL_API + "/info",
                        json={"type": "clearinghouseState", "user": self.parent_address, "dex": dex_name},
                        timeout=5,
                    )
                    dex_data = rd.json()
                    dex_margin = dex_data.get("marginSummary", {})
                    self._exch_margin_used += float(dex_margin.get("totalMarginUsed", 0))
                    for p in dex_data.get("assetPositions", []):
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
                except Exception as e:
                    logger.debug(f"Builder dex {dex_name} state fetch failed: {e}")

            self._exch_unrealized_pnl = total_upnl

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

            self._equity_cache = spot
            self._equity_cache_ts = now
            return True
        except Exception as e:
            logger.warning(f"Exchange state fetch failed: {e}")
            return self._equity_cache is not None

    def _verify_order_fill(self, oid: int, coin: str) -> dict | None:
        """Verify a specific order's fill status by order ID.
        Returns fill dict {avgPx, totalSz, status} or None on failure.
        ORDER-LEVEL verification: checks THIS order, not account position."""
        try:
            status = self.info.query_order_by_oid(self.parent_address, oid)
            order = status.get("order", {})
            order_status = order.get("status", "")
            if order_status == "filled":
                return {
                    "avgPx": float(order.get("avgPx", order.get("limitPx", 0))),
                    "totalSz": float(order.get("sz", order.get("origSz", 0))),
                    "status": "filled",
                }
            elif order_status == "canceled":
                # Check if partially filled before cancel
                sz = float(order.get("sz", 0))
                orig = float(order.get("origSz", 0))
                if sz > 0 and sz < orig:
                    return {
                        "avgPx": float(order.get("avgPx", order.get("limitPx", 0))),
                        "totalSz": sz,
                        "status": "partial",
                    }
                return {"avgPx": 0, "totalSz": 0, "status": "canceled"}
            else:
                return {"avgPx": 0, "totalSz": 0, "status": order_status}
        except Exception as e:
            logger.error(f"Order verify failed for oid={oid} {coin}: {e}")
            return None  # None = unknown, don't make decisions

    def _get_equity(self) -> Optional[float]:
        self._refresh_exchange_state()
        return self._equity_cache

    def _get_coin_leverage(self, coin: str) -> int:
        return self.max_leverage.get(coin, 3)

    def _check_margin_budget(self, coin: str, additional_notional: float, wallet: str = None) -> bool:
        """Check if we can afford this entry. Uses per-wallet config for concentration and addon caps."""
        if not self._refresh_exchange_state():
            logger.warning(f"Margin check blocked: no exchange data")
            return False

        equity = self._equity_cache
        if equity is None or equity <= 0:
            logger.warning(f"Margin check blocked: equity={equity}")
            return False

        wc = self._wallet_config(wallet) if wallet else self.default_config
        max_margin_util = self.global_config["max_margin_util"]
        max_coin_conc = wc.get("max_coin_concentration", 0.30)
        max_addon_mult = wc.get("max_addon_multiplier", 50)

        lev = self._get_coin_leverage(coin)
        additional_margin = additional_notional / lev
        total_margin = self._exch_margin_used + self._pending_margin + additional_margin
        util = total_margin / equity
        if util > max_margin_util:
            logger.info(
                f"Margin BLOCKED {coin}: total util {util:.0%} > {max_margin_util:.0%} "
                f"(exch_margin=${self._exch_margin_used:.2f} + pending=${self._pending_margin:.2f} "
                f"+ new=${additional_margin:.2f} / equity=${equity:.2f})"
            )
            return False

        coin_data = self._exch_positions.get(coin, {})
        coin_margin = coin_data.get("marginUsed", 0) + additional_margin
        coin_util = coin_margin / equity
        if coin_util > max_coin_conc:
            logger.info(
                f"Margin BLOCKED {coin}: coin concentration {coin_util:.0%} > {max_coin_conc:.0%}"
            )
            return False

        coin_notional = coin_data.get("positionValue", 0) + additional_notional
        if coin_notional > max_addon_mult * self.order_size:
            logger.info(
                f"Margin BLOCKED {coin}: notional ${coin_notional:.0f} > {max_addon_mult}x base ${self.order_size}"
            )
            return False

        # Hard cap: no single coin > 35% of equity in notional terms
        max_notional_pct = wc.get("max_coin_notional_pct", 0.35)
        if coin_notional > max_notional_pct * equity:
            logger.info(
                f"Margin BLOCKED {coin}: notional ${coin_notional:.0f} = "
                f"{coin_notional/equity:.0%} of equity > {max_notional_pct:.0%} cap"
            )
            return False

        return True

    def _compute_unrealized_pnl(self) -> float:
        self._refresh_exchange_state()
        return getattr(self, '_exch_unrealized_pnl', 0.0)

    def _track_market_pnl(self, market_type: str, pnl_usd: float):
        """Update per-market-type PnL tracking."""
        if market_type not in self.pnl_by_market:
            self.pnl_by_market[market_type] = {"pnl": 0.0, "trades": 0}
        self.pnl_by_market[market_type]["pnl"] += pnl_usd
        self.pnl_by_market[market_type]["trades"] += 1

    def _market_pnl_summary(self) -> str:
        """Format per-market-type PnL for logging."""
        if not self.pnl_by_market:
            return ""
        parts = []
        for mt in sorted(self.pnl_by_market.keys()):
            v = self.pnl_by_market[mt]
            parts.append(f"{mt}=${v['pnl']:+.2f}({v['trades']})")
        return " | ".join(parts)

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _exchange_position_size(self, coin: str) -> float:
        """Return signed position size on exchange for `coin` across main + builder dexes.
        Hard Rule 8: exchange is source of truth. Used to recover from in-memory misses.
        Cached for 5 seconds to avoid hot-path API spam.
        2026-05-22: add-on bug fix dependency.
        """
        now = time.time()
        if not hasattr(self, "_exch_pos_cache"):
            self._exch_pos_cache = {}
            self._exch_pos_cache_ts = 0
        if now - self._exch_pos_cache_ts > 5:
            cache = {}
            try:
                r = requests.post(
                    f"{HL_API}/info",
                    json={"type": "clearinghouseState", "user": self.parent_address},
                    timeout=3,
                )
                for ap in r.json().get("assetPositions", []):
                    p = ap["position"]
                    cache[p["coin"]] = float(p["szi"])
            except Exception:
                pass
            for dex_name in BUILDER_DEXES:
                try:
                    r = requests.post(
                        f"{HL_API}/info",
                        json={"type": "clearinghouseState", "user": self.parent_address, "dex": dex_name},
                        timeout=3,
                    )
                    for ap in r.json().get("assetPositions", []):
                        p = ap["position"]
                        cache[p["coin"]] = float(p["szi"])
                except Exception:
                    pass
            self._exch_pos_cache = cache
            self._exch_pos_cache_ts = now
        return self._exch_pos_cache.get(coin, 0.0)

    def _round_price(self, px: float) -> float:
        if px <= 0:
            return 0.0
        mag = math.floor(math.log10(abs(px)))
        decimals = min(4 - mag, 5)
        return round(px, max(decimals, 0))

    def _round_size(self, coin: str, sz: float) -> float:
        dec = self.sz_decimals.get(coin, 2)
        return round(sz, dec)

    # ── Dynamic l2Book subscription ──────────────────────────────────────────

    def _get_needed_l2_coins(self) -> set:
        """Coins that need l2Book data: coins we hold + coins targets hold."""
        coins = set()
        for pos in self.positions:
            if pos.get('filled'):
                coins.add(pos['coin'])
        for addr, positions in self._target_positions.items():
            for coin, sz in positions.items():
                if abs(sz) > 1e-9:
                    coins.add(coin)
        return coins

    async def _sync_l2_subscriptions(self, ws):
        """Subscribe/unsubscribe l2Book channels to match needed coins."""
        needed = self._get_needed_l2_coins()
        # Subscribe new
        to_add = needed - self._l2_subscribed
        for coin in to_add:
            try:
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "subscription": {"type": "l2Book", "coin": coin}
                }))
                self._l2_subscribed.add(coin)
                logger.debug(f"L2 subscribed: {coin}")
            except Exception as e:
                logger.warning(f"L2 subscribe failed for {coin}: {e}")
        # Unsubscribe stale (only if not needed and we have many subscriptions)
        if len(self._l2_subscribed) > len(needed) + 20:
            to_remove = self._l2_subscribed - needed
            for coin in to_remove:
                try:
                    await ws.send(json.dumps({
                        "method": "unsubscribe",
                        "subscription": {"type": "l2Book", "coin": coin}
                    }))
                    self._l2_subscribed.discard(coin)
                except Exception:
                    pass

    # ── Entry ────────────────────────────────────────────────────────────────

    async def _enter_position(self, coin: str, is_buy: bool, twap_dedup_key=None, wallet: str = None,
                              skip_cooldown: bool = False):
        """Place an order to copy the target wallet's trade. Supports add-ons per wallet config.

        skip_cooldown: set True when the CALLER already checked + set the (wallet,coin) cooldown
        immediately before spawning this task. The instant-entry handler does exactly that ("Fix #5:
        set cooldown BEFORE async task"); without this flag _enter_position re-reads the just-set
        cooldown, sees elapsed ~= 0 < cooldown_s, and returns before placing the order -- which
        silently blocked ALL instant-mode (original_v10) entries (2026-05-31 incident).
        """
        if getattr(self, '_kill_switch_active', False):
            logger.debug(f"Entry blocked (kill switch active): {coin}")
            return

        now = time.time()
        twap_wallet = wallet if wallet else "unknown"
        wc = self._wallet_config(twap_wallet)
        cooldown_s = self.global_config["cooldown_s"]

        # Cooldown check (skipped when the caller already gated + set it -- see skip_cooldown docstring)
        cooldown_key = (twap_wallet, coin)
        if not skip_cooldown and now - self.last_entry.get(cooldown_key, 0) < cooldown_s:
            logger.debug(f"Cooldown active for {coin} from {twap_wallet[:10]}")
            return

        # Check existing position for add-on vs new entry
        existing = None
        max_addon_mult = wc.get("max_addon_multiplier", 50)
        for p in self.positions:
            if p.get("filled") and p["coin"] == coin and p.get("wallet") == twap_wallet:
                existing = p
                break

        # 2026-05-22: if in-memory existing missing, check EXCHANGE truth.
        # Hard Rule 8: exchange is source of truth for positions.
        # Prevents the add-on persistence bug where transient in-memory miss
        # causes "new" entry to overwrite cumulative size on (wallet, coin) upsert.
        #
        # CRITICAL: only reconstruct if NO OTHER wallet already tracks this coin.
        # Otherwise we double-count (exchange is net of all wallets, not per-wallet).
        if existing is None:
            other_wallet_tracks = any(
                p.get("filled") and p["coin"] == coin and p.get("wallet") != twap_wallet
                for p in self.positions
            )
            if other_wallet_tracks:
                logger.info(
                    f"add-on for {coin} {twap_wallet[:10]}: in-memory miss BUT another wallet "
                    f"already tracks this coin -- skipping reconstruct, treating as new position"
                )
            else:
                try:
                    exch_sz = self._exchange_position_size(coin)
                except Exception as exc:
                    logger.debug(f"add-on exch lookup failed for {coin}: {exc}")
                    exch_sz = 0.0
                if abs(exch_sz) > 1e-10:
                    exch_side = "BUY" if exch_sz > 0 else "SELL"
                    if (exch_side == "BUY") == is_buy:
                        # Reconstruct from exchange. Use last known entry price; will be
                        # blended via fill response avgPx on the next add-on.
                        last_px = self.mid_prices.get(coin, 0) or 0
                        existing = {
                            "coin": coin, "side": exch_side,
                            "entry_px": last_px,
                            "entry_time": now, "fill_time": now,
                            "size": abs(exch_sz), "oid": 0, "filled": True,
                            "wallet": twap_wallet, "target_coin": coin,
                            "_reconstructed_from_exchange": True,
                        }
                        self.positions.append(existing)
                        self._persist_position(existing)
                        logger.warning(
                            f"ADD-ON RECONSTRUCT: {coin} {twap_wallet[:10]} -- in-memory miss, "
                            f"adopting exchange position sz={abs(exch_sz):.6f} {exch_side} as add-on base"
                        )

        if existing:
            if max_addon_mult <= 1:
                logger.debug(f"Skipping {coin}: add-ons disabled for {twap_wallet[:10]}")
                return
            if (existing["side"] == "BUY") != is_buy:
                logger.debug(f"Skipping {coin}: existing {existing['side']}, new {'BUY' if is_buy else 'SELL'}")
                return
            logger.info(f"ADD-ON: {twap_wallet[:10]} {coin} -- existing size={existing['size']}")

        # Margin budget check
        if self.shadow_mode:
            shadow_margin = sum(
                abs(p.get('size', 0) * p.get('entry_px', 0)) / self._get_coin_leverage(p['coin'])
                for p in self.positions if p.get('filled')
            )
            equity = self._equity_cache or 500.0  # conservative fallback matching actual account
            shadow_util = (shadow_margin + self.order_size / self._get_coin_leverage(coin)) / equity
            if shadow_util > self.global_config["max_margin_util"]:
                logger.info(f"SHADOW margin blocked {coin}: {shadow_util:.0%} util")
                return
        elif not self._check_margin_budget(coin, self.order_size, wallet=twap_wallet):
            return

        # Use WS-fed book data, fall back to mid-price from trade feed
        book = self._book_depth.get(coin)
        if not book or book.get("ts", 0) == 0 or book.get("best_bid", 0) <= 0:
            # No l2Book for this coin yet. Use mid-price as fallback book.
            fallback_mid = self.mid_prices.get(coin, 0)
            if fallback_mid > 0:
                spread_est = fallback_mid * 0.0005  # estimate 5bps spread
                book = {
                    "best_bid": fallback_mid - spread_est,
                    "best_ask": fallback_mid + spread_est,
                    "bid_usd": 10000,  # assume sufficient depth
                    "ask_usd": 10000,
                    "ts": time.time(),
                }
                logger.info(f"Entry {coin}: using trade-price fallback book (mid={fallback_mid:.4f})")
            else:
                logger.debug(f"Entry skipped for {coin}: no book or mid-price data")
                return
        best_bid = book["best_bid"]
        best_ask = book["best_ask"]
        mid = (best_bid + best_ask) / 2
        self.mid_prices[coin] = mid
        self._mid_price_ts[coin] = time.time()

        sz = self._round_size(coin, self.order_size / mid)
        if sz <= 0:
            return

        # Track pending margin BEFORE await
        lev = self._get_coin_leverage(coin)
        pending_add = self.order_size / lev
        self._pending_margin += pending_add

        # Shadow mode: simulate fill
        if self.shadow_mode:
            fill_px = mid
            fill_sz = sz
            if existing and max_addon_mult > 1:
                old_notional = existing["size"] * existing["entry_px"]
                new_notional = fill_sz * fill_px
                total_sz = existing["size"] + fill_sz
                avg_entry = (old_notional + new_notional) / total_sz if total_sz > 0 else fill_px
                existing["size"] = total_sz
                existing["entry_px"] = avg_entry
                existing.pop("_peak_pnl_bps", None)
            else:
                self.positions.append({
                    "coin": coin, "side": "BUY" if is_buy else "SELL",
                    "entry_px": fill_px, "entry_time": now, "fill_time": now,
                    "size": fill_sz, "oid": 0, "filled": True,
                    "wallet": twap_wallet, "target_coin": coin,
                    "_shadow": True,
                })
            self.last_entry[cooldown_key] = now
            wallet_group = self.wallet_groups.get(twap_wallet, "unknown")
            logger.info(
                f"SHADOW ENTRY: {coin} {'BUY' if is_buy else 'SELL'} {fill_sz:.4f} @ {fill_px:.4f} "
                f"(simulated, group={wallet_group})"
            )
            self.db[DB_SHADOW_COLLECTION].insert_one({
                "type": "entry", "coin": coin, "side": "BUY" if is_buy else "SELL",
                "sim_fill_px": fill_px, "sim_fill_sz": fill_sz,
                "mid_at_signal": mid, "spread_bps": (best_ask - best_bid) / mid * 10000,
                "wallet": twap_wallet, "wallet_group": wallet_group,
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
                entry_oid = statuses[0]["filled"].get("oid", 0)
                self._record_oid(entry_oid, coin, "BUY" if is_buy else "SELL", "entry", wallet=twap_wallet)
                fill_px = float(statuses[0]["filled"]["avgPx"])
                fill_sz = float(statuses[0]["filled"].get("totalSz", sz))
                wallet_group = self.wallet_groups.get(twap_wallet, "unknown")

                if existing and max_addon_mult > 1:
                    # Add-on: merge with averaged entry price
                    # 2026-05-22: instrumentation — capture before/after state to nail down
                    # the recurring HYPE drift root cause (where existing.size resets between merges).
                    _pre_size = existing["size"]
                    _pre_entry = existing["entry_px"]
                    _matches = sum(
                        1 for p in self.positions
                        if p.get("filled") and p["coin"] == coin and p.get("wallet") == twap_wallet
                    )
                    _reconstructed = existing.get("_reconstructed_from_exchange", False)
                    old_notional = existing["size"] * existing["entry_px"]
                    new_notional = fill_sz * fill_px
                    total_sz = existing["size"] + fill_sz
                    avg_entry = (old_notional + new_notional) / total_sz if total_sz > 0 else fill_px
                    existing["size"] = total_sz
                    existing["entry_px"] = avg_entry
                    # Codex R9 fix #6: reset peak PnL on add-on
                    existing.pop("_peak_pnl_bps", None)
                    self._persist_position(existing)  # Update persistent state
                    logger.info(
                        f"ADD-ON FILLED: {coin} {'BUY' if is_buy else 'SELL'} +{fill_sz} @ {fill_px} "
                        f"-> merged: size={total_sz:.4f} avg_entry={avg_entry:.4f} [{wallet_group}] "
                        f"(pre_size={_pre_size:.4f} pre_entry={_pre_entry:.4f} matches={_matches} reconstructed={_reconstructed})"
                    )
                    _tg(f"ADD-ON: {coin} +{fill_sz} @ {fill_px} [{wallet_group}]")
                else:
                    # New position -- use fill response values (order-level truth)
                    new_pos = {
                        "coin": coin, "side": "BUY" if is_buy else "SELL",
                        "entry_px": fill_px, "entry_time": now, "fill_time": now,
                        "size": fill_sz, "oid": 0, "filled": True,
                        "wallet": twap_wallet, "target_coin": coin,
                    }
                    self.positions.append(new_pos)
                    self._persist_position(new_pos)  # Persist to DB
                    logger.info(
                        f"ENTRY FILLED (IOC): {coin} {'BUY' if is_buy else 'SELL'} {fill_sz} @ {fill_px} [{wallet_group}]"
                    )
                    _tg(f"ENTRY: {coin} {'BUY' if is_buy else 'SELL'} {fill_sz} @ {fill_px} [{wallet_group}]")

                self.last_entry[cooldown_key] = now
                self._equity_cache_ts = 0

                # Track TARGET's accumulated notional
                acc_key = (twap_wallet, coin)
                for (w, c), tbuf in self._twap_buffer.items():
                    if c == coin and w == twap_wallet:
                        target_entry_notional = abs(tbuf["buy_notional"] - tbuf["sell_notional"])
                        self._position_accumulated[acc_key] = self._position_accumulated.get(acc_key, 0) + target_entry_notional
                        break

            elif statuses and "error" in statuses[0]:
                logger.warning(f"Entry rejected: {statuses[0]['error']}")
            else:
                # IOC didn't fill and no explicit error -- log it, don't swallow silently
                logger.warning(
                    f"ENTRY NOT FILLED: {coin} {'BUY' if is_buy else 'SELL'} sz={sz} px={px} "
                    f"statuses={statuses} -- IOC returned no fill and no error"
                )

        except Exception as e:
            logger.error(f"Entry error: {e}")
        finally:
            self._pending_margin = max(0, self._pending_margin - pending_add)

    # ── Exit ─────────────────────────────────────────────────────────────────

    async def _check_exits(self):
        """Exit when TARGET exits, not on fixed timer."""
        now = time.time()
        still_open = []
        exited_ids = set()

        for pos in self.positions:
            if not pos['filled']:
                still_open.append(pos)
                continue

            # WS handler marked this position as exited; just skip it
            if pos.get('_ws_exited'):
                exited_ids.add(id(pos))
                continue

            # Force-exit unmatched orphan positions immediately
            if pos.get('_force_exit'):
                coin = pos['coin']
                force_attempts = pos.get('_force_exit_attempts', 0) + 1
                pos['_force_exit_attempts'] = force_attempts
                if force_attempts > 30:
                    mid = self.mid_prices.get(coin, 0)
                    notional = pos['size'] * mid if mid > 0 else 0
                    logger.error(
                        f"FORCE EXIT GAVE UP: {coin} after {force_attempts} attempts "
                        f"(${notional:.2f} notional). Dropping from tracker."
                    )
                    _tg(f"FORCE EXIT FAILED: {coin} ${notional:.2f} -- gave up after {force_attempts} tries")
                    exited_ids.add(id(pos))
                else:
                    logger.info(f"FORCE EXIT: {coin} (unmatched orphan, attempt {force_attempts})")
                    if await self._exit_position(pos):
                        exited_ids.add(id(pos))
                    else:
                        still_open.append(pos)
                continue

            fill_elapsed = now - pos.get('fill_time', pos['entry_time'])
            wallet = pos.get('wallet', '')
            coin = pos['coin']
            wc = self._wallet_config(wallet)

            # Compute current PnL in bps
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

            # EXIT LAYER 1: Hard stop-loss (per-wallet)
            sl_bps = wc.get("sl_bps", -500)
            if sl_bps is not None and mid > 0 and entry_px > 0 and current_pnl_bps <= sl_bps:
                _risk_exit_attempted = True
                wallet_group = self.wallet_groups.get(wallet, "unknown")
                logger.warning(f"HARD SL: {coin} at {current_pnl_bps:.0f}bps (limit: {sl_bps}bps) [{wallet_group}]")
                # TG cooldown: prevents per-loop spam when exit fails and trigger persists.
                # Fix 2026-05-27 (Alberto TG 7510/7512): xyz:MRVL was firing every second.
                _now_ts = time.time()
                if (_now_ts - pos.get("_last_hard_sl_tg_ts", 0)) >= 300:
                    _tg(f"HARD SL: {coin} at {current_pnl_bps:.0f}bps [{wallet_group}]")
                    pos["_last_hard_sl_tg_ts"] = _now_ts
                exited = await self._exit_position(pos)
                if exited:
                    acc_key = (wallet, coin)
                    self._position_accumulated.pop(acc_key, None)
                    self._exit_twap_buffer.pop(acc_key, None)
                    exited_ids.add(id(pos))
                    continue
                # If exit failed, fall through to TWAP exit

            # EXIT LAYER 2: Trailing stop (per-wallet)
            trail_activate = wc.get("trail_activate_bps")
            trail_dist = wc.get("trail_bps")
            if not _risk_exit_attempted and trail_activate is not None and trail_dist is not None and mid > 0 and entry_px > 0:
                if peak_pnl_bps >= trail_activate and (peak_pnl_bps - current_pnl_bps) >= trail_dist:
                    _risk_exit_attempted = True
                    wallet_group = self.wallet_groups.get(wallet, "unknown")
                    logger.warning(
                        f"TRAILING STOP: {coin} peak={peak_pnl_bps:.0f}bps, "
                        f"current={current_pnl_bps:.0f}bps, trail={trail_dist}bps [{wallet_group}]"
                    )
                    # TG cooldown: prevents per-loop spam when exit fails and trigger persists.
                    # Fix 2026-05-27 (Alberto TG 7510/7512): xyz:MRVL was firing every second.
                    _now_ts = time.time()
                    if (_now_ts - pos.get("_last_trail_tg_ts", 0)) >= 300:
                        _tg(f"TRAILING STOP: {coin} peak={peak_pnl_bps:.0f} now={current_pnl_bps:.0f}bps [{wallet_group}]")
                        pos["_last_trail_tg_ts"] = _now_ts
                    exited = await self._exit_position(pos)
                    if exited:
                        acc_key = (wallet, coin)
                        self._position_accumulated.pop(acc_key, None)
                        self._exit_twap_buffer.pop(acc_key, None)
                        exited_ids.add(id(pos))
                        continue
                    # If exit failed, fall through to TWAP exit

            # EXIT LAYER 4: Per-wallet max hold (safety valve)
            wallet_max_hold = wc.get("max_hold_s", 604800)
            if fill_elapsed >= wallet_max_hold:
                exit_attempts = pos.get('_exit_attempts', 0) + 1
                pos['_exit_attempts'] = exit_attempts
                if exit_attempts > 60:
                    if not pos.get('_gave_up'):
                        logger.error(f"GIVING UP on {pos['coin']} after {exit_attempts} exit attempts -- removing from tracker")
                        _tg(f"GAVE UP exiting {pos['coin']} after {exit_attempts} attempts -- position dropped from tracker")
                        pos['_gave_up'] = True
                    acc_key = (pos.get('wallet', ''), pos['coin'])
                    self._position_accumulated.pop(acc_key, None)
                    self._exit_twap_buffer.pop(acc_key, None)
                    exited_ids.add(id(pos))
                    continue
                logger.warning(f"MAX HOLD reached for {pos['coin']} -- force closing (attempt {exit_attempts})")
                if exit_attempts == 1:
                    _tg(f"MAX HOLD: {pos['coin']} -- force closing after {wallet_max_hold}s")
                exited = await self._exit_position(pos)
                if exited:
                    acc_key = (pos.get('wallet', ''), pos['coin'])
                    self._position_accumulated.pop(acc_key, None)
                    self._exit_twap_buffer.pop(acc_key, None)
                    exited_ids.add(id(pos))
                else:
                    still_open.append(pos)
                continue

            # PRIMARY EXIT: trade-stream based reverse TWAP detection
            exit_key = (wallet, coin)
            exit_min_trim_pct = self.global_config.get("exit_min_trim_pct", 0.05)
            # Fix #7: use per-wallet exit_twap_min_notional instead of global
            exit_min_trim_usd = wc.get("exit_twap_min_notional", self.global_config.get("exit_min_trim_usd", 3.0))

            if exit_key in self._exit_twap_buffer:
                ebuf = self._exit_twap_buffer[exit_key]
                exit_elapsed = now - ebuf['first_ts']

                wallet_exit = wc.get("exit_type", "FIRST_CLOSE")
                acc_key = (wallet, coin)
                accumulated = self._position_accumulated.get(acc_key, 0)

                reverse_notional = ebuf['reverse_notional']
                if accumulated <= 0:
                    mid_val = self.mid_prices.get(coin, 0)
                    accumulated = pos['size'] * mid_val if mid_val > 0 else pos['size'] * pos.get('entry_px', 1)
                    self._position_accumulated[acc_key] = accumulated
                trim_pct = reverse_notional / accumulated if accumulated > 0 else 0.0
                trim_pct = min(trim_pct, 1.0)

                should_trim = False
                if wallet_exit == "FIRST_CLOSE":
                    should_trim = (trim_pct >= exit_min_trim_pct
                                   or reverse_notional >= exit_min_trim_usd)
                elif exit_elapsed >= EXIT_TWAP_WINDOW_S:
                    should_trim = (trim_pct >= exit_min_trim_pct
                                   or reverse_notional >= exit_min_trim_usd)

                if should_trim:
                    trim_pct_display = trim_pct * 100
                    is_full_exit = trim_pct >= 0.90
                    if not pos.get('_exit_logged') or pos.get('_last_trim_pct', 0) != round(trim_pct, 2):
                        action_str = "FULL EXIT" if is_full_exit else f"TRIM {trim_pct_display:.0f}%"
                        logger.info(
                            f"TARGET {action_str} ({wallet_exit}): {wallet[:10]} {coin} "
                            f"reverse=${reverse_notional:,.0f} = {trim_pct_display:.0f}% of ${accumulated:,.0f}"
                        )
                        _tg(f"{'FULL EXIT' if is_full_exit else 'TRIM'}: {coin} -- {'closing' if is_full_exit else 'trimming'}")
                        pos['_exit_logged'] = True
                        pos['_last_trim_pct'] = round(trim_pct, 2)

                    last_exit_attempt = pos.get('_last_exit_attempt', 0)
                    if now - last_exit_attempt < 10:
                        still_open.append(pos)
                        continue
                    pos['_last_exit_attempt'] = now

                    if is_full_exit:
                        exited = await self._exit_position(pos)
                        if exited:
                            self._refresh_target_position(wallet, coin)
                            del self._exit_twap_buffer[exit_key]
                            self._position_accumulated.pop(acc_key, None)
                            exited_ids.add(id(pos))
                        else:
                            still_open.append(pos)
                    else:
                        trim_size = pos['size'] * trim_pct
                        exited = await self._exit_position(pos, trim_size=trim_size)
                        if exited:
                            pos_sz = self._target_positions.get(wallet, {}).get(coin, 0)
                            reverse_sz = reverse_notional / (self.mid_prices.get(coin, 1) or 1)
                            if pos_sz > 0:
                                self._target_positions.setdefault(wallet, {})[coin] = max(0, pos_sz - reverse_sz)
                            else:
                                self._target_positions.setdefault(wallet, {})[coin] = min(0, pos_sz + reverse_sz)
                            self._position_accumulated[acc_key] = max(0, accumulated - reverse_notional)
                            del self._exit_twap_buffer[exit_key]
                            pos['_exit_logged'] = False
                            if pos['size'] * (self.mid_prices.get(coin, 1) or 1) < 1.0:
                                pass  # dust remaining
                            else:
                                still_open.append(pos)
                        else:
                            still_open.append(pos)
                    continue

                elif exit_elapsed >= EXIT_TWAP_WINDOW_S and not should_trim and wallet_exit == "GRADUAL":
                    if acc_key in self._position_accumulated:
                        self._position_accumulated[acc_key] = max(0,
                            self._position_accumulated[acc_key] - ebuf['reverse_notional'])
                    del self._exit_twap_buffer[exit_key]

            still_open.append(pos)

        # Remove exited positions from persistent DB state
        for pos in self.positions:
            if id(pos) in exited_ids:
                self._remove_persisted_position(pos.get('wallet', ''), pos['coin'])

        # Merge still_open with NEW entries appended during awaits
        known_ids = {id(pos) for pos in still_open} | exited_ids
        new_during_exit = [p for p in self.positions if id(p) not in known_ids]
        self.positions = still_open + new_during_exit

    async def _exit_position(self, pos: dict, trim_size: float = None) -> bool:
        """Exit a position (full or partial trim).
        Shadow mode: simulate. Live: try maker first, IOC fallback."""
        coin = pos['coin']
        is_buy = pos['side'] == 'SELL'
        is_trim = trim_size is not None and trim_size < pos['size'] * 0.99
        exit_sz = trim_size if trim_size is not None else pos['size']

        # Tag trim mode so WS handler knows not to remove the full position
        if is_trim:
            pos['_trim_mode'] = True
            pos['_trim_target_sz'] = exit_sz
        else:
            pos.pop('_trim_mode', None)
            pos.pop('_trim_target_sz', None)

        # Safety clamp: bound exit by actual exchange position to prevent overshooting
        self._refresh_exchange_state()
        exch_pos = self._exch_positions.get(coin, {})
        actual_sz = abs(exch_pos.get('szi', 0))
        if actual_sz < 1e-10:
            wallet = pos.get('wallet', '')
            # Builder dex coins may fail to query -- don't drop if the coin is xyz:/flx:
            is_builder = coin.startswith("xyz:") or coin.startswith("flx:")
            if is_builder and coin not in self._exch_positions:
                logger.warning(f"Exit retry {coin}: builder dex query may have failed (not in _exch_positions), keeping position")
                return False  # keep trying, don't drop
            logger.warning(f"Exit skip {coin}: no exchange position (tracker had {pos['size']})")
            # Record post-exit cooldown
            self._post_exit_cooldown[(wallet, coin)] = time.time()
            return True  # remove from tracker, nothing to close
        # Direction check: if tracked position side doesn't match exchange side,
        # exit would INCREASE position (reduce-only reject). This happens when
        # multiple wallets trade the same coin and the net position flips direction.
        exch_szi = exch_pos.get('szi', 0)
        exch_is_long = exch_szi > 0
        tracker_is_long = pos['side'] == 'BUY'
        if exch_is_long != tracker_is_long:
            wallet = pos.get('wallet', '')
            wallet_group = self.wallet_groups.get(wallet, "unknown")
            logger.warning(
                f"Exit direction mismatch {coin}: tracker={pos['side']} but exchange={'LONG' if exch_is_long else 'SHORT'} "
                f"(multi-wallet netting). Removing tracked position. [{wallet_group}]"
            )
            self._post_exit_cooldown[(wallet, coin)] = time.time()
            return True  # remove from tracker, exchange position is netted differently

        if exit_sz > actual_sz:
            logger.warning(
                f"Exit clamp {coin}: tracker={exit_sz:.6f} > exchange={actual_sz:.6f}, "
                f"clamping to exchange size"
            )
            exit_sz = actual_sz

        sz = self._round_size(coin, exit_sz)
        if sz <= 0:
            logger.warning(f"Exit skip {coin}: size {pos['size']} rounds to 0")
            return False

        # Fix: HL requires minimum $10 order value. If rounded size is sub-minimum,
        # try the TRACKED position size (not full exchange, which may include other wallets).
        # Only as last resort, use full exchange size if this IS the only tracked position for this coin.
        mid = self.mid_prices.get(coin, 0)
        HL_MIN_ORDER_VALUE = 10.0
        if mid > 0 and sz * mid < HL_MIN_ORDER_VALUE:
            # First try: use the full tracked position size (pos['size'])
            tracked_sz = self._round_size(coin, pos['size'])
            if tracked_sz > 0 and tracked_sz * mid >= HL_MIN_ORDER_VALUE:
                logger.info(
                    f"Exit bump {coin}: sz={sz} (${sz*mid:.2f}) below $10 min, "
                    f"using full tracked size sz={tracked_sz} (${tracked_sz*mid:.2f})"
                )
                sz = tracked_sz
            else:
                # Check if we're the ONLY wallet tracking this coin
                other_wallets_same_coin = [
                    p for p in self.positions
                    if p['coin'] == coin and p.get('wallet', '') != pos.get('wallet', '')
                    and p.get('filled')
                ]
                if not other_wallets_same_coin and actual_sz > 0:
                    # Safe to use exchange size: no other wallet holds this coin
                    full_sz = self._round_size(coin, actual_sz)
                    if full_sz > 0 and full_sz * mid >= HL_MIN_ORDER_VALUE:
                        logger.info(
                            f"Exit bump {coin}: tracked=${tracked_sz*mid:.2f} also sub-min, "
                            f"using exchange sz={full_sz} (${full_sz*mid:.2f}) [sole holder]"
                        )
                        sz = full_sz
                    else:
                        # Hold until closeable: price movement or add-ons may push above $10
                        if not pos.get('_sub_min_logged'):
                            logger.warning(
                                f"SUB-MINIMUM HOLD: {coin} sz={actual_sz} notional=${actual_sz*mid:.2f} "
                                f"< $10 HL minimum -- holding until closeable"
                            )
                            pos['_sub_min_logged'] = True
                        return False  # keep in tracker, retry later
                else:
                    # Other wallets hold this coin too: hold until closeable
                    if not pos.get('_sub_min_logged'):
                        logger.warning(
                            f"SUB-MINIMUM HOLD (multi-wallet): {coin} sz={sz} (${sz*mid:.2f}) "
                            f"< $10 min, {len(other_wallets_same_coin)} other wallet(s) hold same coin "
                            f"-- holding until closeable"
                        )
                        pos['_sub_min_logged'] = True
                    return False  # keep in tracker, retry later

        wallet = pos.get('wallet', '')
        wallet_group = self.wallet_groups.get(wallet, "unknown")

        # Shadow exit
        if self.shadow_mode:
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
                f"PnL={pnl_bps:+.0f}bps hold={hold_s:.0f}s [{wallet_group}]"
            )
            self.db[DB_SHADOW_COLLECTION].insert_one({
                "type": "exit", "coin": coin, "side": pos['side'],
                "entry_px": entry_px, "exit_px": mid,
                "pnl_bps": pnl_bps, "hold_s": hold_s,
                "wallet": wallet, "wallet_group": wallet_group,
                "timestamp": datetime.now(timezone.utc),
            })
            pnl_usd = pnl_bps / 10000 * pos.get('size', 0) * mid
            # PnL tracked by exchange fill sync, not here
            self._post_exit_cooldown[(wallet, coin)] = time.time()
            return True

        try:
            # Dust check: only drop positions that are TRULY uncloseable
            # (below HL minimum order value AND were recovered, not actively entered)
            mid = self.mid_prices.get(coin, 0)
            notional = sz * mid if mid > 0 else 0
            if notional > 0 and notional < 1.0 and pos.get('_recovered'):
                # Only drop recovered positions below $1 (true residual dust)
                if not pos.get('_dust_logged'):
                    logger.warning(
                        f"DUST POSITION: {coin} sz={sz} notional=${notional:.2f} < $1, "
                        f"recovered residual -- dropping from tracker"
                    )
                    pos['_dust_logged'] = True
                self._post_exit_cooldown[(wallet, coin)] = time.time()
                return True  # remove from tracker, true dust
            # Positions V11 entered are NEVER dust regardless of notional
            # They must be properly exited or held until target closes

            ws_book = self._book_depth.get(coin)
            if not ws_book or ws_book.get("best_bid", 0) <= 0:
                logger.debug(f"Exit deferred for {coin}: no WS book data")
                return False

            best_bid = ws_book["best_bid"]
            best_ask = ws_book["best_ask"]

            # Try maker exit first
            if not pos.get('_maker_exit_tried'):
                if is_buy:
                    px = self._round_price(best_bid)
                else:
                    px = self._round_price(best_ask)

                result = await asyncio.to_thread(
                    self.exchange.order, coin, is_buy, sz, px,
                    {"limit": {"tif": "Alo"}}, True
                )
                statuses = result.get("response", {}).get("data", {}).get("statuses", [{}])
                if statuses and "resting" in statuses[0]:
                    pos['exit_oid'] = statuses[0]["resting"]["oid"]
                    self._record_oid(pos['exit_oid'], coin, "BUY" if is_buy else "SELL", "exit", wallet=pos.get('wallet', ''))
                    pos['_maker_exit_tried'] = True
                    pos['_maker_exit_time'] = time.time()
                    logger.info(f"EXIT MAKER: {coin} {'BUY' if is_buy else 'SELL'} {sz} @ {px}")
                    return False
                elif statuses and "filled" in statuses[0]:
                    # Dedup guard: if WS _on_order_update already recorded this exit, skip
                    if pos.get('_exit_recorded'):
                        logger.debug(f"EXIT dedup: {coin} already recorded by WS handler")
                        return True
                    pos['_exit_recorded'] = True
                    exit_px = float(statuses[0]["filled"]["avgPx"])
                    filled_sz = float(statuses[0]["filled"].get("totalSz", sz))
                    if pos['side'] == 'BUY':
                        pnl_bps = (exit_px - pos['entry_px']) / pos['entry_px'] * 10000
                    else:
                        pnl_bps = (pos['entry_px'] - exit_px) / pos['entry_px'] * 10000
                    pnl_usd = pnl_bps / 10000 * filled_sz * exit_px
                    # PnL tracked by exchange fill sync, not here
                    self._equity_cache_ts = 0
                    self._refresh_target_position(wallet, coin)
                    logger.info(
                        f"EXIT FILLED (MAKER): {coin} {pos['side']} "
                        f"entry={pos['entry_px']} exit={exit_px} filled={filled_sz}/{pos['size']} "
                        f"pnl={pnl_bps:+.1f}bp (${pnl_usd:+.4f}) acct=${self._exch_pnl['account_net']:+.4f} [{wallet_group}]"
                    )
                    mt = _get_market_type(coin)
                    self.db[DB_COLLECTION].insert_one({
                        "target_wallet": wallet,
                        "wallet_group": wallet_group,
                        "coin": coin, "side": pos['side'],
                        "entry_px": pos['entry_px'], "exit_px": exit_px,
                        "size": filled_sz, "pnl_bps": pnl_bps, "pnl_usd": pnl_usd,
                        "exit_type": "maker_immediate",
                        "market_type": mt,
                        "hold_s": time.time() - pos.get('fill_time', pos['entry_time']),
                        "timestamp": datetime.now(timezone.utc),
                    })
                    self._track_market_pnl(mt, pnl_usd)
                    self._post_exit_cooldown[(wallet, coin)] = time.time()
                    return True
                else:
                    error_msg = statuses[0] if statuses else "empty status"
                    logger.warning(f"EXIT MAKER REJECTED: {coin} sz={sz} px={px} -- {error_msg}")

            # Maker exit state machine
            if pos.get('_maker_exit_tried'):
                elapsed_since_maker = time.time() - pos.get('_maker_exit_time', 0)
                if elapsed_since_maker < 60:
                    return False
                if pos.get('exit_oid'):
                    try:
                        status = await asyncio.to_thread(
                            self.info.query_order_by_oid, self.parent_address, int(pos['exit_oid']))
                        order_status = status.get("order", {}).get("status", "")
                        if order_status == "filled":
                            exit_px = float(status["order"].get("limitPx", best_bid if not is_buy else best_ask))
                            # BUG FIX: use actual filled size, not origSz (requested)
                            # origSz is what we ASKED for, not what actually filled
                            filled_sz = float(status["order"].get("sz", status["order"].get("origSz", "0")))
                            orig_sz = float(status["order"].get("origSz", "0"))
                            if orig_sz > 0 and filled_sz < orig_sz * 0.99:
                                logger.warning(
                                    f"PARTIAL MAKER EXIT: {coin} filled={filled_sz}/{orig_sz} "
                                    f"-- residual {orig_sz - filled_sz:.6f} remains on exchange"
                                )
                            if pos['side'] == 'BUY':
                                pnl_bps = (exit_px - pos['entry_px']) / pos['entry_px'] * 10000
                            else:
                                pnl_bps = (pos['entry_px'] - exit_px) / pos['entry_px'] * 10000
                            pnl_usd = pnl_bps / 10000 * filled_sz * exit_px
                            # PnL tracked by exchange fill sync, not here
                            logger.info(
                                f"EXIT MAKER FILLED (detected at timeout): {coin} {pos['side']} "
                                f"entry={pos['entry_px']} exit={exit_px} sz={filled_sz} "
                                f"pnl={pnl_bps:+.1f}bp (${pnl_usd:+.4f}) [{wallet_group}]")
                            mt = _get_market_type(coin)
                            self.db[DB_COLLECTION].insert_one({
                                "target_wallet": wallet,
                                "wallet_group": wallet_group,
                                "coin": coin, "side": pos['side'],
                                "entry_px": pos['entry_px'], "exit_px": exit_px,
                                "size": filled_sz, "pnl_bps": pnl_bps, "pnl_usd": pnl_usd,
                                "exit_type": "maker_late_detect",
                                "market_type": mt,
                                "hold_s": time.time() - pos.get('fill_time', pos['entry_time']),
                                "timestamp": datetime.now(timezone.utc),
                            })
                            self._track_market_pnl(mt, pnl_usd)
                            self._equity_cache_ts = 0
                            pos.pop('exit_oid', None)
                            pos['_maker_exit_tried'] = False

                            # Check trim mode OR partial fill
                            is_trim_exit = pos.get('_trim_mode', False)
                            is_partial_fill = orig_sz > 0 and filled_sz < orig_sz * 0.99

                            if is_trim_exit or is_partial_fill:
                                # Trim or partial: reduce size, keep tracking
                                old_size = pos['size']
                                pos['size'] = max(0, old_size - filled_sz)
                                pos.pop('_trim_mode', None)
                                pos.pop('_trim_target_sz', None)
                                label = "TRIM" if is_trim_exit else "PARTIAL"
                                logger.info(
                                    f"{label} EXIT: {coin} filled={filled_sz:.4f}, "
                                    f"residual={pos['size']:.4f} (was {old_size:.4f})"
                                )
                                if pos['size'] * (self.mid_prices.get(coin, 1) or 1) < 1.0:
                                    # Dust residual, clean up
                                    self._refresh_target_position(wallet, coin)
                                    self._position_accumulated.pop((wallet, coin), None)
                                    self._exit_twap_buffer.pop((wallet, coin), None)
                                    self._post_exit_cooldown[(wallet, coin)] = time.time()
                                    return True
                                self._persist_position(pos)
                                return False  # keep position for future exits
                            else:
                                # Full fill, full exit: clean removal
                                self._refresh_target_position(wallet, coin)
                                acc_key = (wallet, coin)
                                self._position_accumulated.pop(acc_key, None)
                                self._exit_twap_buffer.pop(acc_key, None)
                                self._post_exit_cooldown[(wallet, coin)] = time.time()
                                return True
                    except Exception as e:
                        logger.debug(f"Order status check failed for {coin}: {e}")
                    try:
                        await asyncio.to_thread(self.exchange.cancel, coin, int(pos['exit_oid']))
                    except Exception:
                        pass
                pos['_maker_exit_tried'] = False
                pos.pop('exit_oid', None)
                logger.info(f"EXIT MAKER timeout -- falling back to IOC for {coin}")

            # IOC fallback with escalating aggression
            ioc_attempts = pos.get('_ioc_exit_attempts', 0)
            pos['_ioc_exit_attempts'] = ioc_attempts + 1
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
                {"limit": {"tif": "Ioc"}}, True
            )
            statuses = result.get("response", {}).get("data", {}).get("statuses", [{}])

            if statuses and "filled" in statuses[0]:
                # Dedup: if WS already recorded this exit, skip
                if pos.get('_exit_recorded') or pos.get('exit_filled'):
                    logger.debug(f"EXIT dedup (IOC): {coin} already recorded")
                    return True
                pos['_exit_recorded'] = True
                ioc_exit_oid = statuses[0]["filled"].get("oid", 0)
                self._record_oid(ioc_exit_oid, coin, "BUY" if is_buy else "SELL", "exit", wallet=pos.get('wallet', ''))
                pos.pop('_ioc_exit_attempts', None)
                exit_px = float(statuses[0]["filled"]["avgPx"])
                filled_sz = float(statuses[0]["filled"].get("totalSz", sz))

                if pos['side'] == 'BUY':
                    pnl_bps = (exit_px - pos['entry_px']) / pos['entry_px'] * 10000
                else:
                    pnl_bps = (pos['entry_px'] - exit_px) / pos['entry_px'] * 10000

                pnl_usd = pnl_bps / 10000 * filled_sz * exit_px
                # PnL tracked by exchange fill sync, not here
                self._equity_cache_ts = 0

                requested_sz = sz
                remainder = requested_sz - filled_sz
                is_partial = remainder > 1e-8

                is_full_position_exit = (trim_size is None) and not is_partial
                if is_full_position_exit:
                    self._refresh_target_position(wallet, coin)

                logger.info(
                    f"EXIT{'(PARTIAL)' if is_partial else ''}: {coin} {pos['side']} "
                    f"entry={pos['entry_px']} exit={exit_px} filled={filled_sz}/{pos['size']} "
                    f"pnl={pnl_bps:+.1f}bp (${pnl_usd:+.4f}) acct=${self._exch_pnl['account_net']:+.4f} [{wallet_group}]"
                )

                mt = _get_market_type(coin)
                self.db[DB_COLLECTION].insert_one({
                    "target_wallet": wallet,
                    "wallet_group": wallet_group,
                    "coin": coin, "side": pos['side'],
                    "entry_px": pos['entry_px'], "exit_px": exit_px,
                    "size": filled_sz, "pnl_bps": pnl_bps, "pnl_usd": pnl_usd,
                    "exit_type": "ioc" + ("_partial" if is_partial else ""),
                    "market_type": mt,
                    "hold_s": time.time() - pos.get('fill_time', pos['entry_time']),
                    "timestamp": datetime.now(timezone.utc),
                })
                self._track_market_pnl(mt, pnl_usd)

                if is_partial:
                    pos['_maker_exit_tried'] = False
                    # Fix #4: reduce pos size by filled amount to prevent over-close on retry
                    pos['size'] = pos['size'] - filled_sz
                    self._persist_position(pos)  # Update DB with reduced size
                    logger.warning(f"PARTIAL EXIT: {coin} filled={filled_sz}/{requested_sz}, remaining={pos['size']:.4f} -- will retry")
                    return False

                if trim_size is not None:
                    pos['size'] = pos['size'] - filled_sz
                    if pos['size'] * (self.mid_prices.get(coin, 1) or 1) < 1.0:
                        pos['size'] = 0
                    if pos['size'] > 0:
                        self._persist_position(pos)  # Persist reduced size after trim

                self._post_exit_cooldown[(wallet, coin)] = time.time()
                return True

            error_detail = statuses[0] if statuses else "empty status"
            logger.warning(f"EXIT FAILED: {coin} IOC not filled sz={sz} px={px} -- {error_detail}")
            return False

        except Exception as e:
            logger.error(f"Exit error for {coin}: {e}")
            return False

    # ── Trade handler ────────────────────────────────────────────────────────

    def _on_hl_trade(self, trade: dict):
        """Process HL WS trade -- detect target wallet from users field.
        No coin filter: subscribed to ALL coins, check wallet only."""
        users = trade.get("users", [])
        if len(users) < 2:
            return

        buyer = users[0].lower()
        seller = users[1].lower()

        is_target = False
        is_buy = False
        matched_wallet = ""

        if buyer in self.target_set:
            is_target = True
            is_buy = True
            matched_wallet = buyer
        elif seller in self.target_set:
            is_target = True
            is_buy = False
            matched_wallet = seller

        if not is_target:
            return

        # Resolve vault leaders back to vault address for config/position tracking
        if matched_wallet in self.leader_to_vault:
            matched_wallet = self.leader_to_vault[matched_wallet]

        coin = trade.get("coin", "")
        sz = float(trade.get("sz", 0))
        px = float(trade.get("px", 0))
        notional = sz * px
        tid = trade.get("tid", "")

        # De-dupe by trade ID
        if tid:
            if tid in self._seen_tids:
                return
            self._seen_tids[tid] = time.time()
        if len(self._seen_tids) > 10000:
            cutoff = time.time() - 300
            self._seen_tids = {k: v for k, v in self._seen_tids.items() if v > cutoff}

        # Store raw target fill for forensics
        wallet_group = self.wallet_groups.get(matched_wallet, "unknown")
        mt = _get_market_type(coin)
        self.db[DB_FILLS_COLLECTION].insert_one({
            "wallet": matched_wallet, "coin": coin,
            "side": "BUY" if is_buy else "SELL",
            "price": px, "size": sz, "notional": notional,
            "wallet_group": wallet_group,
            "market_type": mt,
            "timestamp": datetime.now(timezone.utc),
            "ts_epoch": time.time(),
        })

        # Get per-wallet config
        wc = self._wallet_config(matched_wallet)
        entry_mode = wc.get("entry_mode", "twap")
        now = time.time()

        if entry_mode == "instant":
            # ── INSTANT ENTRY (V10 style): enter on first fill ──
            self._handle_instant_entry(matched_wallet, coin, is_buy, px, notional, now, wc)
        else:
            # ── TWAP ENTRY (V9 style): accumulate fills ──
            self._handle_twap_entry(matched_wallet, coin, is_buy, px, notional, now, wc)

        # ── EXIT TWAP: detect reverse flow for open positions ──
        for pos in self.positions:
            if pos['coin'] == coin and pos.get('wallet') == matched_wallet and pos['filled']:
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

    def _handle_instant_entry(self, wallet: str, coin: str, is_buy: bool,
                               px: float, notional: float, now: float, wc: dict):
        """V10-style immediate entry with entry guards."""
        cooldown_s = self.global_config["cooldown_s"]
        max_chase_bps = self.global_config["max_chase_bps"]
        max_spread_bps = self.global_config["max_spread_bps"]
        min_book_depth = self.global_config["min_book_depth_usd"]
        # Adaptive limits for synthetic markets (xyz:, flx:, etc.)
        if ":" in coin:
            book = self._book_depth.get(coin, {})
            bid = book.get("best_bid", 0)
            ask = book.get("best_ask", 0)
            mid = self.mid_prices.get(coin, 0)
            live_spread_bps = (ask - bid) / mid * 10000 if mid > 0 and bid > 0 and ask > 0 else 20
            # Chase = max(default, 3x current spread), capped at 50 bps
            max_chase_bps = min(50, max(max_chase_bps, live_spread_bps * 3))
            # Allow wider spreads for synthetics, up to 40 bps
            max_spread_bps = min(40, max(max_spread_bps, live_spread_bps * 2))
            # Lower depth requirement for synthetics
            min_book_depth = min(min_book_depth, 1000)

        logger.info(
            f"FILL: {wallet[:10]} {coin} {'BUY' if is_buy else 'SELL'} "
            f"${notional:,.0f} @ {px}"
        )

        # Skip if we already have a position and add-ons are disabled
        max_addon_mult = wc.get("max_addon_multiplier", 50)
        existing = None
        for pos in self.positions:
            if pos['coin'] == coin and pos.get('wallet') == wallet and pos['filled']:
                existing = pos
                break
        if existing and max_addon_mult <= 1:
            logger.debug(f"V11: skip {coin} -- already have position, no add-ons")
            return

        cooldown_key = (wallet, coin)
        if now - self.last_entry.get(cooldown_key, 0) < cooldown_s:
            logger.debug(f"V11: cooldown active for {coin}")
            return

        if not self._is_opening_trade(wallet, coin, is_buy):
            logger.debug(f"V11: not an opening trade for {coin}")
            return

        if not self.shadow_mode and not self._check_margin_budget(coin, self.order_size, wallet=wallet):
            return

        # Entry guard: chase distance
        mid = self.mid_prices.get(coin, 0)
        if mid <= 0:
            # Fix #1: No l2Book for this coin yet. Use trade price as fallback mid,
            # subscribe l2Book dynamically, and proceed (don't skip the trade).
            mid = px
            self.mid_prices[coin] = px
            self._mid_price_ts[coin] = time.time()
            if coin not in self._l2_subscribed:
                logger.info(f"V11: dynamic l2Book subscribe for {coin} (first target fill)")
                # Will be subscribed on next _sync_l2_subscriptions cycle
                self._l2_subscribed.add(coin)  # Mark for subscription
        chase_bps = abs(mid - px) / px * 10000
        if chase_bps > max_chase_bps:
            logger.info(f"V11 SKIP {coin}: chase {chase_bps:.0f}bps > {max_chase_bps}bps")
            return

        # Entry guard: spread
        book = self._book_depth.get(coin, {})
        bid = book.get("best_bid", 0)
        ask = book.get("best_ask", 0)
        spread_bps = (ask - bid) / mid * 10000 if mid > 0 and bid > 0 and ask > 0 else 999
        if spread_bps > max_spread_bps:
            logger.info(f"V11 SKIP {coin}: spread {spread_bps:.0f}bps > {max_spread_bps}bps")
            return

        # Entry guard: book depth
        depth = self._get_book_depth(coin)
        entry_depth = depth["ask_usd"] if is_buy else depth["bid_usd"]
        if entry_depth < min_book_depth:
            logger.info(f"V11 SKIP {coin}: depth ${entry_depth:.0f} < ${min_book_depth}")
            return

        # ALL GUARDS PASS -- enter immediately
        wallet_group = self.wallet_groups.get(wallet, "unknown")
        dedup_key = (wallet, coin, int(now))

        # Fix #5: set cooldown BEFORE async task to prevent burst duplicates
        cooldown_key = (wallet, coin)
        self.last_entry[cooldown_key] = now

        logger.info(
            f"V11 ENTRY: {wallet[:10]} {coin} {'BUY' if is_buy else 'SELL'} "
            f"${notional:,.0f} -- chase={chase_bps:.0f}bps spread={spread_bps:.0f}bps "
            f"depth=${entry_depth:,.0f} [{wallet_group}]"
        )
        _tg(f"ENTRY: {coin} {'BUY' if is_buy else 'SELL'} -- {wallet[:10]} [{wallet_group}]")
        self._twap_entered.add(dedup_key)
        trade_sz = notional / mid if mid > 0 else 0
        if not is_buy:
            trade_sz = -trade_sz
        prev = self._target_positions.get(wallet, {}).get(coin, 0)
        self._target_positions.setdefault(wallet, {})[coin] = prev + trade_sz
        # Fix #6: seed accumulated notional for instant entries (needed for exit trim math)
        acc_key = (wallet, coin)
        self._position_accumulated[acc_key] = self._position_accumulated.get(acc_key, 0) + notional
        asyncio.get_event_loop().create_task(
            self._enter_position(coin, is_buy, twap_dedup_key=dedup_key, wallet=wallet,
                                 skip_cooldown=True)  # handler already gated+set the cooldown above
        )

    def _handle_twap_entry(self, wallet: str, coin: str, is_buy: bool,
                            px: float, notional: float, now: float, wc: dict):
        """V9-style TWAP accumulation entry."""
        twap_window_s = wc.get("twap_window_s", 120)
        min_twap_notional = wc.get("min_twap_notional", 100)

        # Aggregate into TWAP buffer
        twap_key = (wallet, coin)
        if twap_key not in self._twap_buffer:
            self._twap_buffer[twap_key] = {
                'first_ts': now, 'last_ts': now,
                'buy_notional': 0, 'sell_notional': 0,
                'buy_sz': 0, 'sell_sz': 0,  # for VWAP computation
                'count': 0, 'wallet': wallet,
                'first_mid': self.mid_prices.get(coin, 0),  # baseline for chase guard
            }
            logger.info(
                f"TWAP START: {wallet[:14]} {'BUY' if is_buy else 'SELL'} {coin} "
                f"${notional:,.0f} -- aggregating for {twap_window_s}s"
            )

        buf = self._twap_buffer[twap_key]
        buf['last_ts'] = now
        sz = notional / px if px > 0 else 0  # derive size from notional and price
        if is_buy:
            buf['buy_notional'] += notional
            buf['buy_sz'] = buf.get('buy_sz', 0) + sz
        else:
            buf['sell_notional'] += notional
            buf['sell_sz'] = buf.get('sell_sz', 0) + sz
        buf['count'] += 1

        # Set mid-price from fill if not yet known (needed for entry at TWAP completion)
        if coin not in self.mid_prices or self.mid_prices[coin] <= 0:
            self.mid_prices[coin] = px
            self._mid_price_ts[coin] = now
            if coin not in self._l2_subscribed:
                logger.info(f"TWAP: dynamic l2Book subscribe for {coin} (first target fill)")
                self._l2_subscribed.add(coin)
        else:
            # Update mid from latest fill price if our data is stale (>30s)
            if now - self._mid_price_ts.get(coin, 0) > 30:
                self.mid_prices[coin] = px
                self._mid_price_ts[coin] = now

        net = buf['buy_notional'] - buf['sell_notional']
        abs_net = abs(net)
        gross = buf['buy_notional'] + buf['sell_notional']
        dir_pct = abs_net / gross if gross > 0 else 0

        logger.info(
            f"TWAP FILL #{buf['count']}: {coin} {'BUY' if is_buy else 'SELL'} ${notional:,.0f} "
            f"(net: ${net:+,.0f} in {buf['count']} fills)"
        )

        # Track target accumulation for open positions (debounced)
        in_active_twap = twap_key in self._twap_buffer
        twap_completed_at = self._twap_completed_ts.get(twap_key, 0)
        in_debounce = (time.time() - twap_completed_at) < 10.0
        for pos in self.positions:
            if pos['coin'] == coin and pos.get('wallet') == wallet and pos['filled']:
                pos_is_long = pos['side'] == 'BUY'
                same_direction = (pos_is_long and is_buy) or (not pos_is_long and not is_buy)
                if same_direction and not in_active_twap and not in_debounce:
                    acc_key = (wallet, coin)
                    self._position_accumulated[acc_key] = self._position_accumulated.get(acc_key, 0) + notional

        # Mid-TWAP early entry: check OB impact
        dedup_key = (wallet, coin, int(buf['first_ts']))
        if (dedup_key not in self._twap_entered
                and abs_net >= min_twap_notional
                and dir_pct >= 0.6
                and buf['count'] >= 2):
            depth = self._get_book_depth(coin)
            book_side = depth["ask_usd"] if net > 0 else depth["bid_usd"]
            impact_frac = abs_net / book_side if book_side > 0 else 0

            if impact_frac >= 0.05:
                is_entry_buy = net > 0
                opening = self._is_opening_trade(wallet, coin, is_entry_buy)
                if opening and self._check_margin_budget(coin, self.order_size, wallet=wallet):
                    cooldown_key = (wallet, coin)
                    cooldown_s = self.global_config["cooldown_s"]
                    if now - self.last_entry.get(cooldown_key, 0) >= cooldown_s:
                        # Apply entry guards even for TWAP wallets
                        mid = self.mid_prices.get(coin, 0)
                        max_chase_bps = self.global_config["max_chase_bps"]
                        max_spread_bps = self.global_config["max_spread_bps"]
                        min_book_depth = self.global_config["min_book_depth_usd"]
                        # Adaptive limits for synthetic markets
                        if ":" in coin:
                            book_data = self._book_depth.get(coin, {})
                            _bid = book_data.get("best_bid", 0)
                            _ask = book_data.get("best_ask", 0)
                            live_sp = (_ask - _bid) / mid * 10000 if mid > 0 and _bid > 0 and _ask > 0 else 20
                            max_chase_bps = min(50, max(max_chase_bps, live_sp * 3))
                            max_spread_bps = min(40, max(max_spread_bps, live_sp * 2))
                            min_book_depth = min(min_book_depth, 1000)

                        guards_pass = True
                        if mid > 0:
                            chase_bps = abs(mid - px) / px * 10000
                            if chase_bps > max_chase_bps:
                                logger.info(f"MID-TWAP SKIP {coin}: chase {chase_bps:.0f}bps > {max_chase_bps}bps")
                                guards_pass = False
                            book_data = self._book_depth.get(coin, {})
                            bid = book_data.get("best_bid", 0)
                            ask = book_data.get("best_ask", 0)
                            spread_bps = (ask - bid) / mid * 10000 if bid > 0 and ask > 0 else 999
                            if guards_pass and spread_bps > max_spread_bps:
                                logger.info(f"MID-TWAP SKIP {coin}: spread {spread_bps:.0f}bps > {max_spread_bps}bps")
                                guards_pass = False
                            entry_depth = depth["ask_usd"] if is_entry_buy else depth["bid_usd"]
                            if guards_pass and entry_depth < min_book_depth:
                                logger.info(f"MID-TWAP SKIP {coin}: depth ${entry_depth:.0f} < ${min_book_depth}")
                                guards_pass = False

                        if guards_pass:
                            wallet_group = self.wallet_groups.get(wallet, "unknown")
                            logger.info(
                                f"MID-TWAP ENTRY: {wallet[:10]} {coin} impact={impact_frac:.1%} of book "
                                f"(${abs_net:,.0f} vs ${book_side:,.0f} book) [{wallet_group}]"
                            )
                            _tg(f"ENTRY SIGNAL: {coin} {'BUY' if is_entry_buy else 'SELL'} -- {wallet[:10]} impact={impact_frac:.1%} [{wallet_group}]")
                            self._twap_entered.add(dedup_key)
                            trade_sz = abs_net / (self.mid_prices.get(coin, 1) or 1)
                            if not is_entry_buy:
                                trade_sz = -trade_sz
                            prev = self._target_positions.get(wallet, {}).get(coin, 0)
                            self._target_positions.setdefault(wallet, {})[coin] = prev + trade_sz
                            asyncio.get_event_loop().create_task(
                                self._enter_position(coin, is_entry_buy, twap_dedup_key=dedup_key, wallet=wallet)
                            )

    def _on_order_update(self, updates: list):
        """Detect fills on our entry AND exit orders."""
        for update in updates:
            order = update.get("order", {})
            oid = order.get("oid")
            status = update.get("status")

            if status == "filled" and oid:
                for pos in self.positions:
                    if pos['oid'] == oid and not pos['filled']:
                        pos['filled'] = True
                        pos['entry_px'] = float(order.get("limitPx", pos['entry_px']))
                        pos['fill_time'] = time.time()
                        self._persist_position(pos)  # Persist on fill
                        logger.info(f"ENTRY FILLED: {pos['coin']} {pos['side']} @ {pos['entry_px']}")
                        _tg(f"ENTRY: {pos['coin']} {pos['side']} @ {pos['entry_px']}")
                        break
                    if pos.get('exit_oid') and int(pos['exit_oid']) == oid and not pos.get('exit_filled'):
                        # Dedup guard: check if poll path already recorded this exit
                        if pos.get('_exit_recorded'):
                            logger.debug(f"EXIT dedup (WS): {pos['coin']} already recorded by poll handler")
                            break
                        # BUG 3 fix: prefer avgPx (real fill price), warn if only limitPx available
                        avg_px = order.get("avgPx")
                        if avg_px:
                            exit_px = float(avg_px)
                        else:
                            exit_px = float(order.get("limitPx", 0))
                            logger.warning(f"WS exit {pos['coin']}: avgPx missing, using limitPx={exit_px}")
                        # BUG 4 fix: use order's sz for PnL, not pos['size'] (may be stale after partials)
                        filled_sz = float(order.get("sz", order.get("origSz", pos['size'])))
                        pos['exit_filled'] = True
                        pos['_exit_recorded'] = True
                        wallet = pos.get('wallet', '')
                        wallet_group = self.wallet_groups.get(wallet, "unknown")

                        if pos['side'] == 'BUY':
                            pnl_bps = (exit_px - pos['entry_px']) / pos['entry_px'] * 10000
                        else:
                            pnl_bps = (pos['entry_px'] - exit_px) / pos['entry_px'] * 10000

                        pnl_usd = pnl_bps / 10000 * filled_sz * exit_px
                        # PnL tracked by exchange fill sync, not here

                        logger.info(
                            f"EXIT FILLED (MAKER): {pos['coin']} {pos['side']} "
                            f"entry={pos['entry_px']} exit={exit_px} "
                            f"pnl={pnl_bps:+.1f}bp (${pnl_usd:+.4f}) "
                            f"acct=${self._exch_pnl['account_net']:+.4f} [{wallet_group}]"
                        )
                        _tg(f"EXIT (maker): {pos['coin']} {pos['side']} {pnl_bps:+.1f}bp ${pnl_usd:+.4f} [{wallet_group}]")

                        mt = _get_market_type(pos['coin'])
                        self.db[DB_COLLECTION].insert_one({
                            "target_wallet": wallet,
                            "wallet_group": wallet_group,
                            "coin": pos['coin'], "side": pos['side'],
                            "entry_px": pos['entry_px'], "exit_px": exit_px,
                            "size": filled_sz, "pnl_bps": pnl_bps,
                            "pnl_usd": pnl_usd, "exit_type": "maker_ws",
                            "market_type": mt,
                            "hold_s": time.time() - pos.get('fill_time', pos['entry_time']),
                            "timestamp": datetime.now(timezone.utc),
                        })
                        self._track_market_pnl(mt, pnl_usd)
                        acc_key = (wallet, pos['coin'])

                        if pos.get('_trim_mode'):
                            # TRIM: reduce position size, keep tracking
                            old_size = pos['size']
                            pos['size'] = max(0, old_size - filled_sz)
                            pos.pop('_trim_mode', None)
                            pos.pop('_trim_target_sz', None)
                            pos.pop('exit_oid', None)
                            pos.pop('_maker_exit_tried', None)
                            pos['exit_filled'] = False
                            pos['_exit_recorded'] = False
                            # 2026-05-22 (Alberto correction): only full-remove if size is
                            # arithmetically zero, not below a dollar threshold. A position at
                            # 9.70 USD could rebound; the DB row must persist. Use a tight
                            # floating-point epsilon based on sz_decimals, not notional.
                            sz_dec = self.sz_decimals.get(pos['coin'], 4)
                            sz_eps = 0.5 * (10 ** -sz_dec)  # half a unit at HL's quantization
                            if pos['size'] < sz_eps:
                                self._position_accumulated.pop(acc_key, None)
                                self._exit_twap_buffer.pop(acc_key, None)
                                self._remove_persisted_position(wallet, pos['coin'])
                                pos['_ws_exited'] = True
                                logger.info(
                                    f"TRIM->FULL EXIT (residual={pos['size']:.6f} below sz_eps={sz_eps:.6f}): "
                                    f"{pos['coin']} {filled_sz:.4f} sold, removing row"
                                )
                            else:
                                self._persist_position(pos)  # Update DB with reduced size
                                logger.info(
                                    f"TRIM FILLED (WS): {pos['coin']} {filled_sz:.4f} sold, "
                                    f"residual {pos['size']:.4f} (was {old_size:.4f})"
                                )
                        else:
                            # FULL EXIT: remove position entirely
                            self._position_accumulated.pop(acc_key, None)
                            self._exit_twap_buffer.pop(acc_key, None)
                            self._remove_persisted_position(wallet, pos['coin'])
                            pos['_ws_exited'] = True
                        break

    async def _check_twap_windows(self):
        """Check if any TWAP aggregation window has expired."""
        now = time.time()
        expired = []

        for twap_key, buf in list(self._twap_buffer.items()):
            wallet, coin = twap_key
            wc = self._wallet_config(wallet)
            twap_window_s = wc.get("twap_window_s", 120)
            min_twap_notional = wc.get("min_twap_notional", 100)

            elapsed = now - buf['first_ts']
            if elapsed < twap_window_s:
                continue

            expired.append(twap_key)

            dedup_key = (wallet, coin, int(buf['first_ts']))
            if dedup_key in self._twap_entered:
                continue

            net = buf['buy_notional'] - buf['sell_notional']
            gross = buf['buy_notional'] + buf['sell_notional']
            is_buy = net > 0
            abs_net = abs(net)

            if abs_net < min_twap_notional:
                logger.info(
                    f"TWAP SKIP: {wallet[:10]} {coin} net=${net:+,.0f} < ${min_twap_notional} min "
                    f"(buys=${buf['buy_notional']:,.0f} sells=${buf['sell_notional']:,.0f} "
                    f"{buf['count']} fills)"
                )
                continue

            if gross > 0 and abs_net / gross < 0.6:
                logger.info(
                    f"TWAP SKIP: {wallet[:10]} {coin} ambiguous -- net/gross={abs_net/gross:.1%} "
                    f"(buys=${buf['buy_notional']:,.0f} sells=${buf['sell_notional']:,.0f})"
                )
                continue

            side_str = 'BUY' if is_buy else 'SELL'
            opening = self._is_opening_trade(wallet, coin, is_buy)

            if not opening:
                logger.info(
                    f"TWAP SKIP (CLOSING): {wallet[:10]} {coin} {side_str} -- target reducing position"
                )
                continue

            # Apply entry guards for TWAP completion
            mid = self.mid_prices.get(coin, 0)
            max_chase_bps = self.global_config["max_chase_bps"]
            max_spread_bps = self.global_config["max_spread_bps"]
            min_book_depth = self.global_config["min_book_depth_usd"]
            # Adaptive limits for synthetic markets
            if ":" in coin:
                book_data = self._book_depth.get(coin, {})
                _bid = book_data.get("best_bid", 0)
                _ask = book_data.get("best_ask", 0)
                live_sp = (_ask - _bid) / mid * 10000 if mid > 0 and _bid > 0 and _ask > 0 else 20
                max_chase_bps = min(50, max(max_chase_bps, live_sp * 3))
                max_spread_bps = min(40, max(max_spread_bps, live_sp * 2))
                min_book_depth = min(min_book_depth, 1000)
            guards_pass = True

            if mid > 0:
                # Chase guard: compare current mid to target's VWAP entry price
                net_sz = abs(buf.get("buy_sz", 0) - buf.get("sell_sz", 0))
                if net_sz > 0:
                    vwap = abs_net / net_sz  # actual volume-weighted average price
                else:
                    vwap = buf.get('first_mid', mid)  # fallback to first fill's mid
                chase_bps = abs(mid - vwap) / vwap * 10000 if vwap > 0 else 0
                if chase_bps > max_chase_bps:
                    logger.info(f"TWAP COMPLETE SKIP {coin}: chase {chase_bps:.0f}bps > {max_chase_bps}bps")
                    guards_pass = False
                book_data = self._book_depth.get(coin, {})
                bid = book_data.get("best_bid", 0)
                ask = book_data.get("best_ask", 0)
                spread_bps = (ask - bid) / mid * 10000 if bid > 0 and ask > 0 else 999
                if guards_pass and spread_bps > max_spread_bps:
                    logger.info(f"TWAP COMPLETE SKIP {coin}: spread {spread_bps:.0f}bps > {max_spread_bps}bps")
                    guards_pass = False
                depth = self._get_book_depth(coin)
                entry_depth = depth["ask_usd"] if is_buy else depth["bid_usd"]
                if guards_pass and entry_depth < min_book_depth:
                    logger.info(f"TWAP COMPLETE SKIP {coin}: depth ${entry_depth:.0f} < ${min_book_depth}")
                    guards_pass = False

            if not guards_pass:
                continue

            wallet_group = self.wallet_groups.get(wallet, "unknown")
            logger.info(
                f"TWAP COMPLETE: {wallet[:10]} {coin} NET {side_str} ${abs_net:,.0f} "
                f"(buys=${buf['buy_notional']:,.0f} sells=${buf['sell_notional']:,.0f}) "
                f"{buf['count']} fills -- OPENING confirmed [{wallet_group}]"
            )
            _tg(
                f"TWAP DONE: {wallet[:10]} {side_str} {coin} "
                f"net=${abs_net:,.0f} ({buf['count']} fills) [{wallet_group}]"
            )

            trade_sz = abs_net / (self.mid_prices.get(coin, 1) or 1)
            if not is_buy:
                trade_sz = -trade_sz
            prev = self._target_positions.get(wallet, {}).get(coin, 0)
            self._target_positions.setdefault(wallet, {})[coin] = prev + trade_sz
            logger.info(f"TARGET POS UPDATE: {wallet[:14]} {coin} -> {prev + trade_sz:.4f}")

            await self._enter_position(coin, is_buy, twap_dedup_key=dedup_key, wallet=wallet)

        for key in expired:
            self._twap_completed_ts[key] = time.time()
            del self._twap_buffer[key]

    # ── Reconciliation & stats ───────────────────────────────────────────────

    def _target_wallet_positions(self, wallet: str) -> tuple:
        """Return (positions_dict, complete_bool) for a target wallet's positions
        across main perps + builder dexes. Cached 300s per wallet on SUCCESS only;
        partial/failed fetches do not poison the cache. Used to honor Alberto's
        broader rule (2026-05-23): never remove tracking while the target is still
        holding the underlying coin.

        Returns:
            (positions, complete): positions is {coin: signed_size}.
            complete=True iff every venue (main + each builder dex) responded
            with a parseable assetPositions list. complete=False means caller
            MUST NOT use absence-of-key as evidence of zero position.
        """
        if not hasattr(self, "_tw_pos_cache"):
            self._tw_pos_cache = {}
        now_ts = time.time()
        cached = self._tw_pos_cache.get(wallet)
        if cached and now_ts - cached[1] < 300:
            return cached[0], True  # cached entries are only stored when complete
        out = {}
        all_ok = True
        for dex in [None] + list(BUILDER_DEXES):
            payload = {"type": "clearinghouseState", "user": wallet}
            if dex:
                payload["dex"] = dex
            try:
                r = requests.post(f"{HL_API}/info", json=payload, timeout=5)
                data = r.json()
                if isinstance(data, dict) and "assetPositions" in data:
                    for ap in data["assetPositions"]:
                        ep = ap["position"]
                        try:
                            sz = float(ep["szi"])
                        except (TypeError, ValueError):
                            continue
                        if abs(sz) > 1e-10:
                            out[ep["coin"]] = sz
                else:
                    all_ok = False
                    logger.debug(
                        f"target_wallet_positions {wallet[:10]} dex={dex}: "
                        f"unexpected response shape"
                    )
            except Exception as exc:
                all_ok = False
                logger.debug(
                    f"target_wallet_positions {wallet[:10]} dex={dex} failed: {exc}"
                )
        if all_ok:
            self._tw_pos_cache[wallet] = (out, now_ts)
        return out, all_ok

    def _reconcile_positions(self):
        """Check exchange for actual positions every 5min."""
        now = time.time()
        if now - self._last_reconcile < 300:
            return
        # 2026-05-23 (codex review): defer setting _last_reconcile until after queries.
        # Otherwise a transient query failure delays retry by 5 min unnecessarily.
        try:
            # 2026-05-23 (Alberto correction): track which exchange queries succeeded.
            # Never remove a position just because a query silently failed (defaulting to 0).
            # Also capture markPx as a backup for stale mid prices.
            exchange_positions = {}
            exchange_mark_prices = {}
            queries_ok = {"main": False}
            for dex_name in BUILDER_DEXES:
                queries_ok[dex_name] = False

            try:
                r = requests.post(f"{HL_API}/info", json={
                    "type": "clearinghouseState", "user": self.parent_address
                }, timeout=5)
                data = r.json()
                if isinstance(data, dict) and "assetPositions" in data:
                    queries_ok["main"] = True
                    for ap in data["assetPositions"]:
                        ep = ap["position"]
                        exchange_positions[ep["coin"]] = float(ep["szi"])
                        if ep.get("markPx"):
                            try:
                                exchange_mark_prices[ep["coin"]] = float(ep["markPx"])
                            except (TypeError, ValueError):
                                pass
            except Exception as exc:
                logger.warning(f"Reconcile main perps query failed: {exc}")

            # Also fetch builder dex positions (xyz, flx) for reconciliation
            for dex_name in BUILDER_DEXES:
                try:
                    rd = requests.post(f"{HL_API}/info", json={
                        "type": "clearinghouseState", "user": self.parent_address,
                        "dex": dex_name,
                    }, timeout=5)
                    data = rd.json()
                    if isinstance(data, dict) and "assetPositions" in data:
                        queries_ok[dex_name] = True
                        for ap in data["assetPositions"]:
                            ep = ap["position"]
                            exchange_positions[ep["coin"]] = float(ep["szi"])
                            if ep.get("markPx"):
                                try:
                                    exchange_mark_prices[ep["coin"]] = float(ep["markPx"])
                                except (TypeError, ValueError):
                                    pass
                except Exception as exc:
                    logger.warning(f"Reconcile {dex_name} dex query failed: {exc}")

            # 2026-05-23 (codex review): gate the cancel-pending cleanup behind
            # successful queries too. Same class of bug — `exchange_positions.get(coin, 0)`
            # defaults to 0 on missing key, which can falsely drop a real position.
            all_queries_ok = all(queries_ok.values())
            if all_queries_ok:
                removed_cancel_pending = [
                    tp for tp in self.positions
                    if tp.get('_cancel_pending')
                    and tp.get('coin') in exchange_positions
                    and not (abs(exchange_positions.get(tp['coin'], 0)) * self.mid_prices.get(tp['coin'], 1) > 1.0)
                ]
                for tp in removed_cancel_pending:
                    self._remove_persisted_position(tp.get('wallet', ''), tp['coin'])
                self.positions = [
                    tp for tp in self.positions
                    if not tp.get('_cancel_pending')
                    or tp.get('coin') not in exchange_positions
                    or (abs(exchange_positions.get(tp['coin'], 0)) * self.mid_prices.get(tp['coin'], 1) > 1.0)
                ]

            # Phantom position cleanup
            # 2026-05-23 (Alberto correction): only remove if ALL relevant exchange queries
            # succeeded. A silent query failure must NEVER cause us to forget a real position.
            if not all_queries_ok:
                failed = [k for k, v in queries_ok.items() if not v]
                logger.warning(
                    f"Reconcile: skipping phantom cleanup, queries failed: {failed}. "
                    f"Will retry next cycle."
                )
            phantom_keys = []
            if not self.shadow_mode and all_queries_ok:
                now_ts = time.time()
                for tp in self.positions:
                    if not tp.get('filled'):
                        continue
                    grace_s = 300 if tp.get('_recovered') else 60
                    fill_time = tp.get('fill_time', tp.get('entry_time', 0))
                    if now_ts - fill_time < grace_s:
                        continue
                    coin = tp['coin']
                    # 2026-05-23: positive-confirmation needed; absent != zero
                    if coin not in exchange_positions:
                        # Coin had no entry in exchange_positions even though queries succeeded.
                        # In HL, absent == zero. But we are conservative: log + skip.
                        logger.info(
                            f"PHANTOM CHECK: {coin} not in exchange_positions; treating as zero"
                        )
                        exch_sz = 0.0
                    else:
                        exch_sz = abs(exchange_positions[coin])
                    mid = self.mid_prices.get(coin, 0)
                    mid_age = now_ts - self._mid_price_ts.get(coin, 0)
                    if mid <= 0 or mid_age > 120:
                        # 2026-05-23 (Alberto correction): fall back to exchange markPx
                        # before resorting to stale entry price.
                        exch_mark = exchange_mark_prices.get(coin, 0)
                        if exch_mark > 0:
                            logger.info(
                                f"PHANTOM CHECK: using markPx={exch_mark} for {coin} "
                                f"(mid stale {mid_age:.0f}s)"
                            )
                            mid = exch_mark
                        else:
                            mid = tp.get('entry_px', 0)
                            if mid_age > 120 and mid > 0:
                                logger.warning(
                                    f"PHANTOM CHECK: stale mid + no markPx for {coin} "
                                    f"({mid_age:.0f}s old), using entry_px={mid}"
                                )
                    if mid <= 0:
                        continue
                    exch_notional = exch_sz * mid
                    if exch_notional < 1.0 and exch_sz < 0.001:
                        # 2026-05-23 (Alberto broader rule + codex review): even if OUR
                        # exchange shows ~zero, KEEP the row when the TARGET wallet still
                        # holds this coin on its own wallet, OR when the target query was
                        # incomplete (absent != zero). The signal hasn't closed; we just
                        # lost our copy and need to re-establish.
                        wallet = tp.get('wallet', '') or ''
                        if wallet.lower().startswith('0x') and len(wallet) == 42:
                            target_pos, target_complete = self._target_wallet_positions(wallet)
                            if not target_complete:
                                logger.info(
                                    f"PHANTOM CHECK UNKNOWN {coin} {wallet[:10]}: "
                                    f"target wallet query incomplete; will not remove"
                                )
                                continue
                            if abs(target_pos.get(coin, 0)) > 1e-10:
                                logger.info(
                                    f"PHANTOM CHECK SKIP {coin} {wallet[:10]}: "
                                    f"target still holds {target_pos.get(coin):.6f}; "
                                    f"will not remove tracking"
                                )
                                continue
                        key = (tp.get('wallet', ''), coin)
                        phantom_keys.append(key)
                        self._position_accumulated.pop(key, None)
                        self._exit_twap_buffer.pop(key, None)
            if phantom_keys:
                phantom_set = set(phantom_keys)
                # Remove from persistent DB state
                for wallet, coin in phantom_keys:
                    self._remove_persisted_position(wallet, coin)
                self.positions = [
                    tp for tp in self.positions
                    if (tp.get('wallet', ''), tp['coin']) not in phantom_set
                ]
                phantom_coins = [k[1] for k in phantom_keys]
                logger.info(f"RECONCILE: removed {len(phantom_keys)} phantom positions: {phantom_coins}")
                _tg(f"Cleaned {len(phantom_keys)} phantom positions: {', '.join(phantom_coins)}")
            # 2026-05-23 (codex review): only mark cycle complete if queries succeeded.
            # Otherwise allow retry on the next 5-min boundary (no premature backoff).
            if all_queries_ok:
                self._last_reconcile = now

            # Orphan detection: warn only (don't auto-close, could be manual or other strategy)
            tracked_coins = set(tp['coin'] for tp in self.positions if tp.get('filled'))
            for coin, exch_sz in exchange_positions.items():
                if abs(exch_sz) < 1e-10:
                    continue
                mid = self.mid_prices.get(coin, 0)
                notional = abs(exch_sz) * mid if mid > 0 else abs(exch_sz) * 100
                if notional < 1.0:
                    continue  # dust, ignore
                if coin not in tracked_coins:
                    if not hasattr(self, '_orphan_reported'):
                        self._orphan_reported = set()
                    if coin not in self._orphan_reported:
                        logger.warning(
                            f"ORPHAN DETECTED: {coin} sz={exch_sz} (${notional:.0f} notional) "
                            f"exists on exchange but NOT tracked by V11"
                        )
                        _tg(f"ORPHAN: {coin} sz={exch_sz} on exchange, not tracked by V11. Manual close needed.")
                        self._orphan_reported.add(coin)

            # Size reconciliation: compare SUM of tracked sizes per coin vs exchange
            # This handles multi-wallet same-coin correctly
            from collections import defaultdict
            tracked_by_coin = defaultdict(float)
            for tp in self.positions:
                if tp.get('filled'):
                    sign = 1 if tp['side'] == 'BUY' else -1
                    tracked_by_coin[tp['coin']] += tp['size'] * sign

            for coin, tracked_net in tracked_by_coin.items():
                exch_sz = exchange_positions.get(coin, 0)
                if abs(exch_sz) < 1e-10 and abs(tracked_net) < 1e-10:
                    continue
                # Only warn on significant drift (>20% of tracked or >$2 notional difference)
                mid = self.mid_prices.get(coin, 0)
                diff = abs(abs(exch_sz) - abs(tracked_net))
                diff_notional = diff * mid if mid > 0 else 0
                if diff_notional > 2.0 and diff > abs(tracked_net) * 0.20:
                    logger.warning(
                        f"POSITION DRIFT: {coin} tracked_net={tracked_net:.6f} exchange={exch_sz:.6f} "
                        f"diff=${diff_notional:.2f}"
                    )
            # DB INTEGRITY: re-persist all in-memory positions to catch silent drops
            db_keys = set()
            for doc in self.db[DB_OPEN_POSITIONS].find({}, {"wallet": 1, "coin": 1}):
                db_keys.add((doc.get("wallet", ""), doc["coin"]))
            for tp in self.positions:
                if tp.get("filled") and not tp.get("_ws_exited"):
                    key = (tp.get("wallet", ""), tp["coin"])
                    if key not in db_keys:
                        self._persist_position(tp)
                        logger.warning(
                            f"DB REPAIR: re-persisted {tp['coin']} wallet={tp.get('wallet','')[:10]} "
                            f"sz={tp.get('size',0):.4f} (was missing from DB)"
                        )

            # Periodic cleanup: prune stale entries from unbounded dicts
            now_ts = time.time()
            open_keys = {(p.get('wallet', ''), p['coin']) for p in self.positions if p.get('filled')}
            # Prune _exit_twap_buffer for keys with no open position
            stale_exit_keys = [k for k in self._exit_twap_buffer if k not in open_keys]
            for k in stale_exit_keys:
                del self._exit_twap_buffer[k]
            # Prune _post_exit_cooldown older than 600s
            stale_cooldowns = [k for k, ts in self._post_exit_cooldown.items() if now_ts - ts > 600]
            for k in stale_cooldowns:
                del self._post_exit_cooldown[k]
            # Prune _twap_completed_ts older than 3600s
            if hasattr(self, '_twap_completed_ts'):
                stale_twap = [k for k, ts in self._twap_completed_ts.items() if now_ts - ts > 3600]
                for k in stale_twap:
                    del self._twap_completed_ts[k]

        except Exception as e:
            logger.debug(f"Reconcile error: {e}")

    def _sync_exchange_fills(self):
        """Pull exchange fills and compute TRUE PnL from exchange (source of truth).
        Runs every 60 seconds. Filters to V11-owned oids for accurate attribution."""
        now = time.time()
        if now - self._last_fill_sync < 60:
            return
        self._last_fill_sync = now
        self._do_exchange_fill_sync()

    def _do_exchange_fill_sync(self):
        """Actual sync logic. Called at startup (blocking) and periodically.
        Step 1: Pull new fills from exchange API, store to MongoDB (dedup by fill_key).
        Step 2: Compute PnL from MongoDB (permanent store, no 2000-fill cap)."""
        try:
            # Step 1: Ingest new fills from exchange
            fills = self.info.user_fills(self.parent_address)
            v9_epoch_ms = int(datetime(2026, 5, 9, 23, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
            recent = [f for f in fills if int(f['time']) >= v9_epoch_ms]

            # Load V11's known oids for attribution
            v11_oids = set()
            try:
                for doc in self.db[DB_ORDER_IDS].find({}, {"oid": 1}):
                    v11_oids.add(doc["oid"])
            except Exception:
                pass

            # Store new fills (dedup by tid -- unique per fill on exchange)
            new_fills = 0
            for f in recent:
                tid = f.get('tid')
                oid = int(f.get('oid', 0))
                if not tid:
                    continue
                try:
                    self.db[DB_EXCHANGE_FILLS].update_one(
                        {"tid": tid},
                        {"$setOnInsert": {
                            "tid": tid,
                            "fill_key": f"{f.get('oid', '')}_{f['time']}",
                            "coin": f['coin'],
                            "side": f['side'],
                            "sz": f['sz'],
                            "px": f['px'],
                            "fee": f.get('fee', '0'),
                            "closedPnl": f.get('closedPnl', '0'),
                            "time": int(f['time']),
                            "oid": oid,
                            "dir": f.get('dir', ''),
                            "startPosition": f.get('startPosition', ''),
                            "feeToken": f.get('feeToken', 'USDC'),
                        }},
                        upsert=True,
                    )
                    new_fills += 1
                except Exception:
                    pass

            # Step 2: Compute PnL from MongoDB (survives 2000-fill API cap)
            # Fields are strings from exchange API, convert to float for aggregation
            total_pnl = 0.0
            total_fees = 0.0
            total_closes = 0
            v11_pnl = 0.0
            v11_fees = 0.0
            v11_closes = 0

            for doc in self.db[DB_EXCHANGE_FILLS].find({}, {"closedPnl": 1, "fee": 1, "oid": 1}):
                pnl = float(doc.get("closedPnl", 0))
                fee = float(doc.get("fee", 0))
                oid = doc.get("oid")
                total_pnl += pnl
                total_fees += fee
                if abs(pnl) > 0.0001:
                    total_closes += 1
                if oid in v11_oids:
                    v11_pnl += pnl
                    v11_fees += fee
                    if abs(pnl) > 0.0001:
                        v11_closes += 1

            total_net = total_pnl - total_fees
            v11_net = v11_pnl - v11_fees
            unattributed_pnl = total_pnl - v11_pnl

            # Update exchange PnL cache (the ONLY source of PnL truth)
            old_net = self._exch_pnl["account_net"]
            self._exch_pnl = {
                "account_net": total_net,
                "v11_net": v11_net,
                "v11_closes": v11_closes,
                "account_closes": total_closes,
                "fees": total_fees,
                "last_sync": time.time(),
            }
            self._last_successful_sync = time.time()

            if abs(old_net - total_net) > 0.01:
                logger.info(
                    f"PNL SYNC: account=${total_net:+.4f} (closes={total_closes} fees=${total_fees:.4f}) "
                    f"| V11=${v11_net:+.4f} (closes={v11_closes} fees=${v11_fees:.4f}) "
                    f"| unattributed=${unattributed_pnl:+.4f}"
                )

            total_stored = self.db[DB_EXCHANGE_FILLS].count_documents({})
            if new_fills > 0:
                logger.debug(f"FILL SYNC: {total_stored} fills stored ({len(v11_oids)} V11 oids)")

        except Exception as e:
            logger.warning(f"Exchange fill sync failed: {e}")

    def _log_stats(self):
        now = time.time()
        if now - self._last_stats < 60:
            return
        self._last_stats = now

        # Sync exchange fills FIRST so PnL is fresh
        self._sync_exchange_fills()
        self._reconcile_positions()

        # Kill switch with separate reason tracking (no shared flag confusion)
        # Reason 1: Staleness
        sync_age = now - self._last_successful_sync
        if sync_age > 180 and self._last_successful_sync > 0:
            if not self._kill_reasons.get("stale"):
                logger.error(f"SYNC STALE: last successful sync {sync_age:.0f}s ago. Blocking entries.")
                _tg(f"SYNC STALE: exchange data {sync_age:.0f}s old. Entries blocked.")
            self._kill_reasons["stale"] = True
        elif self._kill_reasons.get("stale"):
            self._kill_reasons.pop("stale")
            logger.info("SYNC RESTORED: exchange data fresh again")

        # Reason 2: Daily loss with hysteresis (lift only at 50% recovery)
        total_upnl = self._compute_unrealized_pnl()
        realized = self._exch_pnl["account_net"]
        max_daily_loss = self.global_config["max_daily_loss"]
        net_pnl = realized + total_upnl
        if net_pnl < max_daily_loss:
            if not self._kill_reasons.get("loss"):
                logger.error(f"KILL SWITCH (LOSS): net ${net_pnl:.4f} < ${max_daily_loss}")
                logger.error("KILL SWITCH: new entries DISABLED. Exit monitoring continues.")
                _tg(f"KILL SWITCH: net ${net_pnl:.4f} -- entries disabled")
            self._kill_reasons["loss"] = True
        elif self._kill_reasons.get("loss"):
            # Hysteresis: only lift when recovered to 50% of max_daily_loss
            lift_threshold = max_daily_loss * 0.5
            if net_pnl > lift_threshold:
                self._kill_reasons.pop("loss")
                logger.info(f"KILL SWITCH LIFTED (LOSS): net ${net_pnl:.4f} > ${lift_threshold:.4f} (50% recovery)")
                _tg(f"Kill switch lifted: net ${net_pnl:.4f}")

        # Unified kill switch: active if ANY reason is present
        self._kill_switch_active = bool(self._kill_reasons)

        open_pos = [p for p in self.positions if p['filled']]
        open_coins = " ".join(f"{p['coin']}" for p in open_pos) if open_pos else "none"
        equity = self._equity_cache or 0
        margin_used = getattr(self, '_exch_margin_used', 0)
        margin_pct = (margin_used / equity * 100) if equity > 0 else 0
        ep = self._exch_pnl
        sync_age = int(now - ep["last_sync"]) if ep["last_sync"] > 0 else 999
        logger.info(
            f"STATS: acct=${ep['account_net']:+.4f}({ep['account_closes']}) "
            f"v11=${ep['v11_net']:+.4f}({ep['v11_closes']}) "
            f"fees=${ep['fees']:.2f} uPnL=${total_upnl:+.4f} "
            f"open={len(open_pos)}[{open_coins}] margin={margin_pct:.0f}% eq=${equity or 0:.2f} "
            f"sync={sync_age}s"
        )

        # V11 internal TG report DISABLED -- replaced by exchange-truth pnl_tracker.py
        # The old _send_performance_report used internal collections (not exchange truth)
        # and produced numbers that conflicted with the pnl_tracker. All TG reporting
        # now goes through scripts/pnl_tracker.py (15-min loop in tmux pnl-tracker).

    def _send_performance_report(self):
        """Send detailed performance report to Telegram every 15 min.
        Shows per-wallet-group breakdown."""
        try:
            v8_epoch = datetime(2026, 5, 9, 23, 26, 0, tzinfo=timezone.utc)
            today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            v8_epoch_naive = v8_epoch.replace(tzinfo=None)
            today_start_naive = today_start.replace(tzinfo=None)

            # Collect from all collections
            all_closed = []
            for coll_name in [DB_COLLECTION, "hl_copy_trades", "v10_copy_trades"]:
                try:
                    trades = list(self.db[coll_name].find(
                        {"timestamp": {"$gte": v8_epoch_naive}}
                    ).sort("timestamp", 1))
                    all_closed.extend(trades)
                except Exception:
                    pass
            # Sort by timestamp
            all_closed.sort(key=lambda t: t.get("timestamp", datetime.min))
            closed_today = [t for t in all_closed if t.get("timestamp") and t["timestamp"] >= today_start_naive]
            closed = all_closed

            # Account equity
            acct_val = 0.0
            try:
                _r2 = requests.post(f"{HL_API}/info", json={"type": "spotClearinghouseState", "user": self.parent_address}, timeout=5)
                for _b in _r2.json().get("balances", []):
                    if _b.get("coin") == "USDC":
                        acct_val = float(_b.get("total", 0))
            except Exception:
                pass

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
                total_usd = self._exch_pnl["account_net"]
                avg_bps = 0
                best = worst = 0

            # Unrealized PnL
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
                    wallet_group = self.wallet_groups.get(pos.get('wallet', ''), '?')
                    et = pos.get('entry_time')
                    if isinstance(et, (int, float)):
                        et = datetime.fromtimestamp(et, tz=timezone.utc)
                    elif isinstance(et, datetime) and et.tzinfo is None:
                        et = et.replace(tzinfo=timezone.utc)
                    hold_min = (datetime.now(timezone.utc) - et).total_seconds() / 60 if et else 0
                    open_lines.append(
                        f"  {pos['coin']} {pos['side']} {upnl_bps:+.0f}bp ${upnl_usd:+.3f} "
                        f"({hold_min:.0f}m, {wallet_group})"
                    )

            # Per-wallet-group breakdown
            group_stats = {}
            for t in closed:
                # Determine group: use wallet_group field if present, else lookup
                wg = t.get("wallet_group", "")
                if not wg:
                    tw = t.get("target_wallet", "")
                    wg = self.wallet_groups.get(tw, "legacy")
                if wg not in group_stats:
                    group_stats[wg] = {"trades": 0, "wins": 0, "pnl": 0, "coins": set()}
                group_stats[wg]["trades"] += 1
                group_stats[wg]["pnl"] += t.get("pnl_usd", 0)
                group_stats[wg]["coins"].add(t.get("coin", "?"))
                if t.get("pnl_bps", 0) > 0:
                    group_stats[wg]["wins"] += 1

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

            active_targets = set()
            for pos in self.positions:
                if pos.get('filled'):
                    active_targets.add(str(pos.get('wallet', ''))[:10])

            uptime_h = (datetime.now(timezone.utc) - self._deploy_time).total_seconds() / 3600

            n_today = len(closed_today)
            today_usd = sum(t.get("pnl_usd", 0) for t in closed_today)
            today_wins = sum(1 for t in closed_today if t.get("pnl_bps", 0) > 0)
            today_wr = today_wins / n_today * 100 if n_today > 0 else 0

            # Build report
            net_pnl = total_usd + total_upnl
            emoji = "+" if net_pnl >= 0 else "-"
            lines = [f"COPY TRADER V11 -- {datetime.now(timezone.utc).strftime('%H:%M')} UTC"]
            lines.append(f"Equity: ${acct_val:.2f} | Wallets: {len(self.target_set)} tracked, {len(active_targets)} active")
            lines.append("")
            lines.append(f"Today: {n_today}t ${today_usd:+.3f} ({today_wr:.0f}%WR)")
            lines.append(f"All-time: {n_trades}t ${total_usd:+.3f} ({wr:.0f}%WR, avg {avg_bps:+.0f}bp)")
            if n_trades > 0:
                lines.append(f"  Best: {best:+.0f}bp | Worst: {worst:+.0f}bp")
            lines.append(f"Unrealized: {len(open_lines)} pos ${total_upnl:+.3f}")
            lines.append(f"NET: ${net_pnl:+.3f}")

            if n_trades > 0:
                lines.append(f"\nL: {len(longs)}t ${long_pnl:+.3f} | S: {len(shorts)}t ${short_pnl:+.3f}")

            # Per market type
            market_type_stats = {}
            for t in closed:
                mt = t.get("market_type", _get_market_type(t.get("coin", "")))
                if mt not in market_type_stats:
                    market_type_stats[mt] = {"trades": 0, "pnl": 0.0}
                market_type_stats[mt]["trades"] += 1
                market_type_stats[mt]["pnl"] += t.get("pnl_usd", 0)
            if len(market_type_stats) > 1:
                lines.append("\nPer market:")
                for mt, s in sorted(market_type_stats.items(), key=lambda x: -x[1]["pnl"]):
                    lines.append(f"  {mt}: {s['trades']}t ${s['pnl']:+.3f}")

            # Per group
            if group_stats:
                lines.append("\nPer group:")
                for g, s in sorted(group_stats.items(), key=lambda x: -x[1]["pnl"]):
                    g_wr = s["wins"] / s["trades"] * 100 if s["trades"] > 0 else 0
                    lines.append(f"  {g}: {s['trades']}t {g_wr:.0f}%WR ${s['pnl']:+.3f}")

            # Per wallet
            if wallet_stats:
                lines.append("\nPer wallet:")
                for w, s in sorted(wallet_stats.items(), key=lambda x: -x[1]["pnl"]):
                    w_wr = s["wins"] / s["trades"] * 100 if s["trades"] > 0 else 0
                    coins_str = ",".join(sorted(s["coins"]))
                    lines.append(f"  {w} {s['trades']}t {w_wr:.0f}%WR ${s['pnl']:+.3f} [{coins_str}]")

            if open_lines:
                lines.append(f"\nOpen positions:")
                lines.extend(sorted(open_lines, key=lambda x: x)[:8])
                if len(open_lines) > 8:
                    lines.append(f"  +{len(open_lines)-8} more")

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
        """Generate equity curve PNG and return path."""
        try:
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            import matplotlib.dates as mdates

            if len(closed_trades) < 1:
                return None

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

            fig, ax = plt.subplots(figsize=(8, 4))
            ax.plot(timestamps[:-1], cum_pnl[:-1], 'b-', linewidth=1.5, label='Realized')
            ax.plot(timestamps[-2:], cum_pnl[-2:], 'b--', linewidth=1.5, alpha=0.6, label='+ Unrealized')
            ax.fill_between(timestamps, cum_pnl, alpha=0.08, color='blue')
            ax.axhline(y=0, color='gray', linestyle='--', linewidth=0.5)

            for i, t in enumerate(closed_trades):
                ts = t.get("timestamp")
                if isinstance(ts, datetime):
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    pnl_bps = t.get("pnl_bps", 0)
                    color = '#2ecc71' if pnl_bps >= 0 else '#e74c3c'
                    ax.plot(ts, cum_pnl[i + 1], 'o', color=color, markersize=6, zorder=5)

            final = cum_pnl[-1]
            color = '#2ecc71' if final >= 0 else '#e74c3c'
            ax.plot(timestamps[-1], final, 's', color=color, markersize=10, zorder=5)
            ax.annotate(f'${final:+.3f}', (timestamps[-1], final),
                       textcoords="offset points", xytext=(10, 5),
                       fontsize=11, fontweight='bold', color=color)

            n_open = sum(1 for p in self.positions if p.get('filled'))
            if n_open > 0:
                ax.annotate(f'{n_open} open', (timestamps[-1], final),
                           textcoords="offset points", xytext=(10, -12),
                           fontsize=8, color='gray')

            ax.set_title(f'Copy Trader V11 -- {len(closed_trades)} closed, {n_open} open', fontsize=12)
            ax.set_ylabel('Cumulative PnL ($)', fontsize=10)

            span_hours = (timestamps[-1] - timestamps[0]).total_seconds() / 3600
            if span_hours < 24:
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            else:
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))

            ax.legend(loc='upper left', fontsize=8)
            ax.grid(True, alpha=0.3)
            fig.autofmt_xdate()
            plt.tight_layout()

            path = '/tmp/copy_equity_curve_v11.png'
            fig.savefig(path, dpi=120)
            plt.close(fig)
            return path

        except Exception as e:
            logger.error(f"Chart generation error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    # ── Main loop ────────────────────────────────────────────────────────────

    async def run(self):
        logger.info(
            f"Copy trader V11 starting: {len(self.target_set)} wallets, "
            f"size=${self.order_size}, shadow={self.shadow_mode}"
        )

        def shutdown(sig, frame):
            self.running = False
        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

        while self.running:
            try:
                async with websockets.connect(HL_WS, ping_interval=20) as ws:
                    # Clear stale entry TWAP buffers on reconnect
                    # NOTE: preserve _exit_twap_buffer across reconnects -- if target
                    # already closed their position before reconnect, no further trades
                    # will arrive to refill the buffer, leaving our position stranded
                    # until max_hold_s expires. Entry buffer is safe to clear.
                    self._twap_buffer.clear()
                    self._l2_subscribed.clear()
                    # Fix #9: clear stale mid prices and book data on reconnect
                    self.mid_prices.clear()
                    self._mid_price_ts.clear()
                    self._book_depth.clear()

                    # Subscribe to trades for ALL coins: perps + builder dexes
                    all_coins = self.all_perp_coins + self.all_builder_coins
                    logger.info(
                        f"Subscribing to trades for {len(self.all_perp_coins)} perp + "
                        f"{len(self.all_builder_coins)} builder coins..."
                    )
                    for i, coin in enumerate(all_coins):
                        await ws.send(json.dumps({
                            "method": "subscribe",
                            "subscription": {"type": "trades", "coin": coin}
                        }))
                        # Batch subscriptions: don't sleep on every one
                        if (i + 1) % 50 == 0:
                            await asyncio.sleep(0.5)
                    logger.info(f"Trade subscriptions complete ({len(all_coins)} coins)")

                    # Subscribe to l2Book for needed coins (held + target coins)
                    await self._sync_l2_subscriptions(ws)

                    # Subscribe to our order updates
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "orderUpdates", "user": self.parent_address}
                    }))

                    logger.info("WS subscribed")
                    if not hasattr(self, '_ws_ever_connected'):
                        self._ws_ever_connected = True
                        _tg(
                            f"V11 STARTED: {len(self.target_set)} wallets, "
                            f"{len(self.all_perp_coins)}+{len(self.all_builder_coins)} coins "
                            f"(perp+builder), size=${self.order_size}"
                            f"{' [SHADOW]' if self.shadow_mode else ''}"
                        )
                    else:
                        logger.info("WS reconnected (no TG notification)")

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
                                    self._update_book_depth_from_ws(coin, levels)

                            elif channel == "trades":
                                trades_data = data.get("data", [])
                                if isinstance(trades_data, list):
                                    for t in trades_data:
                                        self._on_hl_trade(t)

                            elif channel == "orderUpdates":
                                self._on_order_update(data.get("data", []))

                        except asyncio.TimeoutError:
                            pass

                        # Throttle expensive checks to max 1/sec
                        now_check = time.time()
                        if not hasattr(self, '_last_check') or now_check - self._last_check >= 1.0:
                            self._last_check = now_check
                            await self._check_twap_windows()
                            await self._check_exits()
                            self._log_stats()

                        # Sync l2Book subscriptions every 30s
                        if not hasattr(self, '_last_l2_sync') or now_check - self._last_l2_sync >= 30:
                            self._last_l2_sync = now_check
                            await self._sync_l2_subscriptions(ws)

            except Exception as e:
                logger.error(f"WS error: {e}, reconnecting in 5s...")
                await asyncio.sleep(5)

        # Shutdown: preserve positions
        logger.info(f"Shutting down -- preserving {len([p for p in self.positions if p['filled']])} positions for recovery")
        for pos in self.positions:
            if not pos['filled']:
                try:
                    self.exchange.cancel(pos['coin'], int(pos['oid']))
                    logger.info(f"Cancelled pending order: {pos['coin']}")
                except Exception:
                    pass
        ep = self._exch_pnl
        logger.info(f"FINAL: acct=${ep['account_net']:+.4f}({ep['account_closes']}) v11=${ep['v11_net']:+.4f}({ep['v11_closes']})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HL Copy Trader V11 -- Unified Engine")
    parser.add_argument("--config", default="config/copy_trader_wallets.json",
                       help="Path to wallet config JSON")
    parser.add_argument("--size", type=float, default=None,
                       help="Override order size in USD (default: from config)")
    parser.add_argument("--shadow", action="store_true",
                       help="Shadow mode: log signals but don't place orders")
    args = parser.parse_args()

    config_path = args.config
    if not os.path.isabs(config_path):
        # Resolve relative to repo root. Walk up to the ancestor that holds the
        # repo markers (config/ + app/) so this works regardless of how deep the
        # script lives (it moved scripts/ -> strategies/live/ in the 2026-05-30
        # restructure; a hardcoded parent.parent would silently mis-resolve).
        repo_root = Path(__file__).resolve().parent
        for _ in range(6):
            if (repo_root / "config").is_dir() and (repo_root / "app").is_dir():
                break
            repo_root = repo_root.parent
        config_path = str(repo_root / config_path)

    if not os.path.exists(config_path):
        logger.error(f"Config file not found: {config_path}")
        exit(1)

    trader = CopyTrader(config_path, order_size_override=args.size, shadow=args.shadow)
    if args.shadow:
        logger.info("SHADOW MODE: logging signals only, no orders placed")
    asyncio.run(trader.run())
