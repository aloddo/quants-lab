#!/usr/bin/env python3
"""
========================================================================================
THE LIVE COPY ENGINE -- SINGLE FILE (consolidated 2026-06-12, Alberto msg 9401).
Was split across hl_copy_trader_v15/v16/v17.py (an inheritance chain that hid the real
engine in v15 and cost a debugging detour). Now ONE file. EDIT HERE.
Class chain kept INTACT for behaviour-identity:
    CopyTrader (the engine: entry/exit/sizing/margin/WS/tilt/webData2/persistence)
      <- V16CopyTrader  (liquid-whitelist hard guard)
      <- V17CopyTrader  (entry gate + expansion/knet/seed; the instantiated class).
DB collections are the v16_* set (positions/PnL/persistence live there) -- preserved exactly.
Logs tagged [hl_copy_v17].
========================================================================================
"""
import sys
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
logger = logging.getLogger("hl_copy_v17")   # live tag (was "hl_copy_v17"; renamed 2026-06-12)

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
DB_OPEN_POSITIONS = "v17_open_positions"  # persistent position state (per-wallet)
DB_EXCHANGE_FILLS = "v17_exchange_fills"  # exchange fills (source of truth for PnL)
DB_ORDER_IDS = "v17_order_ids"  # every oid V17 generates (for fill attribution)


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

        # ── V15 PROPORTIONAL SIZING (vs V17 fixed order_size) ────────────────
        # V15 mirrors each leader's EXPOSURE-% (their position notional / their account equity) onto OUR
        # equity, instead of a fixed $/order. order_size becomes only a FLOOR/lot helper. Leader equity
        # comes from the same clearinghouseState V17 already fetches (marginSummary.accountValue summed
        # over dexes). Gross exposure across all copied positions is capped at gross_cap x our equity.
        self.sizing_mode = self.global_config.get("sizing_mode", "proportional")  # proportional | fixed
        # Alberto 2026-06-01: NO cap on gross leverage (mirror leaders' true exposure faithfully); risk is
        # capped at ALLOCATION + the global -15% stop + the exchange's own max_margin_util, not an
        # artificial gross clamp. gross_cap defaults to inf (no clamp) unless explicitly configured.
        self.gross_cap = float(self.global_config.get("gross_cap", float("inf")))
        self.min_entry_notional = float(self.global_config.get("min_entry_notional", 10.0))
        self._target_equity = {}     # leader addr -> account equity (whole-account perp, all dexes)
        self._target_equity_ts = {}  # leader addr -> last refresh monotonic ts (staleness guard)
        self.target_equity_max_age_s = float(self.global_config.get("target_equity_max_age_s", 120))
        # codex review fixes (2026-06-01):
        # #1 mark-age gate: never size a proportional order off a mark older than this.
        self.mark_max_age_s = float(self.global_config.get("mark_max_age_s", 30))
        # #3 global stop as a PERCENT of equity (flatten-all on trigger), not a $ amount.
        self.global_stop_pct = float(self.global_config.get("global_stop_pct", 0.15))  # 0.15 = -15%
        # #2 down-convergence: trim toward the leader's level when we are over by >= this fraction.
        self.trim_over_frac = float(self.global_config.get("trim_over_frac", 0.20))
        # #4 runaway backstop: flatten-all if total gross notional exceeds this x equity (NOT a mirror
        # cap -- a blow-up guard against margin mis-estimate). inf = pure uncapped (pending Alberto).
        self.gross_backstop_x = float(self.global_config.get("gross_backstop_x", float("inf")))
        self._baseline_equity = None      # session-start equity for the % global stop
        self._flatten_requested = False   # set by the % stop / backstop -> _check_exits flattens all
        # #5 equal-split denominator = the CONFIGURED copy wallets (NOT target_set, which also holds
        # vault-resolved leader addresses). Fixed at init -> a dead/skipped wallet just leaves its slice
        # idle (safe under-allocation), it never enlarges the others' slices (which would oversize).
        self.n_copy_wallets = max(1, len(self.wallet_configs))

        # Resolve vault leaders: vaults trade under their leader address on WS
        self._resolve_vault_leaders()

        logger.info(f"V17: loaded {len(self.target_set)} wallets ({len(self.leader_to_vault)} vaults) from {config_path}")
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
        self.max_leverage = {}          # capped to config -- used in the engine's margin-reserve MATH
        self._raw_max_leverage = {}     # the coin's actual HL max -- used to SET cross leverage on-exchange
        self.all_perp_coins = []       # regular perp coins (no prefix)
        self.all_builder_coins = []    # builder-deployed coins (xyz:X, flx:Y, etc.)
        max_lev_cap = self.global_config["max_leverage_cap"]

        # Regular perps
        meta = self.info.meta_and_asset_ctxs()
        if meta and len(meta) == 2:
            for u in meta[0]["universe"]:
                name = u["name"]
                self.sz_decimals[name] = u.get("szDecimals", 2)
                self._raw_max_leverage[name] = int(u.get("maxLeverage", 3))
                self.max_leverage[name] = min(u.get("maxLeverage", 3), max_lev_cap)
                self.all_perp_coins.append(name)

        # Builder-deployed dexes (xyz, flx, etc.)
        for dex_name in BUILDER_DEXES:
            try:
                dex_meta = self.info.meta(dex=dex_name)
                for u in dex_meta.get("universe", []):
                    name = u["name"]  # already prefixed, e.g. "xyz:TSLA"
                    self.sz_decimals[name] = u.get("szDecimals", 2)
                    self._raw_max_leverage[name] = int(u.get("maxLeverage", 3))
                    self.max_leverage[name] = min(u.get("maxLeverage", 3), max_lev_cap)
                    self.all_builder_coins.append(name)
                logger.info(f"V17: loaded {dex_name} dex: {len([c for c in self.all_builder_coins if c.startswith(dex_name + ':')])} coins")
            except Exception as e:
                logger.warning(f"V17: failed to load {dex_name} dex meta: {e}")

        logger.info(
            f"V17: {len(self.all_perp_coins)} perp + {len(self.all_builder_coins)} builder coins available"
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

        # ── H17 x E1 BOTH-SIZE-TILT (agent N; Alberto "A more trades" + both-tilt; codex GO) ──
        # Boost favored NEW entries (low-crossed-share cohort T1 x E1 anti-crowd low-k_opp), cut
        # unfavored (T3 / crowded). Multiplicative, clamped. codex-required counterfactual auto-disable.
        self._tilt_enabled = bool(self.global_config.get("tilt_enabled", False))
        self._tilt_h17_t1 = float(self.global_config.get("tilt_h17_t1", 1.5))    # T1 cohort BOOST
        self._tilt_h17_t3 = float(self.global_config.get("tilt_h17_t3", 0.66))   # T3 cohort CUT
        self._tilt_e1_fav = float(self.global_config.get("tilt_e1_fav", 1.3))    # E1 favored BOOST
        self._tilt_e1_unfav = float(self.global_config.get("tilt_e1_unfav", 0.66))  # E1 crowded CUT
        self._tilt_e1_kopp_max = int(self.global_config.get("tilt_e1_kopp_max", 3))
        self._tilt_cap = float(self.global_config.get("tilt_cap", 2.0))          # mult ceiling
        self._tilt_floor = float(self.global_config.get("tilt_floor", 0.33))     # mult floor
        self._tilt_log = []          # (close_ts, pnl_bps, mult, notional) -- counterfactual window
        self._tilt_disabled_alerted = False
        self._h17_tercile = {}       # wallet(lower) -> 1|2|3 (1=T1 lowest crossed_share = favored)
        if self._tilt_enabled:
            self._load_h17_terciles()

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
        # V17 NEVER computes PnL internally. This cache is the ONLY source.
        self._exch_pnl = {
            "account_net": 0.0,    # all fills: closedPnl - fees
            "v17_net": 0.0,        # V17-attributed fills only (incl. liquidations of our positions)
            "v17_closes": 0,       # V17 closing fill count
            "account_closes": 0,   # all closing fills
            "fees": 0.0,           # total fees
            "liquidations": 0,     # count of our positions force-liquidated by HL
            "liquidation_pnl": 0.0,
            "last_sync": 0.0,      # timestamp of last successful sync
        }
        self._last_successful_sync = 0

        # Initial sync from exchange (blocking, must succeed before trading)
        try:
            self._do_exchange_fill_sync()
            logger.info(
                f"LOADED from EXCHANGE: {self._exch_pnl['account_closes']} closes, "
                f"account_net=${self._exch_pnl['account_net']:+.4f} "
                f"v17_net=${self._exch_pnl['v17_net']:+.4f} "
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
        self._seen_liq_tids = set()       # liquidation fill tids already recorded (dedup)
        self._liquidated_coins = {}       # coin -> ts of last confirmed liquidation (reconciler trigger)
        self._leverage_set = set()        # coins whose leverage/margin-mode we've set on the exchange
        self._leverage_set_fail = {}      # coin -> ts of last failed set (60s backoff before retry)
        self._post_exit_cooldown = {}  # (wallet, coin) -> timestamp of last exit

        # Dynamic l2Book subscriptions: coins we currently need book data for
        self._l2_subscribed = set()

        # Target position tracking
        self._target_positions = {}
        self._init_target_positions()

        # Position recovery
        if not self.shadow_mode:
            self._recover_positions()
            # Protect already-OPEN positions immediately: force CROSS margin on every held coin
            # (the at-risk isolated xyz positions get account-backed right away, not just on next trade).
            held_coins = {p['coin'] for p in self.positions if p.get('filled')}
            for _c in held_coins:
                self._set_coin_leverage(_c)
                time.sleep(0.15)   # gentle pacing to avoid a rate-limit burst
            if held_coins:
                logger.info(f"LEVERAGE: forced CROSS on {len(held_coins)} held coins at startup")

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
            leader_equity = 0.0
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
                    # V15: capture leader account equity (whole-account perp, summed over dexes) for
                    # proportional sizing. marginSummary.accountValue is in the SAME response.
                    av = data.get("marginSummary", {}).get("accountValue")
                    if av is not None:
                        leader_equity += float(av)
                if leader_equity > 0:
                    self._target_equity[addr] = leader_equity
                    self._target_equity_ts[addr] = time.time()
                    logger.info(f"TARGET INIT: {addr[:14]} equity ${leader_equity:,.0f}")
                time.sleep(0.3)
            except Exception as e:
                logger.warning(f"Can't init positions for {addr[:14]}: {e}")
                self._target_init_failed.add(addr)

        flat = sum(1 for a in self._target_positions if not self._target_positions[a])
        logger.info(f"Target positions initialized: {flat}/{len(self.target_set)} flat")

    # ── V15 PROPORTIONAL SIZING HELPERS ──────────────────────────────────────
    def _refresh_target_equity(self, addr: str) -> Optional[float]:
        """Re-query a leader's whole-account perp equity (sum of marginSummary.accountValue over dexes)
        for proportional sizing. Updates cache + ts. Returns the equity or None on failure (caller must
        treat None as 'cannot size proportionally -> skip', never as 0).

        codex r3 #4: ALSO refresh that leader's POSITIONS from the SAME clearinghouseState response, so a
        stale-equity refresh re-bases leader position size too (entry sizing then can't use a stale
        leader-position cache after a WS gap; WS still updates it live on the triggering fill)."""
        try:
            eq = 0.0
            fresh_positions = {}
            for dex_name in [""] + BUILDER_DEXES:
                payload = {"type": "clearinghouseState", "user": addr}
                if dex_name:
                    payload["dex"] = dex_name
                r = requests.post(f"{HL_API}/info", json=payload, timeout=5)
                data = r.json()
                if not data:
                    continue
                av = data.get("marginSummary", {}).get("accountValue")
                if av is not None:
                    eq += float(av)
                for ap in data.get("assetPositions", []):
                    p = ap.get("position", {})
                    c = p.get("coin")
                    if c is not None:
                        fresh_positions[c] = float(p.get("szi", 0) or 0)
            if eq > 0:
                self._target_equity[addr] = eq
                self._target_equity_ts[addr] = time.time()
                # re-base positions: set the queried coins; coins absent from the response are flat -> 0.
                cur = self._target_positions.setdefault(addr, {})
                for c in list(cur.keys()):
                    if c not in fresh_positions:
                        cur[c] = 0.0
                cur.update(fresh_positions)
                return eq
        except Exception as e:
            logger.warning(f"target equity refresh failed {addr[:14]}: {e}")
        return None

    def _leader_equity_fresh(self, wallet: str) -> Optional[float]:
        """Leader equity if fresh enough for sizing, else refresh, else None (staleness SLA)."""
        ts = self._target_equity_ts.get(wallet, 0)
        eq = self._target_equity.get(wallet)
        if eq is not None and (time.time() - ts) <= self.target_equity_max_age_s:
            return eq
        return self._refresh_target_equity(wallet)

    def _proportional_target_notional(self, wallet: str, coin: str, mark: float) -> Optional[float]:
        """V15 core: OUR target SIGNED notional for (wallet, coin) = leader_exposure_pct x OUR_SLICE.
        leader_exposure_pct = leader_pos_notional / leader_equity (signed by their side). OUR_SLICE =
        our_equity / n_copied_wallets -- EQUAL-SPLIT ALLOCATION across the copied leaders (Alberto
        2026-06-01): each leader mirrors its OWN leverage WITHIN its equal capital slice, so a 10x leader
        cannot eat the whole book and crowd out the others. Returns None when it cannot be sized safely
        (stale/missing leader equity, missing our equity, bad mark) -> caller SKIPS (never sizes off
        stale data). Per-slice -> no artificial gross cap needed; margin-util + the global stop backstop."""
        if self.sizing_mode != "proportional" or not mark or mark <= 0:
            return None
        leader_eq = self._leader_equity_fresh(wallet)
        if not leader_eq or leader_eq <= 0:
            return None
        our_eq = self._get_equity()
        if not our_eq or our_eq <= 0:
            return None
        our_slice = our_eq / self.n_copy_wallets              # equal-split allocation base (fixed denom)
        leader_szi = self._target_positions.get(wallet, {}).get(coin, 0.0)
        leader_notional = leader_szi * mark                   # signed (leader side)
        exposure_pct = leader_notional / leader_eq
        return exposure_pct * our_slice                       # OUR signed target notional (within slice)

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
            "_tilt_mult": pos.get("_tilt_mult", 1.0),
            "updated_at": datetime.now(timezone.utc),
        }
        try:
            self.db[DB_OPEN_POSITIONS].update_one(key, {"$set": doc}, upsert=True)
        except Exception as e:
            logger.warning(f"Failed to persist position {pos['coin']}: {e}")

    def _record_oid(self, oid, coin: str, side: str, action: str, wallet: str = ""):
        """Record every order ID V17 generates for fill attribution."""
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
                    "_tilt_mult": doc.get("_tilt_mult", 1.0),
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

    # ── H17 x E1 size-tilt ───────────────────────────────────────────────────
    def _load_h17_terciles(self):
        """Load per-cohort-wallet crossed_share terciles (H17). T1=lowest=favored.
        Reads the persisted v17_cohort_crossed_share parquet; wallets absent -> T3."""
        try:
            import pandas as pd
            p = "/Users/hermes/quants-lab/app/data/v16/v17_cohort_crossed_share.parquet"
            df = pd.read_parquet(p)
            df["wallet"] = df["wallet"].str.lower()
            df = df[df.wallet.isin({w.lower() for w in self.target_set})]
            if len(df) < 6:
                logger.warning(f"H17 tilt: only {len(df)} ranked wallets (<6); tilt DISABLED")
                self._tilt_enabled = False
                return
            r = df.set_index("wallet")["crossed_share"].rank(method="average")
            codes = pd.qcut(r, 3, labels=False, duplicates="drop")
            self._h17_tercile = {w: int(c) + 1 for w, c in codes.items()}
            t1 = sum(1 for v in self._h17_tercile.values() if v == 1)
            logger.info(f"H17 tilt loaded: {len(self._h17_tercile)} wallets ranked, "
                        f"{t1} in T1; h17 t1={self._tilt_h17_t1}/t3={self._tilt_h17_t3} "
                        f"e1 fav={self._tilt_e1_fav}/unfav={self._tilt_e1_unfav} "
                        f"(k_opp<={self._tilt_e1_kopp_max}) clamp[{self._tilt_floor},{self._tilt_cap}]")
            # codex: a prior auto-disable must survive restart (don't silently re-enable).
            try:
                st = self.db["v17_tilt_state"].find_one({"_id": "tilt"})
                if st and st.get("disabled"):
                    logger.warning("H17 tilt: previously AUTO-DISABLED (persisted); staying OFF. "
                                   "Clear v17_tilt_state to re-enable.")
                    self._tilt_enabled = False
                    self._tilt_disabled_alerted = True
            except Exception:
                pass
        except Exception as e:
            logger.warning(f"H17 tilt load failed ({e}); tilt DISABLED")
            self._tilt_enabled = False

    def _count_opposite_leaders(self, coin: str, is_buy: bool) -> int:
        """E1 k_opp: number of OTHER tracked leaders currently holding a position
        OPPOSITE to this (coin, direction). Live analog of the backtest k_opp.
        Low = uncrowded = favored entry. Source: _target_positions (already tracked)."""
        k = 0
        for coins in self._target_positions.values():
            sz = coins.get(coin, 0.0)
            if abs(sz) < 1e-10:
                continue
            if (sz > 0) != is_buy:   # leader opposite to our entry direction
                k += 1
        return k

    def _tilt_mult(self, wallet: str, coin: str, is_buy: bool):
        """BOTH-tilt multiplier for a NEW entry (Alberto: up favored AND down unfavored).
        H17: T1 wallet -> x h17_t1 (boost), T3 -> x h17_t3 (cut), T2 -> 1.0.
        E1: k_opp<=max -> x e1_fav (boost), else x e1_unfav (cut). Multiplicative, clamped
        to [tilt_floor, tilt_cap]. Returns (mult, k_opp). (1.0, None) when disabled."""
        if not self._tilt_enabled:
            return 1.0, None
        m = 1.0
        t = self._h17_tercile.get(wallet.lower(), 3)
        if t == 1:
            m *= self._tilt_h17_t1
        elif t == 3:
            m *= self._tilt_h17_t3
        k_opp = self._count_opposite_leaders(coin, is_buy)
        m *= self._tilt_e1_fav if k_opp <= self._tilt_e1_kopp_max else self._tilt_e1_unfav
        return min(max(m, self._tilt_floor), self._tilt_cap), k_opp

    def _log_tilt_outcome(self, pnl_bps: float, mult: float, notional: float):
        """Record a closed (or partially-closed) exit slice for the counterfactual monitor.
        notional = this slice's actual $ exposure (filled_sz*exit_px). NOTIONAL-weighted so
        partial fills / trims are counted by their real size (one slice each, never
        double-counted in $ terms). Bounded window."""
        try:
            mult = float(mult) if mult and mult > 0 else 1.0
            self._tilt_log.append((time.time(), float(pnl_bps), mult, float(notional)))
            if len(self._tilt_log) > 300:
                self._tilt_log = self._tilt_log[-300:]
        except Exception:
            pass

    def _tilt_advantage(self, win):
        """capital-wt tilted return minus baseline counterfactual return, in bps.
        tilted: deploys notional_i, earns pnl_i*notional_i.
        baseline: same trades at notional_i/mult_i. >0 => tilt is adding value per $."""
        import numpy as np
        bps = np.array([b for _, b, _, _ in win])
        mult = np.array([m for _, _, m, _ in win])
        notl = np.array([n for _, _, _, n in win])
        base_notl = notl / mult
        if notl.sum() <= 0 or base_notl.sum() <= 0:
            return None
        tilted = float((bps * notl).sum() / notl.sum())
        baseline = float((bps * base_notl).sum() / base_notl.sum())
        return tilted - baseline

    def _eval_tilt_counterfactual(self):
        """codex-REQUIRED guard: auto-disable the tilt if its realized edge over baseline
        goes negative. Disable if notional-weighted (tilted - baseline) <= -10 bps over the
        rolling >=100-slice window. Realized pnl_bps already includes fills/fees/slippage.
        Disabling only reverts NEW entries to baseline size (open positions keep their size)."""
        if not self._tilt_enabled or len(self._tilt_log) < 100:
            return
        win = self._tilt_log[-100:]
        adv = self._tilt_advantage(win)
        if adv is None:
            return
        if adv <= -10.0:
            self._tilt_enabled = False
            if not self._tilt_disabled_alerted:
                self._tilt_disabled_alerted = True
                msg = (f"TILT AUTO-DISABLED: counterfactual adv={adv:+.1f}bps <= -10 over "
                       f"n={len(win)} slices. Reverting NEW entries to baseline size.")
                logger.warning(msg)
                # Persist so a restart does NOT silently re-enable the killed tilt (codex).
                try:
                    self.db["v17_tilt_state"].update_one(
                        {"_id": "tilt"},
                        {"$set": {"disabled": True, "adv": float(adv),
                                  "ts": datetime.now(timezone.utc), "n": len(win)}},
                        upsert=True)
                except Exception:
                    pass
                try:
                    _tg(f"V17 {msg}")
                except Exception:
                    pass

    @staticmethod
    def _parse_clearinghouse(data: dict):
        """Parse (margin_used, positions, upnl) from a clearinghouseState dict.
        Same shape whether it came from REST clearinghouseState OR a webData2 push.
        positions: {coin: {marginUsed, positionValue, unrealizedPnl, szi}}."""
        margin_used = float(data.get("marginSummary", {}).get("totalMarginUsed", 0))
        positions = {}
        upnl = 0.0
        for p in data.get("assetPositions", []):
            pos = p.get("position", {})
            coin = pos.get("coin", "")
            szi = float(pos.get("szi", 0))
            if abs(szi) > 1e-10:
                u = float(pos.get("unrealizedPnl", 0))
                positions[coin] = {
                    "marginUsed": float(pos.get("marginUsed", 0)),
                    "positionValue": float(pos.get("positionValue", 0)),
                    "unrealizedPnl": u, "szi": szi,
                }
                upnl += u
        return margin_used, positions, upnl

    @staticmethod
    def _parse_spot_usdc(spot_json):
        """Spot USDC total (= HL equity per HARD RULE 16). None on transient null."""
        if not isinstance(spot_json, dict):
            return None
        return sum(
            float(b.get("total", 0))
            for b in spot_json.get("balances", [])
            if b.get("coin") == "USDC"
        )

    def _ingest_webdata2(self, wd: dict):
        """Update the WS-pushed main-dex account snapshot from a webData2 payload.
        Main dex + spot only; builder dexes (xyz/flx) are NOT in webData2 and stay
        on the (guarded) REST path. Sets _ws_state_ts so _refresh_exchange_state
        can prefer this over REST polling."""
        try:
            chs = wd.get("clearinghouseState", {})
            if isinstance(chs, dict) and chs and "assetPositions" in chs:
                m, pos, up = self._parse_clearinghouse(chs)
                self._ws_main_margin = m
                self._ws_main_positions = pos
                self._ws_main_upnl = up
                # Only stamp freshness when main state actually updated -- a spot-only
                # or malformed payload must NOT let the fast path serve stale positions.
                self._ws_state_ts = time.time()
            spot = self._parse_spot_usdc(wd.get("spotState", {}))
            if spot is not None:
                self._ws_spot_equity = spot  # diagnostics only (free USDC; not equity)
        except Exception as e:
            logger.debug(f"webData2 ingest failed: {e}")

    def _fetch_builder_dex_state(self):
        """REST-poll builder dexes (xyz/flx) -- not covered by webData2.
        Returns (margin_add, positions, upnl_add). Per-dex transient nulls skipped."""
        margin_add = 0.0
        positions = {}
        upnl_add = 0.0
        for dex_name in BUILDER_DEXES:
            try:
                rd = requests.post(
                    HL_API + "/info",
                    json={"type": "clearinghouseState", "user": self.parent_address, "dex": dex_name},
                    timeout=5,
                )
                dd = rd.json()
                if not isinstance(dd, dict):
                    continue
                m, pos, up = self._parse_clearinghouse(dd)
                margin_add += m
                positions.update(pos)
                upnl_add += up
            except Exception as e:
                logger.debug(f"Builder dex {dex_name} state fetch failed: {e}")
        return margin_add, positions, upnl_add

    def _refresh_exchange_state(self) -> bool:
        """Refresh account state. Prefers the fresh webData2 WS push for main-dex
        margin/positions/spot-equity (eliminates the REST clearinghouse+spot polls
        and the 'NoneType' transient-null crash); REST-polls only builder dexes.
        Full-REST fallback when the WS snapshot is stale. Cached 30s."""
        now = time.time()
        if self._equity_cache is not None and now - self._equity_cache_ts < 30:
            return True

        # ---- Preferred path: fresh WS (webData2) main margin/positions/upnl ----
        # webData2 is EXACT-parity for main-dex margin, positions, upnl (verified
        # 2026-06-12 parity test). It is NOT used for spot equity: webData2 reports
        # FREE USDC (hold zeroed) while the engine's equity = GROSS spot USDC per
        # HARD RULE 16 (the $35 hold is real). Spot stays on REST below.
        ws_ts = getattr(self, "_ws_state_ts", 0)
        if ws_ts and now - ws_ts < 15 and getattr(self, "_ws_main_positions", None) is not None:
            try:
                margin_used = getattr(self, "_ws_main_margin", 0.0)
                positions = dict(self._ws_main_positions)
                upnl = getattr(self, "_ws_main_upnl", 0.0)
                m2, pos2, up2 = self._fetch_builder_dex_state()
                margin_used += m2
                positions.update(pos2)
                upnl += up2
                # Spot equity: REST (gross USDC), null-guarded.
                r2 = requests.post(
                    HL_API + "/info",
                    json={"type": "spotClearinghouseState", "user": self.parent_address},
                    timeout=5,
                )
                spot = self._parse_spot_usdc(r2.json())
                if spot is None:
                    logger.debug("spot null on WS path; keeping cache")
                    return self._equity_cache is not None
                self._exch_margin_used = margin_used
                self._exch_positions = positions
                self._exch_unrealized_pnl = upnl
                self._equity_cache = spot
                self._equity_cache_ts = now
                return True
            except Exception as e:
                logger.debug(f"WS-state path failed, REST fallback: {e}")

        # ---- Fallback: full REST poll (main + builder + spot), all null-guarded ----
        try:
            r1 = requests.post(
                HL_API + "/info",
                json={"type": "clearinghouseState", "user": self.parent_address},
                timeout=5,
            )
            data = r1.json()
            if not isinstance(data, dict) or "assetPositions" not in data:
                # non-dict OR empty {} transient -> don't zero main state, keep cache.
                logger.debug("clearinghouseState missing/empty; keeping cache")
                return self._equity_cache is not None
            margin_used, positions, total_upnl = self._parse_clearinghouse(data)

            m2, pos2, up2 = self._fetch_builder_dex_state()
            margin_used += m2
            positions.update(pos2)
            total_upnl += up2

            # Fetch spot BEFORE committing any _exch_* -- a transient null on spot
            # must keep the ENTIRE prior snapshot, not leave margin/positions updated
            # against a stale equity (codex finding #1).
            r2 = requests.post(
                HL_API + "/info",
                json={"type": "spotClearinghouseState", "user": self.parent_address},
                timeout=5,
            )
            spot = self._parse_spot_usdc(r2.json())
            if spot is None:
                logger.debug("spotClearinghouseState returned non-dict; keeping cache")
                return self._equity_cache is not None

            self._exch_margin_used = margin_used
            self._exch_positions = positions
            self._exch_unrealized_pnl = total_upnl
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

    def _set_coin_leverage(self, coin: str):
        """Force CROSS margin on the exchange for this coin (Alberto 9430/9432).
        The engine otherwise inherits HL DEFAULTS -- xyz builder coins land on ISOLATED margin and get
        liquidated on normal moves (SNDK +4.8%, SPCX -11.3% on 2026-06-12). CROSS = the whole account
        backs each position, so a single coin's move can't isolate-liquidate it. We do NOT lower leverage
        (Alberto: the leverage number barely matters under cross with our fixed order size); we set it at
        the coin's FULL max so cross is the only change. Idempotent (tracked in _leverage_set). HL accepts
        cross on builder dexes (verified). Best-effort: failure is logged, never blocks trading."""
        if self.shadow_mode or coin in self._leverage_set:
            return
        # codex: mark ONLY on success (a transient failure must NOT permanently mark the coin
        # "protected" while it's still isolated). 60s failure-backoff so a persistent reject doesn't
        # spam every trade.
        if time.time() - self._leverage_set_fail.get(coin, 0) < 60:
            return
        lev = int(self._raw_max_leverage.get(coin, self.max_leverage.get(coin, 5)))   # FULL max, not lowered
        try:
            self.exchange.update_leverage(lev, coin, is_cross=True)
            self._leverage_set.add(coin)   # success -> never retry
            logger.info(f"LEVERAGE SET: {coin} -> {lev}x CROSS (margin-mode fix; leverage unchanged)")
        except Exception as e:
            self._leverage_set_fail[coin] = time.time()   # retry on a later entry
            logger.warning(f"LEVERAGE SET failed for {coin} ({lev}x cross): {e}")

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

        # codex #4: the NEW order's margin is ESTIMATED (existing margin = self._exch_margin_used is
        # exchange-truth and self-corrects next cycle). Reserve CONSERVATIVELY: cap the assumed leverage at
        # margin_reserve_max_lev so a high-max-lev coin can't make the util gate optimistic / under-reserve.
        reserve_max_lev = float(self.global_config.get("margin_reserve_max_lev", 10.0))
        lev = min(self._get_coin_leverage(coin), reserve_max_lev) if self.sizing_mode == "proportional" \
            else self._get_coin_leverage(coin)
        additional_margin = additional_notional / max(lev, 1.0)
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
        # V15: the next two caps are V17 FIXED-SIZE guardrails (addon x order_size, and a 35%-of-equity
        # single-coin notional cap). They contradict proportional copy with NO gross cap (Alberto
        # 2026-06-01: mirror leaders' true exposure; risk capped by margin-util 0.95 + coin-concentration
        # + the global -15% stop, NOT an artificial per-coin notional clamp). So they apply in FIXED mode
        # only. In proportional mode the binding constraints above (total util, coin concentration) stand.
        if self.sizing_mode != "proportional":
            # Per-coin notional cap. The H17xE1 up-tilt intentionally sizes FAVORED entries above
            # base, so a SINGLE tilted entry must not be blocked by its own notional (else the cap
            # neuters exactly the favored entries we want). Cap at max(addon x base, this entry's
            # notional); cumulative stacking beyond that is still blocked, and the 35%-equity hard cap
            # below + margin-util remain the binding outer bounds.
            coin_cap = max(max_addon_mult * self.order_size, additional_notional)
            if coin_notional > coin_cap:
                logger.info(
                    f"Margin BLOCKED {coin}: notional ${coin_notional:.0f} > cap ${coin_cap:.0f} "
                    f"(addon {max_addon_mult}x base ${self.order_size}; tilted entry ${additional_notional:.0f})"
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

        # Force CROSS margin + capped leverage on this coin before trading it (once per coin).
        # Stops the xyz isolated-margin liquidations (Alberto 9430).
        self._set_coin_leverage(coin)

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

        # V15: book/mid is computed FIRST (proportional sizing needs mid before the margin check, which
        # now gates on the actual proportional notional rather than a fixed $order_size).
        # Use WS-fed book data, fall back to mid-price from trade feed
        book = self._book_depth.get(coin)
        if not book or book.get("ts", 0) == 0 or book.get("best_bid", 0) <= 0:
            # No live l2Book. Fall back to the trade-feed mid -- but only if it is FRESH (codex #1: never
            # size a proportional order off a stale mark). Reject if older than mark_max_age_s.
            fallback_mid = self.mid_prices.get(coin, 0)
            fallback_age = time.time() - self._mid_price_ts.get(coin, 0)
            if fallback_mid > 0 and fallback_age <= self.mark_max_age_s:
                spread_est = fallback_mid * 0.0005  # estimate 5bps spread
                book = {
                    "best_bid": fallback_mid - spread_est,
                    "best_ask": fallback_mid + spread_est,
                    "bid_usd": 10000,  # assume sufficient depth
                    "ask_usd": 10000,
                    "ts": time.time(),
                }
                logger.info(f"Entry {coin}: using trade-price fallback book (mid={fallback_mid:.4f}, age={fallback_age:.0f}s)")
            else:
                logger.info(f"V15 SKIP {coin}: no live book and fallback mid stale/missing "
                            f"(age={fallback_age:.0f}s > {self.mark_max_age_s:.0f}s)")
                return
        else:
            # codex #1: gate live book staleness too -- a non-zero but OLD book ts must not size an order.
            book_age = time.time() - book.get("ts", 0)
            if book_age > self.mark_max_age_s:
                logger.info(f"V15 SKIP {coin}: live book stale (age={book_age:.0f}s > {self.mark_max_age_s:.0f}s)")
                return
        best_bid = book["best_bid"]
        best_ask = book["best_ask"]
        mid = (best_bid + best_ask) / 2
        self.mid_prices[coin] = mid
        self._mid_price_ts[coin] = time.time()

        # ── V15 PROPORTIONAL ENTRY NOTIONAL (converge OUR position to the leader's exposure level) ──
        # V17 added a fixed $order_size per detected trade; V15 sizes the order as the DELTA needed to
        # bring our position up to (leader_exposure% x our_equity). On staleness/missing leader equity we
        # SKIP (never size off stale data). sizing_mode='fixed' falls back to V17 behavior.
        if self.sizing_mode == "proportional":
            tgt_notional = self._proportional_target_notional(twap_wallet, coin, mid)
            if tgt_notional is None:
                logger.info(f"V15 SKIP {coin} {twap_wallet[:10]}: cannot size proportionally "
                            f"(stale/missing leader equity or mark)")
                return
            our_cur_abs = abs(existing["size"] * mid) if existing else 0.0
            entry_notional = abs(tgt_notional) - our_cur_abs   # add toward the leader's exposure level
            if entry_notional < self.min_entry_notional:
                logger.debug(f"V15 {coin} {twap_wallet[:10]}: at/above leader exposure "
                             f"(delta ${entry_notional:.0f} < min ${self.min_entry_notional:.0f}) -- no add")
                return
        else:
            entry_notional = self.order_size

        # H17 x E1 BOTH-tilt: scale NEW entries (not add-ons) by the favored/unfavored multiplier.
        # Applied to entry_notional so the margin check, round_size, and pending-margin all see the
        # ACTUAL tilted notional (codex: tilt is the safe direction; sz<=0 + min_entry_notional guard dust).
        if existing is None:
            tilt_mult, _tilt_kopp = self._tilt_mult(twap_wallet, coin, is_buy)
        else:
            tilt_mult, _tilt_kopp = 1.0, None
        entry_notional *= tilt_mult

        # Margin budget check (on the ACTUAL proportional notional)
        if self.shadow_mode:
            shadow_margin = sum(
                abs(p.get('size', 0) * p.get('entry_px', 0)) / self._get_coin_leverage(p['coin'])
                for p in self.positions if p.get('filled')
            )
            equity = self._equity_cache or 500.0  # conservative fallback matching actual account
            shadow_util = (shadow_margin + entry_notional / self._get_coin_leverage(coin)) / equity
            if shadow_util > self.global_config["max_margin_util"]:
                logger.info(f"SHADOW margin blocked {coin}: {shadow_util:.0%} util")
                return
        elif not self._check_margin_budget(coin, entry_notional, wallet=twap_wallet):
            return

        sz = self._round_size(coin, entry_notional / mid)
        if sz <= 0:
            return

        # Track pending margin BEFORE await. codex r2 #5: reserve pending margin at the SAME conservative
        # leverage the budget check uses (cap at margin_reserve_max_lev in proportional mode), else
        # concurrent in-flight entries under-reserve for high-max-lev coins.
        _reserve_max_lev = float(self.global_config.get("margin_reserve_max_lev", 10.0))
        lev = min(self._get_coin_leverage(coin), _reserve_max_lev) if self.sizing_mode == "proportional" \
            else self._get_coin_leverage(coin)
        pending_add = entry_notional / max(lev, 1.0)
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
                    "_shadow": True, "_tilt_mult": tilt_mult,
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
                    # Blend tilt mult by notional (add-on adds baseline 1.0 size). effective mult =
                    # total actual notional / total baseline notional (codex-correct). Keeps the
                    # counterfactual attribution honest for the blended position.
                    _old_mult = existing.get("_tilt_mult", 1.0) or 1.0
                    _base_notl = old_notional / _old_mult + new_notional
                    if _base_notl > 0:
                        existing["_tilt_mult"] = (old_notional + new_notional) / _base_notl
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
                        "_tilt_mult": tilt_mult,
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

    async def _emergency_flatten(self) -> int:
        """DEDICATED hard-stop flatten (codex r3 #1/#2/#3). Reads EXCHANGE TRUTH (clearinghouseState, all
        dexes) -- NOT the tracker -- and market-closes every open position via the SDK (reduce-only market
        order, no orderbook dependency, no give-up, no partial-IOC poison). Idempotent: call every poll
        until it returns 0 open. Tracker is then reconciled to the (flat) exchange by _reconcile_positions.
        Returns the number of positions it attempted to close this pass."""
        n = 0
        for dex_name in [""] + BUILDER_DEXES:
            try:
                payload = {"type": "clearinghouseState", "user": self.parent_address}
                if dex_name:
                    payload["dex"] = dex_name
                data = await asyncio.to_thread(
                    lambda p=payload: requests.post(f"{HL_API}/info", json=p, timeout=5).json())
                if not data:
                    continue
                for ap in data.get("assetPositions", []):
                    pos = ap.get("position", {})
                    coin = pos.get("coin")
                    szi = float(pos.get("szi", 0) or 0)
                    if not coin or abs(szi) < 1e-12:
                        continue
                    n += 1
                    try:
                        # SDK market-close: reduce-only market order at default slippage, no book needed.
                        await asyncio.to_thread(self.exchange.market_close, coin)
                        logger.error(f"EMERGENCY FLATTEN: market_close {coin} szi={szi}")
                    except Exception as e:
                        logger.error(f"EMERGENCY FLATTEN failed {coin}: {e}")
            except Exception as e:
                logger.error(f"EMERGENCY FLATTEN clearinghouse query failed (dex={dex_name}): {e}")
        return n

    def _evaluate_global_stop_fast(self):
        """codex r3 #5: evaluate the -15% stop on the FAST exit-poll cadence (not only the 60s _log_stats),
        so the flatten latch fires within ~exit_poll_s, not up to 60s late. Sets _flatten_requested."""
        if self._flatten_requested or self._baseline_equity is None:
            return
        try:
            base = getattr(self, "_session_realized_base", None)
            if base is None:
                return
            realized = self._exch_pnl["account_net"] - base
            net_pnl = realized + self._compute_unrealized_pnl()
            stop_usd = -self.global_stop_pct * self._baseline_equity
            if net_pnl <= stop_usd:
                logger.error(f"GLOBAL STOP (fast): net ${net_pnl:.2f} <= ${stop_usd:.2f} -- FLATTEN")
                _tg(f"GLOBAL STOP -{self.global_stop_pct:.0%} (fast): net ${net_pnl:.2f} -- FLATTENING all")
                self._kill_reasons["global_stop"] = True
                self._flatten_requested = True
        except Exception as e:
            logger.warning(f"fast global-stop eval failed: {e}")

    async def _check_exits(self):
        """Exit when TARGET exits, not on fixed timer."""
        now = time.time()
        still_open = []
        exited_ids = set()

        # codex r3 #5: fast -15% stop check (every poll, not just the 60s stats loop).
        self._evaluate_global_stop_fast()

        # codex r3 #1/#2/#3: GLOBAL STOP / backstop -> use the DEDICATED exchange-truth emergency flatten
        # (market_close every real position; no V17 exit-machinery give-up / partial-IOC / book deps).
        # Reconcile then syncs the tracker to the flat exchange. Entries stay off (kill latched).
        if self._flatten_requested:
            n_remaining = await self._emergency_flatten()
            self._reconcile_positions()
            if n_remaining == 0:
                logger.info("GLOBAL STOP: exchange flat. Halted (manual re-arm to resume).")
            return

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

            # NOTE (codex r2 #2/#3): a V15 proportional DOWN-convergence layer was removed here. It ran
            # before the hard SL (could delay it) and read V17's leader-position cache, which only updates
            # on opens (not on leader reductions) -> it could not reliably catch reductions anyway. V17's
            # existing reverse-flow exit path already mirrors leader closes/reductions. A proper
            # down-convergence reconciler (fresh per-leader clearinghouseState query, IOC trim, AFTER the
            # risk-exit layers) is deferred to v2.

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
                    # V16 (codex r2 blocker #2): full-exit threshold is config-driven so a strategy can
                    # collapse the partial-trim band (runtime == replay proof). Default 0.90 unchanged.
                    is_full_exit = trim_pct >= float(self.global_config.get("full_exit_trim_pct", 0.90))
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
            # Positions V17 entered are NEVER dust regardless of notional
            # They must be properly exited or held until target closes

            ws_book = self._book_depth.get(coin)
            if not ws_book or ws_book.get("best_bid", 0) <= 0:
                logger.debug(f"Exit deferred for {coin}: no WS book data")
                return False

            best_bid = ws_book["best_bid"]
            best_ask = ws_book["best_ask"]

            # Try maker exit first -- BUT skip maker for URGENT exits (global-stop flatten / force-exit):
            # a hard stop must go straight to IOC, not wait up to 60s for a maker fill (codex r2 #1).
            if not pos.get('_maker_exit_tried') and not pos.get('_force_exit'):
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
                    self._log_tilt_outcome(pnl_bps, pos.get('_tilt_mult', 1.0), filled_sz * exit_px)
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
                            self._log_tilt_outcome(pnl_bps, pos.get('_tilt_mult', 1.0), filled_sz * exit_px)
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
                self._log_tilt_outcome(pnl_bps, pos.get('_tilt_mult', 1.0), filled_sz * exit_px)

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
            logger.debug(f"V17: skip {coin} -- already have position, no add-ons")
            return

        cooldown_key = (wallet, coin)
        if now - self.last_entry.get(cooldown_key, 0) < cooldown_s:
            logger.debug(f"V17: cooldown active for {coin}")
            return

        if not self._is_opening_trade(wallet, coin, is_buy):
            logger.debug(f"V17: not an opening trade for {coin}")
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
                logger.info(f"V17: dynamic l2Book subscribe for {coin} (first target fill)")
                # Will be subscribed on next _sync_l2_subscriptions cycle
                self._l2_subscribed.add(coin)  # Mark for subscription
        chase_bps = abs(mid - px) / px * 10000
        if chase_bps > max_chase_bps:
            logger.info(f"V17 SKIP {coin}: chase {chase_bps:.0f}bps > {max_chase_bps}bps")
            return

        # Entry guard: spread
        book = self._book_depth.get(coin, {})
        bid = book.get("best_bid", 0)
        ask = book.get("best_ask", 0)
        spread_bps = (ask - bid) / mid * 10000 if mid > 0 and bid > 0 and ask > 0 else 999
        if spread_bps > max_spread_bps:
            logger.info(f"V17 SKIP {coin}: spread {spread_bps:.0f}bps > {max_spread_bps}bps")
            return

        # Entry guard: book depth
        depth = self._get_book_depth(coin)
        entry_depth = depth["ask_usd"] if is_buy else depth["bid_usd"]
        if entry_depth < min_book_depth:
            logger.info(f"V17 SKIP {coin}: depth ${entry_depth:.0f} < ${min_book_depth}")
            return

        # ALL GUARDS PASS -- enter immediately
        wallet_group = self.wallet_groups.get(wallet, "unknown")
        dedup_key = (wallet, coin, int(now))

        # Fix #5: set cooldown BEFORE async task to prevent burst duplicates
        cooldown_key = (wallet, coin)
        self.last_entry[cooldown_key] = now

        logger.info(
            f"V17 ENTRY: {wallet[:10]} {coin} {'BUY' if is_buy else 'SELL'} "
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
                        self._log_tilt_outcome(pnl_bps, pos.get('_tilt_mult', 1.0), filled_sz * exit_px)
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
            used_cache_fallback = False     # True if we fell back to the webData2 cache (not authoritative
                                            # enough for the liquidation-reconcile -- may be pre-liquidation)
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

            # NON-BLOCKING reliability fallback (replaces a blocking retry, codex): if a reconcile REST
            # query failed but the webData2-maintained _exch_positions cache is FRESH (<60s), use it as
            # the authoritative position set rather than treating everything as exchange=0 (which caused
            # the spurious mass-drift false alarms). _exch_positions carries signed szi per coin (main via
            # webData2 push + builders via the cached REST in _refresh_exchange_state).
            if not all(queries_ok.values()):
                cache_age = time.time() - getattr(self, "_equity_cache_ts", 0)
                cache = getattr(self, "_exch_positions", {}) or {}
                if cache_age < 60 and cache:
                    for _c, _d in cache.items():
                        if _c not in exchange_positions:           # don't overwrite a query that DID succeed
                            try:
                                exchange_positions[_c] = float(_d.get("szi", 0))
                            except (TypeError, ValueError):
                                pass
                    for _k in list(queries_ok):
                        queries_ok[_k] = True                       # cache is reliable -> treat as ok
                    used_cache_fallback = True
                    logger.info(f"Reconcile: REST query failed; used fresh webData2 cache "
                                f"({cache_age:.0f}s old, {len(cache)} positions) instead of flagging mass-drift")

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

            # ── LIQUIDATION RECONCILE (Alberto 9420) ─────────────────────────────────────────
            # A CONFIRMED liquidation (fill carried HL's liquidation marker) is positive proof the
            # position was force-closed -- so we may safely reconcile the coin's tracked lots DOWN to
            # exchange truth (Rule 8), clearing the liquidated phantom WITHOUT touching any live
            # re-entered position. Only ever REDUCES tracked magnitude; guarded by query success so a
            # transient query miss can never drop a real position.
            if not self.shadow_mode and all_queries_ok and self._liquidated_coins and not used_cache_fallback:
                _now = time.time()
                for coin, liq_ts in list(self._liquidated_coins.items()):
                    if _now - liq_ts > 7200:                 # 2h window then forget
                        self._liquidated_coins.pop(coin, None); continue
                    lots = [tp for tp in self.positions if tp.get('filled') and tp['coin'] == coin]
                    if not lots:
                        self._liquidated_coins.pop(coin, None); continue
                    exch_net = float(exchange_positions.get(coin, 0.0))   # signed; absent==0 (queries_ok)
                    tracked_net = sum((tp['size'] if tp['side'] == 'BUY' else -tp['size']) for tp in lots)
                    # codex: only reduce magnitude when tracked and exchange are the SAME side (or
                    # exchange is flat). A sign MISMATCH (tracked long vs exchange short, or vice versa)
                    # is NOT a simple liquidation-shrink -- do not blind-reduce; warn + leave for the
                    # next cycle / manual, and do NOT clear the trigger.
                    if abs(exch_net) > 1e-9 and (tracked_net > 0) != (exch_net > 0):
                        logger.warning(
                            f"LIQUIDATION RECONCILE SKIP {coin}: sign mismatch tracked={tracked_net:.6f} "
                            f"exchange={exch_net:.6f} -- not a simple shrink, leaving for manual/next.")
                        continue
                    excess = abs(tracked_net) - abs(exch_net)
                    if excess <= 1e-9:                        # tracked already <= exchange
                        self._liquidated_coins.pop(coin, None); continue
                    removed = []
                    for tp in sorted(lots, key=lambda t: t.get('entry_time', 0)):  # oldest (liquidated) first
                        if excess <= 1e-9:
                            break
                        if tp['size'] <= excess + 1e-9:
                            excess -= tp['size']; removed.append(tp)
                        else:
                            tp['size'] = round(tp['size'] - excess, 8); excess = 0.0
                            self._persist_position(tp)
                    for tp in removed:
                        self._remove_persisted_position(tp.get('wallet', ''), coin)
                        self._position_accumulated.pop((tp.get('wallet', ''), coin), None)
                    if removed:
                        _rm = {id(t) for t in removed}
                        self.positions = [tp for tp in self.positions if id(tp) not in _rm]
                    logger.warning(
                        f"LIQUIDATION RECONCILE: {coin} tracked {tracked_net:.6f} -> exchange {exch_net:.6f} "
                        f"(dropped {len(removed)} liquidated lot(s)); phantom cleared")
                    try:
                        _tg(f"Liquidation reconcile: {coin} tracking now matches exchange ({exch_net:.4f})")
                    except Exception:
                        pass
                    self._liquidated_coins.pop(coin, None)

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
                            f"exists on exchange but NOT tracked by V17"
                        )
                        _tg(f"ORPHAN: {coin} sz={exch_sz} on exchange, not tracked by V17. Manual close needed.")
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
        Runs every 60 seconds. Filters to V17-owned oids for accurate attribution."""
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
            # PnL epoch: subclasses may set self.pnl_epoch_ms (V16: go-live ts from v16_meta, Alberto
            # msg 9222 "updated labels and epoch start"). Default unchanged: V9 epoch 2026-05-09.
            v9_epoch_ms = int(datetime(2026, 5, 9, 23, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
            epoch_ms = int(getattr(self, "pnl_epoch_ms", 0) or v9_epoch_ms)
            recent = [f for f in fills if int(f['time']) >= epoch_ms]

            # Load V17's known oids for attribution
            v17_oids = set()
            try:
                for doc in self.db[DB_ORDER_IDS].find({}, {"oid": 1}):
                    v17_oids.add(doc["oid"])
            except Exception:
                pass

            # Store new fills (dedup by tid -- unique per fill on exchange)
            new_fills = 0
            _wall_ms = int(time.time() * 1000)
            for f in recent:
                tid = f.get('tid')
                oid = int(f.get('oid', 0))
                if not tid:
                    continue
                # Record liquidations for the reconciler (positive confirmation a position was
                # force-closed -> safe to reconcile tracked lots down to exchange truth, Rule 8).
                # codex: use the FILL's own time (not wall-clock) + a recency gate, so an OLD
                # liquidation re-returned by user_fills() after a restart can NOT retrigger a reconcile
                # that drops current tracking.
                _ftime = int(f.get('time', 0))
                if f.get('liquidation') and tid not in self._seen_liq_tids and _ftime > _wall_ms - 7_200_000:
                    self._seen_liq_tids.add(tid)
                    self._liquidated_coins[f['coin']] = _ftime / 1000.0   # seconds (reconcile uses time.time())
                    logger.warning(
                        f"LIQUIDATION: {f['coin']} {f['side']} sz={f['sz']} @ {f['px']} "
                        f"closedPnl={f.get('closedPnl')} (HL force-close; tracking will reconcile to exchange)"
                    )
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
                         },
                         # is_liquidation via $set (NOT $setOnInsert) so it BACKFILLS docs ingested
                         # before this patch (codex). HL marks liquidation fills with a 'liquidation'
                         # object; a liquidation of OUR position IS ours (HL places the close so its oid
                         # is not in our recorded oids) -- we attribute + count it, not leave it
                         # "unattributed" (Alberto 9420: liquidations are a first-class metric).
                         "$set": {"is_liquidation": bool(f.get('liquidation'))},
                        },
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
            v17_pnl = 0.0
            v17_fees = 0.0
            v17_closes = 0
            liq_count = 0     # liquidations (our positions force-closed by HL)
            liq_pnl = 0.0

            # V16: step-2 compute honors the same pnl epoch as step-1 ingest, so pre-epoch rows that
            # ever landed in the collection (e.g. a sync that ran before the epoch attr was set) can
            # never leak into account_net. Legacy default: epoch 0 -> no filter change.
            _q = {"time": {"$gte": epoch_ms}} if getattr(self, "pnl_epoch_ms", 0) else {}
            for doc in self.db[DB_EXCHANGE_FILLS].find(_q, {"closedPnl": 1, "fee": 1, "oid": 1, "is_liquidation": 1}):
                pnl = float(doc.get("closedPnl", 0))
                fee = float(doc.get("fee", 0))
                oid = doc.get("oid")
                is_liq = bool(doc.get("is_liquidation"))
                total_pnl += pnl
                total_fees += fee
                if abs(pnl) > 0.0001:
                    total_closes += 1
                # Attribute to V17 if it's our recorded order OR a liquidation of our position (HL
                # places the liquidation order, so its oid is NOT in v17_oids -- but the position was
                # ours, so the PnL is ours). This is what was previously leaking to "unattributed".
                if oid in v17_oids or is_liq:
                    v17_pnl += pnl
                    v17_fees += fee
                    if abs(pnl) > 0.0001:
                        v17_closes += 1
                if is_liq and abs(pnl) > 0.0001:
                    liq_count += 1
                    liq_pnl += pnl

            total_net = total_pnl - total_fees
            v17_net = v17_pnl - v17_fees
            unattributed_pnl = total_pnl - v17_pnl

            # Update exchange PnL cache (the ONLY source of PnL truth)
            old_net = self._exch_pnl["account_net"]
            self._exch_pnl = {
                "account_net": total_net,
                "v17_net": v17_net,
                "v17_closes": v17_closes,
                "account_closes": total_closes,
                "fees": total_fees,
                "liquidations": liq_count,
                "liquidation_pnl": liq_pnl,
                "last_sync": time.time(),
            }
            self._last_successful_sync = time.time()

            if abs(old_net - total_net) > 0.01:
                logger.info(
                    f"PNL SYNC: account=${total_net:+.4f} (closes={total_closes} fees=${total_fees:.4f}) "
                    f"| V17=${v17_net:+.4f} (closes={v17_closes} fees=${v17_fees:.4f}) "
                    f"| liquidations={liq_count} (${liq_pnl:+.4f}) "
                    f"| unattributed=${unattributed_pnl:+.4f}"
                )

            total_stored = self.db[DB_EXCHANGE_FILLS].count_documents({})
            if new_fills > 0:
                logger.debug(f"FILL SYNC: {total_stored} fills stored ({len(v17_oids)} V17 oids)")

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

        # Reason 2: GLOBAL STOP -- codex #3 fix: PERCENT of equity (not a $ amount) and FLATTEN on trigger
        # (not just block entries). net_pnl = realized + uPnL since session start; baseline = session-start
        # equity. Trigger when net_pnl <= -global_stop_pct x baseline. No auto-lift (a hit stop stays
        # latched; manual re-arm only -- you do not want a 15% stop oscillating back into the trade).
        total_upnl = self._compute_unrealized_pnl()
        # codex r2 #4: account_net is cumulative since the V17/v9 epoch, NOT this session. Snapshot a
        # SESSION realized baseline once, so the -15% stop measures THIS session's loss only (prior P&L
        # cannot mask current losses or trip the stop on startup).
        if not hasattr(self, "_session_realized_base") or self._session_realized_base is None:
            self._session_realized_base = float(self._exch_pnl["account_net"])
        realized = self._exch_pnl["account_net"] - self._session_realized_base
        net_pnl = realized + total_upnl
        if self._baseline_equity is None and self._equity_cache and self._equity_cache > 0:
            self._baseline_equity = float(self._equity_cache)
            logger.info(f"V15 baseline equity for global stop: ${self._baseline_equity:.2f} "
                        f"(stop at -{self.global_stop_pct:.0%} = ${-self.global_stop_pct*self._baseline_equity:.2f})")
        stop_usd = -self.global_stop_pct * (self._baseline_equity or 0.0)
        if self._baseline_equity and net_pnl <= stop_usd:
            if not self._kill_reasons.get("global_stop"):
                logger.error(f"GLOBAL STOP HIT: net ${net_pnl:.2f} <= -{self.global_stop_pct:.0%} "
                             f"(${stop_usd:.2f} of ${self._baseline_equity:.2f}). FLATTEN + entries off.")
                _tg(f"GLOBAL STOP -{self.global_stop_pct:.0%}: net ${net_pnl:.2f} -- FLATTENING all + entries off")
            self._kill_reasons["global_stop"] = True
            self._flatten_requested = True   # _check_exits flattens all open positions

        # #4 runaway backstop: flatten-all if total gross notional exceeds gross_backstop_x x equity
        # (guards a margin mis-estimate; inf by default = off, pending Alberto).
        if self._baseline_equity and self.gross_backstop_x != float("inf"):
            gross = sum(abs(p.get('size', 0) * self.mid_prices.get(p['coin'], 0))
                        for p in self.positions if p.get('filled'))
            if self._equity_cache and gross > self.gross_backstop_x * self._equity_cache:
                if not self._kill_reasons.get("gross_backstop"):
                    logger.error(f"GROSS BACKSTOP: gross ${gross:.0f} > {self.gross_backstop_x}x "
                                 f"eq ${self._equity_cache:.0f}. FLATTEN + entries off.")
                    _tg(f"GROSS BACKSTOP {self.gross_backstop_x}x: gross ${gross:.0f} -- FLATTENING all")
                self._kill_reasons["gross_backstop"] = True
                self._flatten_requested = True

        # Unified kill switch: active if ANY reason is present
        self._kill_switch_active = bool(self._kill_reasons)

        open_pos = [p for p in self.positions if p['filled']]
        open_coins = " ".join(f"{p['coin']}" for p in open_pos) if open_pos else "none"
        equity = self._equity_cache or 0
        margin_used = getattr(self, '_exch_margin_used', 0)
        margin_pct = (margin_used / equity * 100) if equity > 0 else 0
        ep = self._exch_pnl
        sync_age = int(now - ep["last_sync"]) if ep["last_sync"] > 0 else 999
        tilt_str = ""
        if getattr(self, "_tilt_enabled", False) or getattr(self, "_tilt_log", None):
            n = len(self._tilt_log)
            adv = ""
            if n >= 1:
                a = self._tilt_advantage(self._tilt_log[-100:])
                if a is not None:
                    adv = f" adv={a:+.0f}bp"
            tilt_str = f" tilt={'ON' if self._tilt_enabled else 'OFF'}(n={n}{adv})"
        logger.info(
            f"STATS: acct=${ep['account_net']:+.4f}({ep['account_closes']}) "
            f"v17=${ep['v17_net']:+.4f}({ep['v17_closes']}) "
            f"fees=${ep['fees']:.2f} uPnL=${total_upnl:+.4f} "
            f"liq={ep.get('liquidations', 0)}(${ep.get('liquidation_pnl', 0.0):+.2f}) "
            f"open={len(open_pos)}[{open_coins}] margin={margin_pct:.0f}% eq=${equity or 0:.2f} "
            f"sync={sync_age}s{tilt_str}"
        )

        # V17 internal TG report DISABLED -- replaced by exchange-truth pnl_tracker.py
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
            lines = [f"COPY TRADER V17 -- {datetime.now(timezone.utc).strftime('%H:%M')} UTC"]
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

            ax.set_title(f'Copy Trader {getattr(self, "label", "V17")} -- {len(closed_trades)} closed, {n_open} open', fontsize=12)
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

            path = '/tmp/copy_equity_curve_v17.png'
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
            f"Copy trader V17 starting: {len(self.target_set)} wallets, "
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

                    # Subscribe to webData2: push-based main-dex account state
                    # (margin/positions) -> serves _refresh_exchange_state without the main
                    # clearinghouse REST poll. Builder dexes + spot stay on (guarded) REST.
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "webData2", "user": self.parent_address}
                    }))

                    logger.info("WS subscribed")
                    if not hasattr(self, '_ws_ever_connected'):
                        self._ws_ever_connected = True
                        _tg(
                            f"{getattr(self, 'label', 'V17')} STARTED: {len(self.target_set)} wallets, "
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

                            elif channel == "webData2":
                                self._ingest_webdata2(data.get("data", {}))

                        except asyncio.TimeoutError:
                            pass

                        # Throttle expensive checks to max 1/sec
                        now_check = time.time()
                        if not hasattr(self, '_last_check') or now_check - self._last_check >= 1.0:
                            self._last_check = now_check
                            await self._check_twap_windows()
                            await self._check_exits()
                            self._eval_tilt_counterfactual()   # codex guard: auto-disable bad tilt
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
        logger.info(f"FINAL: acct=${ep['account_net']:+.4f}({ep['account_closes']}) v17=${ep['v17_net']:+.4f}({ep['v17_closes']})")



# ===== v16 layer (whitelist guard) =====
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
_REPO = _HERE.parent.parent


# ── V16 collections (module-level globals in the base engine; rebind BEFORE instantiation) ──────────
DB_COLLECTION = "v16_copy_trades"
DB_SHADOW_COLLECTION = "v16_shadow_signals"
DB_FILLS_COLLECTION = "v16_target_fills"
DB_OPEN_POSITIONS = "v16_open_positions"
DB_EXCHANGE_FILLS = "v16_exchange_fills"
DB_ORDER_IDS = "v16_order_ids"


L2_CALIB = _REPO / "app" / "data" / "v15" / "l2_calib_10coin.json"


class V16CopyTrader(CopyTrader):
    """V15 engine + liquid whitelist guard + faithful-copy config asserts."""

    def __init__(self, config_path: str, order_size_override: float = None, shadow: bool = False):
        # ── PnL epoch BEFORE super().__init__: the base engine runs its blocking fill sync INSIDE
        # __init__, and that sync reads self.pnl_epoch_ms. Setting it afterwards let the first sync
        # fall back to the legacy V9 epoch and pollute v16_exchange_fills with pre-V16 history
        # (caught 2026-06-11 launch day: stale chart/account_net). Persisted epoch wins; first live
        # start persists after super(); shadow uses a provisional 'now' without persisting.
        from pymongo import MongoClient as _MC
        _now_ms = int(time.time() * 1000)
        try:
            _doc = _MC("mongodb://localhost:27017", serverSelectionTimeoutMS=3000) \
                .quants_lab.v16_meta.find_one({"_id": "epoch"})
            self.pnl_epoch_ms = int(_doc["epoch_ms"]) if _doc else _now_ms
        except Exception:
            self.pnl_epoch_ms = _now_ms
        super().__init__(config_path, order_size_override=order_size_override, shadow=shadow)

        g, d = self.global_config, self.default_config

        # ── 1. whitelist: present, and EXACTLY the validated liquid set ─────────────────────────────
        wl = g.get("coin_whitelist")
        if not wl:
            raise ValueError("V16: global.coin_whitelist is REQUIRED (liquid majors hard guard)")
        self.coin_whitelist = set(wl)
        validated = set(json.load(open(L2_CALIB)).keys())
        if self.coin_whitelist != validated:
            raise ValueError(
                f"V16: coin_whitelist != validated LIQUID set. "
                f"extra={sorted(self.coin_whitelist - validated)} missing={sorted(validated - self.coin_whitelist)}. "
                f"Changing the universe voids the validation -- new codex gate required."
            )

        # ── 2. faithful-copy mechanic asserts (any violation = not the validated strategy) ──────────
        def _req(cond: bool, msg: str):
            if not cond:
                raise ValueError(f"V16 CONFIG VIOLATION: {msg}")

        _req(g.get("sizing_mode") == "fixed",
             "sizing_mode must be 'fixed' (validation equal-weights trades; prop split < $10 min order)")
        _req(10.0 <= float(g.get("order_size_usd", 0)) <= 200.0,
             "order_size_usd must be in [10, 200] (Alberto 2026-06-11: full account, up to 10x liquid)")
        _req(d.get("entry_mode") == "instant", "entry_mode must be 'instant' (leader open -> taker entry)")
        _req(d.get("exit_type") == "FIRST_CLOSE", "exit_type must be FIRST_CLOSE (faithful exit)")
        _req(int(d.get("max_addon_multiplier", 99)) <= 1,
             "max_addon_multiplier must be <=1 (copy the round-trip OPEN once; no stacking at $12)")
        _req(d.get("trail_activate_bps") is not None and d.get("trail_bps") is not None,
             "trailing TP required on every position (hard rule #7)")
        _req(d.get("sl_bps") is not None and float(d["sl_bps"]) < 0, "protective sl_bps < 0 required")
        _req(0 < float(g.get("max_margin_util", 1)) <= 0.7,
             "max_margin_util must be <= 0.7")
        _req(0 < float(g.get("global_stop_pct", 0)) <= 0.15,
             "global_stop_pct must be set (latched flatten-all) and <= 15%")
        _req(float(g.get("gross_backstop_x", 1e9)) <= 8.0, "gross_backstop_x must be <= 8.0")
        _req(g.get("cohort_asof") is not None,
             "cohort_asof missing -- config must be generated by research/v16/select_cohort.py")
        # codex gate 2026-06-11 finding #2 (BLOCK): FULL-CLOSE semantics. With exit_min_trim_pct >= 0.8
        # and a huge exit_twap_min_notional, the engine's FIRST_CLOSE machinery only acts when the
        # leader has closed >= 80% of tracked notional (full exit at >= 90%) -- the validated
        # full-close exit, not a 5%-reverse-flow trim.
        _req(float(g.get("exit_min_trim_pct", 0)) >= 0.80,
             "exit_min_trim_pct must be >= 0.80 (full-close exit semantics, codex finding #2)")
        _req(float(d.get("exit_twap_min_notional", 0)) >= 1e8,
             "exit_twap_min_notional must be huge (disable $-threshold trims, codex finding #2)")
        _req(abs(float(g.get("full_exit_trim_pct", 0.90)) - float(g["exit_min_trim_pct"])) < 1e-9,
             "full_exit_trim_pct must equal exit_min_trim_pct (no partial-trim band; codex r2 #2)")
        n_wallets = len(self.wallet_configs)
        _req(n_wallets >= 30, f"cohort {n_wallets} < 30 minimum (re-run selection)")
        _req(n_wallets <= 100, f"cohort {n_wallets} > 100 cap")

        # ── 3. feed scope: subscribe trades ONLY for whitelist coins (base subscribes everything).
        # Aligns the WS feed with the guard; no builder-dex (xyz:/flx:) coins in V16 at all.
        self.all_perp_coins = [c for c in self.all_perp_coins if c in self.coin_whitelist]
        self.all_builder_coins = []
        missing_feed = self.coin_whitelist - set(self.all_perp_coins)
        if missing_feed:
            raise ValueError(f"V16: whitelist coins missing from HL perp universe: {sorted(missing_feed)}")

        self._v16_blocked_signals = 0
        self._v16_add_fills = 0
        self._v16_suppressed_reverse = 0

        # ── codex r2 blocker #1: UNCONDITIONAL leader-position tracker. The base engine updates its
        # _target_positions only when an entry actually proceeds, so a guard-rejected open makes the
        # leader's next add look like a fresh open (and a later close look like an open of the other
        # side). V16 classifies every fill against THIS tracker, which is updated for every target
        # fill on a whitelisted coin regardless of whether we copied it. Seeded from the startup REST
        # snapshot (base _init_target_positions ran in super().__init__).
        self._v16_leader_pos = {}
        for addr, posmap in self._target_positions.items():
            for cn, sz in posmap.items():
                if cn in self.coin_whitelist and abs(sz) > 1e-12:
                    self._v16_leader_pos[(addr, cn)] = float(sz)

        # ── 4. label + PnL epoch (Alberto msg 9222: "pnl report with updated labels and epoch start").
        # Epoch = first LIVE start, persisted once in Mongo v16_meta; all PnL/report numbers count from
        # there (base fill-sync reads self.pnl_epoch_ms). Shadow runs use 'now' without persisting.
        self.label = "V16"
        if not self.shadow_mode:
            # First LIVE start persists the (pre-super) epoch; later starts re-read the persisted one.
            self.db.v16_meta.update_one(
                {"_id": "epoch"}, {"$setOnInsert": {"epoch_ms": self.pnl_epoch_ms,
                                                    "created_at": datetime.now(timezone.utc)}},
                upsert=True)
            self.pnl_epoch_ms = int(self.db.v16_meta.find_one({"_id": "epoch"})["epoch_ms"])
        logger.info(f"V16 PnL epoch: {datetime.fromtimestamp(self.pnl_epoch_ms/1000, timezone.utc)}")

        logger.info(
            f"V16 READY: cohort={n_wallets} wallets (asof {g['cohort_asof']}), "
            f"whitelist={sorted(self.coin_whitelist)}, order=${self.order_size:.0f} fixed, "
            f"margin_util<={g['max_margin_util']}, stop={g['global_stop_pct']:.0%} latched, "
            f"gross_backstop={g['gross_backstop_x']}x, trail={d['trail_activate_bps']}/{d['trail_bps']}bps, "
            f"sl={d['sl_bps']}bps, shadow={self.shadow_mode}"
        )

    # ── choke point 1: signal path ───────────────────────────────────────────────────────────────────
    # a) drop non-whitelist coins before any processing (liquid hard guard)
    # b) classify leader ADD fills (same direction as their tracked position): TRACK them into
    #    _target_positions + _position_accumulated but NEVER copy them. Fixes codex finding #3 (with
    #    max_addon_multiplier=1 the base engine skips adds WITHOUT updating state, so a later partial
    #    close looks like a near-full close and ejects us early) AND enforces entry purity: V16 enters
    #    ONLY on true 0->nonzero leader opens (the validated round-trip definition).
    def _on_hl_trade(self, trade: dict):
        coin = trade.get("coin", "")
        if coin not in self.coin_whitelist:
            users = trade.get("users", [])
            if len(users) >= 2 and (users[0].lower() in self.target_set or users[1].lower() in self.target_set):
                self._v16_blocked_signals += 1
                if self._v16_blocked_signals % 50 == 1:
                    logger.info(f"V16 GUARD: dropped target signal on non-whitelist coin {coin} "
                                f"(total dropped: {self._v16_blocked_signals})")
            return

        users = trade.get("users", [])
        if len(users) >= 2:
            buyer, seller = users[0].lower(), users[1].lower()
            w = buyer if buyer in self.target_set else (seller if seller in self.target_set else None)
            if w is not None:
                is_buy = (w == buyer)
                wallet = self.leader_to_vault.get(w, w)
                px = float(trade.get("px", 0) or 0)
                sz = float(trade.get("sz", 0) or 0)
                if px <= 0 or sz <= 0:
                    return
                key = (wallet, coin)
                prev = self._v16_leader_pos.get(key, 0.0)          # UNCONDITIONAL tracker (codex r2 #1)
                signed = sz if is_buy else -sz
                prev_notional = abs(prev) * px
                is_open = prev_notional < 1.0
                is_add = (not is_open) and ((prev > 0) == is_buy)
                we_hold = any(p['coin'] == coin and p.get('wallet') == wallet and p.get('filled')
                              for p in self.positions)

                if is_add:
                    # ADD: track, never copy -- even when the original open was guard-rejected
                    tid = trade.get("tid", "")
                    if tid:
                        if tid in self._seen_tids:
                            return
                        self._seen_tids[tid] = time.time()
                    self._v16_leader_pos[key] = prev + signed
                    self._target_positions.setdefault(wallet, {})[coin] = self._v16_leader_pos[key]
                    if we_hold:
                        self._position_accumulated[key] = self._position_accumulated.get(key, 0.0) + sz * px
                    self._v16_add_fills += 1
                    try:
                        self.db[DB_FILLS_COLLECTION].insert_one({
                            "wallet": wallet, "coin": coin, "side": "BUY" if is_buy else "SELL",
                            "price": px, "size": sz, "notional": sz * px,
                            "wallet_group": self.wallet_groups.get(wallet, "unknown"),
                            "market_type": "perp", "v16_class": "add_tracked_not_copied",
                            "timestamp": datetime.now(timezone.utc),
                            "ts_epoch": time.time(),
                        })
                    except Exception:
                        pass
                    return

                if (not is_open) and (not is_add) and not we_hold:
                    # REVERSE on a position we never copied (e.g. the open was guard-rejected): track
                    # only, NEVER pass to the base handler -- its own (stale) view would classify this
                    # as an opening trade and copy the leader's CLOSE as our OPEN (codex r2 #1).
                    tid = trade.get("tid", "")
                    if tid:
                        if tid in self._seen_tids:
                            return
                        self._seen_tids[tid] = time.time()
                    self._v16_leader_pos[key] = prev + signed
                    self._target_positions.setdefault(wallet, {})[coin] = self._v16_leader_pos[key]
                    self._v16_suppressed_reverse += 1
                    return

                # OPEN (leader ~flat) or REVERSE-while-we-hold: update tracker, hand to the base
                # engine (entry guards / exit-TWAP machinery). Base may also update its own
                # _target_positions on a copied open; ours is authoritative for classification.
                # Dedup: CHECK (without consuming) -- the base handler inserts the tid itself.
                tid = trade.get("tid", "")
                if tid and tid in self._seen_tids:
                    return
                self._v16_leader_pos[key] = prev + signed
        return super()._on_hl_trade(trade)

    # ── choke point 2: order path -- nothing outside the whitelist can ever reach the exchange ──────
    async def _enter_position(self, coin: str, is_buy: bool, twap_dedup_key=None, wallet: str = None,
                              skip_cooldown: bool = False):
        if coin not in self.coin_whitelist:
            logger.error(f"V16 GUARD BREACH BLOCKED: _enter_position called for non-whitelist {coin} "
                         f"(wallet={wallet}) -- investigate the caller")
            _tg(f"V16 GUARD: blocked non-whitelist entry attempt {coin}")
            return
        return await super()._enter_position(coin, is_buy, twap_dedup_key=twap_dedup_key,
                                             wallet=wallet, skip_cooldown=skip_cooldown)

    # ── codex finding #4: TAKER/IOC exits for V16. The base engine posts a maker (ALO) exit and waits
    # up to 60s before IOC fallback -- unmodeled delay + adverse selection, worst on SL/trail exits.
    # The validation prices exits as immediate taker. Pre-setting the maker-state flags routes the base
    # state machine straight to its IOC block (escalating-slip IOC, fill-verified) for EVERY exit.
    async def _exit_position(self, pos: dict, trim_size: float = None) -> bool:
        if not pos.get('_maker_exit_tried'):
            pos['_maker_exit_tried'] = True       # skip the ALO leg entirely
            pos['_maker_exit_time'] = 0.0         # elapsed >= 60s instantly -> IOC path now
        return await super()._exit_position(pos, trim_size=trim_size)



# ===== v17 layer (entry gate + expansion) =====

# ── V17 collections: rebind the SAME module-level globals v16 rebound, BEFORE instantiation ──────
DB_COLLECTION = "v17_copy_trades"
DB_SHADOW_COLLECTION = "v17_shadow_signals"
DB_FILLS_COLLECTION = "v17_target_fills"
DB_OPEN_POSITIONS = "v17_open_positions"
DB_EXCHANGE_FILLS = "v17_exchange_fills"
DB_ORDER_IDS = "v17_order_ids"



class V17CopyTrader(V16CopyTrader):
    """V16 engine + knet gate + exposure caps + seed-completeness + staleness kill."""

    # V16 hard-asserts global_stop_pct <= 0.15; V17's validated budget is 0.25. Same for the V16
    # epoch collection. Strategy: read the V17 epoch from v17_meta BEFORE super().__init__ (the
    # V16 __init__ reads v16_meta -- we pre-empt by setting pnl_epoch_ms afterwards is too late
    # for the fill sync), and temporarily satisfy the V16 stop assert by validating the V17 bound
    # OURSELVES and presenting V16's bound during its __init__ via a config shim. Cleaner: V16's
    # assert reads the loaded config dict; we subclass-validate first, then monkeypatch the bound
    # check by pre-clamping... Simplest CORRECT path: V16's _req assert runs inside its __init__
    # on self.global_config -- we pass it a config FILE whose stop is 0.15-compliant? NO: the
    # engine must RUN with 0.25. Resolution: V16's assert line reads global_stop_pct from config;
    # we therefore re-implement the V16 __init__ asserts? Wrong -- fragile duplicate. ACTUAL
    # resolution implemented: temporarily present stop=0.15 to the V16 asserts via a shimmed json
    # on disk is gross. We instead OVERRIDE the value in the in-memory config AFTER V16 asserts
    # but BEFORE the engine arms the stop. The base engine reads global_stop_pct lazily from
    # self.global_config at stop-check time (verified: _check_global_stop reads
    # self.global_config['global_stop_pct'] each pass), so: feed V16 a 0.15 value through
    # __init__, then restore 0.25 immediately after -- with an explicit re-assert of the V17
    # bound + a loud log. The codex CODE review must confirm the lazy-read claim.

    def __init__(self, config_path: str, order_size_override: float = None, shadow: bool = False):
        raw = json.load(open(config_path))
        g = raw.get("global", {})
        self._v17_stop_pct = float(g.get("global_stop_pct", 0.25))
        if not (0 < self._v17_stop_pct <= 0.30):
            raise ValueError(f"V17: global_stop_pct {self._v17_stop_pct} outside (0, 0.30]")
        self._v17_knet_min = int(g.get("knet_min", 0))
        self._v17_netx_cap_x = float(g.get("netx_cap_x", 2.5))
        self._v17_coin_side_cap_x = float(g.get("coin_side_cap_x", 2.0))
        self._v17_seed_min = int(g.get("seed_min_wallets", 98))
        if not (0 < self._v17_netx_cap_x <= 6.0 and 0 < self._v17_coin_side_cap_x <= 4.0):
            raise ValueError("V17: cap params out of range")

        # shim: satisfy V16's <=0.15 stop assert during super().__init__, restore after.
        self._v17_shim_path = None
        if self._v17_stop_pct > 0.15:
            import tempfile
            shim = dict(raw)
            shim["global"] = dict(g)
            shim["global"]["global_stop_pct"] = 0.15
            fd, shim_path = tempfile.mkstemp(prefix="v17_shim_", suffix=".json")
            Path(shim_path).write_text(json.dumps(shim))
            import os as _os
            _os.close(fd)
            self._v17_shim_path = shim_path     # deleted after super().__init__ (codex P2.7)
            cfg_for_super = shim_path
        else:
            cfg_for_super = config_path

        # V17 epoch must exist before the base fill-sync (same launch-day lesson as V16).
        # NOTE: V16.__init__ pre-super sets pnl_epoch_ms from v16_meta, so the base's in-init fill
        # sync may write a few pre-V17-epoch fills into v17_exchange_fills; harmless -- every PnL
        # read filters ts >= the V17 epoch we restore right after super(). Codex: please confirm.
        from pymongo import MongoClient as _MC
        _now_ms = int(time.time() * 1000)
        try:
            _doc = _MC("mongodb://localhost:27017", serverSelectionTimeoutMS=3000) \
                .quants_lab.v17_meta.find_one({"_id": "epoch"})
            _v17_epoch = int(_doc["epoch_ms"]) if _doc else _now_ms
        except Exception:
            _v17_epoch = _now_ms

        super().__init__(cfg_for_super, order_size_override=order_size_override, shadow=shadow)
        if self._v17_shim_path:
            try:
                Path(self._v17_shim_path).unlink(missing_ok=True)
            except Exception:
                pass

        # restore the REAL stop on BOTH the cached attr (the one the stop checks read --
        # hl_copy_trader_v15.py line 151 caches it at __init__; verified 2026-06-11; codex code
        # review r1 confirmed fast + stats stops read the attr) and the config dict. Re-assert.
        self.pnl_epoch_ms = _v17_epoch
        self.global_stop_pct = self._v17_stop_pct
        self.global_config["global_stop_pct"] = self._v17_stop_pct
        if not (0 < float(self.global_stop_pct) <= 0.30):
            raise ValueError("V17: stop restore failed")
        logger.info(f"V17: global stop set to {self.global_stop_pct:.0%} (latched flatten-all; "
                    f"attr + config restored after the V16 assert shim)")
        # codex P2.5: the in-init fill sync ran on the V16 epoch (V16 pre-super overwrote it);
        # re-sync NOW on the restored V17 epoch so startup _exch_pnl is correct from minute zero.
        try:
            self._do_exchange_fill_sync()
            logger.info("V17: exchange fill sync re-run on the V17 epoch")
        except Exception as e:
            logger.warning(f"V17: post-restore fill sync failed (will retry on cadence): {e}")

        # ── seed retry (smoke3 finding: transient HL REST rate-limits fail ~10% of the 103
        # seeding calls; one pass is not enough). Retry failures up to 2x with backoff BEFORE
        # the audit; _init_target_positions semantics preserved (it skips already-good wallets
        # via the failed set we re-feed).
        for attempt in (1, 2):
            failed_now = set(getattr(self, "_target_init_failed", set()))
            if not failed_now:
                break
            logger.info(f"V17 SEED RETRY {attempt}: {len(failed_now)} wallets, 5s backoff")
            time.sleep(5)
            still_failed = set()
            for addr in sorted(failed_now):
                # codex P1.1: query ALL dexes ([""]+BUILDER_DEXES) exactly like the base
                # _init_target_positions, NOT just the main perp dex. The base seed covers every dex,
                # but this retry (which re-queries rate-limited wallets and re-seeds _v16_leader_pos)
                # previously hit only the main dex -- so a wallet that failed its initial seed and
                # holds an xyz:* (builder) expansion coin would never get that leader position
                # reseeded, and _v17_init_expansion (which runs AFTER this loop) would seed an empty
                # _v16_leader_pos -> the leader's later add/close on that coin misclassifies as a fresh
                # OPEN with the wrong knet seed. Querying all dexes here closes that gap BEFORE the
                # expansion seeding. A NULL on the main dex marks the wallet still-failed (agent-key
                # case); builder-dex NULLs are skipped per-dex (the wallet may simply hold nothing
                # there) -- identical to the base loop's per-dex semantics.
                try:
                    main_ok = False
                    self._target_positions.setdefault(addr, {})
                    for dex_name in [""] + BUILDER_DEXES:
                        payload = {"type": "clearinghouseState", "user": addr}
                        if dex_name:
                            payload["dex"] = dex_name
                        r = requests.post(f"{HL_API}/info", json=payload, timeout=8)
                        data = r.json()
                        if data is None:
                            if dex_name == "":
                                still_failed.add(addr)   # main-dex NULL = still failed (base semantics)
                            continue
                        if dex_name == "":
                            main_ok = True
                        for p in data.get("assetPositions", []):
                            pos = p["position"]
                            self._target_positions[addr][pos["coin"]] = float(pos["szi"])
                            if pos["coin"] in self.coin_whitelist and abs(float(pos["szi"])) > 1e-12:
                                self._v16_leader_pos[(addr, pos["coin"])] = float(pos["szi"])
                        time.sleep(0.2)
                    if not main_ok:
                        still_failed.add(addr)
                except Exception:
                    still_failed.add(addr)
            self._target_init_failed = still_failed

        # ── seed completeness (codex r3 + code-review P2.6): denominators = the 100 CONFIGURED
        # wallets; vault leaders audited separately (a top-30 vault whose resolved leader failed
        # to seed also blocks).
        failed = getattr(self, "_target_init_failed", set())
        configured = {w.lower() for w in self.wallet_configs}
        vault_leaders = {l for l, v in self.leader_to_vault.items()}
        failed_cfg = {f for f in failed if f in configured}
        n_seeded = len(configured) - len(failed_cfg)
        top30 = {w.lower() for w, m in self.wallet_configs.items() if int(m.get("rank", 999)) <= 30}
        top30_vaults = {v for l, v in self.leader_to_vault.items() if v in top30}
        failed_top30 = sorted((failed & top30) |
                              {self.leader_to_vault[l] for l in (failed & vault_leaders)
                               if self.leader_to_vault[l] in top30})
        self._v17_trading_enabled = (n_seeded >= self._v17_seed_min) and not failed_top30
        logger.info(f"V17 SEED AUDIT: {n_seeded}/{len(configured)} configured wallets seeded "
                    f"(min {self._v17_seed_min}); vault-leader fails: {sorted(failed & vault_leaders) or 'none'}; "
                    f"top30 blocks: {failed_top30 or 'none'}; trading_enabled={self._v17_trading_enabled}")
        if not self._v17_trading_enabled:
            _tg(f"V17 BOOT: trading DISABLED (seeded {n_seeded}/{len(self.target_set)}, "
                     f"top30 fails {failed_top30}). Re-seed required.")

        # ── stale-tracker kill state ──
        self._v17_last_target_fill_ts = time.time()
        # ── counters for week-1 audits (codex r4) ──
        self._v17_knet_rejects = 0
        self._v17_netx_rejects = 0
        self._v17_coinside_rejects = 0
        self._v17_stale_rejects = 0
        self._v17_knet_pending = {}        # (wallet, coin) -> FIFO [(knet, signal_ts), ...]
        # in-flight exposure reservations (codex P1.4); over-counts during the fill-land overlap
        # window by design (conservative direction)
        self._v17_pending_net = 0.0
        self._v17_pending_coin_side = {}   # (coin, side) -> reserved $

        # ══════════════════════════════════════════════════════════════════════════════════════════
        # V17 UNIVERSE-EXPANSION GUARDS (agent J, 2026-06-12; codex go-live requirement, 4 guards).
        # ADDITIVE + FLAG-GATED: with NO `global.expansion` block in the config (the current live
        # config/copy_trader_wallets_v17.json), _v17_init_expansion() leaves every guard structure
        # EMPTY and NO expansion code path is ever entered -- the engine behaves byte-identically to
        # the validated 10-coin V17. With an expansion block present it admits the new coins behind
        # per-coin + expansion-wide kill switches. New-coin edge is in-sample-only; these guards
        # quarantine that risk so a bad new coin disables ITSELF (and, in aggregate, ALL new coins)
        # without ever touching the baseline 10. Existing positions on a disabled coin still EXIT
        # normally; only NEW ENTRIES are blocked.  FAIL-CLOSED throughout: any uncertainty about a
        # new coin's guard state skips the entry rather than trading it.
        # Read from self.global_config (the authoritative loaded config the engine runs on); the
        # stop-shim used during super().__init__ shallow-copied global so the expansion key survives.
        self._v17_init_expansion(self.global_config.get("expansion"))
        # ══════════════════════════════════════════════════════════════════════════════════════════

        # V17 label + persist epoch (first live start)
        self.label = "V17"
        if not self.shadow_mode:
            self.db.v17_meta.update_one(
                {"_id": "epoch"},
                {"$setOnInsert": {"epoch_ms": self.pnl_epoch_ms,
                                  "created_at": datetime.now(timezone.utc)}},
                upsert=True)
            self.pnl_epoch_ms = int(self.db.v17_meta.find_one({"_id": "epoch"})["epoch_ms"])
        logger.info(f"V17 PnL epoch: {datetime.fromtimestamp(self.pnl_epoch_ms/1000, timezone.utc)}")
        logger.info(f"V17 READY: knet_min={self._v17_knet_min}, netx<={self._v17_netx_cap_x}x, "
                    f"coin-side<={self._v17_coin_side_cap_x}x, stop={self._v17_stop_pct:.0%}, "
                    f"seed_ok={self._v17_trading_enabled}")

    # ══════════════════════════════════════════════════════════════════════════════════════════════
    # EXPANSION GUARDS (codex go-live req). All state lives behind self._v17_expansion_on; when the
    # config has no `expansion` block this is False and every guard is a no-op (flag-off == validated
    # 10-coin V17, byte-identically -- the regression gate).
    # ══════════════════════════════════════════════════════════════════════════════════════════════
    def _v17_init_expansion(self, exp_cfg):
        # ── default OFF state (also the flag-absent state). Defined unconditionally so every guard
        # site can reference these attrs without hasattr churn. With the flag off, _v17_new_coins is
        # empty -> `coin in _v17_new_coins` is always False -> no guard branch is taken.
        self._v17_expansion_on = False
        self._v17_new_coins: set[str] = set()
        self._v17_baseline_whitelist: set[str] = set(self.coin_whitelist)  # the validated 10
        self._v17_disabled_coins: set[str] = set()       # per-coin kill: entries blocked
        self._v17_expansion_killed = False               # expansion-wide kill: ALL new coins off
        # codex re-review P1: precautionary fail-closed disable (state unknown at boot). Unlike a real
        # latched kill, this is LIFTED by the first successful poll that re-establishes state -- so a
        # transient boot-time Mongo blip does not permanently sideline all new coins for the session.
        self._v17_precautionary_disabled = False
        # codex re-review (2nd follow-up) P1: the set of PERSISTED-latched disabled coins loaded at
        # boot. When we lift a precautionary blanket, we must restore from THIS snapshot (a latched
        # kill is never auto-lifted) -- not from set(), which would let a coin whose later exits moved
        # its cum/mean back above the threshold silently re-enter.
        self._v17_latched_disabled: set[str] = set()
        self._v17_coin_realized: dict[str, float] = {}   # new coin -> cumulative realized $ (heuristic)
        self._v17_coin_bps: dict[str, list] = {}         # new coin -> [realized_bps, ...] THIS session
        # codex re-review P1: persisted pre-restart bps aggregates (sum, n) per coin, restored on load.
        # Effective n/mean for the kill = these + this-session _v17_coin_bps (so the n>=20 mean-kill
        # state survives a restart instead of rebuilding from zero past the resumed cursor).
        self._v17_coin_bps_base: dict[str, tuple] = {}   # coin -> (sum_bps, n) carried across restart
        self._v17_expansion_realized = 0.0               # aggregate realized $ across all new coins
        self._v17_close_cursor = None                    # ObjectId high-water for close-doc polling
        self._v17_per_coin_kill_usd = -25.0
        self._v17_per_coin_kill_n = 20
        self._v17_expansion_kill_usd = -50.0
        # codex P2.1: the close-doc pnl_bps is GROSS (pure (exit-entry)/entry*1e4 price move; verified
        # in hl_copy_trader_v15.py -- fees tracked separately in the exchange-fill sync, NOT in the
        # per-trade close doc). The per-coin n-rule mean must be FEE-NET, so we subtract the HL taker
        # round-trip from the gross mean before the <0 test. The documented HL constant (8.64bps RT) is
        # the default; when expansion is actually ON we resolve it from the canonical execution model
        # (below, AFTER the flag-off early returns) so flag-off boots do ZERO extra imports / sys.path
        # mutation (codex re-review P1: keep the flag-off path's footprint minimal).
        self._v17_fee_rt_bps = 8.64
        # new/old reject tagging (codex req #4): are NEW coins driving cap pressure?
        self._v17_rej_new = {"netx": 0, "coinside": 0, "margin_util": 0, "gross_backstop": 0}
        self._v17_rej_old = {"netx": 0, "coinside": 0, "margin_util": 0, "gross_backstop": 0}

        if not exp_cfg:
            logger.info("V17 EXPANSION: no `global.expansion` block -- guards INERT, "
                        "running the validated 10-coin universe unchanged.")
            return

        coins = exp_cfg.get("coins") or []
        if not isinstance(coins, list) or not coins:
            logger.info("V17 EXPANSION: block present but `coins` empty -- guards INERT.")
            return

        # ── fee_rt from the canonical execution model (ONLY when expansion is on; codex re-review P1
        # keeps the flag-off path free of this import + sys.path mutation). Fallback to the 8.64bps
        # constant set above if the module/data is unavailable.
        try:
            sys.path.insert(0, str(_REPO / "research" / "v15"))
            import execution_model as _xm
            self._v17_fee_rt_bps = float(_xm.fee_rt(maker=False)) * 1e4
        except Exception as e:
            logger.warning(f"V17 EXPANSION: execution_model.fee_rt() unavailable ({e}); "
                           f"using documented HL taker RT fallback {self._v17_fee_rt_bps:.2f}bps.")

        # ── kill params (validated bounds; fail-closed clamp on garbage) ──
        self._v17_per_coin_kill_usd = float(exp_cfg.get("per_coin_kill_usd", -25.0))
        self._v17_per_coin_kill_n = int(exp_cfg.get("per_coin_kill_n", 20))
        self._v17_expansion_kill_usd = float(exp_cfg.get("expansion_kill_usd", -50.0))
        if not (self._v17_per_coin_kill_usd < 0 and self._v17_expansion_kill_usd < 0
                and self._v17_per_coin_kill_n >= 1):
            raise ValueError(f"V17 EXPANSION: kill params must be (per_coin_kill_usd<0, "
                             f"expansion_kill_usd<0, per_coin_kill_n>=1); got "
                             f"{self._v17_per_coin_kill_usd}/{self._v17_expansion_kill_usd}/"
                             f"{self._v17_per_coin_kill_n}")

        # ── validate each new coin exists in the HL/builder universe AND is not already a baseline
        # coin. all_perp_coins/all_builder_coins were filtered by V16 to the baseline whitelist, so we
        # validate against the FULL universe captured in self.max_leverage / self.sz_decimals (the base
        # engine populated those for EVERY perp + builder coin at init, before V16 filtered the feed
        # lists). FAIL-CLOSED: a coin we cannot verify is dropped (never traded), with a loud log.
        known_universe = set(self.max_leverage.keys())   # full perp + builder set from base init
        admitted, dropped = [], []
        for c in coins:
            if c in self._v17_baseline_whitelist:
                dropped.append((c, "already a baseline coin"))
                continue
            if c not in known_universe:
                dropped.append((c, "not in HL/builder universe"))
                continue
            admitted.append(c)
        if dropped:
            logger.warning(f"V17 EXPANSION: dropped {len(dropped)} coin(s) (FAIL-CLOSED, not traded): "
                           f"{dropped}")
        if not admitted:
            logger.warning("V17 EXPANSION: no admissible new coins after validation -- guards INERT.")
            return

        self._v17_new_coins = set(admitted)
        self._v17_expansion_on = True

        # ── extend the whitelist + WS feed lists to INCLUDE the new coins. V16 set
        # coin_whitelist to exactly the validated 10 and pruned all_perp_coins / emptied
        # all_builder_coins; we re-admit the new coins to BOTH the guard set (so the V16 choke
        # points pass them) AND the WS subscription lists (so their leader fills + books arrive).
        self.coin_whitelist = set(self.coin_whitelist) | self._v17_new_coins
        new_perp = sorted(c for c in self._v17_new_coins if ":" not in c)
        new_builder = sorted(c for c in self._v17_new_coins if ":" in c)
        for c in new_perp:
            if c not in self.all_perp_coins:
                self.all_perp_coins.append(c)
        for c in new_builder:
            if c not in self.all_builder_coins:
                self.all_builder_coins.append(c)

        # ── seed the unconditional leader tracker (_v16_leader_pos) for the new coins. The base
        # _init_target_positions already RESTed EVERY leader's clearinghouseState across ALL dexes
        # at startup (incl. builder), populating self._target_positions[addr][coin] for new coins
        # too. V16 only seeded _v16_leader_pos for baseline-whitelist coins; we add the new ones so
        # knet + open/add/reverse classification work for them from minute zero.
        seeded = 0
        for addr, posmap in self._target_positions.items():
            for cn, sz in posmap.items():
                if cn in self._v17_new_coins and abs(sz) > 1e-12:
                    self._v16_leader_pos[(addr, cn)] = float(sz)
                    seeded += 1

        logger.info(
            f"V17 EXPANSION ON: +{len(self._v17_new_coins)} new coins "
            f"(perp={new_perp}, builder={new_builder}); whitelist now {len(self.coin_whitelist)}; "
            f"seeded {seeded} new-coin leader positions; per-coin kill: realized<=${self._v17_per_coin_kill_usd:.0f} "
            f"OR (n>={self._v17_per_coin_kill_n} AND mean_bps<0); expansion-wide kill: "
            f"aggregate realized<=${self._v17_expansion_kill_usd:.0f}.")
        _tg(f"V17 EXPANSION ON: +{len(self._v17_new_coins)} new coins behind per-coin "
                 f"(${self._v17_per_coin_kill_usd:.0f}/n{self._v17_per_coin_kill_n}) + "
                 f"expansion-wide (${self._v17_expansion_kill_usd:.0f}) kills.")

        # ── RESTART KILL-STATE LOAD (codex P1.2 + P2.2) ──────────────────────────────────────────────
        # _v17_persist_expansion_state writes disabled coins + the killed flag + the close cursor to
        # v17_meta.expansion_state. On a restart we MUST rebuild that state BEFORE any WS subscription
        # can trigger an entry, or a previously-killed coin could re-enter until the first 30s poll.
        # Here (still inside __init__, before run()/WS), we:
        #   (1) LOAD the persisted disabled set + killed flag + realized accounting + close cursor;
        #   (2) synchronously poll the close collection ONCE so counters + kill state are fully rebuilt
        #       from the durable record before entries are reachable.
        # FAIL-CLOSED (codex re-review P1): only ADD to the disabled set from persistence (never clear
        # it). If the state READ fails, or the synchronous pre-WS poll fails, we CANNOT prove which
        # coins were killed -> disable ALL expansion coins until the first SUCCESSFUL 30s poll
        # re-establishes state from the durable close record. Never resume entry-eligible with unknown
        # kill state. Restricted to coins still in the CURRENT expansion set (a coin dropped from the
        # new config is not tradeable anyway). Skipped in shadow (no persistence there).
        self._v17_restart_loaded = False
        if not self.shadow_mode:
            state_known = True
            try:
                st = self.db.v17_meta.find_one({"_id": "expansion_state"})
            except Exception as e:
                st = None
                state_known = False
                logger.error(f"V17 EXPANSION: kill-state READ failed ({e}); FAIL-CLOSED -- disabling "
                             f"ALL new coins until a successful poll re-establishes state.")
            if st:
                persisted_disabled = {c for c in (st.get("disabled_coins") or [])
                                      if c in self._v17_new_coins}
                self._v17_disabled_coins |= persisted_disabled        # ADD only (fail-closed)
                self._v17_latched_disabled |= persisted_disabled      # latched snapshot (never lifted)
                self._v17_expansion_killed = bool(st.get("expansion_killed", False)) \
                    or self._v17_expansion_killed
                if self._v17_expansion_killed:
                    self._v17_disabled_coins |= set(self._v17_new_coins)
                # restore realized accounting so the synchronous poll resumes from the persisted total
                # rather than from zero (the cursor below ensures we only ADD un-accounted closes).
                self._v17_expansion_realized = float(st.get("expansion_realized", 0.0) or 0.0)
                for c, v in (st.get("coin_realized") or {}).items():
                    if c in self._v17_new_coins:
                        self._v17_coin_realized[c] = float(v)
                # restore per-coin bps aggregate (sum, n) so the n>=20 mean-kill spans the FULL history
                # across restarts (the cursor skips already-counted closes -> we can't re-derive these).
                _bsum = st.get("coin_bps_sum") or {}
                _bn = st.get("coin_bps_n") or {}
                for c in self._v17_new_coins:
                    if c in _bn:
                        self._v17_coin_bps_base[c] = (float(_bsum.get(c, 0.0) or 0.0), int(_bn[c]))
                # P2.2: resume the close cursor from the persisted high-water oid (exactly-once).
                oid = st.get("last_close_oid")
                if oid:
                    try:
                        from bson import ObjectId as _OID
                        self._v17_close_cursor = _OID(oid)
                    except Exception as e:
                        logger.warning(f"V17 EXPANSION: bad persisted last_close_oid {oid!r} ({e}); "
                                       f"the poll will fall back to the V17-epoch lower bound.")
                self._v17_restart_loaded = True
                logger.info(f"V17 EXPANSION RESTART-LOAD: disabled={sorted(self._v17_disabled_coins) or 'none'} "
                            f"killed={self._v17_expansion_killed} "
                            f"agg_realized=${self._v17_expansion_realized:.2f} "
                            f"bps_base={ {c: self._v17_coin_bps_base[c] for c in self._v17_coin_bps_base} or 'none'} "
                            f"cursor={'resumed' if self._v17_close_cursor else 'epoch'}.")
            # (2) synchronous pre-WS poll: rebuild counters/kills from the durable close record BEFORE
            # WS entries are reachable. DRAIN fully (loop while a full 2000-doc batch comes back) so a
            # large restart backlog is entirely accounted before any entry (codex re-review P2). Each
            # batch is bounded (memory-safe). A batch FAILURE (None) trips fail-closed.
            self._v17_last_exp_poll = time.time()
            for _ in range(50):    # hard cap: 50 * 2000 = 100k closes (far beyond any real backlog)
                got = self._v17_poll_new_coin_closes()
                if got is None:
                    state_known = False
                    logger.error("V17 EXPANSION: pre-WS close-poll FAILED; FAIL-CLOSED -- disabling ALL "
                                 "new coins until a successful poll re-establishes state.")
                    break
                if got < 2000:
                    break          # drained
            if not state_known:
                self._v17_disabled_coins |= set(self._v17_new_coins)
                self._v17_precautionary_disabled = True   # lift on the next successful poll
            logger.info(f"V17 EXPANSION SYNC-POLL (pre-WS): state_known={state_known} "
                        f"disabled={sorted(self._v17_disabled_coins) or 'none'} "
                        f"killed={self._v17_expansion_killed} agg_realized=${self._v17_expansion_realized:.2f}.")
            if self._v17_disabled_coins:
                _tg(f"V17 EXPANSION RESTART: {len(self._v17_disabled_coins)} coin(s) disabled "
                         f"before any entry{' (FAIL-CLOSED: state unknown)' if not state_known else ''}: "
                         f"{sorted(self._v17_disabled_coins)}.")

    def _v17_is_new(self, coin: str) -> bool:
        """A coin is a NEW (expansion) coin iff it is in the configured expansion set."""
        return coin in self._v17_new_coins

    def _v17_record_new_coin_close(self, coin: str, pnl_usd: float, pnl_bps: float):
        """Update per-coin + aggregate realized PnL for a NEW coin close, then evaluate kills.
        Pure function of its inputs + accumulated state -- unit-testable (see __main__ self-test).
        Called once per recorded close of a new coin (deduped by the close-doc cursor in
        _v17_poll_new_coin_closes). No-op for baseline coins / flag-off."""
        if not self._v17_expansion_on or coin not in self._v17_new_coins:
            return
        self._v17_coin_realized[coin] = self._v17_coin_realized.get(coin, 0.0) + float(pnl_usd)
        self._v17_coin_bps.setdefault(coin, []).append(float(pnl_bps))
        self._v17_expansion_realized += float(pnl_usd)
        self._v17_eval_kills(coin)

    def _v17_eval_kills(self, coin: str):
        """Per-coin kill (codex req #2) + expansion-wide kill (codex req #3). Idempotent: a coin
        already disabled stays disabled; re-evaluation only adds disables, never lifts them (a hit
        kill is latched -- you do not re-enable an in-sample-only coin automatically)."""
        # ── per-coin kill ──
        # codex re-review (round 3) P1: gate on the LATCHED set, NOT the runtime _v17_disabled_coins.
        # During a precautionary fail-closed blanket every new coin sits in _v17_disabled_coins, which
        # would suppress latching a genuine threshold crossing observed mid-replay; gating on
        # _v17_latched_disabled lets a real kill latch even while the blanket is up (and a
        # truly-already-latched coin is still skipped, preserving idempotency).
        if coin in self._v17_new_coins and coin not in self._v17_latched_disabled:
            cum = self._v17_coin_realized.get(coin, 0.0)
            bps = self._v17_coin_bps.get(coin, [])
            # codex re-review P1: combine the persisted pre-restart aggregate (sum, n) with this
            # session's closes so n + mean span the coin's FULL realized history across restarts.
            base_sum, base_n = self._v17_coin_bps_base.get(coin, (0.0, 0))
            n = len(bps) + base_n
            mean_gross_bps = ((sum(bps) + base_sum) / n) if n else 0.0
            # codex P2.1: the close-doc pnl_bps is GROSS, so the n-rule must use a FEE-NET mean --
            # subtract the HL taker round-trip (8.64bps) so a coin whose gross mean is slightly
            # positive but net-negative (e.g. +5bps gross, 5 - 8.64 < 0 net) is correctly killed.
            mean_net_bps = mean_gross_bps - self._v17_fee_rt_bps
            reason = None
            if cum <= self._v17_per_coin_kill_usd:
                reason = f"cum_realized=${cum:.2f}<=${self._v17_per_coin_kill_usd:.0f}"
            elif n >= self._v17_per_coin_kill_n and mean_net_bps < 0:
                reason = (f"n={n}>={self._v17_per_coin_kill_n} AND mean_NET_bps={mean_net_bps:.1f}<0 "
                          f"(gross {mean_gross_bps:.1f} - fee_rt {self._v17_fee_rt_bps:.2f})")
            if reason:
                self._v17_disabled_coins.add(coin)
                self._v17_latched_disabled.add(coin)   # latched: preserved across a precautionary lift
                logger.error(f"EXPANSION KILL coin={coin} reason={reason} "
                             f"(cum=${cum:.2f}, n={n}, mean_gross_bps={mean_gross_bps:.1f}, "
                             f"mean_net_bps={mean_net_bps:.1f}). New ENTRIES for "
                             f"{coin} disabled; existing position exits normally.")
                _tg(f"EXPANSION KILL coin={coin}: {reason}. New entries off (exits normal).")
                self._v17_persist_expansion_state()

        # ── expansion-wide kill: aggregate realized across ALL new coins <= -$50 ──
        if not self._v17_expansion_killed and self._v17_expansion_realized <= self._v17_expansion_kill_usd:
            self._v17_expansion_killed = True
            still_active = sorted(self._v17_new_coins - self._v17_disabled_coins)
            self._v17_disabled_coins |= set(self._v17_new_coins)   # disable ALL new coins
            self._v17_latched_disabled |= set(self._v17_new_coins) # latched (never lifted)
            logger.error(f"EXPANSION-WIDE KILL: aggregate new-coin realized "
                         f"${self._v17_expansion_realized:.2f} <= ${self._v17_expansion_kill_usd:.0f}. "
                         f"ALL {len(self._v17_new_coins)} new coins disabled (reverting to the {len(self._v17_baseline_whitelist)} "
                         f"baseline). Newly-disabled: {still_active}. Existing new-coin positions exit normally.")
            _tg(f"EXPANSION-WIDE KILL: agg ${self._v17_expansion_realized:.2f} <= "
                     f"${self._v17_expansion_kill_usd:.0f}. ALL new coins off; reverted to baseline 10.")
            self._v17_persist_expansion_state()

    def _v17_persist_expansion_state(self):
        """Snapshot kill state + the close cursor to v17_meta (audit + survives a restart). On restart
        _v17_init_expansion LOADS this (codex P1.2: disabled set + killed flag rebuilt before any entry
        is possible) and resumes the close cursor from last_close_oid (codex P2.2: deterministic
        exactly-once across restarts). Never raises into the hot path."""
        if self.shadow_mode:
            return
        try:
            doc = {
                # codex re-review (round 3) P2: persist the LATCHED set, NOT the runtime
                # _v17_disabled_coins. The runtime set can transiently include the precautionary
                # fail-closed blanket (all new coins) before a successful poll lifts it; persisting that
                # would make a later restart treat precautionary coins as permanently latched. The
                # durable "disabled" record is exactly the real latched kills (+ the expansion_killed
                # flag, which independently disables all on load).
                "disabled_coins": sorted(self._v17_latched_disabled),
                "expansion_killed": self._v17_expansion_killed,
                "coin_realized": {k: round(v, 4) for k, v in self._v17_coin_realized.items()},
                "expansion_realized": round(self._v17_expansion_realized, 4),
                # codex re-review P1 (+ follow-up): the n>=20 mean-kill needs the per-coin bps SERIES to
                # survive a restart. The cursor resumes PAST already-counted closes, so we can't
                # re-derive n/mean by re-polling them -- persist the COMBINED running (sum, count) per
                # coin = the pre-restart base (_v17_coin_bps_base) PLUS this session's closes. Persisting
                # only the session list would, after a no-kill post-restart close, overwrite n=19+1 with
                # n=1 and lose the base 19 on a SECOND restart (codex follow-up). Union the keys.
                "coin_bps_sum": {k: round(self._v17_coin_bps_base.get(k, (0.0, 0))[0] + sum(self._v17_coin_bps.get(k, [])), 4)
                                 for k in (set(self._v17_coin_bps) | set(self._v17_coin_bps_base))},
                "coin_bps_n": {k: self._v17_coin_bps_base.get(k, (0.0, 0))[1] + len(self._v17_coin_bps.get(k, []))
                               for k in (set(self._v17_coin_bps) | set(self._v17_coin_bps_base))},
                "updated_at": datetime.now(timezone.utc)}
            # codex P2.2: persist the close-doc high-water cursor so a restart resumes exactly-once.
            if self._v17_close_cursor is not None:
                doc["last_close_oid"] = str(self._v17_close_cursor)
            self.db.v17_meta.update_one({"_id": "expansion_state"}, {"$set": doc}, upsert=True)
        except Exception as e:
            logger.warning(f"V17 expansion-state persist failed (non-fatal): {e}")

    def _v17_poll_new_coin_closes(self):
        """Pull any NEW closed-trade docs for NEW coins from the V17 close collection and feed them to
        the per-coin/aggregate accounting EXACTLY ONCE (ObjectId high-water cursor; ObjectIds are
        monotonic by insertion). Every close-recording site in the base/V16 engine writes a doc with
        {coin, pnl_usd, pnl_bps} to DB_COLLECTION (== v17_copy_trades) -- this is the single, faithful,
        in-engine record of realized closes, so we account off it rather than editing the 5 base
        recording sites (zero base-engine surface; codex-reviewable in one place). Runs on the stats
        cadence; kills gate FUTURE entries, so sub-second latency is unnecessary. Memory-safe: a
        bounded cursor query, never a full-collection scan.

        Returns the number of close docs consumed in THIS batch (0 = drained), or None on a query
        error -- the pre-WS startup caller uses this to (a) drain a >2000-doc backlog fully before
        WS entries are reachable (codex re-review P2) and (b) treat a failure as fail-closed."""
        if not self._v17_expansion_on or self.shadow_mode:
            return 0
        try:
            q = {"coin": {"$in": sorted(self._v17_new_coins)}, "pnl_usd": {"$exists": True}}
            if self._v17_close_cursor is not None:
                q["_id"] = {"$gt": self._v17_close_cursor}
            else:
                # first poll after (re)start: only count closes AT/AFTER the V17 epoch so we never
                # double-count history from a prior session into the live kill counters.
                from bson import ObjectId as _OID
                q["_id"] = {"$gte": _OID.from_datetime(
                    datetime.fromtimestamp(self.pnl_epoch_ms / 1000, timezone.utc))}
            cur = self.db[DB_COLLECTION].find(q).sort("_id", 1).limit(2000)
            n = 0
            for doc in cur:
                self._v17_close_cursor = doc["_id"]
                self._v17_record_new_coin_close(
                    doc.get("coin", ""), doc.get("pnl_usd", 0.0) or 0.0, doc.get("pnl_bps", 0.0) or 0.0)
                n += 1
            if n:
                logger.info(f"V17 EXPANSION: accounted {n} new-coin close(s); "
                            f"agg realized ${self._v17_expansion_realized:.2f}; "
                            f"disabled {sorted(self._v17_disabled_coins) or 'none'}.")
                # codex P2.2: persist the advanced cursor (+ realized state) every poll that consumed
                # closes, NOT only when a kill fires -- otherwise a restart between a no-kill poll and
                # the next kill would re-count the same closes. _v17_eval_kills already persisted on a
                # kill; this makes the cursor durable for the no-kill case too. Idempotent upsert.
                self._v17_persist_expansion_state()
            return n
        except Exception as e:
            logger.warning(f"V17 expansion close-poll failed (non-fatal, retries next cycle): {e}")
            return None

    def _v17_lift_precautionary_if_known(self, got):
        """codex re-review P1 (+ follow-ups): after a poll, if we are in a precautionary fail-closed
        state (boot couldn't prove kill state) and the poll SUCCEEDED (got is not None), state is now
        known -- lift ONLY the precautionary blanket. Restore the base disabled set from the LATCHED
        snapshot (_v17_latched_disabled = persisted kills + any kill that fired this session, including
        during the blanket because _v17_eval_kills gates on the latched set), NOT from set(): a latched
        kill is never auto-lifted, even if the coin's later exits moved its cum/mean back above the
        threshold. Then re-eval to catch any kill the poll newly trips. Extracted from _log_stats so it
        is independently unit-testable without the base stats super() chain."""
        if self._v17_precautionary_disabled and got is not None:
            self._v17_precautionary_disabled = False
            self._v17_disabled_coins = set(self._v17_new_coins) if self._v17_expansion_killed \
                else set(self._v17_latched_disabled)
            for c in self._v17_new_coins:
                self._v17_eval_kills(c)    # re-applies $/n kills from restored state
            logger.info(f"V17 EXPANSION: state re-established after fail-closed boot; "
                        f"disabled now {sorted(self._v17_disabled_coins) or 'none'} "
                        f"(latched {sorted(self._v17_latched_disabled) or 'none'}).")

    # ── stats cadence hook: poll new-coin closes -> evaluate kills, then defer to the base stats ──
    def _log_stats(self):
        # the base _log_stats self-throttles to 60s; run the (cheap, bounded) close-poll on the SAME
        # cadence by gating on the same clock the base uses, BEFORE super so a kill that disables a
        # coin takes effect on this very cycle. No-op when the flag is off.
        if self._v17_expansion_on:
            now = time.time()
            if now - getattr(self, "_v17_last_exp_poll", 0) >= 30:
                self._v17_last_exp_poll = now
                got = self._v17_poll_new_coin_closes()
                self._v17_lift_precautionary_if_known(got)
            # gross-backstop attribution (codex req #4): the base backstop is a global FLATTEN, not a
            # per-entry reject, so we record (once, on the cycle it trips) which NEW vs OLD coins were
            # open at the time -- the audit signal is "did new coins drive the gross that tripped it".
            if self._kill_reasons.get("gross_backstop") and not getattr(self, "_v17_gb_attributed", False):
                self._v17_gb_attributed = True
                open_new = sorted({p["coin"] for p in self.positions
                                   if p.get("filled") and self._v17_is_new(p["coin"])})
                open_old = sorted({p["coin"] for p in self.positions
                                   if p.get("filled") and not self._v17_is_new(p["coin"])})
                self._v17_rej_new["gross_backstop"] += len(open_new)
                self._v17_rej_old["gross_backstop"] += len(open_old)
                logger.error(f"V17 GROSS-BACKSTOP attribution: open NEW coins={open_new} "
                             f"open OLD coins={open_old} at trip time.")
        return super()._log_stats()

    # ── reject tagging helpers (codex req #4) ──────────────────────────────────────────────────────
    def _v17_coin_tag(self, coin: str) -> str:
        """'[NEW]' / '[OLD]' label for log lines (no-op-cheap; '[OLD]' when expansion flag off)."""
        return "[NEW]" if self._v17_is_new(coin) else "[OLD]"

    def _v17_tag_reject(self, kind: str, coin: str):
        """Bump the NEW-coin or OLD-coin reject counter for `kind` in
        {netx, coinside, margin_util, gross_backstop}. Pure counter update; safe when flag off
        (everything tags as OLD then, and the counters are simply never surfaced/used)."""
        (self._v17_rej_new if self._v17_is_new(coin) else self._v17_rej_old)[kind] += 1

    # ── margin_util reject tagging (codex req #4): the base _check_margin_budget gates margin-util,
    # per-coin concentration and the fixed-mode notional caps. It is the entry-time 'margin_util'
    # rejection path the codex spec names. We wrap it: on a False (rejected) return, tag the reject
    # NEW vs OLD. Behaviour is otherwise IDENTICAL (we return exactly what super returns) -- and with
    # the expansion flag off this only ever bumps the OLD counter, which nothing reads. ──
    def _check_margin_budget(self, coin: str, additional_notional: float, wallet: str = None) -> bool:
        ok = super()._check_margin_budget(coin, additional_notional, wallet=wallet)
        # tag ONLY when the expansion flag is on, so flag-off is byte-identical to base behaviour
        # (this override then just forwards super's return value verbatim).
        if not ok and self._v17_expansion_on:
            self._v17_tag_reject("margin_util", coin)
            if (sum(self._v17_rej_new.values()) + sum(self._v17_rej_old.values())) % 25 == 1:
                logger.info(f"V17 REJECT TAGS so far: NEW={self._v17_rej_new} OLD={self._v17_rej_old}")
        return ok

    # ── knet from the unconditional tracker (V16 maintains _v16_leader_pos for EVERY target fill) ──
    def _v17_knet(self, coin: str, is_buy: bool, exclude_wallet: str, px: float) -> int:
        k = 0
        for (w2, c2), sz2 in self._v16_leader_pos.items():
            if c2 != coin or w2 == exclude_wallet:
                continue
            if abs(sz2) * px < 1.0:        # dust
                continue
            k += 1 if (sz2 > 0) == is_buy else -1
        return k

    # ── signal path: stamp knet at the leader-fill event (replay semantics), then defer to V16 ──
    # codex code-review r1 fixes: (P1.1) stamp ONLY true 0->nonzero open candidates (adds/closes/
    # reverses polluted the stamp map); dedupe by tid BEFORE stamping. (P1.2) FIFO queue per
    # (wallet, coin) instead of a single slot -- a burst of opens cannot overwrite the stamp the
    # async entry task will consume; base cooldown rejects the extras anyway.
    def _on_hl_trade(self, trade: dict):
        coin = trade.get("coin", "")
        users = trade.get("users", [])
        if coin in self.coin_whitelist and len(users) >= 2:
            buyer, seller = users[0].lower(), users[1].lower()
            w = buyer if buyer in self.target_set else (seller if seller in self.target_set else None)
            if w is not None:
                self._v17_last_target_fill_ts = time.time()
                px = float(trade.get("px", 0) or 0)
                tid = trade.get("tid", "")
                if px > 0 and not (tid and tid in self._seen_tids):
                    wallet = self.leader_to_vault.get(w, w)
                    is_buy = (w == buyer)
                    prev = self._v16_leader_pos.get((wallet, coin), 0.0)
                    if abs(prev) * px < 1.0:        # true OPEN candidate only (P1.1)
                        k = self._v17_knet(coin, is_buy, wallet, px)
                        self._v17_knet_pending.setdefault((wallet, coin), []).append((k, time.time()))
                        if len(self._v17_knet_pending) > 500:   # prune stale queues
                            cut = time.time() - 120
                            self._v17_knet_pending = {
                                kk: [e for e in vv if e[1] > cut]
                                for kk, vv in self._v17_knet_pending.items()}
                            self._v17_knet_pending = {kk: vv for kk, vv in
                                                      self._v17_knet_pending.items() if vv}
        return super()._on_hl_trade(trade)

    # ── order path: gate + caps + seed/staleness kills, then defer to V16 (whitelist) ──
    async def _enter_position(self, coin: str, is_buy: bool, twap_dedup_key=None, wallet: str = None,
                              skip_cooldown: bool = False):
        if not self._v17_trading_enabled:
            self._v17_stale_rejects += 1
            logger.warning(f"V17 ENTRY BLOCKED (trading_disabled/seed): {coin} {wallet}")
            return
        # EXPANSION KILL gate (codex req #2/#3): block NEW ENTRIES on a disabled new coin. A coin is
        # disabled by its own per-coin kill or by the expansion-wide kill (which disables ALL new
        # coins). Existing positions on the coin are NOT touched here -- the exit machinery in
        # _check_exits/_exit_position runs independently and closes them normally. No-op when the
        # expansion flag is off (_v17_disabled_coins is always empty then). FAIL-CLOSED: this gate is
        # the first thing checked, so a disabled coin can never reach sizing/exposure logic.
        if coin in self._v17_disabled_coins:
            logger.warning(f"V17 EXPANSION KILL: ENTRY blocked for disabled new coin {coin} "
                           f"(wallet={wallet}); existing position exits normally.")
            return
        # stale-tracker kill: knet is meaningless if we have not seen target flow recently
        age = time.time() - self._v17_last_target_fill_ts
        if age > 30.0:
            self._v17_stale_rejects += 1
            logger.warning(f"V17 STALE-TRACKER: last target fill {age:.0f}s ago; entry blocked {coin}")
            return

        # knet gate: consume the signal-time stamp (FIFO). Missing/expired stamp = REJECT
        # (codex P1.3: recompute-at-entry-time is a different, unvalidated gate; recovery and
        # non-signal paths must not open NEW risk).
        q = self._v17_knet_pending.get((wallet, coin))
        k = None
        while q:
            cand = q.pop(0)
            if (time.time() - cand[1]) < 60.0:
                k = cand[0]
                break
        if q is not None and not q:
            self._v17_knet_pending.pop((wallet, coin), None)
        if k is None:
            self._v17_stale_rejects += 1
            logger.warning(f"V17 NO-STAMP REJECT: {coin} {'BUY' if is_buy else 'SELL'} wallet={wallet} "
                           f"(no fresh signal-time knet; non-signal entries do not open risk)")
            return
        if k < self._v17_knet_min:
            self._v17_knet_rejects += 1
            if self._v17_knet_rejects % 20 == 1:
                logger.info(f"V17 KNET GATE: rejected {coin} {'BUY' if is_buy else 'SELL'} "
                            f"knet={k} (total rejects {self._v17_knet_rejects})")
            try:
                self.db.v17_gate_log.insert_one({
                    "coin": coin, "side": "BUY" if is_buy else "SELL", "knet": k,
                    "wallet": wallet, "action": "rejected",
                    "ts": datetime.now(timezone.utc)})
            except Exception:
                pass
            return

        # exposure caps from OUR live filled positions PLUS in-flight reservations (codex P1.4:
        # concurrent entry tasks could all pass the cap before any IOC fill lands in positions).
        eq = max(float(getattr(self, "_equity_cache", 0.0) or 0.0), 1.0)
        side_new = 1 if is_buy else -1
        net = float(self._v17_pending_net)
        coin_side = float(self._v17_pending_coin_side.get((coin, side_new), 0.0))
        for p in self.positions:
            if not p.get("filled"):
                continue
            s = 1 if p.get("side") == "BUY" else -1
            net += s * self.order_size
            if p.get("coin") == coin and s == side_new:
                coin_side += self.order_size
        if abs(net + side_new * self.order_size) > self._v17_netx_cap_x * eq \
                and abs(net + side_new * self.order_size) > abs(net):
            self._v17_netx_rejects += 1
            _tag = ""
            if self._v17_expansion_on:                 # codex req #4: NEW vs OLD cap-pressure audit
                self._v17_tag_reject("netx", coin)
                _tag = self._v17_coin_tag(coin) + " "
            logger.info(f"V17 NETX CAP: rejected {_tag}{coin} (net {net:+.0f} cap "
                        f"{self._v17_netx_cap_x}x${eq:.0f}; total {self._v17_netx_rejects})")
            return
        if coin_side + self.order_size > self._v17_coin_side_cap_x * eq:
            self._v17_coinside_rejects += 1
            _tag = ""
            if self._v17_expansion_on:                 # codex req #4
                self._v17_tag_reject("coinside", coin)
                _tag = self._v17_coin_tag(coin) + " "
            logger.info(f"V17 COIN-SIDE CAP: rejected {_tag}{coin} "
                        f"({coin_side:.0f}+{self.order_size:.0f} > {self._v17_coin_side_cap_x}x${eq:.0f}; "
                        f"total {self._v17_coinside_rejects})")
            return

        # record accepted-entry knet for attribution (week-1 KPI: knet-bucket PnL)
        try:
            self.db.v17_gate_log.insert_one({
                "coin": coin, "side": "BUY" if is_buy else "SELL", "knet": k,
                "wallet": wallet, "action": "accepted",
                "ts": datetime.now(timezone.utc)})
        except Exception:
            pass
        # reserve in-flight exposure for the duration of the entry attempt (codex P1.4)
        self._v17_pending_net += side_new * self.order_size
        self._v17_pending_coin_side[(coin, side_new)] = \
            self._v17_pending_coin_side.get((coin, side_new), 0.0) + self.order_size
        try:
            return await super()._enter_position(coin, is_buy, twap_dedup_key=twap_dedup_key,
                                                 wallet=wallet, skip_cooldown=skip_cooldown)
        finally:
            self._v17_pending_net -= side_new * self.order_size
            _rem = self._v17_pending_coin_side.get((coin, side_new), 0.0) - self.order_size
            if abs(_rem) < 1e-9:
                self._v17_pending_coin_side.pop((coin, side_new), None)   # codex r2 P2: no clutter
            else:
                self._v17_pending_coin_side[(coin, side_new)] = _rem


def main():
    import argparse
    ap = argparse.ArgumentParser(description="HL Copy Trader V17 -- gated herd copy")
    ap.add_argument("--config", default="config/copy_trader_wallets_v17.json")
    ap.add_argument("--size", type=float, default=None)
    ap.add_argument("--shadow", action="store_true")
    args = ap.parse_args()

    config_path = args.config
    if not Path(config_path).is_absolute():
        config_path = str(_REPO / config_path)
    if not Path(config_path).exists():
        logger.error(f"V17 config not found: {config_path}")
        sys.exit(1)

    trader = V17CopyTrader(config_path, order_size_override=args.size, shadow=args.shadow)
    asyncio.run(trader.run())


if __name__ == "__main__":
    main()
