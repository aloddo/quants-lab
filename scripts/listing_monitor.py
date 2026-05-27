#!/usr/bin/env python3
"""
CEX Listing Announcement Monitor.

Polls Bybit announcement API for new perp listings every 30 seconds.
Cross-references against HL, MEXC, and Raydium availability.
Sends Telegram alerts on new listings detected.

Usage:
    python scripts/listing_monitor.py              # Run once
    python scripts/listing_monitor.py --loop        # Poll every 30s
    python scripts/listing_monitor.py --backfill    # Load existing announcements
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [listing_monitor] %(levelname)s: %(message)s",
)
logger = logging.getLogger("listing_monitor")

BYBIT_API = "https://api.bybit.com"
HL_API = "https://api.hyperliquid.xyz"
MEXC_API = "https://api.mexc.com"
TG_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "-1003576397888")
DB_NAME = "quants_lab"
COLLECTION = "listing_monitor_events"
POLL_INTERVAL = 30  # seconds


class ListingMonitor:
    def __init__(self):
        self.client = MongoClient("mongodb://localhost:27017")
        self.db = self.client[DB_NAME]
        self.col = self.db[COLLECTION]
        self.col.create_index("announce_id", unique=True)

        # Cache exchange universes (refresh every 10 min)
        self._hl_coins = set()
        self._mexc_coins = set()
        self._exchange_cache_ts = 0
        # 2026-05-22: Jupiter (Solana) per-symbol lookup cache. symbol -> (best_match dict or None, ts)
        # Datapi search returns multiple results, we pick the highest-liquidity match.
        self._jup_cache = {}
        self._jup_min_liquidity_usd = 5000  # ignore dust pools below this

    def _refresh_exchange_universe(self):
        """Refresh the set of coins available on HL and MEXC."""
        now = time.time()
        if now - self._exchange_cache_ts < 600:
            return

        # HL perps
        try:
            r = requests.post(HL_API + "/info", json={"type": "meta"}, timeout=10)
            self._hl_coins = set(u["name"] for u in r.json().get("universe", []))
            logger.debug(f"HL coins refreshed: {len(self._hl_coins)}")
        except Exception as e:
            logger.warning(f"HL meta fetch failed: {e}")

        # Also check HL builder dexes
        for dex in ["xyz", "flx"]:
            try:
                r = requests.post(HL_API + "/info", json={"type": "meta", "dex": dex}, timeout=10)
                for u in r.json().get("universe", []):
                    self._hl_coins.add(u["name"])
            except Exception:
                pass

        # MEXC spot
        try:
            r = requests.get(MEXC_API + "/api/v3/exchangeInfo", timeout=15)
            self._mexc_coins = set(
                s["baseAsset"].upper()
                for s in r.json().get("symbols", [])
                if s["symbol"].endswith("USDT")
            )
            logger.debug(f"MEXC coins refreshed: {len(self._mexc_coins)}")
        except Exception as e:
            logger.warning(f"MEXC fetch failed: {e}")

        self._exchange_cache_ts = now

    def _jup_lookup(self, symbol: str) -> dict | None:
        """Query Jupiter datapi for symbol. Returns best-liquidity match dict or None.
        Caches per symbol for 1 hour. Filters out dust pools below _jup_min_liquidity_usd.
        2026-05-22: added so Bybit-only listings are checked for a Solana venue.
        """
        if not symbol:
            return None
        now = time.time()
        cached = self._jup_cache.get(symbol)
        if cached and now - cached[1] < 3600:
            return cached[0]
        result = None
        try:
            r = requests.get(
                "https://datapi.jup.ag/v1/assets/search",
                params={"query": symbol, "limit": 5},
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "application/json",
                    "Origin": "https://jup.ag",
                },
                timeout=5,
            )
            if r.status_code != 200:
                logger.debug(f"jupiter lookup {symbol}: HTTP {r.status_code}")
                self._jup_cache[symbol] = (None, now)
                return None
            for item in r.json():
                if not isinstance(item, dict):
                    continue
                # require exact symbol match (case-insensitive) to avoid SPCXCUP for SPCX
                if (item.get("symbol") or "").upper() != symbol.upper():
                    continue
                liq = item.get("liquidity")
                if liq is None or float(liq) < self._jup_min_liquidity_usd:
                    continue
                # pick the first (highest-liquidity) qualifying match
                result = {
                    "mint": item.get("id", ""),
                    "symbol": item.get("symbol", ""),
                    "liquidity": float(liq),
                    "mcap": float(item.get("mcap") or 0),
                    "usd_price": float(item.get("usdPrice") or 0),
                    "launchpad": item.get("launchpad"),
                }
                break
        except Exception as e:
            logger.debug(f"jupiter lookup {symbol} failed: {e}")
        self._jup_cache[symbol] = (result, now)
        return result

    def _extract_symbol(self, title: str) -> str:
        """Extract token symbol from announcement title.

        Examples:
            'New listing: SOXLUSDT Perpetual Contract' -> 'SOXL'
            'New listing: DRAMUSDT Perpetual Contract' -> 'DRAM'
        """
        # Pattern: find XXXUSDT in the title
        match = re.search(r"(\w+)USDT", title)
        if match:
            return match.group(1)
        return ""

    def fetch_announcements(self, pages: int = 1) -> list:
        """Fetch new_crypto announcements from Bybit."""
        all_items = []
        for page in range(1, pages + 1):
            try:
                r = requests.get(
                    f"{BYBIT_API}/v5/announcements/index",
                    params={"locale": "en-US", "type": "new_crypto", "limit": 50, "page": page},
                    timeout=15,
                )
                items = r.json().get("result", {}).get("list", [])
                if not items:
                    break
                all_items.extend(items)
                time.sleep(0.3)
            except Exception as e:
                logger.error(f"Bybit announcement fetch failed: {e}")
                break
        return all_items

    def process_announcement(self, item: dict) -> dict | None:
        """Process a single announcement. Returns event dict if new, None if already seen."""
        announce_id = item.get("url", "") or str(item.get("dateTimestamp", ""))
        title = item.get("title", "")
        publish_ts = int(item.get("dateTimestamp", 0) or item.get("publishTime", 0))

        # Only process perpetual listings
        if "Perpetual" not in title:
            return None

        # Check if already processed
        if self.col.find_one({"announce_id": announce_id}):
            return None

        symbol = self._extract_symbol(title)
        if not symbol:
            return None

        # Cross-reference against exchanges
        self._refresh_exchange_universe()
        on_hl = symbol in self._hl_coins
        on_mexc = symbol in self._mexc_coins
        # 2026-05-22: also check Jupiter (Solana spot)
        jup = self._jup_lookup(symbol)
        on_jupiter = jup is not None

        # Determine if it's a stock synthetic
        stock_indicators = ["ETF", "Index", "Equity"]
        is_stock = any(ind.lower() in title.lower() for ind in stock_indicators)
        # Also check common stock tickers
        stock_syms = {
            "SPY", "QQQ", "IWM", "EWY", "EWJ", "SOXL", "LLY", "ARM", "BABA",
            "AVGO", "MU", "NVDA", "MSFT", "AAPL", "AMZN", "TSLA", "META",
            "GOOG", "MSTR", "COIN", "BRK", "HOOD", "PLTR", "MRVL", "BZ",
        }
        if symbol in stock_syms:
            is_stock = True

        event = {
            "announce_id": announce_id,
            "title": title,
            "symbol": symbol,
            "pair": f"{symbol}USDT",
            "publish_ts": publish_ts,
            "publish_time": datetime.fromtimestamp(
                publish_ts / 1000, tz=timezone.utc
            ).isoformat() if publish_ts else None,
            "on_hl": on_hl,
            "on_mexc": on_mexc,
            "on_jupiter": on_jupiter,
            "jup_mint": jup["mint"] if jup else None,
            "jup_liquidity_usd": jup["liquidity"] if jup else None,
            "jup_mcap_usd": jup["mcap"] if jup else None,
            "is_stock": is_stock,
            "is_crypto": not is_stock,
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "alerted": False,
            "traded": False,
        }

        # Save to DB
        try:
            self.col.insert_one(event)
        except Exception:
            return None  # duplicate

        return event

    def send_alert(self, event: dict):
        """Send Telegram alert for a new listing."""
        if not TG_BOT_TOKEN:
            logger.warning("No TG_BOT_TOKEN, skipping alert")
            return

        venues = []
        if event["on_hl"]:
            venues.append("HL")
        if event["on_mexc"]:
            venues.append("MEXC")
        if event.get("on_jupiter"):
            liq = event.get("jup_liquidity_usd") or 0
            venues.append(f"JUPITER (liq ${liq:,.0f})")
        if not venues:
            venues.append("NO VENUE")

        emoji = "🚀" if event["is_crypto"] else "📊"
        hl_tag = " [HL TRADEABLE]" if event["on_hl"] else ""
        if event.get("on_jupiter") and not event["on_hl"]:
            hl_tag = " [SOLANA TRADEABLE]"

        text = (
            f"{emoji} NEW BYBIT PERP LISTING{hl_tag}\n"
            f"Token: {event['symbol']}\n"
            f"Pair: {event['pair']}\n"
            f"Available on: {', '.join(venues)}\n"
            f"Type: {'Crypto' if event['is_crypto'] else 'Stock/ETF'}\n"
            f"Announced: {event['publish_time']}\n"
            f"Title: {event['title'][:80]}"
        )

        try:
            requests.post(
                f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                json={"chat_id": TG_CHAT_ID, "text": text},
                timeout=10,
            )
            self.col.update_one(
                {"announce_id": event["announce_id"]},
                {"$set": {"alerted": True}},
            )
            logger.info(f"Alert sent: {event['symbol']} ({', '.join(venues)})")
        except Exception as e:
            logger.error(f"TG alert failed: {e}")

    def run_once(self):
        """Poll once and process new announcements."""
        items = self.fetch_announcements(pages=1)
        new_count = 0
        for item in items:
            event = self.process_announcement(item)
            if event:
                new_count += 1
                logger.info(
                    f"NEW: {event['symbol']} | HL={event['on_hl']} MEXC={event['on_mexc']} "
                    f"crypto={event['is_crypto']}"
                )
                self.send_alert(event)

        if new_count == 0:
            logger.debug("No new listings detected")
        return new_count

    def backfill(self, pages: int = 10):
        """Load existing announcements without alerting."""
        items = self.fetch_announcements(pages=pages)
        loaded = 0
        for item in items:
            event = self.process_announcement(item)
            if event:
                loaded += 1
        logger.info(f"Backfilled {loaded} announcements from {len(items)} total")

    def stats(self):
        """Print stats about stored events."""
        total = self.col.count_documents({})
        crypto = self.col.count_documents({"is_crypto": True})
        on_hl = self.col.count_documents({"on_hl": True, "is_crypto": True})
        on_mexc = self.col.count_documents({"on_mexc": True, "is_crypto": True})
        logger.info(
            f"Events: {total} total, {crypto} crypto, "
            f"{on_hl} on HL ({on_hl/crypto*100:.0f}%), "
            f"{on_mexc} on MEXC ({on_mexc/crypto*100:.0f}%)" if crypto > 0 else
            f"Events: {total} total, {crypto} crypto"
        )


def main():
    parser = argparse.ArgumentParser(description="CEX Listing Announcement Monitor")
    parser.add_argument("--loop", action="store_true", help="Poll every 30 seconds")
    parser.add_argument("--backfill", action="store_true", help="Load existing announcements")
    parser.add_argument("--stats", action="store_true", help="Print stats")
    parser.add_argument("--interval", type=int, default=30, help="Poll interval in seconds")
    args = parser.parse_args()

    monitor = ListingMonitor()

    if args.backfill:
        monitor.backfill(pages=10)
        monitor.stats()
        return

    if args.stats:
        monitor.stats()
        return

    if args.loop:
        logger.info(f"Starting listing monitor loop (interval={args.interval}s)")
        # Initial backfill to avoid alerting on old listings
        monitor.backfill(pages=3)
        monitor.stats()
        logger.info("Backfill complete. Now monitoring for NEW listings only.")

        while True:
            try:
                monitor.run_once()
            except Exception as e:
                logger.error(f"Loop error: {e}")
            time.sleep(args.interval)
    else:
        monitor.run_once()
        monitor.stats()


if __name__ == "__main__":
    main()
