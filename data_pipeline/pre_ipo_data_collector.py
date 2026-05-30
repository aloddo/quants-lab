"""
Pre-IPO / Synthetic Market Data Collector

Collects:
1. HL vntl: market prices + trades (pre-IPO perps)
2. HL @272 QQQ spot prices (for after-hours analysis)
3. News via RSS feeds (Anthropic, SpaceX, OpenAI mentions)
4. SEC EDGAR filings (13F, insider transactions for known holders)
5. FOMC/macro calendar (hardcoded dates + FRED when key available)

Run: python scripts/pre_ipo_data_collector.py
Stores: MongoDB quants_lab.pre_ipo_* collections
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import feedparser
import requests
import websockets
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [pre_ipo_collector] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# MongoDB
db = MongoClient("mongodb://localhost:27017").quants_lab

# HL API
HL_API = "https://api.hyperliquid.xyz/info"
HL_WS = "wss://api.hyperliquid.xyz/ws"

# Markets to track
VNTL_MARKETS = ["vntl:SPACEX", "vntl:ANTHROPIC", "vntl:OPENAI"]
SPOT_MARKETS = ["@272"]  # QQQ
PERP_MARKETS = ["PAXG"]
ALL_MARKETS = VNTL_MARKETS + SPOT_MARKETS + PERP_MARKETS

# RSS Feeds for news detection
RSS_FEEDS = {
    "techcrunch": "https://techcrunch.com/feed/",
    "theverge_ai": "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml",
    "reuters_tech": "https://www.reutersagency.com/feed/?best-topics=tech",
    "hackernews": "https://hnrss.org/newest?q=anthropic+OR+spacex+OR+openai",
    "google_news_anthropic": "https://news.google.com/rss/search?q=Anthropic+funding+OR+valuation&hl=en",
    "google_news_spacex": "https://news.google.com/rss/search?q=SpaceX+funding+OR+valuation+OR+launch&hl=en",
    "google_news_openai": "https://news.google.com/rss/search?q=OpenAI+funding+OR+valuation&hl=en",
}

# SEC EDGAR - companies whose investors file 13F
# SpaceX holders: Fidelity, T. Rowe Price, Valor Equity
# We track their CIKs for 13F amendments
EDGAR_CIKS = {
    "fidelity": "0000315066",
    "t_rowe_price": "0000080255",
    "google_ventures": "0001571801",
}
EDGAR_BASE = "https://efts.sec.gov/LATEST/search-index?q="

# Known event calendar (2026)
MACRO_CALENDAR = [
    # FOMC meetings
    {"date": "2026-05-14", "event": "FOMC Minutes Release", "impact": "QQQ/GLD"},
    {"date": "2026-06-11", "event": "FOMC Rate Decision", "impact": "QQQ/GLD"},
    {"date": "2026-06-18", "event": "FOMC Minutes", "impact": "QQQ/GLD"},
    {"date": "2026-07-29", "event": "FOMC Rate Decision", "impact": "QQQ/GLD"},
    # CPI
    {"date": "2026-05-13", "event": "CPI Release", "impact": "QQQ/GLD"},
    {"date": "2026-06-11", "event": "CPI Release", "impact": "QQQ/GLD"},
    # Known pre-IPO events (update as discovered)
    {"date": "2026-05-15", "event": "Anthropic Claude 4.5 rumored", "impact": "vntl:ANTHROPIC"},
]


class PreIPOCollector:
    """Collects price, news, and filing data for pre-IPO strategy research."""

    def __init__(self):
        self.prices_col = db["pre_ipo_prices"]
        self.trades_col = db["pre_ipo_trades"]
        self.news_col = db["pre_ipo_news"]
        self.filings_col = db["pre_ipo_filings"]
        self.calendar_col = db["pre_ipo_calendar"]

        # Ensure indexes
        self.prices_col.create_index([("coin", 1), ("ts", 1)])
        self.trades_col.create_index([("coin", 1), ("time", 1)])
        self.news_col.create_index([("published", 1)])
        self.news_col.create_index([("link", 1)], unique=True)

        # Seed calendar
        for event in MACRO_CALENDAR:
            self.calendar_col.update_one(
                {"date": event["date"], "event": event["event"]},
                {"$set": event},
                upsert=True,
            )

        self.running = True
        self._seen_news_links = set()
        # Pre-load seen links to avoid re-alerting
        for doc in self.news_col.find({}, {"link": 1}):
            self._seen_news_links.add(doc.get("link"))

    async def collect_prices_ws(self):
        """Stream L2 book snapshots for all markets via WS."""
        while self.running:
            try:
                async with websockets.connect(HL_WS, ping_interval=20) as ws:
                    # Subscribe to L2 books
                    for coin in ALL_MARKETS:
                        await ws.send(json.dumps({
                            "method": "subscribe",
                            "subscription": {"type": "l2Book", "coin": coin}
                        }))
                        await asyncio.sleep(0.1)

                    # Subscribe to trades
                    for coin in ALL_MARKETS:
                        await ws.send(json.dumps({
                            "method": "subscribe",
                            "subscription": {"type": "trades", "coin": coin}
                        }))
                        await asyncio.sleep(0.1)

                    logger.info(f"WS connected, tracking {len(ALL_MARKETS)} markets")

                    last_snapshot = {}  # coin -> last snapshot time

                    while self.running:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        data = json.loads(msg)
                        channel = data.get("channel")

                        if channel == "l2Book":
                            book = data.get("data", {})
                            coin = book.get("coin", "")
                            levels = book.get("levels", [[], []])
                            now = time.time()

                            # Snapshot every 60s per coin
                            if now - last_snapshot.get(coin, 0) >= 60:
                                bids = levels[0][:5] if levels[0] else []
                                asks = levels[1][:5] if levels[1] else []
                                best_bid = float(bids[0]["px"]) if bids else 0
                                best_ask = float(asks[0]["px"]) if asks else 0
                                mid = (best_bid + best_ask) / 2 if best_bid and best_ask else 0
                                spread_bps = ((best_ask - best_bid) / mid * 10000) if mid else 0
                                bid_depth = sum(float(b["px"]) * float(b["sz"]) for b in bids)
                                ask_depth = sum(float(a["px"]) * float(a["sz"]) for a in asks)

                                self.prices_col.insert_one({
                                    "coin": coin,
                                    "ts": datetime.now(timezone.utc),
                                    "mid": mid,
                                    "best_bid": best_bid,
                                    "best_ask": best_ask,
                                    "spread_bps": spread_bps,
                                    "bid_depth_usd": bid_depth,
                                    "ask_depth_usd": ask_depth,
                                })
                                last_snapshot[coin] = now

                        elif channel == "trades":
                            trades = data.get("data", [])
                            for t in trades:
                                self.trades_col.insert_one({
                                    "coin": t.get("coin"),
                                    "side": t.get("side"),
                                    "px": float(t.get("px", 0)),
                                    "sz": float(t.get("sz", 0)),
                                    "time": datetime.fromtimestamp(
                                        int(t.get("time", 0)) / 1000, tz=timezone.utc
                                    ),
                                    "users": t.get("users", []),
                                    "notional": float(t.get("px", 0)) * float(t.get("sz", 0)),
                                })

            except asyncio.TimeoutError:
                logger.warning("WS timeout, reconnecting...")
            except Exception as e:
                logger.error(f"WS error: {e}")
                await asyncio.sleep(5)

    def collect_news_rss(self):
        """Poll RSS feeds for pre-IPO relevant news."""
        keywords = [
            "anthropic", "spacex", "openai", "claude",
            "valuation", "funding round", "series",
            "pre-ipo", "secondary market", "tender offer",
        ]
        new_articles = []

        for feed_name, feed_url in RSS_FEEDS.items():
            try:
                feed = feedparser.parse(feed_url)
                for entry in feed.entries[:10]:
                    link = entry.get("link", "")
                    if link in self._seen_news_links:
                        continue

                    title = entry.get("title", "").lower()
                    summary = entry.get("summary", "").lower()
                    text = f"{title} {summary}"

                    # Check relevance
                    matched_keywords = [k for k in keywords if k in text]
                    if not matched_keywords:
                        continue

                    # Determine which market this affects
                    affected_markets = []
                    if any(k in text for k in ["anthropic", "claude"]):
                        affected_markets.append("vntl:ANTHROPIC")
                    if "spacex" in text or "starlink" in text:
                        affected_markets.append("vntl:SPACEX")
                    if "openai" in text or "chatgpt" in text:
                        affected_markets.append("vntl:OPENAI")
                    if any(k in text for k in ["fed", "fomc", "rate", "cpi", "inflation"]):
                        affected_markets.extend(["@272", "PAXG"])

                    published = entry.get("published_parsed")
                    pub_dt = datetime(*published[:6], tzinfo=timezone.utc) if published else datetime.now(timezone.utc)

                    doc = {
                        "source": feed_name,
                        "title": entry.get("title", ""),
                        "link": link,
                        "summary": entry.get("summary", "")[:500],
                        "published": pub_dt,
                        "collected_at": datetime.now(timezone.utc),
                        "keywords": matched_keywords,
                        "affected_markets": affected_markets,
                        "sentiment": None,  # TODO: LLM sentiment scoring
                    }

                    try:
                        self.news_col.insert_one(doc)
                        self._seen_news_links.add(link)
                        new_articles.append(doc)
                        logger.info(f"NEWS: [{feed_name}] {entry.get('title', '')[:80]} -> {affected_markets}")
                    except Exception:
                        pass  # Duplicate link

            except Exception as e:
                logger.debug(f"RSS feed {feed_name} error: {e}")

        return new_articles

    def collect_sec_edgar(self):
        """Check SEC EDGAR for new filings from known pre-IPO investors."""
        headers = {"User-Agent": "QuantsLab Research alberto@loddo.eu"}
        new_filings = []

        for name, cik in EDGAR_CIKS.items():
            try:
                url = f"https://data.sec.gov/submissions/CIK{cik}.json"
                resp = requests.get(url, headers=headers, timeout=10)
                if resp.status_code != 200:
                    continue

                data = resp.json()
                recent = data.get("filings", {}).get("recent", {})
                forms = recent.get("form", [])
                dates = recent.get("filingDate", [])
                accessions = recent.get("accessionNumber", [])

                # Look for recent 13F or 13F-HR filings (last 30 days)
                for i, form in enumerate(forms[:20]):
                    if form in ["13F-HR", "13F-HR/A", "SC 13G", "SC 13G/A", "SC 13D"]:
                        filing_date = dates[i] if i < len(dates) else ""
                        accession = accessions[i] if i < len(accessions) else ""

                        # Check if already stored
                        if self.filings_col.find_one({"accession": accession}):
                            continue

                        doc = {
                            "filer": name,
                            "cik": cik,
                            "form": form,
                            "filing_date": filing_date,
                            "accession": accession,
                            "collected_at": datetime.now(timezone.utc),
                            "url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={form}",
                        }
                        self.filings_col.insert_one(doc)
                        new_filings.append(doc)
                        logger.info(f"SEC FILING: {name} filed {form} on {filing_date}")

            except Exception as e:
                logger.debug(f"EDGAR error for {name}: {e}")

        return new_filings

    async def run(self):
        """Main loop: WS prices + periodic news/filings polling."""
        logger.info(f"Starting pre-IPO collector: {len(ALL_MARKETS)} markets, {len(RSS_FEEDS)} feeds")

        # Start WS price collection in background
        ws_task = asyncio.create_task(self.collect_prices_ws())

        # Poll news and filings every 5 minutes
        poll_interval = 300
        last_poll = 0

        try:
            while self.running:
                now = time.time()
                if now - last_poll >= poll_interval:
                    # Run news + filings collection
                    news = self.collect_news_rss()
                    filings = self.collect_sec_edgar()

                    # Stats
                    price_count = self.prices_col.count_documents({})
                    trade_count = self.trades_col.count_documents({})
                    news_count = self.news_col.count_documents({})

                    logger.info(
                        f"STATS: prices={price_count}, trades={trade_count}, "
                        f"news={news_count}, new_news={len(news)}, new_filings={len(filings)}"
                    )
                    last_poll = now

                await asyncio.sleep(10)

        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.running = False
            ws_task.cancel()


if __name__ == "__main__":
    collector = PreIPOCollector()
    asyncio.run(collector.run())
