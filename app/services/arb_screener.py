"""
Cross-exchange arbitrage screener — Binance SPOT ↔ Bybit PERP.

Monitors bid/ask spreads between exchanges via REST polling.
Logs opportunities to MongoDB. Feeds the arb executor.

Usage:
    python -m app.services.arb_screener          # screener only (paper)
    python -m app.services.arb_screener --live    # screener + execution
"""
import asyncio
import logging
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

import aiohttp
from pymongo import MongoClient

logger = logging.getLogger(__name__)

# ── Fee model ────────────────────────────────────────────────
# Binance spot: maker 0.1%, taker 0.1% (no BNB discount assumed)
# Bybit perp:   maker 0.02%, taker 0.055%
BINANCE_SPOT_TAKER = 0.001
BINANCE_SPOT_MAKER = 0.001
BYBIT_PERP_TAKER = 0.00055
BYBIT_PERP_MAKER = 0.0002

# Strategy: MAKER on Bybit (limit), TAKER on Binance (market) — fastest execution
# Realistic fee: Binance taker + Bybit maker = 0.12% = 12bp
FEE_OPTIMISTIC = BINANCE_SPOT_MAKER + BYBIT_PERP_MAKER   # 12bp (both maker — rare)
FEE_REALISTIC = BINANCE_SPOT_TAKER + BYBIT_PERP_MAKER     # 12bp
FEE_PESSIMISTIC = BINANCE_SPOT_TAKER + BYBIT_PERP_TAKER   # 15.5bp

# Minimum spread to consider (after realistic fees)
MIN_NET_SPREAD_BPS = 5.0  # 5bp minimum profit per trade


@dataclass
class ArbOpportunity:
    symbol: str               # e.g. "LINKUSDT"
    pair: str                 # e.g. "LINK-USDT"
    timestamp: float
    bn_bid: float
    bn_ask: float
    bb_bid: float
    bb_ask: float
    best_spread_bps: float
    net_spread_bps: float     # after realistic fees
    direction: str            # "BUY_BN_SELL_BB" or "BUY_BB_SELL_BN"
    bn_vol_24h: float = 0
    bb_vol_24h: float = 0
    profitable: bool = False


@dataclass
class ScreenerState:
    """Running state of the screener."""
    polls: int = 0
    opportunities_found: int = 0
    last_poll_time: float = 0
    last_poll_latency: float = 0
    active_opportunities: dict = field(default_factory=dict)  # symbol -> ArbOpportunity


class ArbScreener:
    """Cross-exchange spread monitor: Binance SPOT ↔ Bybit PERP."""

    def __init__(
        self,
        poll_interval: float = 2.0,
        min_net_spread_bps: float = MIN_NET_SPREAD_BPS,
        min_volume_24h: float = 500_000,
        mongo_uri: str | None = None,
        mongo_db: str | None = None,
    ):
        self.poll_interval = poll_interval
        self.min_net_spread_bps = min_net_spread_bps
        self.min_volume_24h = min_volume_24h
        self.fee = FEE_REALISTIC
        self.state = ScreenerState()

        # MongoDB
        uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
        db_name = mongo_db or os.getenv("MONGO_DATABASE", "quants_lab")
        self._mongo = MongoClient(uri)
        self._db = self._mongo[db_name]
        self._coll = self._db["arb_opportunities"]
        self._coll.create_index([("timestamp", -1)])
        self._coll.create_index([("symbol", 1), ("timestamp", -1)])

        # Ensure we have a stats collection for tracking
        self._stats_coll = self._db["arb_screener_stats"]

        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()

    async def _fetch_binance_spot(self) -> dict[str, dict]:
        """Fetch all Binance SPOT USDT book tickers."""
        await self._ensure_session()
        async with self._session.get("https://api.binance.com/api/v3/ticker/bookTicker") as resp:
            data = await resp.json()

        # Also get 24h volume
        async with self._session.get("https://api.binance.com/api/v3/ticker/24hr") as resp:
            vol_data = await resp.json()
        vol_map = {t["symbol"]: float(t["quoteVolume"]) for t in vol_data if t["symbol"].endswith("USDT")}

        tickers = {}
        for t in data:
            sym = t["symbol"]
            if not sym.endswith("USDT"):
                continue
            bid = float(t["bidPrice"])
            ask = float(t["askPrice"])
            if bid <= 0 or ask <= 0:
                continue
            tickers[sym] = {
                "bid": bid,
                "ask": ask,
                "vol": vol_map.get(sym, 0),
            }
        return tickers

    async def _fetch_bybit_perp(self) -> dict[str, dict]:
        """Fetch all Bybit linear USDT perp tickers."""
        await self._ensure_session()
        async with self._session.get(
            "https://api.bybit.com/v5/market/tickers?category=linear"
        ) as resp:
            data = await resp.json()

        tickers = {}
        for t in data["result"]["list"]:
            sym = t["symbol"]
            if not sym.endswith("USDT") or not t["bid1Price"] or not t["ask1Price"]:
                continue
            tickers[sym] = {
                "bid": float(t["bid1Price"]),
                "ask": float(t["ask1Price"]),
                "vol": float(t["turnover24h"]),
            }
        return tickers

    def _compute_spread(
        self, symbol: str, bn: dict, bb: dict
    ) -> ArbOpportunity:
        """Compute the best arb spread between Binance spot and Bybit perp."""
        # Direction 1: BUY on Binance (ask), SELL on Bybit (bid)
        s1 = (bb["bid"] - bn["ask"]) / bn["ask"]
        # Direction 2: BUY on Bybit (ask), SELL on Binance (bid)
        s2 = (bn["bid"] - bb["ask"]) / bb["ask"]

        if s1 >= s2:
            best_spread = s1
            direction = "BUY_BN_SELL_BB"
        else:
            best_spread = s2
            direction = "BUY_BB_SELL_BN"

        net = best_spread - self.fee
        pair = symbol.replace("USDT", "") + "-USDT"
        # Handle 1000-prefix pairs
        if pair.startswith("1000"):
            pair = "1000" + pair[4:]

        return ArbOpportunity(
            symbol=symbol,
            pair=pair,
            timestamp=time.time(),
            bn_bid=bn["bid"],
            bn_ask=bn["ask"],
            bb_bid=bb["bid"],
            bb_ask=bb["ask"],
            best_spread_bps=best_spread * 10000,
            net_spread_bps=net * 10000,
            direction=direction,
            bn_vol_24h=bn["vol"],
            bb_vol_24h=bb["vol"],
            profitable=net > 0,
        )

    async def poll_once(self) -> list[ArbOpportunity]:
        """Single poll: fetch both exchanges, compute spreads, return opportunities."""
        t0 = time.time()

        try:
            bn_tickers, bb_tickers = await asyncio.gather(
                self._fetch_binance_spot(),
                self._fetch_bybit_perp(),
            )
        except Exception as e:
            logger.error(f"Fetch error: {e}")
            return []

        common = set(bn_tickers.keys()) & set(bb_tickers.keys())
        latency = time.time() - t0

        opportunities = []
        for sym in common:
            bn = bn_tickers[sym]
            bb = bb_tickers[sym]
            min_vol = min(bn["vol"], bb["vol"])
            if min_vol < self.min_volume_24h:
                continue

            opp = self._compute_spread(sym, bn, bb)
            if opp.net_spread_bps >= self.min_net_spread_bps:
                opportunities.append(opp)

        # Sort by net spread
        opportunities.sort(key=lambda x: x.net_spread_bps, reverse=True)

        # Update state
        self.state.polls += 1
        self.state.last_poll_time = time.time()
        self.state.last_poll_latency = latency
        self.state.opportunities_found += len(opportunities)
        self.state.active_opportunities = {o.symbol: o for o in opportunities}

        return opportunities

    def _log_opportunities(self, opportunities: list[ArbOpportunity]):
        """Write opportunities to MongoDB."""
        if not opportunities:
            return
        docs = []
        for opp in opportunities:
            doc = asdict(opp)
            doc["timestamp_utc"] = datetime.fromtimestamp(opp.timestamp, tz=timezone.utc)
            docs.append(doc)
        try:
            self._coll.insert_many(docs)
        except Exception as e:
            logger.error(f"MongoDB write error: {e}")

    def _print_status(self, opportunities: list[ArbOpportunity]):
        """Print human-readable status to stdout."""
        ts = datetime.now().strftime("%H:%M:%S")
        n = len(opportunities)
        lat = self.state.last_poll_latency

        if not opportunities:
            print(f"[{ts}] Poll #{self.state.polls} | {lat:.2f}s | 0 opportunities")
            return

        print(f"\n[{ts}] Poll #{self.state.polls} | {lat:.2f}s | {n} opportunities")
        print(f"  {'Symbol':<14} {'Spread':>7} {'Net':>7} {'Direction':<18} {'MinVol':>12}")
        for opp in opportunities[:15]:
            min_vol = min(opp.bn_vol_24h, opp.bb_vol_24h)
            print(
                f"  {opp.symbol:<14} {opp.best_spread_bps:>6.1f}bp "
                f"{opp.net_spread_bps:>6.1f}bp {opp.direction:<18} "
                f"${min_vol:>10,.0f}"
            )

    async def run(self, max_polls: int | None = None):
        """Main loop: poll → log → print → sleep."""
        self._running = True
        logger.info(
            f"Arb screener starting: poll={self.poll_interval}s, "
            f"min_spread={self.min_net_spread_bps}bp, "
            f"fee={self.fee*10000:.1f}bp"
        )

        poll_count = 0
        try:
            while self._running:
                opportunities = await self.poll_once()
                self._log_opportunities(opportunities)
                self._print_status(opportunities)

                poll_count += 1
                if max_polls and poll_count >= max_polls:
                    break

                await asyncio.sleep(self.poll_interval)
        except KeyboardInterrupt:
            logger.info("Screener stopped by user")
        finally:
            if self._session and not self._session.closed:
                await self._session.close()
            self._running = False

    def stop(self):
        self._running = False


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Cross-exchange arb screener")
    parser.add_argument("--interval", type=float, default=3.0, help="Poll interval (seconds)")
    parser.add_argument("--min-spread", type=float, default=5.0, help="Min net spread (bps)")
    parser.add_argument("--min-vol", type=float, default=500_000, help="Min 24h volume ($)")
    parser.add_argument("--polls", type=int, default=None, help="Max polls (None=infinite)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    screener = ArbScreener(
        poll_interval=args.interval,
        min_net_spread_bps=args.min_spread,
        min_volume_24h=args.min_vol,
    )
    await screener.run(max_polls=args.polls)


if __name__ == "__main__":
    asyncio.run(main())
