"""
Pair Screener — Scan all 230 HL pairs every 15min (Spec Section 0).

Pipeline:
  1. Fetch meta + asset contexts for all pairs (1 REST call)
  2. Filter pairs with dayNtlVlm > $100K
  3. For each: fetch L2 book (200ms stagger), compute spread, depth, microprice
  4. Score = edge_room_per_side * sqrt(daily_volume) * depth_factor * anchor_bonus
  5. Rank by score. Top N = ACTIVE.
  6. Store rankings in MongoDB: hl_mm_pair_rankings

Pair lifecycle:
  - ACTIVE:  top N pairs, actively quoting
  - SHADOW:  newly promoted, 1h data collection before quoting
  - IDLE:    not in top N, no quoting, may be finishing inventory exit
  - BLOCKED: permanent ban (manipulation, delisting, broken feed)

Rate limiting:
  - HL REST is IP-level, ~5-6 rapid calls before 429
  - meta_and_asset_ctxs is 1 call (always safe)
  - L2 fetches staggered at 200ms
  - Full scan of 50 qualifying pairs takes ~10s
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import numpy as np
from hyperliquid.info import Info
from pymongo import MongoClient, DESCENDING
from pymongo.collection import Collection

logger = logging.getLogger(__name__)


# Known pairs to never trade
BLOCKED_PAIRS = frozenset({
    "MEGA",      # spread collapsed to 1.4bps, not stable
    "PURR",      # HL native token, unpredictable
    "HYPE",      # HL native token
    "JEFF",      # meme, extreme manipulation risk
})


@dataclass
class PairRanking:
    """Screener output for one pair."""
    coin: str
    timestamp: float
    score: float
    spread_bps: float
    edge_room_bps: float
    daily_volume_usd: float
    depth_bid_usd: float
    depth_ask_usd: float
    anchor_type: str           # "direct", "sparse", "synthetic", "none"
    tox_estimate: float        # estimated toxicity buffer (bps)
    status: str                # ACTIVE, SHADOW, IDLE, BLOCKED
    native_half_spread: float
    trade_count_hint: float    # proxy from volume/avg_trade_size
    sz_decimals: int
    leverage_available: int


@dataclass
class ScreenerConfig:
    """Screener configuration."""
    min_daily_volume_usd: float = 100_000.0
    maker_fee_bps: float = 1.44
    default_tox_buffer_bps: float = 1.0
    min_edge_room_bps: float = 1.0       # minimum edge to consider
    depth_factor_notional: float = 500.0  # median_depth / (10 * notional_per_order)
    max_live_pairs: int = 2
    shadow_duration_s: float = 3600.0    # 1 hour in shadow
    demotion_grace_periods: int = 2      # must fail 2 consecutive scans
    l2_fetch_stagger_s: float = 0.2      # 200ms between L2 calls
    rescore_interval_s: float = 900.0    # 15 minutes
    depth_levels: int = 20               # top-20 depth


# Known Bybit perp pairs (for anchor bonus)
# Updated from arb_hl_bybit_perp_snapshots collection
BYBIT_PERPS: frozenset[str] = frozenset({
    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LINK", "DOT",
    "UNI", "NEAR", "APT", "ARB", "OP", "SUI", "SEI", "WLD", "LTC",
    "BCH", "BNB", "CRV", "ALGO", "GALA", "ONT", "TAO", "ZEC",
    "ORDI", "APE", "PENDLE", "BIO", "DASH", "AXS", "PNUT",
})


class PairScreener:
    """Scan HL pairs and rank by MM profitability."""

    def __init__(
        self,
        info: Info,
        mongo_uri: str = "mongodb://localhost:27017/quants_lab",
        config: Optional[ScreenerConfig] = None,
        rate_limit_fn: Optional[object] = None,
    ):
        self.info = info
        self.config = config or ScreenerConfig()
        # Bug #3 (Codex R4): Shared rate limiter callback
        self._rate_limit_fn = rate_limit_fn

        # MongoDB
        client = MongoClient(mongo_uri)
        db_name = mongo_uri.split("/")[-1]
        self._db = client[db_name]
        self._rankings_col: Collection = self._db["hl_mm_pair_rankings"]
        self._rankings_col.create_index([("timestamp", DESCENDING)])
        self._rankings_col.create_index([("coin", 1), ("timestamp", DESCENDING)])

        # State
        self._last_scan: float = 0.0
        self._rankings: list[PairRanking] = []
        self._shadow_start: dict[str, float] = {}   # coin -> when entered SHADOW
        self._demotion_count: dict[str, int] = {}    # coin -> consecutive failed scans
        self._active_pairs: set[str] = set()
        self._sz_decimals: dict[str, int] = {}
        self._max_leverage: dict[str, int] = {}

    @property
    def active_pairs(self) -> set[str]:
        return self._active_pairs.copy()

    @property
    def shadow_pairs(self) -> set[str]:
        now = time.time()
        return {
            coin for coin, start in self._shadow_start.items()
            if now - start < self.config.shadow_duration_s
        }

    def get_sz_decimals(self) -> dict[str, int]:
        """Return cached sz_decimals from last meta fetch."""
        return self._sz_decimals.copy()

    def should_rescan(self) -> bool:
        """Check if it is time for a new scan."""
        return time.time() - self._last_scan >= self.config.rescore_interval_s

    async def scan(self) -> list[PairRanking]:
        """Run the full screener pipeline. Call every 15 minutes.

        This is async because L2 fetches are staggered with sleeps.
        Returns sorted list of pair rankings (best first).
        """
        logger.info("Pair screener: starting scan")
        cfg = self.config

        # Step 1: Fetch meta + asset contexts
        try:
            # Bug #3 (Codex R4): Gate through shared rate limiter
            if self._rate_limit_fn and not self._rate_limit_fn():
                logger.debug("Screener: rate limited on meta fetch, skipping scan")
                return self._rankings
            meta_data = await asyncio.to_thread(self.info.meta_and_asset_ctxs)
        except Exception as e:
            logger.error(f"Screener: meta fetch failed: {e}")
            return self._rankings

        if not meta_data or len(meta_data) != 2:
            logger.error("Screener: unexpected meta format")
            return self._rankings

        meta, contexts = meta_data
        universe = meta.get("universe", [])

        # Cache sz_decimals and max leverage
        for pair_info in universe:
            name = pair_info.get("name", "")
            self._sz_decimals[name] = pair_info.get("szDecimals", 4)
            self._max_leverage[name] = pair_info.get("maxLeverage", 5)

        # Step 2: Filter by volume
        candidates = []
        for pair_info, ctx in zip(universe, contexts):
            coin = pair_info.get("name", "")
            if coin in BLOCKED_PAIRS:
                continue

            daily_vol = float(ctx.get("dayNtlVlm", 0) or 0)
            if daily_vol < cfg.min_daily_volume_usd:
                continue

            candidates.append((coin, pair_info, ctx, daily_vol))

        logger.info(f"Screener: {len(candidates)} pairs above ${cfg.min_daily_volume_usd/1000:.0f}K volume")

        # Step 3: Fetch L2 for each candidate (Gap 7: rate limit batching)
        # If more than 20 pairs qualify, only fetch top 20 by volume
        MAX_L2_FETCHES = 20
        L2_STAGGER_MS = 300  # 300ms spacing between fetches

        if len(candidates) > MAX_L2_FETCHES:
            candidates.sort(key=lambda x: x[3], reverse=True)  # sort by daily_vol
            logger.info(
                f"Screener: {len(candidates)} candidates exceed L2 limit, "
                f"fetching top {MAX_L2_FETCHES} by volume"
            )
            l2_candidates = candidates[:MAX_L2_FETCHES]
        else:
            l2_candidates = candidates

        rankings = []
        for coin, pair_info, ctx, daily_vol in l2_candidates:
            try:
                # Bug #3 (Codex R4): Gate through shared rate limiter
                if self._rate_limit_fn and not self._rate_limit_fn():
                    await asyncio.sleep(0.5)  # back off if rate limited
                    if self._rate_limit_fn and not self._rate_limit_fn():
                        continue  # still limited, skip this coin
                l2 = await asyncio.to_thread(self.info.l2_snapshot, coin)
                await asyncio.sleep(L2_STAGGER_MS / 1000.0)
            except Exception as e:
                if "429" in str(e):
                    logger.debug(f"Screener: rate limited on {coin}, backing off 2s")
                    await asyncio.sleep(2.0)
                    continue
                logger.debug(f"Screener: L2 fetch failed for {coin}: {e}")
                continue

            ranking = self._score_pair(coin, pair_info, ctx, daily_vol, l2)
            if ranking:
                rankings.append(ranking)

        # Also include non-L2 candidates with score=0 for ranking completeness
        l2_coins = {r.coin for r in rankings}
        for coin, pair_info, ctx, daily_vol in candidates:
            if coin not in l2_coins:
                # Score without L2 data: just volume-based, low priority
                rankings.append(PairRanking(
                    coin=coin, timestamp=time.time(), score=0.0,
                    spread_bps=0.0, edge_room_bps=0.0,
                    daily_volume_usd=daily_vol, depth_bid_usd=0.0,
                    depth_ask_usd=0.0, anchor_type="none",
                    tox_estimate=cfg.default_tox_buffer_bps, status="IDLE",
                    native_half_spread=0.0, trade_count_hint=0.0,
                    sz_decimals=self._sz_decimals.get(coin, 4),
                    leverage_available=self._max_leverage.get(coin, 5),
                ))

        # Step 4: Sort by score
        rankings.sort(key=lambda r: r.score, reverse=True)

        # Step 5: Manage promotions/demotions
        self._manage_pair_lifecycle(rankings)

        # Step 6: Persist to MongoDB
        self._persist_rankings(rankings)

        self._rankings = rankings
        self._last_scan = time.time()

        # Log top pairs
        for i, r in enumerate(rankings[:10]):
            logger.info(
                f"  #{i+1} {r.coin}: score={r.score:.1f} spread={r.spread_bps:.1f}bps "
                f"edge={r.edge_room_bps:.1f}bps vol=${r.daily_volume_usd/1000:.0f}K "
                f"anchor={r.anchor_type} status={r.status}"
            )

        return rankings

    def _score_pair(
        self,
        coin: str,
        pair_info: dict,
        ctx: dict,
        daily_vol: float,
        l2: dict,
    ) -> Optional[PairRanking]:
        """Score a single pair from L2 snapshot."""
        cfg = self.config

        if not l2 or "levels" not in l2:
            return None

        bids, asks = l2.get("levels", ([], []))
        if not bids or not asks:
            return None

        # Best bid/ask
        best_bid = float(bids[0]["px"])
        best_ask = float(asks[0]["px"])
        if best_bid <= 0 or best_ask <= 0:
            return None

        mid = (best_bid + best_ask) / 2.0
        spread_bps = (best_ask - best_bid) / best_bid * 10000
        native_half_spread = spread_bps / 2.0

        # Top-N depth (USD)
        n = min(cfg.depth_levels, len(bids), len(asks))
        depth_bid = sum(float(bids[i]["sz"]) * float(bids[i]["px"]) for i in range(n))
        depth_ask = sum(float(asks[i]["sz"]) * float(asks[i]["px"]) for i in range(n))

        # Anchor type
        if coin in BYBIT_PERPS:
            # Check if it is in arb snapshots (liquid) or just listed
            if daily_vol > 1_000_000:
                anchor_type = "direct"
            else:
                anchor_type = "sparse"
        else:
            anchor_type = "none"

        # Tox buffer (use known values or default)
        from .quote_engine import DEFAULT_TOX_BUFFERS
        tox_buffer = DEFAULT_TOX_BUFFERS.get(coin, cfg.default_tox_buffer_bps)

        # Edge room per side
        edge_room = native_half_spread - cfg.maker_fee_bps - tox_buffer
        if edge_room < cfg.min_edge_room_bps:
            # Still include in rankings but with low score
            pass

        # Score components
        edge_component = max(0, edge_room)
        volume_component = np.sqrt(daily_vol)
        depth_factor = min(1.0, min(depth_bid, depth_ask) / (10 * cfg.depth_factor_notional))
        anchor_bonus = {"direct": 1.5, "sparse": 1.0, "none": 0.6}.get(anchor_type, 0.6)

        score = edge_component * volume_component * depth_factor * anchor_bonus

        return PairRanking(
            coin=coin,
            timestamp=time.time(),
            score=score,
            spread_bps=spread_bps,
            edge_room_bps=edge_room,
            daily_volume_usd=daily_vol,
            depth_bid_usd=depth_bid,
            depth_ask_usd=depth_ask,
            anchor_type=anchor_type,
            tox_estimate=tox_buffer,
            status="IDLE",  # will be set by lifecycle manager
            native_half_spread=native_half_spread,
            trade_count_hint=daily_vol / (mid * 0.5) if mid > 0 else 0,
            sz_decimals=self._sz_decimals.get(coin, 4),
            leverage_available=self._max_leverage.get(coin, 5),
        )

    def _manage_pair_lifecycle(self, rankings: list[PairRanking]) -> None:
        """Promote/demote pairs based on rankings.

        Top N -> ACTIVE (or SHADOW if new)
        Dropped pairs -> IDLE after grace period
        """
        cfg = self.config
        now = time.time()

        top_coins = {r.coin for r in rankings[:cfg.max_live_pairs] if r.edge_room_bps > 0}

        # Promotions: new pairs entering top N go to SHADOW first
        for coin in top_coins:
            if coin not in self._active_pairs:
                if coin not in self._shadow_start:
                    self._shadow_start[coin] = now
                    logger.info(f"Screener: {coin} entering SHADOW (1h before ACTIVE)")
                elif now - self._shadow_start[coin] >= cfg.shadow_duration_s:
                    self._active_pairs.add(coin)
                    del self._shadow_start[coin]
                    self._demotion_count.pop(coin, None)
                    logger.info(f"Screener: {coin} promoted to ACTIVE")

        # Demotions: pairs dropping out of top N
        for coin in list(self._active_pairs):
            if coin not in top_coins:
                self._demotion_count[coin] = self._demotion_count.get(coin, 0) + 1
                if self._demotion_count[coin] >= cfg.demotion_grace_periods:
                    self._active_pairs.discard(coin)
                    self._demotion_count.pop(coin, None)
                    logger.info(f"Screener: {coin} demoted to IDLE (failed {cfg.demotion_grace_periods} scans)")
            else:
                self._demotion_count.pop(coin, None)

        # Clean shadow for coins no longer in top N
        for coin in list(self._shadow_start.keys()):
            if coin not in top_coins:
                del self._shadow_start[coin]

        # Update status in rankings
        for r in rankings:
            if r.coin in self._active_pairs:
                r.status = "ACTIVE"
            elif r.coin in self._shadow_start:
                r.status = "SHADOW"
            elif r.coin in BLOCKED_PAIRS:
                r.status = "BLOCKED"
            else:
                r.status = "IDLE"

    def _persist_rankings(self, rankings: list[PairRanking]) -> None:
        """Write rankings to MongoDB."""
        if not rankings:
            return

        docs = [
            {
                "timestamp": datetime.fromtimestamp(r.timestamp, tz=timezone.utc),
                "coin": r.coin,
                "score": r.score,
                "spread_bps": r.spread_bps,
                "edge_room_bps": r.edge_room_bps,
                "daily_volume_usd": r.daily_volume_usd,
                "depth_bid_usd": r.depth_bid_usd,
                "depth_ask_usd": r.depth_ask_usd,
                "anchor_type": r.anchor_type,
                "tox_estimate": r.tox_estimate,
                "status": r.status,
                "native_half_spread": r.native_half_spread,
            }
            for r in rankings
        ]

        try:
            self._rankings_col.insert_many(docs, ordered=False)
            logger.debug(f"Screener: persisted {len(docs)} rankings to MongoDB")
        except Exception as e:
            logger.warning(f"Screener: MongoDB write failed: {e}")

    def force_active(self, coin: str) -> None:
        """Force a pair to ACTIVE (bypass screener). For manual override."""
        self._active_pairs.add(coin)
        self._shadow_start.pop(coin, None)
        logger.info(f"Screener: {coin} forced to ACTIVE")

    def force_block(self, coin: str) -> None:
        """Block a pair permanently."""
        self._active_pairs.discard(coin)
        self._shadow_start.pop(coin, None)
        logger.info(f"Screener: {coin} BLOCKED")

    def get_pair_ranking(self, coin: str) -> Optional[PairRanking]:
        """Get the most recent ranking for a specific coin."""
        for r in self._rankings:
            if r.coin == coin:
                return r
        return None
