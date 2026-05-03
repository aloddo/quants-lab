"""
Competitor MM Tracker — Monitor other market makers on HL.

HL exposes every wallet's positions via public API. We use this to:
1. Identify which wallets are active MMs on our target coins
2. Track their position changes as informed flow signals
3. Compute crowding scores (how many MMs warehouse same direction)
4. Demote pairs where MM competition is too high

Data flow:
  - On startup: scan leaderboard for wallets with positions on our coins
  - Every 60s: poll top wallets' positions (1 REST call each)
  - Compute per-coin crowding score and position-change velocity
  - Feed into pair selection and quoting gates

This runs in the background — does NOT need to be real-time.
Uses the HL REST API (subject to rate limits).
"""
import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

import requests

logger = logging.getLogger(__name__)

HL_INFO_API = "https://api.hyperliquid.xyz/info"
HL_LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"


@dataclass
class MMWallet:
    """Tracked MM wallet."""
    address: str
    account_value: float = 0.0
    # Per-coin positions: {coin: size}
    positions: dict = field(default_factory=dict)
    prev_positions: dict = field(default_factory=dict)
    last_polled: float = 0.0
    is_mm: bool = False  # identified as MM by two-sided quoting pattern


@dataclass
class CoinCrowding:
    """Crowding metrics for one coin."""
    coin: str
    mm_count: int = 0             # how many tracked MMs have a position
    net_mm_direction: float = 0.0  # sum of MM position signs (-1/0/+1)
    total_mm_notional: float = 0.0
    position_change_velocity: float = 0.0  # bps change in aggregate MM position per minute
    crowding_score: float = 0.0   # normalized 0..1, higher = more crowded


class MMTracker:
    """Track competitor MMs on HL via public position data."""

    def __init__(
        self,
        target_coins: list[str] = None,
        min_account_value: float = 100_000.0,
        max_tracked: int = 20,
        poll_interval_s: float = 60.0,
    ):
        self.target_coins = set(target_coins or [])
        self.min_account_value = min_account_value
        self.max_tracked = max_tracked
        self.poll_interval_s = poll_interval_s

        self._wallets: dict[str, MMWallet] = {}
        self._crowding: dict[str, CoinCrowding] = {}
        self._last_scan: float = 0.0
        self._last_poll: float = 0.0
        self._initialized: bool = False

    def update_target_coins(self, coins: set[str]) -> None:
        """Update which coins we're tracking MMs for."""
        self.target_coins = coins

    async def initialize(self) -> None:
        """Scan leaderboard to find wallets with positions on our coins.

        Called once on startup. Uses aiohttp-style but wraps sync requests
        in to_thread to avoid blocking.
        """
        try:
            resp = await asyncio.to_thread(
                requests.get, HL_LEADERBOARD_URL, timeout=15
            )
            if resp.status_code != 200:
                logger.warning(f"MM Tracker: leaderboard fetch failed: {resp.status_code}")
                return

            data = resp.json()
            rows = data.get("leaderboardRows", data) if isinstance(data, dict) else data

            candidates = []
            for entry in rows:
                try:
                    av = float(entry.get("accountValue", 0))
                    if av >= self.min_account_value:
                        candidates.append({
                            "address": entry["ethAddress"],
                            "account_value": av,
                        })
                except (ValueError, KeyError):
                    continue

            candidates.sort(key=lambda x: x["account_value"], reverse=True)
            logger.info(f"MM Tracker: {len(candidates)} wallets above ${self.min_account_value/1000:.0f}K")

            # Check top wallets for positions on our coins
            checked = 0
            for cand in candidates[:50]:  # check top 50
                if checked >= self.max_tracked:
                    break
                addr = cand["address"]
                try:
                    positions = await self._fetch_positions(addr)
                    if positions:
                        has_our_coins = any(
                            coin in self.target_coins for coin in positions
                        )
                        if has_our_coins:
                            self._wallets[addr] = MMWallet(
                                address=addr,
                                account_value=cand["account_value"],
                                positions=positions,
                                last_polled=time.time(),
                            )
                            checked += 1
                    await asyncio.sleep(0.8)  # rate limit
                except Exception as e:
                    logger.debug(f"MM Tracker: failed to check {addr[:10]}: {e}")
                    await asyncio.sleep(1.5)

            self._initialized = True
            self._last_scan = time.time()
            logger.info(
                f"MM Tracker: tracking {len(self._wallets)} wallets with positions "
                f"on {self.target_coins}"
            )

        except Exception as e:
            logger.error(f"MM Tracker initialization failed: {e}")

    async def poll_positions(self) -> None:
        """Poll tracked wallets for position updates. Call every 60s."""
        now = time.time()
        if now - self._last_poll < self.poll_interval_s:
            return

        for addr, wallet in list(self._wallets.items()):
            try:
                new_positions = await self._fetch_positions(addr)
                if new_positions is not None:
                    wallet.prev_positions = dict(wallet.positions)
                    wallet.positions = new_positions
                    wallet.last_polled = now
                await asyncio.sleep(0.8)
            except Exception as e:
                logger.debug(f"MM Tracker: poll failed for {addr[:10]}: {e}")
                await asyncio.sleep(1.5)

        self._last_poll = now
        self._compute_crowding()

    def _compute_crowding(self) -> None:
        """Compute per-coin crowding metrics from tracked MM positions."""
        self._crowding.clear()

        for coin in self.target_coins:
            mm_count = 0
            net_direction = 0.0
            total_notional = 0.0
            total_change = 0.0

            for wallet in self._wallets.values():
                size = wallet.positions.get(coin, 0)
                prev_size = wallet.prev_positions.get(coin, 0)

                if abs(size) > 0:
                    mm_count += 1
                    net_direction += 1 if size > 0 else -1
                    total_notional += abs(size)  # in base units, not USD

                if abs(prev_size) > 0 or abs(size) > 0:
                    change = size - prev_size
                    total_change += change

            # Crowding score: 0 = no MMs, 1 = all MMs same direction
            if mm_count > 0:
                alignment = abs(net_direction) / mm_count  # 0..1
                crowding_score = alignment * min(1.0, mm_count / 5.0)  # scale by count
            else:
                crowding_score = 0.0

            self._crowding[coin] = CoinCrowding(
                coin=coin,
                mm_count=mm_count,
                net_mm_direction=net_direction,
                total_mm_notional=total_notional,
                position_change_velocity=total_change,
                crowding_score=crowding_score,
            )

    def get_crowding(self, coin: str) -> CoinCrowding:
        """Get crowding metrics for a coin."""
        return self._crowding.get(coin, CoinCrowding(coin=coin))

    def is_crowded(self, coin: str, threshold: float = 0.6) -> bool:
        """Check if a coin is too crowded with competing MMs."""
        crowding = self._crowding.get(coin)
        if not crowding:
            return False
        return crowding.crowding_score > threshold

    def mm_reducing_side(self, coin: str) -> Optional[str]:
        """Check if MMs are reducing on one side (informed exit signal).

        Returns "bid" if MMs are net reducing longs, "ask" if reducing shorts,
        None if no clear signal.
        """
        crowding = self._crowding.get(coin)
        if not crowding:
            return None

        # If velocity is strongly negative (MMs reducing longs) → bearish signal
        if crowding.position_change_velocity < -0.1 * crowding.total_mm_notional:
            return "bid"  # MMs selling → don't bid
        elif crowding.position_change_velocity > 0.1 * crowding.total_mm_notional:
            return "ask"  # MMs buying → don't ask

        return None

    def get_tracked_wallets(self) -> list[MMWallet]:
        """Get all tracked MM wallets for reporting."""
        return list(self._wallets.values())

    def to_mongo_docs(self) -> list[dict]:
        """Export crowding data to MongoDB."""
        return [
            {
                "coin": c.coin,
                "mm_count": c.mm_count,
                "net_direction": c.net_mm_direction,
                "total_notional": c.total_mm_notional,
                "velocity": c.position_change_velocity,
                "crowding_score": c.crowding_score,
                "timestamp": time.time(),
            }
            for c in self._crowding.values()
        ]

    async def _fetch_positions(self, address: str) -> Optional[dict]:
        """Fetch positions for a wallet. Returns {coin: size} dict."""
        try:
            resp = await asyncio.to_thread(
                requests.post,
                HL_INFO_API,
                json={"type": "clearinghouseState", "user": address},
                timeout=5,
            )
            if resp.status_code == 429:
                return None
            if resp.status_code != 200:
                return None

            state = resp.json()
            positions = {}
            for p in state.get("assetPositions", []):
                pos = p.get("position", {})
                coin = pos.get("coin", "")
                size = float(pos.get("szi", 0))
                if abs(size) > 0:
                    positions[coin] = size
            return positions

        except Exception:
            return None
