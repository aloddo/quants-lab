#!/usr/bin/env python3
"""
HL MM V4 Shadow Quoter — Phase 1

Connects to HL WebSocket, computes where we WOULD place quotes,
tracks theoretical fills (when market trades through our quote level),
and measures markout on those theoretical fills.

NO REAL ORDERS. This proves EV before risking money.

Usage:
    python scripts/hl_mm_shadow_quoter.py --pairs ORDI,DASH,AXS --capital 200

Outputs to MongoDB: hl_mm_shadow_fills, hl_mm_shadow_stats
Telegram alerts for theoretical fills and hourly P&L.
"""
import argparse
import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

import numpy as np
import websockets
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("hl_mm_shadow")

# ── Configuration ──────────────────────────────────────────

MAKER_FEE_PER_SIDE = 0.000144  # 1.44bp
TAKER_FEE_PER_SIDE = 0.000432  # 4.32bp
FEE_RT_MAKER = 2 * MAKER_FEE_PER_SIDE  # 2.88bp

# Quote parameters
MIN_HALF_SPREAD_BPS = 3.0   # minimum half-spread to quote (must exceed fee)
MAX_INVENTORY_FRAC = 0.25   # max 25% of capital in one pair
REQUOTE_INTERVAL_S = 5.0    # refresh quotes every 5s
INVENTORY_SKEW_MAX_BPS = 5.0  # max skew from inventory

# Adverse selection filters
IMBALANCE_WIDEN_THRESHOLD = 0.3
IMBALANCE_PAUSE_THRESHOLD = 0.5
MOMENTUM_PAUSE_BPS = 15.0   # raised from 5 — alts routinely move 5-10bp/min
TOXICITY_PAUSE_THRESHOLD = 0.6
TOXICITY_WINDOW_S = 5.0

HL_WS_URL = "wss://api.hyperliquid.xyz/ws"


@dataclass
class PairState:
    """Live state for one pair."""
    coin: str
    mid: float = 0.0
    best_bid: float = 0.0
    best_ask: float = 0.0
    spread_bps: float = 0.0
    bid_depth_5: float = 0.0
    ask_depth_5: float = 0.0
    imbalance: float = 0.0

    # Price history for momentum
    ema5: float = 0.0
    ema20: float = 0.0
    last_close_1m: float = 0.0
    bar_closes: List[float] = field(default_factory=list)
    mid_history: List[tuple] = field(default_factory=list)  # [(timestamp, mid)]

    # Trade toxicity
    buy_volume_5s: float = 0.0
    sell_volume_5s: float = 0.0
    recent_trades: List[dict] = field(default_factory=list)

    # Inventory
    inventory_qty: float = 0.0
    inventory_cost: float = 0.0

    # Shadow quotes
    shadow_bid: float = 0.0
    shadow_ask: float = 0.0

    # Stats
    theoretical_fills: int = 0
    theoretical_pnl_bps: float = 0.0
    regime_counts: Dict = field(default_factory=lambda: defaultdict(int))


@dataclass
class ShadowFill:
    """A theoretical fill event."""
    coin: str
    side: str  # "buy" or "sell"
    price: float
    size_usd: float
    mid_at_fill: float
    spread_at_fill: float
    imbalance_at_fill: float
    regime: str
    timestamp: float
    # Markout (filled later)
    markout_5s: Optional[float] = None
    markout_30s: Optional[float] = None
    markout_5m: Optional[float] = None
    exit_price: Optional[float] = None
    cycle_pnl_bps: Optional[float] = None


class ShadowQuoter:
    """Shadow MM engine — computes quotes, detects theoretical fills, tracks markout."""

    def __init__(self, pairs: List[str], capital_per_pair: float):
        self.pairs = pairs
        self.capital = capital_per_pair
        self.states: Dict[str, PairState] = {p: PairState(coin=p) for p in pairs}
        self.pending_fills: List[ShadowFill] = []
        self.completed_fills: List[ShadowFill] = []

        self.db = MongoClient("mongodb://localhost:27017").quants_lab
        self.start_time = time.time()
        self._last_quote_update = 0.0
        self._last_stats_log = 0.0

    def _compute_regime(self, state: PairState) -> str:
        """Determine market regime from filters."""
        if abs(state.imbalance) > IMBALANCE_PAUSE_THRESHOLD:
            return "TRENDING"
        if state.spread_bps < 1.0:
            return "TIGHT"  # too tight to MM

        # One-sided inventory check: if inventory is heavily directional, market is trending
        inv_usd = abs(state.inventory_qty * state.mid) if state.mid > 0 else 0
        if inv_usd > self.capital * MAX_INVENTORY_FRAC * 0.5:
            return "TRENDING"  # we're accumulating one-sided → trend detected

        # Fast momentum: 60-second price change vs recent volatility
        if len(state.mid_history) >= 40:
            now_ts = time.time()
            # Compute recent 5-minute realized vol (std of 60s returns)
            mids = [m for ts, m in state.mid_history if now_ts - ts < 300]
            if len(mids) >= 30:
                returns = [(mids[i] - mids[i-1]) / mids[i-1] * 10000
                           for i in range(1, len(mids)) if mids[i-1] > 0]
                if returns:
                    vol_1m = float(np.std(returns)) if len(returns) > 5 else 999
                    # Momentum: 60s price change
                    old_mid = None
                    for ts, m in reversed(state.mid_history):
                        if now_ts - ts >= 55:
                            old_mid = m
                            break
                    if old_mid and old_mid > 0 and vol_1m > 0:
                        momentum_bps = abs((state.mid - old_mid) / old_mid * 10000)
                        # Trending if momentum > 2x typical volatility
                        if momentum_bps > max(vol_1m * 2, MOMENTUM_PAUSE_BPS):
                            return "TRENDING"

        # Toxicity check (need minimum volume to be meaningful)
        total_vol = state.buy_volume_5s + state.sell_volume_5s
        if total_vol > 500:  # at least $500 traded in 5s to be meaningful
            toxicity = (state.buy_volume_5s - state.sell_volume_5s) / total_vol
            if abs(toxicity) > TOXICITY_PAUSE_THRESHOLD:
                return "TRENDING"

        if abs(state.imbalance) < 0.2:
            return "RANGING"

        return "NEUTRAL"

    def _compute_shadow_quotes(self, state: PairState) -> tuple:
        """Compute where we WOULD quote. Returns (bid, ask) or (None, None) if paused."""
        if state.mid <= 0 or state.spread_bps < 1.0:
            return None, None

        # Warmup: don't quote until we have 60s of mid history for momentum
        if len(state.mid_history) < 40:
            return None, None

        regime = self._compute_regime(state)

        # Base half-spread: at least MIN_HALF_SPREAD_BPS, at most natural half-spread
        natural_half = state.spread_bps / 2
        half_spread_bps = max(MIN_HALF_SPREAD_BPS, natural_half * 0.8)

        # Regime adjustment
        if regime == "TRENDING":
            return None, None  # pause quoting
        elif regime == "NEUTRAL":
            half_spread_bps *= 1.3
        elif regime == "TIGHT":
            return None, None

        # Inventory skew (correct direction per Codex review)
        inv_frac = state.inventory_qty * state.mid / self.capital if self.capital > 0 else 0
        skew_bps = inv_frac * INVENTORY_SKEW_MAX_BPS
        # If long (inv_frac > 0): skew DOWN → lower ask (encourage selling), raise bid (discourage buying)

        # Adverse selection: widen the toxic side
        bid_adjust = 0.0
        ask_adjust = 0.0
        if state.imbalance > IMBALANCE_WIDEN_THRESHOLD:
            # Buying pressure → ask is toxic → widen ask
            ask_adjust = half_spread_bps * 0.5
        elif state.imbalance < -IMBALANCE_WIDEN_THRESHOLD:
            # Selling pressure → bid is toxic → widen bid
            bid_adjust = half_spread_bps * 0.5

        bid = state.mid * (1 - (half_spread_bps + bid_adjust + skew_bps) / 10000)
        ask = state.mid * (1 + (half_spread_bps + ask_adjust - skew_bps) / 10000)

        return bid, ask

    def _check_theoretical_fills(self, state: PairState, trades: List[dict]):
        """Check if any market trades would have filled our shadow quotes."""
        if state.shadow_bid is None and state.shadow_ask is None:
            return  # Both sides paused

        for trade in trades:
            try:
                price = float(trade.get("px", 0))
                size = float(trade.get("sz", 0))
            except (ValueError, TypeError):
                continue
            side = trade.get("side", "")

            if not price or not size:
                continue

            fill = None
            # A sell trade at or below our bid → we buy (bid filled)
            if side == "S" and state.shadow_bid is not None and price <= state.shadow_bid:
                fill = ShadowFill(
                    coin=state.coin, side="buy", price=state.shadow_bid,
                    size_usd=min(size * price, self.capital * MAX_INVENTORY_FRAC),
                    mid_at_fill=state.mid, spread_at_fill=state.spread_bps,
                    imbalance_at_fill=state.imbalance,
                    regime=self._compute_regime(state),
                    timestamp=time.time(),
                )
            # A buy trade at or above our ask → we sell (ask filled)
            elif side == "B" and state.shadow_ask is not None and price >= state.shadow_ask:
                fill = ShadowFill(
                    coin=state.coin, side="sell", price=state.shadow_ask,
                    size_usd=min(size * price, self.capital * MAX_INVENTORY_FRAC),
                    mid_at_fill=state.mid, spread_at_fill=state.spread_bps,
                    imbalance_at_fill=state.imbalance,
                    regime=self._compute_regime(state),
                    timestamp=time.time(),
                )

            if fill:
                # CHECK INVENTORY LIMIT before accepting fill
                current_inv_usd = abs(state.inventory_qty * state.mid)
                if current_inv_usd >= self.capital * MAX_INVENTORY_FRAC:
                    continue  # inventory full, skip this fill

                self.pending_fills.append(fill)
                state.theoretical_fills += 1
                # Update shadow inventory
                if fill.side == "buy":
                    state.inventory_qty += fill.size_usd / state.mid
                else:
                    state.inventory_qty -= fill.size_usd / state.mid

                logger.info(
                    f"SHADOW FILL {fill.coin} {fill.side.upper()} @ {fill.price:.6f} "
                    f"spread={fill.spread_at_fill:.1f}bp regime={fill.regime} "
                    f"inv={state.inventory_qty:.4f}"
                )

    def _update_markouts(self):
        """Update markout for pending fills based on current mid prices."""
        now = time.time()
        still_pending = []

        for fill in self.pending_fills:
            state = self.states.get(fill.coin)
            if not state or state.mid <= 0:
                still_pending.append(fill)
                continue

            elapsed = now - fill.timestamp
            mid_change_bps = (state.mid - fill.mid_at_fill) / fill.mid_at_fill * 10000

            # Direction-adjusted markout
            if fill.side == "buy":
                markout = mid_change_bps  # positive = price went up = good for buyer
            else:
                markout = -mid_change_bps  # positive = price went down = good for seller

            if elapsed >= 5 and fill.markout_5s is None:
                fill.markout_5s = markout
            if elapsed >= 30 and fill.markout_30s is None:
                fill.markout_30s = markout
            if elapsed >= 300 and fill.markout_5m is None:
                fill.markout_5m = markout

                # Calculate cycle P&L (half-spread captured + markout - fee)
                half_spread = fill.spread_at_fill / 2
                fill.cycle_pnl_bps = half_spread + fill.markout_5m - MAKER_FEE_PER_SIDE * 10000
                fill.exit_price = state.mid

                self.completed_fills.append(fill)
                self._store_fill(fill)
                logger.info(
                    f"CYCLE COMPLETE {fill.coin} {fill.side}: "
                    f"spread={fill.spread_at_fill:.1f}bp markout_5m={fill.markout_5m:+.1f}bp "
                    f"cycle_pnl={fill.cycle_pnl_bps:+.1f}bp"
                )
                continue

            still_pending.append(fill)

        self.pending_fills = still_pending

    def _store_fill(self, fill: ShadowFill):
        """Store completed fill to MongoDB."""
        self.db["hl_mm_shadow_fills"].insert_one({
            "coin": fill.coin,
            "side": fill.side,
            "price": fill.price,
            "size_usd": fill.size_usd,
            "mid_at_fill": fill.mid_at_fill,
            "spread_at_fill": fill.spread_at_fill,
            "imbalance_at_fill": fill.imbalance_at_fill,
            "regime": fill.regime,
            "markout_5s": fill.markout_5s,
            "markout_30s": fill.markout_30s,
            "markout_5m": fill.markout_5m,
            "cycle_pnl_bps": fill.cycle_pnl_bps,
            "exit_price": fill.exit_price,
            "timestamp": fill.timestamp,
            "recorded_at": datetime.now(timezone.utc),
        })

    def _log_stats(self):
        """Log hourly stats."""
        now = time.time()
        if now - self._last_stats_log < 60:  # every 1 min
            return
        self._last_stats_log = now

        uptime_h = (now - self.start_time) / 3600
        total_fills = sum(s.theoretical_fills for s in self.states.values())
        completed = len(self.completed_fills)
        pending = len(self.pending_fills)

        if completed > 0:
            avg_pnl = np.mean([f.cycle_pnl_bps for f in self.completed_fills])
            wr = np.mean([1 if f.cycle_pnl_bps > 0 else 0 for f in self.completed_fills])
        else:
            avg_pnl = 0
            wr = 0

        logger.info(
            f"STATS: uptime={uptime_h:.1f}h fills={total_fills} "
            f"completed={completed} pending={pending} "
            f"avg_pnl={avg_pnl:+.2f}bp WR={wr:.0%}"
        )

        for coin, state in self.states.items():
            regime_str = dict(state.regime_counts)
            total_obs = sum(state.regime_counts.values()) or 1
            ranging_pct = state.regime_counts.get("RANGING", 0) / total_obs * 100
            logger.info(
                f"  {coin}: spread={state.spread_bps:.1f}bp "
                f"regime={self._compute_regime(state)} "
                f"fills={state.theoretical_fills} "
                f"ranging={ranging_pct:.0f}% "
                f"regimes={regime_str}"
            )

    def process_l2_update(self, coin: str, data: dict):
        """Process L2 book update from WS."""
        state = self.states.get(coin)
        if not state:
            return

        levels = data.get("levels", [])
        if len(levels) < 2:
            return

        bids = levels[0]
        asks = levels[1]

        if not bids or not asks:
            return

        state.best_bid = float(bids[0]["px"])
        state.best_ask = float(asks[0]["px"])
        state.mid = (state.best_bid + state.best_ask) / 2
        state.spread_bps = (state.best_ask - state.best_bid) / state.mid * 10000

        # Depth
        state.bid_depth_5 = sum(float(b["px"]) * float(b["sz"]) for b in bids[:5])
        state.ask_depth_5 = sum(float(a["px"]) * float(a["sz"]) for a in asks[:5])
        total_depth = state.bid_depth_5 + state.ask_depth_5
        state.imbalance = (state.bid_depth_5 - state.ask_depth_5) / total_depth if total_depth > 0 else 0

        # Track regime distribution
        regime = self._compute_regime(state)
        state.regime_counts[regime] += 1

        # Track mid history (keep last 5 min = 300 entries at ~1/s)
        now = time.time()
        state.mid_history.append((now, state.mid))
        if len(state.mid_history) > 300:
            state.mid_history = state.mid_history[-300:]

        # Update shadow quotes on EVERY L2 update (not just every 5s)
        # This ensures quotes are cleared immediately when regime changes
        bid, ask = self._compute_shadow_quotes(state)
        state.shadow_bid = bid
        state.shadow_ask = ask

    def process_trades(self, coin: str, trades: List[dict]):
        """Process trade updates from WS."""
        state = self.states.get(coin)
        if not state:
            return

        now = time.time()

        # Update trade toxicity (5s window)
        for t in trades:
            t["_ts"] = now
            state.recent_trades.append(t)

        # Prune old trades
        state.recent_trades = [t for t in state.recent_trades if now - t.get("_ts", 0) < TOXICITY_WINDOW_S]

        try:
            state.buy_volume_5s = sum(
                float(t.get("sz", 0)) * float(t.get("px", 0))
                for t in state.recent_trades if t.get("side") == "B"
            )
            state.sell_volume_5s = sum(
                float(t.get("sz", 0)) * float(t.get("px", 0))
                for t in state.recent_trades if t.get("side") == "S"
            )
        except (ValueError, TypeError):
            pass

        # Check for theoretical fills
        self._check_theoretical_fills(state, trades)

    async def run(self):
        """Main event loop — connect to HL WS, process updates."""
        logger.info(f"Shadow quoter starting: pairs={self.pairs}, capital=${self.capital}/pair")

        while True:
            try:
                async with websockets.connect(HL_WS_URL) as ws:
                    # Subscribe to L2 book and trades for each pair
                    for pair in self.pairs:
                        coin = pair.replace("-USDT", "")
                        await ws.send(json.dumps({
                            "method": "subscribe",
                            "subscription": {"type": "l2Book", "coin": coin}
                        }))
                        await ws.send(json.dumps({
                            "method": "subscribe",
                            "subscription": {"type": "trades", "coin": coin}
                        }))
                        logger.info(f"Subscribed to {coin} L2 + trades")

                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                            data = json.loads(msg)

                            channel = data.get("channel")
                            coin_data = data.get("data", {})

                            if channel == "l2Book":
                                coin = coin_data.get("coin", "")
                                pair = f"{coin}-USDT"
                                self.process_l2_update(pair, coin_data)

                            elif channel == "trades":
                                if isinstance(coin_data, list) and coin_data:
                                    coin = coin_data[0].get("coin", "")
                                    pair = f"{coin}-USDT"
                                    self.process_trades(pair, coin_data)

                            # Update markouts on pending fills
                            self._update_markouts()
                            self._log_stats()

                        except asyncio.TimeoutError:
                            # Send ping to keep connection alive
                            await ws.ping()

            except Exception as e:
                logger.error(f"WS error: {e}, reconnecting in 5s...")
                await asyncio.sleep(5)


def main():
    parser = argparse.ArgumentParser(description="HL MM Shadow Quoter")
    parser.add_argument("--pairs", default="ORDI,DASH,AXS,APE",
                        help="Comma-separated coin names (without -USDT)")
    parser.add_argument("--capital", type=float, default=200,
                        help="Capital per pair in USD")
    args = parser.parse_args()

    pairs = [f"{p.strip()}-USDT" for p in args.pairs.split(",")]

    quoter = ShadowQuoter(pairs=pairs, capital_per_pair=args.capital)
    asyncio.run(quoter.run())


if __name__ == "__main__":
    main()
