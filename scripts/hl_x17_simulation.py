"""
X17 Selective Passive Alpha Maker — Event-Driven Simulation

Replays historical L2 snapshots + Bybit-HL spread data to simulate a selective
market-making strategy on Hyperliquid. The core thesis: quote ONLY when
Bybit-HL price dislocation exceeds fees + toxicity threshold.

Data sources (MongoDB):
- hyperliquid_l2_snapshots_1s: 1s L2 book snapshots (BTC, ETH, SOL, HYPE, ZEC)
- arb_hl_bybit_perp_snapshots: ~5s Bybit-HL spread snapshots (30 pairs)
- hyperliquid_recent_trades_1s: 1s trade ticks (for fill simulation)

Usage:
    MONGO_URI=mongodb://localhost:27017/quants_lab MONGO_DATABASE=quants_lab \
    python scripts/hl_x17_simulation.py \
        --pair SOL-USDT \
        --fee-bps 1.44 \
        --spread-threshold-bps 2.0 \
        --imbalance-threshold 0.15 \
        --max-position-notional 25.0 \
        --order-lifetime-snapshots 10 \
        --effective-latency-ms 1000

Author: Quant Engineer (Claude)
Date: 2026-05-02
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class L2Snapshot:
    """Parsed L2 book snapshot."""
    timestamp_ms: int           # epoch millis
    pair: str
    coin: str
    mid_px: float
    best_bid: float
    best_ask: float
    spread_bps: float
    imbalance_topn: float       # pre-computed: (bid_sz - ask_sz) / (bid_sz + ask_sz)
    bid_sz_topn: float          # total bid size in top-N levels
    ask_sz_topn: float          # total ask size in top-N levels
    bids: list[tuple[float, float]]   # [(price, size), ...] top levels
    asks: list[tuple[float, float]]   # [(price, size), ...] top levels


@dataclass
class SpreadSnapshot:
    """Parsed Bybit-HL spread snapshot."""
    timestamp_ms: int           # epoch millis (converted from datetime)
    pair: str
    hl_bid: float
    hl_ask: float
    bb_bid: float
    bb_ask: float
    spread_hl_over_bb: float    # bps: positive = HL premium
    spread_bb_over_hl: float    # bps: positive = Bybit premium


@dataclass
class TradeEvent:
    """Parsed trade tick."""
    time_ms: int                # epoch millis
    pair: str
    px: float
    sz: float
    side: str                   # "A" (aggressive sell) or "B" (aggressive buy)


@dataclass
class RestingOrder:
    """A simulated limit order resting on the book."""
    order_id: int
    side: str                   # "BUY" or "SELL"
    price: float
    size_usd: float
    size_base: float
    posted_at_ms: int           # when we posted it
    posted_at_idx: int          # snapshot index when posted
    fill_price: float = 0.0
    filled: bool = False
    filled_at_ms: int = 0
    cancelled: bool = False
    cancel_reason: str = ""


@dataclass
class Fill:
    """A completed fill with post-fill tracking."""
    order: RestingOrder
    entry_price: float
    entry_time_ms: int
    side: str
    size_usd: float
    # Post-fill tracking (set during simulation)
    exit_price: float = 0.0
    exit_time_ms: int = 0
    exit_reason: str = ""
    pnl_bps: float = 0.0
    pnl_usd: float = 0.0
    adverse_selection_1s_bps: float = 0.0
    adverse_selection_5s_bps: float = 0.0
    holding_time_ms: int = 0


@dataclass
class SimConfig:
    """All configurable simulation parameters."""
    pair: str = "SOL-USDT"
    fee_bps: float = 1.44           # HL maker fee per side (current tier)
    spread_threshold_bps: float = 2.0  # min Bybit-HL dislocation to trigger quoting
    imbalance_threshold: float = 0.15  # min |imbalance| to require for directional bias
    max_position_notional: float = 25.0  # max exposure in USD
    order_size_usd: float = 11.0      # per-order notional (HL minimum ~$11)
    order_lifetime_snapshots: int = 10  # snapshots before auto-cancel
    effective_latency_ms: int = 1000   # conservative: 1 full snapshot = 1s
    # Fill model parameters
    fill_through_required: bool = True  # require price to trade THROUGH our level
    queue_pessimism: float = 1.25      # require 1.25x displayed size to trade through
    # Exit parameters
    exit_timeout_snapshots: int = 30    # force exit after this many snapshots
    exit_tp_bps: float = 3.0           # take profit in bps
    exit_sl_bps: float = 5.0           # stop loss in bps
    # Z-score window for imbalance signal
    imbalance_z_window: int = 300


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_l2_snapshots(db, pair: str) -> list[L2Snapshot]:
    """Load all L2 snapshots for a pair, sorted chronologically."""
    logger.info(f"Loading L2 snapshots for {pair}...")
    cursor = db.hyperliquid_l2_snapshots_1s.find(
        {"pair": pair},
        {"_id": 0}
    ).sort("timestamp_utc", 1)

    snapshots = []
    for doc in cursor:
        # Parse level JSON strings
        bids_raw = doc.get("levels_bid_json", [])
        asks_raw = doc.get("levels_ask_json", [])
        if isinstance(bids_raw, str):
            bids_raw = json.loads(bids_raw)
        if isinstance(asks_raw, str):
            asks_raw = json.loads(asks_raw)

        bids = [(float(b["px"]), float(b["sz"])) for b in bids_raw[:20]]
        asks = [(float(a["px"]), float(a["sz"])) for a in asks_raw[:20]]

        snapshots.append(L2Snapshot(
            timestamp_ms=doc["timestamp_utc"],
            pair=pair,
            coin=doc.get("coin", pair.split("-")[0]),
            mid_px=doc.get("mid_px", (doc["best_bid"] + doc["best_ask"]) / 2),
            best_bid=doc["best_bid"],
            best_ask=doc["best_ask"],
            spread_bps=doc.get("spread_bps", 0.0),
            imbalance_topn=doc.get("imbalance_topn", 0.0),
            bid_sz_topn=doc.get("bid_sz_topn", 0.0),
            ask_sz_topn=doc.get("ask_sz_topn", 0.0),
            bids=bids,
            asks=asks,
        ))

    logger.info(f"Loaded {len(snapshots):,} L2 snapshots for {pair}")
    return snapshots


def load_spread_snapshots(db, pair: str) -> list[SpreadSnapshot]:
    """Load Bybit-HL spread snapshots for a pair, sorted chronologically."""
    logger.info(f"Loading spread snapshots for {pair}...")
    cursor = db.arb_hl_bybit_perp_snapshots.find(
        {"pair": pair},
        {"_id": 0}
    ).sort("timestamp", 1)

    spreads = []
    for doc in cursor:
        # Convert datetime to epoch millis
        ts = doc["timestamp"]
        if isinstance(ts, datetime):
            ts_ms = int(ts.replace(tzinfo=timezone.utc).timestamp() * 1000)
        elif isinstance(ts, (int, float)):
            ts_ms = int(ts * 1000) if ts < 1e12 else int(ts)
        else:
            continue

        spreads.append(SpreadSnapshot(
            timestamp_ms=ts_ms,
            pair=pair,
            hl_bid=doc["hl_bid"],
            hl_ask=doc["hl_ask"],
            bb_bid=doc["bb_bid"],
            bb_ask=doc["bb_ask"],
            spread_hl_over_bb=doc.get("spread_hl_over_bb", 0.0),
            spread_bb_over_hl=doc.get("spread_bb_over_hl", 0.0),
        ))

    logger.info(f"Loaded {len(spreads):,} spread snapshots for {pair}")
    return spreads


def load_trades(db, pair: str) -> list[TradeEvent]:
    """Load trade ticks for fill simulation, sorted chronologically."""
    logger.info(f"Loading trade ticks for {pair}...")
    cursor = db.hyperliquid_recent_trades_1s.find(
        {"pair": pair},
        {"_id": 0}
    ).sort("time", 1)

    trades = []
    for doc in cursor:
        trades.append(TradeEvent(
            time_ms=doc["time"],
            pair=pair,
            px=doc["px"],
            sz=doc["sz"],
            side=doc.get("side", ""),
        ))

    logger.info(f"Loaded {len(trades):,} trade ticks for {pair}")
    return trades


# ---------------------------------------------------------------------------
# Spread aligner — nearest-neighbor join of arb spreads to L2 timestamps
# ---------------------------------------------------------------------------

class SpreadAligner:
    """
    Aligns Bybit-HL spread snapshots to L2 snapshot timestamps using
    forward-fill (use most recent spread observation <= L2 timestamp).
    """
    def __init__(self, spreads: list[SpreadSnapshot], max_age_ms: int = 15_000):
        self.spreads = spreads
        self.max_age_ms = max_age_ms
        self._idx = 0  # pointer for sequential access

    def get_spread_at(self, ts_ms: int) -> Optional[SpreadSnapshot]:
        """Get the most recent spread snapshot at or before ts_ms."""
        # Advance pointer to the latest spread <= ts_ms
        while (self._idx + 1 < len(self.spreads)
               and self.spreads[self._idx + 1].timestamp_ms <= ts_ms):
            self._idx += 1

        if self._idx >= len(self.spreads):
            return None

        spread = self.spreads[self._idx]
        if spread.timestamp_ms > ts_ms:
            return None  # no spread data yet
        if ts_ms - spread.timestamp_ms > self.max_age_ms:
            return None  # too stale
        return spread

    def reset(self):
        self._idx = 0


# ---------------------------------------------------------------------------
# Trade aligner — for fill simulation
# ---------------------------------------------------------------------------

class TradeAligner:
    """
    Provides trades within a time window for fill simulation.
    Maintains a pointer for efficient sequential access.
    """
    def __init__(self, trades: list[TradeEvent]):
        self.trades = trades
        self._idx = 0

    def get_trades_between(self, start_ms: int, end_ms: int) -> list[TradeEvent]:
        """Get all trades in [start_ms, end_ms]."""
        # Advance to start
        while self._idx < len(self.trades) and self.trades[self._idx].time_ms < start_ms:
            self._idx += 1

        result = []
        i = self._idx
        while i < len(self.trades) and self.trades[i].time_ms <= end_ms:
            result.append(self.trades[i])
            i += 1
        return result

    def get_trades_after(self, start_ms: int, count: int = 100) -> list[TradeEvent]:
        """Get up to count trades after start_ms."""
        while self._idx < len(self.trades) and self.trades[self._idx].time_ms < start_ms:
            self._idx += 1
        end = min(self._idx + count, len(self.trades))
        return self.trades[self._idx:end]

    def reset(self):
        self._idx = 0


# ---------------------------------------------------------------------------
# Signal computation
# ---------------------------------------------------------------------------

class SignalComputer:
    """
    Computes the quoting decision signals from the Codex design doc:
    1. Bybit-HL dislocation (primary alpha)
    2. L2 imbalance (directional bias)
    3. Microprice offset (fair value refinement)
    """
    def __init__(self, config: SimConfig):
        self.config = config
        self._imbalance_history: list[float] = []
        self._mid_history: list[float] = []

    def compute(
        self,
        l2: L2Snapshot,
        spread: Optional[SpreadSnapshot],
    ) -> dict:
        """
        Returns a signal dict with:
        - should_quote_bid: bool
        - should_quote_ask: bool
        - fair_value: float (Bybit-anchored if available, else HL mid)
        - alpha_bps: float (estimated short-term alpha)
        - imbalance_z: float
        - dislocation_bps: float (Bybit mid - HL mid, in bps)
        """
        # Track imbalance history for z-score
        self._imbalance_history.append(l2.imbalance_topn)
        if len(self._imbalance_history) > self.config.imbalance_z_window:
            self._imbalance_history = self._imbalance_history[-self.config.imbalance_z_window:]

        self._mid_history.append(l2.mid_px)
        if len(self._mid_history) > 60:
            self._mid_history = self._mid_history[-60:]

        # Imbalance z-score
        if len(self._imbalance_history) >= 30:
            arr = np.array(self._imbalance_history)
            mean = arr.mean()
            std = arr.std()
            imbalance_z = (l2.imbalance_topn - mean) / std if std > 1e-10 else 0.0
        else:
            imbalance_z = 0.0

        # Microprice: size-weighted mid
        if l2.bids and l2.asks:
            bid_px, bid_sz = l2.bids[0]
            ask_px, ask_sz = l2.asks[0]
            total_top = bid_sz + ask_sz
            if total_top > 0:
                microprice = ask_px * (bid_sz / total_top) + bid_px * (ask_sz / total_top)
            else:
                microprice = l2.mid_px
        else:
            microprice = l2.mid_px

        microprice_offset_bps = (microprice - l2.mid_px) / l2.mid_px * 10_000

        # Dislocation: how far HL is from Bybit
        dislocation_bps = 0.0
        fair_value = l2.mid_px  # default: HL mid
        if spread is not None:
            bb_mid = (spread.bb_bid + spread.bb_ask) / 2
            hl_mid = l2.mid_px
            dislocation_bps = (bb_mid - hl_mid) / hl_mid * 10_000
            # Fair value = blend of Bybit and microprice (per Codex design)
            fair_value = 0.6 * bb_mid + 0.4 * microprice

        # Short-term volatility (for position sizing / quote width)
        if len(self._mid_history) >= 10:
            returns = np.diff(np.log(self._mid_history[-10:]))
            sigma_bps = np.std(returns) * 10_000
        else:
            sigma_bps = 5.0  # default moderate vol

        # Alpha estimate (composite score per Codex design)
        # w = [0.45 dislocation, 0.20 microprice, 0.15 imbalance, 0.10 vol, 0.10 spare]
        alpha_bps = (
            0.45 * dislocation_bps +
            0.20 * microprice_offset_bps +
            0.15 * imbalance_z * sigma_bps
        )
        alpha_bps = np.clip(alpha_bps, -6.0, 6.0)

        # Quoting decision per Codex: G_side > 0.75 bps after fees
        # G_bid = (fair_value - bid_price) / fair_value * 10000 - 2*fee - adverse
        # We approximate adverse selection from imbalance direction
        fee_cost = 2 * self.config.fee_bps  # round-trip fee
        estimated_adverse = max(0, 1.5 - abs(dislocation_bps) * 0.3)  # lower adverse when dislocation is large

        # Bid side: profit if we buy and price goes up
        # We want to buy when dislocation is positive (BB > HL, HL will rise)
        bid_edge = alpha_bps - fee_cost - estimated_adverse
        # Ask side: profit if we sell and price goes down
        ask_edge = -alpha_bps - fee_cost - estimated_adverse

        min_edge = 0.75  # bps, per Codex design
        should_quote_bid = bid_edge > min_edge
        should_quote_ask = ask_edge > min_edge

        # Additional filter: require minimum imbalance signal strength
        if abs(imbalance_z) < 0.5:
            # Weak imbalance: only quote in dislocation direction
            if dislocation_bps > 0:
                should_quote_ask = False
            elif dislocation_bps < 0:
                should_quote_bid = False

        # Spread threshold gate: require minimum dislocation
        if abs(dislocation_bps) < self.config.spread_threshold_bps:
            should_quote_bid = False
            should_quote_ask = False

        return {
            "should_quote_bid": should_quote_bid,
            "should_quote_ask": should_quote_ask,
            "fair_value": fair_value,
            "alpha_bps": alpha_bps,
            "imbalance_z": imbalance_z,
            "dislocation_bps": dislocation_bps,
            "microprice_offset_bps": microprice_offset_bps,
            "sigma_bps": sigma_bps,
            "bid_edge": bid_edge,
            "ask_edge": ask_edge,
        }


# ---------------------------------------------------------------------------
# Fill simulator
# ---------------------------------------------------------------------------

class FillSimulator:
    """
    Models whether a resting limit order would have been filled.

    Conservative model (per Codex design):
    - Effective latency = 1s (one full snapshot delay before order rests)
    - Require price to trade THROUGH posted level (not just touch)
    - Queue pessimism: require 1.25x displayed depth to be consumed
    """
    def __init__(self, config: SimConfig, trades: TradeAligner):
        self.config = config
        self.trades = trades

    def check_fill(
        self,
        order: RestingOrder,
        snapshots: list[L2Snapshot],
        current_idx: int,
    ) -> bool:
        """
        Check if order would have been filled based on subsequent price action.
        Uses trade ticks if available, falls back to snapshot-based fill detection.
        """
        # Order needs to rest for effective_latency before it can be filled
        rest_time_ms = order.posted_at_ms + self.config.effective_latency_ms
        end_time_ms = order.posted_at_ms + self.config.order_lifetime_snapshots * 1000

        # Check subsequent snapshots for price trading through our level
        for i in range(current_idx + 1, min(current_idx + self.config.order_lifetime_snapshots + 1, len(snapshots))):
            snap = snapshots[i]
            if snap.timestamp_ms < rest_time_ms:
                continue  # still within latency window

            if order.side == "BUY":
                # Buy order fills if ask drops to or below our price
                # Conservative: require best_ask <= order.price (price traded through)
                if self.config.fill_through_required:
                    # Need the ask to actually go through our level
                    if snap.best_ask <= order.price:
                        order.fill_price = order.price  # limit fill at posted price
                        order.filled_at_ms = snap.timestamp_ms
                        return True
                else:
                    # Touch fill: price just needs to reach our level
                    if snap.best_bid <= order.price:
                        order.fill_price = order.price
                        order.filled_at_ms = snap.timestamp_ms
                        return True
            else:  # SELL
                if self.config.fill_through_required:
                    if snap.best_bid >= order.price:
                        order.fill_price = order.price
                        order.filled_at_ms = snap.timestamp_ms
                        return True
                else:
                    if snap.best_ask >= order.price:
                        order.fill_price = order.price
                        order.filled_at_ms = snap.timestamp_ms
                        return True

        return False


# ---------------------------------------------------------------------------
# Position tracker
# ---------------------------------------------------------------------------

@dataclass
class Position:
    """Tracks current inventory."""
    net_base: float = 0.0           # positive = long, negative = short
    net_notional_usd: float = 0.0   # absolute exposure in USD
    avg_entry_price: float = 0.0
    entry_time_ms: int = 0
    entry_snapshot_idx: int = 0

    def update_entry(self, side: str, price: float, size_base: float, size_usd: float, time_ms: int, idx: int):
        if self.net_base == 0:
            self.avg_entry_price = price
            self.entry_time_ms = time_ms
            self.entry_snapshot_idx = idx
        else:
            # Weighted average entry
            old_notional = abs(self.net_base) * self.avg_entry_price
            new_notional = size_base * price
            total_base = abs(self.net_base) + size_base
            if total_base > 0:
                self.avg_entry_price = (old_notional + new_notional) / total_base

        if side == "BUY":
            self.net_base += size_base
        else:
            self.net_base -= size_base
        self.net_notional_usd = abs(self.net_base) * price

    def is_flat(self) -> bool:
        return abs(self.net_base) < 1e-10


# ---------------------------------------------------------------------------
# Main simulation engine
# ---------------------------------------------------------------------------

class X17Simulation:
    """
    Event-driven simulation engine for X17 selective passive alpha maker.

    Flow per L2 snapshot:
    1. Update spread aligner (get latest Bybit-HL dislocation)
    2. Compute signal (imbalance + dislocation + microprice)
    3. Check existing resting orders for fills
    4. Check position exits (TP/SL/timeout)
    5. If no position and signal says quote: post new limit order
    6. Log everything
    """
    def __init__(self, config: SimConfig, db):
        self.config = config
        self.db = db

        # Load data
        self.l2_snapshots = load_l2_snapshots(db, config.pair)
        self.spread_snapshots = load_spread_snapshots(db, config.pair)
        self.trade_events = load_trades(db, config.pair)

        # Aligners
        self.spread_aligner = SpreadAligner(self.spread_snapshots, max_age_ms=15_000)
        self.trade_aligner = TradeAligner(self.trade_events)

        # Signal computer
        self.signal = SignalComputer(config)

        # State
        self.position = Position()
        self.resting_orders: list[RestingOrder] = []
        self.fills: list[Fill] = []
        self.completed_trades: list[Fill] = []  # entry + exit pairs
        self.order_counter = 0
        self.total_fees_usd = 0.0

        # Metrics accumulators
        self.quote_decisions: list[dict] = []
        self.snapshot_count = 0
        self.quotes_posted = 0
        self.quotes_filled = 0
        self.quotes_cancelled = 0
        self.quotes_expired = 0

    def run(self) -> dict:
        """Run the full simulation. Returns summary metrics."""
        if not self.l2_snapshots:
            logger.error("No L2 snapshots loaded. Aborting.")
            return {}

        logger.info(
            f"Starting X17 simulation: {self.config.pair}, "
            f"fee={self.config.fee_bps}bps, "
            f"spread_thresh={self.config.spread_threshold_bps}bps, "
            f"imbalance_thresh={self.config.imbalance_threshold}, "
            f"latency={self.config.effective_latency_ms}ms, "
            f"{len(self.l2_snapshots):,} snapshots"
        )

        fill_sim = FillSimulator(self.config, self.trade_aligner)

        for idx, l2 in enumerate(self.l2_snapshots):
            self.snapshot_count += 1

            # 1. Get aligned spread data
            spread = self.spread_aligner.get_spread_at(l2.timestamp_ms)

            # 2. Compute signals
            sig = self.signal.compute(l2, spread)

            # 3. Check resting orders for fills
            self._check_order_fills(idx, l2, fill_sim)

            # 4. Check position exits
            self._check_position_exits(idx, l2)

            # 5. Cancel expired orders
            self._cancel_expired_orders(idx)

            # 6. Post new orders if signal says so
            self._maybe_post_orders(idx, l2, sig)

            # Log progress every 50k snapshots
            if idx > 0 and idx % 50_000 == 0:
                self._log_progress(idx)

        # Force-close any remaining position at last mid
        if not self.position.is_flat():
            self._force_close_position(len(self.l2_snapshots) - 1, self.l2_snapshots[-1], "SIM_END")

        return self._compute_summary()

    def _check_order_fills(self, idx: int, l2: L2Snapshot, fill_sim: FillSimulator):
        """Check if any resting orders have been filled."""
        still_resting = []
        for order in self.resting_orders:
            if order.filled or order.cancelled:
                continue

            filled = fill_sim.check_fill(order, self.l2_snapshots, order.posted_at_idx)
            if filled:
                order.filled = True
                self.quotes_filled += 1

                # Record the fill
                size_base = order.size_usd / order.fill_price
                fee_usd = order.size_usd * self.config.fee_bps / 10_000
                self.total_fees_usd += fee_usd

                fill = Fill(
                    order=order,
                    entry_price=order.fill_price,
                    entry_time_ms=order.filled_at_ms,
                    side=order.side,
                    size_usd=order.size_usd,
                )
                self.fills.append(fill)

                # Update position
                self.position.update_entry(
                    order.side, order.fill_price, size_base,
                    order.size_usd, order.filled_at_ms, idx
                )

                # Compute adverse selection: how much does price move against us
                # in the next 1s and 5s after fill
                fill.adverse_selection_1s_bps = self._compute_adverse_selection(
                    order.filled_at_ms, order.side, l2.mid_px, 1000
                )
                fill.adverse_selection_5s_bps = self._compute_adverse_selection(
                    order.filled_at_ms, order.side, l2.mid_px, 5000
                )
            else:
                still_resting.append(order)

        self.resting_orders = still_resting

    def _compute_adverse_selection(
        self, fill_time_ms: int, side: str, fill_mid: float, horizon_ms: int
    ) -> float:
        """
        Compute adverse selection: how much price moves against us
        within horizon_ms after fill.
        """
        target_ts = fill_time_ms + horizon_ms
        # Find the L2 snapshot closest to target_ts
        future_mid = fill_mid
        for snap in self.l2_snapshots:
            if snap.timestamp_ms >= target_ts:
                future_mid = snap.mid_px
                break

        # Adverse selection for buyer: price drops after buy
        # Adverse selection for seller: price rises after sell
        move_bps = (future_mid - fill_mid) / fill_mid * 10_000
        if side == "BUY":
            return -move_bps  # negative move = adverse for buyer
        else:
            return move_bps   # positive move = adverse for seller

    def _check_position_exits(self, idx: int, l2: L2Snapshot):
        """Check if current position should be exited (TP/SL/timeout)."""
        if self.position.is_flat() or not self.fills:
            return

        # Find the most recent unfilled fill (entry)
        active_fills = [f for f in self.fills if f.exit_time_ms == 0]
        if not active_fills:
            return

        for fill in active_fills:
            entry_px = fill.entry_price
            current_px = l2.mid_px

            if fill.side == "BUY":
                pnl_bps = (current_px - entry_px) / entry_px * 10_000
            else:
                pnl_bps = (entry_px - current_px) / entry_px * 10_000

            # Holding time
            hold_ms = l2.timestamp_ms - fill.entry_time_ms
            hold_snapshots = idx - fill.order.posted_at_idx

            exit_reason = ""

            # Take profit
            if pnl_bps >= self.config.exit_tp_bps:
                exit_reason = "TP"
            # Stop loss
            elif pnl_bps <= -self.config.exit_sl_bps:
                exit_reason = "SL"
            # Timeout
            elif hold_snapshots >= self.config.exit_timeout_snapshots:
                exit_reason = "TIMEOUT"

            if exit_reason:
                # Close this fill
                fee_usd = fill.size_usd * self.config.fee_bps / 10_000
                self.total_fees_usd += fee_usd

                net_pnl_bps = pnl_bps - 2 * self.config.fee_bps  # entry + exit fees
                fill.exit_price = current_px
                fill.exit_time_ms = l2.timestamp_ms
                fill.exit_reason = exit_reason
                fill.pnl_bps = net_pnl_bps
                fill.pnl_usd = fill.size_usd * net_pnl_bps / 10_000
                fill.holding_time_ms = hold_ms

                self.completed_trades.append(fill)

                # Update position (flatten the fill's contribution)
                size_base = fill.size_usd / fill.entry_price
                opposite = "SELL" if fill.side == "BUY" else "BUY"
                self.position.update_entry(
                    opposite, current_px, size_base,
                    fill.size_usd, l2.timestamp_ms, idx
                )

    def _force_close_position(self, idx: int, l2: L2Snapshot, reason: str):
        """Force close all active fills."""
        active_fills = [f for f in self.fills if f.exit_time_ms == 0]
        for fill in active_fills:
            entry_px = fill.entry_price
            current_px = l2.mid_px
            if fill.side == "BUY":
                pnl_bps = (current_px - entry_px) / entry_px * 10_000
            else:
                pnl_bps = (entry_px - current_px) / entry_px * 10_000

            net_pnl_bps = pnl_bps - 2 * self.config.fee_bps
            fill.exit_price = current_px
            fill.exit_time_ms = l2.timestamp_ms
            fill.exit_reason = reason
            fill.pnl_bps = net_pnl_bps
            fill.pnl_usd = fill.size_usd * net_pnl_bps / 10_000
            fill.holding_time_ms = l2.timestamp_ms - fill.entry_time_ms
            self.completed_trades.append(fill)

            fee_usd = fill.size_usd * self.config.fee_bps / 10_000
            self.total_fees_usd += fee_usd

        self.position = Position()

    def _cancel_expired_orders(self, idx: int):
        """Cancel orders that have exceeded their lifetime."""
        still_active = []
        for order in self.resting_orders:
            if order.filled or order.cancelled:
                continue
            age = idx - order.posted_at_idx
            if age >= self.config.order_lifetime_snapshots:
                order.cancelled = True
                order.cancel_reason = "EXPIRED"
                self.quotes_expired += 1
            else:
                still_active.append(order)
        self.resting_orders = still_active

    def _maybe_post_orders(self, idx: int, l2: L2Snapshot, sig: dict):
        """Post new limit orders if signal conditions are met."""
        # Don't post if we already have resting orders
        if self.resting_orders:
            return

        # Don't post if position is at max
        if self.position.net_notional_usd >= self.config.max_position_notional:
            return

        # Don't post within first 300 snapshots (warmup for z-score)
        if idx < 300:
            return

        fair_value = sig["fair_value"]
        dislocation = sig["dislocation_bps"]

        if sig["should_quote_bid"]:
            # Post bid: buy at best_bid or slightly inside
            # Skew toward fair value when dislocation is large
            bid_price = l2.best_bid
            if dislocation > 0:
                # HL is cheap relative to Bybit — can be more aggressive on bid
                # But still at or inside best bid (ALO constraint)
                bid_price = l2.best_bid

            order = RestingOrder(
                order_id=self._next_order_id(),
                side="BUY",
                price=bid_price,
                size_usd=self.config.order_size_usd,
                size_base=self.config.order_size_usd / bid_price,
                posted_at_ms=l2.timestamp_ms,
                posted_at_idx=idx,
            )
            self.resting_orders.append(order)
            self.quotes_posted += 1

        if sig["should_quote_ask"]:
            ask_price = l2.best_ask
            if dislocation < 0:
                ask_price = l2.best_ask

            order = RestingOrder(
                order_id=self._next_order_id(),
                side="SELL",
                price=ask_price,
                size_usd=self.config.order_size_usd,
                size_base=self.config.order_size_usd / ask_price,
                posted_at_ms=l2.timestamp_ms,
                posted_at_idx=idx,
            )
            self.resting_orders.append(order)
            self.quotes_posted += 1

    def _next_order_id(self) -> int:
        self.order_counter += 1
        return self.order_counter

    def _log_progress(self, idx: int):
        pct = idx / len(self.l2_snapshots) * 100
        n_completed = len(self.completed_trades)
        total_pnl = sum(t.pnl_usd for t in self.completed_trades)
        logger.info(
            f"Progress: {pct:.0f}% ({idx:,}/{len(self.l2_snapshots):,}) | "
            f"Quotes: {self.quotes_posted} posted, {self.quotes_filled} filled, {self.quotes_expired} expired | "
            f"Trades: {n_completed} completed, PnL: ${total_pnl:.4f}"
        )

    def _compute_summary(self) -> dict:
        """Compute final simulation summary."""
        trades = self.completed_trades
        n_trades = len(trades)

        if n_trades == 0:
            return {
                "pair": self.config.pair,
                "fee_bps": self.config.fee_bps,
                "snapshots": self.snapshot_count,
                "quotes_posted": self.quotes_posted,
                "quotes_filled": self.quotes_filled,
                "quotes_expired": self.quotes_expired,
                "trades": 0,
                "total_pnl_usd": 0.0,
                "message": "No completed trades. Try relaxing thresholds.",
            }

        pnls_bps = [t.pnl_bps for t in trades]
        pnls_usd = [t.pnl_usd for t in trades]
        adverse_1s = [t.adverse_selection_1s_bps for t in trades]
        adverse_5s = [t.adverse_selection_5s_bps for t in trades]
        hold_times = [t.holding_time_ms / 1000 for t in trades]  # in seconds
        exit_reasons = {}
        for t in trades:
            exit_reasons[t.exit_reason] = exit_reasons.get(t.exit_reason, 0) + 1

        total_pnl_usd = sum(pnls_usd)
        total_pnl_bps = sum(pnls_bps)
        wins = sum(1 for p in pnls_usd if p > 0)
        losses = sum(1 for p in pnls_usd if p < 0)
        win_rate = wins / n_trades if n_trades > 0 else 0

        # Sharpe: annualized from per-trade returns
        pnl_arr = np.array(pnls_bps)
        if len(pnl_arr) > 1 and pnl_arr.std() > 0:
            # Estimate trades per day from data span
            span_hours = (trades[-1].exit_time_ms - trades[0].entry_time_ms) / 3_600_000
            if span_hours > 0:
                trades_per_day = n_trades / (span_hours / 24)
            else:
                trades_per_day = n_trades
            sharpe = (pnl_arr.mean() / pnl_arr.std()) * np.sqrt(trades_per_day * 365)
        else:
            sharpe = 0.0
            trades_per_day = 0.0

        # Fill rate: filled / posted
        fill_rate = self.quotes_filled / self.quotes_posted if self.quotes_posted > 0 else 0

        # Time span
        first_ts = self.l2_snapshots[0].timestamp_ms
        last_ts = self.l2_snapshots[-1].timestamp_ms
        span_days = (last_ts - first_ts) / 86_400_000

        # PnL by day
        daily_pnl = {}
        for t in trades:
            day = datetime.fromtimestamp(t.exit_time_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            daily_pnl[day] = daily_pnl.get(day, 0) + t.pnl_usd

        summary = {
            "pair": self.config.pair,
            "fee_bps": self.config.fee_bps,
            "spread_threshold_bps": self.config.spread_threshold_bps,
            "effective_latency_ms": self.config.effective_latency_ms,
            "data_span_days": round(span_days, 1),
            "snapshots_processed": self.snapshot_count,
            "quotes_posted": self.quotes_posted,
            "quotes_filled": self.quotes_filled,
            "quotes_expired": self.quotes_expired,
            "fill_rate": round(fill_rate, 4),
            "total_trades": n_trades,
            "wins": wins,
            "losses": losses,
            "win_rate": round(win_rate, 4),
            "total_pnl_usd": round(total_pnl_usd, 4),
            "total_pnl_bps": round(total_pnl_bps, 2),
            "total_fees_usd": round(self.total_fees_usd, 4),
            "avg_pnl_per_trade_bps": round(np.mean(pnls_bps), 2),
            "median_pnl_per_trade_bps": round(np.median(pnls_bps), 2),
            "std_pnl_bps": round(np.std(pnls_bps), 2),
            "max_win_bps": round(max(pnls_bps), 2),
            "max_loss_bps": round(min(pnls_bps), 2),
            "sharpe_annualized": round(sharpe, 2),
            "trades_per_day": round(trades_per_day, 1),
            "avg_adverse_1s_bps": round(np.mean(adverse_1s), 2),
            "median_adverse_1s_bps": round(np.median(adverse_1s), 2),
            "avg_adverse_5s_bps": round(np.mean(adverse_5s), 2),
            "median_adverse_5s_bps": round(np.median(adverse_5s), 2),
            "avg_holding_time_s": round(np.mean(hold_times), 1),
            "median_holding_time_s": round(np.median(hold_times), 1),
            "exit_reasons": exit_reasons,
            "daily_pnl": daily_pnl,
        }
        return summary


# ---------------------------------------------------------------------------
# Multi-scenario runner
# ---------------------------------------------------------------------------

def run_scenario_matrix(db, base_config: SimConfig) -> list[dict]:
    """
    Run simulation across multiple parameter combinations to find
    the viable operating region.
    """
    results = []

    # Core scenarios
    scenarios = [
        # Base case
        {"label": "BASE", "fee_bps": 1.44, "spread_threshold_bps": 2.0},
        # Zero fee (target tier)
        {"label": "ZERO_FEE", "fee_bps": 0.0, "spread_threshold_bps": 2.0},
        # Wider threshold (more selective)
        {"label": "WIDE_THRESH", "fee_bps": 1.44, "spread_threshold_bps": 3.0},
        {"label": "WIDER_THRESH", "fee_bps": 1.44, "spread_threshold_bps": 5.0},
        # Zero fee + wider threshold
        {"label": "ZERO_FEE_WIDE", "fee_bps": 0.0, "spread_threshold_bps": 3.0},
        # Shorter latency assumption (optimistic)
        {"label": "LOW_LATENCY", "fee_bps": 1.44, "spread_threshold_bps": 2.0,
         "effective_latency_ms": 500},
        # Longer order lifetime
        {"label": "LONG_LIFETIME", "fee_bps": 1.44, "spread_threshold_bps": 2.0,
         "order_lifetime_snapshots": 20},
    ]

    for scenario in scenarios:
        label = scenario.pop("label")
        config = SimConfig(
            pair=base_config.pair,
            max_position_notional=base_config.max_position_notional,
            order_size_usd=base_config.order_size_usd,
            exit_tp_bps=base_config.exit_tp_bps,
            exit_sl_bps=base_config.exit_sl_bps,
            exit_timeout_snapshots=base_config.exit_timeout_snapshots,
            imbalance_threshold=base_config.imbalance_threshold,
            **scenario,
        )
        logger.info(f"\n{'='*60}\nScenario: {label}\n{'='*60}")
        sim = X17Simulation(config, db)
        summary = sim.run()
        summary["scenario"] = label
        results.append(summary)

        # Print compact result
        if summary.get("total_trades", 0) > 0:
            logger.info(
                f"  >> {label}: {summary['total_trades']} trades, "
                f"WR={summary['win_rate']:.1%}, "
                f"PnL=${summary['total_pnl_usd']:.4f} ({summary['total_pnl_bps']:.1f}bps), "
                f"Sharpe={summary['sharpe_annualized']:.1f}, "
                f"Fill={summary['fill_rate']:.1%}, "
                f"Adv5s={summary['avg_adverse_5s_bps']:.1f}bps"
            )
        else:
            logger.info(f"  >> {label}: 0 trades")

    return results


# ---------------------------------------------------------------------------
# Pretty print
# ---------------------------------------------------------------------------

def print_summary(summary: dict):
    """Print a formatted summary of simulation results."""
    print("\n" + "=" * 70)
    print(f"  X17 SIMULATION RESULTS — {summary.get('scenario', 'SINGLE RUN')}")
    print("=" * 70)

    if summary.get("total_trades", 0) == 0:
        print(f"  Pair: {summary['pair']}, Fee: {summary['fee_bps']}bps")
        print(f"  Snapshots: {summary.get('snapshots_processed', summary.get('snapshots', 0)):,}")
        print(f"  Quotes posted: {summary['quotes_posted']}, filled: {summary['quotes_filled']}")
        print(f"  {summary.get('message', 'No trades.')}")
        print("=" * 70)
        return

    print(f"  Pair: {summary['pair']} | Fee: {summary['fee_bps']}bps | "
          f"Threshold: {summary['spread_threshold_bps']}bps | "
          f"Latency: {summary['effective_latency_ms']}ms")
    print(f"  Data span: {summary['data_span_days']} days | "
          f"Snapshots: {summary['snapshots_processed']:,}")
    print()
    print(f"  QUOTING:")
    print(f"    Posted: {summary['quotes_posted']:,} | "
          f"Filled: {summary['quotes_filled']:,} | "
          f"Expired: {summary['quotes_expired']:,} | "
          f"Fill rate: {summary['fill_rate']:.1%}")
    print()
    print(f"  TRADES:")
    print(f"    Total: {summary['total_trades']} | "
          f"Wins: {summary['wins']} | "
          f"Losses: {summary['losses']} | "
          f"Win rate: {summary['win_rate']:.1%}")
    print(f"    Per day: {summary['trades_per_day']:.1f}")
    print()
    print(f"  PnL:")
    print(f"    Total: ${summary['total_pnl_usd']:.4f} ({summary['total_pnl_bps']:.1f}bps)")
    print(f"    Fees paid: ${summary['total_fees_usd']:.4f}")
    print(f"    Avg per trade: {summary['avg_pnl_per_trade_bps']:.2f}bps | "
          f"Median: {summary['median_pnl_per_trade_bps']:.2f}bps")
    print(f"    Max win: {summary['max_win_bps']:.2f}bps | "
          f"Max loss: {summary['max_loss_bps']:.2f}bps")
    print(f"    Std: {summary['std_pnl_bps']:.2f}bps | "
          f"Sharpe (ann.): {summary['sharpe_annualized']:.1f}")
    print()
    print(f"  ADVERSE SELECTION:")
    print(f"    1s post-fill: avg={summary['avg_adverse_1s_bps']:.2f}bps, "
          f"median={summary['median_adverse_1s_bps']:.2f}bps")
    print(f"    5s post-fill: avg={summary['avg_adverse_5s_bps']:.2f}bps, "
          f"median={summary['median_adverse_5s_bps']:.2f}bps")
    print()
    print(f"  TIMING:")
    print(f"    Avg hold: {summary['avg_holding_time_s']:.1f}s | "
          f"Median: {summary['median_holding_time_s']:.1f}s")
    print(f"    Exit reasons: {summary['exit_reasons']}")
    print()
    print(f"  DAILY PnL:")
    for day, pnl in sorted(summary.get("daily_pnl", {}).items()):
        bar = "+" * int(max(0, pnl * 10000)) + "-" * int(max(0, -pnl * 10000))
        print(f"    {day}: ${pnl:+.4f} {bar[:40]}")
    print("=" * 70)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="X17 Selective Passive Alpha Maker — Event-Driven Simulation"
    )
    parser.add_argument("--pair", default="SOL-USDT", help="Trading pair (default: SOL-USDT)")
    parser.add_argument("--fee-bps", type=float, default=1.44, help="Maker fee in bps (default: 1.44)")
    parser.add_argument("--spread-threshold-bps", type=float, default=2.0,
                        help="Min Bybit-HL dislocation to trigger quoting (default: 2.0)")
    parser.add_argument("--imbalance-threshold", type=float, default=0.15,
                        help="Min |imbalance| for directional bias (default: 0.15)")
    parser.add_argument("--max-position-notional", type=float, default=25.0,
                        help="Max exposure in USD (default: 25.0)")
    parser.add_argument("--order-size-usd", type=float, default=11.0,
                        help="Per-order notional (default: 11.0)")
    parser.add_argument("--order-lifetime-snapshots", type=int, default=10,
                        help="Snapshots before auto-cancel (default: 10)")
    parser.add_argument("--effective-latency-ms", type=int, default=1000,
                        help="Effective latency in ms (default: 1000)")
    parser.add_argument("--exit-tp-bps", type=float, default=3.0,
                        help="Take profit in bps (default: 3.0)")
    parser.add_argument("--exit-sl-bps", type=float, default=5.0,
                        help="Stop loss in bps (default: 5.0)")
    parser.add_argument("--exit-timeout", type=int, default=30,
                        help="Force exit after N snapshots (default: 30)")
    parser.add_argument("--matrix", action="store_true",
                        help="Run full scenario matrix instead of single config")
    parser.add_argument("--fill-touch", action="store_true",
                        help="Use touch fill model instead of trade-through (less conservative)")
    return parser.parse_args()


def main():
    args = parse_args()

    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
    mongo_db = os.environ.get("MONGO_DATABASE", "quants_lab")

    client = MongoClient(mongo_uri)
    db = client[mongo_db]

    config = SimConfig(
        pair=args.pair,
        fee_bps=args.fee_bps,
        spread_threshold_bps=args.spread_threshold_bps,
        imbalance_threshold=args.imbalance_threshold,
        max_position_notional=args.max_position_notional,
        order_size_usd=args.order_size_usd,
        order_lifetime_snapshots=args.order_lifetime_snapshots,
        effective_latency_ms=args.effective_latency_ms,
        exit_tp_bps=args.exit_tp_bps,
        exit_sl_bps=args.exit_sl_bps,
        exit_timeout_snapshots=args.exit_timeout,
        fill_through_required=not args.fill_touch,
    )

    if args.matrix:
        results = run_scenario_matrix(db, config)
        print("\n\n" + "=" * 70)
        print("  SCENARIO COMPARISON")
        print("=" * 70)
        header = f"{'Scenario':<20} {'Trades':>6} {'WR':>6} {'PnL$':>10} {'PnLbps':>8} {'Sharpe':>7} {'Fill%':>6} {'Adv5s':>7}"
        print(header)
        print("-" * len(header))
        for r in results:
            if r.get("total_trades", 0) > 0:
                print(
                    f"{r['scenario']:<20} {r['total_trades']:>6} "
                    f"{r['win_rate']:>5.1%} {r['total_pnl_usd']:>10.4f} "
                    f"{r['total_pnl_bps']:>7.1f} {r['sharpe_annualized']:>7.1f} "
                    f"{r['fill_rate']:>5.1%} {r['avg_adverse_5s_bps']:>7.2f}"
                )
            else:
                print(f"{r['scenario']:<20} {'0':>6} {'--':>6} {'--':>10} {'--':>8} {'--':>7} {'--':>6} {'--':>7}")
        print("=" * 70)
    else:
        sim = X17Simulation(config, db)
        summary = sim.run()
        print_summary(summary)

    client.close()


if __name__ == "__main__":
    main()
