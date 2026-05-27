"""
HL-Bybit Perp/Perp Spread Arb V1 — Dry Run Prototype.

Delta-neutral spread arb: SHORT on expensive venue, LONG on cheap venue.
When spread reverts, close both positions.

Signal logic: H2-style adaptive P90/P25 thresholds.
- Entry: spread > P90 AND excess (P90-P25) > fee threshold
- Exit: spread reverts below P25

Venues: Hyperliquid perp + Bybit perp (both taker).
Fee RT: HL 4.32bp + Bybit 5.5bp = 9.82bp per side × 2 sides = 19.64bp total.

Mode:
  --dry-run (default): signal detection + logging only
  --paper: signals + Bybit demo execution (no real money)
  --live: both venues live (requires explicit --confirm-live)

Usage:
    set -a && source .env && set +a
    python scripts/arb_hl_bybit_perp_v1.py --dry-run
    python scripts/arb_hl_bybit_perp_v1.py --dry-run --pairs CHIP-USDT,AXS-USDT,APE-USDT
"""
import argparse
import logging
import os
import signal
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests as req_lib
import numpy as np
from pymongo import MongoClient

# ── Setup ──────────────────────────────────────────────────────────

_env_file = Path(__file__).resolve().parents[1] / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))

logger = logging.getLogger("arb_hl_bybit_perp")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
_fh = logging.FileHandler("/tmp/arb-hl-bybit-perp-v1.log", mode="w")
_fh.setFormatter(_fmt)
logger.addHandler(_sh)
logger.addHandler(_fh)

# ── Config ─────────────────────────────────────────────────────────

POLL_INTERVAL_S = 1.5          # Price poll interval
THRESHOLD_WINDOW = 720         # Rolling window for P90/P25 (720 × 1.5s ≈ 18 min)
MIN_WARMUP = 360               # Minimum observations before trading (360 × 1.5s ≈ 9 min)
ENTRY_PERCENTILE = 90          # Entry: spread > P90
EXIT_PERCENTILE = 25           # Exit: spread reverts to P25
FEE_RT_BPS = 19.64             # Total round-trip fees (HL + Bybit, both sides)
MIN_EXCESS_BPS = 25.0          # P90 - P25 must exceed this (> fee RT for profitability)
STOP_LOSS_MULTIPLE = 2.5       # Spread > 2.5× entry = stop loss
MAX_HOLD_S = 3600              # Max hold time (1h)
MIN_ENTRY_TICKS = 2            # Spread must be above P90 for N consecutive ticks
POSITION_USD = 100.0           # Position size per side (dry run)
MAX_CONCURRENT = 3             # Max simultaneous arb positions

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")

# Default pairs to monitor (top performers from backtest)
DEFAULT_PAIRS = [
    "APE-USDT", "CHIP-USDT", "AXS-USDT", "FARTCOIN-USDT",
    "HYPE-USDT", "AAVE-USDT", "OP-USDT", "DYDX-USDT",
    "PENGU-USDT", "IP-USDT",
]

# ── Data Types ─────────────────────────────────────────────────────

@dataclass
class VenueQuote:
    """Best bid/ask from a venue."""
    bid: float = 0.0
    ask: float = 0.0
    ts: float = 0.0  # local time of update


@dataclass
class SpreadSnapshot:
    """Cross-venue spread at a point in time."""
    pair: str
    hl_bid: float
    hl_ask: float
    bb_bid: float
    bb_ask: float
    spread_hl_over_bb: float  # bps: (hl_bid - bb_ask) / bb_ask * 10000
    spread_bb_over_hl: float  # bps: (bb_bid - hl_ask) / hl_ask * 10000
    best_spread: float        # max of the two (positive = arb exists)
    direction: str            # "HL_PREMIUM" or "BB_PREMIUM"
    ts: float


@dataclass
class ArbPosition:
    """Open arb position."""
    pair: str
    direction: str            # "SHORT_HL_LONG_BB" or "SHORT_BB_LONG_HL"
    entry_spread: float       # spread at entry (bps)
    entry_time: float
    entry_p90: float
    exit_p25: float
    hl_entry_price: float
    bb_entry_price: float


# ── Adaptive Thresholds ────────────────────────────────────────────

class AdaptiveThresholds:
    """Rolling percentile thresholds from spread history."""

    def __init__(self, window: int = THRESHOLD_WINDOW):
        self.window = window
        self.history: dict[str, deque] = defaultdict(lambda: deque(maxlen=window))

    def update(self, pair: str, spread_bps: float):
        self.history[pair].append(abs(spread_bps))

    def ready(self, pair: str) -> bool:
        return len(self.history[pair]) >= MIN_WARMUP

    def get_thresholds(self, pair: str) -> Optional[dict]:
        h = self.history[pair]
        if len(h) < MIN_WARMUP:
            return None
        arr = np.array(h)
        p25 = float(np.percentile(arr, EXIT_PERCENTILE))
        p50 = float(np.percentile(arr, 50))
        p90 = float(np.percentile(arr, ENTRY_PERCENTILE))
        excess = p90 - p25
        return {
            "p25": p25, "median": p50, "p90": p90,
            "excess": excess, "viable": excess > MIN_EXCESS_BPS,
        }

    def seed_from_mongodb(self, pairs: list[str]):
        """Bootstrap from historical HL-Bybit spread snapshots."""
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            db_name = MONGO_URI.rsplit("/", 1)[-1]
            db = client[db_name]
            col = db["arb_hl_bybit_perp_snapshots"]

            for pair in pairs:
                docs = list(col.find(
                    {"pair": pair},
                    {"best_spread": 1, "_id": 0}
                ).sort("timestamp", -1).limit(self.window))

                if docs:
                    for d in reversed(docs):
                        self.history[pair].append(abs(d.get("best_spread", 0)))
                    logger.info(f"  {pair}: seeded {len(docs)} snapshots from MongoDB")
                else:
                    logger.warning(f"  {pair}: no historical data")
            client.close()
        except Exception as e:
            logger.error(f"Failed to seed from MongoDB: {e}")


# ── Price Feed ─────────────────────────────────────────────────────

class PerpPriceFeed:
    """Poll HL + Bybit for best bid/ask prices (sync)."""

    def __init__(self, pairs: list[str]):
        self.pairs = pairs

    def poll(self) -> list[SpreadSnapshot]:
        """Fetch prices from both venues and compute spreads."""
        snapshots = []

        try:
            hl_mids = req_lib.post(
                "https://api.hyperliquid.xyz/info",
                json={"type": "allMids"}, timeout=5
            ).json()

            bb_data = req_lib.get(
                "https://api.bybit.com/v5/market/tickers",
                params={"category": "linear"}, timeout=5
            ).json()

            now = time.time()

            if not bb_data or not bb_data.get("result"):
                return snapshots

            bb_tickers = {}
            for t in bb_data["result"].get("list", []):
                sym = t.get("symbol", "")
                bb_tickers[sym] = {
                    "bid": float(t.get("bid1Price", 0)),
                    "ask": float(t.get("ask1Price", 0)),
                }

            for pair in self.pairs:
                coin = pair.replace("-USDT", "")
                bb_sym = pair.replace("-", "")

                hl_mid = hl_mids.get(coin)
                bb_tick = bb_tickers.get(bb_sym)

                if not hl_mid or not bb_tick:
                    continue

                hl_mid_f = float(hl_mid)
                hl_bid = hl_mid_f
                hl_ask = hl_mid_f
                bb_bid = bb_tick["bid"]
                bb_ask = bb_tick["ask"]

                if bb_ask <= 0 or hl_ask <= 0:
                    continue

                spread_hl_over_bb = (hl_bid - bb_ask) / bb_ask * 10000
                spread_bb_over_hl = (bb_bid - hl_ask) / hl_ask * 10000
                best_spread = max(spread_hl_over_bb, spread_bb_over_hl)
                direction = "HL_PREMIUM" if spread_hl_over_bb >= spread_bb_over_hl else "BB_PREMIUM"

                snapshots.append(SpreadSnapshot(
                    pair=pair,
                    hl_bid=hl_bid, hl_ask=hl_ask,
                    bb_bid=bb_bid, bb_ask=bb_ask,
                    spread_hl_over_bb=spread_hl_over_bb,
                    spread_bb_over_hl=spread_bb_over_hl,
                    best_spread=best_spread,
                    direction=direction,
                    ts=now,
                ))

        except Exception as e:
            logger.error(f"Price poll failed: {e}")

        return snapshots


# ── Signal Engine ──────────────────────────────────────────────────

class PerpSignalEngine:
    """Detect entry/exit signals from spread snapshots."""

    def __init__(self, pairs: list[str]):
        self.pairs = pairs
        self.thresholds = AdaptiveThresholds()
        self.positions: dict[str, ArbPosition] = {}
        self._entry_streak: dict[str, int] = defaultdict(int)
        self._tick_count: dict[str, int] = defaultdict(int)
        self._subsample = 4  # Update thresholds every Nth tick

    def seed(self):
        """Bootstrap thresholds from MongoDB."""
        self.thresholds.seed_from_mongodb(self.pairs)

    def process_snapshot(self, snap: SpreadSnapshot) -> Optional[str]:
        """
        Process a spread snapshot. Returns action string or None.

        Returns:
          "ENTRY:<direction>" — open arb position
          "EXIT_REVERT" — spread reverted, close for profit
          "EXIT_STOP_LOSS" — spread widened, close for loss
          "EXIT_MAX_HOLD" — max hold time exceeded
          None — no action
        """
        pair = snap.pair
        self._tick_count[pair] += 1

        # Update thresholds (subsampled)
        if self._tick_count[pair] % self._subsample == 0:
            self.thresholds.update(pair, snap.best_spread)

        # Check exit first (if we have a position)
        if pair in self.positions:
            return self._check_exit(snap)

        # Check entry
        return self._check_entry(snap)

    def _check_entry(self, snap: SpreadSnapshot) -> Optional[str]:
        pair = snap.pair

        # Already at max capacity
        if len(self.positions) >= MAX_CONCURRENT:
            return None

        thresh = self.thresholds.get_thresholds(pair)
        if not thresh or not thresh["viable"]:
            self._entry_streak[pair] = 0
            return None

        # Spread must exceed P90
        if snap.best_spread >= thresh["p90"]:
            self._entry_streak[pair] += 1
            if self._entry_streak[pair] >= MIN_ENTRY_TICKS:
                # Fee gate: excess must cover fees
                if thresh["excess"] < MIN_EXCESS_BPS:
                    return None

                # ENTRY!
                self._entry_streak[pair] = 0

                pos = ArbPosition(
                    pair=pair,
                    direction=f"SHORT_{snap.direction.split('_')[0]}_LONG_{'BB' if snap.direction == 'HL_PREMIUM' else 'HL'}",
                    entry_spread=snap.best_spread,
                    entry_time=time.time(),
                    entry_p90=thresh["p90"],
                    exit_p25=thresh["p25"],
                    hl_entry_price=snap.hl_bid if snap.direction == "HL_PREMIUM" else snap.hl_ask,
                    bb_entry_price=snap.bb_ask if snap.direction == "HL_PREMIUM" else snap.bb_bid,
                )
                self.positions[pair] = pos
                return f"ENTRY:{snap.direction}"
        else:
            self._entry_streak[pair] = 0

        return None

    def _check_exit(self, snap: SpreadSnapshot) -> Optional[str]:
        pair = snap.pair
        pos = self.positions[pair]
        now = time.time()
        hold_s = now - pos.entry_time

        # Reversion exit
        if snap.best_spread <= pos.exit_p25:
            return "EXIT_REVERT"

        # Stop loss: spread widened beyond threshold
        if snap.best_spread > pos.entry_spread * STOP_LOSS_MULTIPLE:
            return "EXIT_STOP_LOSS"

        # Max hold
        if hold_s > MAX_HOLD_S:
            return "EXIT_MAX_HOLD"

        return None

    def close_position(self, pair: str) -> Optional[ArbPosition]:
        """Remove and return closed position."""
        return self.positions.pop(pair, None)


# ── MongoDB Logger ─────────────────────────────────────────────────

class TradeLogger:
    """Log signals and trades to MongoDB."""

    def __init__(self):
        try:
            self.client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            db_name = MONGO_URI.rsplit("/", 1)[-1]
            self.db = self.client[db_name]
            self.col = self.db["arb_hl_bybit_perp_trades"]
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            self.db = None

    def log_entry(self, snap: SpreadSnapshot, pos: ArbPosition, thresh: dict):
        if not self.db:
            return
        self.col.insert_one({
            "event": "ENTRY",
            "pair": snap.pair,
            "direction": pos.direction,
            "entry_spread_bps": snap.best_spread,
            "hl_price": snap.hl_bid,
            "bb_price": snap.bb_ask,
            "p90": thresh.get("p90"),
            "p25": thresh.get("p25"),
            "excess": thresh.get("excess"),
            "timestamp": datetime.now(timezone.utc),
        })

    def log_exit(self, pair: str, exit_type: str, pos: ArbPosition, snap: SpreadSnapshot):
        if not self.db:
            return
        hold_s = time.time() - pos.entry_time
        pnl_bps = pos.entry_spread - snap.best_spread - FEE_RT_BPS
        self.col.insert_one({
            "event": "EXIT",
            "exit_type": exit_type,
            "pair": pair,
            "direction": pos.direction,
            "entry_spread_bps": pos.entry_spread,
            "exit_spread_bps": snap.best_spread,
            "pnl_bps": pnl_bps,
            "hold_s": hold_s,
            "hl_entry": pos.hl_entry_price,
            "bb_entry": pos.bb_entry_price,
            "hl_exit": snap.hl_bid,
            "bb_exit": snap.bb_ask,
            "timestamp": datetime.now(timezone.utc),
        })


# ── Main Loop ──────────────────────────────────────────────────────

def main(args):
    pairs = args.pairs.split(",") if args.pairs else DEFAULT_PAIRS
    logger.info(f"HL-Bybit Perp/Perp Arb V1 — {'DRY RUN' if args.dry_run else 'LIVE'}")
    logger.info(f"Pairs: {pairs}")
    logger.info(f"Config: poll={POLL_INTERVAL_S}s, window={THRESHOLD_WINDOW}, "
                f"entry=P{ENTRY_PERCENTILE}, exit=P{EXIT_PERCENTILE}, "
                f"fee_rt={FEE_RT_BPS}bp, min_excess={MIN_EXCESS_BPS}bp")

    feed = PerpPriceFeed(pairs)
    engine = PerpSignalEngine(pairs)
    trade_log = TradeLogger()

    # Seed thresholds from historical data
    logger.info("Seeding thresholds from MongoDB...")
    engine.seed()

    # Stats
    total_entries = 0
    total_exits = 0
    total_pnl_bps = 0.0
    start_time = time.time()

    # Graceful shutdown
    running = True

    def handle_signal(sig, frame):
        nonlocal running
        logger.info(f"Signal {sig}, shutting down...")
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    poll_count = 0
    while running:
        try:
            snapshots = feed.poll()
        except Exception as e:
            logger.error(f"Poll error: {e}")
            time.sleep(5)
            continue
        poll_count += 1

        if poll_count == 1:
            logger.info(f"First poll: {len(snapshots)} snapshots received")
            for s in snapshots[:3]:
                logger.info(f"  {s.pair}: HL={s.hl_bid:.4f} BB_bid={s.bb_bid:.4f} BB_ask={s.bb_ask:.4f} spread={s.best_spread:.1f}bp")

        for snap in snapshots:
            action = engine.process_snapshot(snap)

            if action and action.startswith("ENTRY:"):
                total_entries += 1
                pos = engine.positions[snap.pair]
                thresh = engine.thresholds.get_thresholds(snap.pair)

                logger.info(
                    f"ENTRY {snap.pair} {pos.direction} | "
                    f"spread={snap.best_spread:.1f}bp > P90={pos.entry_p90:.1f}bp | "
                    f"P25={pos.exit_p25:.1f}bp excess={thresh['excess']:.1f}bp | "
                    f"HL={snap.hl_bid:.4f} BB={snap.bb_ask:.4f}"
                )
                trade_log.log_entry(snap, pos, thresh or {})

            elif action in ("EXIT_REVERT", "EXIT_STOP_LOSS", "EXIT_MAX_HOLD"):
                pos = engine.close_position(snap.pair)
                if pos:
                    total_exits += 1
                    hold_s = time.time() - pos.entry_time
                    pnl_bps = pos.entry_spread - snap.best_spread - FEE_RT_BPS
                    total_pnl_bps += pnl_bps

                    win = "WIN" if pnl_bps > 0 else "LOSS"
                    logger.info(
                        f"{win} {action} {snap.pair} | "
                        f"entry={pos.entry_spread:.1f}bp exit={snap.best_spread:.1f}bp | "
                        f"PnL={pnl_bps:.1f}bp hold={hold_s:.0f}s | "
                        f"cumPnL={total_pnl_bps:.1f}bp ({total_exits} trades)"
                    )
                    trade_log.log_exit(snap.pair, action, pos, snap)

        # Periodic status log
        if poll_count % 60 == 0:  # every ~90s
            uptime_min = (time.time() - start_time) / 60
            ready_count = sum(1 for p in pairs if engine.thresholds.ready(p))
            open_count = len(engine.positions)

            spread_str = " | ".join(
                f"{s.pair.replace('-USDT','')}: {s.best_spread:.1f}bp"
                for s in snapshots[:5]
            )

            logger.info(
                f"[{uptime_min:.0f}m] ready={ready_count}/{len(pairs)} "
                f"open={open_count} entries={total_entries} exits={total_exits} "
                f"cumPnL={total_pnl_bps:.1f}bp | {spread_str}"
            )

        time.sleep(POLL_INTERVAL_S)

    logger.info(
        f"Shutdown. entries={total_entries} exits={total_exits} "
        f"totalPnL={total_pnl_bps:.1f}bp"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HL-Bybit Perp/Perp Spread Arb")
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Signal detection only, no execution (default)")
    parser.add_argument("--pairs", type=str, default=None,
                        help="Comma-separated pairs (e.g., CHIP-USDT,AXS-USDT)")
    args = parser.parse_args()
    main(args)
