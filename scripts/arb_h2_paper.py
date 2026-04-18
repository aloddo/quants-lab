"""
H2 Spike Fade — Paper trader with adaptive per-pair thresholds.

Polls Binance SPOT + Bybit PERP bid/ask every 5 seconds.
Entry: abs_spread > pair's P90 (from rolling 4h window)
Exit: abs_spread < pair's P25 (from rolling 4h window)
Minimum excess (P90 - P25) must exceed 30bp to trade a pair.

Usage:
    python scripts/arb_h2_paper.py
    python scripts/arb_h2_paper.py --warmup-hours 2  # use quote collector history
"""
import asyncio
import argparse
import logging
import os
import signal
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from pymongo import MongoClient

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.services.arb_notifier import notify_open, notify_close, notify_stats, _send, _fire

# Load .env
_env_file = Path(__file__).resolve().parents[1] / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/tmp/arb-h2-paper.log"),
    ],
)
logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────
PAIRS = [
    "NOMUSDT", "ALICEUSDT", "THETAUSDT", "AXLUSDT",
    "ILVUSDT", "PORTALUSDT", "ARPAUSDT", "HIGHUSDT",
]
POLL_INTERVAL = 5.0
FEE_RT_BPS = 24.0
MIN_EXCESS_BPS = 30.0       # P90 - P25 must exceed this
WARMUP_BARS = 720           # 1h at 5s = 720 bars minimum before trading
MAX_CONCURRENT = 7
POSITION_USD = 200.0
ENTRY_PERCENTILE = 0.90
EXIT_PERCENTILE = 0.25


@dataclass
class H2Position:
    position_id: str
    symbol: str
    direction: str          # BUY_BN_SELL_BB or BUY_BB_SELL_BN
    entry_time: float
    entry_spread: float     # abs spread at entry
    entry_baseline: float   # rolling median at entry (frozen)
    entry_threshold: float  # P90 at entry
    exit_threshold: float   # P25 at entry (frozen)
    bn_price: float
    bb_price: float
    status: str = "OPEN"    # OPEN, CLOSED
    exit_time: float = 0
    exit_spread: float = 0
    gross_capture: float = 0
    net_pnl_bps: float = 0
    net_pnl_usd: float = 0
    hold_seconds: float = 0
    close_reason: str = ""


class AdaptiveThresholds:
    """Rolling per-pair spread distribution for adaptive entry/exit."""

    def __init__(self, window: int = WARMUP_BARS):
        self.window = window
        self.history: dict[str, deque] = defaultdict(lambda: deque(maxlen=window))

    def update(self, symbol: str, abs_spread: float):
        self.history[symbol].append(abs_spread)

    def ready(self, symbol: str) -> bool:
        return len(self.history[symbol]) >= self.window // 2

    def get_thresholds(self, symbol: str) -> dict | None:
        h = self.history[symbol]
        if len(h) < self.window // 2:
            return None
        import numpy as np
        arr = np.array(h)
        p25 = np.percentile(arr, 25)
        p50 = np.percentile(arr, 50)
        p90 = np.percentile(arr, 90)
        p95 = np.percentile(arr, 95)
        excess = p90 - p25
        return {
            "p25": p25, "median": p50, "p90": p90, "p95": p95,
            "excess": excess, "viable": excess > MIN_EXCESS_BPS,
        }

    def load_from_mongodb(self, db):
        """Bootstrap from quote collector history."""
        for sym in PAIRS:
            docs = list(db.arb_quote_snapshots.find(
                {"symbol": sym},
                {"best_spread": 1, "_id": 0},
            ).sort("timestamp", -1).limit(self.window))
            for d in reversed(docs):
                self.history[sym].append(abs(d["best_spread"]))
            if docs:
                logger.info(f"  {sym}: loaded {len(docs)} historical quotes")


class H2PaperTrader:
    def __init__(self):
        uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
        db_name = os.getenv("MONGO_DATABASE", "quants_lab")
        self._db = MongoClient(uri)[db_name]
        self._coll = self._db["arb_h2_positions"]
        self._coll.create_index([("symbol", 1), ("status", 1)])

        self.thresholds = AdaptiveThresholds()
        self.positions: list[H2Position] = []
        self.closed: list[H2Position] = []
        self._running = True
        self._poll_count = 0
        self._start_time = time.time()

    def _load_history(self):
        """Bootstrap thresholds from quote collector data."""
        logger.info("Loading historical quotes from arb_quote_snapshots...")
        self.thresholds.load_from_mongodb(self._db)
        ready = sum(1 for s in PAIRS if self.thresholds.ready(s))
        logger.info(f"  {ready}/{len(PAIRS)} pairs have enough history to trade")

    async def _poll_spreads(self, session: aiohttp.ClientSession) -> dict:
        """Fetch current bid/ask from both exchanges."""
        try:
            bn_task = session.get("https://api.binance.com/api/v3/ticker/bookTicker")
            bb_task = session.get("https://api.bybit.com/v5/market/tickers?category=linear")
            bn_resp, bb_resp = await asyncio.gather(bn_task, bb_task)
            bn_data = await bn_resp.json()
            bb_data = await bb_resp.json()

            bn = {b["symbol"]: {"bid": float(b["bidPrice"]), "ask": float(b["askPrice"])}
                  for b in bn_data if b["symbol"] in PAIRS}
            bb = {t["symbol"]: {"bid": float(t["bid1Price"]), "ask": float(t["ask1Price"])}
                  for t in bb_data["result"]["list"]
                  if t["symbol"] in PAIRS and t["bid1Price"]}

            result = {}
            for sym in PAIRS:
                if sym not in bn or sym not in bb:
                    continue
                # Executable spread both directions
                s1 = (bb[sym]["bid"] - bn[sym]["ask"]) / bn[sym]["ask"] * 10000
                s2 = (bn[sym]["bid"] - bb[sym]["ask"]) / bb[sym]["ask"] * 10000
                best = max(s1, s2)
                direction = "BUY_BN_SELL_BB" if s1 >= s2 else "BUY_BB_SELL_BN"
                result[sym] = {
                    "abs_spread": abs(best),
                    "signed_spread": best,
                    "direction": direction,
                    "bn_mid": (bn[sym]["bid"] + bn[sym]["ask"]) / 2,
                    "bb_mid": (bb[sym]["bid"] + bb[sym]["ask"]) / 2,
                }
            return result
        except Exception as e:
            logger.error(f"Poll error: {e}")
            return {}

    def _check_entries(self, spreads: dict):
        """Check for new entry signals."""
        open_count = len([p for p in self.positions if p.status == "OPEN"])
        if open_count >= MAX_CONCURRENT:
            return

        open_symbols = {p.symbol for p in self.positions if p.status == "OPEN"}

        for sym, data in spreads.items():
            if sym in open_symbols:
                continue
            if open_count >= MAX_CONCURRENT:
                break

            thresh = self.thresholds.get_thresholds(sym)
            if not thresh or not thresh["viable"]:
                continue

            if data["abs_spread"] >= thresh["p90"]:
                pos = H2Position(
                    position_id=f"h2_{sym}_{int(time.time()*1000)}",
                    symbol=sym,
                    direction=data["direction"],
                    entry_time=time.time(),
                    entry_spread=data["abs_spread"],
                    entry_baseline=thresh["median"],
                    entry_threshold=thresh["p90"],
                    exit_threshold=thresh["p25"],
                    bn_price=data["bn_mid"],
                    bb_price=data["bb_mid"],
                )
                self.positions.append(pos)
                open_count += 1
                self._save_position(pos)
                logger.info(
                    f"OPEN {sym} | spread={data['abs_spread']:.1f}bp "
                    f"(P90={thresh['p90']:.1f}, base={thresh['median']:.1f}, "
                    f"exit@{thresh['p25']:.1f}) | {data['direction']}"
                )
                _fire(
                    f"📝 <b>H2 OPEN</b> <code>{sym}</code>\n"
                    f"Spread: <b>{data['abs_spread']:.0f}bp</b> (P90={thresh['p90']:.0f})\n"
                    f"Base: {thresh['median']:.0f}bp | Exit@{thresh['p25']:.0f}bp\n"
                    f"Excess: {data['abs_spread']-thresh['p25']:.0f}bp (fees=24bp)"
                )

    def _check_exits(self, spreads: dict):
        """Check for exit signals on open positions."""
        for pos in self.positions:
            if pos.status != "OPEN":
                continue
            if pos.symbol not in spreads:
                continue

            data = spreads[pos.symbol]
            now = time.time()
            hold = now - pos.entry_time

            should_exit = False
            reason = ""

            # Exit: spread dropped below frozen P25
            if data["abs_spread"] <= pos.exit_threshold:
                should_exit = True
                reason = f"REVERTED: {data['abs_spread']:.1f}bp < P25={pos.exit_threshold:.1f}bp"

            # Max hold: 24h
            elif hold > 86400:
                should_exit = True
                reason = f"MAX_HOLD: {hold/3600:.1f}h"

            if should_exit:
                pos.status = "CLOSED"
                pos.exit_time = now
                pos.exit_spread = data["abs_spread"]
                pos.hold_seconds = hold
                pos.gross_capture = pos.entry_spread - pos.exit_spread
                pos.net_pnl_bps = pos.gross_capture - FEE_RT_BPS
                pos.net_pnl_usd = pos.net_pnl_bps / 10000 * POSITION_USD
                pos.close_reason = reason
                self.closed.append(pos)
                self._save_position(pos)
                logger.info(
                    f"CLOSE {pos.symbol} | {reason} | "
                    f"capture={pos.gross_capture:.1f}bp net={pos.net_pnl_bps:.1f}bp "
                    f"${pos.net_pnl_usd:.4f} | hold={hold:.0f}s"
                )
                emoji = "✅" if pos.net_pnl_bps > 0 else "❌"
                _fire(
                    f"📝 <b>H2 CLOSE</b> {emoji} <code>{pos.symbol}</code>\n"
                    f"Entry: {pos.entry_spread:.0f}bp → Exit: {pos.exit_spread:.0f}bp\n"
                    f"Capture: {pos.gross_capture:.0f}bp | Net: <b>{pos.net_pnl_bps:.0f}bp (${pos.net_pnl_usd:.3f})</b>\n"
                    f"Hold: {hold/60:.0f}min | {pos.close_reason}"
                )

    def _save_position(self, pos: H2Position):
        self._coll.update_one(
            {"position_id": pos.position_id},
            {"$set": asdict(pos)},
            upsert=True,
        )

    def _print_status(self):
        open_pos = [p for p in self.positions if p.status == "OPEN"]
        total_pnl = sum(p.net_pnl_usd for p in self.closed)
        wins = sum(1 for p in self.closed if p.net_pnl_bps > 0)
        wr = wins / len(self.closed) * 100 if self.closed else 0
        uptime = (time.time() - self._start_time) / 60

        # Threshold status
        ready = sum(1 for s in PAIRS if self.thresholds.ready(s))
        viable = 0
        for s in PAIRS:
            t = self.thresholds.get_thresholds(s)
            if t and t["viable"]:
                viable += 1

        status = (
            f"[{uptime:.0f}m] poll#{self._poll_count} | "
            f"ready={ready} viable={viable} | "
            f"open={len(open_pos)} closed={len(self.closed)} | "
            f"PnL=${total_pnl:.2f} WR={wr:.0f}%"
        )
        logger.info(status)
        print(status)

        for pos in open_pos:
            hold = (time.time() - pos.entry_time) / 60
            print(f"  {pos.symbol:<15} entry={pos.entry_spread:.0f}bp "
                  f"exit@{pos.exit_threshold:.0f}bp hold={hold:.0f}m")

    async def run(self):
        logger.info("=" * 60)
        logger.info("H2 SPIKE FADE — PAPER TRADING")
        logger.info(f"Pairs: {PAIRS}")
        logger.info(f"Entry: P{ENTRY_PERCENTILE*100:.0f} | Exit: P{EXIT_PERCENTILE*100:.0f}")
        logger.info(f"Min excess: {MIN_EXCESS_BPS}bp | Fees: {FEE_RT_BPS}bp RT")
        logger.info(f"Max concurrent: {MAX_CONCURRENT} | Size: ${POSITION_USD}")
        logger.info("=" * 60)

        self._load_history()

        # Startup notification
        viable = [s for s in PAIRS if (t := self.thresholds.get_thresholds(s)) and t["viable"]]
        viable_info = []
        for s in viable:
            t = self.thresholds.get_thresholds(s)
            viable_info.append(f"  {s}: P90={t['p90']:.0f} exit={t['p25']:.0f} excess={t['excess']:.0f}bp")
        _fire(
            f"🚀 <b>H2 Spike Fade STARTED</b>\n"
            f"Pairs: {len(PAIRS)} monitored, {len(viable)} viable\n"
            f"Entry: P90 | Exit: P25 | Fees: {FEE_RT_BPS}bp\n"
            + "\n".join(viable_info)
        )

        async with aiohttp.ClientSession() as session:
            try:
                while self._running:
                    t0 = time.time()
                    self._poll_count += 1

                    spreads = await self._poll_spreads(session)

                    # Update rolling distributions
                    for sym, data in spreads.items():
                        self.thresholds.update(sym, data["abs_spread"])

                    # Trade logic
                    self._check_exits(spreads)
                    self._check_entries(spreads)

                    # Console status every 60 polls (~5 min)
                    if self._poll_count % 60 == 0:
                        self._print_status()

                    # Telegram stats every 360 polls (~30 min)
                    if self._poll_count % 360 == 0:
                        self._send_telegram_stats()

                    elapsed = time.time() - t0
                    await asyncio.sleep(max(0, POLL_INTERVAL - elapsed))

            except asyncio.CancelledError:
                pass

        # Final summary
        self._print_summary()

    def _send_telegram_stats(self):
        """Send periodic stats to Telegram."""
        open_pos = [p for p in self.positions if p.status == "OPEN"]
        total_pnl = sum(p.net_pnl_usd for p in self.closed)
        wins = sum(1 for p in self.closed if p.net_pnl_bps > 0)
        wr = wins / len(self.closed) if self.closed else 0
        uptime_h = (time.time() - self._start_time) / 3600

        viable = sum(1 for s in PAIRS if (t := self.thresholds.get_thresholds(s)) and t["viable"])

        lines = [
            f"📊 <b>H2 Spike Fade</b> ({uptime_h:.1f}h)",
            f"Open: {len(open_pos)} | Closed: {len(self.closed)} | Viable: {viable}/{len(PAIRS)}",
            f"PnL: <b>${total_pnl:.2f}</b> | WR: {wr:.0%}",
        ]
        if open_pos:
            lines.append("")
            for p in open_pos:
                hold_m = (time.time() - p.entry_time) / 60
                lines.append(f"  {p.symbol} {p.entry_spread:.0f}bp exit@{p.exit_threshold:.0f}bp {hold_m:.0f}m")

        _fire("\n".join(lines))

    def _print_summary(self):
        total_pnl = sum(p.net_pnl_usd for p in self.closed)
        wins = sum(1 for p in self.closed if p.net_pnl_bps > 0)
        wr = wins / len(self.closed) * 100 if self.closed else 0
        uptime_h = (time.time() - self._start_time) / 3600

        logger.info("\n" + "=" * 60)
        logger.info("H2 PAPER TRADE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Uptime: {uptime_h:.1f}h")
        logger.info(f"Closed trades: {len(self.closed)}")
        logger.info(f"Total PnL: ${total_pnl:.2f}")
        logger.info(f"Win rate: {wr:.0f}%")
        if self.closed:
            avg_capture = sum(p.gross_capture for p in self.closed) / len(self.closed)
            avg_hold = sum(p.hold_seconds for p in self.closed) / len(self.closed)
            logger.info(f"Avg capture: {avg_capture:.1f}bp")
            logger.info(f"Avg hold: {avg_hold:.0f}s ({avg_hold/60:.0f}min)")
        logger.info(f"Open positions: {len([p for p in self.positions if p.status == 'OPEN'])}")


def main():
    trader = H2PaperTrader()

    loop = asyncio.new_event_loop()
    task = loop.create_task(trader.run())

    def shutdown(sig, frame):
        logger.info("Shutting down...")
        trader._running = False
        task.cancel()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()
