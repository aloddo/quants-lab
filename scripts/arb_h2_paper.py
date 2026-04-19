"""
H2 Spike Fade — Paper trader with adaptive per-pair thresholds.

Polls Binance SPOT + Bybit PERP bid/ask every 5 seconds.
Entry: abs_spread > pair's P90 (from rolling window)
Exit: abs_spread < pair's P25 (from rolling window)
Minimum excess (P90 - P25) must exceed 30bp to trade a pair.

Telegram: open/close alerts, 30-min stats with equity curve, /report command.

Usage:
    python scripts/arb_h2_paper.py
"""
import asyncio
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
import numpy as np
from pymongo import MongoClient

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
MIN_EXCESS_BPS = 30.0
WARMUP_BARS = 720           # 1h at 5s
MAX_CONCURRENT = 7
POSITION_USD = 200.0
ENTRY_PERCENTILE = 0.90
EXIT_PERCENTILE = 0.25

# Telegram
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
TG_URL = f"https://api.telegram.org/bot{TG_TOKEN}"
_tg_last_send = time.time() - 10  # allow immediate first send
_tg_last_update_id = 0


# ── Telegram helpers (async, never block) ────────────────────

async def tg_send(text: str, session: aiohttp.ClientSession | None = None):
    """Send Telegram message. Creates own session if needed."""
    global _tg_last_send
    if not TG_TOKEN or not TG_CHAT:
        logger.warning("TG send skipped: no token or chat_id")
        return
    now = time.time()
    wait = 3 - (now - _tg_last_send)
    if wait > 0:
        await asyncio.sleep(wait)
    try:
        own_session = session is None
        if own_session:
            session = aiohttp.ClientSession()
        resp = await session.post(f"{TG_URL}/sendMessage", json={
            "chat_id": TG_CHAT, "text": text,
            "parse_mode": "HTML", "disable_web_page_preview": True,
        }, timeout=aiohttp.ClientTimeout(total=5))
        result = await resp.json()
        if not result.get("ok"):
            logger.warning(f"TG send failed: {result}")
        _tg_last_send = time.time()
        if own_session:
            await session.close()
    except Exception as e:
        logger.warning(f"TG send error: {e}")


async def tg_send_photo(path: str, caption: str, session: aiohttp.ClientSession):
    """Send image to Telegram."""
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        with open(path, "rb") as f:
            data = aiohttp.FormData()
            data.add_field("chat_id", TG_CHAT)
            data.add_field("caption", caption, content_type="text/plain")
            data.add_field("parse_mode", "HTML")
            data.add_field("photo", f, filename="equity.png")
            await session.post(f"{TG_URL}/sendPhoto", data=data,
                               timeout=aiohttp.ClientTimeout(total=10))
    except Exception as e:
        logger.debug(f"TG photo failed: {e}")


async def tg_poll_commands(session: aiohttp.ClientSession) -> str | None:
    """Check for /report or /status commands."""
    global _tg_last_update_id
    if not TG_TOKEN:
        return None
    try:
        async with session.get(
            f"{TG_URL}/getUpdates",
            params={"offset": _tg_last_update_id + 1, "timeout": 0, "limit": 5},
            timeout=aiohttp.ClientTimeout(total=3),
        ) as resp:
            data = await resp.json()
        if not data.get("ok"):
            return None
        for update in data.get("result", []):
            _tg_last_update_id = update["update_id"]
            msg = update.get("message", {})
            text = msg.get("text", "").strip().lower()
            if text in ("/report", "/status", "/h2"):
                return text
        return None
    except Exception:
        return None


# ── Equity curve chart ───────────────────────────────────────

def render_equity_curve(closed_positions: list, path: str = "/tmp/h2_equity.png"):
    """Render equity curve as PNG using matplotlib."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        if not closed_positions:
            return None

        # Sort by exit time
        sorted_pos = sorted(closed_positions, key=lambda p: p.exit_time)
        times = [datetime.fromtimestamp(p.exit_time, tz=timezone.utc) for p in sorted_pos]
        cum_pnl = np.cumsum([p.net_pnl_usd for p in sorted_pos])

        fig, ax = plt.subplots(figsize=(10, 4))
        ax.step(times, cum_pnl, where="post", color="#2196F3", linewidth=1.5)
        ax.fill_between(times, cum_pnl, step="post", alpha=0.15, color="#2196F3")
        ax.axhline(0, color="gray", linewidth=0.5, linestyle="--")

        # Mark wins/losses
        for t, p in zip(times, sorted_pos):
            c = "#4CAF50" if p.net_pnl_bps > 0 else "#F44336"
            ax.scatter([t], [cum_pnl[sorted_pos.index(p)]], color=c, s=20, zorder=5)

        ax.set_title("H2 Spike Fade — Paper Trading Equity", fontsize=12)
        ax.set_ylabel("Cumulative PnL ($)")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M"))
        fig.autofmt_xdate()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(path, dpi=120)
        plt.close(fig)
        return path
    except Exception as e:
        logger.warning(f"Equity chart failed: {e}")
        return None


# ── Data classes ─────────────────────────────────────────────

@dataclass
class H2Position:
    position_id: str
    symbol: str
    direction: str
    entry_time: float
    entry_spread: float
    entry_baseline: float
    entry_threshold: float
    exit_threshold: float
    bn_price: float
    bb_price: float
    status: str = "OPEN"
    exit_time: float = 0
    exit_spread: float = 0
    gross_capture: float = 0
    net_pnl_bps: float = 0
    net_pnl_usd: float = 0
    hold_seconds: float = 0
    close_reason: str = ""


class AdaptiveThresholds:
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
        arr = np.array(h)
        p25 = float(np.percentile(arr, 25))
        p50 = float(np.percentile(arr, 50))
        p90 = float(np.percentile(arr, 90))
        p95 = float(np.percentile(arr, 95))
        excess = p90 - p25
        return {
            "p25": p25, "median": p50, "p90": p90, "p95": p95,
            "excess": excess, "viable": excess > MIN_EXCESS_BPS,
        }

    def load_from_mongodb(self, db):
        for sym in PAIRS:
            docs = list(db.arb_quote_snapshots.find(
                {"symbol": sym}, {"best_spread": 1, "_id": 0},
            ).sort("timestamp", -1).limit(self.window))
            for d in reversed(docs):
                self.history[sym].append(abs(d["best_spread"]))
            if docs:
                logger.info(f"  {sym}: loaded {len(docs)} historical quotes")


# ── Main trader ──────────────────────────────────────────────

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

    def _recover_open_positions(self):
        """Reload OPEN positions from MongoDB so restarts don't orphan them."""
        open_docs = list(self._coll.find({"status": "OPEN"}))
        if not open_docs:
            logger.info("No open positions to recover from MongoDB")
            return
        for doc in open_docs:
            pos = H2Position(
                position_id=doc["position_id"],
                symbol=doc["symbol"],
                direction=doc["direction"],
                entry_time=doc["entry_time"],
                entry_spread=doc["entry_spread"],
                entry_baseline=doc["entry_baseline"],
                entry_threshold=doc["entry_threshold"],
                exit_threshold=doc["exit_threshold"],
                bn_price=doc["bn_price"],
                bb_price=doc["bb_price"],
                status="OPEN",
            )
            self.positions.append(pos)
        logger.info(f"Recovered {len(open_docs)} open positions from MongoDB: "
                     f"{[d['symbol'] for d in open_docs]}")

    def _load_history(self):
        logger.info("Loading historical quotes from arb_quote_snapshots...")
        self.thresholds.load_from_mongodb(self._db)
        ready = sum(1 for s in PAIRS if self.thresholds.ready(s))
        logger.info(f"  {ready}/{len(PAIRS)} pairs have enough history to trade")

    async def _poll_spreads(self, session: aiohttp.ClientSession) -> dict:
        try:
            bn_resp, bb_resp = await asyncio.gather(
                session.get("https://api.binance.com/api/v3/ticker/bookTicker"),
                session.get("https://api.bybit.com/v5/market/tickers?category=linear"),
            )
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
                s1 = (bb[sym]["bid"] - bn[sym]["ask"]) / bn[sym]["ask"] * 10000
                s2 = (bn[sym]["bid"] - bb[sym]["ask"]) / bb[sym]["ask"] * 10000
                best = max(s1, s2)
                result[sym] = {
                    "abs_spread": abs(best),
                    "signed_spread": best,
                    "direction": "BUY_BN_SELL_BB" if s1 >= s2 else "BUY_BB_SELL_BN",
                    "bn_mid": (bn[sym]["bid"] + bn[sym]["ask"]) / 2,
                    "bb_mid": (bb[sym]["bid"] + bb[sym]["ask"]) / 2,
                }
            return result
        except Exception as e:
            logger.error(f"Poll error: {e}")
            return {}

    def _check_entries(self, spreads: dict):
        open_count = len([p for p in self.positions if p.status == "OPEN"])
        if open_count >= MAX_CONCURRENT:
            return
        open_symbols = {p.symbol for p in self.positions if p.status == "OPEN"}

        new_entries = []
        for sym, data in spreads.items():
            if sym in open_symbols or open_count >= MAX_CONCURRENT:
                continue
            thresh = self.thresholds.get_thresholds(sym)
            if not thresh or not thresh["viable"]:
                continue
            if data["abs_spread"] >= thresh["p90"]:
                pos = H2Position(
                    position_id=f"h2_{sym}_{int(time.time()*1000)}",
                    symbol=sym, direction=data["direction"],
                    entry_time=time.time(), entry_spread=data["abs_spread"],
                    entry_baseline=thresh["median"],
                    entry_threshold=thresh["p90"], exit_threshold=thresh["p25"],
                    bn_price=data["bn_mid"], bb_price=data["bb_mid"],
                )
                self.positions.append(pos)
                open_count += 1
                self._save_position(pos)
                new_entries.append((pos, thresh, data))
                logger.info(
                    f"OPEN {sym} | spread={data['abs_spread']:.1f}bp "
                    f"(P90={thresh['p90']:.1f}, base={thresh['median']:.1f}, "
                    f"exit@{thresh['p25']:.1f}) | {data['direction']}"
                )
        return new_entries

    def _check_exits(self, spreads: dict):
        closed_this_round = []
        for pos in self.positions:
            if pos.status != "OPEN" or pos.symbol not in spreads:
                continue
            data = spreads[pos.symbol]
            now = time.time()
            hold = now - pos.entry_time

            should_exit = False
            reason = ""

            # Track unrealized P&L on every tick
            pos._current_spread = data["abs_spread"]
            pos._unrealized_bps = pos.entry_spread - data["abs_spread"] - FEE_RT_BPS

            if data["abs_spread"] <= pos.exit_threshold:
                should_exit = True
                reason = f"REVERTED: {data['abs_spread']:.1f}bp < P25={pos.exit_threshold:.1f}bp"
            # Stop loss: spread widened 2x beyond entry (spike got worse, not reverting)
            elif data["abs_spread"] > pos.entry_spread * 2:
                should_exit = True
                reason = f"STOP_LOSS: {data['abs_spread']:.1f}bp > 2x entry ({pos.entry_spread*2:.0f}bp)"
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
                closed_this_round.append(pos)
                logger.info(
                    f"CLOSE {pos.symbol} | {reason} | "
                    f"capture={pos.gross_capture:.1f}bp net={pos.net_pnl_bps:.1f}bp "
                    f"${pos.net_pnl_usd:.4f} | hold={hold:.0f}s"
                )
        return closed_this_round

    def _save_position(self, pos: H2Position):
        self._coll.update_one(
            {"position_id": pos.position_id},
            {"$set": asdict(pos)},
            upsert=True,
        )

    def _build_report_text(self) -> str:
        """Build full report text for /report command and periodic stats."""
        open_pos = [p for p in self.positions if p.status == "OPEN"]
        total_pnl = sum(p.net_pnl_usd for p in self.closed)
        wins = sum(1 for p in self.closed if p.net_pnl_bps > 0)
        wr = wins / len(self.closed) if self.closed else 0
        uptime_h = (time.time() - self._start_time) / 3600

        viable = sum(1 for s in PAIRS if (t := self.thresholds.get_thresholds(s)) and t["viable"])

        lines = [
            f"<b>H2 Spike Fade</b> ({uptime_h:.1f}h)",
            f"Closed: {len(self.closed)} | WR: {wr:.0%} | PnL: <b>${total_pnl:.2f}</b>",
            f"Open: {len(open_pos)} | Viable: {viable}/{len(PAIRS)}",
        ]

        if self.closed:
            avg_cap = sum(p.gross_capture for p in self.closed) / len(self.closed)
            avg_hold = sum(p.hold_seconds for p in self.closed) / len(self.closed)
            lines.append(f"Avg capture: {avg_cap:.0f}bp | Avg hold: {avg_hold/60:.0f}min")

        # Per-pair breakdown
        if self.closed:
            lines.append("")
            by_pair = defaultdict(list)
            for p in self.closed:
                by_pair[p.symbol].append(p)
            for sym in sorted(by_pair):
                pp = by_pair[sym]
                pnl = sum(p.net_pnl_usd for p in pp)
                w = sum(1 for p in pp if p.net_pnl_bps > 0)
                lines.append(f"  {sym}: {len(pp)} trades ${pnl:.3f} WR={w/len(pp):.0%}")

        # Open positions with uPnL
        if open_pos:
            lines.append("")
            lines.append("<b>Open:</b>")
            total_upnl = 0
            for p in open_pos:
                hold_m = (time.time() - p.entry_time) / 60
                current = getattr(p, '_current_spread', p.entry_spread)
                upnl_bps = getattr(p, '_unrealized_bps', 0)
                upnl_usd = upnl_bps / 10000 * POSITION_USD
                total_upnl += upnl_usd
                emoji = "🟢" if upnl_bps > 0 else "🔴"
                lines.append(
                    f"  {emoji} {p.symbol} {p.entry_spread:.0f}bp now={current:.0f}bp "
                    f"exit@{p.exit_threshold:.0f}bp "
                    f"uPnL={upnl_bps:+.0f}bp (${upnl_usd:+.3f}) {hold_m:.0f}m"
                )
            lines.append(f"  Total uPnL: ${total_upnl:+.3f}")

        # Current thresholds
        lines.append("")
        lines.append("<b>Thresholds:</b>")
        for sym in PAIRS:
            t = self.thresholds.get_thresholds(sym)
            if t:
                v = "Y" if t["viable"] else "N"
                lines.append(
                    f"  {sym}: P90={t['p90']:.0f} P25={t['p25']:.0f} "
                    f"ex={t['excess']:.0f}bp [{v}]"
                )

        return "\n".join(lines)

    async def run(self):
        logger.info("=" * 60)
        logger.info("H2 SPIKE FADE -- PAPER TRADING")
        logger.info(f"Pairs: {PAIRS}")
        logger.info(f"Entry: P{ENTRY_PERCENTILE*100:.0f} | Exit: P{EXIT_PERCENTILE*100:.0f}")
        logger.info(f"Min excess: {MIN_EXCESS_BPS}bp | Fees: {FEE_RT_BPS}bp RT")
        logger.info(f"Max concurrent: {MAX_CONCURRENT} | Size: ${POSITION_USD}")
        logger.info("=" * 60)

        self._load_history()
        self._recover_open_positions()

        async with aiohttp.ClientSession() as session:
            # Startup notification (fire and forget, don't block main loop)
            viable = [s for s in PAIRS if (t := self.thresholds.get_thresholds(s)) and t["viable"]]
            viable_lines = []
            for s in viable:
                t = self.thresholds.get_thresholds(s)
                viable_lines.append(f"  {s}: P90={t['p90']:.0f} exit={t['p25']:.0f} ex={t['excess']:.0f}bp")
            asyncio.create_task(tg_send(
                f"🚀 <b>H2 Spike Fade STARTED</b>\n"
                f"Pairs: {len(PAIRS)} monitored, {len(viable)} viable\n"
                f"Entry: P90 | Exit: P25 | Fees: {FEE_RT_BPS:.0f}bp\n"
                + "\n".join(viable_lines),
                session,
            ))

            try:
                while self._running:
                    t0 = time.time()
                    self._poll_count += 1

                    spreads = await self._poll_spreads(session)

                    for sym, data in spreads.items():
                        self.thresholds.update(sym, data["abs_spread"])

                    # Trade
                    closed_now = self._check_exits(spreads)
                    new_entries = self._check_entries(spreads)

                    # Telegram: notify opens
                    if new_entries:
                        for pos, thresh, data in new_entries:
                            await tg_send(
                                f"📝 <b>H2 OPEN</b> <code>{pos.symbol}</code>\n"
                                f"Spread: <b>{data['abs_spread']:.0f}bp</b> (P90={thresh['p90']:.0f})\n"
                                f"Base: {thresh['median']:.0f}bp | Exit@{thresh['p25']:.0f}bp\n"
                                f"Excess: {data['abs_spread']-thresh['p25']:.0f}bp (fees=24bp)",
                                session,
                            )

                    # Telegram: notify closes
                    for pos in closed_now:
                        emoji = "✅" if pos.net_pnl_bps > 0 else "❌"
                        # Escape HTML entities in close_reason (< in "71bp < P25" breaks HTML parser)
                        safe_reason = pos.close_reason.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                        await tg_send(
                            f"📝 <b>H2 CLOSE</b> {emoji} <code>{pos.symbol}</code>\n"
                            f"Entry: {pos.entry_spread:.0f}bp -&gt; Exit: {pos.exit_spread:.0f}bp\n"
                            f"Capture: {pos.gross_capture:.0f}bp | Net: <b>{pos.net_pnl_bps:.0f}bp "
                            f"(${pos.net_pnl_usd:.3f})</b>\n"
                            f"Hold: {pos.hold_seconds/60:.0f}min | {safe_reason}",
                            session,
                        )

                    # Console status every 5 min
                    if self._poll_count % 60 == 0:
                        self._print_status()

                    # Telegram stats + chart every 30 min
                    if self._poll_count % 360 == 0:
                        await self._send_report(session)

                    # Poll for /report command every 30s
                    if self._poll_count % 6 == 0:
                        cmd = await tg_poll_commands(session)
                        if cmd in ("/report", "/status", "/h2"):
                            await self._send_report(session)

                    elapsed = time.time() - t0
                    await asyncio.sleep(max(0, POLL_INTERVAL - elapsed))

            except asyncio.CancelledError:
                pass

        self._print_summary()

    async def _send_report(self, session: aiohttp.ClientSession):
        """Send report with equity curve chart."""
        report = self._build_report_text()

        # Generate equity curve
        chart_path = render_equity_curve(self.closed)
        if chart_path:
            await tg_send_photo(chart_path, f"📊 {report}", session)
        else:
            await tg_send(f"📊 {report}", session)

    def _print_status(self):
        open_pos = [p for p in self.positions if p.status == "OPEN"]
        total_pnl = sum(p.net_pnl_usd for p in self.closed)
        wins = sum(1 for p in self.closed if p.net_pnl_bps > 0)
        wr = wins / len(self.closed) * 100 if self.closed else 0
        uptime = (time.time() - self._start_time) / 60
        viable = sum(1 for s in PAIRS if (t := self.thresholds.get_thresholds(s)) and t["viable"])

        status = (
            f"[{uptime:.0f}m] poll#{self._poll_count} | "
            f"viable={viable} | open={len(open_pos)} closed={len(self.closed)} | "
            f"PnL=${total_pnl:.2f} WR={wr:.0f}%"
        )
        logger.info(status)
        print(status)
        for pos in open_pos:
            hold = (time.time() - pos.entry_time) / 60
            print(f"  {pos.symbol:<15} entry={pos.entry_spread:.0f}bp "
                  f"exit@{pos.exit_threshold:.0f}bp hold={hold:.0f}m")

    def _print_summary(self):
        total_pnl = sum(p.net_pnl_usd for p in self.closed)
        wins = sum(1 for p in self.closed if p.net_pnl_bps > 0)
        wr = wins / len(self.closed) * 100 if self.closed else 0
        uptime_h = (time.time() - self._start_time) / 3600
        logger.info(f"\nH2 SUMMARY: {uptime_h:.1f}h, {len(self.closed)} trades, "
                    f"${total_pnl:.2f}, WR={wr:.0f}%")


def main():
    trader = H2PaperTrader()

    async def _run():
        def shutdown():
            logger.info("Shutting down...")
            trader._running = False

        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, shutdown)
        loop.add_signal_handler(signal.SIGTERM, shutdown)
        await trader.run()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
