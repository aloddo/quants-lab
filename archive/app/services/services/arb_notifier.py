"""
Telegram notifications for arb engine — opens, closes, and periodic stats.

Uses Telegram Bot API directly (no MCP dependency).
Fire-and-forget: never blocks the trading loop.
"""
import asyncio
import logging
import os
import time
from datetime import datetime, timezone

import aiohttp

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
API_URL = f"{API_BASE}/sendMessage"

# Rate limit: max 1 message per 3 seconds
_last_send = 0
_MIN_INTERVAL = 3

# Telegram command polling state
_last_update_id = 0


async def _send(text: str):
    """Send a Telegram message. Swallows all errors."""
    global _last_send
    if not BOT_TOKEN or not CHAT_ID:
        return

    now = time.time()
    if now - _last_send < _MIN_INTERVAL:
        await asyncio.sleep(_MIN_INTERVAL - (now - _last_send))

    try:
        async with aiohttp.ClientSession() as session:
            await session.post(API_URL, json={
                "chat_id": CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }, timeout=aiohttp.ClientTimeout(total=5))
        _last_send = time.time()
    except Exception as e:
        logger.debug(f"Telegram send failed: {e}")


def _fire(text: str):
    """Schedule send without blocking the caller."""
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_send(text))
    except RuntimeError:
        pass  # no event loop, skip


def notify_open(symbol: str, direction: str, spread_bps: float, size_usd: float, paper: bool = True):
    mode = "📝" if paper else "🔥"
    arrow = "⬆️" if "BUY_BB" in direction else "⬇️"
    _fire(
        f"{mode} <b>OPEN</b> {arrow} <code>{symbol}</code>\n"
        f"Spread: <b>{spread_bps:.0f}bp</b> | Size: ${size_usd:.0f}"
    )


def notify_close(
    symbol: str, entry_bps: float, exit_bps: float,
    net_pnl: float, hold_hours: float, reason: str, paper: bool = True,
):
    mode = "📝" if paper else "🔥"
    emoji = "✅" if net_pnl > 0 else "❌"
    _fire(
        f"{mode} <b>CLOSE</b> {emoji} <code>{symbol}</code>\n"
        f"Entry: {entry_bps:.0f}bp → Exit: {exit_bps:.1f}bp\n"
        f"P&L: <b>${net_pnl:.3f}</b> | Hold: {hold_hours:.1f}h | {reason}"
    )


def notify_stats(
    open_count: int, closed_count: int, total_pnl: float,
    win_rate: float, uptime_hours: float, paper: bool = True,
):
    mode = "📝 PAPER" if paper else "🔥 LIVE"
    _fire(
        f"📊 <b>{mode} Stats</b> ({uptime_hours:.1f}h)\n"
        f"Open: {open_count} | Closed: {closed_count}\n"
        f"P&L: <b>${total_pnl:.2f}</b> | WR: {win_rate:.0%}"
    )


async def poll_commands():
    """Check for incoming Telegram commands. Call this periodically from the engine loop."""
    global _last_update_id
    if not BOT_TOKEN:
        return

    try:
        url = f"{API_BASE}/getUpdates?offset={_last_update_id + 1}&timeout=0&allowed_updates=[\"message\"]"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                data = await resp.json()

        if not data.get("ok") or not data.get("result"):
            return

        for update in data["result"]:
            _last_update_id = update["update_id"]
            msg = update.get("message", {})
            text = msg.get("text", "")
            chat_id = str(msg.get("chat", {}).get("id", ""))

            # Only respond to our chat
            if chat_id != CHAT_ID:
                continue

            if text.strip().lower() in ("/paper_report", "/paperreport", "/report", "/paper-report"):
                report = generate_report_text()
                await _send(report)
            elif text.strip().lower() in ("/status", "/s"):
                await _send("🤖 Arb engine is running. Use /report for full stats.")
            elif text.strip().lower() in ("/help", "/h"):
                await _send(
                    "🤖 <b>Arb Bot Commands</b>\n"
                    "/report — paper trading report\n"
                    "/status — engine alive check"
                )

    except Exception as e:
        logger.debug(f"Telegram poll failed: {e}")


def generate_report_text() -> str:
    """Generate the paper report as text (same as arb_paper_report.py)."""
    from pymongo import MongoClient
    import numpy as np

    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
    db_name = os.getenv("MONGO_DATABASE", "quants_lab")
    db = MongoClient(uri)[db_name]

    positions = list(db["arb_positions"].find().sort("created_at", -1))
    closed = [p for p in positions if p["status"] == "CLOSED"]
    opened = [p for p in positions if p["status"] == "OPEN"]

    if not closed:
        return "📊 <b>Paper Report</b>\nNo closed trades yet."

    pnls = [p.get("net_pnl", 0) for p in closed]
    total = sum(pnls)
    wr = sum(1 for p in pnls if p > 0) / len(closed)
    wins = sum(p for p in pnls if p > 0)
    losses = abs(sum(p for p in pnls if p < 0))
    pf = wins / losses if losses > 0 else 99

    holds = [(p.get("exit_time", 0) - p.get("entry_time", 0)) / 3600
             for p in closed if p.get("exit_time")]
    avg_hold = np.mean(holds) if holds else 0

    equity = np.cumsum(pnls)
    dd = (equity - np.maximum.accumulate(equity)).min()

    # Per-pair
    pair_pnl = {}
    for p in closed:
        s = p["symbol"]
        pair_pnl[s] = pair_pnl.get(s, 0) + p.get("net_pnl", 0)

    top3 = sorted(pair_pnl.items(), key=lambda x: x[1], reverse=True)[:3]

    # Gates
    gates = [
        ("≥100 trades", len(closed) >= 100),
        ("WR ≥60%", wr >= 0.60),
        ("PF ≥1.5", pf >= 1.5),
        ("Hold <24h", avg_hold < 24),
        ("DD < $50", dd > -50),
    ]
    all_pass = all(g[1] for g in gates)
    gate_str = " ".join("✅" if g[1] else "❌" for g in gates)

    lines = [
        f"📊 <b>Paper Report</b>",
        f"Closed: <b>{len(closed)}</b> | Open: {len(opened)}",
        f"P&L: <b>${total:.2f}</b> | WR: <b>{wr:.0%}</b> | PF: {pf:.2f}",
        f"Avg hold: {avg_hold:.1f}h | Max DD: ${dd:.2f}",
        f"",
        f"Top: {', '.join(f'{s} ${p:.2f}' for s, p in top3)}",
        f"",
        f"Gates: {gate_str}",
        f"{'✅ READY' if all_pass else '⏳ Need more data' if len(closed) < 100 else '❌ NOT READY'}",
    ]
    return "\n".join(lines)
