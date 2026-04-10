"""
Telegram Bot — command handler for /positions, /status, /pnl.

Runs every 60 seconds via cron. Each execution polls Telegram getUpdates
(with a short long-poll timeout), processes any commands, and replies.

Coexists with the send-only TelegramNotifier — getUpdates and sendMessage
are independent API methods and don't conflict.
"""
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiohttp

from core.tasks import BaseTask, TaskContext
from app.engines.fmt import fp, fmt_duration, fmt_pnl
from app.services.bybit_rest import fetch_all_prices
from app.services.hb_api_client import HBApiClient

logger = logging.getLogger(__name__)


class TelegramBotTask(BaseTask):
    """Telegram bot command handler."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.poll_timeout = task_config.get("poll_timeout", 5)
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.allowed_chat_ids = set()
        # Allow configured chat IDs + env var
        env_chat = os.getenv("TELEGRAM_CHAT_ID", "")
        if env_chat:
            self.allowed_chat_ids.add(str(env_chat))
        for cid in task_config.get("allowed_chat_ids", []):
            self.allowed_chat_ids.add(str(cid))

        self._offset = 0  # getUpdates offset for dedup
        self._handlers = {
            "/positions": self._handle_positions,
            "/status": self._handle_status,
            "/pnl": self._handle_pnl,
            "/heal": self._handle_heal,
            "/help": self._handle_help,
        }

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        if not self.bot_token:
            return {"status": "completed", "note": "no bot token configured"}

        stats = {"updates": 0, "commands": 0, "errors": 0}

        try:
            updates = await self._get_updates()
            stats["updates"] = len(updates)

            for update in updates:
                update_id = update.get("update_id", 0)
                self._offset = max(self._offset, update_id + 1)

                msg = update.get("message", {})
                chat_id = str(msg.get("chat", {}).get("id", ""))
                text = (msg.get("text") or "").strip()

                if not chat_id or chat_id not in self.allowed_chat_ids:
                    continue

                # Parse command (strip @botname suffix)
                cmd = text.split()[0].split("@")[0].lower() if text else ""

                handler = self._handlers.get(cmd)
                if handler:
                    try:
                        response = await handler()
                        await self._send_reply(chat_id, response)
                        stats["commands"] += 1
                    except Exception as e:
                        logger.error(f"Command {cmd} failed: {e}")
                        await self._send_reply(chat_id, f"Command failed: {e}")
                        stats["errors"] += 1

        except Exception as e:
            logger.warning(f"TelegramBot poll error: {e}")
            stats["errors"] += 1

        return {
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_id": context.execution_id,
            "stats": stats,
        }

    # ── Telegram API ─────────────────────────────────────────

    async def _get_updates(self) -> List[dict]:
        """Poll Telegram for new messages."""
        url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
        params = {"timeout": self.poll_timeout, "allowed_updates": '["message"]'}
        if self._offset:
            params["offset"] = self._offset

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=self.poll_timeout + 5)) as resp:
                data = await resp.json()
                if not data.get("ok"):
                    logger.warning(f"getUpdates failed: {data}")
                    return []
                return data.get("result", [])

    async def _send_reply(self, chat_id: str, text: str) -> None:
        """Send a reply message."""
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(f"sendMessage failed: {resp.status} {body}")

    # ── Command handlers ─────────────────────────────────────

    async def _handle_help(self) -> str:
        return (
            "<b>QuantsLab Bot Commands</b>\n\n"
            "/positions — Active positions with live P&amp;L\n"
            "/status — System health check\n"
            "/pnl — Performance summary\n"
            "/heal — Recent watchdog self-healing actions\n"
            "/help — This message"
        )

    async def _handle_heal(self) -> str:
        """Show recent watchdog self-healing actions from watchdog_state."""
        from pymongo import MongoClient
        mongo_uri = os.getenv("MONGO_URI")
        mongo_db_name = os.getenv("MONGO_DATABASE", "quants_lab")
        if not mongo_uri:
            return "No MONGO_URI configured."

        client = MongoClient(mongo_uri)
        db = client[mongo_db_name]

        lines = ["<b>\U0001fa79 Watchdog Self-Healing</b>\n"]

        # Recent healing actions
        heal_log = db.watchdog_state.find_one({"_id": "heal_log"})
        actions = (heal_log or {}).get("actions", [])

        if actions:
            # Show last 10 actions
            for a in actions[-10:]:
                ts = a.get("timestamp")
                ts_str = ts.strftime("%m-%d %H:%M") if ts else "?"
                lines.append(f"  {ts_str} \u2705 {a.get('action', '?')}")
        else:
            lines.append("  No healing actions recorded yet.")

        # HB API state
        hb_state = db.watchdog_state.find_one({"_id": "hb_api"})
        if hb_state:
            fails = hb_state.get("consecutive_failures", 0)
            status = "\u2705 Healthy" if fails == 0 else f"\u26a0\ufe0f {fails} consecutive failures"
            lines.append(f"\n<b>HB API:</b> {status}")

        # Recent restarts
        six_hours_ago = datetime.utcnow() - timedelta(hours=6)
        restarts = list(db.watchdog_state.find({
            "_id": {"$regex": "^hb_restart_"},
            "timestamp": {"$gte": six_hours_ago},
        }))
        if restarts:
            lines.append(f"  Docker restarts (6h): {len(restarts)}/2")
            for r in restarts:
                ts = r.get("timestamp")
                ts_str = ts.strftime("%H:%M") if ts else "?"
                healthy = "\u2705" if r.get("healthy_after") else "\u274c"
                lines.append(f"    {ts_str} {healthy}")

        # Data retrigger state
        for key, label in [("retrigger_candles", "Candles"), ("retrigger_features", "Features")]:
            state = db.watchdog_state.find_one({"_id": key})
            if state and state.get("last_triggered"):
                ts = state["last_triggered"]
                ago = (datetime.utcnow() - ts).total_seconds() / 60
                lines.append(f"\n<b>{label} retrigger:</b> {ago:.0f}m ago")

        client.close()
        return "\n".join(lines)

    async def _handle_positions(self) -> str:
        db = self.mongodb_client.get_database()
        active = await db["candidates"].find(
            {"disposition": "TESTNET_ACTIVE"}
        ).to_list(20)

        if not active:
            return "No active positions."

        # Fetch live prices
        async with aiohttp.ClientSession() as session:
            prices = await fetch_all_prices(session)

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        lines = []

        for doc in active:
            pair = doc.get("pair", "?")
            engine = doc.get("engine", "?")
            direction = doc.get("direction", "?")
            entry = doc.get("decision_price") or doc.get("testnet_fill_price")
            amount_usd = doc.get("testnet_amount_usd", 0)
            amount = doc.get("testnet_amount", 0)
            tp_pct = doc.get("testnet_tp_pct")
            sl_pct = doc.get("testnet_sl_pct")
            time_limit = doc.get("testnet_time_limit")
            placed_at = doc.get("testnet_placed_at", 0)

            # Fall back to registry defaults for positions placed before this code
            if tp_pct is None or sl_pct is None or time_limit is None:
                try:
                    from app.engines.strategy_registry import get_strategy
                    meta = get_strategy(engine)
                    if tp_pct is None:
                        tp_pct = float(meta.exit_params.get("take_profit", 0.03))
                    if sl_pct is None:
                        sl_pct = float(meta.exit_params.get("stop_loss", 0.015))
                    if time_limit is None:
                        time_limit = meta.exit_params.get("time_limit", 86400)
                except Exception:
                    pass

            live_price = prices.get(pair)

            # Compute unrealized PnL
            upnl_str = "N/A"
            upnl_pct_str = ""
            price_emoji = ""
            if entry and live_price and amount:
                entry_f = float(entry)
                side_mult = 1.0 if direction == "LONG" else -1.0
                upnl = (live_price - entry_f) * float(amount) * side_mult
                upnl_pct = (live_price - entry_f) / entry_f * side_mult * 100
                upnl_str = fmt_pnl(upnl, amount_usd)
                price_emoji = "\U0001f7e2" if upnl >= 0 else "\U0001f534"

            # TP/SL prices and distances
            tp_str = ""
            sl_str = ""
            if entry and tp_pct is not None:
                entry_f = float(entry)
                if direction == "LONG":
                    tp_price = entry_f * (1 + tp_pct)
                    sl_price = entry_f * (1 - sl_pct) if sl_pct else None
                else:
                    tp_price = entry_f * (1 - tp_pct)
                    sl_price = entry_f * (1 + sl_pct) if sl_pct else None

                if live_price and tp_price:
                    tp_dist = abs(live_price - tp_price) / live_price * 100
                    tp_str = f"TP: {fp(tp_price)} ({tp_dist:.1f}% away)"
                if live_price and sl_price and sl_pct is not None:
                    sl_dist = abs(live_price - sl_price) / live_price * 100
                    sl_str = f"SL: {fp(sl_price)} ({sl_dist:.1f}% away)"

            # Time elapsed
            time_str = ""
            if placed_at and time_limit:
                elapsed_s = (now_ms - int(placed_at)) / 1000
                pct_time = elapsed_s / float(time_limit) * 100
                time_str = f"Time: {fmt_duration(elapsed_s)} / {fmt_duration(float(time_limit))} ({pct_time:.0f}%)"

            block = f"<b>{engine}/{pair} {direction}</b>\n"
            block += f"  Entry: {fp(entry)} | Live: {fp(live_price)} {price_emoji}\n"
            block += f"  uPnL: {upnl_str}\n"
            if tp_str or sl_str:
                block += f"  {tp_str} | {sl_str}\n"
            if time_str:
                block += f"  {time_str}\n"
            block += f"  Size: ${amount_usd:.0f}"

            lines.append(block)

        # Portfolio summary
        total_upnl = 0
        for doc in active:
            pair = doc.get("pair", "?")
            entry = doc.get("decision_price") or doc.get("testnet_fill_price")
            amount = doc.get("testnet_amount", 0)
            direction = doc.get("direction", "LONG")
            live_price = prices.get(pair)
            if entry and live_price and amount:
                side_mult = 1.0 if direction == "LONG" else -1.0
                total_upnl += (live_price - float(entry)) * float(amount) * side_mult

        max_pos = 40
        header = f"<b>\U0001f4ca Active Positions ({len(active)}/{max_pos})</b>\n"
        footer = f"\nPortfolio: {fmt_pnl(total_upnl)} uPnL | {len(active)}/{max_pos} slots"

        return header + "\n" + "\n\n".join(lines) + footer

    async def _handle_status(self) -> str:
        lines = ["<b>\U0001f527 System Status</b>\n"]

        # HB API
        hb = HBApiClient()
        try:
            if await hb.health_check():
                lines.append("\u2705 HB API: Running")
            else:
                lines.append("\u274c HB API: Unreachable")
        except Exception:
            lines.append("\u274c HB API: Error")
        finally:
            await hb.close()

        # MongoDB
        try:
            db = self.mongodb_client.get_database()
            feat_count = await db["features"].count_documents({})
            lines.append(f"\u2705 MongoDB: {feat_count} features")
        except Exception:
            lines.append("\u274c MongoDB: Error")

        # Last task runs
        lines.append("\n<b>Last task runs:</b>")
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        task_names = [
            "candles_downloader_bybit", "feature_computation",
            "signal_scan", "breakout_monitor", "testnet_resolver",
        ]
        for task_name in task_names:
            try:
                doc = await db["task_executions"].find_one(
                    {"task_name": task_name},
                    sort=[("started_at", -1)],
                )
                if doc:
                    started = doc.get("started_at")
                    status = doc.get("status", "?")
                    emoji = "\u2705" if status == "completed" else "\u274c"
                    if started:
                        if isinstance(started, datetime):
                            started_ms = int(started.timestamp() * 1000)
                        else:
                            started_ms = int(started)
                        ago_s = (now_ms - started_ms) / 1000
                        lines.append(f"  {task_name}: {fmt_duration(ago_s)} ago {emoji}")
                    else:
                        lines.append(f"  {task_name}: {status} {emoji}")
                else:
                    lines.append(f"  {task_name}: never ran")
            except Exception:
                lines.append(f"  {task_name}: error")

        return "\n".join(lines)

    async def _handle_pnl(self) -> str:
        db = self.mongodb_client.get_database()
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        windows = [
            ("Today", 1),
            ("7d", 7),
            ("30d", 30),
            ("All-time", 36500),
        ]

        lines = ["<b>\U0001f4b0 P&amp;L Summary</b>\n"]

        for label, days in windows:
            cutoff_ms = now_ms - days * 86400 * 1000
            pipeline = [
                {"$match": {
                    "disposition": "RESOLVED_TESTNET",
                    "testnet_resolved_at": {"$gte": cutoff_ms},
                }},
                {"$group": {
                    "_id": None,
                    "total_pnl": {"$sum": {"$toDouble": {"$ifNull": ["$testnet_pnl", 0]}}},
                    "count": {"$sum": 1},
                    "wins": {"$sum": {"$cond": [{"$gt": [{"$toDouble": {"$ifNull": ["$testnet_pnl", 0]}}, 0]}, 1, 0]}},
                }},
            ]
            result = await db["candidates"].aggregate(pipeline).to_list(1)

            if result:
                r = result[0]
                total = r["total_pnl"]
                count = r["count"]
                wins = r["wins"]
                losses = count - wins
                wr = (wins / count * 100) if count > 0 else 0
                sign = "+" if total >= 0 else ""
                lines.append(f"  {label:10s} {sign}${total:.2f}  ({wins}W/{losses}L  {wr:.0f}%)")
            else:
                lines.append(f"  {label:10s} —")

        # Best/worst trades
        lines.append("")
        for sort_dir, label in [(("testnet_pnl", -1), "Best"), (("testnet_pnl", 1), "Worst")]:
            doc = await db["candidates"].find_one(
                {"disposition": "RESOLVED_TESTNET", "testnet_pnl": {"$ne": None}},
                sort=[sort_dir],
            )
            if doc:
                pnl = float(doc.get("testnet_pnl", 0))
                sign = "+" if pnl >= 0 else ""
                lines.append(f"{label}: {doc.get('engine')}/{doc.get('pair')} {sign}${pnl:.2f}")

        return "\n".join(lines)
