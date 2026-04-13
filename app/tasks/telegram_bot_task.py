"""
Telegram Bot — command handler for /positions, /status, /pnl.

Runs every 60 seconds via cron. Each execution polls Telegram getUpdates
(with a short long-poll timeout), processes any commands, and replies.

Strategy-agnostic: queries HB API /bot-orchestration/status for ALL
running bots (any strategy), plus legacy MongoDB candidates for E2.

Proactive alerts: detects new trades by comparing close_type_counts
between polls and sends Telegram notifications on open/close events.

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
from app.services.bybit_exchange_client import BybitExchangeClient
from app.services.hb_api_client import HBApiClient

logger = logging.getLogger(__name__)


class TelegramBotTask(BaseTask):
    """Telegram bot command handler with strategy-agnostic HB bot monitoring."""

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
            "/shadow": self._handle_shadow,
            "/help": self._handle_help,
        }

        # Track last-known state for proactive trade alerts
        # Key: "bot_name/controller_name", Value: {close_type_counts, volume_traded}
        self._last_known: Dict[str, Dict] = {}

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        if not self.bot_token:
            return {"status": "completed", "note": "no bot token configured"}

        stats = {"updates": 0, "commands": 0, "errors": 0, "alerts": 0}

        # 1. Process incoming commands
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

        # 2. Proactive trade alerts — detect new trades from orchestration API
        try:
            alerts_sent = await self._check_trade_events()
            stats["alerts"] = alerts_sent
        except Exception as e:
            logger.warning(f"Trade event check failed: {e}")

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

    # ── HB Orchestration helpers ────────────────────────────

    async def _fetch_orchestration_status(self) -> Dict[str, Any]:
        """Fetch all bot performance data from HB API orchestration endpoint.

        Returns dict of {bot_name: {status, performance: {ctrl_name: {...}}}}
        or empty dict on failure.
        """
        hb = HBApiClient()
        try:
            result = await hb.get_bot_status()
            return result.get("data", {}) if isinstance(result, dict) else {}
        except Exception as e:
            logger.debug(f"Orchestration status unavailable: {e}")
            return {}
        finally:
            await hb.close()

    def _parse_ctrl_pair(self, ctrl_name: str) -> str:
        """Extract trading pair from controller name.

        e.g. 'e3_ada_usdt' -> 'ADA-USDT', 'e3_1000pepe_usdt' -> '1000PEPE-USDT'
        """
        parts = ctrl_name.split("_")
        if len(parts) >= 3 and parts[-1].upper() == "USDT":
            # Everything between the engine prefix and the last part is the base
            base = "_".join(parts[1:-1]).upper()
            return f"{base}-USDT"
        return ctrl_name.upper()

    def _parse_ctrl_engine(self, ctrl_name: str) -> str:
        """Extract engine name from controller name. e.g. 'e3_ada_usdt' -> 'E3'"""
        return ctrl_name.split("_")[0].upper() if "_" in ctrl_name else ctrl_name.upper()

    async def _check_trade_events(self) -> int:
        """Compare orchestration state with last-known to detect new trades.

        Sends Telegram alerts for new opens (volume increase) and closes
        (close_type_counts change). Returns number of alerts sent.
        """
        orch = await self._fetch_orchestration_status()
        if not orch:
            return 0

        alerts_sent = 0
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        if not chat_id:
            return 0

        for bot_name, bot_data in orch.items():
            perf = bot_data.get("performance", {})
            if not isinstance(perf, dict):
                continue

            for ctrl_name, ctrl_data in perf.items():
                p = ctrl_data.get("performance", {})
                key = f"{bot_name}/{ctrl_name}"
                vol = p.get("volume_traded", 0)
                closes = p.get("close_type_counts", {})
                upnl = p.get("unrealized_pnl_quote", 0)

                prev = self._last_known.get(key)

                if prev is None:
                    # First time seeing this controller — record baseline, no alert
                    self._last_known[key] = {
                        "volume_traded": vol,
                        "close_type_counts": dict(closes),
                        "total_closes": sum(closes.values()) if closes else 0,
                    }
                    continue

                pair = self._parse_ctrl_pair(ctrl_name)
                engine = self._parse_ctrl_engine(ctrl_name)

                # Detect new position opened (volume increased but total closes same)
                prev_closes_total = prev.get("total_closes", 0)
                curr_closes_total = sum(closes.values()) if closes else 0
                prev_vol = prev.get("volume_traded", 0)

                if vol > prev_vol and curr_closes_total == prev_closes_total:
                    # New position opened
                    side = "SHORT" if upnl < 0 and vol > prev_vol else "LONG/SHORT"
                    size = vol - prev_vol
                    msg = (
                        f"<b>New Position Opened</b>\n"
                        f"{engine}/{pair}\n"
                        f"Size: ~${size:.0f}"
                    )
                    try:
                        await self._send_reply(chat_id, msg)
                        alerts_sent += 1
                    except Exception as e:
                        logger.warning(f"Failed to send open alert: {e}")

                # Detect position closed (close_type_counts changed)
                if curr_closes_total > prev_closes_total:
                    new_closes = {}
                    prev_counts = prev.get("close_type_counts", {})
                    for ctype, count in closes.items():
                        prev_count = prev_counts.get(ctype, 0)
                        if count > prev_count:
                            new_closes[ctype] = count - prev_count

                    rpnl = p.get("realized_pnl_quote", 0)
                    prev_rpnl = prev.get("realized_pnl_quote", 0)
                    delta_pnl = rpnl - prev_rpnl if prev_rpnl else rpnl

                    for ctype, count in new_closes.items():
                        emoji = {"TAKE_PROFIT": "TP", "STOP_LOSS": "SL", "TIME_LIMIT": "TL"}.get(ctype, ctype)
                        sign = "+" if delta_pnl >= 0 else ""
                        msg = (
                            f"<b>Position Closed [{emoji}]</b>\n"
                            f"{engine}/{pair}\n"
                            f"PnL: {sign}${delta_pnl:.2f}"
                        )
                        try:
                            await self._send_reply(chat_id, msg)
                            alerts_sent += 1
                        except Exception as e:
                            logger.warning(f"Failed to send close alert: {e}")

                # Update last-known state
                self._last_known[key] = {
                    "volume_traded": vol,
                    "close_type_counts": dict(closes),
                    "total_closes": curr_closes_total,
                    "realized_pnl_quote": p.get("realized_pnl_quote", 0),
                }

        return alerts_sent

    # ── Command handlers ─────────────────────────────────────

    async def _handle_help(self) -> str:
        return (
            "<b>QuantsLab Bot Commands</b>\n\n"
            "/positions — Active positions across all bots\n"
            "/status — System health + bot overview\n"
            "/pnl — P&amp;L per bot and controller\n"
            "/heal — Recent watchdog self-healing actions\n"
            "/shadow — Shadow engine vs live comparison\n"
            "/help — This message\n\n"
            "<i>Trade open/close alerts are sent automatically.</i>"
        )

    async def _handle_shadow(self) -> str:
        """Compare shadow engines vs their live counterparts via signal attribution."""
        from pymongo import MongoClient
        from app.engines.strategy_registry import STRATEGY_REGISTRY

        mongo_uri = os.getenv("MONGO_URI")
        mongo_db_name = os.getenv("MONGO_DATABASE", "quants_lab")
        if not mongo_uri:
            return "No MONGO_URI configured."

        client = MongoClient(mongo_uri)
        db = client[mongo_db_name]

        # Find shadow engines
        shadows = {
            name: meta for name, meta in STRATEGY_REGISTRY.items()
            if meta.shadow_of
        }

        if not shadows:
            client.close()
            return (
                "<b>Shadow Engines</b>\n\n"
                "No shadow engines registered.\n\n"
                "To create one, add a StrategyMetadata entry with "
                "<code>shadow_of=\"E1\"</code> to STRATEGY_REGISTRY."
            )

        lines = ["<b>Shadow Engine Comparison</b>\n"]

        cutoff_ms = int(
            (__import__("datetime").datetime.utcnow() -
             __import__("datetime").timedelta(days=14)).timestamp() * 1000
        )

        for shadow_name, shadow_meta in shadows.items():
            live_name = shadow_meta.shadow_of
            lines.append(f"\n<b>{shadow_name}</b> vs <b>{live_name}</b> (14d)\n")

            for eng in [live_name, shadow_name]:
                # Attributed candidates (theoretical outcomes)
                attr_pipeline = [
                    {"$match": {
                        "engine": eng,
                        "trigger_fired": True,
                        "attr_computed_at": {"$exists": True},
                        "timestamp_utc": {"$gte": cutoff_ms},
                    }},
                    {"$group": {
                        "_id": None,
                        "total": {"$sum": 1},
                        "wins": {"$sum": {"$cond": [
                            {"$eq": ["$attr_theoretical_close", "TP"]}, 1, 0
                        ]}},
                        "avg_pnl": {"$avg": "$attr_theoretical_pnl_pct"},
                    }},
                ]
                attr_res = list(db.candidates.aggregate(attr_pipeline))
                if attr_res:
                    a = attr_res[0]
                    total = a["total"]
                    wr = a["wins"] / total * 100 if total > 0 else 0
                    avg_pnl = a.get("avg_pnl") or 0
                    label = "[SHADOW]" if eng == shadow_name else "[LIVE]"
                    lines.append(
                        f"  {label} {eng}: {total} signals, "
                        f"WR={wr:.0f}%, avg_pnl={avg_pnl:.2f}%"
                    )
                else:
                    label = "[SHADOW]" if eng == shadow_name else "[LIVE]"
                    lines.append(f"  {label} {eng}: no attributed data yet")

        lines.append(
            "\n<i>Attribution runs daily. "
            "Promote: <code>python cli.py promote-shadow --engine SHADOW --to LIVE</code></i>"
        )

        client.close()
        return "\n".join(lines)

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
        lines = []
        total_upnl = 0.0
        total_rpnl = 0.0
        total_positions = 0

        async with aiohttp.ClientSession() as session:
            # Fetch real exchange positions + controller configs in parallel
            positions = await BybitExchangeClient().fetch_positions(session)

        if positions:
            # Map controller configs for TP/SL/TL by pair
            ctrl_configs = await self._load_controller_configs()
            now_s = datetime.now(timezone.utc).timestamp()

            for pos in positions:
                pair = pos["pair"]
                side = pos["side"]  # BUY or SELL
                direction = "LONG" if side == "BUY" else "SHORT"
                entry = pos["entry_price"]
                mark = pos["mark_price"]
                qty = pos["qty"]
                upnl = pos["unrealised_pnl"]
                rpnl = pos["realised_pnl"]
                pos_value = pos["position_value"]
                created_ms = pos["created_time"]

                total_upnl += upnl
                total_rpnl += rpnl
                total_positions += 1

                # uPnL percentage
                upnl_pct = (upnl / pos_value * 100) if pos_value > 0 else 0
                emoji = "\U0001f7e2" if upnl >= 0 else "\U0001f534"

                # Get TP/SL/TL from controller config
                cfg = ctrl_configs.get(pair, {})
                tp_pct = float(cfg.get("take_profit", 0))
                sl_pct = float(cfg.get("stop_loss", 0))
                time_limit = int(cfg.get("time_limit", 0))
                engine = cfg.get("controller_name", "?").split("_")[0].upper() if cfg.get("controller_name") else "?"

                # TP/SL price levels
                tp_str = ""
                sl_str = ""
                if tp_pct > 0:
                    if direction == "LONG":
                        tp_price = entry * (1 + tp_pct)
                        sl_price = entry * (1 - sl_pct) if sl_pct > 0 else None
                    else:
                        tp_price = entry * (1 - tp_pct)
                        sl_price = entry * (1 + sl_pct) if sl_pct > 0 else None
                    tp_dist = abs(mark - tp_price) / mark * 100
                    tp_str = f"TP {fp(tp_price)} ({tp_dist:.1f}% away)"
                    if sl_price:
                        sl_dist = abs(mark - sl_price) / mark * 100
                        sl_str = f"SL {fp(sl_price)} ({sl_dist:.1f}% away)"

                # Time elapsed
                time_str = ""
                if time_limit > 0 and created_ms > 0:
                    elapsed_s = now_s - created_ms / 1000
                    pct_time = elapsed_s / time_limit * 100
                    time_str = f"{fmt_duration(elapsed_s)}/{fmt_duration(float(time_limit))} ({pct_time:.0f}%)"

                sign_u = "+" if upnl >= 0 else ""
                block = f"<b>{engine}/{pair} {direction}</b> {emoji}\n"
                block += f"  Entry: {fp(entry)} | Mark: {fp(mark)}\n"
                block += f"  uPnL: {sign_u}${upnl:.2f} ({sign_u}{upnl_pct:.2f}%)\n"
                if tp_str:
                    block += f"  {tp_str}"
                    if sl_str:
                        block += f" | {sl_str}"
                    block += "\n"
                if time_str:
                    block += f"  Time: {time_str}\n"
                block += f"  Size: ${pos_value:.0f} ({qty} @ {fp(entry)})"
                lines.append(block)

        # ── Legacy MongoDB candidates (E2 flow) ─────────────
        try:
            db = self.mongodb_client.get_database()
            active = await db["candidates"].find(
                {"disposition": "TESTNET_ACTIVE"}
            ).to_list(20)
        except Exception:
            active = []

        if active:
            async with aiohttp.ClientSession() as session:
                prices = await fetch_all_prices(session)
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

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
                upnl = 0.0
                if entry and live_price and amount:
                    entry_f = float(entry)
                    side_mult = 1.0 if direction == "LONG" else -1.0
                    upnl = (live_price - entry_f) * float(amount) * side_mult
                total_upnl += upnl
                total_positions += 1
                upnl_pct = (upnl / amount_usd * 100) if amount_usd else 0
                emoji = "\U0001f7e2" if upnl >= 0 else "\U0001f534"

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
                        tp_str = f"TP {fp(tp_price)} ({tp_dist:.1f}% away)"
                    if live_price and sl_price:
                        sl_dist = abs(live_price - sl_price) / live_price * 100
                        sl_str = f"SL {fp(sl_price)} ({sl_dist:.1f}% away)"

                time_str = ""
                if placed_at and time_limit:
                    elapsed_s = (now_ms - int(placed_at)) / 1000
                    pct_time = elapsed_s / float(time_limit) * 100
                    time_str = f"{fmt_duration(elapsed_s)}/{fmt_duration(float(time_limit))} ({pct_time:.0f}%)"

                sign_u = "+" if upnl >= 0 else ""
                block = f"<b>{engine}/{pair} {direction}</b> {emoji}\n"
                block += f"  Entry: {fp(entry)} | Live: {fp(live_price)}\n"
                block += f"  uPnL: {sign_u}${upnl:.2f} ({sign_u}{upnl_pct:.2f}%)\n"
                if tp_str:
                    block += f"  {tp_str}"
                    if sl_str:
                        block += f" | {sl_str}"
                    block += "\n"
                if time_str:
                    block += f"  Time: {time_str}\n"
                block += f"  Size: ${amount_usd:.0f}"
                lines.append(block)

        if not lines:
            return "No active positions."

        sign = "+" if total_upnl >= 0 else ""
        header = f"<b>Active Positions ({total_positions})</b>\n"
        footer = f"\nPortfolio uPnL: {sign}${total_upnl:.2f}"
        if total_rpnl != 0:
            sign_r = "+" if total_rpnl >= 0 else ""
            footer += f" | rPnL: {sign_r}${total_rpnl:.2f}"
        return header + "\n" + "\n\n".join(lines) + footer

    async def _load_controller_configs(self) -> Dict[str, Dict]:
        """Load controller configs from the deployed bots directory.

        Returns dict keyed by trading pair (e.g. 'APT-USDT') with config values.
        """
        import glob
        import yaml

        configs = {}
        config_dir = "/Users/hermes/hummingbot/hummingbot-api/bots/conf/controllers"
        try:
            for path in glob.glob(f"{config_dir}/*.yml"):
                with open(path) as f:
                    cfg = yaml.safe_load(f) or {}
                pair = cfg.get("trading_pair", "")
                if pair:
                    configs[pair] = cfg
        except Exception as e:
            logger.debug(f"Failed to load controller configs: {e}")
        return configs

    async def _handle_status(self) -> str:
        lines = ["<b>System Status</b>\n"]

        # HB API + all running bots (strategy-agnostic)
        orch = await self._fetch_orchestration_status()
        if orch:
            lines.append("HB API: Running")

            # Get real exchange PnL
            async with aiohttp.ClientSession() as session:
                positions = await BybitExchangeClient().fetch_positions(session)
            exchange_upnl = sum(p["unrealised_pnl"] for p in positions)
            exchange_rpnl = sum(p["realised_pnl"] for p in positions)

            for bot_name, bot_data in orch.items():
                status = bot_data.get("status", "?")
                perf = bot_data.get("performance", {})
                ctrl_count = len(perf) if isinstance(perf, dict) else 0
                active_ctrls = sum(
                    1 for cd in (perf.values() if isinstance(perf, dict) else [])
                    if cd.get("performance", {}).get("volume_traded", 0) > 0
                )

                sign_u = "+" if exchange_upnl >= 0 else ""
                sign_r = "+" if exchange_rpnl >= 0 else ""
                lines.append(
                    f"  {bot_name}: {status}\n"
                    f"    {ctrl_count} controllers ({active_ctrls} active, {len(positions)} positions)\n"
                    f"    uPnL: {sign_u}${exchange_upnl:.2f} | rPnL: {sign_r}${exchange_rpnl:.2f}"
                )
        else:
            hb = HBApiClient()
            try:
                if await hb.health_check():
                    lines.append("HB API: Running (no bots)")
                else:
                    lines.append("HB API: Unreachable")
            except Exception:
                lines.append("HB API: Error")
            finally:
                await hb.close()

        # MongoDB
        try:
            db = self.mongodb_client.get_database()
            feat_count = await db["features"].count_documents({})
            lines.append(f"\nMongoDB: {feat_count} features")
        except Exception:
            lines.append("\nMongoDB: Error")

        # Last task runs
        lines.append("\n<b>Pipeline tasks:</b>")
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        task_names = [
            "candles_downloader_bybit", "bybit_derivatives",
            "feature_computation", "watchdog",
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
                    emoji = "ok" if status == "completed" else "FAIL"
                    if started:
                        if isinstance(started, datetime):
                            started_ms = int(started.timestamp() * 1000)
                        else:
                            started_ms = int(started)
                        ago_s = (now_ms - started_ms) / 1000
                        lines.append(f"  {task_name}: {fmt_duration(ago_s)} ago [{emoji}]")
                    else:
                        lines.append(f"  {task_name}: {status} [{emoji}]")
                else:
                    lines.append(f"  {task_name}: never ran")
            except Exception:
                lines.append(f"  {task_name}: error")

        return "\n".join(lines)

    async def _handle_pnl(self) -> str:
        lines = ["<b>P&amp;L Summary</b>\n"]

        # ── Exchange positions (real-time from Bybit) ──────────
        async with aiohttp.ClientSession() as session:
            positions = await BybitExchangeClient().fetch_positions(session)

        if positions:
            lines.append("<b>Open positions:</b>")
            total_upnl = 0.0
            total_rpnl = 0.0
            for pos in positions:
                pair = pos["pair"]
                upnl = pos["unrealised_pnl"]
                rpnl = pos["realised_pnl"]
                total_upnl += upnl
                total_rpnl += rpnl
                direction = "L" if pos["side"] == "BUY" else "S"
                sign = "+" if upnl >= 0 else ""
                lines.append(f"  {pair} [{direction}]: {sign}${upnl:.2f}")

            global_pnl = total_upnl + total_rpnl
            sign = "+" if global_pnl >= 0 else ""
            lines.append(f"  <b>Total: {sign}${global_pnl:.2f}</b> (u:{'+' if total_upnl >= 0 else ''}${total_upnl:.2f} r:{'+' if total_rpnl >= 0 else ''}${total_rpnl:.2f})")

        # ── Orchestration status (close counts, volume) ──────
        orch = await self._fetch_orchestration_status()
        if orch:
            for bot_name, bot_data in orch.items():
                perf = bot_data.get("performance", {})
                if not isinstance(perf, dict):
                    continue
                total_closes = {}
                for ctrl_data in perf.values():
                    for ct, count in ctrl_data.get("performance", {}).get("close_type_counts", {}).items():
                        total_closes[ct] = total_closes.get(ct, 0) + count
                if total_closes:
                    close_str = " ".join(
                        f"{k.replace('TAKE_PROFIT','TP').replace('STOP_LOSS','SL').replace('TIME_LIMIT','TL')}:{v}"
                        for k, v in total_closes.items()
                    )
                    lines.append(f"  Closes: {close_str}")

        # ── Legacy MongoDB candidates (E2 flow) ──────────────
        try:
            db = self.mongodb_client.get_database()
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

            windows = [("Today", 1), ("7d", 7), ("30d", 30), ("All-time", 36500)]
            has_legacy = False

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
                    if not has_legacy:
                        lines.append("\n<b>Legacy positions:</b>")
                        has_legacy = True
                    r = result[0]
                    total = r["total_pnl"]
                    count = r["count"]
                    wins = r["wins"]
                    losses = count - wins
                    wr = (wins / count * 100) if count > 0 else 0
                    sign = "+" if total >= 0 else ""
                    lines.append(f"  {label:10s} {sign}${total:.2f}  ({wins}W/{losses}L  {wr:.0f}%)")
        except Exception as e:
            logger.debug(f"Legacy PnL query failed: {e}")

        return "\n".join(lines)
