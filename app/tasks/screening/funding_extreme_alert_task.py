"""
X9 Funding Extreme Alert — opportunistic signal scanner.

Runs hourly. Checks all pairs for funding rate extremes (z>1, streak>=2,
OI rising). When conditions are met, sends a Telegram alert with pair,
direction, z-score, streak, and historical backtest stats.

NOT an automated trading strategy — Alberto decides whether to act.

Evidence (May 2026, 38-pair backtest):
  ADA PF=1.57/75%WR, CRV PF=2.05/85%WR, LINK PF=3.62/83%WR
  Signal is rare (~30-70 trades/year/pair) but high WR when it fires.
"""
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import aiohttp
import numpy as np

from core.tasks import BaseTask, TaskContext

logger = logging.getLogger(__name__)

# Pairs with backtest evidence (PF > 1.0 in X9 bulk backtest)
PAIR_STATS = {
    "ADA-USDT":  {"pf": 1.57, "wr": 75.0},
    "CRV-USDT":  {"pf": 2.05, "wr": 84.6},
    "LINK-USDT": {"pf": 3.62, "wr": 83.3},
    "KAS-USDT":  {"pf": 3.18, "wr": 83.3},
    "BLUR-USDT": {"pf": 2.02, "wr": 70.4},
    "BNB-USDT":  {"pf": 1.50, "wr": 72.0},
    "AAVE-USDT": {"pf": 1.32, "wr": 68.0},
    "SUI-USDT":  {"pf": 1.51, "wr": 62.5},
    "TAO-USDT":  {"pf": 1.27, "wr": 73.3},
    "ALGO-USDT": {"pf": 1.19, "wr": 63.2},
    "AVAX-USDT": {"pf": 1.18, "wr": 46.2},
    "ETH-USDT":  {"pf": 1.15, "wr": 66.7},
    "XRP-USDT":  {"pf": 1.12, "wr": 41.2},
    "DOGE-USDT": {"pf": 1.07, "wr": 69.2},
}


class FundingExtremeAlertTask(BaseTask):
    """Scan for funding rate extremes and alert via Telegram."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.z_threshold = task_config.get("z_threshold", 1.0)
        self.streak_min = task_config.get("streak_min", 2)
        self.oi_z_min = task_config.get("oi_z_min", 0.0)
        self.z_window = task_config.get("z_window", 30)
        self.min_pf = task_config.get("min_pf", 1.0)  # only alert on pairs with backtest edge
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = str(task_config.get("telegram_chat_id", os.getenv("TELEGRAM_CHAT_ID", "")))

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB required for FundingExtremeAlertTask")

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        db = self.mongodb_client.get_database()
        alerts = []

        for pair, stats in PAIR_STATS.items():
            if stats["pf"] < self.min_pf:
                continue

            try:
                alert = await self._check_pair(db, pair, stats)
                if alert:
                    alerts.append(alert)
            except Exception as e:
                logger.warning(f"Error checking {pair}: {e}")

        if alerts:
            msg = self._format_alerts(alerts)
            await self._send_telegram(msg)
            logger.info(f"X9 alert fired: {len(alerts)} pairs — {[a['pair'] for a in alerts]}")
        else:
            logger.info(f"X9 scan: no funding extremes detected across {len(PAIR_STATS)} pairs")

        return {
            "pairs_scanned": len(PAIR_STATS),
            "alerts_fired": len(alerts),
            "alert_pairs": [a["pair"] for a in alerts],
        }

    async def _check_pair(self, db, pair: str, stats: dict) -> dict | None:
        """Check if a pair has extreme funding conditions right now."""
        symbol = pair.replace("-", "")

        # Fetch funding rate history (8h intervals, need ~30 data points)
        funding_docs = await (
            db["bybit_funding_rates"]
            .find({"pair": pair})
            .sort("timestamp_utc", -1)
            .limit(self.z_window + 10)
            .to_list(length=self.z_window + 10)
        )
        if len(funding_docs) < self.z_window // 2:
            return None

        rates = [float(d.get("funding_rate", 0)) for d in reversed(funding_docs)]
        arr = np.array(rates)

        # Z-score
        window = min(self.z_window, len(arr))
        mean = np.mean(arr[-window:])
        std = np.std(arr[-window:])
        if std <= 0:
            return None

        current_rate = arr[-1]
        z = (current_rate - mean) / std

        if abs(z) < self.z_threshold:
            return None

        # Streak: consecutive same-sign funding events
        sign = 1 if current_rate > 0 else -1
        streak = 0
        for r in reversed(rates):
            if (r > 0 and sign > 0) or (r < 0 and sign < 0):
                streak += 1
            else:
                break

        if streak < self.streak_min:
            return None

        # OI z-score check
        oi_docs = await (
            db["bybit_open_interest"]
            .find({"pair": pair})
            .sort("timestamp_utc", -1)
            .limit(self.z_window + 10)
            .to_list(length=self.z_window + 10)
        )
        oi_z = 0.0
        if len(oi_docs) >= 10:
            oi_vals = [float(d.get("oi_value", 0)) for d in reversed(oi_docs)]
            oi_arr = np.array(oi_vals)
            # Log-delta z-score
            log_deltas = np.diff(np.log(oi_arr[oi_arr > 0] + 1e-10))
            if len(log_deltas) >= 10:
                oi_z = (log_deltas[-1] - np.mean(log_deltas)) / (np.std(log_deltas) + 1e-10)

        if oi_z < self.oi_z_min:
            return None

        # Direction: fade the funding payers
        direction = "SHORT" if current_rate > 0 else "LONG"

        return {
            "pair": pair,
            "direction": direction,
            "funding_rate": current_rate,
            "z_score": z,
            "streak": streak,
            "oi_z": oi_z,
            "backtest_pf": stats["pf"],
            "backtest_wr": stats["wr"],
        }

    def _format_alerts(self, alerts: List[dict]) -> str:
        lines = ["🚨 <b>X9 Funding Extreme Alert</b>\n"]
        for a in sorted(alerts, key=lambda x: abs(x["z_score"]), reverse=True):
            emoji = "🔴" if a["direction"] == "SHORT" else "🟢"
            lines.append(
                f"{emoji} <b>{a['pair']}</b> → {a['direction']}\n"
                f"   Funding: {a['funding_rate']*100:.4f}% (z={a['z_score']:.1f}, streak={a['streak']})\n"
                f"   OI z: {a['oi_z']:.1f} | Backtest: PF={a['backtest_pf']:.1f}, WR={a['backtest_wr']:.0f}%"
            )
        lines.append(f"\n<i>Scan: {len(PAIR_STATS)} pairs checked at {datetime.now(timezone.utc).strftime('%H:%M UTC')}</i>")
        return "\n".join(lines)

    async def _send_telegram(self, text: str) -> None:
        if not self.bot_token or not self.chat_id:
            logger.warning("Telegram not configured, skipping alert")
            return

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.warning(f"Telegram send failed: {resp.status} {body}")
        except Exception as e:
            logger.warning(f"Telegram send error: {e}")
