"""
Telegram Notifier -- alerts for important HL MM events.

Sends notifications for:
  - Fills (coin, side, size, price, edge)
  - Circuit breaker triggers (coin, reason, duration)
  - Daily PnL summary (at 00:00 UTC)
  - Hedge execution (coin, size, price, slippage)
  - Engine start/stop

Rate limited to max 1 message per 3s to avoid Telegram API 429s.
Uses requests.post to Telegram Bot API (no SDK dependency).
"""
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org"


class TelegramNotifier:
    """Simple Telegram notifier with rate limiting."""

    def __init__(
        self,
        bot_token: Optional[str] = None,
        chat_id: Optional[str] = None,
        min_interval_s: float = 3.0,
        enabled: bool = True,
    ):
        self._token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self._chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID", "")
        self._min_interval = min_interval_s
        self._enabled = enabled and bool(self._token) and bool(self._chat_id)
        self._last_send: float = 0.0
        self._queue: deque[str] = deque(maxlen=50)
        self._lock = threading.Lock()

        if not self._enabled:
            logger.info("TelegramNotifier disabled (missing token or chat_id)")

    @property
    def enabled(self) -> bool:
        return self._enabled

    def _send_raw(self, text: str) -> bool:
        """Send a message via Telegram Bot API. Returns True on success."""
        if not self._enabled:
            return False

        now = time.time()
        if now - self._last_send < self._min_interval:
            # Queue for later
            self._queue.append(text)
            return False

        try:
            url = f"{TELEGRAM_API_BASE}/bot{self._token}/sendMessage"
            resp = requests.post(
                url,
                json={
                    "chat_id": self._chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=5,
            )
            self._last_send = time.time()
            if resp.status_code == 200:
                return True
            else:
                logger.warning(f"Telegram send failed: {resp.status_code} {resp.text[:200]}")
                return False
        except Exception as e:
            logger.warning(f"Telegram send error: {e}")
            return False

    def flush_queue(self) -> int:
        """Send queued messages (one per call, respects rate limit). Returns sent count."""
        sent = 0
        while self._queue:
            now = time.time()
            if now - self._last_send < self._min_interval:
                break
            msg = self._queue.popleft()
            if self._send_raw(msg):
                sent += 1
        return sent

    # ------------------------------------------------------------------
    # High-level notification methods
    # ------------------------------------------------------------------

    def notify_fill(
        self,
        coin: str,
        side: str,
        size: float,
        price: float,
        size_usd: float,
        fee: float,
        edge_bps: float = 0.0,
    ) -> None:
        """Notify about a fill."""
        arrow = "BUY" if side == "bid" else "SELL"
        text = (
            f"<b>FILL</b> {coin}\n"
            f"{arrow} {size:.6f} @ ${price:.6f} (${size_usd:.2f})\n"
            f"Fee: ${fee:.4f} | Edge: {edge_bps:.1f}bps"
        )
        self._send_raw(text)

    def notify_circuit_breaker(
        self,
        coin: str,
        level: int,
        reason: str,
        duration_s: float = 0.0,
    ) -> None:
        """Notify about a circuit breaker trigger."""
        dur_str = f"{duration_s/60:.0f}min" if duration_s > 0 else "rest of day"
        text = (
            f"<b>CIRCUIT BREAKER L{level}</b> {coin}\n"
            f"Reason: {reason}\n"
            f"Duration: {dur_str}"
        )
        self._send_raw(text)

    def notify_hedge(
        self,
        coin: str,
        side: str,
        size: float,
        price: float,
        slippage_bps: float,
        venue: str = "Bybit",
    ) -> None:
        """Notify about a hedge execution."""
        arrow = "BUY" if side.lower() in ("buy", "bid") else "SELL"
        text = (
            f"<b>HEDGE</b> {coin} on {venue}\n"
            f"{arrow} {size:.6f} @ ${price:.6f}\n"
            f"Slippage: {slippage_bps:.1f}bps"
        )
        self._send_raw(text)

    def notify_daily_summary(
        self,
        daily_pnl: float,
        total_fills: int,
        gross_notional: float,
        active_pairs: list[str],
        uptime_hours: float,
    ) -> None:
        """Send daily PnL summary."""
        pnl_emoji = "+" if daily_pnl >= 0 else ""
        text = (
            f"<b>DAILY SUMMARY</b> {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n"
            f"PnL: {pnl_emoji}${daily_pnl:.2f}\n"
            f"Fills: {total_fills}\n"
            f"Gross notional: ${gross_notional:.2f}\n"
            f"Active: {', '.join(active_pairs) if active_pairs else 'none'}\n"
            f"Uptime: {uptime_hours:.1f}h"
        )
        self._send_raw(text)

    def notify_engine_event(self, event: str, details: str = "") -> None:
        """Notify about engine start/stop/error events."""
        text = f"<b>HL MM</b> {event}"
        if details:
            text += f"\n{details}"
        self._send_raw(text)
