"""Tests for config.py and notifier.py — Configuration loading and Telegram safety."""
import os
from unittest.mock import patch

import pytest

from app.services.hl_mm.config import (
    HLMMConfig, load_config, PairConfig, FeeConfig, DEFAULT_PAIR_CONFIGS,
)
from app.services.hl_mm.notifier import TelegramNotifier


class TestConfig:
    def test_load_config_returns_defaults(self):
        cfg = load_config()
        assert isinstance(cfg, HLMMConfig)
        assert cfg.risk.daily_stop_usd > 0
        assert cfg.fees.hl_maker_fee_bps == 1.44

    def test_pair_configs_exist(self):
        assert "BIO" in DEFAULT_PAIR_CONFIGS
        assert "ORDI" in DEFAULT_PAIR_CONFIGS
        assert DEFAULT_PAIR_CONFIGS["BIO"].tox_buffer_bps == 0.9

    def test_fee_config_values(self):
        cfg = load_config()
        assert cfg.fees.hl_taker_fee_bps == 3.50
        assert cfg.fees.bybit_taker_fee_bps == 5.50


class TestTelegramNotifier:
    def test_disabled_without_token(self):
        """Should not crash when token/chat_id missing."""
        with patch.dict(os.environ, {"TELEGRAM_BOT_TOKEN": "", "TELEGRAM_CHAT_ID": ""}, clear=False):
            n = TelegramNotifier(bot_token="", chat_id="")
            assert n.enabled is False

    def test_enabled_with_token(self):
        n = TelegramNotifier(bot_token="123:ABC", chat_id="-100123")
        assert n.enabled is True

    def test_rate_limiting(self):
        n = TelegramNotifier(bot_token="123:ABC", chat_id="-100123", min_interval_s=5.0)
        # The notifier should have rate limiting configured
        assert n._min_interval == 5.0

    def test_notify_fill_doesnt_crash_when_disabled(self):
        n = TelegramNotifier(bot_token="", chat_id="")
        # Should not raise
        n.notify_fill(coin="BIO", side="bid", size=100, price=0.05, size_usd=5.0, fee=0.01)

    def test_notify_engine_event_doesnt_crash(self):
        n = TelegramNotifier(bot_token="", chat_id="")
        n.notify_engine_event("STARTED", "test message")
