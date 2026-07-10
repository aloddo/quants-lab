import time
from datetime import datetime, timezone

import pytest

from app.services.arb_hlbb.config import ArbConfig
from app.services.arb_hlbb.instrument_rules import PairRules
from app.services.arb_hlbb.order_api import BybitOrderAPI
from app.services.arb_hlbb.orchestrator import Orchestrator, RunMode
from app.services.arb_hlbb.price_feed import SpreadSnapshot
from app.services.arb_hlbb.signal_engine import SignalEvent
from app.tasks.data_collection.tick_aggregation_task import _utc_naive


def _snap(pair="APE-USDT", spread=35.0, direction="HL_PREMIUM", ts=None):
    return SpreadSnapshot(
        pair=pair,
        hl_bid=100.5,
        hl_ask=100.6,
        bb_bid=100.0,
        bb_ask=100.1,
        spread_hl_over_bb_bps=spread if direction == "HL_PREMIUM" else 1.0,
        spread_bb_over_hl_bps=spread if direction == "BB_PREMIUM" else 1.0,
        best_spread_bps=spread,
        direction=direction,
        ts=time.time() if ts is None else ts,
    )


def _signal(snap, p90=30.0, p25=5.0):
    return SignalEvent(
        pair=snap.pair,
        signal_type="ENTRY",
        spread_snapshot=snap,
        threshold_p90=p90,
        threshold_p25=p25,
        excess_bps=p90 - p25,
        timestamp=time.time(),
    )


class DummyFeed:
    def __init__(self, snap):
        self.snap = snap

    def get_spread(self, pair):
        return self.snap if self.snap.pair == pair else None


def test_live_entry_requote_rejects_collapsed_edge():
    orch = Orchestrator(ArbConfig(), RunMode.LIVE)
    original = _snap(spread=40.0)
    orch.price_feed = DummyFeed(_snap(spread=20.0))

    assert orch._requote_entry_snapshot("APE-USDT", _signal(original), original) is None


def test_live_entry_requote_uses_fresh_quote_when_edge_survives():
    orch = Orchestrator(ArbConfig(), RunMode.LIVE)
    original = _snap(spread=40.0)
    fresh = _snap(spread=36.0)
    orch.price_feed = DummyFeed(fresh)

    assert orch._requote_entry_snapshot("APE-USDT", _signal(original), original) is fresh


@pytest.mark.asyncio
async def test_trim_entry_mismatch_closes_larger_hl_leg():
    orch = Orchestrator(ArbConfig(), RunMode.LIVE)
    calls = []

    async def fake_unwind_hl(coin, qty, is_buy):
        calls.append((coin, qty, is_buy))
        return True

    orch._unwind_hl = fake_unwind_hl
    rules = PairRules(pair="APE-USDT", coin="APE", bb_symbol="APEUSDT")

    ok = await orch._trim_entry_mismatch(
        rules=rules,
        hl_entry_is_buy=False,
        bb_entry_side="Buy",
        actual_hl=101.0,
        actual_bb=100.0,
    )

    assert ok
    assert calls == [("APE", 1.0, True)]


@pytest.mark.asyncio
async def test_bybit_fill_poll_waits_for_lagged_fill():
    api = BybitOrderAPI(fill_poll_attempts=3, fill_poll_delay_s=0.0)
    calls = {"n": 0}

    async def fake_check(order_id, symbol, requested_qty):
        calls["n"] += 1
        if calls["n"] < 3:
            return {"filled": False, "qty": 0.0, "price": 0.0, "status": "NOT_FILLED"}
        return {"filled": True, "qty": 10.0, "price": 1.0, "status": "FILLED"}

    api._check_fill = fake_check
    fill = await api._wait_for_fill("oid", "APEUSDT", 10.0)

    assert calls["n"] == 3
    assert fill["status"] == "FILLED"
    assert fill["qty"] == 10.0


def test_tick_aggregation_datetime_normalization_to_mongo_shape():
    aware = datetime(2026, 5, 7, 8, 0, tzinfo=timezone.utc)
    normalized = _utc_naive(aware)

    assert normalized.tzinfo is None
    assert normalized == datetime(2026, 5, 7, 8, 0)
