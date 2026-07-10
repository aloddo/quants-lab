"""Tests for the Copy A controller decision core (pure logic).

Current API is decide_net_mirror(before, after, ...) (net-position mirror, the fee-churn fix) +
risk_exit(...). The old decide_on_leader_fill string-`dir` API is gone.

Run: pytest tests/copy_a/test_controller.py -q
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "strategies" / "live"))
from copy_a.controller import (  # noqa: E402
    decide_net_mirror, risk_exit, FollowerPos, ControllerConfig,
)

W = "0xABC0000000000000000000000000000000000001"
PAIR = (W.lower(), "BTC")


def cfg(**kw):
    base = dict(allowed_pairs=frozenset({PAIR}), order_size_usd=50.0, cooldown_s=30.0,
                stop_frac=0.05, trail_frac=0.03, trail_arm_frac=0.02, max_hold_s=6 * 3600)
    base.update(kw)
    return ControllerConfig(**base)


def long_pos(sz=0.001, entry=50000.0):
    return FollowerPos(W.lower(), "BTC", signed_sz=sz, entry_px=entry, peak_gain_frac=0, opened_ts=0)


# ---- entries (leader crosses flat -> directional) ----
def test_open_long_creates_buy_entry():
    it = decide_net_mirror(0.0, 2.0, W, "BTC", 50000.0, None, cfg(), now=1000, last_entry_ts=0)
    assert it is not None and it.is_buy and not it.reduce_only
    assert abs(it.sz - 50.0 / 50000.0) < 1e-12


def test_open_short_creates_sell_entry():
    it = decide_net_mirror(0.0, -2.0, W, "BTC", 50000.0, None, cfg(), now=1000, last_entry_ts=0)
    assert it is not None and not it.is_buy and not it.reduce_only


def test_unbound_pair_ignored():
    it = decide_net_mirror(0.0, 2.0, W, "ETH", 3000.0, None, cfg(), now=1000, last_entry_ts=0)
    assert it is None


def test_no_pyramiding_same_direction():
    # leader scales in (both net long, no zero-cross) while we already hold -> hold, no order
    it = decide_net_mirror(1.0, 2.0, W, "BTC", 51000.0, long_pos(), cfg(), now=1000, last_entry_ts=0)
    assert it is None


def test_cooldown_blocks_entry():
    it = decide_net_mirror(0.0, 2.0, W, "BTC", 50000.0, None, cfg(), now=1010, last_entry_ts=1000)
    assert it is None  # 10s < 30s cooldown


# ---- exits mirroring leader return-to-flat ----
def test_leader_flat_exits_long_reduce_only():
    it = decide_net_mirror(2.0, 0.0, W, "BTC", 51000.0, long_pos(), cfg(), now=2000, last_entry_ts=0)
    assert it is not None and it.reduce_only and not it.is_buy  # sell to close long
    assert abs(it.sz - 0.001) < 1e-12


def test_leader_flat_with_no_position_noop():
    it = decide_net_mirror(2.0, 0.0, W, "BTC", 51000.0, None, cfg(), now=2000, last_entry_ts=0)
    assert it is None


def test_leader_flip_exits_first():
    # leader long -> short while we hold long: exit (reduce-only) first; a later once-flat call re-enters
    it = decide_net_mirror(2.0, -2.0, W, "BTC", 51000.0, long_pos(), cfg(), now=2000, last_entry_ts=0)
    assert it is not None and it.reduce_only and not it.is_buy


# ---- follower risk exits ----
def test_stop_loss_flattens_long():
    it, _ = risk_exit(long_pos(), 47000.0, now=100, cfg=cfg())  # -6% <= -5% stop
    assert it is not None and it.reduce_only and not it.is_buy


def test_trailing_tp_flattens_after_giveback():
    f = long_pos()
    risk_exit(f, 52000.0, now=100, cfg=cfg())  # arm at +4%
    assert f.peak_gain_frac >= 0.04
    it, _ = risk_exit(f, 50250.0, now=200, cfg=cfg())  # give back >3% from peak
    assert it is not None and it.reduce_only


def test_no_trail_before_armed():
    f = long_pos()
    risk_exit(f, 50500.0, now=100, cfg=cfg())  # +1% < 2% arm
    it, _ = risk_exit(f, 50000.0, now=200, cfg=cfg())
    assert it is None


def test_max_hold_flattens():
    it, _ = risk_exit(long_pos(), 50100.0, now=6 * 3600 + 1, cfg=cfg())
    assert it is not None and it.reduce_only


def test_short_stop_uses_inverted_gain():
    f = FollowerPos(W.lower(), "BTC", -0.001, entry_px=50000, peak_gain_frac=0, opened_ts=0)
    it, _ = risk_exit(f, 53000.0, now=100, cfg=cfg())  # short down 6% -> stop, buy to close
    assert it is not None and it.reduce_only and it.is_buy
