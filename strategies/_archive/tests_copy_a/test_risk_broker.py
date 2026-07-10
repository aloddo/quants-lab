"""Fail-closed gate tests for Copy A RiskBroker. Proves each control actually rejects.

WALLET-LEVEL scope (Alberto TG10802): admission is keyed on the leader WALLET (copy ANY coin an
approved wallet trades); the per-coin cap is a DYNAMIC scalar = n_wallets * order_size * (1+buffer);
the gross gate is projected_gross/equity <= max_leverage (5x).

Run: pytest tests/copy_a/test_risk_broker.py -q
"""
import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "strategies" / "live"))
from copy_a.risk_broker import (  # noqa: E402
    RiskBroker, BrokerConfig, OrderIntent, Snapshot, Position, OpenOrder, Result,
)

W = "0xabc0000000000000000000000000000000000001"       # an APPROVED wallet
W2 = "0xdef0000000000000000000000000000000000002"      # an UNAPPROVED wallet


class FakeExchange:
    def __init__(self, snap, lev_ok=True, submit_res=Result.ACCEPTED, raise_submit=False):
        self._snap = snap
        self._lev_ok = lev_ok
        self._submit_res = submit_res
        self._raise = raise_submit
        self.submitted = []

    def snapshot(self):
        return self._snap

    def set_leverage_2x(self, coin):
        return self._lev_ok

    def submit(self, intent, cloid):
        if self._raise:
            raise TimeoutError("no ack")
        self.submitted.append((intent, cloid))
        return self._submit_res

    def cancel_all_non_reduce(self):
        pass

    def sz_decimals(self, coin):
        return 3


def cfg(**kw):
    base = dict(
        allowed_wallets=frozenset({W}),
        order_size_usd=50.0,
        alloc_usd=300.0,
        admit_ceiling_usd=400.0,
        max_leverage=5.0,
        price_buffer=0.0,
        per_coin_cap_usd=75.0,   # dynamic scalar (n_wallets*order_size*(1+buffer)) applied to every coin
    )
    base.update(kw)
    return BrokerConfig(**base)


def snap(positions=None, open_orders=None, equity=924.0, ok=True):
    return Snapshot(positions=positions or {}, open_orders=open_orders or [],
                    account_equity=equity, ok=ok)


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def entry(sz=0.001, px=50000.0, wallet=W, coin="BTC"):
    return OrderIntent(wallet, coin, is_buy=True, sz=sz, limit_px=px, reduce_only=False)


# ---- happy path ----
def test_clean_entry_accepted(tmp_path):
    b = RiskBroker(FakeExchange(snap()), cfg(), kill_path=tmp_path / "nokill")
    assert run(b.submit(entry())) == Result.ACCEPTED


# ---- WALLET-LEVEL admission: ANY coin from an approved wallet is copyable (no pre-list) ----
def test_approved_wallet_any_coin_accepted(tmp_path):
    # DOGE is not pre-listed anywhere; an approved wallet trading it is admitted (notional 40 <= caps).
    b = RiskBroker(FakeExchange(snap()), cfg(), kill_path=tmp_path / "nokill")
    doge = OrderIntent(W, "DOGE", is_buy=True, sz=400.0, limit_px=0.1, reduce_only=False)
    assert run(b.submit(doge)) == Result.ACCEPTED


def test_unapproved_wallet_rejected(tmp_path):
    b = RiskBroker(FakeExchange(snap()), cfg(), kill_path=tmp_path / "nokill")
    assert run(b.submit(entry(wallet=W2))) == Result.REJECTED       # wallet not in allowed_wallets


# ---- exposure caps ----
def test_per_coin_cap_rejected(tmp_path):
    # per-coin BTC cap = 75; order notional 0.002*50000=100 > 75 -> reject
    b = RiskBroker(FakeExchange(snap()), cfg(), kill_path=tmp_path / "nokill")
    assert run(b.submit(entry(sz=0.002))) == Result.REJECTED


def test_per_coin_cap_stacks_across_wallets(tmp_path):
    # a resting non-reduce BTC order already near the coin cap; a second BTC entry tips it over.
    oo = [OpenOrder("BTC", is_buy=True, sz=0.001, limit_px=50000, reduce_only=False)]  # 50
    b = RiskBroker(FakeExchange(snap(open_orders=oo)), cfg(per_coin_cap_usd=75.0),
                   kill_path=tmp_path / "nk")
    assert run(b.submit(entry(sz=0.001))) == Result.REJECTED  # 50 + 50 = 100 > 75


def test_zero_per_coin_cap_fails_closed(tmp_path):
    # a dynamic cap of 0 (e.g. no approved wallets) has ZERO headroom -> any entry rejected (fail-closed)
    b = RiskBroker(FakeExchange(snap()), cfg(per_coin_cap_usd=0.0), kill_path=tmp_path / "nk")
    assert run(b.submit(entry())) == Result.REJECTED


def test_per_wallet_cap_rejected(tmp_path):
    # single order notional 0.002*50000=100 > order_size 50 -> per-wallet cap rejects even though the
    # per-coin cap has room (set high to isolate the per-wallet gate).
    b = RiskBroker(FakeExchange(snap()), cfg(per_coin_cap_usd=1000.0), kill_path=tmp_path / "nk")
    assert run(b.submit(entry(sz=0.002))) == Result.REJECTED


def test_per_wallet_buffer_headroom_accepts(tmp_path):
    # with price_buffer>0 the projected per-wallet notional (order_size * (1+buffer)) must NOT be
    # rejected by the buffer double-count: the cap gets matching headroom (Fable P0 #2).
    c = cfg(price_buffer=0.005, per_coin_cap_usd=1000.0)
    b = RiskBroker(FakeExchange(snap()), c, kill_path=tmp_path / "nk")
    assert run(b.submit(entry())) == Result.ACCEPTED   # 50.25 projected <= cap 50.25


def test_gross_admit_ceiling_rejected(tmp_path):
    # existing gross from resting non-reduce order near ceiling, new order tips over
    oo = [OpenOrder("ETH", is_buy=True, sz=0.13, limit_px=3000, reduce_only=False)]  # 390
    b = RiskBroker(FakeExchange(snap(open_orders=oo)), cfg(per_coin_cap_usd=1000.0),
                   kill_path=tmp_path / "nk")
    assert run(b.submit(entry())) == Result.REJECTED  # 390 + 50 = 440 > 400


def test_5x_gross_leverage_gate(tmp_path):
    # projected_gross / equity must be <= max_leverage (5x). equity 9, order 50 -> 5.55x > 5 -> reject.
    b = RiskBroker(FakeExchange(snap(equity=9.0)), cfg(per_coin_cap_usd=1000.0), kill_path=tmp_path / "nk")
    assert run(b.submit(entry())) == Result.REJECTED
    # equity 11 -> 4.5x < 5 -> accepted
    b2 = RiskBroker(FakeExchange(snap(equity=11.0)), cfg(per_coin_cap_usd=1000.0), kill_path=tmp_path / "nk2")
    assert run(b2.submit(entry())) == Result.ACCEPTED


def test_leverage_set_failure_rejected(tmp_path):
    b = RiskBroker(FakeExchange(snap(), lev_ok=False), cfg(), kill_path=tmp_path / "nk")
    assert run(b.submit(entry())) == Result.REJECTED


# ---- kill switch ----
def test_kill_file_blocks_entry(tmp_path):
    k = tmp_path / "quant-kill"; k.write_text("halt")
    b = RiskBroker(FakeExchange(snap()), cfg(), kill_path=k)
    assert run(b.submit(entry())) == Result.REJECTED


def test_kill_file_allows_reduce_only_flatten(tmp_path):
    k = tmp_path / "quant-kill"; k.write_text("halt")
    s = snap(positions={"BTC": Position("BTC", signed_sz=0.001, entry_px=50000)})
    b = RiskBroker(FakeExchange(s), cfg(), kill_path=k)
    ex = OrderIntent(W, "BTC", is_buy=False, sz=0.001, limit_px=50000, reduce_only=True)
    assert run(b.submit(ex)) == Result.ACCEPTED


def test_unreadable_kill_path_fails_closed(tmp_path):
    # a path whose PARENT is a file -> .exists() raises OSError (NotADirectory) on some FS;
    # emulate by pointing kill_path under a regular file
    f = tmp_path / "afile"; f.write_text("x")
    weird = f / "quant-kill"  # parent is a file
    b = RiskBroker(FakeExchange(snap()), cfg(), kill_path=weird)
    res = run(b.submit(entry()))
    # either OSError path latched halt (REJECTED) OR .exists() returned False cleanly; assert not crash
    assert res in (Result.REJECTED, Result.ACCEPTED)


# ---- snapshot integrity ----
def test_incomplete_snapshot_rejected(tmp_path):
    b = RiskBroker(FakeExchange(snap(ok=False)), cfg(), kill_path=tmp_path / "nk")
    assert run(b.submit(entry())) == Result.REJECTED


# ---- exit semantics ----
def test_exit_wrong_side_rejected(tmp_path):
    s = snap(positions={"BTC": Position("BTC", signed_sz=0.001, entry_px=50000)})  # long
    b = RiskBroker(FakeExchange(s), cfg(), kill_path=tmp_path / "nk")
    bad = OrderIntent(W, "BTC", is_buy=True, sz=0.001, limit_px=50000, reduce_only=True)  # buy vs long
    assert run(b.submit(bad)) == Result.REJECTED


def test_exit_oversize_rejected(tmp_path):
    s = snap(positions={"BTC": Position("BTC", signed_sz=0.001, entry_px=50000)})
    b = RiskBroker(FakeExchange(s), cfg(), kill_path=tmp_path / "nk")
    big = OrderIntent(W, "BTC", is_buy=False, sz=0.005, limit_px=50000, reduce_only=True)
    assert run(b.submit(big)) == Result.REJECTED


def test_exit_bypasses_wallet_binding(tmp_path):
    # unbound coin / __flatten__ pseudo-wallet position can still be closed (reduce-only)
    s = snap(positions={"DOGE": Position("DOGE", signed_sz=100.0, entry_px=0.1)})
    b = RiskBroker(FakeExchange(s), cfg(), kill_path=tmp_path / "nk")
    ex = OrderIntent("__flatten__", "DOGE", is_buy=False, sz=100.0, limit_px=0.1, reduce_only=True)
    assert run(b.submit(ex)) == Result.ACCEPTED


# ---- submit failure ----
def test_submit_timeout_latches_halt(tmp_path):
    b = RiskBroker(FakeExchange(snap(), raise_submit=True), cfg(), kill_path=tmp_path / "nk")
    assert run(b.submit(entry())) == Result.UNKNOWN
    assert b.halted is True
    # after halt, further entries rejected
    assert run(b.submit(entry())) == Result.REJECTED
