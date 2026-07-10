"""Tests for the Copy A DRAFT live_runner -- config asserts, snapshot->before/after event diffing
(first-sight no-trade, reconnect reset), kill-file halt, and the dry-run-never-signs guarantee.

No network: the WS feed and the exchange are fakes.

Run (from repo root): PYTHONPATH=strategies/live:research/v15 pytest tests/copy_a/test_live_runner.py -q
"""
import asyncio
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "strategies" / "live"))

from copy_a.live_runner import (  # noqa: E402
    LiveRunner, DryRunAdapter, load_probe_config, PARENT_ADDRESS,
)
from copy_a.risk_broker import (  # noqa: E402
    RiskBroker, BrokerConfig, Snapshot, Position, Result, OrderIntent, Fill,
)
from copy_a.controller import FollowerPos  # noqa: E402

DRAFT_CFG = str(ROOT / "config" / "copy_a_probe_gate1.DRAFT.json")
LEADER = "0x36c097864a03c7f0215c0d43165a734152a12e0b"   # trades BTC/ETH/HYPE/SOL in the DRAFT config


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


# --------------------------------------------------------------------------- fakes
class FakeFeed:
    def __init__(self):
        self.states: dict = {}     # addr_lower -> {coin: szi}   (already dex-scoped: main+xyz)
        self.mids: dict = {}       # main-dex marks
        self.xyz_mids: dict = {}   # builder-dex (xyz) marks -- served by get_mid as a fallback
        self.gen = 1
        self.connected = True
        self.fresh = True
        self.equity = 5000.0       # own-account equity reported as "av" (for the max_loss abort)

    def set_state(self, addr, pos):
        self.states[addr.lower()] = dict(pos)

    def user_aggregate(self, addr, max_age_s, strict=True, exclude_dexes=frozenset(),
                       include_dexes=None):
        # the fake already returns main+xyz-scoped positions, so include_dexes is accepted (the runner
        # passes it) but the scoping itself is unit-tested directly against the real HLWSFeed below.
        if not self.connected:
            return None, self.gen, False
        v = self.states.get(addr.lower())
        if v is None or not self.fresh:
            return None, self.gen, self.connected
        return {"av": self.equity, "mu": 0.0, "upnl": 0.0, "pos": dict(v)}, self.gen, self.connected

    def get_mid(self, coin, max_age_s):
        # main mark first, then the builder-dex (xyz) mark -- mirrors HLWSFeed.get_mid's fallback.
        m = self.mids.get(coin)
        return m if m is not None else self.xyz_mids.get(coin)


class RecordingBroker:
    """Stands in for RiskBroker; records intents, returns a configurable Result."""
    def __init__(self, result=Result.ACCEPTED):
        self.intents: list = []
        self.result = result
        self._halted = False
        self.last_fill = None           # runner reads this for ACTUAL-fill bookkeeping
        self._rest_snapshot = None      # optional Snapshot for the REST-fallback flatten path
        self._rest_marks: dict = {}

    @property
    def halted(self):
        return self._halted

    async def submit(self, intent):
        self.intents.append(intent)
        return self.result

    def snapshot(self):
        return self._rest_snapshot

    def mark_px(self, coin):
        return self._rest_marks.get(coin)


class FakeInnerAdapter:
    """Real-adapter stand-in for the DryRunAdapter to wrap. Its submit MUST never be called in dry-run."""
    def __init__(self, equity=10000.0):
        self._equity = equity
        self.submitted: list = []
        self.leverage_calls: list = []

    def snapshot(self):
        return Snapshot(positions={}, open_orders=[], account_equity=self._equity, ok=True)

    def set_leverage_2x(self, coin):
        self.leverage_calls.append(coin)
        return True

    def submit(self, intent, cloid):
        self.submitted.append((intent, cloid))     # <-- reaching here in dry-run is a FAILURE
        return Result.ACCEPTED

    def cancel_all_non_reduce(self):
        pass

    def sz_decimals(self, coin):
        return 4


def make_runner(broker, live=False, isolate_kill=True):
    cfg = load_probe_config(DRAFT_CFG)
    r = LiveRunner(cfg, FakeFeed(), broker, live=live)
    if isolate_kill:
        # point kill_paths at a guaranteed-absent temp path so a stray /tmp/quant-kill on the host
        # cannot halt the diffing tests.
        r.kill_paths = [Path("/tmp/copy_a_test_absent_kill_" + str(id(r)))]
    # seed OUR own parent state fresh+flat and a BTC mark so ticks proceed
    r.feed.set_state(PARENT_ADDRESS, {})
    r.feed.mids["BTC"] = 50000.0
    return r


# --------------------------------------------------------------------------- config asserts
def test_config_loads_and_validates():
    cfg = load_probe_config(DRAFT_CFG)
    assert len(cfg["leader_wallets"]) == 6            # WALLET-level: a flat approved-wallet list
    assert LEADER.lower() in cfg["allowed_wallets"]
    assert cfg["broker_cfg"].order_size_usd == 150.0
    assert cfg["broker_cfg"].max_leverage == 5.0
    assert cfg["position_dexes"] == frozenset({"", "xyz"})   # main + xyz
    # dynamic per-coin cap = n_wallets * order_size * (1+buffer) = 6 * 150 * 1.005 = 904.5
    assert abs(cfg["broker_cfg"].per_coin_cap_usd - 6 * 150.0 * 1.005) < 1e-6


def test_config_missing_global_key_refuses(tmp_path):
    raw = json.load(open(DRAFT_CFG))
    del raw["global"]["stop_frac"]
    p = tmp_path / "c.json"; p.write_text(json.dumps(raw))
    with pytest.raises(ValueError):
        load_probe_config(str(p))


def test_config_out_of_bounds_stop_frac_refuses(tmp_path):
    raw = json.load(open(DRAFT_CFG))
    raw["global"]["stop_frac"] = 1.5   # not in (0,1)
    p = tmp_path / "c.json"; p.write_text(json.dumps(raw))
    with pytest.raises(ValueError):
        load_probe_config(str(p))


def test_config_bad_wallet_refuses(tmp_path):
    raw = json.load(open(DRAFT_CFG))
    raw["wallets"][0] = "0xNOTANADDRESS"
    p = tmp_path / "c.json"; p.write_text(json.dumps(raw))
    with pytest.raises(ValueError):
        load_probe_config(str(p))


def test_config_empty_wallets_refuses(tmp_path):
    raw = json.load(open(DRAFT_CFG))
    raw["wallets"] = []
    p = tmp_path / "c.json"; p.write_text(json.dumps(raw))
    with pytest.raises(ValueError):
        load_probe_config(str(p))


# --------------------------------------------------------------------------- snapshot -> event diffing
def test_first_sight_seeds_without_trading():
    b = RecordingBroker()
    r = make_runner(b)
    r.feed.set_state(LEADER, {"BTC": 5.0})   # leader ALREADY holds a position at first sight
    run(r.tick())
    assert b.intents == []                    # never enter into a pre-existing leader position
    assert r._last_seen[(LEADER, "BTC")] == 5.0


def test_flat_to_long_emits_entry_then_flat_emits_exit():
    b = RecordingBroker()
    r = make_runner(b)
    r.feed.set_state(LEADER, {"BTC": 0.0})   # start flat
    run(r.tick())                             # seed last_seen = 0
    assert b.intents == []

    r.feed.set_state(LEADER, {"BTC": 5.0})   # leader opens long
    run(r.tick())
    assert len(b.intents) == 1
    entry = b.intents[-1]
    assert entry.is_buy and not entry.reduce_only and entry.coin == "BTC"
    assert (LEADER, "BTC") in r._followers   # follower ledger recorded on ACCEPTED

    r.feed.set_state(LEADER, {"BTC": 0.0})   # leader returns to flat
    run(r.tick())
    assert len(b.intents) == 2
    ex = b.intents[-1]
    assert ex.reduce_only and not ex.is_buy   # reduce-only exit of the long
    assert (LEADER, "BTC") not in r._followers


def test_reject_does_not_advance_last_seen_so_transition_retries():
    b = RecordingBroker(result=Result.REJECTED)
    r = make_runner(b)
    r.feed.set_state(LEADER, {"BTC": 0.0})
    run(r.tick())                             # seed 0
    r.feed.set_state(LEADER, {"BTC": 5.0})    # open long, but broker REJECTS
    run(r.tick())
    assert len(b.intents) == 1
    assert r._last_seen[(LEADER, "BTC")] == 0.0   # NOT advanced -> retried next tick
    run(r.tick())
    assert len(b.intents) == 2                # same transition retried


def test_reconnect_generation_change_resets_and_blocks_trade():
    b = RecordingBroker()
    r = make_runner(b)
    r.feed.set_state(LEADER, {"BTC": 0.0})
    run(r.tick())                             # sync generation, seed
    assert r._generation == r.feed.gen
    r.feed.set_state(LEADER, {"BTC": 5.0})    # a transition is pending...
    r.feed.gen = 2                            # ...but a reconnect happened first
    run(r.tick())
    assert b.intents == []                     # no trade across a reconnect
    assert r._last_seen == {}                  # last_seen wiped -> re-seed on next sight
    assert r._generation == 2


def test_stale_own_state_skips_tick():
    b = RecordingBroker()
    r = make_runner(b)
    r.feed.set_state(LEADER, {"BTC": 0.0})
    run(r.tick())
    r.feed.set_state(LEADER, {"BTC": 5.0})
    r.feed.fresh = False                       # our own WS state went stale
    run(r.tick())
    assert b.intents == []                      # fail-closed: no trade on stale own state


# --------------------------------------------------------------------------- kill file
def test_kill_file_halts_and_blocks_entry(tmp_path):
    b = RecordingBroker()
    r = make_runner(b)
    kill = tmp_path / "quant-kill"; kill.write_text("halt")
    r.kill_paths = [kill]
    r.feed.set_state(LEADER, {"BTC": 0.0})
    run(r.tick())
    r.feed.set_state(LEADER, {"BTC": 5.0})     # a would-be entry
    run(r.tick())
    assert b.intents == []                       # killed -> no entry
    assert r._halted is True


# --------------------------------------------------------------------------- dry-run never signs
def test_dry_run_never_calls_real_submit():
    inner = FakeInnerAdapter()
    dry = DryRunAdapter(inner)
    bcfg = BrokerConfig(allowed_wallets=frozenset({LEADER.lower()}), order_size_usd=150.0,
                        alloc_usd=4000.0, admit_ceiling_usd=5000.0, max_leverage=5.0,
                        price_buffer=0.0, per_coin_cap_usd=1000.0)
    broker = RiskBroker(dry, bcfg, kill_path=Path("/tmp/copy_a_test_absent_kill_dryrun"))
    r = make_runner(broker, live=False)
    r.feed.set_state(LEADER, {"BTC": 0.0})
    run(r.tick())
    r.feed.set_state(LEADER, {"BTC": 5.0})
    run(r.tick())
    assert inner.submitted == []                 # the REAL signer was never reached
    assert len(dry.dry_submits) == 1             # the dry-run logged exactly the one intent
    assert (LEADER, "BTC") in r._followers        # paper ledger advanced on the simulated fill


# --------------------------------------------------------------------------- max_loss abort (Fix 2)
def test_max_loss_halts_and_flattens():
    b = RecordingBroker()
    r = make_runner(b)
    r.max_loss_usd = 120.0
    r.feed.set_state(LEADER, {"BTC": 0.0})
    run(r.tick())                                 # records start equity 5000
    assert r._start_equity == 5000.0
    # enter a position so there is something to flatten
    r.feed.set_state(LEADER, {"BTC": 5.0})
    run(r.tick())
    assert (LEADER, "BTC") in r._followers
    n_before = len(b.intents)
    # equity drops past the max_loss threshold -> halt + reduce-only flatten
    r.feed.equity = 5000.0 - 121.0
    run(r.tick())
    assert r._halted is True
    flat = b.intents[-1]
    assert flat.reduce_only and flat.coin == "BTC"   # emitted a reduce-only flatten
    assert len(b.intents) > n_before


# --------------------------------------------------------------------------- opposite-sign guard (Fix 3)
def test_opposite_sign_same_coin_entry_skipped():
    import time as _t
    b = RecordingBroker()
    r = make_runner(b)
    W2 = "0x6f83ab8890ed38bf38a31010aa9a5e9ca743bfad"   # a second BTC wallet in the DRAFT config
    # wallet A already holds a LONG BTC follower (seed its ledger + last_seen). opened_ts=now so the
    # 30-day max_hold does not flatten it before the guard runs.
    r._followers[(LEADER, "BTC")] = FollowerPos(LEADER, "BTC", signed_sz=0.003,
                                                entry_px=50000, peak_gain_frac=0, opened_ts=_t.time())
    r._last_seen[(LEADER, "BTC")] = 5.0
    # wallet B is flat at first sight then opens a SHORT -> opposing entry must be SKIPPED
    r.feed.set_state(W2, {"BTC": 0.0})
    run(r.tick())                                 # seed B last_seen = 0
    r.feed.set_state(W2, {"BTC": -5.0})
    run(r.tick())
    entries = [i for i in b.intents if not i.reduce_only]
    assert entries == []                          # opposing short skipped (miss-over-double)
    assert r._last_seen[(W2, "BTC")] == -5.0      # baseline advanced (accept the miss)


# --------------------------------------------------------------------------- reconnect-seed exit (Fix 4)
def test_reconnect_reseed_emits_exit_when_leader_closed_during_disconnect():
    b = RecordingBroker()
    r = make_runner(b)
    r.feed.set_state(LEADER, {"BTC": 0.0})
    run(r.tick())
    r.feed.set_state(LEADER, {"BTC": 5.0})        # leader opens; we mirror -> hold long
    run(r.tick())
    assert (LEADER, "BTC") in r._followers
    # a reconnect wipes last_seen (followers survive); the leader CLOSED during the disconnect
    r.feed.gen = 2
    run(r.tick())                                 # generation change: reset, no trade this tick
    assert r._last_seen == {}
    r.feed.set_state(LEADER, {"BTC": 0.0})        # leader is now flat at re-sight
    n_before = len(b.intents)
    run(r.tick())
    ex = b.intents[-1]
    assert ex.reduce_only and not ex.is_buy       # we EXIT on reseed instead of holding a stale copy
    assert (LEADER, "BTC") not in r._followers
    assert len(b.intents) == n_before + 1


# --------------------------------------------------------------------------- cooldown defer (Fix 5b)
def test_cooldown_deferred_entry_not_dropped():
    import time as _t
    b = RecordingBroker()
    r = make_runner(b)
    key = (LEADER, "BTC")
    r.feed.set_state(LEADER, {"BTC": 0.0})
    run(r.tick())                                 # seed last_seen 0
    r._last_entry_ts[key] = _t.time()             # a very recent entry -> cooldown active
    r.feed.set_state(LEADER, {"BTC": 5.0})        # leader opens long, but cooldown blocks the entry
    run(r.tick())
    assert b.intents == []                        # deferred, not submitted
    assert r._last_seen[key] == 0.0               # NOT advanced -> transition retries after cooldown
    r._last_entry_ts[key] = _t.time() - 10_000    # cooldown long past
    run(r.tick())
    assert len(b.intents) == 1                    # the same transition finally fires
    assert not b.intents[0].reduce_only


# --------------------------------------------------------------------------- flip re-mirrors 2nd leg (Fix 5a)
def test_flip_exit_then_reenters_opposite_side():
    b = RecordingBroker()
    r = make_runner(b)
    key = (LEADER, "BTC")
    r.feed.set_state(LEADER, {"BTC": 0.0})
    run(r.tick())
    r.feed.set_state(LEADER, {"BTC": 5.0})        # open long -> we hold long
    run(r.tick())
    assert (LEADER, "BTC") in r._followers
    r.feed.set_state(LEADER, {"BTC": -5.0})       # leader FLIPS to short
    run(r.tick())                                 # we exit the long (reduce-only)
    assert r._last_seen[key] == 0.0               # reset to 0 so the flipped leg is mirrored next tick
    r._last_entry_ts[key] = 0.0                   # clear cooldown (all ticks run in one wall-clock second)
    run(r.tick())                                 # now diff 0 -> -5 opens the short
    entries = [i for i in b.intents if not i.reduce_only]
    assert len(entries) == 2                      # original long entry + the flipped short entry
    assert not entries[-1].is_buy                 # short side mirrored


# --------------------------------------------------------------------------- partial-fill actuals (Fix 5c)
def test_partial_fill_records_actual_size():
    b = RecordingBroker()
    r = make_runner(b)
    key = (LEADER, "BTC")
    r.feed.set_state(LEADER, {"BTC": 0.0})
    run(r.tick())
    # broker reports a PARTIAL fill: only 0.001 BTC filled at 50100 (not the intended 150/50000)
    b.last_fill = Fill(coin="BTC", signed_sz=0.001, avg_px=50100.0)
    r.feed.set_state(LEADER, {"BTC": 5.0})
    run(r.tick())
    foll = r._followers[key]
    assert abs(foll.signed_sz - 0.001) < 1e-12    # ACTUAL filled size, not intended-size-at-mid
    assert foll.entry_px == 50100.0               # ACTUAL avg fill price


# --------------------------------------------------------------------------- dry-run paper overlay exits (Fix 6)
def test_dry_run_paper_overlay_exercises_exit():
    pair = (LEADER, "BTC")
    inner = FakeInnerAdapter()                    # real account is FLAT
    dry = DryRunAdapter(inner)
    bcfg = BrokerConfig(allowed_wallets=frozenset({LEADER.lower()}), order_size_usd=150.0,
                        alloc_usd=4000.0, admit_ceiling_usd=5000.0, max_leverage=5.0,
                        price_buffer=0.0, per_coin_cap_usd=1000.0)
    broker = RiskBroker(dry, bcfg, kill_path=Path("/tmp/copy_a_test_absent_kill_overlay"))
    r = make_runner(broker, live=False)
    r.feed.set_state(LEADER, {"BTC": 0.0})
    run(r.tick())
    r.feed.set_state(LEADER, {"BTC": 5.0})        # ENTER -> paper long created
    run(r.tick())
    assert pair in r._followers
    # the overlay must now show the paper long in the broker's snapshot (so the reduce-only exit gate passes)
    snap = dry.snapshot()
    assert "BTC" in snap.positions and snap.positions["BTC"].signed_sz > 0
    r.feed.set_state(LEADER, {"BTC": 0.0})        # leader flat -> reduce-only EXIT must be ACCEPTED
    run(r.tick())
    assert pair not in r._followers               # exit went through against the paper overlay
    assert inner.submitted == []                  # real signer never touched
    assert abs(dry.snapshot().positions.get("BTC", Position("BTC", 0.0)).signed_sz) < 1e-9  # paper flat


# --------------------------------------------------------------------------- WS-outage REST flatten (Fix 7)
def test_dead_ws_beyond_budget_triggers_rest_flatten():
    b = RecordingBroker()
    r = make_runner(b, live=True)
    r.max_ws_outage_s = 30.0
    # a foreign-free known position exists on the REST snapshot the broker returns
    b._rest_snapshot = Snapshot(positions={"BTC": Position("BTC", signed_sz=0.002, entry_px=50000)},
                                open_orders=[], account_equity=5000.0, ok=True)
    b._rest_marks = {"BTC": 50000.0}
    r.flatten_foreign = True                      # allow flattening the (foreign, in this test) BTC pos
    # our own WS state goes stale
    r.feed.fresh = False
    run(r.tick())                                 # first stale tick: under budget -> just skip
    assert b.intents == []
    r._own_stale_since -= 100                      # simulate the outage exceeding the hard budget
    run(r.tick())
    assert r._halted is True
    assert any(i.reduce_only and i.coin == "BTC" for i in b.intents)   # REST-fallback flatten fired


# --------------------------------------------------------------------------- foreign cold-start guard (Fix 8)
def test_foreign_position_not_flattened_without_flag():
    b = RecordingBroker()
    r = make_runner(b, live=True)
    r.flatten_foreign = False
    b._rest_snapshot = Snapshot(positions={"DOGE": Position("DOGE", signed_sz=100.0, entry_px=0.1)},
                                open_orders=[], account_equity=5000.0, ok=True)
    b._rest_marks = {"DOGE": 0.1}
    # force the REST-fallback flatten path with a stale WS
    r.feed.fresh = False
    r._own_stale_since = 0.0                       # already way past budget
    run(r.tick())
    assert r._halted is True
    # DOGE is FOREIGN (not in our ledger) and the flag is off -> it must NOT be flattened
    assert not any(i.coin == "DOGE" for i in b.intents)


# --------------------------------------------------------------------------- WALLET-LEVEL: any coin
def test_approved_wallet_any_coin_is_copied():
    """Wallet-level scope: an approved wallet opening a coin that is NOT pre-listed anywhere is copied
    (dynamic discovery from the feed). Proves the runner is no longer gated to a (wallet,coin) pair list."""
    b = RecordingBroker()
    r = make_runner(b)
    r.feed.mids["WIF"] = 2.5                       # a coin never listed in any config
    key = (LEADER, "WIF")
    r.feed.set_state(LEADER, {"WIF": 0.0})
    run(r.tick())                                  # seed flat
    r.feed.set_state(LEADER, {"WIF": 1000.0})      # approved wallet opens WIF
    run(r.tick())
    entries = [i for i in b.intents if not i.reduce_only]
    assert len(entries) == 1 and entries[0].coin == "WIF" and entries[0].is_buy
    assert key in r._followers                     # follower ledger recorded for the discovered coin


def test_unapproved_wallet_never_iterated():
    """A wallet not on the approved list is never read/copied by the runner (it iterates only approved
    leader_wallets); the broker's allowed_wallets gate is a second layer (test_risk_broker)."""
    b = RecordingBroker()
    r = make_runner(b)
    stranger = "0x1111111111111111111111111111111111111111"
    assert stranger not in r.leader_wallets
    r.feed.set_state(stranger, {"BTC": 5.0})       # a non-approved wallet opens a position
    run(r.tick())
    assert b.intents == []                          # never copied


# --------------------------------------------------------------------------- main+xyz dex scoping
def test_user_aggregate_scopes_positions_to_main_and_xyz():
    """HLWSFeed.user_aggregate(include_dexes={'','xyz'}) copies main+xyz positions, EXCLUDES flx, and
    keeps equity ACCOUNT-WIDE across all dexes."""
    import time as _t
    from hl_ws_feed import HLWSFeed
    addr = "0xaaaa000000000000000000000000000000000001"
    f = HLWSFeed(users=[addr])
    now = _t.time()
    f._connected = True
    f._generation = 1
    f._last_msg_ts = now
    f._user_state[addr] = {"ts": now, "states": {
        "":    {"av": 100.0, "mu": 10.0, "upnl": 1.0, "pos": {"BTC": 0.5}},
        "xyz": {"av": 50.0,  "mu": 5.0,  "upnl": 0.5, "pos": {"PURR": 300.0}},
        "flx": {"av": 25.0,  "mu": 2.0,  "upnl": 0.2, "pos": {"FLXCOIN": 9.0}},
    }}
    agg, gen, conn = f.user_aggregate(addr, max_age_s=1e9, strict=False,
                                      include_dexes=frozenset({"", "xyz"}))
    assert conn and agg is not None
    assert agg["pos"] == {"BTC": 0.5, "PURR": 300.0}     # main + xyz only; flx excluded
    assert "FLXCOIN" not in agg["pos"]
    assert abs(agg["av"] - 175.0) < 1e-9                 # equity sums ALL dexes (100+50+25)


def test_get_mid_falls_back_to_builder_dex_mark():
    """get_mid serves a builder-dex (xyz) coin's mark from the dedicated _dex_mids cache when it is
    absent from the main allMids (HARD SAFETY requirement 5: xyz coins get a fresh mark)."""
    import time as _t
    from hl_ws_feed import HLWSFeed
    f = HLWSFeed(users=["0xaaaa000000000000000000000000000000000001"])
    now = _t.time()
    f._connected = True
    f._mids["BTC"] = (50000.0, now)                 # main coin
    f._dex_mids["PURR"] = (0.42, now)               # xyz-only coin
    assert f.get_mid("BTC", 30) == 50000.0
    assert f.get_mid("PURR", 30) == 0.42            # served from the builder-dex cache
    assert f.get_mid("PURR", 30) is not None
    # stale builder mark -> None (fail-closed)
    f._dex_mids["PURR"] = (0.42, now - 100)
    assert f.get_mid("PURR", 30) is None


# --------------------------------------------------------------------------- HARD SAFETY req 5: no mark
def test_no_mark_coin_entry_refused():
    """An approved wallet opens a coin with NO fresh mark on ANY source (WS main+xyz AND REST) -> the
    runner REFUSES to open (never hold a position it cannot stop-protect)."""
    b = RecordingBroker()                            # mark_px() returns None (no _rest_marks)
    r = make_runner(b)
    r.feed.set_state(LEADER, {"ZRO": 0.0})           # ZRO has no mark anywhere
    run(r.tick())                                    # seed flat
    r.feed.set_state(LEADER, {"ZRO": 5.0})           # leader opens ZRO
    run(r.tick())
    assert b.intents == []                            # entry refused (no mark => fail-closed)
    assert (LEADER, "ZRO") not in r._followers


def test_xyz_coin_with_mark_allowed():
    """An xyz coin priced ONLY via the builder-dex mark source IS copyable (entry allowed)."""
    b = RecordingBroker()
    r = make_runner(b)
    r.feed.xyz_mids["PURR"] = 0.5                     # mark available only via the xyz source
    r.feed.set_state(LEADER, {"PURR": 0.0})
    run(r.tick())
    r.feed.set_state(LEADER, {"PURR": 1000.0})
    run(r.tick())
    entries = [i for i in b.intents if not i.reduce_only]
    assert len(entries) == 1 and entries[0].coin == "PURR"
    assert (LEADER, "PURR") in r._followers


def test_held_position_rest_mark_keeps_stop_alive():
    """WS mark gone but REST mark present -> the disaster stop runs on the REST mark (graceful); no halt,
    position still protected."""
    import time as _t
    b = RecordingBroker()
    r = make_runner(b)
    r.feed.mids.clear()                              # WS mark gone
    b._rest_marks = {"BTC": 40000.0}                 # REST still prices it (down 20%, not past -25%)
    r._followers[(LEADER, "BTC")] = FollowerPos(LEADER, "BTC", signed_sz=0.003, entry_px=50000,
                                                peak_gain_frac=0, opened_ts=_t.time())
    r._last_seen[(LEADER, "BTC")] = 5.0
    r.feed.set_state(LEADER, {"BTC": 5.0})
    run(r.tick())
    assert r._halted is False                         # REST mark kept the stop evaluable
    assert (LEADER, "BTC") in r._followers            # still held (-20% has not hit the -25% stop)


def test_held_position_no_mark_anywhere_escalates_halt():
    """WS AND REST marks both gone for a HELD position -> the disaster stop is blind -> HALT + escalate
    to the REST-fallback flatten (never sit on an unprotectable position)."""
    import time as _t
    b = RecordingBroker()                            # mark_px None, snapshot None
    r = make_runner(b)
    r.feed.mids.clear()                              # WS mark gone
    b._rest_marks = {}                               # REST also cannot price it
    r._followers[(LEADER, "BTC")] = FollowerPos(LEADER, "BTC", signed_sz=0.003, entry_px=50000,
                                                peak_gain_frac=0, opened_ts=_t.time())
    r._last_seen[(LEADER, "BTC")] = 5.0
    r.feed.set_state(LEADER, {"BTC": 5.0})
    run(r.tick())
    assert r._halted is True                          # fail-closed: halted (stops opening) on an
    # unprotectable held position; flatten is retried each pass once a mark returns
