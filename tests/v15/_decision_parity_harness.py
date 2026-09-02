"""STAGE-1 decision-parity harness: one leader-event stream -> (a) parity-configured M07, (b) the
live V17 decision core, -> normalized decision tuples, compared order-by-order.

Spec: docs/research/backtest_live_parity_map_20260806.md section C + PLAN v2 (coin-admission,
refusal-to-chase). Pattern: tests/v15/test_exit_parity_leader_flat.py (uninitialized instance via
object.__new__ carrying ONLY the state the driven decision path reads) extended from one pure
function (_leader_flat_or_flipped) to the full decision pipeline:

    V16CopyTrader._on_hl_trade  (verb classification via the shared classify_leader_fill + routing:
                                 OPEN / ADD / REDUCE_NOT_HELD / REVERSE / REDUCE_WE_HOLD)
      -> CopyTrader._on_hl_trade -> _handle_instant_entry (entry gate chain: cooldown, opening-trade,
                                    chase <= max_chase_bps, spread <= max_spread_bps,
                                    depth >= min_book_depth_usd)
      -> CopyTrader._check_exits (SL layer, LEADER_FLAT via _leader_flat_or_flipped on published
                                  snapshots, _pending_reverse via _execute_pending_reverse)

WHAT IS COMPARED (per scenario): the ordered list of decision tuples
    (coin, side, ~notional_usd, reduce_only, verb)        verb in {open, close}
Notional is compared within a relative tolerance (m07 prices fills through its slippage model; the
live side records the $ order size / mark-to-mid close size). Timestamps are deliberately NOT
compared: m07 is a discrete action-cursor replay (copy_latency_ms=4000, exit_latency_ms=30000)
while live is continuous (WS + 10s REST poll x confirms + 90s grace). The fixture asserts the
DECISION SEQUENCE, per the parity map.

WHAT IS DELIBERATELY OUT OF SCOPE (stubbed execution layer, documented per scenario in the tests):
  * IOC fill probability / partial fills (live `_enter_position` L2081+/L7418+): the harness entry
    stub records the decision and simulates a guaranteed fill, mirroring m07's guaranteed fill.
  * knet gate, margin/gross caps, expansion kills (V17 `_enter_position`/`_check_margin_budget`
    L7327+/L7418+): these live INSIDE the stubbed `_enter_position`, i.e. outside the driven core.
  * kill-switch entry blocking (base `_enter_position` L2096 via `_kill_switch_active`) is normally
    inside the stubbed boundary, EXCEPT scenario 10b, which re-points `_enter_position` at the REAL
    base method after performing the L4646 stats-loop sync explicitly (the F5 sync-lag seam) so the
    post-halt refusal is genuinely certified.
  * Mongo persistence and Telegram (stubbed no-ops), REST snapshots (published directly, exactly as
    tests/v15/test_exit_parity_leader_flat.py does).

CLOCK: `time.time` in the engine module is monkeypatched to a controllable fake clock so the
engine's real cooldowns (30s entry, 300s post-exit) and the 90s LEADER_FLAT grace run against the
stream's own timeline instead of wall time. No sleeping, no network, no Mongo.
"""
import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "strategies" / "live"))
sys.path.insert(0, str(REPO / "research" / "v15"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import v15_m07_engine as E                                   # noqa: E402
from test_m07 import FakeMarketData, _ohlc_path, _action, T0  # noqa: E402
import hl_copy_trader_v17 as mod                              # noqa: E402

MS_MIN = E.MS_MIN
W = "0xleader"
CP = "0xcounterparty"       # the other side of every leader print


# ---------------------------------------------------------------------------------------------- #
# The shared leader-event stream element
# ---------------------------------------------------------------------------------------------- #
@dataclass(frozen=True)
class LeaderFill:
    ts_ms: int
    coin: str
    side: str            # "BUY" | "SELL"
    sz: float
    px: float
    start_position: float
    tid: str

    @property
    def signed(self) -> float:
        return self.sz if self.side == "BUY" else -self.sz

    @property
    def position_after(self) -> float:
        after = self.start_position + self.signed
        return 0.0 if abs(after) < 1e-12 else after


# ---------------------------------------------------------------------------------------------- #
# Side A: M07 in PARITY CONFIG
# ---------------------------------------------------------------------------------------------- #
def parity_params(**kw) -> E.EngineParams:
    """The parity configuration from the fixture contract (plan v2 stage 1)."""
    base = dict(copy_latency_ms=4_000, sizing_mode="fixed_notional", fixed_notional_usd=100.0,
                reversal_mode="flatten_only", exit_latency_ms=30_000, exit_entry_grace_ms=90_000,
                leader_dust_floor_usd=10.0)
    base.update(kw)
    return E.EngineParams(**base)


def to_m02_actions(stream) -> pd.DataFrame:
    """Convert the leader-fill stream to m02-style action rows (ENTRY/ADDON/TRIM/EXIT with
    position_after + signed_size), exactly the shape tests/v15/test_m07.py `_action` builds.
    Flip rows are ENTRY rows with a through-zero position_after, matching how
    tests/v15/test_m07_parity_features.py expresses them."""
    rows = []
    for f in stream:
        after = f.position_after
        if abs(f.start_position) < 1e-12:
            at = "ENTRY"
        elif after == 0.0:
            at = "EXIT"
        elif (f.start_position > 0) != (after > 0):
            at = "ENTRY"                    # single-fill flip through zero
        elif abs(after) > abs(f.start_position):
            at = "ADDON"
        else:
            at = "TRIM"
        rows.append(_action(f.coin, f.ts_ms, 0.0, at, position_after=after, signed_size=f.signed))
    return pd.DataFrame(rows)


def run_m07(stream, prices_by_coin, end_ts_ms, params=None, start_equity=10_000.0):
    ohlc = {}
    for coin, prices in prices_by_coin.items():
        ohlc.update(_ohlc_path(coin, prices))
    md = FakeMarketData(ohlc=ohlc)
    return E.step_subaccount(to_m02_actions(stream), md, start_equity,
                             params or parity_params(), end_ts_ms=end_ts_ms)


def m07_decisions(res, with_ts=False):
    """Normalize the m07 fill stream into decision tuples by replaying position deltas.
    reduce_only == the fill moves the held position toward zero; verb open/close from flatness.
    with_ts=True appends the fill's our_ts (ms) as a 6th element so scenarios can certify the
    latency model (copy_latency_ms on entries, exit_latency_ms / entry grace on exits) — codex
    review P1-4: without this the fixture passed with exit_latency_ms=4000 and grace=0."""
    pos, out = {}, []
    for f in res["fills"]:
        coin = f["coin"]
        delta = float(f["our_fill_size"])
        before = pos.get(coin, 0.0)
        after = before + delta
        if abs(after) < 1e-9:
            after = 0.0
        reduce_only = before != 0.0 and (delta > 0) != (before > 0)
        if before == 0.0:
            verb = "open"
        elif after == 0.0:
            verb = "close"
        else:
            verb = "reduce" if reduce_only else "add"
        tup = (coin, "BUY" if delta > 0 else "SELL",
               abs(delta) * float(f["our_fill_px"]), reduce_only, verb)
        out.append(tup + (int(f["our_ts"]),) if with_ts else tup)
        pos[coin] = after
    return out


# ---------------------------------------------------------------------------------------------- #
# Side B: the live V17 decision core on an uninitialized instance
# ---------------------------------------------------------------------------------------------- #
class Clock:
    def __init__(self, t0_s: float):
        self.t = float(t0_s)

    def now(self) -> float:
        return self.t

    def advance(self, dt_s: float) -> None:
        self.t += dt_s


class _Coll:
    """Minimal Mongo-collection stand-in: swallows the forensic writes on the decision path."""
    def insert_one(self, *a, **k):
        pass

    def update_one(self, *a, **k):
        pass

    def delete_one(self, *a, **k):
        pass

    def delete_many(self, *a, **k):
        pass

    def find(self, *a, **k):
        return []

    def find_one(self, *a, **k):
        return None


class _DB:
    def __getitem__(self, name):
        return _Coll()

    def __getattr__(self, name):
        return _Coll()


class LiveHarness:
    """Uninitialized V17CopyTrader carrying ONLY the state the driven decision paths read.

    Decisions are captured by instance-level stubs of the two execution boundaries
    (`_enter_position`, `_exit_position`); everything upstream of those calls is REAL engine code.
    The full attribute inventory below IS the measured decision-core coupling surface -- it is
    reported by the fixture, not hidden.
    """

    def __init__(self, monkeypatch, whitelist, confirms=2, sl_bps=None,
                 t0_s=T0 / 1000.0, order_size=100.0):
        self.clock = Clock(t0_s)
        monkeypatch.setattr(mod.time, "time", self.clock.now)
        monkeypatch.setattr(mod, "_tg", lambda *a, **k: None)

        eng = object.__new__(mod.V17CopyTrader)
        self.eng = eng
        self.decisions = []
        self.exit_calls = []      # raw _exit_position calls: {coin, wallet, trim_size, held_sz, close_sz, ts}

        # -- config surface (matches config/copy_trader_v15recent9 semantics) --
        eng.global_config = {
            "cooldown_s": 30, "max_chase_bps": 15, "max_spread_bps": 20,
            "min_book_depth_usd": 3000,
            "leader_flat_notional_usd": 10.0, "leader_flat_poll_s": 10.0,
            "leader_flat_confirms": confirms, "leader_flat_min_age_s": 90.0,
            "exit_min_trim_pct": 0.85, "exit_min_trim_usd": 1e9, "full_exit_trim_pct": 0.90,
        }
        eng.default_config = {
            "entry_mode": "instant", "exit_type": "LEADER_FLAT",
            "sl_bps": sl_bps, "trail_activate_bps": None, "trail_bps": None,
            "max_hold_s": 2_592_000, "max_addon_multiplier": 50, "exit_twap_min_notional": 1e9,
        }
        eng.wallet_configs = {W: {}}
        eng.target_set = {W}
        eng.leader_to_vault = {}
        eng.coin_whitelist = set(whitelist)
        eng.wallet_groups = {}
        eng.order_size = float(order_size)
        eng.shadow_mode = True          # skips _check_margin_budget (exchange-state gate, out of scope)
        eng.cluster_mode = False
        eng.decoupled_exit = False
        eng.copy_adds_enabled = False        # deployed default (config L321)
        eng.copy_reverse_enabled = False     # deployed default: flatten-only (key absent from config)
        eng.reverse_min_notional = 10.0

        # -- mutable decision state --
        eng.positions = []
        eng._seen_tids = {}
        eng._target_positions = {W: {}}
        eng._position_accumulated = {}
        eng._exit_twap_buffer = {}
        eng._twap_buffer = {}
        eng._twap_entered = set()
        eng.last_entry = {}
        eng._post_exit_cooldown = {}
        eng.mid_prices = {}
        eng._mid_price_ts = {}
        eng._book_depth = {}
        eng._l2_subscribed = set()
        eng._v16_leader_pos = {}
        eng._v16_leg_first = {}
        eng._v16_blocked_signals = 0
        eng._v16_add_fills = 0
        eng._v16_suppressed_reverse = 0
        eng._v16_reverse_fills = 0
        eng._reverse_gen = {}
        # V17 signal-path state: _on_hl_trade (L7353) stamps knet at signal time into this FIFO;
        # the stamps are CONSUMED inside the stubbed _enter_position, so they are minted-but-unread
        # here. _v17_last_target_fill_ts feeds the 30s stale-tracker kill (also in the stubbed layer).
        eng._v17_knet_pending = {}
        eng._v17_last_target_fill_ts = 0.0
        eng._leg_locks = {}
        eng._reverse_opens = []
        eng._reverse_opens_loaded = True
        eng._leader_snapshot = {}
        eng._leader_flat_decision = {}
        eng._leader_flat_confirms = {}
        eng._leader_flat_outage = {}
        eng._leader_flat_outage_alerted = {}
        eng._flatten_requested = False
        eng._trim_requested = False
        eng._baseline_equity = None     # disables the fast global stop unless a test arms it
        eng._kill_reasons = {}
        eng.db = _DB()

        # -- execution-boundary stubs (the decision capture points) --
        harness = self

        async def _enter_stub(coin, is_buy, twap_dedup_key=None, wallet=None,
                              skip_cooldown=False, notional_override=None, **kw):
            notional = float(notional_override or eng.order_size)
            mid = eng.mid_prices.get(coin) or 0.0
            harness.decisions.append((coin, "BUY" if is_buy else "SELL", notional, False, "open"))
            # Simulated guaranteed fill (parity with m07's guaranteed fill; IOC fill probability is
            # deliberately out of scope for the decision fixture).
            eng.positions.append({
                "coin": coin, "wallet": wallet or "", "side": "BUY" if is_buy else "SELL",
                "size": (notional / mid) if mid > 0 else 0.0, "entry_px": mid,
                "filled": True, "entry_time": harness.clock.t, "fill_time": harness.clock.t,
            })
            return True

        async def _exit_stub(pos, trim_size=None):
            # codex review P1-1: the stub must NOT manufacture "full reduce-only close" — it derives
            # the close size from the ACTUAL position state and the ACTUAL trim_size at call time,
            # and records the raw call so scenarios can assert full-vs-trim was the ENGINE's choice.
            mid = eng.mid_prices.get(pos["coin"]) or pos.get("entry_px") or 0.0
            held_sz = abs(pos["size"])
            close_sz = held_sz if trim_size is None else min(abs(trim_size), held_sz)
            full = trim_size is None or close_sz >= held_sz - 1e-12
            side = "SELL" if pos["side"] == "BUY" else "BUY"
            harness.decisions.append((pos["coin"], side, close_sz * mid, True,
                                      "close" if full else "reduce"))
            harness.exit_calls.append({"coin": pos["coin"], "wallet": pos.get("wallet", ""),
                                       "trim_size": trim_size, "held_sz": held_sz,
                                       "close_sz": close_sz, "ts": harness.clock.t})
            # The real _exit_position success path stamps the 300s post-exit cooldown (L3406).
            eng._post_exit_cooldown[(pos.get("wallet", ""), pos["coin"])] = harness.clock.t
            return True

        async def _noop_prefetch(keys):     # snapshots are published by observe() below
            return None

        async def _noop_sweep():            # _leader_book_sweep: alert-only REST reconcile
            return None

        eng._enter_position = _enter_stub
        eng._exit_position = _exit_stub
        eng._persist_position = lambda pos: True
        eng._remove_persisted_position = lambda w, c: True
        eng._prefetch_leader_snapshots = _noop_prefetch
        eng._leader_book_sweep = _noop_sweep
        eng._exchange_position_size_strict = lambda coin: 0.0   # flat after every confirmed exit

    # -- drivers ------------------------------------------------------------------------------- #
    def set_book(self, coin, mid, spread_bps=10.0, depth_usd=50_000.0):
        """Book state such that the chase/spread/depth gates PASS (defaults) or fail (overrides)."""
        half = mid * spread_bps / 2.0 / 1e4
        self.eng.mid_prices[coin] = mid
        self.eng._mid_price_ts[coin] = self.clock.t
        self.eng._book_depth[coin] = {"best_bid": mid - half, "best_ask": mid + half,
                                      "bid_usd": depth_usd, "ask_usd": depth_usd,
                                      "ts": self.clock.t}

    def deliver(self, f: LeaderFill):
        """Feed one leader fill through the REAL WS trade handler chain (V16 -> base)."""
        self.clock.t = max(self.clock.t, f.ts_ms / 1000.0)
        users = [W, CP] if f.side == "BUY" else [CP, W]
        trade = {"coin": f.coin, "px": str(f.px), "sz": str(f.sz), "tid": f.tid,
                 "time": f.ts_ms, "users": users}

        async def _drive():
            self.eng._on_hl_trade(trade)
            for _ in range(4):                 # let the spawned entry task run to completion
                await asyncio.sleep(0)
        asyncio.run(_drive())

    def observe(self, szi_map, wallet=W, dex=""):
        """Publish ONE fresh REST snapshot (a new observation), advancing the clock by the live
        poll interval. Mirrors _prefetch_leader_snapshots: a usable snapshot also flows into the
        shared _target_positions tracker (only for coins PRESENT in the body, exactly as live)."""
        self.clock.advance(float(self.eng.global_config["leader_flat_poll_s"]))
        self.eng._leader_snapshot[(wallet, dex)] = (
            self.clock.t, None if szi_map is None else dict(szi_map))
        if szi_map is not None:
            for c, s in szi_map.items():
                self.eng._target_positions.setdefault(wallet, {})[c] = s

    def sweep(self):
        """One pass of the REAL exit lifecycle owner (_check_exits): SL layer, pending-reverse
        flatten, LEADER_FLAT confirm/exit."""
        asyncio.run(self.eng._check_exits())

    def held(self, coin):
        return [p for p in self.eng.positions
                if p["coin"] == coin and p.get("filled") and not p.get("_ws_exited")]

    def arm_global_stop(self, monkeypatch, baseline_equity, unrealized_pnl,
                        realized_pnl=0.0, stop_pct=0.15):
        """Arm the live fast global stop with injected exchange-truth PnL. The stop's INPUT is
        exchange state (REST `_refresh_exchange_state`), not decision-core state, so the fixture
        injects it as scenario data.

        codex review P1-2: the REAL `_emergency_flatten` (L2447-2479) runs. Only its two lower
        dependencies are stubbed, at their natural boundaries:
          * the clearinghouseState REST read (module `requests.post`, same seam the existing
            test_exit_parity_leader_flat.py patches) serves a fake exchange book mirroring the
            currently held simulated fills;
          * `close_exchange.market_close(coin)` is the decision recorder — the SDK call that would
            place the reduce-only market order. WHICH coins get closed is therefore decided by the
            real engine code walking the (fake) exchange truth, not by the harness.
        `_reconcile_positions` (a separate REST-heavy method) is mirrored minimally: it drops
        tracker rows whose coin is no longer on the exchange, which is its net effect here."""
        eng = self.eng
        eng._baseline_equity = float(baseline_equity)
        eng._session_realized_base = 0.0
        eng._exch_pnl = {"account_net": float(realized_pnl)}
        eng.global_stop_pct = float(stop_pct)
        eng._compute_unrealized_pnl = lambda: float(unrealized_pnl)
        eng.parent_address = "0xparent"
        harness = self

        # Fake exchange truth = the simulated fills currently held (signed szi per coin, main dex).
        self.exchange_book = {p["coin"]: (p["size"] if p["side"] == "BUY" else -p["size"])
                              for p in eng.positions if p.get("filled")}

        class _Resp:
            def __init__(self, payload):
                self._payload = payload

            def json(self):
                return self._payload

        def _fake_post(url, json=None, timeout=None, **kw):
            payload = json or {}
            if payload.get("type") == "clearinghouseState" and not payload.get("dex"):
                aps = [{"position": {"coin": c, "szi": str(s)}}
                       for c, s in harness.exchange_book.items()]
                return _Resp({"assetPositions": aps})
            return _Resp({"assetPositions": []})     # builder dexes: empty

        monkeypatch.setattr(mod.requests, "post", _fake_post)

        class _CloseExchange:
            def market_close(self, coin):
                szi = harness.exchange_book.pop(coin, 0.0)
                mid = eng.mid_prices.get(coin) or 0.0
                harness.decisions.append((coin, "SELL" if szi > 0 else "BUY",
                                          abs(szi) * mid, True, "close"))

        eng.close_exchange = _CloseExchange()

        def _reconcile():
            eng.positions = [p for p in eng.positions if p["coin"] in harness.exchange_book]

        eng._reconcile_positions = _reconcile


# ---------------------------------------------------------------------------------------------- #
# Comparison
# ---------------------------------------------------------------------------------------------- #
def assert_decisions_match(m07, live, rel_tol=0.01):
    """Order-by-order equality of (coin, side, reduce_only, verb); notional within rel_tol.
    rel_tol=1% (codex review P2-1): the deterministic fake liquidity prices m07 fills within ~1bp
    of the mark, so a $91-vs-$100 sizing mutant must fail. CROSS-SIDE timestamps are not compared
    (discrete replay cursor vs continuous live detection, by design); the m07-side latency model is
    certified separately via m07_decisions(with_ts=True) in scenarios 4/4b."""
    assert len(m07) == len(live), (
        f"decision COUNT diverges: m07={len(m07)} live={len(live)}\n  m07: {m07}\n  live: {live}")
    for i, (a, b) in enumerate(zip(m07, live)):
        assert (a[0], a[1], a[3], a[4]) == (b[0], b[1], b[3], b[4]), (
            f"decision #{i} diverges:\n  m07:  {a}\n  live: {b}")
        hi = max(abs(a[2]), abs(b[2]), 1e-9)
        assert abs(a[2] - b[2]) <= rel_tol * hi, (
            f"decision #{i} notional diverges beyond {rel_tol:.0%}: m07 ${a[2]:.2f} live ${b[2]:.2f}")
