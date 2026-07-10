"""Copy A -- DRAFT supervised LIVE runner (Gate-1 probe).

STATUS: DRAFT. DRY-RUN by DEFAULT. This file places NO live order unless `--live` is passed AND the
RiskBroker admits it. Pending Fable + Codex code review + a final Alberto go before any `--live` run.

WHAT THIS DOES
--------------
Wires the already-reviewed pure components into one supervised loop:
  hl_ws_feed.HLWSFeed        -> real-time leader + own (parent) clearinghouse snapshots (fail-closed)
  copy_a.controller          -> decide_net_mirror() / risk_exit()  (SAME code the shadow harness drives)
  copy_a.risk_broker         -> the SOLE signer + admission gate (caps / kill / reserve / reconcile)
  copy_a.hl_sdk_adapters     -> the SOLE audited HL SDK surface (held privately by RiskBroker)

SNAPSHOT -> BEFORE/AFTER EVENT DIFFING (the core of turning a position-snapshot feed into the
before/after events decide_net_mirror expects):
  The WS feed reports each leader's CURRENT net signed position per coin (HL pushes on change only).
  We keep a per-(wallet,coin) LAST-SEEN net. On each tick, for a fresh reading:
      leader_pos_before = last_seen[(w,c)]     (what we last acted on)
      leader_pos_after  = current net from the feed
  decide_net_mirror consumes exactly that pair. Because the controller mirrors the leader's NET
  position (not each fill) and keys only on sign(before)->sign(after), collapsing many intermediate
  fills into one before/after diff is CORRECT: any round-trip the feed skipped between polls that
  returns the leader to the same net is a genuine no-op for us too. First-sight seeds last_seen
  WITHOUT trading (never enter into a leader's pre-existing position). A WS generation change
  (reconnect) RESETS all last_seen and blocks trading until re-synced (never diff across a reconnect
  -- a V11 failure mode).

FAIL-CLOSED ORDERING each tick: (1) kill-file / latched-halt / WS freshness+generation gate FIRST,
(2) risk_exit (the -25% disaster stop) on our own positions, (3) leader mirror. Every resulting
OrderIntent goes through RiskBroker.submit (await) -- the runner NEVER signs.
"""
from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import logging
import os
import time
from pathlib import Path

# strategies/live must be on sys.path (PYTHONPATH=strategies/live) for these imports.
from copy_a.controller import (
    ControllerConfig, FollowerPos, decide_net_mirror, risk_exit,
)
from copy_a.risk_broker import (
    BrokerConfig, OrderIntent, Result, RiskBroker, Snapshot, Position, Fill,
)

logger = logging.getLogger("copy_a.live_runner")

_EPS = 1e-9


def _sign(x: float) -> int:
    return 1 if x > _EPS else (-1 if x < -_EPS else 0)

# HL addressing (query the PARENT for funds/positions; the AGENT signs).
PARENT_ADDRESS = os.environ.get("HL_QUERY_ADDRESS", "0x11ca20aeb7cd014cf8406560ae405b12601994b4")
AGENT_ADDRESS = os.environ.get("HL_ADDRESS", "0xdf67eda0bc0223060891d49dde9a780a4538c2e3")

# Extra always-checked kill path (config kill_path is checked too).
GLOBAL_KILL_PATH = Path("/tmp/quant-kill")

# Freshness budgets (seconds).
MAX_AGE_OWN_S = 20.0       # OUR account -- strict per-user freshness (keeps the disaster stop fresh)
MAX_AGE_LEADER_S = 30.0    # idle leaders -- feed-liveness (HL only pushes on change)
MAX_AGE_MID_S = 10.0       # a mark older than this is not tradeable


# ---------------------------------------------------------------------------
# config load + assert-on-load
# ---------------------------------------------------------------------------
def _require(cond: bool, msg: str):
    if not cond:
        raise ValueError(f"copy_a config invalid: {msg}")


def _is_addr(s) -> bool:
    return isinstance(s, str) and s.startswith("0x") and len(s) == 42


def load_probe_config(path: str):
    """Load + VALIDATE the WALLET-LEVEL probe config. Raises ValueError (refuse to start) on any
    missing/OOB field.

    Wallet-level scope (Alberto TG10802): config is a list of approved `wallets` + `global` params;
    there is NO per-coin pre-list. The runner discovers coins dynamically per approved wallet from the
    WS feed and copies ANY coin they trade. Positions are scoped to main+xyz dexes (`position_dexes`).

    Returns dict with: raw, ctrl_cfg (base), broker_cfg, order_size_usd, leader_wallets, allowed_wallets,
    position_dexes, kill_paths, max_loss_usd, cross_frac.
    """
    with open(path) as f:
        c = json.load(f)
    _require(isinstance(c, dict) and "global" in c and "wallets" in c,
             "top-level {global,wallets} required")
    g = c["global"]
    wallets = c["wallets"]

    req_g = ["order_size_usd", "alloc_usd", "admit_ceiling_usd", "max_leverage", "max_loss_usd",
             "cooldown_s", "stop_frac", "trail_frac", "trail_arm_frac", "max_hold_s", "kill_path"]
    for k in req_g:
        _require(k in g, f"global.{k} missing")
    _require(g["order_size_usd"] > 0, "order_size_usd must be > 0")
    _require(g["alloc_usd"] > 0, "alloc_usd must be > 0")
    _require(g["admit_ceiling_usd"] >= g["order_size_usd"], "admit_ceiling_usd must be >= order_size_usd")
    _require(0 < g["max_leverage"] <= 10, "max_leverage out of (0,10]")
    _require(g["max_loss_usd"] > 0, "max_loss_usd must be > 0")
    _require(0 < g["stop_frac"] < 1, "stop_frac out of (0,1)")
    _require(g["trail_frac"] > 0 and g["trail_arm_frac"] > 0, "trail_frac/trail_arm_frac must be > 0")
    _require(g["cooldown_s"] >= 0, "cooldown_s must be >= 0")
    _require(g["max_hold_s"] > 0, "max_hold_s must be > 0")
    _require(isinstance(g["kill_path"], str) and g["kill_path"], "kill_path must be a non-empty string")
    # book-cross pad (adapter crosses IOC by this so orders fill); default 0.005. price_buffer (broker
    # projection inflation) is set >= cross_frac so the gate never under-projects the true worst fill.
    cross_frac = float(g.get("cross_frac", 0.005))
    _require(0 < cross_frac < 0.05, "cross_frac out of (0,0.05)")
    price_buffer = max(0.002, cross_frac)

    # position dexes to copy (main "" + xyz). Equity stays account-wide; only POSITIONS are dex-scoped.
    position_dexes = g.get("position_dexes", ["", "xyz"])
    _require(isinstance(position_dexes, list) and len(position_dexes) > 0,
             "global.position_dexes must be a non-empty list")
    _require(all(isinstance(d, str) for d in position_dexes), "position_dexes entries must be strings")
    position_dexes = frozenset(position_dexes)

    _require(isinstance(wallets, list) and len(wallets) > 0, "wallets must be a non-empty list")
    approved: set = set()
    for w in wallets:
        _require(_is_addr(w), f"wallet {w!r} must be a 0x..40 address")
        wl = w.lower()
        _require(wl not in approved, f"duplicate wallet {wl}")
        approved.add(wl)
    allowed_wallets = frozenset(approved)

    # DYNAMIC per-COIN summed-notional cap = n_wallets x order_size x (1+buffer): the worst case if EVERY
    # approved wallet piled into a single coin. One scalar (coins are not pre-listed) bounds single-coin
    # concentration. (6 wallets x $150 x 1.005 ~= $904.5.)
    per_coin_cap_usd = len(allowed_wallets) * float(g["order_size_usd"]) * (1 + price_buffer)

    # base controller cfg. allowed_pairs left EMPTY here; the runner rebinds it per dynamically-discovered
    # (wallet,coin) in _ctrl_cfg_for (wallet-level admission is enforced by the runner iterating only
    # approved wallets + the broker's allowed_wallets gate; the controller stays a pure decision core).
    ctrl_cfg = ControllerConfig(
        allowed_pairs=frozenset(), order_size_usd=float(g["order_size_usd"]),
        cooldown_s=float(g["cooldown_s"]), stop_frac=float(g["stop_frac"]),
        trail_frac=float(g["trail_frac"]), trail_arm_frac=float(g["trail_arm_frac"]),
        max_hold_s=float(g["max_hold_s"]),
    )
    broker_cfg = BrokerConfig(
        allowed_wallets=allowed_wallets, order_size_usd=float(g["order_size_usd"]),
        alloc_usd=float(g["alloc_usd"]), admit_ceiling_usd=float(g["admit_ceiling_usd"]),
        max_leverage=float(g["max_leverage"]), price_buffer=price_buffer,
        per_coin_cap_usd=per_coin_cap_usd,
    )
    kill_paths = [GLOBAL_KILL_PATH, Path(os.path.expanduser(g["kill_path"]))]
    return {
        "raw": c, "ctrl_cfg": ctrl_cfg, "broker_cfg": broker_cfg,
        "order_size_usd": float(g["order_size_usd"]),
        "leader_wallets": sorted(allowed_wallets),
        "allowed_wallets": allowed_wallets, "position_dexes": position_dexes,
        "kill_paths": kill_paths, "max_loss_usd": float(g["max_loss_usd"]), "cross_frac": cross_frac,
    }


# ---------------------------------------------------------------------------
# dry-run adapter: real reads, NO signing (submit/cancel/leverage are logged no-ops)
# ---------------------------------------------------------------------------
class DryRunAdapter:
    """Wraps a real ExchangeAdapter but NEVER signs. A paper-trading adapter: submit() applies the
    intent to an internal PAPER ledger and snapshot() OVERLAYS that ledger onto the real (flat) snapshot.

    Why the overlay (Fable, below-the-line): the real probe account is flat, so a broker that gates on
    the real snapshot would reject every reduce-only mirror EXIT, never exercise the -25% disaster stop,
    and never see cap-stacking -- a dry-run soak would test ENTRIES ONLY. Overlaying the paper fills lets
    the SAME gate path exercise exits + per-coin cap stacking end-to-end without ever placing an order.

    submit() also sets self.last_fill (like the real adapter) so the runner records ACTUAL paper size."""

    def __init__(self, inner):
        self._inner = inner
        self.dry_submits: list = []
        self.last_fill = None
        self._paper: dict = {}     # coin -> Position (signed paper position)

    def snapshot(self):
        base = self._inner.snapshot()
        if not base or not base.ok:
            return base
        merged = dict(base.positions)
        for coin, pp in self._paper.items():
            real = base.positions.get(coin)
            signed = (real.signed_sz if real else 0.0) + pp.signed_sz
            if abs(signed) <= 1e-12:
                merged.pop(coin, None)
            else:
                merged[coin] = Position(coin=coin, signed_sz=signed,
                                        entry_px=pp.entry_px or (real.entry_px if real else 0.0))
        return Snapshot(positions=merged, open_orders=list(base.open_orders),
                        account_equity=base.account_equity, ok=base.ok)

    def set_leverage_2x(self, coin: str) -> bool:
        return True  # no-op (matches the real adapter's leverage no-op)

    def submit(self, intent: OrderIntent, cloid: str) -> Result:
        self.dry_submits.append((intent, cloid))
        logger.warning("[DRY-RUN] WOULD SUBMIT %s cloid=%s", intent, cloid)
        # apply to the paper ledger (mid fill; dry-run does not model slippage)
        delta = intent.sz if intent.is_buy else -intent.sz
        cur = self._paper.get(intent.coin)
        cur_sz = cur.signed_sz if cur else 0.0
        new_sz = cur_sz + delta
        if abs(new_sz) <= 1e-12:
            self._paper.pop(intent.coin, None)
        else:
            # keep the opening entry_px for an increasing position; keep prior for a reduce
            entry = intent.limit_px if (cur is None or (cur_sz >= 0) == (delta >= 0)) else (cur.entry_px or intent.limit_px)
            self._paper[intent.coin] = Position(coin=intent.coin, signed_sz=new_sz, entry_px=entry)
        self.last_fill = Fill(coin=intent.coin, signed_sz=delta, avg_px=intent.limit_px)
        return Result.ACCEPTED  # simulated fill so the loop's follower ledger advances

    def cancel_all_non_reduce(self) -> None:
        logger.warning("[DRY-RUN] WOULD cancel_all_non_reduce (skipped)")

    def sz_decimals(self, coin: str) -> int:
        return self._inner.sz_decimals(coin)

    def mark_px(self, coin: str):
        fn = getattr(self._inner, "mark_px", None)
        return fn(coin) if fn is not None else None


# ---------------------------------------------------------------------------
# the runner
# ---------------------------------------------------------------------------
class LiveRunner:
    """Supervised live loop. Inject `feed` (HLWSFeed-shaped) + `broker` (RiskBroker) + a loaded config
    so the whole thing is unit-testable with fakes (no network)."""

    # Dexes whose POSITIONS we copy: main ("") + xyz (builder). Every OTHER dex (e.g. flx) is excluded
    # from leader position aggregation so it does not perturb the before/after diff. Equity stays
    # account-wide (all dexes) -- only positions are dex-scoped. Overridable from config.position_dexes.
    POSITION_DEXES = frozenset({"", "xyz"})

    def __init__(self, cfg: dict, feed, broker: RiskBroker, live: bool = False,
                 parent_address: str = PARENT_ADDRESS, poll_s: float = 1.5,
                 max_age_own_s: float = MAX_AGE_OWN_S, max_age_leader_s: float = MAX_AGE_LEADER_S,
                 max_age_mid_s: float = MAX_AGE_MID_S, flatten_foreign: bool = False,
                 max_ws_outage_s: float = 60.0):
        self.cfg = cfg
        self.feed = feed
        self.broker = broker
        self.live = live
        self.parent = parent_address.lower()
        self.poll_s = poll_s
        self.max_age_own_s = max_age_own_s
        self.max_age_leader_s = max_age_leader_s
        self.max_age_mid_s = max_age_mid_s
        self.flatten_foreign = flatten_foreign      # cold-start guard: never auto-flatten foreign capital
        self.max_ws_outage_s = max_ws_outage_s      # hard WS-outage budget -> emergency REST flatten

        self.leader_wallets: list[str] = cfg["leader_wallets"]
        self.order_size_usd: float = cfg["order_size_usd"]         # uniform fixed size (no per-pair sizes)
        self.position_dexes: frozenset = cfg.get("position_dexes", self.POSITION_DEXES)  # main+xyz
        self.kill_paths: list[Path] = cfg["kill_paths"]
        self.max_loss_usd = cfg.get("max_loss_usd")  # pre-registered abort #1 (drawdown-from-start halt)

        # state
        self._last_seen: dict = {}        # (wallet,coin) -> last leader net signed size we acted on
        self._followers: dict = {}        # (wallet,coin) -> FollowerPos (our intent ledger; reconciled)
        self._last_entry_ts: dict = {}    # (wallet,coin) -> ts of last entry (cooldown)
        self._generation = None           # WS generation we are synced to
        self._halted = False              # runner-level latch: stop OPENING (reduce-only flatten still ok)
        self._stop = False
        self._start_equity = None         # account equity at launch (for the max_loss abort)
        self._own_stale_since = None      # first ts our own WS state went stale (WS-outage budget)

    # ---- per-(wallet,coin) controller cfg (wallet-level: bind allowed_pairs to THIS discovered key so the
    # pure controller's pair-gate passes; uniform fixed order size) ----
    def _ctrl_cfg_for(self, key) -> ControllerConfig:
        return dataclasses.replace(self.cfg["ctrl_cfg"], order_size_usd=self.order_size_usd,
                                   allowed_pairs=frozenset({(key[0].lower(), key[1])}))

    # ---- fresh tradeable mark: WS mid (main+xyz) first, REST fallback second. None => no fresh mark
    # anywhere (fail-closed: refuse to OPEN; escalate an already-HELD coin to flatten). ----
    def _fresh_mark(self, coin: str):
        m = self.feed.get_mid(coin, self.max_age_mid_s)
        if m is not None:
            return m
        return self.broker.mark_px(coin)

    # ---- kill file ----
    def _kill_present(self) -> bool:
        for p in self.kill_paths:
            try:
                if p.exists():
                    return True
            except OSError:
                return True  # cannot even stat -> fail-closed
        return False

    def _halt(self, reason: str):
        if not self._halted:
            logger.error("HALT LATCHED: %s", reason)
        self._halted = True

    # ---- own-state reconciliation (live only) ----
    def _reconcile_own(self, own_pos: dict) -> bool:
        """own_pos = {coin: net_szi} from the feed on our parent. Compare against the SUM of our local
        follower ledger per coin; any divergence beyond lot tolerance => foreign activity / missed
        fill => latch HALT (fail-closed). Returns True if reconciled OK (safe to keep trading).

        DRY-RUN: skipped -- orders were never actually placed, so the real account stays flat while the
        paper ledger grows; the ledger IS the paper truth."""
        if not self.live:
            return True
        # expected net per coin from local ledger
        expected: dict = {}
        for (w, c), f in self._followers.items():
            expected[c] = expected.get(c, 0.0) + f.signed_sz
        coins = set(expected) | set(own_pos)
        for c in coins:
            exp = expected.get(c, 0.0)
            act = float(own_pos.get(c, 0.0))
            tol = max(1e-6, 0.02 * max(abs(exp), abs(act)))
            if abs(exp - act) > tol:
                self._halt(f"own-state divergence coin={c} ledger={exp:.6f} exchange={act:.6f}")
                return False
        return True

    # ---- submit wrapper ----
    async def _submit(self, intent: OrderIntent, key, reason: str) -> Result:
        logger.info("submit(%s) key=%s intent=%s live=%s", reason, key, intent, self.live)
        try:
            res = await self.broker.submit(intent)
        except Exception as e:  # broker should not raise, but never let the loop die on submit
            self._halt(f"broker.submit raised: {e!r}")
            return Result.UNKNOWN
        logger.info("  -> %s", res)
        return res

    # ---- flatten everything reduce-only (best effort; broker allows reduce-only while halted) ----
    async def flatten_all(self):
        rest_marks: dict = {}      # coin -> REST-derived crossing reference when the WS mid is stale
        if self.live:
            own, _gen, conn = self.feed.user_aggregate(self.parent, self.max_age_own_s, strict=True)
            if own is not None and conn:
                own_pos = {coin: float(sz) for coin, sz in own["pos"].items() if abs(float(sz)) > 0}
            else:
                # WS own-state unavailable -> REST fallback via the broker's read-only snapshot so a dead
                # WS does not disable the stop + flatten (Fable below-the-line).
                logger.error("flatten_all: WS own-state unavailable -> REST snapshot fallback")
                snap = None
                try:
                    snap = self.broker.snapshot()
                except Exception:
                    logger.exception("flatten_all: broker REST snapshot failed")
                if snap is None or not snap.ok:
                    logger.error("flatten_all: REST snapshot also unavailable; cannot size exits")
                    return
                own_pos = {p.coin: p.signed_sz for p in snap.positions.values() if p.signed_sz != 0}
                for p in snap.positions.values():
                    rest_marks[p.coin] = self.broker.mark_px(p.coin) or (p.entry_px or None)
            # cold-start guard: only flatten KNOWN (our-ledger) coins unless --flatten-foreign was passed;
            # foreign pre-existing capital is halt-only (never auto-flattened).
            known = {f.coin for f in self._followers.values()}
            targets = []
            for coin, signed in own_pos.items():
                if coin not in known and not self.flatten_foreign:
                    logger.error("flatten_all: FOREIGN position %s (%.6f) NOT flattened (halt-only; "
                                 "pass --flatten-foreign to override)", coin, signed)
                    continue
                targets.append((coin, signed))
        else:
            targets = [(f.coin, f.signed_sz) for f in self._followers.values() if f.signed_sz != 0]
        for coin, signed in targets:
            mark = (self.feed.get_mid(coin, self.max_age_mid_s) or rest_marks.get(coin)
                    or self.broker.mark_px(coin))
            if mark is None or signed == 0:
                logger.error("flatten_all: no mark for %s -> cannot flatten this pass", coin)
                continue
            intent = OrderIntent(wallet="__flatten__", coin=coin, is_buy=(signed < 0),
                                 sz=abs(signed), limit_px=mark, reduce_only=True)
            res = await self._submit(intent, ("__flatten__", coin), reason="flatten")
            if res == Result.ACCEPTED:
                for k in [k for k, f in self._followers.items() if f.coin == coin]:
                    self._followers.pop(k, None)

    # ---- one supervised tick ----
    async def tick(self):
        now = time.time()

        # (1) kill / halt gate FIRST
        if self._kill_present():
            self._halt("kill file present")
        if self._halted or self.broker.halted:
            await self.flatten_all()
            return

        # own state freshness (strict) + generation sync
        own, gen, conn = self.feed.user_aggregate(self.parent, self.max_age_own_s, strict=True)
        if own is None or not conn:
            # track how long our own WS state has been stale. Under the hard budget we skip (fail-closed);
            # beyond it we latch HALT and run an EMERGENCY flatten (which falls back to REST) so a dead WS
            # cannot silently disable the disaster stop + flatten (Fable below-the-line).
            if self._own_stale_since is None:
                self._own_stale_since = now
            stale_for = now - self._own_stale_since
            if stale_for > self.max_ws_outage_s:
                self._halt(f"own WS state stale for {stale_for:.0f}s > budget {self.max_ws_outage_s:.0f}s")
                await self.flatten_all()
            else:
                logger.warning("own WS state stale/partial for %.0fs -> skip tick (fail-closed)", stale_for)
            return
        self._own_stale_since = None
        if self._generation is None:
            self._generation = gen
            logger.info("synced to WS generation %s", gen)
        elif gen != self._generation:
            logger.warning("WS generation %s->%s (reconnect): reset last_seen, resync, no trade this tick",
                           self._generation, gen)
            self._last_seen = {}
            self._generation = gen
            return

        # (1b) max-loss abort (pre-registered abort #1): record equity at launch; halt+flatten if the
        # account draws down more than max_loss_usd from the launch equity.
        equity = float(own.get("av", 0.0) or 0.0)
        if self._start_equity is None and equity > 0:
            self._start_equity = equity
            logger.info("launch equity recorded: %.2f (max_loss abort at %.2f)",
                        equity, equity - (self.max_loss_usd or 0.0))
        if (self.max_loss_usd is not None and self._start_equity is not None
                and equity > 0 and equity < self._start_equity - self.max_loss_usd):
            self._halt(f"max_loss breached: equity={equity:.2f} < start={self._start_equity:.2f} "
                       f"- max_loss={self.max_loss_usd:.2f}")
            await self.flatten_all()
            return

        # reconcile our own positions against exchange truth (live only)
        if not self._reconcile_own(own["pos"]):
            await self.flatten_all()
            return

        # (2) risk exits FIRST (disaster stop) on our own follower positions
        for key in list(self._followers.keys()):
            foll = self._followers.get(key)
            if foll is None or foll.signed_sz == 0:
                continue
            mark = self._fresh_mark(foll.coin)
            if mark is None:
                # HARD SAFETY (requirement 5): we HOLD a position we can no longer price on ANY source
                # (WS main+xyz mid AND REST both failed) -> the -25% disaster stop is blind. Never sit on
                # an unprotectable position: latch HALT and escalate to the REST-fallback flatten (which
                # retries pricing each pass). Applies equally to main and xyz coins.
                logger.error("risk_exit: NO fresh mark for HELD %s (WS+REST both stale) -> HALT + flatten",
                             foll.coin)
                self._halt(f"no fresh mark for held position {foll.coin}")
                await self.flatten_all()
                return
            intent, foll2 = risk_exit(foll, mark, now, self._ctrl_cfg_for(key))
            self._followers[key] = foll2
            if intent is not None:
                # rebind flatten intent's wallet to the real leader wallet so exposure attribution is clean
                intent = dataclasses.replace(intent, wallet=key[0])
                res = await self._submit(intent, key, reason="risk_exit")
                if res == Result.ACCEPTED:
                    self._followers.pop(key, None)

        # (3) leader mirror: snapshot -> before/after diff
        for wallet in self.leader_wallets:
            # main+xyz dex positions only (config position_dexes): any OTHER dex (flx, ...) is excluded so
            # it does not perturb the before/after diff. Equity (av) stays account-wide.
            agg, lgen, lconn = self.feed.user_aggregate(wallet, self.max_age_leader_s, strict=False,
                                                        include_dexes=self.position_dexes)
            if agg is None or not lconn or lgen != self._generation:
                continue  # stale/partial leader or spans a reconnect -> skip (fail-closed)
            # DYNAMIC coin discovery (wallet-level, no pre-list): copy ANY coin this approved wallet trades.
            # Iterate the union of coins the leader CURRENTLY holds AND coins we already track for this
            # wallet -- a leader that CLOSED a coin drops it from `pos` (szi=0 unreported), so we must still
            # process that coin's transition-to-0 to mirror the exit.
            coins = set(agg["pos"].keys())
            coins |= {c for (w, c) in self._last_seen if w == wallet}
            coins |= {c for (w, c) in self._followers if w == wallet}
            for coin in sorted(coins):
                key = (wallet, coin)
                cur = float(agg["pos"].get(coin, 0.0))
                if key not in self._last_seen:
                    # FIRST SIGHT (incl. post-reconnect re-seed). Normally seed WITHOUT trading (never enter
                    # a leader's pre-existing pos). BUT if we still HOLD a follower here and the freshly-seen
                    # leader sign differs (leader closed or flipped during our blind window), EMIT the
                    # reduce-only exit instead of silently baselining -- else we hold a stale copy until the
                    # -25% stop / max_hold (Fable P0 #4).
                    foll = self._followers.get(key)
                    if foll is not None and foll.signed_sz != 0 and _sign(foll.signed_sz) != _sign(cur):
                        mark = self._fresh_mark(coin)
                        if mark is not None:
                            ex = OrderIntent(wallet=wallet, coin=coin, is_buy=(foll.signed_sz < 0),
                                             sz=abs(foll.signed_sz), limit_px=mark, reduce_only=True)
                            res = await self._submit(ex, key, reason="reseed_exit")
                            if res == Result.ACCEPTED:
                                self._followers.pop(key, None)
                                self._last_seen[key] = cur
                            # on reject: leave last_seen UNSEEDED so we retry the exit next tick
                            continue
                        # no mark -> retry next tick (do not seed, so we re-evaluate)
                        continue
                    self._last_seen[key] = cur
                    continue
                before = self._last_seen[key]
                mark = self._fresh_mark(coin)
                if mark is None:
                    # HARD SAFETY (requirement 5): no fresh mark anywhere for this coin (main or xyz). We
                    # must NEVER OPEN a position we cannot price for the disaster stop + order crossing.
                    # Refuse: keep last_seen so the transition is preserved and re-evaluated once a mark
                    # returns; if we already HELD this coin the risk-exit loop above already escalated.
                    logger.warning("no fresh mark for %s -> refuse to act this tick (fail-closed)", coin)
                    continue
                foll = self._followers.get(key)
                intent = decide_net_mirror(before, cur, wallet, coin, mark, foll,
                                           self._ctrl_cfg_for(key), now,
                                           self._last_entry_ts.get(key, -1e18))
                if intent is None:
                    # distinguish a genuine no-op (advance baseline) from a COOLDOWN-DEFERRED entry (do NOT
                    # advance, so the transition retries once cooldown clears -- Fable P0 #5b).
                    sb, sa = _sign(before), _sign(cur)
                    holding = foll is not None and foll.signed_sz != 0
                    deferred_entry = (sa != 0 and not holding and sa != sb
                                      and (now - self._last_entry_ts.get(key, -1e18)) < self._ctrl_cfg_for(key).cooldown_s)
                    if not deferred_entry:
                        self._last_seen[key] = cur
                    continue
                # OPPOSITE-SIGN SAME-COIN guard (Fable P0 #3): entering opposite the summed same-coin
                # follower ledger would net-close another wallet's position on HL (one net pos per coin),
                # wedging its reduce-only exit forever. Prefer MISS over DOUBLE: skip the entry + log.
                if not intent.reduce_only:
                    net_same_coin = sum(f.signed_sz for k, f in self._followers.items() if f.coin == coin)
                    entry_sign = 1 if intent.is_buy else -1
                    if _sign(net_same_coin) != 0 and _sign(net_same_coin) != entry_sign:
                        logger.warning("opposite-sign skip: %s entry sign=%d opposes same-coin ledger net "
                                       "%.6f -> SKIP (miss-over-double)", key, entry_sign, net_same_coin)
                        self._last_seen[key] = cur      # accept the miss; do not re-evaluate this transition
                        continue
                res = await self._submit(intent, key, reason="mirror")
                if res == Result.ACCEPTED:
                    if intent.reduce_only:
                        self._followers.pop(key, None)
                        # FLIP fix (Fable P0 #5a): if the leader is now non-flat (a flip, not a return to
                        # flat), reset last_seen to 0 so the NEXT tick diffs 0->new-side and mirrors the
                        # flipped leg. On a plain return-to-flat cur==0 anyway.
                        self._last_seen[key] = 0.0 if _sign(cur) != 0 else cur
                    else:
                        # record ACTUAL fill size/price if the broker parsed one (partial IOC), else fall
                        # back to intended-size-at-mid (Fable P0 #5c).
                        fill = getattr(self.broker, "last_fill", None)
                        if fill is not None and fill.coin == coin and fill.signed_sz != 0:
                            signed_sz = fill.signed_sz
                            entry_px = fill.avg_px or mark
                        else:
                            base = self.order_size_usd / mark
                            signed_sz = base if intent.is_buy else -base
                            entry_px = mark
                        self._followers[key] = FollowerPos(
                            wallet=wallet, coin=coin, signed_sz=signed_sz,
                            entry_px=entry_px, peak_gain_frac=0.0, opened_ts=now)
                        self._last_entry_ts[key] = now
                        self._last_seen[key] = cur      # advance ONLY on confirmed action (retry on reject)
                # NOTE: on REJECTED/UNKNOWN we do NOT advance last_seen -> the transition is retried
                # next tick (at-least-once for both entries and exits). UNKNOWN also latches broker HALT.

    # ---- supervised loop ----
    async def run(self):
        logger.warning("copy_a live_runner starting: live=%s wallets=%d dexes=%s size=$%.0f "
                       "(DRY-RUN unless --live)", self.live, len(self.leader_wallets),
                       sorted(self.position_dexes), self.order_size_usd)
        while not self._stop:
            try:
                await self.tick()
            except Exception as e:
                # unhandled: latch halt (stop opening), log loudly, keep supervising for flatten only.
                logger.exception("unhandled loop error: %s", e)
                self._halt(f"unhandled loop error: {e!r}")
                try:
                    await self.flatten_all()
                except Exception:
                    logger.exception("flatten during halt also failed")
            # if a kill was seen, flatten then exit trading entirely
            if self._kill_present():
                logger.error("kill file present: flatten + exit trading loop")
                self._halt("kill file present")
                try:
                    await self.flatten_all()
                except Exception:
                    logger.exception("flatten on kill failed")
                break
            await asyncio.sleep(self.poll_s)
        logger.warning("copy_a live_runner loop exited (halted=%s)", self._halted)

    def stop(self):
        self._stop = True


# ---------------------------------------------------------------------------
# real wiring (main) -- imports the SDK lazily so tests never touch the network
# ---------------------------------------------------------------------------
def build_from_config(config_path: str, live: bool, flatten_foreign: bool = False):
    from copy_a.hl_sdk_adapters import HLExchangeAdapter  # lazy: pulls in the hyperliquid SDK
    from hl_ws_feed import HLWSFeed

    cfg = load_probe_config(config_path)
    private_key = os.environ["HL_PRIVATE_KEY"]
    # perp_dexs = the copied position dexes (main "" + xyz) so the adapter loads meta (szDecimals) and can
    # REST-price xyz coins for the disaster stop / order crossing (HARD SAFETY requirement 5).
    perp_dexs = sorted(cfg.get("position_dexes", frozenset({"", "xyz"})))
    real = HLExchangeAdapter(private_key=private_key, parent_address=PARENT_ADDRESS,
                             agent_address=AGENT_ADDRESS, cross_frac=cfg["cross_frac"],
                             perp_dexs=perp_dexs)
    adapter = real if live else DryRunAdapter(real)
    # RiskBroker uses the CONFIG kill_path as its primary kill file (expanded, absolute).
    kill_path = Path(os.path.expanduser(cfg["raw"]["global"]["kill_path"]))
    broker = RiskBroker(adapter, cfg["broker_cfg"], kill_path=kill_path)

    users = list(cfg["leader_wallets"]) + [PARENT_ADDRESS.lower()]
    # builder dexes (non-main) get dedicated mark connections so xyz coins have a fresh live mark.
    mark_dexes = [d for d in perp_dexs if d]
    feed = HLWSFeed(users=users, mark_dexes=mark_dexes)
    runner = LiveRunner(cfg, feed, broker, live=live, flatten_foreign=flatten_foreign)
    return runner, feed


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description="Copy A DRAFT live runner (DRY-RUN by default)")
    ap.add_argument("--config", default="config/copy_a_probe_gate1.DRAFT.json")
    ap.add_argument("--live", action="store_true",
                    help="ACTUALLY submit (default: DRY-RUN, log intents only). Requires review + go.")
    ap.add_argument("--flatten-foreign", action="store_true",
                    help="On divergence/halt, also flatten FOREIGN (non-ledger) positions. Default: "
                         "halt-only, never auto-flatten unknown capital on the main account.")
    ap.add_argument("--poll-s", type=float, default=1.5)
    args = ap.parse_args()

    runner, feed = build_from_config(args.config, live=args.live, flatten_foreign=args.flatten_foreign)
    runner.poll_s = args.poll_s
    if not args.live:
        logger.warning("DRY-RUN mode: no real orders will be placed (pass --live to arm, after review).")
    feed.start()
    try:
        asyncio.run(runner.run())
    finally:
        feed.stop()


if __name__ == "__main__":
    main()
