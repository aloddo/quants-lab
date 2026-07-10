"""Copy A RiskBroker -- the SOLE holder of the signing capability.

Per the dual-reviewed plan v2 (work/quant-engineer/copy-a-executor-plan): every order that could
change exposure passes through RiskBroker.submit(OrderIntent). RiskBroker is the ONLY object that
holds the HL `Exchange` (private key). The controller, watchdog, recovery, and flatten paths have no
raw SDK access -- a structural test (tests/copy_a/test_no_raw_signer.py) enforces zero
`Exchange.order`/`market_close`/`update_leverage` call sites outside this module.

Fail-closed admission (Fable + Codex P0s):
  - kill file present OR unreadable -> latch HALT; entries rejected, reduce-only flatten still allowed
  - snapshot incomplete/stale (any query failed) -> reject (no stale cache)
  - entries only: the leader `wallet` must be an APPROVED WALLET (wallet-level whitelist; ANY coin an
    approved wallet trades is copyable -- no per-coin pre-listing)
  - gate on PROJECTED worst-case exposure (fresh signed positions + all non-reduce open orders +
    unresolved reservations + THIS rounded order), not a caller notional
  - per-coin summed notional <= DYNAMIC cap = n_wallets * order_size * (1+buffer); per-wallet order
    <= order_size*(1+buffer); gross <= admit_ceiling; projected_gross / equity <= max_leverage (5x)
  - exposure-increasing size floored to lot precision (never rounded up)
  - one asyncio lock serializes snapshot -> gate -> reserve -> submit -> reconcile
  - submit timeout/unknown -> latch HALT, keep reservation, return UNKNOWN (never silent reject)

This module has NO network/SDK import. The exchange is injected as an adapter implementing
ExchangeAdapter so the whole gate is unit-testable with a fake. hl_sdk_adapters.py provides the real
implementation later.
"""
from __future__ import annotations
import asyncio
import math
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Protocol


# canonical, validated absolute kill path (NO '~' string; expanduser via Path.home()).
DEFAULT_KILL_PATH = Path.home() / ".hermes-kernel" / "quant" / "quant-kill"


class Result(str, Enum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    UNKNOWN = "unknown"      # submitted but terminal status not confirmed -> HALT latched


@dataclass(frozen=True)
class OrderIntent:
    wallet: str            # leader wallet this mirrors ("__flatten__" for risk-driven exits)
    coin: str
    is_buy: bool
    sz: float              # base units, > 0
    limit_px: float
    reduce_only: bool = False


@dataclass(frozen=True)
class Fill:
    """Actual fill parsed from the exchange order response (totalSz/avgPx). signed_sz is + for a buy
    fill, - for a sell fill. Used by the runner to record ACTUAL follower size/price (not intended
    size-at-mid), so a partial IOC fill does not drift the ledger into an oversize-exit wedge."""
    coin: str
    signed_sz: float
    avg_px: float


@dataclass
class Position:
    coin: str
    signed_sz: float       # + long, - short (base units)
    entry_px: float = 0.0


@dataclass
class OpenOrder:
    coin: str
    is_buy: bool
    sz: float
    limit_px: float
    reduce_only: bool


@dataclass
class Snapshot:
    positions: dict         # coin -> Position (signed)
    open_orders: list       # list[OpenOrder]
    account_equity: float   # clearinghouse account value (NOT spot USDC)
    ok: bool = True         # False if ANY underlying query failed / stale / incomplete


@dataclass
class BrokerConfig:
    allowed_wallets: frozenset        # {wallet_lc} -- WALLET-level whitelist (copy ANY coin these trade)
    order_size_usd: float             # fixed per-entry notional target
    alloc_usd: float                  # allocation base (retained for reporting; NOT the per-coin cap)
    admit_ceiling_usd: float          # gross admission ceiling (belt-and-suspenders; the real gate is 5x)
    max_leverage: float = 5.0
    # worst-case fill price buffer for projected notional. MUST be >= the adapter's book-cross pad
    # (cross_frac): the adapter crosses the book at submit (buy mark*(1+cross), sell mark*(1-cross)),
    # so the pre-submit gate must inflate the projected notional by at least the cross to avoid
    # under-projecting the true worst fill (Fable P0 #1).
    price_buffer: float = 0.005
    # DYNAMIC per-COIN summed-notional cap ($). With no pre-listed pairs the cap cannot be precomputed
    # per coin, so the loader sets ONE scalar = n_wallets * order_size_usd * (1 + price_buffer): the max
    # notional if EVERY approved wallet piled into a single coin. Applied to every coin's summed notional
    # (positions + resting + reservations + this order). <= 0 => NO admit headroom (fail-closed).
    per_coin_cap_usd: float = 0.0


class ExchangeAdapter(Protocol):
    """Thin audited surface over the HL SDK. The ONLY object with signing power is an instance of
    this held privately by RiskBroker. Implemented for real in hl_sdk_adapters.py."""
    def snapshot(self) -> Snapshot: ...
    def set_leverage_2x(self, coin: str) -> bool: ...        # set + verify (re-query); False on any doubt
    def submit(self, intent: OrderIntent, cloid: str) -> Result: ...
    def cancel_all_non_reduce(self) -> None: ...
    def sz_decimals(self, coin: str) -> int: ...


def _floor_to(sz: float, decimals: int) -> float:
    q = 10 ** decimals
    return math.floor(sz * q) / q


class RiskBroker:
    def __init__(self, exchange: ExchangeAdapter, cfg: BrokerConfig,
                 kill_path: Path = DEFAULT_KILL_PATH):
        self._ex = exchange                 # SOLE signer. Do not expose.
        self._cfg = cfg
        self._kill_path = Path(kill_path)
        self._lock = asyncio.Lock()
        self._halted = False
        self._reservations: dict = {}       # cloid -> (coin, signed_notional) pending
        self._cloid_seq = 0
        # last ACTUAL fill parsed by the adapter for the most recent ACCEPTED submit (Fill or None).
        # The runner reads this to record actuals instead of intended-size-at-mid.
        self.last_fill = None

    # ---- read-only passthroughs (NOT signing; used by the runner's REST-fallback flatten) ----
    def snapshot(self) -> Snapshot:
        """Read-only fresh snapshot via the adapter's REST surface. Contains NO signing capability.
        Used by the runner to size a reduce-only flatten when the WS feed is stale beyond budget."""
        return self._ex.snapshot()

    def mark_px(self, coin: str):
        """Best-effort REST mark for a coin (None if the adapter does not expose one). Used only as a
        crossing reference for an emergency flatten when the WS mid is stale."""
        fn = getattr(self._ex, "mark_px", None)
        try:
            return fn(coin) if fn is not None else None
        except Exception:
            return None

    # ---- kill / halt (fail-closed) ----
    def _kill_active(self) -> bool:
        try:
            return self._kill_path.exists()
        except OSError:
            # cannot even stat the kill file -> assume killed (fail-closed)
            self._halted = True
            return True

    def _latch_halt(self):
        self._halted = True

    @property
    def halted(self) -> bool:
        return self._halted or self._kill_active()

    def _new_cloid(self) -> str:
        self._cloid_seq += 1
        return f"copya-{self._cloid_seq:08d}"

    # ---- projected worst-case exposure ----
    def _projected(self, snap: Snapshot, intent: OrderIntent):
        """Return (per_position_usd, per_wallet_usd, gross_usd) assuming this order AND every
        resting non-reduce order fully fills at a buffered-worst price."""
        buf = 1 + self._cfg.price_buffer
        # base: current signed positions
        coin_notl: dict = {}
        for c, p in snap.positions.items():
            coin_notl[c] = coin_notl.get(c, 0.0) + abs(p.signed_sz) * p.entry_px
        # add all resting NON-reduce orders (they can still fill)
        for o in snap.open_orders:
            if not o.reduce_only:
                coin_notl[o.coin] = coin_notl.get(o.coin, 0.0) + o.sz * o.limit_px * buf
        # add unresolved reservations (in-flight, invisible to snapshot)
        for _cloid, (c, notl) in self._reservations.items():
            coin_notl[c] = coin_notl.get(c, 0.0) + abs(notl)
        # add THIS order (worst price)
        this_notl = intent.sz * intent.limit_px * buf
        coin_notl[intent.coin] = coin_notl.get(intent.coin, 0.0) + this_notl
        gross = sum(coin_notl.values())
        per_position = coin_notl[intent.coin]
        # per-wallet: this order's contribution only (fixed-size probe, one pair per wallet)
        per_wallet = this_notl
        return per_position, per_wallet, gross, this_notl

    # ---- the single admission gate ----
    async def submit(self, intent: OrderIntent) -> Result:
        async with self._lock:
            self.last_fill = None
            # 1. kill / halt
            if self._halted or self._kill_active():
                if intent.reduce_only:
                    pass  # flatten still allowed while halted/killed
                else:
                    return Result.REJECTED
            # 2. fresh complete snapshot
            snap = self._ex.snapshot()
            if not snap or not snap.ok:
                return Result.REJECTED
            # 3. exits: reduce-only, opposite side, size <= fresh position; pair-binding NOT applied
            if intent.reduce_only:
                pos = snap.positions.get(intent.coin)
                if pos is None or pos.signed_sz == 0:
                    return Result.REJECTED
                pos_is_long = pos.signed_sz > 0
                if intent.is_buy == pos_is_long:                 # must oppose the position
                    return Result.REJECTED
                if intent.sz > abs(pos.signed_sz) + 1e-12:
                    return Result.REJECTED
            else:
                # 4. entries: WALLET whitelist + projected exposure caps + leverage. ANY coin an approved
                # wallet trades is copyable -- there is NO per-coin pre-list (wallet-level scope).
                if intent.wallet.lower() not in self._cfg.allowed_wallets:
                    return Result.REJECTED
                # floor exposure-increasing size to lot precision (never up)
                dec = self._ex.sz_decimals(intent.coin)
                floored = _floor_to(intent.sz, dec)
                if floored <= 0:
                    return Result.REJECTED
                intent = OrderIntent(intent.wallet, intent.coin, intent.is_buy, floored,
                                     intent.limit_px, reduce_only=False)
                per_pos, per_wal, gross, this_notl = self._projected(snap, intent)
                # DYNAMIC per-COIN summed-notional cap (positions + resting + reservations + this order for
                # this coin). One scalar = n_wallets x order_size x (1+buffer) bounds single-coin
                # concentration even if every approved wallet piles in. Fail-closed: cap <= 0 -> rejected.
                coin_cap = self._cfg.per_coin_cap_usd
                if coin_cap <= 0 or per_pos > coin_cap + 1e-6:
                    return Result.REJECTED
                # per-WALLET cap: this order's own contribution <= order_size with price_buffer headroom so
                # a legitimate one-lot entry (order_size x (1+buffer) worst-fill projection) is NOT rejected
                # by the buffer double-count (Fable P0 #2).
                if per_wal > self._cfg.order_size_usd * (1 + self._cfg.price_buffer) + 1e-6:
                    return Result.REJECTED
                if gross > self._cfg.admit_ceiling_usd:
                    return Result.REJECTED
                if snap.account_equity <= 0 or gross / snap.account_equity > self._cfg.max_leverage:
                    return Result.REJECTED
                if not self._ex.set_leverage_2x(intent.coin):    # set + verify; False on any doubt
                    return Result.REJECTED
            # 5. reserve (closes query/fill race), submit, reconcile
            cloid = self._new_cloid()
            signed_notl = intent.sz * intent.limit_px * (1 if intent.is_buy else -1)
            self._reservations[cloid] = (intent.coin, signed_notl if not intent.reduce_only else 0.0)
            try:
                res = self._ex.submit(intent, cloid)
            except Exception:
                self._latch_halt()
                return Result.UNKNOWN            # keep reservation; halted
            if res == Result.UNKNOWN:
                self._latch_halt()
                return Result.UNKNOWN            # keep reservation; halted
            # terminal: capture the ACTUAL fill (if the adapter parsed one) and release the reservation
            # (position now reflected in the next snapshot).
            if res == Result.ACCEPTED:
                self.last_fill = getattr(self._ex, "last_fill", None)
            self._reservations.pop(cloid, None)
            return res
