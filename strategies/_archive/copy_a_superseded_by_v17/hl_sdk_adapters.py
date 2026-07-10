"""Copy A -- the SOLE audited HL SDK surface (ExchangeAdapter implementation).

This is the ONLY module in the copy_a package allowed to hold the signing `Exchange` / touch the SDK
(enforced by tests/copy_a/test_no_raw_signer.py). RiskBroker holds ONE instance of HLExchangeAdapter
privately; nothing else gets raw signing power.

Patterns follow the proven V17 usage (strategies/live/hl_copy_trader_v17.py): agent key signs, PARENT
address holds positions; IOC limit orders; clearinghouseState for positions+equity; meta for
szDecimals.

STATUS: UNVERIFIED pending testnet. Every method here places or reads real orders/state and MUST be
(a) run against HL testnet and (b) reviewed by Fable+Codex before any mainnet use. Do not wire to
live capital until both are done.
"""
from __future__ import annotations
import math
import os
import time

import eth_account
import requests
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils.signing import OrderType
try:
    from hyperliquid.utils.types import Cloid
except Exception:  # SDK layout differs across versions; Cloid optional
    Cloid = None

from copy_a.risk_broker import (
    Snapshot, Position, OpenOrder, Result, OrderIntent, Fill,
)

HL_API = os.environ.get("HL_API", "https://api.hyperliquid.xyz")
IOC: OrderType = {"limit": {"tif": "Ioc"}}

# HL perp price rule: at most 5 significant figures AND at most (MAX_DECIMALS - szDecimals) decimal
# places, MAX_DECIMALS = 6 for perps. Book-cross pad: IOC limit at the RAW mid sits inside the book and
# cancels (Fable P0 #1) -> we cross by CROSS_FRAC so the marketable IOC actually fills.
MAX_PX_DECIMALS_PERP = 6
DEFAULT_CROSS_FRAC = 0.005


def round_price(px: float, sz_decimals: int) -> float:
    """Round px to HL's tick rule: 5 significant figures, then clamp to (6 - szDecimals) decimals.
    Ported from V17 `_round_price` and extended with the szDecimals decimal clamp."""
    if px <= 0:
        return 0.0
    mag = math.floor(math.log10(abs(px)))
    sig_decimals = 4 - mag                      # decimals that keep 5 significant figures
    max_decimals = MAX_PX_DECIMALS_PERP - int(sz_decimals)
    decimals = min(sig_decimals, max_decimals)
    return round(px, max(decimals, 0))


class HLExchangeAdapter:
    """Implements the ExchangeAdapter Protocol used by RiskBroker.

    sole-writer model (Alberto 2026-07-04, no subaccount): Copy A is the only writer on this HL
    account. `snapshot()` reports the PARENT account's true positions/equity; RiskBroker additionally
    reconciles against its own tracked positions and HALTs on divergence (foreign activity guard).
    """

    def __init__(self, private_key: str, parent_address: str, agent_address: str,
                 perp_dexs: list[str] | None = None, timeout: int = 10,
                 cross_frac: float = DEFAULT_CROSS_FRAC):
        self._parent = parent_address
        self._agent = agent_address
        self._timeout = timeout
        self._cross_frac = cross_frac
        self._account = eth_account.Account.from_key(private_key)
        dexes = perp_dexs if perp_dexs is not None else [""]
        self._dexes = list(dexes)
        # builder (non-main) dexes we also price for xyz coin marks (HARD SAFETY requirement 5).
        self._mark_dexes = [d for d in self._dexes if d]
        self._info = Info(HL_API, skip_ws=True, perp_dexs=dexes)
        # agent signs; account_address=agent for order signing (positions live on parent)
        self._ex = Exchange(self._account, HL_API, account_address=self._agent, perp_dexs=dexes)
        self._sz_dec: dict = {}
        self.last_fill = None      # Fill parsed from the most recent submit (None if not filled)
        self._load_meta(dexes)

    def _load_meta(self, dexes):
        # load szDecimals for EVERY copied dex (main + xyz). A missing coin defaults to 2 in sz_decimals().
        for dex in dexes:
            try:
                meta = self._info.meta(dex=dex) if dex else self._info.meta()
            except TypeError:
                meta = self._info.meta()   # older SDK: no dex kwarg (main only)
            except Exception:
                continue
            universe = meta[0]["universe"] if isinstance(meta, tuple) else meta["universe"]
            for u in universe:
                self._sz_dec.setdefault(u["name"], u.get("szDecimals", 2))

    # ---- reads ----
    def _info_post(self, payload: dict):
        r = requests.post(f"{HL_API}/info", json=payload, timeout=5)
        r.raise_for_status()
        return r.json()

    def snapshot(self) -> Snapshot:
        """Fresh, COMPLETE snapshot of the parent account. Any failed query -> ok=False (reject)."""
        try:
            chs = self._info_post({"type": "clearinghouseState", "user": self._parent})
            oo = self._info_post({"type": "frontendOpenOrders", "user": self._parent})
        except Exception:
            return Snapshot(positions={}, open_orders=[], account_equity=0.0, ok=False)
        try:
            equity = float(chs["marginSummary"]["accountValue"])
            positions: dict = {}
            for ap in chs.get("assetPositions", []):
                p = ap["position"]
                szi = float(p["szi"])
                if szi == 0:
                    continue
                positions[p["coin"]] = Position(
                    coin=p["coin"], signed_sz=szi, entry_px=float(p.get("entryPx") or 0.0),
                )
            open_orders = []
            for o in oo:
                open_orders.append(OpenOrder(
                    coin=o["coin"], is_buy=(o["side"] == "B"), sz=float(o["sz"]),
                    limit_px=float(o["limitPx"]), reduce_only=bool(o.get("reduceOnly", False)),
                ))
            return Snapshot(positions=positions, open_orders=open_orders,
                            account_equity=equity, ok=True)
        except (KeyError, TypeError, ValueError):
            return Snapshot(positions={}, open_orders=[], account_equity=0.0, ok=False)

    def sz_decimals(self, coin: str) -> int:
        return int(self._sz_dec.get(coin, 2))

    def mark_px(self, coin: str):
        """Best-effort REST mid for a coin. Tries MAIN allMids first, then each builder dex's allMids
        (allMids?dex=xyz) so xyz coins ALSO get a REST crossing reference when the WS mid is stale (HARD
        SAFETY requirement 5). Used only as a fallback for the disaster stop / emergency flatten. None on
        any failure or if the coin is not priced on any copied dex."""
        for dex in [""] + self._mark_dexes:
            try:
                payload = {"type": "allMids", "dex": dex} if dex else {"type": "allMids"}
                mids = self._info_post(payload)
                px = mids.get(coin) if isinstance(mids, dict) else None
                if px is not None and float(px) > 0:
                    return float(px)
            except Exception:
                continue
        return None

    # ---- leverage (NO-OP: use HL per-coin DEFAULT leverage) ----
    def set_leverage_2x(self, coin: str) -> bool:
        # ALLOWED CHANGE (Alberto 2026-07-05, Gate-1 probe): DO NOT set leverage on-exchange.
        # We deliberately keep each coin at HL's per-coin DEFAULT leverage and NEVER call
        # update_leverage (that is why there is no `.update_leverage(` call site here anymore).
        # RiskBroker still enforces the gross/equity effective-leverage cap (max_leverage) as the
        # real risk control; this method is retained only to satisfy the ExchangeAdapter contract
        # and is now a pure no-op that always succeeds. Signature is unchanged.
        return True

    # ---- order submit ----
    def _cross_and_round(self, intent: OrderIntent) -> float:
        """Turn the intent's mid `limit_px` into a MARKETABLE, tick-legal IOC limit: cross the book by
        cross_frac (buy up, sell down) then round to HL's 5-sig-fig + szDecimals rule. ALL intents route
        here -- mirror entries, mirror exits, the -25% disaster stop, and kill-flatten -- so every one of
        them can actually fill (Fable P0 #1). RiskBroker's price_buffer is set >= cross_frac so the
        pre-submit projection is not under the true worst fill."""
        cf = self._cross_frac
        crossed = intent.limit_px * (1 + cf) if intent.is_buy else intent.limit_px * (1 - cf)
        return round_price(crossed, self.sz_decimals(intent.coin))

    def submit(self, intent: OrderIntent, cloid: str) -> Result:
        """IOC limit order, crossing the book so it fills. Returns ACCEPTED on confirmed resting/filled,
        UNKNOWN on any ambiguity (RiskBroker latches HALT on UNKNOWN). reduce_only passed to the exchange
        as a second enforcement layer (HL rejects reduce-only that would increase exposure). Parses
        totalSz/avgPx into self.last_fill so the runner records the ACTUAL fill (Fable P0 #5)."""
        self.last_fill = None
        px = self._cross_and_round(intent)
        if px <= 0:
            return Result.UNKNOWN
        kwargs = {"reduce_only": intent.reduce_only}
        if Cloid is not None:
            try:
                kwargs["cloid"] = Cloid.from_str(_cloid_hex(cloid))
            except Exception:
                pass
        try:
            resp = self._ex.order(intent.coin, intent.is_buy, intent.sz, px, IOC, **kwargs)
        except Exception:
            return Result.UNKNOWN
        # parse SDK response
        try:
            if resp.get("status") != "ok":
                return Result.UNKNOWN
            statuses = resp["response"]["data"]["statuses"]
            st = statuses[0]
            if "error" in st:
                # a hard reject (e.g. reduce-only would increase) is a clean REJECT, not UNKNOWN
                return Result.REJECTED
            if "filled" in st:
                fl = st["filled"]
                try:
                    filled_sz = float(fl.get("totalSz", intent.sz))
                    avg_px = float(fl.get("avgPx", px))
                    signed = filled_sz if intent.is_buy else -filled_sz
                    self.last_fill = Fill(coin=intent.coin, signed_sz=signed, avg_px=avg_px)
                except (TypeError, ValueError):
                    self.last_fill = None
                return Result.ACCEPTED
            if "resting" in st:
                # IOC that only rested (no immediate fill) leaves no actual fill to record; the reservation
                # + next snapshot reconcile it. Treat as ACCEPTED (order was live) with no last_fill.
                return Result.ACCEPTED
            return Result.UNKNOWN
        except (KeyError, IndexError, AttributeError, TypeError):
            return Result.UNKNOWN

    def cancel_all_non_reduce(self) -> None:
        try:
            oo = self._info_post({"type": "frontendOpenOrders", "user": self._parent})
        except Exception:
            return
        for o in oo:
            if not o.get("reduceOnly", False):
                try:
                    self._ex.cancel(o["coin"], int(o["oid"]))
                except Exception:
                    pass


def _cloid_hex(cloid: str) -> str:
    """Map our string cloid to a 16-byte hex the SDK Cloid expects."""
    h = abs(hash(cloid)) & ((1 << 128) - 1)
    return "0x" + format(h, "032x")
