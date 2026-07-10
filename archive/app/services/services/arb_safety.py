"""
Arb safety layer — symbol precision, partial fill handling, slippage modeling.

Step 2: Symbol precision (qty rounding, min notional)
Step 3: Partial fill recovery (unwind orphan legs)
Step 4: Spread persistence scoring
Step 5: Slippage estimation from orderbook depth
"""
import logging
import math
from dataclasses import dataclass
from typing import Optional

from pymongo import MongoClient

logger = logging.getLogger(__name__)


# ── Step 2: Symbol Precision ─────────────────────────────────

@dataclass
class SymbolSpec:
    symbol: str
    bn_min_qty: float
    bn_qty_step: float
    bn_min_notional: float
    bn_price_tick: float
    bb_min_qty: float
    bb_qty_step: float
    bb_min_notional: float
    bb_price_tick: float


class SymbolPrecision:
    """Load and apply exchange-specific quantity/price rules."""

    def __init__(self, mongo_uri: str = "mongodb://localhost:27017/quants_lab"):
        self._db = MongoClient(mongo_uri)["quants_lab"]
        self._cache: dict[str, SymbolSpec] = {}
        self._load()

    def _load(self):
        for doc in self._db["arb_symbol_info"].find():
            bn = doc.get("binance", {})
            bb = doc.get("bybit", {})
            self._cache[doc["symbol"]] = SymbolSpec(
                symbol=doc["symbol"],
                bn_min_qty=bn.get("min_qty", 0),
                bn_qty_step=bn.get("qty_step", 1e-8),
                bn_min_notional=bn.get("min_notional", 5),
                bn_price_tick=bn.get("price_tick", 1e-8),
                bb_min_qty=bb.get("min_qty", 0),
                bb_qty_step=bb.get("qty_step", 1e-8),
                bb_min_notional=bb.get("min_notional", 5),
                bb_price_tick=bb.get("price_tick", 1e-8),
            )
        logger.info(f"Loaded precision for {len(self._cache)} symbols")

    def get(self, symbol: str) -> Optional[SymbolSpec]:
        return self._cache.get(symbol)

    def round_qty(self, symbol: str, qty: float, exchange: str) -> float:
        """Round qty to valid step size for the given exchange."""
        spec = self._cache.get(symbol)
        if not spec:
            return qty

        if exchange == "binance":
            step = spec.bn_qty_step
            min_qty = spec.bn_min_qty
        else:
            step = spec.bb_qty_step
            min_qty = spec.bb_min_qty

        if step <= 0:
            return qty

        # Round down to step
        rounded = math.floor(qty / step) * step
        # Clamp to min
        if rounded < min_qty:
            return 0  # can't trade
        return round(rounded, 10)  # avoid float artifacts

    def round_price(self, symbol: str, price: float, exchange: str) -> float:
        """Round price to valid tick size."""
        spec = self._cache.get(symbol)
        if not spec:
            return price

        tick = spec.bn_price_tick if exchange == "binance" else spec.bb_price_tick
        if tick <= 0:
            return price

        return round(math.floor(price / tick) * tick, 10)

    def validate_order(self, symbol: str, qty: float, price: float, exchange: str) -> tuple[bool, str]:
        """Check if an order meets exchange requirements."""
        spec = self._cache.get(symbol)
        if not spec:
            return True, "No spec (allowing)"

        if exchange == "binance":
            min_qty = spec.bn_min_qty
            min_notional = spec.bn_min_notional
        else:
            min_qty = spec.bb_min_qty
            min_notional = spec.bb_min_notional

        if qty < min_qty:
            return False, f"Qty {qty} < min {min_qty}"

        notional = qty * price
        if notional < min_notional:
            return False, f"Notional ${notional:.2f} < min ${min_notional}"

        return True, "OK"


# ── Step 3: Partial Fill Recovery ────────────────────────────

class OrphanDetector:
    """Detect and recover from partial fills (one leg filled, other failed)."""

    def __init__(self):
        self.orphan_legs: list[dict] = []

    def check_fill_pair(
        self,
        bn_status: str,
        bb_status: str,
        symbol: str,
        direction: str,
    ) -> Optional[dict]:
        """
        After attempting both legs, check if we have an orphan.
        Returns recovery action if needed.
        """
        bn_ok = "FILLED" in bn_status or bn_status == "FILLED_PAPER"
        bb_ok = "SUBMITTED" in bb_status or "FILLED" in bb_status or bb_status == "FILLED_PAPER"

        if bn_ok and bb_ok:
            return None  # both legs filled, no problem

        if bn_ok and not bb_ok:
            # Binance filled but Bybit failed — need to unwind Binance
            action = {
                "type": "UNWIND_BINANCE",
                "symbol": symbol,
                "reason": f"Bybit leg failed: {bb_status}",
                "action": "SELL" if "BUY" in direction else "BUY",
            }
            self.orphan_legs.append(action)
            logger.error(f"🚨 ORPHAN: {symbol} Binance filled, Bybit failed. Recovery: {action['action']} on Binance")
            return action

        if bb_ok and not bn_ok:
            # Bybit filled but Binance failed — need to close Bybit position
            action = {
                "type": "CLOSE_BYBIT",
                "symbol": symbol,
                "reason": f"Binance leg failed: {bn_status}",
                "action": "Sell" if "Buy" in direction else "Buy",
            }
            self.orphan_legs.append(action)
            logger.error(f"🚨 ORPHAN: {symbol} Bybit filled, Binance failed. Recovery: {action['action']} on Bybit")
            return action

        # Both failed — no position, no problem
        return None


# ── Step 4: Spread Persistence Scoring ───────────────────────

class SpreadTracker:
    """Track how persistent spreads are over time for each symbol."""

    def __init__(self, mongo_uri: str = "mongodb://localhost:27017/quants_lab"):
        self._db = MongoClient(mongo_uri)["quants_lab"]
        self._history: dict[str, list[float]] = {}  # symbol -> recent spreads
        self._MAX_HISTORY = 60  # keep last 60 polls

    def record(self, symbol: str, net_spread_bps: float):
        if symbol not in self._history:
            self._history[symbol] = []
        self._history[symbol].append(net_spread_bps)
        if len(self._history[symbol]) > self._MAX_HISTORY:
            self._history[symbol].pop(0)

    def persistence_score(self, symbol: str, min_spread_bps: float = 5.0) -> float:
        """
        What fraction of recent polls had a profitable spread?
        1.0 = always profitable, 0.0 = never.
        """
        history = self._history.get(symbol, [])
        if not history:
            return 0.0
        above = sum(1 for s in history if s >= min_spread_bps)
        return above / len(history)

    def avg_spread(self, symbol: str) -> float:
        history = self._history.get(symbol, [])
        if not history:
            return 0.0
        return sum(history) / len(history)

    def spread_stability(self, symbol: str) -> float:
        """
        Coefficient of variation (lower = more stable).
        Stable spreads are better for arb.
        """
        history = self._history.get(symbol, [])
        if len(history) < 5:
            return float("inf")
        mean = sum(history) / len(history)
        if mean <= 0:
            return float("inf")
        variance = sum((s - mean) ** 2 for s in history) / len(history)
        return (variance ** 0.5) / mean

    def get_top_persistent(self, n: int = 20, min_polls: int = 10) -> list[dict]:
        """Get the most persistent profitable spreads."""
        results = []
        for symbol, history in self._history.items():
            if len(history) < min_polls:
                continue
            results.append({
                "symbol": symbol,
                "persistence": self.persistence_score(symbol),
                "avg_spread": self.avg_spread(symbol),
                "stability": self.spread_stability(symbol),
                "polls": len(history),
            })
        results.sort(key=lambda x: (-x["persistence"], -x["avg_spread"]))
        return results[:n]


# ── Step 5: Slippage Estimation ──────────────────────────────

@dataclass
class SlippageEstimate:
    symbol: str
    side: str  # BUY or SELL
    target_usd: float
    estimated_fill_price: float
    best_price: float
    slippage_bps: float
    depth_available_usd: float
    sufficient_depth: bool


async def estimate_slippage(
    session,
    symbol: str,
    side: str,
    target_usd: float,
    exchange: str = "binance",
) -> SlippageEstimate:
    """
    Walk the orderbook to estimate fill price for a given USD size.
    Returns estimated slippage in bps.
    """
    if exchange == "binance":
        url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=20"
    else:
        url = f"https://api.bybit.com/v5/market/orderbook?category=linear&symbol={symbol}&limit=25"

    async with session.get(url) as resp:
        data = await resp.json()

    if exchange == "binance":
        if side == "BUY":
            levels = [(float(p), float(q)) for p, q in data.get("asks", [])]
        else:
            levels = [(float(p), float(q)) for p, q in data.get("bids", [])]
    else:
        book = data.get("result", {})
        if side == "BUY" or side == "Buy":
            raw = book.get("a", [])  # asks
        else:
            raw = book.get("b", [])  # bids
        levels = [(float(p), float(q)) for p, q in raw]

    if not levels:
        return SlippageEstimate(
            symbol=symbol, side=side, target_usd=target_usd,
            estimated_fill_price=0, best_price=0, slippage_bps=0,
            depth_available_usd=0, sufficient_depth=False,
        )

    best_price = levels[0][0]
    filled_usd = 0
    filled_qty = 0

    for price, qty in levels:
        level_usd = price * qty
        remaining = target_usd - filled_usd
        if level_usd >= remaining:
            fill_qty = remaining / price
            filled_qty += fill_qty
            filled_usd += remaining
            break
        else:
            filled_qty += qty
            filled_usd += level_usd

    if filled_qty <= 0:
        avg_price = best_price
    else:
        avg_price = filled_usd / filled_qty

    slippage = abs(avg_price - best_price) / best_price * 10000
    depth_usd = sum(p * q for p, q in levels)

    return SlippageEstimate(
        symbol=symbol,
        side=side,
        target_usd=target_usd,
        estimated_fill_price=avg_price,
        best_price=best_price,
        slippage_bps=slippage,
        depth_available_usd=depth_usd,
        sufficient_depth=filled_usd >= target_usd * 0.95,
    )
