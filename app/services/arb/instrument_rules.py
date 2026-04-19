"""
H2 InstrumentRules — Exchange-specific qty/price rounding and min notional checks.

Queries instrument info on startup, caches rules, provides round_qty/round_price/check_notional.
Without this, orders REJECT on both Bybit and Binance.
"""
import logging
import math
from dataclasses import dataclass

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class PairRules:
    """Trading rules for a single pair on a single venue."""
    symbol: str
    venue: str
    min_qty: float = 0.0
    qty_step: float = 1.0
    min_price: float = 0.0
    price_tick: float = 0.01
    min_notional: float = 5.0

    def round_qty(self, qty: float) -> float:
        """Round qty DOWN to nearest step size."""
        if self.qty_step <= 0:
            return qty
        precision = max(0, -int(math.log10(self.qty_step))) if self.qty_step < 1 else 0
        return round(math.floor(qty / self.qty_step) * self.qty_step, precision)

    def round_price(self, price: float) -> float:
        """Round price to nearest tick size."""
        if self.price_tick <= 0:
            return price
        precision = max(0, -int(math.log10(self.price_tick))) if self.price_tick < 1 else 0
        return round(round(price / self.price_tick) * self.price_tick, precision)

    def check_notional(self, qty: float, price: float) -> bool:
        """Check if qty * price >= min_notional."""
        return qty * price >= self.min_notional

    def check_min_qty(self, qty: float) -> bool:
        return qty >= self.min_qty


class InstrumentRules:
    """
    Cache of trading rules for all active pairs on both venues.
    Load on startup, refresh daily.
    """

    def __init__(self):
        self.rules: dict[str, PairRules] = {}  # key: "venue:symbol" e.g. "bybit:NOMUSDT"

    def get(self, venue: str, symbol: str) -> PairRules | None:
        return self.rules.get(f"{venue}:{symbol}")

    async def load_bybit(self, session: aiohttp.ClientSession, symbols: list[str]):
        """Load instrument rules from Bybit for linear perps."""
        try:
            async with session.get(
                "https://api.bybit.com/v5/market/instruments-info",
                params={"category": "linear"},
            ) as resp:
                data = await resp.json()

            target = set(symbols)
            for item in data.get("result", {}).get("list", []):
                sym = item["symbol"]
                if sym not in target:
                    continue
                lot = item.get("lotSizeFilter", {})
                price = item.get("priceFilter", {})
                rules = PairRules(
                    symbol=sym,
                    venue="bybit",
                    min_qty=float(lot.get("minOrderQty", 0)),
                    qty_step=float(lot.get("qtyStep", 1)),
                    min_price=float(price.get("minPrice", 0)),
                    price_tick=float(price.get("tickSize", 0.01)),
                    min_notional=float(lot.get("minNotionalValue", 5)),
                )
                self.rules[f"bybit:{sym}"] = rules
                logger.info(f"  Bybit {sym}: qty_step={rules.qty_step} tick={rules.price_tick} min_notional={rules.min_notional}")

        except Exception as e:
            logger.error(f"Failed to load Bybit instrument rules: {e}")

    async def load_binance(self, session: aiohttp.ClientSession, symbols: list[str]):
        """Load instrument rules from Binance for spot pairs."""
        try:
            # Binance exchangeInfo returns all symbols; filter locally
            async with session.get("https://api.binance.com/api/v3/exchangeInfo") as resp:
                data = await resp.json()

            target = set(symbols)
            for sym_info in data.get("symbols", []):
                sym = sym_info["symbol"]
                if sym not in target:
                    continue
                filters = {f["filterType"]: f for f in sym_info.get("filters", [])}
                lot = filters.get("LOT_SIZE", {})
                price = filters.get("PRICE_FILTER", {})
                notional = filters.get("NOTIONAL", {})

                rules = PairRules(
                    symbol=sym,
                    venue="binance",
                    min_qty=float(lot.get("minQty", 0)),
                    qty_step=float(lot.get("stepSize", 1)),
                    min_price=float(price.get("minPrice", 0)),
                    price_tick=float(price.get("tickSize", 0.01)),
                    min_notional=float(notional.get("minNotional", 5)),
                )
                self.rules[f"binance:{sym}"] = rules
                logger.info(f"  Binance {sym}: qty_step={rules.qty_step} tick={rules.price_tick} min_notional={rules.min_notional}")

        except Exception as e:
            logger.error(f"Failed to load Binance instrument rules: {e}")

    async def load_all(self, session: aiohttp.ClientSession, bb_symbols: list[str], bn_symbols: list[str]):
        """Load rules for all active pairs on both venues."""
        logger.info("Loading instrument rules...")
        await self.load_bybit(session, bb_symbols)
        await self.load_binance(session, bn_symbols)
        logger.info(f"Loaded rules for {len(self.rules)} symbol-venues")
