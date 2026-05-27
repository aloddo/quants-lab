"""
Instrument rules for HL + Bybit — lot sizing, price rounding, min notional.
"""
import logging
import math
from dataclasses import dataclass
from typing import Optional

import requests

logger = logging.getLogger(__name__)


@dataclass
class PairRules:
    """Trading rules for a pair on both venues."""
    pair: str
    coin: str               # e.g., "APE"
    bb_symbol: str          # e.g., "APEUSDT"

    # HL rules
    hl_sz_decimals: int = 0       # Size decimals (szDecimals from meta)
    hl_min_sz: float = 1.0        # Minimum order size
    hl_max_decimals: int = 5      # Max price decimal places (HL sig-fig rule: 5 sig figs)

    # Bybit rules
    bb_qty_step: float = 0.1      # Quantity step
    bb_min_qty: float = 0.1       # Minimum quantity
    bb_price_step: float = 0.001  # Price tick
    bb_min_notional: float = 5.0  # Min order value in USDT

    def round_hl_qty(self, qty: float) -> float:
        """Round qty to HL szDecimals."""
        factor = 10 ** self.hl_sz_decimals
        return math.floor(qty * factor) / factor

    def round_bb_qty(self, qty: float) -> float:
        """Round qty to Bybit qty step (floor). Uses small epsilon to handle FP."""
        # Add tiny epsilon to avoid floor(2.9999999998) = 2 instead of 3
        eps = self.bb_qty_step * 1e-9
        return round(math.floor((qty + eps) / self.bb_qty_step) * self.bb_qty_step, 10)

    def format_bb_qty(self, qty: float) -> str:
        """Format qty to correct decimal precision from qty step."""
        qty = self.round_bb_qty(qty)
        if self.bb_qty_step >= 1:
            return str(int(qty))
        decimals = max(0, -math.floor(math.log10(self.bb_qty_step)))
        return f"{qty:.{decimals}f}"

    def round_hl_price(self, price: float, is_buy: bool) -> float:
        """Round HL price to valid precision (5 significant figures, venue-safe).

        HL rejects prices with too many significant figures. For buys, round UP
        (aggressive). For sells, round DOWN (aggressive). This ensures IOC fills.
        """
        if price <= 0:
            return price
        # HL uses 5 significant figures max
        sig_figs = 5
        magnitude = math.floor(math.log10(abs(price)))
        decimals = max(0, sig_figs - 1 - magnitude)
        factor = 10 ** decimals
        if is_buy:
            return math.ceil(price * factor) / factor
        else:
            return math.floor(price * factor) / factor

    def round_bb_price_buy(self, price: float) -> float:
        """Round buy price UP to Bybit tick (aggressive = overpay to ensure fill)."""
        return math.ceil(price / self.bb_price_step) * self.bb_price_step

    def round_bb_price_sell(self, price: float) -> float:
        """Round sell price DOWN to Bybit tick (aggressive = underprice to ensure fill)."""
        return math.floor(price / self.bb_price_step) * self.bb_price_step

    def format_bb_price(self, price: float) -> str:
        """Format Bybit price to correct decimal precision from tick size."""
        if self.bb_price_step >= 1:
            return str(int(price))
        decimals = max(0, -math.floor(math.log10(self.bb_price_step)))
        return f"{price:.{decimals}f}"

    def common_qty(self, target_usd: float, price: float) -> tuple[float, float]:
        """
        Compute matching qty for both venues given target USD.

        Returns (hl_qty, bb_qty) — both within venue rules.
        The smaller of the two is used to keep positions matched.
        """
        raw_qty = target_usd / price
        hl_qty = self.round_hl_qty(raw_qty)
        bb_qty = self.round_bb_qty(raw_qty)

        # Use the smaller to keep delta-neutral
        min_qty = min(hl_qty, bb_qty)
        hl_qty = self.round_hl_qty(min_qty)
        bb_qty = self.round_bb_qty(min_qty)

        return hl_qty, bb_qty

    def is_tradeable(self, qty: float, price: float) -> bool:
        """Check if qty meets minimum requirements on both venues."""
        notional = qty * price
        hl_ok = qty >= self.hl_min_sz
        bb_ok = qty >= self.bb_min_qty and notional >= self.bb_min_notional
        return hl_ok and bb_ok


class InstrumentManager:
    """Fetch and cache instrument rules from both venues."""

    def __init__(self):
        self.rules: dict[str, PairRules] = {}

    def fetch_rules(self, pairs: list[str]) -> dict[str, PairRules]:
        """Fetch rules from HL meta + Bybit instruments."""
        hl_rules = self._fetch_hl_meta()
        bb_rules = self._fetch_bb_instruments()

        for pair in pairs:
            coin = pair.replace("-USDT", "")
            bb_sym = pair.replace("-", "")

            rules = PairRules(
                pair=pair, coin=coin, bb_symbol=bb_sym,
            )

            # HL rules
            if coin in hl_rules:
                rules.hl_sz_decimals = hl_rules[coin].get("szDecimals", 0)
                # Min size is 1 unit at szDecimals precision
                rules.hl_min_sz = 10 ** (-rules.hl_sz_decimals)

            # Bybit rules
            if bb_sym in bb_rules:
                br = bb_rules[bb_sym]
                rules.bb_qty_step = br.get("qtyStep", 0.1)
                rules.bb_min_qty = br.get("minQty", 0.1)
                rules.bb_price_step = br.get("tickSize", 0.001)
                rules.bb_min_notional = br.get("minNotionalValue", 5.0)

            self.rules[pair] = rules
            logger.info(
                f"  {pair}: HL szDec={rules.hl_sz_decimals} "
                f"BB step={rules.bb_qty_step} tick={rules.bb_price_step}"
            )

        return self.rules

    def get_rules(self, pair: str) -> Optional[PairRules]:
        return self.rules.get(pair)

    def _fetch_hl_meta(self) -> dict:
        """Fetch HL universe metadata."""
        try:
            resp = requests.post(
                "https://api.hyperliquid.xyz/info",
                json={"type": "meta"},
                timeout=10,
            )
            data = resp.json()
            result = {}
            for asset in data.get("universe", []):
                result[asset["name"]] = {
                    "szDecimals": asset.get("szDecimals", 0),
                }
            logger.info(f"HL meta: {len(result)} assets")
            return result
        except Exception as e:
            logger.error(f"HL meta fetch failed: {e}")
            return {}

    def _fetch_bb_instruments(self) -> dict:
        """Fetch Bybit linear instrument info."""
        try:
            resp = requests.get(
                "https://api.bybit.com/v5/market/instruments-info",
                params={"category": "linear", "limit": "1000"},
                timeout=10,
            )
            data = resp.json()
            result = {}
            for inst in data.get("result", {}).get("list", []):
                sym = inst.get("symbol", "")
                lot = inst.get("lotSizeFilter", {})
                price = inst.get("priceFilter", {})
                result[sym] = {
                    "qtyStep": float(lot.get("qtyStep", 0.1)),
                    "minQty": float(lot.get("minOrderQty", 0.1)),
                    "tickSize": float(price.get("tickSize", 0.001)),
                    "minNotionalValue": float(lot.get("minNotionalValue", 5.0)),
                }
            logger.info(f"Bybit instruments: {len(result)} symbols")
            return result
        except Exception as e:
            logger.error(f"Bybit instruments fetch failed: {e}")
            return {}
