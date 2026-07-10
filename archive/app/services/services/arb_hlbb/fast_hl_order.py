"""
Fast HL order placement — bypasses SDK overhead.

The SDK's exchange.order() calls info.name_to_asset() on every order (~300ms REST).
This module pre-caches asset IDs and posts directly, cutting ~575ms of overhead.

Target: 950ms → ~350ms per order.
"""
import asyncio
import logging
import time
from typing import Optional

import requests
from eth_account import Account
from hyperliquid.utils.signing import (
    OrderType,
    get_timestamp_ms,
    order_request_to_order_wire,
    order_wires_to_order_action,
    sign_l1_action,
)
from hyperliquid.utils.types import Cloid

logger = logging.getLogger(__name__)

MAINNET_URL = "https://api.hyperliquid.xyz"


class FastHLOrderClient:
    """Low-latency HL order placement with pre-cached asset IDs.

    Eliminates per-order REST calls by caching coin→asset_id at init.
    Uses a persistent HTTP session with keep-alive.
    """

    def __init__(self, private_key: str, account_address: str = ""):
        self.wallet = Account.from_key(private_key)
        self.account_address = account_address or None
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})
        self._asset_ids: dict[str, int] = {}
        self._meta_cache: dict = {}

    def init_assets(self, coins: list[str]) -> int:
        """Pre-cache asset IDs for all coins. Call once at startup.

        Returns number of coins successfully resolved.
        """
        try:
            resp = self._session.post(
                f"{MAINNET_URL}/info",
                json={"type": "meta"},
                timeout=10,
            )
            meta = resp.json()
            universe = meta.get("universe", [])
            self._meta_cache = {a["name"]: i for i, a in enumerate(universe)}

            for coin in coins:
                if coin in self._meta_cache:
                    self._asset_ids[coin] = self._meta_cache[coin]
                else:
                    logger.warning(f"FastHL: coin {coin} not found in HL universe")

            logger.info(f"FastHL: cached {len(self._asset_ids)}/{len(coins)} asset IDs")
            return len(self._asset_ids)
        except Exception as e:
            logger.error(f"FastHL: meta fetch failed: {e}")
            return 0

    def _build_and_sign(
        self,
        coin: str,
        is_buy: bool,
        sz: float,
        price: float,
        tif: str = "Ioc",
        reduce_only: bool = False,
        cloid: Optional[str] = None,
    ) -> Optional[dict]:
        """Build order payload and sign it. Pure CPU, no network.

        Returns the full POST body ready to send, or None if coin not cached.
        """
        asset_id = self._asset_ids.get(coin)
        if asset_id is None:
            logger.error(f"FastHL: no cached asset ID for {coin}")
            return None

        order_request = {
            "coin": coin,
            "is_buy": is_buy,
            "sz": sz,
            "limit_px": price,
            "order_type": OrderType(limit={"tif": tif}),
            "reduce_only": reduce_only,
        }
        if cloid:
            order_request["cloid"] = Cloid.from_str(cloid)

        wire = order_request_to_order_wire(order_request, asset_id)
        action = order_wires_to_order_action([wire], None, "na")
        timestamp = get_timestamp_ms()

        signature = sign_l1_action(
            self.wallet,
            action,
            None,  # vault_address — only for vault trading
            timestamp,
            None,  # expires_after
            True,  # is_mainnet
        )

        return {
            "action": action,
            "nonce": timestamp,
            "signature": signature,
            "vaultAddress": None,  # Only for vault trading, not sub-accounts
            "expiresAfter": None,
        }

    async def place_ioc(
        self,
        coin: str,
        is_buy: bool,
        sz: float,
        price: float,
        reduce_only: bool = False,
        cloid: str = "",
    ) -> dict:
        """Place IOC order with minimal latency.

        Returns: {"success": bool, "status": str, "filled_qty": float,
                  "avg_price": float, "latency_ms": float, "raw": dict}
        """
        t0 = time.time()

        # Build + sign (CPU only, ~1ms)
        payload = self._build_and_sign(
            coin, is_buy, sz, price, "Ioc", reduce_only, cloid or None,
        )
        if not payload:
            return {
                "success": False, "status": "NO_ASSET_ID",
                "filled_qty": 0, "avg_price": 0,
                "latency_ms": (time.time() - t0) * 1000,
                "error": f"No cached asset ID for {coin}",
                "raw": {},
            }

        t_signed = time.time()

        # Single POST (the only network call)
        try:
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: self._session.post(
                    f"{MAINNET_URL}/exchange",
                    json=payload,
                    timeout=5,
                ),
            )
            result = resp.json()
        except Exception as e:
            return {
                "success": False, "status": "ERROR",
                "filled_qty": 0, "avg_price": 0,
                "latency_ms": (time.time() - t0) * 1000,
                "error": str(e),
                "raw": {},
            }

        latency = (time.time() - t0) * 1000
        sign_ms = (t_signed - t0) * 1000

        # Parse response
        if result.get("status") == "ok":
            statuses = result.get("response", {}).get("data", {}).get("statuses", [])
            if statuses and isinstance(statuses[0], dict):
                s = statuses[0]
                if "filled" in s:
                    fill = s["filled"]
                    return {
                        "success": True, "status": "FILLED",
                        "filled_qty": float(fill.get("totalSz", sz)),
                        "avg_price": float(fill.get("avgPx", price)),
                        "latency_ms": latency, "sign_ms": sign_ms,
                        "order_id": str(fill.get("oid", "")),
                        "raw": result,
                    }
                elif "resting" in s:
                    # IOC should not rest
                    return {
                        "success": False, "status": "RESTING",
                        "filled_qty": 0, "avg_price": 0,
                        "latency_ms": latency, "sign_ms": sign_ms,
                        "error": "IOC order resting (unexpected)",
                        "raw": result,
                    }
                elif "error" in s:
                    return {
                        "success": False, "status": "REJECTED",
                        "filled_qty": 0, "avg_price": 0,
                        "latency_ms": latency, "sign_ms": sign_ms,
                        "error": s["error"],
                        "raw": result,
                    }

        return {
            "success": False, "status": "UNKNOWN",
            "filled_qty": 0, "avg_price": 0,
            "latency_ms": latency, "sign_ms": sign_ms,
            "error": str(result),
            "raw": result,
        }
