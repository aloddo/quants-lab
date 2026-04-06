"""
Async client for the Hummingbot API at localhost:8000.

Used by:
- TestnetResolverTask: create/poll executors on bybit_perpetual_testnet
- SignalScanTask: get portfolio state for position sizing
"""
import logging
import os
from typing import Any, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)


class HBApiClient:
    """Async wrapper for the Hummingbot REST API."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.host = host or os.getenv("HUMMINGBOT_API_HOST", "localhost")
        self.port = port or int(os.getenv("HUMMINGBOT_API_PORT", "8000"))
        self.username = username or os.getenv("HUMMINGBOT_API_USERNAME", "admin")
        self.password = password or os.getenv("HUMMINGBOT_API_PASSWORD", "admin")
        self.base_url = f"http://{self.host}:{self.port}"
        self._session: Optional[aiohttp.ClientSession] = None

    def _auth(self) -> aiohttp.BasicAuth:
        return aiohttp.BasicAuth(self.username, self.password)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                auth=self._auth(),
                timeout=aiohttp.ClientTimeout(total=30),
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        session = await self._get_session()
        url = f"{self.base_url}{path}"
        async with session.request(method, url, **kwargs) as resp:
            if resp.status >= 400:
                text = await resp.text()
                raise RuntimeError(f"HB API {method} {path} returned {resp.status}: {text}")
            return await resp.json()

    async def health_check(self) -> bool:
        """Ping HB API. Returns True if reachable, False otherwise."""
        try:
            session = await self._get_session()
            async with session.get(f"{self.base_url}/", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                return resp.status == 200
        except Exception:
            return False

    async def get_portfolio_state(
        self, account_name: str = "master_account"
    ) -> Dict[str, Any]:
        """Get portfolio overview for position sizing."""
        return await self._request("GET", "/portfolio/overview", params={"account_names": account_name})

    async def ensure_trading_pair(
        self, connector_name: str, trading_pair: str, account_name: str = "master_account"
    ) -> bool:
        """Pre-register a trading pair so the connector builds its rate limits.

        Calls /market-data/trading-pair/add which goes through the proper
        add_trading_pair() path (rebuilds the throttler).  Without this,
        pairs added only via _trading_pairs.append() crash with
        'NoneType' object has no attribute 'weight'.
        """
        try:
            result = await self._request(
                "POST",
                "/market-data/trading-pair/add",
                json={
                    "connector_name": connector_name,
                    "trading_pair": trading_pair,
                    "account_name": account_name,
                    "timeout": 30.0,
                },
            )
            return result.get("success", False)
        except Exception as e:
            logger.warning(f"ensure_trading_pair({trading_pair}): {e}")
            return False

    async def get_trading_rules(
        self, connector_name: str, trading_pair: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch trading rules via HB API (native endpoint).

        CLOBDataSource excludes testnet connectors, so we go through the
        API which has the live connector instance.
        """
        params = {}
        if trading_pair:
            params["trading_pairs"] = trading_pair
        return await self._request(
            "GET", f"/connectors/{connector_name}/trading-rules", params=params
        )

    async def create_executor(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new executor (position, grid, etc.)."""
        return await self._request("POST", "/executors/", json=config)

    async def get_executor_status(self, executor_id: str) -> Dict[str, Any]:
        """Get executor status by ID."""
        return await self._request("GET", f"/executors/{executor_id}")

    async def stop_executor(self, executor_id: str) -> Dict[str, Any]:
        """Stop a running executor."""
        return await self._request("POST", f"/executors/{executor_id}/stop")

    async def get_historical_candles(
        self,
        connector: str,
        trading_pair: str,
        interval: str,
        max_records: int = 500,
    ) -> Dict[str, Any]:
        """Fetch historical candles via HB API."""
        return await self._request(
            "GET",
            "/market-data/historical-candles",
            params={
                "connector_name": connector,
                "trading_pair": trading_pair,
                "interval": interval,
                "max_records": max_records,
            },
        )
