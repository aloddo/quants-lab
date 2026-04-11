"""
Async client for the Hummingbot API at localhost:8000.

Used by:
- Bot orchestration: deploy/start/stop HB-native strategy bots
- TestnetResolverTask: create/poll executors on bybit_perpetual_testnet (legacy)
- CLI deploy command: upload controllers, create configs, deploy bots
"""
import asyncio
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
        max_retries = 2
        for attempt in range(max_retries + 1):
            try:
                async with session.request(method, url, **kwargs) as resp:
                    if resp.status >= 500 and attempt < max_retries:
                        text = await resp.text()
                        logger.warning(f"HB API {method} {path} returned {resp.status}, "
                                       f"retrying ({attempt + 1}/{max_retries})...")
                        await asyncio.sleep(1 * (2 ** attempt))
                        continue
                    if resp.status >= 400:
                        text = await resp.text()
                        raise RuntimeError(f"HB API {method} {path} returned {resp.status}: {text}")
                    return await resp.json()
            except (aiohttp.ClientConnectionError, aiohttp.ClientOSError, asyncio.TimeoutError) as e:
                if attempt < max_retries:
                    logger.warning(f"HB API {method} {path} connection error: {e}, "
                                   f"retrying ({attempt + 1}/{max_retries})...")
                    await asyncio.sleep(1 * (2 ** attempt))
                    continue
                raise

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

    async def get_active_executors(self) -> list:
        """Search for active executors via HB API."""
        result = await self._request("POST", "/executors/search", json={"status": "active"})
        return result if isinstance(result, list) else []

    async def get_exchange_positions(
        self,
        connector_name: str = "bybit_perpetual_testnet",
        account_name: str = "master_account",
    ) -> list:
        """Query actual open positions on the exchange (not executors)."""
        result = await self._request(
            "POST",
            "/trading/positions",
            json={
                "connector_name": connector_name,
                "account_name": account_name,
            },
        )
        data = result.get("data", result) if isinstance(result, dict) else result
        return data if isinstance(data, list) else []

    async def search_executors(self, **filters) -> list:
        """Search executors with optional filters (status, trading_pair, etc.)."""
        result = await self._request("POST", "/executors/search", json=filters)
        if isinstance(result, dict) and "data" in result:
            return result["data"]
        return result if isinstance(result, list) else []

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

    # ── Bot Orchestration ─────────────────────────────────────

    async def upload_controller(
        self, controller_type: str, controller_name: str, source_code: str,
    ) -> Dict[str, Any]:
        """Upload controller source code to HB API.

        The code is written to bots/controllers/{type}/{name}.py
        and becomes available for bot deployment.
        """
        return await self._request(
            "POST",
            f"/controllers/{controller_type}/{controller_name}",
            json={"content": source_code},
        )

    async def create_controller_config(
        self, config_name: str, config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Save a controller config (YAML/JSON) to HB API."""
        return await self._request(
            "POST", f"/controllers/configs/{config_name}", json=config,
        )

    async def delete_controller_config(self, config_name: str) -> Dict[str, Any]:
        """Delete a controller config."""
        return await self._request("DELETE", f"/controllers/configs/{config_name}")

    async def list_controller_configs(self) -> list:
        """List all saved controller configs."""
        return await self._request("GET", "/controllers/configs/")

    async def get_config_template(
        self, controller_type: str, controller_name: str,
    ) -> Dict[str, Any]:
        """Get the config template for a controller (shows available fields + defaults)."""
        return await self._request(
            "GET", f"/controllers/{controller_type}/{controller_name}/config/template",
        )

    async def deploy_v2_bot(
        self,
        instance_name: str,
        credentials_profile: str,
        controllers_config: list,
        max_global_drawdown_quote: Optional[float] = None,
        max_controller_drawdown_quote: Optional[float] = None,
        image: str = "hummingbot/hummingbot:latest",
        headless: bool = True,
    ) -> Dict[str, Any]:
        """Deploy a V2 bot with controllers via bot orchestration.

        Creates a new Docker container running the specified controllers.
        """
        body = {
            "instance_name": instance_name,
            "credentials_profile": credentials_profile,
            "controllers_config": controllers_config,
            "image": image,
            "headless": headless,
        }
        if max_global_drawdown_quote is not None:
            body["max_global_drawdown_quote"] = max_global_drawdown_quote
        if max_controller_drawdown_quote is not None:
            body["max_controller_drawdown_quote"] = max_controller_drawdown_quote
        return await self._request(
            "POST", "/bot-orchestration/deploy-v2-controllers", json=body,
        )

    async def start_bot(self, bot_name: str) -> Dict[str, Any]:
        """Start a deployed bot."""
        return await self._request(
            "POST", "/bot-orchestration/start-bot", json={"bot_name": bot_name},
        )

    async def stop_bot(
        self, bot_name: str, skip_order_cancellation: bool = False,
    ) -> Dict[str, Any]:
        """Stop a running bot."""
        return await self._request(
            "POST", "/bot-orchestration/stop-bot",
            json={"bot_name": bot_name, "skip_order_cancellation": skip_order_cancellation},
        )

    async def get_bot_status(self, bot_name: Optional[str] = None) -> Dict[str, Any]:
        """Get bot status. If bot_name is None, returns status for all bots."""
        if bot_name:
            return await self._request("GET", f"/bot-orchestration/{bot_name}/status")
        return await self._request("GET", "/bot-orchestration/status")

    async def get_bot_history(self, bot_name: str) -> Dict[str, Any]:
        """Get historical bot run data."""
        return await self._request("GET", f"/bot-orchestration/{bot_name}/history")

    async def add_credential(
        self,
        account_name: str,
        connector_name: str,
        credentials: Dict[str, str],
    ) -> Dict[str, Any]:
        """Add connector credentials to an account."""
        return await self._request(
            "POST",
            f"/accounts/add-credential/{account_name}/{connector_name}",
            json=credentials,
        )
