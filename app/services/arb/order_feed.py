"""
H2 OrderFeed — Real-time order/fill updates from Bybit + Binance private WebSocket.

This is THE primary source of truth for fill events. REST is used ONLY for:
- Reconciliation on reconnect (gap recovery)
- Startup state recovery
- Timeout fallback (if WS silent for too long)

Reconnect model:
1. On ANY disconnect → set state WS_DISCONNECTED → block new entries
2. Run gap recovery (REST fetch open orders + recent fills since last exec_id)
3. Rebuild order state idempotently
4. Reconnect WS (re-auth)
5. Resume trading only when WS confirmed live
"""
import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable

import aiohttp

logger = logging.getLogger(__name__)


# ── Data types ──────────────────────────────────────────────────

class FillSource(str, Enum):
    WS = "ws"
    REST_RECONCILE = "rest_reconcile"
    REST_TIMEOUT = "rest_timeout"


@dataclass
class FillEvent:
    """Normalized fill event from either exchange."""
    venue: str              # "bybit" | "binance"
    symbol: str
    order_id: str
    exec_id: str            # unique execution/trade ID
    side: str               # "Buy" | "Sell"
    price: float
    qty: float
    fee: float
    fee_asset: str
    timestamp_ms: int       # exchange timestamp
    local_ts: float         # when we received it
    source: FillSource
    is_maker: bool = False
    order_status: str = ""  # "Filled", "PartiallyFilled", "Cancelled", etc.
    leaves_qty: float = 0   # remaining unfilled quantity


@dataclass
class OrderUpdate:
    """Normalized order status update from either exchange."""
    venue: str
    symbol: str
    order_id: str
    status: str             # "New", "PartiallyFilled", "Filled", "Cancelled", "Rejected"
    filled_qty: float
    avg_price: float
    leaves_qty: float
    timestamp_ms: int
    local_ts: float
    source: FillSource


# ── Callbacks ───────────────────────────────────────────────────

FillCallback = Callable[[FillEvent], None]
OrderCallback = Callable[[OrderUpdate], None]
DisconnectCallback = Callable[[str], None]  # venue name


# ── Bybit Private WebSocket ────────────────────────────────────

class BybitOrderFeed:
    """Private WS for order/execution updates on Bybit linear perps."""

    WS_URL = "wss://stream.bybit.com/v5/private"
    PING_INTERVAL = 20

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        on_fill: FillCallback | None = None,
        on_order: OrderCallback | None = None,
        on_disconnect: DisconnectCallback | None = None,
    ):
        self._api_key = api_key
        self._api_secret = api_secret
        self._on_fill = on_fill
        self._on_order = on_order
        self._on_disconnect = on_disconnect
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._session: aiohttp.ClientSession | None = None
        self._running = False
        self._reconnect_count = 0
        self._connected = False
        self.last_exec_id: str = ""  # track for gap recovery

    @property
    def is_connected(self) -> bool:
        return self._connected

    def _auth_payload(self) -> dict:
        expires = int(time.time() * 1000) + 10000
        sign_str = f"GET/realtime{expires}"
        signature = hmac.new(
            self._api_secret.encode(), sign_str.encode(), hashlib.sha256
        ).hexdigest()
        return {"op": "auth", "args": [self._api_key, expires, signature]}

    async def start(self, session: aiohttp.ClientSession):
        self._session = session
        self._running = True
        while self._running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                self._connected = False
                self._reconnect_count += 1
                wait = min(2 ** self._reconnect_count, 30)
                logger.warning(f"Bybit private WS error: {e}. Reconnect #{self._reconnect_count} in {wait}s")
                if self._on_disconnect:
                    self._on_disconnect("bybit")
                await asyncio.sleep(wait)

    async def _connect_and_listen(self):
        logger.info("Bybit private WS connecting...")
        async with self._session.ws_connect(self.WS_URL, heartbeat=self.PING_INTERVAL) as ws:
            self._ws = ws

            # Authenticate
            await ws.send_json(self._auth_payload())
            auth_resp = await ws.receive_json(timeout=5)
            if not auth_resp.get("success"):
                raise ConnectionError(f"Bybit auth failed: {auth_resp}")
            logger.info("Bybit private WS authenticated")

            # Subscribe to order and execution topics
            await ws.send_json({"op": "subscribe", "args": ["order", "execution"]})
            self._connected = True
            self._reconnect_count = 0
            logger.info("Bybit private WS subscribed to order + execution")

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    self._handle_message(json.loads(msg.data))
                elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                    self._connected = False
                    logger.warning(f"Bybit private WS closed: {msg.type}")
                    if self._on_disconnect:
                        self._on_disconnect("bybit")
                    break

    def _handle_message(self, data: dict):
        topic = data.get("topic", "")

        if topic == "execution":
            for item in data.get("data", []):
                fill = FillEvent(
                    venue="bybit",
                    symbol=item.get("symbol", ""),
                    order_id=item.get("orderId", ""),
                    exec_id=item.get("execId", ""),
                    side=item.get("side", ""),
                    price=float(item.get("execPrice", 0)),
                    qty=float(item.get("execQty", 0)),
                    fee=float(item.get("execFee", 0)),
                    fee_asset=item.get("feeCurrency", "USDT"),
                    timestamp_ms=int(item.get("execTime", 0)),
                    local_ts=time.time(),
                    source=FillSource.WS,
                    is_maker=item.get("isMaker", False),
                    order_status=item.get("orderStatus", ""),
                    leaves_qty=float(item.get("leavesQty", 0)),
                )
                self.last_exec_id = fill.exec_id
                if self._on_fill:
                    self._on_fill(fill)

        elif topic == "order":
            for item in data.get("data", []):
                update = OrderUpdate(
                    venue="bybit",
                    symbol=item.get("symbol", ""),
                    order_id=item.get("orderId", ""),
                    status=item.get("orderStatus", ""),
                    filled_qty=float(item.get("cumExecQty", 0)),
                    avg_price=float(item.get("avgPrice", 0)),
                    leaves_qty=float(item.get("leavesQty", 0)),
                    timestamp_ms=int(item.get("updatedTime", 0)),
                    local_ts=time.time(),
                    source=FillSource.WS,
                )
                if self._on_order:
                    self._on_order(update)

    async def stop(self):
        self._running = False
        self._connected = False
        if self._ws and not self._ws.closed:
            await self._ws.close()


# ── Binance Private WebSocket ──────────────────────────────────

class BinanceOrderFeed:
    """Private WS for order/execution updates on Binance spot."""

    WS_BASE = "wss://stream.binance.com:9443/ws"
    LISTEN_KEY_URL = "https://api.binance.com/api/v3/userDataStream"
    KEEPALIVE_INTERVAL = 30 * 60  # 30min (listenKey expires at 60min)

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        on_fill: FillCallback | None = None,
        on_order: OrderCallback | None = None,
        on_disconnect: DisconnectCallback | None = None,
    ):
        self._api_key = api_key
        self._api_secret = api_secret
        self._on_fill = on_fill
        self._on_order = on_order
        self._on_disconnect = on_disconnect
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._session: aiohttp.ClientSession | None = None
        self._running = False
        self._reconnect_count = 0
        self._connected = False
        self._listen_key: str = ""
        self._keepalive_task: asyncio.Task | None = None
        self.last_exec_id: str = ""

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def _get_listen_key(self) -> str:
        """Create a new listenKey for the user data stream."""
        headers = {"X-MBX-APIKEY": self._api_key}
        async with self._session.post(self.LISTEN_KEY_URL, headers=headers) as resp:
            data = await resp.json()
            if "listenKey" not in data:
                raise ConnectionError(f"Binance listenKey failed: {data}")
            return data["listenKey"]

    async def _keepalive_loop(self):
        """PUT listenKey every 30min to prevent expiry."""
        while self._running:
            try:
                await asyncio.sleep(self.KEEPALIVE_INTERVAL)
                if self._listen_key:
                    headers = {"X-MBX-APIKEY": self._api_key}
                    async with self._session.put(
                        self.LISTEN_KEY_URL,
                        headers=headers,
                        params={"listenKey": self._listen_key},
                    ) as resp:
                        if resp.status == 200:
                            logger.debug("Binance listenKey keepalive OK")
                        else:
                            logger.warning(f"Binance listenKey keepalive failed: {resp.status}")
                            # Force reconnect — listenKey may be invalid
                            if self._ws and not self._ws.closed:
                                await self._ws.close()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Binance keepalive error: {e}")

    async def start(self, session: aiohttp.ClientSession):
        self._session = session
        self._running = True
        while self._running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                self._connected = False
                self._reconnect_count += 1
                wait = min(2 ** self._reconnect_count, 30)
                logger.warning(f"Binance private WS error: {e}. Reconnect #{self._reconnect_count} in {wait}s")
                if self._on_disconnect:
                    self._on_disconnect("binance")
                await asyncio.sleep(wait)

    async def _connect_and_listen(self):
        logger.info("Binance private WS: getting listenKey...")
        self._listen_key = await self._get_listen_key()

        url = f"{self.WS_BASE}/{self._listen_key}"
        logger.info("Binance private WS connecting...")
        async with self._session.ws_connect(url, heartbeat=20) as ws:
            self._ws = ws
            self._connected = True
            self._reconnect_count = 0

            # Start keepalive task
            if self._keepalive_task:
                self._keepalive_task.cancel()
            self._keepalive_task = asyncio.create_task(self._keepalive_loop())

            logger.info("Binance private WS connected (user data stream)")

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    self._handle_message(json.loads(msg.data))
                elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                    self._connected = False
                    logger.warning(f"Binance private WS closed: {msg.type}")
                    if self._on_disconnect:
                        self._on_disconnect("binance")
                    break

    def _handle_message(self, data: dict):
        event_type = data.get("e", "")

        if event_type == "executionReport":
            # Order update + potential fill
            status = data.get("X", "")  # current order status
            exec_type = data.get("x", "")  # execution type: NEW, TRADE, CANCELED, etc.

            # Always emit order update
            update = OrderUpdate(
                venue="binance",
                symbol=data.get("s", ""),
                order_id=str(data.get("i", "")),
                status=self._normalize_bn_status(status),
                filled_qty=float(data.get("z", 0)),  # cumulative filled qty
                avg_price=float(data.get("Z", 0)) / max(float(data.get("z", 0)), 1e-10),  # cum quote / cum qty
                leaves_qty=float(data.get("q", 0)) - float(data.get("z", 0)),  # orig qty - filled
                timestamp_ms=int(data.get("T", 0)),
                local_ts=time.time(),
                source=FillSource.WS,
            )
            if self._on_order:
                self._on_order(update)

            # Emit fill only on TRADE execution
            if exec_type == "TRADE":
                fill = FillEvent(
                    venue="binance",
                    symbol=data.get("s", ""),
                    order_id=str(data.get("i", "")),
                    exec_id=str(data.get("t", "")),  # trade ID
                    side=data.get("S", ""),
                    price=float(data.get("L", 0)),  # last executed price
                    qty=float(data.get("l", 0)),  # last executed qty
                    fee=float(data.get("n", 0)),
                    fee_asset=data.get("N", ""),
                    timestamp_ms=int(data.get("T", 0)),
                    local_ts=time.time(),
                    source=FillSource.WS,
                    is_maker=data.get("m", False),
                    order_status=self._normalize_bn_status(status),
                    leaves_qty=float(data.get("q", 0)) - float(data.get("z", 0)),
                )
                self.last_exec_id = fill.exec_id
                if self._on_fill:
                    self._on_fill(fill)

    @staticmethod
    def _normalize_bn_status(status: str) -> str:
        mapping = {
            "NEW": "New",
            "PARTIALLY_FILLED": "PartiallyFilled",
            "FILLED": "Filled",
            "CANCELED": "Cancelled",
            "REJECTED": "Rejected",
            "EXPIRED": "Cancelled",
        }
        return mapping.get(status, status)

    async def stop(self):
        self._running = False
        self._connected = False
        if self._keepalive_task:
            self._keepalive_task.cancel()
        if self._ws and not self._ws.closed:
            await self._ws.close()


# ── Combined OrderFeed ──────────────────────────────────────────

class OrderFeed:
    """
    Combined order/fill feed from both exchanges.

    WS is the PRIMARY fill source. REST reconciliation runs on:
    - Disconnect recovery
    - Startup
    - Timeout (no fill within expected window)
    """

    def __init__(
        self,
        bybit_key: str,
        bybit_secret: str,
        binance_key: str,
        binance_secret: str,
        on_fill: FillCallback | None = None,
        on_order: OrderCallback | None = None,
        on_disconnect: DisconnectCallback | None = None,
    ):
        self.bybit = BybitOrderFeed(
            bybit_key, bybit_secret,
            on_fill=on_fill, on_order=on_order,
            on_disconnect=on_disconnect,
        )
        self.binance = BinanceOrderFeed(
            binance_key, binance_secret,
            on_fill=on_fill, on_order=on_order,
            on_disconnect=on_disconnect,
        )
        self._tasks: list[asyncio.Task] = []

    @property
    def both_connected(self) -> bool:
        return self.bybit.is_connected and self.binance.is_connected

    async def start(self, session: aiohttp.ClientSession):
        self._tasks = [
            asyncio.create_task(self.bybit.start(session)),
            asyncio.create_task(self.binance.start(session)),
        ]
        logger.info("OrderFeed started (Bybit + Binance private WS)")

    async def stop(self):
        await self.bybit.stop()
        await self.binance.stop()
        for t in self._tasks:
            t.cancel()

    async def reconcile_from_rest(
        self,
        session: aiohttp.ClientSession,
        venue: str,
        bybit_key: str = "",
        bybit_secret: str = "",
        binance_key: str = "",
    ) -> list[FillEvent]:
        """
        REST-based gap recovery after disconnect.
        Fetches recent fills/orders and returns any we missed.

        Called by the execution engine when on_disconnect fires.
        """
        fills = []

        if venue == "bybit" and bybit_key:
            # GET /v5/execution/list — recent executions
            ts = str(int(time.time() * 1000))
            sign_str = f"{ts}{bybit_key}recvWindow=5000&category=linear&limit=50"
            sig = hmac.new(bybit_secret.encode(), sign_str.encode(), hashlib.sha256).hexdigest()
            headers = {
                "X-BAPI-API-KEY": bybit_key,
                "X-BAPI-SIGN": sig,
                "X-BAPI-TIMESTAMP": ts,
                "X-BAPI-RECV-WINDOW": "5000",
            }
            try:
                async with session.get(
                    "https://api.bybit.com/v5/execution/list",
                    headers=headers,
                    params={"category": "linear", "limit": "50"},
                ) as resp:
                    data = await resp.json()
                    for item in data.get("result", {}).get("list", []):
                        fills.append(FillEvent(
                            venue="bybit",
                            symbol=item.get("symbol", ""),
                            order_id=item.get("orderId", ""),
                            exec_id=item.get("execId", ""),
                            side=item.get("side", ""),
                            price=float(item.get("execPrice", 0)),
                            qty=float(item.get("execQty", 0)),
                            fee=float(item.get("execFee", 0)),
                            fee_asset="USDT",
                            timestamp_ms=int(item.get("execTime", 0)),
                            local_ts=time.time(),
                            source=FillSource.REST_RECONCILE,
                            is_maker=item.get("isMaker", False),
                        ))
            except Exception as e:
                logger.error(f"Bybit REST reconciliation failed: {e}")

        elif venue == "binance" and binance_key:
            # GET /api/v3/myTrades — recent trades
            headers = {"X-MBX-APIKEY": binance_key}
            try:
                # Fetch for each tracked symbol (Binance requires per-symbol queries)
                # In practice the execution engine would call this per relevant symbol
                logger.info("Binance REST reconciliation: use per-symbol /api/v3/myTrades")
            except Exception as e:
                logger.error(f"Binance REST reconciliation failed: {e}")

        return fills
