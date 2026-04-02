"""
Async Bybit REST client for derivatives data (funding rates, open interest, L/S ratio).

All endpoints are public (no auth required).
Rate limiting: 0.3s between calls, exponential backoff on failure.
"""
import asyncio
import logging
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

BASE_URL = "https://api.bybit.com"
BYBIT_CATEGORY = "linear"  # USDT perpetuals

# Rate limiting
REQUEST_DELAY = 0.3  # seconds between API calls
MAX_RETRIES = 3


def _bybit_pair(pair: str) -> str:
    """Convert 'BTC-USDT' -> 'BTCUSDT' for Bybit API."""
    return pair.replace("-", "")


async def _get(session: aiohttp.ClientSession, path: str, params: dict) -> dict:
    """Make a GET request to Bybit API and return the result field."""
    url = f"{BASE_URL}{path}"
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        data = await resp.json()
    if data.get("retCode") != 0:
        raise ValueError(f"Bybit API error: {data.get('retMsg')} | path={path} params={params}")
    return data["result"]


async def _fetch_with_retry(session: aiohttp.ClientSession, path: str, params: dict) -> dict:
    """Fetch with retry + exponential backoff. Respects rate limiting."""
    await asyncio.sleep(REQUEST_DELAY)
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            return await _get(session, path, params)
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES - 1:
                backoff = 2 ** attempt
                logger.warning(f"Bybit API retry {attempt + 1}/{MAX_RETRIES}: {e} — sleeping {backoff}s")
                await asyncio.sleep(backoff)
    raise last_err


async def fetch_funding_rates(
    session: aiohttp.ClientSession,
    pair: str,
    limit: int = 200,
    start_ms: Optional[int] = None,
    end_ms: Optional[int] = None,
) -> list[dict]:
    """Fetch funding rate history for a pair."""
    params = {
        "category": BYBIT_CATEGORY,
        "symbol": _bybit_pair(pair),
        "limit": limit,
    }
    if start_ms:
        params["startTime"] = start_ms
    if end_ms:
        params["endTime"] = end_ms

    result = await _fetch_with_retry(session, "/v5/market/funding/history", params)
    rates = []
    for row in reversed(result["list"]):  # Bybit returns newest-first
        rates.append({
            "pair": pair,
            "timestamp_utc": int(row["fundingRateTimestamp"]),
            "funding_rate": float(row["fundingRate"]),
        })
    return rates


async def fetch_open_interest(
    session: aiohttp.ClientSession,
    pair: str,
    interval: str = "1h",
    limit: int = 200,
    start_ms: Optional[int] = None,
    end_ms: Optional[int] = None,
) -> list[dict]:
    """Fetch open interest history for a pair."""
    bybit_intervals = {"5m": "5min", "15m": "15min", "1h": "1h", "4h": "4h", "1d": "1d"}
    iv = bybit_intervals.get(interval, interval)

    params = {
        "category": BYBIT_CATEGORY,
        "symbol": _bybit_pair(pair),
        "intervalTime": iv,
        "limit": limit,
    }
    if start_ms:
        params["startTime"] = start_ms
    if end_ms:
        params["endTime"] = end_ms

    result = await _fetch_with_retry(session, "/v5/market/open-interest", params)
    oi_list = []
    for row in reversed(result["list"]):
        oi_list.append({
            "pair": pair,
            "timestamp_utc": int(row["timestamp"]),
            "oi_value": float(row["openInterest"]),
        })
    return oi_list


async def fetch_ls_ratio(
    session: aiohttp.ClientSession,
    pair: str,
    period: str = "1h",
    limit: int = 200,
) -> list[dict]:
    """Fetch long/short account ratio for a pair."""
    params = {
        "category": BYBIT_CATEGORY,
        "symbol": _bybit_pair(pair),
        "period": period,
        "limit": limit,
    }

    result = await _fetch_with_retry(session, "/v5/market/account-ratio", params)
    ls_list = []
    for row in reversed(result["list"]):
        ls_list.append({
            "pair": pair,
            "timestamp_utc": int(row["timestamp"]),
            "buy_ratio": float(row["buyRatio"]),
            "sell_ratio": float(row["sellRatio"]),
        })
    return ls_list


async def fetch_tickers(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch all USDT perpetual tickers (for universe discovery)."""
    result = await _fetch_with_retry(session, "/v5/market/tickers", {"category": BYBIT_CATEGORY})
    tickers = []
    for t in result["list"]:
        symbol = t["symbol"]
        if not symbol.endswith("USDT"):
            continue
        # Convert BTCUSDT -> BTC-USDT
        base = symbol[:-4]
        pair = f"{base}-USDT"
        tickers.append({
            "pair": pair,
            "last_price": float(t.get("lastPrice", 0)),
            "volume_24h": float(t.get("turnover24h", 0)),
            "funding_rate": float(t.get("fundingRate", 0)),
            "open_interest": float(t.get("openInterestValue", 0)),
        })
    return tickers
