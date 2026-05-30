"""
Live Mainnet Executor — REST-only, exchange-managed TP/SL.

No HB dependency. No WebSocket. No monitoring loop for exits.
Exchange owns TP/SL/Trail — survives our crashes.

Signal loop: fetch data → compute signal → place order with TP/SL → done.
Lifecycle loop: check if positions closed → record PnL → notify.

Env vars required:
    BYBIT_MAINNET_API_KEY
    BYBIT_MAINNET_API_SECRET
    MONGO_URI
    MONGO_DATABASE
    TELEGRAM_BOT_TOKEN
    TELEGRAM_CHAT_ID

Usage:
    source .env && python scripts/live_mainnet.py
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import signal
import sys
import time
import urllib.request
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas_ta as ta
from pymongo import MongoClient

# ── Config ───────────────────────────────────────────────────────────────────

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("live_mainnet")

# Bybit mainnet (same keys used by H2 arb engine)
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BASE_URL = "https://api.bybit.com"

# Telegram — use LIVE_TG_TOKEN / LIVE_TG_CHAT_ID to separate from CryptoSignals bot
# Set these env vars to enable live trade notifications to the Quant group
TG_TOKEN = os.getenv("LIVE_TG_TOKEN", "")
TG_CHAT_ID = os.getenv("LIVE_TG_CHAT_ID", "")

# MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
MONGO_DB = os.getenv("MONGO_DATABASE", "quants_lab")

# Position sizing — MICRO for initial live
POSITION_SIZE_USD = float(os.getenv("LIVE_POSITION_SIZE_USD", "75"))  # $75 per trade
MAX_CONCURRENT_POSITIONS = 5
LEVERAGE = 3  # conservative

# Signal check interval
SIGNAL_INTERVAL_S = 300  # 5 minutes
LIFECYCLE_INTERVAL_S = 60  # 1 minute

# ── Strategy Configs ─────────────────────────────────────────────────────────

STRATEGIES = {
    "X14": {
        "pairs": ["ALGO-USDT", "GALA-USDT", "APT-USDT", "BCH-USDT", "ONT-USDT"],
        "zscore_threshold": 2.0,
        "zscore_window": 168,  # 7 days of 1h bars
        "min_buy_ratio": 0.52,
        "cooldown_hours": 8,
        "direction": "SHORT_ONLY",
        # ATR-based exits (multiples)
        "atr_period": 14,
        "tp_atr_mult": 2.0,
        "sl_atr_mult": 2.5,
        "trailing_act_atr_mult": 1.2,
        "trailing_delta_atr_mult": 0.6,
        # Safety clamps
        "exit_pct_floor": 0.003,
        "exit_pct_ceiling": 0.12,
        "fallback_tp_pct": 0.03,
        "fallback_sl_pct": 0.04,
        "time_limit_s": 172800,  # 2 days
    },
    "X9": {
        "pairs": ["BTC-USDT", "ETH-USDT", "SOL-USDT", "DOGE-USDT", "ADA-USDT",
                  "AVAX-USDT", "LINK-USDT", "BNB-USDT", "CRV-USDT", "1000PEPE-USDT"],
        "funding_z_threshold": 1.0,
        "streak_min": 2,
        "oi_z_min": 0.0,
        "z_window": 30,
        "z_min_periods": 15,
        "cooldown_hours": 48,  # 6 funding events
        "direction": "BOTH",
        # ATR-based exits
        "atr_period": 14,
        "tp_atr_mult": 2.0,
        "sl_atr_mult": 2.5,
        "trailing_act_atr_mult": 1.5,
        "trailing_delta_atr_mult": 0.7,
        # Safety clamps
        "exit_pct_floor": 0.003,
        "exit_pct_ceiling": 0.12,
        "fallback_tp_pct": 0.04,
        "fallback_sl_pct": 0.05,
        "time_limit_s": 172800,  # 48h
    },
    "X17": {
        "pairs": ["BTC-USDT", "ETH-USDT", "ADA-USDT", "APT-USDT", "BCH-USDT",
                  "BNB-USDT", "AVAX-USDT", "LINK-USDT", "SOL-USDT", "ARB-USDT"],
        "blocked_pairs": ["XRP-USDT", "DOT-USDT", "SUI-USDT", "DOGE-USDT"],
        "oi_lookback": 24,
        "oi_drop_threshold": -0.03,
        "use_oi_zscore": True,
        "oi_z_threshold": -1.5,
        "oi_z_window": 168,
        "funding_z_min": 0.0,
        "funding_z_window": 30,
        "anti_signal_z": -1.0,
        "cooldown_hours": 24,
        "direction": "LONG_ONLY",
        # ATR-based exits
        "atr_period": 14,
        "tp_atr_mult": 2.0,
        "sl_atr_mult": 2.5,
        "trailing_act_atr_mult": 1.0,
        "trailing_delta_atr_mult": 0.5,
        # Safety clamps
        "exit_pct_floor": 0.003,
        "exit_pct_ceiling": 0.10,
        "fallback_tp_pct": 0.03,
        "fallback_sl_pct": 0.04,
        "time_limit_s": 86400,  # 24h
    },
    # X8 DISABLED — carry strategies don't make money (Alberto directive)
    # "X8": { ... },
}


# ── Bybit REST Client (minimal, no aiohttp needed) ──────────────────────────

def _sign(ts: str, payload: str) -> str:
    sign_str = ts + API_KEY + "5000" + payload
    return hmac.new(
        API_SECRET.encode(), sign_str.encode(), hashlib.sha256
    ).hexdigest()


def _headers(ts: str, signature: str) -> dict:
    return {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": "5000",
        "X-BAPI-SIGN": signature,
        "Content-Type": "application/json",
    }


def bybit_get(path: str, params: dict) -> dict:
    """Authenticated GET request."""
    ts = str(int(time.time() * 1000))
    from urllib.parse import urlencode
    param_str = urlencode(params)
    signature = _sign(ts, param_str)
    hdrs = _headers(ts, signature)

    url = f"{BASE_URL}{path}"
    if param_str:
        url = f"{url}?{param_str}"

    req = urllib.request.Request(url, headers=hdrs)
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())

    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit GET {path}: [{data.get('retCode')}] {data.get('retMsg')}")
    return data.get("result", {})


def bybit_post(path: str, body: dict) -> dict:
    """Authenticated POST request."""
    ts = str(int(time.time() * 1000))
    payload = json.dumps(body)
    signature = _sign(ts, payload)
    hdrs = _headers(ts, signature)

    url = f"{BASE_URL}{path}"
    req = urllib.request.Request(url, data=payload.encode(), headers=hdrs, method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())

    ret_code = data.get("retCode", -1)
    if ret_code != 0:
        if ret_code == 110043:  # leverage already set
            return data.get("result", {})
        raise RuntimeError(f"Bybit POST {path}: [{ret_code}] {data.get('retMsg')}")
    return data.get("result", {})


# ── Telegram ─────────────────────────────────────────────────────────────────

def tg_send(text: str):
    """Send Telegram notification."""
    if not TG_TOKEN:
        logger.info(f"[TG] {text}")
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        body = json.dumps({"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"})
        req = urllib.request.Request(url, data=body.encode(), headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")


# ── Signal Logic ─────────────────────────────────────────────────────────────

def fetch_ls_ratio(pair: str, limit: int = 200) -> list:
    """Fetch long/short ratio from Bybit public API."""
    symbol = pair.replace("-", "")
    url = (
        f"{BASE_URL}/v5/market/account-ratio"
        f"?category=linear&symbol={symbol}&period=1h&limit={limit}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "LiveMainnet/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        return data.get("result", {}).get("list", [])
    except Exception as e:
        logger.warning(f"LS fetch failed for {pair}: {e}")
        return []


def fetch_klines(pair: str, interval: str = "60", limit: int = 200) -> list:
    """Fetch kline data from Bybit public API."""
    symbol = pair.replace("-", "")
    url = (
        f"{BASE_URL}/v5/market/kline"
        f"?category=linear&symbol={symbol}&interval={interval}&limit={limit}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "LiveMainnet/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        return data.get("result", {}).get("list", [])
    except Exception as e:
        logger.warning(f"Kline fetch failed for {pair}: {e}")
        return []


def compute_x14_signal(pair: str, config: dict) -> Optional[dict]:
    """
    Compute X14 signal for a pair.
    Returns dict with signal details or None if no signal.
    """
    # Fetch LS ratio
    ls_data = fetch_ls_ratio(pair, limit=config["zscore_window"] + 10)
    if len(ls_data) < config["zscore_window"] // 2:
        return None

    # Extract buy_ratio (API returns newest first)
    ratios = [float(r.get("buyRatio", 0.5)) for r in ls_data]
    ratios.reverse()  # oldest first

    arr = np.array(ratios)
    window = min(config["zscore_window"], len(arr))
    mean = np.mean(arr[-window:])
    std = np.std(arr[-window:])

    if std <= 0:
        return None

    current = arr[-1]
    zscore = (current - mean) / std

    if zscore > config["zscore_threshold"] and current > config["min_buy_ratio"]:
        # Compute ATR for exit sizing
        klines = fetch_klines(pair, interval="60", limit=config["atr_period"] + 5)
        if len(klines) < config["atr_period"]:
            return None

        # Klines: [startTime, open, high, low, close, volume, turnover]
        # Newest first from API, reverse
        klines.reverse()
        highs = np.array([float(k[2]) for k in klines])
        lows = np.array([float(k[3]) for k in klines])
        closes = np.array([float(k[4]) for k in klines])
        current_price = closes[-1]

        # Manual ATR calculation
        tr_list = []
        for i in range(1, len(closes)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            tr_list.append(tr)

        if len(tr_list) < config["atr_period"]:
            return None

        atr = np.mean(tr_list[-config["atr_period"]:])

        if atr <= 0 or current_price <= 0:
            return None

        # Compute exit percentages
        tp_pct = config["tp_atr_mult"] * atr / current_price
        sl_pct = config["sl_atr_mult"] * atr / current_price

        # Clamp
        tp_pct = max(config["exit_pct_floor"], min(tp_pct, config["exit_pct_ceiling"]))
        sl_pct = max(config["exit_pct_floor"], min(sl_pct, config["exit_pct_ceiling"]))

        # Trailing stop
        trail_act_pct = config["trailing_act_atr_mult"] * atr / current_price
        trail_delta_pct = config["trailing_delta_atr_mult"] * atr / current_price
        trail_act_pct = max(0.001, min(trail_act_pct, 0.05))
        trail_delta_pct = max(0.001, min(trail_delta_pct, 0.03))

        # For SHORT: TP below entry, SL above entry
        tp_price = round(current_price * (1 - tp_pct), 4)
        sl_price = round(current_price * (1 + sl_pct), 4)

        return {
            "pair": pair,
            "direction": "SHORT",
            "price": current_price,
            "zscore": zscore,
            "buy_ratio": current,
            "atr": atr,
            "tp_pct": tp_pct,
            "sl_pct": sl_pct,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "trail_act_pct": trail_act_pct,
            "trail_delta_pct": trail_delta_pct,
        }

    return None


def fetch_funding_rate(pair: str, limit: int = 50) -> list:
    """Fetch funding rate history from Bybit public API."""
    symbol = pair.replace("-", "")
    url = (
        f"{BASE_URL}/v5/market/funding/history"
        f"?category=linear&symbol={symbol}&limit={limit}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "LiveMainnet/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        return data.get("result", {}).get("list", [])
    except Exception as e:
        logger.warning(f"Funding fetch failed for {pair}: {e}")
        return []


def fetch_open_interest(pair: str, interval: str = "1h", limit: int = 50) -> list:
    """Fetch OI history from Bybit public API."""
    symbol = pair.replace("-", "")
    url = (
        f"{BASE_URL}/v5/market/open-interest"
        f"?category=linear&symbol={symbol}&intervalTime={interval}&limit={limit}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "LiveMainnet/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        return data.get("result", {}).get("list", [])
    except Exception as e:
        logger.warning(f"OI fetch failed for {pair}: {e}")
        return []


def compute_x9_signal(pair: str, config: dict) -> Optional[dict]:
    """
    X9 Funding Crowding Reversion: fade overleveraged side when funding is extreme.
    Returns dict with signal details or None.
    """
    # Fetch funding rate history
    funding_data = fetch_funding_rate(pair, limit=config["z_window"] + 10)
    if len(funding_data) < config["z_min_periods"]:
        return None

    # Newest first from API → reverse for chronological
    funding_data.reverse()
    rates = np.array([float(f.get("fundingRate", 0)) for f in funding_data])

    # Z-score of latest funding rate
    window = min(config["z_window"], len(rates))
    mean_fr = np.mean(rates[-window:])
    std_fr = np.std(rates[-window:])
    if std_fr <= 0:
        return None

    current_fr = rates[-1]
    fr_z = (current_fr - mean_fr) / std_fr

    # Check threshold
    if abs(fr_z) < config["funding_z_threshold"]:
        return None

    # Streak: consecutive same-sign funding events
    streak = 0
    sign = 1 if current_fr > 0 else -1
    for r in reversed(rates):
        if (r > 0 and sign > 0) or (r < 0 and sign < 0):
            streak += 1
        else:
            break

    if streak < config["streak_min"]:
        return None

    # OI z-score filter (optional)
    if config["oi_z_min"] > 0:
        oi_data = fetch_open_interest(pair, limit=config["z_window"] + 5)
        if len(oi_data) >= 10:
            oi_data.reverse()
            oi_vals = np.array([float(o.get("openInterest", 0)) for o in oi_data])
            oi_log_delta = np.diff(np.log(np.maximum(oi_vals, 1)))
            if len(oi_log_delta) >= 10:
                oi_z = (oi_log_delta[-1] - np.mean(oi_log_delta)) / max(np.std(oi_log_delta), 1e-10)
                if oi_z < config["oi_z_min"]:
                    return None

    # Direction: fade the payers
    # Positive funding → longs pay → SHORT (fade longs)
    # Negative funding → shorts pay → LONG (fade shorts)
    direction = "SHORT" if current_fr > 0 else "LONG"

    # Compute ATR for exit sizing
    klines = fetch_klines(pair, interval="60", limit=config["atr_period"] + 5)
    if len(klines) < config["atr_period"]:
        return None

    klines.reverse()
    highs = np.array([float(k[2]) for k in klines])
    lows = np.array([float(k[3]) for k in klines])
    closes = np.array([float(k[4]) for k in klines])
    current_price = closes[-1]

    # ATR
    tr_list = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        tr_list.append(tr)

    if len(tr_list) < config["atr_period"]:
        return None

    atr = np.mean(tr_list[-config["atr_period"]:])
    if atr <= 0 or current_price <= 0:
        return None

    # Exit levels
    tp_pct = config["tp_atr_mult"] * atr / current_price
    sl_pct = config["sl_atr_mult"] * atr / current_price
    tp_pct = max(config["exit_pct_floor"], min(tp_pct, config["exit_pct_ceiling"]))
    sl_pct = max(config["exit_pct_floor"], min(sl_pct, config["exit_pct_ceiling"]))

    if direction == "SHORT":
        tp_price = round(current_price * (1 - tp_pct), 4)
        sl_price = round(current_price * (1 + sl_pct), 4)
    else:
        tp_price = round(current_price * (1 + tp_pct), 4)
        sl_price = round(current_price * (1 - sl_pct), 4)

    return {
        "pair": pair,
        "direction": direction,
        "price": current_price,
        "funding_z": fr_z,
        "funding_rate": current_fr,
        "streak": streak,
        "atr": atr,
        "tp_pct": tp_pct,
        "sl_pct": sl_pct,
        "tp_price": tp_price,
        "sl_price": sl_price,
    }


def compute_x17_signal(pair: str, config: dict) -> Optional[dict]:
    """
    X17 OI Flush Recovery: LONG when OI drops + funding elevated (crowded longs flushed).
    Returns dict with signal details or None.
    """
    # Fetch OI history
    oi_data = fetch_open_interest(pair, interval="1h", limit=config["oi_lookback"] + config["oi_z_window"] + 10)
    if len(oi_data) < config["oi_lookback"] + 5:
        return None

    # Newest first → reverse for chronological
    oi_data.reverse()
    oi_vals = np.array([float(o.get("openInterest", 0)) for o in oi_data])

    if len(oi_vals) < config["oi_lookback"] + 1:
        return None

    # OI change over lookback period
    current_oi = oi_vals[-1]
    past_oi = oi_vals[-(config["oi_lookback"] + 1)]
    if past_oi <= 0:
        return None
    oi_chg_24h = (current_oi - past_oi) / past_oi

    # Check OI drop (adaptive z-score or fixed threshold)
    if config["use_oi_zscore"]:
        # Compute rolling OI changes
        changes = []
        for i in range(config["oi_lookback"], len(oi_vals)):
            chg = (oi_vals[i] - oi_vals[i - config["oi_lookback"]]) / max(oi_vals[i - config["oi_lookback"]], 1)
            changes.append(chg)
        if len(changes) < 20:
            return None
        changes_arr = np.array(changes)
        oi_z = (oi_chg_24h - np.mean(changes_arr)) / max(np.std(changes_arr), 1e-10)
        if oi_z > config["oi_z_threshold"]:  # threshold is negative, e.g. -1.5
            return None
    else:
        if oi_chg_24h > config["oi_drop_threshold"]:  # threshold is negative, e.g. -0.03
            return None

    # Funding confirmation: must be elevated (crowded longs before flush)
    funding_data = fetch_funding_rate(pair, limit=config["funding_z_window"] + 5)
    if len(funding_data) < config.get("funding_z_min_periods", 15):
        return None

    funding_data.reverse()
    rates = np.array([float(f.get("fundingRate", 0)) for f in funding_data])
    window = min(config["funding_z_window"], len(rates))
    mean_fr = np.mean(rates[-window:])
    std_fr = np.std(rates[-window:])
    if std_fr <= 0:
        return None

    funding_z = (rates[-1] - mean_fr) / std_fr

    # Anti-signal: low funding + OI drop = bearish continuation, NOT recovery
    if funding_z < config.get("anti_signal_z", -1.0):
        return None

    # Must have positive or elevated funding (confirms crowded-long prior)
    if funding_z < config["funding_z_min"]:
        return None

    # LONG only
    direction = "LONG"

    # Compute ATR
    klines = fetch_klines(pair, interval="60", limit=config["atr_period"] + 5)
    if len(klines) < config["atr_period"]:
        return None

    klines.reverse()
    highs = np.array([float(k[2]) for k in klines])
    lows = np.array([float(k[3]) for k in klines])
    closes = np.array([float(k[4]) for k in klines])
    current_price = closes[-1]

    tr_list = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        tr_list.append(tr)

    if len(tr_list) < config["atr_period"]:
        return None

    atr = np.mean(tr_list[-config["atr_period"]:])
    if atr <= 0 or current_price <= 0:
        return None

    tp_pct = config["tp_atr_mult"] * atr / current_price
    sl_pct = config["sl_atr_mult"] * atr / current_price
    tp_pct = max(config["exit_pct_floor"], min(tp_pct, config["exit_pct_ceiling"]))
    sl_pct = max(config["exit_pct_floor"], min(sl_pct, config["exit_pct_ceiling"]))

    tp_price = round(current_price * (1 + tp_pct), 4)
    sl_price = round(current_price * (1 - sl_pct), 4)

    return {
        "pair": pair,
        "direction": direction,
        "price": current_price,
        "oi_chg_24h": oi_chg_24h,
        "funding_z": funding_z,
        "atr": atr,
        "tp_pct": tp_pct,
        "sl_pct": sl_pct,
        "tp_price": tp_price,
        "sl_price": sl_price,
    }


def compute_x8_signal(pair: str, config: dict) -> Optional[dict]:
    """
    X8 DeFi-CEX Funding Spread: HL funding leads CEX. Trade the spread reversion.
    Fetches HL funding from MongoDB, Bybit from REST.
    Returns dict with signal details or None.
    """
    # Get HL cumulative 8h funding from MongoDB
    try:
        db = MongoClient(MONGO_URI)[MONGO_DB]
        # Get HL funding for this pair
        pair_coin = pair.replace("-USDT", "")
        hl_docs = list(db.hyperliquid_funding_rates.find(
            {"coin": pair_coin},
            {"funding_rate": 1, "timestamp_utc": 1}
        ).sort("timestamp_utc", -1).limit(config["z_window"] + 50))

        if len(hl_docs) < 50:
            return None

        hl_docs.reverse()  # chronological
        hl_rates = np.array([float(d["funding_rate"]) for d in hl_docs])

        # Resample HL to 8h cumulative windows
        resample = config["funding_resample_hours"]
        if len(hl_rates) < resample:
            return None
        # Rolling sum of last `resample` hourly rates
        hl_cum_8h = np.convolve(hl_rates, np.ones(resample), mode='valid')

    except Exception as e:
        logger.warning(f"X8 HL funding fetch failed for {pair}: {e}")
        return None

    # Get Bybit funding rate
    bybit_data = fetch_funding_rate(pair, limit=config["z_window"] // 8 + 5)
    if len(bybit_data) < 5:
        return None

    bybit_data.reverse()
    bybit_rates = np.array([float(f.get("fundingRate", 0)) for f in bybit_data])

    # Align: take latest HL cumulative and latest Bybit rate
    latest_hl_cum = hl_cum_8h[-1]
    latest_bybit = bybit_rates[-1]

    # Compute spread
    spread = latest_hl_cum - latest_bybit

    # Z-score of spread (using HL cumulative history)
    spreads = []
    # Use as many aligned points as we can
    min_len = min(len(hl_cum_8h), len(bybit_rates))
    if min_len < 20:
        # Not enough aligned data — use HL cumulative distribution
        spread_mean = np.mean(hl_cum_8h[-config["z_window"]:])
        spread_std = np.std(hl_cum_8h[-config["z_window"]:])
    else:
        for i in range(min_len):
            spreads.append(hl_cum_8h[-(min_len - i)] - bybit_rates[-(min_len - i)])
        spreads_arr = np.array(spreads)
        spread_mean = np.mean(spreads_arr)
        spread_std = np.std(spreads_arr)

    if spread_std <= 0:
        return None

    spread_z = (spread - spread_mean) / spread_std

    # Check threshold
    if abs(spread_z) < config["z_threshold"]:
        return None

    # Signal direction
    if config["signal_mode"] == "carry":
        # Positive spread (HL > CEX) → next Bybit will be high → SHORT to earn funding
        # Negative spread (HL < CEX) → next Bybit will be low → LONG to earn
        direction = "SHORT" if spread_z > config["z_threshold"] else "LONG"
    else:  # directional (BTC)
        # Only SHORT on positive spread (DeFi longs overleveraged)
        if spread_z > config["z_threshold"]:
            direction = "SHORT"
        else:
            return None  # no LONG signals in directional mode

    # Compute ATR
    klines = fetch_klines(pair, interval="60", limit=config["atr_period"] + 5)
    if len(klines) < config["atr_period"]:
        return None

    klines.reverse()
    highs = np.array([float(k[2]) for k in klines])
    lows = np.array([float(k[3]) for k in klines])
    closes = np.array([float(k[4]) for k in klines])
    current_price = closes[-1]

    tr_list = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        tr_list.append(tr)

    if len(tr_list) < config["atr_period"]:
        return None

    atr = np.mean(tr_list[-config["atr_period"]:])
    if atr <= 0 or current_price <= 0:
        return None

    tp_pct = config["tp_atr_mult"] * atr / current_price
    sl_pct = config["sl_atr_mult"] * atr / current_price
    tp_pct = max(config["exit_pct_floor"], min(tp_pct, config["exit_pct_ceiling"]))
    sl_pct = max(config["exit_pct_floor"], min(sl_pct, config["exit_pct_ceiling"]))

    if direction == "SHORT":
        tp_price = round(current_price * (1 - tp_pct), 4)
        sl_price = round(current_price * (1 + sl_pct), 4)
    else:
        tp_price = round(current_price * (1 + tp_pct), 4)
        sl_price = round(current_price * (1 - sl_pct), 4)

    return {
        "pair": pair,
        "direction": direction,
        "price": current_price,
        "spread_z": spread_z,
        "hl_cum_8h": latest_hl_cum,
        "bybit_rate": latest_bybit,
        "atr": atr,
        "tp_pct": tp_pct,
        "sl_pct": sl_pct,
        "tp_price": tp_price,
        "sl_price": sl_price,
    }


# ── Order Execution ──────────────────────────────────────────────────────────

def get_instrument_info(pair: str) -> dict:
    """Get min qty, tick size, etc."""
    symbol = pair.replace("-", "")
    result = bybit_get("/v5/market/instruments-info", {
        "category": "linear",
        "symbol": symbol,
    })
    instruments = result.get("list", [])
    if instruments:
        return instruments[0]
    return {}


def get_positions() -> list:
    """Fetch all open positions."""
    result = bybit_get("/v5/position/list", {
        "category": "linear",
        "settleCoin": "USDT",
    })
    positions = []
    for p in result.get("list", []):
        size = float(p.get("size", 0))
        if size > 0:
            positions.append({
                "pair": p["symbol"].replace("USDT", "") + "-USDT" if "-" not in p["symbol"] else p["symbol"],
                "side": p["side"],
                "qty": size,
                "entry_price": float(p.get("avgPrice", 0)),
                "unrealised_pnl": float(p.get("unrealisedPnl", 0)),
                "leverage": p.get("leverage", "1"),
            })
    return positions


def get_wallet_balance() -> float:
    """Get USDT equity."""
    result = bybit_get("/v5/account/wallet-balance", {
        "accountType": "UNIFIED",
    })
    for account in result.get("list", []):
        for coin in account.get("coin", []):
            if coin.get("coin") == "USDT":
                return float(coin.get("equity", 0))
    return 0.0


def get_best_price(pair: str, side: str) -> float:
    """Get best bid/ask for limit order pricing.

    For entries we want maker fees (0.02% vs 0.055% taker).
    - Sell (SHORT entry): place at best ask (or 1 tick below) to sit on book
    - Buy (LONG entry): place at best bid (or 1 tick above) to sit on book
    """
    symbol = pair.replace("-", "")
    url = f"{BASE_URL}/v5/market/orderbook?category=linear&symbol={symbol}&limit=1"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "LiveMainnet/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        result = data.get("result", {})
        bids = result.get("b", [])
        asks = result.get("a", [])
        if side == "Sell" and asks:
            # Place at ask - will sit as maker (or at bid+tick to be slightly passive)
            return float(asks[0][0])
        elif side == "Buy" and bids:
            return float(bids[0][0])
    except Exception as e:
        logger.warning(f"Orderbook fetch failed {pair}: {e}")
    return 0.0


def place_order_with_tp_sl(
    pair: str,
    side: str,
    qty: float,
    tp_price: float,
    sl_price: float,
) -> dict:
    """Place LIMIT order with exchange-managed TP/SL.

    Uses PostOnly to guarantee maker fee (0.02% vs 0.055% taker).
    If PostOnly is rejected (would cross spread), falls back to GTC limit
    at a slightly passive price. Signals last hours, so missing one cycle is fine.

    TP/SL exits remain Market type for guaranteed fill on protection orders.
    """
    symbol = pair.replace("-", "")

    # Set leverage first
    try:
        bybit_post("/v5/position/set-leverage", {
            "category": "linear",
            "symbol": symbol,
            "buyLeverage": str(LEVERAGE),
            "sellLeverage": str(LEVERAGE),
        })
    except RuntimeError as e:
        if "110043" not in str(e):  # already set is fine
            raise

    # Get limit price from orderbook
    limit_price = get_best_price(pair, side)
    if limit_price <= 0:
        # Fallback: use last traded price
        klines = fetch_klines(pair, interval="1", limit=1)
        if klines:
            limit_price = float(klines[0][4])  # close price
        else:
            raise RuntimeError(f"Cannot determine price for {pair}")

    # Get tick size for price rounding
    info = get_instrument_info(pair)
    price_filter = info.get("priceFilter", {})
    tick_size = float(price_filter.get("tickSize", "0.01"))

    # Round price to tick size
    limit_price = round(limit_price / tick_size) * tick_size
    # Determine decimal places from tick size string (handle scientific notation)
    tick_str = price_filter.get("tickSize", "0.01")  # use original string from API
    if '.' in tick_str:
        decimals = len(tick_str.rstrip('0').split('.')[1])
    else:
        decimals = 0
    limit_price = round(limit_price, max(decimals, 1))

    # Place PostOnly limit order — guarantees maker fee or rejection
    body = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Limit",
        "price": str(limit_price),
        "qty": str(qty),
        "timeInForce": "PostOnly",
        "takeProfit": str(tp_price),
        "stopLoss": str(sl_price),
        "tpslMode": "Full",
        "tpOrderType": "Market",  # exits use Market for guaranteed fill
        "slOrderType": "Market",
    }

    try:
        result = bybit_post("/v5/order/create", body)
        result["_limit_price"] = limit_price
        result["_order_type"] = "PostOnly"
        return result
    except RuntimeError as e:
        if "170136" in str(e) or "PostOnly" in str(e):
            # PostOnly rejected (would cross spread) — use GTC limit
            # Be slightly aggressive: 1 tick through for near-instant fill
            if side == "Sell":
                limit_price -= tick_size  # lower = more aggressive for sell
            else:
                limit_price += tick_size  # higher = more aggressive for buy
            limit_price = round(limit_price, max(decimals, 1))

            body["timeInForce"] = "GTC"
            body["price"] = str(limit_price)
            result = bybit_post("/v5/order/create", body)
            result["_limit_price"] = limit_price
            result["_order_type"] = "GTC_aggressive"
            return result
        raise


def compute_qty(pair: str, price: float) -> float:
    """Compute quantity from USD size and instrument constraints."""
    notional = POSITION_SIZE_USD * LEVERAGE
    raw_qty = notional / price

    # Get instrument info for min/step
    info = get_instrument_info(pair)
    lot_filter = info.get("lotSizeFilter", {})
    min_qty = float(lot_filter.get("minOrderQty", "0.001"))
    step = float(lot_filter.get("qtyStep", "0.001"))

    if raw_qty < min_qty:
        logger.warning(f"{pair}: qty {raw_qty} < min {min_qty}, skipping")
        return 0.0

    # Round down to step
    qty = int(raw_qty / step) * step
    # Round to avoid floating point issues
    decimals = len(str(step).rstrip('0').split('.')[-1]) if '.' in str(step) else 0
    qty = round(qty, decimals)

    return qty


# ── Main Loop ────────────────────────────────────────────────────────────────

class LiveMainnet:
    def __init__(self):
        self.db = MongoClient(MONGO_URI)[MONGO_DB]
        self._orders_this_cycle = set()  # pairs with orders placed this scan cycle
        self.running = True

        # Restore cooldowns from MongoDB (survives restarts)
        self._restore_cooldowns()

        # Graceful shutdown
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _restore_cooldowns(self):
        """Restore cooldown state from live_executions collection.

        Cooldown = don't re-enter same engine+pair within cooldown_hours of:
        - Last signal fire (opened_at) if position is still OPEN
        - Last close time (closed_at) if position was closed by SL

        This ensures restarts don't wipe cooldown state.
        """
        self.last_signal_time = {}  # "engine:pair" -> timestamp (epoch)

        for engine, config in STRATEGIES.items():
            cooldown_s = config["cooldown_hours"] * 3600
            for pair in config["pairs"]:
                key = f"{engine}:{pair}"
                # Find most recent execution for this engine+pair
                doc = self.db.live_executions.find_one(
                    {"engine": engine, "pair": pair},
                    sort=[("opened_at", -1)],
                )
                if not doc:
                    continue

                if doc.get("status") == "OPEN":
                    # Position still open — cooldown from open time
                    opened = doc.get("opened_at")
                    if opened:
                        self.last_signal_time[key] = opened.timestamp()
                elif doc.get("close_type") == "SL":
                    # SL close — cooldown from CLOSE time (not open time)
                    closed = doc.get("closed_at")
                    if closed:
                        self.last_signal_time[key] = closed.timestamp()
                    else:
                        # Fallback to opened_at
                        opened = doc.get("opened_at")
                        if opened:
                            self.last_signal_time[key] = opened.timestamp()
                else:
                    # TP or other close — cooldown from open time (normal)
                    opened = doc.get("opened_at")
                    if opened:
                        self.last_signal_time[key] = opened.timestamp()

        restored = {k: v for k, v in self.last_signal_time.items()
                    if time.time() - v < max(c["cooldown_hours"] for c in STRATEGIES.values()) * 3600}
        logger.info(f"Restored {len(restored)} active cooldowns from MongoDB")

    def _shutdown(self, *_):
        logger.info("Shutdown signal received")
        self.running = False

    def _can_trade(self, engine: str, pair: str, config: dict) -> bool:
        """Check cooldown, position conflicts, and concurrent limits."""
        # Cooldown check (per engine+pair)
        key = f"{engine}:{pair}"
        last = self.last_signal_time.get(key, 0)
        cooldown_s = config["cooldown_hours"] * 3600
        if time.time() - last < cooldown_s:
            return False

        # Check if ANY engine already placed an order on this pair this scan cycle
        # (prevents X9 SHORT + X8 LONG on same pair cancelling each other in one-way mode)
        if pair in self._orders_this_cycle:
            return False

        # Check if we already have a position on this pair
        positions = get_positions()
        if any(p["pair"] == pair for p in positions):
            return False

        # Max concurrent positions (include orders placed this cycle)
        total = len(positions) + len(self._orders_this_cycle)
        if total >= MAX_CONCURRENT_POSITIONS:
            return False

        return True

    def _compute_signal(self, engine: str, pair: str, config: dict) -> Optional[dict]:
        """Dispatch to the correct signal function per engine."""
        if engine == "X14":
            return compute_x14_signal(pair, config)
        elif engine == "X9":
            return compute_x9_signal(pair, config)
        elif engine == "X17":
            return compute_x17_signal(pair, config)
        elif engine == "X8":
            return compute_x8_signal(pair, config)
        return None

    def _format_signal_details(self, engine: str, sig: dict) -> str:
        """Format signal-specific details for logging."""
        if engine == "X14":
            return f"z={sig.get('zscore', 0):.2f} br={sig.get('buy_ratio', 0):.4f}"
        elif engine == "X9":
            return f"fr_z={sig.get('funding_z', 0):.2f} streak={sig.get('streak', 0)}"
        elif engine == "X17":
            return f"oi_chg={sig.get('oi_chg_24h', 0):.3f} fr_z={sig.get('funding_z', 0):.2f}"
        elif engine == "X8":
            return f"spread_z={sig.get('spread_z', 0):.2f} hl_cum={sig.get('hl_cum_8h', 0):.6f}"
        return ""

    def signal_loop(self):
        """Check all strategies for signals."""
        logger.info("Signal scan starting...")
        self._orders_this_cycle = set()  # reset per scan
        for engine, config in STRATEGIES.items():
            pairs = config["pairs"]
            # Filter blocked pairs (X17 has explicit blocked_pairs)
            blocked = config.get("blocked_pairs", [])
            for pair in pairs:
                if pair in blocked:
                    continue
                try:
                    if not self._can_trade(engine, pair, config):
                        continue

                    sig = self._compute_signal(engine, pair, config)
                    if sig is None:
                        continue

                    # We have a signal!
                    details = self._format_signal_details(engine, sig)
                    logger.info(
                        f"SIGNAL: {engine} {sig['direction']} {pair} "
                        f"{details} price={sig['price']}"
                    )

                    # Compute qty
                    qty = compute_qty(pair, sig["price"])
                    if qty <= 0:
                        continue

                    # Place order
                    side = "Sell" if sig["direction"] == "SHORT" else "Buy"
                    result = place_order_with_tp_sl(
                        pair=pair,
                        side=side,
                        qty=qty,
                        tp_price=sig["tp_price"],
                        sl_price=sig["sl_price"],
                    )

                    order_id = result.get("orderId", "unknown")
                    limit_price = result.get("_limit_price", sig["price"])
                    order_type = result.get("_order_type", "PostOnly")
                    self.last_signal_time[f"{engine}:{pair}"] = time.time()
                    self._orders_this_cycle.add(pair)

                    # Record to MongoDB (generic — store full signal dict)
                    execution_doc = {
                        "engine": engine,
                        "pair": pair,
                        "direction": sig["direction"],
                        "side": side,
                        "qty": qty,
                        "limit_price": limit_price,
                        "entry_price": None,
                        "tp_price": sig["tp_price"],
                        "sl_price": sig["sl_price"],
                        "tp_pct": sig["tp_pct"],
                        "sl_pct": sig["sl_pct"],
                        "atr": sig.get("atr"),
                        "signal_details": {k: v for k, v in sig.items()
                                           if k not in ("pair", "direction", "price",
                                                        "tp_price", "sl_price", "tp_pct", "sl_pct", "atr")},
                        "order_id": order_id,
                        "order_type": order_type,
                        "position_size_usd": POSITION_SIZE_USD,
                        "leverage": LEVERAGE,
                        "opened_at": datetime.now(timezone.utc),
                        "filled_at": None,
                        "closed_at": None,
                        "close_type": None,
                        "pnl": None,
                        "status": "PENDING",
                    }
                    self.db.live_executions.insert_one(execution_doc)

                    # Telegram alert
                    dir_emoji = "🔴" if sig["direction"] == "SHORT" else "🟢"
                    tg_send(
                        f"{dir_emoji} <b>LIVE {engine} {sig['direction']} {pair}</b>\n"
                        f"Limit: ${limit_price:.4f} ({order_type})\n"
                        f"Size: {qty} (${POSITION_SIZE_USD})\n"
                        f"TP: ${sig['tp_price']:.4f} ({sig['tp_pct']*100:.1f}%)\n"
                        f"SL: ${sig['sl_price']:.4f} ({sig['sl_pct']*100:.1f}%)\n"
                        f"{details}\n"
                        f"Order: {order_id}"
                    )

                    logger.info(f"ORDER PLACED: {engine} {pair} {side} qty={qty} limit={limit_price} order_id={order_id}")

                except Exception as e:
                    logger.error(f"Signal check error {engine}/{pair}: {e}", exc_info=True)
                    tg_send(f"⚠️ Signal error {engine}/{pair}: {e}")

        logger.info("Signal scan complete (no signals = below threshold)")

    def lifecycle_loop(self):
        """Check for fills, closed positions, and stale orders."""
        positions = get_positions()
        pos_pairs = {p["pair"]: p for p in positions}

        # 1. Handle PENDING orders (waiting for fill)
        pending = list(self.db.live_executions.find({"status": "PENDING"}))
        for exc in pending:
            pair = exc["pair"]
            if pair in pos_pairs:
                # Filled! Transition to OPEN
                entry_price = pos_pairs[pair]["entry_price"]
                self.db.live_executions.update_one(
                    {"_id": exc["_id"]},
                    {"$set": {
                        "status": "OPEN",
                        "entry_price": entry_price,
                        "filled_at": datetime.now(timezone.utc),
                    }}
                )
                logger.info(f"FILLED: {exc['engine']} {pair} @ {entry_price}")
                tg_send(f"✅ <b>FILLED {exc['engine']} {pair}</b> @ ${entry_price:.4f}")
            else:
                # Check if order is stale (>5 min unfilled) — cancel
                opened = exc["opened_at"]
                if opened.tzinfo is None:
                    opened = opened.replace(tzinfo=timezone.utc)
                age_s = (datetime.now(timezone.utc) - opened).total_seconds()
                if age_s > 300:  # 5 min fill timeout
                    try:
                        symbol = pair.replace("-", "")
                        bybit_post("/v5/order/cancel", {
                            "category": "linear",
                            "symbol": symbol,
                            "orderId": exc["order_id"],
                        })
                    except Exception as e:
                        logger.warning(f"Cancel failed {pair}: {e}")

                    self.db.live_executions.update_one(
                        {"_id": exc["_id"]},
                        {"$set": {"status": "CANCELLED", "close_type": "UNFILLED_TIMEOUT"}}
                    )
                    # Reset cooldown so signal can re-fire next cycle
                    self.last_signal_time.pop(f"{exc['engine']}:{pair}", None)
                    logger.info(f"CANCELLED (unfilled 5min): {exc['engine']} {pair}")

        # 2. Handle OPEN positions (waiting for TP/SL)
        open_execs = list(self.db.live_executions.find({"status": "OPEN"}))
        for exc in open_execs:
            pair = exc["pair"]
            if pair not in pos_pairs:
                # Position closed by exchange (TP or SL hit)
                pnl = self._fetch_closed_pnl(pair, exc["opened_at"])
                close_type = "TP" if pnl and pnl > 0 else "SL" if pnl and pnl < 0 else "UNKNOWN"

                now_utc = datetime.now(timezone.utc)
                self.db.live_executions.update_one(
                    {"_id": exc["_id"]},
                    {"$set": {
                        "status": "CLOSED",
                        "closed_at": now_utc,
                        "close_type": close_type,
                        "pnl": pnl,
                    }}
                )

                # Update in-memory cooldown — especially critical after SL
                # to prevent immediate re-entry on same pair
                key = f"{exc['engine']}:{pair}"
                self.last_signal_time[key] = time.time()

                emoji = "✅" if pnl and pnl > 0 else "❌"
                pnl_str = f"${pnl:.2f}" if pnl else "pending"
                entry_str = f"${exc['entry_price']:.4f}" if exc.get('entry_price') else "unknown"
                tg_send(
                    f"{emoji} <b>CLOSED {exc['engine']} {pair}</b>\n"
                    f"Type: {close_type}\n"
                    f"PnL: {pnl_str}\n"
                    f"Entry: {entry_str}"
                )

                logger.info(f"CLOSED: {exc['engine']} {pair} type={close_type} pnl={pnl}")

        # 3. Check time limits on still-open positions
        self._check_time_limits(open_execs, pos_pairs)

    def _check_time_limits(self, open_execs: list, pos_pairs: dict):
        """Force-close positions that exceed their strategy's time limit."""
        for exc in open_execs:
            pair = exc["pair"]
            engine = exc["engine"]
            if pair not in pos_pairs:
                continue  # already closed, handled above

            # Get time limit for this engine
            engine_cfg = STRATEGIES.get(engine, {})
            time_limit_s = engine_cfg.get("time_limit_s")
            if not time_limit_s:
                continue

            # Check elapsed time
            opened = exc.get("filled_at") or exc["opened_at"]
            if opened.tzinfo is None:
                opened = opened.replace(tzinfo=timezone.utc)
            elapsed = (datetime.now(timezone.utc) - opened).total_seconds()

            if elapsed < time_limit_s:
                continue

            # Time limit exceeded — market close
            pos = pos_pairs[pair]
            close_side = "Sell" if pos["side"] == "Buy" else "Buy"
            symbol = pair.replace("-", "")

            logger.info(f"TIME_LIMIT: {engine} {pair} open {elapsed/3600:.1f}h > limit {time_limit_s/3600:.0f}h — closing")
            try:
                bybit_post("/v5/order/create", {
                    "category": "linear",
                    "symbol": symbol,
                    "side": close_side,
                    "orderType": "Market",
                    "qty": str(pos["qty"]),
                    "reduceOnly": True,
                })
            except Exception as e:
                logger.error(f"TIME_LIMIT close failed {pair}: {e}")
                tg_send(f"⚠️ TIME_LIMIT close FAILED {engine} {pair}: {e}")
                continue

            # Fetch PnL after close
            time.sleep(2)  # brief wait for exchange to settle
            pnl = self._fetch_closed_pnl(pair, exc["opened_at"])

            self.db.live_executions.update_one(
                {"_id": exc["_id"]},
                {"$set": {
                    "status": "CLOSED",
                    "closed_at": datetime.now(timezone.utc),
                    "close_type": "TIME_LIMIT",
                    "pnl": pnl,
                }}
            )

            emoji = "⏰" if pnl and pnl > 0 else "⏰❌"
            pnl_str = f"${pnl:.2f}" if pnl else "pending"
            tg_send(
                f"{emoji} <b>TIME_LIMIT {engine} {pair}</b>\n"
                f"Open: {elapsed/3600:.1f}h (limit: {time_limit_s/3600:.0f}h)\n"
                f"PnL: {pnl_str}"
            )
            logger.info(f"TIME_LIMIT closed: {engine} {pair} pnl={pnl}")

    def _fetch_closed_pnl(self, pair: str, since: datetime) -> Optional[float]:
        """Fetch closed PnL from Bybit."""
        try:
            symbol = pair.replace("-", "")
            start_ms = int(since.timestamp() * 1000)
            result = bybit_get("/v5/position/closed-pnl", {
                "category": "linear",
                "symbol": symbol,
                "startTime": str(start_ms),
                "limit": "5",
            })
            records = result.get("list", [])
            if records:
                return sum(float(r.get("closedPnl", 0)) for r in records)
        except Exception as e:
            logger.warning(f"Closed PnL fetch failed {pair}: {e}")
        return None

    def run(self):
        """Main event loop."""
        logger.info("=" * 60)
        logger.info("LIVE MAINNET EXECUTOR STARTING")
        logger.info(f"Strategies: {list(STRATEGIES.keys())}")
        logger.info(f"Position size: ${POSITION_SIZE_USD} | Leverage: {LEVERAGE}x")
        logger.info(f"Max concurrent: {MAX_CONCURRENT_POSITIONS}")
        logger.info("=" * 60)

        # Verify credentials
        if not API_KEY or not API_SECRET:
            logger.error("BYBIT_MAINNET_API_KEY / BYBIT_MAINNET_API_SECRET not set!")
            sys.exit(1)

        # Test connection
        try:
            balance = get_wallet_balance()
            logger.info(f"Connected to Bybit mainnet. USDT equity: ${balance:.2f}")
            tg_send(
                f"🟢 <b>Live executor started</b>\n"
                f"Equity: ${balance:.2f}\n"
                f"Size/trade: ${POSITION_SIZE_USD}\n"
                f"Strategies: {', '.join(STRATEGIES.keys())}"
            )
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            tg_send(f"🔴 Live executor FAILED to start: {e}")
            sys.exit(1)

        # Reconcile: check if we have open positions from before
        self._reconcile_on_start()

        last_signal = 0
        last_lifecycle = 0

        while self.running:
            now = time.time()

            # Signal check every 5 min
            if now - last_signal >= SIGNAL_INTERVAL_S:
                try:
                    self.signal_loop()
                except Exception as e:
                    logger.error(f"Signal loop error: {e}", exc_info=True)
                last_signal = now

            # Lifecycle check every 1 min
            if now - last_lifecycle >= LIFECYCLE_INTERVAL_S:
                try:
                    self.lifecycle_loop()
                except Exception as e:
                    logger.error(f"Lifecycle loop error: {e}", exc_info=True)
                last_lifecycle = now

            time.sleep(5)  # 5s tick

        logger.info("Executor stopped gracefully")
        tg_send("🔴 Live executor stopped")

    def _reconcile_on_start(self):
        """Check for positions that closed while we were down."""
        open_execs = list(self.db.live_executions.find({"status": "OPEN"}))
        if not open_execs:
            return

        positions = get_positions()
        pos_pairs = {p["pair"] for p in positions}

        for exc in open_execs:
            if exc["pair"] not in pos_pairs:
                pnl = self._fetch_closed_pnl(exc["pair"], exc["opened_at"])
                close_type = "TP" if pnl and pnl > 0 else "SL" if pnl and pnl < 0 else "RECONCILED"
                self.db.live_executions.update_one(
                    {"_id": exc["_id"]},
                    {"$set": {
                        "status": "CLOSED",
                        "closed_at": datetime.now(timezone.utc),
                        "close_type": close_type,
                        "pnl": pnl,
                    }}
                )
                logger.info(f"Reconciled: {exc['engine']} {exc['pair']} -> {close_type} pnl={pnl}")


if __name__ == "__main__":
    executor = LiveMainnet()
    executor.run()
