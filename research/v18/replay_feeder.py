#!/usr/bin/env python3
"""Replay event FEEDER (pillar 2, engine 1:1 replay harness) -- 2026-07-07.

Loads recorded HL events for a window and re-emits them in the EXACT WebSocket JSON shapes the live engine's
dispatch loop consumes (hl_copy_trader_v17.py ~L4436), in time order. The harness (next step) drives the
engine's real decision methods with this stream + a mocked clock + shadow fills, so backtest == live.

Sources (verified 2026-07-07):
  - leader `trades`: mongo `v17_target_fills` (wallet/coin/side/price/size/ts_epoch[SECONDS]).
  - `l2Book`:        mongo `hyperliquid_l2_snapshots_1s` (coin, levels_bid_json/levels_ask_json = [{px,sz,n}]).
orderUpdates/webData2 are NOT replayed: the engine runs in shadow_mode and simulates its own fills + account.

This module is PURE (read-only mongo, no engine import). Testable in isolation.
"""
from __future__ import annotations
import json
import pandas as pd
from pymongo import MongoClient

DUMMY = "0x0000000000000000000000000000000000000000"  # counterparty placeholder (engine only checks OUR wallets)
BOOK_COINS = {"ADA", "AVAX", "BNB", "BTC", "CRV", "DOGE", "ETH", "HYPE", "LINK", "SOL"}  # L2 collector coverage


def _db():
    return MongoClient("mongodb://localhost:27017/")["quants_lab"]


def _trade_event(doc):
    """v17_target_fills row -> ('trades' WS event, time_ms). Leader in buyer slot for BUY, seller for SELL."""
    w = doc["wallet"].lower()
    is_buy = str(doc.get("side", "")).upper() == "BUY"
    users = [w, DUMMY] if is_buy else [DUMMY, w]
    ts_ms = int(float(doc["ts_epoch"]) * 1000)
    trade = {
        "coin": doc["coin"],
        "side": "B" if is_buy else "A",
        "px": str(doc["price"]),
        "sz": str(doc["size"]),
        "time": ts_ms,
        "users": users,
        "tid": f"rp-{doc['_id']}",           # synthetic, unique+stable (dedup via _seen_tids)
    }
    return ts_ms, {"channel": "trades", "data": [trade]}


def _book_event(doc):
    """l2 snapshot row -> ('l2Book' WS event, time_ms). levels = [bids, asks], each [{px,sz,n}]."""
    bids = json.loads(doc["levels_bid_json"]) if isinstance(doc.get("levels_bid_json"), str) else doc.get("levels_bid_json", [])
    asks = json.loads(doc["levels_ask_json"]) if isinstance(doc.get("levels_ask_json"), str) else doc.get("levels_ask_json", [])
    ts_ms = int(doc["timestamp_utc"])
    return ts_ms, {"channel": "l2Book", "data": {"coin": doc["coin"], "levels": [bids, asks]}}


def load_events(t0_ms: int, t1_ms: int, coins=None):
    """Return a time-ordered list of (time_ms, ws_event_dict) for [t0_ms, t1_ms).
    `coins`: optional set to restrict BOTH trades and books (default: all)."""
    db = _db()
    events = []
    # leader trades (ts_epoch is SECONDS)
    q = {"ts_epoch": {"$gte": t0_ms / 1000.0, "$lt": t1_ms / 1000.0}}
    for d in db["v17_target_fills"].find(q):
        if coins and d.get("coin") not in coins:
            continue
        events.append(_trade_event(d))
    # l2 books (timestamp_utc is MS)
    qb = {"timestamp_utc": {"$gte": t0_ms, "$lt": t1_ms}}
    if coins:
        qb["coin"] = {"$in": list(coins)}
    for d in db["hyperliquid_l2_snapshots_1s"].find(qb):
        events.append(_book_event(d))
    events.sort(key=lambda e: e[0])
    return events


if __name__ == "__main__":
    import sys
    t0 = int(pd.Timestamp("2026-07-06", tz="UTC").timestamp() * 1000)
    t1 = int(pd.Timestamp("2026-07-08", tz="UTC").timestamp() * 1000)
    evs = load_events(t0, t1)
    n_tr = sum(1 for _, e in evs if e["channel"] == "trades")
    n_bk = sum(1 for _, e in evs if e["channel"] == "l2Book")
    print(f"loaded {len(evs)} events Jul6-8: {n_tr} trades + {n_bk} l2Book")
    # ordering + shape asserts
    assert all(evs[i][0] <= evs[i + 1][0] for i in range(len(evs) - 1)), "NOT time-ordered"
    tr = next(e for _, e in evs if e["channel"] == "trades")
    bk = next(e for _, e in evs if e["channel"] == "l2Book")
    t = tr["data"][0]
    assert {"coin", "side", "px", "sz", "time", "users", "tid"} <= set(t), f"trade missing fields: {t.keys()}"
    assert len(t["users"]) == 2 and (t["users"][0] != DUMMY or t["users"][1] != DUMMY), "leader not in users"
    lv = bk["data"]["levels"]
    assert len(lv) == 2 and lv[0] and "px" in lv[0][0] and "sz" in lv[0][0], f"bad l2 levels: {lv[0][:1]}"
    print("SHAPE OK: trades carry users[leader]+tid; l2Book levels=[bids,asks] of {px,sz}")
    print(f"  sample trade: coin={t['coin']} side={t['side']} px={t['px']} sz={t['sz']} users0={t['users'][0][:10]}")
    print(f"  sample book:  coin={bk['data']['coin']} bid0={lv[0][0]['px']} ask0={lv[1][0]['px'] if lv[1] else None}")
    print(f"  time span: {pd.Timestamp(evs[0][0],unit='ms',tz='UTC')} -> {pd.Timestamp(evs[-1][0],unit='ms',tz='UTC')}")
