#!/usr/bin/env python3
"""
v11_heal_drift.py -- one-shot reconciliation of v11_open_positions to exchange truth.

Hard Rule 8: exchange is source of truth. V12 mongo state has drifted because
_enter_position's add-on path could overwrite cumulative size on transient in-memory miss.
This script forces v11_open_positions to match per-coin exchange net.

Strategy per coin:
- Compute exchange_net signed size across main + builder dexes
- Compute tracked_net from v11_open_positions sums (signed)
- If equal (within epsilon), skip
- If tracked_net > exchange_net: prune smallest wallet rows until sum <= exchange
- If tracked_net < exchange_net: bump the largest wallet row to absorb the missing units
- Update entry_px on the surviving row to exchange volume-weighted entry

USAGE: run while V12 is stopped (e.g. between SIGTERM and launchd respawn).
"""
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests
from pymongo import MongoClient

HL_API = "https://api.hyperliquid.xyz"
PARENT = "0x11ca20aeb7cd014cf8406560ae405b12601994b4"
BUILDER_DEXES = ["xyz", "flx"]
DB_OPEN_POSITIONS = "v11_open_positions"

EPSILON = 1e-9
NOTIONAL_THRESHOLD = 2.0  # ignore drift below this notional

def fetch_exchange_positions():
    """Return {coin: (signed_size, entry_px, market_type)} across all dexes."""
    out = {}
    # main perps
    r = requests.post(f"{HL_API}/info", json={"type": "clearinghouseState", "user": PARENT}, timeout=5)
    for ap in r.json().get("assetPositions", []):
        p = ap["position"]
        coin = p["coin"]
        out[coin] = (float(p["szi"]), float(p.get("entryPx", 0)), "perp")
    # builder dexes
    for dex in BUILDER_DEXES:
        try:
            r = requests.post(f"{HL_API}/info", json={"type": "clearinghouseState", "user": PARENT, "dex": dex}, timeout=5)
            for ap in r.json().get("assetPositions", []):
                p = ap["position"]
                coin = p["coin"]
                out[coin] = (float(p["szi"]), float(p.get("entryPx", 0)), dex)
        except Exception as e:
            print(f"  warn: {dex} dex fetch failed: {e}", file=sys.stderr)
    return out


def main():
    client = MongoClient("mongodb://localhost:27017")
    db = client.quants_lab
    col = db[DB_OPEN_POSITIONS]

    print("=== v11_heal_drift (one-shot exchange reconciliation) ===")
    print(f"  ts = {datetime.now(timezone.utc).isoformat()}")
    print()

    exch = fetch_exchange_positions()
    print(f"Exchange positions: {len(exch)} nonzero coins")
    for coin, (sz, px, mt) in exch.items():
        print(f"  exch {coin}: size={sz} entry={px} dex={mt}")
    print()

    # Group DB rows by coin
    db_rows_by_coin = defaultdict(list)
    for doc in col.find():
        db_rows_by_coin[doc["coin"]].append(doc)

    print(f"DB rows: {sum(len(v) for v in db_rows_by_coin.values())} across {len(db_rows_by_coin)} coins")
    print()

    actions = []
    all_coins = set(db_rows_by_coin.keys()) | set(exch.keys())
    for coin in sorted(all_coins):
        rows = db_rows_by_coin.get(coin, [])
        exch_sz, exch_px, _ = exch.get(coin, (0.0, 0.0, ""))
        tracked_net = sum((r["size"] if r["side"] == "BUY" else -r["size"]) for r in rows)

        diff = abs(exch_sz - tracked_net)
        notional = diff * max(exch_px, 1e-9) if exch_px else diff
        if diff < EPSILON or notional < NOTIONAL_THRESHOLD:
            continue

        print(f"DRIFT {coin}: tracked={tracked_net:.6f}, exchange={exch_sz:.6f}, diff={diff:.6f}")

        # Case A: tracked > exchange (phantom rows): prune smallest until sum matches
        if abs(tracked_net) > abs(exch_sz) + EPSILON:
            tgt_side = "BUY" if exch_sz > 0 else "SELL"
            # Drop rows on the dominant side, smallest first
            same_side = sorted([r for r in rows if r["side"] == tgt_side], key=lambda r: r["size"])
            opp_side = [r for r in rows if r["side"] != tgt_side]
            # Drop all opposite-side rows first (they shouldn't exist if exchange is net same-side)
            for r in opp_side:
                col.delete_one({"_id": r["_id"]})
                actions.append(f"  DEL {coin} {r.get('wallet','?')[:12]} {r['side']} sz={r['size']} (opposite side)")
                rows.remove(r)
            # Recompute
            tracked_net = sum((r["size"] if r["side"] == "BUY" else -r["size"]) for r in rows)
            # Now drop smallest same-side rows until tracked <= exchange
            same_side = sorted([r for r in rows if r["side"] == tgt_side], key=lambda r: r["size"])
            for r in list(same_side):
                if abs(tracked_net) <= abs(exch_sz) + EPSILON:
                    break
                col.delete_one({"_id": r["_id"]})
                rows.remove(r)
                actions.append(f"  DEL {coin} {r.get('wallet','?')[:12]} {r['side']} sz={r['size']} (phantom)")
                tracked_net = sum((rr["size"] if rr["side"] == "BUY" else -rr["size"]) for rr in rows)

        # Recompute after deletions
        tracked_net = sum((r["size"] if r["side"] == "BUY" else -r["size"]) for r in rows)

        # Case B: tracked < exchange: bump the largest row to absorb missing units
        if abs(tracked_net) < abs(exch_sz) - EPSILON:
            missing = abs(exch_sz) - abs(tracked_net)
            tgt_side = "BUY" if exch_sz > 0 else "SELL"
            same_side_rows = sorted([r for r in rows if r["side"] == tgt_side], key=lambda r: r["size"], reverse=True)
            if same_side_rows:
                target = same_side_rows[0]
                new_size = target["size"] + missing
                col.update_one(
                    {"_id": target["_id"]},
                    {"$set": {"size": new_size, "entry_px": exch_px}},
                )
                actions.append(
                    f"  BUMP {coin} {target.get('wallet','?')[:12]} {target['side']} "
                    f"sz {target['size']:.6f}->{new_size:.6f}, entry->{exch_px}"
                )
            else:
                # No same-side row: create an __orphan__ entry
                col.insert_one({
                    "wallet": "__orphan__", "coin": coin, "side": tgt_side,
                    "entry_px": exch_px, "size": missing,
                    "entry_time": time.time(), "fill_time": time.time(),
                    "_force_exit": True, "_recovered": True,
                })
                actions.append(f"  ORPHAN-ADD {coin} sz={missing:.6f} {tgt_side} (no same-side row to bump)")

        # Update entry_px on surviving row(s) if they don't match exchange (volume-weighted)
        if rows and exch_px > 0:
            for r in rows:
                if abs(r.get("entry_px", 0) - exch_px) / max(exch_px, 1e-9) > 0.005:
                    col.update_one(
                        {"_id": r["_id"]},
                        {"$set": {"entry_px": exch_px}},
                    )
                    actions.append(
                        f"  ENTRY-PX {coin} {r.get('wallet','?')[:12]} {r['side']} "
                        f"{r['entry_px']:.4f}->{exch_px:.4f}"
                    )

    if not actions:
        print("No drift to heal.")
    else:
        print()
        print("Actions taken:")
        for a in actions:
            print(a)

    print()
    print("Post-heal DB state:")
    for doc in col.find().sort("coin", 1):
        print(f"  {doc['coin']:<20} {doc.get('wallet','?')[:14]:<14} {doc['side']:<4} sz={doc['size']} entry={doc.get('entry_px',0)}")


if __name__ == "__main__":
    main()
