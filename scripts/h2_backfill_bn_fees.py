#!/usr/bin/env python3
"""
One-time migration: backfill missing Binance fees on old H2 arb trades.

Before the fee tracking fix (2026-04-23), Binance leg fees were recorded as 0
on all closed positions in `arb_h2_positions_v2`. This script queries the
Binance `/api/v3/myTrades` REST endpoint to recover the actual commission
for each entry and exit BN leg, then recomputes PnL accordingly.

Usage:
    # Dry run (default) — prints what would change, writes nothing
    python scripts/h2_backfill_bn_fees.py

    # Apply changes to MongoDB
    python scripts/h2_backfill_bn_fees.py --apply

Idempotent: positions whose BN fees are already nonzero are skipped.
"""
import argparse
import hashlib
import hmac
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlencode

import requests
from pymongo import MongoClient

# ── Load .env ───────────────────────────────────────────────────

def load_dotenv(path: str):
    """Minimal .env loader (no dependency on python-dotenv)."""
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip("'\"")
            os.environ.setdefault(key, val)

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(str(REPO_ROOT / ".env"))

BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
MONGO_DB = os.environ.get("MONGO_DATABASE", "quants_lab")

BINANCE_BASE = "https://api.binance.com"
COLLECTION = "arb_h2_positions_v2"

# ── Binance API helpers ─────────────────────────────────────────

def bn_sign(params: dict) -> str:
    qs = urlencode(params)
    return hmac.new(BINANCE_API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()


def bn_my_trades(symbol: str, start_ms: int, end_ms: int) -> list[dict]:
    """GET /api/v3/myTrades with time window."""
    params = {
        "symbol": symbol,
        "startTime": str(start_ms),
        "endTime": str(end_ms),
        "limit": "100",
        "timestamp": str(int(time.time() * 1000)),
        "recvWindow": "5000",
    }
    params["signature"] = bn_sign(params)
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    resp = requests.get(f"{BINANCE_BASE}/api/v3/myTrades", params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected myTrades response: {data}")
    return data


# ── Trade matching ──────────────────────────────────────────────

def expected_bn_side(direction: str, phase: str) -> bool:
    """
    Return expected isBuyer value for the BN leg.

    BUY_BB_SELL_BN:  entry BN = Sell (isBuyer=False), exit BN = Buy (isBuyer=True)
    SELL_BB_BUY_BN:  entry BN = Buy  (isBuyer=True),  exit BN = Sell (isBuyer=False)
    """
    if direction == "BUY_BB_SELL_BN":
        return phase == "exit"   # entry=Sell(False), exit=Buy(True)
    else:  # SELL_BB_BUY_BN
        return phase == "entry"  # entry=Buy(True), exit=Sell(False)


def match_trades(
    trades: list[dict],
    is_buyer_expected: bool,
    target_qty: float,
) -> list[dict]:
    """
    Filter trades by side match and aggregate qty.
    Accept trades whose cumulative qty is within 5% of target.
    """
    matched = []
    for t in trades:
        if t.get("isBuyer") != is_buyer_expected:
            continue
        matched.append(t)

    if not matched:
        return []

    total_qty = sum(float(t["qty"]) for t in matched)
    if target_qty > 0 and abs(total_qty - target_qty) / target_qty > 0.05:
        # Qty mismatch too large — could be a different trade in the window.
        # Try to find a subset that matches more closely.
        # Simple greedy: accumulate until within 5%.
        subset = []
        running = 0.0
        for t in sorted(matched, key=lambda x: int(x.get("time", 0))):
            subset.append(t)
            running += float(t["qty"])
            if abs(running - target_qty) / target_qty <= 0.05:
                return subset
        # If greedy didn't converge, return all matches anyway (best effort)
        return matched

    return matched


def sum_commission_usd(trades: list[dict]) -> tuple[float, str]:
    """Sum commission from matched trades, converting to USD.

    Binance fees can be denominated in:
    - USDC/USDT (quote assets) — already ~USD
    - Base asset (KERNEL, NOM, etc.) — convert at trade price
    - BNB (if BNB fee discount enabled) — convert at trade price * rough BNB rate

    Returns (total_fee_usd, primary_fee_asset).
    """
    if not trades:
        return 0.0, ""

    total_usd = 0.0
    primary_asset = ""
    for t in trades:
        commission = float(t.get("commission", 0))
        asset = t.get("commissionAsset", "")
        price = float(t.get("price", 0))

        if not primary_asset:
            primary_asset = asset

        if asset in ("USDC", "USDT", "BUSD", "USD"):
            total_usd += commission
        elif asset == "BNB":
            # BNB discount fees — use rough BNB price ($600-700 range)
            # For <$0.20 trades the BNB fee is negligible anyway
            total_usd += commission * 600.0
        else:
            # Base asset fee — convert at the trade's fill price
            total_usd += commission * price if price > 0 else 0.0

    return total_usd, primary_asset


# ── Main ────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backfill missing Binance fees on H2 positions")
    parser.add_argument("--apply", action="store_true", help="Write changes to MongoDB (default: dry run)")
    args = parser.parse_args()
    dry_run = not args.apply

    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        print("ERROR: BINANCE_API_KEY / BINANCE_API_SECRET not set in environment or .env")
        sys.exit(1)

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[MONGO_DB]
    coll = db[COLLECTION]

    # Find CLOSED positions where at least one BN fee is 0
    closed = list(coll.find({"state": "CLOSED"}))
    print(f"Total CLOSED positions: {len(closed)}\n")

    updated = 0
    skipped = 0
    errors = 0

    for pos in closed:
        pid = pos["position_id"]
        bn_symbol = pos.get("bn_symbol", "")
        direction = pos.get("direction", "")
        entry = pos.get("entry", {})
        exit_data = pos.get("exit", {})
        pnl = pos.get("pnl", {})

        entry_bn_fee = float(entry.get("bn", {}).get("fee", 0))
        exit_bn_fee = float(exit_data.get("bn", {}).get("fee", 0))

        if entry_bn_fee > 0 and exit_bn_fee > 0:
            skipped += 1
            continue

        if not bn_symbol:
            print(f"  SKIP {pid}: no bn_symbol")
            skipped += 1
            continue

        entry_time = pos.get("entry_time", 0)
        exit_time = pos.get("exit_time", 0)

        # Entry BN leg timing from submitted_at or entry_time
        entry_bn_submitted = float(entry.get("bn", {}).get("submitted_at", 0)) or entry_time
        exit_bn_submitted = float(exit_data.get("bn", {}).get("submitted_at", 0)) or exit_time

        entry_bn_qty = float(entry.get("bn", {}).get("filled_qty", 0))
        exit_bn_qty = float(exit_data.get("bn", {}).get("filled_qty", 0))

        print(f"{'='*70}")
        print(f"Position: {pid}")
        print(f"  bn_symbol={bn_symbol}  direction={direction}")
        print(f"  entry_time={entry_bn_submitted:.0f}  exit_time={exit_bn_submitted:.0f}")

        updates = {}
        new_entry_bn_fee = entry_bn_fee
        new_entry_bn_fee_asset = entry.get("bn", {}).get("fee_asset", "")
        new_exit_bn_fee = exit_bn_fee
        new_exit_bn_fee_asset = exit_data.get("bn", {}).get("fee_asset", "")

        # --- Backfill entry BN fee ---
        if entry_bn_fee == 0 and entry_bn_qty > 0 and entry_bn_submitted > 0:
            try:
                start_ms = int((entry_bn_submitted - 10) * 1000)
                end_ms = int((entry_bn_submitted + 120) * 1000)
                trades = bn_my_trades(bn_symbol, start_ms, end_ms)
                is_buyer = expected_bn_side(direction, "entry")
                matched = match_trades(trades, is_buyer, entry_bn_qty)
                fee, fee_asset = sum_commission_usd(matched)
                print(f"  ENTRY BN: {len(matched)} trades matched, fee={fee:.8f} {fee_asset}")
                if fee > 0:
                    new_entry_bn_fee = fee
                    new_entry_bn_fee_asset = fee_asset
                    updates["entry.bn.fee"] = fee
                    updates["entry.bn.fee_asset"] = fee_asset
                else:
                    print(f"  ENTRY BN: WARNING no fee found ({len(trades)} raw trades in window)")
            except Exception as e:
                print(f"  ENTRY BN: ERROR {e}")
                errors += 1
        else:
            print(f"  ENTRY BN: already has fee={entry_bn_fee:.8f} (skip)")

        # --- Backfill exit BN fee ---
        if exit_bn_fee == 0 and exit_bn_qty > 0 and exit_bn_submitted > 0:
            try:
                start_ms = int((exit_bn_submitted - 10) * 1000)
                end_ms = int((exit_bn_submitted + 120) * 1000)
                trades = bn_my_trades(bn_symbol, start_ms, end_ms)
                is_buyer = expected_bn_side(direction, "exit")
                matched = match_trades(trades, is_buyer, exit_bn_qty)
                fee, fee_asset = sum_commission_usd(matched)
                print(f"  EXIT BN:  {len(matched)} trades matched, fee={fee:.8f} {fee_asset}")
                if fee > 0:
                    new_exit_bn_fee = fee
                    new_exit_bn_fee_asset = fee_asset
                    updates["exit.bn.fee"] = fee
                    updates["exit.bn.fee_asset"] = fee_asset
                else:
                    print(f"  EXIT BN:  WARNING no fee found ({len(trades)} raw trades in window)")
            except Exception as e:
                print(f"  EXIT BN:  ERROR {e}")
                errors += 1

        # Also check for market escalation orders on exit —
        # only if we didn't already find exit fees above (avoid double-count)
        mkt_order_id = exit_data.get("bn", {}).get("market_order_id", "")
        if mkt_order_id and new_exit_bn_fee == 0:
            mkt_submitted = float(exit_data.get("bn", {}).get("market_submitted_at", 0))
            if mkt_submitted > 0:
                try:
                    start_ms = int((mkt_submitted - 10) * 1000)
                    end_ms = int((mkt_submitted + 120) * 1000)
                    trades = bn_my_trades(bn_symbol, start_ms, end_ms)
                    is_buyer = expected_bn_side(direction, "exit")
                    mkt_trades = [t for t in trades if t.get("isBuyer") == is_buyer and int(t.get("time", 0)) >= int(mkt_submitted * 1000) - 5000]
                    mkt_fee, mkt_asset = sum_commission_usd(mkt_trades)
                    if mkt_fee > 0:
                        print(f"  EXIT BN (mkt escalation): +${mkt_fee:.6f} from {len(mkt_trades)} trades")
                        new_exit_bn_fee = mkt_fee
                        new_exit_bn_fee_asset = mkt_asset
                        updates["exit.bn.fee"] = new_exit_bn_fee
                        updates["exit.bn.fee_asset"] = new_exit_bn_fee_asset
                except Exception as e:
                    print(f"  EXIT BN (mkt escalation): ERROR {e}")

        if not updates:
            print(f"  No changes needed")
            skipped += 1
            continue

        # --- Recompute PnL ---
        # Strategy: gross_usd = old net_usd + old total_fees.
        # Then new_net = gross_usd - new_total_fees.
        old_net_usd = float(pnl.get("net_usd", 0))
        old_bb_entry_fee = float(entry.get("bb", {}).get("fee", 0))
        old_bb_exit_fee = float(exit_data.get("bb", {}).get("fee", 0))
        old_bn_entry_fee = entry_bn_fee  # was 0
        old_bn_exit_fee = exit_bn_fee    # was 0
        old_total_fees = old_bb_entry_fee + old_bb_exit_fee + old_bn_entry_fee + old_bn_exit_fee

        gross_usd = old_net_usd + old_total_fees

        new_total_fees = old_bb_entry_fee + old_bb_exit_fee + new_entry_bn_fee + new_exit_bn_fee
        new_net_usd = gross_usd - new_total_fees

        # Compute bps from notional
        bb_ep = float(entry.get("bb", {}).get("avg_fill_price", 0))
        bb_eq = float(entry.get("bb", {}).get("filled_qty", 0))
        bn_ep = float(entry.get("bn", {}).get("avg_fill_price", 0))
        bn_eq = float(entry.get("bn", {}).get("filled_qty", 0))
        avg_notional = (bb_ep * bb_eq + bn_ep * bn_eq) / 2 if (bb_ep * bb_eq + bn_ep * bn_eq) > 0 else 1

        new_fees_bps = new_total_fees / avg_notional * 10000
        new_net_bps = new_net_usd / avg_notional * 10000
        # gross_bps stays the same (gross didn't change)

        updates["pnl.fees_bps"] = new_fees_bps
        updates["pnl.net_bps"] = new_net_bps
        updates["pnl.net_usd"] = new_net_usd

        # --- Report ---
        old_net_bps = float(pnl.get("net_bps", 0))
        old_fees_bps = float(pnl.get("fees_bps", 0))
        delta_fees = new_total_fees - old_total_fees
        delta_net = new_net_usd - old_net_usd

        print(f"  BEFORE: fees=${old_total_fees:.6f} ({old_fees_bps:.2f}bps)  net=${old_net_usd:.6f} ({old_net_bps:.2f}bps)")
        print(f"  AFTER:  fees=${new_total_fees:.6f} ({new_fees_bps:.2f}bps)  net=${new_net_usd:.6f} ({new_net_bps:.2f}bps)")
        print(f"  DELTA:  fees +${delta_fees:.6f}  net {delta_net:+.6f}")

        if dry_run:
            print(f"  [DRY RUN] Would update {len(updates)} fields")
        else:
            coll.update_one(
                {"position_id": pid, "state": "CLOSED"},
                {"$set": updates},
            )
            print(f"  [APPLIED] Updated {len(updates)} fields")
        updated += 1

        # Rate limit: Binance weight limit is 1200/min, myTrades costs 20 weight
        time.sleep(0.3)

    print(f"\n{'='*70}")
    print(f"Summary: {updated} updated, {skipped} skipped, {errors} errors")
    if dry_run:
        print("Run with --apply to write changes to MongoDB")


if __name__ == "__main__":
    main()
