#!/usr/bin/env python3
"""
Wallet rotation monitor (READ-ONLY) for the Gate-1 copy probe.

Wires the pre-registered per-wallet kill thresholds to REAL Hyperliquid fills.
The eviction thresholds have lived only as a doc number in state; this makes
them measurable per-wallet so a breach is a MECHANICAL, auditable action.

Source of truth = HL exchange fills (Rule 8). Per-wallet attribution via the
engine's own OID->wallet map (v17_order_ids). Leader inactivity via each
leader's own userFillsByTime. NOTHING here mutates the live engine or config.

Pre-registered thresholds (projects/quant/copy/gate1-probe-kill-thresholds):
  Per-wallet EVICT if ANY:
    T1  cumulative realized copy PnL (net of fees) < -$30
    T2  >= 20 copied round-trips AND mean net per trip < 0
    T3  leader inactive (no leader fills) > 10 days
  Promote-from-bench is a RECOMMENDATION only (needs Alberto GO + codex).

Usage:
    python tools/wallet_rotation_monitor.py            # scorecard to stdout
    python tools/wallet_rotation_monitor.py --sync     # refresh our fills first
    python tools/wallet_rotation_monitor.py --leaders  # also probe leader activity (10 API calls)
    python tools/wallet_rotation_monitor.py --json      # machine-readable dump
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import pymongo
import requests

HL_API = "https://api.hyperliquid.xyz"
PARENT_ADDRESS = "0x11ca20aeb7cd014cf8406560ae405b12601994b4"
DB_NAME = "quants_lab"
FILLS_COLLECTION = "v17_exchange_fills"
OID_COLLECTION = "v17_order_ids"
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "copy_trader_wallets_gate1_v4.json")

# Gate-1 probe started ~2026-07-06 11:45 UTC (first real capital). Attribute only
# realized copy PnL from the live probe forward; earlier fills are prior probes.
PROBE_START_UTC = datetime(2026, 7, 6, 11, 45, 0, tzinfo=timezone.utc)
PROBE_START_MS = int(PROBE_START_UTC.timestamp() * 1000)

# Pre-registered per-wallet kill thresholds (LOCKED, do not tune here).
T1_CUM_PNL_USD = -30.0
T2_MIN_TRIPS = 20
T3_INACTIVE_DAYS = 10

# Bench (Copy A funnel, L3 full-pass / near-miss). Promote candidates only.
BENCH = {
    "0x552b6ad871f27a9729162c18d769050363f2d57e": "CopyA L3 full-pass (n=33, mir_med 47bps, $3.7k acct)",
}


def _db():
    return pymongo.MongoClient("mongodb://localhost:27017")[DB_NAME]


def load_cohort():
    with open(os.path.abspath(CONFIG_PATH)) as fh:
        cfg = json.load(fh)
    wallets = cfg["wallets"]
    return {a.lower(): (v.get("group", "") if isinstance(v, dict) else "") for a, v in wallets.items()}


def sync_fills(db, since_ms):
    """Pull recent fills from HL (read-only external) and upsert by tid."""
    try:
        fills = requests.post(f"{HL_API}/info", json={
            "type": "userFillsByTime", "user": PARENT_ADDRESS, "startTime": since_ms,
        }, timeout=15).json()
    except Exception as e:
        print(f"[sync] fetch failed: {e}", file=sys.stderr)
        return 0
    if not fills:
        return 0
    n = 0
    for f in fills:
        tid = f.get("tid")
        if not tid:
            continue
        doc = {k: f.get(k) for k in ("coin", "px", "sz", "side", "time", "dir",
                                     "closedPnl", "oid", "fee", "feeToken", "startPosition")}
        doc["tid"] = tid
        r = db[FILLS_COLLECTION].update_one({"tid": tid}, {"$set": doc}, upsert=True)
        if r.upserted_id:
            n += 1
    return n


def load_oid_wallet(db):
    """OID -> lowercased leader wallet."""
    m = {}
    for d in db[OID_COLLECTION].find({}, {"oid": 1, "wallet": 1}):
        w = (d.get("wallet") or "").lower()
        if w:
            m[d["oid"]] = w
    return m


def leader_last_fill_ms(wallet, now_ms=None):
    """Most recent LEADER fill time (ms) for inactivity test. None on error.

    BUGFIX 2026-07-09: the unbounded `userFills` endpoint returns FALSE-EMPTY for some active wallets
    (verified: 0x8c364082 was 3.1d-active but userFills came back []), and the old code mapped [] -> 0
    (epoch) -> ~20643d inactive -> a FALSE T3 EVICT. Use a BOUNDED `userFillsByTime` over a window a bit
    wider than the T3 bar. If fills land in-window -> real last-fill time. If genuinely empty over the whole
    window -> the leader is inactive for AT LEAST the window; return (now - window) as a sane floor so T3
    fires on a real number (>window d), never on epoch-0. Non-list/error -> None (skip T3, do not evict on noise)."""
    import time as _t
    if now_ms is None:
        now_ms = int(_t.time() * 1000)
    def _q(start):
        r = None
        for attempt in range(5):                     # retry on rate-limit/non-list (heavier endpoint throttles)
            try:
                r = requests.post(f"{HL_API}/info",
                                  json={"type": "userFillsByTime", "user": wallet, "startTime": start, "endTime": now_ms},
                                  timeout=15).json()
            except Exception:
                r = None
            if isinstance(r, list):
                return r
            _t.sleep(1.2 * (attempt + 1))            # backoff; do NOT silently skip -> a skip masks a real T3
        return None                                  # exhausted retries -> unknown

    # CAP-IMMUNE two-stage (bugfix 2026-07-09): userFillsByTime caps at 2000 fills; for an ultra-HF wallet a
    # wide window truncates and max(time) can be a stale page value (verified: 0xccf595 15d-query maxed at
    # 06-26 = 13.2d, but it actually traded 1.9h ago). Stage 1: short recent window (few fills, never capped)
    # -> if any fill, that IS the true recent activity. Stage 2: only for genuinely quiet wallets, widen to the
    # T3 window (an inactive wallet has < cap fills there too, so max is exact).
    recent = _q(now_ms - 2 * 86_400_000)             # last 2 days
    if recent is None:
        return None                                  # unknown, do NOT evict on noise
    if recent:
        return max(int(f.get("time", 0)) for f in recent)
    win_ms = (T3_INACTIVE_DAYS + 5) * 86_400_000     # 15d floor window for the 10d bar
    wide = _q(now_ms - win_ms)
    if wide is None:
        return None
    if not wide:
        return now_ms - win_ms                       # no fills in 15d -> inactive >= 15d floor (real T3, sane number)
    return max(int(f.get("time", 0)) for f in wide)


def compute(db, cohort, probe_start_ms, check_leaders=False):
    oid_wallet = load_oid_wallet(db)
    fills = list(db[FILLS_COLLECTION].find({"time": {"$gte": probe_start_ms}}).sort("time", 1))

    stats = {w: {"trips": 0, "wins": 0, "gross_pnl": 0.0, "fees": 0.0,
                 "net": 0.0, "last_fill_ms": 0, "unrealized_note": ""} for w in cohort}
    unattributed = {"trips": 0, "net": 0.0}

    for f in fills:
        pnl = float(f.get("closedPnl") or 0)
        fee = float(f.get("fee") or 0)
        t = int(f.get("time") or 0)
        w = oid_wallet.get(f.get("oid"))
        is_close = pnl != 0
        if w not in stats:
            if is_close:
                unattributed["trips"] += 1
                unattributed["net"] += pnl - fee
            continue
        s = stats[w]
        s["gross_pnl"] += pnl
        s["fees"] += fee
        s["net"] += pnl - fee
        s["last_fill_ms"] = max(s["last_fill_ms"], t)
        if is_close:
            s["trips"] += 1
            if pnl > 0:
                s["wins"] += 1

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    leader_last = {}
    if check_leaders:
        for w in cohort:
            leader_last[w] = leader_last_fill_ms(w, now_ms)
            time.sleep(0.8)  # gentle: shared IP rate budget (429 incident 2026-07-06)

    rows = []
    for w, s in stats.items():
        trips = s["trips"]
        mean_net = (s["net"] / trips) if trips else 0.0
        win_rate = (s["wins"] / trips) if trips else 0.0
        # Threshold evaluation
        breaches = []
        if s["net"] < T1_CUM_PNL_USD:
            breaches.append(f"T1 cum ${s['net']:.2f} < ${T1_CUM_PNL_USD}")
        if trips >= T2_MIN_TRIPS and mean_net < 0:
            breaches.append(f"T2 {trips} trips, mean ${mean_net:.3f} < 0")
        inactive_days = None
        if check_leaders and leader_last.get(w) is not None:
            inactive_days = (now_ms - leader_last[w]) / 86400000.0
            if inactive_days > T3_INACTIVE_DAYS:
                breaches.append(f"T3 leader inactive {inactive_days:.1f}d")
        rows.append({
            "wallet": w, "group": cohort[w], "trips": trips, "win_rate": round(win_rate, 3),
            "net": round(s["net"], 2), "mean_net": round(mean_net, 3),
            "dist_to_T1": round(s["net"] - T1_CUM_PNL_USD, 2),
            "inactive_days": (round(inactive_days, 1) if inactive_days is not None else None),
            "verdict": ("EVICT" if breaches else "HOLD"),
            "breaches": breaches,
        })
    rows.sort(key=lambda r: r["net"])
    return {"rows": rows, "unattributed": unattributed,
            "n_fills": len(fills), "probe_start_ms": probe_start_ms}


def render(result, check_leaders):
    rows = result["rows"]
    evict = [r for r in rows if r["verdict"] == "EVICT"]
    out = []
    out.append("=" * 78)
    out.append("WALLET ROTATION MONITOR (read-only) -- Gate-1 probe")
    out.append(f"probe fills attributed: {result['n_fills']} | "
               f"unattributed closes: {result['unattributed']['trips']} "
               f"(${result['unattributed']['net']:.2f})")
    out.append(f"thresholds: T1 cum<${T1_CUM_PNL_USD}  T2 >={T2_MIN_TRIPS} trips & mean<0  "
               f"T3 leader inactive>{T3_INACTIVE_DAYS}d{'' if check_leaders else '  [T3 NOT checked: pass --leaders]'}")
    out.append("-" * 78)
    hdr = f"{'wallet':14} {'grp':6} {'trips':>5} {'win%':>5} {'net$':>8} {'mean$':>7} {'dT1$':>7} {'inact':>6} verdict"
    out.append(hdr)
    for r in rows:
        inact = f"{r['inactive_days']:.1f}" if r["inactive_days"] is not None else "-"
        out.append(f"{r['wallet'][:12]:14} {r['group'][-6:]:6} {r['trips']:>5} "
                   f"{r['win_rate']*100:>4.0f}% {r['net']:>8.2f} {r['mean_net']:>7.3f} "
                   f"{r['dist_to_T1']:>7.2f} {inact:>6} {r['verdict']}"
                   + ("  <-- " + "; ".join(r["breaches"]) if r["breaches"] else ""))
    out.append("-" * 78)
    if evict:
        out.append(f"EVICT candidates: {len(evict)} -> {[r['wallet'][:10] for r in evict]}")
        out.append("BENCH (promote candidates, need Alberto GO + codex):")
        for a, why in BENCH.items():
            out.append(f"  {a[:12]}  {why}")
    else:
        out.append("No wallet breaches a locked threshold. Roster HOLD. No rotation action.")
    out.append("=" * 78)
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sync", action="store_true", help="refresh our fills from HL first")
    ap.add_argument("--leaders", action="store_true", help="probe leader activity (T3; 10 API calls)")
    ap.add_argument("--json", action="store_true", help="machine-readable dump")
    ap.add_argument("--since", help="override probe-start (YYYY-MM-DD)")
    args = ap.parse_args()

    db = _db()
    cohort = load_cohort()
    since_ms = PROBE_START_MS
    if args.since:
        since_ms = int(datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    if args.sync:
        n = sync_fills(db, since_ms)
        print(f"[sync] {n} new fills", file=sys.stderr)

    result = compute(db, cohort, since_ms, check_leaders=args.leaders)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(render(result, args.leaders))


if __name__ == "__main__":
    main()
