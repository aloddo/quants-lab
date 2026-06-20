#!/usr/bin/env python
"""
hb_check.py -- one-shot heartbeat health check for the quant-engineer cycle. Consolidates the per-HB checks
into a single call so nothing is skipped: (1) portfolio all venues (HL spot-only per rule 16 + Bybit), (2)
engine liveness + recent real errors (WS-reconnect noise filtered), (3) scaling verdict n + close-rate, (4)
knet bypass tally + LIVE-CORRECTNESS of recent SELL rejects (cross-checked vs our net-long book = exchange
truth, rule 8), (5) post-reset close count. Read-only; observe, do not touch the running engine.

Run: ~/miniforge3/envs/quants-lab/bin/python scripts/hb_check.py
"""
import subprocess
from datetime import datetime, timezone
from pymongo import MongoClient
import requests

PARENT = "0x11ca20aeb7cd014cf8406560ae405b12601994b4"
HL = "https://api.hyperliquid.xyz/info"
LOG = "/tmp/ql-v12-copy-trader-launchd.log"
PY = "/Users/hermes/miniforge3/envs/quants-lab/bin/python"
ENABLE = datetime(2026, 6, 19, 7, 27, 24, tzinfo=timezone.utc)


def sh(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=60).stdout.strip()
    except Exception as e:
        return f"(err {e})"


def main():
    print(f"=== HB CHECK {datetime.now(timezone.utc):%Y-%m-%d %H:%M}Z ===")
    # 1. portfolio (canonical script, rule 16)
    print("[portfolio]", sh(f"cd ~/quants-lab && set -a && source .env 2>/dev/null && set +a && {PY} tools/portfolio_snapshot.py 2>/dev/null | tail -1"))
    # 2. engine
    pid = sh("pgrep -f 'hl_copy_trader_v17.py --config' | head -1")
    # categorize errors: HL-infra (external, transient: WS 502/504/gateway/timeout/handshake/sync-fail) vs
    # ENGINE (internal -> investigate). WS 1000 (normal close) already excluded.
    allerr = sh(f"tail -250 {LOG} 2>/dev/null | grep -E 'ERROR|Traceback' | grep -vE 'WS error: received 1000'")
    lines = [l for l in allerr.splitlines() if l.strip()]
    infra = sum(1 for l in lines if any(k in l for k in ("HTTP 50", "Gateway Timeout", "timed out", "no close frame", "sync failed", "server rejected", "Connection reset", "Temporary failure")))
    engine_err = len(lines) - infra
    stats = sh(f"tail -250 {LOG} 2>/dev/null | grep 'STATS:' | tail -1")
    flag = "" if engine_err == 0 else "  <-- INVESTIGATE"
    print(f"[engine] pid={pid or 'DOWN!'} | errors(250): engine={engine_err}{flag} | HL-infra(transient)={infra}")
    if stats:
        print("        ", stats.split("] ", 1)[-1][:110])
    # 3. verdict
    print("[verdict]", sh(f"cd ~/quants-lab && {PY} scripts/scaling_verdict_computer.py 2>/dev/null | grep -E 'closes n=|close-rate' | tr '\\n' ' | '"))
    # 4. knet bypass + live-correctness of recent SELL rejects
    db = MongoClient("mongodb://localhost:27017").quants_lab
    byp = db.v17_gate_log.count_documents({"action": "knet_derisk_bypass"})
    rej = db.v17_gate_log.count_documents({"action": "rejected", "ts": {"$gte": ENABLE}})
    recent = list(db.v17_gate_log.find({"action": "rejected", "side": "SELL", "ts": {"$gte": ENABLE}}).sort("ts", -1).limit(8))
    longs = set()
    try:
        for dex in ["", "xyz"]:
            r = requests.post(HL, json={"type": "clearinghouseState", "user": PARENT, **({"dex": dex} if dex else {})}, timeout=8).json()
            for ap in r.get("assetPositions", []):
                p = ap["position"]
                if float(p["szi"]) > 0:
                    longs.add(p["coin"])
    except Exception:
        pass
    # point-in-time guard: only flag a reject as a possible bypass-miss if it is RECENT (<=15min), so the
    # CURRENT net-long book is a valid proxy for the position at reject time. Older rejects on a coin we only
    # went long AFTERWARDS are NOT bugs (the 2026-06-20 HYPE false-positive: rejects 06-19, long opened 06-20).
    nowdt = datetime.now(timezone.utc)
    bug = [r["coin"] for r in recent if r.get("coin") in longs
           and (nowdt - r["ts"].replace(tzinfo=timezone.utc)).total_seconds() <= 900]
    print(f"[knet] bypasses={byp} | rejects-since-enable={rej} | recent SELL-reject coins={[r.get('coin') for r in recent]}")
    print(f"       our net-longs={sorted(longs)}")
    print(f"       bypass-correctness: {'*** CHECK: recent(<=15min) reject on our long ' + str(bug) + ' -- verify point-in-time' if bug else 'OK (no recent reject on a net-long coin)'}")
    print("=== end ===")


if __name__ == "__main__":
    main()
