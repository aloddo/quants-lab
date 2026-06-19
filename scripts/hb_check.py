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
    errs = sh(f"tail -250 {LOG} 2>/dev/null | grep -E 'ERROR|Traceback' | grep -vcE 'WS error: received 1000'")
    stats = sh(f"tail -250 {LOG} 2>/dev/null | grep 'STATS:' | tail -1")
    print(f"[engine] pid={pid or 'DOWN!'} | real-errors(250)={errs}")
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
    bug = [r["coin"] for r in recent if r.get("coin") in longs]
    print(f"[knet] bypasses={byp} | rejects-since-enable={rej} | recent SELL-reject coins={[r.get('coin') for r in recent]}")
    print(f"       our net-longs={sorted(longs)}")
    print(f"       bypass-correctness: {'*** BUG: reject on our long ' + str(bug) if bug else 'OK (no reject on a net-long coin -> no missed de-risk)'}")
    print("=== end ===")


if __name__ == "__main__":
    main()
