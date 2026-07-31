#!/usr/bin/env python
"""
cohort_health_monitor.py -- operational edge protection: the skill cohort is asof 2026-05-23; leaders go
DORMANT over time, silently shrinking the signal source and diluting the validated edge. This classifies all
100 cohort wallets each run:
  ACTIVE-captured  : engine observed a fill (v17_target_fills) within 24h  -> live signal contributor
  SLOW-captured    : observed fill 1-7d ago
  OFF-UNIVERSE     : alive on-chain (HL userFills <7d) but trades coins outside our whitelist -> no signal to us
  DORMANT          : no on-chain trade in >7d (HL userFills) -> dead weight -> cohort-refresh candidate
Flags the DORMANT set for the next cohort refresh (a refresh is a live change -> validation+codex+Alberto, not
done here). Read-only.

Run: ~/miniforge3/envs/quants-lab/bin/python scripts/cohort_health_monitor.py
"""
import argparse
import json
import time
from datetime import datetime, timezone
import requests
from pymongo import MongoClient

HL = "https://api.hyperliquid.xyz/info"
CFG = "config/copy_trader_wallets_v17_expansion.json"   # legacy default; pass --config for the live roster


def main():
    # 2026-07-31: the roster was hardcoded to the 2026-05-23 cohort, so running this against the LIVE
    # roster silently reported on wallets we no longer trade. Parameterised rather than rewritten.
    ap = argparse.ArgumentParser(description="cohort liveness / dormancy monitor (read-only)")
    ap.add_argument("--config", default=CFG, help="roster JSON to classify")
    args = ap.parse_args()
    db = MongoClient("mongodb://localhost:27017").quants_lab
    cfg = json.load(open(args.config))
    sk = list(cfg["wallets"].keys())
    whitelist = set(cfg.get("global", {}).get("coins", cfg.get("coins", [])) or [])
    now = datetime.now(timezone.utc).timestamp()

    # last captured fill per wallet (engine's observed leader flow on whitelisted coins)
    pipe = [{"$group": {"_id": {"$toLower": "$wallet"}, "last": {"$max": "$ts_epoch"}, "n": {"$sum": 1}}}]
    seen = {r["_id"]: r["last"] for r in db.v17_target_fills.aggregate(pipe)}
    unit = 1000.0 if (seen and max(seen.values()) > 1e12) else 1.0

    active = slow = off_uni = dormant = 0
    dormant_list = []
    for w in sk:
        wl = w.lower()
        if wl in seen:
            age_h = (now - seen[wl] / unit) / 3600
            if age_h < 24:
                active += 1; continue
            if age_h < 168:
                slow += 1; continue
        # not captured recently -> check on-chain liveness
        try:
            r = requests.post(HL, json={"type": "userFills", "user": w}, timeout=8).json()
            if isinstance(r, list) and r:
                last = max(f["time"] for f in r) / 1000
                age_d = (now - last) / 86400
                if age_d <= 7:
                    off_uni += 1
                else:
                    dormant += 1; dormant_list.append((w, round(age_d, 1)))
            else:
                dormant += 1; dormant_list.append((w, 999))
        except Exception:
            pass
        time.sleep(0.12)

    n = len(sk)
    print(f"=== cohort health ({n} wallets, {args.config}) {datetime.now(timezone.utc):%Y-%m-%d %H:%M}Z ===")
    print(f"  ACTIVE-captured (<24h):   {active}")
    print(f"  SLOW-captured (1-7d):     {slow}")
    print(f"  OFF-UNIVERSE (alive,off): {off_uni}")
    print(f"  DORMANT (>7d on-chain):   {dormant}")
    eff = active + slow
    print(f"\nEFFECTIVE signal contributors (captured <7d): {eff}/{n} ({eff/n*100:.0f}%)")
    print(f"DEAD WEIGHT (dormant): {dormant}/{n} ({dormant/n*100:.0f}%) -> cohort-refresh candidates")
    if dormant_list:
        print("DORMANT leaders (addr, days since last on-chain trade):")
        for w, d in sorted(dormant_list, key=lambda x: -x[1]):
            print(f"  {w} {d}d")
    # health verdict
    if dormant / n >= 0.15:
        print("\nVERDICT: >=15% dormant -> RECOMMEND cohort refresh (flag to Alberto; swap needs validation+codex).")
    elif dormant / n >= 0.08:
        print("\nVERDICT: 8-15% dormant -> WATCH; refresh worth scheduling. Edge intact for now.")
    else:
        print("\nVERDICT: <8% dormant -> cohort healthy, no refresh needed.")


if __name__ == "__main__":
    main()
