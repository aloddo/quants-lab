#!/usr/bin/env python
"""
skill_granular_tracker.py -- DEEP granular live attribution of the skill cohort (Alberto 2026-06-15:
"instrument the deepest most granular tracking possible, by wallet, by coin"). Sourced from EXCHANGE TRUTH
(Rule 8): v17_exchange_fills (closedPnl per close) JOINED to v17_order_ids (oid -> leader wallet).

Outputs: overall skill stats; PER-WALLET (which leaders are profitable to copy live); PER-COIN; the worst
single trades (tail); liquidations. So as fills accumulate we see the STRUCTURE of the edge (is it 20% of
wallets / which coins drag) early, not just the aggregate.

Run: ~/miniforge3/envs/quants-lab/bin/python scripts/skill_granular_tracker.py
"""
import json
import statistics as st
from collections import defaultdict
from pymongo import MongoClient

BASELINE_BPS = 16.0
# RESET EPOCH (Alberto 2026-06-15): measure from THIS MORNING's engine change (gross-gate + trim
# bundle, commit d8ef6c6, engine READY 10:33:57 CEST = 08:33:57 UTC), NOT the 06-14 23:11 deploy.
RESET_MS = 1781512437000  # 2026-06-15 08:33:57 UTC


def main():
    db = MongoClient("mongodb://localhost:27017").quants_lab
    sk_cfg = json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"]
    sk = set(sk_cfg.keys())
    grp = {w: (m.get("skill_sharpe"), m.get("skill_win")) for w, m in sk_cfg.items()}
    oid_wallet = {d["oid"]: d.get("wallet") for d in db.v17_order_ids.find()}

    def coin_class(c):
        if c.startswith("xyz:"):
            return "xyz"
        return "major" if c in {"ADA", "AVAX", "BNB", "BTC", "CRV", "DOGE", "ETH", "HYPE", "LINK", "SOL"} else "alt"

    rows = []  # (wallet, coin, pnl_usd, notional, bps, is_liq, dir, klass)
    for f in db.v17_exchange_fills.find({"time": {"$gte": RESET_MS}}):
        pnl = float(f.get("closedPnl", 0) or 0)
        if abs(pnl) < 1e-6:
            continue   # not a close
        notional = abs(float(f.get("sz", 0) or 0) * float(f.get("px", 0) or 0))
        if notional < 1:
            continue
        wallet = oid_wallet.get(f.get("oid"))
        coin = f.get("coin")
        pdir = "LONG" if f.get("dir") == "Close Long" else ("SHORT" if f.get("dir") == "Close Short" else "?")
        rows.append((wallet, coin, pnl, notional, pnl / notional * 1e4,
                     bool(f.get("is_liquidation")), pdir, coin_class(coin)))

    print(f"=== SKILL GRANULAR TRACKER (exchange-truth closes since 2026-06-15 08:34 UTC reset: {len(rows)}) ===")
    if not rows:
        print("no closes yet."); return
    tot = sum(r[2] for r in rows)
    bps = [r[4] for r in rows]
    wins = sum(1 for r in rows if r[2] > 0)
    liqs = [r for r in rows if r[5]]
    print(f"OVERALL: n={len(rows)} sum=${tot:+.2f} | mean={st.mean(bps):+.0f}bps median={st.median(bps):+.0f}bps "
          f"| win={wins}/{len(rows)}={wins/len(rows)*100:.0f}% | liqs={len(liqs)} (${sum(r[2] for r in liqs):+.2f})")
    big = sorted([r for r in rows if r[4] < -300], key=lambda r: r[4])
    print(f"BIG LOSERS (<-300bps): {len(big)} -> " + ", ".join(f"{r[1]} {r[4]:+.0f}bps(${r[2]:+.1f})" for r in big[:6]))

    def agg(key_idx, label, only_skill=False):
        d = defaultdict(list)
        for r in rows:
            if only_skill and r[0] not in sk:
                continue
            k = r[key_idx]
            if k is None:
                k = "UNKNOWN"
            d[k].append(r)
        out = []
        for k, rs in d.items():
            p = sum(x[2] for x in rs)
            w = sum(1 for x in rs if x[2] > 0)
            out.append((k, len(rs), p, st.mean([x[4] for x in rs]), w / len(rs) * 100))
        return sorted(out, key=lambda x: x[2], reverse=True)

    print(f"\n=== PER-COIN (which coins win/lose live) ===")
    print(f"{'coin':<14}{'n':>4}{'sum$':>9}{'mean_bps':>10}{'win%':>7}")
    for k, n, p, mb, wr in agg(1, "coin"):
        print(f"{k:<14}{n:>4}{p:>9.2f}{mb:>10.0f}{wr:>7.0f}")

    print(f"\n=== PER-WALLET (leaders, SKILL cohort only; which are profitable to copy) ===")
    print(f"{'wallet':<14}{'n':>4}{'sum$':>9}{'mean_bps':>10}{'win%':>7}{'skill_sh':>9}")
    pw = agg(0, "wallet", only_skill=True)
    for k, n, p, mb, wr in pw[:10]:
        sh = grp.get(k, (None, None))[0]
        print(f"{(k[:12] if k!='UNKNOWN' else k):<14}{n:>4}{p:>9.2f}{mb:>10.0f}{wr:>7.0f}{(sh if sh else 0):>9.1f}")
    if len(pw) > 10:
        print("  ... worst:")
        for k, n, p, mb, wr in pw[-3:]:
            sh = grp.get(k, (None, None))[0]
            print(f"{k[:12]:<14}{n:>4}{p:>9.2f}{mb:>10.0f}{wr:>7.0f}{(sh if sh else 0):>9.1f}")
    print(f"\n  distinct skill wallets traded: {len(pw)} | non-skill(carried) closes: "
          f"{sum(1 for r in rows if r[0] not in sk)}")

    def split(idx, vals, label):
        print(f"\n=== {label} ===")
        print(f"{'bucket':<10}{'n':>4}{'sum$':>9}{'mean_bps':>10}{'win%':>7}")
        for v in vals:
            rs = [r for r in rows if r[idx] == v]
            if not rs:
                continue
            p = sum(x[2] for x in rs); w = sum(1 for x in rs if x[2] > 0)
            print(f"{v:<10}{len(rs):>4}{p:>9.2f}{st.mean([x[4] for x in rs]):>10.0f}{w/len(rs)*100:>7.0f}")

    split(6, ["LONG", "SHORT"], "LONG vs SHORT (the squeeze risk -- shorts in a rising market)")
    split(7, ["major", "alt", "xyz"], "COIN CLASS (major / liquid-alt / builder-xyz)")
    # long/short x class cross
    print(f"\n=== DIR x CLASS cross ===")
    print(f"{'bucket':<14}{'n':>4}{'sum$':>9}{'mean_bps':>10}{'win%':>7}")
    for d in ["LONG", "SHORT"]:
        for cl in ["major", "alt", "xyz"]:
            rs = [r for r in rows if r[6] == d and r[7] == cl]
            if not rs:
                continue
            p = sum(x[2] for x in rs); w = sum(1 for x in rs if x[2] > 0)
            print(f"{d+'/'+cl:<14}{len(rs):>4}{p:>9.2f}{st.mean([x[4] for x in rs]):>10.0f}{w/len(rs)*100:>7.0f}")
    print(f"\n(Baseline old PnL cohort ~{BASELINE_BPS:.0f}bps. Deepens as more fills land. Feeds the "
          f"reverse-engineering step -- Alberto 2026-06-15: validate -> extract the alpha -> own signal.)")


if __name__ == "__main__":
    main()
