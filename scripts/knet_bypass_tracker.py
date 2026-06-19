#!/usr/bin/env python
"""
knet_bypass_tracker.py -- live instrumentation for the knet de-risk bypass (enabled 2026-06-19 09:27 CEST,
Alberto msg 9750). Every time the engine lets a knet-blocked de-risking SHORT through it writes
v17_gate_log{action:'knet_derisk_bypass', coin, knet, coin_net, ts}. This ties each bypass to the SHORT
round-trip it produced (nearest-following Open Short fill on that coin) and the realized PnL booked when that
short closes (closedPnl on Close Short fills, coin-level FIFO after the bypass). Reports live count + realized
edge_bps / win-rate and BENCHMARKS them against the validated backtest (+91 bps, 87% win, n=2782) so we catch
live-vs-validation divergence early.

Read-only. Run: ~/miniforge3/envs/quants-lab/bin/python scripts/knet_bypass_tracker.py
"""
from datetime import datetime, timezone
from pymongo import MongoClient

ENABLE_MS = 1781854044000     # 2026-06-19 09:27:24 CEST / 07:27:24 UTC (engine restart that turned bypass on)
BENCH_EDGE = 91.0; BENCH_WIN = 0.87; BENCH_N = 2782
RT = 11.0


def main():
    db = MongoClient("mongodb://localhost:27017").quants_lab
    byp = list(db.v17_gate_log.find({"action": "knet_derisk_bypass"}).sort("ts", 1))
    print(f"=== knet de-risk bypass tracker (since enable 2026-06-19 09:27 CEST) ===")
    print(f"backtest benchmark: edge +{BENCH_EDGE:.0f}bps, win {BENCH_WIN*100:.0f}%, n={BENCH_N}\n")
    if not byp:
        # also surface that the gate is live + how many knet REJECTS still happen (context)
        rej = db.v17_gate_log.count_documents({"action": "rejected", "ts": {"$gte": datetime.fromtimestamp(ENABLE_MS/1000, tz=timezone.utc)}})
        print(f"0 bypasses so far -- gate is ARMED. knet rejects since enable: {rej} "
              f"(each is a herd-unconfirmed short that did NOT de-risk our book -> correctly still blocked).")
        print("Bypasses are occasional (need a leader to short a coin we are net-LONG on). Check back each HB.")
        return

    rows = []
    for b in byp:
        coin = b["coin"]; ts = b["ts"]
        t0 = int(ts.timestamp() * 1000) if hasattr(ts, "timestamp") else int(ts)
        # nearest-following Open Short fill on that coin (the short the bypass produced)
        op = db.v17_exchange_fills.find_one(
            {"coin": coin, "dir": "Open Short", "time": {"$gte": t0 - 2000, "$lte": t0 + 180000}},
            sort=[("time", 1)])
        if not op:
            rows.append((coin, t0, None, None, "no-open-fill")); continue
        entry_px = float(op["px"]); entry_sz = float(op["sz"]); entry_t = int(op["time"])
        # realized PnL: sum closedPnl of Close Short fills on that coin AFTER entry (coin-level FIFO approx),
        # capped to the size opened (so a later unrelated short on the same coin is not double-counted).
        closes = db.v17_exchange_fills.find(
            {"coin": coin, "dir": "Close Short", "time": {"$gte": entry_t}}, sort=[("time", 1)])
        rem = entry_sz; pnl = 0.0; closed = 0.0
        for c in closes:
            csz = min(float(c["sz"]), rem)
            if csz <= 0:
                break
            pnl += float(c.get("closedPnl", 0) or 0) * (csz / float(c["sz"]) if float(c["sz"]) else 0)
            closed += csz; rem -= csz
            if rem <= 1e-9:
                break
        notional = entry_sz * entry_px
        if closed <= 0 or notional <= 0:
            rows.append((coin, t0, entry_px, None, "open")); continue
        edge_bps = pnl / notional * 1e4 - RT     # net of RT cost
        rows.append((coin, t0, entry_px, edge_bps, "closed"))

    print(f"{'coin':<10}{'when':<17}{'status':<9}{'edge_bps':>9}")
    closed_edges = []
    for coin, t0, epx, edge, status in rows:
        when = datetime.fromtimestamp(t0/1000, tz=timezone.utc).strftime("%m-%d %H:%M")
        e = f"{edge:+.0f}" if edge is not None else "-"
        print(f"{coin:<10}{when:<17}{status:<9}{e:>9}")
        if status == "closed":
            closed_edges.append(edge)

    print(f"\nbypasses: {len(byp)} | closed round-trips: {len(closed_edges)}")
    if closed_edges:
        import statistics as st
        m = st.mean(closed_edges); win = sum(1 for e in closed_edges if e > 0) / len(closed_edges)
        print(f"LIVE realized: edge {m:+.0f}bps, win {win*100:.0f}% (n={len(closed_edges)})")
        print(f"BENCHMARK    : edge +{BENCH_EDGE:.0f}bps, win {BENCH_WIN*100:.0f}% (n={BENCH_N})")
        if len(closed_edges) >= 10:
            div = abs(m - BENCH_EDGE)
            flag = "ALIGNED" if div < 60 and abs(win - BENCH_WIN) < 0.2 else "DIVERGENCE -- investigate"
            print(f"VERDICT (n>=10): {flag} (edge gap {div:.0f}bps)")
        else:
            print("(need n>=10 closed bypasses for a live-vs-backtest verdict)")


if __name__ == "__main__":
    main()
