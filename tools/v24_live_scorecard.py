#!/usr/bin/env python3
"""Live copy scorecard (v24->v25 probe) -- proves/disproves the OOS +15bps two-sided NET edge on real fills.

Parses closed-trip EXIT fill lines from the bot log since go-live, and MERGES them into a durable
append-only ledger (dedup by trip key) so trips survive log rotation / /tmp clears / bot restarts.
Stats are computed from the UNION (ledger + current log). The bot log lives in /tmp (no rotation,
but /tmp is cleared on reboot), and reaching n>=30 takes days -- the ledger protects the proof.

Run: python tools/v24_live_scorecard.py
"""
import re, sys, os, csv
LOG = "/tmp/ql-v12-copy-trader-launchd.log"
GOLIVE = "2026-07-01 19:57"  # v24 go-live
LEDGER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app", "data", "live", "copy_scorecard_ledger.csv")
FEE_RT_BPS = 8.64  # canonical HL round-trip TAKER fee (FEE_RT=0.000864); instant entry + IOC exit = taker both legs

# EXIT pnl bps = raw entry/exit PRICE delta = GROSS of fees. OOS +15bps is NET. Subtract RT taker to
# compare live on the SAME basis as OOS.
EXIT_RE = re.compile(r'EXIT: ((?:xyz:)?[A-Za-z0-9]+) (BUY|SELL) entry=([\d.]+) exit=([\d.]+) filled=\S+ pnl=([-+][\d.]+)bp \(\$([-+][\d.]+)\)')

def parse_log():
    """Yield (ts, coin, pos, bp, usd) for each EXIT line since go-live."""
    if not os.path.exists(LOG):
        return
    for line in open(LOG, errors="ignore"):
        if line[:16] < GOLIVE:
            continue
        m = EXIT_RE.search(line)
        if not m:
            continue
        ts = line[:19]  # 'YYYY-MM-DD HH:MM:SS'
        coin, side = m.group(1), m.group(2)
        bp, usd = float(m.group(5)), float(m.group(6))
        # exit-line side encodes POSITION direction (BUY=long, SELL=short); verified against pnl sign.
        pos = "LONG" if side == "BUY" else "SHORT"
        yield (ts, coin, pos, bp, usd)

def key(r):
    return f"{r[0]}|{r[1]}|{r[2]}|{r[3]:.4f}"

def load_ledger():
    trips = {}
    if os.path.exists(LEDGER):
        with open(LEDGER) as f:
            for row in csv.reader(f):
                if not row or row[0] == "ts":
                    continue
                r = (row[0], row[1], row[2], float(row[3]), float(row[4]))
                trips[key(r)] = r
    return trips

def save_ledger(trips):
    os.makedirs(os.path.dirname(LEDGER), exist_ok=True)
    rows = sorted(trips.values(), key=lambda r: r[0])
    with open(LEDGER, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "coin", "pos", "gross_bps", "usd"])
        for r in rows:
            w.writerow([r[0], r[1], r[2], f"{r[3]:.4f}", f"{r[4]:.4f}"])

# merge ledger + fresh log parse (union, dedup)
trips = load_ledger()
n_before = len(trips)
for r in parse_log():
    trips[key(r)] = r
new = len(trips) - n_before
save_ledger(trips)

rows = sorted(trips.values(), key=lambda r: r[0])
if not rows:
    print("no closed live trips yet since go-live"); sys.exit()

import statistics as st
gross = [r[3] for r in rows]; net = [r[3] - FEE_RT_BPS for r in rows]; usd = [r[4] for r in rows]
longs = [r for r in rows if r[2] == "LONG"]; shorts = [r for r in rows if r[2] == "SHORT"]
wins_net = sum(1 for b in net if b > 0)
print(f"=== LIVE COPY SCORECARD (since {GOLIVE}, v24->v25) ===")
print(f"ledger: {LEDGER}  (+{new} new this run, {len(rows)} total durable)")
print(f"closed trips: {len(rows)} | net wins: {wins_net} ({100*wins_net/len(rows):.0f}%) | cumulative gross: ${sum(usd):+.2f}")
print(f"avg GROSS/trade: {st.mean(gross):+.1f}bps | avg NET/trade (RT taker -{FEE_RT_BPS}bps): {st.mean(net):+.1f}bps  [OOS expectation +15bps NET]")
print(f"median NET: {st.median(net):+.1f}bps")
print(f"LONG trips: {len(longs)} avg net {st.mean([r[3]-FEE_RT_BPS for r in longs]):+.1f}bps" if longs else "LONG trips: 0")
print(f"SHORT trips: {len(shorts)} avg net {st.mean([r[3]-FEE_RT_BPS for r in shorts]):+.1f}bps" if shorts else "SHORT trips: 0")
print(f"two-sided: {'YES' if longs and shorts else 'not yet'} | coins: {sorted(set(r[1] for r in rows))}")
sig = f"n={len(rows)}<30 -> NOT yet statistically significant; keep accumulating" if len(rows) < 30 else f"n={len(rows)}>=30 -> sample usable"
print(f"significance: {sig}")
print("\nlast 10 trips (net):")
for r in rows[-10:]:
    print(f"  {r[1]:>8} {r[2]:<5} {r[3]-FEE_RT_BPS:+6.1f}bp net  ${r[4]:+.3f} gross  [{r[0]}]")
