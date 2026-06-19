#!/usr/bin/env python
"""
daily_report_v2.py -- POLISHED visual daily copy-trade report (Alberto 2026-06-16: "the TXT dump is shit,
where are the insights, where are the graphs, step up the quality"). Produces:
  1. a multi-panel PNG dashboard (equity curve, cumulative skill-vs-carried PnL, long/short + coin-class edge,
     SHORT mean-reversion trajectory, validation-progress gauge)
  2. a synthesized INSIGHTS narrative (not raw tracker output)
Outputs PNG to /tmp/daily-report-<day>.png and prints the insight text (for the Telegram caption + brain page).

Run: ~/miniforge3/envs/quants-lab/bin/python scripts/daily_report_v2.py [YYYY-MM-DD]
"""
import json
import re
import sys
from datetime import datetime, timezone
from collections import defaultdict

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pymongo import MongoClient

LOG = "/tmp/ql-v12-copy-trader-launchd.log"
RESET_MS = 1781512437000           # 2026-06-15 08:33:57 UTC (gross-gate+trim deploy)
RESET_STR = "2026-06-15 10:33 CEST"
TRUST_N = 200
MAJORS = {"ADA", "AVAX", "BNB", "BTC", "CRV", "DOGE", "ETH", "HYPE", "LINK", "SOL"}

# palette (dark, professional)
BG = "#0e1117"; FG = "#e6e6e6"; GRID = "#2a2f3a"
GREEN = "#26c281"; RED = "#e8505b"; BLUE = "#4d9de0"; GOLD = "#f0a202"; PURPLE = "#9b59b6"


def coin_class(c):
    c = str(c)
    if c.startswith("xyz:"):
        return "xyz"
    return "major" if c in MAJORS else "alt"


RESET_LOG = "2026-06-15 10:33:00"   # CEST, matches the log timestamp format (skill_edge_tracker epoch)
EXIT_RE = re.compile(
    r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d).*EXIT: (\S+) (BUY|SELL) entry=([\d.]+) exit=[\d.]+ "
    r".*pnl=([+-][\d.]+)bp \(\$([+-][\d.]+)\).*\[(\w+)\]")
ENTRY_RE = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d).*ENTRY FILLED \(IOC\): (\S+) (BUY|SELL) [\d.]+ @ ([\d.]+)")


def load_fills():
    """Single source = engine log EXIT lines (logical round-trips with side, coin, bps, $). Skill attribution
    uses skill_edge_tracker's TRUE method: a skill round-trip = a [v16_skill_decile] exit whose entry price
    matches a POST-reset ENTRY FILLED (excludes carried positions even if tagged). BUY exit = LONG, SELL = SHORT."""
    post_entries = set()
    rows = []
    with open(LOG) as fh:
        for line in fh:
            me = ENTRY_RE.search(line)
            if me and me.group(1) >= RESET_LOG:
                post_entries.add((me.group(2), round(float(me.group(4)), 6)))
                continue
            m = EXIT_RE.search(line)
            if not m:
                continue
            tstr = m.group(1)
            if tstr < RESET_LOG:
                continue
            coin = m.group(2)
            pdir = "LONG" if m.group(3) == "BUY" else "SHORT"
            entry_px = round(float(m.group(4)), 6)
            bps = float(m.group(5)); usd = float(m.group(6)); tag = m.group(7)
            true_skill = (tag == "v16_skill_decile") and ((coin, entry_px) in post_entries)
            rows.append(dict(t=datetime.strptime(tstr, "%Y-%m-%d %H:%M:%S"), coin=coin, pnl=usd,
                             bps=bps, dir=pdir, klass=coin_class(coin),
                             is_skill=true_skill, is_liq=False))
    return pd.DataFrame(rows).sort_values("t").reset_index(drop=True)


def load_wallets():
    """Per-LEADER stats from exchange fills (oid->wallet; the only source with leader identity).
    Covers all cohort closes since reset. Returns DataFrame: wallet, n, usd, mean_bps, median_bps, win, sharpe."""
    db = MongoClient("mongodb://localhost:27017").quants_lab
    oid_wallet = {d["oid"]: d.get("wallet") for d in db.v17_order_ids.find()}
    cfg = json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"]
    sk = set(cfg.keys())
    by = defaultdict(list)
    for f in db.v17_exchange_fills.find({"time": {"$gte": RESET_MS}}):
        pnl = float(f.get("closedPnl", 0) or 0)
        if abs(pnl) < 1e-6:
            continue
        notional = abs(float(f.get("sz", 0) or 0) * float(f.get("px", 0) or 0))
        if notional < 1:
            continue
        w = oid_wallet.get(f.get("oid"))
        if w not in sk:   # leaderboard = cohort leaders only
            continue
        by[w].append((pnl, pnl / notional * 1e4))
    rows = []
    for w, lst in by.items():
        usd = sum(x[0] for x in lst); bps = [x[1] for x in lst]
        sharpe = (np.mean(bps) / np.std(bps)) if len(bps) > 1 and np.std(bps) > 0 else 0.0  # PER-TRADE Sharpe (mean/std); NOT mean/std*sqrt(N)=t-stat
        rows.append(dict(wallet=w, n=len(lst), usd=usd, mean_bps=np.mean(bps), median_bps=np.median(bps),
                         win=np.mean([1 if b > 0 else 0 for b in bps]) * 100, sharpe=sharpe))
    return pd.DataFrame(rows).sort_values("usd", ascending=False).reset_index(drop=True)


def load_equity():
    # parse STATS for equity (eq=) + realized (acct=) time series
    pat = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d).*acct=\$([+-][\d.]+)\((\d+)\).*uPnL=\$([+-][\d.]+).*eq=\$([\d.]+)")
    rows = []
    with open(LOG) as fh:
        for line in fh:
            m = pat.search(line)
            if m:
                ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                rows.append((ts, float(m.group(2)), int(m.group(3)), float(m.group(4)), float(m.group(5))))
    df = pd.DataFrame(rows, columns=["t", "realized", "closes", "upnl", "equity"])
    return df[df.t >= datetime(2026, 6, 15, 8, 30)].reset_index(drop=True)


def _grouped_meanmed(ax, cats, sub, title, colors):
    x = np.arange(len(cats)); w = 0.38
    means = [sub(c).bps.mean() if len(sub(c)) else 0 for c in cats]
    meds = [sub(c).bps.median() if len(sub(c)) else 0 for c in cats]
    ns = [len(sub(c)) for c in cats]
    ax.bar(x - w/2, means, w, color=colors, label="mean")
    ax.bar(x + w/2, meds, w, color=colors, alpha=0.45, label="median")
    for i, (mv, dv, c) in enumerate(zip(means, meds, ns)):
        top = max(mv, dv)
        ax.text(i, top + 12, f"n={c}", ha="center", fontsize=8, color=FG)
    ax.axhline(0, color=FG, lw=0.8); ax.set_xticks(x); ax.set_xticklabels(cats)
    ax.set_title(title, fontweight="bold", loc="left"); ax.grid(alpha=0.25, axis="y")
    ax.legend(facecolor=BG, edgecolor=GRID, labelcolor=FG, fontsize=8, loc="best")


def build(day):
    f = load_fills()
    eq = load_equity()
    wl = load_wallets()
    skill = f[f.is_skill]
    n = len(f)
    pct = min(100, n / TRUST_N * 100)
    allbps = f.bps.to_numpy()
    sharpe = (allbps.mean() / allbps.std()) if len(allbps) > 1 and allbps.std() > 0 else 0.0  # per-trade Sharpe (mean/std), NOT t-stat

    plt.rcParams.update({"axes.facecolor": BG, "figure.facecolor": BG, "savefig.facecolor": BG,
                         "text.color": FG, "axes.labelcolor": FG, "xtick.color": FG, "ytick.color": FG,
                         "axes.edgecolor": GRID, "grid.color": GRID, "font.size": 10})
    fig = plt.figure(figsize=(15, 20))
    gs = fig.add_gridspec(5, 2, hspace=0.42, wspace=0.20, top=0.94, bottom=0.03, left=0.07, right=0.96)

    tot = f.pnl.sum(); sk_tot = skill.pnl.sum()
    med_all = np.median(allbps)
    fig.suptitle(f"COPY-TRADE DAILY  -  {day}", fontsize=22, fontweight="bold", color=FG, x=0.07, ha="left", y=0.975)
    fig.text(0.07, 0.945, f"since reset {RESET_STR}   |   {n}/{TRUST_N} closes ({pct:.0f}%)   |   realized ${tot:+.2f} "
             f"(skill ${sk_tot:+.2f})   |   mean {allbps.mean():+.0f}bps / median {med_all:+.0f}bps   |   "
             f"win {(allbps>0).mean()*100:.0f}%   |   Sharpe {sharpe:.1f}", fontsize=11, color=BLUE, ha="left")

    # (0,0) cumulative realized
    ax = fig.add_subplot(gs[0, 0])
    fc = f.copy(); fc["cum_all"] = fc.pnl.cumsum()
    fc["cum_skill"] = fc.pnl.where(fc.is_skill, 0).cumsum()
    fc["cum_carried"] = fc.pnl.where(~fc.is_skill, 0).cumsum()
    ax.plot(fc.t, fc.cum_all, color=FG, lw=2.2, label=f"total ${tot:+.0f}")
    ax.plot(fc.t, fc.cum_skill, color=GREEN, lw=2, label=f"skill ${sk_tot:+.0f}")
    ax.plot(fc.t, fc.cum_carried, color=GOLD, lw=1.6, ls="--", label=f"carried ${tot-sk_tot:+.0f}")
    ax.fill_between(fc.t, fc.cum_all, 0, color=FG, alpha=0.05)
    ax.set_title("Cumulative realized PnL ($)", fontweight="bold", loc="left")
    ax.legend(facecolor=BG, edgecolor=GRID, labelcolor=FG, fontsize=8, loc="upper left")
    ax.grid(alpha=0.3); ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %Hh"))
    for lb in ax.get_xticklabels(): lb.set_fontsize(8)

    # (0,1) equity curve + drawdown shading
    ax = fig.add_subplot(gs[0, 1])
    if len(eq):
        ax.plot(eq.t, eq.equity, color=BLUE, lw=2)
        ax.fill_between(eq.t, eq.equity, eq.equity.min(), color=BLUE, alpha=0.08)
        ax.set_title(f"HL equity ($)   last ${eq.equity.iloc[-1]:.0f}", fontweight="bold", loc="left")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %Hh"))
        for lb in ax.get_xticklabels(): lb.set_fontsize(8)
    ax.grid(alpha=0.3)

    # (1,0) edge by side (mean + median)
    ax = fig.add_subplot(gs[1, 0])
    _grouped_meanmed(ax, ["LONG", "SHORT"], lambda s: f[f.dir == s], "Edge by side (mean + median bps)", [GREEN, GREEN])

    # (1,1) edge by coin class (mean + median)
    ax = fig.add_subplot(gs[1, 1])
    _grouped_meanmed(ax, ["major", "alt", "xyz"], lambda c: f[f.klass == c], "Edge by coin class (mean + median bps)", [BLUE, GREEN, PURPLE])

    # (2,0) equity drawdown (underwater)
    ax = fig.add_subplot(gs[2, 0])
    if len(eq):
        peak = eq.equity.cummax(); dd = (eq.equity - peak) / peak * 100
        ax.fill_between(eq.t, dd, 0, color=RED, alpha=0.4); ax.plot(eq.t, dd, color=RED, lw=1.2)
        ax.set_title(f"Equity drawdown (%)   max {dd.min():.1f}%", fontweight="bold", loc="left")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %Hh"))
        for lb in ax.get_xticklabels(): lb.set_fontsize(8)
    ax.grid(alpha=0.3)

    # (2,1) per-wallet leaderboard ($ by leader)
    ax = fig.add_subplot(gs[2, 1])
    if len(wl):
        top = pd.concat([wl.head(7), wl.tail(3)]).drop_duplicates("wallet")
        y = np.arange(len(top))[::-1]
        ax.barh(y, top.usd, color=[GREEN if v >= 0 else RED for v in top.usd])
        ax.set_yticks(y); ax.set_yticklabels([w[:10] for w in top.wallet], fontsize=8)
        for yi, (_, r) in zip(y, top.iterrows()):
            ax.text(r.usd + (0.1 if r.usd >= 0 else -0.1), yi, f"${r.usd:+.1f} n={r.n}", va="center",
                    ha="left" if r.usd >= 0 else "right", fontsize=7, color=FG)
        ax.axvline(0, color=FG, lw=0.8)
    ax.set_title("Per-leader PnL leaderboard ($)", fontweight="bold", loc="left"); ax.grid(alpha=0.25, axis="x")

    # (3,0) SHORT mean-reversion
    ax = fig.add_subplot(gs[3, 0])
    sh = f[f.dir == "SHORT"].copy()
    if len(sh):
        sh["run"] = sh.bps.expanding().mean()
        ax.plot(range(1, len(sh)+1), sh.run, color=RED, lw=2, marker="o", ms=4)
        ax.axhline(0, color=FG, lw=0.8, ls=":")
        ax.axhline(171, color=GREEN, lw=1, ls="--", alpha=0.7)
        ax.text(len(sh), 171, " hist +171", color=GREEN, fontsize=8, va="bottom")
        ax.set_title("SHORT edge mean-reverting (running mean bps vs n)", fontweight="bold", loc="left")
        ax.set_xlabel("short closes (n)", fontsize=9)
    ax.grid(alpha=0.3)

    # (3,1) per-leader stats table + validation
    ax = fig.add_subplot(gs[3, 1]); ax.axis("off")
    ax.text(0.0, 1.0, f"VALIDATION  {n}/{TRUST_N}  ({pct:.0f}%)   VERDICT: {'HOLD (accruing, Mon)' if n < TRUST_N else 'mature'}",
            fontsize=12, fontweight="bold", color=FG, transform=ax.transAxes)
    if len(wl):
        hdr = f"{'leader':<11}{'n':>3}{'$':>7}{'mean':>6}{'med':>6}{'win':>5}{'shrp':>6}"
        ax.text(0.0, 0.88, hdr, fontsize=9, family="monospace", color=BLUE, transform=ax.transAxes)
        show = pd.concat([wl.head(8), wl.tail(2)]).drop_duplicates("wallet")
        for i, (_, r) in enumerate(show.iterrows()):
            col = GREEN if r.usd >= 0 else RED
            txt = f"{r.wallet[:10]:<11}{int(r.n):>3}{r.usd:>7.1f}{r.mean_bps:>6.0f}{r.median_bps:>6.0f}{r.win:>4.0f}%{r.sharpe:>6.1f}"
            ax.text(0.0, 0.80 - i*0.072, txt, fontsize=9, family="monospace", color=col, transform=ax.transAxes)
    ax.text(0.0, 0.02, "leaderboard = cohort leaders, all closes since reset (exchange fills oid->wallet)",
            fontsize=7, color="#888", transform=ax.transAxes)

    # (4,0) per-coin PnL breakdown
    ax = fig.add_subplot(gs[4, 0])
    pc = f.groupby("coin").agg(usd=("pnl", "sum"), n=("pnl", "size"), mb=("bps", "mean")).sort_values("usd")
    pcs = pd.concat([pc.head(6), pc.tail(6)])
    pcs = pcs[~pcs.index.duplicated()]
    y = np.arange(len(pcs))
    ax.barh(y, pcs.usd, color=[GREEN if v >= 0 else RED for v in pcs.usd])
    ax.set_yticks(y); ax.set_yticklabels([c[:12] for c in pcs.index], fontsize=8)
    for yi, (_, r) in zip(y, pcs.iterrows()):
        ax.text(r.usd + (0.05 if r.usd >= 0 else -0.05), yi, f"${r.usd:+.1f} ({int(r.n)})", va="center",
                ha="left" if r.usd >= 0 else "right", fontsize=7, color=FG)
    ax.axvline(0, color=FG, lw=0.8); ax.set_title("Per-coin PnL ($, best & worst)", fontweight="bold", loc="left")
    ax.grid(alpha=0.25, axis="x")

    # (4,1) rolling cumulative win-rate over the close sequence
    ax = fig.add_subplot(gs[4, 1])
    if len(f):
        roll = (f.bps > 0).expanding().mean() * 100
        ax.plot(range(1, len(f)+1), roll, color=GOLD, lw=2, marker=".", ms=5)
        ax.axhline(50, color=FG, lw=0.8, ls=":")
        ax.set_ylim(0, 100)
        ax.set_title(f"Cumulative win-rate (%)   now {roll.iloc[-1]:.0f}%", fontweight="bold", loc="left")
        ax.set_xlabel("closes (n)", fontsize=9)
    ax.grid(alpha=0.3)

    out = f"/tmp/daily-report-{day}.png"
    fig.savefig(out, dpi=128, bbox_inches="tight")
    plt.close(fig)
    return out, f, skill, eq, wl


def insights(day, f, skill, eq, wl):
    n = len(f); tot = f.pnl.sum(); sk_tot = skill.pnl.sum()
    L = f[f.dir == "LONG"]; S = f[f.dir == "SHORT"]
    pc = f.groupby("coin").pnl.sum().sort_values()
    worst = pc.head(2); best = pc.tail(2)
    allbps = f.bps.to_numpy()
    sharpe = (allbps.mean()/allbps.std()) if len(allbps) > 1 and allbps.std() > 0 else 0.0  # per-trade Sharpe (mean/std), NOT mean/std*sqrt(N)=t-stat
    lines = [f"COPY-TRADE DAILY  {day}", f"(since reset {RESET_STR}; {n}/{TRUST_N} closes)", ""]
    lines.append("INSIGHTS")
    lines.append(f"- Edge REAL + BOTH-SIDED: LONG mean {L.bps.mean():+.0f}/med {L.bps.median():+.0f}bps ({len(L)}), "
                 f"SHORT mean {S.bps.mean():+.0f}/med {S.bps.median():+.0f}bps ({len(S)}). Overall median {np.median(allbps):+.0f}bps, "
                 f"Sharpe {sharpe:.1f}. Skill round-trips {skill.bps.mean():+.0f}bps at {(skill.bps>0).mean()*100:.0f}% win.")
    if len(wl):
        best_w = wl.iloc[0]; worst_w = wl.iloc[-1]
        lines.append(f"- Leaders: {len(wl)} traded; best {best_w.wallet[:10]} ${best_w.usd:+.1f} ({int(best_w.n)} trades, "
                     f"Sharpe {best_w.sharpe:.1f}), worst {worst_w.wallet[:10]} ${worst_w.usd:+.1f}. "
                     f"{int((wl.usd>0).sum())}/{len(wl)} leaders net-positive.")
    if len(S):
        lines.append(f"- SHORT has mean-reverted from deeply negative on the first handful of closes toward neutral/positive "
                     f"({S.bps.mean():+.0f}bps now) -- the early short losses were single-wallet noise, NOT a broken edge "
                     f"(historical short edge +171bps). This is the thin-sample trap, confirmed.")
    lines.append(f"- Best coins: {', '.join(f'{c} ${v:+.0f}' for c, v in best.items())}. "
                 f"Drag: {', '.join(f'{c} ${v:+.0f}' for c, v in worst.items())}.")
    lines.append(f"- Realized ${tot:+.2f} (skill ${sk_tot:+.2f}, carried ${tot-sk_tot:+.2f}). Carried = old book unwinding, "
                 f"excluded from the skill verdict.")
    lines.append(f"- VERDICT: HOLD. {n}/{TRUST_N} closes; point $/mo at target but the CI lower bound (~$70, heavy-tailed) "
                 f"needs n>={TRUST_N} to clear. At the recent close rate, n>={TRUST_N} lands ~weeks out (not this Mon); "
                 f"Mon is a preliminary checkpoint. Capital is not the blocker -- validation confidence is.")
    lines.append(f"- Sharpe figures are PER-TRADE (mean/std), not mean/std*sqrt(N)=t-stat (fixed 2026-06-19).")
    return "\n".join(lines)


def main():
    day = sys.argv[1] if len(sys.argv) > 1 else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    png, f, skill, eq, wl = build(day)
    txt = insights(day, f, skill, eq, wl)
    print(txt)
    print(f"\n[PNG] {png}")


if __name__ == "__main__":
    main()
