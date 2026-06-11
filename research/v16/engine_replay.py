#!/usr/bin/env python3
"""V16 ENGINE-FAITHFUL OOS REPLAY -- the codex-required proof before first order.

Codex gate 2026-06-11: "Run a single end-to-end OOS/live-engine replay using the actual config."
This simulates the V16 RUNTIME (hl_copy_trader_v16.py wrapper + hl_copy_trader_v15.py engine), not the
idealized research replay, with EVERY parameter read from config/copy_trader_wallets_v16.json:

  ENTRY  leader 0->nonzero OPEN only (wrapper entry purity); $order_size fixed; 30s wallet-coin
         cooldown; 300s post-exit cooldown; chase guard (proxy: |mark-fillpx|); margin-util gate
         (gross/lev vs util_cap x equity); gross backstop; stop latch blocks entries.
  ADDS   tracked into leader position + accumulated notional, never copied (wrapper fix, codex #3).
  EXIT   FULL-CLOSE semantics (codex #2): leader reverse flow >= exit_min_trim_pct (0.85) of tracked
         accumulated notional -> we exit fully, taker, at mark(ts+latency) (codex #4: IOC, no maker).
  RISK   per-minute on 1-min marks: SL, trail (activate/giveback), max_hold; latched global stop on
         equity INCLUDing unrealized (flatten all, entries off); gross backstop (flatten, latch).
  PRICES execution_model entry/exit (slip both ways) + taker fee. Per-trade clip +-500bps on
         leader-close/max-hold exits only (validated convention); SL/trail/stop exits are raw.

Fold protocol identical to the validated procedure: select top-decile (cap 100) on TRAIN by trailing
faithful taker edge; replay TEST through the runtime sim. Both folds must show positive book PnL with
no stop trip and per-trade economics consistent with the overlay expectation (~+20bps median).

Run: python research/v16/engine_replay.py
"""
from __future__ import annotations
import json, sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parent.parent
sys.path.insert(0, str(_REPO / "research" / "v15"))
sys.path.insert(0, str(_HERE))

import leadlag_clean_rank_sim as S
from fidelity_replay import roundtrips
from execution_model import fee_rt, apply_entry, apply_exit, set_latency_ms
from _streaming_io import install_memory_guard
from select_cohort import load_wallet_fills, edge, LIQUID, CAP, MIN_COHORT, MAX_COHORT

CFG = json.load(open(_REPO / "config" / "copy_trader_wallets_v16.json"))
G, D = CFG["global"], CFG["defaults"]

# ---- ALL knobs from the shipped config (codex finding #1) ----
ORDER = float(G["order_size_usd"])
UTIL_CAP = float(G["max_margin_util"])
LEV = float(G["max_leverage_cap"])
STOP_PCT = float(G["global_stop_pct"])
BACKSTOP_X = float(G["gross_backstop_x"])
COOLDOWN_MS = int(G["cooldown_s"]) * 1000
CHASE_BPS = float(G["max_chase_bps"])
TRIM_PCT = float(G["exit_min_trim_pct"])
SL = float(D["sl_bps"])
TR_ACT = float(D["trail_activate_bps"])
TR_GIVE = float(D["trail_bps"])
MAX_HOLD_MS = int(D["max_hold_s"]) * 1000
POST_EXIT_CD_MS = 300_000           # engine constant (_post_exit_cooldown)
LAT = 2_000
FEE_T = fee_rt(maker=False)
EQUITY0 = 486.0
MINUTE = 60_000

FOLDS = [
    ("fold1", "2025-12-01", "2026-03-15", "2026-05-17"),
    ("fold2", "2025-12-15", "2026-04-15", "2026-05-23"),
]


class Book:
    def __init__(self):
        self.pos = {}            # (wallet,coin) -> dict(side, entry_px, entry_ts, peak_bps, coin)
        self.leader = defaultdict(float)      # (wallet,coin) -> leader signed size
        self.acc = defaultdict(float)         # (wallet,coin) -> leader accumulated notional (ours tracked)
        self.rev = defaultdict(float)         # (wallet,coin) -> leader reverse notional while we hold
        self.last_entry = {}                  # (wallet,coin) -> ts (cooldown)
        self.post_exit = {}                   # (wallet,coin) -> ts (300s)
        self.realized = 0.0
        self.stopped = False                  # latched global stop / backstop
        self.trades = []                      # per-trade net fractions (for bps stats)
        self.rejects = defaultdict(int)
        self.exits = defaultdict(int)
        self.max_coin_gross = defaultdict(float)

    def gross(self):
        return len(self.pos) * ORDER

    def equity(self, unreal):
        return EQUITY0 + self.realized + unreal


def unrealized(book, ts):
    u = 0.0
    for k, p in book.pos.items():
        m = S.mark_at(p["coin"], ts)
        if m and m > 0:
            d = 1 if p["side"] > 0 else -1
            u += d * (m - p["entry_px"]) / p["entry_px"] * ORDER
    return u


def close_pos(book, key, ts, reason, clip=True):
    p = book.pos.pop(key)
    book.rev.pop(key, None)
    book.acc.pop(key, None)
    m = S.mark_at(p["coin"], ts + LAT)
    if m is None or m <= 0:
        m = p["entry_px"]                     # degenerate; flat close
    exit_px = apply_exit(p["coin"], m, p["side"] > 0)
    g = p["side"] * (exit_px - p["entry_px"]) / p["entry_px"]
    if clip:
        g = max(-CAP, min(CAP, g))
    net = g - FEE_T
    book.realized += net * ORDER
    book.trades.append(net)
    book.exits[reason] += 1
    book.post_exit[key] = ts


def run_fold(fname, f_start, f_split, f_end, uni):
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    start, split, end = ms(f_start), ms(f_split), ms(f_end)
    print(f"\n=== {fname}: train {f_start}->{f_split} | TEST replay {f_split}->{f_end} ===")
    wf = load_wallet_fills(uni, start, end)

    # TRAIN selection (validated procedure)
    rows = []
    for w, fl in wf.items():
        fl.sort(key=lambda x: x[0])
        rts = roundtrips(fl)
        tr, trn, _ = edge(rts, start, split, LAT, FEE_T)
        ten = sum(1 for c, d_, e_, x_, *_ in rts if split <= e_ < end and c in LIQUID)
        if tr is not None and trn >= 15 and ten >= 15:
            rows.append({"wallet": w, "train_taker": tr})
    df = pd.DataFrame(rows).sort_values("train_taker", ascending=False).reset_index(drop=True)
    cohort = list(df.head(min(max(MIN_COHORT, len(df) // 10), MAX_COHORT)).wallet)
    print(f"  cohort {len(cohort)} of {len(df)} rankable")

    # merged TEST fill stream for the cohort (ts, wallet, coin, signed_size, price)
    stream = []
    for w in cohort:
        for t, c, ssz, px in wf[w]:
            if split <= t <= end and c in LIQUID and px and px > 0 and ssz:
                stream.append((t, w, c, float(ssz), float(px)))
    del wf
    stream.sort()
    print(f"  {len(stream)} cohort TEST fills on liquid majors")

    book = Book()
    eq_min = EQUITY0
    eq_path_day = {}
    next_risk_ts = stream[0][0] if stream else split

    def risk_pass(now):
        """Per-minute risk checks + equity/stop/backstop bookkeeping."""
        nonlocal eq_min
        # SL / trail / max_hold
        for key in list(book.pos.keys()):
            p = book.pos[key]
            m = S.mark_at(p["coin"], now)
            if m and m > 0:
                pnl = p["side"] * (m - p["entry_px"]) / p["entry_px"] * 1e4
                p["peak_bps"] = max(p["peak_bps"], pnl)
                if pnl <= SL:
                    close_pos(book, key, now, "sl", clip=False)
                    continue
                if p["peak_bps"] >= TR_ACT and (p["peak_bps"] - pnl) >= TR_GIVE:
                    close_pos(book, key, now, "trail", clip=False)
                    continue
            if now - p["entry_ts"] >= MAX_HOLD_MS:
                close_pos(book, key, now, "max_hold", clip=True)
        # equity incl unrealized -> latched stop
        u = unrealized(book, now)
        eq = book.equity(u)
        eq_min = min(eq_min, eq)
        day = (now // 86_400_000) * 86_400_000
        eq_path_day[day] = eq
        if not book.stopped and (eq - EQUITY0) <= -STOP_PCT * EQUITY0:
            book.stopped = True
            book.exits["GLOBAL_STOP_FLATTEN"] += len(book.pos)
            for key in list(book.pos.keys()):
                close_pos(book, key, now, "stop_flatten", clip=False)
        if not book.stopped and book.gross() > BACKSTOP_X * EQUITY0:
            book.stopped = True
            book.exits["BACKSTOP_FLATTEN"] += len(book.pos)
            for key in list(book.pos.keys()):
                close_pos(book, key, now, "backstop_flatten", clip=False)

    for ts, w, c, ssz, px in stream:
        while next_risk_ts <= ts:
            risk_pass(next_risk_ts)
            next_risk_ts += MINUTE
        key = (w, c)
        prev = book.leader[key]
        is_buy = ssz > 0
        same_dir = (prev > 0) == is_buy
        prev_notional = abs(prev) * px

        if prev_notional >= 1.0 and same_dir:
            # ADD: track, never copy (wrapper semantics)
            book.leader[key] = prev + ssz
            if key in book.pos:
                book.acc[key] += abs(ssz) * px
            continue

        if prev_notional >= 1.0 and not same_dir:
            # REVERSE: leader reducing/closing/flipping
            book.leader[key] = prev + ssz
            if key in book.pos:
                book.rev[key] += min(abs(ssz), abs(prev)) * px
                if book.acc[key] > 0 and book.rev[key] / book.acc[key] >= TRIM_PCT:
                    close_pos(book, key, ts, "leader_close", clip=True)
            continue

        # OPEN (leader ~flat -> nonzero). Flip residuals land here only if prev was ~dust.
        book.leader[key] = prev + ssz
        if book.stopped:
            book.rejects["stop_latched"] += 1
            continue
        if key in book.pos:
            book.rejects["already_holding"] += 1
            continue
        if ts - book.last_entry.get(key, -10**15) < COOLDOWN_MS:
            book.rejects["cooldown"] += 1
            continue
        if ts - book.post_exit.get(key, -10**15) < POST_EXIT_CD_MS:
            book.rejects["post_exit_cooldown"] += 1
            continue
        m = S.mark_at(c, ts)
        if m is None or m <= 0:
            book.rejects["no_mark"] += 1
            continue
        if abs(m - px) / px * 1e4 > CHASE_BPS:
            book.rejects["chase_guard"] += 1
            continue
        if (book.gross() + ORDER) / LEV > UTIL_CAP * book.equity(0):
            book.rejects["margin_util"] += 1
            continue
        if (book.gross() + ORDER) > BACKSTOP_X * EQUITY0:
            book.rejects["gross_backstop"] += 1
            continue
        ment = S.mark_at(c, ts + LAT)
        if ment is None or ment <= 0:
            book.rejects["no_mark"] += 1
            continue
        side = 1 if is_buy else -1
        book.pos[key] = {"coin": c, "side": side, "entry_px": apply_entry(c, ment, is_buy),
                         "entry_ts": ts, "peak_bps": 0.0}
        book.acc[key] = abs(ssz) * px
        book.rev[key] = 0.0
        book.last_entry[key] = ts
        coin_gross = sum(ORDER for k2 in book.pos if k2[1] == c)
        book.max_coin_gross[c] = max(book.max_coin_gross[c], coin_gross)

    # drain risk to end + close stragglers at end mark (mark-to-market, not a real exit)
    while next_risk_ts <= end:
        risk_pass(next_risk_ts)
        next_risk_ts += MINUTE
    for key in list(book.pos.keys()):
        close_pos(book, key, end, "eof_mtm", clip=True)

    tr = np.array(book.trades) * 1e4
    days = sorted(eq_path_day)
    eqs = np.array([eq_path_day[d] for d in days])
    dd = float(np.max(np.maximum.accumulate(eqs) - eqs)) if len(eqs) else 0.0
    res = {
        "fold": fname, "entries": len(book.trades), "book_pnl": book.realized,
        "trade_med_bps": float(np.median(tr)) if len(tr) else 0.0,
        "trade_mean_bps": float(np.mean(tr)) if len(tr) else 0.0,
        "max_dd": dd, "eq_min": eq_min, "stopped": book.stopped,
        "rejects": dict(book.rejects), "exits": dict(book.exits),
        "max_coin_gross": dict(sorted(book.max_coin_gross.items(), key=lambda kv: -kv[1])[:4]),
    }
    print(f"  ENTRIES {res['entries']} | book PnL ${res['book_pnl']:+.0f} | per-trade med {res['trade_med_bps']:+.1f} "
          f"mean {res['trade_mean_bps']:+.1f} bps | maxDD ${dd:.0f} ({dd/EQUITY0*100:.1f}%) | eq_min ${eq_min:.0f} "
          f"| STOPPED: {book.stopped}")
    print(f"  exits: {res['exits']}")
    print(f"  rejects: {res['rejects']}")
    print(f"  herd stress (max one-coin gross): {res['max_coin_gross']}")
    return res


def main():
    install_memory_guard(soft_gb=12.0, label="v16_engine_replay")
    set_latency_ms(LAT)
    print(f"SHIPPED CONFIG: order ${ORDER} | util {UTIL_CAP} lev {LEV} | stop {STOP_PCT} | backstop {BACKSTOP_X}x "
          f"| sl {SL} trail {TR_ACT}/{TR_GIVE} hold {MAX_HOLD_MS//3600000}h | trim_pct {TRIM_PCT} | chase {CHASE_BPS}bps")
    uni = set(l.strip().lower() for l in open(S._DATA / "m01_nonerroring_wallets.txt")
              if l.strip() and not l.startswith("#"))
    out = [run_fold(*f, uni) for f in FOLDS]
    pd.DataFrame(out).to_parquet(_REPO / "app" / "data" / "v16" / "engine_replay.parquet")
    ok = all(o["book_pnl"] > 0 and not o["stopped"] for o in out)
    print(f"\n=== VERDICT: {'PASS' if ok else 'FAIL'} (book PnL>0 + no stop trip, both folds) ===")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
