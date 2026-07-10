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

import os
_CFG_PATH = os.environ.get("V16_REPLAY_CONFIG", str(_REPO / "config" / "copy_trader_wallets_v16.json"))
CFG = json.load(open(_CFG_PATH))
G, D = CFG["global"], CFG["defaults"]
print(f"[replay] config: {_CFG_PATH}")

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
_kg = os.environ.get("V16_REPLAY_KNET_MIN")
KNET_MIN = int(_kg) if _kg not in (None, "") else None   # sprint: net-consensus entry gate
_nx = os.environ.get("V16_REPLAY_NETX_CAP")
NETX_CAP = float(_nx) if _nx not in (None, "") else None  # sprint: |net book exposure| cap (x equity)
_cs = os.environ.get("V16_REPLAY_COINSIDE_CAP")
COINSIDE_CAP = float(_cs) if _cs not in (None, "") else None  # sprint: per coin-side gross cap (x equity)
# sprint H51: regime-conditional exits. Entries taken at knet >= this threshold IGNORE the
# leader-close signal and ride to max_hold (72h via config) / trail / SL. None = off.
_rx = os.environ.get("V16_REPLAY_RIDE_KNET")
RIDE_KNET = int(_rx) if _rx not in (None, "") else None
RIDE_HOLD_MS = int(os.environ.get("V16_REPLAY_RIDE_HOLD_H", "72")) * 3_600_000
# sprint 2026-06-11: FUNDING ACCRUAL (hole flagged by codex + the V18 scout: 30-70h holds span
# many hourly funding intervals). Loads hyperliquid_funding_rates once; accrues hourly in
# risk_pass: long pays positive funding, short earns it. V16_REPLAY_FUNDING=1 to enable.
FUNDING_ON = os.environ.get("V16_REPLAY_FUNDING", "") == "1"
_FUNDING = {}   # coin -> (ts_sec_array, rate_array)
# H-NB-FILTER card (agent G, 2026-06-11): per-trade log + raw-cohort net-book regime filter.
# ALL default-off; with these envs unset behavior is byte-identical to the shipped replay.
TRADE_LOG = os.environ.get("V16_REPLAY_TRADE_LOG")   # parquet path: one row per closed trade
_TRADE_ROWS: list = []
NB_FILTER = os.environ.get("V16_REPLAY_NB_FILTER")   # block_opposed | block_opposed_neutral
NB_SERIES = os.environ.get("V16_REPLAY_NB_SERIES",
                           str(_REPO / "app" / "data" / "v18" / "nb_pct_series.parquet"))
OUT_PQ = os.environ.get("V16_REPLAY_OUT")            # optional result-parquet redirect
if NB_FILTER not in (None, "", "block_opposed", "block_opposed_neutral"):
    raise SystemExit(f"bad V16_REPLAY_NB_FILTER={NB_FILTER!r}")
_NB = {}   # (fold, coin) -> (hour_ts_i64, pct_f64), hour-grid causal NB percentile

# ---- AGENT N (2026-06-12): H17 cohort filter + E1 anti-crowd entry filter ----
# Backtest the 2 OOS-validated copy filters as cohort/entry conditioning. ALL default-off;
# with every env below unset the run is byte-identical to the shipped replay (regression-proven).
#
# E1 (anti-crowd, agent M / L21, OOS +52bps): ENTRY filter. At OPEN, count cohort wallets
# currently net-OPPOSITE our side on this coin (excl self + dust) -- the live-faithful k_opp.
# Reject (or down-weight) the entry when k_opp > E1_KOPP_MAX. Agent M favored bucket = k_opp<=3.
#   V16_REPLAY_E1_KOPP_MAX = int (e.g. "3"); default off.
#   V16_REPLAY_E1_MODE     = "drop" (skip entry) | "tilt" (allow but ORDER*tilt sizing); default drop.
#   V16_REPLAY_E1_TILT     = float weight for tilt mode on non-favored entries (default 0.5).
_e1 = os.environ.get("V16_REPLAY_E1_KOPP_MAX")
E1_KOPP_MAX = int(_e1) if _e1 not in (None, "") else None
E1_MODE = os.environ.get("V16_REPLAY_E1_MODE", "drop")
E1_TILT = float(os.environ.get("V16_REPLAY_E1_TILT", "0.5"))
if E1_MODE not in ("drop", "tilt"):
    raise SystemExit(f"bad V16_REPLAY_E1_MODE={E1_MODE!r}")
#
# H17 (keep-T1, agent A/D, PASS both folds): COHORT filter. Tercile the SELECTED replay cohort
# by crossed_share computed from each fold's TRAIN-window leader fills (agent D method, exact),
# then keep / down-weight by tercile. Rank-tilt preferred over hard-drop to preserve cohort size.
#   V16_REPLAY_H17 = "keep_t1" | "drop_t3" | "tilt"; default off.
#     keep_t1  -> cohort = T1 only (lowest crossed_share); hard.
#     drop_t3  -> cohort = T1+T2 (drop highest-crossed tercile); hard.
#     tilt     -> keep full cohort; size ORDER by tercile weight (T1=1.0, T2=H17_T2, T3=H17_T3).
#   V16_REPLAY_H17_T2 / _T3 = tilt weights (defaults 0.66 / 0.33).
H17 = os.environ.get("V16_REPLAY_H17")
H17_T2 = float(os.environ.get("V16_REPLAY_H17_T2", "0.66"))
H17_T3 = float(os.environ.get("V16_REPLAY_H17_T3", "0.33"))
if H17 not in (None, "", "keep_t1", "drop_t3", "tilt"):
    raise SystemExit(f"bad V16_REPLAY_H17={H17!r}")
H17_FILLS_DIR = _REPO / "app" / "data" / "hl_s3_fills_v2"
# fold -> (train_d0, train_d1) inclusive day strings; mirrors agent D WINDOWS (TRAIN only).
_H17_TRAIN_DAYS = {
    "fold1": ("20251201", "20260314"),
    "fold2": ("20251215", "20260414"),
    # forward (OOS) fold: 90d pre-forward train window (agent N deployable-H17, no look-ahead).
    "forward": ("20260222", "20260522"),
}
_H17_TERCILE: dict = {}   # (fold, wallet) -> "T1"|"T2"|"T3"


def _h17_terciles(fname, cohort):
    """Per-fold crossed_share terciles for the SELECTED replay cohort, computed from that
    fold's TRAIN-window leader fills ONLY (agent D method: n_crossed/n_fills, rank+qcut(3))."""
    from collections import Counter
    d0, d1 = _H17_TRAIN_DAYS[fname]
    cset = set(cohort)
    n_fills, n_crossed = Counter(), Counter()
    days = sorted(f for f in os.listdir(H17_FILLS_DIR)
                  if f.endswith(".parquet") and d0 <= f[:8] <= d1)
    for fn in days:
        df = pd.read_parquet(H17_FILLS_DIR / fn, columns=["wallet", "crossed"],
                             filters=[("wallet", "in", list(cset))])
        if len(df):
            n_fills.update(df["wallet"].value_counts().to_dict())
            n_crossed.update(df.loc[df["crossed"].astype(bool), "wallet"].value_counts().to_dict())
    share = {w: n_crossed.get(w, 0) / n_fills[w] for w in n_fills if n_fills[w] > 0}
    feat = pd.Series(share)
    r = feat.rank(method="average")
    codes = pd.qcut(r, 3, labels=False, duplicates="drop")
    terc = {w: f"T{int(c) + 1}" for w, c in codes.items()}
    for w in cohort:                       # wallets w/o TRAIN fills -> treat as T3 (most aggressive prior)
        _H17_TERCILE[(fname, w)] = terc.get(w, "T3")
    from collections import Counter as _C
    comp = _C(terc.get(w, "T3") for w in cohort)
    print(f"  [H17 {fname}] cohort terciles: {dict(comp)} (mode={H17}, "
          f"with TRAIN fills: {len(share)}/{len(cohort)})")


def _entry_size(fname, w, k_opp):
    """Per-entry order size. Default = ORDER (baseline). H17 tilt scales by crossed-share
    tercile; E1 tilt scales non-favored (high-k_opp) entries. Multiplicative."""
    sz = ORDER
    if H17 == "tilt":
        t = _H17_TERCILE.get((fname, w), "T3")
        sz *= 1.0 if t == "T1" else (H17_T2 if t == "T2" else H17_T3)
    if E1_KOPP_MAX is not None and E1_MODE == "tilt" and k_opp is not None and k_opp > E1_KOPP_MAX:
        sz *= E1_TILT
    return sz


def _load_nb():
    if not NB_FILTER or _NB:
        return
    df = pd.read_parquet(NB_SERIES)
    for (f, c), g in df.groupby(["fold", "coin"]):
        g = g.sort_values("hour_ts")
        _NB[(f, c)] = (g["hour_ts"].values.astype(np.int64),
                       g["pct"].values.astype(np.float64))
    print(f"[replay] NB filter '{NB_FILTER}': {NB_SERIES} ({len(df)} rows, {len(_NB)} fold-coin keys)")


def _nb_bucket(fold, coin, ts, is_buy):
    """Bucket vs the last COMPLETED-hour NB percentile STRICTLY before ts (causal asof).
    ALIGNED: pct>=0.6 long / pct<=0.4 short; OPPOSED: pct<=0.4 long / pct>=0.6 short;
    NEUTRAL between. no_coverage when the asof hour is absent, stale (>1h), or NaN."""
    s = _NB.get((fold, coin))
    if s is None:
        return "no_coverage", float("nan")
    import bisect
    i = bisect.bisect_left(s[0], ts) - 1          # largest hour_ts < ts (ts==hour excluded)
    if i < 0 or ts - int(s[0][i]) > 3_600_000:
        return "no_coverage", float("nan")
    p = float(s[1][i])
    if not np.isfinite(p):
        return "no_coverage", float("nan")
    if is_buy:
        b = "ALIGNED" if p >= 0.6 else ("OPPOSED" if p <= 0.4 else "NEUTRAL")
    else:
        b = "ALIGNED" if p <= 0.4 else ("OPPOSED" if p >= 0.6 else "NEUTRAL")
    return b, p

def _load_funding(coins):
    if not FUNDING_ON or _FUNDING:
        return
    from pymongo import MongoClient
    import numpy as _np
    db = MongoClient("mongodb://localhost:27017")["quants_lab"]
    for c in coins:
        rows = list(db["hyperliquid_funding_rates"].find(
            {"coin": c}, {"timestamp_utc": 1, "funding_rate": 1, "_id": 0}).sort("timestamp_utc", 1))
        if rows:
            ts = _np.array([r["timestamp_utc"] for r in rows], dtype="float64") / 1000.0
            rt = _np.array([r["funding_rate"] for r in rows], dtype="float64")
            _FUNDING[c] = (ts, rt)
    print(f"[replay] funding loaded for {len(_FUNDING)} coins")

def _funding_rate(coin, ts_ms):
    f = _FUNDING.get(coin)
    if f is None:
        return 0.0
    import bisect
    i = bisect.bisect_right(f[0], ts_ms / 1000.0) - 1
    return float(f[1][i]) if i >= 0 else 0.0

FOLDS = [
    ("fold1", "2025-12-01", "2026-03-15", "2026-05-17"),
    ("fold2", "2025-12-15", "2026-04-15", "2026-05-23"),
]
# sprint 2026-06-11: custom fold override, e.g. "forward,2025-12-01,2026-05-23,2026-06-10"
_fo = os.environ.get("V16_REPLAY_FOLDS")
if _fo:
    FOLDS = [tuple(part.split(",")) for part in _fo.split(";")]


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
        return sum(p.get("size", ORDER) for p in self.pos.values())

    def equity(self, unreal):
        return EQUITY0 + self.realized + unreal


def unrealized(book, ts):
    u = 0.0
    for k, p in book.pos.items():
        m = S.mark_at(p["coin"], ts)
        if m and m > 0:
            d = 1 if p["side"] > 0 else -1
            u += d * (m - p["entry_px"]) / p["entry_px"] * p.get("size", ORDER)
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
    book.realized += net * p.get("size", ORDER)
    book.trades.append(net)
    if TRADE_LOG:   # H-NB-FILTER step 1: observational, default-off
        _TRADE_ROWS.append(dict(fold=book.fold, coin=p["coin"], dir=p["side"],
                                entry_ts_ms=int(p["entry_ts"]), exit_ts_ms=int(ts),
                                net_bps=net * 1e4, exit_reason=reason))
    book.exits[reason] += 1
    book.post_exit[key] = ts


def run_fold(fname, f_start, f_split, f_end, uni):
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    start, split, end = ms(f_start), ms(f_split), ms(f_end)
    print(f"\n=== {fname}: train {f_start}->{f_split} | TEST replay {f_split}->{f_end} ===")
    _load_funding(list(LIQUID))
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
    _topn = int(os.environ.get("V16_REPLAY_TOPN", "0"))   # sprint 2026-06-11: cohort-depth variant
    if _topn > 0:
        cohort = list(df.head(_topn).wallet)
    else:
        cohort = list(df.head(min(max(MIN_COHORT, len(df) // 10), MAX_COHORT)).wallet)
    print(f"  cohort {len(cohort)} of {len(df)} rankable (topn_override={_topn or 'off'})")

    # AGENT N H17 cohort filter: tercile selected cohort by TRAIN crossed_share (agent D method).
    if H17:
        _h17_terciles(fname, cohort)
        if H17 == "keep_t1":
            cohort = [w for w in cohort if _H17_TERCILE[(fname, w)] == "T1"]
        elif H17 == "drop_t3":
            cohort = [w for w in cohort if _H17_TERCILE[(fname, w)] in ("T1", "T2")]
        # "tilt" keeps the full cohort; sizing applied per-entry via _h17_weight.
        print(f"  [H17 {fname}] cohort after '{H17}': {len(cohort)} wallets")

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
    book.fold = fname                 # H-NB-FILTER: fold label for trade log / NB asof
    book.nb_counts = defaultdict(int)  # H-NB-FILTER: gate-eval bucket composition
    eq_min = EQUITY0
    eq_path_day = {}
    next_risk_ts = stream[0][0] if stream else split
    # codex round-2 telemetry: gross distribution + cluster maxima (sampled per risk minute)
    tele = {"gross": [], "max_coin_side": 0.0, "max_all_long": 0.0, "max_all_short": 0.0}

    last_funding_hour = [0]

    def risk_pass(now):
        """Per-minute risk checks + equity/stop/backstop bookkeeping."""
        nonlocal eq_min
        # hourly funding accrual on open positions (long pays positive rate, short earns)
        if FUNDING_ON:
            hour = now // 3_600_000
            if hour > last_funding_hour[0]:
                last_funding_hour[0] = hour
                for key2, p2 in book.pos.items():
                    fr = _funding_rate(p2["coin"], now)
                    if fr:
                        _sz2 = p2.get("size", ORDER)
                        book.realized -= p2["side"] * fr * _sz2
                        book.funding_paid = getattr(book, "funding_paid", 0.0) + p2["side"] * fr * _sz2
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
            _cap = RIDE_HOLD_MS if p.get("ride") else MAX_HOLD_MS
            if now - p["entry_ts"] >= _cap:
                close_pos(book, key, now, "max_hold", clip=True)
        # codex telemetry: gross + clusters at this minute
        g_now = book.gross()
        tele["gross"].append(g_now)
        cs = defaultdict(float)
        n_long = n_short = 0
        for (w2, c2), p2 in book.pos.items():
            cs[(c2, p2["side"])] += ORDER
            if p2["side"] > 0:
                n_long += 1
            else:
                n_short += 1
        if cs:
            tele["max_coin_side"] = max(tele["max_coin_side"], max(cs.values()))
        tele["max_all_long"] = max(tele["max_all_long"], n_long * ORDER)
        tele["max_all_short"] = max(tele["max_all_short"], n_short * ORDER)
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
                    if book.pos[key].get("ride"):
                        book.exits["ride_ignored_leader_close"] = book.exits.get(
                            "ride_ignored_leader_close", 0) + 1
                    else:
                        close_pos(book, key, ts, "leader_close", clip=True)
            continue

        # OPEN (leader ~flat -> nonzero). Flip residuals land here only if prev was ~dust.
        book.leader[key] = prev + ssz
        if book.stopped:
            book.rejects["stop_latched"] += 1
            continue
        k = None
        if KNET_MIN is not None or RIDE_KNET is not None:
            # sprint 2026-06-11: net-consensus gate. knet = (# cohort wallets net-long this coin
            # minus net-short), sign-aligned with OUR side, EXCLUDING this leader. Live equivalent:
            # the engine's unconditional leader tracker (+ userState seed at boot).
            k = 0
            for (w2, c2), sz2 in book.leader.items():
                if c2 != c or w2 == w or abs(sz2) * px < 1.0:
                    continue
                k += 1 if (sz2 > 0) == is_buy else -1
            if KNET_MIN is not None and k < KNET_MIN:
                book.rejects["knet_gate"] += 1
                continue
        # AGENT N E1 anti-crowd filter: k_opp = # cohort wallets currently net-OPPOSITE our
        # side on this coin (excl self + dust) -- live-faithful analog of forward_test's k_opp
        # (D != D[j] count). Agent M OOS-confirmed favored bucket = k_opp <= 3 (+52bps).
        k_opp = None
        if E1_KOPP_MAX is not None:
            k_opp = 0
            for (w2, c2), sz2 in book.leader.items():
                if c2 != c or w2 == w or abs(sz2) * px < 1.0:
                    continue
                if (sz2 > 0) != is_buy:
                    k_opp += 1
            if E1_MODE == "drop" and k_opp > E1_KOPP_MAX:
                book.rejects["e1_kopp"] += 1
                continue
        if NB_FILTER:
            # H-NB-FILTER step 4: regime filter on V17's gated entries. Causal asof
            # (last completed hour strictly before ts). no_coverage = NEUTRAL pass-through
            # (no signal -> never block), counted for disclosure.
            nb_b, _nbp = _nb_bucket(fname, c, ts, is_buy)
            book.nb_counts[nb_b] += 1
            if (nb_b == "OPPOSED") or (NB_FILTER == "block_opposed_neutral" and nb_b == "NEUTRAL"):
                book.rejects["nb_filter"] += 1
                continue
        if NETX_CAP is not None:
            # sprint: net-exposure cap. The knet gate removes counter-herd entries that previously
            # HEDGED the aligned book (v17b fold1 death); bound the one-directional swing instead.
            net_now = sum(p2["side"] * ORDER for p2 in book.pos.values())
            side_ = 1 if is_buy else -1
            if abs(net_now + side_ * ORDER) > NETX_CAP * EQUITY0 and abs(net_now + side_ * ORDER) > abs(net_now):
                book.rejects["netx_cap"] += 1
                continue
        if COINSIDE_CAP is not None:
            cs_now = sum(ORDER for (w3, c3), p3 in book.pos.items()
                         if c3 == c and p3["side"] == (1 if is_buy else -1))
            if cs_now + ORDER > COINSIDE_CAP * EQUITY0:
                book.rejects["coin_side_cap"] += 1
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
        _sz = _entry_size(fname, w, k_opp)   # AGENT N: ORDER unless H17/E1 tilt active
        if (book.gross() + _sz) / LEV > UTIL_CAP * book.equity(0):
            book.rejects["margin_util"] += 1
            continue
        if (book.gross() + _sz) > BACKSTOP_X * EQUITY0:
            book.rejects["gross_backstop"] += 1
            continue
        ment = S.mark_at(c, ts + LAT)
        if ment is None or ment <= 0:
            book.rejects["no_mark"] += 1
            continue
        side = 1 if is_buy else -1
        book.pos[key] = {"coin": c, "side": side, "entry_px": apply_entry(c, ment, is_buy),
                         "entry_ts": ts, "peak_bps": 0.0, "size": _sz,
                         "ride": bool(RIDE_KNET is not None and k is not None and k >= RIDE_KNET)}
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
    ga = np.array(tele["gross"]) if tele["gross"] else np.array([0.0])
    res = {
        "fold": fname, "entries": len(book.trades), "book_pnl": book.realized,
        "trade_med_bps": float(np.median(tr)) if len(tr) else 0.0,
        "trade_mean_bps": float(np.mean(tr)) if len(tr) else 0.0,
        "max_dd": dd, "eq_min": eq_min, "stopped": book.stopped,
        "rejects": dict(book.rejects), "exits": dict(book.exits),
        "max_coin_gross": dict(sorted(book.max_coin_gross.items(), key=lambda kv: -kv[1])[:4]),
        "gross_max_x": float(ga.max() / EQUITY0), "gross_p99_x": float(np.percentile(ga, 99) / EQUITY0),
        "gross_mean_x": float(ga.mean() / EQUITY0),
        "max_coin_side_x": tele["max_coin_side"] / EQUITY0,
        "max_all_long_x": tele["max_all_long"] / EQUITY0,
        "max_all_short_x": tele["max_all_short"] / EQUITY0,
        "funding_paid": float(getattr(book, "funding_paid", 0.0)),
    }
    if NB_FILTER:   # H-NB-FILTER: extra key only when the (default-off) filter is on
        res["nb_counts"] = dict(book.nb_counts)
        print(f"  nb_counts: {res['nb_counts']}")
    print(f"  ENTRIES {res['entries']} | book PnL ${res['book_pnl']:+.0f} | per-trade med {res['trade_med_bps']:+.1f} "
          f"mean {res['trade_mean_bps']:+.1f} bps | maxDD ${dd:.0f} ({dd/EQUITY0*100:.1f}%) | eq_min ${eq_min:.0f} "
          f"| STOPPED: {book.stopped}")
    print(f"  exits: {res['exits']}")
    print(f"  rejects: {res['rejects']}")
    print(f"  herd stress (max one-coin gross): {res['max_coin_gross']}")
    print(f"  gross: max {res['gross_max_x']:.2f}x | p99 {res['gross_p99_x']:.2f}x | mean {res['gross_mean_x']:.2f}x "
          f"| max coin-side {res['max_coin_side_x']:.2f}x | all-long {res['max_all_long_x']:.2f}x "
          f"| all-short {res['max_all_short_x']:.2f}x")
    return res


def main():
    install_memory_guard(soft_gb=12.0, label="v16_engine_replay")
    set_latency_ms(LAT)
    _md = os.environ.get("V16_SPRINT_MARKS_DIR")
    if _md:
        S.ASSETCTX_DIR = Path(_md)
        print(f"[replay] marks dir override: {_md}")
    _m02 = os.environ.get("V16_REPLAY_M02")
    if _m02:
        import select_cohort as SC
        SC.M02 = Path(_m02)
        print(f"[replay] m02 override: {_m02}")
    # AGENT H (2026-06-12) universe-expansion backtest, ADDITIVE + default-off. When
    # V16_REPLAY_EXPAND_COINS is set (JSON {coin: oneway_frac}), expand the LIQUID coin gate with
    # those coins and register their measured per-coin slippage into the canonical execution model.
    # The gate is select_cohort.LIQUID (used inside edge()) AND engine_replay's imported LIQUID alias
    # (used in run_fold's `c in LIQUID` + funding load) -- BOTH must be mutated. With the env unset,
    # LIQUID is untouched and the run is byte-identical to the shipped replay (regression-proven).
    global LIQUID
    _exp = os.environ.get("V16_REPLAY_EXPAND_COINS")
    if _exp:
        import select_cohort as SC
        from execution_model import register_slip_oneway
        emap = json.load(open(_exp)) if os.path.exists(_exp) else json.loads(_exp)
        added = []
        for coin, ow in emap.items():
            ow = float(ow)
            if ow <= 0:
                continue
            register_slip_oneway(coin, ow)            # additive: never touches the 10 majors
            if coin not in LIQUID:
                LIQUID.add(coin); SC.LIQUID.add(coin)
                added.append(coin)
        print(f"[replay] EXPANDED universe: +{len(added)} coins (gate now {len(LIQUID)}); "
              f"slip registered for {len(emap)} expansion coins")
        print(f"[replay] added: {sorted(added)}")
    print(f"SHIPPED CONFIG: order ${ORDER} | util {UTIL_CAP} lev {LEV} | stop {STOP_PCT} | backstop {BACKSTOP_X}x "
          f"| sl {SL} trail {TR_ACT}/{TR_GIVE} hold {MAX_HOLD_MS//3600000}h | trim_pct {TRIM_PCT} | chase {CHASE_BPS}bps")
    _load_nb()   # H-NB-FILTER: no-op unless V16_REPLAY_NB_FILTER is set
    uni = set(l.strip().lower() for l in open(S._DATA / "m01_nonerroring_wallets.txt")
              if l.strip() and not l.startswith("#"))
    out = [run_fold(*f, uni) for f in FOLDS]
    if TRADE_LOG:   # H-NB-FILTER step 1: observational dump, default-off
        pd.DataFrame(_TRADE_ROWS).to_parquet(TRADE_LOG, index=False)
        print(f"[replay] trade log: {len(_TRADE_ROWS)} rows -> {TRADE_LOG}")
    _out_path = Path(OUT_PQ) if OUT_PQ else (_REPO / "app" / "data" / "v16" / "engine_replay.parquet")
    pd.DataFrame(out).to_parquet(_out_path)
    ok = all(o["book_pnl"] > 0 and not o["stopped"] for o in out)
    print(f"\n=== VERDICT: {'PASS' if ok else 'FAIL'} (book PnL>0 + no stop trip, both folds) ===")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
