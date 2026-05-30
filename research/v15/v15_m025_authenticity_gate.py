#!/usr/bin/env python3
"""V15 M02.5 — Authenticity HARD GATE (codex-approved spec v3).

Runs BEFORE ranking. Decides which wallets are SAFE to copy: true, standalone,
unhedged, DIRECTIONAL risk. Conservative by design (drop a real edge before copying
a phantom at leverage).

Spec: brain projects/quant/v15/m025-authenticity-gate-spec (codex 3-round APPROVED).
Input set: the non-erroring M01 wallets (app/data/v15/m01_nonerroring_wallets.txt).

Pipeline order (codex note 1): STAGE A scalars for ALL wallets -> STAGE B entities ->
STAGE C/D/E/F -> combine. No interleave.

Reuses M01 proven loaders (fills/funding/ledger/anchors). Weekly anchors come from the
disk cache populated by v15_prefetch_anchors.py (zero API calls when warm).

CLI:
    python v15_m025_authenticity_gate.py --wallets-file W.txt \
        --output app/data/v15/m025_authenticity.parquet [--lookback-days 90]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, "/Users/hermes/quants-lab/research/v15")
import v15_m01_equity_reconstruct as m01  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [m025] %(message)s",
                    stream=sys.stdout)
log = logging.getLogger("m025")

# ---- thresholds (spec v3) ------------------------------------------------- #
LOOKBACK_DAYS = 90
T_WASH_S = 60
WASH_PRICE_BPS = 5e-4
WASH_SIZE_TOL = 0.10
NET_GROSS_NEUTRAL = 0.20        # L3 condition 1
PRICE_VAR_NEUTRAL = 0.30        # L3 condition 2
NET_GROSS_BORDER = 0.30         # REVIEW band upper
WASH_EXCLUDE = 0.50
WASH_REVIEW = 0.20
DEX_CONC = 0.90
STALE_DAYS = 21
MIN_ANCHORS = 8
MIN_ACTIVE_DAYS = 30
MIRROR_NET_GROSS = 0.15
MIRROR_HOURS_FRAC = 0.60
HEDGE_SIZE_TOL = 0.35           # FIX 4: per-coin abs-size match for exposure hedge
ENTITY_MAX_WALLETS = 8
SHARPE_FLAG = 5.0
LEV_FLAG = 5.0
# Only the "main" HL perp dex is executable via our connector for now; other perp
# dexes (HIP-3) cannot be mirrored. Anything outside this set, if >90% of gross, is
# flagged unexecutable.
EXECUTABLE_DEXES = frozenset({"main"})
FUNDING_FARM_FRAC = 0.5  # FIX 7(b): |funding| as fraction of |total pnl| above this -> EXCLUDE


@dataclass
class WalletScores:
    wallet: str
    n_fills: int = 0
    n_anchors: int = 0
    active_days: int = 0
    days_since_last_fill: float = 1e9
    net_gross_ratio: float = float("nan")
    price_pnl_var_frac: float = float("nan")
    funding_frac: float = float("nan")
    wash_frac: float = 0.0
    dex_concentration: float = 0.0
    unexecutable: bool = False
    sharpe: float = float("nan")
    median_lev: float = float("nan")
    sharpe_vs_lev_flag: bool = False
    l3_pass_standalone: bool = False     # passes L3 directional test standalone
    confidence: str = "HIGH"
    # filled in stage B/combine
    entity_id: int = -1
    is_entity_primary: bool = True
    verdict: str = "PASS"
    reason_codes: list = field(default_factory=list)
    # FIX 1(c): distinguishes anchor_cache_missing vs anchors_out_of_window vs thin
    anchor_reason: str = ""


# --------------------------------------------------------------------------- #
# STAGE A — per-wallet scalars
# --------------------------------------------------------------------------- #

def _anchor_window(wallet, lo_ms, hi_ms):
    """FIX 1: select usable weekly anchors for [lo_ms, hi_ms].

    (a) Normalize anchor timestamps to ms (a value < 1e12 looks like seconds ->
        multiply by 1000).
    (b) Include the single anchor immediately BEFORE lo_ms so the first in-window
        week has a valid left edge for its delta.
    (c) Report flags so the caller can distinguish:
          - anchor_cache_missing  : 0 raw anchors (disk cache not warmed)
          - anchors_out_of_window : >=2 raw anchors but <2 fall in the window
          - thin_history          : genuinely few anchors

    Returns (selected, flags); selected = sorted [(ts_ms, val)],
    flags = {n_raw_anchors, n_in_window}.
    """
    raw = m01.get_portfolio_perp(wallet) or []
    norm = []
    for t, v in raw:
        if not (v > 0.01):
            continue
        t = int(t)
        if t < 1_000_000_000_000:  # < 1e12 -> seconds, convert to ms
            t *= 1000
        norm.append((t, v))
    norm.sort()
    flags = {"n_raw_anchors": len(norm), "n_in_window": 0}
    in_window = [(t, v) for t, v in norm if lo_ms <= t <= hi_ms]
    flags["n_in_window"] = len(in_window)
    before = [(t, v) for t, v in norm if t < lo_ms]
    selected = ([before[-1]] + in_window) if before else in_window
    selected.sort()
    return selected, flags


def _weekly_anchor_series(wallet, fills, funding, ledger, lo_ms, hi_ms):
    """Return (anchor_ts, total_change, funding_in_week, residual) arrays.

    residual = total_change - funding - net_external_flow  (trading/price PnL).
    Uses TRUE perpAllTime anchor values (cached) + funding + ledger ext-flow.

    FIX 1: anchors now come from _anchor_window (ms-normalized + the anchor just
    before lo_ms as the first week's left edge). On failure this returns
    (status, flags) where status in {"cache_missing","out_of_window","thin"} so the
    caller can emit a distinct reason code; on success returns ("ok", payload...).
    """
    avh, _flags = _anchor_window(wallet, lo_ms, hi_ms)
    if len(avh) < 2:
        if _flags["n_raw_anchors"] == 0:
            return ("cache_missing", _flags)
        if _flags["n_raw_anchors"] >= 2 and _flags["n_in_window"] < 2:
            return ("out_of_window", _flags)
        return ("thin", _flags)
    avh.sort()
    ts = [t for t, _ in avh]
    vals = [v for _, v in avh]
    # funding usdc per week bucket (prev, cur]
    fund_ev = sorted((int(e["time"]), m01.funding_cash_delta(e)) for e in funding)
    ext_ev = sorted((int(e["time"]),
                     m01.ledger_cash_delta(e, wallet.lower()).ext_flow) for e in ledger)
    total_change, fund_week, resid = [], [], []
    for i in range(1, len(vals)):
        t0, t1 = ts[i-1], ts[i]
        tc = vals[i] - vals[i-1]
        fw = sum(u for t, u in fund_ev if t0 < t <= t1)
        ef = sum(x for t, x in ext_ev if t0 < t <= t1)
        total_change.append(tc)
        fund_week.append(fw)
        resid.append(tc - fw - ef)
    return ("ok", np.array(ts[1:]), np.array(total_change), np.array(fund_week),
            np.array(resid), np.array(vals))


def _last_price_by_coin(*fill_lists):
    """Last known fill price per coin across the given fill lists (by time)."""
    px = {}  # coin -> (time, price)
    for fills in fill_lists:
        for f in fills:
            c = f["coin"]
            p = float(f.get("price", 0) or 0)
            if p <= 0:
                continue
            t = f["time"]
            if c not in px or t > px[c][0]:
                px[c] = (t, p)
    return {c: p for c, (_, p) in px.items()}


def _net_gross_ratio(fills, lo_ms=None, hi_ms=None, seed_pos=None):
    """Book-level time-weighted |net dollar delta| / gross, over open-position time.

    Per coin, maintain cumulative position from fills; between events position is
    constant. Value at last trade price per coin. book_net = Σ pos*px (signed),
    book_gross = Σ |pos*px|. Weight each segment by dt*book_gross. Cross-asset does
    NOT cancel within a coin (per-coin scalar); book-level offsets DO net (intended:
    flat-book farmer -> ~0). Paired with price_pnl_var_frac via AND so a real
    relative-value trader with price PnL survives.

    FIX 2: ``fills`` may now span a pre-window history (lo_ms - 365d). Only the
    window [lo_ms, hi_ms] is time-weighted; ``seed_pos`` (from m01.positions_at at
    lo_ms) seeds the position carried into the window so exposure does not start
    flat at 0. When lo_ms/hi_ms are None the legacy whole-series behaviour is kept.

    ISSUE C: the held exposure from the last in-window fill to ``win_hi`` is now
    time-weighted with one final segment, so exposure at window end is not ignored.
    """
    # merged event timeline
    ev = sorted(fills, key=lambda f: f["time"])
    if len(ev) < 2:
        return float("nan")
    # FIX 2: seed pre-window position; restrict accumulation to the window.
    pos = defaultdict(float, dict(seed_pos) if seed_pos else {})
    last_px = {}
    # prime last_px from pre-window fills so the seeded position has a price.
    win_lo = lo_ms if lo_ms is not None else ev[0]["time"]
    win_hi = hi_ms if hi_ms is not None else ev[-1]["time"]
    for f in ev:
        if f["time"] < win_lo:
            last_px[f["coin"]] = f["price"]
    num = den = 0.0
    prev_t = win_lo
    for f in ev:
        t = f["time"]
        if t < win_lo:
            continue
        if t > win_hi:
            break
        dt = t - prev_t
        if dt > 0:
            book_net = sum(pos[c]*last_px.get(c, 0.0) for c in pos)
            book_gross = sum(abs(pos[c]*last_px.get(c, 0.0)) for c in pos)
            if book_gross > 1.0:
                num += dt * abs(book_net)
                den += dt * book_gross
        pos[f["coin"]] += f["signed_sz"]
        last_px[f["coin"]] = f["price"]
        prev_t = t
    # ISSUE C: final segment from the last in-window fill (prev_t) to win_hi so the
    # exposure HELD at window end is time-weighted (same book_net/book_gross logic).
    dt = win_hi - prev_t
    if dt > 0:
        book_net = sum(pos[c]*last_px.get(c, 0.0) for c in pos)
        book_gross = sum(abs(pos[c]*last_px.get(c, 0.0)) for c in pos)
        if book_gross > 1.0:
            num += dt * abs(book_net)
            den += dt * book_gross
    return (num/den) if den > 0 else float("nan")


def _wash_frac(fills):
    """Round-trip volume / total volume. Round-trip = same coin, opposite side
    within T_WASH_S, price within WASH_PRICE_BPS, size within WASH_SIZE_TOL."""
    by_coin = defaultdict(list)
    for f in fills:
        by_coin[f["coin"]].append(f)
    total = sum(abs(f["size"])*f["price"] for f in fills)
    if total <= 0:
        return 0.0
    wash = 0.0
    for coin, fs in by_coin.items():
        fs.sort(key=lambda x: x["time"])
        used = [False]*len(fs)
        for i, a in enumerate(fs):
            if used[i]:
                continue
            # bound the forward scan: time-break handles normal wallets, the count
            # cap bounds worst-case for HFT wallets doing thousands of fills/min.
            for j in range(i+1, min(i+1+500, len(fs))):
                b = fs[j]
                if b["time"] - a["time"] > T_WASH_S*1000:
                    break
                if used[j] or (a["signed_sz"] > 0) == (b["signed_sz"] > 0):
                    continue
                if abs(b["price"]-a["price"])/a["price"] > WASH_PRICE_BPS:
                    continue
                if abs(abs(b["size"])-abs(a["size"]))/max(abs(a["size"]), 1e-9) > WASH_SIZE_TOL:
                    continue
                wash += (abs(a["size"])*a["price"] + abs(b["size"])*b["price"])
                used[i] = used[j] = True
                break
    return min(wash/total, 1.0)


def stage_a(wallet, lo_ms, hi_ms) -> WalletScores:
    s = WalletScores(wallet=wallet)
    # FIX 2 + ISSUE B: load fills from 365 days BEFORE lo_ms so positions opened
    # up to a year before the window are seeded. The position carried into lo_ms
    # seeds the net/gross and leverage accumulators; only the window [lo_ms, hi_ms]
    # is time-weighted.
    PRE_WINDOW_MS = 365 * 86_400_000
    all_fills = m01.load_wallet_fills(wallet, lo_ms - PRE_WINDOW_MS, hi_ms)
    fills = [f for f in all_fills if lo_ms <= f["time"] <= hi_ms]
    s.n_fills = len(fills)
    if not fills:
        s.confidence = "LOW"
        s.reason_codes.append("no_fills")
        return s
    # FIX 2: position carried into the window from pre-window fills (fills_ctx spans
    # lo_ms-30d..hi_ms). seed = cumulative per-coin position held AT lo_ms, so the
    # net/gross and leverage accumulators start from real carried exposure instead
    # of flat 0. m01.positions_at is order-independent (cumulative signed_sz).
    # ISSUE 2: seed STRICTLY pre-window (lo_ms - 1) so fills at t == lo_ms are NOT
    # double-counted — the window loops below include t == lo_ms via `if t < win_lo:
    # continue`, so seeding at lo_ms would count those fills twice.
    seed_pos = m01.positions_at(all_fills, lo_ms - 1)
    funding = m01.load_wallet_funding(wallet, lo_ms, hi_ms)
    ledger = m01.load_wallet_ledger(wallet, lo_ms, hi_ms)
    last_t = max(f["time"] for f in fills)
    s.days_since_last_fill = (hi_ms - last_t)/86_400_000
    days = {pd.Timestamp(f["time"], unit="ms", tz="UTC").date() for f in fills}
    s.active_days = len(days)

    # dex concentration
    gross_by_dex = defaultdict(float)
    for f in fills:
        gross_by_dex[m01.coin_dex(f["coin"])] += abs(f["size"])*f["price"]
    tot_gross = sum(gross_by_dex.values())
    if tot_gross > 0:
        top_dex, top_g = max(gross_by_dex.items(), key=lambda kv: kv[1])
        s.dex_concentration = top_g/tot_gross
        if s.dex_concentration > DEX_CONC and top_dex not in EXECUTABLE_DEXES:
            s.unexecutable = True

    # FIX 2: pass full (pre-window + window) fills with the window bounds and the
    # seeded carry-in position; only the window is time-weighted.
    s.net_gross_ratio = _net_gross_ratio(all_fills, lo_ms, hi_ms, seed_pos=seed_pos)
    s.wash_frac = _wash_frac(fills)

    # FIX 1(c): _weekly_anchor_series returns a status string first. On failure,
    # record WHY anchors are insufficient (cache vs window vs thin) so COMBINE can
    # emit a distinct reason code.
    wk = _weekly_anchor_series(wallet, fills, funding, ledger, lo_ms, hi_ms)
    if wk[0] != "ok":
        s.anchor_reason = {"cache_missing": "anchor_cache_missing",
                           "out_of_window": "anchors_out_of_window",
                           "thin": "thin_history"}.get(wk[0], "thin_history")
    else:
        _status, anchor_ts, total_change, fund_week, resid, vals = wk
        s.n_anchors = len(vals)
        var_tot = float(np.var(total_change)) if len(total_change) >= 2 else 0.0
        if var_tot > 1e-9:
            s.price_pnl_var_frac = float(np.var(resid))/var_tot
        # FIX 7(a) + ISSUE F: absolute funding contribution as a fraction of the
        # TRADING+FUNDING pnl, EXCLUDING external deposits/withdrawals. resid =
        # total_change - fund_week - ext_flow (trading/price pnl), so trading+funding
        # pnl per week = resid + fund_week. funding_frac = sum(|fund_week|) /
        # sum(|resid + fund_week|). Guard ~0 denominator -> nan.
        trade_fund_pnl = resid + fund_week
        denom = float(np.sum(np.abs(trade_fund_pnl)))
        s.funding_frac = (float(np.sum(np.abs(fund_week))) / denom
                          if denom > 1e-9 else float("nan"))
        # weekly return Sharpe + leverage flag
        rets = total_change/np.maximum(vals[:-1], 1e-9)
        if len(rets) >= MIN_ANCHORS and np.std(rets) > 1e-9:
            s.sharpe = float(np.mean(rets)/np.std(rets)*np.sqrt(52))
            gross_hr = _median_leverage(all_fills, vals, lo_ms=lo_ms, seed_pos=seed_pos)
            s.median_lev = gross_hr
            max_dd = _max_weekly_dd(vals)
            if (s.sharpe >= SHARPE_FLAG and (gross_hr == gross_hr)
                    and gross_hr >= LEV_FLAG and max_dd > -0.10):
                s.sharpe_vs_lev_flag = True

    # confidence
    if s.n_anchors < MIN_ANCHORS or s.active_days < MIN_ACTIVE_DAYS:
        s.confidence = "LOW"

    # L3 standalone pass = is this wallet a real directional trader on its own?
    ng, pv = s.net_gross_ratio, s.price_pnl_var_frac
    neutral = (ng == ng and ng < NET_GROSS_NEUTRAL) and (pv == pv and pv < PRICE_VAR_NEUTRAL)
    s.l3_pass_standalone = (not neutral) and (ng == ng) and (pv == pv)
    return s


def _median_leverage(fills, vals, lo_ms=None, seed_pos=None):
    """Median (gross open notional / equity). Rough: peak gross per week vs anchor.

    FIX 2: ``seed_pos`` seeds the position carried into the window so peak gross
    notional reflects pre-window holdings rather than starting at 0.

    ISSUE 3: ``fills`` may span pre-window history (lo_ms - 365d). When ``lo_ms`` is
    given, prime ``last_px`` from the last pre-window fill price per coin (same as
    _net_gross_ratio) so a seeded carried coin is priced from its first segment
    instead of contributing 0 notional until it trades again in-window.
    """
    if len(vals) < 2:
        return float("nan")
    # use mean gross position notional proxy / median equity
    pos = defaultdict(float, dict(seed_pos) if seed_pos else {})
    last_px = {}
    # ISSUE 3: prime last_px from pre-window fills so seeded positions are priced.
    if lo_ms is not None:
        for f in sorted(fills, key=lambda x: x["time"]):
            if f["time"] < lo_ms:
                last_px[f["coin"]] = f["price"]
    peak_gross = 0.0
    for f in sorted(fills, key=lambda x: x["time"]):
        if lo_ms is not None and f["time"] < lo_ms:
            continue
        pos[f["coin"]] += f["signed_sz"]
        last_px[f["coin"]] = f["price"]
        g = sum(abs(pos[c]*last_px.get(c, 0.0)) for c in pos)
        peak_gross = max(peak_gross, g)
    med_eq = float(np.median(vals))
    return peak_gross/med_eq if med_eq > 1.0 else float("nan")


def _max_weekly_dd(vals):
    peak = -1e18
    mdd = 0.0
    for v in vals:
        peak = max(peak, v)
        if peak > 0:
            mdd = min(mdd, (v-peak)/peak)
    return mdd


# --------------------------------------------------------------------------- #
# STAGE B — entity linkage
# --------------------------------------------------------------------------- #

def build_entities(wallets, lo_ms, hi_ms):
    """Union-find over ledger transfer edges + temporal funder match.

    Returns dict wallet->entity_id and dict entity_id->[wallets].
    """
    parent = {w: w for w in wallets}
    wset = set(wallets)

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        if a in parent and b in parent:
            parent[find(a)] = find(b)

    # direct transfer edges
    funder_events = []   # ISSUE 1: one global time-sorted list of (t, w, dir, amt)
    for w in wallets:
        for e in m01.load_wallet_ledger(w, lo_ms, hi_ms):
            d = e.get("delta", {})
            typ = d.get("type")
            if typ in ("send", "internalTransfer", "subAccountTransfer"):
                dest = (d.get("destination") or "").lower()
                if dest in wset:
                    union(w, dest)
            # temporal funder match prep: deposits & withdrawals (global list)
            if typ in ("deposit", "withdraw"):
                try:
                    amt = abs(float(d.get("usdc") or 0))
                except (TypeError, ValueError):
                    amt = 0.0
                if amt > 1.0:
                    funder_events.append(
                        (int(e["time"]), w, "out" if typ == "withdraw" else "in", amt))

    # ISSUE 1: temporal funder match over a SINGLE global time-sorted list (no per-
    # bucket grouping). The prior round(amt,-1) bucketing missed cross-bucket pairs
    # (e.g. 1004 -> bucket 1000 vs 1006 -> bucket 1010 never compared). Now every
    # event is scanned forward in time while t1-t0 <= 10min; link A and B ONLY when a
    # WITHDRAW from A (dir0=="out") is followed by a DEPOSIT to B (dir1=="in"),
    # w0!=w1, and the actual float amounts agree within +/-0.5% (capital hand-off).
    # The 10-min window bounds the forward inner scan; total cost O(n log n + matches).
    AMT_TOL = 0.005
    funder_events.sort()
    n = len(funder_events)
    for i in range(n):
        t0, w0, dir0, amt0 = funder_events[i]
        for j in range(i + 1, n):
            t1, w1, dir1, amt1 = funder_events[j]
            if t1 - t0 > 600_000:
                break
            if w0 == w1:
                continue
            # strict: earlier=withdraw (out) -> later=deposit (in) only
            if not (dir0 == "out" and dir1 == "in"):
                continue
            ref = max(abs(amt0), abs(amt1), 1e-9)
            if abs(amt0 - amt1) / ref <= AMT_TOL:
                union(w0, w1)

    # collect components
    comp = defaultdict(list)
    for w in wallets:
        comp[find(w)].append(w)
    ent_id = {}
    ent_members = {}
    for i, (_, members) in enumerate(comp.items()):
        for w in members:
            ent_id[w] = i
        ent_members[i] = members
    return ent_id, ent_members


# --------------------------------------------------------------------------- #
# STAGE C — within-entity mirror
# --------------------------------------------------------------------------- #

def internal_hedge(fillsA, fillsB, lo_ms, hi_ms):
    """FIX 4: exposure-based (not flow-based) internal-hedge test.

    ISSUE D(2): sample timestamps every 6 HOURS across [lo_ms, hi_ms] (denser grid
    than the prior daily sampling). At each t, reconstruct the HELD
    position of each wallet (m01.positions_at -> coin -> signed size). A coin is
    "hedged" at t when A and B hold OPPOSITE signs and their magnitudes (abs size)
    are within HEDGE_SIZE_TOL of each other. A sampled timestamp is "predominantly
    hedged" when, among coins both wallets have exposure to, the majority are hedged.
    The PAIR is an internal hedge when, over sampled timestamps where BOTH wallets
    have any exposure, the fraction of predominantly-hedged timestamps is
    >= MIRROR_HOURS_FRAC. Coins are NOT netted across each other."""
    if hi_ms <= lo_ms:
        return False
    both_active = hedged_ts = 0
    t = lo_ms
    while t <= hi_ms:
        posA = m01.positions_at(fillsA, t)
        posB = m01.positions_at(fillsB, t)
        if posA and posB:
            both_active += 1
            shared = [c for c in (set(posA) & set(posB))
                      if abs(posA[c]) > 1e-12 and abs(posB[c]) > 1e-12]
            if shared:
                coin_hedged = 0
                for c in shared:
                    a, b = posA[c], posB[c]
                    if (a > 0) == (b > 0):
                        continue  # same sign -> not a hedge on this coin
                    ref = max(abs(a), abs(b), 1e-9)
                    if abs(abs(a) - abs(b)) / ref <= HEDGE_SIZE_TOL:
                        coin_hedged += 1
                if coin_hedged * 2 >= len(shared):  # predominantly hedged
                    hedged_ts += 1
        t += 21_600_000  # ISSUE D(2): every 6 hours
    if both_active == 0:
        return False
    return hedged_ts / both_active >= MIRROR_HOURS_FRAC


# --------------------------------------------------------------------------- #
# COMBINE
# --------------------------------------------------------------------------- #

def run(wallets, lo_ms, hi_ms):
    log.info(f"STAGE A: {len(wallets)} wallets")
    scores = {}
    t0 = time.time()
    for i, w in enumerate(wallets, 1):
        scores[w] = stage_a(w, lo_ms, hi_ms)
        if i % 500 == 0:
            log.info(f"  A [{i}/{len(wallets)}] ({(time.time()-t0)/60:.1f}min)")

    log.info("STAGE B: entities")
    ent_id, ent_members = build_entities(wallets, lo_ms, hi_ms)
    for w in wallets:
        scores[w].entity_id = ent_id[w]

    log.info("STAGE B/C: entity verdicts")
    fills_cache = {}

    def get_fills(w):
        # ISSUE D(1): load from 365d before lo_ms so m01.positions_at sees positions
        # opened pre-window (held exposure carried into the window is visible to the
        # internal-hedge exposure test).
        if w not in fills_cache:
            fills_cache[w] = m01.load_wallet_fills(w, lo_ms - 365 * 86_400_000, hi_ms)
        return fills_cache[w]

    entity_excluded = {}   # entity_id -> reason ("internal_hedge"/"no_l3_passer"/"too_big")
    entity_primary = {}    # entity_id -> wallet
    for eid, members in ent_members.items():
        if len(members) == 1:
            entity_primary[eid] = members[0]
            continue
        if len(members) > ENTITY_MAX_WALLETS:
            entity_excluded[eid] = "entity_too_big_review"
            continue
        # primary = best standalone directional (highest Sharpe that passes L3 standalone)
        passers = [w for w in members if scores[w].l3_pass_standalone]
        if not passers:
            # codex r4 fix: distinguish "L3 known-and-failed" (genuinely neutral)
            # from "L3 unknown" (NaN net_gross / price_var from thin/missing anchors).
            # Only HARD-EXCLUDE when EVERY member has VALID L3 metrics and all failed
            # (the entity is provably non-directional). If ANY member's L3 is unknown,
            # we cannot prove non-directionality -> route to REVIEW, never hard-exclude
            # a possibly-clean directional trader on missing data.
            def _l3_known(w):
                ng, pv = scores[w].net_gross_ratio, scores[w].price_pnl_var_frac
                return ng == ng and pv == pv
            if all(_l3_known(w) for w in members):
                entity_excluded[eid] = "entity_no_l3_passer"       # hard exclude
            else:
                entity_excluded[eid] = "entity_l3_unknown_review"  # REVIEW
            continue
        primary = max(passers, key=lambda w: (scores[w].sharpe
                                              if scores[w].sharpe == scores[w].sharpe else -1e9))
        entity_primary[eid] = primary
        # within-entity mirror: if primary is in an internal hedge with any member -> exclude
        for other in members:
            if other == primary:
                continue
            if internal_hedge(get_fills(primary), get_fills(other), lo_ms, hi_ms):
                entity_excluded[eid] = "internal_hedge"
                break

    log.info("COMBINE: verdicts")
    rows = []
    for w in wallets:
        s = scores[w]
        rc = list(s.reason_codes)
        verdict = "PASS"
        eid = s.entity_id
        members = ent_members.get(eid, [w])

        is_primary = (entity_primary.get(eid) == w) if len(members) > 1 else True
        s.is_entity_primary = is_primary

        # FIX 5: HARD EXCLUDES ALWAYS WIN. Evaluate every hard-exclude condition
        # FIRST, independent of any entity REVIEW (e.g. entity_too_big). Only if NO
        # hard exclude fires do we fall through to REVIEW reasons. This fixes the
        # prior bug where an entity REVIEW (entity_too_big) short-circuited the
        # per-wallet hard excludes via `if verdict == "PASS"`.
        hard_codes = []

        # entity-level hard excludes (internal_hedge / no_l3_passer)
        ent_reason = entity_excluded.get(eid)
        is_too_big = (ent_reason == "entity_too_big_review")
        is_l3_unknown = (ent_reason == "entity_l3_unknown_review")
        # review-only entity states are NOT hard excludes; only genuine entity hard
        # reasons (internal_hedge / entity_no_l3_passer) become hard codes here.
        if ent_reason and not is_too_big and not is_l3_unknown:
            hard_codes.append(ent_reason)
        # ISSUE A: entity fragment is a HARD exclude for non-primary wallets in a
        # multi-wallet entity — but a too-big entity has NO primary set, so EVERY
        # member would wrongly trip entity_fragment and get hard-EXCLUDED, defeating
        # the intended REVIEW. For too-big entities, SKIP the fragment check (treat
        # as primary for fragment purposes) so a clean member routes to REVIEW
        # (entity_too_big); genuine per-wallet hard excludes below still fire.
        if len(members) > 1 and not is_primary and not is_too_big and not is_l3_unknown:
            hard_codes.append("entity_fragment")
        # per-wallet hard excludes (all evaluated, all codes recorded)
        if s.unexecutable:
            hard_codes.append("unexecutable_dex")
        if s.days_since_last_fill > STALE_DAYS:
            hard_codes.append("stale")
        if s.wash_frac > WASH_EXCLUDE:
            hard_codes.append("wash")
        ng, pv = s.net_gross_ratio, s.price_pnl_var_frac
        # FIX 6: delta-neutral hard exclude requires BOTH low net-gross AND low
        # price-PnL variance.
        if (ng == ng and ng < NET_GROSS_NEUTRAL) and (pv == pv and pv < PRICE_VAR_NEUTRAL):
            hard_codes.append("delta_neutral")
        # FIX 7(b): funding farm — |funding| dominates |total pnl|.
        if s.funding_frac == s.funding_frac and s.funding_frac > FUNDING_FARM_FRAC:
            hard_codes.append("funding_farm")

        if hard_codes:
            verdict = "EXCLUDE"
            rc.extend(hard_codes)
        else:
            # REVIEW reasons (only when NO hard exclude fired)
            if ent_reason == "entity_too_big_review":
                verdict = "REVIEW"; rc.append("entity_too_big")
            elif ent_reason == "entity_l3_unknown_review":
                verdict = "REVIEW"; rc.append("entity_l3_unknown")
            elif s.confidence == "LOW":
                verdict = "REVIEW"; rc.append(s.anchor_reason or "thin_history")
            elif ng != ng or pv != pv:
                verdict = "REVIEW"; rc.append("nan_metric")
            elif s.sharpe_vs_lev_flag:
                verdict = "REVIEW"; rc.append("sharpe_too_smooth")
            elif WASH_REVIEW < s.wash_frac <= WASH_EXCLUDE:
                verdict = "REVIEW"; rc.append("wash_borderline")
            # FIX 6: low net-gross alone (price PnL var present) -> REVIEW, not PASS.
            elif ng == ng and ng < NET_GROSS_NEUTRAL:
                verdict = "REVIEW"; rc.append("low_net_gross")
            elif ng == ng and NET_GROSS_NEUTRAL <= ng < NET_GROSS_BORDER:
                verdict = "REVIEW"; rc.append("net_gross_borderline")

        s.verdict = verdict
        s.reason_codes = rc
        rows.append({
            "wallet": w, "verdict": verdict, "reason_codes": ",".join(rc),
            "entity_id": eid, "is_entity_primary": is_primary,
            "n_entity_wallets": len(members),
            "net_gross_ratio": s.net_gross_ratio,
            "price_pnl_var_frac": s.price_pnl_var_frac,
            "funding_frac": s.funding_frac, "wash_frac": s.wash_frac,
            "sharpe": s.sharpe, "median_lev": s.median_lev,
            "sharpe_vs_lev_flag": s.sharpe_vs_lev_flag,
            "dex_concentration": s.dex_concentration, "unexecutable": s.unexecutable,
            "days_since_last_fill": s.days_since_last_fill,
            "n_anchors": s.n_anchors, "active_days": s.active_days,
            "n_fills": s.n_fills, "confidence": s.confidence,
        })
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets-file", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--lookback-days", type=int, default=LOOKBACK_DAYS)
    ap.add_argument("--end", default="2026-05-23")
    args = ap.parse_args()

    hi_ms = int((pd.Timestamp(args.end, tz="UTC")+pd.Timedelta(days=1)).timestamp()*1000-1)
    lo_ms = hi_ms - args.lookback_days*86_400_000
    wallets = [w.strip().lower() for w in open(args.wallets_file)
               if w.strip() and not w.startswith("#")]
    log.info(f"{len(wallets)} wallets, window {args.lookback_days}d ending {args.end}")

    df = run(wallets, lo_ms, hi_ms)

    # codex note 5: sanity assertions
    hard = {"entity_fragment", "entity_no_l3_passer", "internal_hedge", "delta_neutral",
            "wash", "unexecutable_dex", "stale", "funding_farm"}
    for _, r in df.iterrows():
        codes = set(c for c in r["reason_codes"].split(",") if c)
        if r["verdict"] == "EXCLUDE":
            assert codes, f"EXCLUDE with no reason: {r['wallet']}"
        if r["verdict"] == "PASS":
            assert not (codes & hard), f"PASS with hard code: {r['wallet']} {codes}"
            assert r["confidence"] != "LOW", f"PASS with LOW conf: {r['wallet']}"

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.output, index=False, compression="snappy")

    vc = df["verdict"].value_counts()
    log.info(f"VERDICTS: {dict(vc)}")
    # reason histogram
    from collections import Counter
    rh = Counter()
    for codes in df["reason_codes"]:
        for c in codes.split(","):
            if c:
                rh[c] += 1
    log.info(f"REASON HISTOGRAM: {dict(rh.most_common())}")
    log.info(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
