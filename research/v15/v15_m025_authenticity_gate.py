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
ENTITY_MAX_WALLETS = 8
SHARPE_FLAG = 5.0
LEV_FLAG = 5.0
# Non-executable HIP-3 dexes (we cannot place orders there). main/xyz tradeable subset
# kept conservative: anything outside this set, if >90% of gross, is unexecutable.
EXECUTABLE_DEXES = frozenset({"main"})


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


# --------------------------------------------------------------------------- #
# STAGE A — per-wallet scalars
# --------------------------------------------------------------------------- #

def _weekly_anchor_series(wallet, fills, funding, ledger, lo_ms, hi_ms):
    """Return (anchor_ts, total_change, funding_in_week, residual) arrays.

    residual = total_change - funding - net_external_flow  (trading/price PnL).
    Uses TRUE perpAllTime anchor values (cached) + funding + ledger ext-flow.
    """
    avh = [(t, v) for t, v in m01.get_portfolio_perp(wallet)
           if v > 0.01 and lo_ms <= t <= hi_ms]
    if len(avh) < 2:
        return None
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
    return (np.array(ts[1:]), np.array(total_change), np.array(fund_week),
            np.array(resid), np.array(vals))


def _net_gross_ratio(fills):
    """Book-level time-weighted |net dollar delta| / gross, over open-position time.

    Per coin, maintain cumulative position from fills; between events position is
    constant. Value at last trade price per coin. book_net = Σ pos*px (signed),
    book_gross = Σ |pos*px|. Weight each segment by dt*book_gross. Cross-asset does
    NOT cancel within a coin (per-coin scalar); book-level offsets DO net (intended:
    flat-book farmer -> ~0). Paired with price_pnl_var_frac via AND so a real
    relative-value trader with price PnL survives.
    """
    # merged event timeline
    ev = sorted(fills, key=lambda f: f["time"])
    if len(ev) < 2:
        return float("nan")
    pos = defaultdict(float)
    last_px = {}
    num = den = 0.0
    prev_t = ev[0]["time"]
    for f in ev:
        t = f["time"]
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
    fills = m01.load_wallet_fills(wallet, lo_ms, hi_ms)
    s.n_fills = len(fills)
    if not fills:
        s.confidence = "LOW"
        s.reason_codes.append("no_fills")
        return s
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

    s.net_gross_ratio = _net_gross_ratio(fills)
    s.wash_frac = _wash_frac(fills)

    wk = _weekly_anchor_series(wallet, fills, funding, ledger, lo_ms, hi_ms)
    if wk is not None:
        anchor_ts, total_change, fund_week, resid, vals = wk
        s.n_anchors = len(vals)
        var_tot = float(np.var(total_change)) if len(total_change) >= 2 else 0.0
        if var_tot > 1e-9:
            s.price_pnl_var_frac = float(np.var(resid))/var_tot
        s.funding_frac = (float(np.sum(fund_week)) /
                          float(np.sum(np.abs(total_change)))
                          if np.sum(np.abs(total_change)) > 1e-9 else float("nan"))
        # weekly return Sharpe + leverage flag
        rets = total_change/np.maximum(vals[:-1], 1e-9)
        if len(rets) >= MIN_ANCHORS and np.std(rets) > 1e-9:
            s.sharpe = float(np.mean(rets)/np.std(rets)*np.sqrt(52))
            gross_hr = _median_leverage(fills, vals)
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


def _median_leverage(fills, vals):
    """Median (gross open notional / equity). Rough: peak gross per week vs anchor."""
    if len(vals) < 2:
        return float("nan")
    # use mean gross position notional proxy / median equity
    pos = defaultdict(float)
    last_px = {}
    peak_gross = 0.0
    for f in sorted(fills, key=lambda x: x["time"]):
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
    deposits_by_bucket = defaultdict(list)   # for temporal funder match
    for w in wallets:
        for e in m01.load_wallet_ledger(w, lo_ms, hi_ms):
            d = e.get("delta", {})
            typ = d.get("type")
            if typ in ("send", "internalTransfer", "subAccountTransfer"):
                dest = (d.get("destination") or "").lower()
                if dest in wset:
                    union(w, dest)
            # temporal funder match prep: deposits & withdrawals by rounded amount
            if typ in ("deposit", "withdraw"):
                try:
                    amt = abs(float(d.get("usdc") or 0))
                except (TypeError, ValueError):
                    amt = 0.0
                if amt > 1.0:
                    bucket = round(amt, -1)  # 10-USDC bucket key (hash bucket, codex note 3)
                    deposits_by_bucket[bucket].append(
                        (int(e["time"]), w, "out" if typ == "withdraw" else "in"))

    # temporal funder match: within an amount bucket, a withdraw then a deposit
    # within +/-10min and amount +/-0.5% links the two wallets (capital hand-off).
    for bucket, evs in deposits_by_bucket.items():
        evs.sort()
        for i, (t0, w0, dir0) in enumerate(evs):
            for t1, w1, dir1 in evs[i+1:]:
                if t1 - t0 > 600_000:
                    break
                if w0 != w1 and dir0 != dir1:
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

def internal_hedge(wA_fills, wB_fills):
    """True if the pair is an internal hedge: per coin per hour, summed signed
    notional across the pair nets to ~0 (<MIRROR_NET_GROSS) over >=MIRROR_HOURS_FRAC
    of overlapping active hours."""
    def hourly(fills):
        h = defaultdict(lambda: defaultdict(float))  # hour -> coin -> signed notional
        for f in fills:
            hr = f["time"]//3_600_000
            h[hr][f["coin"]] += f["signed_sz"]*f["price"]
        return h
    a, b = hourly(wA_fills), hourly(wB_fills)
    hrs = set(a) & set(b)
    if not hrs:
        return False
    hedge_hours = 0
    for hr in hrs:
        coins = set(a[hr]) | set(b[hr])
        net = sum(a[hr].get(c, 0)+b[hr].get(c, 0) for c in coins)
        gross = sum(abs(a[hr].get(c, 0))+abs(b[hr].get(c, 0)) for c in coins)
        if gross > 1.0 and abs(net)/gross < MIRROR_NET_GROSS:
            hedge_hours += 1
    return hedge_hours/len(hrs) >= MIRROR_HOURS_FRAC


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
        if w not in fills_cache:
            fills_cache[w] = m01.load_wallet_fills(w, lo_ms, hi_ms)
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
            entity_excluded[eid] = "entity_no_l3_passer"
            continue
        primary = max(passers, key=lambda w: (scores[w].sharpe
                                              if scores[w].sharpe == scores[w].sharpe else -1e9))
        entity_primary[eid] = primary
        # within-entity mirror: if primary is in an internal hedge with any member -> exclude
        for other in members:
            if other == primary:
                continue
            if internal_hedge(get_fills(primary), get_fills(other)):
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

        # entity-level hard excludes
        if eid in entity_excluded:
            reason = entity_excluded[eid]
            if reason == "entity_too_big_review":
                verdict = "REVIEW"; rc.append("entity_too_big")
            else:
                verdict = "EXCLUDE"; rc.append(reason)
        # entity fragment: non-primary in a multi-wallet entity
        is_primary = (entity_primary.get(eid) == w) if len(members) > 1 else True
        s.is_entity_primary = is_primary
        if verdict == "PASS" and len(members) > 1 and not is_primary:
            verdict = "EXCLUDE"; rc.append("entity_fragment")

        # per-wallet hard excludes
        if verdict == "PASS":
            if s.unexecutable:
                verdict = "EXCLUDE"; rc.append("unexecutable_dex")
            elif s.days_since_last_fill > STALE_DAYS:
                verdict = "EXCLUDE"; rc.append("stale")
            elif s.wash_frac > WASH_EXCLUDE:
                verdict = "EXCLUDE"; rc.append("wash")
            else:
                ng, pv = s.net_gross_ratio, s.price_pnl_var_frac
                if (ng == ng and ng < NET_GROSS_NEUTRAL) and (pv == pv and pv < PRICE_VAR_NEUTRAL):
                    verdict = "EXCLUDE"; rc.append("delta_neutral")

        # REVIEW conditions (only if not already excluded)
        if verdict == "PASS":
            if s.confidence == "LOW":
                verdict = "REVIEW"; rc.append("thin_history")
            elif s.net_gross_ratio != s.net_gross_ratio or s.price_pnl_var_frac != s.price_pnl_var_frac:
                verdict = "REVIEW"; rc.append("nan_metric")
            elif s.sharpe_vs_lev_flag:
                verdict = "REVIEW"; rc.append("sharpe_too_smooth")
            elif WASH_REVIEW < s.wash_frac <= WASH_EXCLUDE:
                verdict = "REVIEW"; rc.append("wash_borderline")
            elif (s.net_gross_ratio == s.net_gross_ratio
                  and NET_GROSS_NEUTRAL <= s.net_gross_ratio < NET_GROSS_BORDER):
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
            "wash", "unexecutable_dex", "stale"}
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
