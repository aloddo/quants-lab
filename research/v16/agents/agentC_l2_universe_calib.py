"""AgentC universe execution-cost calibration (Alberto: "faster turnover less liquid pairs").

Measures per-coin HL execution costs for EVERY coin we have orderbook data for, so the copy
universe can expand beyond the 10 calibrated majors with the same rigor.

Three data sources, three quality grades (best available per coin):
  1. L2_full        hyperliquid_l2_snapshots_1s   10 majors, 20-level books @1s, May 22-27.
                    Exact book walk: half-spread + impact at $1k/$10k/$50k/$100k + shortfall.
  2. tob_ticks      arb_hl_bybit_perp_snapshots   63 pairs, real HL l2Book level-0 bid/ask
                    every 5s, Apr 25 -> Jun 11 (7k-414k ticks/pair). Measured half-spread;
                    impact MODELED from top-20 depth (hl_mm_pair_rankings) via an empirical
                    curve fit on the 10 majors' full books (leave-one-coin-out validated).
  3. rankings_only  hl_mm_pair_rankings           159 coins, live l2Book spread + top-20 depth
                    sampled ~229x over May 2-13. PROVISIONAL (n << 1000).

Impact semantics (matches app/data/v15/l2_calib_10coin.json, verified: BTC/ETH/SOL impact_10k
== half_spread exactly where level-0 depth >> $10k):
  impact_Nk_bps (JSON)   = median TOTAL VWAP cost vs mid of a $N market order (INCLUDES half-spread).
  impact_extra_Nk_bps    = median cost BEYOND half-spread (the task-report definition).
  one_way_1k_bps         = half_spread + extra_1k = total VWAP cost vs mid at $1k.
  RT_cost_1k_bps         = 2 * one_way_1k + 8.64 (HL RT taker).

Outputs (read-only elsewhere): /tmp/agentC_universe_calib.md, /tmp/agentC_l2_calib_expanded.json
Run:
  /Users/hermes/miniforge3/envs/quants-lab/bin/python research/v16/agents/agentC_l2_universe_calib.py
  ... --quick   (smoke: 4000 L2 docs, 200k ticks)
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sys
import time
from array import array
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

try:
    import orjson as _oj
    _loads = _oj.loads
except Exception:  # pragma: no cover
    _loads = json.loads

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "v15"))
from _streaming_io import install_memory_guard  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("agentC")

MONGO = "mongodb://localhost:27017"
ORIG_CALIB = Path(__file__).resolve().parents[3] / "app" / "data" / "v15" / "l2_calib_10coin.json"
OUT_MD = Path("/tmp/agentC_universe_calib.md")
OUT_JSON = Path("/tmp/agentC_l2_calib_expanded.json")
SOURCE_TAG = "agentC 2026-06-11"

FEE_RT_BPS = 8.64                      # HL RT taker (CLAUDE.md)
SIZES = (1_000.0, 10_000.0, 50_000.0, 100_000.0)
SZ_KEY = {1_000.0: "1k", 10_000.0: "10k", 50_000.0: "50k", 100_000.0: "100k"}
MIN_TICKS = 1_000                      # task gate: >= 1000 snapshots for the headline table
MIN_RANK_OBS = 30                      # rankings-only coins below this are dropped (too thin)

# Bybit '1000X' -> HL 'kX' (from data_pipeline/arb_hl_bybit_collector.py)
BYBIT_TO_HL = {"1000PEPE": "kPEPE", "1000BONK": "kBONK", "1000FLOKI": "kFLOKI",
               "1000SHIB": "kSHIB", "1000LUNC": "kLUNC", "1000XEC": "kXEC", "1000SATS": "kSATS"}


def pair_to_hl(pair: str) -> str:
    base = pair[:-5] if pair.endswith("-USDT") else pair
    return BYBIT_TO_HL.get(base, base)


# ---------------------------------------------------------------- stage 1: full L2 walk
def walk_total_bps(levels: list, mid: float, size_usd: float):
    """Total VWAP cost (bps vs mid) of a $size market order walking [(px, sz), ...] best-first.
    None if top-of-book depth can't fill it (shortfall)."""
    rem = size_usd
    base = 0.0
    for px, sz in levels:
        cap = px * sz
        if cap >= rem:
            base += rem / px
            rem = 0.0
            break
        base += sz
        rem -= cap
    if rem > 1e-9 or base <= 0:
        return None
    vwap = size_usd / base
    return abs(vwap - mid) / mid * 1e4


def stage1_l2_walk(db, limit: int = 0):
    """Stream hyperliquid_l2_snapshots_1s; per coin: spread/half-spread, per-side book-walk
    costs at SIZES, top-20 depth, shortfalls. Also pooled per-(snapshot,side) rows for the
    impact-vs-depth model. Bounded accumulators only (floats per row, never raw docs)."""
    coll = db["hyperliquid_l2_snapshots_1s"]
    proj = {"_id": 0, "coin": 1, "best_bid": 1, "best_ask": 1,
            "levels_bid_json": 1, "levels_ask_json": 1, "timestamp_utc": 1}
    cur = coll.find({}, proj).batch_size(300)
    if limit:
        cur = cur.limit(limit)

    acc: dict[str, dict] = {}
    pooled = {"coin": array("b"), "hs": array("f"), "depth": array("f"),
              "e": {k: array("f") for k in SZ_KEY.values()}}
    coin_idx: dict[str, int] = {}
    n_docs = n_bad = n_crossed = n_l0_mismatch = 0
    t_start = time.time()

    for doc in cur:
        n_docs += 1
        if n_docs % 50_000 == 0:
            log.info("stage1: %d docs (%.0fs)", n_docs, time.time() - t_start)
        try:
            bids = _loads(doc["levels_bid_json"])
            asks = _loads(doc["levels_ask_json"])
            bl = [(float(l["px"]), float(l["sz"])) for l in bids]
            al = [(float(l["px"]), float(l["sz"])) for l in asks]
        except Exception:
            n_bad += 1
            continue
        if not bl or not al:
            n_bad += 1
            continue
        b0, a0 = bl[0][0], al[0][0]
        if b0 <= 0 or a0 <= 0:
            n_bad += 1
            continue
        if b0 >= a0:
            n_crossed += 1
            continue
        if abs(b0 - float(doc.get("best_bid") or b0)) > 1e-9 or \
           abs(a0 - float(doc.get("best_ask") or a0)) > 1e-9:
            n_l0_mismatch += 1  # level0 vs stored best fields disagree (count, still use levels)

        coin = doc["coin"]
        a = acc.get(coin)
        if a is None:
            ci = coin_idx.setdefault(coin, len(coin_idx))
            a = acc[coin] = {
                "ci": ci, "n": 0, "tmin": 1 << 62, "tmax": 0,
                "spread": array("f"), "depth_bid": array("f"), "depth_ask": array("f"),
                "t": {k: array("f") for k in SZ_KEY.values()},   # total cost rows (2/snap), NaN=shortfall
                "sf": {k: 0 for k in SZ_KEY.values()},           # shortfall row counts
            }
        mid = (b0 + a0) / 2.0
        spread_bps = (a0 - b0) / mid * 1e4
        hs = spread_bps / 2.0
        d_bid = math.fsum(px * sz for px, sz in bl)
        d_ask = math.fsum(px * sz for px, sz in al)

        a["n"] += 1
        ts = int(doc.get("timestamp_utc") or 0)
        if ts:
            a["tmin"] = min(a["tmin"], ts)
            a["tmax"] = max(a["tmax"], ts)
        a["spread"].append(spread_bps)
        a["depth_bid"].append(d_bid)
        a["depth_ask"].append(d_ask)

        for side_levels, side_depth in ((al, d_ask), (bl, d_bid)):  # buy walks asks, sell walks bids
            row_e = {}
            for s in SIZES:
                k = SZ_KEY[s]
                t = walk_total_bps(side_levels, mid, s)
                if t is None:
                    a["t"][k].append(float("nan"))
                    a["sf"][k] += 1
                    row_e[k] = float("nan")
                else:
                    a["t"][k].append(t)
                    row_e[k] = max(0.0, t - hs)
            pooled["coin"].append(a["ci"])
            pooled["hs"].append(hs)
            pooled["depth"].append(side_depth)
            for k in SZ_KEY.values():
                pooled["e"][k].append(row_e[k])

    log.info("stage1 done: %d docs, %d bad, %d crossed, %d l0-mismatch (%.0fs)",
             n_docs, n_bad, n_crossed, n_l0_mismatch, time.time() - t_start)

    summary = {}
    for coin, a in acc.items():
        sp = np.frombuffer(a["spread"], dtype=np.float32)
        db_ = np.frombuffer(a["depth_bid"], dtype=np.float32)
        da_ = np.frombuffer(a["depth_ask"], dtype=np.float32)
        rows = {k: np.frombuffer(a["t"][k], dtype=np.float32) for k in SZ_KEY.values()}
        n_rows = max(len(rows["1k"]), 1)
        med_sp = float(np.median(sp))
        hs_med = med_sp / 2.0
        s = {
            "n": a["n"], "tmin": a["tmin"], "tmax": a["tmax"],
            "med_spread_bps": med_sp, "half_spread_bps": hs_med,
            "med_depth_side_usd": float(np.median(np.concatenate([db_, da_]))),
            "med_depth_sum_usd": float(np.median(db_ + da_)),
        }
        for k in SZ_KEY.values():
            t = rows[k]
            fin = t[np.isfinite(t)]
            s[f"t{k}_med"] = float(np.median(fin)) if fin.size else None
            s[f"e{k}_med"] = float(np.median(np.maximum(fin - hs_med, 0.0))) if fin.size else None
            s[f"sf{k}"] = a["sf"][k] / n_rows
        # per-row extra (vs row hs) medians for model validation
        summary[coin] = s

    pooled_np = {
        "coin": np.frombuffer(pooled["coin"], dtype=np.int8),
        "hs": np.frombuffer(pooled["hs"], dtype=np.float32),
        "depth": np.frombuffer(pooled["depth"], dtype=np.float32),
        "e": {k: np.frombuffer(pooled["e"][k], dtype=np.float32) for k in SZ_KEY.values()},
    }
    return summary, pooled_np, coin_idx, {"docs": n_docs, "bad": n_bad, "crossed": n_crossed,
                                          "l0_mismatch": n_l0_mismatch}


# ---------------------------------------------------------------- stage 2: top-of-book ticks
def stage2_ticks(db, l2_t0_ms: int, l2_t1_ms: int, quick: bool = False):
    """Server-side per-pair median half-spread from arb_hl_bybit_perp_snapshots (real HL l2Book
    level-0 quotes, unconditional 5s sampling). Full window + restricted-to-L2-window (for the
    cross-source sanity check on the 10 majors)."""
    coll = db["arb_hl_bybit_perp_snapshots"]
    spread = {"$multiply": [{"$divide": [
        {"$subtract": ["$hl_ask", "$hl_bid"]},
        {"$divide": [{"$add": ["$hl_ask", "$hl_bid"]}, 2]}]}, 10000]}

    def run(match_extra: dict | None):
        match: dict = {"hl_bid": {"$gt": 0}, "hl_ask": {"$gt": 0}}
        if match_extra:
            match.update(match_extra)
        pipe: list = [{"$match": match}]
        if quick:
            pipe.append({"$limit": 200_000})
        pipe += [
            {"$project": {"pair": 1, "timestamp": 1, "s": spread}},
            {"$match": {"s": {"$gte": 0}}},
            {"$group": {"_id": "$pair", "n": {"$sum": 1},
                        "med": {"$median": {"input": "$s", "method": "approximate"}},
                        "pct": {"$percentile": {"input": "$s", "p": [0.25, 0.75],
                                                "method": "approximate"}},
                        "tmin": {"$min": "$timestamp"}, "tmax": {"$max": "$timestamp"}}},
        ]
        return {d["_id"]: d for d in coll.aggregate(pipe, allowDiskUse=True)}

    t0 = time.time()
    full = run(None)
    log.info("stage2 full-window: %d pairs (%.0fs)", len(full), time.time() - t0)
    win = run({"timestamp": {
        "$gte": datetime.fromtimestamp(l2_t0_ms / 1000, tz=timezone.utc),
        "$lte": datetime.fromtimestamp(l2_t1_ms / 1000, tz=timezone.utc)}})
    log.info("stage2 L2-window: %d pairs", len(win))
    return full, win


# ---------------------------------------------------------------- stage 3: pair rankings
def stage3_rankings(db):
    """Per-coin medians from hl_mm_pair_rankings (live l2Book spread + top-20 USD depth,
    159 coins, May 2-13, ~229 obs/coin)."""
    coll = db["hl_mm_pair_rankings"]
    acc: dict[str, dict[str, list]] = {}
    for d in coll.find({}, {"_id": 0, "coin": 1, "spread_bps": 1, "depth_bid_usd": 1,
                            "depth_ask_usd": 1, "daily_volume_usd": 1}):
        c = d.get("coin")
        sp = d.get("spread_bps")
        if not c or sp is None or sp <= 0:
            continue
        a = acc.setdefault(c, {"sp": [], "db": [], "da": [], "vol": []})
        a["sp"].append(float(sp))
        a["db"].append(float(d.get("depth_bid_usd") or 0))
        a["da"].append(float(d.get("depth_ask_usd") or 0))
        a["vol"].append(float(d.get("daily_volume_usd") or 0))
    out = {}
    for c, a in acc.items():
        depth_side = (np.median(a["db"]) + np.median(a["da"])) / 2.0
        out[c] = {"n": len(a["sp"]), "med_spread_bps": float(np.median(a["sp"])),
                  "med_depth_side_usd": float(depth_side),
                  "med_depth_sum_usd": float(np.median(a["db"]) + np.median(a["da"])),
                  "med_daily_volume_usd": float(np.median(a["vol"]))}
    log.info("stage3: %d ranking coins", len(out))
    return out


# ---------------------------------------------------------------- stage 3b: live book walk
# WHY: hl_mm_pair_rankings depth_*_usd is CORRUPT for non-major coins (kPEPE p50=$501 vs
# max=$911k; BTC p90=$116B; ENA flat $2.5k for a $10M/day coin) -- only the 10 majors (Mongo
# L2-stats screener path) are sane. So tail impact is MEASURED directly: sample live l2Book
# (full precision, same API the arb collector uses) and walk the real book with the exact
# stage-1 code. Majors-vs-live drift table validates the procedure.
LIVE_CACHE = Path("/tmp/agentC_live_l2_samples.json")


def stage3b_live_walk(coins: list[str], rounds: int = 12, conc: int = 8,
                      refresh: bool = False) -> dict:
    """K rounds x all coins: fetch l2Book, walk both sides at SIZES. Returns
    {coin: [{hs, depth, '1k': total_bps|None, ...}, ...]} (2 rows per good fetch).
    Cached to /tmp so re-runs don't refetch."""
    if LIVE_CACHE.exists() and not refresh:
        log.info("stage3b: using cached live samples %s", LIVE_CACHE)
        return json.loads(LIVE_CACHE.read_text())
    import asyncio
    import aiohttp
    URL = "https://api.hyperliquid.xyz/info"
    samples: dict[str, list] = {c: [] for c in coins}

    async def fetch(sess, sem, coin):
        async with sem:
            try:
                async with sess.post(URL, json={"type": "l2Book", "coin": coin},
                                     timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != 200:
                        return coin, None
                    d = await r.json()
            except Exception:
                return coin, None
        lv = d.get("levels") if isinstance(d, dict) else None
        if not lv or len(lv) < 2 or not lv[0] or not lv[1]:
            return coin, None
        try:
            bids = [(float(x["px"]), float(x["sz"])) for x in lv[0]]
            asks = [(float(x["px"]), float(x["sz"])) for x in lv[1]]
        except Exception:
            return coin, None
        return coin, (bids, asks)

    async def run():
        sem = asyncio.Semaphore(conc)
        async with aiohttp.ClientSession() as sess:
            for rd in range(rounds):
                t0 = time.time()
                res = await asyncio.gather(*[fetch(sess, sem, c) for c in coins])
                ok = 0
                for coin, book in res:
                    if not book:
                        continue
                    bids, asks = book
                    b0, a0 = bids[0][0], asks[0][0]
                    if b0 <= 0 or a0 <= 0 or b0 >= a0:
                        continue
                    mid = (b0 + a0) / 2.0
                    hs = (a0 - b0) / mid / 2.0 * 1e4
                    for side in (asks, bids):
                        row = {"hs": hs,
                               "depth": math.fsum(px * sz for px, sz in side)}
                        for s in SIZES:
                            row[SZ_KEY[s]] = walk_total_bps(side, mid, s)
                        samples[coin].append(row)
                    ok += 1
                log.info("stage3b live round %d/%d: %d/%d coins ok (%.0fs)",
                         rd + 1, rounds, ok, len(coins), time.time() - t0)
                if rd < rounds - 1:
                    await asyncio.sleep(max(0.0, 20.0 - (time.time() - t0)))

    asyncio.run(run())
    LIVE_CACHE.write_text(json.dumps(samples))
    log.info("stage3b: cached -> %s", LIVE_CACHE)
    return samples


def summarize_live(samples: dict) -> dict:
    """Per coin: median live half-spread / side depth / total walk cost per size, per-row
    extra-beyond-half-spread medians, shortfall fractions. Needs >= 8 (round,side) rows."""
    out = {}
    for coin, rows in samples.items():
        if len(rows) < 4:  # >= 2 rounds x 2 sides
            continue
        hs = np.array([r["hs"] for r in rows], dtype=float)
        depth = np.array([r["depth"] for r in rows], dtype=float)
        rec = {"n_samples": len(rows), "hs_live": float(np.median(hs)),
               "depth_side_live": float(np.median(depth))}
        for k in SZ_KEY.values():
            t = np.array([r[k] if r[k] is not None else np.nan for r in rows], dtype=float)
            fin = np.isfinite(t)
            rec[f"t{k}"] = float(np.median(t[fin])) if fin.any() else None
            rec[f"sf{k}"] = float(np.mean(~fin))
            e = np.maximum(t[fin] - hs[fin], 0.0)
            rec[f"e{k}"] = float(np.median(e)) if e.size else None
        out[coin] = rec
    return out


# ---------------------------------------------------------------- impact model
BUCKET_EDGES = np.linspace(-5.0, 1.0, 31)  # log10(size/side_depth)


def build_curves(pooled, size_key: str, size_usd: float, exclude_ci: int = -1):
    """Empirical impact curves from pooled (snapshot,side) rows: x = log10(size/depth) ->
    median extra bps (abs form) and median extra/half-spread (ratio form). Plus per-bucket
    shortfall fraction."""
    m = pooled["coin"] != exclude_ci
    hs = pooled["hs"][m]
    depth = pooled["depth"][m]
    e = pooled["e"][size_key][m]
    ok_depth = depth > 0
    hs, depth, e = hs[ok_depth], depth[ok_depth], e[ok_depth]
    x = np.log10(size_usd / depth)
    fin = np.isfinite(e)
    idx = np.digitize(x, BUCKET_EDGES)
    centers, med_abs, med_ratio, sf_frac = [], [], [], []
    for b in range(1, len(BUCKET_EDGES)):
        mb = idx == b
        n_all = int(mb.sum())
        if n_all < 50:
            continue
        mb_fin = mb & fin
        if mb_fin.sum() < 30:
            continue
        eb = e[mb_fin]
        hb = hs[mb_fin]
        centers.append((BUCKET_EDGES[b - 1] + BUCKET_EDGES[b]) / 2.0)
        med_abs.append(float(np.median(eb)))
        r = eb[hb > 0] / hb[hb > 0]
        med_ratio.append(float(np.median(r)) if r.size else 0.0)
        sf_frac.append(1.0 - mb_fin.sum() / n_all)
    return (np.array(centers), np.array(med_abs), np.array(med_ratio), np.array(sf_frac))


def predict_extra(curve, r: float, hs_bps: float, form: str) -> tuple[float, bool]:
    """Interp the curve at log10(r). Returns (extra_bps, extrapolated_beyond_curve)."""
    centers, med_abs, med_ratio, _ = curve
    if centers.size == 0:
        return float("nan"), True
    lx = math.log10(max(r, 1e-12))
    extrap = lx > centers[-1]
    if form == "ratio":
        v = float(np.interp(lx, centers, med_ratio, left=0.0, right=med_ratio[-1])) * hs_bps
    else:
        v = float(np.interp(lx, centers, med_abs, left=0.0, right=med_abs[-1]))
    return max(0.0, v), bool(extrap)


def loo_validate(summary, pooled, coin_idx):
    """Leave-one-coin-out: predict each major's median extra at $1k/$10k from the other 9
    coins' pooled curves, using only that coin's (med half-spread, med side depth) -- exactly
    the information available for tail coins. Returns per-point table + MAE per form."""
    rows = []
    for coin, s in summary.items():
        ci = coin_idx[coin]
        for size in (1_000.0, 10_000.0):
            k = SZ_KEY[size]
            actual = s[f"e{k}_med"]
            if actual is None:
                continue
            curve = build_curves(pooled, k, size, exclude_ci=ci)
            r = size / max(s["med_depth_side_usd"], 1e-9)
            p_ratio, _ = predict_extra(curve, r, s["half_spread_bps"], "ratio")
            p_abs, _ = predict_extra(curve, r, s["half_spread_bps"], "abs")
            rows.append({"coin": coin, "size": k, "actual": actual,
                         "pred_ratio": p_ratio, "pred_abs": p_abs})
    mae_ratio = float(np.mean([abs(r["pred_ratio"] - r["actual"]) for r in rows]))
    mae_abs = float(np.mean([abs(r["pred_abs"] - r["actual"]) for r in rows]))
    return rows, mae_ratio, mae_abs


# ---------------------------------------------------------------- assembly
def tier_of(rt_bps: float) -> str:
    if rt_bps < 6:
        return "A"
    if rt_bps < 15:
        return "B"
    if rt_bps < 30:
        return "C"
    return "D"


def assemble(summary, pooled, tick_full, rank_stats, live_sum, form: str, orig):
    """One record per coin, best source wins: L2_full > tob_ticks > rankings_only.
    Tail impact = LIVE book walk (direct measurement); the majors-trained bucket model is the
    cross-check (model_extra_1k_bps). rankings depth is NOT used (corrupt, see stage3b note)."""
    curves = {k: build_curves(pooled, k, s) for s, k in SZ_KEY.items()}
    records = {}

    def tail_impact(coin, hs_primary):
        """(fields dict) for a non-L2 coin: live-walk extras + shortfalls + depth + model
        cross-check. hs_primary = the long-window spread measurement (ticks or rankings)."""
        lv = live_sum.get(coin)
        if not lv:
            return None
        depth = lv["depth_side_live"]
        model_e1k, _ = predict_extra(curves["1k"], 1_000.0 / max(depth, 1.0), hs_primary, form)
        f = {
            "med_depth_side_usd": round(depth),
            "med_topn_depth_usd": round(depth),
            "depth_source": "live_l2book_walk",
            "n_depth_obs": lv["n_samples"],
            "hs_live_bps": round(lv["hs_live"], 3),
            "impact_extra_1k_bps": round(lv["e1k"], 3) if lv["e1k"] is not None else float("nan"),
            "impact_extra_10k_bps": round(lv["e10k"], 3) if lv["e10k"] is not None else None,
            "impact_1k_bps": round(hs_primary + lv["e1k"], 3) if lv["e1k"] is not None else None,
            "impact_10k_bps": round(hs_primary + lv["e10k"], 3) if lv["e10k"] is not None else None,
            "impact_50k_bps": round(hs_primary + lv["e50k"], 3) if lv["e50k"] is not None else None,
            "impact_100k_bps": round(hs_primary + lv["e100k"], 3) if lv["e100k"] is not None else None,
            "shortfall_rate_1k": round(lv["sf1k"], 4),
            "shortfall_rate_10k": round(lv["sf10k"], 4),
            "model_extra_1k_bps": round(model_e1k, 3),
        }
        return f

    def finish(rec):
        hs = rec["half_spread_bps"]
        e1k = rec["impact_extra_1k_bps"]
        ow = hs + (e1k if e1k == e1k else 0.0)  # NaN-safe; NaN extra -> spread-only floor
        rec["one_way_1k_bps"] = round(ow, 3)
        rec["rt_cost_1k_bps"] = round(2 * ow + FEE_RT_BPS, 3)
        rec["rt_ex_fee_1k_bps"] = round(2 * ow, 3)
        rec["tier"] = tier_of(rec["rt_cost_1k_bps"])
        rec["tier_ex_fee"] = tier_of(rec["rt_ex_fee_1k_bps"])
        rec["source"] = SOURCE_TAG
        return rec

    # 1) L2 full-walk majors
    for coin, s in summary.items():
        records[coin] = finish({
            "quality": "L2_full", "n": s["n"],
            "window": f"{datetime.fromtimestamp(s['tmin']/1000, tz=timezone.utc):%m-%d}"
                      f"..{datetime.fromtimestamp(s['tmax']/1000, tz=timezone.utc):%m-%d}",
            "med_spread_bps": round(s["med_spread_bps"], 3),
            "half_spread_bps": round(s["half_spread_bps"], 3),
            # original-file semantics: per-SIDE median top-20 depth (verified: ETH 14.62M vs
            # orig 14.59M, BTC 5.78M vs 5.90M -- the sum-of-sides would be ~2x off)
            "med_topn_depth_usd": round(s["med_depth_side_usd"]),
            "med_depth_side_usd": round(s["med_depth_side_usd"]),
            "impact_1k_bps": round(s["t1k_med"], 3) if s["t1k_med"] is not None else None,
            "impact_10k_bps": round(s["t10k_med"], 3) if s["t10k_med"] is not None else None,
            "impact_50k_bps": round(s["t50k_med"], 3) if s["t50k_med"] is not None else None,
            "impact_100k_bps": round(s["t100k_med"], 3) if s["t100k_med"] is not None else None,
            "impact_extra_1k_bps": round(s["e1k_med"], 3) if s["e1k_med"] is not None else float("nan"),
            "impact_extra_10k_bps": round(s["e10k_med"], 3) if s["e10k_med"] is not None else None,
            "shortfall_rate_1k": round(s["sf1k"], 4),
            "shortfall_rate_10k": round(s["sf10k"], 4),
        })

    # 2) tick-measured pairs (not already L2): spread from weeks of l2Book level-0 ticks,
    #    impact from the live book walk.
    for pair, t in sorted(tick_full.items()):
        coin = pair_to_hl(pair)
        if coin in records or t["n"] < MIN_TICKS:
            continue
        hs = t["med"] / 2.0
        rk = rank_stats.get(coin)
        rec = {
            "quality": "tob_ticks", "n": int(t["n"]),
            "window": f"{t['tmin']:%m-%d}..{t['tmax']:%m-%d}",
            "med_spread_bps": round(t["med"], 3),
            "half_spread_bps": round(hs, 3),
            "spread_p25_bps": round(t["pct"][0], 3), "spread_p75_bps": round(t["pct"][1], 3),
            "med_daily_volume_usd": round(rk["med_daily_volume_usd"]) if rk else None,
        }
        ti = tail_impact(coin, hs)
        if ti:
            rec.update(ti)
        else:  # no live book (delisted / fetch failed): spread-only floor, flagged
            rec.update({"impact_1k_bps": None, "impact_10k_bps": None,
                        "impact_50k_bps": None, "impact_100k_bps": None,
                        "impact_extra_1k_bps": float("nan"), "impact_extra_10k_bps": None,
                        "shortfall_rate_1k": None, "shortfall_rate_10k": None,
                        "med_topn_depth_usd": None, "med_depth_side_usd": None,
                        "depth_source": "none_spread_only", "n_depth_obs": 0})
        records[coin] = finish(rec)

    # 3) rankings-only coins (PROVISIONAL: spread n ~30-229 << 1000); impact from live walk.
    for coin, rk in sorted(rank_stats.items()):
        if coin in records or rk["n"] < MIN_RANK_OBS:
            continue
        hs = rk["med_spread_bps"] / 2.0
        rec = {
            "quality": "rankings_only_provisional", "n": int(rk["n"]),
            "window": "05-02..05-13",
            "med_spread_bps": round(rk["med_spread_bps"], 3),
            "half_spread_bps": round(hs, 3),
            "med_daily_volume_usd": round(rk["med_daily_volume_usd"]),
        }
        ti = tail_impact(coin, hs)
        if ti:
            rec.update(ti)
        else:
            rec.update({"impact_1k_bps": None, "impact_10k_bps": None,
                        "impact_50k_bps": None, "impact_100k_bps": None,
                        "impact_extra_1k_bps": float("nan"), "impact_extra_10k_bps": None,
                        "shortfall_rate_1k": None, "shortfall_rate_10k": None,
                        "med_topn_depth_usd": None, "med_depth_side_usd": None,
                        "depth_source": "none_spread_only", "n_depth_obs": 0})
        records[coin] = finish(rec)
    return records


# ---------------------------------------------------------------- report
def fmt(v, nd=2):
    if v is None or (isinstance(v, float) and not np.isfinite(v)):
        return "-"
    return f"{v:.{nd}f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true", help="smoke: 4000 L2 docs, 200k ticks, 3 live rounds")
    ap.add_argument("--live-rounds", type=int, default=12)
    ap.add_argument("--live-refresh", action="store_true", help="ignore live-sample cache")
    args = ap.parse_args()
    install_memory_guard(soft_gb=10.0, label="agentC_calib")

    from pymongo import MongoClient
    db = MongoClient(MONGO)["quants_lab"]
    orig = json.loads(ORIG_CALIB.read_text())

    # ---- stage 1
    summary, pooled, coin_idx, s1meta = stage1_l2_walk(db, limit=4000 if args.quick else 0)

    # ---- sanity gate vs original calib (method must reproduce the knowns)
    sane = []
    for coin, o in orig.items():
        s = summary.get(coin)
        if not s:
            continue
        sane.append((coin, s["med_spread_bps"], o["med_spread_bps"],
                     s["t10k_med"], o["impact_10k_bps"],
                     s["med_depth_sum_usd"], s["med_depth_side_usd"], o["med_topn_depth_usd"]))
    log.info("== sanity vs l2_calib_10coin (mine | orig): spread, impact10k_total, depth(sum|side|orig)")
    for c, ms, os_, mt, ot, dsum, dside, od in sane:
        log.info("  %-5s spread %.3f|%.3f  imp10k %.3f|%.3f  depth %.0f|%.0f|%.0f",
                 c, ms, os_, mt if mt else float("nan"), ot if ot else float("nan"), dsum, dside, od)
    btc, eth = summary.get("BTC"), summary.get("ETH")
    if btc and not (0.04 <= btc["half_spread_bps"] <= 0.30):
        log.error("SANITY FAIL: BTC half-spread %.3f outside [0.04,0.30] -- method broken, aborting",
                  btc["half_spread_bps"])
        sys.exit(2)
    if eth and not (0.10 <= eth["half_spread_bps"] <= 0.90):
        log.error("SANITY FAIL: ETH half-spread %.3f outside [0.10,0.90] -- aborting",
                  eth["half_spread_bps"])
        sys.exit(2)

    l2_t0 = min(s["tmin"] for s in summary.values())
    l2_t1 = max(s["tmax"] for s in summary.values())

    # ---- stages 2+3
    tick_full, tick_win = stage2_ticks(db, l2_t0, l2_t1, quick=args.quick)
    rank_stats = stage3_rankings(db)

    # ---- cross-source checks on the majors
    cross = []
    for coin in summary:
        pair = next((p for p in tick_full if pair_to_hl(p) == coin), None)
        tw = tick_win.get(pair) if pair else None
        tf = tick_full.get(pair) if pair else None
        rk = rank_stats.get(coin)
        cross.append({
            "coin": coin, "l2_spread": summary[coin]["med_spread_bps"],
            "tick_win_spread": tw["med"] if tw else None,
            "tick_full_spread": tf["med"] if tf else None,
            "rank_spread": rk["med_spread_bps"] if rk else None,
            "l2_depth_side": summary[coin]["med_depth_side_usd"],
            "rank_depth_side": rk["med_depth_side_usd"] if rk else None,
        })

    # ---- impact model validation (LOO by coin) -- used as CROSS-CHECK column for tail coins
    loo_rows, mae_ratio, mae_abs = loo_validate(summary, pooled, coin_idx)
    form = "ratio" if mae_ratio <= mae_abs else "abs"
    log.info("LOO impact model: MAE ratio-form %.3f bps | abs-form %.3f bps -> using %s",
             mae_ratio, mae_abs, form)

    # ---- stage 3b: live book walk for the whole universe (tail impact MEASUREMENT)
    live_coins = sorted({pair_to_hl(p) for p in tick_full}
                        | {c for c, r in rank_stats.items() if r["n"] >= MIN_RANK_OBS}
                        | set(summary))
    live_raw = stage3b_live_walk(live_coins, rounds=3 if args.quick else args.live_rounds,
                                 refresh=args.live_refresh)
    live_sum = summarize_live(live_raw)
    log.info("stage3b: %d/%d coins with usable live books", len(live_sum), len(live_coins))

    # majors: live walk vs May L2 medians (regime-drift validation of the live procedure)
    drift = []
    for coin, s in summary.items():
        lv = live_sum.get(coin)
        if lv:
            drift.append({"coin": coin, "hs_l2": s["half_spread_bps"], "hs_live": lv["hs_live"],
                          "e10k_l2": s["e10k_med"], "e10k_live": lv["e10k"],
                          "d_l2": s["med_depth_side_usd"], "d_live": lv["depth_side_live"]})
            log.info("  drift %-5s hs %.3f->%.3f  e10k %.3f->%.3f  depth %.0fk->%.0fk",
                     coin, s["half_spread_bps"], lv["hs_live"],
                     s["e10k_med"] or 0, lv["e10k"] or 0,
                     s["med_depth_side_usd"] / 1e3, lv["depth_side_live"] / 1e3)

    # ---- assemble + write
    records = assemble(summary, pooled, tick_full, rank_stats, live_sum, form, orig)
    OUT_JSON.write_text(json.dumps(
        {c: records[c] for c in sorted(records, key=lambda c: records[c]["rt_cost_1k_bps"])},
        indent=1, default=str))
    log.info("wrote %s (%d coins)", OUT_JSON, len(records))

    # ---- markdown report
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    measured = {c: r for c, r in records.items() if r["quality"] in ("L2_full", "tob_ticks")}
    prov = {c: r for c, r in records.items() if r["quality"] == "rankings_only_provisional"}
    by_cost = sorted(measured.items(), key=lambda kv: kv[1]["rt_cost_1k_bps"])
    prov_by_cost = sorted(prov.items(), key=lambda kv: kv[1]["rt_cost_1k_bps"])
    tiers = {"A": [], "B": [], "C": [], "D": []}
    for c, r in by_cost:
        tiers[r["tier"]].append(c)

    L = []
    L.append(f"# AgentC: HL Universe Execution-Cost Calibration ({now})\n")
    L.append("Mission: expand the copy-strategy universe beyond the 10 calibrated majors "
             "(Alberto: 'faster turnover less liquid pairs if it works'). Per-coin half-spread, "
             "$1k/$10k market-order impact, shortfall, RT cost @ $1k, tiering.\n")
    L.append("## Data sources & quality grades\n")
    L.append("| grade | source | coins | obs/coin | window | what is measured |")
    L.append("|---|---|---|---|---|---|")
    L.append(f"| L2_full | hyperliquid_l2_snapshots_1s | {len(summary)} | 35k-41k | "
             f"May 22-27 | 20-level book walk: exact spread+impact+shortfall |")
    L.append(f"| tob_ticks | arb_hl_bybit_perp_snapshots | "
             f"{sum(1 for r in measured.values() if r['quality']=='tob_ticks')} | 7k-414k | "
             f"Apr 25-Jun 11 | real HL l2Book level-0 spread (5s unconditional poll) |")
    L.append(f"| rankings_only | hl_mm_pair_rankings | {len(prov)} | ~30-229 | May 2-13 | "
             f"live l2Book spread at screener cadence. PROVISIONAL (n<1000) |")
    live_n_med = int(np.median([v["n_samples"] for v in live_sum.values()])) if live_sum else 0
    L.append(f"| live_walk | api.hyperliquid.xyz l2Book | {len(live_sum)} | "
             f"~{live_n_med} (rounds x 2 sides) | "
             f"2026-06-11 | direct $1k/$10k/$50k/$100k book walks on the REAL current book -- "
             f"impact + depth + shortfall for every non-major |\n")
    L.append("The 1s L2 collection only covers the 10 majors -- it was built for the maker/copy "
             "calibration. Tail spread comes from the tick feed (weeks of data); tail impact is "
             "walked DIRECTLY on live books (today), validated by the majors' drift table below.\n")

    L.append("### Data-quality finding (matters beyond this report)\n")
    L.append("`hl_mm_pair_rankings.depth_{bid,ask}_usd` is CORRUPT for non-major coins: kPEPE "
             "p50=$501 (max $911k), PENGU p50=$169, ENA flat ~$2.5k for a $10M/day coin, and BTC "
             "p90=$116B. Only the 10 majors (Mongo L2-stats screener path) are sane; the REST "
             "l2Book screener path that produced the tail rows is buggy. Do NOT consume those "
             "depth fields anywhere (the MM pair scorer's depth_factor is affected too). This is "
             "why tail impact here is measured by live walks instead.\n")

    L.append("## Method\n")
    L.append("- half_spread_bps = (ask0-bid0)/2/mid * 1e4, median over snapshots (per-coin best "
             "long-window source: L2 1s collection or 5s tick feed or screener rows).")
    L.append("- impact (report tables) = median cost BEYOND half-spread of walking the book to "
             "fill a $N market order (both sides pooled). JSON `impact_Nk_bps` keeps the original "
             "file's semantics = TOTAL VWAP cost vs mid (includes half-spread), verified by "
             "reproducing l2_calib_10coin.json. For tail coins impact_Nk = long-window "
             "half-spread + live-walked extra.")
    L.append("- shortfall_rate = fraction of (snapshot,side) where top-20 depth < order size "
             "(majors: 35-41k snapshots; tail: live-walk samples).")
    L.append("- one_way_1k_bps = half_spread + impact_extra_1k (= total VWAP cost vs mid at $1k).")
    L.append(f"- RT_cost_1k_bps = 2 x one_way + {FEE_RT_BPS} (HL RT taker).")
    L.append(f"- Majors-trained impact curve (median extra vs log10(order/side-depth), 760k "
             f"(snapshot,side) rows, LOO-validated: ratio-form MAE {mae_ratio:.3f} bps vs abs "
             f"{mae_abs:.3f}) is kept as a CROSS-CHECK column (model_extra_1k_bps in the JSON) "
             f"against the live walks.\n")

    L.append("## Sanity check vs existing 10-coin calib\n")
    L.append("Same collection, full 380k snapshots vs the original n=3000 sample. My impact "
             "columns here use the ORIGINAL total-cost semantics for comparability.\n")
    L.append("| coin | spread mine/orig | impact10k mine/orig | top20 SIDE depth mine/orig |")
    L.append("|---|---|---|---|")
    for c, ms, os_, mt, ot, dsum, dside, od in sane:
        L.append(f"| {c} | {fmt(ms,3)} / {fmt(os_,3)} | {fmt(mt,3)} / {fmt(ot,3)} | "
                 f"{dside:,.0f} / {od:,.0f} |")
    L.append("")
    L.append("Cross-source agreement on the majors (median spread bps):\n")
    L.append("| coin | L2 (May22-27) | ticks same window | ticks full (Apr25-Jun11) | rankings (May2-13) | depth L2-side / rankings-side |")
    L.append("|---|---|---|---|---|---|")
    for r in cross:
        L.append(f"| {r['coin']} | {fmt(r['l2_spread'],3)} | {fmt(r['tick_win_spread'],3)} | "
                 f"{fmt(r['tick_full_spread'],3)} | {fmt(r['rank_spread'],3)} | "
                 f"{r['l2_depth_side']:,.0f} / "
                 f"{(r['rank_depth_side'] or 0):,.0f} |")
    L.append("")
    L.append("Majors, May L2 medians vs TODAY's live walk (validates using live walks for the "
             "tail; differences = real regime drift + thin live sample):\n")
    L.append("| coin | half-spread May -> live | extra@10k May -> live | side depth May -> live |")
    L.append("|---|---|---|---|")
    for d in drift:
        L.append(f"| {d['coin']} | {fmt(d['hs_l2'],3)} -> {fmt(d['hs_live'],3)} | "
                 f"{fmt(d['e10k_l2'],3)} -> {fmt(d['e10k_live'],3)} | "
                 f"{d['d_l2']/1e3:,.0f}k -> {d['d_live']/1e3:,.0f}k |")
    L.append("")
    L.append("LOO impact-model validation (predict each major's measured extra-cost from the "
             "other 9 coins only; the model is the cross-check column for tail coins):\n")
    L.append("| coin | size | actual extra bps | pred(ratio) | pred(abs) |")
    L.append("|---|---|---|---|---|")
    for r in loo_rows:
        L.append(f"| {r['coin']} | {r['size']} | {fmt(r['actual'],3)} | "
                 f"{fmt(r['pred_ratio'],3)} | {fmt(r['pred_abs'],3)} |")
    L.append("")

    L.append("## Main table -- measured coins (>= 1000 snapshots), sorted by total one-way cost @ $1k\n")
    L.append("impact columns = EXTRA beyond half-spread. RT all-in = 2x one-way + 8.64 taker fees.\n")
    L.append("| coin | quality | n | half_spread | impact_1k | impact_10k | shortfall10k | one_way_1k | RT_1k all-in | RT_1k ex-fee | tier | tier ex-fee |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for c, r in by_cost:
        sf = r["shortfall_rate_10k"]
        sf_s = fmt(sf * 100, 2) + "%" if isinstance(sf, float) else "-"
        L.append(f"| {c} | {r['quality']} | {r['n']:,} | {fmt(r['half_spread_bps'],3)} | "
                 f"{fmt(r['impact_extra_1k_bps'],3)} | {fmt(r['impact_extra_10k_bps'],3)} | {sf_s} | "
                 f"{fmt(r['one_way_1k_bps'],3)} | {fmt(r['rt_cost_1k_bps'],2)} | "
                 f"{fmt(r['rt_ex_fee_1k_bps'],2)} | {r['tier']} | {r['tier_ex_fee']} |")
    L.append("")
    L.append(f"NOTE: the 8.64bps taker-fee floor alone exceeds the 6bps tier-A bound, so tier A "
             f"is structurally EMPTY under the all-in definition -- BTC itself lands in B. The "
             f"ex-fee tier (pure venue execution cost, 2x one-way) is the discriminating view.\n")

    L.append("## Provisional coins (rankings only, n < 1000 -- screen, do not size off these)\n")
    L.append("| coin | n | half_spread | impact_1k | one_way_1k | RT_1k all-in | tier | med daily vol |")
    L.append("|---|---|---|---|---|---|---|---|")
    for c, r in prov_by_cost:
        L.append(f"| {c} | {r['n']} | {fmt(r['half_spread_bps'],3)} | "
                 f"{fmt(r['impact_extra_1k_bps'],3)} | {fmt(r['one_way_1k_bps'],3)} | "
                 f"{fmt(r['rt_cost_1k_bps'],2)} | {r['tier']} | "
                 f"{(r['med_daily_volume_usd'] or 0)/1e6:,.1f}M |")
    L.append("")

    L.append("## Tier counts (measured coins, all-in definition)\n")
    for t in "ABCD":
        L.append(f"- Tier {t}: {len(tiers[t])} -- {', '.join(tiers[t]) if tiers[t] else '(empty)'}")
    L.append("")
    L.append("## Caveats\n")
    L.append("- Tail impact/depth/shortfall are from TODAY's live books (~24 walk samples per "
             "coin over ~4 min), not weeks of snapshots. The majors' drift table bounds the "
             "regime error; spread (the dominant $1k cost) is from weeks of ticks regardless.")
    L.append("- Tail spread (tick feed) is the median over Apr 25-Jun 11; coins whose collection "
             "window was short (e.g. ME, BIO, MEGA ~7k ticks over 1-2 days) are weaker.")
    L.append("- Coins with depth_source=none_spread_only failed the live fetch (delisted or "
             "renamed): impact unknown, one_way floored at half-spread -- do not trade these "
             "without a fresh book check.")
    L.append("- At our actual $50-400 copy sizes, costs are <= the $1k numbers (still >= "
             "half-spread + fees).")
    L.append("")
    L.append("## Recommendation\n")
    L.append("1. For any expansion shortlist coin, extend the micro-1s L2 collector "
             "(archive/scripts/collect_hyperliquid_micro_1s.py pattern) to it for 3-5 days and "
             "re-run this script -- the coin upgrades to L2_full automatically.")
    L.append("2. Fix or stop consuming hl_mm_pair_rankings depth fields (corrupt for non-majors; "
             "see finding above).")
    L.append("3. JSON: /tmp/agentC_l2_calib_expanded.json (every coin tagged source='agentC "
             "2026-06-11' + quality grade). Original l2_calib_10coin.json untouched. If wired "
             "into execution_model.py, note its slip formula adds half_spread to impact_10k "
             "(deliberate conservatism) -- consistent with how the 10 majors are priced today.\n")
    OUT_MD.write_text("\n".join(L))
    log.info("wrote %s", OUT_MD)

    # console summary
    print("\n=== TIER TABLE (measured, sorted by RT all-in @ $1k) ===")
    for c, r in by_cost:
        print(f"{c:>10s} {r['quality']:<10s} n={r['n']:>7,} hs={r['half_spread_bps']:>7.3f} "
              f"e1k={r['impact_extra_1k_bps']:>6.3f} ow={r['one_way_1k_bps']:>7.3f} "
              f"RT={r['rt_cost_1k_bps']:>6.2f} tier={r['tier']}/{r['tier_ex_fee']}")
    print(f"\ntiers: " + " ".join(f"{t}={len(tiers[t])}" for t in "ABCD") +
          f" | provisional={len(prov)}")


if __name__ == "__main__":
    main()
