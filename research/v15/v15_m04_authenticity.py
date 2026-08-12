#!/usr/bin/env python3
"""V15 M4 — Authenticity kills + entity dedup (confidence tiers).

The CHEAP/PROVABLE stage of the former M0e (split 2026-05-31: M4 cheap on-HL kills + entity
dedup PRE-engine; M8 counterfactual-survival tiering POST-engine). Design codex-SHIP:
projects/quant/v15/modules/m4-design.

REUSES the codex-SHIP m025 gate helpers VERBATIM (STAGE A scalars, STAGE B entity union-find,
STAGE C internal-hedge) and REWRITES only the COMBINE step: PASS/EXCLUDE/REVIEW -> a danger-only
CONFIDENCE TIER {KILL 0 / UNCERTAIN 0.25 / SUSPICIOUS 0.05-0.1 / CLEAN 1.0} that sets an
ALLOCATION-WEIGHT multiplier, + a clean (lower-bound) entity-dedup map.

Boundary (binding): M4 = DANGER only. NOT quality (-> M5), NOT copyability (-> M5: hold-time vs
latency, liquidity, accessible mechanics), NOT survival (-> M8). (M3 = fold geometry, separate.)
KILL only on PROVABLE on-HL signals; inferential -> SUSPICIOUS; thin -> UNCERTAIN.

AS-OF: every signal uses only data with ts < as_of. Backtest fold k consumes the run as-of
M3 test_start[k] (fold-pure). A full-window run (--as-of END) = ex-post universe curation (live).

CLI:
    python v15_m04_authenticity.py --wallets-file W.txt --as-of 2026-02-09 \
        --out app/data/v15/m04_authenticity_f1.parquet \
        --entities-out app/data/v15/m04_entities_f1.parquet [--lookback-days 90]
"""
from __future__ import annotations

import argparse
import gc
import logging
import os
import sys
import time
from collections import Counter
from multiprocessing import Pool
from pathlib import Path

import pandas as pd

sys.path.insert(0, "/Users/hermes/quants-lab/research/v15")
import v15_m025_authenticity_gate as g  # noqa: E402  (codex-SHIP helpers)
import v15_m01_equity_reconstruct as m01  # noqa: E402
import hl_fills_io as fio  # noqa: E402  (consolidated HOT fills store — same source M02 builds from)
from _streaming_io import install_memory_guard, plan_memory_budget, require_mem_safe_run  # noqa: E402

# Route M4 fills through the wallet-partitioned shard (per-wallet partition reads) instead of re-scanning the
# 11GB store per chunk. That re-read is memory-MAPPED/file-backed -> consumed physical RAM (avail->2.8G, CoS
# kill 2026-07-17) AND was slow. Byte-identical (order_wallet_fills_causally). Falls back to the day-file scan
# if the shard is absent/incomplete or does not cover the requested window.
_M04_FILLS_SHARD = os.environ.get(
    "QL_M04_FILLS_SHARD", str(Path(__file__).resolve().parents[1] / "app" / "data" / "v15" / "m2_fills_wallet_shards"))


def _grouped_ff(wallets, lo, hi):
    if _M04_FILLS_SHARD and os.path.exists(os.path.join(_M04_FILLS_SHARD, "._complete")):
        return fio.load_grouped_fills_funding_sharded(wallets, lo, hi, _M04_FILLS_SHARD)
    return fio.load_grouped_fills_funding(wallets, lo, hi)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [m04] %(message)s", stream=sys.stdout)
log = logging.getLogger("m04")

# Tier -> allocation-weight multiplier (codex r2: weight order is SEPARATE from tier precedence).
ALLOC_WEIGHT = {"KILL": 0.0, "SUSPICIOUS": 0.10, "UNCERTAIN": 0.25, "CLEAN": 1.0}
# Tier precedence (which tier wins when several fire): KILL > UNCERTAIN > SUSPICIOUS > CLEAN.
TIER_PRECEDENCE = {"KILL": 0, "UNCERTAIN": 1, "SUSPICIOUS": 2, "CLEAN": 3}


def _own_tier(s: "g.WalletScores") -> tuple[str, list[str]]:
    """Per-wallet DANGER tier from the wallet's own signals (NO entity-level logic here).

    Precedence KILL > UNCERTAIN > SUSPICIOUS > CLEAN; evaluate all, most-severe FIRED tier wins.
    """
    rc: list[str] = []
    kill, uncertain, suspicious = [], [], []

    ng, pv = s.net_gross_ratio, s.price_pnl_var_frac
    # --- KILL: provable on-HL disqualifiers ---
    if s.wash_frac > g.WASH_EXCLUDE:
        kill.append("wash")
    if (ng == ng and ng < g.NET_GROSS_NEUTRAL) and (pv == pv and pv < g.PRICE_VAR_NEUTRAL):
        kill.append("delta_neutral")            # BOTH conditions required (I5)
    if s.funding_frac == s.funding_frac and s.funding_frac > g.FUNDING_FARM_FRAC:
        kill.append("carry_pnl")                # funding-dominated; out-of-scope by strategy def
    # --- UNCERTAIN: thin / unknown evidence (dominates suspicious) ---
    if s.confidence == "LOW":
        uncertain.append(s.anchor_reason or "thin_history")
    if ng != ng or pv != pv:
        uncertain.append("nan_metric")
    # --- SUSPICIOUS: cheap inferential smell ---
    if g.WASH_REVIEW < s.wash_frac <= g.WASH_EXCLUDE:
        suspicious.append("wash_borderline")
    # quality (NOT danger) reason_codes carried for M5, never tiering:
    if ng == ng and ng < g.NET_GROSS_NEUTRAL:
        rc.append("q:low_net_gross")            # q: = quality signal handed to M5
    elif ng == ng and g.NET_GROSS_NEUTRAL <= ng < g.NET_GROSS_BORDER:
        rc.append("q:net_gross_borderline")

    if kill:
        return "KILL", kill + rc
    if uncertain:
        return "UNCERTAIN", uncertain + rc
    if suspicious:
        return "SUSPICIOUS", suspicious + rc
    # CLEAN requires positive directional evidence + sufficient confidence
    if s.l3_pass_standalone and s.confidence != "LOW":
        return "CLEAN", rc
    # fallback: valid-but-not-directional, no danger -> UNCERTAIN (can't confirm clean)
    return "UNCERTAIN", (["not_directional"] + rc)


def _stage_a_worker(args):
    """Top-level (picklable) STAGE-A worker: pure per-wallet scalars (disk anchor cache + per-wallet
    fills + page-cached marks; no shared global). Returns (wallet, WalletScores)."""
    w, lo_ms, hi_ms = args
    return w, g.stage_a(w, lo_ms, hi_ms)


def _worker_init(label: str = "m04-worker", soft_gb: float = 12.0) -> None:
    """Pool initializer: install the Rule 8 memory guard ONCE at worker-process start (not per
    task — a per-call guard would leak a watchdog thread per imap chunk). Each Pool worker is a
    separate process and needs its own guard so a runaway aborts LOUDLY, not via silent SIGKILL.
    soft_gb comes from the aggregate plan_memory_budget so worker guards sum within physical RAM."""
    install_memory_guard(soft_gb=soft_gb, label=label)


def _fills_worker(args):
    """Top-level (picklable) worker: load one wallet's fills (the slow sequential I/O in STAGE C's
    internal-hedge check). Returns (wallet, fills_list). Logic-neutral: only the I/O is parallelized;
    the hedge COMPUTATION stays in the main loop, unchanged."""
    w, lo_ms, hi_ms = args
    return w, fio.load_wallet_fills(w, lo_ms - 365 * 86_400_000, hi_ms)


def run(wallets, lo_ms, hi_ms, as_of_ms, procs: int = 1, worker_soft_gb: float = 12.0,
        hot_prefetch: bool = False, cached_scores: dict | None = None,
        delta_wallets=None, return_scores: bool = False):
    # PHASE-2b DAILY-INCREMENTAL (2026-07-16): cached_scores = prior STAGE-A WalletScores per wallet.
    # STAGE A is the dominant cost (~14.7min/2000 wallets); B/C are cheap (~0.4min). For an incremental daily
    # run we recompute STAGE A ONLY for wallets whose journeys/fills changed (delta_wallets) or are missing
    # from the cache, reuse the cached scores for the rest, then re-run STAGE B (union-find) + STAGE C fully
    # (cheap, deterministic over the scalar set) -> ROW-IDENTICAL to a full run by construction (STAGE A is
    # pure per-wallet; B/C depend only on the scores set). return_scores=True returns (df, scores) so the
    # daily driver can persist the STAGE-A cache.
    # NOTE: default procs=1 (sequential) so in-process callers + monkeypatched tests work; main()
    # passes the aggregate-budget-capped procs for the real parallel run. Pool workers are separate
    # processes and do NOT see a parent monkeypatch of stage_a.
    #
    # UNIFIED SINGLE-PASS SOURCE (2026-07-16, hot_prefetch): read fills+funding+ledger for the WHOLE
    # universe ONCE per fold from the fio hot store (the single canonical source; the stale m01
    # per-wallet parquet is retired) and REDIRECT the m01 per-wallet loaders + fio.load_wallet_fills
    # to serve from that in-memory cache. Stage A/B/C then do pure compute with ZERO per-wallet
    # re-reads. The redirect is in-process, so STAGE A runs SEQUENTIALLY (procs forced to 1) — the
    # per-wallet I/O that made parallelism worthwhile is gone. No restore: each fold is a fresh
    # process (mem_safe_run spawns python per fold), so leaving the module loaders redirected is safe.
    # hot_prefetch memory model (codex P1 fix): the whole-universe fills prefetch (~17M dicts, >10GB)
    # blows the process budget. Instead: prefetch ONLY the small ledger globally (build_entities needs
    # it for ALL wallets at once) over the narrow [lo,hi] window (codex P2), and run STAGE A in bounded
    # CHUNKS, each loading its own fills+funding once then freeing them. All redirected loaders are
    # RESTORED in finally (codex P2). procs forced to 1 (the in-process redirect can't cross Pool workers).
    _hp_orig = None
    if hot_prefetch:
        procs = 1
        pre_lo = lo_ms - 365 * 86_400_000  # widest lookback stage_a/stage_c fills need
        _hp_orig = (m01.load_wallet_fills, m01.load_wallet_funding, m01.load_wallet_ledger,
                    fio.load_wallet_fills)
        log.info(f"PREFETCH ledger (global, [lo,hi]) for {len(wallets)} wallets")
        _t_pre = time.time()
        _ledger_full = fio.load_grouped_ledger(set(wallets), lo_ms, hi_ms)

        # GC FREEZE (2026-08-12) -- this is what made M4 look "hung".
        # _ledger_full is a dict of ~11.6k wallets -> lists of entry dicts: MILLIONS of long-lived
        # container objects, every one of them GC-tracked. STAGE A then allocates continuously (the
        # _cache_ledger comprehension below builds a fresh list per wallet), and CPython triggers a
        # gen-2 collection on allocation COUNT, not on how much is actually garbage. So every gen-2
        # pass re-traversed the entire prefetch cache to prove it was still reachable.
        #
        # Measured live on 2026-08-12: two independent 3s samples of the running process put
        # 2180/2180 and 2136/2180 stack samples inside gc_collect_main -> list_traverse ->
        # visit_decref. The process was at 16% CPU and had emitted ZERO of its per-500-wallet
        # progress lines in 89 minutes. It was not hung and it had not crashed -- it was spending
        # essentially all of its time garbage-collecting a cache that is immutable by construction.
        # This is also the most likely explanation for the 2026-08-10 run that announced STAGE A and
        # was never heard from again.
        #
        # gc.freeze() moves everything currently tracked into a permanent generation the collector
        # never examines again. The cache is read-only for the rest of the run, so nothing here can
        # become garbage; new allocations are still collected normally. Cheap, and it removes the
        # cache from every future traversal.
        _n_before = len(gc.get_objects())
        gc.collect()
        gc.freeze()
        log.info(f"PREFETCH done in {time.time()-_t_pre:.1f}s; gc.freeze() applied to "
                 f"~{_n_before:,} tracked objects (the prefetch cache is immutable for the rest of "
                 f"the run; without this, every gen-2 pass re-walks it -- measured 98%+ of wall time)")

        def _cache_ledger(w, t0, t1, _c=_ledger_full):
            return [e for e in _c.get(str(w).lower(), []) if t0 <= int(e["time"]) <= t1]
        m01.load_wallet_ledger = _cache_ledger  # spans STAGE A (per-wallet) + STAGE B (build_entities)

    scores = {}
    # INCREMENTAL: reuse cached STAGE-A scores for unchanged wallets; recompute only delta/missing wallets.
    _delta = {str(w).lower() for w in (delta_wallets or [])}
    if cached_scores:
        for w in wallets:
            if w in cached_scores and str(w).lower() not in _delta:
                scores[w] = cached_scores[w]
    recompute = [w for w in wallets if w not in scores]
    log.info(f"STAGE A: {len(recompute)}/{len(wallets)} wallets need recompute "
             f"(cached={len(scores)}, as-of {as_of_ms}), procs={procs}")
    t0 = time.time()
    if hot_prefetch:
        # bounded-memory chunks: hold ONE chunk's fills+funding at a time, free before the next.
        CHUNK = int(os.environ.get("QL_M04_STAGEA_CHUNK", "5000"))
        try:
            for ci in range(0, len(recompute), CHUNK):
                chunk = recompute[ci:ci + CHUNK]
                cf, cfu = _grouped_ff(set(chunk), pre_lo, hi_ms)
                m01.load_wallet_fills = (lambda w, a, b, _c=cf:
                                         [f for f in _c.get(str(w).lower(), []) if a <= f["time"] <= b])
                fio.load_wallet_fills = m01.load_wallet_fills
                m01.load_wallet_funding = (lambda w, a, b, _c=cfu:
                                           [x for x in _c.get(str(w).lower(), []) if a <= int(x["time"]) <= b])
                for w in chunk:
                    scores[w] = g.stage_a(w, lo_ms, hi_ms)
                del cf, cfu
                log.info(f"  A [{min(ci+CHUNK,len(wallets))}/{len(wallets)}] ({(time.time()-t0)/60:.1f}min)")
        finally:
            # restore fills+funding loaders (ledger stays redirected through STAGE B)
            m01.load_wallet_fills, m01.load_wallet_funding = _hp_orig[0], _hp_orig[1]
            fio.load_wallet_fills = _hp_orig[3]
    elif procs > 1:
        tasks = [(w, lo_ms, hi_ms) for w in recompute]
        with Pool(procs, initializer=_worker_init, initargs=("m04-stageA", worker_soft_gb)) as pool:
            for i, (w, sc) in enumerate(pool.imap_unordered(_stage_a_worker, tasks, chunksize=16), 1):
                scores[w] = sc
                if i % 2000 == 0:
                    log.info(f"  A [{i}/{len(recompute)}] ({(time.time()-t0)/60:.1f}min)")
    else:
        for i, w in enumerate(recompute, 1):
            scores[w] = g.stage_a(w, lo_ms, hi_ms)
            if i % 500 == 0:
                log.info(f"  A [{i}/{len(recompute)}] ({(time.time()-t0)/60:.1f}min)")
    log.info(f"STAGE A done in {(time.time()-t0)/60:.1f}min")

    log.info("STAGE B: entities (union-find)")
    ent_id, ent_members = g.build_entities(wallets, lo_ms, hi_ms)
    if hot_prefetch:  # ledger no longer needed after build_entities -> restore + free (codex P2)
        m01.load_wallet_ledger = _hp_orig[2]
        _ledger_full = None
    for w in wallets:
        scores[w].entity_id = ent_id[w]

    # own danger tier per wallet (no entity logic) — needed by entity resolution + combine.
    own = {w: _own_tier(scores[w]) for w in wallets}

    log.info("STAGE C: entity resolution + internal-hedge")
    fills_cache = {}

    # STAGE C memory bound (2026-07-18): the internal-hedge check only ever needs a given entity's OWN
    # members' fills, and each wallet belongs to exactly ONE union-find entity -> once an entity is checked
    # its members' fills are never read again. So we load an entity's members RIGHT BEFORE its hedge check
    # and EVICT them right after (see the entity loop below). The old design eager-preloaded EVERY hedge
    # wallet's fills at once and held them all simultaneously; for later folds (365d lookback -> many hedge
    # wallets) that was a ~2GB+ resident peak that bled system-available RAM below the mem_safe_run floor at
    # STAGE C and got the fold killed on the tight box (verified: fold 3 killed at STAGE C every attempt).
    # Per-entity load via the wallet-shard is a cheap partition-pruned read; get_fills output is
    # BYTE-IDENTICAL to the old per-wallet path (hl_fills_io equivalence + shard gate), so tiers are unchanged.
    def get_fills(w):
        if w not in fills_cache:
            _fb, _ = _grouped_ff({w}, lo_ms - 365 * 86_400_000, hi_ms)
            fills_cache[w] = _fb.get(str(w).lower(), [])  # grouped loader keys are lowercased (HL addrs)
        return fills_cache[w]

    entity_primary = {}     # eid -> wallet or None
    entity_tier = {}        # eid -> tier
    entity_codes = {}       # eid -> [codes]
    entity_evidence = {}    # eid -> link evidence
    entity_conf = {}        # eid -> high|medium
    _hedge_entities = []    # (eid, members, primary) that reach the fills-based internal-hedge check

    for eid, members in ent_members.items():
        # link evidence/confidence for the dedup map (lower-bound graph, stated honestly)
        if len(members) == 1:
            entity_evidence[eid] = "none"
            entity_conf[eid] = "high"
        else:
            entity_evidence[eid] = "transfer_or_funder_match"
            entity_conf[eid] = "medium"

        if len(members) == 1:
            entity_primary[eid] = members[0]
            entity_tier[eid] = None  # resolved per-wallet below
            continue
        # codex code-r2: a provable own-KILL on ANY member (wash/carry/delta_neutral) is the
        # survivorship/manipulation trick (4c) — it KILLs the whole ENTITY, regardless of size /
        # primary. Provable on-HL, so this is a hard KILL, not inference.
        kill_members = [m for m in members if own[m][0] == "KILL"]
        if kill_members:
            entity_primary[eid] = None
            entity_tier[eid] = "KILL"
            entity_codes[eid] = ["entity_member_kill"]
            continue
        if len(members) > g.ENTITY_MAX_WALLETS:
            entity_primary[eid] = None
            entity_tier[eid] = "UNCERTAIN"
            entity_codes[eid] = ["entity_too_big"]
            continue
        passers = [w for w in members if scores[w].l3_pass_standalone]
        if not passers:
            def _l3_known(w):
                ng, pv = scores[w].net_gross_ratio, scores[w].price_pnl_var_frac
                return ng == ng and pv == pv
            # bare entity_no_l3_passer -> UNCERTAIN (NOT KILL); l3_unknown -> UNCERTAIN
            code = "entity_no_l3_passer" if all(_l3_known(w) for w in members) else "entity_l3_unknown"
            entity_primary[eid] = None
            entity_tier[eid] = "UNCERTAIN"
            entity_codes[eid] = [code]
            continue
        primary = max(passers, key=lambda w: (scores[w].sharpe
                                              if scores[w].sharpe == scores[w].sharpe else -1e9))
        entity_primary[eid] = primary
        # Defer the fills-based internal-hedge check to the BATCHED pass below. Provisional tier None;
        # overwritten to KILL there if an internal hedge is found. (Primary selection needs no fills, so
        # keep it here; only the hedge check is I/O-bound and gets batched.)
        entity_tier[eid] = None
        _hedge_entities.append((eid, members, primary))

    # BATCHED internal-hedge pass (2026-07-18): load fills for a BATCH of entities in ONE partition-pruned
    # shard scan, run each entity's hedge check on the warm cache, then EVICT the whole batch. This bounds
    # the resident peak to one batch's fills (NOT all hedge wallets — the old eager-preload OOM) while doing
    # ~1 scan per BATCH instead of one per wallet (the per-entity version was memory-safe but ~5-8x too slow
    # on the 20k-partition shard). Byte-identical: identical fills (grouped loader == per-wallet loader) and
    # identical per-entity computation; batching changes only I/O grouping, and each wallet is in exactly one
    # entity so evicting a finished batch can never drop fills a later entity needs.
    _EB = max(1, int(os.environ.get("QL_M04_HEDGE_ENTITY_BATCH", "300")))  # >=1: range step must be >0
    for _bi in range(0, len(_hedge_entities), _EB):
        _batch = _hedge_entities[_bi:_bi + _EB]
        _bmembers = {m for _e, _mem, _p in _batch for m in _mem}
        _need = [m for m in _bmembers if m not in fills_cache]
        if _need:
            _efb, _ = _grouped_ff(set(_need), lo_ms - 365 * 86_400_000, hi_ms)
            for _m in _need:
                fills_cache[_m] = _efb.get(str(_m).lower(), [])  # grouped keys lowercased (HL addrs)
        for _eid, _members, _primary in _batch:
            hedged = False
            for other in _members:
                if other == _primary:
                    continue
                if g.internal_hedge(get_fills(_primary), get_fills(other), lo_ms, hi_ms):
                    hedged = True
                    break
            entity_tier[_eid] = "KILL" if hedged else None
            if hedged:
                entity_codes[_eid] = ["internal_hedge"]
        for _m in _bmembers:
            fills_cache.pop(_m, None)  # batch fully resolved -> free before the next batch

    log.info("COMBINE: tiers")
    rows = []
    tier_by_wallet = {}
    for w in wallets:
        s = scores[w]
        eid = s.entity_id
        members = ent_members.get(eid, [w])
        primary = entity_primary.get(eid)
        is_primary = (primary == w) if len(members) > 1 else True
        s.is_entity_primary = is_primary

        ent_tier = entity_tier.get(eid)
        ent_codes = entity_codes.get(eid, [])

        if ent_tier == "KILL":
            # entity internal-hedge KILLs every member (provable on-HL deception)
            tier, codes = "KILL", list(ent_codes)
        else:
            # own tier: primary/single uses its own signals; a fragment inherits the PRIMARY's
            # tier (we copy the primary once). When the entity has NO primary (too_big /
            # l3_unknown -> ent_tier UNCERTAIN), evaluate each member on ITS OWN signals so a
            # provable own-KILL is not masked (codex code-r1 #1: KILL > UNCERTAIN).
            own_w = primary if (len(members) > 1 and primary is not None) else w
            own_tier, own_codes = own[own_w]
            if len(members) > 1 and primary is not None and not is_primary:
                own_codes = ["entity_fragment"] + own_codes  # dedup label, not a KILL
            # combine own tier with any entity-level tier (UNCERTAIN) by PRECEDENCE (most severe).
            cands = [(own_tier, own_codes)]
            if ent_tier is not None:
                cands.append((ent_tier, list(ent_codes)))
            tier = min((t for t, _ in cands), key=lambda t: TIER_PRECEDENCE[t])
            # merge codes (winning + entity reason), preserve order, dedup
            merged, seen = [], set()
            for t, cs in sorted(cands, key=lambda x: TIER_PRECEDENCE[x[0]]):
                for c in cs:
                    if c and c not in seen:
                        merged.append(c); seen.add(c)
            codes = merged

        copyable = (tier != "KILL") and (primary is not None) and is_primary
        tier_by_wallet[w] = tier
        rows.append({
            "wallet": w, "entity_id": eid, "is_entity_primary": is_primary,
            "n_entity_wallets": len(members), "tier": tier,
            "alloc_weight": ALLOC_WEIGHT[tier], "reason_codes": ",".join(codes),
            "copyable": copyable,
            "net_gross_ratio": s.net_gross_ratio, "price_pnl_var_frac": s.price_pnl_var_frac,
            "funding_frac": s.funding_frac, "wash_frac": s.wash_frac, "sharpe": s.sharpe,
            "median_lev": s.median_lev, "sharpe_vs_lev_flag": s.sharpe_vs_lev_flag,
            "l3_pass_standalone": s.l3_pass_standalone, "confidence": s.confidence,
            "n_anchors": s.n_anchors, "active_days": s.active_days, "n_fills": s.n_fills,
            "as_of_ms": as_of_ms,
        })
    df = pd.DataFrame(rows)

    # per-entity view (codex code-r1 #7: O(1) lookup via tier_by_wallet, no per-entity df filter)
    erows = []
    for eid, members in ent_members.items():
        primary = entity_primary.get(eid)
        # entity tier = the primary's tier (or the entity-level tier when no primary)
        if primary is not None:
            etier = tier_by_wallet[primary]
        else:
            etier = entity_tier.get(eid, "UNCERTAIN")
        erows.append({
            "entity_id": eid, "primary_wallet": primary,
            "member_wallets": ",".join(members), "n_members": len(members),
            "entity_tier": etier, "entity_alloc_weight": ALLOC_WEIGHT[etier],
            "entity_link_evidence": entity_evidence.get(eid, "none"),
            "entity_confidence": entity_conf.get(eid, "high"),
            "copyable": (etier != "KILL") and (primary is not None),
            "as_of_ms": as_of_ms,  # fold-pure provenance (M6b --m04-dir requires it; codex M4 re-review)
        })
    edf = pd.DataFrame(erows)
    if return_scores:
        return df, edf, scores   # scores = per-wallet WalletScores (STAGE-A cache for incremental runs)
    return df, edf


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets-file", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--entities-out", required=True)
    ap.add_argument("--as-of", required=True, help="YYYY-MM-DD; signals use only ts < this date")
    ap.add_argument("--lookback-days", type=int, default=g.LOOKBACK_DAYS)
    ap.add_argument("--procs", type=int, default=8,
                    help="REQUESTED parallel workers for STAGE A (ceiling; auto-capped to fit RAM).")
    ap.add_argument("--per-worker-gb", type=float, default=2.0,
                    help="Per-worker peak RSS (GB). MEASURED 2026-06-10: base 0.15GB + full marks-cache "
                         "ceiling 0.73GB + transient per-wallet 365d fills -> ~1.3GB worst case; 2.0 adds "
                         "~50%% margin. Aggregate budget caps procs by this.")
    ap.add_argument("--headroom-gb", type=float, default=6.0,
                    help="RAM reserved for the live baseline (agents + mongod + postgres).")
    args = ap.parse_args()

    # 2026-07-30: refuse to run outside scripts/mem_safe_run.sh. m04 fold 11 was OOM-killed TWICE at
    # rc=137 precisely because its driver invoked this module bare, so nothing was watching
    # system-available memory. The in-process guard bounds THIS process; only the wrapper bounds the box.
    require_mem_safe_run("v15_m04_authenticity")

    # AGGREGATE memory budget (2026-06-10 OOM fix): per-process guards do not compose; cap worker
    # count from ACTUAL free RAM so N x per_worker + baseline cannot blow past physical RAM.
    budget = plan_memory_budget(requested_procs=args.procs, per_worker_gb=args.per_worker_gb,
                                headroom_gb=args.headroom_gb)
    install_memory_guard(soft_gb=budget.main_soft_gb, label="m04-main")

    as_of_ms = int(pd.Timestamp(args.as_of, tz="UTC").timestamp() * 1000)
    hi_ms = as_of_ms - 1  # strict: ts < as_of (inclusive m01 loaders)
    # codex code-r1 #3: exact [as_of - lookback, as_of) window (no off-by-1ms).
    lo_ms = as_of_ms - args.lookback_days * 86_400_000
    wallets = [w.strip().lower() for w in open(args.wallets_file)
               if w.strip() and not w.startswith("#")]
    log.info(f"{len(wallets)} wallets, {args.lookback_days}d window ending as-of {args.as_of}")

    df, edf = run(wallets, lo_ms, hi_ms, as_of_ms, procs=budget.procs,
                  worker_soft_gb=budget.worker_soft_gb, hot_prefetch=True)

    # sanity assertions
    for _, r in df.iterrows():
        assert r["alloc_weight"] == ALLOC_WEIGHT[r["tier"]]
        if r["tier"] == "KILL":
            assert not r["copyable"], f"KILL copyable: {r['wallet']}"
        if r["tier"] == "CLEAN" and r["is_entity_primary"]:
            # CLEAN-from-own-signals (primary/singleton) MUST have valid+confident directional
            # evidence. A non-primary FRAGMENT INHERITS the primary's CLEAN (design §3f) and may
            # itself be LOW-confidence; it is copyable=False, so the own-signal invariant does not
            # apply to it. (Real-data edge: a thin fragment of a CLEAN entity.)
            assert r["confidence"] != "LOW" and r["l3_pass_standalone"], f"bad CLEAN primary {r['wallet']}"

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.out, index=False, compression="snappy")
    edf.to_parquet(args.entities_out, index=False, compression="snappy")

    log.info(f"TIERS: {dict(df['tier'].value_counts())}")
    log.info(f"entities: {len(edf)} (from {len(df)} wallets); copyable entities: {int(edf['copyable'].sum())}")
    rh = Counter()
    for codes in df["reason_codes"]:
        for c in codes.split(","):
            if c:
                rh[c] += 1
    log.info(f"REASON HISTOGRAM: {dict(rh.most_common())}")
    log.info(f"Wrote {args.out} + {args.entities_out}")


if __name__ == "__main__":
    main()
