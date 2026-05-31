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
import logging
import sys
import time
from collections import Counter
from multiprocessing import Pool
from pathlib import Path

import pandas as pd

sys.path.insert(0, "/Users/hermes/quants-lab/research/v15")
import v15_m025_authenticity_gate as g  # noqa: E402  (codex-SHIP helpers)
import v15_m01_equity_reconstruct as m01  # noqa: E402

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


def run(wallets, lo_ms, hi_ms, as_of_ms, procs: int = 8):
    log.info(f"STAGE A: {len(wallets)} wallets (as-of {as_of_ms}), procs={procs}")
    scores = {}
    t0 = time.time()
    if procs > 1:
        tasks = [(w, lo_ms, hi_ms) for w in wallets]
        with Pool(procs) as pool:
            for i, (w, sc) in enumerate(pool.imap_unordered(_stage_a_worker, tasks, chunksize=16), 1):
                scores[w] = sc
                if i % 2000 == 0:
                    log.info(f"  A [{i}/{len(wallets)}] ({(time.time()-t0)/60:.1f}min)")
    else:
        for i, w in enumerate(wallets, 1):
            scores[w] = g.stage_a(w, lo_ms, hi_ms)
            if i % 500 == 0:
                log.info(f"  A [{i}/{len(wallets)}] ({(time.time()-t0)/60:.1f}min)")
    log.info(f"STAGE A done in {(time.time()-t0)/60:.1f}min")

    log.info("STAGE B: entities (union-find)")
    ent_id, ent_members = g.build_entities(wallets, lo_ms, hi_ms)
    for w in wallets:
        scores[w].entity_id = ent_id[w]

    # own danger tier per wallet (no entity logic) — needed by entity resolution + combine.
    own = {w: _own_tier(scores[w]) for w in wallets}

    log.info("STAGE C: entity resolution + internal-hedge")
    fills_cache = {}

    def get_fills(w):
        if w not in fills_cache:
            fills_cache[w] = m01.load_wallet_fills(w, lo_ms - 365 * 86_400_000, hi_ms)
        return fills_cache[w]

    entity_primary = {}     # eid -> wallet or None
    entity_tier = {}        # eid -> tier
    entity_codes = {}       # eid -> [codes]
    entity_evidence = {}    # eid -> link evidence
    entity_conf = {}        # eid -> high|medium

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
        # internal-hedge on-HL -> entity KILL
        hedged = False
        for other in members:
            if other == primary:
                continue
            if g.internal_hedge(get_fills(primary), get_fills(other), lo_ms, hi_ms):
                hedged = True
                break
        entity_tier[eid] = "KILL" if hedged else None
        if hedged:
            entity_codes[eid] = ["internal_hedge"]

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
        })
    edf = pd.DataFrame(erows)
    return df, edf


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets-file", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--entities-out", required=True)
    ap.add_argument("--as-of", required=True, help="YYYY-MM-DD; signals use only ts < this date")
    ap.add_argument("--lookback-days", type=int, default=g.LOOKBACK_DAYS)
    ap.add_argument("--procs", type=int, default=8, help="parallel workers for STAGE A (per-wallet).")
    args = ap.parse_args()

    as_of_ms = int(pd.Timestamp(args.as_of, tz="UTC").timestamp() * 1000)
    hi_ms = as_of_ms - 1  # strict: ts < as_of (inclusive m01 loaders)
    # codex code-r1 #3: exact [as_of - lookback, as_of) window (no off-by-1ms).
    lo_ms = as_of_ms - args.lookback_days * 86_400_000
    wallets = [w.strip().lower() for w in open(args.wallets_file)
               if w.strip() and not w.startswith("#")]
    log.info(f"{len(wallets)} wallets, {args.lookback_days}d window ending as-of {args.as_of}")

    df, edf = run(wallets, lo_ms, hi_ms, as_of_ms, procs=args.procs)

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
