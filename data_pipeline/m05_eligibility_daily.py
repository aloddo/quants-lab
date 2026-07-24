#!/usr/bin/env python3
"""M5 eligibility daily-incremental (copyability mode, equity-optional): recompute per-(entity,fold) gates
ONLY for entities whose journeys / m04-tier changed in the M2 delta; merge the cached prior. Per-entity ->
ROW-IDENTICAL to a full recompute by construction (proven /tmp/m05_incremental_test.py). Consumes M4 tiers
(m04 daily driver), the fixed M3 fold calendar, and the M2 journeys store.

CLI:
  python m05_eligibility_daily.py --run    # incremental off the M2 delta + the M4 tiers
  python m05_eligibility_daily.py --gate   # incremental == full on real wallets
"""
from __future__ import annotations
import argparse, json, sys, time
from dataclasses import asdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "research" / "v15"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import pandas as pd
import v15_m05_eligibility as m5, v15_m03_fold_geometry as m3
from m02_journeys_daily import load_active_closed, DEFAULT_OUT_DIR, DEFAULT_STATE_DIR, committed_run_id
from m03_folds_daily import _m2_new_run_wallets
from m04_authenticity_daily import DEFAULT_M4_DIR

REPO = Path(__file__).resolve().parents[1]
DEFAULT_M5_DIR = REPO / "app" / "data" / "v15" / "m05_eligibility_daily"
_EMPTY_EQ = pd.DataFrame(columns=["wallet", "date"])


def _folds_df() -> pd.DataFrame:
    return pd.DataFrame([asdict(f) for f in m3.build_folds()])


def _run_m5(m04: pd.DataFrame, journeys: pd.DataFrame):
    df, edf, _wf = m5.run(_folds_df(), journeys, _EMPTY_EQ, m04, equity_required=False)
    return df, edf


def _entity_m4_hash(m04: pd.DataFrame) -> dict:
    """entity_id -> stable hash of its FULL M4 rows (all columns except the always-changing bookkeeping
    as_of_ms), sorted. Captures BOTH topology (membership) AND value changes (tier / scores), so an M4
    tier/score change with UNCHANGED membership and NO M2 wallet delta still invalidates the entity in M5
    (codex re-review: M5 must be driven by CHANGED M4 OUTPUT, not just the M2 delta)."""
    import hashlib
    cols = [c for c in m04.columns if c != "as_of_ms"]
    out = {}
    for eid, g in m04.groupby("entity_id"):
        payload = g[cols].sort_values(list(cols)).to_csv(index=False)
        out[str(eid)] = hashlib.md5(payload.encode()).hexdigest()  # noqa: S324  non-crypto keying
    return out


def run_daily(out_dir: Path = DEFAULT_M5_DIR, m2_out_dir: Path = DEFAULT_OUT_DIR,
              m4_dir: Path = DEFAULT_M4_DIR, delta_wallets: set | None = None,
              m2_state_dir: Path = DEFAULT_STATE_DIR) -> dict:
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    # Read ONLY committed M2 runs (codex P1 #2): an uncommitted/failed run is invisible to M5.
    _m2_cap = committed_run_id(Path(m2_state_dir))
    elig_p, ent_p, state_p = out_dir / "m5_eligibility.parquet", out_dir / "m5_entities.parquet", out_dir / "m5_run_state.json"
    m04 = pd.read_parquet(Path(m4_dir) / "m4_tiers.parquet")
    m04["wallet"] = m04["wallet"].astype(str).str.lower()
    cur_hash = _entity_m4_hash(m04)
    prior = elig_p.exists() and ent_p.exists()
    prev_hash = {}
    if delta_wallets is None and prior and state_p.exists():
        st = json.loads(state_p.read_text())
        last = int(st.get("last_m2_run_id", 0)); prev_hash = st.get("entity_m4_hash", {}) or {}
        delta_wallets, new_max = _m2_new_run_wallets(Path(m2_out_dir), last, max_run_id=_m2_cap)
    else:
        _, new_max = _m2_new_run_wallets(Path(m2_out_dir), 0, max_run_id=_m2_cap)   # 0 = nothing consumed yet; -1 probes run_id 0 which never exists -> freezes the delta forever (codex P1)

    if not prior or delta_wallets is None or not prev_hash:
        jn = load_active_closed(Path(m2_out_dir), max_run_id=_m2_cap); jn["wallet"] = jn["wallet"].astype(str).str.lower()
        df, edf = _run_m5(m04, jn); mode = "full"
    else:
        dw = {str(w).lower() for w in delta_wallets}
        # CODEX P1 #4 + re-review (2026-07-16): recompute an entity if EITHER
        #  (a) a member wallet is in the M2 delta (its journeys/PnL changed -> M5 fold gates change), OR
        #  (b) its FULL M4-row hash changed vs the prior run or it is NEW -- this captures topology
        #      (merge/split, incl. ledger-only merges with an empty wallet delta) AND M4 tier/score changes
        #      that don't alter membership (the M4->M5 propagation gap the re-review flagged).
        delta_ent = set(m04[m04["wallet"].isin(dw)]["entity_id"].astype(str).unique())
        delta_ent |= {eid for eid, h in cur_hash.items() if prev_hash.get(eid) != h}
        # entity_ids that VANISHED from M4 (removed / renumbered by a merge) must be pruned from the cache.
        removed_ent = set(prev_hash) - set(cur_hash)
        pe, pen = pd.read_parquet(elig_p), pd.read_parquet(ent_p)
        drop_ent = delta_ent | removed_ent
        if not drop_ent:
            df, edf = pe, pen; mode = "incremental(0 changed)"
        else:
            m04_d = m04[m04["entity_id"].astype(str).isin(delta_ent)]
            dw_all = set(m04_d["wallet"].astype(str).str.lower())
            jn = load_active_closed(Path(m2_out_dir), wallets=dw_all, max_run_id=_m2_cap) if dw_all else _EMPTY_EQ.iloc[0:0]
            if not jn.empty:
                jn["wallet"] = jn["wallet"].astype(str).str.lower()
            df_d, edf_d = _run_m5(m04_d, jn) if len(m04_d) else (pe.iloc[0:0], pen.iloc[0:0])
            # filter cached rows by STR-cast entity_id inline; do NOT mutate the stored dtype (keeps the
            # incremental output byte/dtype-identical to a full recompute -- caught by the e2e gate).
            df = pd.concat([pe[~pe["entity_id"].astype(str).isin(drop_ent)], df_d], ignore_index=True)
            edf = pd.concat([pen[~pen["entity_id"].astype(str).isin(drop_ent)], edf_d], ignore_index=True)
            mode = f"incremental({len(delta_ent)} recompute, {len(removed_ent)} pruned)"
    df.to_parquet(elig_p, index=False); edf.to_parquet(ent_p, index=False)
    state_p.write_text(json.dumps({"last_m2_run_id": int(new_max), "entity_m4_hash": cur_hash,
                                   "updated_utc": pd.Timestamp.utcnow().isoformat()}, indent=2))
    return {"mode": mode, "elig_rows": len(df), "entity_rows": len(edf), "last_m2_run_id": int(new_max)}


def gate(m2_out_dir: Path = DEFAULT_OUT_DIR, n: int = 40, m2_state_dir: Path = DEFAULT_STATE_DIR) -> bool:
    import v15_m04_authenticity as m4
    from m02_journeys_daily import day_end_ms
    _cap = committed_run_id(Path(m2_state_dir))
    wallets = sorted(set(load_active_closed(Path(m2_out_dir), wallets=None, max_run_id=_cap)["wallet"].astype(str).str.lower()))[:n]
    as_of = day_end_ms("20260714") + 1
    m04, _e, _s = m4.run(wallets, as_of - 120*86_400_000, as_of - 1, as_of, procs=1, hot_prefetch=True, return_scores=True)
    m04["wallet"] = m04["wallet"].astype(str).str.lower()
    jn = load_active_closed(Path(m2_out_dir), wallets=set(wallets), max_run_id=_cap); jn["wallet"] = jn["wallet"].astype(str).str.lower()
    df_f, edf_f = _run_m5(m04, jn)
    ent = sorted(m04["entity_id"].unique()); de = set(ent[: max(1, len(ent)//4)])
    m04_d = m04[m04["entity_id"].isin(de)]
    df_d, edf_d = _run_m5(m04_d, load_active_closed(Path(m2_out_dir), wallets=set(m04_d["wallet"]), max_run_id=_cap))
    merged = pd.concat([df_f[~df_f["entity_id"].isin(de)], df_d], ignore_index=True)
    def sk(d): return d.sort_values(list(d.columns)).reset_index(drop=True) if len(d) else d
    ok = sk(df_f).equals(sk(merged))
    print(f"M5 GATE: full_elig={len(df_f)} merged={len(merged)} identical={ok} delta_ent={len(de)}/{len(ent)}")
    return ok


def main():
    ap = argparse.ArgumentParser(description="M5 eligibility daily-incremental (copyability mode).")
    ap.add_argument("--run", action="store_true"); ap.add_argument("--gate", action="store_true")
    a = ap.parse_args(); t0 = time.time()
    if a.gate:
        ok = gate(); print("M5 INCREMENTAL GATE:", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
    r = run_daily(); print(f"m5 daily: {r} | wall {(time.time()-t0)/60:.2f} min")


if __name__ == "__main__":
    main()
