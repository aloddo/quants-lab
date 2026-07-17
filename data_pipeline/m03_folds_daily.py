#!/usr/bin/env python3
"""M3 daily-incremental fold activity: recompute per-(wallet, fold, window) activity ONLY for the wallets
whose journeys changed in the M2 delta, merge with the cached prior M3 output. Row-identical to a full
recompute BY CONSTRUCTION (build_activity + _summarize are pure per-key aggregation -- proven
/tmp/m03_incremental_test.py). Journeys-only: n_actions is unused by M5/M6a (0 refs), so activity is derived
from the journey-open stream (entry_ts); active/n_journeys (the selection signal) come from journeys.

CLI:
  python m03_folds_daily.py --run   # incremental daily run off the M2 store + delta
  python m03_folds_daily.py --gate  # verify incremental == full on the real M2 journeys store
"""
from __future__ import annotations
import argparse, glob, json, re, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "research" / "v15"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import pandas as pd
import v15_m03_fold_geometry as m3
from m02_journeys_daily import load_active_closed, DEFAULT_OUT_DIR

REPO = Path(__file__).resolve().parents[1]
DEFAULT_M3_DIR = REPO / "app" / "data" / "v15" / "m03_folds_daily"


def _journeys_to_activity_inputs(j: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Journeys-only inputs for build_activity: derive the 'actions' stream from journey opens (entry_ts).
    active/n_journeys come from journeys; n_actions == journey-open count (n_actions is ~unused downstream)."""
    j = j.copy()
    j["wallet"] = j["wallet"].astype(str).str.lower()
    jj = j[["wallet", "entry_ts"]].copy()
    jj["lifecycle_valid"] = True
    jj["stream_replay_valid"] = True
    acts = jj.rename(columns={"entry_ts": "ts"})[["wallet", "ts"]].copy()
    acts["stream_replay_valid"] = True
    return acts, jj


def build_m3(journeys: pd.DataFrame):
    folds = m3.build_folds()
    acts, jj = _journeys_to_activity_inputs(journeys)
    return m3.build_activity(folds, acts, jj)


def _m2_run_id(path: str) -> int:
    # match run_NNNNNN.parquet, run_NNNNNN.parquet.parts, AND run_NNNNNN.wallets.parquet (touched sidecar)
    m = re.match(r"run_(\d+)\.", Path(path).name)
    return int(m.group(1)) if m else -1


def _m2_new_run_wallets(m2_out_dir: Path, last_run_id: int) -> tuple[set, int]:
    """AUTHORITATIVE daily DELTA: the union of touched wallets across M2 runs with run_id > last_run_id,
    plus the new cursor.

    CODEX P1 #2 (2026-07-16): prefer the `run_<id>.wallets.parquet` TOUCHED sidecar (every wallet whose
    journeys/state changed this run, incl. new-OPEN-only wallets absent from the closed parquet). Fall back
    to scanning the closed run parquet's wallet column only if the sidecar is missing (legacy runs).

    CODEX P1 #3 (2026-07-16): advance the cursor CONTIGUOUSLY and NEVER past a run that is mid-write
    (`.parts`) or fails to read. A gap (unreadable/partial run) freezes the cursor at the last fully-read
    contiguous run; later runs' wallets are still collected (idempotent union, harmless) but the frozen
    cursor guarantees the gapped run is reprocessed next time rather than silently skipped forever."""
    closed = Path(m2_out_dir) / "closed"
    # Gather every run-id's available sources. A `.parquet.parts` dir is a MID-WRITE (possibly
    # partially-flushed) source and is NEVER treated as complete; the touched sidecar and the final closed
    # parquet are the only COMPLETE sources (sidecar preferred, closed parquet as the fallback for the same
    # run-id -- codex re-review: an unreadable sidecar must NOT mask a readable closed parquet).
    by_rid: dict[int, dict] = {}
    for p in glob.glob(str(closed / "run_*.wallets.parquet")):
        rid = _m2_run_id(p)
        if rid >= 0: by_rid.setdefault(rid, {})["sidecar"] = p
    for p in glob.glob(str(closed / "run_*.parquet")):
        if p.endswith(".wallets.parquet"): continue
        rid = _m2_run_id(p)
        if rid >= 0: by_rid.setdefault(rid, {})["closed"] = p
    for p in glob.glob(str(closed / "run_*.parquet.parts")):
        rid = _m2_run_id(p)
        if rid >= 0: by_rid.setdefault(rid, {})["parts"] = p

    # Consume ONLY the CONTIGUOUS COMPLETE PREFIX after last_run_id. The touched SIDECAR is the run's COMMIT
    # MARKER (M2 writes it LAST, atomically). Commit-order safety (codex final review): the closed parquet
    # becomes visible before the sidecar, so a NEWEST run with a closed parquet but no readable sidecar may be
    # MID-WRITE -- treat it as incomplete and STOP (reprocess next cycle) rather than consume its
    # under-representing parquet (which omits new-OPEN-only wallets). A run is finalized-proven when a
    # strictly-higher run_id exists in the store, so an older no-sidecar (legacy) run may safely fall back to
    # its closed parquet. The returned union is then EXACTLY the delta for (last_run_id, mx], never partial.
    max_rid = max(by_rid) if by_rid else last_run_id
    wallets: set = set(); mx = last_run_id
    while (rid := mx + 1) in by_rid:
        srcs = by_rid[rid]
        got = None
        sidecar_unreadable = False
        if "sidecar" in srcs:
            try:
                got = set(pd.read_parquet(srcs["sidecar"], columns=["wallet"])["wallet"].astype(str).str.lower().unique())
            except Exception:  # noqa: BLE001  corrupt sidecar
                got = None; sidecar_unreadable = True
        # legacy/finalized fallback: closed parquet ONLY when the sidecar is ABSENT (legacy run) and this run is
        # provably finalized (a higher run exists). A PRESENT-BUT-UNREADABLE sidecar must NOT fall back to the
        # closed parquet -- that parquet intentionally omits open-only touched wallets, so falling back while
        # advancing the cursor would silently lose those wallets forever (codex P1). Freeze the cursor instead:
        # the unreadable sidecar leaves got=None -> break -> the run is reprocessed next cycle.
        if got is None and not sidecar_unreadable and "closed" in srcs and rid < max_rid:
            try:
                got = set(pd.read_parquet(srcs["closed"], columns=["wallet"])["wallet"].astype(str).str.lower().unique())
            except Exception:  # noqa: BLE001
                got = None
        if got is None:   # newest-no-sidecar (mid-write), .parts-only, or unreadable -> stop the prefix
            break
        wallets |= got; mx = rid
    return wallets, mx


def run_daily(out_dir: Path = DEFAULT_M3_DIR, m2_out_dir: Path = DEFAULT_OUT_DIR,
              delta_wallets: set | None = None) -> dict:
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    m2_out_dir = Path(m2_out_dir)
    wide_p, summ_p = out_dir / "m3_wide.parquet", out_dir / "m3_summary.parquet"
    state_p = out_dir / "m3_run_state.json"
    prior_exists = wide_p.exists() and summ_p.exists()

    # AUTO-DELTA: changed wallets = wallets in M2 run files newer than the last M3 run (M2->M3 wiring).
    if delta_wallets is None and prior_exists and state_p.exists():
        last = int(json.loads(state_p.read_text()).get("last_m2_run_id", 0))
        delta_wallets, new_max_run = _m2_new_run_wallets(m2_out_dir, last)
    else:
        _, new_max_run = _m2_new_run_wallets(m2_out_dir, 0)   # current max for bookkeeping (0 = nothing consumed yet; -1 probes run_id 0 which never exists -> freezes the delta, codex P1)

    incremental = prior_exists and delta_wallets is not None
    if not incremental:
        all_j = load_active_closed(m2_out_dir)                 # FULL: first run / no prior
        if all_j.empty:
            raise SystemExit("m03: no active journeys in the M2 store")
        all_j["wallet"] = all_j["wallet"].astype(str).str.lower()
        wide, summ = build_m3(all_j); mode = "full"
    else:
        dw = {str(w).lower() for w in delta_wallets}
        pw, ps = pd.read_parquet(wide_p), pd.read_parquet(summ_p)
        if not dw:
            wide, summ = pw, ps; mode = "incremental(0 changed)"   # no new wallets -> prior stands
        else:
            jd = load_active_closed(m2_out_dir, wallets=dw)    # load ONLY the delta wallets' journeys (fast)
            if not jd.empty:
                jd["wallet"] = jd["wallet"].astype(str).str.lower()
                wide_d, summ_d = build_m3(jd)
            else:
                wide_d, summ_d = pd.DataFrame(columns=pw.columns), pd.DataFrame(columns=ps.columns)
            wide = pd.concat([pw[~pw["key"].isin(dw)], wide_d], ignore_index=True)
            summ = pd.concat([ps[~ps["key"].isin(dw)], summ_d], ignore_index=True)
            mode = f"incremental({len(dw)} changed)"
    wide.to_parquet(wide_p, index=False); summ.to_parquet(summ_p, index=False)
    state_p.write_text(json.dumps({"last_m2_run_id": int(new_max_run),
                                   "updated_utc": pd.Timestamp.utcnow().isoformat()}, indent=2))
    return {"mode": mode, "wide_rows": len(wide), "summary_rows": len(summ),
            "delta_wallets": (0 if delta_wallets is None else len(delta_wallets)), "last_m2_run_id": int(new_max_run)}


def gate(m2_out_dir: Path = DEFAULT_OUT_DIR) -> bool:
    """incremental (prior=full-minus-delta + recompute delta + merge) == full, on the REAL M2 store."""
    all_j = load_active_closed(Path(m2_out_dir))
    all_j["wallet"] = all_j["wallet"].astype(str).str.lower()
    wide_full, summ_full = build_m3(all_j)
    keys = sorted(wide_full["key"].unique())
    delta = set(keys[: max(1, len(keys) // 5)])            # 20% delta
    pw = wide_full[~wide_full["key"].isin(delta)]; ps = summ_full[~summ_full["key"].isin(delta)]
    jd = all_j[all_j["wallet"].isin(delta)]
    wide_d, summ_d = build_m3(jd)
    merged_w = pd.concat([pw, wide_d], ignore_index=True)
    merged_s = pd.concat([ps, summ_d], ignore_index=True)
    def sk(df): return df.sort_values(list(df.columns)).reset_index(drop=True)
    ok_w = sk(wide_full).equals(sk(merged_w)); ok_s = sk(summ_full).equals(sk(merged_s))
    print(f"M3 GATE (real M2 store): full_wide={len(wide_full)} merged={len(merged_w)} wide_identical={ok_w} "
          f"| summ={len(summ_full)}=={len(merged_s)} identical={ok_s} | delta={len(delta)}/{len(keys)} keys")
    return ok_w and ok_s


def main():
    ap = argparse.ArgumentParser(description="M3 daily-incremental fold activity off the M2 delta.")
    ap.add_argument("--gate", action="store_true"); ap.add_argument("--run", action="store_true")
    ap.add_argument("--out-dir", default=str(DEFAULT_M3_DIR)); ap.add_argument("--m2-out-dir", default=str(DEFAULT_OUT_DIR))
    a = ap.parse_args()
    t0 = time.time()
    if a.gate:
        ok = gate(Path(a.m2_out_dir)); print("M3 INCREMENTAL GATE:", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
    r = run_daily(Path(a.out_dir), Path(a.m2_out_dir))
    print(f"m3 daily: {r} | wall {(time.time()-t0)/60:.2f} min")


if __name__ == "__main__":
    main()
