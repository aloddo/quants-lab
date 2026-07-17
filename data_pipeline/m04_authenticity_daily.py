#!/usr/bin/env python3
"""M4 authenticity = FULL recompute over the whole wallet universe (STAGE A per-wallet danger scalars ->
STAGE B entity union-find -> STAGE C tiers). NO incremental caching.

CADENCE (Alberto decision 2026-07-17, TG 11429): M4 runs WEEKLY, not daily. Codex FINAL proved the
anchored/stage-A daily-incremental UNSOUND -- Stage A selects WEEKLY ANCHORS through hi_ms, so as as_of
advances a new anchor can enter the window for an IDLE, FLAT wallet that is NOT in the M2 delta, silently
changing its tier -> stale cached row. A full recompute is correct-by-construction; a weekly cadence absorbs
the ~3.4hr single-proc cost. The daily M5 run picks up whatever tiers change here via its _entity_m4_hash
(no M5 change). Decision: projects/quant/decisions/2026-07-17-m4-weekly-full-recompute.

CLI:
  python m04_authenticity_daily.py --run --as-of 20260715     # full recompute (weekly)
  python m04_authenticity_daily.py --gate --as-of 20260714    # publish/reload fidelity on real wallets
"""
from __future__ import annotations
import argparse, glob, hashlib, json, os, sys, time
from dataclasses import asdict, fields
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "research" / "v15"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import pandas as pd
import v15_m04_authenticity as m4
import v15_m025_authenticity_gate as g
import hl_fills_io as fio
from m02_journeys_daily import (load_active_closed, DEFAULT_OUT_DIR, day_end_ms, day_start_ms,
                                hot_available_days)
from m03_folds_daily import _m2_new_run_wallets

REPO = Path(__file__).resolve().parents[1]
DEFAULT_M4_DIR = REPO / "app" / "data" / "v15" / "m04_authenticity_daily"
_FNAMES = {f.name for f in fields(g.WalletScores)}


def _scores_to_df(scores: dict) -> pd.DataFrame:
    return pd.DataFrame([{"wallet": w, **asdict(s)} for w, s in scores.items()])


def _df_to_scores(df: pd.DataFrame) -> dict:
    out = {}
    for r in df.to_dict("records"):
        kw = {k: v for k, v in r.items() if k in _FNAMES}
        rc = kw.get("reason_codes")
        kw["reason_codes"] = list(rc) if rc is not None and not isinstance(rc, float) else []
        out[r["wallet"]] = g.WalletScores(**kw)
    return out


def _universe(m2_out_dir: Path) -> list:
    j = load_active_closed(Path(m2_out_dir), )
    return sorted(set(j["wallet"].astype(str).str.lower().unique()))


def _anchor_lo_ms() -> int:
    """ANCHOR START (Alberto decision 2026-07-16 msg 11423 = option A): authenticity is scored over the
    wallet's FULL tracked history [earliest tracked day, as_of], an EXTEND-ONLY window, NOT a trailing 365d
    sliding window. Because the window only grows, a wallet's in-window fills never age off the back edge, so
    its score changes ONLY when it has new activity -> the M2 touched-delta is a COMPLETE delta signal and
    the incremental is SOUND (closes codex P1 #1; no back-edge hack needed). Raw data never deletes (CLAUDE.md
    rule 7) so the earliest day is stable."""
    days = hot_available_days()
    if not days:
        raise SystemExit("m04: no hot fills days available for the anchor start")
    return day_start_ms(days[0])


def _atomic_write(path: Path, df: pd.DataFrame) -> None:
    """Publish a parquet atomically (write a UNIQUE .tmp in the same dir, then os.replace). The weekly full-M4
    run rewrites m4_tiers.parquet while a daily M5 run may be reading it; os.replace is atomic on POSIX so a
    concurrent reader always sees either the whole old file or the whole new file, never a half-written one.
    The tmp name is per-process (pid) so two accidental concurrent --run invocations can't clobber each other's
    temp file (codex P2)."""
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)


def _hot_store_days(d: Path) -> list:
    return sorted(Path(p).stem for p in glob.glob(str(Path(d) / "*.parquet"))
                  if len(Path(p).stem) == 8 and Path(p).stem.isdigit())


def _source_watermark_day() -> str:
    """The newest day PRESENT IN ALL THREE hot stores (fills/funding/ledger) = the real data freshness of this
    M4 run. Persisted so the daily staleness guard measures SOURCE freshness, not the launcher's clock: if the
    S3 refresh stalls, this day stops advancing even though a weekly run still stamps 'today' (codex P1). Uses
    the INTERSECTION max, NOT min(per-store max): with gaps, min(max) can name a day absent from some store and
    overstate freshness (codex P1 round 3). Empty string if any store is empty or they share no common day."""
    sets = []
    for d in (fio.HOT_FILLS_DIR, fio.HOT_FUNDING_DIR, fio.HOT_LEDGER_DIR):
        days = set(_hot_store_days(Path(d)))
        if not days:
            return ""
        sets.append(days)
    common = set.intersection(*sets)
    return max(common) if common else ""


def _hot_inputs_fingerprint() -> str:
    """Fingerprint every parquet in the hot fills/funding/ledger stores (name + size + mtime_ns). Guards the
    ~3.4hr weekly M4 against the S3 refresh REWRITING source day-files mid-run: if the fingerprint changes
    between the start and end of the compute, different wallet chunks may have observed different source
    versions -> the 'full' result corresponds to no coherent snapshot -> we REFUSE to publish (codex P1 #3).
    Cheap: stat only, no reads."""
    h = hashlib.sha256()
    for d in (fio.HOT_FILLS_DIR, fio.HOT_FUNDING_DIR, fio.HOT_LEDGER_DIR):
        for p in sorted(glob.glob(str(Path(d) / "*.parquet"))):
            try:
                st = os.stat(p)
                h.update(f"{Path(p).name}:{st.st_size}:{st.st_mtime_ns}|".encode())
            except FileNotFoundError:
                h.update(f"{Path(p).name}:missing|".encode())
    return h.hexdigest()


def run_daily(as_of: str, out_dir: Path = DEFAULT_M4_DIR,
              m2_out_dir: Path = DEFAULT_OUT_DIR, procs: int = 1) -> dict:
    """M4 authenticity = FULL recompute over the ENTIRE wallet universe. NO incremental caching.

    Cadence (Alberto decision 2026-07-17, TG 11429): M4 runs WEEKLY, not daily. The anchored/stage-A
    incremental was proven UNSOUND by codex FINAL (weekly-anchor drift silently restages idle FLAT wallets
    that are NOT in the M2 delta -> stale cached tiers). A full recompute is correct-by-construction; a
    weekly cadence absorbs the cost. Decision: projects/quant/decisions/2026-07-17-m4-weekly-full-recompute.

    Window = anchored extend-only full history [earliest tracked day, as_of] (Alberto TG 11423). procs is
    ignored in practice: hot_prefetch=True forces the single-pass, memory-audited sequential path (m4.run
    line ~146). The daily M5 run picks up whatever tiers change here via its _entity_m4_hash (no M5 change)."""
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    scores_p, tier_p, ent_p = out_dir / "m4_scores.parquet", out_dir / "m4_tiers.parquet", out_dir / "m4_entities.parquet"
    state_p = out_dir / "m4_run_state.json"
    as_of_ms = day_end_ms(as_of) + 1
    hi_ms, lo_ms = as_of_ms - 1, _anchor_lo_ms()   # ANCHORED extend-only window (full history)
    wallets = _universe(m2_out_dir)
    # bookkeeping: the max COMPLETE M2 run this full score reflects. Bootstrap cursor is 0 (M2 run ids are
    # 1-based; -1 makes _m2_new_run_wallets probe run_id 0, which never exists -> empty, freezing the delta
    # forever -- codex P1). 0 is the same "nothing consumed yet" sentinel the incremental path defaults to.
    _, new_max = _m2_new_run_wallets(Path(m2_out_dir), 0)
    fp_before = _hot_inputs_fingerprint()   # coherence guard (codex P1 #3)
    source_wm = _source_watermark_day()     # SOURCE freshness of the snapshot M4 reads, captured PRE-run (codex
                                            # P1 r3: post-run would risk stamping a just-arrived, unprocessed day)
    df, edf, scores = m4.run(wallets, lo_ms, hi_ms, as_of_ms, procs=procs, hot_prefetch=True,
                             cached_scores=None, delta_wallets=None, return_scores=True)
    if _hot_inputs_fingerprint() != fp_before:
        raise SystemExit("m04: hot fills/funding/ledger store changed DURING the run (S3 refresh overlap) -> "
                         "incoherent snapshot; NOT publishing. Re-run when the S3 refresh is idle.")
    # atomic publish (scores + entities first, tiers LAST so a reader that sees new tiers also sees consistent
    # scores/entities; a crash between them leaves the OLD tiers in place -- next weekly run overwrites all).
    _atomic_write(scores_p, _scores_to_df(scores))
    _atomic_write(ent_p, edf)
    _atomic_write(tier_p, df)
    state_p.write_text(json.dumps({"last_m2_run_id": int(new_max), "as_of": as_of, "mode": "full",
                                   "source_watermark_day": source_wm,
                                   "updated_utc": pd.Timestamp.utcnow().isoformat()}, indent=2))
    return {"mode": "full", "wallets": len(wallets), "tiers": len(df), "last_m2_run_id": int(new_max),
            "source_watermark_day": source_wm}


def gate(as_of: str = "20260714", m2_out_dir: Path = DEFAULT_OUT_DIR, n: int = 60) -> bool:
    """M4 is FULL-recompute-only now, so the invariant that matters is PUBLISH/RELOAD FIDELITY: a full run,
    published through the atomic parquet writer and read back, must be byte/dtype-identical to the in-memory
    result. This exercises the arrow coercions (list<->ndarray, None/NaN in WalletScores) where dtype bugs
    hide (the ones codex P3 flagged) without depending on the deleted incremental path."""
    import tempfile, shutil
    as_of_ms = day_end_ms(as_of) + 1; hi_ms, lo_ms = as_of_ms - 1, _anchor_lo_ms()   # anchored window
    wallets = _universe(m2_out_dir)[:n]
    df, edf, scores = m4.run(wallets, lo_ms, hi_ms, as_of_ms, procs=1, hot_prefetch=True, return_scores=True)
    d = Path(tempfile.mkdtemp())
    tier_p, ent_p, scores_p = d / "t.parquet", d / "e.parquet", d / "s.parquet"
    _atomic_write(tier_p, df); _atomic_write(ent_p, edf); _atomic_write(scores_p, _scores_to_df(scores))
    df2, edf2, sdf2 = pd.read_parquet(tier_p), pd.read_parquet(ent_p), pd.read_parquet(scores_p)
    shutil.rmtree(d, ignore_errors=True)
    def _arraylike(v): return isinstance(v, (list, tuple)) or hasattr(v, "shape")  # list or np.ndarray
    def sk(x):   # sort on hashable columns only; normalize array-like cells (list vs arrow-reloaded ndarray)
        arr_cols = [c for c in x.columns if x[c].map(_arraylike).any()]
        keys = [c for c in x.columns if c not in arr_cols]
        y = x.sort_values(keys).reset_index(drop=True).copy()
        for c in arr_cols:   # container-type-agnostic compare: [a,b]==ndarray([a,b]) after -> list
            y[c] = y[c].map(lambda v: list(v) if _arraylike(v) else v)
        return y
    ok = (sk(df).equals(sk(df2)) and sk(edf).equals(sk(edf2))
          and sk(_scores_to_df(scores)).equals(sk(sdf2)))
    print(f"M4 FULL GATE: tiers={len(df)} entities={len(edf)} publish_reload_identical={ok}")
    return ok


def main():
    ap = argparse.ArgumentParser(description="M4 authenticity — FULL recompute (weekly cadence, Alberto TG 11429).")
    ap.add_argument("--run", action="store_true"); ap.add_argument("--gate", action="store_true")
    ap.add_argument("--as-of", default="20260714")
    ap.add_argument("--procs", type=int, default=1)   # hot_prefetch forces single-proc anyway
    a = ap.parse_args(); t0 = time.time()
    if a.gate:
        ok = gate(a.as_of); print("M4 FULL GATE:", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
    r = run_daily(a.as_of, procs=a.procs)
    print(f"m4 full: {r} | wall {(time.time()-t0)/60:.2f} min")


if __name__ == "__main__":
    main()
