#!/usr/bin/env python3
"""Acceptance test for OPTION A canonical actions persistence in the M02 daily worker.

Proves the day/run-partitioned incremental ACTIONS store written by
``data_pipeline/m02_journeys_daily.run_daily`` reduces (via the memory-safe DuckDB active-view
reducer ``load_active_actions``) to a set of actions BYTE-EQUIVALENT (on all content columns) to
the canonical full-batch trace ``research/v15/v15_m02_journey_trace.process_wallet`` over the same
wallets/window.

It runs entirely against the REAL hot store, on a stratified ~100-wallet subset (current-ish
holders, REVERSE fills, liquidations, same-ms open/close bursts, and ``xyz:`` builder-perp coins),
into TEMP state/out/actions dirs (production stores untouched). Eight assertions:

  STEP 1  first_run over [START..D1]         -> reduce -> == batch over [START..D1]
  STEP 2  advance ONE day to D2 (incremental) -> reduce -> == batch over [START..D2]   (proves the
          tombstone-by-range incremental step: holders replay from last_flat, others carry forward)
  STEP 3  re-run target D2 (idempotent)       -> reduce -> == STEP 2 active view        (idempotency)
  STEP 4  drop an ACTIVE run part NEWER than the committed checkpoint run_id -> the reducer's committed
          cap IGNORES it (== STEP 2 view), and WITHOUT the cap it provably leaks   (codex P1 #2)
  STEP 5  LEGACY journeys checkpoint (no actions marker) -> run_daily FAILS LOUD       (codex P1 #1
          migration guard; never silently produce a partial actions store)
  STEP 6  committed-but-EMPTY actions store (marker set) -> next run does NOT false-trigger  (codex P2)
  STEP 7  reader load_active_closed with committed cap IGNORES an uncommitted journey run (codex P1 #2
          wired into m03/m04/m05)
  STEP 8  the DELTA CURSOR (_m2_new_run_wallets) shares the committed ceiling: it does NOT advance past
          committed with uncommitted parts present, and picks the run up after it commits  (codex P1 final)

Content columns compared (WINDOW-LOCAL event_order/journey_id EXCLUDED -- they restart per replay
window and are diagnostic-only):
  coin, ts, fill_id, action_type, signed_size, price, position_after, is_liquidation,
  carry_in_status, state_resynced, causal_order_ok, lifecycle_valid, stream_replay_valid
Rows are matched on the window-invariant action identity (wallet, coin, ts, this_fill_ord).

Run (small slice; safe to run unwrapped):
  /usr/bin/time -l /Users/hermes/miniforge3/envs/quants-lab/bin/python \
      scripts/verify_m02_actions_canonical.py
"""
from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "research" / "v15"))
sys.path.insert(0, str(REPO / "data_pipeline"))

import hl_fills_io as fio                 # noqa: E402
import v15_m02_journey_trace as m02       # noqa: E402
import m02_journeys_daily as daily        # noqa: E402

# Window: three consecutive available hot days (START, D1, D2=D1+1). Small so the run is fast.
START, D1, D2 = "20260717", "20260718", "20260719"

LIQ_DIRS = {"Liquidated Cross Long", "Liquidated Cross Short", "Liquidated Isolated Long",
            "Liquidated Isolated Short", "Backstop Borrow Liquidation",
            "Partial Borrow Liquidation", "Auto-Deleveraging"}
REVERSE_DIRS = {"Long > Short", "Short > Long"}

# Content columns asserted identical incremental-vs-batch (window-local cols EXCLUDED on purpose).
CMP_COLS = ["coin", "ts", "fill_id", "action_type", "signed_size", "price", "position_after",
            "is_liquidation", "carry_in_status", "state_resynced", "causal_order_ok",
            "lifecycle_valid", "stream_replay_valid"]
KEY = ["wallet", "coin", "ts", "this_fill_ord"]
FLOAT_COLS = {"signed_size", "price", "position_after"}


def select_wallets(days: list[str], per_stratum: int = 22) -> list[str]:
    """Stratified subset across REVERSE / liquidation / same-ms-burst / xyz-coin / high-activity."""
    frames = []
    for d in days:
        frames.append(pd.read_parquet(Path(fio.HOT_FILLS_DIR) / f"{d}.parquet",
                                      columns=["wallet", "coin", "time", "dir"]))
    df = pd.concat(frames, ignore_index=True)
    df["wallet"] = df["wallet"].astype(str).str.lower()
    picks: list[str] = []
    seen: set[str] = set()

    def take(cands):
        for w in cands:
            if w not in seen:
                seen.add(w); picks.append(w)
            if len(picks) % 999999 == 0:
                pass

    rev = list(df[df["dir"].isin(REVERSE_DIRS)].wallet.drop_duplicates())[:per_stratum]
    liq = list(df[df["dir"].isin(LIQ_DIRS)].wallet.drop_duplicates())[:per_stratum]
    xyz = list(df[df["coin"].astype(str).str.startswith("xyz:")].wallet.drop_duplicates())[:per_stratum]
    g = df.groupby(["wallet", "coin", "time"]).size()
    burst = [ix[0] for ix in g[g >= 2].index][:per_stratum]
    top = list(df.wallet.value_counts().head(per_stratum * 3).index)   # high-activity ~ holders/active
    for grp in (rev, liq, xyz, burst, top):
        take(grp)
    return picks


def batch_actions(wallets: list[str], t0: int, t1: int) -> pd.DataFrame:
    """CANONICAL full-WINDOW batch actions: one grouped read of [t0,t1], then the UNCHANGED tracer
    (v15_m02_journey_trace.trace_wallet via process_wallet_preloaded) over each wallet's full-window
    fills -- exactly the canonical batch trace, in a single non-incremental pass. Uses the grouped
    loader (reads each in-window day ONCE) instead of the per-wallet load_wallet_fills path, which is
    O(wallets x ALL_days) full-store rescans (minutes/wallet on a 258-day store). The fill SET + causal
    order are identical (fio.load_grouped_fills_funding is the same normalizer), so the emitted actions
    are byte-identical to process_wallet -- the equivalence this test asserts is store+reducer, not the
    loader, and journeys already gate-prove grouped==per-wallet."""
    gf, gfund = fio.load_grouped_fills_funding(set(wallets), t0, t1)
    rows: list[dict] = []
    for w in wallets:
        wf = fio.order_wallet_fills_causally([f for f in gf.get(w, []) if int(f["time"]) >= t0])
        wfund = [x for x in gfund.get(w, []) if int(x["time"]) >= t0]
        res = m02.process_wallet_preloaded((w, wf, wfund, t1))
        if "error" in res:
            raise SystemExit(f"batch trace error for {w}: {res['error']}")
        rows.extend(res.get("actions") or [])
    return pd.DataFrame(rows)


def _norm(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["wallet"] = df["wallet"].astype(str).str.lower()
    df["coin"] = df["coin"].astype(str)
    for c in ("ts", "fill_id", "this_fill_ord"):
        df[c] = df[c].astype("int64")
    for c in ("is_liquidation", "state_resynced", "causal_order_ok",
              "lifecycle_valid", "stream_replay_valid"):
        df[c] = df[c].astype(bool)
    # carry_in_status is nullable (None when the action is not a carry-in seed). Normalize BOTH
    # sources' nulls to one token so a Python-None (batch) vs parquet-NaN (store) round-trip is not a
    # false diff -- a genuine value-vs-null mismatch would still show up as token-vs-value.
    for c in ("carry_in_status", "action_type"):
        df[c] = df[c].fillna("__NULL__").astype(str)
    return df.sort_values(KEY, kind="mergesort").reset_index(drop=True)


def assert_equiv(label: str, store: pd.DataFrame, batch: pd.DataFrame) -> None:
    store, batch = _norm(store), _norm(batch)
    print(f"\n[{label}] store rows={len(store):,} batch rows={len(batch):,}")
    # 1) exact row-count parity
    if len(store) != len(batch):
        # surface the key-set delta
        sk = set(map(tuple, store[KEY].values))
        bk = set(map(tuple, batch[KEY].values))
        raise AssertionError(f"[{label}] ROW COUNT MISMATCH store={len(store)} batch={len(batch)} "
                             f"| only_store={len(sk - bk)} only_batch={len(bk - sk)} "
                             f"| e.g. only_batch={list(bk - sk)[:3]} only_store={list(sk - bk)[:3]}")
    # 2) identical key sets + exact content equality (join on the window-invariant action identity)
    m = store.merge(batch, on=KEY, how="outer", suffixes=("_s", "_b"), indicator=True)
    bad = m[m["_merge"] != "both"]
    if len(bad):
        raise AssertionError(f"[{label}] KEY SET MISMATCH: {len(bad)} unmatched keys "
                             f"(only_store={sum(bad['_merge']=='left_only')}, "
                             f"only_batch={sum(bad['_merge']=='right_only')})")
    mismatches = []
    # coin/ts are join KEYS (guaranteed equal, unsuffixed after merge); compare the rest.
    for c in [c for c in CMP_COLS if c not in KEY]:
        cs, cb = m[f"{c}_s"], m[f"{c}_b"]
        if c in FLOAT_COLS:
            a = cs.astype("float64").to_numpy(); b = cb.astype("float64").to_numpy()
            neq = ~np.isclose(a, b, rtol=0.0, atol=1e-12, equal_nan=True)
        else:
            neq = (cs.astype(str).to_numpy() != cb.astype(str).to_numpy())
        n = int(neq.sum())
        if n:
            ex = m.loc[neq, KEY + [f"{c}_s", f"{c}_b"]].head(3).to_dict("records")
            mismatches.append((c, n, ex))
    if mismatches:
        for c, n, ex in mismatches:
            print(f"  MISMATCH col={c} n={n} examples={ex}")
        raise AssertionError(f"[{label}] CONTENT MISMATCH on {[c for c, _, _ in mismatches]}")
    print(f"[{label}] PASS: {len(store):,} rows, all {len(CMP_COLS)} content cols exactly equal.")


def main() -> None:
    # TEST-SLICE memory calibration (this 114-wallet/3-day slice peaks <520MB measured). The daily
    # planner's static serial reserves (_WRITER_GB/_TRACE_WORKING_GB/_SERIAL_FREE_MARGIN_GB) are sized
    # for the FULL wallet universe and would refuse to start on a transiently RAM-tight box even for a
    # trivial slice. Shrink them for the test ONLY (production defaults untouched); the run stays
    # genuinely memory-safe (bounded chunk + streaming writers + DuckDB out-of-core reducer).
    daily._WRITER_GB = 0.2
    daily._TRACE_WORKING_GB = 0.2
    daily._SERIAL_FREE_MARGIN_GB = 0.3

    tmp = Path(tempfile.mkdtemp(prefix="m02_actions_verify_"))
    state_dir = tmp / "state"
    out_dir = tmp / "journeys"
    actions_dir = tmp / "actions"
    wl_file = tmp / "wallets.txt"
    try:
        wallets = select_wallets([START, D1, D2])
        wl_file.write_text("\n".join(wallets) + "\n")
        print(f"selected {len(wallets)} stratified wallets over [{START}..{D2}] -> {wl_file}")

        t0 = daily.day_start_ms(START)
        end_d1 = daily.day_end_ms(D1)
        end_d2 = daily.day_end_ms(D2)

        def reduce_store(max_run_id="committed") -> pd.DataFrame:
            # Realistic downstream read: cap to the committed run_id (single source of truth = the
            # checkpoint), so an uncommitted/failed run's parts are never consumed.
            mrid = daily.committed_run_id(state_dir) if max_run_id == "committed" else max_run_id
            df = daily.load_active_actions(actions_dir, wallets=set(wallets), max_run_id=mrid)
            return df if df is not None else pd.DataFrame()

        # -- STEP 1: first_run over [START..D1] --------------------------------
        r1 = daily.run_daily(target_day=D1, state_dir=state_dir, out_dir=out_dir,
                             actions_dir=actions_dir, wallets_file=str(wl_file),
                             start_day=START, procs=2)
        print(f"run1: {r1}")
        assert_equiv("STEP1 first_run [START..D1]", reduce_store(),
                     batch_actions(wallets, t0, end_d1))

        # -- STEP 2: advance ONE day to D2 (incremental) -----------------------
        r2 = daily.run_daily(target_day=D2, state_dir=state_dir, out_dir=out_dir,
                             actions_dir=actions_dir, wallets_file=str(wl_file),
                             start_day=START, procs=2)
        print(f"run2: {r2}")
        step2_view = reduce_store()
        assert_equiv("STEP2 incremental [START..D2]", step2_view,
                     batch_actions(wallets, t0, end_d2))

        # -- STEP 3: idempotent re-run of the SAME target ----------------------
        r3 = daily.run_daily(target_day=D2, state_dir=state_dir, out_dir=out_dir,
                             actions_dir=actions_dir, wallets_file=str(wl_file),
                             start_day=START, procs=2)
        print(f"run3 (idempotent re-run): {r3}")
        assert_equiv("STEP3 idempotency (== STEP2 view)", reduce_store(), step2_view)

        # -- STEP 4: a run part NEWER than the committed checkpoint run_id is IGNORED (codex P1 #2) --
        # Drop a bogus, ACTIVE, run_id=99 part with a sentinel-mutated price. Under the committed cap it
        # must be ignored (active view unchanged == STEP2); without the cap it MUST leak (proving the
        # part is real + corrupting, so the filter -- not luck -- is what protects the read).
        committed = daily.committed_run_id(state_dir)
        stray = pd.read_parquet(actions_dir / "run_000001.parquet").copy()
        stray["run_id"] = 99
        stray["active"] = True
        stray["price"] = stray["price"].astype("float64") + 999.0
        stray_path = actions_dir / "run_000099.parquet"
        stray.to_parquet(stray_path, index=False)
        assert_equiv("STEP4 committed-run cap ignores uncommitted part", reduce_store(), step2_view)
        leaked = daily.load_active_actions(actions_dir, wallets=set(wallets), max_run_id=None)
        n_leaked = int((leaked["run_id"] == 99).sum()) if (leaked is not None and not leaked.empty) else 0
        assert n_leaked > 0, ("STEP4 sanity FAILED: stray run_99 did not leak even WITHOUT the cap -- "
                              "the test is not actually exercising the committed-run filter")
        print(f"[STEP4] PASS: stray run_99 IGNORED under committed cap (committed_run_id={committed}); "
              f"it would have leaked {n_leaked:,} corrupted rows without the cap.")
        stray_path.unlink()   # clean up so it cannot affect anything else

        # -- STEP 5: MIGRATION guard -- LEGACY journeys checkpoint (no actions marker) must FAIL LOUD
        # (codex P1 #1). Real deployment state: run_id-6 checkpoint written BEFORE the actions feature,
        # so it has no 'actions_bootstrapped' marker and no actions store. An incremental run there would
        # silently persist only today's actions, leaving all history absent. run_daily MUST refuse. The
        # trigger is MARKER ABSENCE (codex P2), so simulate a legacy checkpoint by STRIPPING the marker.
        tmp2 = Path(tempfile.mkdtemp(prefix="m02_actions_migrate_"))
        try:
            sd2, od2, ad2 = tmp2 / "state", tmp2 / "journeys", tmp2 / "actions"
            daily.run_daily(target_day=D1, state_dir=sd2, out_dir=od2, actions_dir=ad2,
                            wallets_file=str(wl_file), start_day=START, procs=2)
            cpf = sd2 / "checkpoint.json"
            cpj = json.loads(cpf.read_text())
            cpj.pop("actions_bootstrapped", None)   # simulate a LEGACY checkpoint predating the feature
            cpf.write_text(json.dumps(cpj))
            for p in ad2.glob("run_*.parquet"):
                p.unlink()                          # ... and its actions store was never built
            raised, msg = False, ""
            try:
                daily.run_daily(target_day=D2, state_dir=sd2, out_dir=od2, actions_dir=ad2,
                                wallets_file=str(wl_file), start_day=START, procs=2)
            except SystemExit as e:
                raised, msg = True, str(e)
            assert raised, ("STEP5 MIGRATION FAILED: expected a fail-loud SystemExit on a legacy "
                            "checkpoint with no actions marker, but the run proceeded silently")
            assert "bootstrap" in msg.lower(), \
                f"STEP5 MIGRATION: raised but the message lacks bootstrap guidance: {msg!r}"
            print(f"[STEP5] PASS: legacy checkpoint (no marker) FAILED LOUD as required -> {msg.splitlines()[0]}")
        finally:
            shutil.rmtree(tmp2, ignore_errors=True)

        # -- STEP 6: committed-but-EMPTY actions store must NOT false-trigger the migration guard --------
        # (codex P2). A first_run over a universe that produces ZERO qualifying actions is legitimately
        # bootstrapped (marker set) even though the store is physically empty. The NEXT run must proceed,
        # not demand an impossible re-bootstrap. Use a universe of NONEXISTENT wallets -> zero fills ->
        # zero actions, marker set.
        tmp3 = Path(tempfile.mkdtemp(prefix="m02_actions_empty_"))
        try:
            sd3, od3, ad3 = tmp3 / "state", tmp3 / "journeys", tmp3 / "actions"
            fake_wl = tmp3 / "wallets.txt"
            fake_wl.write_text("\n".join("0x" + f"{i:040x}" for i in range(5)) + "\n")
            r_empty = daily.run_daily(target_day=D1, state_dir=sd3, out_dir=od3, actions_dir=ad3,
                                      wallets_file=str(fake_wl), start_day=START, procs=2)
            assert r_empty["actions_fresh"] == 0, \
                f"STEP6 setup: expected 0 fresh actions for a nonexistent-wallet universe, got {r_empty['actions_fresh']}"
            cpj3 = json.loads((sd3 / "checkpoint.json").read_text())
            assert cpj3.get("actions_bootstrapped") is True, \
                "STEP6: committed-empty first_run did NOT set the actions_bootstrapped marker"
            # the next run over the same (empty) store must NOT raise the migration guard
            raised6 = False
            try:
                daily.run_daily(target_day=D2, state_dir=sd3, out_dir=od3, actions_dir=ad3,
                                wallets_file=str(fake_wl), start_day=START, procs=2)
            except SystemExit as e:
                raised6 = True; msg6 = str(e)
            assert not raised6, ("STEP6 FAILED: committed-but-empty store FALSE-TRIGGERED the migration "
                                 f"guard: {msg6!r}")
            print("[STEP6] PASS: committed-but-EMPTY actions store carries the marker and does NOT "
                  "false-trigger the migration guard.")
        finally:
            shutil.rmtree(tmp3, ignore_errors=True)

        # -- STEP 7: a downstream READER (load_active_closed) with the committed cap IGNORES an ---------
        # uncommitted JOURNEY run (codex P1 #2, the fix now wired into m03/m04/m05). Drop an ACTIVE,
        # run_id=99 journeys part with a sentinel-mutated realized_pnl into the closed store. Under the
        # committed cap it is ignored; without the cap it leaks -- proving the cap (not luck) protects
        # the readers that call load_active_closed.
        cap_j = daily.committed_run_id(state_dir)
        cdir = out_dir / "closed"
        jstray = pd.read_parquet(cdir / "run_000001.parquet").copy()
        jstray["run_id"] = 99
        jstray["active"] = True
        if "realized_pnl" in jstray.columns:
            jstray["realized_pnl"] = jstray["realized_pnl"].astype("float64") + 999.0
        jstray_path = cdir / "run_000099.parquet"
        jstray.to_parquet(jstray_path, index=False)
        capped = daily.load_active_closed(out_dir, max_run_id=cap_j)
        uncapped = daily.load_active_closed(out_dir, max_run_id=None)
        n_capped99 = int((capped["run_id"] == 99).sum()) if not capped.empty else 0
        n_uncapped99 = int((uncapped["run_id"] == 99).sum()) if not uncapped.empty else 0
        assert n_capped99 == 0, \
            f"STEP7 FAILED: committed cap did NOT hide the uncommitted journey run (leaked {n_capped99} rows)"
        assert n_uncapped99 > 0, \
            "STEP7 sanity FAILED: stray journey run did not leak even WITHOUT the cap (not exercising the filter)"
        jstray_path.unlink()
        print(f"[STEP7] PASS: reader committed cap (committed_run_id={cap_j}) IGNORES the uncommitted "
              f"journey run_99; it would leak {n_uncapped99:,} corrupted journey rows without it.")

        # -- STEP 8: the DELTA CURSOR shares the committed ceiling -- it must NOT advance past committed --
        # when uncommitted run parts are present, and MUST pick the run up once it commits (codex P1 final).
        # Store has committed runs 1..3 (checkpoint run_id=4 -> committed=3, next-convention). Drop TWO
        # UNCOMMITTED run parts (4 and 5) so run 4 is no longer newest -> WITHOUT the cap it would be
        # consumed and the cursor would jump to 4 (the exact bug: cursor past committed while the content
        # read is capped at 3 -> run 4 permanently skipped after it later commits).
        import m03_folds_daily as m3
        for rid in (4, 5):
            shutil.copyfile(cdir / "run_000001.parquet", cdir / f"run_{rid:06d}.parquet")
        cap = daily.committed_run_id(state_dir)          # == 3
        delta_capped, max_capped = m3._m2_new_run_wallets(out_dir, 0, max_run_id=cap)
        assert max_capped <= cap, \
            f"STEP8 FAILED: capped cursor advanced to {max_capped} past committed {cap}"
        # invisibility: the capped result must equal the result as if runs 4/5 did not exist at all
        for rid in (4, 5):
            (cdir / f"run_{rid:06d}.parquet").rename(cdir / f"run_{rid:06d}.parquet.hidden")
        delta_clean, max_clean = m3._m2_new_run_wallets(out_dir, 0, max_run_id=cap)
        for rid in (4, 5):
            (cdir / f"run_{rid:06d}.parquet.hidden").rename(cdir / f"run_{rid:06d}.parquet")
        assert (max_capped == max_clean and delta_capped == delta_clean), \
            (f"STEP8 FAILED: capped cursor NOT identical to the as-if-absent result "
             f"(capped max={max_capped}/{len(delta_capped)}w vs clean max={max_clean}/{len(delta_clean)}w)")
        # bug reproduction: WITHOUT the cap the cursor advances past committed into the uncommitted runs
        _, max_uncapped = m3._m2_new_run_wallets(out_dir, 0, max_run_id=None)
        assert max_uncapped > cap, \
            (f"STEP8 sanity FAILED: uncapped cursor did not advance past committed (max={max_uncapped}, "
             f"committed={cap}) -- the cap is not being exercised")
        # pickup-after-commit: advance the checkpoint (commit runs 4 & 5), cursor must now include run 4
        cpf = state_dir / "checkpoint.json"
        cpj = json.loads(cpf.read_text()); cpj["run_id"] = 6; cpf.write_text(json.dumps(cpj))
        cap2 = daily.committed_run_id(state_dir)          # == 5 now
        _, max_after = m3._m2_new_run_wallets(out_dir, max_capped, max_run_id=cap2)
        assert max_after > max_capped, \
            (f"STEP8 FAILED: after committing, the previously-skipped run was NOT picked up "
             f"(cursor stayed at {max_after} <= {max_capped})")
        for rid in (4, 5):
            (cdir / f"run_{rid:06d}.parquet").unlink()
        print(f"[STEP8] PASS: delta cursor capped at committed ({cap}) with uncommitted parts present "
              f"(max_capped={max_capped}, invisible==as-if-absent); WITHOUT the cap it advanced to "
              f"{max_uncapped} (bug); after commit (committed={cap2}) it picked the run up (max={max_after}).")

        print("\n===== ALL ASSERTIONS PASSED =====")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
