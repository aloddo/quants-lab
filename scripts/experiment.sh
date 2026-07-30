#!/bin/bash
# THE canonical experiment entrypoint.  Usage:  bash scripts/experiment.sh <manifest.yml> [--stage NAME]
#
# WHY THIS EXISTS (Alberto 2026-07-30): "FIX THE PIPELINE AND LIVE ENGINE FULLY SO WE CAN RUN ANY
# EXPERIMENT ANY TIME WITH EASE AND A CANONICAL PROCESS/INFRA". Three weeks of research produced no
# deployable cohort partly because every new hypothesis became a new bespoke script -- the standing
# correction is "ANY SELECTION MUST GO THROUGH THE FUCKING PIPELINE" and "NEVER AGAIN waste tokens
# building custom shit". So: a new experiment is a COPY OF A MANIFEST WITH FIELDS CHANGED. This script
# ORCHESTRATES and RECORDS; it contains ZERO analysis logic. All computation stays in the v15 modules.
#
# Guarantees (Fable plan-gate acceptance conditions):
#   1. Golden reproduction  -- same manifest twice => identical output hashes.
#
# STAGES: m05 -> m06a -> m07(pretest+test) -> m06b -> m08 -> oos, all from one manifest.
# (2026-07-30: the first version invoked ONLY m07/m06b/oos and consumed a pre-existing m06a shortlist,
#  so it could RE-RANK a shortlist but could not FIND a cohort. Alberto caught the overclaim; codex had
#  flagged "the runner has no M05 stage at all" inside defect #11 and I fixed only the arg-forwarding
#  half. m05/m06a/m08 are now real stages.)
#   2. Second experiment, zero code -- change the m06b weight vector in YAML, no .py touched.
#   3. Negative test        -- a mark source that cannot cover the OOS window exits non-zero.
#   4. Hostile box          -- every heavy stage runs under mem_safe_run.sh, which refuses or aborts
#                              with a reason rather than dying at rc=137 three hours in.
#   + a PROVENANCE record per run: manifest hash, input hashes, module git SHA, wall clock. Every number
#     argued about for three weeks was arguable because "which inputs produced this" had no answer.
#
# Resumable: a stage whose output already exists is SKIPPED (logged), so a failed run resumes cheaply.
# codex 2026-07-30 #12: `set -e` was ABSENT, so a failed provenance write, mkdir, or manifest eval fell
# through and the script could still print DONE and exit 0. Stage failures were checked explicitly but
# everything else was not. -e -u -o pipefail: fail on any unchecked error, on any unset variable
# (codex #13), and never let a pipeline hide a non-zero left-hand side.
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

MANIFEST="${1:?usage: experiment.sh <manifest.yml> [--stage NAME]}"
[ -f "$MANIFEST" ] || { echo "no such manifest: $MANIFEST" >&2; exit 2; }
ONLY_STAGE=""
if [ "${2:-}" = "--stage" ]; then
  ONLY_STAGE="${3:?--stage needs a name}"
  # codex #11: an UNKNOWN stage name silently ran nothing and exited 0. Validate against the real list.
  case "$ONLY_STAGE" in
    m05|m06a|m07|m06b|m08|oos) ;;
    *) echo "unknown --stage '$ONLY_STAGE' (valid: m05 m06a m07 m06b m08 oos)" >&2; exit 2;;
  esac
fi

PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
set -a; source .env 2>/dev/null || true; set +a

# ---- read the manifest into shell vars (python does the YAML; no yq dependency) ----------------- #
eval "$($PY - "$MANIFEST" <<'PYEOF'
import sys, yaml, shlex
m = yaml.safe_load(open(sys.argv[1]))
def emit(k, v):
    if v is None: return
    print(f"{k}={shlex.quote(str(v))}")
emit("M_NAME", m.get("name"))
for k, v in (m.get("inputs") or {}).items():   emit(f"IN_{k.upper()}", v)
emit("OUT_DIR", (m.get("output") or {}).get("dir"))
for k, v in (m.get("m05") or {}).items():      emit(f"M05_{k.upper()}", v)
for k, v in (m.get("m06a") or {}).items():     emit(f"M06A_{k.upper()}", v)
m07 = m.get("m07") or {}
for k, v in m07.items():
    if k != "windows": emit(f"M07_{k.upper()}", v)
emit("M07_WINDOWS", " ".join(m07.get("windows") or ["pretest", "test"]))
for k, v in (m.get("m06b") or {}).items():     emit(f"M06B_{k.upper()}", v)
o = m.get("oos") or {}
for k, v in o.items():
    if k != "windows": emit(f"OOS_{k.upper()}", v)
emit("OOS_WINDOWS", ",".join(str(w) for w in (o.get("windows") or [])))
emit("MEM_FLOOR", (m.get("resources") or {}).get("mem_safe_floor_gb", 2))
PYEOF
)"

RUN="$OUT_DIR"
mkdir -p "$RUN"
LOG="$RUN/experiment.log"
PROV="$RUN/provenance.json"
SAFE="scripts/mem_safe_run.sh --floor-gb ${MEM_FLOOR:-2}"
log(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

# ---- stage fingerprints (codex #1): bind every artifact to the manifest slice that produced it ------ #
# stage_fp <stage> -> sha256 of the manifest sections that stage depends on + its input file hashes.
stage_fp(){ $PY - "$MANIFEST" "$1" <<'PYEOF'
import sys, yaml, json, hashlib, os
m = yaml.safe_load(open(sys.argv[1])) or {}
stage = sys.argv[2]
DEPS = {  # which manifest sections + inputs each stage's result actually depends on
    "m05":  (["m05"], ["journeys", "folds", "m03_activity"]),
    "m06a": (["m05", "m06a"], ["folds", "actions"]),
    "m07":  (["m05", "m06a", "m07"], ["actions", "folds", "slip_calib"]),
    "m06b": (["m05", "m06a", "m07", "m06b"], ["m04_dir"]),
    "m08":  (["m05", "m06a", "m07", "m08"], ["slip_calib"]),
    "oos":  (["m05", "m06a", "m07", "m06b", "oos"], []),
}
secs, ins = DEPS[stage]
blob = {s: m.get(s) for s in secs}
for key in ins:
    path = (m.get("inputs") or {}).get(key)
    if isinstance(path, str) and os.path.isfile(path):
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for b in iter(lambda: f.read(1 << 20), b""): h.update(b)
        blob[f"input:{key}"] = h.hexdigest()
    else:
        blob[f"input:{key}"] = f"nonfile:{path}"
print(hashlib.sha256(json.dumps(blob, sort_keys=True, default=str).encode()).hexdigest()[:16])
PYEOF
}
# skip_stage <stage> <sentinel_artifact> -> 0 (skip) only if the artifact exists AND its fingerprint matches
skip_stage(){
  local stage="$1" artifact="$2" fpfile="$RUN/.fp_$1"
  [ -s "$artifact" ] || return 1
  [ -f "$fpfile" ] || { log "[$stage] artifact exists but has NO fingerprint -- rerunning (cannot prove it came from this manifest)"; return 1; }
  local want; want=$(stage_fp "$stage")
  if [ "$(cat "$fpfile")" = "$want" ]; then return 0; fi
  log "[$stage] fingerprint MISMATCH (manifest or inputs changed since that artifact) -- rerunning"
  return 1
}
mark_stage(){ stage_fp "$1" > "$RUN/.fp_$1"; }
stage_wanted(){ [ -z "$ONLY_STAGE" ] || [ "$ONLY_STAGE" = "$1" ]; }

# Refuse to write into a symlinked run dir -- census20k's m05 files are symlinks into funnel20k, and
# writing through one silently overwrites another run's inputs (near miss, 2026-07-30).
# codex #14: checking only the FINAL component let a symlinked ANCESTOR through, and the log +
# provenance writes happen before any ShardedParquetWriter guard can object. Walk every component.
_p="$RUN"
while [ "$_p" != "." ] && [ "$_p" != "/" ]; do
  if [ -L "$_p" ]; then
    echo "REFUSING: run dir path component $_p is a SYMLINK -> $(readlink "$_p")" >&2; exit 2
  fi
  _p="$(dirname "$_p")"
done

log "=== EXPERIMENT '$M_NAME' manifest=$MANIFEST run_dir=$RUN ==="
T_START=$(date +%s)

# ---- PROVENANCE: what produced this, recorded BEFORE any compute --------------------------------- #
$PY - "$MANIFEST" "$PROV" "$RUN" <<'PYEOF'
import sys, json, hashlib, subprocess, os, datetime
manifest, out, run = sys.argv[1], sys.argv[2], sys.argv[3]
def sha(p):
    h = hashlib.sha256()
    try:
        with open(p, "rb") as f:
            for b in iter(lambda: f.read(1 << 20), b""): h.update(b)
        return h.hexdigest()
    except Exception as e:
        return f"UNREADABLE:{e}"
import yaml
m = yaml.safe_load(open(manifest))
inputs = {}
for k, v in (m.get("inputs") or {}).items():
    if isinstance(v, str) and os.path.isfile(v):
        inputs[k] = {"path": v, "sha256": sha(v), "bytes": os.path.getsize(v),
                     "mtime": datetime.datetime.fromtimestamp(os.path.getmtime(v)).isoformat()}
    else:
        inputs[k] = {"path": v, "note": "dir or absent"}
git = lambda a: subprocess.run(["git"] + a, capture_output=True, text=True).stdout.strip()
json.dump({
    "experiment": m.get("name"),
    "manifest_path": manifest,
    "manifest_sha256": sha(manifest),
    "manifest_resolved": m,
    "git_sha": git(["rev-parse", "HEAD"]),
    "git_dirty": bool(git(["status", "--porcelain"])),
    "inputs": inputs,
    "started_utc": datetime.datetime.now(datetime.UTC).isoformat(),
}, open(out, "w"), indent=1, default=str)
print(f"provenance -> {out}")
PYEOF
log "provenance written"

# ---- STAGE m05: eligibility ---------------------------------------------------------------------- #
# The equity lane needs M1, which is OUT OF SCOPE (Alberto 2026-07-17, reconfirmed 07-30 "No M1 no
# equity"), so mode is copyability and the account-quality gates are intentionally absent here.
if stage_wanted m05; then
  if skip_stage "m05" "$RUN/m05_eligibility.parquet"; then
    log "[m05] SKIP (fingerprint matches)"
  else
    rc=0
    log "[m05] START mode=${M05_MODE:-copyability}"
    $SAFE --label m05_"$M_NAME" -- $PY research/v15/v15_m05_eligibility.py \
      --mode "${M05_MODE:-copyability}" --folds "$IN_FOLDS" --journeys "$IN_JOURNEYS" \
      --m04-dir "$IN_M04_DIR" --m03-activity "$IN_M03_ACTIVITY" --outdir "$RUN" >>"$LOG" 2>&1 || rc=$?
    log "[m05] rc=${rc:-0}"
    if [ "${rc:-0}" -ne 0 ]; then log "[m05] FAILED -- stopping"; exit 1; fi
    mark_stage m05
  fi
fi

# ---- STAGE m06a: shortlist ---------------------------------------------------------------------- #
if stage_wanted m06a; then
  if skip_stage "m06a" "$RUN/m06a_shortlist.parquet"; then
    log "[m06a] SKIP (fingerprint matches)"
  else
    rc=0
    log "[m06a] START${M06A_MANIFEST:+ manifest=$(basename "$M06A_MANIFEST")}"
    $SAFE --label m06a_"$M_NAME" -- $PY research/v15/v15_m06a_shortlist.py \
      --eligibility "$RUN/m05_eligibility.parquet" --pool-summary "$RUN/m05_pool_summary.parquet" \
      --folds "$IN_FOLDS" --m04-dir "$IN_M04_DIR" --actions "$IN_ACTIONS" \
      ${M06A_MANIFEST:+--manifest "$M06A_MANIFEST"} \
      --outdir "$RUN" >>"$LOG" 2>&1 || rc=$?
    log "[m06a] rc=${rc:-0}"
    if [ "${rc:-0}" -ne 0 ]; then log "[m06a] FAILED -- stopping"; exit 1; fi
    mark_stage m06a
  fi
fi

# ---- STAGE m07: the copy simulation, ONE RUN PER WINDOW ----------------------------------------- #
# pretest = the ranking input [train_start, test_start); test = the held-out OOS confirm window.
# m06b requires the pretest run as --m07-dir; passing the test run there is look-ahead and its
# provenance gate refuses it (verified: first golden run failed exactly this way).
if stage_wanted m07; then
  for W in $M07_WINDOWS; do
    M07_OUT="$RUN/m07_${W}"
    # codex #4: the summary is written BEFORE equity and positions, so a run that failed after the
    # summary closed would leave a non-empty summary and be skipped as complete. Require ALL of them.
    if [ -s "$M07_OUT/m07_summary.parquet" ] && [ -s "$M07_OUT/m07_positions.parquet" ] \
       && [ -s "$M07_OUT/m07_equity.parquet" ] && skip_stage "m07" "$M07_OUT/m07_summary.parquet"; then
      log "[m07:$W] SKIP (complete + fingerprint matches)"
      continue
    fi
    # Use the shortlist THIS run produced when m06a ran; fall back to a manifest-pinned one only if
    # the manifest explicitly supplies it (e.g. the golden reproduction, which pins the census artifact).
    SL="${M07_SHORTLIST:-$RUN/m06a_shortlist.parquet}"
    if [ ! -s "$SL" ]; then log "[m07:$W] FAILED -- no shortlist at $SL"; exit 1; fi
    log "[m07:$W] START sizing=$M07_SIZING_MODE policy=$M07_COPY_POLICY shortlist=$(basename "$SL")${M07_LIMIT_ENTITIES:+ limit=$M07_LIMIT_ENTITIES}"
    rc=0
    $SAFE --label m07_"${M_NAME}_$W" -- $PY research/v15/v15_m07_engine.py \
      --actions "$IN_ACTIONS" --shortlist "$SL" --folds "$IN_FOLDS" \
      --out "$M07_OUT" --window "$W" --slip-calib "$IN_SLIP_CALIB" \
      --copy-latency-ms "$M07_COPY_LATENCY_MS" --sizing-mode "$M07_SIZING_MODE" \
      --fixed-target-exposure "$M07_FIXED_TARGET_EXPOSURE" \
      --copy-policy "$M07_COPY_POLICY" ${M07_LIMIT_ENTITIES:+--limit $M07_LIMIT_ENTITIES} \
      >>"$LOG" 2>&1 || rc=$?
    log "[m07:$W] rc=${rc:-0}"
    if [ "${rc:-0}" -ne 0 ]; then log "[m07:$W] FAILED -- stopping (fail closed)"; exit 1; fi
  done
  mark_stage m07
fi

# ---- STAGE m06b: ranking + walk-forward confirm (THE SELECTION HYPOTHESIS) ----------------------- #
if stage_wanted m06b; then
  if skip_stage "m06b" "$RUN/m06b_confirmed.parquet"; then
    log "[m06b] SKIP (fingerprint matches)"
  else
    rc=0
    log "[m06b] START weights mean_r=$M06B_W_PP_MEAN_R t=$M06B_W_PP_T std=$M06B_W_PP_STD mtm_dd=$M06B_W_PP_MTM_DD quick=$M06B_W_PP_QUICK fdr_q=$M06B_FDR_Q"
    # Arg list derived FROM THE MANIFEST (codex #11). Every m06b key must map to a real CLI flag or the
    # run refuses -- a manifest must never record a hypothesis it did not execute.
    M06B_ARGS=$($PY - "$MANIFEST" <<'PYEOF'
import sys, yaml, shlex
FLAG = {  # manifest key -> m06b CLI flag. Anything absent here is a HARD ERROR, never silently dropped.
    "w_pp_mean_r": "--w-pp-mean-r", "w_pp_t": "--w-pp-t", "w_pp_std": "--w-pp-std",
    "w_pp_mtm_dd": "--w-pp-mtm-dd", "w_pp_quick": "--w-pp-quick",
    "pp_min_positions": "--pp-min-positions", "pp_min_lcb_mean_r": "--pp-min-lcb-mean-r",
    "pp_max_med_hold_h": "--pp-max-med-hold-h", "pp_max_mtm_dd": "--pp-max-mtm-dd",
    "fdr_q": "--fdr-q", "oos_min_folds": "--oos-min-folds",
    "oos_min_journeys_pooled": "--oos-min-journeys-pooled",
    "oos_min_frac_folds_pos": "--oos-min-frac-folds-pos", "oos_margin": "--oos-margin",
    "fee_schedule_version": "--fee-schedule-version",
    "slippage_calibration_version": "--slippage-calibration-version",
}
m6 = (yaml.safe_load(open(sys.argv[1])) or {}).get("m06b") or {}
unknown = [k for k in m6 if k not in FLAG]
if unknown:
    sys.exit(f"MANIFEST ERROR: m06b keys {unknown} have no CLI flag -- they would be recorded in "
             f"provenance and NEVER APPLIED. Add them to FLAG in experiment.sh or remove them.")
print(" ".join(f"{FLAG[k]} {shlex.quote(str(v))}" for k, v in m6.items()))
PYEOF
) || { log "[m06b] FAILED -- manifest/flag mismatch"; exit 1; }
    log "[m06b] forwarding: $M06B_ARGS"
    # shellcheck disable=SC2086  # deliberate word-split: M06B_ARGS is a pre-quoted flag list
    $SAFE --label m06b_"$M_NAME" -- $PY research/v15/v15_m06b_ranking.py \
      --m07-dir "$RUN/m07_pretest" --m07-test-dir "$RUN/m07_test" \
      --m04-dir "$IN_M04_DIR" --out "$RUN" $M06B_ARGS >>"$LOG" 2>&1 || rc=$?
    log "[m06b] rc=${rc:-0}"
    if [ "${rc:-0}" -ne 0 ]; then log "[m06b] FAILED -- stopping"; exit 1; fi
    mark_stage m06b
  fi
fi

# ---- STAGE m08: counterfactual survival tiering (POST-engine) ----------------------------------- #
if stage_wanted m08; then
  if skip_stage "m08" "$RUN/m08_survival.parquet"; then
    log "[m08] SKIP (fingerprint matches)"
  else
    rc=0
    log "[m08] START"
    $SAFE --label m08_"$M_NAME" -- $PY research/v15/v15_m08_survival.py \
      --m07-dir "$RUN/m07_pretest" --out "$RUN" --m04-dir "$IN_M04_DIR" \
      --slip-calib "$IN_SLIP_CALIB" >>"$LOG" 2>&1 || rc=$?
    log "[m08] rc=${rc:-0}"
    if [ "${rc:-0}" -ne 0 ]; then log "[m08] FAILED -- stopping"; exit 1; fi
    mark_stage m08
  fi
fi

# ---- STAGE oos: forward validation on LOCAL data (never the API) --------------------------------- #
if stage_wanted oos; then
  OOS_OUT="$RUN/forward_oos.parquet"
  if skip_stage "oos" "$OOS_OUT"; then
    log "[oos] SKIP (fingerprint matches)"
  elif [ ! -s "$RUN/m06b_confirmed.parquet" ]; then
    # If the caller ASKED for oos, having no input is a FAILURE, not a skip -- otherwise "the guard
    # fired" and "nothing ran" are indistinguishable and a negative test reports a false pass (codex #11).
    if [ -n "$ONLY_STAGE" ]; then
      log "[oos] FAILED -- --stage oos requested but $RUN/m06b_confirmed.parquet does not exist"
      exit 1
    fi
    log "[oos] SKIP -- no m06b_confirmed.parquet to validate (full-pipeline run)"
  else
    UNIV="$RUN/oos_universe.txt"
    rc=0
    # NO PIPE HERE. `... | tee` masks the python exit code with tee's, so a failure to build the universe
    # file sailed through and the next stage died on FileNotFoundError instead (2026-07-30). That is the
    # third time today a pipe hid an exit code from me; check the code, then append.
    $PY -c "
import pandas as pd
w = pd.read_parquet('$RUN/m06b_confirmed.parquet').primary_wallet.str.lower().tolist()
if not w:
    raise SystemExit('m06b_confirmed.parquet has ZERO rows -- nothing confirmed, so there is nothing '
                     'to validate out of sample. This is a real result, not a plumbing failure: check '
                     'the m06b waterfall (pretest_rankable_entities/eligible/confirmed) above.')
open('$UNIV', 'w').write('\n'.join(w) + '\n')
print(f'{len(w)} wallets -> $UNIV')" >>"$LOG" 2>&1 || rc=$?
    if [ "${rc:-0}" -ne 0 ]; then
      log "[oos] SKIP -- could not build the OOS universe (rc=${rc}); see the log for why"
      tail -3 "$LOG" | sed 's/^/    /'
      exit 1
    fi
    log "[oos] universe: $(wc -l < "$UNIV" | tr -d ' ') wallets"
    rc=0
    # An unverifiable mark source is a hard refusal unless the MANIFEST acknowledges it (auditable in
    # provenance), rather than an env var someone sets by hand and forgets.
    if [ "${OOS_ALLOW_UNVERIFIED_MARKS:-false}" = "True" ] || [ "${OOS_ALLOW_UNVERIFIED_MARKS:-false}" = "true" ]; then
      export QL_ALLOW_UNVERIFIED_MARKS=1
      log "[oos] manifest acknowledges UNVERIFIED mark coverage -- any n=0 window in this run is SUSPECT, not evidence"
    else
      unset QL_ALLOW_UNVERIFIED_MARKS || true
    fi
    log "[oos] START mark_source=$OOS_MARK_SOURCE windows=$OOS_WINDOWS"
    $PY research/v15/forward_oos_hot.py --universe-file "$UNIV" \
      --windows "$OOS_WINDOWS" --mark-source "$OOS_MARK_SOURCE" \
      --min-trades "$OOS_MIN_TRADES" --out "$OOS_OUT" >>"$LOG" 2>&1 || rc=$?
    log "[oos] rc=${rc:-0}"
    # rc!=0 here is usually the mark-coverage assertion firing -- that is a CORRECT refusal, not a bug.
    if [ "${rc:-0}" -ne 0 ]; then log "[oos] FAILED (check for MARK COVERAGE above) -- stopping"; exit 1; fi
    mark_stage oos
  fi
fi

# ---- close out provenance: hashes of everything produced + wall clock ---------------------------- #
T_END=$(date +%s)
$PY - "$PROV" "$RUN" "$((T_END-T_START))" <<'PYEOF'
import sys, json, hashlib, os
prov, run, secs = sys.argv[1], sys.argv[2], int(sys.argv[3])
def sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""): h.update(b)
    return h.hexdigest()
d = json.load(open(prov))
outs = {}
for root, _dirs, files in os.walk(run):
    for f in sorted(files):
        if f.endswith((".parquet", ".json", ".csv")) and f != "provenance.json":
            p = os.path.join(root, f)
            outs[os.path.relpath(p, run)] = {"sha256": sha(p), "bytes": os.path.getsize(p)}
d["outputs"] = outs
d["wall_clock_s"] = secs
json.dump(d, open(prov, "w"), indent=1, default=str)
print(f"provenance closed: {len(outs)} outputs, {secs}s")
PYEOF
log "=== DONE in $((T_END-T_START))s -- provenance: $PROV ==="
