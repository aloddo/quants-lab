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
# STAGES: m05 -> m06a -> m07(pretest+test) -> m06b -> m08 -> m09 -> m10 -> oos,
# all from one manifest.
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
    m05|m06a|m07|m06b|m08|m09|m10|oos) ;;
    *) echo "unknown --stage '$ONLY_STAGE' (valid: m05 m06a m07 m06b m08 m09 m10 oos)" >&2; exit 2;;
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
for k, v in (m.get("m09") or {}).items():      emit(f"M09_{k.upper()}", v)
for k, v in (m.get("m10") or {}).items():      emit(f"M10_{k.upper()}", v)
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
import sys, yaml, json, hashlib, os, glob
m = yaml.safe_load(open(sys.argv[1])) or {}
stage = sys.argv[2]
DEPS = {  # which manifest sections + inputs each stage's result actually depends on
    "m05":  (["m05"], ["journeys", "folds", "m03_activity"]),
    "m06a": (["m05", "m06a"], ["folds", "actions"]),
    "m07":  (["m05", "m06a", "m07"], ["actions", "folds", "slip_calib"]),
    "m06b": (["m05", "m06a", "m07", "m06b"], ["m04_dir", "leader_panel"]),
    "m08":  (["m05", "m06a", "m07", "m08", "m09"], ["slip_calib"]),
    "m09":  (["m05", "m06a", "m07", "m06b", "m08", "m09"],
             ["actions", "action_shards", "folds", "m04_dir", "slip_calib"]),
    "m10":  (["m05", "m06a", "m07", "m06b", "m08", "m09", "m10"], ["action_shards"]),
    "oos":  (["m05", "m06a", "m07", "m06b", "oos"], []),
}
secs, ins = DEPS[stage]
blob = {s: m.get(s) for s in secs}
for key in ins:
    path = (m.get("inputs") or {}).get(key)
    if key == "leader_panel" and path is None:
        # codex 2026-08-07 #2: the leader-space profile panel is a REAL m06b stage input but is set
        # under the m06b: manifest section (it is a module knob), not inputs:. Hash the FILE so a
        # changed panel invalidates the m06b fingerprint instead of silently reusing a stale one;
        # an unset/absent panel falls through to the existing missing:-marker behavior below.
        path = (m.get("m06b") or {}).get("leader_panel")
    if isinstance(path, str) and os.path.isfile(path):
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for b in iter(lambda: f.read(1 << 20), b""): h.update(b)
        blob[f"input:{key}"] = h.hexdigest()
    elif isinstance(path, str) and os.path.isdir(path):
        patterns = (["m04_entities_f*.parquet", "m04_authenticity_f*.parquet"]
                    if key == "m04_dir" else (["._complete"] if key == "action_shards" else ["**/*"]))
        files = sorted({p for pattern in patterns for p in glob.glob(os.path.join(path, pattern), recursive=True)
                        if os.path.isfile(p)})
        inventory = []
        for p in files:
            h = hashlib.sha256()
            with open(p, "rb") as f:
                for b in iter(lambda: f.read(1 << 20), b""): h.update(b)
            inventory.append((os.path.relpath(p, path), h.hexdigest(), os.path.getsize(p)))
        blob[f"input:{key}"] = inventory
    else:
        blob[f"input:{key}"] = f"missing:{path}"
# Bind resumability to the implementation and the concrete upstream artifacts, not only the YAML.
# Otherwise a repaired stage can be silently skipped because its manifest did not change.
CODE_FILES = {
    "m05": ["research/v15/v15_m05_eligibility.py"],
    "m06a": ["research/v15/v15_m06a_shortlist.py"],
    "m07": ["research/v15/v15_m07_engine.py"],
    "m06b": ["research/v15/v15_m06b_ranking.py"],
    "m08": ["research/v15/v15_m08_survival.py"],
    "m09": ["research/v15/v15_m09_sim.py"],
    "m10": ["research/v15/v15_m10_gates.py", "research/v15/v15_m09_sim.py"],
    "oos": ["research/v15/forward_oos_hot.py"],
}
run = (m.get("output") or {}).get("dir") or ""
UPSTREAM = {
    "m06a": ["m05_eligibility.parquet", "m05_pool_summary.parquet"],
    "m07": ["m06a_shortlist.parquet"],
    "m06b": [".fp_m07", "m05_eligibility.parquet", "m06a_shortlist.parquet"],
    "m08": [".fp_m06b", "m06b_pool.parquet"],
    "m09": [".fp_m08", "m06b_pool.parquet", "m08_tiers.parquet"],
    "m10": [".fp_m09", "m09_result.json"],
    "oos": [".fp_m06b", "m06b_confirmed.parquet"],
}
for label, paths in (("code", CODE_FILES.get(stage, [])),
                     ("upstream", [os.path.join(run, p) for p in UPSTREAM.get(stage, [])])):
    for path in paths:
        if not os.path.isfile(path):
            blob[f"{label}:{path}"] = "missing"
            continue
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for b in iter(lambda: f.read(1 << 20), b""): h.update(b)
        blob[f"{label}:{path}"] = h.hexdigest()
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

# Every canonical attempt gets a hash-chained START before computation and one
# terminal COMPLETE/FAIL event. A dangling START therefore means interruption,
# never an ambiguous success. The per-run terminal receipt can later be pinned
# by a deployment manifest without pinning the ever-growing global registry.
REGISTRY="${EXPERIMENT_REGISTRY:-app/data/v15/experiment_registry.jsonl}"
RUN_ID=$($PY tools/experiment_registry.py start --registry "$REGISTRY" \
  --manifest "$MANIFEST" --run-dir "$RUN")
REGISTRY_RECEIPT="$RUN/registry_${RUN_ID}.json"
RUN_TERMINAL=0
record_failed_run(){
  local rc=$?
  if [ "$rc" -ne 0 ] && [ "$RUN_TERMINAL" -eq 0 ]; then
    set +e
    if [ -f "$PROV" ]; then
      $PY - "$PROV" "$rc" <<'PYEOF'
import datetime, json, sys
p, rc = sys.argv[1], int(sys.argv[2])
try:
    d = json.load(open(p))
except Exception:
    d = {}
d["status"] = "failed"
d["exit_code"] = rc
d["completed_utc"] = datetime.datetime.now(datetime.UTC).isoformat()
json.dump(d, open(p, "w"), indent=1, default=str)
PYEOF
    fi
    $PY tools/experiment_registry.py fail --registry "$REGISTRY" --run-id "$RUN_ID" \
      --manifest "$MANIFEST" --run-dir "$RUN" --provenance "$PROV" \
      --receipt "$REGISTRY_RECEIPT" --reason "experiment.sh exit_code=$rc" >/dev/null 2>&1
  fi
  return "$rc"
}
trap record_failed_run EXIT

# Provision the run dir with the small artifacts modules expect to find beside their outputs.
# m06b (--data-dir) and m08 both read m03_folds.parquet from their data dir and neither has a flag for
# it in every path; it is 11KB, and a run dir that carries its own fold geometry is better provenance.
# The BIG inputs (m02_actions 4.65GB, m02_journeys 1GB) are passed explicitly instead -- never copied,
# never symlinked (symlinking is the cross-run corruption hazard).
if [ ! -s "$RUN/m03_folds.parquet" ]; then cp "$IN_FOLDS" "$RUN/m03_folds.parquet"; fi

log "=== EXPERIMENT '$M_NAME' manifest=$MANIFEST run_dir=$RUN ==="
T_START=$(date +%s)

# ---- PROVENANCE: what produced this, recorded BEFORE any compute --------------------------------- #
$PY - "$MANIFEST" "$PROV" "$RUN" "$RUN_ID" <<'PYEOF'
import sys, json, hashlib, subprocess, os, datetime, glob
manifest, out, run, run_id = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
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
    elif isinstance(v, str) and os.path.isdir(v):
        patterns = (["m04_entities_f*.parquet", "m04_authenticity_f*.parquet"]
                    if k == "m04_dir" else ["**/*"])
        files = sorted({p for pattern in patterns for p in glob.glob(os.path.join(v, pattern), recursive=True)
                        if os.path.isfile(p)})
        inventory = [{"path": os.path.relpath(p, v), "sha256": sha(p), "bytes": os.path.getsize(p)}
                     for p in files]
        tree_raw = json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()
        inputs[k] = {"path": v, "kind": "directory", "patterns": patterns,
                     "tree_sha256": hashlib.sha256(tree_raw).hexdigest(),
                     "n_files": len(inventory), "bytes": sum(x["bytes"] for x in inventory),
                     "files": inventory}
    else:
        inputs[k] = {"path": v, "error": "missing input"}
git = lambda a: subprocess.run(["git"] + a, capture_output=True, text=True).stdout.strip()
json.dump({
    "experiment": m.get("name"),
    "run_id": run_id,
    "status": "running",
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
  # 2026-07-30 (Alberto TG 12095/12097): start_equity is REQUIRED, never defaulted. m07 sizes every
  # position as `fixed_target_exposure * cur_eq` (v15_m07_engine.py:786), so start_equity IS the
  # position size and therefore decides which leader actions clear the $10 MIN_ORDER_NOTIONAL floor
  # (:1049). The engine's own CLI default is $10,000 and this runner never passed it, so EVERY
  # experiment silently simulated a book 10.7x our real $937 -- the cohort search included. At $1,015
  # simulated positions the sim copied 0.13 adds/position against the leaders' 2.32; at our real size
  # the mean surviving add ($25.21, 2.5% of position) falls to $2.36 and every add vanishes under the
  # floor. A capital assumption that changes WHICH actions execute must be declared by the experiment,
  # not inherited from a library default.
  if [ -z "${M07_START_EQUITY:-}" ]; then
    log "[m07] FAILED -- manifest does not set m07.start_equity. It is REQUIRED: it sets position size"
    log "[m07]           and thus which actions clear the \$10 minimum. Declare it (ours: 937.47)."
    exit 1
  fi
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
    M07_NWORKERS="${M07_WORKERS:-1}"
    if ! [[ "$M07_NWORKERS" =~ ^[1-9][0-9]*$ ]]; then
      log "[m07:$W] FAILED -- m07.workers must be a positive integer, got '$M07_NWORKERS'"; exit 1
    fi
    log "[m07:$W] START sizing=$M07_SIZING_MODE policy=$M07_COPY_POLICY start_equity=\$$M07_START_EQUITY shortlist=$(basename "$SL") workers=$M07_NWORKERS${M07_LIMIT_ENTITIES:+ limit=$M07_LIMIT_ENTITIES}"
    rc=0
    if [ "$M07_NWORKERS" -eq 1 ]; then
      $SAFE --label m07_"${M_NAME}_$W" -- $PY research/v15/v15_m07_engine.py \
        --actions "$IN_ACTIONS" --shortlist "$SL" --folds "$IN_FOLDS" \
        --out "$M07_OUT" --window "$W" --slip-calib "$IN_SLIP_CALIB" \
        --copy-latency-ms "$M07_COPY_LATENCY_MS" --sizing-mode "$M07_SIZING_MODE" \
        --fixed-target-exposure "$M07_FIXED_TARGET_EXPOSURE" \
        --fixed-notional-usd "${M07_FIXED_NOTIONAL_USD:-100.0}" \
        --reversal-mode "${M07_REVERSAL_MODE:-flip}" \
        ${M07_EXIT_LATENCY_MS:+--exit-latency-ms "$M07_EXIT_LATENCY_MS"} \
        --exit-entry-grace-ms "${M07_EXIT_ENTRY_GRACE_MS:-90000}" \
        --leader-dust-floor-usd "${M07_LEADER_DUST_FLOOR_USD:-0.0}" \
        ${M07_SL_BPS:+--sl-bps "$M07_SL_BPS"} \
        ${M07_GLOBAL_STOP_PCT:+--global-stop-pct "$M07_GLOBAL_STOP_PCT"} \
        --start-equity "$M07_START_EQUITY" \
        --copy-policy "$M07_COPY_POLICY" ${M07_LIMIT_ENTITIES:+--limit $M07_LIMIT_ENTITIES} \
        >>"$LOG" 2>&1 || rc=$?
    else
      pids=(); shard_dirs=(); shard_logs=()
      for ((i=0; i<M07_NWORKERS; i++)); do
        sd="$RUN/.m07_${W}_worker_${i}"
        slog="$RUN/.m07_${W}_worker_${i}.log"
        shard_dirs+=("$sd"); shard_logs+=("$slog")
        $SAFE --label m07_"${M_NAME}_${W}_w${i}" -- $PY research/v15/v15_m07_engine.py \
          --actions "$IN_ACTIONS" --shortlist "$SL" --folds "$IN_FOLDS" \
          --out "$sd" --window "$W" --slip-calib "$IN_SLIP_CALIB" \
          --copy-latency-ms "$M07_COPY_LATENCY_MS" --sizing-mode "$M07_SIZING_MODE" \
          --fixed-target-exposure "$M07_FIXED_TARGET_EXPOSURE" --start-equity "$M07_START_EQUITY" \
          --fixed-notional-usd "${M07_FIXED_NOTIONAL_USD:-100.0}" \
          --reversal-mode "${M07_REVERSAL_MODE:-flip}" \
          ${M07_EXIT_LATENCY_MS:+--exit-latency-ms "$M07_EXIT_LATENCY_MS"} \
          --exit-entry-grace-ms "${M07_EXIT_ENTRY_GRACE_MS:-90000}" \
          --leader-dust-floor-usd "${M07_LEADER_DUST_FLOOR_USD:-0.0}" \
          ${M07_SL_BPS:+--sl-bps "$M07_SL_BPS"} \
          ${M07_GLOBAL_STOP_PCT:+--global-stop-pct "$M07_GLOBAL_STOP_PCT"} \
          --copy-policy "$M07_COPY_POLICY" --shard-index "$i" --shard-count "$M07_NWORKERS" \
          ${M07_LIMIT_ENTITIES:+--limit $M07_LIMIT_ENTITIES} >"$slog" 2>&1 &
        pids+=("$!")
      done
      for pid in "${pids[@]}"; do wait "$pid" || rc=$?; done
      for slog in "${shard_logs[@]}"; do cat "$slog" >>"$LOG"; done
      if [ "$rc" -eq 0 ]; then
        merge_args=()
        for sd in "${shard_dirs[@]}"; do merge_args+=(--merge-shard-dir "$sd"); done
        $SAFE --label m07_"${M_NAME}_${W}_merge" -- $PY research/v15/v15_m07_engine.py \
          --out "$M07_OUT" "${merge_args[@]}" >>"$LOG" 2>&1 || rc=$?
      fi
    fi
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
    # NATIVE BEHAVIOR GATES (2026-08-07, Alberto TG-12245): post-FDR confirmed-set vetoes, formerly
    # the side scripts post_m06b_hard_gates.py / build_roster_freeze.py. leader_panel is REQUIRED
    # iff any leader-tier gate (uw_add / leader_liq / leader_mae_p90) is set (three-tier law).
    "leader_panel": "--leader-panel",
    "gate_uw_add_max": "--gate-uw-add-max",
    "gate_leader_liq_max": "--gate-leader-liq-max",
    "gate_leader_mae_p90_max": "--gate-leader-mae-p90-max",
    "gate_two_sided_lo": "--gate-two-sided-lo",
    "gate_two_sided_hi": "--gate-two-sided-hi",
    "gate_latency_ratio_max": "--gate-latency-ratio-max",
    "copy_latency_s": "--copy-latency-s",
    # MARK-COVERAGE gate (2026-08-07): per-fold unpriced-action refusal knobs.
    "unpriced_warn_pct": "--unpriced-warn-pct",
    "unpriced_refuse_pct": "--unpriced-refuse-pct",
    "allow_unpriced_folds": "--allow-unpriced-folds",
}
# store_true flags take NO value: emit the bare flag iff the manifest value is truthy.
BARE = {"allow_unpriced_folds"}
m6 = (yaml.safe_load(open(sys.argv[1])) or {}).get("m06b") or {}
unknown = [k for k in m6 if k not in FLAG]
if unknown:
    sys.exit(f"MANIFEST ERROR: m06b keys {unknown} have no CLI flag -- they would be recorded in "
             f"provenance and NEVER APPLIED. Add them to FLAG in experiment.sh or remove them.")
# codex 2026-08-07 #1 (mirrors the m9 P2-F fix): an explicit YAML null (e.g. `gate_uw_add_max: null`)
# parses to Python None and would serialize as the literal string "None" into argv. A null means
# "knob off / module default" -> emit no flag.
print(" ".join(FLAG[k] if k in BARE else f"{FLAG[k]} {shlex.quote(str(v))}"
               for k, v in m6.items() if v is not None and (k not in BARE or bool(v))))
PYEOF
) || { log "[m06b] FAILED -- manifest/flag mismatch"; exit 1; }
    log "[m06b] forwarding: $M06B_ARGS"
    # shellcheck disable=SC2086  # deliberate word-split: M06B_ARGS is a pre-quoted flag list
    $SAFE --label m06b_"$M_NAME" -- $PY research/v15/v15_m06b_ranking.py \
      --m07-dir "$RUN/m07_pretest" --m07-test-dir "$RUN/m07_test" \
      --m04-dir "$IN_M04_DIR" --out "$RUN" --data-dir "$RUN" \
      --m02-journeys "$IN_JOURNEYS" $M06B_ARGS >>"$LOG" 2>&1 || rc=$?
    log "[m06b] rc=${rc:-0}"
    if [ "${rc:-0}" -ne 0 ]; then log "[m06b] FAILED -- stopping"; exit 1; fi
    mark_stage m06b
  fi
fi

# ---- STAGE m08: counterfactual survival tiering (POST-engine) ----------------------------------- #
if stage_wanted m08; then
  if skip_stage "m08" "$RUN/m08_tiers.parquet"; then
    log "[m08] SKIP (fingerprint matches)"
  else
    # m08 has no --folds flag: it reads m03_folds.parquet from its --out dir (v15_m08_survival.py:283).
    # COPY it in (11KB) rather than symlink -- the symlink-write guard correctly refuses links, and a run
    # dir that carries its own fold geometry is better provenance anyway.
    rc=0
    log "[m08] START"
    # codex 2026-08-07 P1: m08's replica controls must match the M09-EFFECTIVE configuration (its
    # tier feeds m09 allocation). EVERY knob resolves M09-override -> M07 -> default, consistently.
    M08_EXIT_LAT="${M09_EXIT_LATENCY_MS:-${M07_EXIT_LATENCY_MS:-}}"
    M08_SLB="${M09_SL_BPS:-${M07_SL_BPS:-}}"
    M08_GSP="${M09_GLOBAL_STOP_PCT:-${M07_GLOBAL_STOP_PCT:-}}"
    $SAFE --label m08_"$M_NAME" -- $PY research/v15/v15_m08_survival.py \
      --m07-dir "$RUN/m07_pretest" --out "$RUN" --m04-dir "$IN_M04_DIR" \
      --actions "$IN_ACTIONS" --folds "$IN_FOLDS" \
      --nominal-capital "${M09_B0:-$M07_START_EQUITY}" \
      --sizing-mode "${M09_SIZING_MODE:-$M07_SIZING_MODE}" \
      --fixed-target-exposure "${M09_FIXED_TARGET_EXPOSURE:-${M07_FIXED_TARGET_EXPOSURE:-0.10}}" \
      --fixed-notional-usd "${M09_FIXED_NOTIONAL_USD:-${M07_FIXED_NOTIONAL_USD:-100.0}}" \
      --reversal-mode "${M09_REVERSAL_MODE:-${M07_REVERSAL_MODE:-flip}}" \
      ${M08_EXIT_LAT:+--exit-latency-ms "$M08_EXIT_LAT"} \
      --exit-entry-grace-ms "${M09_EXIT_ENTRY_GRACE_MS:-${M07_EXIT_ENTRY_GRACE_MS:-90000}}" \
      --leader-dust-floor-usd "${M09_LEADER_DUST_FLOOR_USD:-${M07_LEADER_DUST_FLOOR_USD:-0.0}}" \
      ${M08_SLB:+--sl-bps "$M08_SLB"} \
      ${M08_GSP:+--global-stop-pct "$M08_GSP"} \
      --slip-calib "$IN_SLIP_CALIB" >>"$LOG" 2>&1 || rc=$?
    log "[m08] rc=${rc:-0}"
    if [ "${rc:-0}" -ne 0 ]; then log "[m08] FAILED -- stopping"; exit 1; fi
    mark_stage m08
  fi
fi

# ---- STAGE m09: fixed-bankroll chained portfolio simulation ------------------------------------ #
if stage_wanted m09; then
  if [ -z "${M09_B0:-}" ] || [ -z "${M09_TARGET_COUNT:-}" ]; then
    log "[m09] FAILED -- manifest must declare m09.b0 and m09.target_count"
    exit 1
  fi
  if skip_stage "m09" "$RUN/m09_result.json"; then
    log "[m09] SKIP (fingerprint matches)"
  else
    rc=0
    M09_ARGS=$($PY - "$MANIFEST" <<'PYEOF'
import sys, yaml, shlex
FLAG = {
    "b0": "--b0", "target_count": "--target-count", "rho_max": "--rho-max",
    "gross_cap": "--gross-cap", "global_dd_derisk": "--global-dd-derisk",
    "g4_intrafold_kill": "--g4-intrafold-kill", "per_entity_cap": "--per-entity-cap",
    "suspicious_cohort_cap": "--suspicious-cohort-cap",
    "min_order_notional": "--min-order-notional", "min_accessible_frac": "--min-accessible-frac",
    "sizing_mode": "--sizing-mode", "fixed_target_exposure": "--fixed-target-exposure",
    "fixed_notional_usd": "--fixed-notional-usd", "reversal_mode": "--reversal-mode",
    "exit_latency_ms": "--exit-latency-ms", "exit_entry_grace_ms": "--exit-entry-grace-ms",
    "leader_dust_floor_usd": "--leader-dust-floor-usd", "sl_bps": "--sl-bps",
    "global_stop_pct": "--global-stop-pct",
}
m9 = (yaml.safe_load(open(sys.argv[1])) or {}).get("m09") or {}
unknown = [k for k in m9 if k not in FLAG]
if unknown:
    sys.exit(f"MANIFEST ERROR: m09 keys {unknown} have no CLI flag")
# codex P2-F: an explicit YAML null (e.g. `exit_latency_ms: null`) parses to Python None and would
# serialize as the literal string "None" into argv. A null means "engine default" -> emit no flag.
print(" ".join(f"{FLAG[k]} {shlex.quote(str(v))}" for k, v in m9.items() if v is not None))
PYEOF
) || { log "[m09] FAILED -- manifest/flag mismatch"; exit 1; }
    log "[m09] START b0=$M09_B0 target_count=$M09_TARGET_COUNT"
    M09_SHARDS_ARGS=()
    if [ -n "${IN_ACTION_SHARDS:-}" ]; then M09_SHARDS_ARGS=(--action-shards "$IN_ACTION_SHARDS"); fi
    # shellcheck disable=SC2086 -- M09_ARGS is generated from the fixed flag map above.
    $SAFE --label m09_"$M_NAME" -- $PY research/v15/v15_m09_sim.py \
      --m06b-pool "$RUN/m06b_pool.parquet" --m08-survival "$RUN/m08_tiers.parquet" \
      --m04-dir "$IN_M04_DIR" --folds "$IN_FOLDS" --actions "$IN_ACTIONS" \
      "${M09_SHARDS_ARGS[@]}" --slip-calib "$IN_SLIP_CALIB" --out "$RUN" \
      $M09_ARGS >>"$LOG" 2>&1 || rc=$?
    log "[m09] rc=${rc:-0}"
    if [ "${rc:-0}" -ne 0 ]; then log "[m09] FAILED -- stopping"; exit 1; fi
    mark_stage m09
  fi
fi

# ---- STAGE m10: exact M09 quality-matched null + frozen G1-G7 gates ------------------------------ #
if stage_wanted m10; then
  if [ -z "${M10_N_NULL_DEV:-}" ] || [ -z "${M10_EXPECTED_N_FOLDS:-}" ] \
     || [ -z "${M10_MIN_POSITIVE_FOLDS:-}" ]; then
    log "[m10] FAILED -- manifest must declare n_null_dev, expected_n_folds, min_positive_folds"
    exit 1
  elif [ ! -s "$RUN/m09_result.json" ]; then
    log "[m10] FAILED -- missing canonical M09 result $RUN/m09_result.json"
    exit 1
  elif skip_stage "m10" "$RUN/m10_result.json"; then
    log "[m10] SKIP (fingerprint matches)"
  else
    rc=0
    M10_SHARDS_ARGS=()
    if [ -n "${IN_ACTION_SHARDS:-}" ]; then M10_SHARDS_ARGS=(--action-shards "$IN_ACTION_SHARDS"); fi
    M10_WORKER_COUNT="${M10_WORKERS:-1}"
    if ! [[ "$M10_WORKER_COUNT" =~ ^[1-9][0-9]*$ ]]; then
      log "[m10] FAILED -- m10.workers must be a positive integer"; exit 1
    fi
    M10_COMMON=(research/v15/v15_m10_gates.py \
      --m09-result "$RUN/m09_result.json" --m06b-pool "$RUN/m06b_pool.parquet" \
      --m08-survival "$RUN/m08_tiers.parquet" --m04-dir "$IN_M04_DIR" \
      --folds "$IN_FOLDS" --actions "$IN_ACTIONS" "${M10_SHARDS_ARGS[@]}" \
      --slip-calib "$IN_SLIP_CALIB" \
      --out "$RUN" --n-null-dev "$M10_N_NULL_DEV" \
      --gate-percentile "${M10_GATE_PERCENTILE:-95}" \
      --expected-n-folds "$M10_EXPECTED_N_FOLDS" \
      --min-positive-folds "$M10_MIN_POSITIVE_FOLDS" \
      --g1-chained-roe "${M10_G1_CHAINED_ROE:-0.5302}")
    log "[m10] START exact matched null n=$M10_N_NULL_DEV gate_p=${M10_GATE_PERCENTILE:-95} workers=$M10_WORKER_COUNT"
    if [ "$M10_WORKER_COUNT" -eq 1 ]; then
      $SAFE --label m10_"$M_NAME" -- $PY "${M10_COMMON[@]}" >>"$LOG" 2>&1 || rc=$?
    else
      pids=()
      for ((i=0; i<M10_WORKER_COUNT; i++)); do
        slog="$RUN/.m10_worker_${i}.log"
        $SAFE --label m10_"$M_NAME"_w"$i" -- $PY "${M10_COMMON[@]}" \
          --worker-index "$i" --worker-count "$M10_WORKER_COUNT" >"$slog" 2>&1 &
        pids+=("$!")
      done
      for pid in "${pids[@]}"; do wait "$pid" || rc=1; done
      if [ "$rc" -eq 0 ]; then
        $PY "${M10_COMMON[@]}" --merge-workers "$M10_WORKER_COUNT" >>"$LOG" 2>&1 || rc=$?
      fi
    fi
    log "[m10] rc=${rc:-0}"
    if [ "${rc:-0}" -ne 0 ]; then log "[m10] FAILED -- stopping"; exit 1; fi
    mark_stage m10
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
$PY - "$PROV" "$RUN" "$((T_END-T_START))" "$ONLY_STAGE" <<'PYEOF'
import sys, json, hashlib, os, datetime
prov, run, secs, only_stage = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
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
d["status"] = "stage_complete" if only_stage else "complete"
d["completed_utc"] = datetime.datetime.now(datetime.UTC).isoformat()
json.dump(d, open(prov, "w"), indent=1, default=str)
print(f"provenance closed: {len(outs)} outputs, {secs}s")
PYEOF
$PY tools/experiment_registry.py complete --registry "$REGISTRY" --run-id "$RUN_ID" \
  --manifest "$MANIFEST" --run-dir "$RUN" --provenance "$PROV" \
  --receipt "$REGISTRY_RECEIPT" >/dev/null
RUN_TERMINAL=1
log "=== DONE in $((T_END-T_START))s -- provenance: $PROV ==="
