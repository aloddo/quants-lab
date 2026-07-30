#!/bin/bash
# m04 authenticity at 20k scale for folds 9-12 — the OVERNIGHT job that extends the profile from
# "Jan 26 - May 18" to "Jan 26 - Jul 13".
#
# WHY: today's profile rests on folds 1-8, whose TEST windows end 2026-05-18. Alberto wants to copy
# NOW. If the attribute->outcome relationship is structural it will hold, but that has to be shown on
# recent ground, not assumed. The M02 store already reaches 2026-07-13, so folds 9-12 are available
# data-wise; only M4 is missing (f1..f8 already exist at 20k and are provenance-verified via as_of_ms).
#
# Fold 12's test window is [2026-06-29, 2026-07-13) = exactly the store edge. Fold 13 would need data
# to 2026-07-27 and is NOT possible from this store.
#
# COST: unverified. m04 on 1,630 wallets took 11.5 min (first fold, cold) / 3.5-8 min (warm). STAGE A
# is per-wallet, so 20,378/1,630 = 12.5x implies ~2.4h/fold => ~9.6h for four folds. MEASURE fold 9
# before assuming the rest; do not report an ETA off the extrapolation alone.
#
# RUN ONLY WHEN M7 IS DONE — it needs the RAM (plan_memory_budget will abort rather than thrash, and
# last night's lesson was that a second concurrent worker made everything SLOWER, not faster).
set -u
cd /Users/hermes/quants-lab

# ── PRECONDITION GATE (2026-07-29). Folds 9-10 completed here; fold 11 was OOM-KILLED (rc=137)
# because M6b ranking was running concurrently -- which the note above already forbade, in prose.
# Prose did not stop it. This checks.
#
# 2026-07-29 FIX: the first version grepped the module NAMES anywhere in a command line, so an
# unrelated shell whose heredoc merely QUOTED "v15_m04_authenticity" (a markdown note on its way to
# the brain) tripped the gate and refused a legitimate run at 5.7GB free. Decide on the EXECUTABLE
# that was actually run -- first token of the command line -- so a shell can never match, whatever it
# happens to quote. `comm` is unusable here: macOS truncates it to 16 chars.
# Verified by running it, not by reading it: idle->0, real python invocation->1, a zsh merely echoing
# the module path->no change, after exit->0.
_BUSY_AWK='{ exe=$2; n=split(exe,p,"/"); base=p[n];
             if (base !~ /^[Pp]ython[0-9.]*$/) next;
             if ($0 ~ /research\/v15\/(v15_m06b_ranking|v15_m07_engine|v15_m04_authenticity)\.py/) c++ }
           END { print c+0 }'
_busy=$(ps -axo pid=,args= | awk "$_BUSY_AWK")
if [ "${_busy:-0}" -gt 0 ]; then
  echo "REFUSING TO START: another heavy V15 job is running:" >&2
  ps -axo pid=,args= | awk '{ exe=$2; n=split(exe,p,"/"); base=p[n];
      if (base ~ /^[Pp]ython[0-9.]*$/ && $0 ~ /research\/v15\/(v15_m06b_ranking|v15_m07_engine|v15_m04_authenticity)\.py/) print }' >&2
  exit 1
fi
# AVAILABLE = free + inactive, which is what the M4 budget planner actually measures. Using RAW
# free pages here was wrong and would have refused forever: right after heavy parquet work raw free
# sits near 0.1GB while 3GB+ is reclaimable inactive. Caught by running the gate instead of
# assuming it -- the same lesson as everything else today.
_freegb=$(vm_stat | awk '/Pages free/ {gsub(/\./,"",$3); f=$3} /Pages inactive/ {gsub(/\./,"",$3); i=$3} END {printf "%.1f", (f+i)*16384/1073741824}')
# M4 needs usable = avail - 0.5 headroom - 1.5 main_reserve >= 1.9 per worker, i.e. ~4GB available.
if [ "$(printf '%.0f' "${_freegb:-0}")" -lt 4 ]; then
  echo "REFUSING TO START: only ${_freegb}GB available (need ~4GB). macOS compressor may hold" >&2
  echo "reclaimable pages after heavy parquet work; let the box settle or reboot, then re-run." >&2
  exit 1
fi
echo "precondition OK: ${_freegb}GB available, no competing V15 job"
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
OUT=app/data/v15
W=$OUT/m01_universe_20k_wallets.txt
LOG=/tmp/ql_m04_20k_f9to12.log

# fold_id:as_of  (as_of = that fold's test_start; start 2025-12-01, 42d train / 14d val / 14d test, step 14d)
FOLDS="9:2026-05-18 10:2026-06-01 11:2026-06-15 12:2026-06-29"

echo "=== m04 20k folds 9-12 start $(date -u +%FT%TZ) ===" >> "$LOG"
printf '%s\n' $FOLDS | while read -r pair; do
  fid="${pair%%:*}"; asof="${pair##*:}"
  o="$OUT/m04_authenticity_f${fid}.parquet"
  e="$OUT/m04_entities_f${fid}.parquet"
  if [ -s "$o" ] && [ -s "$e" ]; then echo "[f$fid] SKIP (exists)" >> "$LOG"; continue; fi
  echo "[f$fid] as-of $asof START $(date -u +%FT%TZ)" >> "$LOG"
  # 2026-07-30 rc=137 FIX. Fold 11 was OOM-killed TWICE here, and neither cause was the memory guard:
  #   (1) this invoked $PY DIRECTLY, so scripts/mem_safe_run.sh -- the ONLY component that polls
  #       SYSTEM-AVAILABLE memory and kills the group before the kernel does -- was absent. The
  #       in-process guard bounds THIS PROCESS's RSS on a 15s poll; it is structurally blind to the box
  #       shrinking underneath it, which is exactly what 5 concurrent agents do. The wrapper's own header
  #       calls itself MANDATORY for every heavy job (decision 2026-06-04-mem-safe-run-backstop) and its
  #       2026-07-17 note records that it moved to system-available semantics *because the old RSS
  #       ceiling missed THIS module* reading the 11GB store via mmap.
  #   (2) --headroom-gb 0.5 against a module default of 6.0. Headroom is reserve for mid-run GROWTH of
  #       the live baseline; 0.5GB on this box is a rounding error, so the planner certified an
  #       infeasible plan as feasible. _streaming_io now floors headroom at 2.0GB.
  # m04's main() now calls require_mem_safe_run(), so running this bare refuses outright.
  scripts/mem_safe_run.sh --floor-gb 2 --label m04_f$fid -- \
  $PY research/v15/v15_m04_authenticity.py \
    --wallets-file "$W" --as-of "$asof" \
    --out "$o" --entities-out "$e" \
    --procs 1 --per-worker-gb 1.5 >> "$LOG" 2>&1
  rc=$?
  echo "[f$fid] rc=$rc END $(date -u +%FT%TZ)" >> "$LOG"
  [ $rc -ne 0 ] && { echo "[f$fid] FAILED - stopping (fail closed)" >> "$LOG"; exit 1; }
done
echo "=== m04 20k folds 9-12 done $(date -u +%FT%TZ) ===" >> "$LOG"

# NOTE: these land in app/data/v15/ alongside f1..f8, so the funnel20k run dir's symlinks pick them up.
# Extending the funnel to 12 folds then needs: m03 --n-folds 12 -> m05 -> a fresh profile seat sample
# -> m07 both windows. Do NOT reuse the 8-fold m05/shortlist against a 12-fold m03.
