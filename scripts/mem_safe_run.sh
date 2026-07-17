#!/bin/bash
# mem_safe_run.sh -- MANDATORY wrapper for every heavy/batch/fan-out job on this box.
# Binding decision: projects/quant/decisions/2026-06-04-mem-safe-run-backstop
#
# WHY: 10 OOM reboots in one week. Per-process install_memory_guard bounds each worker
# but NOT the aggregate (N workers + marks-cache copies + standing pipeline + agents +
# VS Code) on a shared 16GB box. This wrapper is the HARD backstop: it does NOT trust
# the caller's --procs sizing. It launches the job in its own process group and a
# watchdog kills that group the instant system-wide reclaimable RAM drops below the
# floor -- BEFORE the box thrashes. The job dies; the machine survives. Always.
#
# USAGE:
#   scripts/mem_safe_run.sh [--floor-gb N] [--label NAME] -- <command...>
# EXAMPLE:
#   scripts/mem_safe_run.sh --floor-gb 4 --label m02 -- \
#     /path/python research/v15/v15_m02_journey_trace.py --procs 3 ...
#
# --floor-gb default 4GB is the JOB-TREE RSS CEILING (the max physical RSS this job's whole
# process group may occupy), NOT a system-wide reclaimable-RAM floor. Bounding our own job's
# RSS is the deterministic lever (macOS "free RAM" looks green right up until swap-death). The
# job is killed the instant its tree RSS exceeds the ceiling, or IMMEDIATELY on kernel-critical
# pressure. Sizing note: a job needs ceiling >= its real peak (m02 full-window serial ~= 3.5GB),
# so 4GB fits m02; raise --floor-gb for heavier jobs rather than let the watchdog kill them.

set -u
FLOOR_GB=4
LABEL="job"
while [ $# -gt 0 ]; do
  case "$1" in
    --floor-gb) FLOOR_GB="$2"; shift 2;;
    --label)    LABEL="$2";    shift 2;;
    --)         shift; break;;
    *) echo "mem_safe_run: unknown arg $1" >&2; exit 2;;
  esac
done
[ $# -ge 1 ] || { echo "mem_safe_run: no command after --" >&2; exit 2; }

# --floor-gb is reinterpreted as the JOB-TREE RSS CEILING (the max physical RSS this
# job's whole process group may occupy). This is the reliable bound: macOS shuffles and
# compresses pages between free/inactive/speculative buckets, so "free RAM" looks green
# right up until swap-death (verified 2026-06-04: 1.4GB allocated, "reclaimable" metric
# barely moved). What we CAN bound deterministically is our own job's RSS. Secondary
# trigger: kernel critical-pressure (the same level that fires jetsam).
# Kill timing (hardened per codex 2026-07-17): both triggers are FIRST-breach + IMMEDIATE SIGKILL, polled every
# 1s. A polling watchdog still has a reaction window (~1-2s of allocation past the ceiling before the kill
# lands), so this is paired with IN-PROCESS bounded chunks (the caller never allocates a single multi-GB spike;
# growth is gradual and sampled). No debounce: legit bounded jobs stay well under the ceiling, so ANY breach is
# genuinely anomalous and a killed job (recoverable) beats a panicked box (15h blackout). SIGKILL can't be
# caught/delayed, so no TERM grace on the ceiling path either.
CEIL_MB=$(( FLOOR_GB * 1024 ))
BREACH_NEEDED=1                 # first breach kills (no transient-spike tolerance; box-safety > a rare rerun)
SAMPLE_SEC=1

# Sum RSS (KB->MB) of every process in the job's process group.
tree_rss_mb() {  # $1 = pgid
  ps -axo pgid=,rss= | awk -v g="$1" '$1==g{s+=$2} END{print int(s/1024)}'
}
# Kernel memory pressure: 1=normal 2=warning 4=critical. 4 immediately precedes jetsam.
pressure_level() { sysctl -n kern.memorystatus_vm_pressure_level 2>/dev/null || echo 1; }

if [ "$(pressure_level)" -ge 4 ]; then
  echo "[mem_safe_run $LABEL] REFUSING launch: kernel memory pressure already CRITICAL. Free RAM first." >&2
  exit 3
fi
echo "[mem_safe_run $LABEL] launching in own process group; job-tree RSS ceiling ${CEIL_MB}MB, kill on critical kernel pressure."

# Export a marker so wrapped children can VERIFY they are under the backstop. m02 refuses to start a
# bulk/parallel/seed run without it, so the 2026-06-04 mandate is enforced by code, not by remembering to wrap.
export MEM_SAFE_RUN=1
export MEM_SAFE_RUN_LABEL="$LABEL"
export MEM_SAFE_RUN_CEIL_MB="$CEIL_MB"

# Launch the job as its own session/process group so the watchdog can signal the whole
# tree. macOS has no `setsid`, so use perl's POSIX::setsid: the perl proc becomes the
# session/group leader, then exec replaces it with the job -> job is the group leader,
# and pgid == JOB_PID. Watchdog signals -PGID to hit every worker the pool spawns.
perl -MPOSIX=setsid -e 'setsid; exec @ARGV or die "exec failed: $!"' -- "$@" &
JOB_PID=$!
PGID=$JOB_PID

_kill_group() {  # $1 = reason ; $2 = "immediate" -> SIGKILL now (no TERM grace)
  echo "[mem_safe_run $LABEL] ABORT: $1 -> killing job group $PGID to protect the box." >&2
  if [ "${2:-}" = "immediate" ]; then
    # Kernel pressure is already CRITICAL; an 8s TERM grace lets a stuck/allocating process swap-death the
    # box. SIGKILL the group NOW (codex P1). No process can catch or delay SIGKILL.
    kill -KILL -"$PGID" 2>/dev/null
  else
    kill -TERM -"$PGID" 2>/dev/null
    sleep 8
    kill -KILL -"$PGID" 2>/dev/null
  fi
  wait "$JOB_PID" 2>/dev/null
  exit 9
}

breaches=0
while kill -0 "$JOB_PID" 2>/dev/null; do
  RSS=$(tree_rss_mb "$PGID")
  PL=$(pressure_level)
  # CRITICAL kernel pressure (level 4 = the level that immediately precedes jetsam) -> kill NOW, no grace.
  # Waiting even one more 3s sample here is how the box swap-deaths (codex P1). RSS-ceiling breaches keep the
  # 2-sample debounce to ignore transient spikes.
  if [ "$PL" -ge 4 ]; then
    _kill_group "kernel pressure CRITICAL (pl=$PL, rss=${RSS}MB)" immediate
  fi
  if [ "$RSS" -gt "$CEIL_MB" ]; then
    breaches=$(( breaches + 1 ))
    echo "[mem_safe_run $LABEL] WARN job-tree RSS ${RSS}MB > ceiling ${CEIL_MB}MB (breach ${breaches}/${BREACH_NEEDED}; pl=${PL})" >&2
    [ "$breaches" -ge "$BREACH_NEEDED" ] && _kill_group "job-tree RSS ${RSS}MB > ceiling ${CEIL_MB}MB" immediate
  else
    breaches=0
  fi
  sleep $SAMPLE_SEC
done

wait "$JOB_PID"
RC=$?
echo "[mem_safe_run $LABEL] job exited rc=$RC; final pressure level $(pressure_level)"
exit $RC
