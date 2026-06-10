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
# FLOOR default 4GB: the machine ALWAYS keeps >=4GB reclaimable for Alberto + the OS +
# the standing pipeline + the 4 agents. Heavy job is killed first, every time.

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
CEIL_MB=$(( FLOOR_GB * 1024 ))
BREACH_NEEDED=2                 # consecutive breaches before kill (ignore transient spikes)
SAMPLE_SEC=3

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

# Launch the job as its own session/process group so the watchdog can signal the whole
# tree. macOS has no `setsid`, so use perl's POSIX::setsid: the perl proc becomes the
# session/group leader, then exec replaces it with the job -> job is the group leader,
# and pgid == JOB_PID. Watchdog signals -PGID to hit every worker the pool spawns.
perl -MPOSIX=setsid -e 'setsid; exec @ARGV or die "exec failed: $!"' -- "$@" &
JOB_PID=$!
PGID=$JOB_PID

breaches=0
while kill -0 "$JOB_PID" 2>/dev/null; do
  RSS=$(tree_rss_mb "$PGID")
  PL=$(pressure_level)
  REASON=""
  [ "$RSS" -gt "$CEIL_MB" ] && REASON="job-tree RSS ${RSS}MB > ceiling ${CEIL_MB}MB"
  [ "$PL" -ge 4 ] && REASON="${REASON:+$REASON; }kernel pressure CRITICAL"
  if [ -n "$REASON" ]; then
    breaches=$(( breaches + 1 ))
    echo "[mem_safe_run $LABEL] WARN $REASON (breach ${breaches}/${BREACH_NEEDED}; rss=${RSS}MB pl=${PL})" >&2
    if [ "$breaches" -ge "$BREACH_NEEDED" ]; then
      echo "[mem_safe_run $LABEL] ABORT: killing job group $PGID to protect the box." >&2
      kill -TERM -"$PGID" 2>/dev/null
      sleep 8
      kill -KILL -"$PGID" 2>/dev/null
      wait "$JOB_PID" 2>/dev/null
      exit 9
    fi
  else
    breaches=0
  fi
  sleep $SAMPLE_SEC
done

wait "$JOB_PID"
RC=$?
echo "[mem_safe_run $LABEL] job exited rc=$RC; final pressure level $(pressure_level)"
exit $RC
