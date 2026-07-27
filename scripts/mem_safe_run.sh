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
# --floor-gb is the SYSTEM-AVAILABLE FLOOR (semantics CHANGED by the 2026-07-17 P0 fix, see the
# comment above AVAIL_FLOOR_MB below). The job group is killed the instant system available RAM
# drops below it, or IMMEDIATELY on kernel-critical pressure; a job-tree RSS hard-stop also applies.
#
# SIZING (corrected 2026-07-25 -- the note previously here still described the PRE-07-17 semantics
# and was actively misleading; it claimed "4GB fits m02", which is no longer true):
# the in-process planner receives MEM_SAFE_RUN_CEIL_MB = (available AT LAUNCH - floor). So a job
# whose real peak is P needs
#     available_at_launch >= floor + P
# m02 full-window serial peak ~= 3.5GB, so --floor-gb 4 requires ~7.5GB AVAILABLE AT LAUNCH. On a
# busy 16GB box (gbrain-postgres VM + agent fleet) that window may not open on its own. Options:
# wait for a quiet window (scripts/m02_bootstrap_when_ram.sh polls for one), or lower --floor-gb --
# it is a KILL THRESHOLD, not a reservation, and the kernel-critical-pressure kill remains the real
# box-death backstop regardless of its value.

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
# --floor-gb is now the SYSTEM-AVAILABLE floor (2026-07-17 P0 fix): kill the job group the instant system
# available RAM drops below it. This is the RELIABLE box-death signal -- the old job-tree RSS ceiling MISSED
# v15_m04 because it read the 11GB store via memory-MAPPED/file-backed parquet (consumes physical RAM, drove
# avail to 2.8G, but does NOT show as process RSS -> ceiling never breached -> box near-crash, CoS killed it).
AVAIL_FLOOR_MB=$(( FLOOR_GB * 1024 ))
RSS_HARDSTOP_MB=${RSS_HARDSTOP_MB:-13312}   # SECONDARY: also kill any single job tree whose RSS exceeds ~13GB
SAMPLE_SEC=1
_SYSCTL=/usr/sbin/sysctl                    # bare `sysctl` is NOT in the conda-env PATH -> would silently fail

# System AVAILABLE RAM in MB via vm_stat (free+inactive+speculative+purgeable == psutil.available, verified).
# Reliable regardless of how a job holds memory (anon RSS, mmap, file cache) -> catches the mmap-undercount.
avail_mb() {
  vm_stat 2>/dev/null | awk '/page size of/{ps=$8} /Pages free/{f=$3} /Pages inactive/{i=$3} /Pages speculative/{s=$3} /Pages purgeable/{p=$3} END{gsub(/\./,"",f);gsub(/\./,"",i);gsub(/\./,"",s);gsub(/\./,"",p); if(ps=="")ps=16384; print int((f+i+s+p)*ps/1048576)}'
}
# Sum RSS (KB->MB) of every process in the job's process group (secondary runaway catch).
tree_rss_mb() {  # $1 = pgid
  ps -axo pgid=,rss= | awk -v g="$1" '$1==g{s+=$2} END{print int(s/1024)}'
}
# Kernel memory pressure: 1=normal 2=warning 4=critical. 4 immediately precedes jetsam. FULL PATH (see above).
pressure_level() { "$_SYSCTL" -n kern.memorystatus_vm_pressure_level 2>/dev/null || echo 1; }

if [ "$(pressure_level)" -ge 4 ]; then
  echo "[mem_safe_run $LABEL] REFUSING launch: kernel memory pressure already CRITICAL. Free RAM first." >&2
  exit 3
fi
_AVAIL_AT_LAUNCH=$(avail_mb)
# BUG 1 FIX (2026-07-27): REFUSE to launch when we are ALREADY below the floor. Previously this only
# refused on kernel-CRITICAL pressure, so with avail < floor it would launch anyway and the monitor
# loop would kill it on its very first poll -- observed live: "launching ... (avail now 3867MB)"
# against a 4096MB floor, followed immediately by "ABORT ... 3866MB < floor ... (rss=0MB)". Starting a
# job we are certain to kill is pure waste, and (see BUG 2) that instant abort is exactly the window
# where the kill is unreliable. Exit 3 = the same "refused launch" code the caller already treats as a
# retryable RESOURCE verdict.
if [ -n "$_AVAIL_AT_LAUNCH" ] && [ "$_AVAIL_AT_LAUNCH" -lt "$AVAIL_FLOOR_MB" ]; then
  echo "[mem_safe_run $LABEL] REFUSING launch: system available ${_AVAIL_AT_LAUNCH}MB already < floor ${AVAIL_FLOOR_MB}MB." >&2
  exit 3
fi
echo "[mem_safe_run $LABEL] launching in own process group; system-available FLOOR ${AVAIL_FLOOR_MB}MB (avail now ${_AVAIL_AT_LAUNCH}MB), RSS hard-stop ${RSS_HARDSTOP_MB}MB, kill on critical kernel pressure."

# Export a marker so wrapped children can VERIFY they are under the backstop. m02 refuses to start a
# bulk/parallel/seed run without it, so the 2026-06-04 mandate is enforced by code, not by remembering to wrap.
export MEM_SAFE_RUN=1
export MEM_SAFE_RUN_LABEL="$LABEL"
# job memory budget for in-process planners (m02 plan_memory_budget): headroom from avail-now down to the floor.
export MEM_SAFE_RUN_CEIL_MB=$(( _AVAIL_AT_LAUNCH > AVAIL_FLOOR_MB ? _AVAIL_AT_LAUNCH - AVAIL_FLOOR_MB : 1024 ))

# Launch the job as its own session/process group so the watchdog can signal the whole
# tree. macOS has no `setsid`, so use perl's POSIX::setsid: the perl proc becomes the
# session/group leader, then exec replaces it with the job -> job is the group leader,
# and pgid == JOB_PID. Watchdog signals -PGID to hit every worker the pool spawns.
perl -MPOSIX=setsid -e 'setsid; exec @ARGV or die "exec failed: $!"' -- "$@" &
JOB_PID=$!
PGID=$JOB_PID

# BUG 2 FIX (2026-07-27) -- THE SERIOUS ONE. setsid() runs INSIDE perl, asynchronously, AFTER the
# shell has recorded $!. If the watchdog fired before perl reached setsid(), the process was still in
# the PARENT's process group, so `kill -KILL -$PGID` addressed a group that did not exist yet, failed
# SILENTLY (2>/dev/null), and _kill_group then exited 9 -- leaving the job ALIVE, ORPHANED and
# COMPLETELY UNGUARDED. Observed live 2026-07-27 09:18: guard reported "killing job group 47299",
# exited, and 47299 kept running and grew to 905MB while system-available fell to 2588MB. That is the
# precise shape of the 2026-07-16 kernel panic this wrapper exists to prevent, with the wrapper
# reporting success.
# The race window is widest exactly when the box is already tight -- i.e. when the guard is most
# likely to fire on its first poll. Wait for the group to actually materialise before monitoring.
_pgid_of() { ps -o pgid= -p "$1" 2>/dev/null | tr -d ' '; }
_waited=0
while [ "$(_pgid_of "$JOB_PID")" != "$JOB_PID" ]; do
  if ! kill -0 "$JOB_PID" 2>/dev/null; then break; fi     # job already exited; nothing to guard
  if [ "$_waited" -ge 50 ]; then                          # 5s at 0.1s -- setsid should take microseconds
    echo "[mem_safe_run $LABEL] setsid did not take within 5s; killing PID $JOB_PID directly and aborting." >&2
    kill -KILL "$JOB_PID" 2>/dev/null
    exit 9
  fi
  sleep 0.1
  _waited=$((_waited + 1))
done

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
  # VERIFY the kill actually landed. A group kill can miss (race above, or the job re-parented), and a
  # silent miss is worse than no guard at all because it reports success. Fall back to the PID, then
  # confirm. NEVER exit claiming we protected the box while the job is still running.
  sleep 0.5
  if kill -0 "$JOB_PID" 2>/dev/null; then
    echo "[mem_safe_run $LABEL] group kill MISSED (pgid=$(_pgid_of "$JOB_PID")); SIGKILLing PID $JOB_PID directly." >&2
    kill -KILL "$JOB_PID" 2>/dev/null
    sleep 0.5
    if kill -0 "$JOB_PID" 2>/dev/null; then
      echo "[mem_safe_run $LABEL] FATAL: PID $JOB_PID SURVIVED SIGKILL -- job is UNGUARDED. Kill it by hand: kill -9 $JOB_PID" >&2
    fi
  fi
  wait "$JOB_PID" 2>/dev/null
  exit 9
}

while kill -0 "$JOB_PID" 2>/dev/null; do
  AVAIL=$(avail_mb)
  PL=$(pressure_level)
  RSS=$(tree_rss_mb "$PGID")
  # PRIMARY (box-death signal): system available RAM below the floor -> kill NOW. Catches memory the job holds
  # as mmap / file cache that RSS misses (the v15_m04 case). First-breach, immediate SIGKILL, no debounce.
  if [ -n "$AVAIL" ] && [ "$AVAIL" -lt "$AVAIL_FLOOR_MB" ]; then
    _kill_group "system available ${AVAIL}MB < floor ${AVAIL_FLOOR_MB}MB (rss=${RSS}MB pl=${PL})" immediate
  fi
  # kernel CRITICAL pressure (level 4 immediately precedes jetsam) -> kill NOW.
  if [ "$PL" -ge 4 ]; then
    _kill_group "kernel pressure CRITICAL (pl=$PL avail=${AVAIL}MB rss=${RSS}MB)" immediate
  fi
  # SECONDARY: a single job tree whose own RSS is enormous -> kill even if avail momentarily looks ok.
  if [ "$RSS" -gt "$RSS_HARDSTOP_MB" ]; then
    _kill_group "job-tree RSS ${RSS}MB > hard-stop ${RSS_HARDSTOP_MB}MB" immediate
  fi
  sleep $SAMPLE_SEC
done

wait "$JOB_PID"
RC=$?
echo "[mem_safe_run $LABEL] job exited rc=$RC; final pressure level $(pressure_level)"
exit $RC
