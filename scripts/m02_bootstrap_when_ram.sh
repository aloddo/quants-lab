#!/bin/bash
# M2 GENESIS BOOTSTRAP retry-runner (quant-engineer, 2026-07-25).
# WHY: the genesis replay needs an irreducible ~3.5GB working set (parent 1.5 + writer 1.0 + trace 1.0)
# UNDER the mem_safe_run ceiling, where ceiling = (system available - floor). On a 16GB box shared with
# gbrain-postgres (Colima VM) + the agent fleet, that window only opens when the fleet goes quiet.
# The job fails FAST and FAIL-CLOSED before loading any data (checkpoint not advanced), so retrying is
# safe + idempotent. This polls until the window opens, then runs it once and stops.
# Manual runs stay possible: scripts/m02_daily_pipeline.sh is untouched.
set -uo pipefail
REPO=/Users/hermes/quants-lab
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
LOG=/tmp/m02_bootstrap_retry.log
FLOOR_GB=${FLOOR_GB:-3}   # lowered 4->3 (2026-07-25, decide-and-report): floor is a KILL THRESHOLD,
                         # not a reservation; kernel-critical-pressure kill remains the real backstop.
                         # floor 4 required 7.5GB avail at launch = a window this box never opens.
NEED_GB=${NEED_GB:-3.6}          # serial_need 3.5 + a little slack
POLL_S=${POLL_S:-300}
MAX_TRIES=${MAX_TRIES:-288}      # ~24h at 5min

cd "$REPO"
set -a; [ -f "$REPO/.env" ] && source "$REPO/.env"; set +a

avail_mb() { $PY -c "import psutil;print(int(psutil.virtual_memory().available/1048576))"; }

echo "=== [$(date +%Y%m%dT%H%M%S)] m02 bootstrap retry-runner start (floor=${FLOOR_GB}GB need=${NEED_GB}GB) ===" >>"$LOG"
for i in $(seq 1 "$MAX_TRIES"); do
  A=$(avail_mb)
  CEIL=$(echo "$A $FLOOR_GB" | awk '{printf "%.2f",($1-$2*1024)/1024}')
  OK=$(echo "$CEIL $NEED_GB" | awk '{print ($1>=$2)?1:0}')
  if [ "$OK" = "1" ]; then
    echo "[$(date +%H:%M:%S)] try $i: avail=${A}MB ceiling=${CEIL}GB >= ${NEED_GB}GB -> LAUNCHING" >>"$LOG"
    "$REPO/scripts/mem_safe_run.sh" --floor-gb "$FLOOR_GB" --label m02-bootstrap -- \
      "$PY" data_pipeline/m02_journeys_daily.py --procs 2 >>"$LOG" 2>&1
    RC=$?
    echo "[$(date +%H:%M:%S)] job rc=$RC" >>"$LOG"
    if [ "$RC" = "0" ]; then
      echo "=== [$(date +%Y%m%dT%H%M%S)] BOOTSTRAP OK after $i tries ===" >>"$LOG"; exit 0
    fi
    echo "[$(date +%H:%M:%S)] rc!=0 (likely lost the window) -> keep polling" >>"$LOG"
  else
    [ $((i % 12)) -eq 1 ] && echo "[$(date +%H:%M:%S)] try $i: avail=${A}MB ceiling=${CEIL}GB < ${NEED_GB}GB -> wait" >>"$LOG"
  fi
  sleep "$POLL_S"
done
echo "=== [$(date +%Y%m%dT%H%M%S)] GAVE UP after $MAX_TRIES tries -- escalate (fleet never went quiet) ===" >>"$LOG"
exit 1
