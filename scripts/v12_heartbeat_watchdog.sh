#!/bin/bash
# v12_heartbeat_watchdog.sh -- SANCTIONED engine stall watchdog (promoted 2026-07-10, Phase-1 cleanup).
# RESOLUTION of the two-killers problem: codex (2026-07-10) showed the OTHER watchdog
# (~/.claude/scripts/v17-copy-engine-stall-watchdog.sh, log-MTIME staleness) is NOT loop-health -- a
# reconnect/error loop can keep the log fresh while the trading loop never reaches _check_exits/_log_stats,
# bypassing an mtime check. THIS watchdog keys on the STATS heartbeat CONTENT (the line emitted only after
# the protected checks run) with generation-correlation guards, so it is strictly more targeted. Decision:
# THIS is the single sanctioned killer; the log-mtime watchdog is retired (CoS unloads
# com.quantslab.v17-stall-watchdog). Codex round-2 PASS of this script is required before it is final.
# --- original draft header (guards documented below) ---
# v12_heartbeat_watchdog.sh -- DRAFT v2 (superseded by the promotion note above)
#
# PURPOSE: bound the unprotected window after a main-event-loop FREEZE of the live V18 copy trader.
# On 2026-06-29 05:52:03 -> 06:27:30 the bot's whole loop (STATS heartbeat 60s + WS feed + fill-sync)
# stalled for 34.6 min while the process stayed ALIVE (host-level freeze: net blackout or macOS App Nap).
# The enforced 5% global stop + exit polling run INSIDE that loop, so an open position was unprotected the
# entire time. v12_launcher.sh has no staleness detection. This watchdog detects a stale STATS heartbeat
# and kills the frozen bot; launchd (KeepAlive=true, ThrottleInterval=30) restarts the supervised slot,
# which re-syncs positions/fills from exchange truth on boot.
#
# SAFETY MODEL (codex round-1 FAIL fixes; guards live capital -- a false kill mid-restart is worse than no WD):
#   1. STALENESS IS LOOP-RELATIVE, NOT CLOCK-PARSED. We count consecutive checks where the newest STATS line
#      is byte-identical. stale after STALE_CHECKS consecutive unchanged checks. Immune to wall-clock jumps,
#      timezone, and date-parse failures (codex P1 clock/parse findings).
#   2. PID-GENERATION CORRELATION. We resolve EXACTLY ONE supervised PID by its full absolute command line
#      AND capture its start time (lstart) as a generation id. If the PID or its start time changes (cold
#      restart), we RESET the unchanged-counter -> a freshly restarted bot is never judged by the old
#      generation's heartbeat (codex P0 cold-restart false-kill + P0 fresh-restart SIGKILL).
#   3. SAME-PID-ONLY ESCALATION. We capture pid+start BEFORE signaling; after SIGTERM we only SIGKILL if the
#      SAME pid with the SAME start time is still alive. We NEVER re-resolve/re-pgrep for the kill phase
#      (codex P0: launchd may have already restarted -> would kill the new healthy instance).
#   4. EXACT SINGLE-MATCH TARGETING. Match the full "python <abs-script>" command, require EXACTLY ONE. >1
#      live instance = P0 operational incident -> log LOUD, do NOT kill (a shared log heartbeat cannot judge
#      multiple processes) (codex P0 multi-instance + fuzzy-pgrep findings).
#   5. PAUSE RE-CHECK before EACH signal (codex P1 pause race). Respects /tmp/v12_pause + .HALT_COPY.
#   6. GRACE for a young process with no current-generation STATS yet (cold boot loads ~146 closes, first
#      STATS ~1-2 min): do not act until the process is older than GRACE_SEC; after grace, a process that has
#      NEVER emitted STATS is treated as frozen (codex P1 truncation/never-heartbeat false-negative).
#   7. Bounded, generation-correlated log read via byte-offset tail -c (codex round-2 P0 + P2).
#   8. ARMED gate (codex round-3 P0): a freeze-kill is permitted only after the CURRENT generation has shown
#      >=2 distinct post-offset STATS lines (proving it is really heartbeating), OR SEEN>=GRACE_CHECKS. This
#      stops a single LATE old-generation STATS line (flushed just after the offset mark) from bypassing the
#      cold-start grace and killing a young healthy bot at ~180s.
#
# OPERATIONAL CONSTRAINT (codex round-3 P1): do NOT externally rotate $LOG while the slot is supervised.
# launchd owns this file and reopens it only on (re)start. External logrotate of the active inode would make
# the bot keep writing to the rotated-away fd while the watchdog sees a silent new file -> false kill.
#
# DEPLOYMENT (Alberto decision; CoS owns plists): run as its own launchd KeepAlive job. This script self-loops.

set -uo pipefail

WORKDIR="/Users/hermes/quants-lab"
LOG="/tmp/ql-v12-copy-trader-launchd.log"
# Full, exact command of the supervised live bot (interpreter + absolute-relative script). Specific enough
# that it cannot match an editor, a grep, or this watchdog.
CMD_MATCH="miniforge3/envs/quants-lab/bin/python strategies/live/hl_copy_trader_v17.py"

CHECK_SEC=30                 # poll cadence
STALE_CHECKS=6               # 6 * 30s = 180s of an unchanged STATS line -> frozen (3 missed 60s beats)
GRACE_SEC=240                # young process w/ no current-gen STATS is not judged until older than this
COOLDOWN_SEC=240             # after a kill, pause arming so a cold restart is never re-killed

# Resolve the single supervised bot. Echoes "PID<TAB>START" or nothing. START is ps lstart string
# (stable per process generation; compared as an opaque id, never arithmetic). ps -axww => no command
# truncation (codex P1). Substring match is fail-SAFE: >1 match => DO NOT kill (handled by caller).
resolve_bot() {
  local lines n
  lines=$(ps -axww -o pid=,lstart=,command= 2>/dev/null | grep -F "$CMD_MATCH" | grep -v "grep" || true)
  n=$(printf '%s' "$lines" | grep -c . || true)
  if [ "$n" -eq 0 ]; then return 1; fi
  if [ "$n" -gt 1 ]; then
    echo "MULTI"   # caller treats as P0 incident
    return 2
  fi
  # one line: "PID  Www Mmm DD HH:MM:SS YYYY  /path/python script ..."
  local pid start
  pid=$(printf '%s' "$lines" | awk '{print $1}')
  start=$(printf '%s' "$lines" | awk '{print $2,$3,$4,$5,$6}')   # lstart 5 fields = generation id
  printf '%s\t%s\n' "$pid" "$start"
}

# is pid alive AND still the same generation (start string match)?
same_gen() {
  local pid="$1" want="$2" cur
  cur=$(ps -p "$pid" -o lstart= 2>/dev/null | sed 's/  */ /g; s/^ //; s/ $//')
  [ -n "$cur" ] && [ "$cur" = "$want" ]
}

# newest STATS line emitted AFTER byte-offset $1 (current generation only). Reading only post-mark bytes
# correlates the heartbeat to the generation: a cold-starting bot that has not yet emitted STATS yields an
# EMPTY result here (the OLD generation's lines are before the mark) -> the no-STATS grace branch applies
# instead of inheriting the prior generation's last line (codex round-2 P0). Each STATS line leads with a
# millisecond wall-clock stamp, so consecutive heartbeats are ALWAYS byte-distinct; a frozen loop appends
# nothing, so the post-mark newest line stays identical -> UNCHANGED climbs (codex round-2 P0 "changing field").
newest_stats() { tail -c "+$(( ${1:-0} + 1 ))" "$LOG" 2>/dev/null | grep -F "INFO: STATS:" | tail -1; }

log_size()  { wc -c < "$LOG" 2>/dev/null | tr -d ' '; }
log_inode() { stat -f %i "$LOG" 2>/dev/null; }

paused() { [ -f /tmp/v12_pause ] || [ -f "$WORKDIR/.HALT_COPY" ]; }

echo "[$(date '+%F %T %Z')] watchdog v2: start stale=$((STALE_CHECKS*CHECK_SEC))s check=${CHECK_SEC}s match='$CMD_MATCH'"

PREV_PID=""
PREV_START=""
PREV_LINE=""
UNCHANGED=0
SEEN=0          # checks observed for the CURRENT generation (loop-relative proc age; no ps clock dep)
GEN_OFFSET=0    # log byte-offset captured at generation start; only STATS after it count
GEN_INODE=""    # log inode at generation start; change => rotation
DISTINCT=0      # count of distinct post-offset STATS lines seen this generation
ARMED=0         # 1 once the generation has proven it heartbeats (>=2 distinct lines) or grace elapsed
GRACE_CHECKS=$(( GRACE_SEC / CHECK_SEC ))

reset_gen() { PREV_PID=""; PREV_START=""; PREV_LINE=""; UNCHANGED=0; SEEN=0; GEN_OFFSET=0; GEN_INODE=""; DISTINCT=0; ARMED=0; }

while true; do
  sleep "$CHECK_SEC"

  if paused; then reset_gen; continue; fi

  res=$(resolve_bot); rc=$?
  if [ "$rc" -eq 2 ]; then
    echo "[$(date '+%F %T %Z')] watchdog: P0 MULTIPLE bot instances match '$CMD_MATCH' -- NOT killing (operational incident, investigate)"
    reset_gen
    continue
  fi
  if [ "$rc" -ne 0 ] || [ -z "$res" ]; then
    reset_gen   # not running; launchd will restart.
    continue
  fi

  pid=$(printf '%s' "$res" | cut -f1)
  start=$(printf '%s' "$res" | cut -f2)
  cur_inode=$(log_inode)

  # generation change (cold restart) OR log rotation/truncation -> mark a fresh offset baseline.
  cur_size=$(log_size); cur_size=${cur_size:-0}
  if [ "$pid" != "$PREV_PID" ] || [ "$start" != "$PREV_START" ]; then
    # cold restart (new generation) -> full staleness reset incl DISTINCT/ARMED so the new gen must
    # re-prove it heartbeats; never inherit the prior generation's armed state (codex round-4 P0).
    PREV_PID="$pid"; PREV_START="$start"; PREV_LINE=""; UNCHANGED=0; SEEN=0; DISTINCT=0; ARMED=0
    GEN_OFFSET="$cur_size"; GEN_INODE="$cur_inode"
  elif [ "$cur_inode" != "$GEN_INODE" ] || [ "$cur_size" -lt "$GEN_OFFSET" ]; then
    # log rotated (new inode) or truncated (shrunk below mark) -> rebaseline + fresh grace, keep generation
    PREV_LINE=""; UNCHANGED=0; SEEN=0; GEN_OFFSET="$cur_size"; GEN_INODE="$cur_inode"; DISTINCT=0; ARMED=0
  fi
  SEEN=$(( SEEN + 1 ))   # one more check observed for this generation

  line=$(newest_stats "$GEN_OFFSET")

  if [ -z "$line" ]; then
    # no STATS in the tail window. If generation is young, grace. If old, it never heartbeats -> frozen.
    if [ "$SEEN" -lt "$GRACE_CHECKS" ]; then
      continue
    fi
    UNCHANGED=$(( UNCHANGED + 1 ))
  elif [ "$line" = "$PREV_LINE" ]; then
    UNCHANGED=$(( UNCHANGED + 1 ))
  else
    PREV_LINE="$line"; UNCHANGED=0
    DISTINCT=$(( DISTINCT + 1 ))
    [ "$DISTINCT" -ge 2 ] && ARMED=1
    continue
  fi

  # arm once the generation has proven it heartbeats, or grace has fully elapsed (covers frozen-before-first-STATS)
  if [ "$DISTINCT" -ge 2 ] || [ "$SEEN" -ge "$GRACE_CHECKS" ]; then ARMED=1; fi

  if [ "$ARMED" -eq 1 ] && [ "$UNCHANGED" -ge "$STALE_CHECKS" ]; then
    # never signal unless the bot is unpaused AND still the exact same pid+generation (codex round-2 P1)
    if paused || ! same_gen "$pid" "$start"; then continue; fi
    echo "[$(date '+%F %T %Z')] watchdog: FREEZE -- STATS unchanged ${UNCHANGED} checks (~$((UNCHANGED*CHECK_SEC))s) pid=$pid gen-checks=${SEEN}; SIGTERM"
    kill -TERM "$pid" 2>/dev/null
    sleep 5
    if ! paused && same_gen "$pid" "$start"; then
      echo "[$(date '+%F %T %Z')] watchdog: pid=$pid (same gen) still alive after TERM; SIGKILL"
      kill -KILL "$pid" 2>/dev/null
    fi
    echo "[$(date '+%F %T %Z')] watchdog: kill issued; launchd KeepAlive restarts slot; cooldown ${COOLDOWN_SEC}s"
    reset_gen
    sleep "$COOLDOWN_SEC"
  fi
done
