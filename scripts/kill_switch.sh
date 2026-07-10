#!/bin/bash
# kill_switch.sh — EMERGENCY STOP for the LIVE HL copy engine (V17).
#
# 2026-07-10 (Phase-1 cleanup): rewritten. The prior kill_switch targeted the RETIRED Hummingbot/API stack
# (localhost:8000/8001, tmux ql-pipeline/ql-api) and did NOT stop the live V17 engine. This one acts on the
# real live stack: the halt flags the launcher respects (/tmp/v12_pause + $WORKDIR/.HALT_COPY), the running
# V17 engine PID, and the canonical exchange-truth flatten (tools/flatten_all_offline.py).
#
# MODES:
#   (default, no args)  PANIC FLATTEN — set halt flags, kill the running V17 engine, then market-close ALL
#                       open HL positions (main+xyz+flx) via flatten_all_offline --execute, and verify flat.
#                       This is the correct emergency: killing the engine stops its in-process trailing stops,
#                       so leaving positions open would be UNMANAGED risk — the safe emergency is to get FLAT.
#   --halt-only         Set halt flags + kill the engine PID, but DO NOT flatten. Positions remain OPEN and
#                       UNMANAGED — only use for maintenance where you will manage exposure by hand.
#   --pause             Softest: set halt flags only. The already-running engine keeps managing its positions;
#                       launchd KeepAlive will NOT relaunch it once it exits. No kill, no flatten.
#
# RESUME after any mode: rm -f /tmp/v12_pause "$WORKDIR/.HALT_COPY"   (launchd KeepAlive relaunches the engine)
set -uo pipefail

WORKDIR="/Users/hermes/quants-lab"
PYTHON="/Users/hermes/miniforge3/envs/quants-lab/bin/python"
# Match the LIVE engine only: require the python-interpreter prefix so this very script (which contains the
# string "hl_copy_trader_v17.py" in this pattern) and editors/greps do NOT self-match (codex Q3).
ENGINE_PAT="bin/python strategies/live/hl_copy_trader_v17.py"
MODE="${1:-flatten}"
ts() { date '+%F %T %Z'; }

# codex: reject extra args (e.g. `--pause --bogus`) so a typo can't ride along with a valid mode.
if [ "$#" -gt 1 ]; then
  echo "kill_switch: too many args ($#). Pass at most one mode." >&2
  echo "usage: kill_switch.sh [--pause | --halt-only | (no arg = PANIC FLATTEN)]" >&2
  exit 2
fi
# codex P0: validate mode BEFORE touching flags/flatten. An unknown arg (e.g. "--halt" typo) must NOT fall
# through to the default flatten.
case "$MODE" in
  flatten|--halt-only|--pause) ;;
  *)
    echo "kill_switch: unknown mode '$MODE'" >&2
    echo "usage: kill_switch.sh [--pause | --halt-only | (no arg = PANIC FLATTEN)]" >&2
    exit 2
    ;;
esac

engine_pids() { pgrep -f "$ENGINE_PAT" || true; }

echo "[$(ts)] kill_switch: mode=$MODE — setting halt flags (/tmp/v12_pause + .HALT_COPY)"
touch /tmp/v12_pause "$WORKDIR/.HALT_COPY"

if [ "$MODE" = "--pause" ]; then
  echo "[$(ts)] kill_switch: PAUSE set. Launcher will not relaunch; running engine left as-is."
  echo "[$(ts)] kill_switch: resume with: rm -f /tmp/v12_pause \"$WORKDIR/.HALT_COPY\""
  exit 0
fi

# Stop the running engine so it takes no further action.
PIDS=$(engine_pids)
if [ -n "$PIDS" ]; then
  N=$(echo "$PIDS" | wc -w | tr -d ' ')
  [ "$N" -gt 1 ] && echo "[$(ts)] kill_switch: WARNING $N engine matches (expected 1): $PIDS — killing all"
  echo "[$(ts)] kill_switch: stopping V17 engine PID(s): $PIDS"
  # shellcheck disable=SC2086
  kill $PIDS 2>/dev/null || true
  sleep 3
  PIDS=$(engine_pids)
  if [ -n "$PIDS" ]; then
    echo "[$(ts)] kill_switch: engine still up, SIGKILL: $PIDS"
    # shellcheck disable=SC2086
    kill -9 $PIDS 2>/dev/null || true
    sleep 2
  fi
else
  echo "[$(ts)] kill_switch: no running V17 engine found"
fi

# codex P1: after kill, PROVE the engine is gone before flattening/reporting success. A surviving engine
# would race the flatten (re-open/re-manage). Abort loudly if any match remains.
PIDS=$(engine_pids)
if [ -n "$PIDS" ]; then
  echo "[$(ts)] kill_switch: !!! engine STILL RUNNING after SIGKILL (PID $PIDS). Aborting before flatten — investigate manually (halt flags ARE set)." >&2
  exit 3
fi

if [ "$MODE" = "--halt-only" ]; then
  echo "[$(ts)] kill_switch: HALT-ONLY done. Engine stopped; positions LEFT OPEN and UNMANAGED."
  echo "[$(ts)] kill_switch: to flatten now, run: bash scripts/kill_switch.sh"
  exit 0
fi

# Default: FLATTEN everything. flatten_all_offline aborts unless BOTH halt markers are present (set above),
# is idempotent, and verifies flatness on-exchange (Rule 8).
echo "[$(ts)] kill_switch: FLATTEN — market-closing ALL HL positions (main+xyz+flx) via flatten_all_offline"
cd "$WORKDIR"
if [ -f .env ]; then
  set -a
  # shellcheck source=/dev/null
  source .env
  set +a
fi
"$PYTHON" tools/flatten_all_offline.py --execute
rc=$?
if [ "$rc" -ne 0 ]; then
  echo "[$(ts)] kill_switch: !!! FLATTEN returned $rc — engine is halted but positions may NOT be flat. Check manually: python tools/flatten_all_offline.py (dry-run) and the HL UI."
  exit "$rc"
fi
echo "[$(ts)] kill_switch: DONE. Engine halted + flat. Resume with: rm -f /tmp/v12_pause \"$WORKDIR/.HALT_COPY\""
