#!/bin/bash
# DISARM the copy trader.
#
# Removing .ARM_COPY revokes permission to START: the launcher fails closed, so the absence of the file
# means "do not trade" (findings/quant/2026-07-29-kill-switch-fails-open).
#
# 2026-07-31 (quant, codex r2 P1): IT DOES NOT STOP A RUNNING ENGINE. The launcher's arm check runs at
# process START only, and the engine never re-reads .ARM_COPY. The old version deleted the file and then
# printed an unqualified "DISARMED", which reads as "we are no longer trading" -- while a live process
# could still be opening positions indefinitely. A safety control whose name overstates what it did is
# worse than one that plainly does less, because it stops you looking any further.
#
# So this script now REPORTS the truth and refuses to claim success while an engine is up. It does NOT
# auto-kill by default, deliberately: killing the engine also kills its in-process trailing stops,
# leaving open positions UNMANAGED. That is exactly why kill_switch.sh's panic mode FLATTENS rather than
# just killing. Choosing between "stop managing" and "get flat" is an operator decision, not a default.
#
#   bash scripts/disarm_copy.sh           # revoke start permission; report any running engine
#   bash scripts/disarm_copy.sh --stop    # also halt + kill the running engine (positions stay OPEN and
#                                         #   UNMANAGED -- delegates to kill_switch.sh --halt-only)
#   bash scripts/kill_switch.sh           # the real emergency: halt + kill + FLATTEN + verify
set -uo pipefail
WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$WORKDIR"

STOP=0
case "${1:-}" in
  "") ;;
  --stop) STOP=1 ;;
  *) echo "unknown arg: $1" >&2; echo "usage: disarm_copy.sh [--stop]" >&2; exit 2 ;;
esac

# Same matcher as kill_switch.sh: require the interpreter prefix so this script, editors and greps do
# not self-match on the bare module name.
ENGINE_PAT="bin/python strategies/live/hl_copy_trader_v17.py"
engine_pids() { pgrep -f "$ENGINE_PAT" || true; }

if [ -f "$WORKDIR/.ARM_COPY" ]; then
  echo "revoking start permission; .ARM_COPY was:"; sed 's/^/  /' "$WORKDIR/.ARM_COPY"
  rm -f "$WORKDIR/.ARM_COPY"
else
  echo "no .ARM_COPY present (already disarmed for START)"
fi

PIDS=$(engine_pids)
if [ -z "$PIDS" ]; then
  echo "DISARMED. No .ARM_COPY and no running engine -- the launcher will refuse to start."
  exit 0
fi

if [ "$STOP" -eq 1 ]; then
  echo "engine RUNNING (PID $PIDS) -- delegating to kill_switch.sh --halt-only"
  bash "$WORKDIR/scripts/kill_switch.sh" --halt-only
  rc=$?
  PIDS=$(engine_pids)
  if [ -n "$PIDS" ]; then
    echo "!! ENGINE STILL RUNNING (PID $PIDS) after --halt-only. NOT disarmed. Investigate." >&2
    exit 1
  fi
  echo "DISARMED and STOPPED. NOTE: any open positions are now UNMANAGED -- no trailing stops are"
  echo "running. To get flat: bash scripts/kill_switch.sh"
  exit $rc
fi

echo ""
echo "!! NOT FULLY DISARMED: the engine is STILL RUNNING (PID $PIDS)."
echo "   Removing .ARM_COPY only revokes permission to START. The arm check runs at process start and"
echo "   the engine does not re-read it, so this process can keep trading until it exits."
echo "   Stop it:   bash scripts/disarm_copy.sh --stop     (positions stay OPEN and UNMANAGED)"
echo "   Get flat:  bash scripts/kill_switch.sh            (halt + kill + market-close + verify)"
exit 1
