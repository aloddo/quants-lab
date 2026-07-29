#!/bin/bash
# DISARM the copy trader. Removing .ARM_COPY is always safe: the launcher fails closed, so the
# absence of this file means "do not trade" (findings/quant/2026-07-29-kill-switch-fails-open).
set -uo pipefail
WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ -f "$WORKDIR/.ARM_COPY" ]; then
  echo "disarming; was:"; sed 's/^/  /' "$WORKDIR/.ARM_COPY"
  rm -f "$WORKDIR/.ARM_COPY"
fi
echo "DISARMED (no .ARM_COPY). Launcher will refuse to start within 30s."
echo "For a hard emergency stop that also beats a re-arm: touch \"$WORKDIR/.HALT_COPY\""
