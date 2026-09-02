#!/bin/bash
# ARM the copy trader. The ONLY sanctioned way to create .ARM_COPY.
#
# WHY (2026-07-29): scripts/v12_launcher.sh now fails CLOSED -- it refuses to run without
# $WORKDIR/.ARM_COPY (findings/quant/2026-07-29-kill-switch-fails-open). This script is what creates
# that file, and it will not create it unless the configured roster passes the exchange-truth account
# gate. The gate is therefore structurally unavoidable: you cannot arm around it, you can only
# deliberately bypass the account-health part, loudly, with --force. Research
# lineage is non-bypassable: an incomplete/non-investable experiment can never
# be converted into permission to trade by an operational override.
#
# The gate exists because two independently-built cohorts were both about to be traded while holding
# lifetime-negative accounts of $969-$2,658 and, in the funnel's case, ten of eleven wallets at ~$0.
#   findings/quant/2026-07-29-live-roster-fails-own-validator
#   findings/quant/2026-07-29-m05-copyability-lane-disabled-account-gates
#
#   bash scripts/arm_copy.sh                     # gate the roster in v12_launcher.sh, then arm
#   bash scripts/arm_copy.sh --config <path>     # gate a different roster
#   bash scripts/arm_copy.sh --force             # arm despite a FAIL (records the override)
#   bash scripts/disarm_copy.sh                  # or: rm -f .ARM_COPY
set -uo pipefail
WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$WORKDIR"
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python

FORCE=0
CONFIG=""
while [ $# -gt 0 ]; do
  case "$1" in
    --force) FORCE=1; shift;;
    --config) CONFIG="${2:?--config needs a path}"; shift 2;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

# Default to whatever roster the launcher is actually configured to trade -- never a hardcoded path,
# so the gate can never check a DIFFERENT roster from the one that goes live.
#
# 2026-07-29: the first version grepped `--config config/*.json` and took `head -1`. v12_launcher.sh
# carries SIX historical "To revert: ARGS=..." lines in COMMENTS above the live assignment, so it
# resolved config/copy_live_basket.json while the launcher actually trades
# copy_trader_totalreturn5_20260726.json. A gate that checks the wrong roster is worse than no gate:
# it manufactures a false PASS. Caught by running it.
#
# Parse the real assignment instead: uncommented `ARGS=` lines only, LAST one wins (shell semantics).
# 2026-07-31 (quant, codex r2 P1): read the launcher's single CONFIG assignment instead of re-parsing
# its $ARGS string. Three parsers (this one, the launcher's, python argparse) disagreed on strings like
# `--config a.json --config b.json` (shell greps take the first, argparse the last) and on prefixed
# paths (`staging/config/x.json` truncated to `config/x.json` by the regex). Any disagreement means we
# gate one roster and trade another -- the exact failure this script exists to prevent.
if [ -z "$CONFIG" ]; then
  CONFIG=$(grep -E '^[[:space:]]*CONFIG=' scripts/v12_launcher.sh \
           | tail -1 | sed -E 's/^[[:space:]]*CONFIG="?([^"]*)"?.*/\1/')
fi
if [ -z "$CONFIG" ] || [ ! -f "$CONFIG" ]; then
  echo "REFUSING TO ARM: could not resolve the launcher's roster config (got '${CONFIG:-<empty>}')" >&2
  exit 2
fi
echo "roster under test: $CONFIG"

# A halt flag beats everything. Arming while halted would be a confusing half-state.
if [ -f /tmp/v12_pause ] || [ -f "$WORKDIR/.HALT_COPY" ]; then
  echo "REFUSING TO ARM: a halt flag is present (/tmp/v12_pause or .HALT_COPY)." >&2
  echo "  Clear it deliberately first:  chflags nouchg .HALT_COPY && rm -f .HALT_COPY /tmp/v12_pause" >&2
  exit 2
fi

# The research decision must be complete, investable, hash-pinned, and tied to
# exactly the wallets in this config. Run this BEFORE the API account gate: it
# is deterministic/local, and --force intentionally cannot bypass it.
LINEAGE_JSON="/tmp/research_lineage_gate_$(date +%Y%m%d_%H%M%S).json"
$PY tools/research_lineage_gate.py --config "$CONFIG" --json-out "$LINEAGE_JSON"
lineage_rc=$?
if [ $lineage_rc -ne 0 ]; then
  echo ""
  echo "NOT ARMED. Research lineage is missing, mutated, incomplete, or non-investable."
  echo "This gate is non-bypassable; --force applies only to the exchange account-health gate."
  exit 1
fi
LINEAGE_DIGEST=$($PY - "$LINEAGE_JSON" <<'PYEOF'
import json, sys
print(json.load(open(sys.argv[1]))["lineage_digest"])
PYEOF
)

# 2026-07-31 (quant): THRESHOLDS COME FROM THE ROSTER, WHEN IT DECLARES THEM.
# The gate's strict defaults (equity>=$10k, lifetime PnL>0, lev<=10x) encode a DIFFERENT thesis from
# the entries-only copy strategy: they failed 9/9 of a cohort that had passed its own selection, purely
# because those leaders are lifetime-negative -- which this thesis expects BY DESIGN, since it copies
# per-position ENTRY edge rather than the leader's outcome. The only honest options were to --force
# around the gate (which makes the arm record mean "the gate failed and we went anyway") or to let a
# roster DECLARE which criteria are load-bearing for it. This is the second. Staleness stays mandatory:
# a dormant leader emits no signal to copy at any size.
# A roster with no `global.account_gate` block still gets the strict defaults.
GATE_ARGS=()
if $PY -c "import json,sys; sys.exit(0 if json.load(open('$CONFIG')).get('global',{}).get('account_gate') else 1)" 2>/dev/null; then
  while IFS= read -r _a; do [ -n "$_a" ] && GATE_ARGS+=("$_a"); done < <($PY - "$CONFIG" <<'PYEOF'
import json, sys
g = json.load(open(sys.argv[1]))["global"]["account_gate"]
for flag, key in (("--min-equity", "min_equity"), ("--min-lifetime-pnl", "min_lifetime_pnl"),
                  ("--max-leverage", "max_leverage"), ("--max-stale-days", "max_stale_days")):
    if key in g:
        print(f"{flag}={g[key]}")      # =-form so negative values are not parsed as flags
PYEOF
)
  echo "gate thresholds declared by the roster: ${GATE_ARGS[*]}"
  echo "  (rationale is recorded in global.account_gate._why in $CONFIG)"
fi

GATE_JSON="/tmp/account_gate_$(date +%Y%m%d_%H%M%S).json"
$PY scripts/account_gate.py --config "$CONFIG" --json-out "$GATE_JSON" "${GATE_ARGS[@]+"${GATE_ARGS[@]}"}"
rc=$?

if [ $rc -ne 0 ] && [ $FORCE -ne 1 ]; then
  echo ""
  echo "NOT ARMED. The roster did not pass the exchange-truth account gate (rc=$rc)."
  echo "Fix the roster, or bypass deliberately with --force (the override is recorded)."
  exit 1
fi

{
  echo "armed_utc=$(date -u +%FT%TZ)"
  echo "config=$CONFIG"
  # Bind the CONTENT, not just the pathname (2026-07-31, codex r2 P1). The gate certified the bytes it
  # read; editing the JSON or repointing a symlink after arming must invalidate the arm, and the
  # launcher refuses on a sha mismatch.
  echo "config_sha256=$(shasum -a 256 "$CONFIG" | awk '{print $1}')"
  echo "gate_rc=$rc"
  echo "gate_result=$GATE_JSON"
  echo "lineage_rc=$lineage_rc"
  echo "lineage_result=$LINEAGE_JSON"
  echo "lineage_digest=$LINEAGE_DIGEST"
  echo "forced=$FORCE"
  echo "git_head=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
} > "$WORKDIR/.ARM_COPY"

if [ $rc -ne 0 ]; then
  echo ""
  echo "!! ARMED WITH --force DESPITE A FAILING GATE. Override recorded in .ARM_COPY."
else
  echo ""
  echo "ARMED. Gate passed."
fi
cat "$WORKDIR/.ARM_COPY"
echo ""
echo "The launcher will pick this up within its 30s ThrottleInterval. Disarm: rm -f .ARM_COPY"
