#!/usr/bin/env bash
# rehydrate_brain_from_kb.sh — re-put KB markdown files back to brain
#
# Context: 2026-05-25 OOM reboot caused brain Postgres durability loss.
# Anything not yet checkpointed at crash time was lost from brain DB.
# KB filesystem survived (it's a git repo, not Postgres).
# This script walks KB markdown files and re-puts each one to brain via gbrain CLI,
# but only when the slug is genuinely missing from brain (idempotent).
#
# Usage:
#   ./rehydrate_brain_from_kb.sh --dry-run                 # show what WOULD be re-put
#   ./rehydrate_brain_from_kb.sh --since 2026-05-24        # only re-put files mtime >= date
#   ./rehydrate_brain_from_kb.sh --agent quant-engineer    # only handoffs/quant-engineer/*
#   ./rehydrate_brain_from_kb.sh                           # full scan + re-put
#
# Safety:
#   * Calls `gbrain get <slug>` first. If brain has the page, SKIPS (no overwrite).
#   * No deletion. No re-embedding of intact pages. Only adds back the missing.
#   * Logs every action to /tmp/rehydrate_brain.log with timestamps.
#
# Author: quant-engineer
# Created: 2026-05-25 22:50 CEST
# Brain-only-writes compliant: writes only to brain (not KB; KB IS the source here).

set -euo pipefail

KB_DIR="${KB_DIR:-/Users/hermes/albertos-kb}"
GBRAIN="${GBRAIN:-/Users/hermes/.bun/bin/gbrain}"
LOG="/tmp/rehydrate_brain.log"
DRY_RUN=0
SINCE=""
AGENT_FILTER=""

usage() {
  sed -n '2,20p' "$0"
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --since)   SINCE="$2"; shift 2 ;;
    --agent)   AGENT_FILTER="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "Unknown arg: $1"; usage ;;
  esac
done

echo "[$(date -u +%FT%TZ)] rehydrate start kb=$KB_DIR dry_run=$DRY_RUN since=$SINCE agent=$AGENT_FILTER" | tee -a "$LOG"

# Build the file list. Filters:
#   - Only .md
#   - Not under archive/ subdir
#   - Optional agent filter
#   - Optional mtime >= since
find_args=("$KB_DIR" -type f -name "*.md" -not -path "*/archive/*")
if [[ -n "$AGENT_FILTER" ]]; then
  find_args+=("-path" "*/$AGENT_FILTER/*")
fi
if [[ -n "$SINCE" ]]; then
  # macOS find supports -newermt
  find_args+=("-newermt" "$SINCE")
fi

n_total=0
n_skip_present=0
n_put=0
n_err=0

while IFS= read -r f; do
  n_total=$((n_total+1))
  # Derive slug = path relative to KB_DIR, minus .md
  rel="${f#$KB_DIR/}"
  slug="${rel%.md}"

  # Check if brain has it (any nonzero output = present in some form)
  if "$GBRAIN" get "$slug" >/dev/null 2>&1; then
    n_skip_present=$((n_skip_present+1))
    continue
  fi

  echo "[$(date -u +%FT%TZ)] MISSING slug=$slug file=$rel" | tee -a "$LOG"

  if [[ $DRY_RUN -eq 1 ]]; then
    continue
  fi

  # Re-put. gbrain put reads content from stdin per CLI help.
  # Use --content via heredoc-like piping. Wrap in error-handler.
  if "$GBRAIN" put "$slug" --content "$(cat "$f")" >>"$LOG" 2>&1; then
    n_put=$((n_put+1))
    echo "[$(date -u +%FT%TZ)] PUT ok slug=$slug" | tee -a "$LOG"
  else
    n_err=$((n_err+1))
    echo "[$(date -u +%FT%TZ)] PUT FAILED slug=$slug (see log)" | tee -a "$LOG"
  fi
done < <(find "${find_args[@]}" 2>/dev/null)

echo "[$(date -u +%FT%TZ)] rehydrate done total=$n_total already_present=$n_skip_present re_put=$n_put errors=$n_err" | tee -a "$LOG"

if [[ $n_err -gt 0 ]]; then
  echo "WARNING: $n_err errors. Inspect $LOG."
  exit 2
fi
