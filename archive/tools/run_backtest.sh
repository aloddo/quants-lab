#!/bin/bash
# Run backtest tasks in isolation — NEVER run these in the live trading pipeline.
#
# Usage:
#   bash scripts/run_backtest.sh e1_bulk_backtest
#   bash scripts/run_backtest.sh e2_bulk_backtest
#   bash scripts/run_backtest.sh e1_walk_forward
#   bash scripts/run_backtest.sh e2_walk_forward
#
# This runs a single task in a fresh process, completely isolated from
# the live trading pipeline. Memory-intensive backtests will not affect
# position monitoring, signal scanning, or resolver tasks.

set -e
cd /Users/hermes/quants-lab

TASK="${1:?Usage: $0 <task_name>}"
PYTHON=/Users/hermes/miniforge3/envs/quants-lab/bin/python

echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Starting backtest: $TASK"

set -a && source .env && set +a

$PYTHON cli.py trigger-task --task "$TASK" --config config/hermes_pipeline.yml

echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Backtest complete: $TASK"
