#!/bin/bash
# Start the QuantsLab pipeline in tmux sessions.
# Run from: /Users/hermes/quants-lab
#
# Creates two tmux sessions:
#   ql-pipeline  — TaskOrchestrator running hermes_pipeline.yml
#   ql-api       — FastAPI task management API (for remote monitoring)
#
# RESILIENCE: Both sessions run inside auto-restart loops.
# If the process crashes for any reason it restarts in 10 seconds.

set -e
cd /Users/hermes/quants-lab

# Load environment
export MONGO_URI=mongodb://localhost:27017/quants_lab
export MONGO_DATABASE=quants_lab
PYTHON=/Users/hermes/miniforge3/envs/quants-lab/bin/python

# Kill existing sessions if running
tmux kill-session -t ql-pipeline 2>/dev/null || true
tmux kill-session -t ql-api 2>/dev/null || true

# Start pipeline orchestrator — auto-restart loop
tmux new-session -d -s ql-pipeline \
  "cd /Users/hermes/quants-lab && \
   export MONGO_URI=$MONGO_URI && \
   export MONGO_DATABASE=$MONGO_DATABASE && \
   source .env 2>/dev/null || true; \
   while true; do \
     echo \"[\\$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Starting pipeline...\"; \
     $PYTHON cli.py run-tasks --config config/hermes_pipeline.yml 2>&1 | tee -a /tmp/ql-pipeline.log; \
     EXIT_CODE=\$?; \
     echo \"[\\$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Pipeline exited (code \$EXIT_CODE). Restarting in 10s...\"; \
     sleep 10; \
   done"

# Start task API — auto-restart loop
tmux new-session -d -s ql-api \
  "cd /Users/hermes/quants-lab && \
   export MONGO_URI=$MONGO_URI && \
   export MONGO_DATABASE=$MONGO_DATABASE && \
   source .env 2>/dev/null || true; \
   while true; do \
     echo \"[\\$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Starting API...\"; \
     $PYTHON cli.py serve --config config/hermes_pipeline.yml --port 8001 2>&1 | tee -a /tmp/ql-api.log; \
     EXIT_CODE=\$?; \
     echo \"[\\$(date -u '+%Y-%m-%d %H:%M:%S UTC')] API exited (code \$EXIT_CODE). Restarting in 10s...\"; \
     sleep 10; \
   done"

echo "Started:"
echo "  ql-pipeline — tmux attach -t ql-pipeline"
echo "  ql-api      — tmux attach -t ql-api (http://localhost:8001)"
echo ""
echo "Logs:"
echo "  tail -f /tmp/ql-pipeline.log"
echo "  tail -f /tmp/ql-api.log"
