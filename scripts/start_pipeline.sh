#!/bin/bash
# Start the QuantsLab pipeline in tmux sessions.
# Run from: /Users/hermes/quants-lab
#
# Creates two tmux sessions:
#   ql-pipeline  — TaskOrchestrator running hermes_pipeline.yml
#   ql-api       — FastAPI task management API (for remote monitoring)

set -e
cd /Users/hermes/quants-lab

# Load environment
export MONGO_URI=mongodb://localhost:27017/quants_lab
export MONGO_DATABASE=quants_lab
PYTHON=/Users/hermes/miniforge3/envs/quants-lab/bin/python

# Kill existing sessions if running
tmux kill-session -t ql-pipeline 2>/dev/null || true
tmux kill-session -t ql-api 2>/dev/null || true

# Start pipeline orchestrator
tmux new-session -d -s ql-pipeline \
  "cd /Users/hermes/quants-lab && \
   export MONGO_URI=$MONGO_URI && \
   export MONGO_DATABASE=$MONGO_DATABASE && \
   source .env 2>/dev/null; \
   $PYTHON cli.py run-tasks --config config/hermes_pipeline.yml 2>&1 | tee /tmp/ql-pipeline.log; \
   echo 'Pipeline exited. Press Enter to close.'; read"

# Start task API (port 8001 to avoid conflict with HB API on 8000)
tmux new-session -d -s ql-api \
  "cd /Users/hermes/quants-lab && \
   export MONGO_URI=$MONGO_URI && \
   export MONGO_DATABASE=$MONGO_DATABASE && \
   source .env 2>/dev/null; \
   $PYTHON cli.py serve --config config/hermes_pipeline.yml --port 8001 2>&1 | tee /tmp/ql-api.log; \
   echo 'API exited. Press Enter to close.'; read"

echo "Started:"
echo "  ql-pipeline — tmux attach -t ql-pipeline"
echo "  ql-api      — tmux attach -t ql-api (http://localhost:8001)"
echo ""
echo "Logs:"
echo "  tail -f /tmp/ql-pipeline.log"
echo "  tail -f /tmp/ql-api.log"
