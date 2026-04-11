#!/bin/bash
# Emergency kill switch — stops all trading activity.
#
# Actions:
#   1. Pause all tasks in the orchestrator (via API)
#   2. Stop all active executors on testnet (via HB API)
#   3. Mark all TESTNET_ACTIVE candidates as EMERGENCY_STOPPED in MongoDB
#   4. Send Telegram notification
#   5. Optionally kill tmux sessions
#
# Usage:
#   bash scripts/kill_switch.sh          # Pause tasks + stop positions
#   bash scripts/kill_switch.sh --full   # Also kill tmux sessions

set -e
cd /Users/hermes/quants-lab

PYTHON=/Users/hermes/miniforge3/envs/quants-lab/bin/python
QL_API="http://localhost:8001"
HB_API="http://localhost:8000"
HB_AUTH="admin:admin"

echo "🛑 KILL SWITCH ACTIVATED"
echo "========================"

# 1. Pause all tasks via QL API
echo "[1/4] Pausing all tasks..."
curl -s -X POST "$QL_API/tasks/pause-all" 2>/dev/null && echo "  Tasks paused" || echo "  QL API not reachable (may already be down)"

# 2+3. Stop executors and conditionally mark candidates
# Uses Python script with proper per-executor error handling:
# - Only marks EMERGENCY_STOPPED if executor stop succeeded
# - Keeps TESTNET_ACTIVE with flag if stop failed (position may still be open)
$PYTHON scripts/kill_switch_logic.py 2>/dev/null || {
    echo "  kill_switch_logic.py failed — falling back to bulk mark"
    mongosh quants_lab --quiet --eval "
var result = db.candidates.updateMany(
    {disposition: {\$in: ['TESTNET_ACTIVE', 'PLACING']}},
    {\$set: {disposition: 'EMERGENCY_STOPPED', stopped_at: Date.now()}}
);
print('  Updated ' + result.modifiedCount + ' candidates');
" 2>/dev/null || echo "  MongoDB not reachable"
}

# 4. Send Telegram notification
echo "[4/4] Sending Telegram alert..."
$PYTHON -c "
import os, asyncio
async def notify():
    try:
        from core.notifiers.manager import get_notification_manager
        from core.notifiers.base import NotificationMessage
        mgr = get_notification_manager()
        if mgr:
            await mgr.send_notification(NotificationMessage(
                title='KILL SWITCH',
                message='<b>🛑 EMERGENCY STOP</b>\nAll tasks paused. All positions stopped.',
                level='error',
            ))
            print('  Telegram sent')
        else:
            print('  Notifier not configured')
    except Exception as e:
        print(f'  Telegram failed: {e}')
asyncio.run(notify())
" 2>/dev/null || echo "  Telegram notification failed"

# 5. Optionally kill tmux sessions
if [ "$1" = "--full" ]; then
    echo ""
    echo "Killing tmux sessions..."
    tmux kill-session -t ql-pipeline 2>/dev/null && echo "  ql-pipeline killed" || echo "  ql-pipeline not running"
    tmux kill-session -t ql-api 2>/dev/null && echo "  ql-api killed" || echo "  ql-api not running"
fi

echo ""
echo "🛑 Kill switch complete."
echo "   To resume: bash scripts/start_pipeline.sh"
