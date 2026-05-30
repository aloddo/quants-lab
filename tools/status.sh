#!/bin/bash
# Quick system status check. Run from anywhere via SSH.
# Usage: bash /Users/hermes/quants-lab/scripts/status.sh

PYTHON="/Users/hermes/miniforge3/envs/quants-lab/bin/python"

echo "═══════════════════════════════════════════"
echo "  QuantsLab System Status"
echo "═══════════════════════════════════════════"

# LaunchDaemon services
echo ""
echo "🔧 LaunchDaemon services:"
for svc in com.quantslab.pipeline com.quantslab.api; do
    pid=$(sudo launchctl list 2>/dev/null | grep "$svc" | awk '{print $1}')
    if [ -n "$pid" ] && [ "$pid" != "-" ]; then
        echo "  $svc: running (PID $pid)"
    elif [ -n "$pid" ]; then
        echo "  $svc: loaded (not running)"
    else
        echo "  $svc: not loaded"
    fi
done

# Pipeline process check (fallback if launchctl needs sudo)
echo ""
echo "📦 Processes:"
PIPE_PID=$(pgrep -f "run-tasks.*hermes_pipeline|orchestrator.*hermes_pipeline" 2>/dev/null | head -1)
API_PID=$(pgrep -f "serve.*8001" 2>/dev/null | head -1)
[ -n "$PIPE_PID" ] && echo "  Pipeline: running (PID $PIPE_PID)" || echo "  Pipeline: NOT RUNNING"
[ -n "$API_PID" ] && echo "  QL API:   running (PID $API_PID)" || echo "  QL API:   NOT RUNNING"

# MongoDB via Python (mongosh not always available)
echo ""
echo "🗄️  MongoDB:"
$PYTHON -c "
from pymongo import MongoClient
c = MongoClient('mongodb://localhost:27017', serverSelectionTimeoutMS=3000)
db = c['quants_lab']
pairs = len(db.features.distinct('trading_pair'))
print(f'  features:       {db.features.count_documents({}):,} docs ({pairs} pairs)')
print(f'  candidates:     {db.candidates.count_documents({}):,}')
print(f'  pair_historical: {db.pair_historical.count_documents({}):,}')
print(f'  backtest_trades: {db.backtest_trades.count_documents({}):,}')
print(f'  funding_rates:  {db.bybit_funding_rates.count_documents({}):,}')
print(f'  executions:     {db.exchange_executions.count_documents({}):,}')
print(f'  closed_pnl:     {db.exchange_closed_pnl.count_documents({}):,}')
" 2>/dev/null || echo "  (not reachable)"

# Parquet cache
echo ""
echo "📊 Parquet cache:"
CANDLES_DIR="/Users/hermes/quants-lab/app/data/cache/candles"
if [ -d "$CANDLES_DIR" ]; then
    N_1H=$(ls "$CANDLES_DIR"/bybit_perpetual*1h.parquet 2>/dev/null | wc -l | tr -d ' ')
    N_1M=$(ls "$CANDLES_DIR"/bybit_perpetual*1m.parquet 2>/dev/null | wc -l | tr -d ' ')
    SIZE=$(du -sh "$CANDLES_DIR" 2>/dev/null | cut -f1)
    echo "  1h: ${N_1H} pairs | 1m: ${N_1M} pairs | Total: ${SIZE}"
else
    echo "  (no cache directory)"
fi

# HB API + Docker
echo ""
echo "🤖 Hummingbot:"
HB_HEALTH=$(curl -s -u admin:admin http://localhost:8000/health 2>/dev/null)
if [ -n "$HB_HEALTH" ]; then
    echo "  API: $HB_HEALTH"
else
    echo "  API: not reachable"
fi
HB_CONTAINER=$(docker ps --filter name=hummingbot --format '{{.Names}} ({{.Status}})' 2>/dev/null)
[ -n "$HB_CONTAINER" ] && echo "  Container: $HB_CONTAINER" || echo "  Container: not running"

# QL API
echo ""
echo "📡 QuantsLab API:"
curl -s http://localhost:8001/health 2>/dev/null && echo "" || echo "  (not reachable)"

# Live exchange positions (via Bybit API, not MongoDB)
echo ""
echo "📈 Live exchange positions:"
$PYTHON -c "
import asyncio, aiohttp, sys
sys.path.insert(0, '/Users/hermes/quants-lab')
from app.services.bybit_exchange_client import BybitExchangeClient
async def main():
    client = BybitExchangeClient()
    if not client.is_configured():
        print('  (no Bybit credentials configured)')
        return
    async with aiohttp.ClientSession() as s:
        positions = await client.fetch_positions(s)
        wallet = await client.fetch_wallet_balance(s)
        if not positions:
            print('  (no open positions)')
        else:
            total_upnl = 0
            for p in positions:
                upnl = p['unrealised_pnl']
                total_upnl += upnl
                print(f\"  {p['side']:5s} {p['pair']:12s} \${p['position_value']:>8.2f}  uPnL: \${upnl:+.2f}\")
            print(f'  ─────────────────────────────────────')
            print(f'  Total uPnL: \${total_upnl:+.2f}  |  Wallet: \${wallet[\"equity\"]:,.2f}')
asyncio.run(main())
" 2>/dev/null || echo "  (error fetching positions)"

# Watchdog status
echo ""
echo "🛡️  Watchdog:"
$PYTHON -c "
from pymongo import MongoClient
from datetime import datetime, timezone
c = MongoClient('mongodb://localhost:27017', serverSelectionTimeoutMS=3000)
db = c['quants_lab']
state = db.watchdog_state.find_one({'_id': 'current'})
if not state:
    print('  (no watchdog state)')
else:
    issues = state.get('issues', [])
    if not issues:
        print('  All clear')
    else:
        age_min = int((datetime.now(timezone.utc) - state.get('first_seen', datetime.now(timezone.utc))).total_seconds() / 60)
        level = 'CRITICAL' if age_min > 120 else ('WARNING' if age_min > 30 else 'INFO')
        print(f'  {level} ({age_min}m): {len(issues)} issue(s)')
        for i in issues[:3]:
            print(f'    - {i[:80]}')
" 2>/dev/null || echo "  (MongoDB not reachable)"

echo ""
echo "═══════════════════════════════════════════"
