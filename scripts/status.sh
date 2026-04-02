#!/bin/bash
# Quick system status check. Run from anywhere via SSH.
# Usage: bash /Users/hermes/quants-lab/scripts/status.sh

echo "═══════════════════════════════════════════"
echo "  QuantsLab System Status"
echo "═══════════════════════════════════════════"

# tmux sessions
echo ""
echo "📦 tmux sessions:"
tmux ls 2>/dev/null || echo "  (none running)"

# MongoDB
echo ""
echo "🗄️  MongoDB:"
mongosh quants_lab --quiet --eval "
print('  features:      ' + db.features.countDocuments() + ' docs (' + db.features.distinct('trading_pair').length + ' pairs)');
print('  candidates:    ' + db.candidates.countDocuments());
print('  pair_historical: ' + db.pair_historical.countDocuments());
print('  funding_rates: ' + db.bybit_funding_rates.countDocuments());
print('  open_interest: ' + db.bybit_open_interest.countDocuments());
" 2>/dev/null || echo "  (not reachable)"

# Parquet cache
echo ""
echo "📊 Parquet cache:"
CANDLES_DIR="/Users/hermes/quants-lab/app/data/cache/candles"
if [ -d "$CANDLES_DIR" ]; then
    N_1H=$(ls "$CANDLES_DIR"/bybit_perpetual*1h.parquet 2>/dev/null | wc -l | tr -d ' ')
    N_5M=$(ls "$CANDLES_DIR"/bybit_perpetual*5m.parquet 2>/dev/null | wc -l | tr -d ' ')
    SIZE=$(du -sh "$CANDLES_DIR" 2>/dev/null | cut -f1)
    echo "  1h: ${N_1H} pairs | 5m: ${N_5M} pairs | Total: ${SIZE}"
else
    echo "  (no cache directory)"
fi

# HB API
echo ""
echo "🤖 Hummingbot API:"
curl -s -u admin:admin http://localhost:8000/health 2>/dev/null && echo "" || echo "  (not reachable)"

# QL API
echo ""
echo "📡 QuantsLab API:"
curl -s http://localhost:8001/health 2>/dev/null && echo "" || echo "  (not reachable)"

# Active positions
echo ""
echo "📈 Active testnet positions:"
mongosh quants_lab --quiet --eval "
var active = db.candidates.find({disposition: 'TESTNET_ACTIVE'}).toArray();
if (active.length === 0) { print('  (none)'); }
else { active.forEach(c => print('  ' + c.engine + ' ' + c.pair + ' ' + c.direction + ' since ' + new Date(c.testnet_placed_at).toISOString())); }
" 2>/dev/null || echo "  (MongoDB not reachable)"

echo ""
echo "═══════════════════════════════════════════"
