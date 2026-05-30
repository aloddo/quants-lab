#!/bin/bash
# Backfill 1m candles for all X9 pairs sequentially (each pair uses 3-5 GB RAM)
set -a && source /Users/hermes/quants-lab/.env && set +a
cd /Users/hermes/quants-lab

PYTHON=/Users/hermes/miniforge3/envs/quants-lab/bin/python
DAYS=1460
PAIRS="ADA-USDT BNB-USDT DOGE-USDT DOT-USDT LINK-USDT NEAR-USDT XRP-USDT"

echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Starting 1m candle backfill for X9 pairs"
echo "Days: $DAYS, Pairs: $PAIRS"
echo ""

for PAIR in $PAIRS; do
    echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Starting $PAIR..."
    $PYTHON scripts/backfill_1m_candles.py --days $DAYS --pair $PAIR 2>&1 | tail -5
    echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Done with $PAIR"
    echo ""
done

echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] All 1m backfills complete"
