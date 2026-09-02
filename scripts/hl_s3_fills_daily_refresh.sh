#!/bin/bash
# Daily bounded HL S3 fills refresh.
#
# This job is independent from the live copy trader and uses S3 only. It does
# not call Hyperliquid REST and it does not load trading credentials.

set -euo pipefail

WORKDIR="/Users/hermes/quants-lab"
PYTHON="/Users/hermes/miniforge3/envs/quants-lab/bin/python"
LOG="/tmp/ql-hl-s3-fills-daily.log"

cd "$WORKDIR"

echo "[$(date -u '+%F %T UTC')] hl_s3_fills_daily_refresh: start" >> "$LOG"
pipeline_rc=0

# 1) Fills + all-market 1m candles (node_fills_by_block). NOT exec'd: the misc job must run after regardless
#    of the fills exit code (fills returns 1 on any failed day; || guard prevents set -e from aborting misc).
fills_rc=0
"$PYTHON" data_pipeline/hl_s3_fills_daily_refresh.py \
  --wallet-source app/data/v13/equity_universe_20k.parquet \
  --wallet-source config/copy_trader_wallets_gate1_v4.json \
  --out-dir app/data/hl_s3_fills_v2_hot \
  --candles-out-dir app/data/hl_s3_candles_1m_hot \
  --manifest app/data/hl_s3_fills_v2_hot_manifest.json \
  --publish-lag-days 1 \
  --rewrite-lookback-days 3 \
  --bootstrap-days 14 \
  --n-workers 3 \
  --no-prune \
  >> "$LOG" 2>&1 || fills_rc=$?
if [ "$fills_rc" -ne 0 ]; then
  pipeline_rc=1
  echo "[$(date -u '+%F %T UTC')] fills exited non-zero ($fills_rc)" >> "$LOG"
fi

# 2) Funding + ledger for the SAME 20K universe (misc_events_by_block). Same cron, same wallets, same pattern.
echo "[$(date -u '+%F %T UTC')] hl_s3_misc_daily_refresh: start" >> "$LOG"
misc_rc=0
"$PYTHON" data_pipeline/hl_s3_misc_daily_refresh.py \
  --wallet-source app/data/v13/equity_universe_20k.parquet \
  --wallet-source config/copy_trader_wallets_gate1_v4.json \
  --funding-out-dir app/data/hl_s3_funding_hot \
  --ledger-out-dir app/data/hl_s3_ledger_hot \
  --manifest app/data/hl_s3_misc_hot_manifest.json \
  --publish-lag-days 1 \
  --rewrite-lookback-days 3 \
  --bootstrap-days 14 \
  --n-workers 3 \
  >> "$LOG" 2>&1 || misc_rc=$?
if [ "$misc_rc" -ne 0 ]; then
  pipeline_rc=1
  echo "[$(date -u '+%F %T UTC')] misc exited non-zero ($misc_rc)" >> "$LOG"
fi

# 3) Mongo candle sync (Fable-approved 2026-08-08): keeps hyperliquid_candles fed from the hot
#    store so the m07 mark source can never silently freeze again (the 06-24 incident). Additive
#    amend+insert writes; invalidates touched ohlc caches (lazy rebuild by consumers). Runs even
#    if the fills step failed — its own staleness check fires loud in that case.
echo "[$(date -u '+%F %T UTC')] hl_candles_mongo_sync: start" >> "$LOG"
sync_rc=0
"$PYTHON" data_pipeline/hl_candles_mongo_sync.py --days 3 >> "$LOG" 2>&1 || sync_rc=$?
if [ "$sync_rc" -ne 0 ]; then
  pipeline_rc=1
  echo "[$(date -u '+%F %T UTC')] candle mongo sync exited non-zero ($sync_rc)" >> "$LOG"
fi

echo "[$(date -u '+%F %T UTC')] hl_s3 daily pipeline: done rc=$pipeline_rc" >> "$LOG"
exit "$pipeline_rc"
