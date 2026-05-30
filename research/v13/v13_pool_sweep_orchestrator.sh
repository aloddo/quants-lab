#!/usr/bin/env bash
# V13 process-sharded full-pool sweep orchestrator.
# Splits eligible wallets into N shards, spawns N python processes (true GIL bypass),
# waits, merges parquets.
#
# Usage:
#   bash scripts/v13_pool_sweep_orchestrator.sh <wallets_file> <output> [n_shards] [k_target]
#
# Example:
#   bash scripts/v13_pool_sweep_orchestrator.sh /tmp/eligible.txt /tmp/full_pool.parquet 6 25

set -euo pipefail

WALLETS_FILE="${1:?wallets-file required}"
OUTPUT="${2:?output parquet required}"
N_SHARDS="${3:-6}"
K_TARGET="${4:-25}"
MARKS_PARQUET="${5:-}"   # optional preloaded marks parquet

PY="/Users/hermes/miniforge3/envs/quants-lab/bin/python"
ROOT="/Users/hermes/quants-lab"
SHARD_DIR="/tmp/v13_pool_shards_$$"
mkdir -p "$SHARD_DIR"

TOTAL=$(wc -l < "$WALLETS_FILE" | tr -d ' ')
PER_SHARD=$(( (TOTAL + N_SHARDS - 1) / N_SHARDS ))

echo "[orchestrator] $TOTAL wallets → $N_SHARDS shards × ~$PER_SHARD each"

# Split wallets
split -l "$PER_SHARD" "$WALLETS_FILE" "$SHARD_DIR/shard_"

PIDS=()
i=0
for SHARD in "$SHARD_DIR"/shard_*; do
    OUT="$SHARD_DIR/result_${i}.parquet"
    LOG="$SHARD_DIR/shard_${i}.log"
    echo "[orchestrator] launching shard $i: $SHARD → $OUT"
    MARKS_ARG=()
    if [ -n "${MARKS_NPZ_DIR:-}" ] && [ -d "$MARKS_NPZ_DIR" ]; then
        MARKS_ARG=(--marks-npz-dir "$MARKS_NPZ_DIR")
    elif [ -n "$MARKS_PARQUET" ]; then
        MARKS_ARG=(--marks-parquet "$MARKS_PARQUET")
    fi
    "$PY" "$ROOT/scripts/v13_pool_sweep_shard.py" \
        --journeys-glob "$ROOT/app/data/v13/journey_chunks/chunk_*.parquet" \
        --wallets-file "$SHARD" \
        --output "$OUT" \
        --K-target "$K_TARGET" \
        "${MARKS_ARG[@]}" > "$LOG" 2>&1 &
    PIDS+=($!)
    i=$((i+1))
done

echo "[orchestrator] spawned ${#PIDS[@]} shards: ${PIDS[*]}"
echo "[orchestrator] waiting..."

START_T=$(date +%s)
FAILED=0
for PID in "${PIDS[@]}"; do
    if wait "$PID"; then
        echo "[orchestrator] shard PID $PID done OK"
    else
        echo "[orchestrator] shard PID $PID FAILED"
        FAILED=$((FAILED+1))
    fi
done
ELAPSED=$(( $(date +%s) - START_T ))
echo "[orchestrator] all shards done in ${ELAPSED}s; failures=$FAILED"

# Merge
echo "[orchestrator] merging shard parquets..."
"$PY" -c "
import pandas as pd
import glob
files = sorted(glob.glob('$SHARD_DIR/result_*.parquet'))
print(f'merging {len(files)} files')
dfs = [pd.read_parquet(f) for f in files if pd.read_parquet(f).shape[0] > 0]
df = pd.concat(dfs, ignore_index=True).sort_values('copy_score', ascending=False)
df.to_parquet('$OUTPUT', index=False)
print(f'wrote $OUTPUT: {len(df):,} rows')
passers = (df['reason'] == 'PASS').sum()
positives = (df['copy_score'] > 0).sum()
print(f'  passers={passers}  positive_score={positives}  max_score={df[\"copy_score\"].max():.5f}')
"

echo "[orchestrator] cleanup: rm -rf $SHARD_DIR (logs preserved in /tmp/v13_pool_shards_${$}_logs)"
mkdir -p "/tmp/v13_pool_shards_${$}_logs"
mv "$SHARD_DIR"/*.log "/tmp/v13_pool_shards_${$}_logs/" 2>/dev/null || true
rm -rf "$SHARD_DIR"
echo "[orchestrator] DONE"
