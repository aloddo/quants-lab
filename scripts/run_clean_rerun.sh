#!/bin/bash
# run_clean_rerun.sh -- ONE-COMMAND deciding clean re-run of the leak-free V15 pipeline.
#
# Built 2026-06-06 after the codex loop closed every look-ahead/fold-leakage in M1-M8
# (branch v15-rigor-causality-fixes). This wires the full clean chain so Alberto's "go"
# is a single command:
#   M1 (causal-by-default, sharded for bounded memory) -> recal_pipeline.sh (consolidate +
#   M2 -> M3 -> build_m4_perfold -> M5/M6a/M6b --m04-dir -> M7 -> M6b -> M8 --m04-dir --nominal-capital)
# Then run the two-regime test separately (Dec-May select -> June validate) for the verdict.
#
# SAFETY: every heavy step runs under scripts/mem_safe_run.sh (the mandatory OOM backstop). M1 is
# sharded and run SEQUENTIALLY so peak memory stays ~1.5GB/shard (streaming-io keeps it flat).
# This script is the LEAK-FREE re-run; it OVERWRITES m01_rerun/ shards with the causal reconstruction.
#
# Usage:  bash scripts/run_clean_rerun.sh [n_shards=10] [procs=3] [ceil_gb=8]
set -euo pipefail
cd /Users/hermes/quants-lab
set -a && source .env 2>/dev/null && set +a
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
D=app/data/v15
WALLETS=$D/m01_nonerroring_wallets.txt
NSHARD=${1:-10}
PROCS=${2:-3}
CEIL=${3:-8}
SAFE="scripts/mem_safe_run.sh --floor-gb $CEIL"
SHARDDIR=$D/m01_rerun
log(){ echo "[clean-rerun $(date +%H:%M:%S)] $*"; }

# Guard: refuse to run if the live copy trader is somehow active (this is research; bot must stay paused).
if pgrep -f hl_prop_copy.py >/dev/null; then echo "ABORT: live trader running; this is a research re-run, bot must be paused."; exit 3; fi

log "Clean re-run START. $(wc -l < $WALLETS) wallets, $NSHARD shards, procs=$PROCS, ceil=${CEIL}GB."
mkdir -p $SHARDDIR
# 0) ARCHIVE the prior (leaky, ex-post-seed) M1 shards so recal_pipeline's shard_*.parquet glob
#    consolidates ONLY the new causal shards. MOVE (never delete -- regenerable but preserved).
if ls $SHARDDIR/shard_*.parquet >/dev/null 2>&1; then
  ARCH=$SHARDDIR/_preclean_$(date +%Y%m%d_%H%M)
  mkdir -p $ARCH && mv $SHARDDIR/shard_*.parquet $ARCH/
  log "archived $(ls $ARCH/*.parquet 2>/dev/null | grep -vc audit) prior M1 shards -> $ARCH"
fi
# 1) split the wallet universe into NSHARD ~equal pieces (deterministic, line-based)
TMPSPLIT=$(mktemp -d)
split -n l/$NSHARD -a 2 $WALLETS $TMPSPLIT/wal_
i=0
for part in $TMPSPLIT/wal_*; do
  sfx=$(printf "%02d" $i)
  log "M1 CAUSAL shard $sfx ($(wc -l < $part) wallets) -> $SHARDDIR/shard_s$sfx.parquet"
  $SAFE --label m1_s$sfx -- $PY research/v15/v15_m01_equity_reconstruct.py \
    --wallets-file $part --start 2025-12-01 --end 2026-05-23 \
    --output $SHARDDIR/shard_s$sfx.parquet 2>&1 | tail -1
  i=$((i+1))
done
rm -rf $TMPSPLIT
log "M1 causal sharded reconstruction DONE ($i shards). Handing to recal_pipeline.sh (M2->M8, fold-pure)."

# 2) recal_pipeline.sh: its step-0 waits for any running M1 (none now), consolidates m01_rerun/shard_*.parquet,
#    then runs the fold-pure M2->M8 chain. Pass through wallets/procs/ceil.
bash scripts/recal_pipeline.sh "$WALLETS" "$PROCS" "$CEIL"

log "CLEAN RE-RUN COMPLETE. Next: run the two-regime test (Dec-May M7 -> June live) for the verdict."
echo "CLEAN_RERUN_DONE"
