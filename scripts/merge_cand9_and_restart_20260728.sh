#!/bin/bash
# Merge the 9 candidate wallets into the screen-set M02 store, then restart m03 + m04 on the
# expanded 1,639-wallet universe and hand off to the funnel chain.
#
# WHY: the 12 wallets in the live copy configs (alpha5 / printalpha3 / totalreturn5, 2026-07-26)
# are only 3/12 present in the 1,630-wallet screen set. 0xab5e6f - the highest-alpha wallet, in ALL
# THREE configs - was missing. A profile built on a universe that excludes the wallets we want to
# copy cannot score them, which is the whole deliverable.
#
# WHY REBUILD m04 FROM SCRATCH: entity_id is a union-find output over the wallet SET, and it is
# joined across folds downstream (m05 L391, m06a L441, m06b groupby). Adding 9 wallets changes the
# ids, so mixing old and new fold files would silently mis-join entities. One universe, all 13 folds.
#
# MEMORY: the actions concat is STREAMED row-group by row-group (CLAUDE.md Key Rule 8). Loading the
# 916MB actions parquet into pandas to concat would need ~3-5GB against ~4GB free.
set -u
cd /Users/hermes/quants-lab
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
DIR=app/data/v15/funnel_20260728
LOG=/tmp/ql_merge_restart_20260728.log
say() { echo "[$(date -u +%FT%TZ)] $*" >> "$LOG"; }

say "=== merge + restart start ==="

# 1. streaming concat -> combined store
$PY - >> "$LOG" 2>&1 <<'PYEOF'
import glob, sys
import pyarrow.parquet as pq
from pathlib import Path

def concat(out_path, inputs):
    out = Path(out_path); out.parent.mkdir(parents=True, exist_ok=True)
    writer = None; total = 0
    base_schema = None
    for p in inputs:
        f = pq.ParquetFile(p)
        if base_schema is None:
            base_schema = f.schema_arrow
        elif f.schema_arrow.names != base_schema.names:
            raise SystemExit(f"SCHEMA MISMATCH {p}\n{f.schema_arrow.names}\nvs\n{base_schema.names}")
        for batch in f.iter_batches(batch_size=50_000):
            if writer is None:
                writer = pq.ParquetWriter(out, batch.schema, compression="snappy")
            writer.write_batch(batch)
            total += batch.num_rows
    if writer: writer.close()
    print(f"wrote {out} rows={total:,}", flush=True)
    return total

acts = ["app/data/v15/m02_screenset_actions/run_000001.parquet"] + \
       sorted(glob.glob("app/data/v15/m02_cand9_actions/*.parquet"))
jrns = ["app/data/v15/m02_screenset/closed/run_000001.parquet"] + \
       sorted(glob.glob("app/data/v15/m02_cand9/closed/*.parquet"))
print("action inputs:", acts, flush=True)
print("journey inputs:", jrns, flush=True)
concat("app/data/v15/m02_combined1639/actions.parquet", acts)
concat("app/data/v15/m02_combined1639/journeys.parquet", jrns)
PYEOF
[ -s app/data/v15/m02_combined1639/actions.parquet ] || { say "ABORT: concat produced no actions"; exit 1; }
say "concat done"

# 2. repoint the run dir at the combined store
ln -sf ../m02_combined1639/actions.parquet  "$DIR/m02_actions.parquet"
ln -sf ../m02_combined1639/journeys.parquet "$DIR/m02_journeys.parquet"

# 3. wallet coverage check - fail closed if any candidate is still missing
$PY - >> "$LOG" 2>&1 <<'PYEOF'
import json, glob, sys
import pandas as pd
a = pd.read_parquet("app/data/v15/m02_combined1639/actions.parquet", columns=["wallet"])
have = set(a["wallet"].astype(str).str.lower().unique())
cand = set()
for f in sorted(glob.glob("config/copy_trader_*_20260726.json")):
    cand |= {w.lower() for w in json.load(open(f))["wallets"]}
missing = sorted(cand - have)
print(f"wallets in combined store: {len(have)}")
print(f"live-config candidates present: {len(cand)-len(missing)}/{len(cand)}")
if missing:
    print("STILL MISSING:", missing)
    sys.exit(1)
PYEOF
[ $? -ne 0 ] && { say "ABORT: candidate wallets still missing after merge"; exit 1; }
say "coverage OK: all live-config candidates present"

# 4. m03 on the expanded universe (13 folds)
rm -f "$DIR"/m03_*.parquet
$PY research/v15/v15_m03_fold_geometry.py \
  --actions "$DIR/m02_actions.parquet" \
  --journeys "$DIR/m02_journeys.parquet" \
  --outdir "$DIR" --n-folds 13 >> "$LOG" 2>&1 || { say "ABORT: m03 failed"; exit 1; }
say "m03 done"

# 5. m04 must be rebuilt for ALL 13 folds on the new universe (entity ids change)
rm -f "$DIR"/m04_authenticity_f*.parquet "$DIR"/m04_entities_f*.parquet
cp app/data/v15/funnel_20260728/wallets_1639.txt "$DIR/screen_wallets_1630.txt"
say "m04 wiped; starting 13-fold rebuild on the 1,639-wallet universe"
bash scripts/m04_funnel_20260728.sh >> "$LOG" 2>&1
rc=$?
say "m04 chain rc=$rc"
[ $rc -ne 0 ] && { say "ABORT: m04 chain failed"; exit 1; }

# 6. hand off to the funnel chain
say "handing off to funnel chain"
bash scripts/funnel_chain_20260728.sh >> "$LOG" 2>&1
say "=== merge + restart COMPLETE ==="
