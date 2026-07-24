"""[Phase 1 seed — still valid input to Phase 1c/1d.]
One-time bootstrap: populate checkpoint['last_flat_ts'] for the holder-replay optimization
WITHOUT a full-history replay.

Root cause (2026-07-16): the daily M2 read-window is bounded by cp['last_flat_ts'] (per-holder last
all-flat instant). The current checkpoint predates that field, so every holder replays from start_day
and each daily increment re-reads the FULL ~354-day history (the ~7hr crawl). The write-side IS coded
(run_daily saves last_flat_ts), so a full run 6 would seed it -- but that costs the 7hr once.

Cheaper + batch-identical: the complete journey interval set for every current holder ALREADY exists
in the materialized active store (closed run parquets) + open_snapshot. Recompute last_flat_ts from
those with the module's OWN _compute_last_flat, inject into the checkpoint. Correctness-identical to
what run 6 would have written; if wrong, the worst case is a holder replays from start (slow, never
incorrect) -- but we use the exact same function on the exact same interval basis, so it matches.

Verify after: run ONE daily increment; the bulk-load superset window must shrink from full-history to
a bounded recent window, and TEST B (incr == full-batch) must still PASS.
"""
import json, sys
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from data_pipeline.m02_journeys_daily import (
    DEFAULT_OUT_DIR, DEFAULT_STATE_DIR, load_active_closed, load_checkpoint,
    save_checkpoint, day_start_ms, day_end_ms, _compute_last_flat,
)

OUT = Path(DEFAULT_OUT_DIR)
STATE = Path(DEFAULT_STATE_DIR)

cp = load_checkpoint(STATE)
assert cp is not None, "no checkpoint to seed"
assert "last_flat_ts" not in cp or not cp["last_flat_ts"], "checkpoint already has last_flat_ts"

start_day = cp["start_day"]
watermark = cp["watermark_day"]
start_ms = day_start_ms(start_day)
target_end = day_end_ms(watermark)   # seed reproduces the state AS OF the current watermark
print(f"seeding last_flat_ts: start_day={start_day} watermark={watermark} "
      f"start_ms={start_ms} target_end={target_end}")

# ---- holders = wallets with an OPEN journey in open_snapshot ----
snap_path = OUT / "open_snapshot" / "open_snapshot.parquet"
snap = pd.read_parquet(snap_path, columns=["wallet", "entry_ts", "n_carry_in_seeds"])
snap["wallet"] = snap["wallet"].astype(str).str.lower()
holders = set(snap["wallet"])
print(f"holders (open journeys): {len(snap):,} rows over {len(holders):,} wallets")

# ---- complete CLOSED interval set for those holders (materialized active store) ----
ac = load_active_closed(OUT, wallets=holders)
if not ac.empty:
    ac = ac[["wallet", "entry_ts", "exit_ts", "n_carry_in_seeds"]].copy()
    ac["wallet"] = ac["wallet"].astype(str).str.lower()
print(f"active closed journeys for holders: {len(ac):,} rows")

# ---- build per-wallet interval tuples (entry, exit_or_None, n_carry_in) ----
by_wallet: dict[str, list] = {}
if not ac.empty:
    for w, e, x, nci in zip(ac["wallet"], ac["entry_ts"], ac["exit_ts"], ac["n_carry_in_seeds"]):
        xx = None if (x is None or (isinstance(x, float) and np.isnan(x))) else int(x)
        by_wallet.setdefault(w, []).append((int(e), xx, int(nci or 0)))
# add each holder's OPEN journey (exit=None => covers to target_end)
for w, e, nci in zip(snap["wallet"], snap["entry_ts"], snap["n_carry_in_seeds"]):
    by_wallet.setdefault(w, []).append((int(e), None, int(nci or 0)))

# ---- compute last_flat per holder (module's own function => batch-identical) ----
last_flat: dict[str, int] = {}
bounded = 0
for w in holders:
    lf = _compute_last_flat(by_wallet.get(w, []), start_ms, target_end)
    last_flat[w] = int(lf)
    if lf > start_ms:
        bounded += 1
print(f"computed last_flat for {len(last_flat):,} holders; "
      f"{bounded:,} bounded (> start_ms), {len(last_flat)-bounded:,} fall back to start_ms")

# distribution sanity
vals = np.array(sorted(last_flat.values()))
for q in (0, 10, 25, 50, 75, 90, 100):
    v = int(np.percentile(vals, q))
    from datetime import datetime, timezone
    print(f"  p{q:>3}: {v}  ({datetime.fromtimestamp(v/1000, timezone.utc).date()})")

# ---- inject + save (atomic via save_checkpoint) ----
new_cp = dict(cp)
new_cp["last_flat_ts"] = last_flat
save_checkpoint(STATE, new_cp)
print(f"SEEDED checkpoint with last_flat_ts ({len(last_flat):,} holders). "
      f"Backup NOT deleted; save_checkpoint is atomic.")
