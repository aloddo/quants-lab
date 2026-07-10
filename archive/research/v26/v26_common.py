#!/usr/bin/env python3
"""v26 config-grid harness -- shared frozen constants + infrastructure.

BINDING specs: /tmp/v26_grid_prereg.md (grid amendment, frozen) on top of
/tmp/v25_prereg_v3.md (base). v25 is FROZEN under app/data/research/v25/FREEZE.json and a
live run consumes it: this package IMPORTS research/v25 modules and READS its cached fold
artifacts; it never modifies any v25 file.

EPISTEMIC STATUS (frozen, codex r2 #1): ALL v26 fold results are EXPLORATORY screening
evidence, never confirmatory. The corrected LCB only ranks configs for the single sealed
holdout slot; the SEALED HOLDOUT (>= 2026-07-16) is the SOLE confirmatory evidence for
any v26 config. Every report carries EXPLORATORY_LABEL.

DOCUMENTED IMPLEMENTATION DECISIONS (ambiguities resolved here, disclosed in reports):
- D1 family definitional bands: F2 = [2min, 15min) (its own definition, half-open like the
  band axis). F1a/F1b ("swing") = [2h, 48h) median hold -- F1a inherits it from R1's own
  eligibility (v25_r1_causal HOLD_MIN/MAX 2h/48h); F1b applies the SAME swing window on
  top of the R2-LCB ranking so that "swing-R2" is a real variant and not plain R2.
  F3/F4a/F4b carry no definitional band. family_forces_band pruning: a band cell is
  pruned iff intersect(def_band, band) is empty OR collapses to a band cell that already
  runs (dedup rule of the amendment: F2 x 'any' == F2 x '2-15min' -> pruned).
- D2 cadence: closed train journeys / distinct UTC days with >= 1 closed-journey EXIT in
  train (open journeys excluded from both, exit-date convention consistent with the
  frozen trip-to-date assignment).
- D3 F4a/F4b ranking: R2-style LCB (the amendment fixes only their cadence filters; the
  objective-aligned LCB is the house ranking for non-R1 families).
- D4 max_hold 7d + no-re-entry apply to ALL exit styles including E1 (the amendment's
  "Common" block lists them for the overlay section; E1's MIRROR logic itself is the
  unchanged v25 FIRST_CLOSE).
- D5 causal-tier trigger geometry: E2/E3 stop/trail scans price the accrued exit cost at
  the snapshot BASE-TIER taker rate. Realized fees are charged at the config's own
  causal-tier rate. At $150-500 order sizes the simulated 14d volume never reaches the
  first VIP cutoff ($5M), so the two rates are identical; the assembly ASSERTS this
  (fail closed, codex code-gate #5): any config whose realized (charged) rate ever
  departs the base-tier schedule raises -> runtime_failure, never silently uses stale
  trigger fees. tier_departed_base counts causal-tier departures in BOTH scenarios.
- D6 fee snapshot mapping: BASE = snapshot tier rates x (1 - activeReferralDiscount)
  (reproduces the measured 4.32bps = 4.5 x 0.96 of v25 BASE); WORST = snapshot base-tier
  rates with NO discount as the FLOOR -- the causal tier engine still evolves tiers
  (mechanism active and honest, codex code-gate #5) but an improved tier can never
  reduce the charged WORST rate (4.5bps taker -- the same number as the v25 WORST file
  schedule, now sourced from the snapshot). HIP-3 multiplier 2.0 (same source as v25:
  8.64/4.32). Slippage: BASE per-coin L2 calib, WORST flat 7.0bps, from the frozen v25
  scenarios; BASE maker fills carry NO slippage by construction (adverse selection is
  embedded in the cross/no-cross rule); WORST maker fills DO pay the frozen 7.0bps
  slip default on entry and exit (codex code-gate #6, amendment codex #9).
- D7 equity/DD clock: the per-config equity, drawdown, and time-weighted gross series are
  evaluated on the 1m mark grid (the amendment's own trigger cadence: "the v25 per-mark
  DD clock") plus the terminal close. avg_gross_d weights = 60s per mark over the FULL
  day (flat minutes contribute gross 0).
- D8 K-scaling delta translation (documented per mandate): "LCB bar delta x M" is
  implemented as an ESTIMAND SHIFT -- the config's tested daily series is
  (PnL_d - M x 10bps x admitted_entry_notional_d) / avg_gross_d, i.e. pass requires the
  adjusted LCB of the BASE estimand to exceed (M - 1) x 10bps x admitted_d / gross_d
  per day in excess-return units. Pass = adjusted LCB(shifted series) > 0 uniformly.
- D9 maker fill-rate dual eval: pooled (across folds) entry fill rate < 50% triggers a
  second evaluation with every missed maker entry executed as the SAME exit-style taker
  trip; the variant with the LOWER mean shifted daily excess is the config's official
  series (criteria use the worse); both are reported.
- D10 runtime failure: any exception / non-finite result in a config's evaluation fails
  that config CLOSED: it stays in the correction family (Holm p = 1), appears in the
  kill ledger as runtime_failure, and can never pass. The family never shrinks.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent.parent
V25_DIR = REPO / "research" / "v25"
V15_DIR = REPO / "research" / "v15"
for _d in (V25_DIR, V15_DIR):
    if str(_d) not in sys.path:
        sys.path.insert(0, str(_d))

# v25 frozen machinery (imported, NEVER edited)
from v25_common import (DROPOUT_SEEDS, DUST_USD, EXIT_TRIGGER_FRAC, INITIAL_EQUITY,  # noqa: F401
                        MAX_COIN_SIDE_X, MAX_MARGIN_UTIL, MS_DAY, MS_MIN, ORDER_USD,
                        REPRICE_MS, REPRICE_WINDOW_MS, RESERVE_LEV, ExecScenario,
                        MarksIndex, ShardedParquetWriter, coin_is_hip3, coin_is_spot,
                        event_dropout, folds, git_commit, install_memory_guard,
                        iter_wallet_frames, scenario_base, scenario_worst, sha256_file)

V25_DATA = REPO / "app" / "data" / "research" / "v25"
V26_DATA = REPO / "app" / "data" / "research" / "v26"
FEE_SNAPSHOT_PATH = V25_DATA / "fee_snapshot_v26.json"
GRID_PREREG_DOC = Path("/tmp/v26_grid_prereg.md")
V25_FREEZE_PATH = V25_DATA / "FREEZE.json"
V25_VERDICT_PATH = V25_DATA / "verdict.json"
FREEZE_GRID_PATH = V26_DATA / "FREEZE-GRID.json"

EXPLORATORY_LABEL = (
    "EXPLORATORY SCREENING EVIDENCE -- NEVER CONFIRMATORY (frozen, codex r2 #1). "
    "Declared v25 fold-1 exposure contaminates fold-level inference for this family; "
    "the corrected LCB only ranks configs for the sealed-holdout slot; the SEALED "
    "HOLDOUT (>= 2026-07-16) is the SOLE confirmatory evidence for any v26 config.")

# ---- frozen grid axes (amendment, codex #2) -------------------------------------------------- #
FAMILY_VARIANTS = ["F1a", "F1b", "F2", "F3", "F4a", "F4b"]
K_GRID = [1, 5, 10, 25, 50, 100, 500, 1000]
HOLD_BANDS = ["2-15min", "15min-2h", "2-24h", "any"]
EXIT_STYLES = ["E1", "E2", "E3"]
GROSS_CAPS = [2.5, 10.0]
EXECUTIONS = ["taker", "maker_entry", "maker_both"]
SIZINGS = ["150", "500", "pct2"]
FULL_CROSS = (len(FAMILY_VARIANTS) * len(K_GRID) * len(HOLD_BANDS) * len(EXIT_STYLES)
              * len(GROSS_CAPS) * len(EXECUTIONS) * len(SIZINGS))          # 10,368

# half-open [lo, hi) intervals in ms (codex #7); None = unconstrained
BAND_BOUNDS_MS = {
    "2-15min": (2 * MS_MIN, 15 * MS_MIN),
    "15min-2h": (15 * MS_MIN, 120 * MS_MIN),
    "2-24h": (120 * MS_MIN, 1440 * MS_MIN),
    "any": None,
}
# family definitional bands (decision D1)
FAMILY_DEF_BAND_MS = {
    "F1a": (120 * MS_MIN, 2880 * MS_MIN),
    "F1b": (120 * MS_MIN, 2880 * MS_MIN),
    "F2": BAND_BOUNDS_MS["2-15min"],
}

# family filter constants (amendment, codex #11 + mandate)
MIN_CLOSED_FOR_BAND = 30            # < 30 closed train journeys => fails any band filter
F2_MIN_CLOSED = 30
F2_MIN_CADENCE = 5.0                # closed journeys per active day
F3_MIN_JOURNEYS = 100
F3_MIN_LOSSES = 20
F3_BIG_RUN_R = 10.0                 # big run = net bps >= 10 x R_unit_w
F4A_MIN_CADENCE = 10.0
F4B_MAX_CADENCE = 2.0
MAX_K = max(K_GRID)

# exit overlays (codex #8, frozen)
R_UNIT_FRAC = 0.01                  # E2/E3 R unit = 1% of entry notional
TRAIL_ACT_R = 3.0                   # E3 trail activates at >= +3R
TRAIL_GIVEBACK = 0.70               # exit when cum net MTM <= 0.70 x running peak (<=)
MAX_HOLD_MS = 7 * MS_DAY

# maker model (codex #9, frozen; structural CLOSE-ONLY, no intrabar, deterministic)
MAKER_TIMEOUT_MS = 60_000
MAKER_MIN_FILL_RATE = 0.50
MAKER_MODEL_VERSION = "v26-close-only-1.0"

# estimand + corrections (codex #4/#5, frozen)
HURDLE_FRAC = 10.0 / 1e4            # 10bps of admitted entry notional, inside the estimand
MIN_NONZERO_DAYS = 30
GRID_RESAMPLES = 100_000            # joint max-stat, SHARED draws
HOLM_RESAMPLES = 200_000            # fallback
GRID_SEED = 42
GRID_BLOCK_DAYS = 7
GRID_BLOCK_DAYS_ROBUST = 14         # inherited v25 block-robustness check: the 14d-block
                                    # corrected conclusion must AGREE with 7d, else FAIL
GRID_FAMILYWISE_LEVEL = 0.95        # one-sided 95% familywise (alpha 0.05, as v25)

EPS = 1e-9

EXIT_REASONS = ["MIRROR", "STOP", "TRAIL", "MAXHOLD", "TERMINAL"]
MISS_REASONS = ["entry_no_mark", "maker_no_post", "maker_no_cross", "maker_cancelled",
                "dust"]


def config_id(fam: str, k: int, band: str, exit_style: str, gross: float,
              execution: str, sizing: str) -> str:
    return f"{fam}|K{k}|{band}|{exit_style}|G{gross:g}|{execution}|{sizing}"


def k_real_tier(k_real_min: int) -> dict:
    """K scaling (codex #6) on the realized min-over-folds roster."""
    if k_real_min >= 15:
        return {"tier": "full", "entity_min": 15, "trips_min": 150, "delta_mult": 1.0,
                "entity_caps": True, "coin_caps": True, "label": None}
    if k_real_min >= 5:
        return {"tier": "mid", "entity_min": k_real_min, "trips_min": 150,
                "delta_mult": 1.25, "entity_caps": False, "coin_caps": True,
                "label": None}
    return {"tier": "concentrated", "entity_min": max(k_real_min, 0), "trips_min": 300,
            "delta_mult": 1.5, "entity_caps": False, "coin_caps": False,
            "label": "CONCENTRATED"}


# ---- v25 artifact access (RUNTIME only; the runner enforces run-state preconditions) --------- #
def artifact_path(name: str, fold: int) -> Path:
    return V25_DATA / f"{name}_fold{fold}.parquet"


def load_artifact(name: str, fold: int, columns: list[str] | None = None) -> pd.DataFrame:
    p = artifact_path(name, fold)
    if not p.exists():
        raise FileNotFoundError(f"v25 fold artifact missing: {p} -- v26 consumes cached "
                                f"v25 artifacts and cannot proceed without them")
    return pd.read_parquet(p, columns=columns)


def v25_run_alive() -> bool:
    """True iff a v25 fold-runner process is alive (the live run owns the artifact dir)."""
    try:
        out = subprocess.check_output(["ps", "-axo", "command"]).decode()
    except Exception:
        return True     # fail-closed: cannot verify => assume alive
    return any("v25_run_folds.py" in line and "grep" not in line
               for line in out.splitlines())


def v25_verdict_complete(n_folds: int = 5) -> bool:
    """True iff verdict.json exists and every rule carries all n_folds folds."""
    if not V25_VERDICT_PATH.exists():
        return False
    try:
        with open(V25_VERDICT_PATH) as fh:
            v = json.load(fh)
        want = {str(k) for k in range(1, n_folds + 1)}
        return all(want <= set(r.get("folds", {}).keys())
                   for r in v.get("per_rule", {}).values()) and bool(v.get("per_rule"))
    except Exception:
        return False


# ---- FoldMarks: minute-grid forward-filled close matrix over one fold window ---------------- #
class FoldMarks:
    """Per-fold 1m mark grid: slot k at start + k*60s, k = 0..n (INCLUSIVE end slot, so the
    terminal asof mark at the window end is readable, exactly like v25 finish()).

    Slot value = last cache close with close_ts <= slot_ts (v25 asof_mark parity: an
    invalid (NaN/<=0) latest close makes the slot unavailable -- NaN -- rather than
    falling back to an earlier bar). Closes only occur on minute boundaries, so the slot
    value at floor(ts/60s) IS asof_mark(ts) for any ts."""

    def __init__(self, marks: MarksIndex, start_ms: int, end_ms: int):
        assert start_ms % MS_MIN == 0 and end_ms % MS_MIN == 0
        self.marks = marks
        self.start_ms = int(start_ms)
        self.end_ms = int(end_ms)
        self.n_slots = (self.end_ms - self.start_ms) // MS_MIN + 1
        self.slot_ts = self.start_ms + np.arange(self.n_slots, dtype="int64") * MS_MIN
        self._cache: dict[str, np.ndarray] = {}

    def series(self, coin: str) -> np.ndarray:
        arr = self._cache.get(coin)
        if arr is not None:
            return arr
        mins, closes = self.marks._series(coin)
        if mins.size == 0:
            arr = np.full(self.n_slots, np.nan)
        else:
            close_ts = np.asarray(mins, dtype="float64") + MS_MIN
            idx = np.searchsorted(close_ts, self.slot_ts.astype("float64"),
                                  side="right") - 1
            vals = np.asarray(closes, dtype="float64")[np.maximum(idx, 0)]
            vals = np.where(idx >= 0, vals, np.nan)
            arr = np.where(vals > 0, vals, np.nan)
        self._cache[coin] = arr
        return arr

    def slot_of(self, ts_ms: int) -> int:
        """Slot index whose value is the asof mark at ts (floor)."""
        return int((int(ts_ms) - self.start_ms) // MS_MIN)

    def slot_ceil(self, ts_ms: int) -> int:
        """First slot index at ts' >= ts."""
        return int(-((self.start_ms - int(ts_ms)) // MS_MIN))

    def asof(self, coin: str, ts_ms: int) -> float:
        i = self.slot_of(ts_ms)
        if i < 0:
            return float("nan")
        i = min(i, self.n_slots - 1)
        return float(self.series(coin)[i])


def taker_exit_fill(marks: MarksIndex, coin: str, anchor_ts: int, end_ms: int):
    """Frozen v25 exit-pricing fallback chain (gate-b #5) anchored at anchor_ts:
    (1) first mark in [anchor+2s, anchor+60s] and < window end;
    (2) first mark >= anchor+2s and < window end (LATE);
    (3) none -> None (terminal MTM settles it).
    Returns (fill_ts, mark, late) or (None, None, False)."""
    fill_ts, mark = marks.next_mark(coin, anchor_ts, REPRICE_MS, REPRICE_WINDOW_MS,
                                    cap_ms=end_ms)
    if fill_ts is not None:
        return fill_ts, mark, False
    fill_ts, mark = marks.next_mark(coin, anchor_ts, REPRICE_MS, None, cap_ms=end_ms)
    if fill_ts is not None:
        return fill_ts, mark, True
    return None, None, False


def taker_entry_fill(marks: MarksIndex, coin: str, signal_ts: int, end_ms: int):
    """Frozen v25 entry repricing: first mark closing at >= signal+2s within 60s, capped
    at the window end; else drop-and-count. Returns (fill_ts, mark) or (None, None)."""
    return marks.next_mark(coin, signal_ts, REPRICE_MS, REPRICE_WINDOW_MS, cap_ms=end_ms)


def canonical_sha256(df: pd.DataFrame) -> str:
    """Deterministic content hash of a DataFrame (sorted columns, CSV bytes) -- parquet
    file bytes are not deterministic across writes, the CONTENT hash is. The EXPLORATORY
    banner column (write_exploratory_parquet) is presentation, not content: excluded."""
    import hashlib
    d = df.drop(columns=["exploratory"], errors="ignore")
    d = d[sorted(d.columns)]
    return hashlib.sha256(d.to_csv(index=False).encode()).hexdigest()


# ---- EXPLORATORY labeling of EVERY output artifact (codex code-gate #8) ---------------------- #
def write_exploratory_parquet(df: pd.DataFrame, path) -> None:
    """Write a parquet artifact carrying the EXPLORATORY label BOTH as a file-level
    metadata key (exploratory=true + the full label text) AND as a banner column, so no
    reader can consume the artifact without seeing the epistemic status."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    d = df.copy()
    d["exploratory"] = True
    table = pa.Table.from_pandas(d, preserve_index=False)
    meta = dict(table.schema.metadata or {})
    meta[b"exploratory"] = b"true"
    meta[b"label"] = EXPLORATORY_LABEL.encode()
    pq.write_table(table.replace_schema_metadata(meta), path)


def stamp_parquet_exploratory(path) -> None:
    """Stamp an already-written parquet file (e.g. a ShardedParquetWriter stitch output)
    with the file-level EXPLORATORY metadata key + banner column."""
    import pyarrow.parquet as pq
    write_exploratory_parquet(pq.read_table(path).to_pandas(), path)
