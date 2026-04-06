"""
E1 Decision Snapshot — P2 backbone
V7.1 spec: snapshot-first, immutable, no live re-reads after creation.

Pipeline order (enforced, non-negotiable):
  1. BUILD SNAPSHOT   — freeze all inputs, assign snapshot_id
  2. VALIDATE         — staleness check from snapshot only
  3. TRIGGER          — 1h compression + 5m first-trigger
  4. FILTERS          — hard (risk-off, volume) + soft (EMA gate)
  5. REGISTER         — write candidate to candidate_log
  6. EXECUTE          — portfolio allocator reads from candidate only

Every downstream object (candidate, order, fill) carries snapshot_id.
No live re-reads after step 1.
"""

import time
import uuid
import sqlite3
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

DB_PATH = Path.home() / "quants-lab/app/data/candidate_log.db"

# ── Execution lifecycle states ────────────────────────────────────────────────

class State:
    CANDIDATE_REGISTERED    = "CANDIDATE_REGISTERED"
    WAITING_TRIGGER         = "WAITING_TRIGGER"       # setup active, no 5m trigger yet
    ENTRY_QUALITY_FAIL      = "ENTRY_QUALITY_FAIL"    # trigger fired, quality checks failed
    ORDER_PLACED            = "ORDER_PLACED"           # limit order submitted
    ORDER_FILLED            = "ORDER_FILLED"           # confirmed fill
    ORDER_EXPIRED           = "ORDER_EXPIRED"          # setup expired before fill
    ORDER_CANCELLED         = "ORDER_CANCELLED"        # manually cancelled
    FILTERED_RISK_OFF       = "FILTERED_RISK_OFF"
    FILTERED_VOLUME_FLOOR   = "FILTERED_VOLUME_FLOOR"
    FILTERED_SHORT_TREND    = "FILTERED_SHORT_TREND"
    FILTERED_STALENESS      = "FILTERED_STALENESS"
    FILTERED_NO_SETUP       = "FILTERED_NO_SETUP"
    SKIPPED_NO_TRIGGER      = "SKIPPED_NO_TRIGGER"


# ── Snapshot ──────────────────────────────────────────────────────────────────

@dataclass
class DecisionSnapshot:
    """
    Immutable once built. All pipeline stages read from this — never from live data.
    snapshot_id is the anchor for every downstream object.
    """
    snapshot_id:         str   = field(default_factory=lambda: str(uuid.uuid4()))
    created_at_ms:       int   = field(default_factory=lambda: int(time.time() * 1000))
    pair:                str   = ""
    # 1h features (frozen)
    atr_pct:             Optional[float] = None
    atr_5m:              Optional[float] = None
    compressed:          bool  = False
    range_high:          Optional[float] = None
    range_low:           Optional[float] = None
    breakout_event:      int   = 0        # +1 long, -1 short, 0 none
    breakout_level:      Optional[float] = None
    volume_floor_ok:     bool  = False
    volume_spike:        bool  = False
    risk_off:            bool  = False
    risk_off_source:     str   = ""       # "external_state" | "ema_proxy"
    short_trend_ok:      bool  = True
    # 5m features (frozen at trigger time)
    trigger_bar_ts:      Optional[int]   = None
    trigger_5m_close:    Optional[float] = None
    trigger_5m_open:     Optional[float] = None
    entry_distance_atr:  Optional[float] = None
    entry_body_atr:      Optional[float] = None
    entry_gap_bps:       Optional[float] = None
    # Execution prices
    signal_level:        Optional[float] = None   # breakout level at signal time
    intended_entry_price: Optional[float] = None  # limit order price
    # Staleness
    staleness_ok:        bool  = True
    staleness_flags:     list  = field(default_factory=list)
    # Market state
    market_state:        str   = "Unknown"


# States from which no further transitions are permitted
TERMINAL_STATES = {
    State.ORDER_FILLED, State.ORDER_CANCELLED, State.ORDER_EXPIRED,
    State.FILTERED_RISK_OFF, State.FILTERED_VOLUME_FLOOR,
    State.FILTERED_SHORT_TREND, State.FILTERED_STALENESS,
    State.FILTERED_NO_SETUP, State.ENTRY_QUALITY_FAIL,
    State.SKIPPED_NO_TRIGGER,
}


def build_snapshot(pair: str, features_1h: dict, features_5m: dict,
                   market_state: str = "Unknown") -> DecisionSnapshot:
    """
    STEP 1: Build and freeze snapshot from controller features.
    Called once — never updated after creation.
    """
    snap = DecisionSnapshot(pair=pair, market_state=market_state)

    # 1h features
    snap.atr_pct         = features_1h.get("atr_pct")
    snap.compressed      = bool(features_1h.get("compressed", False))
    snap.range_high      = features_1h.get("range_high")
    snap.range_low       = features_1h.get("range_low")
    snap.breakout_event  = int(features_1h.get("breakout_event", 0))
    snap.volume_floor_ok = bool(features_1h.get("volume_floor_ok", False))
    snap.volume_spike    = bool(features_1h.get("volume_spike", False))
    snap.risk_off        = bool(features_1h.get("risk_off", False))
    snap.risk_off_source = str(features_1h.get("risk_off_source", "unknown"))
    snap.short_trend_ok  = bool(features_1h.get("short_trend_ok", True))

    # Breakout level
    if snap.breakout_event == 1 and snap.range_high is not None:
        snap.breakout_level = snap.range_high
        snap.signal_level   = snap.range_high
    elif snap.breakout_event == -1 and snap.range_low is not None:
        snap.breakout_level = snap.range_low
        snap.signal_level   = snap.range_low

    # 5m features (may be empty if trigger not yet fired)
    if features_5m:
        snap.atr_5m              = features_5m.get("atr_5m")
        snap.trigger_bar_ts      = features_5m.get("timestamp")
        snap.trigger_5m_close    = features_5m.get("close")
        snap.trigger_5m_open     = features_5m.get("open")
        snap.entry_distance_atr  = features_5m.get("entry_distance_atr")
        snap.entry_body_atr      = features_5m.get("entry_body_atr")
        snap.entry_gap_bps       = features_5m.get("entry_gap_bps")
        # Intended entry = limit at breakout level (same as signal level)
        snap.intended_entry_price = snap.signal_level

    return snap


def validate_snapshot(snap: DecisionSnapshot) -> tuple[bool, list]:
    """
    STEP 2: Staleness check from snapshot only. No live reads.
    Returns (ok, flags).
    """
    flags = []
    now_ms = int(time.time() * 1000)
    age_s  = (now_ms - snap.created_at_ms) / 1000

    if age_s > 75 * 60:      # >75min — stale 1h candle
        flags.append("snapshot_age_1h")
    if snap.atr_pct is None:
        flags.append("atr_pct_missing")
    if snap.range_high is None or snap.range_low is None:
        flags.append("range_missing")

    snap.staleness_ok    = len(flags) == 0
    snap.staleness_flags = flags
    return snap.staleness_ok, flags


# ── Candidate ─────────────────────────────────────────────────────────────────

@dataclass
class Candidate:
    """
    Registered after step 4 (filters). Carries snapshot_id through execution.
    Filled fields updated by execution layer — snapshot itself is never touched.
    """
    candidate_id:       str   = field(default_factory=lambda: str(uuid.uuid4()))
    snapshot_id:        str   = ""
    timestamp_ms:       int   = field(default_factory=lambda: int(time.time() * 1000))
    pair:               str   = ""
    direction:          int   = 0       # +1 long, -1 short

    # Pipeline results
    staleness_ok:       bool  = False
    trigger_fired:      bool  = False
    hard_filters_passed: bool = False
    filter_reason:      str   = ""
    soft_score:         int   = 0
    soft_breakdown:     dict  = field(default_factory=dict)
    entry_quality_pass: bool  = False
    entry_quality_fail_reason: str = ""

    # Lifecycle state
    state:              str   = State.CANDIDATE_REGISTERED

    # Execution fields (set by live layer, never by backtest)
    signal_level:       Optional[float] = None
    intended_entry_price: Optional[float] = None
    touched_signal_price: Optional[bool] = None   # did price reach level?
    fillable_estimate:  Optional[bool]  = None    # heuristic — NOT ground truth
    actual_fill:        Optional[bool]  = None    # ONLY confirmed fill = truth
    actual_fill_price:  Optional[float] = None
    max_adverse_excursion_before_fill: Optional[float] = None


def evaluate_candidate(snap: DecisionSnapshot,
                       trade_direction: str = "BOTH",
                       short_trend_filter: bool = True) -> Candidate:
    """
    STEPS 3+4: Evaluate trigger and filters from snapshot only.
    Returns populated Candidate with state and disposition set.
    """
    cand = Candidate(
        snapshot_id = snap.snapshot_id,
        pair        = snap.pair,
        signal_level = snap.signal_level,
        intended_entry_price = snap.intended_entry_price,
    )

    # STEP 2 result
    cand.staleness_ok = snap.staleness_ok
    if not snap.staleness_ok:
        cand.state         = State.FILTERED_STALENESS
        cand.filter_reason = f"stale: {snap.staleness_flags}"
        return cand

    # STEP 3: Trigger
    be = snap.breakout_event
    if be == 0:
        cand.trigger_fired = False
        cand.state         = State.SKIPPED_NO_TRIGGER
        cand.filter_reason = "no_breakout_event"
        return cand

    take_longs  = trade_direction in ("BOTH", "LONG_ONLY")
    take_shorts = trade_direction in ("BOTH", "SHORT_ONLY")
    if (be == 1 and not take_longs) or (be == -1 and not take_shorts):
        cand.state         = State.FILTERED_NO_SETUP
        cand.filter_reason = f"direction_gate_{trade_direction}"
        return cand

    cand.trigger_fired = True
    cand.direction     = be

    # STEP 4a: Hard filters
    if snap.risk_off:
        cand.state         = State.FILTERED_RISK_OFF
        cand.filter_reason = f"risk_off ({snap.risk_off_source})"
        return cand

    if not snap.volume_floor_ok:
        cand.state         = State.FILTERED_VOLUME_FLOOR
        cand.filter_reason = "volume_below_floor"
        return cand

    if be == -1 and short_trend_filter and not snap.short_trend_ok:
        cand.state         = State.FILTERED_SHORT_TREND
        cand.filter_reason = "short_trend_not_bearish"
        return cand

    cand.hard_filters_passed = True

    # STEP 4b: Soft score (informational, Phase 1 — no gate)
    score = 0
    bd    = {}
    if snap.volume_spike:
        score += 1; bd["volume_spike"] = True
    else:
        bd["volume_spike"] = False
    cand.soft_score     = score
    cand.soft_breakdown = bd

    # Entry quality (from 5m features in snapshot)
    if snap.entry_distance_atr is not None:
        eq_ok   = True
        reasons = []
        if snap.entry_distance_atr > 0.30:
            eq_ok = False; reasons.append(f"dist={snap.entry_distance_atr:.3f}>0.30xATR")
        if snap.entry_body_atr is not None and snap.entry_body_atr > 0.50:
            eq_ok = False; reasons.append(f"body={snap.entry_body_atr:.3f}>0.50xATR")
        if snap.entry_gap_bps is not None:
            atr_bps = (snap.atr_5m / abs(snap.breakout_level) * 10000
                       if snap.atr_5m and snap.breakout_level else 9999)
            gap_thr = min(5.0, 0.15 * atr_bps)
            if snap.entry_gap_bps > gap_thr:
                eq_ok = False; reasons.append(f"gap={snap.entry_gap_bps:.1f}>{gap_thr:.1f}bps")
        cand.entry_quality_pass        = eq_ok
        cand.entry_quality_fail_reason = "|".join(reasons)
        if not eq_ok:
            cand.state = State.ENTRY_QUALITY_FAIL
            return cand

    cand.state = State.CANDIDATE_REGISTERED
    return cand


# ── Candidate logging ─────────────────────────────────────────────────────────

def init_db(db_path: Path = DB_PATH):
    """Create candidate_log table if not exists."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS candidate_log (
            candidate_id                TEXT PRIMARY KEY,
            snapshot_id                 TEXT NOT NULL,
            timestamp_ms                INTEGER,
            pair                        TEXT,
            direction                   INTEGER,
            -- pipeline
            staleness_ok                INTEGER,
            trigger_fired               INTEGER,
            hard_filters_passed         INTEGER,
            filter_reason               TEXT,
            soft_score                  INTEGER,
            soft_breakdown              TEXT,
            entry_quality_pass          INTEGER,
            entry_quality_fail_reason   TEXT,
            state                       TEXT,
            -- execution prices
            signal_level                REAL,
            intended_entry_price        REAL,
            -- fill tracking (updated live)
            touched_signal_price        INTEGER,
            fillable_estimate           INTEGER,
            actual_fill                 INTEGER,
            actual_fill_price           REAL,
            max_adverse_excursion_before_fill REAL,
            -- snapshot fields (denormalized for queryability)
            atr_pct                     REAL,
            compressed                  INTEGER,
            range_high                  REAL,
            range_low                   REAL,
            volume_floor_ok             INTEGER,
            risk_off                    INTEGER,
            risk_off_source             TEXT,
            short_trend_ok              INTEGER,
            atr_5m                      REAL,
            entry_distance_atr          REAL,
            entry_body_atr              REAL,
            entry_gap_bps               REAL,
            market_state                TEXT,
            created_at                  INTEGER DEFAULT (strftime('%s','now') * 1000),
            updated_at                  INTEGER DEFAULT (strftime('%s','now') * 1000)
        )
    """)
    conn.commit()
    conn.close()


def register_candidate(cand: Candidate, snap: DecisionSnapshot,
                       db_path: Path = DB_PATH) -> None:
    """
    STEP 5: Write candidate to candidate_log.
    Called for ALL candidates — including filtered ones.
    Filtered candidates are the audit trail.
    """
    import json
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        INSERT OR IGNORE INTO candidate_log (
            candidate_id, snapshot_id, timestamp_ms, pair, direction,
            staleness_ok, trigger_fired, hard_filters_passed,
            filter_reason, soft_score, soft_breakdown,
            entry_quality_pass, entry_quality_fail_reason, state,
            signal_level, intended_entry_price,
            touched_signal_price, fillable_estimate,
            actual_fill, actual_fill_price,
            max_adverse_excursion_before_fill,
            atr_pct, compressed, range_high, range_low,
            volume_floor_ok, risk_off, risk_off_source, short_trend_ok,
            atr_5m, entry_distance_atr, entry_body_atr, entry_gap_bps,
            market_state, created_at, updated_at
        ) VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )
    """, (
        cand.candidate_id, cand.snapshot_id, cand.timestamp_ms, cand.pair, cand.direction,
        int(cand.staleness_ok), int(cand.trigger_fired), int(cand.hard_filters_passed),
        cand.filter_reason, cand.soft_score, json.dumps(cand.soft_breakdown),
        int(cand.entry_quality_pass), cand.entry_quality_fail_reason, cand.state,
        cand.signal_level, cand.intended_entry_price,
        None, None, None, None, None,  # fill fields — set live
        snap.atr_pct, int(snap.compressed), snap.range_high, snap.range_low,
        int(snap.volume_floor_ok), int(snap.risk_off), snap.risk_off_source,
        int(snap.short_trend_ok),
        snap.atr_5m, snap.entry_distance_atr, snap.entry_body_atr, snap.entry_gap_bps,
        snap.market_state,
        int(time.time() * 1000), int(time.time() * 1000),  # created_at, updated_at
    ))
    conn.commit()
    conn.close()


def update_candidate_fill(candidate_id: str,
                          touched: Optional[bool],
                          fillable: Optional[bool],
                          actual_fill: Optional[bool],
                          actual_fill_price: Optional[float],
                          mae_before_fill: Optional[float],
                          state: str,
                          db_path: Path = DB_PATH) -> bool:
    """
    Update fill fields post-execution. Called by live execution layer only.
    touched_signal_price: did price reach the limit level?
    fillable_estimate: heuristic — NEVER used as ground truth.
    actual_fill: ONLY confirmed exchange fill counts.

    Terminal state guard: refuses to overwrite a terminal state.
    Returns True if update applied, False if blocked by terminal state.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    row = conn.execute(
        "SELECT state FROM candidate_log WHERE candidate_id = ?",
        (candidate_id,)
    ).fetchone()

    if row is None:
        conn.close()
        return False  # candidate not found

    current_state = row["state"]
    if current_state in TERMINAL_STATES:
        conn.close()
        return False  # terminal state — refuse overwrite

    conn.execute("""
        UPDATE candidate_log SET
            touched_signal_price = ?,
            fillable_estimate    = ?,
            actual_fill          = ?,
            actual_fill_price    = ?,
            max_adverse_excursion_before_fill = ?,
            state                = ?,
            updated_at           = ?
        WHERE candidate_id = ?
    """, (
        int(touched) if touched is not None else None,
        int(fillable) if fillable is not None else None,
        int(actual_fill) if actual_fill is not None else None,
        actual_fill_price,
        mae_before_fill,
        state,
        int(time.time() * 1000),
        candidate_id,
    ))
    conn.commit()
    conn.close()
    return True
