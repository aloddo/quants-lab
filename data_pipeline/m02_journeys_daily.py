#!/usr/bin/env python3
"""V15 M02 — DAILY-INCREMENTAL journeys job (append only NEW journeys).

PART 3 of the M2 fix. Maintains a materialized, day/run-partitioned CLOSED-journey
store that is PROVABLY IDENTICAL (on all fills-derived fields + funding_net) to a
full-batch recompute over the same window, while only reprocessing the small set of
wallets ACTIVE (or holding) on each incremental day instead of the full universe.

It REUSES ``research/v15/v15_m02_journey_trace.py:process_wallet`` (and therefore
``trace_wallet``) UNCHANGED. The only added datum is ``journey_uid``, a deterministic
post-hoc column. No journey math is altered here.

Data source: the LIVE hot stores via ``research/v15/hl_fills_io.py`` (PART 1). Fills
day-partitioned ``app/data/hl_s3_fills_v2_hot/YYYYMMDD.parquet``; funding likewise.

--------------------------------------------------------------------------------
WHY REPLAYING A MINIMAL PER-POSITION RANGE IS UNSAFE (design note)
--------------------------------------------------------------------------------
``trace_wallet`` seeds carry-in from the FIRST in-window fill's ``startPosition``.
A journey OPEN before the window start therefore has its ``entry_ts`` truncated to
that first in-window fill. Consequently a replay window MUST begin at (or before) a
still-open journey's TRUE open_ts to reproduce it batch-identically. Starting a
replay at ``min(open_ts of currently-open positions)`` is NOT safe: a DIFFERENT coin
can hold a position that opened even earlier and closed before the watermark (so it
is not in the checkpoint's open_positions) — replaying from that instant truncates
that coin's already-materialized journey and emits a spurious duplicate.

The only replay starts that are guaranteed batch-consistent are instants where the
wallet is provably FLAT on ALL coins:
  * the window START day (both batch and incremental seed carry-in there identically),
  * the WATERMARK, when the checkpoint proves the wallet had zero open positions.

REPLAY POLICY (provably correct; see the mandatory equivalence gate):
  * FIRST RUN / catch-up bootstrap: every universe wallet replays from START-day.
  * A wallet HOLDING any position at the watermark, OR touched by a HISTORICAL
    change (late-published / manifest-diff day <= watermark): replay from START-day
    (full window). Only the holder / changed subset pays this; it is bounded.
  * A wallet FLAT at the watermark and active ONLY on NEW days (> watermark):
    replay from the FIRST NEW day's start (the watermark is a proven flat point, so
    this is batch-identical) — the genuine incremental fast path.

REPLACE-BY-AFFECTED-RANGE: for each affected wallet we tombstone (active=False)
previously-active CLOSED rows with ``entry_ts >= replay_start`` and write the freshly
emitted closed rows (active=True, new run_id). Rows with entry_ts < replay_start are
untouched. Re-running the same target reproduces identical ACTIVE rows (idempotent).

Rule-8 streaming (decisions/2026-05-31): CLOSED rows stream to disk via
``ShardedParquetWriter``; workers run in a ``Pool`` with ``maxtasksperchild`` and
``install_memory_guard``. Fail CLOSED on any worker error (error manifest + exit 1).

CLI: see ``python data_pipeline/m02_journeys_daily.py --help``.
"""
from __future__ import annotations

import argparse
import contextlib
import glob
import hashlib
import json
import logging
import os
import sys
import time
from multiprocessing import Pool
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq

# Reuse the UNCHANGED M02 tracer + the standalone hot-store I/O (PART 1).
_V15 = Path(__file__).resolve().parents[1] / "research" / "v15"
sys.path.insert(0, str(_V15))
import hl_fills_io as fio  # noqa: E402
import v15_m02_journey_trace as m02  # noqa: E402
from _streaming_io import (ShardedParquetWriter, install_memory_guard,  # noqa: E402
                           plan_memory_budget, MemoryBudgetError, _available_ram_gb)

# --- P0 memory safety (2026-07-17): m02 OOM-panicked the 16GB box (CHUNK=1500 over a full ~354d window ->
# ~34GB PARENT grouped-load; Pool had a per-worker guard but NO aggregate budget -> 6x12=72GB ceiling).
# CALIBRATION (measured /usr/bin/time -l): 200 wallets over a 354-day window = 4.5GB peak parent RSS
# -> ~0.020 GB fills+funding per wallet over the full window. Below caps BOTH the per-chunk parent load
# (by window length) AND the aggregate proc count so a single run cannot exceed a safe fraction of RAM.
_GB_PER_WALLET_DAY = 7.0e-5        # grouped fills+funding RAM per wallet per window-DAY (0.020/354, rounded up)
_PARENT_INFLIGHT_MULT = 1.5        # parent chunk dict + in-flight pickled worker slices held concurrently
_DEFAULT_PARENT_GB = 1.5           # target PARENT RSS per grouped-load chunk (kept low so parent+writer+trace
                                   # fits UNDER the mem_safe_run 4GB job-tree ceiling on the serial path)
_WRITER_GB = 1.0                   # ShardedParquetWriter buffers + main-process overhead


def _bounded_chunk(window_ms: int, target_parent_gb: float = _DEFAULT_PARENT_GB, lo: int = 15, hi: int = 2000) -> int:
    """Wallets per grouped-load chunk so the PARENT's per-chunk fills+funding RAM stays ~<= target_parent_gb,
    sized by the window LENGTH (fills scale ~linearly with days). A full-window first-run uses a SMALL chunk
    (kills the 34GB balloon); a short daily-incremental window uses a large chunk (few re-reads). Output is
    chunk-size-invariant (each wallet is traced over its own window regardless of batching). This is a
    CALIBRATED average bound; the HARD per-run ceiling is scripts/mem_safe_run.sh (kills the job group if
    high-volume-wallet skew pushes a chunk past the ceiling), so a mis-estimate degrades to a killed run, not
    an OOM (codex P2)."""
    window_days = max(1.0, window_ms / 86_400_000)
    per_wallet_gb = _GB_PER_WALLET_DAY * window_days * _PARENT_INFLIGHT_MULT
    return max(lo, min(hi, int(target_parent_gb / max(per_wallet_gb, 1e-9))))


def _parent_gb_for(chunk_wallets: int, window_ms: int) -> float:
    """Estimated PARENT RSS for one grouped-load chunk of chunk_wallets over a window (matches _bounded_chunk's
    model). Window-aware so a light daily-incremental run (small window) reserves little and stays runnable,
    while a full first-run reserves the bounded ~parent_gb."""
    window_days = max(1.0, window_ms / 86_400_000)
    return chunk_wallets * _GB_PER_WALLET_DAY * window_days * _PARENT_INFLIGHT_MULT


class _SerialPool:
    """Drop-in for a multiprocessing Pool that runs tasks IN THE MAIN PROCESS (no worker forks). Used when the
    aggregate budget can't afford even one worker process but the run must still make progress. Memory-safe
    ONLY because the grouped-load CHUNK is window-bounded (parent stays ~<= parent_gb); without that bound this
    would be the original OOM. Slower than a real pool, but never panics the box and always runnable."""
    def imap(self, fn, it):
        for x in it:
            yield fn(x)
    imap_unordered = imap
    def __enter__(self): return self
    def __exit__(self, *a): return False


@contextlib.contextmanager
def _worker_pool(b, mem_soft_gb, maxtasks):
    """Yield a real Pool when the budget grants >=1 worker, else an in-main _SerialPool (bounded-chunk safe)."""
    if b is None:
        install_memory_guard(soft_gb=mem_soft_gb, label="m02-serial")
        logger.warning("m02: aggregate memory budget can't afford a worker process at current free RAM -> "
                       "running SERIALLY in-main (bounded chunk keeps it memory-safe; slower).")
        yield _SerialPool()
    else:
        with Pool(b.procs, initializer=_init_worker, initargs=(b.worker_soft_gb,), maxtasksperchild=maxtasks) as p:
            yield p


_SERIAL_FREE_MARGIN_GB = 1.5   # light free-RAM margin for the serial path (the mem_safe_run backstop is the
                               # HARD box guarantee via its ceiling + immediate kernel-critical kill)
_TRACE_WORKING_GB = 1.0        # one wallet's fills/events/journeys held in-main during a serial trace


def _job_ceiling_gb() -> float | None:
    """The mem_safe_run job-tree RSS CEILING (MEM_SAFE_RUN_CEIL_MB, exported by the wrapper), in GB. The
    in-process plan must fit UNDER this or the watchdog kills the job (codex P1: the two layers disagreed).
    None when unwrapped (--allow-unwrapped test slices)."""
    mb = os.environ.get("MEM_SAFE_RUN_CEIL_MB")
    return (float(mb) / 1024.0) if mb else None


def _budget_or_serial(requested_procs: int, parent_gb: float):
    """Plan a run that fits UNDER the mem_safe_run job-tree ceiling (the hard box guarantee) AND free RAM.
    Returns a MemBudget for a Pool, or None for the bounded serial in-main path. Raises MemoryBudgetError ONLY
    when even a single bounded serial chunk cannot fit the ceiling or free RAM -> fail BEFORE loading, cleanly
    (caller FIX-5 -> fail-closed). Layer 2 is graceful degradation + keeping the plan under the ceiling so the
    backstop rarely fires; it does NOT pre-abort on padded estimates -- the backstop guards genuine pressure."""
    ceil_gb = _job_ceiling_gb()
    free = _available_ram_gb()
    serial_need = parent_gb + _WRITER_GB + _TRACE_WORKING_GB
    # MEM_ALLOW_LOW_FREE (Alberto 2026-07-23, explicit + informed): psutil.available EXCLUDES the macOS
    # compressor, which frees on demand -> the free-margin clause is over-conservative on this box. When set,
    # skip ONLY the free-margin clause; the CEILING check (mem_safe_run job-tree guarantee) still holds, and
    # the mem_safe_run system-available floor (polls every 15s, kills the group before OOM/jetsam) remains the
    # real backstop. Default OFF = byte-identical prior behavior. Streaming output => a guard-kill loses nothing.
    _allow_low_free = os.environ.get("MEM_ALLOW_LOW_FREE") == "1"
    over_ceiling = (ceil_gb is not None and serial_need > ceil_gb)
    over_free = (serial_need > free - _SERIAL_FREE_MARGIN_GB)
    if over_ceiling or (over_free and not _allow_low_free):
        raise MemoryBudgetError(
            f"serial infeasible: parent+writer+trace={serial_need:.1f}GB > ceiling={ceil_gb} or "
            f"free-margin={free - _SERIAL_FREE_MARGIN_GB:.1f}GB. Free RAM (pause fleet) or raise --floor-gb.")
    if over_free and _allow_low_free:
        logger.warning(
            f"[mem_budget] MEM_ALLOW_LOW_FREE=1: proceeding despite psutil free-margin "
            f"{free - _SERIAL_FREE_MARGIN_GB:.1f}GB < serial_need {serial_need:.1f}GB (fits ceiling={ceil_gb}GB; "
            f"trusting macOS compressor on-demand release + mem_safe_run floor guard). Alberto 2026-07-23.")
    try:
        b = _budget(requested_procs, parent_gb=parent_gb)   # free-RAM fit (plan_memory_budget)
    except MemoryBudgetError:
        return None   # pool doesn't fit free RAM -> serial (already proven to fit ceiling + free-margin above)
    if ceil_gb is not None:   # also fit the pool plan under the job-tree ceiling
        fit = int((ceil_gb - parent_gb - _WRITER_GB) // max(b.worker_planned_gb, 1e-9))
        if fit < 1:
            return None       # can't fit even one worker under the ceiling -> serial
        if fit < b.procs:
            from _streaming_io import MemBudget
            return MemBudget(procs=fit, main_soft_gb=b.main_soft_gb, worker_soft_gb=b.worker_soft_gb,
                             free_gb=b.free_gb, per_worker_gb=b.per_worker_gb, requested_procs=b.requested_procs,
                             headroom_gb=b.headroom_gb, main_reserve_gb=b.main_reserve_gb,
                             usable_gb=b.usable_gb, worker_planned_gb=b.worker_planned_gb)
    return b


def _budget(requested_procs: int, parent_gb: float, per_worker_gb: float = 1.0, headroom_gb: float = 4.0):
    """AGGREGATE memory plan: cap procs so parent+workers fit free RAM leaving >=headroom_gb for the fleet
    (CoS target 2026-07-17: >=4GB headroom), aborting BEFORE any work if even one worker can't fit (never
    'run serial and hope the per-worker guard fires' -- that IS the OOM we are preventing). main_reserve =
    the (window-aware) bounded parent chunk + streaming-writer slack."""
    return plan_memory_budget(requested_procs=max(1, requested_procs), per_worker_gb=per_worker_gb,
                              headroom_gb=headroom_gb, main_reserve_gb=parent_gb + 1.0)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("m02_daily")

REPO = Path(__file__).resolve().parents[1]
DEFAULT_STATE_DIR = REPO / "app" / "data" / "v15" / "m02_daily_state"
# Phase 2 stateful checkpoint lives in a SEPARATE dir so the one-time seed + the stateful driver never
# clobber the live 1c-1f checkpoint during the cron transition (the store/out-dir is shared).
DEFAULT_STATEFUL_STATE_DIR = REPO / "app" / "data" / "v15" / "m02_stateful_state"
DEFAULT_OUT_DIR = REPO / "app" / "data" / "v15" / "m02_journeys_daily"
# OPTION A canonical actions persistence (2026-07-22): the run_daily driver ALSO persists the
# per-ACTION stream the tracer already computes, day/run-partitioned exactly like the journeys
# closed/ store, byte-equivalent to a full-batch v15_m02 trace. Downstream (m03-m10) reads the
# fresh canonical actions here instead of the stale legacy m02_actions.parquet. run-parts live at
# <ACTIONS_DIR>/run_<id>.parquet (no open/closed split -- every action, open or closed journey, is
# persisted). ONLY run_daily writes it (its correctness comes from every replay starting at an
# all-coin-flat instant); run_daily_stateful deliberately does NOT (see gate note there).
DEFAULT_ACTIONS_DIR = REPO / "app" / "data" / "v15" / "m02_actions_daily"
# Hard memory bound for the out-of-core DuckDB actions reducer/tombstoner. threads alone do NOT cap
# DuckDB RAM; a PRAGMA memory_limit does. Set well under the box so the windowed dedup can never blow
# up before the process memory guard's 15s poll fires (codex P2 #5).
_DUCKDB_MEM_LIMIT_GB = 4.0

# Introspection hook (equivalence gate only): the per-wallet replay_start computed by the LAST
# run_daily call. Prod cost is nil (already in RAM); no return-contract change. The gate asserts a
# holder replays from its last_all_flat point, not start_ms.
_LAST_REPLAY_START: dict[str, int] = {}

# Fields the equivalence gate asserts identical incremental-vs-batch.
CMP_FIELDS = [
    "coin", "side", "entry_ts", "exit_ts", "peak_ts", "duration_h",
    "n_entry_fills", "n_addon_fills", "n_trim_fills", "n_exit_fills",
    "n_reverse_fills", "n_carry_in_seeds", "max_position_notional",
    "realized_pnl", "fees", "funding_net", "net_realized_pnl",
    "journey_class", "liq_closed", "carry_in_status",
    "lifecycle_valid", "state_discontinuity",
]


# --------------------------------------------------------------------------- #
# Hot-store day helpers (read fio dirs DYNAMICALLY so tests can monkeypatch).
# --------------------------------------------------------------------------- #


def _fills_dir() -> Path:
    return Path(fio.HOT_FILLS_DIR)


def hot_available_days() -> list[str]:
    days = []
    for p in glob.glob(str(_fills_dir() / "*.parquet")):
        stem = Path(p).stem
        if len(stem) == 8 and stem.isdigit():
            days.append(stem)
    return sorted(days)


def day_start_ms(day: str) -> int:
    return int(pd.Timestamp(f"{day[:4]}-{day[4:6]}-{day[6:]}", tz="UTC").timestamp() * 1000)


def day_end_ms(day: str) -> int:
    return day_start_ms(day) + 86_400_000 - 1


def day_rowcount(day: str) -> int:
    """Cheap num_rows from parquet metadata (no full read)."""
    p = _fills_dir() / f"{day}.parquet"
    try:
        return int(pq.ParquetFile(p).metadata.num_rows)
    except Exception:  # noqa: BLE001
        return -1


def day_fingerprint(day: str) -> str:
    """Per-day CONTENT fingerprint for manifest change-detection.

    2026-07-12 FIX: the old manifest stored only ``num_rows``, so a LATE HISTORICAL REWRITE with the
    SAME rowcount but CHANGED values (e.g. a corrected fill price / closedPnl) was invisible once the
    day left the lookback window -> the affected wallets were never replayed. The fingerprint combines
    file size + mtime_ns + num_rows: any rewrite updates mtime_ns (and almost always size), so a
    same-rowcount value change is detected and its day's wallets are replayed. Stable across runs when
    the file is untouched (no over-replay). Returns "missing" if the file is absent."""
    p = _fills_dir() / f"{day}.parquet"
    try:
        stt = p.stat()
        return f"{stt.st_size}:{stt.st_mtime_ns}:{day_rowcount(day)}"
    except FileNotFoundError:
        return "missing"


def wallets_in_days(days: list[str], universe: Optional[set[str]]) -> set[str]:
    """Lowercased wallets with >=1 fill on any of ``days`` (optionally intersect universe).

    FAIL CLOSED (FIX 4, 2026-07-12): ``days`` are always inside the REQUESTED [start,target] window,
    so an unreadable / ``wallet``-column-missing day must RAISE (not warn-and-continue). Silently
    skipping a day would omit that day's wallets from ``affected`` and then advance the checkpoint,
    permanently missing those journeys. A genuinely ABSENT file (never collected) is tolerated."""
    out: set[str] = set()
    for d in days:
        p = _fills_dir() / f"{d}.parquet"
        if not p.exists():
            continue
        try:
            w = pd.read_parquet(p, columns=["wallet"])["wallet"].astype(str).str.lower()
        except Exception as e:  # noqa: BLE001
            raise fio.HotStoreReadError(
                f"wallets_in_days: fills {d}.parquet unreadable / missing 'wallet' ({e!r}); "
                f"day is in the requested window -> fail closed") from e
        s = set(w.unique())
        if universe is not None:
            s &= universe
        out |= s
    return out


# --------------------------------------------------------------------------- #
# journey_uid (deterministic primary key)
# --------------------------------------------------------------------------- #


def journey_uid(wallet: str, coin: str, entry_ts, side: str,
                entry_fill_seq=None, entry_fill_tid=0, exit_ts=None, entry_fill_ord=0) -> str:
    """Deterministic, WINDOW-INVARIANT primary key for a journey.

    2026-07-12 COLLISION FIX: ``(wallet, coin, entry_ts, side)`` collides when a wallet opens the SAME
    side on the SAME coin twice in the SAME millisecond (open-long / exit / open-long burst): two live
    journeys hash to one uid and one is hidden in ``load_active_closed``.

    2026-07-16 PHASE 1c — WINDOW INVARIANCE (read-window unlock). The earlier disambiguator embedded
    ``entry_fill_seq`` (GENESIS-RELATIVE fill counter), which forced holders to be read from genesis
    (pinning the daily read window to full history). It is ALSO wrong for a holder re-traced from a
    last_flat point: that journey is re-seeded via CARRY-IN, so its OPENING FILL (and hence seq/tid/ord)
    differs from the from-genesis trace -> different uid (gate A3 caught this). The fix keys the uid on
    WINDOW-STABLE journey CONTENT instead of opening-fill identity: the two colliding same-ms journeys
    close at DIFFERENT times, so ``exit_ts`` disambiguates them and is proven identical across windows
    (field_diffs=0). ``entry_fill_ord`` (same-(coin,ts) ordinal) is a tertiary tiebreak for the residual
    double open+close-in-one-ms burst (both are window-contained CLOSED journeys, so their ord is stable;
    open holders never collide -- one net position per coin+side). journey_uid has ZERO consumers repo-
    wide (store replaces by entry_ts RANGE), so it only needs to be self-consistent + window-invariant."""
    seq = "" if entry_fill_seq is None else str(int(entry_fill_seq))  # retained for schema; NOT in key
    tid = int(entry_fill_tid or 0)
    xt = "OPEN" if (exit_ts is None or (isinstance(exit_ts, float) and exit_ts != exit_ts)) else str(int(exit_ts))
    ordv = int(entry_fill_ord or 0)
    return hashlib.sha256(
        f"{str(wallet).lower()}|{coin}|{int(entry_ts)}|{side}|{xt}|{ordv}".encode()
    ).hexdigest()


def _tag_journey(j: dict, run_id: int, active: bool) -> dict:
    j = dict(j)
    j["journey_uid"] = journey_uid(
        j["wallet"], j["coin"], j["entry_ts"], j["side"],
        j.get("entry_fill_seq"), j.get("entry_fill_tid", 0),
        j.get("exit_ts"), j.get("entry_fill_ord", 0),
    )
    j["run_id"] = int(run_id)
    j["active"] = bool(active)
    return j


# --------------------------------------------------------------------------- #
# action_uid (deterministic, WINDOW-INVARIANT action identity)
# --------------------------------------------------------------------------- #


def action_uid(wallet: str, coin: str, ts, this_fill_ord=0) -> str:
    """Deterministic, WINDOW-INVARIANT primary key for ONE action (one ordered fill event).

    The tracer emits exactly one action row per fill it processes. All fills sharing an exact
    (coin, ts) are present in ANY replay window covering ts (a valid last_flat <= ts), and the
    per-fill guards (spot skip, |signed|<=EPS, non-finite drop) are deterministic, so the same-ts
    ORDINAL ``this_fill_ord`` is identical from-genesis and from-last_flat (same argument as the
    journey_uid Phase-1c disambiguator). Hence ``(wallet, coin, ts, this_fill_ord)`` is unique per
    emitted action within a wallet AND stable across replay windows -> a correct active-view dedup key.

    ``fill_id`` (== tid) is DELIBERATELY EXCLUDED: it is 0/missing on the S3 by-wallet partition, so it
    cannot be the identity, and it is redundant given the same-ts ordinal (it is window-stable when
    present, but adds nothing to uniqueness). The WINDOW-LOCAL columns (event_order, journey_id,
    opening/closing_journey_id) are NEVER part of the key -- they restart per replay window and are
    diagnostic-only. This uid has ZERO external consumers (the store replaces by wallet+ts RANGE); it
    only needs to be self-consistent + window-invariant, exactly like journey_uid."""
    return hashlib.sha256(
        f"{str(wallet).lower()}|{coin}|{int(ts)}|{int(this_fill_ord or 0)}".encode()
    ).hexdigest()


def _tag_action(a: dict, run_id: int, active: bool) -> dict:
    a = dict(a)
    a["action_uid"] = action_uid(a["wallet"], a["coin"], a["ts"], a.get("this_fill_ord", 0))
    a["run_id"] = int(run_id)
    a["active"] = bool(active)
    return a


# --------------------------------------------------------------------------- #
# Actions store: run-partitioned parquet + MEMORY-SAFE active-view reducer
# --------------------------------------------------------------------------- #


def _run_id_of_part(path: str) -> int:
    """``.../run_000123.parquet`` -> 123."""
    return int(Path(path).name[len("run_"):].split(".")[0])


def committed_run_id(state_dir: Path) -> int:
    """Highest run_id whose parts are COMMITTED (published AND checkpointed). The checkpoint is the
    single source of truth and is written LAST (atomically, tmp+rename). A run part on disk with a
    HIGHER run_id is an uncommitted/failed run and MUST be ignored by every reducer
    (non-transactional-publish fail-closed, codex P1 #2). Returns 0 when no checkpoint exists.

    RUN_ID CONVENTION (single documented invariant, codex P2): the two drivers store run_id
    differently, so the checkpoint records WHICH via ``run_id_is_next``:
      * ``run_daily``           -> stores the NEXT run_id to use  (run_id_is_next=True)  -> committed = rid-1
      * ``run_daily_stateful``  -> stores the LAST-written run_id  (run_id_is_next=False) -> committed = rid
    Legacy checkpoints predate the flag; they were ALL written by run_daily's next convention, so the
    default is True (rid-1) -- correct for the live 1c-1f store."""
    cp = load_checkpoint(state_dir)
    if not cp:
        return 0
    rid = int(cp.get("run_id", 1))
    return (rid - 1) if cp.get("run_id_is_next", True) else rid


def _actions_parts_glob(actions_dir: Path, max_run_id: Optional[int] = None) -> list[str]:
    """Real, READABLE, NON-EMPTY action run-parts, optionally capped to the committed run_id.

    Filters out: stray .tmp/.parts staging files; parts with run_id > ``max_run_id`` (uncommitted /
    failed runs -- codex P1 #2); and 0-row or truncated/unreadable parts (a run with zero actions
    writes a schemaless 0-row parquet with no ``action_uid`` column that would break the windowed
    dedup, and a kill mid-write can leave a truncated file -- codex P2 #3). Cheap: run_id from the
    filename, num_rows from parquet metadata (no full read)."""
    out: list[str] = []
    for p in glob.glob(str(Path(actions_dir) / "run_*.parquet")):
        if p.endswith(".tmp") or ".parquet.parts" in p:
            continue
        try:
            rid = _run_id_of_part(p)
        except (ValueError, IndexError):
            continue
        if max_run_id is not None and rid > max_run_id:
            continue   # newer than the committed checkpoint -> uncommitted/failed run, ignore
        try:
            if pq.ParquetFile(p).metadata.num_rows == 0:
                continue   # empty (or the schemaless empty-output) part -> nothing to contribute
        except Exception:  # noqa: BLE001  truncated/unreadable (e.g. killed mid-write) -> skip safely
            continue
        out.append(p)
    return sorted(out)


def _active_actions_sql(parts: list[str], wallets: Optional[set[str]]) -> str:
    """DuckDB SQL that reduces the run-partitioned actions store to its CURRENT active view.

    Per ``action_uid`` keep the highest ``run_id``; within a run ``active=True`` (fresh) beats
    ``active=False`` (tombstone); then keep only active rows. ``union_by_name`` tolerates a column that
    a given run never emitted. Optional wallet filter is pushed DOWN into the scan (bounds the read)."""
    parts_lit = "[" + ",".join("'" + p.replace("'", "''") + "'" for p in parts) + "]"
    where = ""
    if wallets is not None:
        wl = ",".join("'" + str(w).lower().replace("'", "''") + "'" for w in wallets)
        where = f"WHERE lower(wallet) IN ({wl})" if wl else "WHERE 1=0"
    return (
        "SELECT * EXCLUDE (rn) FROM ("
        "  SELECT *, row_number() OVER ("
        "    PARTITION BY action_uid ORDER BY run_id DESC, active DESC) AS rn"
        f"  FROM read_parquet({parts_lit}, union_by_name=true) {where}"
        ") WHERE rn = 1 AND active"
    )


def load_active_actions(actions_dir: Path, wallets: Optional[set[str]] = None,
                        out_path: Optional[str] = None, max_run_id: Optional[int] = None):
    """MEMORY-SAFE reduction of the run-partitioned actions store to the CURRENT active view.

    Analogous to ``load_active_closed`` for journeys, but the actions store is ~86M+ rows so this
    NEVER pandas-concats all parts (that is the banned all-rows materialize). It uses DuckDB, which
    streams the parquet scan + windowed dedup OUT OF CORE with a hard ``memory_limit`` (bounded RAM
    regardless of store size).

      * ``wallets`` -- optional subset; the filter is pushed into the scan so only those wallets are
        read/returned (bounds memory for the incremental tombstone path).
      * ``out_path`` -- when given, the active view is streamed straight to this parquet via DuckDB
        ``COPY`` (fully out-of-core) and the function returns ``None``. Use this for the FULL store.
      * ``max_run_id`` -- ignore run parts newer than this committed run_id (pass
        ``committed_run_id(state_dir)`` so an uncommitted/failed run is never read; codex P1 #2).

    ``wallets=None`` AND ``out_path=None`` is REJECTED: that would ``fetch_df()`` the entire active
    view into pandas RAM (the unbounded materialize this reducer exists to prevent, codex P2 #4). The
    full store MUST use ``out_path`` (COPY) or a bounded ``wallets`` subset."""
    if wallets is None and out_path is None:
        raise ValueError(
            "load_active_actions(wallets=None, out_path=None) would materialize the FULL ~86M-row "
            "active view in pandas RAM (banned). Pass out_path=... to stream the full store via "
            "DuckDB COPY, or wallets=<bounded subset> for an in-RAM slice.")
    import duckdb  # local: only the daily driver / reducer needs it
    parts = _actions_parts_glob(actions_dir, max_run_id=max_run_id)
    if not parts:
        return None if out_path else pd.DataFrame()
    sql = _active_actions_sql(parts, wallets)
    con = duckdb.connect()
    try:
        con.execute(f"PRAGMA memory_limit='{_DUCKDB_MEM_LIMIT_GB}GB'")  # hard RAM cap (codex P2 #5)
        con.execute("PRAGMA threads=2")  # bounded parallelism keeps peak RAM flat
        if out_path:
            Path(out_path).parent.mkdir(parents=True, exist_ok=True)
            con.execute(
                f"COPY ({sql}) TO '{str(out_path).replace(chr(39), chr(39) * 2)}' "
                "(FORMAT PARQUET, COMPRESSION SNAPPY)")
            return None
        return con.execute(sql).fetch_df()
    finally:
        con.close()


def _stream_action_tombstones(actions_dir: Path, replay_start: dict[str, int], run_id: int,
                              writer: ShardedParquetWriter, max_run_id: Optional[int] = None,
                              batch_rows: int = 200_000) -> int:
    """Tombstone prior-ACTIVE actions of the affected wallets by ``(wallet, ts >= replay_start[w])``,
    mirroring the journey tombstone (which uses ``entry_ts >= replay_start``). Because every replay
    starts at a proven all-coin-flat instant, no journey OPEN spans ``replay_start``, so for the vast
    majority of journeys ``action.ts >= replay_start`` iff ``journey.entry_ts >= replay_start`` -- the
    two tombstone sets coincide.

    BOUNDARY (codex P2 #6, claim accuracy): the sets are NOT identical at the exact instant
    ``ts == replay_start``. A journey with ``entry_ts < replay_start`` and ``exit_ts == replay_start``
    is KEPT by the journey reducer (entry_ts < replay_start -> not tombstoned) yet its EXIT action at
    ``ts == replay_start`` IS tombstoned+re-emitted here (ts >= replay_start). That is benign: the
    re-emitted action is byte-identical canonical CONTENT (asserted exactly equal by the acceptance
    test), and ``_compute_last_flat`` merges touching intervals so ``replay_start`` normally lands on a
    genuine all-flat entry_ts rather than mid-position -- so this boundary is rare and content-safe,
    never a lost or duplicated action in the active view.

    MEMORY-SAFE: DuckDB (with a hard ``memory_limit``) joins the active-view against an in-memory
    (wallet, replay_start) map and STREAMS matched rows in Arrow batches into ``writer`` (each
    re-stamped active=False, run_id), never materializing the whole affected-wallet action history
    (which for ~77k holders would be the banned tens-of-millions-row concat). ``max_run_id`` caps the
    scan to committed run parts (codex P1 #2). Returns the tombstone count."""
    import duckdb  # local
    parts = _actions_parts_glob(actions_dir, max_run_id=max_run_id)
    if not parts or not replay_start:
        return 0
    rs_df = pd.DataFrame(
        {"wallet": [str(w).lower() for w in replay_start],
         "_rs": [int(v) for v in replay_start.values()]})
    active_sql = _active_actions_sql(parts, wallets=set(rs_df["wallet"]))
    con = duckdb.connect()
    n = 0
    try:
        con.execute(f"PRAGMA memory_limit='{_DUCKDB_MEM_LIMIT_GB}GB'")  # hard RAM cap (codex P2 #5)
        con.execute("PRAGMA threads=2")
        con.register("rs_map", rs_df)
        q = (f"SELECT av.* FROM ({active_sql}) av JOIN rs_map rs "
             "ON lower(av.wallet) = rs.wallet WHERE av.ts >= rs._rs")
        res = con.execute(q)
        while True:
            batch = res.fetch_df_chunk()  # bounded chunk (out-of-core); empty frame = done
            if batch is None or batch.empty:
                break
            batch = batch.drop(columns=[c for c in ("run_id", "active") if c in batch.columns])
            recs = batch.to_dict("records")
            for rec in recs:
                rec["run_id"] = int(run_id)
                rec["active"] = False
            writer.add_many(recs)
            n += len(recs)
    finally:
        con.close()
    return n


# --------------------------------------------------------------------------- #
# Checkpoint I/O
# --------------------------------------------------------------------------- #


def load_checkpoint(state_dir: Path) -> Optional[dict]:
    p = Path(state_dir) / "checkpoint.json"
    if not p.exists():
        return None
    return json.loads(p.read_text())


def save_checkpoint(state_dir: Path, cp: dict) -> None:
    Path(state_dir).mkdir(parents=True, exist_ok=True)
    p = Path(state_dir) / "checkpoint.json"
    tmp = p.with_suffix(".json.tmp")
    # CODEX P2 #5 (2026-07-16): allow_nan=False -> FAIL LOUD if any carried state value is NaN/Infinity
    # instead of silently writing it (a NaN funding_net_so_far / position would poison all later PnL).
    tmp.write_text(json.dumps(cp, indent=2, sort_keys=True, allow_nan=False))
    tmp.replace(p)


# --------------------------------------------------------------------------- #
# CLOSED store: run-partitioned parquet + active-view reconstruction
# --------------------------------------------------------------------------- #


def _closed_dir(out_dir: Path) -> Path:
    return Path(out_dir) / "closed"


def _write_touched(out_dir: Path, run_id: int, wallets) -> None:
    """CODEX P1 #2 (2026-07-16): emit the AUTHORITATIVE touched-wallet set for this run as a sidecar.
    The closed run-parquet's wallet column under-represents the delta: a wallet that this run only had a
    new OPEN journey for (no close) never appears there, so M3/M4/M5 would reuse stale scores for it. The
    `active`/`late_wallets` set IS every wallet whose journeys/state changed this run -> the true delta.
    Written atomically (tmp + rename) so a mid-write sidecar is never half-read. `.wallets.parquet` next to
    `run_<id>.parquet`; single `wallet` column."""
    p = _closed_dir(out_dir) / f"run_{run_id:06d}.wallets.parquet"
    tmp = p.with_suffix(".parquet.tmp")
    pd.DataFrame({"wallet": sorted({str(w).lower() for w in wallets})}).to_parquet(tmp, index=False)
    tmp.replace(p)


def load_active_closed(out_dir: Path, wallets: Optional[set[str]] = None,
                       max_run_id: Optional[int] = None) -> pd.DataFrame:
    """Reduce the run-partitioned closed store to the CURRENT active view.

    Per ``journey_uid`` keep the row with the highest ``run_id``; within a run,
    ``active=True`` (fresh) wins over ``active=False`` (tombstone). Then keep only
    active rows. Optionally restrict to a wallet subset (bounds memory).

    ``max_run_id`` -- ignore run parts newer than this committed run_id (pass
    ``committed_run_id(state_dir)``); an uncommitted/failed run's parts are then never read, keeping
    the journeys + actions stores consistent with the single-source-of-truth checkpoint (codex P1 #2).
    """
    cdir = _closed_dir(out_dir)
    # EXCLUDE the touched sidecars: `run_*.parquet` also matches `run_<id>.wallets.parquet` (the stateful
    # TOUCHED sidecar), whose schema has no journey_uid/active/run_id -> concatenating it corrupts M3/M5
    # (codex P1). Only real journey partitions here.
    parts = sorted(p for p in glob.glob(str(cdir / "run_*.parquet")) if not p.endswith(".wallets.parquet"))
    if max_run_id is not None:
        _kept = []
        for p in parts:
            try:
                _rid = _run_id_of_part(p)
            except (ValueError, IndexError):
                continue
            if _rid <= max_run_id:
                _kept.append(p)
        parts = _kept
    if not parts:
        return pd.DataFrame()
    frames = []
    for p in parts:
        df = pd.read_parquet(p)
        if wallets is not None and not df.empty:
            df = df[df["wallet"].astype(str).str.lower().isin(wallets)]
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    allrows = pd.concat(frames, ignore_index=True)
    # highest run_id per uid, active=True wins ties -> sort then keep last.
    allrows = allrows.sort_values(["run_id", "active"], kind="mergesort")
    latest = allrows.drop_duplicates("journey_uid", keep="last")
    return latest[latest["active"]].reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Core incremental run
# --------------------------------------------------------------------------- #


def run_daily(
    target_day: Optional[str] = None,
    *,
    state_dir: Path = DEFAULT_STATE_DIR,
    out_dir: Path = DEFAULT_OUT_DIR,
    wallets_file: Optional[str] = None,
    start_day: Optional[str] = None,
    procs: int = 4,
    lookback_days: int = 3,
    flush_rows: int = 100_000,
    mem_soft_gb: float = 12.0,
    actions_dir: Path = DEFAULT_ACTIONS_DIR,
) -> dict:
    """Advance the daily-incremental journeys store to ``target_day``.

    OPTION A (2026-07-22): ALSO persist the canonical per-ACTION stream the tracer already computes,
    into ``actions_dir`` run-partitioned exactly like the journeys closed/ store. Its correctness
    rests on the SAME invariant as the journeys store: every replay starts at a proven all-coin-flat
    instant, so the re-traced actions are complete from a zero seed. Because no journey OPEN spans a
    flat ``replay_start``, ``action.ts >= replay_start`` matches ``journey.entry_ts >= replay_start``
    for the vast majority of journeys -- the action tombstones (by wallet+ts range) and journey
    tombstones (by wallet+entry_ts range) coincide. The ONE boundary where they differ is a kept
    journey with ``entry_ts < replay_start`` and ``exit_ts == replay_start``: its exit action at
    ``ts == replay_start`` is tombstoned+re-emitted while the journey row is not, which is benign
    (the re-emitted action is byte-identical canonical content -- asserted by the acceptance test).
    See ``_stream_action_tombstones`` for the full boundary note. WINDOW-LOCAL
    columns (event_order, journey_id, opening/closing_journey_id) restart per replay window and are
    DIAGNOSTIC-ONLY: the active-view dedup keys on ``action_uid`` (window-invariant content), never on
    them. The journeys store output is UNCHANGED by this (actions are an additive sidecar store).

    Returns a summary dict. Raises SystemExit(1) on any worker error (fail closed)."""
    state_dir = Path(state_dir)
    out_dir = Path(out_dir)
    actions_dir = Path(actions_dir)
    _closed_dir(out_dir).mkdir(parents=True, exist_ok=True)
    (out_dir / "open_snapshot").mkdir(parents=True, exist_ok=True)
    actions_dir.mkdir(parents=True, exist_ok=True)

    hot_days = hot_available_days()
    if not hot_days:
        raise SystemExit("no hot fills days available")
    earliest_hot = hot_days[0]
    start_day = start_day or earliest_hot
    target = target_day or hot_days[-1]
    if target not in hot_days:
        raise SystemExit(f"target_day {target} not in hot store (have {hot_days[0]}..{hot_days[-1]})")
    if target < start_day:
        raise SystemExit(f"target_day {target} < start_day {start_day}")

    universe: Optional[set[str]] = None
    if wallets_file:
        with open(wallets_file) as fh:
            universe = {l.strip().lower() for l in fh if l.strip() and not l.startswith("#")}
        logger.info(f"universe filter: {len(universe):,} wallets from {wallets_file}")

    window_days = [d for d in hot_days if start_day <= d <= target]
    target_end = day_end_ms(target)
    start_ms = day_start_ms(start_day)

    cp = load_checkpoint(state_dir)
    first_run = cp is None
    run_id = 1 if first_run else int(cp["run_id"])
    # COMMITTED runs are those with run_id <= (checkpoint run_id - 1); the current `run_id` is
    # uncommitted until this call writes the checkpoint LAST. Reducers cap their scan here so a
    # prior crashed run's leftover parts (run_id == this run_id, checkpoint not advanced) are ignored
    # -> journeys + actions stay consistent with the single-source-of-truth checkpoint (codex P1 #2).
    committed = run_id - 1

    # OPTION A migration guard (codex P1 #1): a journeys checkpoint that predates the actions feature
    # (NOT a first_run AND no ``actions_bootstrapped`` marker) means the canonical actions store was
    # never built. An incremental run here would persist actions ONLY for today's affected wallets,
    # silently leaving ALL history absent while the store LOOKS valid. FAIL LOUD.
    # The trigger is the ABSENCE OF THE CHECKPOINT MARKER, NOT an empty glob (codex P2): a legitimately
    # committed-but-EMPTY store -- a run that produced zero qualifying actions, e.g. a filtered universe
    # -- has the marker set and MUST NOT false-trigger. run_daily writes the marker atomically with the
    # checkpoint the first time it commits actions (a full first_run), so marker-present <=> bootstrapped.
    if not first_run and not cp.get("actions_bootstrapped"):
        raise SystemExit(
            "OPTION A actions store was NEVER bootstrapped (no 'actions_bootstrapped' checkpoint marker) "
            f"but the journeys checkpoint is already at run_id={run_id} (watermark {cp.get('watermark_day')}). "
            "An INCREMENTAL run would persist actions only for today's affected wallets, silently leaving "
            "all history absent. BOOTSTRAP first via a full first_run replay from genesis (rebuilds "
            "journeys AND actions together, and sets the marker):\n"
            f"  rm -rf '{state_dir}' '{_closed_dir(out_dir)}' '{out_dir}/open_snapshot' '{actions_dir}'\n"
            "then re-run run_daily under scripts/mem_safe_run.sh. (Refusing to write a partial actions "
            "store.)")

    # ---- determine affected wallets + per-wallet replay_start ---------------
    replay_start: dict[str, int] = {}
    # Manifest stores a CONTENT fingerprint (size+mtime+rowcount), NOT rowcount alone, so a late
    # historical rewrite with the SAME rowcount but CHANGED values is still detected (FIX 3).
    cur_manifest = {d: day_fingerprint(d) for d in window_days}

    if first_run:
        watermark = None
        affected = wallets_in_days(window_days, universe)
        for w in affected:
            replay_start[w] = start_ms
        logger.info(f"FIRST RUN: {len(affected):,} wallets replay from {start_day}")
    else:
        watermark = cp["watermark_day"]
        prev_manifest = cp.get("fills_manifest", {})
        prev_open = cp.get("open_positions", [])
        open_pos_wallets = {str(o["wallet"]).lower() for o in prev_open}

        new_days = [d for d in window_days if d > watermark]
        new_day_start = day_start_ms(new_days[0]) if new_days else None

        # historical changes: lookback window (<= watermark) + manifest rowcount diffs (<= watermark)
        lb_days = [d for d in window_days if d <= watermark][-max(0, lookback_days):]
        diff_days = [
            d for d in window_days
            if d <= watermark and cur_manifest.get(d) != prev_manifest.get(d)
        ]
        hist_days = sorted(set(lb_days) | set(diff_days))
        hist_wallets = wallets_in_days(hist_days, universe)
        new_wallets = wallets_in_days(new_days, universe)
        if universe is not None:
            open_pos_wallets &= universe

        # HOLDER-REPLAY OPTIMIZATION (design 2026-07-12-m2-holder-replay-optimization):
        # a wallet HOLDING at the watermark used to replay from start_day EVERY run (its only
        # proven-flat instant), re-tracing full history nightly. Instead replay a holder from its
        # per-wallet last_all_flat_ts -- the LATEST instant <= watermark where it held ZERO net
        # position across ALL coins (start of its last coverage-run). That is a PROVEN-flat instant
        # so carry-in seeds to zero and the replay is BATCH-IDENTICAL to a start_day replay.
        #   * holder AND NOT hist_wallet: replay from cp_last_flat[w] (fallback start_ms).
        #   * holder AND hist_wallet: start_ms (conservative -- a late/lookback change may predate
        #     last_flat, so re-derive from a fully-proven-flat origin).
        #   * hist-only / new-day-fast: UNCHANGED.
        # Fail SAFE on any ambiguity: earlier replay (start_ms).
        full_wallets = set(open_pos_wallets) | set(hist_wallets)
        cp_last_flat = cp.get("last_flat_ts", {}) or {}
        holder_only = set(open_pos_wallets) - set(hist_wallets)
        # PHASE 1d (2026-07-16): hist_wallets used to blanket-replay from GENESIS every run (the 3-day
        # unconditional lookback + manifest diffs flag ~10k wallets/day, each re-reading full history --
        # the dominant remaining cost after Phase 1c). A hist_wallet whose proven last_flat instant lies
        # BEFORE the earliest changed/lookback day can safely replay from that last_flat: every hist
        # change is AFTER it, so re-tracing [last_flat..target] re-reads all changed days (late fills
        # included) from a true zero seed -> BATCH-IDENTICAL. If last_flat falls INSIDE the lookback
        # window (a change may move its flat point) or is absent, fail safe to start_ms. Rescues the
        # holders-with-predating-last_flat subset now; pure non-holders + inside-lookback holders still
        # replay from genesis pending Phase 1e (per-wallet flat-floor from the store).
        earliest_hist_start = day_start_ms(min(hist_days)) if hist_days else target_end
        for w in full_wallets:
            if w in holder_only:
                lf = cp_last_flat.get(w)
                if lf is None:
                    replay_start[w] = start_ms          # no proven flat point yet -> conservative
                else:
                    lf = int(lf)
                    # proven-flat instant must lie inside the window; else fail safe to start_ms.
                    replay_start[w] = lf if (start_ms <= lf <= target_end) else start_ms
            else:
                # hist_wallet (incl. holder-and-hist): bound by last_flat ONLY when it predates the
                # earliest hist day (so no lookback/manifest change can invalidate the flat seed).
                lf = cp_last_flat.get(w)
                if lf is not None and start_ms <= int(lf) <= earliest_hist_start:
                    replay_start[w] = int(lf)
                else:
                    replay_start[w] = start_ms
        # NEW-day fast path: flat-at-watermark wallets active only on new days.
        for w in (new_wallets - full_wallets):
            replay_start[w] = new_day_start if new_day_start is not None else start_ms
        affected = set(replay_start)
        n_holder_lastflat = sum(1 for w in holder_only if replay_start[w] > start_ms)
        logger.info(
            f"INCREMENTAL {watermark} -> {target}: new_days={new_days} hist_days={hist_days} "
            f"| affected={len(affected):,} (full={len(full_wallets):,} "
            f"holder_lastflat={n_holder_lastflat:,} "
            f"newday_fast={len(affected)-len(full_wallets):,})"
        )

    if not affected:
        logger.info("no affected wallets; advancing watermark only")

    # ---- tombstones for affected wallets (replace-by-range) -----------------
    prior_active = (load_active_closed(out_dir, wallets=affected, max_run_id=committed)
                    if not first_run else pd.DataFrame())

    # PHASE 1e (2026-07-16): pure-non-holder hist_wallets (flat at watermark, flagged ONLY by the 3-day
    # late-fill lookback / manifest diff, no stored last_flat) still replay from GENESIS after Phase 1d.
    # If such a wallet is PROVEN FLAT at earliest_hist_start (no materialized journey spans that instant),
    # it can replay from that day boundary with a zero seed -- IDENTICAL mechanism to the newday_fast path
    # (flat-at-watermark wallets replay from new_day_start). Every hist change is AFTER the boundary, so
    # [earliest_hist_start..target] re-reads all changed days from a true flat seed -> BATCH-IDENTICAL.
    # Holders are EXCLUDED (their open position may span the boundary); spanning wallets stay at genesis
    # (fail safe). Reuses prior_active (no extra store read). Read window for these collapses to the
    # lookback window instead of full history.
    if (not first_run) and (not prior_active.empty) and hist_days:
        pure_hist_genesis = {w for w in (set(hist_wallets) - set(open_pos_wallets))
                             if replay_start.get(w) == start_ms}
        if pure_hist_genesis:
            _paw = prior_active["wallet"].astype(str).str.lower()
            _ent = prior_active["entry_ts"].astype("int64")
            _ex = prior_active["exit_ts"].fillna(2**63 - 1).astype("int64")
            span_mask = (_ent < earliest_hist_start) & (_ex > earliest_hist_start)
            spanning = set(_paw[span_mask])          # holding across the boundary -> NOT flat there
            n_1e = 0
            for w in pure_hist_genesis:
                if w not in spanning:                # proven flat at earliest_hist_start
                    replay_start[w] = earliest_hist_start
                    n_1e += 1
            logger.info(f"PHASE 1e: {n_1e:,}/{len(pure_hist_genesis):,} pure-hist non-holders flat-at-"
                        f"{min(hist_days)} -> replay from lookback boundary (was genesis)")

    # PHASE 1f (2026-07-16): the HOLDER mirror of 1e. A holder-AND-hist wallet whose CURRENT open
    # position(s) ALL opened AFTER earliest_hist_start, and with no closed journey spanning that instant,
    # held ZERO net position at earliest_hist_start -> it was FLAT there and can replay from the lookback
    # boundary with a zero seed (batch-identical), not genesis. The open journey is NOT in prior_active, so
    # spanning-by-open is checked via cp open_positions' open_ts_ms (min over the wallet's open coins).
    # Fail safe to genesis if any open position predates the boundary OR a closed journey spans it. This
    # clears the inside-lookback-holder residual that 1d leaves at genesis.
    if (not first_run) and hist_days:
        holder_hist_genesis = {w for w in (set(hist_wallets) & set(open_pos_wallets))
                               if replay_start.get(w) == start_ms}
        if holder_hist_genesis:
            min_open: dict[str, int] = {}
            for o in prev_open:
                w = str(o["wallet"]).lower()
                if w in holder_hist_genesis:
                    t = int(o.get("open_ts_ms", 0) or 0)
                    min_open[w] = min(min_open.get(w, t), t)
            spanning_f: set = set()
            if not prior_active.empty:
                _pawf = prior_active["wallet"].astype(str).str.lower()
                _entf = prior_active["entry_ts"].astype("int64")
                _exf = prior_active["exit_ts"].fillna(2**63 - 1).astype("int64")
                spanning_f = set(_pawf[(_entf < earliest_hist_start) & (_exf > earliest_hist_start)])
            n_1f = 0
            for w in holder_hist_genesis:
                mo = min_open.get(w)
                if mo is not None and mo >= earliest_hist_start and w not in spanning_f:
                    replay_start[w] = earliest_hist_start
                    n_1f += 1
            logger.info(f"PHASE 1f: {n_1f:,}/{len(holder_hist_genesis):,} holder-hist opened-after-"
                        f"{min(hist_days)} flat-at-boundary -> bounded (was genesis)")

    tombstones: list[dict] = []
    if not prior_active.empty:
        pa = prior_active.copy()
        pa["_w"] = pa["wallet"].astype(str).str.lower()
        pa["_rs"] = pa["_w"].map(replay_start)
        kill = pa[pa["entry_ts"] >= pa["_rs"]]
        for rec in kill.drop(columns=["_w", "_rs"]).to_dict("records"):
            rec["run_id"] = int(run_id)
            rec["active"] = False
            tombstones.append(rec)
    logger.info(f"tombstoning {len(tombstones):,} prior-active closed rows")

    # ---- run affected wallets through the UNCHANGED tracer (streamed) -------
    Path(out_dir, "closed").mkdir(parents=True, exist_ok=True)
    run_path = _closed_dir(out_dir) / f"run_{run_id:06d}.parquet"
    cw = ShardedParquetWriter(str(run_path), flush_rows=flush_rows)
    cw.add_many(tombstones)

    # OPTION A: the actions run-part shares this run_id. Tombstone prior-active actions by
    # (wallet, ts >= replay_start[w]) -- the exact mirror of the journey tombstone -- STREAMING the
    # matched rows in bounded batches (never a full affected-wallet action-history concat), then the
    # freshly emitted actions are appended as active=True in _consume below. On first_run there is no
    # prior actions store, so this is a no-op.
    actions_run_path = actions_dir / f"run_{run_id:06d}.parquet"
    aw = ShardedParquetWriter(str(actions_run_path), flush_rows=flush_rows)
    n_action_tombstones = 0
    if not first_run and affected:
        n_action_tombstones = _stream_action_tombstones(actions_dir, replay_start, run_id, aw,
                                                         max_run_id=committed)
    logger.info(f"tombstoning {n_action_tombstones:,} prior-active action rows")

    emitted_open: list[dict] = []
    errors: list[tuple] = []
    n_closed = 0
    n_actions = 0
    # Per-HOLDER journey intervals emitted THIS run (kept small, Rule-8: (entry, exit_or_None,
    # n_carry_in) tuples only -- not full rows). Populated ONLY for wallets that end this run holding
    # an open position (a holder); used to compute last_all_flat_ts. Non-holders are discarded.
    journey_intervals: dict[str, list[tuple]] = {}

    def _consume(r: dict) -> None:
        nonlocal n_closed, n_actions
        if "error" in r:
            errors.append((r["wallet"], r["error"]))
            return
        # OPTION A: persist EVERY action the tracer emitted for this wallet (open AND closed journeys).
        # All have ts >= replay_start[w] (fills are pre-filtered to the replay window), so they exactly
        # cover the tombstoned range. Streamed to disk (Rule-8) via the sharded actions writer.
        acts = r.get("actions") or []
        if acts:
            aw.add_many([_tag_action(a, run_id, True) for a in acts])
            n_actions += len(acts)
        fresh_closed = []
        ivals: list[tuple] = []
        has_open = False
        for j in r.get("journeys", []):
            nci = int(j.get("n_carry_in_seeds", 0) or 0)
            if j.get("open_at_window_end"):
                emitted_open.append(_tag_journey(j, run_id, True))
                ivals.append((int(j["entry_ts"]), None, nci))
                has_open = True
            else:
                fresh_closed.append(_tag_journey(j, run_id, True))
                ivals.append((int(j["entry_ts"]), int(j["exit_ts"]), nci))
        n_closed += len(fresh_closed)
        cw.add_many(fresh_closed)
        if has_open:
            journey_intervals[str(r.get("wallet", "")).lower()] = ivals

    if affected:
        install_memory_guard(soft_gb=mem_soft_gb, label="m02-daily")
        MAXTASKS = 40
        # FIX 5 (2026-07-12): a worker memory-guard os._exit(137) can break the Pool WITHOUT reaching
        # the per-wallet fail-closed path. Wrap the whole pool-consume in except BaseException ->
        # abort the writer + write an error manifest + raise SystemExit(1), so the checkpoint is NEVER
        # advanced on any pool-level crash (Rule-8 fail-closed).
        try:
            if first_run:
                # FIX 1 (2026-07-12): BULK / first-run catch-up. The per-wallet loaders glob ALL
                # day-files and filter to ONE wallet on every call -> O(wallets x days) full-day
                # scans (the ~15k catch-up ran 73 min with zero output). Read each hot day-file ONCE
                # via the grouped loader, then trace per wallet, streaming journeys out. All first-run
                # wallets share replay_start == start_ms, so a single grouped window is correct.
                #
                # FIX 1b (2026-07-12): CHUNK the bulk universe. A SINGLE grouped-load of all ~15k
                # wallets normalizes ~17M fill rows serially in the main process BEFORE any worker
                # gets work (observed: 25+ min in one groupby/normalize, zero output). Process in
                # bounded wallet-chunks: each chunk grouped-loads + normalizes only ~CHUNK wallets
                # (fast), streams journeys immediately, and keeps main-process RAM bounded. Day-files
                # are re-read per chunk but the OS page-cache keeps them warm, so reads stay cheap.
                # P0 memory safety: bound CHUNK by the FULL window (kills the ~34GB parent balloon) and cap
                # procs by an aggregate budget (abort-before-start if the box can't fit it).
                CHUNK = _bounded_chunk(target_end - start_ms)
                _b = _budget_or_serial(procs, parent_gb=_parent_gb_for(min(CHUNK, len(affected)), target_end - start_ms))
                wl_sorted = sorted(affected)
                n_chunks = (len(wl_sorted) + CHUNK - 1) // CHUNK
                logger.info(f"BULK catch-up {len(wl_sorted):,} wallets in {n_chunks} chunks of {CHUNK} "
                            f"over [{start_day}..{target}] (mem-bounded: procs {procs}->{_b.procs if _b else 'serial'}, "
                            f"parent~{_DEFAULT_PARENT_GB}GB/chunk; day-oriented page-cache-warm re-read)")
                with _worker_pool(_b, mem_soft_gb, MAXTASKS) as pool:
                    for ci in range(0, len(wl_sorted), CHUNK):
                        chunk = wl_sorted[ci:ci + CHUNK]
                        logger.info(f"BULK chunk {ci // CHUNK + 1}/{n_chunks}: grouped-load {len(chunk)} wallets")
                        gf, gfund = fio.load_grouped_fills_funding(set(chunk), start_ms, target_end)

                        def _chunk_tasks(chunk=chunk, gf=gf, gfund=gfund):
                            # POP as we dispatch so the chunk's grouped dict shrinks (bounded RAM).
                            for w in chunk:
                                yield (w, gf.pop(w, []), gfund.pop(w, []), target_end)

                        for r in pool.imap(m02.process_wallet_preloaded, _chunk_tasks()):
                            _consume(r)
            else:
                # FIX (2026-07-13, issue #1 = DOMINANT ~10hr driver): route the INCREMENTAL branch
                # through the SAME bulk grouped-loader the first_run/catch-up path already uses (each
                # day-file read ONCE), instead of process_wallet -> load_wallet_fills PER WALLET, which
                # globs+reads ALL 33 hot day-files for every wallet (measured ~18s/wallet -> ~10hr for
                # 11.6k affected, non-viable). The ONLY difference vs first_run is a PER-WALLET
                # replay_start[w] (first_run has a uniform start_ms). So:
                #   1. superset_start = min(replay_start.values())  (= start_ms whenever a holder is
                #      present, the common case).
                #   2. Bulk grouped-load the affected set over [superset_start, target_end], CHUNKED
                #      exactly like first_run (CHUNK=1500, page-cache-warm re-read, bounded RAM).
                #   3. For EACH wallet SLICE its preloaded fills/funding to time >= replay_start[w],
                #      then RE-RUN order_wallet_fills_causally so fill_seq (== event_order ==
                #      entry_fill_seq, which drives journey_uid) is 0-based over [replay_start[w],
                #      target_end]. That reproduces EXACTLY fio.load_wallet_fills(w, replay_start[w],
                #      target_end) (same deduped fill SET -> same causal order -> same 0-based fill_seq;
                #      duplicates share a time so they never straddle rs), so
                #      process_wallet_preloaded output is BYTE-IDENTICAL to
                #      process_wallet((w, replay_start[w], target_end, False)) -- the prior behaviour.
                # Tombstone / manifest / fail-closed / open_snapshot / checkpoint logic = UNCHANGED.
                #
                # HOLDER-REPLAY OPT (2026-07-12): a holder replayed from its last_all_flat point
                # (replay_start > start_ms) must keep the GLOBAL, start_ms-based fill_seq, NOT a
                # 0-based-over-[rs,target] one. journey_uid embeds entry_fill_seq (== fill_seq of the
                # opening fill), so a slice-local fill_seq would give the SAME journey a DIFFERENT uid
                # than a from-start_ms batch -> only_batch/only_incr diffs even though every content
                # field is identical. To stay batch-identical we (a) force the grouped load to START at
                # start_ms for these wallets so gf[w] is causally ordered from genesis, and (b) slice
                # WITHOUT re-basing fill_seq for them. The wallet is PROVEN flat at replay_start, so the
                # trace over [replay_start, target] with global fill_seq reproduces exactly the tail of
                # the from-start_ms batch (identical fills, order, and event_order). Every OTHER wallet
                # keeps the prior 0-based path (rs == start_ms => 0-based == global; new-day fast path
                # unchanged).
                # PHASE 1c (2026-07-16) — THE READ-WINDOW UNLOCK. journey_uid is now WINDOW-INVARIANT
                # (keyed on window-stable journey CONTENT: entry_ts + exit_ts + same-ts ordinal, NOT the
                # genesis-relative fill_seq; see journey_uid docstring + gate A3). A holder re-traced from
                # its last_flat point (re-seeded via carry-in) therefore yields the SAME uid as the from-
                # genesis batch, so we no longer force superset_start=start_ms. Every wallet reads only
                # [replay_start[w]..hi] and is re-ordered 0-based over that slice (journey MATH depends on
                # causal ORDER, not absolute seq; uid no longer uses seq). PHASE 1b: sort affected by
                # replay_start so each CHUNK's read window = [min(replay_start over chunk)..hi] stays tight
                # (one old-flat holder can't stretch a whole chunk back to genesis).
                # P0 memory safety: size CHUNK for the WORST-CASE (oldest replay_start) window in the batch so
                # even old-flat holders that stretch back toward genesis stay parent-bounded; cap procs.
                _oldest = min(replay_start.values()) if replay_start else target_end
                CHUNK = _bounded_chunk(target_end - _oldest)
                _b = _budget_or_serial(procs, parent_gb=_parent_gb_for(min(CHUNK, len(affected)), target_end - _oldest))
                wl_sorted = sorted(affected, key=lambda w: (replay_start[w], w))
                n_chunks = (len(wl_sorted) + CHUNK - 1) // CHUNK
                logger.info(f"INCREMENTAL BULK {len(wl_sorted):,} wallets in {n_chunks} chunks of {CHUNK} "
                            f"over PER-CHUNK bounded windows [min(last_flat)..{target_end}] "
                            f"(mem-bounded: procs {procs}->{_b.procs if _b else 'serial'}; replay_start-sorted; read once/chunk)")
                with _worker_pool(_b, mem_soft_gb, MAXTASKS) as pool:
                    for ci in range(0, len(wl_sorted), CHUNK):
                        chunk = wl_sorted[ci:ci + CHUNK]
                        chunk_lo = min(replay_start[w] for w in chunk)   # tight per-chunk read window
                        logger.info(f"INCR chunk {ci // CHUNK + 1}/{n_chunks}: grouped-load {len(chunk)} "
                                    f"wallets over [{chunk_lo}..{target_end}]")
                        gf, gfund = fio.load_grouped_fills_funding(set(chunk), chunk_lo, target_end)

                        def _chunk_tasks(chunk=chunk, gf=gf, gfund=gfund):
                            # POP as we dispatch so the chunk's grouped dict shrinks (bounded RAM).
                            for w in chunk:
                                rs = replay_start[w]
                                # gf[w] causally ordered over [chunk_lo, target_end]; slice to the wallet's
                                # own replay_start and RE-ORDER 0-based over [rs, target_end] (byte-identical
                                # fill SET + causal order to load_wallet_fills(w, rs, target_end)). uid is
                                # window-invariant so no global-seq special case is needed.
                                wf = [f for f in gf.pop(w, []) if int(f["time"]) >= rs]
                                wf = fio.order_wallet_fills_causally(wf)
                                wfund = [x for x in gfund.pop(w, []) if int(x["time"]) >= rs]
                                yield (w, wf, wfund, target_end)

                        for r in pool.imap(m02.process_wallet_preloaded, _chunk_tasks()):
                            _consume(r)
        except SystemExit:
            raise
        except BaseException as e:  # noqa: BLE001  fail-closed on ANY pool crash (incl. worker os._exit)
            cw.abort()
            aw.abort()   # OPTION A: never leave a half-written actions run-part on a fail-closed path
            manifest = str(run_path.with_suffix(".errors.json"))
            try:
                Path(manifest).write_text(json.dumps(
                    {"pool_error": repr(e), "n_affected": len(affected),
                     "n_wallet_errors": len(errors)}, indent=2))
            except Exception:  # noqa: BLE001
                pass
            logger.error(f"POOL crash ({e!r}) -> fail closed. manifest={manifest}. checkpoint NOT advanced.")
            raise SystemExit(1)

    if errors:
        cw.abort()
        aw.abort()   # OPTION A: discard this run's actions part too (checkpoint is NOT advanced)
        manifest = str(run_path.with_suffix(".errors.json"))
        Path(manifest).write_text(json.dumps(
            {"n_errors": len(errors), "n_affected": len(affected),
             "errors": [{"wallet": w, "error": e} for w, e in errors]}, indent=2))
        logger.error(f"{len(errors)}/{len(affected)} wallet ERRORS -> fail closed. manifest={manifest}. "
                     f"first: {errors[:10]}. checkpoint NOT advanced.")
        raise SystemExit(1)

    cw.close()
    n_actions_written = aw.close()   # OPTION A: stitch this run's actions part (tombstones + fresh)

    # ---- per-wallet last_all_flat_ts (holder-replay optimization) -----------
    # For each CURRENT holder compute the start of its LAST coverage-run over its COMPLETE journey
    # interval set = kept prior-active CLOSED journeys (entry_ts < replay_start[w], NOT tombstoned)
    # PLUS every journey emitted this run. That instant is the latest all-flat point <= target_end;
    # next run the holder replays from there (batch-identical). Only holders (open journey) need it.
    new_last_flat: dict[str, int] = {}
    if journey_intervals:
        holders = set(journey_intervals)
        prior_by_wallet: dict[str, list[tuple]] = {}
        if (not prior_active.empty
                and {"entry_ts", "exit_ts", "n_carry_in_seeds"} <= set(prior_active.columns)):
            paw = prior_active["wallet"].astype(str).str.lower()
            mask = paw.isin(holders)
            if bool(mask.any()):
                sub = prior_active[mask]
                for w, e, x, nci in zip(paw[mask], sub["entry_ts"],
                                        sub["exit_ts"], sub["n_carry_in_seeds"]):
                    rs = int(replay_start.get(w, start_ms))
                    if int(e) < rs:   # NOT tombstoned this run -> a kept materialized journey
                        prior_by_wallet.setdefault(w, []).append((int(e), int(x), int(nci or 0)))
        for w, ivals in journey_intervals.items():
            new_last_flat[w] = _compute_last_flat(
                ivals + prior_by_wallet.get(w, []), start_ms, target_end)

    # ---- rewrite open_snapshot: carry-forward unaffected + fresh open -------
    _rewrite_open_snapshot(out_dir, cp, affected, emitted_open)

    # ---- update checkpoint ---------------------------------------------------
    new_open_positions = _merge_open_positions(cp, affected, emitted_open)
    new_cp = {
        "watermark_day": target,
        "start_day": start_day,
        "open_positions": new_open_positions,
        # last_all_flat_ts per CURRENT holder (== next run's holders). Non-holders omitted (bounded).
        "last_flat_ts": new_last_flat,
        "fills_manifest": {**(cp.get("fills_manifest", {}) if cp else {}), **cur_manifest},
        "run_id": run_id + 1,
        # RUN_ID CONVENTION: run_daily stores the NEXT run_id -> committed_run_id() = run_id - 1 (codex P2).
        "run_id_is_next": True,
        # OPTION A bootstrap marker (codex P2): set the FIRST time run_daily commits actions (a full
        # first_run) and preserved every run thereafter. Its presence is the sole signal that the
        # canonical actions store was built; the migration guard fires on its ABSENCE, so a committed-
        # but-EMPTY actions store (zero qualifying actions) never false-triggers. Once True, stays True.
        "actions_bootstrapped": bool(first_run or (cp and cp.get("actions_bootstrapped"))),
        "universe_file": wallets_file,
        "updated_utc": pd.Timestamp.utcnow().isoformat(),
    }
    save_checkpoint(state_dir, new_cp)
    # Introspection for the equivalence gate (prod cost nil).
    _LAST_REPLAY_START.clear()
    _LAST_REPLAY_START.update(replay_start)

    logger.info(
        f"run {run_id} done: target={target} affected={len(affected):,} "
        f"closed_fresh={n_closed:,} tombstones={len(tombstones):,} "
        f"open={len(emitted_open):,} actions_fresh={n_actions:,} "
        f"action_tombstones={n_action_tombstones:,} -> {run_path.name}"
    )
    return {
        "run_id": run_id, "target": target, "affected": len(affected),
        "closed_fresh": n_closed, "tombstones": len(tombstones),
        "open": len(emitted_open), "run_path": str(run_path),
        "actions_fresh": n_actions, "action_tombstones": n_action_tombstones,
        "actions_written": n_actions_written, "actions_run_path": str(actions_run_path),
    }


def _init_worker(mem_soft_gb: float) -> None:
    install_memory_guard(soft_gb=mem_soft_gb, label=f"m02-daily-worker-{os.getpid()}")


def _compute_last_flat(intervals: list[tuple], start_ms: int, target_end: int) -> int:
    """Latest instant <= target_end at which a wallet held ZERO net position across ALL coins.

    ``intervals``: the wallet's COMPLETE journey set as ``(entry_ts, exit_ts_or_None, n_carry_in)``
    tuples. Each journey covers ``[entry_ts, exit_ts)`` (closed) or ``[entry_ts, target_end]`` (still
    open). A CARRY-IN journey (n_carry_in > 0) opened BEFORE its truncated entry_ts, so it was NOT
    flat there; extend its interval start back to ``start_ms`` so it never fabricates a false flat.

    The wallet is all-flat exactly OUTSIDE the union of these intervals. Merge them (touching
    intervals, next.start == cur.end, merge -> no flat instant between a close and a same-ms reopen:
    fail safe). The start of the LAST merged run is the latest instant where coverage drops to 0
    scanning backward from target_end -- the last all-flat point. Replaying from there tombstones +
    re-traces exactly the still-holding cluster (entry_ts >= that start) from a true zero seed, while
    every earlier journey stays materialized and untouched -> BATCH-IDENTICAL.

    Falls back to ``start_ms`` when the last run reaches back to (or before) start_ms -- i.e. coverage
    is continuous to the watermark (never flat in-window) or a carry-in position reaches the watermark.
    """
    if not intervals:
        return int(start_ms)
    segs: list[tuple[int, int]] = []
    for e, x, nci in intervals:
        s = int(start_ms) if (nci and int(nci) > 0) else int(e)
        end = int(target_end) if x is None else int(x)
        if end < s:
            end = s
        segs.append((s, end))
    segs.sort()
    cs, ce = segs[0]
    for s, e in segs[1:]:
        if s <= ce:                 # overlap OR touch -> extend current run (no proven flat gap)
            if e > ce:
                ce = e
        else:                       # gap -> a new run starts; only the LAST run's start matters
            cs, ce = s, e
    # cs = start of the last coverage-run. If it reaches start_ms, no in-window proven flat -> start_ms.
    return int(start_ms) if cs <= start_ms else int(cs)


def _merge_open_positions(cp: Optional[dict], affected: set[str], emitted_open: list[dict]) -> list[dict]:
    """New open_positions = prior (for UNaffected wallets) + freshly emitted open journeys."""
    out: list[dict] = []
    if cp:
        for o in cp.get("open_positions", []):
            if str(o["wallet"]).lower() not in affected:
                out.append(o)
    for j in emitted_open:
        out.append({"wallet": str(j["wallet"]).lower(), "coin": j["coin"],
                    "open_ts_ms": int(j["entry_ts"])})
    return out


def _rewrite_open_snapshot(out_dir: Path, cp: Optional[dict], affected: set[str],
                           emitted_open: list[dict]) -> None:
    """open_snapshot = carry-forward prior open rows for UNaffected wallets + fresh open rows."""
    snap = Path(out_dir) / "open_snapshot" / "open_snapshot.parquet"
    carry = pd.DataFrame()
    if snap.exists():
        prior = pd.read_parquet(snap)
        if not prior.empty:
            carry = prior[~prior["wallet"].astype(str).str.lower().isin(affected)]
    sw = ShardedParquetWriter(str(snap), flush_rows=1_000_000)
    if not carry.empty:
        sw.add_many(carry.to_dict("records"))
    sw.add_many(emitted_open)
    sw.close()


# --------------------------------------------------------------------------- #
# PHASE 2 — STATEFUL incremental driver (carry per-holder open-state; feed only new-day fills)
# Core mechanism proven row-identical to batch on 37 real wallets / 1,548 journeys
# (projects/quant/v15/2026-07-16-phase2-stateful-core-proven). This is the plumbing around it.
# --------------------------------------------------------------------------- #


def _carry_open_state(final_state: dict) -> Optional[dict]:
    """Keep ONLY currently-OPEN coins' tracer state for checkpoint carry (flat coins re-seed fresh from
    startPosition=0 on next trade -> correct + bounded). JSON-serializable (ints/floats/str/bool/None)."""
    cs = (final_state or {}).get("coin_state", {}) or {}
    cjc = (final_state or {}).get("coin_journey_counter", {}) or {}
    csf = (final_state or {}).get("coin_seen_first_fill", {}) or {}
    # Carry the FULL per-coin state (all coins the wallet has traded, open AND flat) so a resumed run is
    # EXACTLY equivalent to the continuous batch: a coin that went flat then re-trades keeps its
    # coin_seen_first_fill / journey counter / post-close state, so it does not take the first_fill/carry-in
    # path and split the journey (the gate caught that on a hyper-active xyz wallet, 2026-07-16). Bounded per
    # wallet by the number of distinct coins it has traded (perp universe). Only wallets that were ever active
    # carry state; a fully-flat-never-traded wallet returns None.
    if not cs and not cjc and not csf:
        return None
    return {
        "coin_state": {str(c): st for c, st in cs.items()},
        "coin_journey_counter": {str(c): int(v) for c, v in cjc.items()},
        "coin_seen_first_fill": {str(c): bool(v) for c, v in csf.items()},
    }


def _write_open_snapshot_full(out_dir: Path, emitted_open: list[dict]) -> None:
    """Stateful open_snapshot = FULL rewrite from the current open journeys (all holders are reprocessed
    every day, so emitted_open IS the complete current-open set)."""
    snap = Path(out_dir) / "open_snapshot" / "open_snapshot.parquet"
    sw = ShardedParquetWriter(str(snap), flush_rows=1_000_000)
    sw.add_many(emitted_open)
    sw.close()


def run_daily_stateful(
    target_day: Optional[str] = None,
    *,
    state_dir: Path = DEFAULT_STATE_DIR,
    out_dir: Path = DEFAULT_OUT_DIR,
    universe: Optional[set] = None,
    flush_rows: int = 100_000,
    mem_soft_gb: float = 12.0,
) -> dict:
    """Advance the journeys store to target_day by feeding ONLY each new day's fills through the tracer
    resumed from carried per-holder state (zero history re-read). Requires a seeded checkpoint
    (checkpoint['wallet_state']). Newly-CLOSED journeys are APPENDED (settled journeys never change, no
    tombstoning); the open_snapshot is fully rewritten from current opens. Fail-closed on any wallet error.
    NOTE (v1): forward-only. Late/historical OLD-day republishing is NOT yet handled here (Phase 2b);
    the 1c-1f run_daily remains the fallback for that until 2b lands.

    OPTION A canonical-actions persistence is DELIBERATELY NOT enabled in this driver. It is forward-
    only (one day at a time, resumed from carried per-holder state) and is NOT in production (its
    m02_stateful_state/ checkpoint is absent). Incremental-actions correctness is NOT proven for this
    path (a resumed day does not re-trace from a flat seed, so the action tombstone-by-range invariant
    that run_daily relies on does not hold here). Canonical actions come ONLY from run_daily. If this
    driver is ever promoted, actions persistence must be designed + gate-proven separately BEFORE
    turning it on here."""
    state_dir = Path(state_dir); out_dir = Path(out_dir)
    _closed_dir(out_dir).mkdir(parents=True, exist_ok=True)
    (out_dir / "open_snapshot").mkdir(parents=True, exist_ok=True)
    hot_days = hot_available_days()
    if not hot_days:
        raise SystemExit("no hot fills days available")
    target = target_day or hot_days[-1]
    # CODEX P1 #3 (2026-07-16): NEVER advance the watermark past a day whose file is actually present.
    # A future/absent target_day would process only available days yet write watermark_day=target, so a
    # file arriving between the last processed day and target would be seen as already-watermarked and
    # silently skipped. Clamp target to the newest available hot day <= target (mirrors run_daily's guard).
    if target not in hot_days:
        eligible = [d for d in hot_days if d <= target]
        if not eligible:
            raise SystemExit(f"stateful: target_day {target} precedes the hot store (have {hot_days[0]}..)")
        target = eligible[-1]
    cp = load_checkpoint(state_dir)
    if cp is None or "wallet_state" not in cp:
        raise SystemExit("stateful run requires a checkpoint seeded with 'wallet_state' (run the seed first)")
    watermark = cp["watermark_day"]
    wallet_state: dict = dict(cp.get("wallet_state", {}))
    run_id = int(cp["run_id"])
    start_day = cp.get("start_day") or hot_days[0]
    fills_manifest = dict(cp.get("fills_manifest", {}))
    new_days = [d for d in hot_days if watermark < d <= target]
    prev_wm = watermark
    total_closed = 0
    emitted_open: list[dict] = []
    LOOKAHEAD_MS = 2 * 86_400_000

    # PHASE 2b (2026-07-16, closes codex P1 #2): LATE-FILL REPROCESS (hybrid). Fills can be PUBLISHED LATE
    # into a file for an OLD day we already checkpointed past. Detect via day_fingerprint diff vs the
    # checkpoint manifest; for wallets touched by a changed old day, FULL-re-trace [start_day, target]
    # (reuses the proven trace_wallet), REPLACE their store journeys (tombstone their active rows +
    # append fresh), and rebuild their carried state. The forward loop then SKIPS them (already through
    # target). Bounded to the small changed-wallet set; the expensive replay is paid only for them.
    # CODEX2 P1 (2026-07-16): reject a PAST target -- 2b would tombstone every late-wallet journey but
    # only re-trace through target < watermark, silently vanishing journeys in (target, watermark].
    if target < watermark:
        raise SystemExit(f"stateful: target_day {target} < watermark {watermark} (would truncate); refusing")
    tgt_end = day_end_ms(target)
    # CODEX2 P2 (2026-07-16): fingerprint each old day ONCE (here) and reuse that captured value for both
    # detection AND the manifest write, so a mid-replay file change can never record a version the replay
    # did not actually use (TOCTOU silent-miss).
    _cur_fp = {d: day_fingerprint(d) for d in hot_days if d <= watermark}
    diff_days = sorted(d for d in _cur_fp if _cur_fp[d] != fills_manifest.get(d))
    late_wallets = wallets_in_days(diff_days, universe) if diff_days else set()
    if universe is not None:
        late_wallets &= universe
    # CODEX2 P1 (2026-07-16): a rewrite that REMOVES a wallet's fills from a changed day leaves it OUT of
    # the new day-file, so wallets_in_days(diff_days) misses it and its stale journeys survive. Also
    # reprocess any wallet whose EXISTING store journeys OVERLAP the changed-day span (they were active on
    # a changed day per the store, regardless of the new file's contents). Scans the store ONLY on the rare
    # late-fill event (diff_days non-empty), never on a clean forward day.
    if diff_days:
        install_memory_guard(soft_gb=mem_soft_gb, label="m02-2b-overlap")   # guard BEFORE the store load (codex P1)
        _lo = day_start_ms(diff_days[0]); _hi = day_end_ms(diff_days[-1])
        # NOTE (codex P1, backstop-bounded): this loads the whole active-journeys store to find wallets whose
        # stored journeys overlap the changed-day span. JOURNEYS are compact (~161K rows / ~<1GB today) so it
        # sits well under the mem_safe_run 4GB job-tree ceiling, which HARD-bounds it if the store ever grows
        # past that. Future optimization: a column-projected loader (wallet/entry_ts/exit_ts/uid/run_id/active)
        # to cut this ~5x. Only runs on a late-fill event (diff_days non-empty), never on a clean forward day.
        _pa = load_active_closed(out_dir)
        if not _pa.empty and {"entry_ts", "exit_ts", "wallet"} <= set(_pa.columns):
            _ent = _pa["entry_ts"].astype("int64")
            _ex = _pa["exit_ts"].fillna(2**63 - 1).astype("int64")
            _touch = set(_pa["wallet"][(_ent <= _hi) & (_ex >= _lo)].astype(str).str.lower())
            if universe is not None:
                _touch &= universe
            late_wallets |= _touch
    late_open: list[dict] = []

    if not new_days and not late_wallets:
        # CODEX2 P2 (2026-07-16): a day can change fingerprint yet yield NO current wallets (empty/deletion
        # rewrite, or universe exclusion). Persist the new fingerprints so the same diff does not recur every
        # run forever. (Nothing to re-trace: no wallet is affected in the tracked universe.)
        if diff_days:
            for d in diff_days:
                fills_manifest[d] = _cur_fp[d]   # captured-once (TOCTOU-safe)
            _cp = dict(cp); _cp["fills_manifest"] = fills_manifest
            save_checkpoint(state_dir, _cp)
        logger.info(f"stateful: target {target} <= watermark {watermark} and no late fills; nothing to do")
        return {"run_id": run_id, "target": watermark, "affected": 0, "closed_fresh": 0, "open": 0}

    if late_wallets:
        logger.info(f"PHASE 2b: {len(diff_days)} changed old day(s) {diff_days[:5]}... -> re-trace "
                    f"{len(late_wallets):,} late-fill wallets over [{start_day}..{target}]")
        install_memory_guard(soft_gb=mem_soft_gb, label="m02-stateful-2b")
        run_id += 1
        lp = _closed_dir(out_dir) / f"run_{run_id:06d}.parquet"
        lcw = ShardedParquetWriter(str(lp), flush_rows=flush_rows)
        prior = load_active_closed(out_dir, wallets=late_wallets)
        if not prior.empty:
            for rec in prior.to_dict("records"):   # tombstone every existing active row for these wallets
                rec = dict(rec); rec["run_id"] = run_id; rec["active"] = False
                lcw.add(rec)
        wl = sorted(late_wallets)
        # P0 memory safety (codex P1): the 2b replay grouped-loads over the FULL [start_day..target] window,
        # so a 1500-wallet chunk is the same ~34GB balloon. Bound by window length.
        _replay_chunk = _bounded_chunk(tgt_end - day_start_ms(start_day))
        for ci in range(0, len(wl), _replay_chunk):
            cwl = wl[ci:ci + _replay_chunk]
            gf2, gf2u = fio.load_grouped_fills_funding(set(cwl), day_start_ms(start_day), tgt_end)
            for w in cwl:
                fills = fio.order_wallet_fills_causally([f for f in gf2.pop(w, []) if int(f["time"]) <= tgt_end])
                if not fills:
                    wallet_state.pop(w, None); continue
                fund = [x for x in gf2u.pop(w, []) if int(x["time"]) <= tgt_end]
                evs = [m02.LifecycleFillEvent(ts=int(f["time"]), event_order=int(f.get("fill_seq", i)), fill=f)
                       for i, f in enumerate(fills)]
                _, jr2, stt = m02.trace_wallet(w, evs, fills, fund, equity_enriched=False,
                                               end_ms=tgt_end, return_state=True)
                for j in jr2:
                    if j.get("open_at_window_end"):
                        late_open.append(_tag_journey(j, run_id, True))
                    else:
                        lcw.add(_tag_journey(j, run_id, True))
                keep = _carry_open_state(stt)
                if keep is not None:
                    wallet_state[w] = keep
                else:
                    wallet_state.pop(w, None)
        lcw.close()
        _write_touched(out_dir, run_id, late_wallets)   # authoritative delta signal (codex P1 #2)
        for d in diff_days:
            fills_manifest[d] = day_fingerprint(d)
        # pure late-fill run (no new days): write the snapshot + checkpoint now and finish.
        if not new_days:
            # CODEX2 P1 (2026-07-16): the pure-late snapshot must KEEP the opens of NON-late holders (they
            # are not re-emitted here) + the late wallets' fresh opens. Carry the prior snapshot's non-late
            # rows forward; otherwise real open positions silently vanish from the materialized snapshot.
            _snap = out_dir / "open_snapshot" / "open_snapshot.parquet"
            _carry_open: list[dict] = []
            if _snap.exists():
                _ps = pd.read_parquet(_snap)
                if not _ps.empty:
                    _carry_open = _ps[~_ps["wallet"].astype(str).str.lower().isin(late_wallets)].to_dict("records")
            _write_open_snapshot_full(out_dir, _carry_open + late_open)
            save_checkpoint(state_dir, {"watermark_day": watermark, "start_day": start_day,
                                        "wallet_state": wallet_state, "fills_manifest": fills_manifest,
                                        "run_id": run_id, "mode": "stateful",
                                        # stateful stores the LAST-written run_id -> committed = run_id (codex P2)
                                        "run_id_is_next": False,
                                        "updated_utc": pd.Timestamp.utcnow().isoformat()})
            logger.info(f"stateful (late-fill only) DONE -> watermark {watermark}, holders {len(wallet_state):,}")
            return {"run_id": run_id, "target": watermark, "closed_fresh": 0,
                    "open": len(late_open), "holders": len(wallet_state)}

    for day in new_days:
        d0, d1 = day_start_ms(day), day_end_ms(day)
        w_start = day_end_ms(prev_wm)   # funding watermark = prior day's end (carry covers <= this)
        # CODEX P1 #1 (2026-07-16): a day-D-trade-time fill can be physically stored in a LATER publish-day
        # file, so wallets_in_days([day]) (which reads only file D) can MISS a new/flat wallet whose only
        # day-D fill lives in file D+1 -> it is absent from `active`, its fill is dropped, checkpoint
        # advances silently. Discover the active set from ALL files in the lookahead window [D..D+2] and
        # let the per-wallet trade-time filter [d0,d1] below drop the over-included future-day wallets.
        # (NOTE: fills published AFTER the lookahead horizon, or after the latest file exists, remain the
        # Phase 2b late-fill-reprocess gap -- codex P1 #2 -- tracked separately; needs manifest-diff replay.)
        lookahead_days = [d for d in hot_days if day <= d and day_start_ms(d) <= d1 + LOOKAHEAD_MS]
        new_fill_wallets = wallets_in_days(lookahead_days, universe)
        # reprocess wallets with an OPEN position each day (funding accrual even without new fills). A
        # flat-but-carried wallet needs no daily work; it is reprocessed only when it has new fills.
        holders = {w for w, s in wallet_state.items()
                   if any(st.get("open") for st in (s.get("coin_state", {}) or {}).values())}
        if universe is not None:
            holders &= universe
        # Phase 2b: late-fill wallets were already fully re-traced through target above; skip them here.
        active = (new_fill_wallets | holders) - late_wallets
        # PARTITION FIX (2026-07-16): day-files are partitioned by PUBLISH day, not trade time -- a trade
        # near midnight can be physically stored in a later day-file (gate caught a 23:59:59.854 fill in the
        # next file -> a narrow [d0,d1] load MISSED it -> wrong carried position -> resync-split). Load with
        # a LOOKAHEAD upper bound and filter to trade-time in [d0,d1] so every day-D-trade-time fill is
        # captured regardless of which file holds it. (Genuine late fills beyond the available files remain
        # Phase 2b.) wallets_in_days for the active set uses the same trade-time filter, so it can also miss a
        # near-midnight wallet whose only day-D fill is in a later file; loading over `active` UNION the
        # lookahead-window's wallets closes that gap.
        hi = min(d1 + LOOKAHEAD_MS, day_end_ms(hot_days[-1]))
        run_id += 1
        run_path = _closed_dir(out_dir) / f"run_{run_id:06d}.parquet"
        cw = ShardedParquetWriter(str(run_path), flush_rows=flush_rows)
        new_ws: dict = {}
        emitted_open = []
        errors: list[tuple] = []
        n_closed_day = 0
        install_memory_guard(soft_gb=mem_soft_gb, label="m02-stateful")   # BEFORE the loads (codex P1)
        # P0 memory safety (codex P1): CHUNK the forward grouped-load by the active set so the parent holds only
        # one chunk's fills over the short forward window, never the whole active universe at once. Per-wallet
        # independent (each uses its own carried init + own fills) -> chunking is output-invariant.
        _fwd_chunk = _bounded_chunk(hi - d0)
        active_sorted = sorted(active)
        for _fi in range(0, len(active_sorted), _fwd_chunk):
          _cwset = active_sorted[_fi:_fi + _fwd_chunk]
          gf, gfund = fio.load_grouped_fills_funding(set(_cwset), d0, hi) if _cwset else ({}, {})
          for w in _cwset:
            init = wallet_state.get(w)
            wf = fio.order_wallet_fills_causally([f for f in gf.pop(w, []) if d0 <= int(f["time"]) <= d1])
            wfund = [x for x in gfund.pop(w, []) if d0 <= int(x["time"]) <= d1]
            if not wf and init is None:
                continue   # flat, no fills this day -> nothing to do
            res = m02.process_wallet_preloaded((w, wf, wfund, d1, init, w_start, True))
            if "error" in res:
                errors.append((w, res["error"])); continue
            for j in res["journeys"]:
                if j.get("open_at_window_end"):
                    emitted_open.append(_tag_journey(j, run_id, True))
                else:
                    cw.add(_tag_journey(j, run_id, True)); n_closed_day += 1
            keep = _carry_open_state(res.get("state"))
            if keep is not None:
                new_ws[w] = keep
        if errors:
            cw.abort()
            man = str(run_path.with_suffix(".errors.json"))
            Path(man).write_text(json.dumps({"day": day, "n_errors": len(errors),
                                             "errors": [{"wallet": w, "error": e} for w, e in errors[:50]]}, indent=2))
            logger.error(f"stateful {day}: {len(errors)} wallet errors -> fail closed. {man}. checkpoint NOT advanced.")
            raise SystemExit(1)
        cw.close()
        _write_touched(out_dir, run_id, active)   # authoritative delta signal for this run (codex P1 #2)
        # CODEX P2 #4 (2026-07-16): do NOT wholesale-replace wallet_state with only-processed wallets --
        # that would DROP a flat-carried wallet on any idle day, so its later re-trade resumes init=None
        # (coin_seen_first_fill / journey counters reset) and diverges from continuous batch. RETAIN every
        # non-active wallet's carried state; update the active ones; drop only those that ended fully flat
        # with no carry. (Bounded by the ever-active universe; prune-by-idle-age is a future size opt.)
        for w in active:
            if w in new_ws:
                wallet_state[w] = new_ws[w]
            else:
                wallet_state.pop(w, None)
        # open_snapshot = this day's forward opens + the late-fill wallets' opens (fixed from the 2b pass).
        _write_open_snapshot_full(out_dir, emitted_open + late_open)
        fills_manifest[day] = day_fingerprint(day)
        total_closed += n_closed_day
        logger.info(f"stateful {prev_wm}->{day}: active={len(active):,} closed_fresh={n_closed_day:,} "
                    f"open={len(emitted_open):,} holders_next={len(wallet_state):,} -> {run_path.name}")
        prev_wm = day

    new_cp = {
        "watermark_day": target,
        "start_day": start_day,
        "wallet_state": wallet_state,
        "fills_manifest": fills_manifest,
        "run_id": run_id,
        "mode": "stateful",
        # stateful stores the LAST-written run_id -> committed_run_id() = run_id (codex P2)
        "run_id_is_next": False,
        "updated_utc": pd.Timestamp.utcnow().isoformat(),
    }
    save_checkpoint(state_dir, new_cp)
    logger.info(f"stateful DONE -> watermark {target}, holders {len(wallet_state):,}, closed_fresh {total_closed:,}")
    return {"run_id": run_id, "target": target, "closed_fresh": total_closed,
            "open": len(emitted_open) + len(late_open), "late_wallets": len(late_wallets),
            "holders": len(wallet_state)}


def seed_stateful_checkpoint(
    watermark_day: str,
    *,
    state_dir: Path = DEFAULT_STATE_DIR,
    out_dir: Path = DEFAULT_OUT_DIR,
    universe: Optional[set] = None,
    procs: int = 6,
    mem_soft_gb: float = 12.0,
    chunk: int = 1500,
) -> dict:
    """ONE-TIME bootstrap: build checkpoint['wallet_state'] (the per-holder carried tracer state) that
    run_daily_stateful resumes from, WITHOUT changing the existing closed store (already built row-identical
    by 1c-1f). For each holder (a wallet with an open journey in open_snapshot), batch-trace its fills over
    [start_day, watermark] with return_state and keep _carry_open_state(final). Flat wallets need no seed
    (they re-seed fresh, uid-correct). Reuses the proven trace_wallet + bulk grouped loader; STREAMING-safe
    (chunked). After this, forward daily runs are seconds. Costs one full read of holders' history (once)."""
    state_dir = Path(state_dir); out_dir = Path(out_dir)
    snap = out_dir / "open_snapshot" / "open_snapshot.parquet"
    if not snap.exists():
        raise SystemExit(f"seed: no open_snapshot at {snap}")
    holders = set(pd.read_parquet(snap, columns=["wallet"])["wallet"].astype(str).str.lower())
    if universe is not None:
        holders &= universe
    _cp0 = load_checkpoint(state_dir) or {}
    start_day = _cp0.get("start_day") or hot_available_days()[0]   # match the store's genesis (1c-1f)
    t0, t1 = day_start_ms(start_day), day_end_ms(watermark_day)
    logger.info(f"seed: {len(holders):,} holders, batch-trace [{start_day}..{watermark_day}] for state")
    wallet_state: dict = {}
    wl = sorted(holders)
    # P0 memory safety: the seed grouped-loads each chunk over the FULL [start_day..watermark] window, so a
    # 1500-wallet chunk is the same ~34GB parent balloon that panicked the box. Bound it by the window length
    # (single-process, but the per-chunk parent load is what OOMs). Respect a smaller explicit override.
    chunk = min(chunk, _bounded_chunk(t1 - t0))
    logger.info(f"seed: mem-bounded chunk={chunk} wallets over [{start_day}..{watermark_day}] (~{_DEFAULT_PARENT_GB}GB/chunk)")
    install_memory_guard(soft_gb=mem_soft_gb, label="m02-seed")
    for ci in range(0, len(wl), chunk):
        cwl = wl[ci:ci + chunk]
        gf, gfund = fio.load_grouped_fills_funding(set(cwl), t0, t1)
        for w in cwl:
            fills = fio.order_wallet_fills_causally([f for f in gf.pop(w, []) if int(f["time"]) <= t1])
            if not fills:
                continue
            fund = [x for x in gfund.pop(w, []) if int(x["time"]) <= t1]
            events = [m02.LifecycleFillEvent(ts=int(f["time"]), event_order=int(f.get("fill_seq", i)), fill=f)
                      for i, f in enumerate(fills)]
            res = m02.trace_wallet(w, events, fills, fund, equity_enriched=False, end_ms=t1, return_state=True)
            keep = _carry_open_state(res[2])
            if keep is not None:
                wallet_state[w] = keep
        logger.info(f"seed chunk {ci // chunk + 1}: state for {len(wallet_state):,} wallets so far")
    cp = load_checkpoint(state_dir) or {}
    cp.update({
        "watermark_day": watermark_day,
        "start_day": start_day,
        "wallet_state": wallet_state,
        "fills_manifest": {d: day_fingerprint(d) for d in hot_available_days() if d <= watermark_day},
        "run_id": int(cp.get("run_id", 1)),
        "mode": "stateful",
        # seed writes the LAST-written run_id (matches run_daily_stateful) -> committed = run_id (codex P2)
        "run_id_is_next": False,
        "updated_utc": pd.Timestamp.utcnow().isoformat(),
    })
    save_checkpoint(state_dir, cp)
    logger.info(f"seed DONE: wallet_state for {len(wallet_state):,} holders @ watermark {watermark_day}")
    return {"holders_seeded": len(wallet_state), "watermark": watermark_day}


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def main() -> None:
    ap = argparse.ArgumentParser(
        description="V15 M02 daily-incremental journeys (append only NEW journeys; "
                    "provably batch-identical active closed store).")
    ap.add_argument("--wallets-file", default=None,
                    help="Universe filter (one wallet per line). Default: ALL wallets present "
                         "in the hot fills store are eligible.")
    ap.add_argument("--target-day", default=None,
                    help="YYYYMMDD to advance to. Default: latest available hot fills day.")
    ap.add_argument("--start-day", default=None,
                    help="YYYYMMDD window origin (the earliest day ever considered / carry-in seed "
                         "point). Default: earliest hot fills day.")
    ap.add_argument("--state-dir", default=str(DEFAULT_STATE_DIR),
                    help="Checkpoint dir (checkpoint.json).")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR),
                    help="Output store dir (closed/, open_snapshot/).")
    ap.add_argument("--procs", type=int, default=4, help="Worker processes.")
    ap.add_argument("--allow-unwrapped", action="store_true",
                    help="Bypass the mem_safe_run backstop requirement (ONLY for tiny test/gate slices).")
    ap.add_argument("--lookback-days", type=int, default=3,
                    help="Treat the last N days at/before the watermark as changed (late "
                         "publication) => their wallets are full-window replayed.")
    ap.add_argument("--flush-rows", type=int, default=100_000,
                    help="Rule-8 streaming: flush a parquet part every N buffered rows.")
    ap.add_argument("--mem-soft-gb", type=float, default=12.0,
                    help="Memory-guard soft cap (GB); abort loud above this.")
    ap.add_argument("--stateful", action="store_true",
                    help="Run the Phase-2 STATEFUL incremental driver (run_daily_stateful): carry per-holder "
                         "state, feed only new-day fills (~seconds/day). Requires a seeded --state-dir.")
    ap.add_argument("--seed", metavar="WATERMARK", default=None,
                    help="ONE-TIME: seed the stateful wallet_state from the store at WATERMARK (YYYYMMDD), "
                         "then exit. Run once before switching the cron to --stateful.")
    args = ap.parse_args()

    # ENFORCE the 2026-06-04 mem_safe_run mandate BY CODE (not by remembering to wrap): every real m02 run
    # (bulk run_daily / stateful / seed) MUST be launched under scripts/mem_safe_run.sh, which sets
    # MEM_SAFE_RUN=1 and kills the job group before the box thrashes. Refuse otherwise. --allow-unwrapped is
    # the explicit escape hatch for tiny test/gate slices only. (Postmortem 2026-07-17-m02-oom-kernel-panic.)
    if os.environ.get("MEM_SAFE_RUN") != "1" and not args.allow_unwrapped:
        sys.stderr.write(
            "REFUSING to run m02 unwrapped: this pipeline OOM-panicked the 16GB box (postmortem 2026-07-17).\n"
            "Launch it under the mandatory backstop, e.g.:\n"
            "  scripts/mem_safe_run.sh --floor-gb 4 --label m02 -- \\\n"
            f"    {sys.executable} {Path(__file__).name} " + " ".join(a for a in sys.argv[1:]) + "\n"
            "or pass --allow-unwrapped for a tiny test slice only.\n")
        sys.exit(2)

    # Stateful/seed default to a SEPARATE state dir so they never clobber the 1c-1f checkpoint (the live
    # cron path) during the transition; the store (--out-dir) is shared.
    state_dir = Path(args.state_dir)
    if (args.stateful or args.seed) and args.state_dir == str(DEFAULT_STATE_DIR):
        state_dir = DEFAULT_STATEFUL_STATE_DIR

    t0 = time.time()
    if args.seed:
        seed_stateful_checkpoint(args.seed, state_dir=state_dir, out_dir=Path(args.out_dir), procs=args.procs)
    elif args.stateful:
        run_daily_stateful(target_day=args.target_day, state_dir=state_dir, out_dir=Path(args.out_dir),
                           flush_rows=args.flush_rows, mem_soft_gb=args.mem_soft_gb)
    else:
        run_daily(
            target_day=args.target_day,
            state_dir=state_dir,
            out_dir=Path(args.out_dir),
            wallets_file=args.wallets_file,
            start_day=args.start_day,
            procs=args.procs,
            lookback_days=args.lookback_days,
            flush_rows=args.flush_rows,
            mem_soft_gb=args.mem_soft_gb,
        )
    logger.info(f"wall: {(time.time()-t0)/60:.2f} min")


if __name__ == "__main__":
    main()
