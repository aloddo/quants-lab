#!/usr/bin/env python3
"""MANDATORY memory-safe streaming I/O for ALL fan-out / backtest / simulation scripts.

BINDING RULE (Alberto 2026-05-31, after the 4th-5th OOM crash of the week; decision
projects/quant/decisions/2026-05-31-mandatory-streaming-io; CLAUDE.md Key Rule):
  No script that fans out over the wallet universe (or produces per-action / per-fill / per-tick
  rows) may accumulate the FULL result set in RAM before writing. It MUST stream output to disk in
  bounded chunks via `ShardedParquetWriter` and MUST install `install_memory_guard()` so a runaway
  aborts LOUDLY (with the partial parts safe on disk) instead of a silent OS SIGKILL.

Root cause of the recurring OOMs: `results = []; for r in pool.imap(...): results.append(r);
df = pd.DataFrame(all_rows)`. At ~18k wallets x thousands of per-action rows that list exceeds
16 GB and the OS SIGKILLs the process (no traceback, workers BrokenPipe, leaked semaphores).

USAGE
    from _streaming_io import ShardedParquetWriter, install_memory_guard
    install_memory_guard(soft_gb=12, label="m02")
    aw = ShardedParquetWriter(actions_out, flush_rows=2_000_000)
    for r in pool.imap_unordered(process_wallet, tasks):
        aw.add_many(r["actions"])          # buffers; auto-flushes a part to disk past flush_rows
    n = aw.close()                          # flushes remainder + stitches parts -> single parquet

Memory stays flat (bounded by flush_rows), regardless of universe size.
"""
from __future__ import annotations

import logging
import os
import sys
import threading
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger("streaming_io")

# Opt-out for the rare deliberate case. Unset by default: the guard must be the default, not the flag.
_ALLOW_SYMLINK_WRITE_ENV = "QL_ALLOW_SYMLINK_WRITE"

# Memory-safety floors (2026-07-30, Fable plan gate Step 5). See plan_memory_budget / require_mem_safe_run.
_MIN_HEADROOM_GB = 2.0
_ALLOW_LOW_HEADROOM_ENV = "QL_ALLOW_LOW_HEADROOM"
_REQUIRE_WRAPPER_ENV = "QL_ALLOW_UNWRAPPED_HEAVY"


def require_mem_safe_run(module: str) -> None:
    """Heavy modules must run under scripts/mem_safe_run.sh. Refuse otherwise.

    WHY (2026-07-30): m04 fold 11 was OOM-killed twice at rc=137. The in-process memory guard did not
    fail and its design is not wrong -- the guard bounds THE JOB (per-process RSS, 15s poll) while the
    wrapper bounds THE BOX (polls system-available continuously and kills the group on first breach).
    scripts/m04_20k_folds9to12.sh invoked the module DIRECTLY, so the only component watching the right
    variable was absent. The wrapper's own header calls itself "MANDATORY wrapper for every heavy/batch
    /fan-out job on this box" (binding decision 2026-06-04-mem-safe-run-backstop), and its 2026-07-17 P0
    note records that it switched to system-available semantics *specifically because* the old RSS
    ceiling MISSED v15_m04 reading the 11GB store via mmap.

    Prose did not stop it; this does -- and it closes the hole against future drivers too, not just the
    one that broke.
    """
    if os.environ.get("MEM_SAFE_RUN") == "1" or os.environ.get("MEM_SAFE_RUN_CEIL_MB"):
        return
    if os.environ.get(_REQUIRE_WRAPPER_ENV) == "1":
        logger.warning("[mem_safe] %s running UNWRAPPED (permitted by %s=1). Nothing is watching "
                       "system-available; an OOM SIGKILL will look like a crash.",
                       module, _REQUIRE_WRAPPER_ENV)
        return
    raise RuntimeError(
        f"{module} must run under scripts/mem_safe_run.sh -- it is the only component that polls "
        f"SYSTEM-AVAILABLE memory and kills the job group before the kernel does. Running bare is how "
        f"m04 fold 11 was OOM-killed twice (rc=137). Wrap it:\n"
        f"    scripts/mem_safe_run.sh --floor-gb 2 --label {module} -- <python ...>\n"
        f"(Override for a genuinely light run: {_REQUIRE_WRAPPER_ENV}=1)"
    )


def assert_not_symlinked_output(path: str | Path, what: str = "output") -> Path:
    """Refuse to write to a path that IS a symlink, or that lives under a symlinked directory.

    WHY (2026-07-30, Fable plan gate Step 3): `app/data/v15/` holds three parallel run dirs wired
    together with symlinks -- 18 in funnel20k_20260728, 27 in census20k_20260728, of which 9 point INTO
    funnel20k, with a MIX of relative and absolute targets. Writing m05 output into census20k therefore
    writes THROUGH into funnel20k and silently overwrites the inputs of a different run. On 2026-07-30 I
    came one command away from doing exactly that and caught it only by running `ls -l` by hand. The
    hash inventory in app/data/v15/_provenance/ exists because of that near miss.

    Silent cross-run corruption is the worst failure mode available here: the clobbered file is then
    read as an input and reasoned from. So this is a hard refusal, not a warning.

    Set QL_ALLOW_SYMLINK_WRITE=1 to override deliberately (logged loudly).
    """
    p = Path(path)
    if os.environ.get(_ALLOW_SYMLINK_WRITE_ENV) == "1":
        logger.warning("SYMLINK-WRITE GUARD BYPASSED via %s=1 for %s %s",
                       _ALLOW_SYMLINK_WRITE_ENV, what, p)
        return p
    if p.is_symlink():
        raise ValueError(
            f"REFUSING to write {what} through a SYMLINK: {p} -> {os.readlink(p)}. "
            f"Writing here would silently overwrite another run's file. Write to a real path inside "
            f"this run's own directory. (Override: {_ALLOW_SYMLINK_WRITE_ENV}=1)"
        )
    # Parent walk is SCOPED TO THE REPO. Walking to `/` produced a false positive on the first version:
    # on macOS `/var` and `/tmp` are themselves symlinks (-> private/var), so every legitimate temp write
    # was refused. An over-broad guard is worse than none -- it is the kind operators learn to disable.
    # Only symlinked parents INSIDE the repo can cause the cross-run corruption this exists to prevent.
    repo = Path(__file__).resolve().parents[2]
    try:
        p_abs = p if p.is_absolute() else (Path.cwd() / p)
        inside_repo = repo in p_abs.parents
    except Exception:
        inside_repo = False
    if inside_repo:
        for parent in p_abs.parents:
            if parent == repo:
                break
            if parent.is_symlink():
                raise ValueError(
                    f"REFUSING to write {what} {p}: its parent directory {parent} is a SYMLINK -> "
                    f"{os.readlink(parent)}. The write would land in another run's directory. "
                    f"(Override: {_ALLOW_SYMLINK_WRITE_ENV}=1)"
                )
    return p


class ShardedParquetWriter:
    """Buffer rows, flush a part-file every `flush_rows`, stitch parts -> ONE parquet on close().

    Parent/process memory is bounded by `flush_rows` (NOT by universe size). Part files are the
    durable artifact: if `close()`'s stitch step ever fails, the parts remain on disk and the cheap
    stitch can be re-run with no recompute. The combined output schema is the pyarrow union of all
    part schemas (so an all-null column in one chunk that is typed in another reconciles cleanly)."""

    def __init__(self, out_path: str | Path, flush_rows: int = 100_000, keep_parts: bool = False):
        # Symlink-write guard at the ONE chokepoint every v15 module already funnels output through,
        # so it cannot be forgotten by a future caller (Fable plan gate Step 3).
        self.out = assert_not_symlinked_output(out_path, "parquet output")
        # codex 2026-07-30 #5: checking ONLY the final output left both STAGING paths open. An existing
        # `<out>.parts` symlink is followed by mkdir + stale-part deletion + every part write, and an
        # existing `<out>.tmp` symlink is followed and its target TRUNCATED during the final stitch.
        # Either clobbers another run despite the guard. Check every path this class writes to.
        assert_not_symlinked_output(self.out.with_suffix(self.out.suffix + ".parts"), "parts dir")
        assert_not_symlinked_output(self.out.with_suffix(self.out.suffix + ".tmp"), "stitch tmp file")
        self.parts_dir = self.out.with_suffix(self.out.suffix + ".parts")
        self.parts_dir.mkdir(parents=True, exist_ok=True)
        # codex perf-r1 #1: WIPE any stale parts from a prior aborted/failed run so close() can never
        # stitch leftover rows into a fresh output (silent corruption). Each run starts clean.
        stale = list(self.parts_dir.glob("part_*.parquet"))
        for p in stale:
            p.unlink()
        if stale:
            logger.warning(f"ShardedParquetWriter: removed {len(stale)} stale part(s) in {self.parts_dir}")
        self.flush_rows = int(flush_rows)
        self.keep_parts = keep_parts
        self._buf: list[dict] = []
        self._part_idx = 0
        self.total_rows = 0

    def add_many(self, records: list[dict] | None) -> None:
        if not records:
            return
        self._buf.extend(records)
        if len(self._buf) >= self.flush_rows:
            self.flush()

    def add(self, record: dict) -> None:
        self._buf.append(record)
        if len(self._buf) >= self.flush_rows:
            self.flush()

    def flush(self) -> None:
        if not self._buf:
            return
        df = pd.DataFrame(self._buf)
        p = self.parts_dir / f"part_{self._part_idx:06d}.parquet"
        df.to_parquet(p, index=False, compression="snappy")
        self.total_rows += len(df)
        self._part_idx += 1
        self._buf = []          # free RAM immediately
        del df

    def close(self) -> int:
        """Flush remainder, stitch parts -> single output parquet (streaming, one part at a time)."""
        self.flush()
        parts = sorted(self.parts_dir.glob("part_*.parquet"))
        tmp = self.out.with_suffix(self.out.suffix + ".tmp")
        if not parts:
            # ATOMIC empty write (codex P2 #3): temp + rename so a kill / disk error mid-write can
            # never leave a truncated, glob-visible output that breaks every subsequent active-view
            # read. Matches the non-empty stitch path (which already writes to .tmp then replaces).
            pd.DataFrame().to_parquet(tmp, index=False)
            tmp.replace(self.out)
            self._cleanup_parts(parts)
            return 0
        # codex perf-r1 #2: PERMISSIVE promotion so a column that is int64 in all-closed chunks but
        # double (None present) in another chunk (e.g. M02 exit_ts on open journeys) unifies to double
        # instead of failing the stitch at the END of a long run. Fall back for older pyarrow.
        schemas = [pq.read_schema(p) for p in parts]
        try:
            unified = pa.unify_schemas(schemas, promote_options="permissive")
        except TypeError:
            unified = pa.unify_schemas(schemas)
        writer = pq.ParquetWriter(tmp, unified, compression="snappy")
        try:
            for p in parts:
                t = pq.read_table(p)
                if t.schema != unified:
                    # Column ORDER can differ across shards (heterogeneous event dicts, e.g. ledger
                    # deltas); align names to the unified order (adding all-null columns for any
                    # field a shard never saw) before cast, which requires matching names+order.
                    if t.schema.names != unified.names:
                        missing = [n for n in unified.names if n not in t.schema.names]
                        for n in missing:
                            t = t.append_column(n, pa.nulls(t.num_rows, type=unified.field(n).type))
                        t = t.select(unified.names)
                    t = t.cast(unified, safe=False)
                writer.write_table(t)
                del t
        finally:
            writer.close()
        tmp.replace(self.out)
        self._cleanup_parts(parts)
        logger.info(f"ShardedParquetWriter -> {self.out} ({self.total_rows:,} rows, {len(parts)} parts stitched)")
        return self.total_rows

    def abort(self) -> None:
        """Discard any buffered + flushed parts WITHOUT writing the main output parquet. Use on a
        failure path where writing an empty/no-schema artifact to ``self.out`` would CLOBBER a prior
        valid artifact. Leaves ``self.out`` untouched; removes the .parts staging dir."""
        self._buf.clear()
        parts = sorted(self.parts_dir.glob("part_*.parquet"))
        self._cleanup_parts(parts)

    def _cleanup_parts(self, parts) -> None:
        if self.keep_parts:
            return
        for p in parts:
            try:
                p.unlink()
            except FileNotFoundError:
                pass
        try:
            self.parts_dir.rmdir()
        except OSError:
            pass


def _current_rss_gb() -> float:
    """Best-effort CURRENT resident set size in GB."""
    try:
        import psutil  # type: ignore
        return psutil.Process().memory_info().rss / (1024 ** 3)
    except Exception:
        import resource
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # macOS ru_maxrss = bytes; linux = kilobytes (this is PEAK, a conservative proxy)
        return peak / (1024 ** 3) if sys.platform == "darwin" else peak / (1024 ** 2)


_GUARD_STATE: "dict | None" = None   # process-wide: one watchdog thread, mutable soft cap (codex #2)


def install_memory_guard(soft_gb: float = 12.0, label: str = "proc", poll_s: int = 15) -> None:
    """Backstop watchdog: abort LOUDLY (logged, exit 137) if RSS exceeds `soft_gb`, instead of a
    silent OS SIGKILL. Prevention is ShardedParquetWriter; this turns any residual runaway into a
    clear, actionable failure with the flushed parts left safe on disk. MANDATORY on every fan-out
    run (set soft_gb a few GB below the machine's physical RAM)."""
    def _watch():
        while True:
            time.sleep(poll_s)
            try:
                g = _current_rss_gb()
            except Exception:
                continue
            cap = _GUARD_STATE["soft_gb"] if _GUARD_STATE else soft_gb   # honours later tightening
            if g > cap:
                logger.error(
                    f"[memory_guard:{label}] RSS {g:.1f}GB exceeded soft cap {cap}GB. "
                    f"Aborting LOUDLY (exit 137) instead of a silent OOM SIGKILL. Use "
                    f"ShardedParquetWriter / smaller flush_rows. Flushed parts are safe on disk.")
                os._exit(137)
    # codex 2026-08-10 #2: PROCESS-IDEMPOTENT. m10 calls run_m09_chained() once per null seed
    # (~1,000 at defaults), and each call used to spawn another permanent watchdog thread. One guard
    # per process is all the guarantee requires; a later call with a TIGHTER cap still wins by
    # replacing the effective cap on the running guard.
    global _GUARD_STATE
    if _GUARD_STATE is not None:
        if soft_gb < _GUARD_STATE["soft_gb"]:
            _GUARD_STATE["soft_gb"] = float(soft_gb)
            logger.info(f"[memory_guard:{_GUARD_STATE['label']}] soft cap tightened to {soft_gb}GB "
                        f"by nested install ({label})")
        return
    _GUARD_STATE = {"soft_gb": float(soft_gb), "label": label}
    threading.Thread(target=_watch, daemon=True, name=f"memguard-{label}").start()
    logger.info(f"[memory_guard:{label}] installed (soft cap {soft_gb}GB, poll {poll_s}s)")


def _available_ram_gb() -> float:
    """Best-effort AVAILABLE (free + reclaimable) system RAM in GB. Falls back to a small
    conservative value if psutil is missing, so the budget errs toward fewer procs."""
    try:
        import psutil  # type: ignore
        return psutil.virtual_memory().available / (1024 ** 3)
    except Exception:
        logger.warning("[mem_budget] psutil unavailable; assuming 4GB free (conservative)")
        return 4.0


class MemoryBudgetError(RuntimeError):
    """Raised when a fan-out run cannot fit in RAM even at the minimum (main + one worker). Aborting
    BEFORE any work starts is the whole point: on a no-swap box, 'run serial and hope the guard
    catches it' is exactly the OOM that jetsam-killed gbrain-postgres on 2026-06-10."""


class MemBudget:
    """Result of plan_memory_budget: a composable AGGREGATE memory plan for a fan-out run.

    Per-process guards do NOT compose: main(parent) + N workers x per_worker_gb + live baseline can
    blow past physical RAM and get jetsam-killed. This computes one plan from ACTUAL available RAM
    and caps the worker count so the WHOLE run fits WITHOUT relying on the (15s-poll) guard firing.

    Enforced invariant (codex 2026-06-10):
        procs * (per_worker_gb * worker_margin) + main_reserve_gb + headroom_gb <= free_gb
    The soft caps (main_soft_gb / worker_soft_gb) are DIAGNOSTIC tripwires only, never above what the
    plan grants. Memory safety comes from the procs cap, not the guard."""

    __slots__ = ("procs", "main_soft_gb", "worker_soft_gb", "free_gb", "per_worker_gb",
                 "requested_procs", "headroom_gb", "main_reserve_gb", "usable_gb", "worker_planned_gb")

    def __init__(self, procs, main_soft_gb, worker_soft_gb, free_gb, per_worker_gb,
                 requested_procs, headroom_gb, main_reserve_gb, usable_gb, worker_planned_gb):
        self.procs = procs
        self.main_soft_gb = main_soft_gb
        self.worker_soft_gb = worker_soft_gb
        self.free_gb = free_gb
        self.per_worker_gb = per_worker_gb
        self.requested_procs = requested_procs
        self.headroom_gb = headroom_gb
        self.main_reserve_gb = main_reserve_gb
        self.usable_gb = usable_gb
        self.worker_planned_gb = worker_planned_gb

    def planned_peak_gb(self) -> float:
        """The RAM this plan expects to consume (excl. headroom): main + procs margined workers."""
        return self.main_reserve_gb + self.procs * self.worker_planned_gb

    def __repr__(self):
        return (f"MemBudget(free={self.free_gb:.1f}GB headroom={self.headroom_gb:.1f}GB "
                f"main_reserve={self.main_reserve_gb:.1f}GB usable={self.usable_gb:.1f}GB "
                f"requested={self.requested_procs} -> procs={self.procs} "
                f"planned_peak={self.planned_peak_gb():.1f}GB "
                f"main_soft={self.main_soft_gb:.1f}GB worker_soft={self.worker_soft_gb:.1f}GB)")


def plan_memory_budget(requested_procs: int,
                       per_worker_gb: float,
                       headroom_gb: float = 6.0,
                       free_gb: float | None = None,
                       main_reserve_gb: float = 1.5,
                       main_soft_cap: float = 12.0,
                       worker_margin: float = 1.25) -> MemBudget:
    """Compute an AGGREGATE memory plan for a parallel fan-out run, capping worker count so the WHOLE
    job (main parent + workers) fits in available RAM, WITHOUT relying on the slow guard to fire.

    Memory model (all GB):
        free_gb            AVAILABLE system RAM. psutil.virtual_memory().available already nets out
                           the CURRENT live baseline (agents/mongod/postgres/etc).
        headroom_gb        EXTRA safety reserved for mid-run GROWTH of that baseline + FS cache. This
                           is NOT the current baseline (that is already excluded from `available`).
        main_reserve_gb    expected peak RSS of the MAIN/parent process (it holds the marks cache +
                           the Pool parent + streaming writers).
        per_worker_gb      expected peak RSS of ONE worker. worker_planned = per_worker_gb*worker_margin.
        usable_gb          = free_gb - headroom_gb - main_reserve_gb  (RAM left for workers)
        procs              = min(requested, floor(usable_gb / worker_planned))

    If procs < 1 (not even main + one margined worker fits), raise MemoryBudgetError -> abort BEFORE
    any work starts. There is deliberately NO serial fallback in this helper: 'run serial and hope
    the guard fires' is the exact OOM we are preventing. A caller that wants a risky override can
    catch MemoryBudgetError itself.

    Soft caps returned are DIAGNOSTIC tripwires only (15s poll), never set above the plan/grant:
        worker_soft_gb = worker_planned
        main_soft_gb   = min(main_soft_cap, free_gb - headroom_gb)   # never above what is grantable

    Raises ValueError on requested_procs<=0, per_worker_gb<=0, worker_margin<=0, headroom_gb<0,
    or main_reserve_gb<0."""
    if requested_procs <= 0:
        raise ValueError("requested_procs must be > 0")
    if per_worker_gb <= 0:
        raise ValueError("per_worker_gb must be > 0")
    if worker_margin <= 0:
        raise ValueError("worker_margin must be > 0")
    if headroom_gb < 0:
        raise ValueError("headroom_gb must be >= 0")
    if main_reserve_gb < 0:
        raise ValueError("main_reserve_gb must be >= 0")
    # HEADROOM FLOOR (2026-07-30, Fable plan gate Step 5). m04 fold 11 was OOM-killed TWICE at rc=137
    # because scripts/m04_20k_folds9to12.sh passed `--headroom-gb 0.5` against this module's default of
    # 6.0. Headroom is EXTRA reserve for mid-run GROWTH of the live baseline + FS cache; on a box shared
    # with ~5 agents that each move by hundreds of MB, 0.5GB is a rounding error, not a safety margin.
    # The planner did exactly as told and certified an infeasible plan as feasible. A knob that turns a
    # safety check into a formality must be hard to turn: floor it, and shout when it is overridden.
    if headroom_gb < _MIN_HEADROOM_GB:
        if os.environ.get(_ALLOW_LOW_HEADROOM_ENV) == "1":
            logger.warning("[mem_budget] headroom_gb=%.2f is BELOW the %.1fGB floor; permitted only "
                           "because %s=1. On a shared box this is how rc=137 happens.",
                           headroom_gb, _MIN_HEADROOM_GB, _ALLOW_LOW_HEADROOM_ENV)
        else:
            logger.warning("[mem_budget] headroom_gb=%.2f raised to the %.1fGB floor (m04 f11 was "
                           "OOM-killed twice with 0.5GB). Override with %s=1 if truly intended.",
                           headroom_gb, _MIN_HEADROOM_GB, _ALLOW_LOW_HEADROOM_ENV)
            headroom_gb = _MIN_HEADROOM_GB
    if free_gb is None:
        free_gb = _available_ram_gb()

    grantable_gb = free_gb - headroom_gb                 # physically grantable after safety headroom
    usable_gb = grantable_gb - main_reserve_gb           # left for workers after the main process
    worker_planned_gb = per_worker_gb * worker_margin

    if grantable_gb < main_reserve_gb or usable_gb < worker_planned_gb:
        msg = (f"infeasible: free={free_gb:.1f} - headroom={headroom_gb:.1f} - "
               f"main_reserve={main_reserve_gb:.1f} = usable {usable_gb:.1f}GB < one worker "
               f"{worker_planned_gb:.1f}GB. Free RAM first or lower headroom/per-worker.")
        logger.error(f"[mem_budget] ABORT (infeasible): {msg}")
        raise MemoryBudgetError(msg)
    procs = min(int(requested_procs), int(usable_gb // worker_planned_gb))

    worker_soft_gb = worker_planned_gb
    main_soft_gb = min(main_soft_cap, max(0.0, grantable_gb))   # tripwire, never above grantable
    b = MemBudget(procs=procs, main_soft_gb=main_soft_gb, worker_soft_gb=worker_soft_gb,
                  free_gb=free_gb, per_worker_gb=per_worker_gb, requested_procs=int(requested_procs),
                  headroom_gb=headroom_gb, main_reserve_gb=main_reserve_gb, usable_gb=usable_gb,
                  worker_planned_gb=worker_planned_gb)
    logger.info(f"[mem_budget] {b}")
    return b
