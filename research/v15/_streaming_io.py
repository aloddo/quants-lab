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


class ShardedParquetWriter:
    """Buffer rows, flush a part-file every `flush_rows`, stitch parts -> ONE parquet on close().

    Parent/process memory is bounded by `flush_rows` (NOT by universe size). Part files are the
    durable artifact: if `close()`'s stitch step ever fails, the parts remain on disk and the cheap
    stitch can be re-run with no recompute. The combined output schema is the pyarrow union of all
    part schemas (so an all-null column in one chunk that is typed in another reconciles cleanly)."""

    def __init__(self, out_path: str | Path, flush_rows: int = 1_000_000, keep_parts: bool = False):
        self.out = Path(out_path)
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
            pd.DataFrame().to_parquet(self.out, index=False)
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
                    t = t.cast(unified, safe=False)
                writer.write_table(t)
                del t
        finally:
            writer.close()
        tmp.replace(self.out)
        self._cleanup_parts(parts)
        logger.info(f"ShardedParquetWriter -> {self.out} ({self.total_rows:,} rows, {len(parts)} parts stitched)")
        return self.total_rows

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
            if g > soft_gb:
                logger.error(
                    f"[memory_guard:{label}] RSS {g:.1f}GB exceeded soft cap {soft_gb}GB. "
                    f"Aborting LOUDLY (exit 137) instead of a silent OOM SIGKILL. Use "
                    f"ShardedParquetWriter / smaller flush_rows. Flushed parts are safe on disk.")
                os._exit(137)
    threading.Thread(target=_watch, daemon=True, name=f"memguard-{label}").start()
    logger.info(f"[memory_guard:{label}] installed (soft cap {soft_gb}GB, poll {poll_s}s)")
