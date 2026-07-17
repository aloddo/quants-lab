#!/usr/bin/env python3
"""M2 batched-bulk runner (2026-07-14) — scalable full-universe journey trace over the ONE store.

WHY: v15_m02_journey_trace.py main() drives the PER-WALLET loader (process_wallet ->
hl_fills_io.load_wallet_fills), which globs+reads EVERY day-file for EVERY wallet: O(wallets x days)
full-day scans + RSS past the 0.5GB/worker core cap on the consolidated store (250 fills + 210 funding
day-files). Fatal for 20k wallets x ~220 days (the code's own docstring says so).

THIS drives the ALREADY-EXISTING, output-equivalent bulk path instead:
  hl_fills_io.load_grouped_fills_funding(batch, t0, t1)  # reads each day-file ONCE per batch
  m02.process_wallet_preloaded((w, fills, funding, end_ms))  # byte-identical to process_wallet(core)
in WALLET batches, streaming to a shared ShardedParquetWriter. ~ceil(N/BATCH) x days reads (e.g. 8 x 220
= ~1760) vs wallets x days (~4.4M). No cross-batch journey split: a wallet's FULL history is in one batch
(batch by wallet, never by time). Memory flat: only one batch's fills held; per-wallet data popped after use.

Fail-CLOSED (same as M2 main): any wallet error -> error manifest + nonzero exit so downstream halts.
NO --equity-enrichment lane here (M1 deprecated, Alberto TG11298): CORE journeys only.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from multiprocessing import Pool
from pathlib import Path

import pandas as pd

V15 = Path(__file__).resolve().parent.parent / "research" / "v15"
sys.path.insert(0, str(V15))

import hl_fills_io as fio  # noqa: E402
import v15_m02_journey_trace as m02  # noqa: E402
from _streaming_io import ShardedParquetWriter, install_memory_guard  # noqa: E402


def _init_worker(worker_soft_gb: float) -> None:
    # per-worker memory-guard backstop (abort loud, not silent OS OOM). A single wallet's preloaded
    # trace is small; the cap only catches a pathological wallet. maxtasksperchild recycles workers.
    install_memory_guard(soft_gb=worker_soft_gb, label=f"m2b-worker-{os.getpid()}")


def _load_wallets(path: str) -> list[str]:
    # DEDUP (codex m2-batched [P1]): a duplicate wallet in the universe file would be double-counted
    # across batches, or get full history once then empty (pop) within a batch. dict.fromkeys dedups
    # while preserving first-seen order (deterministic output).
    with open(path) as fh:
        raw = [l.strip().lower() for l in fh if l.strip() and not l.startswith("#")]
    return list(dict.fromkeys(raw))


def main() -> None:
    ap = argparse.ArgumentParser(description="M2 batched-bulk journey trace (CORE lane)")
    ap.add_argument("--wallets-file", required=True)
    ap.add_argument("--start", default="2025-12-01")
    ap.add_argument("--end", default="2026-07-13")
    ap.add_argument("--actions-out", required=True)
    ap.add_argument("--journeys-out", required=True)
    ap.add_argument("--batch-size", type=int, default=2500)
    ap.add_argument("--procs", type=int, default=8, help="parallel trace workers (leave cores for live engine)")
    ap.add_argument("--worker-gb", type=float, default=3.0, help="per-worker memory-guard soft cap")
    ap.add_argument("--flush-rows", type=int, default=100_000)
    ap.add_argument("--mem-soft-gb", type=float, default=10.0)
    ap.add_argument("--limit", type=int, default=0, help="cap wallets (smoke)")
    ap.add_argument("--fills-shard-dir", default=None,
                    help="wallet-partitioned fills shard (build_fills_wallet_shard.py). If set, read fills per "
                         "wallet from the shard (fast, no 11GB store re-read) instead of scanning all day-files.")
    args = ap.parse_args()

    # guards (codex m2-batched): reject silent-noop / self-overwrite configs.
    if args.batch_size <= 0:
        sys.exit(f"--batch-size must be > 0 (got {args.batch_size})")
    if str(Path(args.actions_out)) == str(Path(args.journeys_out)):
        sys.exit("--actions-out and --journeys-out must be different paths (shared parts dir would corrupt)")

    install_memory_guard(soft_gb=args.mem_soft_gb, label="m2-batched")

    t0 = int(pd.Timestamp(args.start, tz="UTC").timestamp() * 1000)
    t1 = int((pd.Timestamp(args.end, tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1000 - 1)
    end_ms = t1

    wallets = _load_wallets(args.wallets_file)
    if args.limit:
        wallets = wallets[: args.limit]
    n = len(wallets)
    bs = args.batch_size
    n_batches = (n + bs - 1) // bs
    print(f"wallets={n} batch_size={bs} batches={n_batches} window={args.start}..{args.end}", flush=True)

    Path(args.actions_out).parent.mkdir(parents=True, exist_ok=True)
    aw = ShardedParquetWriter(args.actions_out, flush_rows=args.flush_rows)
    jw = ShardedParquetWriter(args.journeys_out, flush_rows=args.flush_rows)

    errors: list[tuple] = []
    n_act = n_jrn = n_ok = 0
    t_start = time.time()
    procs = max(1, args.procs)

    def _consume(r: dict) -> None:
        nonlocal n_act, n_jrn, n_ok
        if "error" in r:
            errors.append((r.get("wallet", "?"), r["error"]))
        else:
            acts = r.get("actions") or []
            jrns = r.get("journeys") or []
            aw.add_many(acts)
            jw.add_many(jrns)
            n_act += len(acts)
            n_jrn += len(jrns)
            n_ok += 1

    # procs==1: run IN-PROCESS (no worker pool). On a RAM-tight box (the OS silently OOM-kills pool
    # workers when 3 x worker + main exceed physical free RAM, then imap hangs forever waiting on the
    # dead worker -> the 2026-07-14 hang). Single-process = bounded ~1.8GB, no worker-death hang, and
    # is the equivalence-verified path. procs>1 uses a pool (only safe when free RAM >> procs x worker).
    pool = None
    if procs > 1:
        pool = Pool(procs, initializer=_init_worker, initargs=(args.worker_gb,), maxtasksperchild=50)
    try:
        for bi in range(n_batches):
            batch = wallets[bi * bs : (bi + 1) * bs]
            tb = time.time()
            if args.fills_shard_dir:
                fills_by, funding_by = fio.load_grouped_fills_funding_sharded(set(batch), t0, t1, args.fills_shard_dir)
            else:
                fills_by, funding_by = fio.load_grouped_fills_funding(set(batch), t0, t1)
            if pool is None:
                for w in batch:
                    _consume(m02.process_wallet_preloaded(
                        (w, fills_by.pop(w, []), funding_by.pop(w, []), end_ms)))
            else:
                tasks = ((w, fills_by.pop(w, []), funding_by.pop(w, []), end_ms) for w in batch)
                for r in pool.imap(m02.process_wallet_preloaded, tasks, chunksize=8):
                    _consume(r)
            fills_by.clear()
            funding_by.clear()
            print(f"  batch {bi+1}/{n_batches} ({len(batch)} wallets) "
                  f"ok={n_ok} act={n_act:,} jrn={n_jrn:,} err={len(errors)} "
                  f"{time.time()-tb:.1f}s", flush=True)
    finally:
        if pool is not None:
            pool.close()
            pool.join()

    n_actions = aw.close()
    n_journeys = jw.close()
    print(f"DONE actions={n_actions:,} journeys={n_journeys:,} ok={n_ok}/{n} "
          f"err={len(errors)} wall={(time.time()-t_start)/60:.2f}min", flush=True)

    if errors:
        man = Path(args.actions_out).parent / "m2_batched_errors.json"
        man.write_text(json.dumps({"n_errors": len(errors), "errors": errors[:200]}, indent=2))
        print(f"FAIL-CLOSED: {len(errors)} wallet errors -> {man}", flush=True)
        sys.exit(2)


if __name__ == "__main__":
    main()
