# Full Data Management Roadmap — Next Session

## Context Snapshot (as of April 21, 2026)

- Hyperliquid `1s` microstructure collection is integrated into pipeline (`hyperliquid_microstructure_1s`) and running every minute.
- Hyperliquid `1m` candles from `candleSnapshot` appear capped to ~5,000 bars per request window (roughly 3.5 days at `1m`).
- Current machine constraints: **16GB RAM, 500GB SSD**.
- Current estimated footprint for continuous 1s collection with 4 pairs is approximately **~1 GB/day total** (Mongo + parquet).

---

## Next Session Objective

Build a production-grade data lifecycle so we can:
1. Keep enough high-frequency data to discover and exploit signals.
2. Avoid uncontrolled storage growth.
3. Preserve only what is useful long-term for research and model iteration.

---

## Requirements

### Functional Requirements

1. **Raw 1s ingestion remains continuous** through pipeline tasks.
2. **Automatic retention** for raw 1s Mongo collections via TTL indexes.
3. **Daily/hourly compaction** from raw 1s chunks to derived feature datasets.
4. **Derived datasets versioned** with timestamped partitions and metadata.
5. **Storage monitoring task** with warning thresholds and trend logging.
6. **Policy-driven pruning** of old parquet raw chunks.
7. **Research-ready marts** for:
   - MM microstructure studies (spread, imbalance, flow)
   - Directional/carry studies at 1m/1h
8. **Reproducible EDA/backtest inputs** with manifest files per run.

### Non-Functional Requirements

1. Keep total data footprint below **200GB target** (hard stop near 300GB).
2. Compaction jobs must be incremental and restart-safe.
3. No full-file rewrites for active raw streams (chunk append only).
4. Task failures must be observable from pipeline logs and stats.
5. End-to-end data freshness SLO:
   - 1s raw: < 2 minutes lag
   - 1m derived: < 10 minutes lag

---

## Target Data Lifecycle

### Tiering Model

1. **Bronze (Raw, short retention)**
   - `hyperliquid_l2_snapshots_1s` (Mongo)
   - `hyperliquid_recent_trades_1s` (Mongo)
   - `app/data/cache/hyperliquid_micro_1s/chunks/*` (parquet)

2. **Silver (Derived, medium/long retention)**
   - 1s/1m aggregated features:
     - `mid_px`, `spread_bps`, `imbalance_topn`, `flow_imb`, `n_trades`, `signed_notional`
   - Stored as partitioned parquet (`pair/date`) + metadata manifests.

3. **Gold (Research marts, long retention)**
   - Signal-label panels for backtests and walk-forward.
   - Event-window vaults around triggers/extremes.

### Retention Policy (Proposed)

| Data Class | Store | Retention | Notes |
|---|---|---:|---|
| Raw 1s Mongo | Mongo | 7 days | TTL on timestamp field |
| Raw 1s parquet chunks | Local SSD | 14 days | prune oldest first |
| Derived 1s features | Parquet | 90 days | compressed, partitioned |
| Derived 1m features | Parquet | 365 days | primary research horizon |
| 1h/4h/1d bars + funding | Mongo + parquet | long-term | keep indefinitely (small) |
| Event-window raw archives | Parquet | 180 days | only high-value windows |

---

## Implementation Plan (Next Session)

### Phase 0 — Baseline + Guardrails (30 min)

1. Snapshot current disk usage and collection stats.
2. Add/verify storage budget thresholds:
   - warning: 70%
   - critical: 85%
3. Save baseline report under `app/data/cache/ops/`.

### Phase 1 — TTL + Prune Policy (45 min)

1. Add TTL indexes:
   - `hyperliquid_l2_snapshots_1s` on `timestamp_utc`
   - `hyperliquid_recent_trades_1s` on `time`
2. Add pruning task for parquet chunk retention (14 days).
3. Add dry-run mode + hard-delete mode.

### Phase 2 — Compaction Pipeline (90 min)

1. New task: `HyperliquidMicroCompactionTask`.
2. Input: last N minutes of chunk files.
3. Output:
   - `app/data/cache/hyperliquid_micro_1s/derived_1s/`
   - `app/data/cache/hyperliquid_micro_1s/derived_1m/`
4. Ensure idempotent upsert behavior by `(pair, second)` and `(pair, minute)`.

### Phase 3 — Data Catalog + Manifests (45 min)

1. Create per-run manifest JSON:
   - source files
   - row counts
   - min/max timestamps
   - schema hash
2. Add simple `dataset_registry.csv` for research reproducibility.

### Phase 4 — Research Workflow Hookup (60 min)

1. Point micro EDA and signal sweep scripts at derived datasets by default.
2. Add rolling nightly run:
   - micro EDA summary
   - top signal candidate export
3. Save artifacts to `app/data/cache/x11_hyperliquid/`.

---

## Concrete Deliverables (End of Next Session)

1. `app/tasks/data_collection/hyperliquid_micro_compaction_task.py`
2. `app/tasks/data_collection/storage_guard_task.py`
3. `scripts/prune_hyperliquid_raw_chunks.py` (or task equivalent)
4. Pipeline config entries in `config/hermes_pipeline.yml` for:
   - compaction
   - storage guard
   - raw prune
5. A retention runbook in `docs/` with rollback steps.

---

## Acceptance Criteria (Definition of Done)

1. Raw 1s collection remains continuous while compaction runs.
2. TTL is active and old docs age out automatically.
3. Parquet raw chunks do not exceed configured retention window.
4. Derived 1m feature tables are produced and queryable for all active pairs.
5. Storage monitor emits a daily report and threshold alerts.
6. End-to-end daily net growth stays within planned envelope.

---

## Risks and Mitigations

1. **Disk fills faster during volatility spikes**
   - Mitigation: dynamic pair cap + emergency prune mode.
2. **Compaction lag**
   - Mitigation: bounded backlog window per run; catch-up mode off-peak.
3. **Schema drift in raw payloads**
   - Mitigation: schema versioning + manifest validation.
4. **Over-pruning hurts future research**
   - Mitigation: event-window archival before prune.

---

## Session Kickoff Checklist

1. Confirm current free disk and target budgets.
2. Enable TTL and prune safeguards first.
3. Implement compaction second.
4. Validate with one full hour of live data.
5. Lock policy and update docs/runbook.

