# Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Mac Mini Server (always-on)                  │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              QuantsLab Orchestrator (tmux: ql-pipeline)     │   │
│  │                                                             │   │
│  │  hermes_pipeline.yml                                        │   │
│  │  ┌──────────┐  ┌──────────────┐  ┌───────────────────┐    │   │
│  │  │ Candles   │  │ Bybit        │  │ E1/E2 Bulk       │    │   │
│  │  │ Downloader│  │ Derivatives  │  │ Backtest (weekly) │    │   │
│  │  └────┬─────┘  └──────┬───────┘  └───────────────────┘    │   │
│  │       └───────┬────────┘                                    │   │
│  │               ▼                                             │   │
│  │  ┌─────────────────────┐                                    │   │
│  │  │ Feature Computation │ ← 7 FeatureBase subclasses         │   │
│  │  └──────────┬──────────┘                                    │   │
│  │             ▼                                               │   │
│  │  ┌─────────────────────┐                                    │   │
│  │  │ Signal Scan         │ ← evaluate_e1() + evaluate_e2()   │   │
│  │  └──────────┬──────────┘                                    │   │
│  │             ▼                                               │   │
│  │  ┌─────────────────────┐    ┌──────────────────────┐       │   │
│  │  │ Testnet Resolver    │───▶│ Hummingbot API :8000 │       │   │
│  │  └─────────────────────┘    │ (Docker + tmux)      │       │   │
│  │                             │ bybit_perpetual_test  │       │   │
│  └─────────────────────────────┴──────────────────────┘       │   │
│                                                                     │
│  ┌──────────┐  ┌──────────────┐  ┌────────────────────────┐       │
│  │ MongoDB  │  │ Parquet      │  │ QL Task API :8001      │       │
│  │ :27017   │  │ Cache (138M) │  │ (tmux: ql-api)         │       │
│  └──────────┘  └──────────────┘  └────────────────────────┘       │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────┐      │
│  │ Telegram Bot → Alberto's DM (signals, fills, errors)     │      │
│  └──────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────┘
         ▲
         │ Tailscale SSH
         ▼
┌─────────────────────┐
│ MacBook Air (remote) │
│ - Claude Code        │
│ - Jupyter via SSH    │
│ - tmux attach        │
│ - status.sh          │
└─────────────────────┘
```

## Data Flow

### Live Pipeline (every 15 min → hourly)

```
Bybit Exchange (public REST API)
    │
    ├──▶ CLOBDataSource ──▶ Parquet files (1h, 5m candles)
    │    (CandlesDownloaderTask, hourly)
    │
    └──▶ bybit_rest.py ──▶ MongoDB (funding, OI, L/S)
         (BybitDerivativesTask, every 15 min)
              │
              ▼
    FeatureComputationTask (dependency-triggered)
    ├── ATRFeature ──────────────┐
    ├── RangeFeature ────────────┤
    ├── VolumeFeature ───────────┤
    ├── MomentumFeature ─────────┼──▶ MongoDB features collection
    ├── CandleStructureFeature ──┤    (upsert per pair per feature)
    ├── DerivativesFeature ──────┤
    └── MarketRegimeFeature ─────┘
              │
              ▼
    SignalScanTask (dependency-triggered)
    ├── Read features from MongoDB
    ├── pair_selector.get_eligible_pairs() ← pair_historical verdicts
    ├── Build DecisionSnapshot (frozen)
    ├── evaluate_e1() / evaluate_e2() ← pure functions
    ├── Store candidate to MongoDB (always)
    └── Telegram alert if CANDIDATE_READY
              │
              ▼
    TestnetResolverTask (every 5 min)
    ├── New signals: POST to HB API → PositionExecutor on testnet
    ├── Active positions: poll HB API for status
    ├── Resolved: record fill, slippage, PnL in MongoDB
    └── Telegram alert on fills
```

### Weekly Re-optimization (Sunday 03:00-04:00 UTC)

```
BulkBacktestTask (E1 at 03:00, E2 at 04:00)
    ├── Load all parquet candles
    ├── For each pair: run BacktestingEngine with controller
    ├── Record PF, WR, Sharpe, max_dd
    ├── Compute verdict: ALLOW (PF≥1.3) / WATCH (PF≥1.0) / BLOCK
    └── Upsert to MongoDB pair_historical
```

## Engine Architecture

### E1 — Compression Breakout

Two-layer deterministic system:
```
Layer 1 (1h): Detect compression (ATR pct < 0.20) + breakout event
    ↓ generates setup: (direction, breakout_level, start_ts, expiry_ts)
Layer 2 (5m): First valid entry within setup window
    ↓ entry quality: distance, body, gap checks
Signal fires → CANDIDATE_READY
```

Edge: Volatility compression → expansion is a documented market microstructure pattern. The 5m entry quality gate reduces adverse selection on the breakout bar.

Known issue: 170+ scans with 0 triggers during P3. The ATR < 20th percentile threshold may be too restrictive. Weekly re-optimization will surface this if the backtest window shifts.

### E2 — Range Fade

Single-layer mean reversion:
```
1h: ATR compressed (< 30th pct) + range stable + boundary touch + rejection
    ↓ rejection confirmed: close back inside range
    ↓ no breakout conviction: low volume, low OI, small body
Signal fires → CANDIDATE_READY with TP=midpoint, SL=boundary-0.75ATR
```

Edge: In chop regimes, range boundaries act as support/resistance. The "no breakout confirmation" filter eliminates the cases where the boundary touch IS the breakout.

### Snapshot-First Architecture

All engine logic reads from frozen `DecisionSnapshot` — never from live data:
```
1. BUILD SNAPSHOT     → freeze all inputs (immutable after this)
2. VALIDATE STALENESS → check from snapshot only
3. ENGINE TRIGGER     → check primary conditions
4. EVALUATE FILTERS   → hard filters, then soft scoring
5. REGISTER CANDIDATE → always log (even filtered)
6. EXECUTION LOGIC    → portfolio allocator (sizing, concurrency)
```

## Feature Store Design

Seven independently computable features, all writing to MongoDB `features` collection:

| Feature | Input | Key outputs | Used by |
|---------|-------|-------------|---------|
| ATR | 1h parquet | atr_14_1h, atr_percentile_90d, compression_flag | E1, E2, Regime |
| Range | 1h parquet | range_high/low_20, range_width, range_expanding | E1, E2, pair_selector |
| Volume | 1h parquet | vol_avg_20, vol_zscore_20, vol_floor_passed | E1, E2 |
| Momentum | 1h parquet | return_1h/4h/24h, ema_50/200, ema_alignment | Regime, Derivatives (RS) |
| CandleStructure | 1h parquet | body_atr_ratio, wick_atr_ratio, price_position | E2, pair_selector |
| Derivatives | MongoDB | oi_change, funding_rate/neutral, ls_ratio, rs_aligned | E1, E2 |
| MarketRegime | Other features | regime_label, risk_off, btc_contagion | SignalScan (hard block) |

Feature values are `Dict[str, float]` (MongoDB-friendly). Booleans stored as 0.0/1.0. String metadata in `info` field.

Upserted by `(feature_name, trading_pair)` — no duplicates. TTL 90 days.

## Risk Management

### Position level
- E1: TP +3%, SL -1.5% (2:1 RR), trailing stop +1.5%/0.5%, time limit 24h
- E2: TP range midpoint, SL boundary -0.75 ATR, time limit 12h

### Engine level
- E1: max 2 concurrent positions
- E2: max 1 concurrent position

### Portfolio level
- Max 3 positions across all engines
- Max 2% total capital exposure
- 0.3% per position

### Global
- BTC Risk-Off Contagion: hard block on all E1 entries
- Funding window avoidance: ±15 min of 00:00/08:00/16:00 UTC
- BTC shock filter: block if BTC > 3% move in last 1h

### Emergency
- Kill switch: `bash scripts/kill_switch.sh`
- Pauses all tasks, stops all executors, marks positions as EMERGENCY_STOPPED
- Telegram notification sent

## Technology Stack

| Component | Technology | Location |
|-----------|-----------|----------|
| Framework | QuantsLab (Hummingbot) | `core/` (upstream) |
| Orchestrator | TaskOrchestrator + YAML DAG | `config/hermes_pipeline.yml` |
| Feature store | MongoDB + FeatureBase | `app/features/` → MongoDB `features` |
| Candle cache | Parquet files | `app/data/cache/candles/` |
| Execution | Hummingbot API (Docker) | `localhost:8000` |
| Backtesting | QL BacktestingEngine + Optuna | `core/backtesting/` |
| Notifications | QL TelegramNotifier | Dedicated bot → Alberto's DM |
| Monitoring | QL Task API + status.sh | `localhost:8001` |
| Remote access | Tailscale SSH | MacBook Air → Mac Mini |
| Version control | Git (fork) | `aloddo/quants-lab` |

## tmux Sessions

| Session | Purpose | Start command |
|---------|---------|---------------|
| `ql-pipeline` | TaskOrchestrator running DAG | `scripts/start_pipeline.sh` |
| `ql-api` | QL Task API on :8001 | `scripts/start_pipeline.sh` |
| `hummingbot` | HB instance (existing) | pre-existing |
| `gateway` | HB gateway (existing) | pre-existing |

Attach: `tmux attach -t ql-pipeline`
List: `tmux ls`

## Remaining Work

### Before go-live
- [ ] Configure `bybit_perpetual_testnet` connector in Hummingbot
- [ ] Run bulk backtests from Jupyter (verify pair verdicts)
- [ ] Start pipeline: `bash scripts/start_pipeline.sh`
- [ ] Monitor first full cycle
- [ ] Set git author: `git config user.name "Alberto Loddo"`

### Future improvements
- [ ] Incremental feature computation (skip fresh pairs)
- [ ] Portfolio-level correlation check (avoid correlated positions)
- [ ] Adaptive ATR thresholds based on regime
- [ ] Fear & Greed external data source (deferred, low priority)
- [ ] Grafana dashboard for long-term monitoring
- [ ] MCP integration for Claude on Air to query system state
