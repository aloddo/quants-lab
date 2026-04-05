# Next Steps — Strengthening the System

Status as of 2026-04-02: Pipeline is live, cycling every 15 min, paper trading on Bybit demo via HB native executors. No signals have fired yet (market conditions don't meet E1/E2 triggers).

---

## Priority 1: Validate Before Trusting

### Run bulk backtests on current data
The pair_historical verdicts (ALLOW/WATCH/BLOCK) were migrated from the old system. They need refreshing with the new data (46 pairs, 365 days).

```bash
# From Jupyter notebook or CLI:
MONGO_URI=... python cli.py trigger-task --task e1_bulk_backtest --config config/hermes_pipeline.yml
MONGO_URI=... python cli.py trigger-task --task e2_bulk_backtest --config config/hermes_pipeline.yml
```

Review results in MongoDB:
```javascript
db.pair_historical.find({engine: "E1"}).sort({profit_factor: -1})
```

**Why first**: The allowlists determine which pairs get scanned. Wrong verdicts = wrong pairs.

### Verify signal quality with historical replay
The signal scan currently runs on live features. To validate it would have caught real setups:
- Load historical candle data into features
- Run signal scan against past dates
- Compare against known setups from the crypto-quant era

**Not yet built**: needs a historical replay mode for FeatureComputationTask + SignalScanTask.

---

## Priority 2: Robustness

### Telegram alerts for pipeline failures
Currently only signals and fills send Telegram. Add notifications for:
- Task failures (any task in the DAG)
- MongoDB connection issues
- HB API unreachable
- Feature computation anomalies (e.g. 0 features computed)

**How**: Override `on_failure()` in each task to send a NotificationMessage with level="error".

### Feed health monitoring
No automated check for stale data. If CandlesDownloaderTask silently fails, features go stale.

**How**: Add a `FeedHealthTask` that checks:
- Latest parquet timestamp per pair (stale if > 2h for 1h data)
- Latest MongoDB derivatives timestamp (stale if > 30min)
- Latest feature timestamp (stale if > 30min)
- Send Telegram warning if any feed is stale

### Incremental feature computation
Currently recomputes ALL features for ALL pairs every cycle (34s). As pairs grow, this becomes the bottleneck.

**How**: Check `feature.timestamp` in MongoDB vs parquet last modified time. Skip pairs where features are fresh.

---

## Priority 3: Strategy Improvements

### E1 trigger sensitivity
170+ scans with 0 triggers in the old system. The ATR < 20th percentile threshold may be too restrictive for current market regime.

**Investigation**:
- Query historical ATR percentiles for all pairs
- Check how often the threshold was nearly met (e.g. 20-25th percentile)
- Consider adaptive threshold based on market regime (e.g. wider in high-vol environments)

### E2 pair expansion
Only 3 ALLOW pairs (BTC, BCH, ADA). The bulk backtest will likely surface more candidates.

**After backtests**: Review WATCH pairs — some may be promotable with regime filtering.

### Portfolio-level correlation
E1 and E2 can both be in positions simultaneously. If BTC dumps, correlated alt longs all get hit.

**How**: Before placing a new position, check if existing positions are in correlated assets. Use the momentum feature's return_4h to detect correlation.

### Position sizing refinement
Currently flat 0.3% per position. Options:
- Kelly criterion based on backtest PF/WR
- Conviction-based: scale size with soft_filter_score
- Volatility-adjusted: smaller size when ATR is high

---

## Priority 4: Infrastructure

### Git author setup
Commits currently show "Hermes" instead of "Alberto Loddo".
```bash
git config user.name "Alberto Loddo"
git config user.email "your@email"
```

### Docker patch persistence
The 4 patches on the HB API Docker container are lost on rebuild (see CLAUDE.md for full commands). Options:
- **Short term**: Re-apply with `sed` after each rebuild (documented in CLAUDE.md)
- **Long term**: Fork the hummingbot-api Docker image, bake patches in, push to private registry
- **Best**: Submit PR to hummingbot to add `bybit_perpetual_demo` as a native domain

### Candle data refresh
CandlesDownloaderTask is set to 365 days retention but only runs hourly incremental updates. The initial 365-day backfill was a one-off. If parquet files are lost, re-run:
```bash
# Use the backfill script approach from the initial session
# Top 50 pairs, 365 days, 1h + 5m
```

### MCP integration for remote Claude
Configure the HB MCP to be accessible from MacBook Air so Claude on Air can query system state, check positions, and trigger tasks conversationally.

---

## Priority 5: Future Engines

### E3+ development workflow
The architecture supports new engines cleanly:
1. Write evaluation function in `app/engines/e3_*.py` (pure function)
2. Add to `SignalScanTask` engines list
3. Run bulk backtest to populate pair_historical
4. Add to pipeline config

### ML feature store usage
The 7 FeatureBase subclasses write to MongoDB with consistent schema. For ML:
- Query features collection for training data
- Each feature is a document with `timestamp`, `trading_pair`, `value` (dict of floats)
- Join features across types by `(trading_pair, timestamp)` window
- The feature store is extensible — add new FeatureBase subclasses for new signals

---

## What Was Completed This Session

| Item | Status |
|------|--------|
| Fork setup (aloddo/quants-lab) | Done |
| Old repo nuked, PAT revoked | Done |
| MongoDB running + indexed + TTL | Done |
| Engine migration (E1, E2 pure functions) | Done |
| 7 FeatureBase subclasses | Done |
| Feature store (322 features, 46 pairs) | Done |
| Data pipeline (46 pairs, 365 days candles, 42k+ derivatives) | Done |
| SignalScanTask (E1/E2 evaluation) | Done |
| TestnetResolverTask (HB native executors on Bybit demo) | Done |
| BulkBacktestTask + E2 controller | Done (code, not yet run) |
| YAML DAG pipeline | Done |
| Telegram notifications | Done |
| Ops scripts (start, stop, status, kill switch) | Done |
| Portfolio-level position limits | Done |
| Weekly re-optimization cron | Done |
| E2E testing (features, signals, orders) | Done |
| Bybit demo connector via HB native (4 patches) | Done |
| CLAUDE.md + ARCHITECTURE.md | Done |
| Pipeline running in tmux | Done |
| 9 commits pushed | Done |

## Completed Apr 2-3 2026

| Item | Status |
|------|--------|
| Engine registry (`app/engines/registry.py`) | Done — centralizes resolution, candles, exit params |
| E1 bulk backtest (365d, 46 pairs, 5m resolution) | Done — 38 ALLOW, 2173 trades, PF 3.78 |
| E1 stress tests (delay, slippage, expiry, regimes) | Done — all passed including +15bps |
| E1 Monte Carlo (10k sims, block=7) | Done — 0% ruin at 0.3% sizing |
| E1 long/short analysis | Done — both sides positive (L: PF 4.08, S: PF 2.86) |
| entry_quality_filter=False validated | Done ��� 9x more signals, 6x more PnL, PF 4.0 |
| Live/backtest parameter alignment | Done — ATR 0.35, range 30, volume 1.6x |
| Telegram failure alerts (NotifyingTaskMixin) | Done — all 6 pipeline tasks |
| Slippage + latency tracking in resolver | Done — bps, bucket, exec_latency_ms |
| Would-have-won tracking (SKIPPED_CONCURRENCY) | Done |
| CLI scripts (bulk_backtest, stress_test, L/S, MC) | Done |
| Claude Code skills (.claude/skills/) | Done — 4 skills, engine-agnostic |
| BulkBacktestTask reads from registry | Done — no hard-coded resolution/controller |
| Pipeline config simplified (hermes_pipeline.yml) | Done — removed resolution overrides |

## What Was NOT Completed

| Item | Why | Next Step |
|------|-----|-----------|
| E2 bulk backtest + validation | E1 first per governance rules | Priority 1 (next) |
| Feature store refactor (raw values only) | Partial — compression check moved, volume flag still baked | Priority 2 |
| Historical signal replay | Not built | Priority 2 |
| Feed health monitoring task | Not built | Priority 2 |
| Daily summary Telegram | Not built | Priority 3 |
| Incremental features | Optimization, not blocking | Priority 3 |
| Docker patch persistence | Short-term sed works | Priority 4 |
| MCP remote integration | Nice-to-have | Priority 4 |
