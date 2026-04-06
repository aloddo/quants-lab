---
name: system-operations
description: "Operate the live trading pipeline on the Mac Mini server. Covers pipeline DAG, tmux sessions, MongoDB queries, Bybit demo patches, emergency procedures, and remote access. Load when managing the running system."
disable-model-invocation: true
---

# System Operations

## Architecture Overview

```
TaskOrchestrator DAG (tmux: ql-pipeline)
  candles_downloader_bybit  (hourly :05)
  bybit_derivatives         (every 15 min)
      -> feature_computation (dependency-triggered)
          -> signal_scan     (dependency-triggered)
              -> testnet_resolver (every 5 min + dependency)
  bulk backtests per engine  (weekly Sunday, staggered)
```

## Quick Commands

```bash
# System health
bash /Users/hermes/quants-lab/scripts/status.sh

# Start pipeline
bash /Users/hermes/quants-lab/scripts/start_pipeline.sh

# Emergency stop
bash /Users/hermes/quants-lab/scripts/kill_switch.sh        # pause + close positions
bash /Users/hermes/quants-lab/scripts/kill_switch.sh --full  # also kill tmux

# Trigger single task
set -a && source /Users/hermes/quants-lab/.env && set +a
/Users/hermes/miniforge3/envs/quants-lab/bin/python cli.py trigger-task \
  --task feature_computation --config config/hermes_pipeline.yml
```

## tmux Sessions

| Session | Purpose | Attach |
|---------|---------|--------|
| `ql-pipeline` | TaskOrchestrator DAG | `tmux attach -t ql-pipeline` |
| `ql-api` | QL Task API on :8001 | `tmux attach -t ql-api` |
| `hummingbot` | HB instance | `tmux attach -t hummingbot` |
| `gateway` | HB gateway | `tmux attach -t gateway` |

Detach: Ctrl+B then D

## Engine Registry

All engine metadata (resolution, params, exit config) is in `app/engines/registry.py`.
Query registered engines:
```python
from app.engines.registry import ENGINE_REGISTRY
for name, meta in ENGINE_REGISTRY.items():
    print(f"{name}: {meta['name']} ({meta['architecture']}, res={meta['backtesting_resolution']})")
```

## MongoDB Collections

| Collection | Purpose | TTL |
|------------|---------|-----|
| `features` | Feature store (7 types x N pairs) | 90 days |
| `candidates` | All signal candidates (even filtered) | 365 days |
| `pair_historical` | Backtest verdicts (ALLOW/WATCH/BLOCK) | none |
| `bybit_funding_rates` | Funding history | 90 days |
| `bybit_open_interest` | OI history | 90 days |
| `bybit_ls_ratio` | L/S ratio history | 90 days |
| `task_executions` | Task run history | 90 days |

### Useful Queries

```javascript
// All resolved positions with PnL
db.candidates.find({disposition: "RESOLVED_TESTNET"}, {engine:1, pair:1, direction:1, testnet_pnl:1, testnet_close_type:1})

// Win rate per engine
db.candidates.aggregate([
  {$match: {disposition: "RESOLVED_TESTNET"}},
  {$group: {_id: "$engine", total: {$sum: 1}, wins: {$sum: {$cond: [{$gt: ["$testnet_pnl", 0]}, 1, 0]}}}}
])

// Active positions
db.candidates.find({disposition: "TESTNET_ACTIVE"})

// Pair verdicts sorted by PF
db.pair_historical.find({engine: "E1"}).sort({profit_factor: -1})

// Feature freshness
db.features.find({}, {feature_name:1, trading_pair:1, timestamp:1}).sort({timestamp:-1}).limit(10)
```

## Portfolio Limits

- Max 3 concurrent positions across all engines
- Max 2% total capital exposure
- 0.3% per position
- Fallback capital: $100k (Bybit demo)
- Per-engine limits configured in `config/hermes_pipeline.yml`

## Bybit Demo Patches

The HB API Docker container needs 3 patches after any rebuild:

```bash
PKG="/opt/conda/envs/hummingbot-api/lib/python3.12/site-packages/hummingbot/connector/derivative/bybit_perpetual"

# 1. URL patches (REST + WebSocket)
docker exec hummingbot-api sed -i 's|api-testnet.bybit.com|api-demo.bybit.com|' "$PKG/bybit_perpetual_constants.py"
docker exec hummingbot-api sed -i 's|stream-testnet.bybit.com/v5/public|stream.bybit.com/v5/public|' "$PKG/bybit_perpetual_constants.py"
docker exec hummingbot-api sed -i 's|stream-testnet.bybit.com/v5/private|stream-demo.bybit.com/v5/private|' "$PKG/bybit_perpetual_constants.py"

# 2. Position mode fix
docker exec hummingbot-api sed -i "s|if self.position_mode == PositionMode.ONEWAY:|if self.position_mode == PositionMode.ONEWAY or 'testnet' in str(self._domain):|" "$PKG/bybit_perpetual_derivative.py"

docker exec hummingbot-api rm -rf "$PKG/__pycache__" && docker restart hummingbot-api
```

## Git Push (no persistent auth)

```bash
git remote set-url origin https://aloddo:<GH_TOKEN>@github.com/aloddo/quants-lab.git
git push origin main
git remote set-url origin https://github.com/aloddo/quants-lab.git
```

## Known Issues

- **CandlesDownloaderTask** writes parquet at the END -- kill mid-run = data lost.
- **Feature TTL 90 days** -- for longer backtest history, query parquet directly.
- **Derivatives timestamps** use milliseconds (Bybit), features use Python datetime objects.
- **FeatureStorage.save_features()** creates duplicates -- our task uses upsert instead.
