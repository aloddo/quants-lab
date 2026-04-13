---
name: system-operations
description: "Operate the live trading pipeline on the Mac Mini server. Covers pipeline DAG, LaunchDaemon supervision, MongoDB collections, exchange client, reconciliation, Bybit demo patches, and remote access. Load when managing the running system."
disable-model-invocation: true
---

# System Operations

## Architecture Overview

```
TaskOrchestrator DAG (33 tasks, config/hermes_pipeline.yml):

Data collection:
  candles_downloader_bybit   (hourly :05, 1h + 1m candles → parquet)
  bybit_derivatives          (*/15 → funding, OI, LS ratio to MongoDB)
  binance_funding            (*/15 → Binance funding rates)
  deribit_options            (*/15 → BTC+ETH options surface)
  coinglass                  (*/15 → liquidations + cross-exchange OI)
  fear_greed                 (daily → sentiment index)

Processing:
  feature_computation        (on_success of data tasks, 8 feature types)

Trading infra:
  trade_recorder             (*/5 → exchange executions, closed PnL, position snapshots)
  telegram_bot               (*/1 → Telegram command handler)
  watchdog                   (*/5 → reconciliation, health checks, auto-heal)

Legacy (E2 only):
  signal_scan                (dependency-triggered)
  testnet_resolver           (*/5 + dependency)

Backtests (all disabled, run via scripts/run_backtest.sh):
  {e1,e2,e3,e4,h2,m1,s6,s7,s9}_{bulk_backtest,walk_forward}
```

## Process Supervision (LaunchDaemon)

Primary supervision via launchd (since 2026-04-10):

```bash
# Check status
sudo launchctl list | grep quantslab

# Restart (launchd auto-restarts within 10s)
sudo launchctl stop com.quantslab.pipeline
sudo launchctl stop com.quantslab.api

# Permanent stop/start
sudo launchctl unload /Library/LaunchDaemons/com.quantslab.pipeline.plist
sudo launchctl load /Library/LaunchDaemons/com.quantslab.pipeline.plist

# Logs
tail -f /tmp/ql-pipeline-launchd.log
tail -f /tmp/ql-api-launchd.log

# Quick restart (kill, launchd restarts automatically)
ps aux | grep "cli.py" | grep -v grep | awk '{print $2}' | xargs kill -9
```

Plist sources: `scripts/launchd/com.quantslab.{pipeline,api}.plist`

## Quick Commands

```bash
# System health
bash /Users/hermes/quants-lab/scripts/status.sh

# Emergency stop
bash /Users/hermes/quants-lab/scripts/kill_switch.sh        # pause + close positions
bash /Users/hermes/quants-lab/scripts/kill_switch.sh --full  # also stop launchd

# Trigger single task
set -a && source /Users/hermes/quants-lab/.env && set +a
/Users/hermes/miniforge3/envs/quants-lab/bin/python cli.py trigger-task \
  --task feature_computation --config config/hermes_pipeline.yml

# Run backtest (isolated, never in live pipeline)
bash scripts/run_backtest.sh e3_bulk_backtest
```

## HB-Native Bot Management

```bash
python cli.py deploy --engine E3                    # all ALLOW pairs
python cli.py deploy --engine E3 --pair ETH-USDT    # single pair
python cli.py deploy --engine E3 --dry-run           # preview
python cli.py deploy --engine E3 --force             # bypass position guard
python cli.py bot-status --engine E3
python cli.py bot-stop --engine E3
```

Pre-deploy guard checks exchange for existing positions. Aborts if overlap found (prevents doubled positions). Use `--force` to override.

## Exchange Client (Source of Truth)

`app/services/bybit_exchange_client.py` — authenticated Bybit V5 REST client.

```python
from app.services.bybit_exchange_client import BybitExchangeClient
exchange = BybitExchangeClient()  # reads BYBIT_DEMO_API_KEY, BYBIT_DEMO_API_SECRET, BYBIT_API_BASE_URL

async with aiohttp.ClientSession() as session:
    positions = await exchange.fetch_positions(session)      # open positions
    orders = await exchange.fetch_order_history(session)     # order history
    fills = await exchange.fetch_executions(session)         # individual fills
    closed = await exchange.fetch_closed_pnl(session)        # round-trip PnL
    wallet = await exchange.fetch_wallet_balance(session)    # equity + available
```

Used by: reconciliation, trade recorder, telegram bot, deploy guard.

## Reconciliation

`app/tasks/resolution/reconciliation.py` — queries exchange directly (not HB API).

Checks:
1. **Untracked positions** — exchange position with no bot controller or candidate
2. **Ghost positions** — MongoDB active but executor gone
3. **Size mismatches** — exchange value >> bot volume (stacked from ghost bots)
4. **Orphan executors** — HB active but not in MongoDB

## Engine Registry

All engine metadata in `app/engines/strategy_registry.py`:

```python
from app.engines.strategy_registry import STRATEGY_REGISTRY, get_strategy, build_backtest_config
meta = get_strategy("E3")
config = build_backtest_config("E3", "bybit_perpetual", "BTC-USDT")
```

## MongoDB Collections

| Collection | Purpose | TTL |
|------------|---------|-----|
| `features` | Feature store (8 types x N pairs) | 90 days |
| `candidates` | Signal candidates (all, even filtered) | 365 days |
| `pair_historical` | Backtest verdicts (ALLOW/WATCH/BLOCK) | none |
| `engine_registry` | Engine metadata | none |
| `bybit_funding_rates` | Bybit funding history | 90 days |
| `binance_funding_rates` | Binance funding rates | 90 days |
| `bybit_open_interest` | OI history | 90 days |
| `bybit_ls_ratio` | L/S ratio history | 90 days |
| `backtest_trades` | Individual backtest trades | none |
| `task_executions` | Task run history | 90 days |
| `deribit_options_surface` | Deribit options snapshots | 180 days |
| `coinglass_liquidations` | Cross-exchange liquidation history | 365 days |
| `coinglass_oi` | Cross-exchange OI OHLC | 365 days |
| `fear_greed_index` | Daily Fear & Greed Index | none |
| `ml_model_registry` | ML training governance reports | none |
| `exchange_executions` | Bybit fills (source of truth), dedup by exec_id | none |
| `exchange_closed_pnl` | Closed round-trips, dedup by order_id+pair | none |
| `paper_trades` | HB API bot history (bot's view) | none |
| `paper_position_snapshots` | Periodic exchange position snapshots | none |
| `paper_controller_stats` | Per-controller HB performance | none |

### Useful Queries

```javascript
// Exchange fills with slippage
db.exchange_executions.find({pair: "BTC-USDT"}).sort({exec_time: -1})

// Closed position PnL
db.exchange_closed_pnl.find().sort({updated_time: -1})

// All resolved positions with PnL
db.candidates.find({disposition: "RESOLVED_TESTNET"}, {engine:1, pair:1, direction:1, testnet_pnl:1, testnet_close_type:1})

// Active candidates
db.candidates.find({disposition: "TESTNET_ACTIVE"})

// Pair verdicts
db.pair_historical.find({engine: "E3"}).sort({profit_factor: -1})
```

## Portfolio Limits

- Max 3 concurrent positions across all engines
- Max 2% total capital exposure
- 0.3% per position ($300 on $100k demo)
- Per-engine limits in `config/hermes_pipeline.yml`

## Bybit Demo Patches

HB API Docker container needs patches after any rebuild:

```bash
PKG="/opt/conda/envs/hummingbot-api/lib/python3.12/site-packages/hummingbot/connector/derivative/bybit_perpetual"

# 1. URL patches (REST + WebSocket)
docker exec hummingbot-api sed -i 's|api-testnet.bybit.com|api-demo.bybit.com|' "$PKG/bybit_perpetual_constants.py"
docker exec hummingbot-api sed -i 's|stream-testnet.bybit.com/v5/public|stream.bybit.com/v5/public|' "$PKG/bybit_perpetual_constants.py"
docker exec hummingbot-api sed -i 's|stream-testnet.bybit.com/v5/private|stream-demo.bybit.com/v5/private|' "$PKG/bybit_perpetual_constants.py"

# 2. Rate limit fix (must match pair allowlist)
docker exec hummingbot-api sed -i 's|return web_utils.build_rate_limits(self.trading_pairs)|pairs = self.trading_pairs or ["BTC-USDT","ETH-USDT","SOL-USDT","XRP-USDT","DOGE-USDT","ADA-USDT","AVAX-USDT","LINK-USDT","DOT-USDT","UNI-USDT","NEAR-USDT","APT-USDT","ARB-USDT","OP-USDT","SUI-USDT","SEI-USDT","WLD-USDT","LTC-USDT","BCH-USDT","BNB-USDT","CRV-USDT","1000PEPE-USDT","ALGO-USDT","GALA-USDT","ONT-USDT","TAO-USDT","ZEC-USDT"]; return web_utils.build_rate_limits(pairs)|' "$PKG/bybit_perpetual_derivative.py"

# 3. Position mode fix
docker exec hummingbot-api sed -i "s|if self.position_mode == PositionMode.ONEWAY:|if self.position_mode == PositionMode.ONEWAY or 'testnet' in str(self._domain):|" "$PKG/bybit_perpetual_derivative.py"

docker exec hummingbot-api rm -rf "$PKG/__pycache__"
docker exec hummingbot-api find /opt/conda/envs/hummingbot-api/lib/python3.12/site-packages/hummingbot/connector -name "__pycache__" -exec rm -rf {} + 2>/dev/null
docker restart hummingbot-api
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
- **LS ratio history is limited** -- Bybit API only returns ~17 days.
- **DerivativesFeature needs 50+ docs** per collection for time-series indicators (RSI, z-score).
- **Colima Docker FS** -- dual-home mounts needed. See `project_colima_fixed` memory.
