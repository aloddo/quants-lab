# CLAUDE.md — Project Instructions

## What is this repo

Fork of `hummingbot/quants-lab` with a systematic crypto perpetual futures trading system built on QuantsLab primitives. 7 registered strategies (E1-E4, S6, S7, S9) at various validation stages. Paper trading on Bybit demo (api-demo.bybit.com) via Hummingbot API. All backtesting uses 1m resolution with derivatives data (funding, OI, LS ratio) merged from MongoDB.

Owner: Alberto Loddo (GitHub: aloddo)

## Repo structure

```
quants-lab/                          # Fork of hummingbot/quants-lab
├── core/                            # UPSTREAM — do not modify. QL framework.
├── app/
│   ├── engines/                     # Strategy factory
│   │   ├── strategy_registry.py     # StrategyMetadata + STRATEGY_REGISTRY (source of truth)
│   │   ├── models.py                # CandidateBase, DecisionSnapshot, FeatureRow
│   │   ├── e1_compression_breakout.py  # E1 eval function + E1Candidate
│   │   ├── e2_range_fade.py         # E2 eval function + E2Candidate
│   │   ├── pair_selector.py         # Pair ranking from pair_historical
│   │   └── registry.py              # DEPRECATED — re-exports from strategy_registry
│   ├── features/                    # 8 FeatureBase subclasses → MongoDB (incl. derivatives w/ OI RSI, funding z-score, LS z-score)
│   ├── services/                    # Bybit REST client, HB API client
│   ├── controllers/directional_trading/  # HB V2 controllers (backtest + live) — E1-E4, S6, S7, S9
│   ├── tasks/
│   │   ├── data_collection/         # CandlesDownloader, BybitDerivativesTask
│   │   ├── screening/               # FeatureComputationTask, SignalScanTask
│   │   ├── resolution/              # TestnetResolverTask
│   │   └── backtesting/             # BulkBacktestTask, WalkForwardTask
│   └── data/cache/candles/          # Parquet files (gitignored)
├── config/hermes_pipeline.yml       # YAML DAG — the full pipeline
├── scripts/                         # start_pipeline.sh, kill_switch.sh, status.sh
├── tests/                           # pytest tests
└── .env                             # Secrets — gitignored
```

## Key rules

1. **Never modify `core/`** — that's upstream QuantsLab. Fetch updates with `git fetch upstream && git merge upstream/main`.
2. **Always use QL primitives** — FeatureBase for features, BaseTask for tasks, TaskOrchestrator for scheduling. No custom cron scripts, no SQLite.
3. **The controller IS the strategy** — one self-contained HB V2 controller file runs in both backtest (BacktestingEngine) and live (HB bot container). No separate eval functions, no custom signal pipelines.
4. **Controllers must be self-contained** — only import from `hummingbot.*`, `pandas`, `pandas_ta`, `numpy`, `pydantic`, stdlib. No `app.*` imports. Inline any helpers.
5. **Use the Strategy Registry** — all engine metadata lives in `app/engines/strategy_registry.py`. Every engine has `deployment_mode="hb_native"`. Add new engines via `python cli.py scaffold-strategy`.
6. **MongoDB for everything dynamic** — features, candidates, pair_historical, derivatives, task executions.
7. **Parquet for candle data** — written by CandlesDownloaderTask via CLOBDataSource.
8. **Telegram bot token and chat IDs are in `.env`** — never commit secrets.

## Strategy lifecycle (HB-native)

**This is the only way to create, validate, and deploy strategies.**

```
IDEA → CONTROLLER → BACKTEST → WALK-FORWARD → DEPLOY (one command)
```

### 1. Scaffold
```bash
python cli.py scaffold-strategy --name E3 --display "Mean Reversion RSI"
```
Generates a self-contained HB V2 controller. No eval function, no quants-lab imports.

### 2. Implement
Edit `app/controllers/directional_trading/e3_mean_reversion_rsi.py`:
- `update_processed_data()` — compute features from candle feeds, emit signal
- `get_executor_config()` — build executor with dynamic TP/SL

### 3. Register
Add `StrategyMetadata` to `STRATEGY_REGISTRY` with `deployment_mode="hb_native"`.

### 4. Backtest + Validate
```bash
python cli.py trigger-task --task e3_bulk_backtest --config config/hermes_pipeline.yml
python cli.py trigger-task --task e3_walk_forward --config config/hermes_pipeline.yml
```
Same controller code. Same `update_processed_data()`. Gates per quant-governance.

### 5. Deploy
```bash
python cli.py deploy --engine E3                    # all ALLOW pairs
python cli.py deploy --engine E3 --pair ETH-USDT    # single pair test
python cli.py deploy --engine E3 --dry-run           # preview configs
python cli.py bot-status --engine E3                 # check status
python cli.py bot-stop --engine E3                   # stop bot
```
Deploys the controller as an HB bot container with WebSocket candle feeds. HB handles signal detection, executor creation, and position lifecycle natively.

### Custom Docker image
Bot containers use `quants-lab/hummingbot:demo` (Bybit demo patches baked in). Rebuild after HB version updates:
```bash
docker build -f /tmp/Dockerfile.hb-demo -t quants-lab/hummingbot:demo .
```

## Environment

- **Server**: Always-on Mac Mini, SSH via Tailscale
- **Python**: `/Users/hermes/miniforge3/envs/quants-lab/bin/python` (3.12, conda)
- **MongoDB**: `mongodb://localhost:27017/quants_lab` (no auth, local only)
- **HB API**: `http://localhost:8000` (admin/admin)
- **Env vars needed**: `MONGO_URI`, `MONGO_DATABASE`, `TELEGRAM_ENABLED`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `COINGLASS_API_KEY`
- **All env vars are in** `/Users/hermes/quants-lab/.env`

## Running commands

Always prefix Python commands with env vars:
```bash
MONGO_URI=mongodb://localhost:27017/quants_lab MONGO_DATABASE=quants_lab \
  /Users/hermes/miniforge3/envs/quants-lab/bin/python <script>
```

Or source the .env first:
```bash
set -a && source /Users/hermes/quants-lab/.env && set +a
```

## Process supervision (resilience)

The pipeline runs under **three layers of protection** so a crash never means lost money:

### Layer 1 — LaunchDaemon (primary, 2026-04-10)
`/Library/LaunchDaemons/com.quantslab.pipeline.plist` and `com.quantslab.api.plist`
- `KeepAlive: true` — launchd restarts the process within 10s of any crash
- `RunAtLoad: true` — starts automatically on machine reboot
- Runs as `hermes` user, no GUI session needed
- Source plists: `scripts/launchd/com.quantslab.pipeline.plist` and `com.quantslab.api.plist`

```bash
# Manage via (as admin/root):
sudo launchctl list | grep quantslab          # check status
sudo launchctl stop com.quantslab.pipeline    # stop (will restart in 10s)
sudo launchctl unload /Library/LaunchDaemons/com.quantslab.pipeline.plist  # permanent stop
sudo launchctl load /Library/LaunchDaemons/com.quantslab.pipeline.plist    # re-enable

# Logs:
tail -f /tmp/ql-pipeline-launchd.log
tail -f /tmp/ql-api-launchd.log

# Reinstall after plist changes:
sudo cp scripts/launchd/com.quantslab.pipeline.plist /Library/LaunchDaemons/
sudo cp scripts/launchd/com.quantslab.api.plist /Library/LaunchDaemons/
sudo chown root:wheel /Library/LaunchDaemons/com.quantslab.*.plist
sudo launchctl unload /Library/LaunchDaemons/com.quantslab.pipeline.plist && sudo launchctl load /Library/LaunchDaemons/com.quantslab.pipeline.plist
sudo launchctl unload /Library/LaunchDaemons/com.quantslab.api.plist && sudo launchctl load /Library/LaunchDaemons/com.quantslab.api.plist
```

### Layer 2 — Restart loops in start_pipeline.sh (fallback)
`scripts/start_pipeline.sh` runs both processes inside `while true; do ... sleep 10; done` loops.
Use this if launchd is not available (e.g., fresh machine setup before daemon install):
```bash
bash /Users/hermes/quants-lab/scripts/start_pipeline.sh
```

### Layer 3 — Backtest isolation (crash prevention)
**Backtests are DISABLED in hermes_pipeline.yml** (`e1_bulk_backtest`, `e2_bulk_backtest`, `e1_walk_forward`, `e2_walk_forward` all `enabled: false`).
Heavy backtests run in a separate process, never sharing memory with the live trading pipeline:
```bash
bash scripts/run_backtest.sh e1_bulk_backtest   # safe, isolated
bash scripts/run_backtest.sh e1_walk_forward
```

## Common operations

### Check system status
```bash
bash /Users/hermes/quants-lab/scripts/status.sh
```

### Emergency stop
```bash
bash /Users/hermes/quants-lab/scripts/kill_switch.sh        # pause + close positions
bash /Users/hermes/quants-lab/scripts/kill_switch.sh --full  # also stop launchd services
```

### Trigger a single task
```bash
MONGO_URI=... python cli.py trigger-task --task feature_computation --config config/hermes_pipeline.yml
```

### Run a backtest (isolated — never in the live pipeline)
```bash
bash scripts/run_backtest.sh e1_bulk_backtest
bash scripts/run_backtest.sh e1_walk_forward
```

### Push to GitHub
The hermes user doesn't have persistent git auth. Push by temporarily setting the token:
```bash
git remote set-url origin https://aloddo:<GH_TOKEN>@github.com/aloddo/quants-lab.git
git push origin main
git remote set-url origin https://github.com/aloddo/quants-lab.git  # clean up immediately
```

## MongoDB collections

| Collection | Purpose | TTL |
|------------|---------|-----|
| `features` | Feature store (7 types × 46 pairs) | 90 days |
| `candidates` | Signal candidates (all, even filtered) | 365 days |
| `pair_historical` | Backtest verdicts (ALLOW/WATCH/BLOCK) | none |
| `engine_registry` | Engine metadata | none |
| `bybit_funding_rates` | Funding rate history | 90 days |
| `binance_funding_rates` | Binance funding rates (cross-exchange spread) | 90 days |
| `bybit_open_interest` | OI history | 90 days |
| `bybit_ls_ratio` | Long/short ratio history | 90 days |
| `backtest_trades` | Individual backtest trades (indexed by engine+pair, run_id) | none |
| `task_executions` | QL task execution history | 90 days |
| `deribit_options_surface` | Raw Deribit options snapshots (BTC+ETH, every 15min) | 180 days |
| `coinglass_liquidations` | Cross-exchange liquidation history | 365 days |
| `coinglass_oi` | Cross-exchange OI OHLC | 365 days |
| `fear_greed_index` | Daily Fear & Greed Index (since 2018) | none |
| `ml_model_registry` | ML training governance reports | none |
| `exchange_executions` | Bybit fills (source of truth), dedup by `exec_id` | none |
| `exchange_closed_pnl` | Closed position round-trips, dedup by `order_id`+`pair` | none |
| `paper_trades` | HB API bot history (bot's view of fills) | none |
| `paper_position_snapshots` | Periodic exchange position snapshots | none |
| `paper_controller_stats` | Per-controller performance from HB orchestration | none |

## Architecture: QL data pipeline + HB-native bots

```
QL Data Pipeline (TaskOrchestrator, hermes_pipeline.yml):
  candles_downloader_bybit  (hourly at :05, 1h + 1m candles)
  bybit_derivatives         (every 15 min → funding, OI, LS ratio to MongoDB)
  binance_funding           (every 15 min → Binance funding rates to MongoDB)
  deribit_options           (every 15 min → BTC+ETH options surface to MongoDB)
  coinglass                 (every 15 min → liquidations + cross-exchange OI)
  fear_greed                (daily → sentiment index)
      ↓ (both on_success)
  feature_computation       (dependency-triggered, 8 feature types incl. derivatives + microstructure)
      ↓ (feeds analytics/monitoring only)

HB-Native Bots (one Docker container per engine):
  Any engine: WebSocket feeds → Controller.update_processed_data() → auto executor
  Deploy: python cli.py deploy --engine E1
  Status: python cli.py bot-status --engine E1
  Stop:   python cli.py bot-stop --engine E1

Backtesting (isolated, never in live pipeline):
  All engines: bulk_backtest + walk_forward tasks (all `enabled: false` in pipeline)
  Run via: bash scripts/run_backtest.sh <task_name>
  Resolution: 1m (all engines, changed Apr 2026)
  Derivatives merge: BulkBacktestTask._merge_derivatives_into_candles() queries MongoDB
    for funding_rate, oi_value, buy_ratio, binance_funding_rate → reindexes to candle
    timestamps with ffill → injected as columns for engines with required_features: ["derivatives"]
  Signals: controllers must compute vectorized signal columns in update_processed_data()
    (BacktestingEngine calls it once, reads signal from processed_data["features"] DataFrame)
```

**Legacy tasks (E2 only, until migrated):** signal_scan, testnet_resolver

## Engines and validation status

All engine metadata lives in `app/engines/strategy_registry.py`. All backtests run at **1m resolution** (changed Apr 12, 2026 — prior results at 1h are invalidated by different SL mechanics).

| Engine | Thesis | Status | Notes |
|--------|--------|--------|-------|
| **E1** Compression Breakout | Low ATR + range breakout | **Abandoned** (1h backtest: 91% TL exits). Needs re-eval at 1m. | `e1_compression_breakout.py` |
| **E2** Range Fade | Low ATR + boundary rejection, LONG ONLY | **ADX filter coded, not yet backtested at 1m** | `e2_range_fade.py` |
| **E3** Funding Carry | Fade persistent funding direction | **Mild edge on majors** (XRP 1.21, SUI 1.18, ADA 1.10). OI filter adds +0.03-0.07 PF on 6/8 pairs. Best candidate for further tuning. | `e3_funding_carry.py` |
| **E4** Crowd Fade | Fade LS ratio extremes + OI rising | **Shelved** (PF ~1.0, no alpha). Vectorized signal fix shipped. | `e4_crowd_fade.py` |
| **H2** Funding Divergence | Fade Bybit-Binance funding spread z-score | **Dead** (365d, 43 pairs, 11K trades, 0 ALLOW). Governance FAILED: positive expectancy only in SHOCK regime. SL drag destroys trailing stop edge. | `h2_funding_divergence.py` |
| **S6** Spread Fade | Alt-BTC spread mean reversion | **Dead at 1h** (PF 0.79-0.92). Untested at 1m. | `s6_spread_fade.py` |
| **S7** Hurst Adaptive | Hurst regime + RSI/EMA | **Dead at 1h** (69K trades, all PF<1.06). Untested at 1m. | `s7_hurst_adaptive.py` |
| **S9** Session Compression | Session-aware ATR breakout | **Dead at 1h** (48% SL, 42% TL). Untested at 1m. | `s9_session_compression.py` |
| **M1** ML Ensemble Signal | XGBoost on microstructure + derivatives + options + liquidation features | **Dead** (v1+v2: 0 ALLOW across 35 pairs, 6503 trades. Global model doesn't generalize — signal counts vary 15-6770 per pair at same threshold.) | `m1_ml_ensemble.py` |

**Important:** E1, S6, S7, S9 were all tested at 1h resolution. The switch to 1m resolution changes SL mechanics fundamentally (intra-bar noise is 60x smaller), so these verdicts may not hold. E3/E4 had a scalar signal bug (producing 0 trades) that was fixed with vectorized `_compute_signals_vectorized()` — first correct E3 run showed DOGE PF=1.40/65% WR.

Strategies tested outside the registry (pandas-only or one-off, all dead): S1 Crowded Fragility, S2 Funding Mean Rev, S3 OI-Price Divergence (inconclusive, N=5), S4 BTC Lead-Lag, S5 Volume Ignition (Sharpe 6.22 in pandas, 0-12% WR in HB at 1h), S8 Funding Carry+Momentum.

### Portfolio limits
- Max 3 concurrent positions across all engines
- Max 2% total capital exposure
- Position size: 0.3% of capital per trade ($300 on $100k demo)

## Position History & Analysis

All candidates (including filtered ones) are stored in the `candidates` collection.
Resolved positions have these additional fields:

```
testnet_placed_at          — timestamp when order was sent to HB API
testnet_amount             — position size in base asset
testnet_amount_usd         — position size in USD
executor_id                — HB executor ID for tracking
testnet_resolved_at        — timestamp when position closed
testnet_fill_price         — actual entry price
testnet_exit_price         — actual exit price
testnet_pnl                — realized PnL in quote
testnet_close_type         — TP, SL, TIME_LIMIT, FAILED, etc.
testnet_filled_amount_quote — filled amount in quote
testnet_status             — final executor status
testnet_raw_result         — full HB API response (for debugging)
```

### Useful MongoDB queries for analysis

```javascript
// All resolved positions with PnL
db.candidates.find({disposition: "RESOLVED_TESTNET"}, {engine:1, pair:1, direction:1, testnet_pnl:1, testnet_close_type:1, testnet_fill_price:1, testnet_exit_price:1})

// Win rate per engine
db.candidates.aggregate([
  {$match: {disposition: "RESOLVED_TESTNET"}},
  {$group: {_id: "$engine", total: {$sum: 1}, wins: {$sum: {$cond: [{$gt: ["$testnet_pnl", 0]}, 1, 0]}}}}
])

// All active positions right now
db.candidates.find({disposition: "TESTNET_ACTIVE"})

// Candidates that triggered but were filtered (for strategy analysis)
db.candidates.find({trigger_fired: true, disposition: {$ne: "CANDIDATE_READY"}})

// All E1 candidates for a specific pair
db.candidates.find({engine: "E1", pair: "XRP-USDT"}).sort({timestamp_utc: -1})
```

## Remote Access (from MacBook Air via Tailscale)

```bash
# SSH in
ssh hermes@<tailscale-ip>

# Check system health
bash /Users/hermes/quants-lab/scripts/status.sh

# Watch pipeline logs live
tmux attach -t ql-pipeline
# Detach: Ctrl+B then D

# Watch QL API logs
tmux attach -t ql-api

# Check QL API endpoints
curl http://localhost:8001/health
curl http://localhost:8001/tasks

# Pull latest code
cd /Users/hermes/quants-lab && git pull

# Run Jupyter for backtesting
cd /Users/hermes/quants-lab
MONGO_URI=mongodb://localhost:27017/quants_lab MONGO_DATABASE=quants_lab \
  /Users/hermes/miniforge3/envs/quants-lab/bin/jupyter lab --port 8888 --no-browser
# Then tunnel: ssh -L 8888:localhost:8888 hermes@<tailscale-ip>
# Or access directly if Tailscale exposes the port

# Emergency stop
bash /Users/hermes/quants-lab/scripts/kill_switch.sh
bash /Users/hermes/quants-lab/scripts/kill_switch.sh --full  # also kills tmux

# Restart pipeline after stop
bash /Users/hermes/quants-lab/scripts/start_pipeline.sh
```

### tmux sessions

| Session | Purpose | Attach |
|---------|---------|--------|
| `ql-pipeline` | TaskOrchestrator DAG | `tmux attach -t ql-pipeline` |
| `ql-api` | QL Task API on :8001 | `tmux attach -t ql-api` |
| `hummingbot` | HB instance | `tmux attach -t hummingbot` |
| `gateway` | HB gateway | `tmux attach -t gateway` |

## Gotchas

- **CandlesDownloaderTask writes parquet only at the end** — if killed mid-run, all data is lost. The scheduled task uses 7-day retention (fast). Use separate backfill scripts for historical data.
- **Feature TTL is 90 days** — if you need longer history for backtesting, query parquet directly, don't rely on MongoDB features.
- **The `timestamp` field in derivatives collections uses milliseconds** (Bybit convention), but QL features use Python datetime objects. Watch for unit mismatches.
- **`core/` FeatureStorage uses `insert_many`** which creates duplicates. Our FeatureComputationTask uses upsert instead — don't use FeatureStorage.save_features() directly.
- **Bybit demo trading** uses `bybit_perpetual_testnet` connector patched to point at `api-demo.bybit.com`. Three patches required inside the Docker container. **IMPORTANT: use `sed` for patching** (Python file I/O doesn't persist reliably in Docker overlay FS). Re-apply after any container rebuild:
  ```bash
  PKG="/opt/conda/envs/hummingbot-api/lib/python3.12/site-packages/hummingbot/connector/derivative/bybit_perpetual"

  # 1. URL patches (REST + WebSocket) — public WS must point to mainnet (demo has no public WS)
  docker exec hummingbot-api sed -i 's|api-testnet.bybit.com|api-demo.bybit.com|' "$PKG/bybit_perpetual_constants.py"
  docker exec hummingbot-api sed -i 's|stream-testnet.bybit.com/v5/public|stream.bybit.com/v5/public|' "$PKG/bybit_perpetual_constants.py"
  docker exec hummingbot-api sed -i 's|stream-testnet.bybit.com/v5/private|stream-demo.bybit.com/v5/private|' "$PKG/bybit_perpetual_constants.py"

  # 2. Rate limit fix — executor creation bypasses add_market(), so the throttler never gets
  #    per-pair rate limit pools. This patch provides defaults matching the pair allowlist.
  #    MUST match pairs in MongoDB pair_historical (verdict=ALLOW). If you add a new pair to
  #    the allowlist, add it here too or it will fail with "Failed to submit BUY order".
  docker exec hummingbot-api sed -i 's|return web_utils.build_rate_limits(self.trading_pairs)|pairs = self.trading_pairs or ["BTC-USDT","ETH-USDT","SOL-USDT","XRP-USDT","DOGE-USDT","ADA-USDT","AVAX-USDT","LINK-USDT","DOT-USDT","UNI-USDT","NEAR-USDT","APT-USDT","ARB-USDT","OP-USDT","SUI-USDT","SEI-USDT","WLD-USDT","LTC-USDT","BCH-USDT","BNB-USDT","CRV-USDT","1000PEPE-USDT","ALGO-USDT","GALA-USDT","ONT-USDT","TAO-USDT","ZEC-USDT"]; return web_utils.build_rate_limits(pairs)|' "$PKG/bybit_perpetual_derivative.py"

  # 3. Position mode fix (demo set-position-mode returns empty, force one-way)
  docker exec hummingbot-api sed -i "s|if self.position_mode == PositionMode.ONEWAY:|if self.position_mode == PositionMode.ONEWAY or 'testnet' in str(self._domain):|" "$PKG/bybit_perpetual_derivative.py"

  # Clear cache and restart
  docker exec hummingbot-api rm -rf "$PKG/__pycache__"
  docker exec hummingbot-api find /opt/conda/envs/hummingbot-api/lib/python3.12/site-packages/hummingbot/connector -name "__pycache__" -exec rm -rf {} + 2>/dev/null
  docker restart hummingbot-api

  # Then add demo credentials
  curl -s -u admin:admin -X POST "http://localhost:8000/accounts/add-credential/master_account/bybit_perpetual_testnet" \
    -H "Content-Type: application/json" \
    -d '{"bybit_perpetual_testnet_api_key":"YOUR_DEMO_KEY","bybit_perpetual_testnet_secret_key":"YOUR_DEMO_SECRET"}'
  ```
  **PAIR ALLOWLIST:** Only trade pairs listed in the rate limiter patch above. The allowlist is mirrored in MongoDB `pair_historical` (verdict=ALLOW). Micro-cap / newly-listed pairs (PIPPIN, PUMPFUN, TRUMP, etc.) are permanently BLOCKED — they must be in BOTH the rate limiter patch AND pair_historical to work. Never add a pair to one without the other.
- **Demo account capital** is ~$100k (virtual). `fallback_capital` in pipeline config is set to 100000. If HB API can't fetch portfolio state (404 on `/portfolio/overview`), it uses this fallback. 0.3% = $300 per position.
- **Bybit demo executor quirks**: The position executor may need position mode set to one-way. If executors keep failing with max retries, run via HB MCP: "Set position mode to one-way for BTC-USDT on bybit_perpetual_demo".
- **Backtesting resolution is 1m for all engines** (Apr 2026). Prior results at 1h are invalidated — the BacktestingEngine checks SL on bar LOW/HIGH (intra-bar), which behaves fundamentally differently at 1m vs 1h. Don't trust old 1h verdicts.
- **Controllers must compute vectorized signals.** The BacktestingEngine calls `update_processed_data()` once for the full DataFrame and reads the signal column. Scalar signal assignment produces 0 trades. Use `_compute_signals_vectorized()` pattern.
- **Derivatives merge for backtesting** requires `required_features: ["derivatives"]` in the strategy registry. `_merge_derivatives_into_candles()` queries MongoDB, forward-fills to candle timestamps, and fills remaining NaN with neutral values (funding_rate=0, oi_value=bfill then 0, buy_ratio=0.5). This is critical because HB's `BacktestingEngineBase.prepare_market_data()` calls `dropna(inplace=True)` (how='any') — any NaN in any column kills the row. The neutral fills ensure no data loss from cross-collection coverage gaps.
- **DerivativesFeature needs 50+ docs** per MongoDB collection for time-series indicators (OI RSI, funding z-score, LS z-score). The `HISTORY_DEPTH = 50` constant controls this. With fewer docs, RSI/z-score fields will be NaN.
- **LS ratio history is limited** — Bybit API only returns ~17 days. Funding has 408 days, OI 367 days. LS-dependent strategies (E4) have shallow backtest windows.
- **Git push requires token** — hermes user has no credential helper. Use the temporary URL method documented above.
