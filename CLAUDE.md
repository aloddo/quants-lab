# CLAUDE.md — Project Instructions

## What is this repo

Fork of `hummingbot/quants-lab` with a systematic crypto trading system. Two execution modes: HB-native controllers (backtest + live via Docker) and custom Python scripts (H2 V3 arb engine). Trading on Bybit demo (api-demo.bybit.com) + Binance spot. All backtesting uses 1m resolution with derivatives data (funding, OI, LS ratio) merged from MongoDB.

**For current strategy status, live positions, and roadmap:** see `~/albertos-kb/projects/quant/overview.md` (canonical source of truth, updated every session).

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
│   ├── services/                    # Bybit REST client, HB API client, arb engine
│   │   └── arb/                     # H2 V3 spike-fade arb engine (20+ modules)
│   │       ├── signal_engine.py     # Spread spike detection
│   │       ├── risk_manager.py      # Position/daily limits, proportional to size
│   │       ├── price_feed.py        # Dual-exchange WS price feeds
│   │       ├── inventory_ledger.py  # Position tracking + PnL
│   │       ├── order_gateway.py     # IOC order execution
│   │       ├── fill_detector.py     # Fill confirmation
│   │       └── tier_engine.py       # Pair selection/rotation
│   ├── controllers/directional_trading/  # HB V2 controllers (backtest + live)
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
3. **Two execution modes:**
   - **HB-native controllers** (E1-E4, S6-S9, X10, X12): self-contained controller file runs in both backtest and live Docker container. Only import from `hummingbot.*`, `pandas`, `pandas_ta`, `numpy`, `pydantic`, stdlib.
   - **Custom scripts** (H2 V3): standalone Python script in `scripts/`, uses `app/services/arb/` modules. For strategies that need cross-exchange execution or low-latency arb logic that HB controllers can't express.
4. **Use the Strategy Registry** — all HB-native engine metadata lives in `app/engines/strategy_registry.py`. Custom scripts (H2) manage their own lifecycle.
5. **MongoDB for everything dynamic** — features, candidates, pair_historical, derivatives, task executions.
6. **Parquet for candle data** — written by CandlesDownloaderTask via CLOBDataSource.
7. **Telegram bot token and chat IDs are in `.env`** — never commit secrets.

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

## Architecture: QL data pipeline + dual execution

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

Execution Mode 1 — Custom Scripts (H2 V3 arb):
  scripts/arb_h2_live_v3.py → app/services/arb/* (20+ modules)
  Runs in tmux (ql-h2-v3), not Docker. Direct Bybit+Binance API.
  Telegram commands: /h2live /h2stats /h2pause /h2resume

Execution Mode 2 — HB-Native Bots (X10, X12, etc.):
  One Docker container per engine, WebSocket candle feeds, auto executor
  Deploy: python cli.py deploy --engine X12
  Status: python cli.py bot-status --engine X12
  Stop:   python cli.py bot-stop --engine X12

Data Collectors (standalone tmux sessions):
  d1-whale-collector    — Hyperliquid whale positions (every 15min)
  arb-dual-collector    — Bitvavo+Binance spread snapshots

Backtesting (isolated, never in live pipeline):
  All HB-native engines: bulk_backtest + walk_forward tasks (all disabled in pipeline)
  Run via: bash scripts/run_backtest.sh <task_name>
  Resolution: 1m (all engines, changed Apr 2026)
  Derivatives merge: BulkBacktestTask._merge_derivatives_into_candles() queries MongoDB
    for funding_rate, oi_value, buy_ratio, binance_funding_rate → reindexes to candle
    timestamps with ffill → injected as columns for engines with required_features: ["derivatives"]
  Signals: controllers must compute vectorized signal columns in update_processed_data()
```

## Strategy status

**Canonical source:** `~/albertos-kb/projects/quant/overview.md` — updated every session with strategy status, kanban, roadmap, and kill evidence. Do NOT maintain a duplicate table here.

**Session handoff:** `~/albertos-kb/handoffs/quant-engineer.md` — last session state, findings, pending work.

HB-native engine metadata (for backtesting/deploy tooling) lives in `app/engines/strategy_registry.py`. All backtests run at **1m resolution** (changed Apr 12, 2026 — prior results at 1h are invalidated).

**Key architectural notes:**
- E3/E4 had a scalar signal bug (producing 0 trades) that was fixed with vectorized `_compute_signals_vectorized()`.
- E1, S6, S7, S9 were tested at 1h only. 1m changes SL mechanics fundamentally (intra-bar noise is 60x smaller).
- H2 V3 is a custom arb engine (`scripts/arb_h2_live_v3.py` + `app/services/arb/`), NOT an HB-native controller. It runs as a standalone Python process in tmux, not in a Docker container.

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
| `ql-h2-v3` | H2 V3 spike-fade arb (LIVE) | `tmux attach -t ql-h2-v3` |
| `d1-whale-collector` | D1 Hyperliquid whale data | `tmux attach -t d1-whale-collector` |
| `arb-dual-collector` | Bitvavo/Binance spread data | `tmux attach -t arb-dual-collector` |
| `hummingbot` | HB instance (if running) | `tmux attach -t hummingbot` |
| `gateway` | HB gateway (if running) | `tmux attach -t gateway` |

Note: active tmux sessions change as strategies are deployed/stopped. Run `tmux ls` for current state.

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
