# CLAUDE.md — Project Instructions

## What is this repo

Fork of `hummingbot/quants-lab` with a systematic crypto perpetual futures trading system built on QuantsLab primitives. Two engines: E1 (compression breakout) and E2 (range fade). Paper trading on Bybit demo (api-demo.bybit.com) via Hummingbot API.

Owner: Alberto Loddo (GitHub: aloddo)

## Repo structure

```
quants-lab/                          # Fork of hummingbot/quants-lab
├── core/                            # UPSTREAM — do not modify. QL framework.
├── app/
│   ├── engines/                     # Pure evaluation functions (E1, E2)
│   ├── features/                    # 7 FeatureBase subclasses → MongoDB
│   ├── services/                    # Bybit REST client, HB API client
│   ├── controllers/directional_trading/  # HB V2 controllers (for backtesting)
│   ├── tasks/
│   │   ├── data_collection/         # CandlesDownloader, BybitDerivativesTask
│   │   ├── screening/               # FeatureComputationTask, SignalScanTask
│   │   ├── resolution/              # TestnetResolverTask
│   │   └── backtesting/             # BulkBacktestTask
│   └── data/cache/candles/          # Parquet files (gitignored)
├── config/hermes_pipeline.yml       # YAML DAG — the full pipeline
├── scripts/                         # start_pipeline.sh, kill_switch.sh, status.sh
└── .env                             # Secrets — gitignored
```

## Key rules

1. **Never modify `core/`** — that's upstream QuantsLab. Fetch updates with `git fetch upstream && git merge upstream/main`.
2. **Always use QL primitives** — FeatureBase for features, BaseTask for tasks, TaskOrchestrator for scheduling. No custom cron scripts, no SQLite.
3. **Engine evaluation functions are pure** — `evaluate_e1()` and `evaluate_e2()` take a `DecisionSnapshot`, return a candidate. No side effects, no DB access.
4. **MongoDB for everything dynamic** — features, candidates, pair_historical, derivatives, task executions.
5. **Parquet for candle data** — written by CandlesDownloaderTask via CLOBDataSource.
6. **Telegram bot token and chat IDs are in `.env`** — never commit secrets.

## Environment

- **Server**: Always-on Mac Mini, SSH via Tailscale
- **Python**: `/Users/hermes/miniforge3/envs/quants-lab/bin/python` (3.12, conda)
- **MongoDB**: `mongodb://localhost:27017/quants_lab` (no auth, local only)
- **HB API**: `http://localhost:8000` (admin/admin)
- **Env vars needed**: `MONGO_URI`, `MONGO_DATABASE`, `TELEGRAM_ENABLED`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
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

## Common operations

### Check system status
```bash
bash /Users/hermes/quants-lab/scripts/status.sh
```

### Start the pipeline (tmux)
```bash
bash /Users/hermes/quants-lab/scripts/start_pipeline.sh
```

### Emergency stop
```bash
bash /Users/hermes/quants-lab/scripts/kill_switch.sh        # pause + close positions
bash /Users/hermes/quants-lab/scripts/kill_switch.sh --full  # also kill tmux sessions
```

### Trigger a single task
```bash
MONGO_URI=... python cli.py trigger-task --task feature_computation --config config/hermes_pipeline.yml
```

### Run bulk backtest
```bash
MONGO_URI=... python cli.py trigger-task --task e1_bulk_backtest --config config/hermes_pipeline.yml
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
| `bybit_open_interest` | OI history | 90 days |
| `bybit_ls_ratio` | Long/short ratio history | 90 days |
| `task_executions` | QL task execution history | 90 days |

## Pipeline DAG

```
candles_downloader_bybit  (hourly at :05)
bybit_derivatives         (every 15 min)
    ↓ (both on_success)
feature_computation       (dependency-triggered)
    ↓ (on_success)
signal_scan               (dependency-triggered)
    ↓ (on_success)
testnet_resolver          (every 5 min + dependency-triggered)

e1_bulk_backtest          (weekly Sunday 03:00 UTC)
e2_bulk_backtest          (weekly Sunday 04:00 UTC)
```

## Engine parameters (locked)

### E1 Compression Breakout
- ATR percentile < 0.20 (compression) + price breaks 20-period range
- Hard filters: BTC not Risk-Off, volume > 1.3x 20-period avg
- 5m entry quality gate: distance < 0.3 ATR, body < 0.5 ATR, gap < 5 bps
- TP: +3%, SL: -1.5%, time limit: 24h
- Allowlist: XRP, OP, WLD, AVAX, APT, ARB, DOT, DOGE, LTC, BCH (BTC BLOCKED)

### E2 Range Fade
- ATR percentile < 0.30, range NOT expanding, boundary touch + rejection
- No breakout confirmation: vol z-score ≤ 1.5, OI change ≤ 2%, body ≤ 0.8 ATR
- TP: range midpoint, SL: range_low - 0.75 ATR, time limit: 12h
- LONG ONLY, allowlist: BTC, BCH, ADA

### Portfolio limits
- Max 3 concurrent positions across all engines
- Max 2% total capital exposure
- Per-engine: E1 max 2, E2 max 1
- Position size: 0.3% of capital per trade

## Gotchas

- **CandlesDownloaderTask writes parquet only at the end** — if killed mid-run, all data is lost. The scheduled task uses 7-day retention (fast). Use separate backfill scripts for historical data.
- **Feature TTL is 90 days** — if you need longer history for backtesting, query parquet directly, don't rely on MongoDB features.
- **The `timestamp` field in derivatives collections uses milliseconds** (Bybit convention), but QL features use Python datetime objects. Watch for unit mismatches.
- **`core/` FeatureStorage uses `insert_many`** which creates duplicates. Our FeatureComputationTask uses upsert instead — don't use FeatureStorage.save_features() directly.
- **Bybit demo connector** (`bybit_perpetual_demo`) uses `api-demo.bybit.com` (real market data, virtual funds). Registered by patching the HB connector in both the conda env (`site-packages`) and the Docker container. **If the HB API Docker container is rebuilt**, re-apply the patch:
  ```bash
  HOST_PKG="/Users/hermes/miniforge3/envs/quants-lab/lib/python3.12/site-packages/hummingbot/connector/derivative/bybit_perpetual"
  CONTAINER_PKG="/opt/conda/envs/hummingbot-api/lib/python3.12/site-packages/hummingbot/connector/derivative/bybit_perpetual"
  docker cp "$HOST_PKG/bybit_perpetual_constants.py" "hummingbot-api:$CONTAINER_PKG/bybit_perpetual_constants.py"
  docker cp "$HOST_PKG/bybit_perpetual_utils.py" "hummingbot-api:$CONTAINER_PKG/bybit_perpetual_utils.py"
  docker restart hummingbot-api
  ```
- **Git push requires token** — hermes user has no credential helper. Use the temporary URL method documented above.
