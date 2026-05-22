# CLAUDE.md -- Codebase Conventions

## Mission

$500 MRR from live crypto trading. Primary venue: Hyperliquid. Secondary: Bybit mainnet, Binance spot.

## What This Is

Fork of `hummingbot/quants-lab`. Two execution modes:
1. **HB-native controllers** -- backtest + live via Docker (E1-X17 series)
2. **Custom scripts** -- HL market making, copy trading, HL-Bybit arb

## Repo Layout

```
core/                               # UPSTREAM -- never modify
app/
  controllers/directional_trading/  # HB V2 controllers (one file = one strategy)
  engines/strategy_registry.py      # StrategyMetadata (source of truth for HB-native)
  services/hl_mm/                   # Hyperliquid market maker engine
  services/arb_hlbb/                # HL-Bybit perp arb engine
  services/arb/                     # Legacy H2 Bybit-Binance arb
  tasks/data_collection/            # 15+ collectors (candles, funding, OI, options)
  tasks/backtesting/                # Bulk backtest + walk-forward
  features/                         # FeatureBase subclasses -> MongoDB
scripts/                            # Live scripts, collectors, backfills
config/hermes_pipeline.yml          # YAML DAG for TaskOrchestrator
docker/                             # Demo Dockerfile + Bybit patches
```

## Key Rules

1. Never modify `core/`.
2. Research before code. Full /research-process before any deployment.
3. All backtests at **1m resolution**. Prior 1h results are invalid.
4. Controllers must compute **vectorized signals** (scalar = 0 trades).
5. MongoDB for dynamic data. Parquet for candle history.
6. Check ALL venues on every portfolio check (HL + Bybit + Binance).
7. **RAW DATA NEVER DELETES** (Alberto directive 2026-05-22). Raw collections (fills, candles, OB snapshots, funding rates, OI, trades, liquidations) MUST NOT have retention rules. Painfully acquired over days. Only DERIVED / OPERATIONAL data (task_executions, opportunities, candidates) may have retention. `data_retention_task.py` audited 2026-05-22; raw entries removed. If disk pressure ever requires retention on raw data, ARCHIVE to remote DB / cold storage FIRST, then prune. Never silent-delete.

## Fees

| Exchange | RT Taker | RT Maker |
|----------|----------|----------|
| Hyperliquid | 8.64 bps | 2.88 bps |
| Bybit | 11 bps | 4 bps |

HL research constant: `FEE_RT = 0.000864`

## Hyperliquid

Unified wallet (no spot/perps split). USDC in spot IS perps margin.
- Parent (funds): `0x11ca20aeb7cd014cf8406560ae405b12601994b4`
- Agent (signs): `0xdf67eda0bc0223060891d49dde9a780a4538c2e3`
- Query parent for balance. Agent trades on behalf of parent.

## Environment

- Python: `/Users/hermes/miniforge3/envs/quants-lab/bin/python` (3.12)
- MongoDB: `mongodb://localhost:27017/quants_lab`
- HB API: `http://localhost:8000` (admin/admin)
- Secrets: `.env` (source with `set -a && source .env && set +a`)

## Common Commands

```bash
bash scripts/status.sh                          # system health
bash scripts/kill_switch.sh                     # emergency stop
python cli.py trigger-task --task <name> --config config/hermes_pipeline.yml
python cli.py deploy --engine X14               # deploy HB bot
python cli.py bot-status --engine X14
bash scripts/run_backtest.sh e1_bulk_backtest   # isolated backtest
```

## Process Supervision

LaunchDaemons (`/Library/LaunchDaemons/com.quantslab.{pipeline,api}.plist`) auto-restart on crash.
Logs: `/tmp/ql-pipeline-launchd.log`, `/tmp/ql-api-launchd.log`

## Strategy Lifecycle (HB-native)

```
IDEA -> CONTROLLER -> REGISTER -> BACKTEST -> WALK-FORWARD -> DEPLOY
```

See `.claude/skills/research-process/SKILL.md` for full process.
See `.claude/skills/quant-governance/SKILL.md` for gates and stop conditions.

## Pointers (don't inline -- read these)

| Topic | Location |
|-------|----------|
| Strategy status + metrics | Brain: `projects/quant/state` |
| Session handoffs | Brain: `handoffs/quant-engineer/YYYY-MM-DD` |
| Governance gates | `.claude/skills/quant-governance/SKILL.md` |
| Research process | `.claude/skills/research-process/SKILL.md` |
| Backtesting skill | `.claude/skills/backtesting/SKILL.md` |
| Data pipeline details | `config/hermes_pipeline.yml` |
| Lessons learned | Brain: `projects/quant/lessons/` |
| Architecture decisions | Brain: `projects/quant/decisions/` |
| HL MM design | `docs/hl_mm_v4_design.md` |
| HL-Bybit arb plan | `docs/arb_hl_bybit_perp_plan.md` |

## Git Push

No credential helper. Temporary token method:
```bash
git remote set-url origin https://aloddo:<GH_TOKEN>@github.com/aloddo/quants-lab.git
git push origin main
git remote set-url origin https://github.com/aloddo/quants-lab.git
```
