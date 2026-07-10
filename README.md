# quants-lab — Hyperliquid copy-trading quant operation

A live crypto quant trading operation on Hyperliquid (secondary venues: Bybit mainnet, Binance spot).
Originally forked from [`hummingbot/quants-lab`](https://github.com/hummingbot/quants-lab); the operation
has since pivoted entirely to **Hyperliquid copy trading**, and the Hummingbot-native / Docker-controller
stack described by the old upstream README is **retired** (archived, not deleted).

> **Canonical source of truth: [`SYSTEM.md`](./SYSTEM.md)** — "what is live right now" (the live engine, the
> one data cron, the supervised processes, the kill path). If anything here disagrees with `SYSTEM.md` or brain
> `projects/quant/state`, THOSE win.

## The two surfaces

### A. EXECUTION (live)
One engine copies a curated cohort of Hyperliquid leaders into a single HL account.
- Engine: `strategies/live/hl_copy_trader_v17.py` (V17), launched + auto-relaunched by launchd.
- Portfolio truth (always live-queried, HL spot-USDC only + Bybit): `python tools/portfolio_snapshot.py`.
- Emergency stop: `bash scripts/kill_switch.sh` (flatten + halt); `--halt-only` / `--pause` variants.
- Live PnL + marks: `tools/pnl_tracker.py` and `hl_live_mark_collector.py`, both launchd-supervised.

### B. RESEARCH (offline) — the m01–m10 selection pipeline
Raw S3 data (fills / funding / ledger / candles / marks under `app/data/`, **sacred, never deleted**) is
ingested + taker-gated, then processed by an 11-module selection engine that reconstructs each leader's causal
whole-account equity and grades copyability **without look-ahead**:

`m01` causal equity → `m02` journeys → `m03` folds → `m04` authenticity (uses `m025`) → `m05` eligibility →
`m06a` shortlist → `m07` engine (pretest+test) → `m06b` ranking → `m08` survival → **[decision]** `m09` chained
sim + `m10` gates.

**All 11 modules are TRUSTED** (2026-07-10): each was adversarial-audited to consensus, made **fail-closed**
(no NaN/missing input can silently pass a gate), and pinned by a golden regression test. See
`projects/quant/cleanup/m1-m10-trust-audit` and the per-module `archive/codex_audit_m*.txt`.

Orchestration (canonical entrypoints — do **not** rebuild):
1. `scripts/run_clean_rerun.sh` — leak-free causal m01 re-run, then calls:
2. `scripts/recal_pipeline.sh` — the deterministic data+ranking chain (m01→m08).
3. `research/v15/v15_forward_select.py::forward_backtest` — the per-rule decision/grading layer (m09 sim +
   m10 gates). The selection rule is a research choice, deliberately not baked into the shell chain.

## Environment

```bash
conda env create -f environment.yml     # creates the quants-lab env (Python 3.12)
# Python:  /Users/hermes/miniforge3/envs/quants-lab/bin/python
# MongoDB: mongodb://localhost:27017/quants_lab   (dynamic data; parquet for candle history)
# Secrets: .env  (source with: set -a && source .env && set +a)
```

## Repo conventions

- Never modify `core/` (upstream).
- Research at **1m resolution**; all backtests price fills through `research/v15/execution_model.py`
  (coin-aware fees/slippage/latency, single source of truth).
- Raw data never deletes (Alberto directive); memory-safe streaming I/O is mandatory for universe-scale runs.
- Full conventions: [`CLAUDE.md`](./CLAUDE.md). Governance gates: `.claude/skills/quant-governance/`.

## Pointers

| Topic | Location |
|-------|----------|
| What is live now | `SYSTEM.md` + brain `projects/quant/state` |
| Trust-audit status | brain `projects/quant/cleanup/m1-m10-trust-audit` |
| Session handoffs | brain `handoffs/quant-engineer/YYYY-MM-DD` |
| Decisions / lessons | brain `projects/quant/{decisions,lessons}/` |

*The retired Hummingbot-native quick-start (Docker tasks, `cli.py deploy --engine`, HB controllers) lives in
git history and `archive/`; it is no longer the operation.*
