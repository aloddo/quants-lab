# SYSTEM — what is live right now (canonical)

Single source of truth for the RUNNING system. If any other doc (README, ARCHITECTURE, strategy.md,
tracker.md, hermes_pipeline.yml) disagrees with this file, THIS file and brain `projects/quant/state` win.
Scope since 2026-07-09: **Hyperliquid copy trading ONLY.** Everything else (Bybit/HB/arb/MM/options,
V11-V28) is retired and being archived (never deleted). Last verified: 2026-07-10.

## The operation = 2 production surfaces + 1 knowledge layer
1. **EXECUTION (live capital)** — the copy-trading engine + its supervision.
2. **RESEARCH (offline)** — the selection+mechanics backtest pipeline over S3 raw data.
3. **KNOWLEDGE** — brain `projects/quant/*` (truth) → KB export.

## A. EXECUTION (live)
| Thing | Value |
|---|---|
| Engine | `strategies/live/hl_copy_trader_v17.py` — PID 85806 (launchd, PPID 1) |
| Live config | `config/copy_trader_wallets_gate1_v4.json` (10 wallets; `global`/`defaults`/`wallets`) — THE live config |
| Supervisor | LaunchDaemon `com.quantslab.v12-copy-trader` (KeepAlive) → `scripts/v12_launcher.sh` |
| Stall watchdog | `com.quantslab.v12-watchdog` → `scripts/v12_heartbeat_watchdog.sh` (STATS-heartbeat content freeze) — SANCTIONED (promoted 2026-07-10; the log-mtime `v17-stall-watchdog` is retired: mtime != loop health, codex) |
| Kill (working today) | `touch /tmp/v12_pause` + `~/quants-lab/.HALT_COPY` (launcher-level halt) |
| Exposure | HL ~$457 (spot-only, Rule 16) + Bybit ~$465 (parked until HL eq >= $550) |
| Wallet addrs | parent `0x11ca20aeb7cd014cf8406560ae405b12601994b4` (funds) / agent `0xdf67eda0bc0223060891d49dde9a780a4538c2e3` (signs) |

Live-stack safety (Phase 1 — RESOLVED + ACTIVATED 2026-07-10, codex 3-round consensus):
- ✅ Mark collector now SUPERVISED: `com.quantslab.hl-mark-collector` (KeepAlive) → `scripts/hl_mark_collector_launcher.sh`. (Was an unsupervised nohup = silent SPOF.)
- ✅ Two-killers resolved: `com.quantslab.v12-watchdog` (STATS-heartbeat content) is the SOLE sanctioned killer; the log-mtime `v17-stall-watchdog` is removed + archived (`ops/launchd/archive/`). Codex: log-mtime != loop health.
- ✅ `scripts/kill_switch.sh` REWRITTEN to actually stop V17 (mode-validated + engine-only pattern + post-SIGKILL abort + default flatten via `tools/flatten_all_offline.py`). The old one targeted the dead HB stack.
- INVARIANT: never rotate/replace `/tmp/ql-v12-copy-trader-launchd.log` while V17 is live (the heartbeat watchdog reads it). See `ops/launchd/README.md`.

## B. RESEARCH (offline) — ONE pipeline: S3 ingestion+gate → m1-m10 processing (COMPLEMENTARY stages)
Raw S3 data (fills/funding/ledger/candles/marks under `app/data/`, SACRED, never deleted) is ingested +
taker-gated, then FEEDS INTO the m1-m10 processing/selection engine. These are complementary stages of ONE
pipeline (Alberto 2026-07-10), NOT two competing funnels — do NOT demote/archive either. All built on
single-source primitives.
- Shared primitives (the Phase-3 consolidation target): `research/v15/execution_model.py` (fees/slippage/
  latency SSOT, always pass `coin=`), `research/v15/fidelity_replay.py::roundtrips` (roundtrip pairer),
  `research/v15/leadlag_clean_rank_sim.py::mark_at` (asof mark index).
- Ingestion + validation gate: `s3_taker_verify.py` (taker gate, CORRECT full-history `load_fills_from_s3`)
  → `research/v16/mae_bag_measure.py` + `rescreen_bag_gate.py` (bag filter) → `forward_oos_hot.py`
  (forward-OOS, boundary-MTM, codex-signed) → `copyability_calib_share.py` (copyability rank).
- Processing/selection engine (the raw data feeds here): `research/v15/v15_m01_equity_reconstruct` (m01) →
  m02 journeys → m03 folds → m025 authenticity → m05 eligibility → m06a/b shortlist+ranking → m07 engine →
  m08 survival → m09 sim → m10 gates → `v15_forward_select`. Cohort/config emit: `research/v16/select_cohort.py`.
Phase-3 scope = consolidate the SHARED PRIMITIVES both stages use (coin-aware fees, one fills loader, one
asof-mark staleness policy); NOT touch either stage's logic. ~195 dead one-off experiments already archived (Phase 3a).

## C. DATA PIPELINE (the one cron)
- LaunchAgent `com.quantslab.hl-s3-fills-daily` (06:20 local) → `scripts/hl_s3_fills_daily_refresh.sh` runs
  `hl_s3_fills_daily_refresh.py` (fills + 1m candles) then `hl_s3_misc_daily_refresh.py` (funding + ledger).
  S3 archive, requester-pays, no HL REST, no trading creds. Outputs under `app/data/hl_s3_*_hot/`.
- Live marks: `hl_live_mark_collector.py` (60s) → `app/data/hl_mark_1m_hot/`, supervised by `com.quantslab.hl-mark-collector` (KeepAlive).
- Live PnL: `tools/pnl_tracker.py` (PID 591) → Telegram, supervised by `com.quantslab.pnl-tracker`.

## Ops quick-reference
- Portfolio truth: `python tools/portfolio_snapshot.py` (HL spot-only + Bybit).
- Kill live engine NOW: `bash scripts/kill_switch.sh` (flatten+halt) or `--halt-only` / `--pause`; or manually `touch /tmp/v12_pause ~/quants-lab/.HALT_COPY`.
- Restart: launchd KeepAlive relaunches on PID death via `v12_launcher.sh`.
- Brain truth: `projects/quant/state`. This file mirrors the live-ops subset of it.

## Retired (archived, never deleted — see the cleanup plan)
Bybit/Binance/OKX collectors, Hummingbot E1-X17 + controllers, arb services, MM (`hl_mm`), options_v4,
the LaunchDaemons `com.quantslab.{api,pipeline,colima,docker-stack}` + retired collector agents. Plan:
brain `projects/quant/plans/2026-07-09-repo-cleanup-hedge-fund-grade` (approved 2026-07-10).
