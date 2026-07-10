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
| Stall watchdog | `com.quantslab.v17-stall-watchdog` → `~/.claude/scripts/v17-copy-engine-stall-watchdog.sh` (120s; kills on log-mtime freeze) — SANCTIONED |
| Kill (working today) | `touch /tmp/v12_pause` + `~/quants-lab/.HALT_COPY` (launcher-level halt) |
| Exposure | HL ~$457 (spot-only, Rule 16) + Bybit ~$465 (parked until HL eq >= $550) |
| Wallet addrs | parent `0x11ca20aeb7cd014cf8406560ae405b12601994b4` (funds) / agent `0xdf67eda0bc0223060891d49dde9a780a4538c2e3` (signs) |

Known live-stack RISKS (Phase 1 of the cleanup fixes these — do NOT rely on them until fixed):
- `data_pipeline/hl_live_mark_collector.py` (PID 1595) runs UNSUPERVISED (no plist) → silent single point of failure.
- `scripts/v12_heartbeat_watchdog.sh` self-labels "DRAFT / NOT deployed" yet is LIVE (`com.quantslab.v12-watchdog`, PID 40377), overlapping the sanctioned stall watchdog = two killers on one engine.
- `scripts/kill_switch.sh` targets the RETIRED HB/API stack (localhost:8001), NOT V17 — it does not stop the live engine.

## B. RESEARCH (offline) — the canonical funnel
Raw S3 data (fills/funding/ledger/candles/marks under `app/data/`, SACRED, never deleted) →
**one funnel**, all built on single-source primitives:
- Primitives: `research/v15/execution_model.py` (fees/slippage/latency SSOT, always pass `coin=`),
  `research/v15/fidelity_replay.py::roundtrips` (roundtrip pairer), `research/v15/leadlag_clean_rank_sim.py::mark_at` (asof mark index).
- Stage 1 taker gate: `research/v15/s3_taker_verify.py` (the CORRECT full-history fills loader `load_fills_from_s3`).
- Bag filter: `research/v16/mae_bag_measure.py` + `rescreen_bag_gate.py`.
- Stage 2 forward-OOS: `research/v15/forward_oos_hot.py` (boundary-MTM, codex-signed 2026-07-10).
- Stage 3 copyability: `research/v15/copyability_calib_share.py`.
- Cohort/config emit: `research/v16/select_cohort.py` / `build_skill_cohort.py`.
NOTE (Phase 3 fixes): a parallel `v15_m01..m10` M-module funnel still coexists; ~230 dead scripts + dead
vN cohorts surround the ~14 keep-set modules; fee-coin-blindness, window-truncating fills loaders, and
divergent roundtrip pairers exist in the dead/duplicate set. Use ONLY the funnel above.

## C. DATA PIPELINE (the one cron)
- LaunchAgent `com.quantslab.hl-s3-fills-daily` (06:20 local) → `scripts/hl_s3_fills_daily_refresh.sh` runs
  `hl_s3_fills_daily_refresh.py` (fills + 1m candles) then `hl_s3_misc_daily_refresh.py` (funding + ledger).
  S3 archive, requester-pays, no HL REST, no trading creds. Outputs under `app/data/hl_s3_*_hot/`.
- Live marks: `hl_live_mark_collector.py` (60s) → `app/data/hl_mark_1m_hot/` (unsupervised — see Phase 1 risk).
- Live PnL: `tools/pnl_tracker.py` (PID 591) → Telegram, supervised by `com.quantslab.pnl-tracker`.

## Ops quick-reference
- Portfolio truth: `python tools/portfolio_snapshot.py` (HL spot-only + Bybit).
- Kill live engine NOW: `touch /tmp/v12_pause ~/quants-lab/.HALT_COPY` (kill_switch.sh is broken, Phase 1).
- Restart: launchd KeepAlive relaunches on PID death via `v12_launcher.sh`.
- Brain truth: `projects/quant/state`. This file mirrors the live-ops subset of it.

## Retired (archived, never deleted — see the cleanup plan)
Bybit/Binance/OKX collectors, Hummingbot E1-X17 + controllers, arb services, MM (`hl_mm`), options_v4,
the LaunchDaemons `com.quantslab.{api,pipeline,colima,docker-stack}` + retired collector agents. Plan:
brain `projects/quant/plans/2026-07-09-repo-cleanup-hedge-fund-grade` (approved 2026-07-10).
