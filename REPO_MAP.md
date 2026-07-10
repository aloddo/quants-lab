# REPO_MAP — quants-lab source of truth

On-disk index of what is CANONICAL and where the data lives. For **"what is live right now"** the canonical
source is **[`SYSTEM.md`](./SYSTEM.md)** + brain `projects/quant/state` — if the LIVE table below disagrees,
SYSTEM.md wins.

Last restructure: 2026-05-30 (flat `scripts/` -> live/research/pipeline/tools/archive).
LIVE + module tables reconciled to reality 2026-07-10 (V17 live; m01-m10 trust audit COMPLETE).

## Top-level layout
```
strategies/live/    LIVE trading strategies (real capital)
data_pipeline/      data collectors + backfills (candles, funding, fills, OI, wallets)
research/v15/       CANONICAL V15 module pipeline (current research line)
research/v13/       V13 infra V15 reuses (modules 1-5) + v13 research scripts
research/options_v4/ options v4 strategy package (built, not live)
tools/              utilities: portfolio_snapshot, kill_switch, status, governance
scripts/            launchd LAUNCHER wiring ONLY (.sh entry points the plists call)
ops/launchd/        launchd plists + install scripts
archive/scripts/    superseded / dead code (git history preserved, never deleted)
core/               UPSTREAM hummingbot — never modify
app/                package code (controllers, services, tasks, features, data/)
config/ docker/ docs/ tests/ research_notebooks/
```

## LIVE (running now — do not break; canonical = SYSTEM.md)
| process | file | run via | notes |
|---|---|---|---|
| V17 copy trader (real capital) | `strategies/live/hl_copy_trader_v17.py` | launchd (KeepAlive via `v12_launcher.sh`) | config `gate1_v4`; kill via `scripts/kill_switch.sh` |
| Live mark collector | `hl_live_mark_collector.py` (60s) | launchd `com.quantslab.hl-mark-collector` (KeepAlive) | -> `app/data/hl_mark_1m_hot/` |
| PnL tracker | `tools/pnl_tracker.py` | launchd `com.quantslab.pnl-tracker` | -> Telegram |
| S3 fills daily cron | `scripts/hl_s3_fills_daily_refresh.sh` (06:20) | launchd `com.quantslab.hl-s3-fills-daily` | fills+candles then funding+ledger |

RETIRED (archived, not live): V11/V12 copy traders, HL-Bybit arb collector, HL wallet collector, listing
monitor, and the `com.quantslab.{api,pipeline,colima,docker-stack,arb-collector,...}` daemons. Their sudo
unload is a CoS task ("once all done"). Do NOT restart them.

## CANONICAL research modules (V15 pipeline)
CANONICAL numbering = BY BUILD ORDER (Alberto 2026-05-31), 6 layers. Spine = projects/quant/v15/
strategy Section 10. The OLD M01-M11 were V13 RUNTIME data-flow order; do NOT use that here.
Build-order pipeline:
**M1 equity -> M2 journeys+per-action sizing -> M3 fold geometry -> M4 authenticity kills + entity
dedup -> M5 eligibility floors -> M6a cheap shortlist -> M7 engine -> M6b copyability-adjusted
ranking -> M8 survival tiering -> M9 mirror sim -> M10 gates+matched-elite-null.** CHEAP selection
(M3-M6a) slims ~18k -> broad ~3-5k with NO engine first. old->new map: M1<-M01, M2<-M02,
M3<-old M09 (fold), M4<-M0e cheap stage, M5<-old M03, M6<-old M04, M7<-old M05+M07+M08-risk,
M8<-M0e survival stage, M9<-old M06+M08, M10<-old M10+M11.

**ALL 11 modules TRUSTED (2026-07-10)** — each adversarial-codex-audited to consensus, made fail-CLOSED, and
pinned by a golden regression test. Tracker: brain `projects/quant/cleanup/m1-m10-trust-audit`; raw audits
`archive/codex_audit_m*.txt`; 405 v15 tests green.

| module | canonical file | trust commit |
|---|---|---|
| M1 equity reconstruct | `research/v15/v15_m01_equity_reconstruct.py` | 0ba7641 (look-ahead-safe + fail-closed) |
| M2 journeys + per-action sizing | `research/v15/v15_m02_journey_trace.py` | d36fa6a |
| M2.5 authenticity gate (scoring engine) | `research/v15/v15_m025_authenticity_gate.py` | 78ef18a |
| M3 fold geometry | `research/v15/v15_m03_fold_geometry.py` | 4265687 |
| M4 authenticity driver (per-fold; imports m025) | `research/v15/v15_m04_authenticity.py` | (via m025) |
| M5 eligibility + copyability floors | `research/v15/v15_m05_eligibility.py` | d0ed4f2 |
| M6a shortlist | `research/v15/v15_m06a_shortlist.py` | 85d022e |
| M6b ranking (post-engine) | `research/v15/v15_m06b_ranking.py` | da1a29a |
| M7 engine | `research/v15/v15_m07_engine.py` | 8e7b83f |
| M8 survival tiering | `research/v15/v15_m08_survival.py` | e3fb00a |
| M9 chained sim / allocation | `research/v15/v15_m09_sim.py` | a1326b9 |
| M10 gates | `research/v15/v15_m10_gates.py` | 693badc |
| decision/grading layer | `research/v15/v15_forward_select.py` | per-rule sweep -> M9 sim + M10 gates |
| anchor prefetch | `research/v15/v15_prefetch_anchors.py` | warms perp_anchor_cache (zero-API future runs) |

Orchestration: `run_clean_rerun.sh` -> `recal_pipeline.sh` (m01->m08) -> `v15_forward_select` (m09/m10). See SYSTEM.md.
V13 (`research/v13/`) is the legacy infra V15 ported; superseded intermediates are in `archive/scripts/v13/`.

## DATA inventory (app/data — RAW DATA IS SACRED, never moved/deleted without per-dir confirm)
| dir | size | content | gitignored |
|---|---|---|---|
| `app/data/hl_s3_fills/` | 39G | v1 raw HL fills (legacy) | yes |
| `app/data/v13/` | 12G | anchors, ledger/funding caches, equity artifacts, marks | yes |
| `app/data/hl_s3_fills_v2/` | 8.1G | enriched daily HL fills (dir/closedPnl schema) | yes (added 2026-05-30) |
| `app/data/cache/` | 3.7G | misc cache | yes |
| `app/data/hl_s3_fills_v2_by_wallet/` | 3.1G | per-wallet partitioned fills (M01 fast path) | yes (added 2026-05-30) |
| `app/data/wallet_alpha/` | 290M | wallet scoring | yes |
| `app/data/v15/` | small | curated inputs (wallet lists json/txt tracked; parquet ignored) | mixed |

## Move map (2026-05-30): old `scripts/<x>` -> new home
- 5 live scripts -> strategies/live + data_pipeline + tools (see LIVE table)
- `v15_*` -> research/v15/ ; `v13_*` -> research/v13/
- collectors/backfills/downloaders -> data_pipeline/
- utilities -> tools/ ; everything dead/superseded -> archive/scripts/
- launchd plist templates `scripts/launchd/*` -> ops/launchd/
- `scripts/options_v4/` package -> research/options_v4/

## Gotchas fixed in the restructure
- `hl_copy_trader_v11.py` repo-root resolver was `parent.parent` (assumed 1-deep);
  now walks up to the dir with config/+app/ (move-resilient).
- 11 research/v13 scripts had `ROOT=parent.parent` / `parents[1]`; bumped +1 level
  (now 2-deep under research/v13/).
- `.gitignore` now excludes hl_s3_fills_v2* + app/data/**/*.parquet (were exposing ~11G).
