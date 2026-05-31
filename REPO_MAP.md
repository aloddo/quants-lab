# REPO_MAP — quants-lab source of truth

Single on-disk index of what is LIVE, what is CANONICAL, and where the data lives.
Updated by the agent on every structural change. If a file's purpose isn't obvious
from its path + this map, the map is wrong — fix the map.

Last restructure: 2026-05-30 (flat `scripts/` -> live/research/pipeline/tools/archive).

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

## LIVE (running now — do not break)
| process | file | run via | restart |
|---|---|---|---|
| V11 copy trader (real capital, observation gate) | `strategies/live/hl_copy_trader_v11.py` | MANUAL (PID, not launchd) | `python strategies/live/hl_copy_trader_v11.py --config config/copy_trader_wallets.json` |
| HL-Bybit arb collector | `data_pipeline/arb_hl_bybit_collector.py` | launchd `com.quantslab.arb-collector` via `scripts/arb_collector_launcher.sh` | launchctl |
| HL wallet collector | `data_pipeline/hl_wallet_collector.py` | launchd `com.quantslab.hl-wallet-collector` via `scripts/hl_wallet_collector_launcher.sh` | launchctl |
| Listing monitor | `data_pipeline/listing_monitor.py` | launchd `com.quantslab.listing-monitor` via `scripts/listing_monitor_launcher.sh` | launchctl |
| PnL tracker | `tools/pnl_tracker.py` | launchd `com.quantslab.pnl-tracker` (plist in `~/Library/LaunchAgents/`) via `scripts/pnl_tracker_launcher.sh` | launchctl |

NOTE: launchers stay in `scripts/` (plists reference them by absolute path). The
`.py` paths INSIDE each launcher were repointed in the 2026-05-30 restructure; the
running processes are unaffected and pick up the new path on next restart. The
`v12-copy-trader` plist is legacy-named but runs `hl_copy_trader_v11.py`; it is NOT
currently loaded (V11 runs manually).

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

| module | canonical file | status |
|---|---|---|
| M1 equity reconstruct | `research/v15/v15_m01_equity_reconstruct.py` | DONE (V15). 14 codex bugs; full 20,378-wallet run; anchor disk cache; causal per-event equity shipped for M2. |
| M2 journeys + per-action sizing | `research/v15/v15_m02_journey_trace.py` | DONE (V15). codex-SHIP design(6)+code(3). 14 tests. 20k run IN FLIGHT -> m02_actions.parquet + m02_journeys.parquet. |
| M3 fold geometry | `research/v15/v15_m03_fold_geometry.py` | DONE (V15). codex-SHIP design(r1-r3)+code(r1-r2). tests/v15/test_m03.py (9 pass). 8x{42/14/14}, 112d chained OOS. VALIDATE on 20k pending. |
| M4 authenticity kills + entity dedup | `research/v15/v15_m04_authenticity.py` | DONE (V15). codex-SHIP design(r1-r3)+code(r1-r3). tests/v15/test_m04.py (16 pass). Reuses m025 helpers; danger-only confidence TIERS {KILL/UNCERTAIN/SUSPICIOUS/CLEAN} -> alloc_weight + entity dedup. as-of fold-pure. Validate on 20k/per-fold pending. modules/m4-design. |
| anchor prefetch | `research/v15/v15_prefetch_anchors.py` | warms perp_anchor_cache (zero-API future runs) |
| M5 eligibility + copyability floors | `research/v15/v15_m05_eligibility.py` | DONE (V15). codex-SHIP design(r1-r3)+code(r1-r3). tests/v15/test_m05.py (15 pass). Fold-pure per-fold floors (port of v13_m03_v2); HOLD_FLOOR=60s (V11-measured). Validate on 20k pending. modules/m05. |
| M6 ranking (a cheap shortlist / b post-engine) | `research/v13/v13_copy_ranker_v2.py` (rewrite) | STUB. V15: M6a source_score shortlist (~3-5k); M6b copyability-adjusted final ranking post-M7. |
| M7-M10 | research/v13/v13_*.py (exec-realism, cold_start, portfolio_ledger, portfolio_simulator[+sizing], pass_fail_gates, strict_random_null) | EXIST (V13). V15 rebuild TODO (M7 engine, M8 survival, M9 sim, M10 gates+null). |
| G5 source-quality filter | `research/v15/v15_g5_filter.py` | |
| source ROE enrichment | `research/v15/v15_source_roe_enrichment.py` | |

V13: 42 research scripts (Sharpe-hunt + module pipeline). Superseded intermediates
(equity_reconstruct v4/v5/v7) are in `archive/scripts/v13/`.

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
