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
CANONICAL V13->V15 module numbering (map: projects/quant/v15/module-review-2026-05-30,
confirmed via tests/v13/test_module_*.py imports). The V15 strategy
(projects/quant/v15/strategy) INSERTS a NEW entity/authenticity layer "M0e" between
M02 and M03. Full pipeline:
**M01 equity -> M02 journey_trace -> M0e authenticity (NEW) -> M03 eligibility ->
M04 ranker -> M05 exec-realism -> M06 cold-start -> M07 ledger -> M08 sim ->
M09 walk-forward -> M10 gates -> M11 random-null.** Sizing lives inside M08 sim
(PROP %-of-equity), constrained by projects/quant/v15/sizing-locks; it is NOT a
separate numbered module. Do NOT renumber.

| module | canonical file | status |
|---|---|---|
| M01 equity reconstruct | `research/v15/v15_m01_equity_reconstruct.py` | DONE (V15). 14 codex bugs fixed; full 20,378-wallet run; anchor disk cache. Accurate at true weekly anchors. |
| M02 journey_trace | `research/v13/v13_journey_trace.py` | EXISTS (V13, 1234 lines). NOT yet V15-ported (point-in-time equity for sizing; xyz; liquidation-close still TODO). |
| M0e authenticity gate (NEW) | `research/v15/v15_m025_authenticity_gate.py` | DONE (V15). Codex SHIP, 7-round loop. Entity/hedge/funding-farm/wash/neutral detection. Output app/data/v15/m025_authenticity.parquet. (File keeps the "m025" name; logically M0e, sits between M02 and M03.) |
| anchor prefetch | `research/v15/v15_prefetch_anchors.py` | warms perp_anchor_cache (zero-API future runs) |
| M03 eligibility_gates (SCREENING — a filter, NOT ranking) | `research/v13/v13_m03_v2.py` | EXISTS (V13, 352 lines). Wallet-universe pass/fail filter (active_days, trade_count, maxDD, flow-adjusted TWR ROE). V15: align gates to G5. |
| M04 copy_ranker (RANKING) | `research/v13/v13_copy_ranker_v2.py` | STUB (main exits 2). V15: full rewrite to codex#7 source_score = source_6m_ROE x min(1,active_folds/6) x log1p(n_journeys) x clamp(1-maxDD). |
| M05-M11 | research/v13/v13_*.py (exec-realism, cold_start, portfolio_ledger, portfolio_simulator[+sizing], walk_forward_folds, pass_fail_gates, strict_random_null) | EXIST (V13). V15 revalidation TODO. |
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
