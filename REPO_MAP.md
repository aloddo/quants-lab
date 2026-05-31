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
Pipeline order (Alberto-locked, projects/quant/v15/sizing-locks):
**M01 equity -> authenticity gate -> ranking -> M02 sizing -> sim -> shadow-live.**
The authenticity gate is the UNNUMBERED gate between M01 and ranking (informally "M2.5"
since it precedes the real M02). M02 = SIZING (Alberto's locked numbering). Do NOT renumber.

| module | canonical file | status |
|---|---|---|
| M01 equity reconstruct | `research/v15/v15_m01_equity_reconstruct.py` | DONE. 14 codex bugs fixed; full 20,378-wallet run done (app/data/v15/m01_universe_20k_series.parquet). Anchor disk cache added. Accurate at true weekly anchors (drift is parked intra-week marks only). |
| authenticity gate (pre-ranking) | `research/v15/v15_m025_authenticity_gate.py` | DONE. Codex SHIP after 7-round loop. Entity/hedge/funding-farm/wash/neutral detection. Output app/data/v15/m025_authenticity.parquet. NOT M02 — it's the gate before ranking. |
| anchor prefetch | `research/v15/v15_prefetch_anchors.py` | warms perp_anchor_cache (zero-API future runs) |
| M01 validation subset | `app/data/v15/m01_validation_wallets.txt` | 11 archetypes |
| M02 sizing | (not built) | the ORIGINAL M02 per sizing-locks. NEXT after gate full run + ranking + Alberto review. Mirror = faithful whole-equity incl leverage + dry powder. |
| G5 source-quality filter | `research/v15/v15_g5_filter.py` | |
| source ROE enrichment | `research/v15/v15_source_roe_enrichment.py` | |
| V13 predecessor (reference) | `research/v13/v13_equity_reconstruct_v8.py` | superseded by v15_m01 |

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
