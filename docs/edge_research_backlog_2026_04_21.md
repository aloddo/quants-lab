# Edge Discovery Research Backlog (April 21, 2026)

## Objective
Build a repeatable research program that discovers at least one scalable, execution-realistic edge across the venues you can actually trade:
- Bybit: spot + perp
- Binance: spot
- Bitvavo: spot (EUR quote)

Primary target:
- Find 2-3 candidate strategies with out-of-sample edge after realistic fees, slippage, queue uncertainty, and capacity limits.

Secondary target:
- Build a "kill fast" hypothesis factory so weak ideas die early and research time compounds.

## Program Principles
1. Execution realism is not optional. No alpha claim without queue/fill/slippage assumptions.
2. No leakage. Every feature is lagged by at least one decision step.
3. No single-split optimism. Use walk-forward or rolling retrain.
4. No outlier dependence. Top 5 trades cannot dominate strategy PnL.
5. Capacity before excitement. A tiny edge with no size is research, not a business.
6. Strategy edges must survive cost stress and stale-quote stress.

## Backtest QA Gate (Mandatory Before Any Conclusion)
1. Data integrity checks:
- Monotonic timestamps
- Missing-bar ratio and stale-bar ratio by symbol
- Outlier prints and crossed-book detection
2. Alignment checks:
- Feature timestamp <= decision timestamp - lag
- Entry price at t+1 bar (or simulated next executable quote)
3. Execution checks:
- Maker/taker mix assumptions explicit
- Queue model assumptions explicit
- Slippage model tied to spread + volatility + size
4. Validation checks:
- Train/test split + walk-forward
- Regime split (trend/chop, high/low vol, session)
- Parameter stability grid
5. Statistical checks:
- Reality check / permutation test for data-mined hypotheses
- Bootstrap confidence intervals on PF, Sharpe, expectancy
6. Concentration checks:
- PnL concentration by day, symbol, and top trades
- Remove top decile trades and re-evaluate

## Prioritization Framework
Use a weighted score for backlog ranking:

`Priority Score = (Edge Potential * 0.30) + (Execution Feasibility * 0.25) + (Capacity * 0.20) + (Data Availability * 0.15) + (Novelty * 0.10)`

Each dimension is scored 1-5.

## Workstreams
- WS1: Data Infrastructure and Quality
- WS2: Arb / Relative-Value Signals
- WS3: Directional Microstructure Signals
- WS4: Structural and Cross-Domain Signals
- WS5: Portfolio Construction and Risk Controls

## WS1 Data Backlog (Counterintuitive Sources Included)

### P0 (Immediate)
| ID | Dataset | Why it may hold edge | Acquisition | Cadence | Done criteria |
|---|---|---|---|---|---|
| D1 | Bitvavo L1 + trades + candles (1m + 5s snapshots) | EUR venue-specific dislocations and stale quoting behavior | Extend `arb_bitvavo_collector.py` and store clean snapshots | 5s + 1m | 90d coverage, per-symbol freshness report |
| D2 | Bybit L1/L2 + trades + perp funding/OI | Needed for lead-lag and queue-aware simulation | Existing collectors + Mongo normalization | sub-second/1m/1h | Unified schema with clock sync diagnostics |
| D3 | Binance spot L1/trades | Independent spot anchor for cross-venue triangulation | Add spot collector aligned to Bybit clock | sub-second/1m | 60d overlap with Bybit/Bitvavo |
| D4 | Synthetic EURUSD from BTC cross + external FX sanity feed | Detect FX-driven false arb vs true dislocation | Build EURUSD quality monitor with drift alarms | 1m | FX residuals within tolerance bands |
| D5 | Exchange metadata stream (maintenance, mode changes, API latency spikes) | Many fake edges cluster around operational incidents | Poll status endpoints + latency logs | 1m | Incident labels joined to trading data |

### P1 (Next)
| ID | Dataset | Why it may hold edge | Acquisition | Cadence | Done criteria |
|---|---|---|---|---|---|
| D6 | Options surface (Deribit skew/term structure) | Dealer positioning can predict perp mean reversion windows | Pull IV, RR, term slope | 1m-5m | Joined panel with perp basis states |
| D7 | Stablecoin CEX/chain flow panel | Flow shocks may lead alt perp pressure with lag | Dune/DefiLlama ETL + lag-safe joins | 1h | Feature panel with lag audit |
| D8 | Liquidation map state variables | Forced flow zones create short-horizon drift/reversal | Coinalyze + local engineered features | 1m/5m | Validated cliff distance features |
| D9 | Borrow/lending rates (Aave, Venus, margin proxies) | Funding crowding can spill into perp basis | API pull + resampling | 1h | Clean historical series with missingness stats |
| D10 | Session microstructure tags (EU open, US open, rollover) | Edge often session-conditional, not global | Deterministic calendar features | event-based | Strategy metrics by session |

### P2 (Advanced / PhD-level)
| ID | Dataset | Why it may hold edge | Acquisition | Cadence | Done criteria |
|---|---|---|---|---|---|
| D11 | Mempool pressure and bridge queues | Pre-trade flow can front-run CEX inventory stress | Chain-specific indexers / APIs | seconds-minutes | Event alignment pipeline |
| D12 | Internet/exchange health proxies (CDN outages, packet loss) | Temporary informational fragmentation creates dislocations | Public telemetry + your collector latency | 1m | Incident-annotated return study |
| D13 | Open-source dev/event telemetry (client releases, incidents) | Structural shifts in token narrative/liquidity | GitHub + project feeds | daily/hourly | Event calendar linked to regimes |
| D14 | Multilingual attention anomalies (not generic sentiment) | Regional retail flow can lead specific venue pressure | Targeted NLP feeds | 5m-1h | Attention shock factor backtested |

## WS2 Arb / Relative-Value Hypothesis Backlog

### A-Series (Execution-First Arb)
| ID | Hypothesis | Signal Definition | Execution Design | Horizon | Priority |
|---|---|---|---|---|---|
| A1 | EUR triangle dislocation mean-reverts when both sides are fresh | Z-score of `Bitvavo(EUR->USD) vs Bybit perp` with stale filters | Maker first on rich side, hedge other venue, strict max stale bars | 5s-3m | High |
| A2 | Dislocations are only tradable when quote-age asymmetry is low | Same spread as A1 + quote age differential | Trade only when both feeds fresh and depth above threshold | 5s-2m | High |
| A3 | Lead-lag alternates by regime, not fixed venue | Kalman-estimated dynamic lead coefficient | Trade only when posterior confidence > threshold | 10s-5m | High |
| A4 | Arb edge appears after volatility shocks, not continuously | Spread trigger conditioned on shock state (RV jump) | Shock-gated entry, fast time stop | 5s-2m | Medium |
| A5 | Basis + spread cointegration gives cleaner entry than spread alone | Error-correction residual using perp basis + spot spread | Enter on residual extremes, exit at half-life | 1m-30m | Medium |
| A6 | Multi-leg synthetic book (Bybit spot/perp + Bitvavo spot) reduces leg risk | Min-cost synthetic mispricing score | Route with cheapest hedge path by liquidity state | 10s-5m | Medium |

### A-Series Promotion Criteria
1. Test PF >= 1.20 net of realistic costs.
2. Positive expectancy under `+30%` cost stress.
3. At least 300 out-of-sample trades with stable monthly PnL.
4. No single day > 20% of total PnL.

## WS3 Directional Microstructure Hypothesis Backlog

### B-Series (Short-Horizon Directional)
| ID | Hypothesis | Signal Definition | Execution Design | Horizon | Priority |
|---|---|---|---|---|---|
| B1 | Queue depletion + aggressive flow predicts continuation | L2 imbalance slope + trade sign Hawkes intensity | Enter with maker leaning, taker fallback if drift starts | 15s-3m | High |
| B2 | Absorption failure predicts reversal burst | Large passive wall repeatedly hit then disappears | Trigger on break event + volume confirmation | 30s-5m | High |
| B3 | Toxic flow regime penalizes fade strategies | VPIN-like toxicity + short-term adverse selection model | Disable fades when toxicity high | 1m-15m | High |
| B4 | Funding event windows create predictable micro drift | Pre/post funding minute flow asymmetry | Event-time strategy around funding timestamps | 5m-30m | Medium |
| B5 | Liquidation cascades overshoot and mean-revert only at depth recovery | Liquidation burst + depth refill ratio | Delay entry until refill condition passes | 1m-20m | Medium |
| B6 | Session handoff transitions produce directional bias | EU->US and Asia->EU transition state model | Trade only in tagged windows | 5m-60m | Medium |
| B7 | Cross-asset leader-follower graph is state-dependent | Dynamic graph lead from BTC/ETH to alts | Portfolio of conditional followers | 1m-30m | Medium |

### B-Series Promotion Criteria
1. Net Sharpe >= 1.5 on walk-forward.
2. Survives spread widening stress and latency perturbation.
3. Stable across at least two market regimes.

## WS4 Structural / Cross-Domain Hypothesis Backlog

### C-Series (Slower, Structural, Regime-Aware)
| ID | Hypothesis | Signal Definition | Execution Design | Horizon | Priority |
|---|---|---|---|---|---|
| C1 | Stablecoin net risk-on flow predicts alt-perp outperformance | Lagged stable inflow imbalance + breadth | 4h rebalance long/short basket | 4h-3d | High |
| C2 | Options skew dislocations predict perp reversals | RR/term-structure shock vs realized move | Fade extreme skew divergence | 1h-24h | High |
| C3 | Token unlock/staking events alter funding crowding | Event clock + pre-positioning/funding slope | Event-driven long/short template | 6h-7d | Medium |
| C4 | On-chain bridge congestion leads exchange-specific pressure | Bridge queue metrics + CEX spread state | Venue-relative trade overlay | 30m-12h | Medium |
| C5 | Macro liquidity proxies explain when micro signals work | Dollar/liquidity regime classifier | Gate micro strategies by regime | 1d-30d | Medium |
| C6 | Narrative attention shocks are tradable only with flow confirmation | Attention anomaly + CEX aggressive flow | Two-stage trigger model | 1h-48h | Medium |

### C-Series Promotion Criteria
1. Information coefficient significant after lag and overlap controls.
2. Strategy alpha remains after adding BTC beta and momentum controls.
3. Transaction-cost-adjusted PF >= 1.25 on OOS.

## WS5 Portfolio and Risk Backlog
| ID | Task | Why | Done criteria |
|---|---|---|---|
| P1 | Strategy correlation matrix (state-conditional) | Avoid stacking same hidden factor | Correlation + tail co-move report |
| P2 | Capital allocator with uncertainty penalty | Size by confidence, not raw backtest returns | Bayesian/robust allocator live sim |
| P3 | Unified kill-switch framework | Prevent death by regime shift | Automated halt rules integrated |
| P4 | Exposure and inventory constraints across venues | Arb can hide directional beta | Hard limits by asset and venue |
| P5 | Latency and API failure risk model | Infra failure can invert expected edge | Real-time risk dashboard + alerts |

## PhD-Level Modeling Tracks (Run in Parallel)
1. Hawkes Process Engine:
- Model self- and cross-excitation of trade flow and liquidations.
- Deliverable: event intensity predictors for B1/B5.

2. Queue-Reactive Fill Model:
- Estimate maker fill probability conditioned on queue position, cancel rates, and opposing aggression.
- Deliverable: realistic expected-value model for A-series.

3. State-Space Lead-Lag Model:
- Time-varying venue leadership using Kalman filtering.
- Deliverable: posterior lead probability for A3/B7.

4. Causal Event Study Framework:
- Synthetic controls for funding flips, unlocks, outages, and listing events.
- Deliverable: effect size library with confidence intervals.

5. Meta-Labeling and Error Decomposition:
- Primary signal + secondary classifier for "when not to trade".
- Deliverable: lift analysis on false-positive reduction.

## Standard Experiment Card (Use for Every Hypothesis)
- Hypothesis ID:
- Market universe:
- Decision timestamp and horizon:
- Feature set and exact lag policy:
- Entry/exit logic:
- Cost model (fees, slippage, maker fill assumptions):
- Risk model (position sizing, stop logic, timeout):
- Validation protocol (split, walk-forward, stress tests):
- Acceptance thresholds:
- Failure mode analysis:
- Artifact paths (raw, processed, metrics, plots):

## 6-Week Execution Plan

### Week 1: Data and Instrumentation Hardening
1. Standardize schema across Bitvavo/Bybit/Binance for L1/trade snapshots.
2. Add freshness, latency, and clock-sync diagnostics.
3. Build dataset quality report auto-generated per day.

### Week 2: Arb Alpha Under Realistic Execution
1. Run A1-A3 with queue-aware simulator (`arb_bitvavo_queue_model.py` extension).
2. Stress test stale bars, spread widening, and maker fill assumptions.
3. Promote only if stable OOS expectancy remains positive.

### Week 3: Microstructure Directional Tests
1. Implement B1-B3 feature pipelines from existing collectors.
2. Run walk-forward on top 15 liquid symbols.
3. Add regime-conditioned performance decomposition.

### Week 4: Structural Signal Integration
1. Build C1-C2 panels (stable flow + options skew).
2. Test standalone and as gating overlays on A/B series.
3. Quantify incremental alpha and turnover impact.

### Week 5: Portfolio Construction
1. Combine surviving strategies with uncertainty-weighted allocator.
2. Simulate correlated drawdown scenarios.
3. Define production guardrails and strategy caps.

### Week 6: Pre-Production Trial
1. Paper trade with live collectors and full execution constraints.
2. Daily drift diagnostics vs expected slippage/fill.
3. Go/No-Go decision with written post-mortem regardless of result.

## Immediate Task Queue (Start Now)
1. Extend `scripts/arb_bitvavo_collector.py` to store quote age and depth-derived liquidity flags.
2. Add queue-position state inputs to `scripts/arb_bitvavo_queue_model.py`.
3. Create `scripts/arb_latency_staleness_study.py` for quote-age asymmetry analysis.
4. Build `scripts/a3_state_space_leadlag.py` (Kalman dynamic lead-lag estimator).
5. Build `scripts/b1_hawkes_flow_signal.py` for micro flow continuation.
6. Build `scripts/b3_toxicity_gate.py` for adverse-selection filtering.
7. Build `scripts/c1_stablecoin_flow_panel.py` with lag-safe joins.
8. Build `scripts/c2_options_skew_panel.py` and event-aligned feature engine.
9. Add `scripts/research_reality_check.py` for permutation/bootstrap validation.
10. Create `docs/research_experiment_registry.md` and log every hypothesis run.

## Decision Rules
Promote a strategy to deeper research only if all conditions hold:
1. OOS expectancy > 0 after realistic costs.
2. Stable performance across at least 2 regimes.
3. No heavy dependence on outlier trades.
4. Capacity supports meaningful deployment size.
5. Failure modes are understood and monitorable.

Archive (do not iterate) if any condition fails twice after reasonable redesign.

## Artifact and Reporting Convention
- Data cache: `app/data/cache/<strategy_family>/<timestamp>_*`
- Strategy docs: `docs/<strategy_family>_<topic>_<YYYY_MM_DD>.md`
- Every run must output:
1. Config snapshot (JSON)
2. Raw trade log (CSV)
3. Summary metrics (JSON)
4. Regime breakdown (CSV)
5. Concentration diagnostics (CSV)

## Final Note
This backlog is intentionally broad, but execution order is strict:
1. Execution-realistic arb first (A-series)
2. Then directional microstructure (B-series)
3. Then structural overlays (C-series)

If A-series cannot survive realistic fills, shift capital and effort to B+C hybrid strategies rather than forcing pure arb.
