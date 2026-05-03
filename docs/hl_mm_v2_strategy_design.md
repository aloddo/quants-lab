# HL MM V2 — Professional Two-Sided Market Making on Hyperliquid

## Mission

Build a professional-grade market making engine for HL shitcoin perpetuals.
Crack profitable MM, prove edge, scale capital, get hired by token projects
as a designated market maker.

$51 is test capital. Strategy must work at $100, $1K, $10K, $100K.
At scale, strategy ITSELF changes: queue dynamics, exit impact, visibility,
and wallet opsec all become first-order concerns.

## Why V1 Failed

1. **Symmetric EV** -- same boolean for bid/ask. One side is almost always more toxic.
2. **Hardcoded spread** -- fixed `min_edge_target_bps` regardless of regime.
3. **No flow awareness** -- 7 signal flags computed, none used for quoting.
4. **500ms REST OMS** -- WS order client exists but unused. Sub-second signals are theater.
5. **No exit-path modeling** -- quoted without knowing if passive exit was likely.
6. **Blind quoting** -- no awareness of WHO is trading or flow direction.
7. **Hedge destruction** -- Bybit hedge costs ~23bps on 5-10bps spreads.
8. **No quote attempt telemetry** -- cannot measure P_fill, cannot compute EV.

## HL-Unique Edge: Wallet Transparency

**This is our #1 differentiator.** No CEX gives you this. On HL:
- Every trade includes wallet addresses (buyer + seller) via `users` field
- Every wallet's positions are queryable via public API
- We already poll top-500 whale positions every 15min (351K docs in MongoDB)

Professional MMs on Binance/Bybit are blind to counterparty identity.
On HL, we can see who is trading, what positions they hold, and whether
their historical trades predict adverse price moves. Wallet intelligence
is NOT a research afterthought -- it is the core alpha source.

## V2 Architecture: Five Layers

```
┌─────────────────────────────────────────────────────────┐
│  Layer 0: EXECUTION INFRASTRUCTURE                       │
│  WS order transport, event-driven cancels, telemetry     │
├─────────────────────────────────────────────────────────┤
│  Layer 1: WALLET + FLOW INTELLIGENCE (HL-unique edge)    │
│  Wallet toxicity scoring, MM competitor tracking,        │
│  smart money flow detection, liquidation maps            │
├─────────────────────────────────────────────────────────┤
│  Layer 2: MICROSTRUCTURE SIGNALS                         │
│  Real L2 OFI, VPIN, cancel/refill churn, cross-venue    │
│  state, event risk detection                             │
├─────────────────────────────────────────────────────────┤
│  Layer 3: SIDE-SPECIFIC EV ENGINE                        │
│  Per-side maker EV, empirical fill tables, markout       │
│  feedback, adaptive spread, distance optimization        │
├─────────────────────────────────────────────────────────┤
│  Layer 4: INVENTORY + RISK                               │
│  Half-life controller, exact exit sizing, pair scoring,  │
│  crowding/event risk gates, wallet opsec                 │
└─────────────────────────────────────────────────────────┘
```

### Layer 0: Execution Infrastructure

**WS Order Transport:**
Harden existing `ws_order_client.py` (currently ~60% complete):
1. Reconnect loop with exponential backoff
2. Heartbeat/ping for HL WS keepalive
3. Stale response cleanup (evict after 30s)
4. Integrate: replace `_place_alo()` and `_cancel_coin_orders()` in quote_engine
5. Keep REST for: open_orders queries, fill sync, reconciliation
6. Note: WS may share IP-level rate budget with REST. Primary benefit is
   latency (~100ms vs ~500ms), not rate bypass. Measure actual behavior.

**Event-driven cancels (requires WS transport):**
Cancel exposed side immediately on WS events, not at next tick:
- Cross-venue mid moves > 1.5bps in 250ms -> cancel both sides
- Same-side top-3 depth drops > 35% in 250ms -> cancel that side
- Spread jumps > 2x its 10s median -> cancel both sides
- Toxic wallet detected in trade feed -> cancel same-side, widen opposite

**Quote Attempt Telemetry:**
Every quote placement logs lifecycle data:
```
{
  coin, side, placed_at, ended_at,
  price, ticks_from_touch, queue_ahead_usd,
  spread_bps, rv_bucket, imbalance_bucket, vpin_bucket,
  filled_qty, outcome: "full_fill" | "partial" | "cancelled" | "expired",
  cancel_reason: "requote" | "state_change" | "hard_gate" | "tick_timeout",
  fill_wallets: [buyer_addr, seller_addr],  // if filled
  markout_1s, markout_5s  // if filled
}
```
Extends existing QuoteLog. This is the data that makes EV computation possible.

**Fix trade data pipeline:**
Current WS trade handler overwrites latest batch per tick (lossy).
Replace with append-to-ring-buffer. ALL trades must reach signal layer
with full fidelity: side, size (not just count), price, wallet addresses.

### Layer 1: Wallet + Flow Intelligence (CORE, not afterthought)

**1a. Live Wallet Toxicity Model**
Data source: `users` field on every HL trade (buyer + seller addresses).
For each wallet, compute rolling markout statistics:
- Track 1s/5s/30s post-trade price move after their aggressive trades
- Use Bayesian shrinkage toward global prior (handles sparse wallets)
- Confidence gate: need >= 50 trades AND >= $2K matched notional AND
  lower confidence bound on markout worse than -2bps at 5s

Action when toxic wallet detected in trade feed:
- Cancel same-side quotes for 2-5s
- Widen opposite side by 1-2 ticks
- Halve position size for that coin for 30s

Start collecting wallet data from DAY 1. Live gating activates once
sufficient data accumulates (typically 3-7 days per coin).

**1b. Competitor MM Tracking**
Data source: HL public positions via `user_state(address)`.
Method:
- On startup: scan leaderboard for wallets with positions on our target coins
- Identify MM wallets by two-sided quoting behavior patterns
- Cluster into MM groups (same entity = correlated position changes)
- Compute crowding score: how many MMs are warehousing the same direction

Action:
- If 3+ large MM wallets reduce same side within 5min -> demote that pair
- Stop warehousing a side where MM crowding score > 2 sigma
- Use as pair selection signal (pairs with fewer competing MMs = better)

Polling frequency: every 60s for top 5-10 wallets (1 REST call each).

**1c. Smart Money Flow Detection**
Cross-reference whale positions (already collected every 15min) with
real-time trade flow:
- If known whale wallet appears as aggressive buyer in trade feed AND
  their position just increased -> informed flow, fade with caution
- If whale flow aligns with L2 OFI and momentum -> strong signal, pull quotes

**1d. Liquidation/Crowding Maps**
Data sources: Coinalyze liquidation events (358K docs), Bybit OI (667K docs),
Bybit LS ratio (150K docs), HL public positions.
- Estimate liquidation price clusters from public position data
- If price is within 0.5 ATR of a liquidation cluster -> do not absorb that side
- If crowding score > 2 sigma AND price approaching cluster -> pull all quotes
- When liquidation cascade is IN PROGRESS -> PAUSE (don't tighten, proved wrong)

### Layer 2: Microstructure Signals

**Real L2 OFI (replace fake OFI):**
Current `ofi_bps` is summed mid-price moves. That is price momentum, not order flow.
Real L2 OFI:
```
L2_OFI = Σ (Δbid_size@level_i - Δask_size@level_i)  for i in [1..5]
```
Per book update (not per tick). 5s rolling sum. Used as regime classifier.

**VPIN (corrected):**
- Fix: ring buffer for ALL trades (not overwrite-latest)
- Fix: track actual $ volume per side (not just direction count)
- Bucket size: coin_daily_volume / 1000
- 50 bucket lookback
- Hard gate: VPIN > 0.8 -> pull all quotes
- Soft gate: VPIN > 0.65 -> widen spread

**Cancel/Refill Churn:**
Per-level tracking: adds, cancels, time-to-refill after depletion.
High cancel ratio + slow refill on one side = institutional exit signal.

**Trade Imbalance (corrected):**
Current code counts prints, not size. Fix: use notional-weighted imbalance.
`imbalance = Σ(buy_notional) / Σ(total_notional)` over 10s rolling window.

**Cross-Venue State (NEW):**
Currently only Bybit ticker as anchor. Professional MMs watch:
- Binance spot+perp top-of-book (via WS)
- OKX perp top-of-book (via WS)
- Multi-venue synthetic mid with confidence weights
- Cross-venue basis divergence as information signal

When cross-venue synthetic mid moves > 1.0-1.5bps in 250ms:
pull stale side immediately via WS cancel.

**Token Event Risk (NEW):**
Data sources (to be collected):
- Token unlock calendars (TokenUnlocks API or scrape)
- Treasury/multisig wallet transfers (on-chain monitoring)
- Exchange deposit address flow spikes (on-chain or API)
- Social sentiment spikes (Twitter/Telegram API)

Action:
- If projected supply event > 1% of ADV in next 6h -> reduce size 50-100%
- If exchange deposit flow spikes > 3 sigma in 15min -> block quoting
- This is "do not warehouse" mode for event risk

**Options Vol as Jump Risk (uses existing Deribit data):**
3.2M Deribit options surface docs already in MongoDB.
- Compute front-end skew and IV-RV gap for BTC/ETH
- When IV-RV gap shocks > 2 sigma -> multiply tox buffers by 1.5-2.0x
- Applies to ALL pairs (crypto vol is correlated)

**Wire Existing Unused Signals:**
7 flags already computed in signal_engine, never used:
- depth_drop_detected -> hard gate (cancel exposed side)
- spread_spike_detected -> hard gate (widen or pull)
- trade_imbalance_toxic -> soft gate (widen adverse side)
- touch_depletion -> implement properly (currently stubbed), soft gate
- imbalance_z -> EV regime bucket
- rv_30s -> EV regime bucket + spread width scaling
- anchor_jump -> hard gate (cancel all)

### Layer 3: Side-Specific EV Engine

**Per-side EV computation every tick:**
```
EV_side = half_spread - 1.44 - E[markout | side, regime] - exit_penalty

Where:
  half_spread    = quote distance from mid (optimized by fill tables)
  E[markout]     = EWMA markout from fill tables, stratified by:
                   side x rv_bucket x imbalance_bucket x ticks_from_touch
                   x wallet_toxicity_bucket
  exit_penalty   = (1 - P_passive_exit) x 3.5bps
  P_passive_exit = from empirical model (holding_time, spread, inv_frac)

Sign convention: current markout stores favorable = positive.
EV formula needs adverse as positive cost:
  E[markout_cost] = max(0, -EWMA(markout_5s))
```

Quote a side only when EV_side > threshold. Both can be positive (two-sided).
Either can be negative independently. Neither may be positive (sit out).

**Empirical Fill Tables (STAGED DIMENSIONALITY):**

The full stratification has too many dimensions for sparse shitcoin data.
Start compressed, expand only as fill count supports each new dimension.

Stage 1 (Phase 1 -- minimal): `side x coin x vol_regime`
  - 2 sides x N coins x 3 vol buckets = 6N cells
  - Need 8+ fills per cell. At 20 fills/day on 2 coins: ~5 days to populate.

Stage 2 (Phase 3 -- expand): add `ticks_from_touch`
  - 2 x N x 3 x 3 distance buckets = 18N cells
  - Need ~2 weeks of data at 20 fills/day/coin.

Stage 3 (Phase 3+ -- full): add `imbalance_bucket, VPIN_bucket, wallet_toxicity`
  - Only add each dimension when 8+ fills exist in the new strata.
  - Use Bayesian shrinkage: sparse cells pull toward the parent stratum mean.

For each populated bucket: P_fill, E_markout_5s, E_capture.

Optimal distance d maximizes:
```
EV(d) = P_fill(d) x (d_bps - 1.44 - E_markout(d)) - (1-P_fill(d)) x opp_cost
```

Do not improve inside touch if: P(fill before cancel) < 15% OR
P(passive exit) < 60% OR expected time-to-fill > cancel horizon.

**Model decay guard**: stale data (>48h) gets exponentially downweighted.
Regime changes (vol spike, spread compression, new MM entering) invalidate
affected strata. Old fill tables are NOT truth -- they are decaying estimates.

**Markout Feedback Loop:**
- EWMA markout per side x coin x regime (with Bayesian shrinkage)
- Negative markout -> widen that side
- Positive markout -> can tighten (carefully, with bounds)
- Auto-disable coin for 30min if 8-fill EWMA < -4bps

### Layer 4: Inventory + Risk

**Half-life controller (15s target passive exit):**
- |q| > 0.33 Q_soft: suppress entry side, exit-only
- |q| > 0.50 Q_soft: boost exit size 1.5x, add 2 ticks skew
- age > 20s OR adverse > 2bps: full exit-only mode
- age > 45s: HL taker close (3.5bps, cheaper than prolonged adverse exposure)

**Exit sizing: EXACT match.**
Exit order size = abs(inventory). No overshoot. No position flipping.

**No Bybit hedging.**
Bybit hedge round-trip ~23bps. Exceeds all shitcoin spread edge. Exit via
HL passive or HL taker close only.

**Pair Scoring (V2 screener):**
Replace current edge-room ranking with expected-value ranking:
```
score = E[fill_rate] x E[net_capture] x survivability

Where:
  E[fill_rate]     = from fill tables (or prior if no data)
  E[net_capture]   = spread - fees - E[markout]
  survivability    = P(passive_exit) x (1 - wallet_toxicity_penalty)
```

Hard pair filters:
- Median spread >= 8bps (5bps names are incentive trades only)
- Anchor healthy >= 80% of time
- Expected 5s adverse markout <= 35% of side edge
- Passive exit probability >= 60%
- Crowding score < 2 sigma (too many competing MMs = bad)

**Wallet Opsec (at scale):**
- At $10K+: shard inventory across multiple agent wallets
- Rotate agent keys periodically
- Monitor whether public visibility of position worsens post-fill markout
- If markout degrades after position becomes visible -> wallet is being targeted

**"Do Not Warehouse" Mode:**
Not all regimes are quoting regimes. Professional MM sits out during:
- Token unlock events (supply shock)
- Liquidation cascades (forced flow, extreme vol)
- High crowding + approaching liquidation cluster
- IV-RV gap shocks > 2 sigma (macro jump risk)
- Exchange deposit spikes (potential large seller incoming)

## Economics Reality Check

```
S - 2.88bps is the passive round-trip CEILING, not expected outcome.

At 8bps spread, side edge before adverse selection = 2.56bps.
If E[adverse markout] = 1.2bps and P(passive exit failure) = 30%:
  EV = 2.56 - 1.2 - 0.30*3.5 = 0.31bps
  Barely tradable.

At 12bps spread, side edge = 4.56bps:
  EV = 4.56 - 1.2 - 0.30*3.5 = 2.31bps
  Viable.

5bps names are fake edge unless you have fee incentives or
exceptionally benign counterparty mix. Treat as incentive trades only
until empirical tables prove otherwise.

Profitability scales with capital BUT NOT linearly at scale:
  - At $1K: viable on sparse names, tiny participation
  - At $10K: viable if WS-based OMS and high P(passive exit)
  - At $100K: strategy must change -- queue dynamics, visibility,
    wallet sharding, and hedge policy all become first-order
```

## Implementation Phases

**Key insight from final review: START COMPRESSED, EXPAND WITH DATA.**
The full 5-layer design is the target architecture. But implementation must
prove a sample-efficient compressed model first, then add dimensions.
Trying to implement all 5 layers before proving the core = failure.

### Phase 0: Measurement Layer (THE BLOCKER -- nothing else matters without this)

This is the single most important component. Not WS transport (faster wrong
quotes just lose faster). Not wallet toxicity (cold-started, depends on data).

Build:
1. **Lossless trade capture**: ring buffer replacing overwrite-latest. ALL trades
   with full fidelity: side, SIZE (not count), price, wallet addresses.
2. **Quote-attempt lifecycle telemetry**: placed_at, ended_at, ticks_from_touch,
   queue_ahead_usd, outcome, cancel_reason, fill_wallets, markout_1s/5s.
3. **Wire all 7 existing signal flags** into quoting gates.
4. **Fix fake OFI** with real L2 size-delta OFI.
5. **Fix trade imbalance** to be notional-weighted.

Deploy in SHADOW MODE on 1-2 widest-spread pairs. Collect data. No capital.

**Gate: telemetry flowing to MongoDB, shadow quotes generating attempt records.**

### Phase 1: Compressed Live Model (first profitable trade)

Minimal live model with LOW-DIMENSIONAL EV: `side x coin x vol_regime` only.
No fill tables yet (not enough data). No wallet toxicity yet (cold-started).

Build:
1. **Side-specific markout gate**: per-side EWMA of recent markout_5s.
   If side markout < -threshold -> don't quote that side. Simple, sample-efficient.
2. **Exact exit sizing**: exit order size = abs(inventory). No overshoot.
3. **Strict age-based flatten**: 45s taker close, not 180s.
4. **Volatility scaling**: rv_30s > 2x baseline -> widen; > 3x -> pull.
5. **Hard signal gates**: depth_drop, spread_spike, anchor_jump -> cancel.

Deploy LIVE with test capital on widest-spread pairs.
WS transport can run in parallel if resources allow, but is NOT the blocker.

**Gate: positive net capture after fees and taker-close fallback on 24h of data.**

### Phase 2: WS Transport + Wallet Data Collection

Now that we have a working model and data flowing:
1. **Harden ws_order_client.py**: reconnect, heartbeat, response cleanup.
2. **Integrate into orchestrator**: replace 3 REST call sites.
3. **Measure actual RTT improvement** (may not be as dramatic as expected
   if rate limits are IP-level).
4. **Start collecting wallet addresses** from trade WS `users` field.
5. **Begin building wallet markout scores** (needs 3-7 days of data).

**Gate: WS RTT < 150ms confirmed. Wallet data accumulating.**

### Phase 3: Expand EV Model

Add dimensions as data supports them:
1. **Empirical fill tables**: `side x coin x touch x vol_regime` first.
   Only add queue/imbalance/VPIN buckets when 8+ fills per stratum exist.
2. **Markout feedback loop**: EWMA per side x coin x regime, auto-widen/tighten.
3. **Wallet toxicity live gating**: only after >= 50 trades per wallet,
   >= $2K notional, lower confidence bound on markout < -2bps.
4. **VPIN computation** (corrected): volume-bucketed, used as regime classifier.

**Model decay guard**: fill tables expire aggressively. Stale data (>48h) gets
downweighted. Regime changes invalidate old strata.

**Gate: fill tables populating with valid P_fill estimates. Markout improving.**

### Phase 4: Advanced Intelligence

Only after Phases 0-3 are profitable:
1. **Competitor MM tracking**: position crowding, pair demotion.
2. **Cross-venue synthetic mid**: Binance/OKX spot+perp TOB.
3. **Token event risk**: unlock calendars, deposit flow spikes.
4. **Options vol overlay**: Deribit IV-RV gap as jump-risk multiplier.
5. **Liquidation maps**: crowding score near liquidation clusters.

**Gate: each component validated offline before live use.**

### Phase dependency graph
```
Phase 0 (measurement) ──→ Phase 1 (compressed live) ──→ Phase 3 (expand EV)
       │                                                       ↑
       └──→ Phase 2 (WS + wallets) ────────────────────────────┘
                                                     Phase 4 (advanced, independent)
```

## Kill Criteria

| Metric | Threshold | Action |
|--------|-----------|--------|
| Session loss | > configured limit | Stop engine |
| Rolling 8-fill EWMA markout (per side) | < -4bps | Disable that side 30min |
| Rolling 50-fill markout (per coin) | < -3bps | Demote coin in screener |
| VPIN sustained > 0.8 for 5min | -- | Pull quotes that coin |
| Fill sync blind | > 1min | Pause all quoting |
| Crowding score > 2 sigma + near liquidation | -- | Do not warehouse mode |
| Token event within 6h > 1% ADV | -- | Reduce size 50-100% |

## Adversarial Defense

Known attack vectors against this strategy:
1. **Inventory baiting** -- fill one side, lean book, wait for forced exit ladder.
   Defense: tight half-life (45s taker close), side-specific EV kills the baited side.
2. **Hysteresis exploitation** -- predictable 1.5-3s stale-quote windows.
   Defense: WS event-driven cancels eliminate the window.
3. **Rate budget exhaustion** -- nudge FV to trigger requote churn.
   Defense: WS transport, requote gating with hysteresis.
4. **Wallet surveillance** -- query our position, lean against forced exit.
   Defense: wallet opsec at scale (sharding, rotation, monitoring).
5. **Toxic flow splitting** -- small prints to evade size-based detection.
   Defense: wallet-level attribution (aggregate per wallet, not per trade).

## Platform + Operational Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| HL changes fee structure mid-session | EV model invalidated instantly | Monitor fee tier on startup + periodic check; auto-pause if fee changes |
| HL removes/changes `users` field in trade WS | Core alpha source disappears | Wallet intelligence is an EDGE, not a dependency. Compressed EV model works without it. |
| HL restricts API access or adds stricter rate limits | Can't quote | Degrade gracefully: reduce pair count, widen requote interval, pause lowest-EV pairs first |
| WS order transport doesn't actually reduce latency | Sub-second signals remain theater | Measure actual RTT before relying on it. Phase 1 works on REST. WS is Phase 2. |
| All shitcoin spreads compress to 3-5bps | No tradeable edge at our cost structure | Pair screener auto-demotes. Engine naturally stops quoting when EV < threshold. |
| Model decay: fill tables go stale during regime change | Trading on wrong estimates | 48h exponential decay. Regime-change detection resets strata. Auto-pause on rapid markout degradation. |
| Queue model risk: queue_ahead_usd != fill probability | Distance optimization is wrong | Validate P_fill empirically before trusting. Use conservative distance (wider) until validated. |

## Confidence Assessment

After 4 rounds of adversarial review:
- Design is directionally sound.
- The compressed implementation path (Phase 0 -> 1 -> 2 -> 3) is realistic.
- 4/10 confidence of net profitability within 30 days (Codex final round).
- Key risk: sample efficiency on sparse shitcoins. Key mitigation: staged dimensionality.
- If Phase 1 compressed model shows positive markout on 50+ fills, confidence rises to 6-7/10.
- If wallet toxicity proves predictive (t-stat > 2), confidence rises to 7-8/10.

## What Already Exists (reuse, don't rebuild)

- `pair_screener.py`: Dynamic pair ranking. Works. Evolve to EV-based scoring.
- `signal_engine.py`: 7 unused flags + fake OFI. Fix OFI, wire all flags.
- `fill_tracker.py`: Markout tracking, QuoteLog. Extend with attempt telemetry.
- `ws_order_client.py`: ~60% complete. Harden, don't rewrite.
- `fair_value.py`: 3-tier anchor. Works. Extend to multi-venue.
- `state_machine.py`: 7 states. Tighten age limits. Works.
- `inventory_manager.py`: AS reservation + position tracking. Equity fixed.
- `quote_engine.py`: Replace hardcoded edge with EV-driven spread.
- `orchestrator.py`: Wire everything. Event-driven cancel path.
- `d1_hl_whale_collector.py`: Whale position polling. Already running.
- MongoDB: 980K funding, 351K whale positions, 667K OI, 1.2M L2, 2.5M trades,
  3.2M Deribit options, 358K liquidations, 150K LS ratio. Mostly untapped.

## Data We Need But Don't Have Yet

| Data | Source | Priority | Use |
|------|--------|----------|-----|
| Wallet addresses per trade | HL trade WS `users` field | P0 | Toxic wallet scoring |
| Binance/OKX spot+perp TOB | WS subscriptions | P1 | Multi-venue synthetic mid |
| Token unlock schedules | TokenUnlocks API / scrape | P2 | Event risk gating |
| Treasury/multisig transfers | On-chain monitoring | P2 | Supply shock detection |
| Exchange deposit flow | On-chain / exchange API | P3 | Large seller detection |
| Social sentiment spikes | Twitter/Telegram API | P3 | Event risk gating |
