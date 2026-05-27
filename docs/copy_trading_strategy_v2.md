# Copy Trading Strategy V2: Framework & Wallet Selection

## Revision History
- v2.0 (2026-05-11): Initial framework after V1 DCA failure analysis
- v2.1 (2026-05-11): Codex adversarial review R1 incorporated (16 findings)
- v2.2 (2026-05-11): Complete metrics, OOS strategy, concrete procedure
- v2.3 (2026-05-11): Codex R2 fixes (13 findings): OOS leak, S3 fields, FDR spec, A1 fix, episode minimums

## 1. The Problem

We observe wallet W's fills on HL's trade stream. We copy their directional bets.

### Full PnL Equation

```
Our PnL per trade = forward_return(from OUR entry to OUR exit)
                  - spread_entry - spread_exit
                  - taker_fee_entry - taker_fee_exit
                  - funding(rate * hold_time)
                  - slippage(IOC vs mid)

Where:
  OUR entry  = W's fill price + detection_lag_bps + spread/2
  OUR exit   = W's exit price - exit_detection_lag_bps - spread/2
  taker fees = 4.32 bps per side (8.64 RT) at current tier
```

W's PnL != our PnL. Our entry is always worse (we're late). Our exit is always worse (we're late again). The gap is the "copy tax." Wallet alpha must exceed this tax.

### What We See vs Don't See

See: fills (coin, side, size, price, timestamp) on HL trade stream.
Don't see: intent, other venues, hedges, whether this is a directional bet or hedge/basis/rebalance/liquidation.

## 2. Copyable Alpha Types

| Type | Edge Source | Decay | Copyable | Requirement |
|------|-----------|-------|----------|-------------|
| Information | Knows something market doesn't | Hours-days | YES | Hold time >> our lag |
| Timing | Enters early in moves | Min-hours | YES | Alpha survives 30s delay |
| DCA/Capital | Averages down with deep pockets | N/A | NO | Need matching capital |
| Impact | Their buy IS the price move | Instant | NO | We buy the moved price |
| Speed | Latency arb | Milliseconds | NO | Too fast |
| MM/Flow | Captures spread | N/A | NO | Not directional |

## 3. Available Data

| Dataset | Records | Wallets | Period | Has Open/Close | Has closedPnl |
|---------|---------|---------|--------|----------------|---------------|
| hl_s3_fills | 4.9M | 10 | Apr 9 - May 8 (30d) | YES | YES |
| hl_wallet_trades | 3.3M | 63K | May 4 - May 11 (7d) | NO | NO |
| hl_copy_target_fills | 8.9K | 12 | May 9 - May 11 (2d) | NO | NO |
| S3 backfiller | unlimited | any | up to 90d | YES | YES |

**S3 backfiller**: can pull full fill history (with Open/Close direction and closedPnl) for ANY wallet address going back 90 days. This is the key to expanding coverage.

## 4. Data & Out-of-Sample Strategy

### Data Source: Already Downloaded

We have 19GB of raw S3 data locally at `app/data/hl_s3_raw/`. This is EVERY fill
on Hyperliquid for EVERY wallet for 30 days (Apr 9 - May 8, 720 hourly files).

Each fill record contains ALL fields:
- `wallet`, `coin`, `side` (B/A), `size`, `price`, `time`
- `dir` (Open Long / Close Long / Open Short / Close Short)
- `closedPnl` (realized PnL on Close fills)
- `fee`, `crossed` (taker/maker), `startPosition`, `hash`

This is the complete universe. No downloading, no backfilling, no collector needed.
~52K fills per hour = ~37M fills total across all wallets.

### Data Pipeline

```
1. PARSE:   Read all 720 .lz4 files from app/data/hl_s3_raw/
2. INDEX:   Group by wallet address, extract episodes (Open -> Close cycles)
3. PROFILE: Compute all Section 5 metrics per wallet
4. FILTER:  Apply Section 6 checklist with train/test split
```

### Temporal Splits

```
TRAIN:  Apr 9 - Apr 29   (21 days, 70%)  -- wallet selection, criteria evaluation
TEST:   Apr 29 - May 8   (9 days, 30%)   -- independent validation
OOS:    May 8 - May 11   (3 days)        -- live collector period (forward validation)
```

A wallet must pass mandatory criteria on TRAIN and TEST independently.
OOS (collector period) is checked last as forward confirmation.

### Anti-Snooping Rules

1. Checklist thresholds FROZEN in this document before running against any data
2. No threshold tuning after seeing results on ANY split
3. S3 raw data is the single analysis dataset (complete, every wallet, every fill)
4. Wallet must pass mandatory filters on train AND test independently
5. Apply Benjamini-Hochberg FDR correction on train split (see Section 6E)
6. Report full funnel: how many wallets at each filter stage

## 5. Wallet Profiling Metrics (complete set)

### 5A. Return Metrics

| Metric | Formula | Purpose |
|--------|---------|---------|
| First-entry PnL (bps) | (exit_px - first_entry_px) / first_entry_px * 10000 | W's timing alpha (THEIR price) |
| Simulated copy PnL (bps) | See formula below | Our REALISTIC PnL after all costs |
| Closed PnL ($) | Sum of closedPnl from S3 fills | Realized profit |
| PnL per $ traded (bps/$) | closed_pnl / total_notional * 10000 | Capital efficiency |
| PnL per episode ($) | closed_pnl / n_episodes | Average dollar return |

**Simulated Copy PnL formula (complete):**
```
For LONG episodes:
  our_entry  = W_entry_px * (1 + entry_lag_bps/10000 + spread_bps/20000)
  our_exit   = W_exit_px  * (1 - exit_lag_bps/10000  - spread_bps/20000)
  gross_bps  = (our_exit - our_entry) / our_entry * 10000
  fees_bps   = 8.64  (4.32 entry + 4.32 exit, taker both sides)
  funding_bps = avg_funding_rate_bps * hold_hours / 8
  sim_copy_pnl = gross_bps - fees_bps - funding_bps

For SHORT episodes:
  our_entry  = W_entry_px * (1 - entry_lag_bps/10000 - spread_bps/20000)
  our_exit   = W_exit_px  * (1 + exit_lag_bps/10000  + spread_bps/20000)
  gross_bps  = (our_entry - our_exit) / our_entry * 10000
  (rest same)

Defaults: entry_lag = 5 bps, exit_lag = 5 bps, spread = coin-specific (use 3 bps for BTC/ETH, 8 bps for alts)
Funding: use historical HL funding rate for that coin during hold period
```

### 5B. Risk-Adjusted Metrics

| Metric | Formula | Purpose |
|--------|---------|---------|
| Sharpe ratio | mean(episode_returns) / std(episode_returns) * sqrt(252) | Risk-adjusted performance |
| Profit factor | gross_wins / gross_losses | Quality of wins vs losses |
| Expectancy (bps) | avg_win * WR - avg_loss * (1-WR) | Expected value per trade |
| Max drawdown ($) | Largest peak-to-trough in cumulative PnL | Worst streak impact |

### 5C. Consistency Metrics

| Metric | Formula | Purpose |
|--------|---------|---------|
| Win rate | wins / total_episodes | Hit rate |
| Positive days % | days_with_positive_pnl / days_with_any_trades | Day-level consistency |
| Top-3 trade concentration | sum(top_3_pnl) / total_pnl | Luck dependence |
| Top-1 coin concentration | best_coin_pnl / total_pnl | Single-coin dependence |
| Max consecutive losses | Longest losing streak | Streak risk |

### 5D. Style Metrics

| Metric | Formula | Purpose |
|--------|---------|---------|
| Entries per episode | mean, median of entry count per episode | DCA vs momentum |
| DCA lift (bps) | avg_entry_pnl - first_entry_pnl | Positive = DCA dependent, negative = timing alpha |
| Hold time | median episode duration | Speed of strategy |
| Long PnL / Short PnL | Split by direction | Directional bias |
| Long WR / Short WR | Split by direction | Side-specific consistency |
| Coins traded | Number of unique coins | Diversification |
| Trade frequency | Episodes per day | Activity level |

### 5E. Alpha Quality Metrics

| Metric | Formula | Purpose |
|--------|---------|---------|
| Forward return T+30s | Price change 30s after W's entry | Alpha survives our detection? |
| Forward return T+5m | Price change 5m after | Alpha persists |
| Forward return T+1h | Price change 1h after | Alpha is slow-moving |
| Alpha decay ratio | fwd_ret_T+30s / fwd_ret_T+exit | How much captured in first 30s |
| BTC-adjusted return | Episode return minus BTC return over same period | Wallet-specific vs beta |
| Regime split | PnL in BTC-up vs BTC-down vs BTC-flat | Regime dependence |

### 5F. Execution Feasibility Metrics

| Metric | Data source | Purpose |
|--------|-------------|---------|
| Avg entry notional | S3 fills | Impact potential |
| Entry notional vs typical book depth | Book data (if available) | Market impact risk |
| Trading hours distribution | Fill timestamps | Time-of-day pattern |
| Coin liquidity profile | HL volume data | Can we actually fill? |

## 6. Wallet Selection Checklist

All thresholds below are FROZEN. Do not modify after seeing data.

### 6A. Mandatory (all must pass on EACH split independently)

| # | Criterion | Threshold | Justification |
|---|-----------|-----------|---------------|
| M1 | Simulated copy PnL | > 0 bps mean | Positive after all costs |
| M2 | Simulated copy WR | > 50% | More wins than losses at our entry |
| M3 | Entries per episode | median <= 2 | Not DCA (our core filter from V1 failure) |
| M4 | Completed episodes | >= 30 per split (>= 60 total) | t-test needs N>=30 for power; PF/Sharpe meaningless below this |
| M5 | Active | Trade in last 7 days | Not a dead wallet |
| M6 | Hold time | median > 5 min, median < 48h | Not HFT (can't copy), not passive (too slow) |
| M7 | Expectancy | > 5 bps after simulated costs | E = avgW*WR - avgL*(1-WR) > fee friction |
| M8 | DCA lift | <= 0 bps (timing alpha, not DCA) | Core lesson: DCA lift > 0 means uncopyable at our capital |

### 6B. Robustness (all must pass on train+test combined)

| # | Criterion | Threshold |
|---|-----------|-----------|
| P1 | Profit factor | > 1.2 |
| P2 | Positive days | > 50% of days with trades |
| P3 | Top-3 trade concentration | < 50% of total PnL |
| P4 | Remove best coin: still profitable | Yes |
| P5 | Remove best day: still profitable | Yes |
| P6 | If trades both sides: Long WR > 35% AND Short WR > 35% | Neither side is a pure loser (single-side wallets exempt) |
| P7 | Sharpe > 0.5 | Meaningful risk-adjusted return |

### 6C. Risk (all must pass)

| # | Criterion | Threshold |
|---|-----------|-----------|
| R1 | 95th pct adverse excursion | < 500 bps |
| R2 | Max consecutive losses | < 6 |
| R3 | Worst single trade | < -800 bps |

### 6D. Anti-Gaming (all must pass)

| # | Criterion | Threshold | Why |
|---|-----------|-----------|-----|
| A1 | Not a market maker | < 30% of episodes have opposing entry within 5 min of prior exit | MMs flip constantly; directional traders don't |
| A2 | Closed PnL > 0 | Sum of closedPnl from S3 fill data | Actually realized, not unrealized |
| A3 | Not a known MM/vault/protocol | Manual check on finalists | Avoid non-trading systematic flows |

### 6E. Statistical Significance (FDR-corrected, on train split)

The FDR correction ensures we don't select noise wallets from a large universe.

```
Null hypothesis H0: Wallet W has zero simulated copy alpha (mean sim_copy_pnl = 0)
Test statistic:     One-sample t-test on per-episode sim_copy_pnl values
P-value:            From t-distribution with df = n_episodes - 1
Correction:         Benjamini-Hochberg across ALL wallets that pass M1-M8
Threshold:          Adjusted q-value < 0.10

Procedure:
1. For each wallet passing M1-M8 on train split, compute t-test p-value
2. Collect all p-values into a vector
3. Apply BH correction
4. Only wallets with adjusted q < 0.10 proceed to robustness checks
5. Report: N wallets tested, N passing, effective FDR
```

Note: with N=30 episodes and typical crypto return noise, this will reject many wallets.
That is the point. If a wallet can't demonstrate statistical significance at 30+ episodes,
we don't have evidence it's not noise.

### 6F. Alpha Baselines (>= 3 of 4 must pass, on test split only)

| # | Criterion | Method |
|---|-----------|--------|
| B1 | vs random entry | Simulated copy PnL > 90th pct of 1000 random entries on same coins |
| B2 | vs BTC beta | PnL still positive after BTC-adjustment |
| B3 | Forward return T+30s | Median > 0 (alpha survives our detection window) |
| B4 | Delayed entry sim at T+30s | Still profitable at 30s lag |

## 7. Wallet Selection Procedure (step by step)

```
STEP 1: PARSE ALL S3 RAW DATA
  - Read all 720 .lz4 files from app/data/hl_s3_raw/
  - Extract all fills for all wallets (no pre-filtering)
  - Store parsed fills in MongoDB or process in-memory
  -> ~37M fills, every wallet on HL for 30 days

STEP 2: PRELIMINARY SCREEN (on TRAIN data only, Apr 9-29, activity only, NO performance)
  - Completed episodes (Open->Close): >= 30 in train period
  - Not a pure MM: < 30% of episodes have opposing entry within 5min of exit
  -> Expected: ~2000-5000 active directional wallets
  NOTE: test data (Apr 29 - May 8) not loaded or examined until Step 6

STEP 3: PROFILE (compute ALL section 5 metrics on full dataset)
  - Split data: train (first 60%) / test (last 40%)
  - Compute every metric in Section 5 on EACH split
  -> Output: wallet_profiles.csv with all metrics

STEP 4: FILTER (apply Section 6 checklist)
  - Apply 6A mandatory on train split
  - Apply 6A mandatory on test split (independent)
  - Apply 6B robustness on combined
  - Apply 6C risk on combined
  - Apply 6D anti-gaming
  - Apply 6E baselines on test split only
  - Apply FDR correction across all wallets evaluated
  -> Output: passing_wallets.csv with full scorecard

STEP 5: RANK (order passing wallets)
  - Primary: Sharpe ratio
  - Secondary: Simulated copy PnL
  - Tertiary: Episode count (prefer more data)
  -> Output: ranked_wallets.csv, top 5-15

STEP 6: VALIDATE ON OOS (7-day collector data)
  - For ranked wallets, check: are they still passing M1-M8 on the
    collector period (which they've never been evaluated on)?
  - If yes: candidate confirmed
  - If no: flag as potentially decaying

STEP 7: REPORT
  - Full funnel: how many wallets at each filter stage
  - Profile cards for top 10 with all metrics
  - Known limitations and confidence level
```

## 8. Execution Mechanics (V10 Code Changes)

### What changes from V9:

| Component | V9 (DCA wallets) | V10 (Momentum wallets) |
|-----------|------------------|------------------------|
| Entry trigger | TWAP accumulation over 120s, $500 min | First fill from qualified wallet |
| Entry speed | Delayed (wait for TWAP) | Immediate (wallet IS the signal) |
| Entry guard | Margin check only | Chase distance + spread + depth + margin |
| Add-ons | Yes (DCA with target) | No (single entry only) |
| Exit primary | Target reverse TWAP | Target reverse flow |
| Exit stop | Max hold 10h | Hard stop -150bps + max hold 12h |
| Position sizing | Fixed $20 | Fixed $11, per-wallet margin budget |
| Wallet selection | Manual (11 whales) | Algorithmic (checklist-qualified) |

### Entry Guards (new in V10):

```python
# Before placing entry order:
if price_moved_since_w_fill > 15_bps: skip("chase too far")
if best_spread > 20_bps: skip("spread too wide")
if book_depth_entry_side < 3000: skip("book too thin")
if existing_position_this_coin: skip("already exposed")
if wallet_margin_budget_exceeded: skip("margin limit")
```

### Exit Logic (simplified from V9):

```python
# Three triggers, first fires:
1. W reverse flow detected -> IOC exit (3 retries, then market)
2. unrealized_pnl < -150_bps -> hard stop
3. hold_time > 12_hours -> force exit
# NO trailing stop (too easily whipsawed on momentum)
# NO add-ons
# NO DCA
```

## 9. Monitoring

### Per-trade logging (mandatory):

```
entry_time, w_fill_price, our_fill_price, detection_lag_ms,
spread_at_entry, book_depth_at_entry, funding_rate,
forward_ret_1m, forward_ret_5m, forward_ret_30m,
exit_time, exit_price, exit_type, pnl_bps, pnl_usd
```

### Kill conditions:

| Condition | Threshold | Action |
|-----------|-----------|--------|
| Per-wallet copy PnL | < -3% of equity after 10+ trades | Pause wallet |
| Per-wallet WR | < 35% rolling 20 trades | Drop wallet |
| Daily loss | > 5% of equity | Kill entries, keep exits |
| Detection lag | > 10 bps consistently (20+ trades) | Infra review |
| Wallet behavior drift | entries/episode > 3 (was <2) | Drop (switched to DCA) |

## 10. Deployment Path

### Phase 1: Selection (THIS WEEK, no capital)
- Run Steps 1-7 above against existing data
- Output: ranked wallet list with full scorecards

### Phase 2: Shadow mode (2 weeks, no capital)
- Track selected wallets live
- Log every signal with simulated fill prices
- Measure simulated copy PnL vs baselines
- Gate: simulated PnL > +20bps after costs over 100+ signals

### Phase 3: Canary (1 week, $11 positions)
- Top 3 wallets only
- Compare live vs shadow fills
- Gate: live PnL within 30bps of shadow

### Phase 4: Scale
- Add wallets, increase size based on track record
- Monthly walk-forward revalidation

## 11. Codex Review Summary

### Round 1 (16 findings)
Key: full PnL equation, simulated entry price, baselines, FDR, entry guards.

### Round 2 (13 findings)
Key fixes incorporated in v2.3:
- OOS leak: fixed with strict temporal embargo (Section 4)
- Step 1 broken: fixed to use only non-performance collector fields (Section 7)
- S3 downloader: code fixed to preserve dir/closedPnl/hash/notional
- FDR hand-waved: concrete spec added (Section 6E) with null, test stat, procedure
- A1 backwards: fixed from "directional bias > 60%" to anti-MM criterion
- Simulated copy PnL: complete formula with both legs, fees, funding, shorts
- Episode minimums: raised to 30 per split (60 total)
- Redundant metrics: acknowledged (closed PnL measures W, not us; Sharpe annualization noted)

Codex R2 verdict: "The most likely false positive is a one-sided wallet that rode a
favorable short-term coin move. Passes everything, but alpha is really beta plus
coin selection plus impact." This is what baselines B1-B4 test for.
