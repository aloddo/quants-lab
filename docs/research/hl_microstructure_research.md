# Hyperliquid Microstructure Research

**Date:** 2026-05-01
**Status:** SIGNAL VALIDATED — architecture design needed
**Signal:** L2 order book imbalance predicts 1-5 min price direction
**Venue:** Hyperliquid (0% maker fee, permissionless, on-chain)

## L2 Imbalance Signal Results (7 days, 1.3M snapshots)

### Edge per pair (z > 2.0, 60s forward)

| Pair | Edge (bps) | WR | N events | Spread (bps) |
|------|-----------|-------|----------|--------------|
| BTC | +1.54 | 62% | 7,477 | 0.13 |
| ETH | +1.67 | 62% | 8,434 | 0.44 |
| SOL | +1.51 | 59% | 8,373 | 0.12 |
| HYPE | +1.55 | 57% | 12,326 | 0.33 |

- All p = 0.0000 (t-stats > 20)
- Signal is SYMMETRIC (works both LONG and SHORT equally)
- Signal is LEADING (order book drives price, not vice versa)
- 1000+ signals/day per pair

### Signal Characteristics

- **Persistence:** autocorr = 0.86 at 1s, 0.59 at 5s, 0.42 at 10s
- **Episode duration:** median 1s, mean 2.8s, 11% last >5s
- **Frequency:** ~1869 episodes per 100K seconds (~27/hour at z>2)
- **Direction:** both directions work (no asymmetry like X14)

## Execution Analysis

### The Fee Problem

| Execution | Cost | Net edge | Viable? |
|-----------|------|----------|---------|
| Taker entry + taker exit | 7.0 bps | -5.3 bps | NO |
| Maker entry + taker exit | 3.5 bps | -1.8 bps | NO |
| Maker entry + maker exit | 0.0 bps | +1.5 bps | YES |
| Taker entry + maker exit | 3.5 bps | -1.8 bps | NO |

**Conclusion:** Only viable with BOTH sides as maker (market making model).

### Market Making Architecture

The imbalance signal doesn't enable directional taking — it enables INTELLIGENT market making:

1. **Always quote both sides** (bid + ask limit orders)
2. **Skew quotes based on imbalance:**
   - Bid-heavy (z > 2): tighten bid, widen ask → accumulate LONG inventory
   - Ask-heavy (z < -2): tighten ask, widen bid → accumulate SHORT inventory
   - Neutral: symmetric, pure spread capture
3. **Inventory management:** hedge on Bybit when inventory exceeds threshold
4. **Revenue:** spread capture + directional skew alpha - hedge costs

### Revenue Model (conservative)

| Parameter | BTC | ETH | Multi-pair |
|-----------|-----|-----|------------|
| Daily fills | 100 | 200 | 500 |
| Avg fill size | $500 | $300 | $200 |
| Half-spread earned | 0.065bps | 0.22bps | 1-3bps |
| Gross spread rev/day | $0.33 | $1.32 | $10-30 |
| Directional alpha | +$5-10 | +$5-10 | +$20-50 |
| Hedge cost | -$1-3 | -$1-3 | -$5-15 |
| **Net daily** | **$5-10** | **$5-10** | **$30-60** |

Monthly estimate: $900-1,800 on multi-pair MM with imbalance intelligence.

## HL Venue Advantages

1. **0% maker fee** — you earn the full spread
2. **All positions public** — no hidden flow, transparent market
3. **230 pairs** — many with wider spreads (3-10bps) than BTC/ETH
4. **Hourly funding** — 8x faster mean-reversion than Bybit
5. **No KYC** — institutional behavior visible
6. **200ms latency** — fast enough for seconds-level strategies
7. **Python SDK available** — `hyperliquid-python-sdk`

## HFT Strategy Candidates (ordered by viability)

### 1. Imbalance-Directed Market Making (READY)
- Signal validated with 7 days data
- Maker-maker execution (0 fee both sides)
- Revenue: directional skew + spread capture
- Need: HL SDK + inventory hedger + Bybit API
- Custom engine (like H2 V3)

### 2. Cross-Venue Arbitrage HL→Bybit (DATA PROVEN)
- 2.5M spread snapshots prove 10-25bps lead on mid-caps
- Execute: buy HL (maker 0%), short Bybit (taker 3.75bps)
- Net edge: 6-21 bps per trade
- Already have detection engine (arb_hl_bybit_perp_snapshots)
- Custom engine, similar to H2 V3

### 3. Whale Position Copy Trading
- All HL positions are public (query any address)
- 314K position docs collected, need to identify top performers
- Follow profitable addresses with 1-2 block delay
- Need: address ranking system + fast execution

### 4. Funding Rate Differential
- HL: hourly. Bybit: 8h. When they diverge, arb the difference.
- Delta-neutral (long cheap, short expensive)
- 966K funding docs (371 days)
- Lower alpha but lower risk

## Cross-Venue Spread Analysis (2.5M snapshots, 5s intervals)

### Persistent Premiums (HL over Bybit)
| Pair | Mean Spread | Std | P5/P95 |
|------|------------|-----|--------|
| DOGE | +8.9bps | 2.9bps | +5.5/+14.6 |
| PENGU | +3.3bps | 2.3bps | 0.0/+6.7 |
| HYPE | +2.0bps | 2.0bps | -1.5/+5.1 |
| ETH | +0.5bps | 1.2bps | -1.4/+2.5 |
| SOL | +0.3bps | 1.3bps | -1.7/+2.4 |
| BTC | -0.2bps | 1.1bps | -1.9/+1.5 |

### Cross-Venue Lead Signal (PENGU, z>2.5)
- HL leads UP: Bybit follows +6.14bps at 5min, 54% WR (n=978, p=0.0008)
- HL leads DOWN: NOT predictive (Bybit does NOT follow down)
- ASYMMETRIC: only the UP direction works (HL is where degens buy first)
- After Bybit taker fee (3.75bps): net ~2.4bps (marginal)

### Why Cross-Venue is Marginal
- Round-trip taker costs (7.5bps) exceed most signals
- Even one-leg maker, one-leg taker (3.75bps) eats most of the 4-6bps edge
- The asymmetry (UP only) halves signal frequency
- Conclusion: cross-venue is a SECONDARY strategy, not primary

## MM Target Pairs (scanned May 1, 230 pairs)

Best pairs for market making = volume > $1M + spread > 1.5bps:

| Pair | Spread | 24h Vol | Est Daily Rev | Notes |
|------|--------|---------|---------------|-------|
| ZEC | 1.6bps | $92M | $144 | Highest volume in sweet spot |
| APE | 6.7bps | $9.7M | $65 | Widest spread, good fill value |
| PENDLE | 2.6bps | $24.6M | $65 | DeFi, high vol/spread ratio |
| VVV | 5.2bps | $5.8M | $30 | Wide spread, moderate vol |
| MON | 2.7bps | $7.4M | $20 | HL ecosystem token |
| kPEPE | 2.5bps | $6.9M | $17 | Meme, volatile |
| FARTCOIN | 1.9bps | $9.0M | $17 | Meme |
| CHIP | 3.0bps | $5.2M | $16 | HL ecosystem |

Revenue estimates assume 1% capture of spread*volume (conservative).
Top 5 combined: ~$324/day = **$9,700/month** potential.

**Priority targets for initial deployment:**
1. ZEC — highest volume, predictable coin
2. PENDLE — best volume/spread ratio
3. APE — widest spread per fill

BTC/ETH/SOL are too tight for standalone MM (0.1-0.4bps) but useful for the
L2 imbalance directional signal (validated at 1.5bps edge with maker-maker).

## Architecture Decision

**Primary: HL-native imbalance MM** (not cross-venue)
- Simpler: single venue, no cross-exchange coordination
- Cheaper: 0% maker fee both sides
- Higher frequency: 1000+ signals/day vs 100-200 for cross-venue
- Lower latency requirement: 200ms vs sub-100ms for cross-venue
- Target wider-spread pairs (ZEC, PENDLE, APE) for higher per-fill revenue

## Next Steps

1. ~~Install HL Python SDK~~ DONE (v0.23.0)
2. Verify Alberto's account access / deposit state
3. Design HL MM engine architecture (similar to H2 V3 but single-venue)
4. Build inventory management module (max position, hedging thresholds)
5. Paper test on ETH (best spread/edge ratio of the 4 pairs)
6. Backfill more L2 data (currently only 7 days — need 30+ for walk-forward)
7. Cross-venue as Phase 2 add-on (after MM engine proves profitable)
