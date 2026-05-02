# HL Market Making Framework — Academic + Practical Synthesis

**Date:** 2026-05-02
**Status:** DESIGN PHASE — engine rewrite pending
**References:** Avellaneda-Stoikov (2008), Gueant-Lehalle (2013), HB AS strategy, HL docs

## Core Model: Avellaneda-Stoikov for HL

### Reservation Price (where we WANT to trade)

```
r(t) = s - q * gamma * sigma^2 * (T - t) + q * E[funding_rate] * (T - t)
```

- `s` = mid-price
- `q` = inventory (positive = long, negative = short)
- `gamma` = risk aversion (0.1-0.5 for small capital)
- `sigma` = realized volatility (30s EMA of mid returns)
- `T - t` = time to horizon (1 hour = funding cycle)
- `E[funding_rate]` = expected hourly funding (positive = longs pay shorts)

**Inventory effect:** Long inventory shifts reservation DOWN (eager to sell).
**Funding effect:** If we're short and funding is positive, we EARN funding — reduces urgency to unwind.

### Optimal Spread

```
delta = gamma * sigma^2 * (T - t) + (2/gamma) * ln(1 + gamma/kappa)
```

- `kappa` = order arrival rate (estimate from recent trade frequency)
- Higher vol → wider spread
- Higher gamma → wider spread (more risk averse)
- Higher kappa (busy book) → tighter spread needed for fills

### Optimal Quotes

```
bid = r - delta/2
ask = r + delta/2
```

## HL-Specific Adaptations

### Fee Structure (CRITICAL)

| Tier | Maker | Taker | Requirement |
|------|-------|-------|-------------|
| Base | 0.015% (1.5bps) | 0.045% (4.5bps) | Default |
| VIP 1 | 0.012% | 0.035% | >$1M 14d vol |
| VIP 4+ | 0.000% | 0.025% | High volume |
| HIP-3 Growth Mode | ~0.0015% | ~0.0045% | New listings |

**Implication:** At base tier, we pay 1.5bps maker. Our min spread must be > 3bps (1.5bps each side) just to break even BEFORE adverse selection.

**HIP-3 Growth Mode:** 90% fee reduction → 0.15bps maker. This makes wide-spread illiquid pairs viable even with small edge.

### Latency Model

HL on-chain: ~200ms median, ~900ms p99.
- Cannot react to fills within 1 tick
- Stale quote risk = expected adverse move in 200ms
- For BTC with sigma=50% annual: 200ms adverse move ≈ 0.5bps (manageable)
- For altcoin with sigma=150% annual: 200ms adverse move ≈ 1.5bps (significant)

### Funding as Inventory Subsidy

HL funding settles hourly (not 8h like Bybit/Binance).
- Typical funding: +/-0.01% per hour
- On $10 position: $0.001/hour = negligible for PnL
- BUT: structurally biases inventory toward the paying direction
- Strategy: when funding is positive (longs pay), bias SHORT inventory (earn funding while providing liquidity)

## Target Universe: NOT Majors

### Why Long-Tail Pairs Win for Small MMs

| Factor | BTC/ETH (Major) | Growth Mode (Long-tail) |
|--------|-----------------|------------------------|
| Spread | 0.1-0.4bps | 50-200bps |
| Competition | Pro MMs, HFT, MEV bots | Minimal |
| Adverse selection | Extreme | Low |
| Volume | $2B/day | $0.1-2M/day |
| Fee effective | 1.5bps (kills edge) | 0.15bps (negligible) |
| Our edge | None | Persistence |

### Niche Strategy

Be the PERSISTENT liquidity provider on thin pairs:
1. Wide spread (50-100bps) = high margin per fill
2. Low volume = few fills/day but each is profitable
3. No competition = we're the only 2-sided quote
4. Revenue model: 5-20 fills/day * $50-200 * 50bps = $1.25-20/day

### Pair Selection Criteria

1. HIP-3 Growth Mode (90% fee reduction)
2. Daily volume $100K-$2M (enough flow, not enough for pro MMs)
3. Spread > 20bps (margin per fill exceeds all costs)
4. Funding rate != 0 (inventory carry subsidy)
5. NOT a known rugpull/delist candidate

## Risk Management

### Inventory Limits

```
max_inventory = 0.20 * account_value  # 20% of capital per pair
soft_limit = 0.15 * account_value     # start aggressive skew
emergency = 0.30 * account_value      # market flatten
```

With $50 capital: max $10 per pair, soft at $7.50, emergency at $15.

### Adverse Selection Defense

1. **Don't be tightest:** Quote 1-2 ticks behind best bid/ask
2. **Pull quotes during vol spikes:** If sigma > 2x normal, widen to max_spread
3. **Filled order delay:** After fill, wait 3-5s before re-quoting (momentum check)
4. **Time-of-day filter:** Reduce quoting during known volatile periods (funding settlement, news)

### Kill Switches

- Max daily loss: 5% of capital ($2.50)
- Max drawdown from peak: 10% ($5.00)
- Max consecutive fills same side: 5 (likely toxic flow)
- Emergency: cancel all + flatten if funding rate spikes > 0.1%

## Implementation Plan

### Phase 1: Single Pair Paper Test (post-maintenance)

1. Find best Growth Mode pair (wide spread, some volume)
2. Implement proper AS quoter with:
   - Volatility estimation (30s EMA)
   - Inventory-adjusted reservation price
   - Funding-aware bias
   - Quote refresh every 5-10s
3. Run for 24h with $10 exposure limit
4. Measure: fills, PnL, adverse selection rate, inventory time

### Phase 2: Add L2 Imbalance Overlay

After base AS is profitable:
- Use imbalance z-score to modulate aggressiveness
- When imbalance confirms our inventory direction: tighten quote (more fills)
- When imbalance opposes: widen quote (fewer fills, less adverse selection)

### Phase 3: Multi-Pair + Bybit Hedge

- Scale to 3-5 Growth Mode pairs
- Add Bybit delta hedge when total inventory exceeds threshold
- Cross-venue pricing: use Bybit mid as better fair value estimate for HL-listed pairs

## Key Difference from V1 Engine

| V1 (naive) | V2 (AS-based) |
|------------|---------------|
| Fixed offset from mid | Dynamic spread from volatility |
| Binary skew (direction) | Continuous reservation price shift |
| Same pairs as majors | Growth Mode long-tail niche |
| No fill management | Filled order delay + momentum check |
| No adverse selection filter | Vol-spike quote pulling |
