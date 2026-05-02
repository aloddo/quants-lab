# HL MM Academic Research Synthesis

**Date:** 2026-05-02
**Purpose:** Strategy redesign based on academic evidence
**Key finding:** Our signal use was INVERTED. Contrarian quoting beats momentum quoting for makers.

## Critical Papers

### 1. "The Market Maker's Dilemma" (Albers et al., 2025, SSRN)
**Key finding:** "Viable maker strategies often require a CONTRARIAN approach, counter-trading the prevailing order book imbalance."

**Why this changes everything:**
- Momentum quoting (lean into imbalance) → higher fill probability, WORSE post-fill returns
- Contrarian quoting (fade imbalance) → lower fill probability, BETTER post-fill returns
- The OPTIMAL tradeoff: tighten the side that OPPOSES current imbalance

**Intuition:** When book is bid-heavy, everyone wants to buy. If you also tighten your bid, you'll get filled when the last buyer arrives — right before reversal. Instead, tighten your ASK (supply to the eager buyers). You sell to aggressive demand → price moves further up → you profit.

### 2. "Market Making with Alpha - Order Book Imbalance" (hftbacktest, 2023-2025)
**Implementation formula:**
```
fair_price = mid_price + c1 * standardized_imbalance
reservation_price = fair_price - skew * normalized_position
bid = reservation_price - half_spread
ask = reservation_price + half_spread
```

**Key parameters (BTC):**
- c1 (imbalance sensitivity): 160
- half_spread: 80 ticks
- skew (inventory sensitivity): 3.5
- Standardization: 1-hour rolling window

**Performance:** Sharpe 10.83 (with realistic queue model), 34% monthly return

**Key insight:** The fair_price shifts WITH imbalance (bid-heavy → fair value UP).
This means: bid = shifted_up_fair_value - spread → bid is HIGHER than vanilla mid-spread.
But the ASK is ALSO higher: ask = shifted_up_fair_value + spread.
Net effect: BOTH quotes shift up. This is a FAIR VALUE adjustment, not a one-sided lean.

### 3. "Explainable Patterns in Cryptocurrency Microstructure" (2026, arXiv)
**Most predictive features (ranked):**
1. Order flow imbalance (Level 1) — dominates
2. Bid-ask spreads
3. VWAP deviations from mid-price

**Critical finding:** "Stable cross-asset patterns — same features work across BTC, LTC, ETC, ENJ, ROSE."
→ We can develop on one pair and deploy across many.

**Flash crash finding:** During Oct 2025 crash, maker strategy suffered "catastrophic losses" on the bid side. → Must have REGIME DETECTION that widens/pauses during high toxicity.

### 4. Microprice (Stoikov, 2017)
**Formula:**
```
microprice = ask * I + bid * (1 - I)
```
Where I = bid_qty / (bid_qty + ask_qty) at best level.

**Why microprice > weighted mid:**
- Simple mid: (bid+ask)/2 (ignores sizes)
- Weighted mid: uses sizes at best level
- Microprice: captures that when bid is heavier, true value is closer to ask

**For MM:** Quote around microprice, not simple mid. This alone improves fill quality.

### 5. Order Flow Imbalance (OFI)
**Formula (Cont et al.):**
```
OFI_t = Σ [I(bid_up)*bid_sz - I(bid_down)*prev_bid_sz - I(ask_down)*ask_sz + I(ask_up)*prev_ask_sz]
```

**Predictive power:** 0.144 bps per unit, R² = 3% (low but consistent)
**Practical use:** Not for trading signals alone. Use for:
- Quoting aggressiveness (high OFI → wider on that side)
- Regime detection (extreme OFI → high information asymmetry → widen all)

## Strategy Redesign: Contrarian Fair-Value MM

### Old approach (WRONG):
```
Signal: bid-heavy (z > 2)
Action: tighten BID (lean into momentum)
Result: get filled right before reversal, adverse selection
```

### New approach (CORRECT):
```
Signal: bid-heavy → fair value is HIGHER than mid
Action: shift BOTH quotes up (fair value adjustment)
         + tighten ASK slightly (supply to eager buyers)
Result: sell to aggressive demand at premium, earn the move
```

### Implementation formula:
```python
# Step 1: Compute microprice (better fair value)
microprice = best_ask * bid_imbalance + best_bid * (1 - bid_imbalance)

# Step 2: Blend with external fair value (Bybit)
fair_value = 0.6 * bybit_mid + 0.4 * microprice

# Step 3: AS reservation price (inventory-aware)
reservation = fair_value - q * gamma * sigma^2 * tau

# Step 4: CONTRARIAN signal application
# Imbalance shifts fair value, which shifts BOTH quotes
# No one-sided tightening — the shift IS the signal
# The signal is already in microprice (step 1)

# Step 5: Spread from markout calibration
# Start wide (40bps), tighten as markout data proves fills are non-toxic
spread = max(min_spread, markout_calibrated_spread)

# Final quotes
bid = reservation - spread/2
ask = reservation + spread/2
```

### Why this works:
1. **Fair value is the signal** — microprice already incorporates imbalance
2. **No artificial one-sided lean** — both quotes move together
3. **Adverse selection is minimized** — we're pricing where value IS, not chasing
4. **Fills are contrarian by construction** — when we buy (bid filled), it's because price moved TOWARD us (sell pressure), and microprice already says value is higher → positive markout expected

## Concrete Parameter Changes for V3

| Parameter | V2 (current) | V3 (redesigned) | Rationale |
|-----------|------|-----|-----------|
| Fair value | 0.6*Bybit + 0.4*HL_mid | 0.6*Bybit + 0.4*microprice | Microprice > simple mid |
| Signal skew | 1.5bps one-sided tighten | REMOVED (signal is in microprice) | Contrarian > momentum |
| min_spread | 30bps fixed | Adaptive from markouts (start 40bps) | Calibrate from data |
| Spread formula | AS vol+arrival terms | max(fee_floor, empirical_markout + buffer) | Empirical > theoretical |
| gamma | 0.3 fixed | Adaptive (higher in high-vol regime) | Regime-aware |
| Pair target | APE | PENDLE (Codex recommendation) | Higher vol, better data |

## Implementation Priority

1. **Replace simple mid with microprice in fair_value.py** (5 min)
2. **Remove one-sided signal skew from orchestrator** (already wrong, remove it)
3. **Fair value = the only place signal matters** (through microprice)
4. **Add regime detection** (widen spread when OFI extreme → high info asymmetry)
5. **Adaptive spread from markout data** (tighten only when fills prove non-toxic)
