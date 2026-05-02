# H3 V3: GLFT Grid Market Maker with Empirical Calibration

## Strategy Specification — Production Grade

**Date:** 2026-05-02
**Version:** 3.0
**Status:** STRATEGY DESIGN (implementation pending)
**Based on:** 8 academic papers + top HL vault competitive intelligence

---

## 1. Why V3 Is Different

| Aspect | V1-V2 (killed) | V3 (this spec) |
|--------|---------|-----|
| Model | Basic AS with fixed params | GLFT with empirical calibration |
| Quoting | Single bid + ask | Multi-level grid (5-10 each side) |
| Fair value | HL mid (exploitable) | Microprice + Bybit blend |
| Calibration | Fixed gamma/kappa | Fit λ=Ae^(-kδ) from live fill data every 5s |
| Signal use | One-sided tighten (WRONG) | Fair value adjustment (microprice) |
| Fills | Assumed ~50% adverse | Accept 80% adverse, design for asymmetric P&L |
| Risk model | Fixed inventory limits | Asymmetric: small losses, let winners run |
| Grid | None | GLFT-derived spacing, auto-reset on breakout |
| Funding | Ignored | Structural carry overlay (delta-neutral component) |

---

## 2. Core Model: GLFT with Grid Extension

### Optimal Quotes (Gueant-Lehalle-Fernandez-Tapia)

```
Half_spread = c1 + (Δ/2) * σ * c2
Skew = σ * c2
Bid = fair_price - (half_spread + skew * q)
Ask = fair_price + (half_spread - skew * q)
```

Where:
```
c1 = (1/(ξΔ)) * log(1 + ξΔ/k)
c2 = sqrt((γ/(2AΔk)) * (1 + ξΔ/k)^(k/(ξΔ) + 1))
```

Parameters:
- `A`, `k`: calibrated from empirical fill data (NOT theoretical)
- `γ`: risk aversion (start 0.05, adaptive)
- `σ`: rolling volatility (5-second recalibration)
- `Δ`: order size (1 unit = $11 minimum on HL)
- `ξ = γ` (standard choice)
- `q`: current inventory (in base units)

### Grid Extension

Instead of 1 bid + 1 ask, place N levels:
```
Grid_interval = max(round(half_spread_ticks) * tick_size, tick_size)
Bid_levels = [base_bid, base_bid - interval, base_bid - 2*interval, ...]
Ask_levels = [base_ask, base_ask + interval, base_ask + 2*interval, ...]
N_levels = 5 (start conservative, scale to 10-20)
```

**Auto-reset**: When price breaks grid boundary, cancel all orders and re-center grid around new mid.

### Why Grids Work

1. More fill opportunities (multiple price levels)
2. Natural DCA effect (deeper fills at better prices during moves)
3. Inventory diversification (multiple entry points)
4. GLFT provides mathematically optimal spacing per volatility regime

---

## 3. Fair Value: Microprice + External Reference

```python
# Level 1: Microprice (order book signal IS the fair value)
microprice = best_ask * bid_imbalance + best_bid * (1 - bid_imbalance)

# Level 2: Blend with external (Bybit more liquid, less manipulable)
fair_value = 0.6 * bybit_mid + 0.4 * microprice

# Level 3: GLFT reservation price (inventory-adjusted fair value)
reservation = fair_value - skew * inventory
```

The signal doesn't SKEW quotes — it defines WHERE fair value IS. Both quotes shift together when microprice moves. This is contrarian by construction (see academic synthesis doc).

---

## 4. Empirical Calibration: λ = A * exp(-k * δ)

### What This Is

The fill intensity function: how likely are we to get filled at distance δ from mid?
- `A` = overall trading intensity (higher = more active market)
- `k` = market depth (higher = fills decay faster with distance)

### Calibration Method (from EIE paper)

1. Define N spread levels: δ₀, δ₁, ..., δₙ (e.g., 1bp, 2bp, 5bp, 10bp, 20bp, 50bp)
2. For each level, measure: "how many seconds until a fill would have occurred?"
3. λ(δₖ) = 1 / avg_waiting_time at that spread
4. Log-regression: log(λ) = log(A) - k*δ → gives A and k

### Recalibration

- Every 5 seconds: update σ (volatility)
- Every 60 seconds: update A, k from recent fill observations
- When no fills in 5 min: widen spread (reduce A assumption)

### HL-Specific Considerations

- 200ms latency means our effective fill distance is δ_quote + δ_latency_slippage
- ALO (Add Liquidity Only) orders prevent accidental taker execution
- Fill rate is LOWER than modeled due to queue position (we're rarely first)

---

## 5. Risk Model: Asymmetric P&L (Not High Win Rate)

### Lesson from Top HL Vaults

| Vault | Win Rate | Profit Factor | Avg Win / Avg Loss |
|-------|----------|---------------|-------------------|
| AceVault | 28% | 3.71 | $26 / $2.70 |
| Growi HF | 38% | 10.76 | — |
| MC Recovery | 48% | 43.1 | $862 / $18 |

**The pattern**: Accept low win rate. Make winners MUCH bigger than losers.

### Applied to Our MM

- **Losers (80% of fills)**: Adverse selection fills. Loss = half_spread or less. BOUNDED by spread width.
- **Winners (20% of fills)**: Non-adverse fills where price moves in our favor. Let these RUN.
- **Mechanism**: Asymmetric exits.
  - Losing side: strict time limit (close at next grid reset, ~30-60s)
  - Winning side: trailing stop or hold until next grid level provides better exit

### Inventory Kill Switches

- Per-pair hard limit: $15 (30% of $50 capital)
- Total exposure: $25 (50%)
- Daily loss stop: $2.50 (5%) — from REAL exchange equity
- Drawdown from peak: $5.00 (10%)
- Consecutive same-side fills > 4: toxic flow, pause 60s

---

## 6. Funding Rate Carry Overlay

HL funding settles HOURLY (8x more frequent than Bybit).

### Strategy

When funding is positive (longs pay shorts):
- Bias inventory toward SHORT (earn funding while providing liquidity)
- Shift reservation price DOWN slightly (makes us sell more, buy less)
- Expected carry: 0.01-0.05% per hour on position = $0.005-0.025/hr on $50

When funding is negative (shorts pay longs):
- Bias inventory toward LONG
- Shift reservation price UP

### Integration with GLFT

Funding enters as a drift term in the reservation price:
```
reservation = fair_value - skew*q - funding_bias
funding_bias = sign(funding_rate) * |funding_rate| * position_usd * carry_weight
carry_weight = 0.3 (conservative — don't overweight carry vs spread capture)
```

---

## 7. Pair Selection

### Primary: PENDLE (Codex recommendation)
- Spread: 2.6bps, Volume: $24.6M/day
- High volume = more calibration data, more fills
- DeFi token = distinct microstructure from BTC-correlated names

### Secondary: APE
- Spread: 7.6bps, Volume: $9.7M/day
- Widest spread = most forgiving for errors
- Lower volume = fewer fills but each is more profitable

### Growth Mode Targets (when available)
- Look for HIP-3 pairs with 90% fee reduction
- These have widest spreads (50-200bps) with minimal competition
- Perfect for grid strategies

---

## 8. Implementation Architecture (V3)

```
scripts/hl_mm_live.py
    │
    └── app/services/hl_mm/
            ├── orchestrator.py          # Main async loop (1.5s tick)
            ├── glft_quoter.py           # NEW: GLFT model + grid generation
            ├── calibrator.py            # NEW: Empirical A,k,σ estimation
            ├── fair_value.py            # Microprice + Bybit blend
            ├── signal_engine.py         # L2 imbalance → microprice
            ├── grid_manager.py          # NEW: Multi-level order placement
            ├── fill_tracker.py          # Markout analysis + toxicity
            ├── inventory_manager.py     # Real equity tracking + kill switches
            ├── funding_overlay.py       # NEW: Hourly funding carry
            └── quote_engine.py          # ALO orders + orphan cleanup
```

### New Modules Needed

1. **glft_quoter.py** — GLFT closed-form formulas, replaces AS quoter
2. **calibrator.py** — Live A, k estimation from fill data + σ from mid changes
3. **grid_manager.py** — Multi-level order grid with auto-reset
4. **funding_overlay.py** — Funding rate polling + carry bias calculation

---

## 9. Performance Expectations (Honest)

### With $50 capital, PENDLE, conservative params:

| Metric | Pessimistic | Base | Optimistic |
|--------|-------------|------|------------|
| Daily fills | 5 | 15 | 30 |
| Net per fill (after 80% adverse) | $0.005 | $0.01 | $0.02 |
| Daily PnL | $0.025 | $0.15 | $0.60 |
| Monthly PnL | $0.75 | $4.50 | $18 |
| Funding carry/month | $0.50 | $1.50 | $5.00 |
| **Total monthly** | **$1.25** | **$6.00** | **$23** |
| Sharpe (annualized) | 0.5 | 2.0 | 5.0 |

### Scaled to $5,000:
| | Pessimistic | Base | Optimistic |
|--|-------------|------|------------|
| Monthly PnL | $125 | $600 | $2,300 |

### What makes this WORTH doing at $50:
1. Prove the calibration works (A, k fit from real fills)
2. Prove adverse selection is manageable (markout data)
3. Prove grid resets work (no runaway inventory)
4. Build the empirical fill database for parameter tuning
5. Once proven → scale capital → hit Gate 1 ($500 MRR)

---

## 10. Deployment Phases

### Phase 0: Build (today, during HL maintenance)
- [ ] Implement GLFT quoter with closed-form formulas
- [ ] Implement calibrator (A, k estimation from L2 data)
- [ ] Implement grid manager (multi-level orders)
- [ ] Implement funding overlay
- [ ] Codex review (strategy + code)

### Phase 1: Paper Test (post-maintenance)
- Single pair (PENDLE), $11 orders, 5 grid levels
- Collect fills for 24h
- Compute markouts, calibrate A/k
- Decision gate: PF > 1.0 after 24h?

### Phase 2: Live Tuning (after 24h positive)
- Use markout data to tighten min_spread
- Add second pair (APE)
- Enable funding overlay
- Run 1 week

### Phase 3: Scale
- Add capital ($500 → $5000)
- Add 3-5 pairs
- Enable adaptive gamma (from fill quality)
- Target: $500/month (Gate 1)

---

## References

1. Gueant-Lehalle-Fernandez-Tapia (2013) — Optimal market making closed-form
2. hftbacktest GLFT tutorial — Sharpe 17-19 implementation
3. Albers et al. (2025) — Fill probability vs post-fill returns tradeoff
4. "Market Simulation under Adverse Selection" (2024) — 80% adverse fill rate
5. "Explainable Patterns in Cryptocurrency Microstructure" (2026) — OFI dominance
6. EIE (GitHub: im1235) — Execution intensity calibration
7. Stoikov (2017) — Microprice as optimal fair value estimator
8. Hyperliquid vault analysis (PANews 2025) — Competitive intelligence
