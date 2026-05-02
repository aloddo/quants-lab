# OKX Funding Rate Exploration

**Date:** 2026-05-01
**Status:** RESEARCH COMPLETE -- signal promising, needs deeper validation

## API Details

- **Endpoint:** `GET /api/v5/public/funding-rate-history?instId={PAIR}-USDT-SWAP&limit=100`
- **Auth:** None required (public)
- **Pagination:** `after={fundingTime}` for older records
- **Rate limit:** ~6 req/s before 403 (conservative: 0.15s delay)
- **Interval:** 8 hours (same as Bybit/Binance)
- **History depth:** ~92 days only (279 records for BTC). Despite claims of 6yr history, the API caps around 3 months. This is a hard constraint for backtesting.
- **Schema:** `{instId, fundingRate, realizedRate, fundingTime (ms), instType, method, formulaType}`
- **Altcoin coverage:** All 15 tested pairs available (SOL, XRP, DOGE, ADA, AVAX, LINK, DOT, UNI, NEAR, APT, ARB, OP, SUI, ALGO, AAVE)

## Cross-Exchange Divergence (BTC, 279 observations, Jan 28 - May 1 2026)

| Metric | OKX vs Bybit | OKX vs Binance | Bybit vs Binance |
|--------|-------------|----------------|-----------------|
| Correlation | 0.51 | 0.56 | 0.51 |
| Mean |spread| (bps) | 0.38 | -- | -- |
| Max |spread| (bps) | 1.41 | 1.39 | 1.49 |
| P95 |spread| (bps) | 0.93 | -- | -- |

Key finding: correlations are **only 0.51-0.59** across all three exchanges. This is surprisingly low -- funding rates are NOT simply tracking the same underlying signal. Each exchange has meaningfully independent positioning dynamics.

## Forward Return Signal (BTC, 8h horizon)

When OKX funding is high relative to Bybit (OKX is more bullish), BTC tends to fall in the next 8h.

| Quintile | Mean 8h Return (bps) | N |
|----------|---------------------|---|
| Q1 (OKX low) | -16.8 | 56 |
| Q2 | -8.5 | 56 |
| Q3 | +36.1 | 55 |
| Q4 | +10.4 | 56 |
| Q5 (OKX high) | -38.5 | 56 |

Extreme divergence returns (z-scored spread):

| Threshold | Long (z<-t) | Short (z>t) | L/S Spread | N (each side) |
|-----------|------------|-------------|------------|---------------|
| z=1.0 | -24.3 bps | -50.7 bps | 26.4 bps | 44-46 |
| z=1.5 | +4.7 bps | -74.3 bps | 79.0 bps | 17-18 |
| z=2.0 | +44.7 bps | -94.4 bps | 139 bps | 6-7 |

The SHORT leg (fade OKX-bullish) is strong and monotonic. The LONG leg is noisy with small N.

## Assessment

**Strengths:**
- Genuine divergence exists (r=0.51, not a copy of Bybit)
- The "fade OKX-high" signal shows 139 bps at z>2.0 with consistent direction
- All our current pairs are available on OKX
- Free, no auth, easy to collect

**Weaknesses:**
- Only 92 days of history -- insufficient for walk-forward validation
- Small N at extreme z (6-7 observations) -- not statistically robust
- The quintile pattern is NOT monotonic (Q3 is the highest, Q1 and Q5 are both negative)
- Spread-return correlation is only -0.05 (weak linear relationship)
- 8h funding interval limits trade frequency

**Verdict:** The OKX-Bybit funding divergence shows a promising SHORT-side signal when OKX is relatively bullish. However, 92 days and 6-7 extreme observations are not enough to pass governance gates. Recommend:
1. Build a collector task to start accumulating history (P1 -- the clock on data depth starts now)
2. Revisit signal testing in 6 months when we have 270+ days
3. Meanwhile, test the same divergence on altcoins where positioning asymmetry may be larger
