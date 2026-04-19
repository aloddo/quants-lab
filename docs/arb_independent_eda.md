# Independent EDA: Binance Spot x Bybit Perp Arb

## Scope

- Full pass over all `45` merged pair files in `app/data/cache/arb_trades/`.
- Combined sample: `17,114,460` synchronized bars across mixed `1s` and `5s` cadences.
- Cost benchmark used for context only: `24bp` round-trip.
- Supporting outputs were written to `app/data/cache/arb_eda_*.csv`.

## Executive Summary

- The merged bars are already overlap-only. Across all `45` pairs, `both_active_share = 100%` and `any_zero_trade_share = 0%`. The main issue is not stale prints from idle bars.
- The dominant pattern is structural one-sidedness, not balanced oscillation. Median `|mean spread| / mean |spread|` is `0.987`; `68.9%` of pairs are above `0.95`, `44.4%` are above `0.99`, and all `45` pairs have negative mean spread. Bybit perp is almost always cheaper than Binance spot in this sample.
- The spread is persistent. Median autocorrelation is `0.741` at `1m`, `0.704` at `5m`, and `0.461` at `1h`. That is slow-moving basis, not fast noise.
- Gross width does not translate into fast capture. `10.0%` of all bars exceed `24bp`, but weighted mean spread compression is only `2.80bp` after `60s`, `4.89bp` after `300s`, and `8.34bp` after `900s`. Net of `24bp`, those are `-21.20bp`, `-19.11bp`, and `-15.66bp`.
- Tail risk is duration as much as size. The longest continuous `|spread| >= 24bp` runs reach `49.3h` in `KSMUSDT`, `41.3h` in `ILVUSDT`, `18.6h` in `ALICEUSDT`, and `16.0h` in `STGUSDT`.
- Liquidity matters, but it does not save the strategy. Cross-sectionally, realized mean absolute spread has Pearson `-0.441` and Spearman `-0.648` versus `min_volume_usd`.
- Funding looks more like an explanation for direction than a free edge. Annualized funding correlates `0.550` with mean signed spread; the wide names are often the same names with strongly negative funding.

## What Is Actually In The Data

### 1. These spreads are mostly basis, not coin-flip noise

The cleanest surprise in the dataset is how directional it is. If this were mostly transient cross-exchange noise, the signed mean would be much smaller than the absolute spread. Instead, the signed mean usually explains almost all of the absolute spread.

Most directional pairs:

| pair | mean_spread_bps | mean_abs_spread_bps | directionality_ratio | lag_1m_autocorr | lag_5m_autocorr | lag_1h_autocorr |
|:--|--:|--:|--:|--:|--:|--:|
| BTCUSDT | -5.05 | 5.06 | 0.9998 | 0.556 | 0.497 | 0.389 |
| LTCUSDT | -6.89 | 6.89 | 0.9997 | 0.498 | 0.410 | 0.187 |
| XRPUSDT | -5.35 | 5.36 | 0.9997 | 0.619 | 0.566 | 0.398 |
| ETHUSDT | -5.11 | 5.11 | 0.9994 | 0.556 | 0.510 | 0.368 |
| AAVEUSDT | -8.09 | 8.09 | 0.9992 | 0.667 | 0.574 | 0.276 |
| BCHUSDT | -8.18 | 8.19 | 0.9990 | 0.664 | 0.594 | 0.355 |
| SUIUSDT | -6.26 | 6.26 | 0.9989 | 0.642 | 0.575 | 0.386 |
| UNIUSDT | -9.39 | 9.40 | 0.9988 | 0.586 | 0.496 | 0.229 |
| QNTUSDT | -19.31 | 19.35 | 0.9980 | 0.582 | 0.487 | 0.307 |
| SOLUSDT | -5.08 | 5.09 | 0.9974 | 0.741 | 0.704 | 0.565 |

Least directional pairs still are not exactly mean-zero. They are just the names with more two-way churn layered on top of a negative base:

| pair | mean_spread_bps | mean_abs_spread_bps | directionality_ratio |
|:--|--:|--:|--:|
| FIOUSDT | -36.39 | 62.64 | 0.581 |
| HIGHUSDT | -17.45 | 27.82 | 0.627 |
| XVSUSDT | -26.11 | 35.44 | 0.737 |
| THETAUSDT | -13.85 | 18.67 | 0.742 |
| METISUSDT | -24.48 | 29.87 | 0.819 |

Interpretation: the strategy is not mainly harvesting a jittery spread around zero. It is fighting a persistent discount in the perp leg.

### 2. The names with the fattest spreads are exactly the names most likely to seduce an overfit

The widest names look incredible gross and much worse once you ask whether the move actually closes quickly.

| pair | mean_spread_bps | mean_abs_spread_bps | p95_abs_spread_bps | share_ge_24bps |
|:--|--:|--:|--:|--:|
| OGNUSDT | -126.42 | 128.52 | 482.56 | 70.1% |
| ONGUSDT | -109.25 | 110.66 | 367.69 | 66.8% |
| ENJUSDT | -101.62 | 104.63 | 464.08 | 59.1% |
| PYRUSDT | -65.06 | 72.29 | 243.94 | 71.6% |
| BLURUSDT | -70.31 | 71.01 | 381.28 | 36.6% |
| ARPAUSDT | -67.45 | 69.04 | 226.45 | 65.4% |
| ALICEUSDT | -62.19 | 67.68 | 381.65 | 38.4% |
| FIOUSDT | -36.39 | 62.64 | 282.00 | 34.4% |
| ILVUSDT | -59.84 | 60.54 | 236.66 | 57.5% |
| ONTUSDT | -53.41 | 54.28 | 185.78 | 48.9% |

The problem is that these same names are not snapping back fast. Many are just sitting at a large, structurally negative basis for long stretches.

### 3. Mean reversion exists, but it is too slow relative to fees

Markouts below use only bars with `|spread| >= 24bp`. Since the merged bars already require overlapping activity, this is a cleaner test of actual decay than a naive threshold count.

| horizon_sec | events | weighted_entry_abs_spread_bps | weighted_future_abs_spread_bps | weighted_capture_bps | weighted_net_after_cost_bps | weighted_share_net_after_cost_positive |
|--:|--:|--:|--:|--:|--:|--:|
| 5 | 1,713,344 | 108.81 | 106.98 | 1.83 | -22.17 | 4.88% |
| 30 | 1,713,302 | 108.82 | 106.43 | 2.39 | -21.61 | 8.89% |
| 60 | 1,713,256 | 108.82 | 106.01 | 2.80 | -21.20 | 11.50% |
| 300 | 1,713,033 | 108.82 | 103.94 | 4.89 | -19.11 | 19.94% |
| 900 | 1,712,423 | 108.84 | 100.50 | 8.34 | -15.66 | 26.43% |

That is the central anti-strategy fact in the dataset. The spread is wide, but it does not collapse nearly fast enough.

### 4. The tail risk hiding in the data is persistence

The biggest risk is not just seeing a large dislocation. It is entering because the spread is large, then discovering that the market is perfectly comfortable leaving it large.

Longest continuous `|spread| >= 24bp` runs:

| pair | longest_run_bars | longest_run_sec | longest_run_hours |
|:--|--:|--:|--:|
| KSMUSDT | 6,828 | 177,528 | 49.31 |
| ILVUSDT | 5,132 | 148,828 | 41.34 |
| BANDUSDT | 1,394 | 85,034 | 23.62 |
| ALICEUSDT | 8,371 | 66,968 | 18.60 |
| STGUSDT | 9,597 | 57,582 | 15.99 |
| COMPUSDT | 8,704 | 52,224 | 14.51 |
| BLURUSDT | 12,270 | 49,080 | 13.63 |
| PORTALUSDT | 1,696 | 44,096 | 12.25 |
| ONGUSDT | 10,972 | 43,888 | 12.19 |
| OGNUSDT | 8,680 | 43,400 | 12.06 |

If a design assumes fast flattening, these episodes are the hidden inventory risk.

### 5. The apparent winners at long horizons are thin and fragile

At `900s`, only a few pairs show non-negative average capture after costs, and those cases are not robust:

| pair | events | entry_abs_spread_bps | mean_capture_bps | mean_net_after_cost_bps | share_net_after_cost_positive |
|:--|--:|--:|--:|--:|--:|
| SOLUSDT | 3 | 37.89 | 34.67 | 10.67 | 100.0% |
| SUIUSDT | 46 | 30.86 | 24.43 | 0.43 | 43.5% |
| LTCUSDT | 170 | 34.43 | 24.38 | 0.38 | 41.8% |

These are exactly the kinds of pair-horizon combinations a strategy designer can overfit to:

- `SOLUSDT` looks great, but it has only `3` qualifying events.
- `SUIUSDT` and `LTCUSDT` are barely positive on average and only clear costs on fewer than half of qualifying events.
- The names with the richest gross spreads, such as `OGNUSDT`, `ALICEUSDT`, `AXLUSDT`, `COMPUSDT`, and `QNTUSDT`, remain decisively negative even after `900s`.

### 6. Liquidity explains a lot, but not all

Thinner names do carry wider spreads. The lowest-liquidity end of the universe is where the monstrous numbers live:

| pair | tier | min_volume_usd | mean_abs_spread_bps | p95_abs_spread_bps | share_ge_24bps |
|:--|:--|--:|--:|--:|--:|
| STGUSDT | T4_MICRO | 680,784 | 25.28 | 61.93 | 30.1% |
| QNTUSDT | T4_MICRO | 834,037 | 19.35 | 29.72 | 20.0% |
| OGNUSDT | T4_MICRO | 952,088 | 128.52 | 482.56 | 70.1% |
| MANAUSDT | T3_SMALLCAP | 1,065,148 | 13.29 | 24.19 | 5.2% |
| ROSEUSDT | T3_SMALLCAP | 1,084,221 | 25.14 | 64.55 | 39.2% |
| TWTUSDT | T3_SMALLCAP | 1,120,394 | 11.43 | 24.72 | 5.8% |
| FIOUSDT | T3_SMALLCAP | 1,131,345 | 62.64 | 282.00 | 34.4% |
| ILVUSDT | T3_SMALLCAP | 1,215,688 | 60.54 | 236.66 | 57.5% |
| XVSUSDT | T3_SMALLCAP | 1,255,022 | 35.44 | 119.41 | 45.2% |
| ARPAUSDT | T3_SMALLCAP | 1,370,410 | 69.04 | 226.45 | 65.4% |

But liquidity is not the whole story. `ENJUSDT` and `ALICEUSDT` are not tiny, yet they still carry very large and persistent basis. That suggests the distortion is partly structural rather than just a function of thin books.

### 7. Funding helps explain the sign of the spread

Funding lines up with spread direction better than it lines up with short-horizon profit opportunity.

Most negative funding names:

| pair | annual_pct | mean_spread_bps | mean_abs_spread_bps | directionality_ratio |
|:--|--:|--:|--:|--:|
| AXLUSDT | -284.90 | -45.95 | 47.73 | 0.963 |
| METISUSDT | -179.33 | -24.48 | 29.87 | 0.819 |
| ENJUSDT | -162.52 | -101.62 | 104.63 | 0.971 |
| ONGUSDT | -80.52 | -109.25 | 110.66 | 0.987 |
| ONTUSDT | -80.37 | -53.41 | 54.28 | 0.984 |
| ALICEUSDT | -58.10 | -62.19 | 67.68 | 0.919 |
| KSMUSDT | -46.12 | -37.46 | 37.61 | 0.996 |
| HIGHUSDT | -30.10 | -17.45 | 27.82 | 0.627 |
| COMPUSDT | -29.81 | -36.70 | 36.80 | 0.997 |
| PORTALUSDT | -20.25 | -30.74 | 33.43 | 0.919 |

The simple read is:

- When funding is very negative, the perp often trades at a persistent discount to spot.
- That can make spreads look permanently attractive to a basis-entry rule.
- But if the spread is a carry regime rather than a temporary dislocation, it can stay wide for hours and still fail to pay back fees.

### 8. Funding-hour seasonality is real for some names, but not universal

There is no clean global “trade the funding timestamp” rule in this sample.

Pairs with the biggest funding-hour widening:

| pair | mean_abs | funding_hour_abs | nonfund_abs | delta |
|:--|--:|--:|--:|--:|
| ENJUSDT | 104.63 | 156.64 | 88.52 | 68.11 |
| ONGUSDT | 110.66 | 127.68 | 97.90 | 29.78 |
| KSMUSDT | 37.61 | 48.98 | 30.26 | 18.72 |
| BLURUSDT | 71.01 | 79.80 | 66.18 | 13.63 |
| XVSUSDT | 35.44 | 40.36 | 32.30 | 8.05 |

Pairs that are actually calmer around funding hours:

| pair | mean_abs | funding_hour_abs | nonfund_abs | delta |
|:--|--:|--:|--:|--:|
| ILVUSDT | 60.54 | 31.07 | 49.27 | -18.20 |
| AXLUSDT | 47.73 | 27.81 | 48.75 | -20.94 |
| OGNUSDT | 128.52 | 88.45 | 116.80 | -28.35 |
| ALICEUSDT | 67.68 | 33.19 | 65.16 | -31.97 |

This is another overfit trap. Clock-time effects exist, but they are pair-specific and sometimes reverse sign.

## If I Had To Bet Against The Strategy

- I would say the strategy is mistaking structural basis for transient arbitrage. The spread is usually one-directional, not balanced around zero.
- I would say the strategy is paying trading costs against an extremely slow state variable. High autocorrelation and long run lengths are exactly what a mean-reversion trader does not want.
- I would say the most visually attractive names are the most dangerous to optimize on. Their gross spread is huge, but their post-entry compression is weak and slow.
- I would say the few pair-horizon combinations that look positive after costs are too sparse to trust. `SOLUSDT` has `3` qualifying events at `900s`; that is not evidence, it is an anecdote.
- I would say funding is the hidden confounder. If negative funding mechanically keeps the perp cheap, a “buy perp / sell spot because the spread is wide” rule can be loading a carry regime, not harvesting mispricing.

## Files Written

- `docs/arb_independent_eda.md`
- `app/data/cache/arb_eda_pair_summary.csv`
- `app/data/cache/arb_eda_cross_section.csv`
- `app/data/cache/arb_eda_threshold_events.csv`
- `app/data/cache/arb_eda_markouts.csv`
- `app/data/cache/arb_eda_hourly_global.csv`
- `app/data/cache/arb_eda_run_lengths.csv`
- `app/data/cache/arb_eda_staleness.csv`

## Notes

- Samples mix `1s` bars over roughly `90d` and `5s` bars over roughly `14d`; markouts were converted to real seconds using each pair's median cadence.
- The local MongoDB funding history was not callable from this sandbox, so funding analysis used `app/data/cache/arb_funding_summary.csv`.
- No yes/no verdict is given here. The goal was to identify what dominates the data, what is fragile, and where a strategy designer is most likely to fool themselves.
