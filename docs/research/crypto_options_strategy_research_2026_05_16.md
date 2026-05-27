# Crypto Options Strategy Research - 2026-05-16

## Data Constraint

The long backtest is model-marked from observed hourly spot + Deribit DVOL because the option mark-history tables contain current listed future expiries, not a complete archive of expired weekly chains. The model uses measured Deribit surface skew as the strike-IV adjustment and includes taker fees plus slippage.

## Volatility Findings

### BTC
- Sample: 227 daily observations from 2025-10-02 00:00:00+00:00 to 2026-05-16 00:00:00+00:00.
- DVOL mean/median/p90: 46.8% / 45.7% / 54.9%.
- 7d RV mean: 44.8%; VRP mean/median: +2.1 / +3.4 vol points.
- VRP positive 63.9% of days.
- Future 7d absolute move after top-quartile VRP: 3.87% vs bottom-quartile VRP: 3.67%.

### ETH
- Sample: 227 daily observations from 2025-10-02 00:00:00+00:00 to 2026-05-16 00:00:00+00:00.
- DVOL mean/median/p90: 67.8% / 69.5% / 76.2%.
- 7d RV mean: 62.5%; VRP mean/median: +5.3 / +8.5 vol points.
- VRP positive 70.5% of days.
- Future 7d absolute move after top-quartile VRP: 4.79% vs bottom-quartile VRP: 6.11%.

## Surface Findings

### BTC
- Surface sample: 35 days.
- ATM IV by DTE: 3-10d 36.2%, 10-24d 38.5%, 24-60d 39.6%, 60-120d 40.4%.
- Term slope, 24-60d minus 3-10d: +3.4 vol points.
- 7-45d put/call skew, 90-95% moneyness puts minus 105-110% calls: +5.5 vol points.

### ETH
- Surface sample: 35 days.
- ATM IV by DTE: 3-10d 50.4%, 10-24d 54.9%, 24-60d 58.2%, 60-120d 59.0%.
- Term slope, 24-60d minus 3-10d: +7.8 vol points.
- 7-45d put/call skew, 90-95% moneyness puts minus 105-110% calls: +2.8 vol points.

## Strategy Specification

- Evaluation time: daily 08:00 UTC, one open position per coin. Expiry target: 10 calendar days; use the closest Bybit weekly expiry in the 8-12 DTE window.
- Entry gate: 7d return <= -6.0%; DVOL - 7d RV <= 4.0 vol points; abs 24h return <= 7.0%; DVOL <= 90%.
- Structure: buy a put debit spread. Buy the 30% absolute-delta put and sell the 12% absolute-delta put, rounded to available Bybit strikes.
- Costs: 0.030% of underlying per option leg plus 3.0% of option mark per leg.
- Exit: take profit at 180% of debit paid, stop at 45% of debit paid, or close after 96 hours.
- Sizing: debit paid is max loss. Allocate 0.75%-1.0% of equity per position; aggregate open debit risk cap 3.0% of equity. Respect Bybit minimums of 0.01 BTC or 0.1 ETH; skip if the minimum contract size breaches risk.

## Backtest Results

| Sample | Trades | Win Rate | Profit Factor | Sharpe | Max DD at 1% Risk | Avg R | Total R | Return at 1% Risk |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Selected put debit spread | 29 | 44.8% | 1.83 | 2.06 | -1.72% | +0.217 | +6.28 | +6.28% |
| Rejected short-vol credit system | 37 | 64.9% | 0.32 | -2.23 | -2.13% | -0.054 | -1.98 | -1.98% |

The selected strategy only trades during downside momentum and did not trigger after 2026-02-27 in this sample. The rejected short-vol system used the best grid result with VRP >= 3.0, trend cutoff 3.5%, and 10%/4% delta credit spreads.

## Bybit Mark Data Availability

- BTC: 500 symbols, 199460 hourly bars, 2026-04-25 15:00:00+00:00 to 2026-05-16 12:00:00+00:00, DTE range 5.8-334.7 days.
- ETH: 3 symbols, 1358 hourly bars, 2026-04-25 17:00:00+00:00 to 2026-05-16 12:00:00+00:00, DTE range 313.8-334.6 days.

## Current Signal

- BTC: WAIT. Spot 77780.00, DVOL 41.2%, RV7 32.4%, VRP +8.8, trend7 -2.9%.
- ETH: WAIT. Spot 2170.10, DVOL 54.8%, RV7 39.4%, VRP +15.4, trend7 -6.0%.

## Interpretation

The data does not support a generic crypto options premium-selling strategy after realistic costs. The cleaner edge in this sample is convex downside continuation when realized volatility is rising but option IV has not fully repriced. The structure is defined-risk and pays debit up front, so gap risk is capped at premium paid.
