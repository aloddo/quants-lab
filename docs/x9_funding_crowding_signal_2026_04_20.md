# X9 Funding-Crowding Signal (Independent Discovery, 2026-04-20)

## Objective
Find a new, executable edge without reusing prior strategy conclusions.

## Data Used (raw)
- Bybit funding events: Mongo `bybit_funding_rates`
- Bybit open interest: Mongo `bybit_open_interest`
- Bybit perp hourly closes: `app/data/cache/candles/bybit_perpetual|*-USDT|1h.parquet`

No prior result files were used as inputs.

## Signal Family
Funding crowding + OI pressure at funding timestamps.

Feature definitions:
- `fr_z`: funding z-score over rolling 30 funding events
- `oi_z`: OI-delta z-score over rolling 30 events
- `streak`: consecutive same-sign funding events

Primary rule:
- Trigger when `|fr_z| >= q`, `streak >= k`, `oi_z >= oiq`
- Side (crowd reversion): `-sign(funding_rate)`
- Hold for `h` funding events (`h * 8h`)

## Validation Protocol (no leakage)
For each candidate config:
1. Split each pair chronologically into train/test (`70/30`).
2. Compute pair-level train/test trade expectancy.
3. Select pairs from train only:
   - `train_trade_bps > 0`
   - `train_trades >= threshold`
4. Build train/test portfolio from those fixed selected pairs.
5. Rank configs by train performance only; inspect holdout test.

## Best Config Found (selected by train only)
- Family: `crowd_revert`
- `q=1.0`, `k=2`, `oiq=0.0`, `hold_events=6` (48h)
- Selected pairs (train-only): `ADA, BNB, DOGE, DOT, LINK, NEAR, XRP`

Portfolio results at one-way cost `4 bps` (round-trip `8 bps`):
- Train: `+170.65 bps/day`, `+101.84 bps/trade`, `p=0.0251`, `248` trades
- Test: `+150.56 bps/day`, `+92.20 bps/trade`, `p=0.0494`, `129` trades
- Test positive-pair share: `85.7%` of selected pairs

## Robustness Sweep (same selected pairs)
Cost/hold sweep stayed positive on test mean in all sampled points:
- Cost one-way `2..6 bps`
- Hold `4, 6, 8` funding events

Example (hold=6 events):
- 2 bps one-way: test `+157.09 bps/day`, `p=0.0408`
- 4 bps one-way: test `+150.56 bps/day`, `p=0.0494`
- 6 bps one-way: test `+144.03 bps/day`, `p=0.0596`

## Risks / Caveats
- Concentration risk: DOGE is the largest contributor; removing DOGE weakens significance.
- Event frequency is low (funding cadence), so monitoring window must be long.
- Regime sensitivity is likely; periodic retraining and pair governance are required.

## Repro Command
```bash
/Users/hermes/miniforge3/envs/quants-lab/bin/python \
  scripts/x9_funding_crowding_discovery.py
```

Artifacts are written under:
- `app/data/cache/x9_funding_crowding/`

