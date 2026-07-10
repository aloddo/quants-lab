# Gate-2 Data Audit

Generated: 2026-07-08T14:29:48.668720+00:00

Verdict: **not_live_sufficient**

## Critical Notes

- historical window is stale by 26.6 days
- duplicate closed journey keys present

## Coverage

- grid start: 2025-12-14T00:00:00+00:00
- declared cutoff: 2026-06-12T00:00:00+00:00
- staleness vs now: 26.6 days
- closed journeys: 7,467,079
- journey wallets: 19,787
- eligible gates: 6,249
- r2 nonterminal trips: 2,031,181
- latest LCB boundary: 17
- latest LCB wallets: 4,780

## Consistency

- duplicate closed journey keys: 20
- duplicate entry keys: 18
- negative durations: 0
- nonpositive notionals: 0
- exits after cutoff: 0
- entries after cutoff: 0

JSON: `app/data/research/v28/gate2_data_audit.json`
