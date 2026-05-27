# Graduation Sniping - Step 2 Data Collection Queries

These SQL queries are designed for Dune Analytics (DuneSQL / Trino).

## Queries

1. **step2_graduation_events.sql** - All PumpSwap-era graduations with pool init params
2. **step2_intrablock_swaps.sql** - Per-swap intra-block ordering for exact reserve replay
3. **step2_bonding_curve_near_graduation.sql** - C5 placebo baseline (near-grad tokens that never graduated)

## Execution

Requires DUNE_API_KEY in environment. Use `dune_client` Python package:

```python
from dune_client.client import DuneClient
dune = DuneClient(api_key=os.environ["DUNE_API_KEY"])
```

## Data Volume Estimate

- Graduation events: ~44K rows (full PumpSwap era)
- Intra-block swaps: ~300 swaps/graduation * 44K graduations = ~13M rows (batch by date)
- Near-graduation tokens: ~100K rows
