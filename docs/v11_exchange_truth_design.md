# V11 Exchange Truth Architecture

## Problem

V11's internal state drifts from exchange reality. Internal PnL showed +$3.82 while exchange truth was -$6.07. Root causes:
1. Multiple code paths remove positions without recording trades
2. Internal PnL is computed across 10+ code paths, each a drift opportunity
3. Position recovery on restart uses heuristic matching (lossy)
4. No attribution: exchange fills from other strategies contaminate PnL
5. Fees are completely untracked internally

## Design Principles

1. **Exchange is the ONLY source of truth** for PnL, fills, and equity
2. **V11 never computes PnL** - it queries exchange-derived data
3. **Every order gets oid recorded** before response processing
4. **Position state is dual-tracked**: internal (for trading logic) + exchange (for truth)
5. **Continuous reconciliation** detects drift in real-time, not every 5 min

## Architecture

### Layer 1: Order Attribution (v11_order_ids)

Every `exchange.order()` call records the oid immediately from the response, BEFORE any other processing. Schema:

```
{oid: int, coin: str, side: str, action: "entry"|"exit", wallet: str, timestamp: datetime}
```

This is the link between V11's intent and exchange reality.

### Layer 2: Exchange Fill Ingestion (v11_exchange_fills)

Periodic sync pulls ALL account fills from exchange. Each fill is tagged:
- `is_v11: bool` (oid exists in v11_order_ids)
- Stored with full exchange data (closedPnl, fee, sz, px, time)

### Layer 3: PnL Computation (read-only, derived)

`self.total_pnl` is DELETED as a mutable field. Replaced by:

```python
def _get_exchange_pnl(self) -> dict:
    """Returns cached exchange PnL. Refreshed every sync."""
    return {
        "account_net": float,      # all fills: closedPnl - fees
        "v11_net": float,          # V11-attributed fills only
        "v11_closes": int,         # V11 closing fill count
        "unattributed": float,     # non-V11 fills
        "fees": float,             # total fees
        "last_sync": datetime,
    }
```

No code path ever modifies PnL. It's always derived from exchange fills.

### Layer 4: Position Reconciliation

Two position views maintained simultaneously:

**Internal positions** (`self.positions`): used by trading logic (entry/exit decisions).
Persisted to `v11_open_positions` on every change.

**Exchange positions**: queried from exchange API.
Used for validation only.

Reconciliation runs every minute:
- Sum internal positions per coin (signed) vs exchange net
- Alert on any drift > $1 notional
- If exchange shows a position V11 doesn't track: orphan alert
- If V11 tracks a position exchange doesn't have: phantom alert

### Layer 5: Health Monitoring

Every stats cycle reports:
```
STATS: trades=N pnl=$X.XX(exch) | v11=$X.XX | open=N margin=X% equity=$X.XX
HEALTH: positions_match=Y/N drift=$X.XX orphans=N phantoms=N last_sync=Xs_ago
```

Kill switch uses exchange equity, not internal computation.

## What Changes in Code

### DELETE:
- `self.total_pnl` as a mutable counter
- `self.total_trades` as a mutable counter
- All `self.total_pnl +=` lines (10+ locations)
- All `self.total_trades +=` lines
- `self.pnl_by_market` accumulator
- PnL loading from MongoDB trade collections on startup

### ADD:
- `self._exchange_pnl_cache` dict (refreshed by sync)
- `_record_oid()` call at every `exchange.order()` return
- `_get_exchange_pnl()` read-only accessor
- Enhanced reconciliation with position-level checks
- Health status in every STATS line

### MODIFY:
- `_log_stats()`: read from cache instead of internal counters
- `_send_performance_report()`: use exchange data
- Kill switch: use exchange equity
- `_sync_exchange_fills()`: runs every 60s (not 300s), updates cache

## Invariants (must hold at ALL times)

1. `len(v11_order_ids where action=entry) >= len(v11_open_positions)`
2. `sum(v11_open_positions per coin, signed) ~= exchange position per coin` (within $1)
3. `exchange_pnl_cache.last_sync < 120 seconds ago` (during active trading)
4. Every `exchange.order()` call has a corresponding `_record_oid()` call
5. No code path modifies PnL counters (they don't exist)
