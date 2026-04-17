# Portfolio Risk Manager — Design Spec
**Status:** APPROVED (Apr 16, 2026)
**Decision:** Model A — leverage = financing only, conviction = position sizing within fixed risk budget

## Core Principles

1. All sizing in % of equity, never absolute dollars
2. Risk per trade is capped: max X% of equity loss per trade
3. Conviction scales position SIZE within the risk envelope, not the risk envelope itself
4. Leverage is derived (financing), never a conviction expression
5. Portfolio manager is the "adult in the room" — caps total portfolio heat
6. Same framework works for $100 and $100k accounts

## Architecture

Three components communicating via MongoDB:

```
┌─────────────────────────────────────────────────────────┐
│              Mac Mini (always-on)                        │
│                                                         │
│  ┌─────────────────────────────────────────────┐       │
│  │  RiskStateTask (QL pipeline, every 60s)      │       │
│  │  - fetch equity from Bybit                   │       │
│  │  - fetch all open positions                   │       │
│  │  - compute portfolio/strategy/pair heat       │       │
│  │  - write to portfolio_risk_state (MongoDB)    │       │
│  └──────────────────┬──────────────────────────┘       │
│                     │ write                              │
│                     ▼                                    │
│  ┌─────────────────────────────────────────────┐       │
│  │  portfolio_risk_state (MongoDB)              │       │
│  │  {equity, positions, heat, risk_config}      │       │
│  └──────────────────┬──────────────────────────┘       │
│                     │ read (30s cache)                   │
│                     ▼                                    │
│  ┌─────────────────────────────────────────────┐       │
│  │  Controllers (X5, X8, in Docker)             │       │
│  │  get_executor_config():                      │       │
│  │    risk_state = read from MongoDB            │       │
│  │    risk_budget = equity * risk_pct * conv    │       │
│  │    notional = risk_budget / sl_distance      │       │
│  │    leverage = min(ceil(notional/margin), 5)  │       │
│  │    CHECK heat limits → allow or reject       │       │
│  └─────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────┘
```

## Risk Parameters (portfolio level)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `risk_per_trade_pct` | 0.01 (1%) | Max equity at risk per position |
| `max_portfolio_heat_pct` | 0.06 (6%) | Max total risk across ALL open positions |
| `max_strategy_heat_pct` | 0.03 (3%) | Max risk from any single strategy |
| `max_pair_heat_pct` | 0.02 (2%) | Max risk on any pair across strategies |
| `max_leverage` | 5 | Hard cap on leverage (financing only) |
| `min_equity_pct` | 0.80 (80%) | Circuit breaker if equity drops below 80% of initial |

## Sizing Formula

```python
def compute_position_size(equity, sl_distance_pct, conviction_mult, risk_per_trade_pct, price, max_leverage):
    risk_budget = equity * risk_per_trade_pct * conviction_mult
    notional = risk_budget / sl_distance_pct
    amount = notional / price
    leverage = min(max_leverage, max(1, math.ceil(notional / (equity * 0.9))))
    margin_needed = notional / leverage
    return {"notional": notional, "amount": amount, "leverage": leverage,
            "risk_amount": risk_budget, "risk_pct": risk_budget / equity,
            "margin_needed": margin_needed}
```

### Examples

**$50k account, 1.5% SL, conviction 1.0:**
- risk = $50k x 1% = $500 max loss
- notional = $500 / 0.015 = $33,333
- leverage = ceil($33,333 / $45,000) = 1x

**$1k account, 1.5% SL, conviction 1.0:**
- risk = $1k x 1% = $10
- notional = $10 / 0.015 = $667
- leverage = 1x

**$50k, 1.5% SL, conviction 2.0:**
- risk = $50k x 1% x 2.0 = $1,000
- notional = $1,000 / 0.015 = $66,667
- leverage = ceil($66,667 / $45,000) = 2x

## MongoDB Schema

### `portfolio_risk_state` (single document, upserted every 60s)

```json
{
  "_id": "current",
  "updated_at": "2026-04-16T12:00:00Z",
  "equity": 48750.0,
  "available_margin": 42000.0,
  "initial_capital": 50000.0,
  "equity_pct_of_initial": 0.975,
  "risk_config": {
    "risk_per_trade_pct": 0.01,
    "max_portfolio_heat_pct": 0.06,
    "max_strategy_heat_pct": 0.03,
    "max_pair_heat_pct": 0.02,
    "max_leverage": 5,
    "min_equity_pct": 0.80,
    "enabled": true
  },
  "portfolio_heat": {
    "total_risk_pct": 0.035,
    "total_notional": 8500.0,
    "position_count": 4,
    "by_strategy": {
      "X5": {"risk_pct": 0.02, "positions": 2, "pairs": ["BTC-USDT", "ETH-USDT"]},
      "X8": {"risk_pct": 0.015, "positions": 2, "pairs": ["SOL-USDT", "DOGE-USDT"]}
    },
    "by_pair": {
      "BTC-USDT": {"risk_pct": 0.01, "strategies": ["X5"]},
      "ETH-USDT": {"risk_pct": 0.01, "strategies": ["X5"]}
    }
  },
  "positions": [...],
  "circuit_breaker": {"triggered": false, "reason": null}
}
```

## Files to Create/Modify

### New Files
- `app/services/portfolio_risk_manager.py` — pure sizing functions (no I/O)
- `app/tasks/risk_state_task.py` — pipeline task (60s cycle)

### Modified Files
- `app/controllers/directional_trading/x5_liq_cascade.py` — risk-based sizing in get_executor_config()
- `app/controllers/directional_trading/x8_defi_cex_funding_spread.py` — same
- `app/engines/strategy_registry.py` — add risk_enabled, risk_per_trade_pct fields
- `config/hermes_pipeline.yml` — add risk_state_update task
- `cli.py` — write risk config to MongoDB at deploy time

## Migration Plan

1. **Phase 1 (non-breaking):** Build risk_state_task + risk_manager. X8 adopts from day one (risk_enabled: true). X5 stays legacy (risk_enabled: false). Fallback to total_amount_quote if risk_state unavailable.
2. **Phase 2:** X5 shadow mode (compute risk size, log, execute legacy). After 1 week, flip.
3. **Phase 3:** All new strategies inherit risk framework. Deprecate total_amount_quote.

## Backtest Compatibility

Phase 1: constant equity (initial_capital) in backtest — conservative, no drawdown adjustment.
Phase 2: SimulatedEquityTracker that adjusts equity as trades close.

## Edge Cases

| Case | Handling |
|------|----------|
| Equity query fails | Last known from MongoDB, then legacy fallback |
| Portfolio heat exceeded | Signal suppressed, logged |
| Circuit breaker (equity < 80%) | ALL signals suppressed, Telegram alert |
| Correlated positions | Phase 2: correlation groups |
| Stale risk_state (>5min) | Fall back to legacy total_amount_quote |
| Backtest mode | Check processed_data["risk_state"] first, then MongoDB, then legacy |

## Design Decision Log

- **Leverage as financing vs conviction:** Discussed Apr 16. Pros/cons analyzed. Decision: Model A (financing only). Conviction expresses through position SIZE within fixed risk envelope. This is more robust early on when conviction calibration is unproven.
