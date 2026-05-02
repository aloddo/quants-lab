I’m treating this as a clean-sheet strategy design, not a code or repo task. I’m working from the constraints you gave and optimizing for something a single developer can actually ship and supervise.
The core conclusion is clear: with a positive maker fee and REST-only latency, continuous touch-making is the wrong game. The viable design is a selective, inventory-light passive alpha strategy: quote only when external fair value, book imbalance, and spread state imply enough gross edge to clear fees and toxicity.
I would **not** deploy an always-on symmetric market maker here. With `~200ms` latency, REST polling, ALO-only passive entry, and a positive maker fee of `1.44bps/side`, the viable strategy is a **selective, inventory-light passive alpha maker**: quote only when short-horizon fair value, book state, and spread regime imply enough gross edge to clear fees and toxicity.

**1. Pair Selection**

- `Live first: SOL`
- `Shadow second: ETH`
- `Do not launch initially: BTC, HYPE, ZEC`

Why:

- `SOL` is the best compromise between external price discovery, enough HL flow to get passive exits, and less brutal competition than `BTC/ETH`.
- `ETH` is cleaner than `HYPE/ZEC` but usually tighter and more efficient, so I would validate it in shadow after `SOL`.
- `BTC` is too competitive for REST-only passive quoting; your edge window will be too short.
- `HYPE` is more idiosyncratic and less cleanly anchored to Bybit.
- `ZEC` may show wide spreads, but the jump risk and thin-book behavior are wrong for a `$50`, unattended proof stage.

If `SOL` is not positive in a conservative backtest, I would stop. I would not rotate into worse pairs just to force deployment.

**2. Quoting Model**

Use a **hybrid empirical state-based model with AS-style inventory skew**.

Not `RL`: too little data, too unstable, too opaque.  
Not pure `AS/GLFT`: those are useful for structure, but their stationary fill assumptions break under ALO rejects, REST latency, and state-dependent toxicity.

Core definitions:

```text
m_t   = (bid_t + ask_t) / 2
mu_t  = (ask_t * bid_size_t + bid_t * ask_size_t) / (bid_size_t + ask_size_t)   # microprice
basis_t = EMA_60s(m_t - bybit_mid_t)
f_t   = bybit_mid_t + basis_t                                                     # fair value anchor
```

Alpha score:

```text
score_t = w1*z((f_t - m_t)/m_t)
        + w2*z((mu_t - m_t)/tick)
        + w3*z(book_imbalance_t)
        + w4*z(trade_imbalance_3s_t)
        + w5*z(bybit_return_1s_t)
```

Good day-1 weights if you want heuristics before fitting:

```text
w = [0.45, 0.20, 0.15, 0.10, 0.10]
```

Convert to short-horizon alpha in bps:

```text
alpha_t = clip(score_t * sigma_3s_bps, -6bps, +6bps)
```

Inventory-skewed reservation price:

```text
inv_skew_t = 4bps * (q_t / Q_max)
r_t = f_t + alpha_t - inv_skew_t
```

Candidate quotes at depths `k in {0,1,2,4}` ticks:

```text
p_bid(k) = min(best_bid_t, floor_tick(r_t)) - k*tick
p_ask(k) = max(best_ask_t, ceil_tick(r_t)) + k*tick
```

Expected-value test per side:

```text
G_bid(k) = (f_t - p_bid(k))/f_t - 2*f_m - A_bid(k, x_t)
G_ask(k) = (p_ask(k) - f_t)/f_t - 2*f_m - A_ask(k, x_t)
```

Where:

- `f_m = 1.44bps`
- `A_side(k, x_t)` = expected adverse markout after fill, estimated from history by state

Decision rule:

- quote side `s` only if `G_s(k*) > 0.75bps` after fees
- choose `k* = argmax P_fill_s(k|x_t) * G_s(k)`
- if only one side passes, quote one-sided
- if neither passes, do not quote

That last line is the whole game. Most of the time, the right action is **no quote**.

**3. Signal Stack**

Ranked by importance:

1. `Bybit-HL dislocation`: `f_t - m_t`
2. `Microprice / top-level imbalance`: `mu_t - m_t`, `I_t = (bid_sz - ask_sz)/(bid_sz + ask_sz)`
3. `Recent signed trade imbalance`: last `3-5s`
4. `Spread regime + short-horizon vol`: filter, not primary alpha
5. `Funding / time-to-funding`: regime filter only

How they combine:

- `Bybit-HL dislocation` tells you where fair value should be.
- `Microprice + imbalance` tells you whether the next local move is toward or away from your quote.
- `Trade imbalance` tells you whether aggression is continuing or exhausted.
- `Spread + vol` decides whether you need to widen or stay out.
- `Funding` tells you when not to hold inventory.

**4. Fill Model**

Model fills as two stages:

```text
P_rest_bid = P(ask_{t+L} > p_bid)
P_rest_ask = P(bid_{t+L} < p_ask)
```

This captures ALO reject risk at latency `L`.

Then conditional fill after resting:

```text
P_fill_side(k|x_t) = P_rest_side * P(fill within H | rested, x_t)
```

Use `H = 3s` for entry-fill modeling and `5s` markout for toxicity.

Offline with your `1s` snapshots, I would be conservative and backtest with **effective latency = 1s**, not `200ms`. If it only works at `200ms`, you do not have evidence yet.

Minimum data needed to calibrate:

- L2 top-of-book prices and sizes
- trade ticks with aggressor side inference
- your own live order logs: send time, ack/reject, rest time, fill time, cancel time

Without your own live logs, fill calibration is provisional. That is the biggest model risk.

Conservative queue rule for the backtest:

- estimate queue ahead as displayed size at your price at rest time
- require cumulative opposite-side traded volume plus visible depth depletion to exceed `1.25x` that queue before granting yourself a fill

If a strategy is only profitable under optimistic fills, reject it.

**5. Inventory Management**

At `$50`:

- `order size: $10-$12 notional`
- `soft max inventory: $20 notional`
- `hard max inventory: $25 notional`
- `one live order per side`, one level only

Behavior:

- if `|q| >= 0.5 * Q_max`, stop quoting the same side
- if `|q| >= Q_max`, quote exit side only
- target inventory half-life: `30-60s`
- if inventory age exceeds `90s`, enter exit-only mode and tighten the exit quote by `1 tick` every `15s`
- do not carry inventory into funding

At `$50`, inventory is not a source of edge. It is a failure mode.

**6. Risk Controls**

Hard kill conditions for the proof stage:

- market data stale `> 2s` from HL or Bybit
- `3` consecutive REST/API failures
- open-order state mismatch at any time
- ALO reject rate `> 35%` over the last `50` order attempts
- median `5s` post-fill markout of last `10` fills worse than `-5bps`
- session P&L below `-$2.50`
- total equity drawdown below `-$5`
- `|HL mid - fair value| > 25bps`
- `1s` realized vol above `40bps`
- no new entries in the last `3 minutes` before funding

Operationally, I would also restrict live trading to a supervised liquid window, e.g. `12:00-22:00 UTC`, until the bot has a clean track record.

**7. Expected P&L**

Be honest: at `$50`, this is a **research deployment**, not an income stream.

Realistic expectation:

- `$50`: about `+$0.02 to +$0.15/day`, `+$1 to +$4/month`, with many flat or negative days
- `$5000`: about `+$2 to +$12/day`, `+$60 to +$360/month` if the live fill model matches sim and you have better infra

This does **not** work if:

- gross edge after fill is usually `< 4bps`
- SOL on HL no longer lags Bybit on the horizons you can see with polling
- live fills are materially more toxic than your backtest
- reject rate is high enough that your realized participation collapses
- you insist on always having both sides quoted

I would **not** sign off on `$5000` under REST-only + no alerts. I would sign off on `$50`, then `$250`, then `$1000`. Above that, upgrade infra first.

**8. Simulation Plan**

Minimum validation before live:

1. Build a conservative event-driven backtest on `SOL`.
2. Use `1s` effective latency, ALO rest/reject logic, and pessimistic queue depletion.
3. Fit signal weights on prior days only; evaluate walk-forward by day.
4. Track net P&L after `2 x 1.44bps` fees, fill rate, reject rate, 5s markout, inventory age, and max drawdown.

Minimum confidence bar:

- positive net P&L on both out-of-sample test days
- positive on at least `5/7` daily slices
- no single day dominates total P&L
- median `5s` markout on fills better than `-2bps`
- at least `20` completed entry/exit cycles per day in sim

Then do a **live shadow run** with no real orders for `24-48h`, followed by `100-200` smallest-size live orders purely to calibrate `P_rest`, `P_fill`, and real markouts.

**9. Scaling Path**

What stays the same:

- same fair-value anchor
- same selective quoting logic
- same inventory-light posture
- same kill conditions conceptually

What changes:

- `$50`: `SOL` only, one level, mostly one-sided, tiny size
- `$250-$1000`: same model, bigger clips, possibly add `ETH`
- `$1000-$5000`: add websocket market data and order updates, persistent live order logging, alerts, and only then consider two quote levels

I would not materially widen the pair set before improving data quality. Size scales better than complexity here.

**10. What Makes This Profitable**

The edge is not “capturing the spread.” With a positive maker fee and slow infra, that is a trap. The edge is that Hyperliquid can sit briefly out of line with a stronger external reference while local order book pressure becomes one-sided; you quote only when that dislocation is large enough to beat fees and expected toxicity, and you stay out the rest of the time. Yes, most passive fills will still look adverse on a very short markout, but the strategy wins because it avoids the worst toxic states, only participates in wider or lagged states, and exits inventory passively when the local imbalance normalizes. The moat is not speed; it is selectivity, external anchoring, and refusing to provide liquidity when liquidity provision is statistically mispriced.

Assumptions checked against official Hyperliquid docs:

- ALO / post-only order behavior: https://hyperliquid.gitbook.io/hyperliquid-docs/trading/order-types
- fee schedule: https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees

If you want, I can turn this into a one-page implementation spec with exact state variables, polling loop timing, and backtest pseudocode.

tokens used: 74282
