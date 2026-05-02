# Codex Market Making Strategy Design — May 2, 2026

**For Alberto's approval before sending.**

---

## PROMPT:

You are a PhD-level quantitative researcher specializing in market microstructure and optimal market making. I need you to design a mathematically rigorous market making strategy for crypto perpetual futures on **Hyperliquid** (on-chain order book) and **Bybit** (CEX).

This is not a brainstorming exercise. I have measured infrastructure constraints, empirical microstructure data, and killed multiple naive approaches already. I need the strategy a professional HFT desk would build given these EXACT constraints. Reference academic literature where applicable (Avellaneda-Stoikov 2008, Gueant-Lehalle-Fernandez-Tapia 2012, Stoikov microprice 2017, Cont OFI, hftbacktest implementations, or any relevant one you find).

### Venues

**Hyperliquid (on-chain order book)**
- L1 blockchain, USDC-margined perpetual futures
- Fee: Maker **1.44bps** (current tier). Taker 3.5bps. At VIP3+: Maker **0bps**
- Order RTT from Amsterdam: **536ms median** (both REST and WS — bottleneck is block consensus ~200ms + network ~300ms)
- WS L2 book updates: **1.83/sec** (530ms median interval), 20 levels per side
- ALO (post-only) rejection rate: **0%** at all depths tested (1bps through 100bps)
- 230 perp pairs, all positions public
- WS supports both subscriptions AND order submission (`{"method": "post", "type": "action"}`)

**Bybit (centralized exchange)**
- Centralized matching engine, USDT-margined perpetual futures
- Fee: Maker **2.0bps**. Taker **5.5bps**
- Order RTT from Amsterdam: **~220ms median** (REST). WS data: tick-by-tick, sub-100ms updates
- 200+ perp pairs, deep liquidity on majors
- No post-only rejection issues
- Standard REST + WS API, well-documented

### Measured Microstructure Data (1.2M L2 snapshots + 2.4M trade ticks, 7 days)

I'm giving you the MongoDB connection and collection schemas. **Query the data directly to validate hypotheses.**

**MongoDB:** `mongodb://localhost:27017/quants_lab`

**Collection: `hyperliquid_l2_snapshots_1s`** (1.2M docs)
```
{pair, coin, timestamp_utc (epoch_ms), mid_px, best_bid, best_ask, 
 spread_bps, bid_sz_topn, ask_sz_topn, imbalance_topn,
 levels_bid_json (JSON string of 20 levels [{px, sz, n}]),
 levels_ask_json (JSON string of 20 levels)}
Pairs: BTC-USDT (309K), ETH-USDT (309K), SOL-USDT (295K), HYPE-USDT (308K), ZEC-USDT (12K)
Timespan: Apr 25 - May 2, 2026
```

**Collection: `hyperliquid_recent_trades_1s`** (2.4M docs)
```
{pair, coin, time (datetime), px, sz, side ("B"/"A"), tid, hash}
Same pairs and timespan as L2.
```

**Collection: `arb_hl_bybit_perp_snapshots`** (2.8M docs)
```
{timestamp (datetime), pair, hl_bid, hl_ask, bb_bid, bb_ask, 
 spread_hl_over_bb, spread_bb_over_hl, best_spread, direction}
~5s intervals, 30 pairs, Apr 25 - May 2
```

**Measured HL book statistics:**
| Pair | Native Spread | Bid Depth (top 20) | Ask Depth | Trades/day |
|------|--------------|-------------------|-----------|-----------|
| SOL | 0.12bps | $443K | $405K | 25K |
| BTC | 0.13bps | $4.8M | $4.3M | 148K |
| ETH | 0.43bps | $12.7M | $10.2M | 63K |
| HYPE | 0.24bps | $158K | $122K | 77K |
| ZEC | 0.27bps | $167K | $150K | 13K |

**Cross-venue spread (Bybit - HL):**
| Pair | Mean | Std | P1 | P99 |
|------|------|-----|-----|-----|
| SOL | -0.22bps | 1.38 | -3.43 | 2.92 |
| BTC | 0.07bps | 1.06 | -2.54 | 2.66 |
| ETH | -0.57bps | 1.22 | -3.51 | 2.50 |

### What We Already Tried and KILLED (with evidence)

**X17 Selective Passive MM:** Post ALO on HL when Bybit-HL dislocation > threshold. Event-driven simulation on 295K L2 snapshots + 93K spread snapshots. Result: **negative PnL at every configuration, even at 0% fee.** Adverse selection 1.3-4bps post-fill. Higher selectivity (larger threshold) made adverse selection WORSE because large dislocations are information-driven (toxic flow), not mean-reverting.

| Threshold | Trades | WR | Avg PnL/trade | Sharpe |
|-----------|--------|----|---------------|--------|
| 2bps | 3,897 | 37% | -1.6bps | -153 |
| 5bps | 1,038 | 41% | -1.8bps | -72 |
| 10bps | 127 | 45% | -2.0bps | -22 |

**Root cause:** The composite signal (dislocation 45%, microprice 20%, L2 imbalance z 15%) used MOMENTUM quoting — leaning into the imbalance. Academic literature (Albers 2025 "Market Maker's Dilemma") says this is WRONG for makers. Contrarian quoting (fade imbalance) has lower fill rate but better post-fill markouts.

### Academic Foundation (papers we've already read)

1. **Avellaneda-Stoikov (2008):** Reservation price with inventory penalty: `r = s - q*gamma*sigma^2*tau`. Provides the skeleton but assumes stationary fills.
2. **GLFT (Gueant-Lehalle-Fernandez-Tapia 2012):** Grid extension of AS. Sharpe 17-19 in simulation. But calibration from own fills is noise-fitting with limited data.
3. **Stoikov Microprice (2017):** `microprice = ask * I + bid * (1-I)` where I = bid_qty/(bid_qty + ask_qty). Better fair value than simple mid.
4. **Cont OFI:** Order flow imbalance predicts 0.144bps per unit. R^2 = 3%. Useful for regime detection, not standalone signal.
5. **Albers (2025) "Market Maker's Dilemma":** Contrarian quoting beats momentum. When book is bid-heavy, tighten ASK (sell to demand), not BID.
6. **hftbacktest (2023-2025):** Fair value = mid + c1*imbalance. BTC: c1=160, spread=80 ticks, skew=3.5. Sharpe 10.83 with realistic queue model.
7. **Flash crash paper (2026, arXiv):** During Oct 2025 crash, maker strategy suffered catastrophic bid-side losses. Regime detection essential.

### Prior Codex Round (V1, killed)

Previous Codex designed a "selective passive alpha maker" with composite scoring. We built and simulated it. It FAILED because:
- Momentum signal application (should be contrarian)
- No realistic fill model (trade-through with 1s latency, not queue position)
- Capital math was broken for grid placement

### Constraints (HARD)

- **HL maker fee: 1.44bps** per fill (current tier). At VIP3+: 0bps. Bybit: maker 2.0bps, taker 5.5bps.
- **~1s see-and-react latency on HL** (530ms L2 update + 536ms order RTT). Bybit: ~220ms REST RTT + tick-by-tick WS.
- **No colocation** — Amsterdam to US, fixed ~300ms network to HL servers. Bybit servers closer (~200ms).
- **$54 HL live capital, $480 Bybit mainnet, $100K Bybit demo** for proof of concept. Can scale with performance.
- **Single operator** — strategy must run unattended with automated risk management.
- **No Hummingbot** — unreliable in production. Custom execution only.
- **Language: open.** Python is the current stack but if Rust, C++, or Go would materially reduce latency or improve execution quality, recommend that. Specify what the latency gain would be and whether it's worth the implementation cost.
- **Both venues available.** If the optimal MM strategy spans Bybit AND Hyperliquid (e.g., Bybit for hedging, HL for passive quoting; or Bybit for price discovery, HL for earning spread or whatever other alpha), design for that. Don't artificially constrain to one venue.

### What I Need From You

1. **Venue+pair selection:** Given the data above, which pair(s)/venue optimize the tradeoff between spread capture, adverse selection, and competition? Query the L2/trade data to check: autocorrelation of imbalance, markout distributions, fill probability at each queue position, time-of-day effects.

2. **Fair value model:** Specify the exact fair value computation. Reference Bybit mid (we have it at 5s resolution), microprice, OFI, or other features. How do you weight them? How do you calibrate?

3. **Quoting logic:** Given 1.44bps fee and ~1s latency, what is the MINIMUM spread we can quote profitably? How do we set it adaptively? How do we handle inventory (AS gamma, position limits)?

4. **Adverse selection defense:** Our simulation showed 1.3-4bps adverse selection. How do we reduce this? Contrarian quoting? Regime detection? Selective quoting windows?

5. **Fill model:** At ~1s latency, we're always behind the queue. What fraction of our resting orders will fill? How does ALO interact with queue priority on HL? Model this from the trade data.

6. **Risk management:** Max position, drawdown limits, regime detection (when to widen/pause). Specific thresholds.

7. **Expected P&L math:** Given all of the above, what is the theoretical edge per trade, expected daily PnL, Sharpe, and required capital? Show your math.

8. **Validation plan:** What should I test in the data BEFORE going live? Specific metrics, thresholds, and kill criteria.

**Access the MongoDB collections directly to validate any hypothesis. Don't theorize — measure.** Use `pymongo` to query the data. The Python path is `/Users/hermes/miniforge3/envs/quants-lab/bin/python`.

### Live Testing Access

If you want to run live tests (place+cancel orders to measure fill behavior, queue priority, or execution quality):

**Hyperliquid:**
- SDK: `hyperliquid-python-sdk` (v0.23.0), installed in the conda env
- Agent wallet key: in `.env` as `HL_PRIVATE_KEY` / `HL_ADDRESS`
- The agent trades on behalf of the main wallet. Initialize with: `Exchange(wallet, MAINNET_API_URL, account_address="0x11ca20aeb7cd014cf8406560ae405b12601994b4")`
- Set leverage first: `exchange.update_leverage(5, "SOL", is_cross=True)`
- Available capital: ~$54 USDC (unified spot+perp margin)
- Minimum order: $10 notional. Size decimals: BTC=5, ETH=4, SOL=2
- WS order submission: `{"method": "post", "id": N, "request": {"type": "action", "payload": {signed action}}}`
- **CRITICAL: this is REAL MONEY. after EVERY test, query open orders and cancel ALL of them.** Check: `requests.post("https://api.hyperliquid.xyz/info", json={"type": "openOrders", "user": "0x11ca20aeb7cd014cf8406560ae405b12601994b4"})`. Cancel any remaining with `exchange.cancel(coin, oid)`. Never leave ghost orders.

**Bybit (mainnet, real money — $480 USDT):**
- REST API base: `https://api.bybit.com`
- API credentials in `.env` as `BYBIT_API_KEY` / `BYBIT_API_SECRET`
- Category: `linear` (USDT perpetuals)
- Pybit SDK installed: `from pybit.unified_trading import HTTP`
- WS: `wss://stream.bybit.com/v5/public/linear` (public), `wss://stream.bybit.com/v5/private` (private)
- **CRITICAL: this is REAL MONEY. Same cleanup rules — cancel all test orders when done. Check open orders via `GET /v5/order/realtime` and cancel with `POST /v5/order/cancel-all`.** Never leave ghost positions.

**General rules for live tests:**
- Place orders FAR from market (5%+ below for buys, 5%+ above for sells) to avoid accidental fills
- Use ALO/PostOnly where available to guarantee no crossing
- Keep test position sizes minimal ($10-15 notional)
- Log all order IDs and verify cancellation before concluding
- If anything goes wrong, the emergency check is: query open orders on both venues and cancel everything

---

**End of prompt. Alberto — review and approve before sending.**
