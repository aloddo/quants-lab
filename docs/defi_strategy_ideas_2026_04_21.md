# DeFi Strategy Ideas (April 21, 2026)

## Context
Exploration of DeFi venues supported by Hummingbot as alternatives/complements to Bybit/Binance/Bitvavo CEX stack.

## HB DeFi Infrastructure (Already Built)

| Component | Status | Notes |
|-----------|--------|-------|
| Hyperliquid perp connector | In HB, currently excluded from CLOB | Un-exclude + configure |
| dYdX v4 perp connector | In HB, currently excluded | Un-exclude + configure |
| Vertex perp connector | In HB, currently excluded | Un-exclude + configure |
| LP executor (Meteora/Raydium/Uni V3) | Built, production-ready | State machine: OPENING -> IN_RANGE <-> OUT_OF_RANGE -> CLOSING -> COMPLETE |
| lp_rebalancer controller | Registered in HB | Dynamic LP range management |
| stat_arb controller | Registered in HB | Cross-venue statistical arbitrage |
| xemm controller | Registered in HB | Cross-exchange market making |
| pmm_dynamic controller | Registered in HB | Dynamic market making |
| arbitrage_controller | Registered in HB | Generic arbitrage |
| GeckoTerminal MCP | Working | DEX pool discovery, OHLCV, trades |
| Solana Gateway | Configured | Required for Meteora/Raydium |
| HL funding data collection | Running | hyperliquid_funding_task.py |
| OKX DEX aggregator client | Code exists (core/services/okx_dex_api.py) | Solana swap routing |

## Strategy Ideas

### D1: HL Whale Positioning Signal (PRIORITY)
**Thesis:** Hyperliquid publishes all trader positions on-chain. Detect large position buildups (whale concentration) and trade the anticipated squeeze/continuation.
**Signal:** Monitor HL open positions API for whale clustering. When top-N traders are 70%+ one-sided, fade (crowd reversion) or follow (momentum) depending on OI trend.
**Edge source:** HL position transparency is unique -- no CEX exposes this. Information asymmetry vs Bybit traders.
**Venue:** Execute on Bybit perps (better liquidity) using HL data as signal.
**Overlap:** X9 funding crowding thesis (crowd reversion) + X5 liquidation cascade (forced flow) + B7 cross-asset leader-follower
**Effort:** Medium. Need HL position data collector + signal research.
**Priority Score:** (4*0.30) + (4*0.25) + (3*0.20) + (5*0.15) + (5*0.10) = 1.20+1.00+0.60+0.75+0.50 = 4.05

### D2: Cross-Venue Funding Arb (HL <-> Bybit)
**Thesis:** HL settles funding hourly (vs Bybit 8h). Different funding calculation methods create persistent spreads.
**Signal:** When HL funding rate diverges >2sigma from Bybit implied rate, arb the spread.
**Edge source:** Different settlement frequencies + different trader populations.
**Venue:** Long on cheap-funding venue, short on expensive-funding venue.
**Overlap:** X8 DeFi-CEX Funding Spread (exactly this thesis), X1 Funding Divergence (Bybit-Binance variant)
**Effort:** Low. X8 research already done (33 pairs with r>0.5, BTC Sharpe 3.47).
**Priority Score:** (3*0.30) + (4*0.25) + (4*0.20) + (5*0.15) + (3*0.10) = 0.90+1.00+0.80+0.75+0.30 = 3.75

### D3: HL Market Making (CLOB)
**Thesis:** Mid-cap pairs on HL have wider spreads than Bybit. Maker rebates (0.01%) + spread capture.
**Signal:** Inventory-managed PMM with dynamic spread based on volatility.
**Edge source:** Lower competition on HL vs Bybit for MM. Transparent order book = better adverse selection management.
**Venue:** HL directly.
**Overlap:** B1 queue depletion, B3 toxic flow regime (both are MM-adjacent signals)
**Effort:** Low. pmm_dynamic controller exists.
**Priority Score:** (3*0.30) + (5*0.25) + (3*0.20) + (4*0.15) + (2*0.10) = 0.90+1.25+0.60+0.60+0.20 = 3.55

### D4: CLMM LP on Solana (Meteora/Raydium)
**Thesis:** Concentrated LP on high-volume stable or major pairs earns trading fees. Hedge directional risk on perps.
**Signal:** Dynamic range based on realized volatility. Rebalance when out-of-range.
**Venue:** Meteora/Raydium for LP, HL/Bybit for hedge.
**Overlap:** lp_rebalancer controller already exists. Portfolio risk (WS5 P4) applies.
**Effort:** Medium. Gateway + Solana wallet setup. LP executor is ready.
**Priority Score:** (3*0.30) + (4*0.25) + (2*0.20) + (4*0.15) + (3*0.10) = 0.90+1.00+0.40+0.60+0.30 = 3.20

### D5: CEX-DEX Statistical Arb
**Thesis:** Price dislocations between Raydium/Meteora and Bybit perps on mid-cap tokens.
**Signal:** Spread z-score with stale-quote filtering (similar to A1/A2 Bitvavo arb).
**Venue:** Buy cheap side, sell rich side.
**Overlap:** A1-A6 arb series (same thesis, different venue pair). xemm controller exists.
**Effort:** Medium-high. Latency profiling needed. Solana 400ms block time is constraint.
**Priority Score:** (3*0.30) + (3*0.25) + (3*0.20) + (3*0.15) + (3*0.10) = 0.90+0.75+0.60+0.45+0.30 = 3.00

### D6: Limit Range Orders (LP as Superior Limit Orders)
**Thesis:** Single-sided CLMM LP positions act as limit orders that EARN fees during conversion.
**Signal:** Use directional signals (X5/X10/D1) to set LP ranges as entries. Auto-close when filled.
**Venue:** Meteora/Raydium.
**Overlap:** Novel execution method, not a signal strategy. Pairs with any directional strategy.
**Effort:** Low once D4 infrastructure exists.
**Priority Score:** (2*0.30) + (4*0.25) + (2*0.20) + (4*0.15) + (4*0.10) = 0.60+1.00+0.40+0.60+0.40 = 3.00

## Overlap Analysis with Existing Research

### Direct Overlaps (Same Thesis, Different Venue)
| DeFi Idea | Existing Research | Overlap | Status of Existing |
|-----------|-------------------|---------|-------------------|
| D2 (HL-Bybit funding arb) | X8 DeFi-CEX Funding Spread | 95% overlap | Phase 0 PASS, controller spec next |
| D5 (CEX-DEX stat arb) | A1-A6 (Bitvavo arb series) | 70% overlap (same mechanics) | Week 2 in Codex 6-week plan |
| D1 (HL whale positioning) | X9 Funding Crowding | 40% overlap (crowd reversion thesis) | Independent discovery, test +150 bps/day |

### Signal Source Overlaps
| DeFi Idea | Existing Signal | How DeFi Extends It |
|-----------|----------------|---------------------|
| D1 (HL whale positions) | B7 Cross-asset leader-follower | HL positions are the DIRECT positioning data B7 tries to infer from price |
| D1 (HL whale positions) | X5 Liquidation cascade | Whale concentration predicts WHERE liquidation cascades will happen |
| D3 (HL market making) | B1 Queue depletion + B3 Toxic flow | HL transparent book means we can DIRECTLY measure B1/B3 signals |
| D4 (CLMM LP) | C5 Macro liquidity proxies | LP yield is a real-time liquidity measure |

### Key Insight
D1 (HL whale positioning) is the most novel because it solves the biggest problem in our existing research: **we've been trying to INFER positioning from derivatives data (funding, OI, LS ratio) when HL literally publishes it.** X9 funding crowding found +150 bps/day fading crowd positioning using INDIRECT signals. D1 uses the DIRECT signal.

## Recommended Priority Order
1. **D1** -- HL whale positioning (highest novelty, unique data, extends X9 thesis)
2. **D2** -- HL-Bybit funding arb (X8 already validated, just needs execution)
3. **D3** -- HL market making (low effort, existing controller)
4. **D4** -- Solana CLMM LP (needs infra but LP executor is ready)
5. **D5/D6** -- CEX-DEX arb and limit range orders (after above prove out)

## What's NOT Worth Pursuing
- Uniswap V3 LP on Ethereum (12s block time + $5-50 gas kills edge)
- dYdX v4 LP (it's a CLOB, not AMM -- no LP executor applies)
- Sub-second on-chain execution (X7 killed this thesis)
- Any DeFi strategy requiring Ethereum mainnet real-time execution
