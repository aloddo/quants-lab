# X7 Real-Time On-Chain Signal Feasibility (April 20, 2026)

## Context and Motivation

Two research tracks have now converged:

1. **Dune hourly on-chain data is dead for directional perps.** 11 hypotheses tested over 365 days, all failed except `stable_buy_usd_z24` at 4h horizon (Sharpe 2.98 OOS, but requires Dune's 5-15 min indexing delay and 4h decision cadence -- this is a slow macro signal, not tradeable alpha at the speed we need).

2. **Sub-second Binance-to-Bybit lead-lag has real alpha.** DOGE shows 5-15 bps gross on tail events, but it dies at +5s latency and requires all-in costs below 0.30 bps one-way. The edge is real but extremely latency-sensitive.

**The question:** Can we get on-chain data fast enough (sub-minute) to create directional signals for Bybit perps, bypassing Dune's delayed indexing entirely?

---

## 1. Real-Time On-Chain Data Sources

### 1.1 Ethereum Mempool Monitoring (Pre-Block)

**What it is:** Subscribe to pending transactions in the Ethereum mempool before they are included in a block. Large DEX swaps ($100K+) are visible as pending transactions 0-12 seconds before block inclusion.

**Latency:** Signal available ~0-12s BEFORE block confirmation. This is the fastest possible on-chain signal.

**Providers:**
| Provider | Method | Free Tier | Paid | Notes |
|----------|--------|-----------|------|-------|
| Blocknative | WebSocket SDK, webhooks | 1,000 events/day, 100 addresses | Custom pricing (Growth: 100K events/day) | Best mempool coverage, used by MEV searchers |
| Alchemy | `alchemy_pendingTransactions` | 30M CU/month (~1.8M requests) | $49/mo (Growth) | Smart WebSockets, good docs |
| QuickNode | `newPendingTransactions` filter | Limited | $49/mo+ | Credit-based pricing |
| Self-hosted node (Geth/Erigon) | `txpool_content` RPC | Hardware cost only (~$200/mo cloud, or local NVMe) | N/A | Full mempool, lowest latency, no rate limits |

**Key constraint:** Public RPCs filter mempool data and add latency. For serious mempool monitoring, you need either a dedicated node or a premium provider like Blocknative.

**What we can detect:**
- Pending large Uniswap/SushiSwap router calls (decoded from calldata)
- Pending large token transfers to/from exchange hot wallets
- Pending Aave/Compound liquidation calls
- MEV bundle submissions (via Flashbots relay, but those are private)

### 1.2 Confirmed Block Events (Post-Block, Real-Time)

**What it is:** Subscribe to `eth_subscribe("logs")` via WebSocket, filtering for specific contract events as blocks are confirmed.

**Latency:** Signal available within ~1-2s of block confirmation (block time is 12s, so average wait from event to signal is ~6s for the block time + ~1s for propagation/parsing).

**Key events to monitor:**

| Event | Contract | Signal Hypothesis |
|-------|----------|-------------------|
| Uniswap V3 `Swap(address,address,int256,int256,uint160,uint128,int24)` | Per-pool (thousands of contracts) | Large swap -> directional pressure on CEX |
| Uniswap V2 `Swap(address,uint,uint,uint,uint)` | Per-pair contract | Same, but V2 pools |
| Aave V3 `LiquidationCall` | Aave Pool proxy | Liquidation cascade -> short-term volatility |
| ERC-20 `Transfer` (large) | Any token contract | Whale movement -> directional signal |
| WETH `Deposit`/`Withdrawal` | WETH contract | ETH wrapping surge -> DEX activity incoming |

**Uniswap V3 architectural constraint:** The router does NOT emit events. Every liquidity pool is a separate contract. To monitor "all ETH-paired swaps" you need to subscribe to logs from hundreds of pool addresses, or use a wildcard topic filter (supported by most providers).

**Providers (same as above):** Alchemy, Infura, QuickNode, Chainstack all support `eth_subscribe("logs")` over WebSocket. Free tiers are sufficient for PoC (we're filtering to maybe 50-100 events/minute for large swaps).

### 1.3 Aave/Compound Liquidation Events

**What it is:** Monitor `LiquidationCall` events on Aave V3 (or Compound V3 `AbsorbCollateral`). Large liquidations ($1M+) cause forced selling on-chain that can spill into CEX prices.

**Latency:** Same as confirmed block events (~6s average from event to signal).

**Signal hypothesis:** A $5M+ ETH liquidation on Aave means forced selling pressure. If CEX hasn't repriced yet (12s window), short ETH-USDT perp on Bybit.

**Recent precedent:** April 2026 KelpDAO exploit triggered $6B in Aave withdrawals and $300M in emergency borrowing. Liquidation cascades are real and tradeable -- the question is whether we can detect and act before CEX reprices.

### 1.4 Large Token Transfers (Whale Alerts)

**What it is:** Monitor ERC-20 `Transfer` events above a USD threshold. Filter for:
- Transfers TO known exchange deposit addresses (selling pressure)
- Transfers FROM known exchange addresses (accumulation)
- Large stablecoin mints (USDT/USDC `Transfer` from zero address)

**Latency:** Same as confirmed block events.

**Known exchange addresses:** Publicly labeled on Etherscan. Arkham Intelligence maintains the best database, but their API is expensive. For PoC, hardcode top-20 exchange hot/cold wallet addresses.

---

## 2. Infrastructure Available on This Machine

### What we have:
- **web3.py 7.14.1** -- installed in quants-lab conda env, fully functional
- **websockets 15.0.1** -- WebSocket client library
- **aiohttp 3.12.15** -- async HTTP client
- **Python 3.12** -- full async/await support
- **MongoDB** -- running locally, can store events with sub-second timestamps
- **Bybit REST/WebSocket client** -- already operational for arb system

### What we need:
- **Ethereum RPC endpoint with WebSocket support** -- NOT configured in `.env`
- **Minimum viable:** Alchemy free tier (30M CU/month) or Infura free tier (100K requests/day)
- **Recommended for PoC:** Alchemy Growth ($49/month) for reliable WebSocket subscriptions
- **For production:** Dedicated Geth/Erigon node (~$200/month cloud or local NVMe SSD)

### Existing codebase assets:
- `scripts/arb_quote_collector.py` -- 5s snapshot collector for Binance/Bybit quotes (template for event collection)
- `scripts/x7_onchain_signal_scan.py` -- Dune-based on-chain scanner (hourly, too slow)
- `scripts/x7_cex_subsecond_eda.py` -- Sub-second lead-lag analysis (can reuse for on-chain->CEX lag measurement)
- `app/services/` -- Bybit REST client for price comparison
- `core/services/okx_dex_api.py` -- Existing DEX API client in upstream (reference only)

---

## 3. Latency Math: Can We Trade This?

### 3.1 The Critical Timeline

```
EVENT OCCURS ON ETHEREUM
  |
  +-- [0-12s] Transaction sits in mempool (MEMPOOL PATH: signal available HERE)
  |
  v
BLOCK CONFIRMED (avg 12s cycle)
  |
  +-- [~1s] Block propagates to our RPC provider
  +-- [~0.1s] WebSocket delivers log event to our listener
  +-- [~0.01s] We decode and evaluate the signal
  |
  v
SIGNAL READY (total: ~13s from event, or ~1s from block if using confirmed events)
  |
  +-- [~0.05s] REST API call to Bybit
  +-- [~0.1-0.5s] Bybit order matching
  |
  v
POSITION OPENED (total: ~13.5s from event, or ~1.5s from block confirmation)
```

### 3.2 The Competition

**Who else is watching?**
- **MEV searchers** -- They monitor the mempool with custom infrastructure, submit bundles via Flashbots. They are 10-100ms faster than us on the mempool path. We cannot compete with them on mempool-based signals.
- **CEX-DEX arbitrageurs** -- They watch DEX events and arbitrage price differences. They operate with dedicated co-located infrastructure. Latency advantage research shows even 1-second subslots would increase arb volume by 535% (Adams et al., Jan 2026).
- **Market makers** -- Bybit/Binance MMs update quotes within 50-200ms of any significant event. They have direct feeds and co-located servers.

### 3.3 The Alpha Window

**Mempool path (pre-block):**
- We see a $500K ETH sell on Uniswap V3 in the mempool
- Signal latency: ~0.5s (mempool detection + decode)
- Bybit execution: ~0.5s
- Total: ~1s from mempool detection
- **BUT:** MEV searchers see it at the same time and will front-run or back-run the DEX trade. The on-chain impact is already being traded by the time we see the mempool tx.

**Confirmed event path (post-block):**
- Large swap confirmed in block N
- Signal latency: ~1s (block propagation + WebSocket + decode)
- Bybit execution: ~0.5s
- Total: ~1.5s from block confirmation, ~13.5s from the actual swap
- **Key question:** Has Bybit already repriced in those 13.5 seconds?

### 3.4 Empirical Evidence from Our Own Research

From `x7_subsecond_handoff_2026_04_20.md`:
- Binance-to-Bybit lead-lag on DOGE: alpha dies at +5 seconds
- Even within the SAME asset class on TWO CEXes, 5 seconds of latency kills the edge
- An on-chain signal has 12s of block time + 1.5s of processing = ~13.5s minimum
- **This is 2.7x the latency that kills our CEX-CEX edge**

From academic research (Adams et al., 2026):
- CEX mid prices update continuously at millisecond frequency
- DEX prices update at 12-second slot boundaries
- CEX-DEX arbitrageurs already extract value from this gap with sub-second infrastructure
- We would be competing for the SAME gap with 10-100x worse latency

---

## 4. Signal-by-Signal Viability Assessment

### 4.1 Large Uniswap Swaps -> Bybit Perp Direction

| Factor | Assessment |
|--------|------------|
| Signal quality | Plausible -- large DEX swaps represent real demand |
| Latency (confirmed) | ~13.5s -- almost certainly too slow |
| Latency (mempool) | ~1s -- competitive but MEV searchers are faster |
| Competition | Extreme -- MEV searchers, CEX-DEX arbs, market makers |
| Expected alpha | Near zero after latency. CEX reprices within 1-3 blocks. |
| Verdict | **DEAD for directional perp trading** |

### 4.2 Aave/Compound Liquidation Cascades -> Bybit Perp Direction

| Factor | Assessment |
|--------|------------|
| Signal quality | Strong during cascades (rare, high-impact events) |
| Latency (confirmed) | ~13.5s -- but cascades unfold over MINUTES |
| Competition | Lower for cascade prediction (most arbs focus on single-block) |
| Expected alpha | **Possible for multi-minute cascades only** |
| Frequency | Very low -- major cascades happen 2-5x/year |
| Verdict | **Niche: worth monitoring but cannot sustain a strategy** |

### 4.3 Whale Transfers to Exchange Addresses -> Bybit Perp Direction

| Factor | Assessment |
|--------|------------|
| Signal quality | Weak -- transfer to exchange does not mean immediate sell |
| Latency (confirmed) | ~13.5s, but actual selling happens minutes to hours later |
| Competition | Moderate -- Whale Alert bots publish within seconds |
| Expected alpha | Near zero. Transfer != immediate sell order. |
| Verdict | **DEAD -- too noisy, too slow causal chain** |

### 4.4 Stablecoin Mints/Burns -> Bybit Perp Direction

| Factor | Assessment |
|--------|------------|
| Signal quality | Our own research shows `stable_buy_usd_z24` works at 4h horizon |
| Latency requirement | Not latency-sensitive -- 4h decision cadence |
| Competition | Low at 4h horizon |
| Expected alpha | **Already validated: Sharpe 2.98 OOS** |
| Verdict | **VIABLE but does NOT need real-time on-chain** -- Dune hourly is fine |

### 4.5 Mempool-Based Pre-Execution Front-Running

| Factor | Assessment |
|--------|------------|
| Signal quality | Extremely high -- you know the trade BEFORE it executes |
| Latency | Sub-second with dedicated infrastructure |
| Competition | This IS MEV extraction. Dominated by Flashbots searchers. |
| Infrastructure required | Dedicated Geth node, Flashbots relay, custom block building |
| Capital required | 10-100 ETH for gas wars, specialized smart contracts |
| Regulatory risk | Legally grey area, active regulatory scrutiny |
| Verdict | **NOT our game -- requires specialized MEV infrastructure, not a perp trading system** |

---

## 5. Proof-of-Concept Architecture (If We Proceed)

Despite the likely negative conclusion, a minimal PoC can PROVE the latency math empirically. Cost: ~$50/month + 2-3 days of engineering.

### 5.1 Components

```
┌─────────────────────┐     ┌──────────────────────┐
│ Alchemy WebSocket    │────>│ Event Listener        │
│ eth_subscribe("logs")│     │ (Python asyncio)      │
│ Uniswap V3 Swap     │     │ - Decode swap events   │
│ Aave LiquidationCall │     │ - Filter $100K+ swaps  │
│ Large ERC20 Transfer │     │ - Compute direction    │
└─────────────────────┘     │ - Timestamp (us prec)  │
                             └──────────┬───────────┘
                                        │
                                        v
                             ┌──────────────────────┐
                             │ Bybit Price Sampler   │
                             │ - Snapshot mid price   │
                             │   at event time        │
                             │ - Snapshot at +5s,     │
                             │   +15s, +30s, +60s     │
                             └──────────┬───────────┘
                                        │
                                        v
                             ┌──────────────────────┐
                             │ MongoDB Collection     │
                             │ onchain_rt_events      │
                             │ - event_type, pair,    │
                             │   direction, usd_size  │
                             │ - bybit_mid_at_event   │
                             │ - bybit_mid_at_5s      │
                             │ - bybit_mid_at_15s     │
                             │ - bybit_mid_at_30s     │
                             │ - bybit_mid_at_60s     │
                             └──────────────────────┘
```

### 5.2 Implementation Requirements

1. **Alchemy account** -- Sign up at alchemy.com, get free API key, add `ALCHEMY_API_KEY` and `ALCHEMY_WSS_URL` to `.env`
2. **Uniswap V3 pool addresses** -- Top 10 WETH-paired pools by TVL (ETH/USDC, ETH/USDT, ETH/WBTC, etc.)
3. **ABI decoding** -- Swap event ABI is fixed, decode `amount0`, `amount1`, `sqrtPriceX96`
4. **Bybit WebSocket** -- Already have this infrastructure in the arb system
5. **~200 lines of Python** -- async event listener + price sampler + MongoDB writer

### 5.3 What the PoC Measures

After 7-14 days of collection:
- **Latency distribution:** Time from block timestamp to our signal timestamp
- **Price impact decay:** Bybit mid-price change at +5s, +15s, +30s, +60s after large DEX swap
- **Directional hit rate:** Does a large WETH buy on Uniswap predict ETH-USDT going up on Bybit?
- **Alpha after latency:** Is there any residual price movement AFTER our realistic signal arrival time?

### 5.4 Expected Outcome

Based on all evidence:
- **Most likely result:** Bybit reprices within 1-3 blocks (12-36s). By the time our confirmed-event signal arrives (~13.5s), 70-90% of the price impact is already absorbed.
- **Best case:** Tail events (liquidation cascades, $10M+ swaps) may have slower absorption. Worth quantifying.
- **Worst case:** Zero residual alpha at any horizon. On-chain events are fully priced by CEX within the block confirmation window.

---

## 6. Cost Estimate

### PoC Phase (7-14 days)

| Item | Cost |
|------|------|
| Alchemy Growth plan (WebSocket) | $49/month |
| Engineering time | 2-3 days |
| Compute (already have Mac Mini) | $0 |
| **Total** | **~$49 + time** |

### Production (if PoC shows alpha)

| Item | Cost |
|------|------|
| Dedicated Geth/Erigon node (for mempool) | $200-400/month (cloud) or $2K one-time (local NVMe) |
| Alchemy/Chainstack backup | $49-99/month |
| Bybit VIP for lower fees | Requires volume |
| **Total** | **$250-500/month** |

---

## 7. Verdict and Recommendation

### Hard No on Real-Time On-Chain for Directional Perps

The latency math does not work. Here is why:

1. **12-second block time is the killer.** Ethereum blocks are 12 seconds. Our confirmed-event path adds ~1.5s on top. Total ~13.5s. Our own CEX-CEX research proves that even 5 seconds of latency kills alpha on the SAME asset between two CEXes. On-chain to CEX is structurally slower.

2. **Mempool path is MEV territory.** Pre-block signals require competing with Flashbots searchers who have dedicated nodes, custom block builders, and 10-100ms latency. This is not our competency and requires fundamentally different infrastructure (smart contracts, not perp trading).

3. **CEX reprices faster than we can act.** Market makers on Bybit update quotes within 50-200ms of significant events. Arbitrageurs close DEX-CEX gaps within 1-2 blocks. By block confirmation + our processing, the move is done.

4. **The only viable on-chain signal (stablecoin flows) does not need real-time data.** Our validated `stable_buy_usd_z24` signal works at 4h cadence. Dune hourly indexing is more than fast enough. Real-time infrastructure adds cost and complexity for zero additional alpha on this signal.

### What IS Worth Doing Instead

1. **Deploy `stable_buy_usd_z24` as a slow overlay signal** via Dune API on 4h cadence. Already validated (Sharpe 2.98 OOS). No real-time infra needed.

2. **Double down on CEX-CEX microstructure** where we have proven alpha. The subsecond lead-lag on DOGE is real but needs:
   - True tick data (not 5s bars)
   - Maker fee tier on Bybit (to get below 0.30 bps one-way)
   - Sub-second execution infrastructure

3. **Run the PoC anyway ($49, 2 days)** purely to MEASURE the price-impact decay curve empirically. If we find that $5M+ liquidation cascades take 60+ seconds to fully reprice on Bybit, there is a narrow niche signal worth automating. The PoC is cheap enough to be worth the data.

### Recommended Next Action

Run the PoC event collector for 14 days, then analyze the decay curves before investing further.

---

## Sources

- [Blocknative Mempool Explorer](https://www.blocknative.com/explorer)
- [Blocknative Pricing](https://www.blocknative.com/pricing)
- [Chainstack Ethereum RPC Providers 2026](https://chainstack.com/best-ethereum-rpc-providers-in-2026/)
- [Alchemy RPC Provider Overview 2026](https://chainstack.com/alchemy-rpc-provider-overview-2026/)
- [QuickNode Uniswap Swaps Detector](https://www.quicknode.com/docs/functions/functions-library/uniswap-swaps-detector)
- [QuickNode Aave V3 Liquidation Tracker](https://www.quicknode.com/sample-app-library/ethereum-aave-liquidation-tracker)
- [Chainstack: Track Swaps on Uniswap in Real-Time](https://chainstack.com/track-swaps-on-uniswap-in-real-time/)
- [Adams et al. (2026): Second Thoughts -- How 1-second subslots transform CEX-DEX Arbitrage on Ethereum](https://ethresear.ch/t/second-thoughts-how-1-second-subslots-transform-cex-dex-arbitrage-on-ethereum/23923)
- [Alexander (2025): Price Discovery and Efficiency in Uniswap Liquidity Pools](https://onlinelibrary.wiley.com/doi/10.1002/fut.22593)
- [Latency Advantage in CEX-DEX Arbitrage (Devcon SEA)](https://speak.devcon.org/devcon7-sea/talk/RPMHLF/)
- [Bitquery: Top Ethereum Mempool Data Providers](https://bitquery.io/blog/mempool-data-providers)
- [WhaleScope: Real-time Ethereum Mempool Monitor](https://github.com/BitMorphX/whale_scope)
