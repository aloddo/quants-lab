# Exotic Signal Factory v2 — Full Execution Plan

## Why v2

v1 tested 10+ strategies across 50K+ trades over 365 days using basic exchange derivatives data (funding rates, OI, LS ratio from Bybit/Binance). Every single strategy produced the same failure pattern:

- Trailing stops capture small gains (+$0.70 avg, 40% of exits)
- Stop losses produce large losses (-$2.70 avg, 25% of exits)
- The asymmetry is always wrong regardless of signal quality

Root cause: the data is commodity-grade (every retail trader has it), the signals are threshold-based (z-scores on public data), and the exit model is static (fixed TripleBarrier). No amount of parameter tuning on this architecture will produce durable alpha.

v2 takes a fundamentally different approach: proprietary data collection, ML-based signal generation, and dynamic execution.

### What we learned from v1 failures

| Finding | Implication for v2 |
|---------|-------------------|
| Derivatives signals have directional info but not enough edge | Use as features in ML model, not standalone signals |
| H2 showed PF=1.07-1.16 when regime + direction aligned | Regime conditioning is mandatory, not optional |
| E3 shorts 80% of the time (structural funding bias) | Don't bet against known market structure; USE it as context |
| Every strategy fails the same way (SL drag) | The exit model is as important as the entry model |
| 50K+ labeled trades sitting unused | We have training data for ML already |
| All data sources are public exchange APIs | Zero information advantage; need proprietary features |

---

## Architecture Overview

```
LAYER 1: Data Collection (the moat)
  Exotic data that most retail algos don't have or haven't processed
  
LAYER 2: Feature Engineering (synthetic intelligence)
  Transform raw data into non-obvious predictive features
  
LAYER 3: ML Signal Generation (the edge)
  Non-linear pattern detection across feature space
  
LAYER 4: Execution (adaptive, not static)
  Dynamic exits, confidence-based sizing
  
LAYER 5: Soft Gates (existing derivatives data)
  Funding/OI/LS as confirmation filters
```

---

## LAYER 1: Data Collection

### 1A. Deribit Options Surface (BTC + ETH)

**Why this matters:** Options market is where institutional smart money expresses views. IV skew tells you what the market EXPECTS, not what happened. Put/call OI ratio reveals hedging demand. Max pain predicts short-term price magnets. This is the single most information-dense data source in crypto that most directional traders ignore.

**API:** `https://www.deribit.com/api/v2` (public, no auth, free)

**What to collect (every 15 minutes):**

```
Endpoint: /public/get_book_summary_by_currency
Params: currency=BTC, kind=option
Returns: mark_iv, open_interest, volume, bid/ask for EVERY active option

Endpoint: /public/get_instruments
Params: currency=BTC, kind=option, expired=false
Returns: all active strikes/expiries (needed to build the surface)
```

**Document schema for MongoDB (`deribit_options_surface`):**
```json
{
  "currency": "BTC",
  "timestamp_utc": 1712930400000,
  "expiry": "2026-04-18",
  "strike": 85000,
  "type": "call",
  "mark_iv": 0.52,
  "open_interest": 1250.5,
  "volume_24h": 89.3,
  "mark_price_btc": 0.0234,
  "mark_price_usd": 1989.0,
  "bid_iv": 0.51,
  "ask_iv": 0.53,
  "underlying_price": 85000,
  "collected_at": 1712930410000
}
```

**Derived metrics (computed in feature engineering, not stored raw):**
- 25-delta skew: IV(25d put) - IV(25d call) per expiry
- Term structure: ATM IV across expiries (front vs back month)
- Put/call OI ratio: sum(put OI) / sum(call OI) per expiry
- Max pain: strike that minimizes total option holder payoff
- GEX (Gamma Exposure): net gamma across all strikes, weighted by OI
- DVol proxy: weighted ATM IV across expiries (30-day constant maturity)
- Skew velocity: rate of change of 25d skew (not level, but momentum)

**Historical limitation:** Deribit API only returns current snapshot. Must poll and store to build history. Start collecting NOW — every day of delay is a day of missing training data.

**Backfill option:** Deribit provides historical trade data downloads at https://www.deribit.com/statistics/BTC/historical-data. These contain option trades with IVs. We can reconstruct approximate IV surfaces from trade-level data going back to 2019.

**Rate limits:** Credit-based, generous for public endpoints. 15-min polling for full surface is well within limits.

**Task class:** `DeribitOptionsSurfaceTask` — new collection task, same pattern as `BinanceFundingTask`

**Collection frequency:** Every 15 minutes (options don't change faster than this for our purposes)

**Priority: HIGH** — free, high information density, no API key needed, BTC+ETH only but those are our most liquid pairs

---

### 1B. Coinglass Liquidation Data

**Why this matters:** Liquidation cascades are the mechanism behind the sharpest moves in crypto. When longs get liquidated, forced selling creates more selling, which triggers more liquidations. The cascade itself is predictable from OI + price levels. Coinglass aggregates liquidation data across ALL exchanges — we currently only see Bybit.

**API:** `https://open-api-v4.coinglass.com` (API key required, free tier available)

**What to collect (every 15 minutes):**

```
Endpoint: /futures/liquidation/history
Params: symbol=BTC, interval=1h, exchange=all
Returns: long_liquidation_usd, short_liquidation_usd, timestamp

Endpoint: /futures/openInterest/ohlc-history  
Params: symbol=BTC, interval=1h
Returns: OI OHLC across all exchanges (not just Bybit)

Endpoint: /futures/longShortRatio
Params: symbol=BTC, interval=1h
Returns: top trader long/short ratio per exchange
```

**Document schema for MongoDB (`coinglass_liquidations`):**
```json
{
  "pair": "BTC-USDT",
  "timestamp_utc": 1712930400000,
  "interval": "1h",
  "long_liquidation_usd": 45000000,
  "short_liquidation_usd": 12000000,
  "liquidation_imbalance": 0.58,
  "total_liquidation_usd": 57000000,
  "source": "coinglass",
  "collected_at": 1712930410000
}
```

**Document schema for MongoDB (`coinglass_oi`):**
```json
{
  "pair": "BTC-USDT",
  "timestamp_utc": 1712930400000,
  "interval": "1h",
  "oi_open": 23400000000,
  "oi_high": 23800000000,
  "oi_low": 23200000000,
  "oi_close": 23600000000,
  "source": "coinglass",
  "collected_at": 1712930410000
}
```

**Derived metrics:**
- Liquidation imbalance: (long_liq - short_liq) / total_liq — directional pressure
- Cascade velocity: rate of change of liquidation volume (acceleration = cascade in progress)
- Liquidation-adjusted OI: OI change minus liquidation volume = organic position changes
- Cross-exchange OI divergence: Bybit OI vs Binance OI vs total — where is positioning concentrated?
- Liquidation heat map: cluster liquidation levels by price to find cascade trigger zones

**Historical depth:** 2-3 years via API, sufficient for ML training

**Backfill:** Paginate through history endpoint with start_time/end_time

**Rate limits:** Free tier, standard limits (details from Coinglass support)

**API key:** Required. Sign up at coinglass.com. Free tier provides liquidation + OI + funding data.

**Task class:** `CoinglassLiquidationTask`, `CoinglassOITask`

**Priority: HIGH** — free tier sufficient, unique cross-exchange aggregation, liquidation data is genuinely not-retail

---

### 1C. Fear & Greed Index + Sentiment

**Why this matters:** Extreme fear/greed are contrarian indicators with documented predictive power at daily resolution. Combined with other features, sentiment adds a dimension that pure price/volume data misses.

**API:** `https://api.alternative.me/fng/` (free, no auth)

**What to collect (daily):**

```
Endpoint: /fng/?limit=0
Returns: full history, daily values 0-100
Components: volatility (25%), momentum/volume (25%), 
            social media (15%), surveys (15%), 
            BTC dominance (10%), Google Trends (10%)
```

**Document schema for MongoDB (`fear_greed_index`):**
```json
{
  "timestamp_utc": 1712880000000,
  "value": 72,
  "classification": "Greed",
  "source": "alternative.me",
  "collected_at": 1712930410000
}
```

**Backfill:** Single API call returns ALL history (2018+). Do this once.

**Task class:** `FearGreedTask` — trivial, one API call per day

**Priority: MEDIUM** — free, easy, adds a sentiment dimension. Daily resolution limits use for intraday signals but good as daily context feature.

---

### 1D. Glassnode On-Chain Metrics (if budget allows)

**Why this matters:** Exchange inflow/outflow is the closest thing to knowing what whales are about to do. A 5,000 BTC transfer to Binance has a documented ~70% probability of preceding a sell within hours. Stablecoin supply changes indicate capital entering/leaving crypto.

**API:** `https://api.glassnode.com` (API key required)

**Pricing reality check:**
- Free tier: basic metrics, 24h resolution, delayed data — usable for daily features
- Professional ($999/mo + API): hourly resolution, entity-adjusted — ideal but expensive
- Recommendation: start with free tier for daily features, upgrade if ML shows on-chain features have predictive power

**What to collect (daily on free tier, hourly on paid):**

```
Endpoint: /v1/metrics/transactions/transfers_volume_exchanges_net
Params: a=BTC, s=<start>, i=24h (free) or 1h (paid)
Returns: net exchange flow in BTC

Endpoint: /v1/metrics/market/sopr
Returns: Spent Output Profit Ratio (>1 = holders in profit, <1 = at loss)

Endpoint: /v1/metrics/market/mvrv
Returns: Market Value / Realized Value (valuation signal)

Endpoint: /v1/metrics/supply/current
Params: a=USDT
Returns: stablecoin total supply (proxy for capital inflow)
```

**Document schema for MongoDB (`glassnode_onchain`):**
```json
{
  "metric": "exchange_netflow",
  "asset": "BTC",
  "timestamp_utc": 1712880000000,
  "value": -1250.5,
  "resolution": "24h",
  "source": "glassnode",
  "collected_at": 1712930410000
}
```

**Derived metrics:**
- Exchange flow momentum: 7d MA of netflow direction
- SOPR regime: >1 sustained = bull, <1 sustained = bear, oscillating = range
- MVRV z-score: how far from historical mean (extreme = reversal likely)
- Stablecoin supply velocity: rate of change of USDT+USDC supply

**Priority: LOW for now** — start with free tier daily data for feature exploration. Upgrade only if ML model shows on-chain features improve predictions significantly.

---

### 1E. Order Book Depth (from existing HB WebSocket)

**Why this matters:** We already have HB WebSocket connections to Bybit. Order book depth data reveals supply/demand imbalance at specific price levels. Book imbalance is a known short-term predictor.

**No new API needed.** HB already subscribes to order book updates. We need to:
1. Add a collection task that snapshots L2 order book every 1-5 minutes
2. Store top 10 bid/ask levels with sizes
3. Compute book imbalance features

**Document schema for MongoDB (`order_book_snapshots`):**
```json
{
  "pair": "BTC-USDT",
  "timestamp_utc": 1712930400000,
  "exchange": "bybit",
  "bids": [[84950, 12.5], [84940, 8.3], ...],
  "asks": [[85000, 15.2], [85010, 6.1], ...],
  "mid_price": 84975,
  "spread_bps": 0.59,
  "bid_depth_10": 156.3,
  "ask_depth_10": 142.7,
  "book_imbalance": 0.045,
  "collected_at": 1712930401000
}
```

**Derived metrics:**
- Book imbalance: (bid_depth - ask_depth) / (bid_depth + ask_depth)
- Imbalance momentum: change in imbalance over last N snapshots
- Depth withdrawal: sudden drop in depth at specific levels (market maker pulling)
- Spread dynamics: widening spread = uncertainty, narrowing = convergence

**Priority: MEDIUM** — free (already have the data feed), useful for short-term signals, but storage-heavy

---

### Data Collection Phase Summary

| Source | Priority | Cost | Frequency | Historical | Task Class |
|--------|----------|------|-----------|-----------|------------|
| Deribit Options | HIGH | Free | 15 min | Poll + trade backfill | `DeribitOptionsSurfaceTask` |
| Coinglass Liquidations | HIGH | Free | 15 min | 2-3 years | `CoinglassLiquidationTask` |
| Fear & Greed | MEDIUM | Free | Daily | 2018+ (single call) | `FearGreedTask` |
| Glassnode On-Chain | LOW | Free/$999mo | Daily/Hourly | Full | `GlassnodeOnChainTask` |
| Order Book Depth | MEDIUM | Free | 1-5 min | Poll only | `OrderBookSnapshotTask` |

**Phase 1 implementation order:**
1. Deribit Options Surface (start collecting immediately — can't backfill IV snapshots)
2. Coinglass Liquidations (backfill 2-3 years, then continuous)
3. Fear & Greed Index (single backfill call, then daily)
4. Order Book Depth (start polling, no backfill possible)
5. Glassnode (free tier daily, evaluate later)

---

## LAYER 2: Feature Engineering

### 2A. Microstructure Features (from EXISTING 1m candle data)

We have 365 days of 1m candles for 43+ pairs and have extracted ZERO sophisticated features from them. These are all computable from price + volume:

**VPIN (Volume-Synchronized Probability of Informed Trading)**
- The single most published microstructure predictor. Called the 2010 Flash Crash.
- Implementation: classify each 1m bar as buy/sell initiated (tick rule or Lee-Ready), bucket by volume (not time), compute order imbalance per bucket, take rolling CDF
- Output: 0-1 probability that informed traders are active. High VPIN → large move imminent (30-60 min lead time)
- Reference: Easley, Lopez de Prado, O'Hara (2012). "Flow Toxicity and Liquidity in a High-Frequency World"

**Kyle's Lambda (Price Impact)**
- Regression of |price_change| on volume per interval
- High lambda = low liquidity = large moves for small volume
- When lambda spikes, liquidity providers are withdrawing — precedes volatility

**Amihud Illiquidity Ratio**
- |return| / dollar_volume per bar, averaged over rolling window
- Simple but effective regime classifier: high Amihud = illiquid = volatile
- Known predictor of next-day volatility in equities, untested in crypto

**Realized Volatility Signature**
- Compute RV at multiple timescales: 1m, 5m, 15m, 1h, 4h
- The ratio RV(1m)/RV(1h) reveals microstructure noise vs. true vol
- When ratio changes regime, market structure is shifting
- A declining ratio = noise decreasing = directional move forming

**Cross-Asset Lead-Lag Matrix**
- For each pair of assets (BTC, ETH, SOL, ...), compute rolling cross-correlation at 5m lag
- Build a directed lead-lag graph that updates hourly
- When lead-lag structure breaks down (correlation regime change), big move is coming
- BTC leads alts ~70% of the time, but the 30% when it doesn't is highly informative

**Volume Profile Features**
- Point of Control (POC): price level with most volume in last N bars
- Value Area High/Low: 70% of volume concentrated between these levels
- Volume-at-price skew: is volume concentrated above or below current price?

### 2B. Options-Derived Features (from Deribit data)

**25-Delta Risk Reversal (Skew)**
- Interpolate IV surface to find 25-delta put IV and 25-delta call IV
- Skew = 25d_put_IV - 25d_call_IV
- Positive skew = puts expensive = market hedging downside
- Skew z-score (30-day rolling): extreme positive = fear, extreme negative = complacency
- This is arguably the BEST single predictor of directional moves in institutional crypto

**Term Structure Slope**
- ATM IV for front-week vs. front-month vs. 3-month
- Backwardation (front > back) = near-term fear, event risk
- Contango (front < back) = calm, carry-friendly
- Slope change velocity matters more than level

**Gamma Exposure (GEX)**
- For each strike: gamma × OI × contract_size × 100
- Net GEX = sum across all strikes
- Positive GEX: market makers are long gamma → they sell rallies, buy dips → dampening
- Negative GEX: market makers are short gamma → they buy rallies, sell dips → amplifying
- GEX flip point: price level where GEX changes sign → key support/resistance

**Max Pain Evolution**
- Calculate max pain for upcoming expiry
- Track how max pain moves over time relative to spot
- As expiry approaches, spot tends to converge toward max pain (market maker pinning)
- Most useful in 48h before major expiry (monthly/quarterly)

**Put/Call Flow Imbalance**
- Not static ratio — track CHANGE in put OI vs. call OI between snapshots
- Sudden put OI increase = new hedging demand = institutional concern
- Sudden call OI increase on far OTM strikes = speculative positioning

### 2C. Liquidation-Derived Features (from Coinglass)

**Liquidation Cascade Detector**
- Acceleration of liquidation volume: if liq_volume(t) > 2 × liq_volume(t-1) for 3+ consecutive intervals → cascade in progress
- Direction: if long_liq >> short_liq → cascade is long-squeeze (bearish flow)
- Cascade magnitude: total liquidated USD as % of 24h volume

**Liquidation Heat Map**
- From OI distribution + current price + leverage estimates, compute where liquidation clusters sit
- Dense liquidation cluster above/below current price = magnetic target for cascades
- Implementation: assume average 10x-20x leverage, compute liquidation prices for OI at each price level

**Organic OI Change**
- OI_change = new_OI - old_OI
- Liquidation_volume = total_liquidations_in_period
- Organic_new_positions = OI_change + liquidation_volume (liquidations reduce OI, so add back)
- Positive organic change = genuine new interest, negative = position cleanup

### 2D. Multi-Scale Temporal Features

Every feature above should be computed at MULTIPLE timescales simultaneously:
- 1h (tactical)
- 4h (swing)
- 1d (positional)
- 1w (strategic)

The DISAGREEMENT between timescales is itself a feature:
- VPIN high at 1h but low at 4h → short-term anomaly, likely mean-reverts
- VPIN high at ALL scales → genuine regime shift

Feed all timescales into the ML model and let it learn which scale matters when.

### 2E. Regime Features (from BTC price action)

Already built in H2 regime analysis, formalize:
- BTC SMA50/SMA200 cross (trend direction)
- BTC ATR percentile (volatility regime)
- BTC 20-bar return (momentum)
- Regime classification: BULL_TREND, BEAR_TREND, RANGE, SHOCK
- One-hot encode + include raw components as features

---

## LAYER 3: ML Signal Generation

### 3A. Training Data (what we already have)

We have 141,176 backtest trades in MongoDB with labels:
- Entry timestamp, exit timestamp
- Side (BUY/SELL)
- Close type (TP, SL, TRAILING_STOP, TIME_LIMIT)
- net_pnl_quote, net_pnl_pct
- Engine, pair

For each trade entry timestamp, we can reconstruct the full feature vector:
- 1m candle features (price, volume, VPIN, etc.)
- Derivatives features (funding, OI, LS ratio)
- Regime features (BTC state)
- Once collected: options features, liquidation features, sentiment

This gives us a labeled dataset: feature_vector → {profitable: yes/no, pnl: float, close_type: str}

### 3B. Model Architecture: Gradient Boosted Ensemble

**Why XGBoost/LightGBM, not deep learning (first):**
- Tabular data with mixed feature types → GBM consistently beats neural nets
- Interpretable via SHAP (critical for understanding what works)
- Fast to train, fast to iterate
- Handles missing data natively (important when new features have shorter history)
- We have 141K labeled samples — enough for GBM, not enough for large neural nets

**Model 1: Entry Classifier**
- Input: full feature vector at candidate entry time
- Output: P(trade profitable), P(close_type=TP), P(close_type=SL)
- Only enter when P(profitable) > threshold (calibrated on validation set)
- Expected effect: filter out the 25% of trades that hit SL

**Model 2: Direction Predictor**
- Input: full feature vector
- Output: P(price_up_next_4h), P(price_up_next_8h), P(price_up_next_24h)
- Multi-horizon prediction — different features matter at different horizons
- Use as signal: strong agreement across horizons = high conviction

**Model 3: Magnitude Estimator**
- Input: full feature vector
- Output: expected |move| in next 4h/8h/24h
- Large expected move + direction confidence = size up
- Small expected move = skip (not worth the spread/fees)

**Feature importance via SHAP:**
- After training, compute SHAP values for every feature
- This tells us which exotic data sources actually matter
- If options skew has high SHAP importance → invest more in Deribit data quality
- If VPIN has low SHAP importance → drop it, simplify pipeline
- SHAP is the feedback loop that tells us where to invest in data

### 3C. Training Protocol

**Time-series cross-validation (no lookahead):**
```
Fold 1: Train [month 1-6]  → Validate [month 7-8]
Fold 2: Train [month 1-8]  → Validate [month 9-10]  
Fold 3: Train [month 1-10] → Validate [month 11-12]
Final:  Train [month 1-10] → Test [month 11-12] (holdout, touch ONCE)
```

**Hyperparameter search:** Optuna with 100 trials per fold
- Learning rate, max depth, num leaves, min child weight, subsample, colsample
- Optimize for: precision at high-confidence threshold (we want few but good trades)

**Overfitting guards:**
- Train/val metric gap > 0.1 AUC → overfit, reduce complexity
- Feature importance: drop features with near-zero SHAP (reduce dimensionality)
- Trade count in validation must be >= 30 (governance gate)
- Performance must be positive in >= 2 regime windows

### 3D. Online Learning (production)

The model must adapt to evolving market structure:
- Retrain weekly on expanding window (always include all historical data)
- Monitor prediction calibration: does P(profitable)=0.7 actually win 70%?
- Track feature drift: alert if distribution of key features shifts significantly
- If live performance degrades > 30% from backtest → auto-pause, investigate

### 3E. Alternative: Temporal Fusion Transformer (Phase 2)

After GBM establishes baseline, explore TFT:
- Handles multi-horizon prediction natively
- Attention mechanism reveals which time steps matter
- Better at capturing temporal patterns than GBM
- Requires more data (wait until we have 6+ months of exotic features)

Implementation: PyTorch Forecasting library, or custom implementation
- Input: multi-variate time series (all features at 1h resolution)
- Output: predicted return distribution at 4h, 8h, 24h horizons
- Attention weights = interpretability (which features at which lag)

---

## LAYER 4: Execution

### 4A. Replace Static TripleBarrier with Signal-Based Exits

The biggest insight from v1: the exit model kills every strategy equally. All 10+ strategies share the same failure — 25% SL exits at -2.7% avg destroy the edge.

**New exit architecture:**

Instead of fixed SL/TP/TL:
1. **Signal-based exit:** close when the ML model's confidence drops below threshold
   - Model produces P(profitable) every 15 minutes on the open position
   - If P drops below entry_threshold × 0.5 → exit immediately
   - This is dynamic — it adapts to evolving market state

2. **Volatility-adjusted stops:**
   - SL = 2 × ATR(14) at entry (not fixed %)
   - In low-vol regimes: tight stop (small ATR = small SL)
   - In high-vol regimes: wide stop (large ATR = survive noise)
   - This directly addresses the #1 failure mode

3. **Partial take-profit scaling:**
   - At +1 ATR: close 50% of position, move SL to breakeven
   - At +2 ATR: close remaining 50%
   - Eliminates the "winners turn to losers" problem from trailing stops

4. **Time-decay exit:**
   - Expected holding period from model's multi-horizon prediction
   - If trade hasn't hit TP by 1.5x expected duration → exit at market
   - No more fixed 8h time limits regardless of signal quality

### 4B. Implementation in HB Controller

This requires a controller that re-evaluates its position at each candle:

```python
def update_processed_data(self):
    # Compute features on latest data
    features = self._compute_features(self.candles)
    
    # Get ML prediction
    prediction = self.model.predict(features)
    
    # If no position: check entry
    if not self.has_position:
        if prediction.confidence > self.entry_threshold:
            self.signal = prediction.direction  # 1 or -1
            self.entry_confidence = prediction.confidence
            self.expected_duration = prediction.horizon
    
    # If in position: check exit
    else:
        if prediction.confidence < self.exit_threshold:
            self.signal = 0  # exit signal
        elif self.pnl > self.atr_at_entry:
            self.scale_down(0.5)  # partial TP
```

### 4C. Position Sizing from Model Confidence

Kelly criterion with model probability:
```
f* = (p × b - q) / b
where:
  p = model's P(profitable)
  q = 1 - p
  b = expected_win / expected_loss (from model)
```

Half-Kelly for safety: position_size = 0.5 × f* × capital

High confidence (P > 0.7) → larger position
Low confidence (P > 0.55 but < 0.7) → small position
Below threshold → no trade

---

## LAYER 5: Soft Gates (Existing Derivatives Data)

The existing funding rate, OI, and LS ratio data remains useful as FILTER features, not primary signals:

- **Funding rate regime:** input to ML model as feature (not threshold signal)
- **OI trend:** input to ML model (rising OI = conviction, falling = uncertainty)
- **LS ratio:** input to ML model (extreme values = crowd positioning)
- **Cross-exchange funding spread:** input to ML model
- **Funding settlement timing:** 00:00/08:00/16:00 UTC flag as binary feature

These are already collected and merged. No new work needed — just feed them as features alongside the new exotic data.

---

## Execution Plan (phased)

### Phase 0: Quick Win — VPIN + Microstructure (1-2 sessions)

**Goal:** Extract maximum value from data we ALREADY have before collecting anything new.

**Deliverables:**
1. `app/features/microstructure_features.py` — compute VPIN, Kyle's Lambda, Amihud, RV signature from existing 1m parquet data
2. Feature exploration notebook: correlate microstructure features with known trade outcomes from backtest_trades
3. Quick XGBoost prototype: can we predict trade profitability from {microstructure + existing derivatives} features?

**Why first:** zero new data collection, tests the ML approach immediately, tells us if the problem is data or methodology

### Phase 1: Deribit + Coinglass Collection (1-2 sessions)

**Deliverables:**
1. `DeribitOptionsSurfaceTask` — collects full options chain every 15 min, stores in MongoDB
2. `CoinglassLiquidationTask` — collects liquidations + cross-exchange OI every 15 min
3. `FearGreedTask` — daily sentiment index
4. Backfill scripts for Coinglass (2-3 years) and Fear & Greed (2018+)
5. Start Deribit collection (no backfill possible for IV snapshots)
6. Deribit trade data backfill (reconstruct historical IV surfaces from trade-level data downloads)
7. All tasks added to `hermes_pipeline.yml` with proper scheduling
8. MongoDB indexes and TTL policies for new collections

### Phase 2: Feature Engineering Pipeline (1-2 sessions)

**Deliverables:**
1. `app/features/options_features.py` — skew, term structure, GEX, max pain from Deribit snapshots
2. `app/features/liquidation_features.py` — cascade detector, organic OI, heat map from Coinglass
3. `app/features/microstructure_features.py` — VPIN, Lambda, Amihud, RV signature from 1m candles
4. Feature computation integrated into existing `FeatureComputationTask` pipeline
5. Feature store: all computed features stored in MongoDB with timestamps for ML training
6. Feature correlation analysis: which features are redundant, which add unique information

### Phase 3: ML Model Training (2-3 sessions)

**Deliverables:**
1. Training data pipeline: join backtest_trades with feature store by timestamp
2. XGBoost entry classifier: P(profitable) with SHAP analysis
3. XGBoost direction predictor: P(up) at 4h/8h/24h horizons
4. Magnitude estimator: expected |move| per horizon
5. Time-series cross-validation with proper train/val/test splits
6. SHAP feature importance report: which data sources actually matter
7. Calibration analysis: are probabilities well-calibrated?
8. Governance gate: positive expectancy in >= 2 regime windows on test set

### Phase 4: ML Controller + Dynamic Exits (1-2 sessions)

**Deliverables:**
1. `app/controllers/directional_trading/ml_ensemble.py` — HB V2 controller
   - Loads trained model
   - Computes features in real-time from candle feeds + MongoDB queries
   - Signal-based entry from model confidence
   - Dynamic exit (signal re-evaluation, ATR-based stops, partial TP)
2. Register as new engine (e.g., `M1`) in strategy_registry
3. Backtest with BacktestingEngine (same controller code)
4. Walk-forward validation: train on month 1-8, test month 9-10, retrain on 1-10, test 11-12

### Phase 5: Paper Trading + Iteration (ongoing)

**Deliverables:**
1. Deploy M1 to Bybit demo via `python cli.py deploy --engine M1`
2. Monitor execution metrics: slippage, fill rate, signal latency
3. Compare live performance vs. backtest expectations
4. Weekly model retraining with new data
5. Feature importance evolution tracking (which features gain/lose importance over time)
6. Iterate: add Glassnode if on-chain features show promise in SHAP

---

## MongoDB Collections (new)

| Collection | Purpose | TTL | Index |
|------------|---------|-----|-------|
| `deribit_options_surface` | Raw options snapshots | 180 days | (currency, timestamp_utc, expiry, strike, type) unique |
| `deribit_options_derived` | Computed skew, GEX, max pain | 180 days | (currency, timestamp_utc) unique |
| `coinglass_liquidations` | Liquidation history | 365 days | (pair, timestamp_utc) unique |
| `coinglass_oi` | Cross-exchange OI OHLC | 365 days | (pair, timestamp_utc) unique |
| `fear_greed_index` | Daily sentiment | none | (timestamp_utc) unique |
| `order_book_snapshots` | L2 book snapshots | 90 days | (pair, timestamp_utc) unique |
| `glassnode_onchain` | On-chain metrics | none | (metric, asset, timestamp_utc) unique |
| `ml_features` | Computed feature vectors | 365 days | (pair, timestamp_utc) unique |
| `ml_predictions` | Model outputs for audit | 365 days | (pair, timestamp_utc, model_version) |
| `ml_model_registry` | Trained model metadata | none | (model_name, version) |

---

## Key Design Decisions

### Why GBM before deep learning
- Tabular mixed-type features → GBM is SOTA (see Grinsztajn et al. 2022)
- SHAP interpretability is critical for understanding feature value
- Fast iteration: train in minutes, not hours
- 141K samples is borderline for DL, plenty for GBM
- DL (TFT) comes in Phase 5+ once we have 6+ months of exotic features

### Why not pure RL for trading
- Sample efficiency: RL needs millions of episodes, we have 141K trades
- Reward shaping is hard: sparse rewards (trade PnL) make RL unstable
- GBM + heuristic exits is a better starting point
- RL for exit policy specifically (not full trading) is Phase 5+ exploration

### Why options data is the highest priority exotic source
- Free (no API cost)
- Highest information density per byte (IV surface encodes market expectations)
- Institutional-grade signal (options market is dominated by sophisticated participants)
- BTC+ETH are our most liquid pairs
- 25-delta skew is arguably the strongest known directional predictor in crypto

### Why microstructure features from existing data come first
- Zero collection cost — data already exists
- Tests the ML approach immediately
- If VPIN/Lambda don't predict in our dataset, the problem is deeper than data quality
- Quick validation of the entire v2 thesis before investing in exotic collection

### Model serving in production
- Train offline (weekly cron or manual trigger)
- Export to ONNX or pickle
- Controller loads model at startup, runs inference on each candle update
- No external model server needed — single-process, low-latency
- Model file stored in `app/models/` directory (gitignored), metadata in MongoDB

---

## Risk Register

| Risk | Mitigation |
|------|------------|
| Deribit API changes/breaks | Abstract behind adapter, store raw + derived separately |
| Coinglass rate limit or paywall | Cache aggressively, compute features from stored data |
| Overfitting ML model to historical regimes | Time-series CV, regime-stratified validation, SHAP monitoring |
| Feature drift in production | Weekly calibration check, auto-pause on degradation |
| Model latency too high for real-time | Pre-compute features, batch inference, ONNX optimization |
| New data doesn't improve predictions | SHAP tells us this early; fall back to microstructure-only model |
| Options data only covers BTC+ETH | Use as portfolio-level features (BTC options → signal for all alts) |
| Backtest overestimates live performance | Governance gates: >= 20 paper trades before live, slippage < 15 bps |

---

## Success Criteria

### Phase 0 (microstructure exploration)
- [ ] VPIN computed for all 43 pairs, 365 days
- [ ] XGBoost on existing features: AUC > 0.55 on held-out test set (above random)
- [ ] SHAP report identifies at least 3 features with meaningful importance

### Phase 3 (ML model)
- [ ] Entry classifier: precision > 60% at recall > 30% (we want accurate signals, okay to miss some)
- [ ] Direction predictor: AUC > 0.58 at 4h horizon
- [ ] Positive expectancy in >= 2 independent regime windows (governance gate)
- [ ] Trade count >= 30 in each validation fold

### Phase 4 (controller backtest)
- [ ] PF > 1.3 on test period (governance ALLOW threshold)
- [ ] Sharpe > 1.0
- [ ] Max DD < -15%
- [ ] SL exit rate < 15% (down from 25% with static TripleBarrier)

### Phase 5 (paper trading)
- [ ] >= 20 resolved signals
- [ ] Avg slippage < 15 bps
- [ ] Edge after slippage positive
- [ ] Live win rate within 30% of backtest (governance gate)
