# HL MM V3 — Wallet-Intelligent Directional Passive Strategy

## Executive Summary

Pivot from symmetric market making (V2: "fade the flow") to wallet-aware directional passive entry (V3: "ride the flow"). The core insight from 2 days of live trading: on HL shitcoins, the counterparty who crosses the spread IS the information. Don't fade them — ride them.

**Target:** $500 MRR from HL shitcoin perps with $50-100 capital.

**Edge source:** Wallet transparency (HL exposes buyer/seller addresses on every trade) + regime gating (only quote when spread is wide enough to absorb adverse markout) + directional bias (quote WITH the detected flow, not against it).

---

## 1. Why V2 Failed (Evidence)

| Metric | Backtest | Live | Gap |
|--------|----------|------|-----|
| Fill rate | Every quotable second | ~10-30/hour | 10-50x lower |
| Adverse selection | 50% (symmetric) | 60% (informed flow) | Breakeven at 53% |
| Exit quality | 100% maker | 30% maker, 70% taker | -3.5bps per taker exit |
| Win rate | ~53% | 40% | Fatal |

**Root cause:** Symmetric two-sided quoting in a market dominated by informed directional flow. We were exit liquidity for traders who knew the next move.

---

## 2. V3 Architecture: Four Layers

```
Layer 4: Execution (passive entry, smart exit)
Layer 3: Direction (which side to quote, informed by wallet + momentum)
Layer 2: Regime Gate (WHEN to quote: wide spread + low toxicity)
Layer 1: Wallet Intelligence (WHO is trading, what does it mean)
```

### Layer 1: Wallet Intelligence Engine

The deepest layer. HL's unique edge: every trade exposes `[buyer_address, seller_address]`. No CEX gives this. We build a real-time wallet classification system.

#### 1.1 Wallet Taxonomy

| Class | Behavior | Signal | Our Action |
|-------|----------|--------|------------|
| **Momentum Whale** | Large single-direction bursts, 50K+ notional. Holds for hours. | STRONG directional. Follow within 2s. | Cancel adverse side. Place with them. |
| **TWAP Bot** | Periodic same-direction trades (every 30-60s, same size). | STRONG continuation for 5-30min. | Detect pattern after 3 ticks, ride full duration. |
| **Arb Bot** | Small, fast, both directions. Trades correlated with Bybit/Binance moves. | NEUTRAL. Not informative for direction. | Ignore — these are noise fills we WANT. |
| **Retail Noise** | Small ($10-50), irregular timing, no persistence. | NEUTRAL/FADE. Often wrong. | These are ideal counterparties for MM. |
| **Liquidation Bot** | Forced close of underwater positions. Predictable size from position data. | MEAN-REVERTING. Price overshoots then reverts. | Fade the liquidation — quote opposite side aggressively. |
| **Market Maker** | Two-sided, frequent cancel/replace, tight spread. | NEUTRAL. Competing, not informative. | Avoid their coins (spread too tight for us). |

#### 1.2 Wallet Classification Algorithm

```python
class WalletClassifier:
    """Real-time wallet classification from trade stream."""
    
    def classify(self, wallet: str, coin: str) -> WalletClass:
        """Classify based on accumulated behavior features."""
        features = self.get_features(wallet, coin)
        
        # Rule-based classification (fast, interpretable)
        if features.burst_score > 0.8 and features.avg_notional > 5000:
            return MOMENTUM_WHALE
        if features.periodicity_score > 0.7 and features.direction_persistence > 0.9:
            return TWAP_BOT
        if features.both_sides_ratio > 0.4 and features.avg_hold_time < 10:
            return ARB_BOT
        if features.avg_notional < 50 and features.trade_frequency < 0.1:
            return RETAIL_NOISE
        if features.forced_close_signature:
            return LIQUIDATION_BOT
        if features.cancel_rate > 0.8 and features.two_sided_ratio > 0.4:
            return MARKET_MAKER
        return UNKNOWN
    
    def get_features(self, wallet: str, coin: str) -> WalletFeatures:
        """Compute features from recent trade history."""
        return WalletFeatures(
            # Directional persistence: what % of trades in last 5min are same direction?
            direction_persistence=...,
            # Burst score: how concentrated are trades in time? (high = whale dump/pump)
            burst_score=...,
            # Average notional per trade
            avg_notional=...,
            # Periodicity: is there a regular cadence? (TWAP detection)
            periodicity_score=...,
            # Both sides ratio: does this wallet trade both directions? (arb/MM)
            both_sides_ratio=...,
            # Post-trade markout: historical 5s/15s/60s markout after this wallet aggresses
            markout_5s=...,
            markout_15s=...,
            # Trade frequency: trades per minute on this coin
            trade_frequency=...,
            # Size relative to historical average (conviction signal)
            size_z_score=...,
        )
```

#### 1.3 Metaorder Detection (TWAP/Iceberg)

The highest-value signal. A wallet executing a large order over minutes will:
- Trade same direction every 30-60s
- Use similar size each clip
- Continue for 5-30 minutes
- Move price 20-100bps cumulatively

Detection algorithm:
```python
def detect_metaorder(self, wallet: str, coin: str) -> Optional[MetaorderSignal]:
    """Detect TWAP/iceberg execution in real-time."""
    recent = self.get_recent_trades(wallet, coin, lookback_s=300)
    if len(recent) < 3:
        return None
    
    # Check: all same direction?
    directions = [t.direction for t in recent]
    if len(set(directions)) > 1:
        return None  # mixed directions, not a metaorder
    
    # Check: regular cadence?
    intervals = [recent[i+1].ts - recent[i].ts for i in range(len(recent)-1)]
    if len(intervals) >= 2:
        cv = np.std(intervals) / np.mean(intervals)  # coefficient of variation
        if cv < 0.5:  # regular cadence (CV < 0.5 = periodic)
            return MetaorderSignal(
                wallet=wallet,
                coin=coin,
                direction=directions[0],
                avg_interval_s=np.mean(intervals),
                avg_size=np.mean([t.size for t in recent]),
                confidence=min(1.0, len(recent) / 5),  # higher with more clips
                estimated_remaining_s=np.mean(intervals) * 3,  # expect 3 more clips
            )
    return None
```

#### 1.4 Real-Time Toxicity Scoring (Enhanced)

Current V2 implementation: simple EWMA markout per wallet (needs 20+ trades).
V3 enhancement: incorporate ALL features for faster, more accurate scoring.

```python
def compute_toxicity_score(self, wallet: str, coin: str) -> float:
    """0.0 = pure noise, 1.0 = maximally toxic (informed).
    
    Uses Bayesian updating: start with class prior, update with observed markout.
    """
    wallet_class = self.classify(wallet, coin)
    
    # Class priors (from empirical analysis)
    class_priors = {
        MOMENTUM_WHALE: 0.85,   # almost always toxic
        TWAP_BOT: 0.90,         # executing a large order = very informed
        ARB_BOT: 0.20,          # moves are mechanical, not predictive
        RETAIL_NOISE: 0.30,     # slightly adverse (retail often follows momentum)
        LIQUIDATION_BOT: 0.10,  # forced close = mean-reverting = ANTI-toxic
        MARKET_MAKER: 0.15,     # neutral, just providing liquidity
        UNKNOWN: 0.50,          # prior for unclassified wallets
    }
    
    prior = class_priors[wallet_class]
    
    # Update with observed markout if available
    features = self.get_features(wallet, coin)
    if features.markout_5s is not None and features.trade_count >= 3:
        # Bayesian update: combine prior with observed adverse markout
        observed_toxicity = max(0, min(1, -features.markout_5s / 10.0))
        # Weight by sample size (more trades = more trust in observation)
        weight = min(0.8, features.trade_count / 20.0)
        return prior * (1 - weight) + observed_toxicity * weight
    
    return prior
```

### Layer 2: Regime Gate

**Principle:** Default state is FLAT. Only activate when conditions favor passive harvesting.

#### 2.1 Quotability Conditions (ALL must be true)

```python
def is_quotable(self, coin: str) -> bool:
    """Should we quote this coin RIGHT NOW?"""
    signal = self.signal_engine.get_signal(coin)
    
    # 1. Wide spread (our data: >8bps = positive markout, <6bps = negative)
    if signal.book.spread_bps < 8.0:
        return False
    
    # 2. No toxic flow detected in last 10s
    toxic_active, toxic_count = self.wallet_scorer.is_toxic_active(coin, lookback_s=10)
    if toxic_active and toxic_count >= 2:
        return False  # multiple toxic wallets active, stay flat
    
    # 3. VPIN below threshold (low informed flow probability)
    if signal.vpin > 0.6:
        return False
    
    # 4. Not in a liquidation cascade (spread spike + depth drop)
    if signal.spread_spike_detected and signal.depth_drop_detected:
        return False  # chaos — wait for it to settle
    
    # 5. Data is fresh
    if signal.is_stale:
        return False
    
    return True
```

#### 2.2 Regime Classification

```python
class RegimeState(Enum):
    FLAT = "flat"           # default, no quoting
    MEAN_REVERTING = "mr"   # both sides quotable (rare on shitcoins)
    TRENDING_UP = "up"      # bid-only (ride the uptrend via passive buys)
    TRENDING_DOWN = "down"  # ask-only (ride the downtrend via passive sells)
    LIQUIDATION = "liq"     # fade the forced close (counter-trend)

def detect_regime(self, coin: str) -> RegimeState:
    """Determine current regime from coin-level signals."""
    signal = self.signal_engine.get_signal(coin)
    
    # Coin's own 5-min momentum
    mom_5m = signal.mid_momentum_5m
    
    # Wallet flow direction (net signed notional from classified wallets)
    whale_flow = self.wallet_intelligence.get_whale_net_flow(coin, lookback_s=60)
    
    # Liquidation detection
    liq_detected = self.wallet_intelligence.is_liquidation_active(coin)
    
    if liq_detected:
        return RegimeState.LIQUIDATION
    
    # Trend detection: combine momentum + wallet flow
    trend_score = 0.0
    if abs(mom_5m) > 5.0:
        trend_score += np.sign(mom_5m) * min(1.0, abs(mom_5m) / 15.0)
    if abs(whale_flow) > 1000:  # $1K+ net whale flow in 60s
        trend_score += np.sign(whale_flow) * 0.5
    
    if trend_score > 0.5:
        return RegimeState.TRENDING_UP
    elif trend_score < -0.5:
        return RegimeState.TRENDING_DOWN
    elif abs(trend_score) < 0.2 and signal.book.spread_bps > 10.0:
        return RegimeState.MEAN_REVERTING  # no trend + wide spread = classic MM
    
    return RegimeState.FLAT  # uncertain, don't quote
```

### Layer 3: Direction (Which Side to Quote)

Based on regime + wallet intelligence, decide which side to quote.

```python
def get_quoting_decision(self, coin: str) -> QuotingDecision:
    """Determine which sides to quote and with what bias."""
    regime = self.detect_regime(coin)
    
    if regime == RegimeState.FLAT:
        return QuotingDecision(quote_bid=False, quote_ask=False)
    
    if regime == RegimeState.MEAN_REVERTING:
        # Classic MM: both sides, symmetric
        return QuotingDecision(quote_bid=True, quote_ask=True, bias=0.0)
    
    if regime == RegimeState.TRENDING_UP:
        # Ride uptrend: passive BID only (buy dips in an uptrend)
        # We get filled when price temporarily dips to our bid
        # Then ride the continuation up
        return QuotingDecision(quote_bid=True, quote_ask=False, bias=+1.0)
    
    if regime == RegimeState.TRENDING_DOWN:
        # Ride downtrend: passive ASK only (sell rallies in a downtrend)
        return QuotingDecision(quote_bid=False, quote_ask=True, bias=-1.0)
    
    if regime == RegimeState.LIQUIDATION:
        # Fade the liquidation: quote OPPOSITE side
        # Liquidations push price too far, then it reverts
        liq_side = self.wallet_intelligence.get_liquidation_side(coin)
        if liq_side == "sell":  # forced selling → price too low → bid
            return QuotingDecision(quote_bid=True, quote_ask=False, bias=+1.5)
        else:  # forced buying → price too high → ask
            return QuotingDecision(quote_bid=False, quote_ask=True, bias=-1.5)
```

### Layer 4: Execution

#### 4.1 Entry: Passive with Conviction Sizing

```python
def compute_entry_quote(self, coin: str, decision: QuotingDecision) -> Optional[Quote]:
    """Compute entry quote with conviction-based sizing."""
    signal = self.signal_engine.get_signal(coin)
    book = signal.book
    
    # Base size: $25 notional (conservative for $50 capital)
    base_size_usd = 25.0
    
    # Conviction scaling from wallet intelligence
    # Higher conviction = larger position (up to 2x base)
    metaorder = self.wallet_intelligence.get_active_metaorder(coin)
    if metaorder and metaorder.confidence > 0.7:
        size_mult = 1.0 + metaorder.confidence  # 1.7-2.0x
    else:
        size_mult = 1.0
    
    size_usd = base_size_usd * size_mult
    
    # Price: inside the spread but with enough edge to cover fee
    # Reservation price from Avellaneda-Stoikov inventory adjustment
    reservation = self.compute_reservation_price(coin, decision.bias)
    
    if decision.quote_bid:
        # Bid price: reservation - fee buffer, capped at best_bid + 1 tick
        bid_px = min(reservation, book.best_bid + self.tick_size(coin))
        return Quote(side="bid", price=bid_px, size=size_usd / bid_px)
    
    if decision.quote_ask:
        ask_px = max(reservation, book.best_ask - self.tick_size(coin))
        return Quote(side="ask", price=ask_px, size=size_usd / ask_px)
```

#### 4.2 Exit: Smart, Not Fixed-Timer

The V2 failure mode: 120s timer → taker close. V3 uses intelligent exit triggers.

```python
class ExitStrategy:
    """Determine how and when to exit an open position."""
    
    def should_exit(self, coin: str, position: Position) -> ExitDecision:
        """Multi-signal exit decision (runs every tick)."""
        signal = self.signal_engine.get_signal(coin)
        
        # 1. PROFIT TARGET: if we've captured > half the spread, exit maker
        unrealized_bps = position.unrealized_pnl_bps()
        if unrealized_bps > signal.book.spread_bps * 0.4:
            return ExitDecision(action="maker_close", urgency=0.5)
        
        # 2. REGIME FLIP: if the trend that got us in has reversed
        regime = self.detect_regime(coin)
        if position.is_long and regime == RegimeState.TRENDING_DOWN:
            return ExitDecision(action="maker_close_aggressive", urgency=0.8)
        if position.is_short and regime == RegimeState.TRENDING_UP:
            return ExitDecision(action="maker_close_aggressive", urgency=0.8)
        
        # 3. TOXIC WALLET APPEARED on our side
        toxic_active, _ = self.wallet_scorer.is_toxic_active(coin)
        if toxic_active:
            toxic_side = self.wallet_intelligence.get_toxic_aggressor_side(coin)
            if (position.is_long and toxic_side == "sell") or \
               (position.is_short and toxic_side == "buy"):
                # Toxic flow going AGAINST our position → exit NOW
                return ExitDecision(action="taker_close", urgency=1.0)
        
        # 4. METAORDER ENDED: the flow that got us in has stopped
        metaorder = self.wallet_intelligence.get_active_metaorder(coin)
        if position.entry_had_metaorder and not metaorder:
            return ExitDecision(action="maker_close", urgency=0.6)
        
        # 5. TIME DECAY: position getting stale (but with much longer patience)
        age_s = position.age_seconds()
        if age_s > 300:  # 5 min (was 120s in V2)
            return ExitDecision(action="maker_close_aggressive", urgency=0.7)
        if age_s > 600:  # 10 min absolute max
            return ExitDecision(action="taker_close", urgency=1.0)
        
        # 6. ADVERSE MOVE: price moved significantly against us
        if position.adverse_move_bps > 30:  # 30bps (was 20 in V2)
            return ExitDecision(action="taker_close", urgency=1.0)
        
        return ExitDecision(action="hold", urgency=0.0)
```

#### 4.3 Fodra-Labadie Directional Extension

The standard Avellaneda-Stoikov model assumes the mid-price is a martingale (no drift). Fodra-Labadie (2012) extends this with a directional alpha term:

```
reservation_price = mid + alpha * tau - q * gamma * sigma^2 * tau
```

Where:
- `alpha` = expected drift per unit time (our directional signal)
- `tau` = time horizon
- `q` = inventory (signed)
- `gamma` = risk aversion
- `sigma` = volatility

In V3, `alpha` comes from the wallet intelligence:

```python
def compute_alpha(self, coin: str) -> float:
    """Compute expected short-term drift from wallet signals.
    
    Returns bps/second expected drift.
    Positive = expecting price to go up.
    """
    # Component 1: Active metaorder direction
    metaorder = self.wallet_intelligence.get_active_metaorder(coin)
    alpha_metaorder = 0.0
    if metaorder:
        # Metaorder historically moves price by ~20bps over its duration
        expected_total_move = 20.0  # bps
        remaining_s = metaorder.estimated_remaining_s
        if remaining_s > 0:
            alpha_metaorder = expected_total_move / remaining_s * metaorder.direction
    
    # Component 2: Net whale flow (last 60s)
    whale_flow = self.wallet_intelligence.get_whale_net_flow(coin, lookback_s=60)
    mid_px = self.signal_engine.get_signal(coin).book.mid
    flow_bps_equiv = whale_flow / (mid_px * 1000) * 10000  # normalize
    alpha_flow = flow_bps_equiv * 0.01  # empirical scaling
    
    # Component 3: Momentum (weaker signal, confirmation only)
    mom = self.signal_engine.get_signal(coin).mid_momentum_5m
    alpha_momentum = mom / 300.0  # 5min momentum spread over 300s
    
    # Combine with confidence weighting
    alpha = alpha_metaorder * 0.5 + alpha_flow * 0.3 + alpha_momentum * 0.2
    
    return alpha
```

The reservation price then incorporates the alpha:
```python
def compute_reservation_price(self, coin: str, bias: float = 0.0) -> float:
    """Fodra-Labadie directional reservation price."""
    signal = self.signal_engine.get_signal(coin)
    fv = signal.book.mid  # fair value
    sigma = signal.sigma_1s
    tau = 8.0  # time horizon (seconds)
    
    # Inventory skew (Avellaneda-Stoikov)
    pos = self.inventory.get_position(coin)
    q_norm = pos.size * fv / self.get_limits(coin).q_soft
    gamma = self.calibrate_gamma(coin, sigma, tau)
    
    # Directional alpha (Fodra-Labadie)
    alpha = self.compute_alpha(coin)
    
    # Combined reservation
    reservation = fv + alpha * tau - q_norm * gamma * sigma**2 * tau
    
    # Bias from regime (additional skew for trend-following)
    reservation += bias * sigma * tau * 10000  # bias in volatility units
    
    return reservation
```

---

## 3. Data Requirements

| Data Source | Purpose | Current State |
|-------------|---------|---------------|
| HL WS trades (with `users` field) | Wallet classification, metaorder detection | Available, partially used |
| HL WS L2 book | Spread, depth, microprice, imbalance | Available, fully used |
| HL REST user_state (per wallet) | Wallet position sizes, leverage | Available but rate-limited |
| MongoDB wallet_scores | Historical toxicity per wallet | Exists, needs enhancement |
| MongoDB metaorder_signals | Detected TWAP patterns | NEW — needs collection |
| Bybit WS (BTC/ETH) | Anchor price + BTC regime | Available, fully used |

---

## 4. Key Parameters

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Min spread to quote | 8.0 bps | Data: <8bps = negative markout |
| Max position notional | $50 (100% of capital) | Conservative for $50 account |
| Base order size | $25 | 50% of capital per position |
| Max conviction multiplier | 2.0x | $50 max position |
| Metaorder detection: min clips | 3 | Need 3 periodic trades to confirm |
| Metaorder detection: max CV | 0.5 | Coefficient of variation for periodicity |
| Whale threshold | $5,000 notional | Defines "large" trade |
| Toxic lookback | 10s | Cancel if toxic in last 10s |
| Regime momentum threshold | 5 bps/5min | Below = flat, above = trending |
| Exit: profit target | 40% of spread | Take profit early |
| Exit: time decay start | 300s (5min) | Start aggressive exit |
| Exit: absolute max hold | 600s (10min) | Taker close hard stop |
| Exit: adverse limit | 30 bps | Cut losses |
| VPIN threshold | 0.6 | Above = don't quote |

---

## 5. Expected Performance

### Conservative estimate (based on V2 live data + wallet filtering):

- Quotable minutes per day: ~120 (20% of time spread > 8bps)
- Fill rate during quotable time: ~20/hour (from live observation)
- Fills per day: ~40
- Average edge per fill (with wallet filtering): +2-4 bps
- Average notional per fill: $25
- **Expected daily PnL: $0.20 - $0.40/day = $6-12/month**

### Optimistic estimate (good regime days):

- Quotable minutes: 240 (40% wide spread)
- Fill rate: 30/hour
- Fills per day: 120
- Edge per fill: +4-6 bps (riding metaorders)
- **Expected daily PnL: $1.20 - $1.80/day = $36-54/month**

### Key improvement over V2:
- Fewer fills (quality over quantity)
- Higher win rate (>55% target, from wallet filtering)
- Smaller taker exit rate (<20%, from smart exit signals)
- Directional bias captures trend continuation (not just spread)

---

## 6. Implementation Plan

### Phase 1: Enhanced Wallet Intelligence (3-4 days)
- Wallet feature computation (direction persistence, burst, periodicity, size z-score)
- Rule-based classification (6 classes)
- Metaorder detection algorithm
- MongoDB collection for wallet profiles + metaorder signals
- Unit tests for classification accuracy

### Phase 2: Regime Gate + Direction (2-3 days)
- Regime classifier (FLAT, MR, UP, DOWN, LIQ)
- One-sided quoting logic integrated into state machine
- Min spread 8bps hard gate
- Fodra-Labadie alpha computation
- Backtest against 7 days of L2 data with regime filtering

### Phase 3: Smart Exit (2 days)
- Multi-signal exit strategy (profit target, regime flip, toxic detection, metaorder end)
- Longer patience (300s/600s vs 120s)
- Conviction-based exit urgency
- Backtest exit quality (% maker vs taker under new rules)

### Phase 4: Live Testing (ongoing)
- Paper mode first (log decisions, don't execute)
- Gradual activation: 1 coin, wide spread only
- 200+ fills before declaring success
- Target: >55% WR, positive Sharpe, <20% taker exit rate

---

## 7. Risk Controls

| Risk | Mitigation |
|------|------------|
| Wallet intelligence wrong (false confidence) | Bayesian: start with class prior, only override with 3+ observed trades |
| Metaorder ends abruptly | Exit trigger on metaorder disappearance |
| Regime misclassification | Default = FLAT (don't quote when uncertain) |
| Flash crash / cascade | VPIN gate + depth drop detection + absolute adverse limit |
| Rate limit exhaustion | Pipeline HL tasks disabled, REST-verified ghost prevention |
| Capital loss | Hard stop at -$5/day, max position $50 |

---

## 8. What Makes This Different From V2

| Aspect | V2 | V3 |
|--------|----|----|
| Default state | Quoting both sides | FLAT (no quoting) |
| Side selection | Symmetric (both) | One-sided (with trend) |
| Fill quality gate | None (quote always) | Spread > 8bps + low toxicity |
| Wallet usage | Post-hoc markout EWMA | Real-time classification + metaorder detection |
| Exit logic | Fixed 120s timer | Multi-signal (profit, regime flip, toxic detect) |
| Directional model | None (martingale assumed) | Fodra-Labadie alpha from wallet flow |
| Adverse selection defense | Kill after 8 fills | Prevent at entry (don't quote toxic regimes) |
| Core question | "Is the spread wide?" | "Is the next fill toxic?" |

---

## 9. Open Questions for Review

1. Is 3 clips enough to confirm a metaorder, or do we need 5? (False positive cost vs detection latency)
2. Should we attempt to front-run detected metaorders (aggressive) or just ride them (passive)?
3. Liquidation fading: is our latency (500ms) fast enough to catch the reversion, or will other bots eat it?
4. With $50 capital and one position at a time, is the opportunity cost of being flat 80% of the time acceptable?
5. Should we run on more coins (10-15) to increase quotable minutes, or focus on 2-3 where we have the most wallet data?
