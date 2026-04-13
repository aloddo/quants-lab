# Next Session Plan — Strategy Improvement v3

## Current State (updated 2026-04-12 23:30)

### E3 Funding Carry: BTC Regime Filter Validated

Full 35-pair bulk backtest with BTC regime filter (threshold=0.0) — **17,967 trades, 365d, 1m resolution.**

**3 ALLOW pairs (PF >= 1.3 + Sharpe >= 1.0):**

| Pair | PF | Sharpe | WR% | Trades | Prod DD | Short PF |
|------|-----|--------|-----|--------|---------|----------|
| **XRP** | **1.36** | 1.22 | 67.1% | 296 | -0.15% | 1.46 |
| **ADA** | **1.35** | 1.93 | 64.9% | 390 | -0.19% | 1.41 |
| **SUI** | **1.32** | 2.05 | 72.0% | 450 | -0.25% | 1.32 |

**Near-ALLOW:**

| Pair | PF | Sharpe | WR% | Trades | Notes |
|------|-----|--------|-----|--------|-------|
| DOGE | 1.33 | 0.57 | 61.0% | 387 | Sharpe < 1.0 (sizing artifact) |
| SOL | 1.27 | 2.17 | 65.5% | 323 | PF just below 1.3 |
| ETH | 1.26 | 0.12 | 66.0% | 237 | Low Sharpe |

### Deep Analysis Insights
- **SHORT-side alpha dominates** (LONG carry is break-even for most pairs)
- **Monthly consistency**: 10-11/13 months profitable
- **Cross-pair correlation**: avg 0.28 (good diversification)
- **Production DD**: 0.15-0.25% (backtest DD inflated 100-300x by $100 account)
- **DD gate fix**: `dd_gate_relaxed` added to strategy registry

### Walk-Forward Validation: RUNNING
- 35 pairs, 2 folds (train=270d, test=90d, step=30d)
- Derivatives merge added to WalkForwardBacktestTask
- Expected completion: ~3-4 hours
- Log: `/tmp/e3_walk_forward.log`

### M1 ML Ensemble: DEAD (Global Model)
| Run | ALLOW | WATCH | BLOCK | Trades | Notes |
|-----|-------|-------|-------|--------|-------|
| v1 | 0 | 1 (FARTCOIN) | 34 | 6,224 | 5/12 features NaN |
| v2 | 0 | 1 (XRP 1.09) | 34 | 6,503 | All features present |

### Data Collection Status
- **Deribit options**: Live (1,636 docs, collecting every 15min)
- **Coinglass**: Blocked — needs `COINGLASS_API_KEY` in `.env`
- **Fear & Greed**: 2,989 docs (2018-2026) ✓
- **Derivatives**: funding/OI/LS ratio all flowing ✓

---

## Execution Order for Next Session

### Step 1: Check Walk-Forward Results
```bash
tail -50 /tmp/e3_walk_forward.log
# Or query MongoDB:
# db.walk_forward_results.find({engine: "E3"}).sort({created_at: -1})
```

If walk-forward passes for XRP/ADA/SUI (test PF > 1.3, no overfitting flag):

### Step 2: Deploy to Paper Trading
```bash
python cli.py deploy --engine E3 --pair XRP-USDT
python cli.py deploy --engine E3 --pair ADA-USDT
python cli.py deploy --engine E3 --pair SUI-USDT
python cli.py bot-status --engine E3
```

### Step 3: Optimize Further
- **SHORT-only mode**: ADA SHORT PF=1.41, DOGE SHORT PF=1.54 — test per-pair direction override
- **DOGE Sharpe fix**: Consider relaxing Sharpe gate to 0.5 for carry strategies (Sharpe distorted by $100 account)
- **TP/SL tuning**: Current 3%/5%. Winners resolve in 9-10h, losers in 18-19h. Test 2.5%/4% (tighter) or trailing stop

### Step 4: Deribit Features → E3
With options data flowing, compute and test as E3 filters:
- Implied vol skew (put/call ratio per expiry)
- Term structure (front-month vs back-month IV)
- Put/call OI ratio

### Step 5: Add Coinglass API Key
```bash
echo "COINGLASS_API_KEY=your_key_here" >> .env
python scripts/backfill_coinglass.py
```

---

## Success Criteria

### Completed:
- [x] M1 v2 backtest (FAILED — 0 ALLOW)
- [x] Deribit collection live
- [x] E3 OI filter A/B tested (6/8 pairs improved)
- [x] E3 BTC regime filter A/B tested (+0.15-0.25 PF on all 3 target pairs)
- [x] E3 full bulk backtest with BTC regime (35 pairs, 3 ALLOW)
- [x] DD gate fix (dd_gate_relaxed flag)
- [x] Walk-forward derivatives merge bug fix
- [x] Deep analysis: trade-level, monthly, cross-pair correlation, production DD

### In progress:
- [ ] E3 walk-forward validation (running)

### Next session:
- [ ] Walk-forward results analyzed
- [ ] E3 deployed to paper trading on XRP/ADA/SUI
- [ ] First 24-48h paper trading results monitored
- [ ] Deribit-derived features computed and tested as E3 filters
- [ ] Coinglass backfill complete
