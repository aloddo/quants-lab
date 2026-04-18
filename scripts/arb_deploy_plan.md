# Arb Deployment Plan

## Weekend (Paper Trading)
Engine running in tmux `arb-paper`. Optimal params: entry=35bp, exit=5bp, hold=24h.

## Monday Morning

### Step 1: Check paper results
```bash
python scripts/arb_paper_report.py
```
All 6 gates must pass. If not, investigate and fix.

### Step 2: Liquidate alts (keep BTC initially)
```bash
# Dry run first
python scripts/arb_liquidate_alts.py --dry-run --keep BTC

# Execute
python scripts/arb_liquidate_alts.py --execute --keep BTC
```
Expected: ~$720 USDT freed from alts.

### Step 3: Start live (conservative)
```bash
# Stop paper session
tmux kill-session -t arb-paper

# Start live — small size, fewer positions
python -m app.services.arb_engine --live \
  --position-size 100 \
  --max-concurrent 5 \
  --max-exposure 750
```

### Step 4: Monitor first 2 hours
- Watch for: fill failures, orphan legs, unexpected fees
- Check: `python scripts/arb_paper_report.py` (works for live too)

### Step 5: Scale up (if stable)
```bash
# After 2h clean operation, increase size
python -m app.services.arb_engine --live \
  --position-size 150 \
  --max-concurrent 8 \
  --max-exposure 1500
```

### Step 6: Deploy BTC capital (Day 2+)
```bash
python scripts/arb_liquidate_alts.py --execute  # sells BTC too
```
Full capital: ~$4.5K USDT on Binance + $395 on Bybit = ~$5K total.

## Capital Allocation

| Phase | Binance USDT | Bybit USDT | Position Size | Max Concurrent | Max Exposure |
|-------|-------------|------------|--------------|----------------|-------------|
| Paper (now) | N/A | N/A | $200 | 10 | $2000 |
| Live Day 1 | ~$720 | $395 | $100 | 5 | $750 |
| Live Day 2+ | ~$4,500 | $395 | $200 | 10 | $2,000 |
| Scaled | $4,500 | $1,000* | $300 | 15 | $4,500 |

*Transfer USDT from Binance to Bybit if margin gets tight.

## Risk Limits (hard stops)

- Max loss per trade: $2.37 (stop at 2.5x entry + fees)
- Max portfolio loss: $12 (5 concurrent stops)
- Max daily loss: $25 → pause engine, investigate
- Exchange downtime: engine auto-pauses if API errors > 5 consecutive
