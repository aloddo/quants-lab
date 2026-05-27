# HL Copy Trader — Codex Adversarial Review Brief

## What We're Building

A copy trader on Hyperliquid (HL) perpetual futures that follows profitable whale wallets detected via the public WebSocket trade feed. We detect their directional entries (TWAPs), enter the same direction, and exit when they exit.

## Architecture

**Single script:** `scripts/hl_copy_trader.py` (789 lines)

**Flow:**
1. Subscribe to HL WebSocket trade feed for target coins
2. Filter trades by target wallet addresses (buyer/seller fields)
3. Aggregate fills into TWAP windows (60s) — net buys vs sells
4. When TWAP exceeds $500 min notional + 60% directionality → entry signal
5. Enter via IOC taker order (4.32bp fee, guaranteed fill)
6. Monitor for target's exit (currently: poll clearinghouseState every 10s)
7. Exit via ALO maker order (1.44bp), IOC fallback after 60s (4.32bp)
8. Hard max hold 1h safety net

**Fee structure:**
- Best case RT: 5.76bp (IOC entry + maker exit)
- Worst case RT: 8.64bp (IOC entry + IOC exit)
- HL maker fee: 1.44bp/side (positive cost, NOT rebate)
- HL taker fee: 4.32bp/side

## Critical Bug Found

**Target wallets are AGENT KEYS, not parent wallets.** HL's `clearinghouseState` returns NULL for agent/signing keys — positions are held by the unknown parent wallet. This means `_query_target_position()` always returns 0, triggering a false exit on every poll cycle.

**Impact:** Our HYPE BUY trade entered correctly at 43.308, but exited 56 minutes early at 43.15 (-36.5bp). The target actually exited at 43.37 (+14.4bp). We left +50.9bp on the table.

**Fix needed:** Replace position polling with trade-stream based exit detection. When we see the target selling the coin they bought (reverse TWAP), that's our exit signal. Same WS feed we already use for entry.

## Data & Evidence

### Trade Database
- **Collection:** `hl_wallet_trades` in MongoDB (2.64M docs, 5 days)
- **Schema:** `{buyer, seller, coin, price, size, notional, timestamp, trade_hash}`
- **Source:** HL WebSocket trade feed, collected by `scripts/hl_wallet_collector.py`

### Copy Trade Results
- **Collection:** `hl_copy_trades` (7 docs)
- 5 trades from V1 (old wallets, pre-fix): avg -26bp
- 2 trades from V5 (agent key bug): HYPE -36.5bp, VVV -32.8bp
- ALL losses caused by premature exit (agent key bug) or false signals (VVV oscillation)

### Universe Scan Results (computed this session)
- 5,171 active wallets scanned (>=20 trades, >=$10K notional)
- 1,686 wallets with >=5 round trips
- **496 wallets NET PROFITABLE after 5.76bp fees** (>=8 round trips)
- Top wallets show +90-290bp net per trade, 50-76% WR, 3-10h hold times

### Target Wallet Analysis

**Original targets (all problematic):**
- 0x5ac19d: 82% directional but oscillates on VVV (market-making). Agent key → NULL positions.
- 0x03ef8f: 89% directional on HYPE, 100% one-sided episodes. But only +6bp gross, +0.3bp net. Agent key → NULL positions.
- 0xf138b3: 54% wash trades. Not copy-worthy.

**Best new targets from universe scan (by total accumulated edge):**
| Rank | Address | Net PnL/trade | WR | Trips | Avg Hold | Coins |
|------|---------|---------------|-----|-------|----------|-------|
| 1 | 0xc1fce740d83a60 | +100bp | 57% | 83 | 7h | 31 |
| 2 | 0xd2ccf2ebc5be12 | +110bp | 61% | 72 | 7h | 27 |
| 5 | 0xab83abe96a71d2 | +142bp | 72% | 43 | 10h | 21 |
| 7 | 0xebc510b00a7013 | +291bp | 63% | 19 | 7h | 5 |
| 9 | 0x543430315c1e05 | +214bp | 76% | 25 | 5h | 8 |
| 10 | 0x2a72b57dea119c | +186bp | 75% | 28 | 3h | 10 |

### Signal Quality Analysis

**Episode classification methodology:**
- 120s gap between fills = new episode
- Directionality: |net| / total_notional (>60% = directional)
- Reversal filter: if next episode reverses within 10min, classify as "oscillating"
- Wash filter: <40% directionality = wash trade

**Conviction filter finding:** For W3 (0x03ef8f), larger entries were LESS profitable (>$10K: -52bp avg vs <$5K: +17bp avg). Smaller, quicker entries had better signal quality. 32 round trips — thin sample.

## Known Issues to Review

1. **Agent key exit bug** — Position polling returns NULL. Must switch to trade-stream exit.
2. **No oscillation filter** — VVV false signal from wallet doing buy/sell oscillation.
3. **60s TWAP window** — Some targets accumulate over 10+ minutes (W3 had a 12.8min TWAP). Window catches it but triggers entry mid-accumulation.
4. **1h MAX_HOLD** — Should be removed. Top targets hold 3-10 hours.
5. **Duplicate shadow fills** — `hl_mm_shadow_fills` has duplicate entries (same timestamp+coin).
6. **No position reconciliation on startup** — If script restarts with open positions, they're orphaned.
7. **wallet field inconsistency** — Some `hl_copy_trades` docs store wallet as a set `{addr1, addr2}`, others as string.

## Code Location

- **Copy trader:** `scripts/hl_copy_trader.py`
- **Wallet collector:** `scripts/hl_wallet_collector.py`
- **Shadow quoter:** `scripts/hl_mm_shadow_quoter.py`
- **MM V4 orchestrator:** `app/services/hl_mm/orchestrator_v4.py`

## What We Want From Codex

1. **Adversarial strategy review:** What are we wrong about? What market microstructure effects would kill this? Survivorship bias? Lookahead bias in our analysis? Is the +100bp/trade from universe scan real or an artifact?
2. **Code review:** `scripts/hl_copy_trader.py` — bugs, race conditions, edge cases, failure modes beyond the agent key issue.
3. **Architecture suggestions:** Better TWAP detection? Exit signal design? Position sizing? Risk management? How to handle the agent key problem robustly?
