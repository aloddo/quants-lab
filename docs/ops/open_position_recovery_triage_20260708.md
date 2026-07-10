# Open Position Recovery Triage - 2026-07-08

## Immediate Control Change

Live config `config/copy_trader_wallets_gate1_v4.json` was changed and the launchd-managed bot was restarted at 2026-07-08 13:06 EEST.

- Removed reject leaders:
  - `0x8c364082b2d8151ef4e06f6b6cef395030c9bc00`
  - `0xe46eafafb60af2eea3a59768106a9342aec59ec3`
  - `0x5a5ec18fcf9db025d24c3674dd48ff40d5305204`
- Reduced `global.min_cohort_wallets` from `10` to `7`.
- Disabled `global.backfill.enabled`.
- Patched the live engine validation to allow explicitly authorized emergency cohorts below 10 wallets; live sub-30 remains gated by `live_below_floor_authorized`.

Fresh process evidence:

- PID `6956`
- Loaded `7` wallets.
- `V16 READY: cohort=7`
- `Copy trader V17 starting: 7 wallets`
- No startup backfill pass was launched.

Important consequence: the 14 existing positions were recovered from persistent state, but the removed leaders are no longer subscribed targets. Positions tied to removed leaders should be managed independently from here unless an explicit exit-watch mechanism is added.

## Live Position Snapshot

Source: Hyperliquid `clearinghouseState` across main + `xyz` + `flx`, plus Mongo `v17_open_positions` attribution.

Machine-readable output:

- `app/data/live/open_position_recovery_snapshot_20260708.csv`

| Coin | Side | uPnL | Move to Breakeven | 24h | 72h | 7d | Beta Ref | Beta | Corr | Triage |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- |
| ADA | Long | -4.37 | +10.8% | -6.6% | -11.4% | +9.7% | BTC | 1.20 | 0.58 | Cut candidate |
| xyz:MU | Long | -4.34 | +10.6% | -4.4% | -13.8% | -19.8% | xyz:SP500 | n/a | n/a | Cut candidate |
| FARTCOIN | Long | -4.34 | +10.7% | -8.2% | -6.8% | +3.1% | BTC | 1.56 | 0.53 | Cut candidate |
| AAVE | Long | -4.19 | +10.2% | -5.9% | -1.5% | +1.3% | BTC | 1.02 | 0.54 | Cut candidate |
| ENA | Long | -3.32 | +8.0% | -4.3% | -6.1% | +1.7% | BTC | 1.41 | 0.69 | Weak hold review |
| SUI | Long | -2.72 | +6.4% | -5.1% | -5.7% | +0.6% | BTC | 1.15 | 0.77 | Hold for beta bounce |
| ONDO | Long | -2.66 | +6.3% | -4.1% | -2.8% | +2.2% | BTC | 1.12 | 0.66 | Hold for beta bounce |
| XRP | Long | -2.55 | +6.1% | -4.1% | -4.6% | +3.6% | BTC | 0.97 | 0.82 | Hold for beta bounce |
| ETH | Long | -1.39 | +3.2% | -2.4% | -1.6% | +10.2% | BTC | 1.08 | 0.88 | Can wait tight |
| BTC | Long | -1.20 | +2.7% | -2.2% | -1.3% | +5.6% | ETH | 0.71 | 0.88 | Can wait tight |
| xyz:COIN | Long | -0.25 | +0.2% | n/a | n/a | n/a | xyz:SP500 | n/a | n/a | Can wait tight |
| xyz:JPY | Short | -0.10 | +0.2% | +0.3% | +0.6% | -0.2% | xyz:SP500 | n/a | n/a | Can wait tight |
| HYPE | Long | -0.06 | +0.0% | -4.6% | -1.3% | +8.6% | BTC | 1.18 | 0.67 | Can wait tight |
| ZEC | Long | +1.26 | -2.7% | +1.6% | +1.3% | +16.8% | BTC | 1.21 | 0.53 | Keep / let exit |

## Recommendation

Do not flatten the whole book blindly.

Recommended first-loss cut set:

- `ADA`
- `xyz:MU`
- `FARTCOIN`
- `AAVE`

Reason: all need roughly a 10% favorable move to breakeven and all have negative short-horizon momentum. `ADA`, `FARTCOIN`, and `AAVE` have only moderate BTC correlation, so they are not clean beta recovery bets. `xyz:MU` is down across 24h, 72h, and 7d, and the position is tied to a leader with an existing large MU bag.

Watch rather than cut immediately:

- `SUI`
- `ONDO`
- `XRP`

Reason: these need only about 6% to breakeven and have useful BTC beta/correlation. They are still weak, but they are better candidates for a market beta bounce than the four above.

Near-flat / manageable:

- `BTC`
- `ETH`
- `HYPE`
- `xyz:COIN`
- `xyz:JPY`
- `ZEC`

Reason: breakeven distance is small or position is already profitable. These should be monitored with tight invalidation rather than panic-closed.

## Execution Note

No flatten orders were placed in this triage. Any execution should use an explicit, paused, audited path because the account is live real capital.
