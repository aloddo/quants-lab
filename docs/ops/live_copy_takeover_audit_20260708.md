# Live Copy Takeover Audit - 2026-07-08

## Scope

Initial Codex takeover audit for the live Hyperliquid copy-trading engine. This is an operational risk baseline, not a strategy approval.

## Active Live Process

- Active process at `2026-07-08 12:11:59 EEST`:
  - PID `24219`
  - command: `/Users/hermes/miniforge3/envs/quants-lab/bin/python strategies/live/hl_copy_trader_v17.py --config config/copy_trader_wallets_gate1_v4.json`
  - elapsed: about `1 day 13 hours`
- Launchd supervisor:
  - `ops/launchd/com.quantslab.v12-copy-trader.plist`
  - `KeepAlive=true`
  - launcher: `scripts/v12_launcher.sh`
  - log: `/tmp/ql-v12-copy-trader-launchd.log`
- Pause markers checked at `2026-07-08 12:11:59 EEST`:
  - `.HALT_COPY` absent
  - `/tmp/v12_pause` absent
  - therefore launchd will restart the bot if it exits.

## Current Live Config

Config: `config/copy_trader_wallets_gate1_v4.json`

Key live settings:

- `sizing_mode`: `fixed`
- `order_size_usd`: `150`
- `max_margin_util`: `0.7`
- `max_daily_loss`: `-25` but live engine comments/logs indicate this is not a separate enforced runtime path.
- `global_stop_pct`: `0.15`
- `max_leverage_cap`: `5`
- `sl_bps`: `-2500`
- `trail_activate_bps`: `100000`
- `max_hold_s`: `2592000` seconds, 30 days
- `backfill.enabled`: `true`
- expansion list includes broad perp universe plus `xyz:*` builder-dex markets.

Conflict noted:

- `scripts/v12_launcher.sh` says gate1 v4 had `xyz stripped`.
- `config/copy_trader_wallets_gate1_v4.json` says `xyz` was re-included and includes many `xyz:*` instruments.

## Live Log Snapshot

From `/tmp/ql-v12-copy-trader-launchd.log` near `2026-07-08 12:11:58 EEST`:

- Account realized PnL: about `-$9.20`
- V17-attributed realized PnL: about `+$32.14`
- Fees: about `$11.10`
- Liquidations: `7`, about `-$41.21`
- Unrealized PnL recently around `-$24` to `-$31`
- Open positions: `14`
- Open names in the latest snapshot include:
  - `ETH`, `ADA`, `BTC`, `SUI`, `XRP`, `AAVE`, `xyz:JPY`, `FARTCOIN`, `xyz:MU`, `ZEC`, `ONDO`, `ENA`, `HYPE`, `xyz:COIN`
- The bot opened `xyz:COIN` at `2026-07-08 11:50:41 EEST` while unrealized losses were already around `-$27`.
- Repeated Hyperliquid `429` responses are visible on exchange fill sync.

## Immediate Operational Risks

- The engine is still allowed to open new positions while the live book is deeply negative mark-to-market.
- The configured hard stop is loose relative to current objective: `15%` of roughly `$440-$460`, while current unrealized loss has already been around `-$30`.
- Hard stop logic is account-level; it does not directly enforce the stated objective of avoiding high-win-rate leaders that accumulate underwater bags.
- `sl_bps=-2500` and `max_hold_s=30d` structurally permit deep loss holding.
- The live process is supervised with `KeepAlive=true`; killing PID `24219` alone is not sufficient unless pause markers are set first.
- Existing `tools/kill_switch.sh` is oriented toward QL/Hummingbot/testnet and does not directly pause or flatten the live copy-trader process.
- Existing `tools/flatten_all_offline.py` is the documented all-dex flatten path. It requires both pause markers and is dry-run by default unless `--execute` is passed.
- Several Claude tmux/cron processes remain active with broad permissions; they should be retired or isolated as part of takeover.

## Current Gate1 Wallet Attribution

Local Mongo attribution from `v17_open_positions`, `v17_order_ids`, and `v17_exchange_fills` shows the current open loss is mostly unclosed exposure, not realized loss in the current 10-wallet cohort.

Current open positions by leader:

- `0x8c364082b2d8151ef4e06f6b6cef395030c9bc00`
  - 6 open, 0 exits, 0 realized closes in current gate1 run.
  - Open: `ETH BUY`, `ADA BUY`, `SUI BUY`, `XRP BUY`, `AAVE BUY`, `xyz:JPY SELL`.
  - These were startup backfill entries from `2026-07-06 20:10-20:11 UTC`.
- `0xe46eafafb60af2eea3a59768106a9342aec59ec3`
  - 3 open, 0 exits, 0 realized closes in current gate1 run.
  - Open: `ONDO BUY`, `ENA BUY`, `xyz:COIN BUY`.
  - `xyz:COIN` was opened fresh at `2026-07-08 08:50 UTC` while account uPnL was already deeply negative.
- `0x6f83ab8890ed38bf38a31010aa9a5e9ca743bfad`
  - 2 open, 1 exit, realized close PnL about `+$4.57`.
  - Open: `FARTCOIN BUY`, `xyz:MU BUY`.
  - Closed `xyz:BRENTOIL` profitably, then still carries two open bags.
- `0x5a5ec18fcf9db025d24c3674dd48ff40d5305204`
  - 2 open, 1 exit, realized close PnL about `-$0.69`.
  - Open: `BTC BUY`, `ZEC BUY`.
- `0x1404109f8cd4a79a0447365edbb7a13acd0b2f27`
  - 1 open, 1 profitable exit, realized close PnL about `+$5.02`.
  - Open: `HYPE BUY`.
- `0x36c097864a03c7f0215c0d43165a734152a12e0b`
  - 0 open, realized close PnL about `+$3.12`.
- `0x760ec8576c2dc5dba2655f7b948c0689b02b6cb0`
  - 0 open, realized close PnL about `+$1.96`.
- `0x03d8c9ce2a103a0094acc96520cf5eb87f85270c`
  - 0 open, no current-run copied fills.
- `0x36a60294f8b77e8ebe2ee32f3d3697952a379514`
  - 0 open, no current-run copied fills.
- `0xccf595171e2e56655fb4d386b7424da16be69d42`
  - 0 open, no current-run copied fills.

Preliminary trust call:

- Do not trust `0x8c3640` for new entries until current leader exchange-truth proves the leader still holds those exact sides and has a credible exit history. It is the dominant bag source.
- Do not trust `0xe46eaf` for new entries until reviewed. It created a fresh `xyz:COIN` entry while the live book was already underwater.
- Treat `0x6f83ab`, `0x5a5ec1`, and `0x140410` as watchlist, not cleared. They have some profitable realized exits but still carry open risk.
- The current bot design backfilled old leader holdings at startup; that is incompatible with an objective of avoiding bag-holder leaders unless backfill is disabled or heavily filtered by leader current MAE/drawdown.

## Containment Choices Requiring Explicit Approval

Option A - Halt automation only:

- Create `.HALT_COPY`
- Create `/tmp/v12_pause`
- Stop PID `24219`
- Result: launchd should not restart the bot, but current exchange positions remain open and unmanaged by this bot.

Option B - Flatten and halt:

- Create `.HALT_COPY`
- Create `/tmp/v12_pause`
- Stop or prevent copy-trader re-entry
- Run `tools/flatten_all_offline.py --execute`
- Verify exchange-truth flatness
- Result: closes current positions with reduce-only market closes across main, `xyz`, and `flx` dexes.

Option C - Observe only:

- Leave live bot running
- Continue diagnosis
- Result: ongoing risk of new entries and further mark-to-market drawdown.

My operational recommendation is Option B if the mandate is to stop the failing live experiment. Option A is only appropriate if the user wants to manage open positions manually outside this bot.

Option D - Freeze entries, keep exit management:

- Add a runtime entry-freeze flag to the live engine.
- Restart the bot under launchd with entries disabled and exits/reconciliation still active.
- Disable startup backfill.
- Result: avoids realizing current losses immediately, but stops new copied risk while the current leaders are reviewed.

After the user's clarification on `2026-07-08`, Option D better matches the desired posture than immediate flattening.
