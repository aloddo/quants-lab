# Gate1 Problem Leader History - 2026-07-08

## Sources

- Historical closed journeys: `app/data/research/v27/journeys_full.parquet`
- Historical MAE scan: `research/v16/mae_bag_measure.py` over `app/data/v15/m02_journeys.parquet`
- Current live copy state: Mongo `v17_open_positions`, `v17_order_ids`, `v17_exchange_fills`
- Current leader exchange-truth positions: read-only Hyperliquid `clearinghouseState` on main and `xyz` dex, queried 2026-07-08.

Important caveat: `n_actions > 2` is only a martingale/add-on proxy. It proves multi-action position management, not always averaging down. The strongest martingale evidence is multi-action losing journeys plus losses held longer than wins plus large MAE.

## Executive Verdict

Do not trust this cohort for new entries without a freeze/review gate.

The problem is not just the current copy bot. Several leaders have the exact profile we were trying to avoid: high win rate, long loss holds, repeated multi-action losing journeys, and large live underwater inventory.

Recommended immediate trust actions:

- Block new entries from `0x8c3640...bc00`, `0xe46eaf...ec3`, and `0x5a5ec1...204`.
- Keep `0x6f83a...fad` and `0x140410...f27` on watchlist, not cleared.
- Disable startup backfill for this strategy class. It copied stale inventory into old leader bags.
- If keeping the bot alive, run it in exits-only mode until the current book resolves.

## Current Problem Leaders

### 0x8c3640...bc00

Current copied open positions:

- `ETH BUY`, `ADA BUY`, `SUI BUY`, `XRP BUY`, `AAVE BUY`, `xyz:JPY SELL`

Current leader exchange-truth:

- Main dex: still long `ETH`, `SUI`, `XRP`, `AAVE`, `ADA`.
- `xyz`: still short `JPY`.
- Large leader uPnL pain at query time:
  - `SUI`: about `-$2,287`
  - `XRP`: about `-$2,470`
  - `xyz:JPY`: about `-$142`

Historical closed journey profile:

- 138 closed journeys, win rate `68.1%`, net `-$15,724`.
- Median hold: wins `26.95h`, losses `15.07h`.
- Losses are not held longer on median, but the tail is ugly.
- Worst closed journey: BTC long, 63.7h hold, `-13,096bps`, 22 actions.
- Add-on/multi-action proxy: `85.5%` overall, `93.2%` on losses.
- MAE scan: median MAE `-1.6%`, p90-tail MAE `-8.4%`, worst MAE `-57.9%`, `1.8%` journeys below `-20%`.

Verdict: reject for new copy. This is not clean conviction. It has negative aggregate closed PnL, extreme loss tails, high multi-action losing journeys, and is currently sitting in underwater directional inventory.

### 0xe46eaf...ec3

Current copied open positions:

- `ONDO BUY`, `ENA BUY`, `xyz:COIN BUY`

Current leader exchange-truth:

- Main dex: still long `ONDO`, `ENA`, plus `ETH`, `AAVE`, `MORPHO`.
- `xyz`: still long `COIN`, plus large broader book including `GOLD`, `CL`, `URNM`, etc.
- Current `xyz:COIN` is slightly profitable for the leader, but `ONDO` and `ENA` are red.
- Leader also has large underwater `xyz:GOLD` and `xyz:URNM`.

Historical closed journey profile:

- 71 closed journeys, win rate `84.5%`, net `+$10,998`.
- Median hold: wins `43.66h`, losses `121.96h`.
- Average hold: wins `122.65h`, losses `405.35h`.
- Loss/win hold ratio: `2.79x` median, `3.31x` average.
- Worst examples include SOL long held 1,823h for `-1,091bps`, LINK long held 1,872h for `-586bps`, and a 48-action BTC long loss.
- Add-on/multi-action proxy: `74.6%` overall, `81.8%` on losses.
- MAE scan: median MAE `-2.5%`, p90-tail MAE `-20.4%`, worst MAE `-30.3%`, `11.1%` journeys below `-20%`.

Verdict: reject for new copy. This is the clearest "high win rate plus long loser holding" profile. Legitimate conviction may exist, but it is not compatible with our stated objective.

### 0x5a5ec1...204

Current copied open positions:

- `BTC BUY`, `ZEC BUY`

Current leader exchange-truth:

- Still long `BTC`, `ZEC`, and `LIT`.
- Deeply underwater:
  - `BTC`: about `-$10,851`, ROE about `-14.54`
  - `ZEC`: about `-$1,098`
  - `LIT`: about `-$321`

Historical closed journey profile:

- 211 closed journeys, win rate `73.9%`, net `+$3,215`.
- Median hold: wins `1.62h`, losses `2.43h`.
- Average hold: wins `31.50h`, losses `48.25h`.
- Worst closed journeys include `xyz:CRCL` short `-1,729bps`, `GRASS` short `-1,530bps`, `xyz:GOLD` long held 403h for `-1,123bps`.
- Add-on/multi-action proxy: `74.9%` overall, `63.6%` on losses.
- MAE scan: median MAE `-0.7%`, p90-tail MAE `-8.9%`, worst MAE `-40.1%`, `3.5%` journeys below `-20%`.

Verdict: reject for new copy while current positions remain open. The live state is a textbook bag. The historical edge is weaker and the current BTC/ZEC losses are too large to trust blindly.

### 0x6f83a...fad

Current copied open positions:

- `FARTCOIN BUY`, `xyz:MU BUY`

Current leader exchange-truth:

- Still long `FARTCOIN`, short `ETH`, long `xyz:MU`.
- `FARTCOIN`: about `-$669`
- `xyz:MU`: about `-$7,939`
- It did exit `xyz:BRENTOIL` profitably in the live bot.

Historical closed journey profile:

- 87 closed journeys, win rate `74.7%`, net `+$105,850`.
- Median hold: wins `23.80h`, losses `8.56h`.
- Average hold: wins `84.43h`, losses `96.19h`.
- Worst closed BTC long loss `-702bps` over 143.5h.
- Add-on/multi-action proxy: `95.4%` overall, `90.9%` on losses.
- MAE scan: median MAE `-1.9%`, p90-tail MAE `-14.3%`, worst MAE `-45.5%`, `5.1%` journeys below `-20%`.

Verdict: watchlist, not cleared. It has real historical profitability, but the current `xyz:MU` loss and very high multi-action behavior mean new entries should stay blocked until reviewed.

### 0x140410...f27

Current copied open positions:

- `HYPE BUY`

Current leader exchange-truth:

- Still long `HYPE`, near flat/profitable.
- Also long `LIT` and `xyz:SPCX`, both underwater.

Historical closed journey profile:

- 185 closed journeys, win rate `96.2%`, net `+$39,354`.
- Median hold: wins `11.67h`, losses `18.57h`.
- Average hold: wins `36.27h`, losses `186.48h`.
- Worst closed BTC long loss: 980h hold, `-821bps`, 103 actions.
- Worst LIT long loss: 69h hold, `-1,508bps`, 27 actions.
- Add-on/multi-action proxy: `90.8%` overall, `85.7%` on losses.
- MAE scan: median MAE `-1.7%`, p90-tail MAE `-9.5%`, worst MAE `-38.5%`, `3.0%` journeys below `-20%`.

Verdict: watchlist. Current copied HYPE is not the main problem, but historical loss handling is still not clean: very high win rate, very long average loss holds, and multi-action tail losses.

## Ranking By Bag Risk

Highest concern:

1. `0xe46eaf...ec3` - long loser holds, high deep-MAE rate, current broad underwater book.
2. `0x8c3640...bc00` - negative aggregate historical net, current six-position copied bag source, extreme closed loss tails.
3. `0x5a5ec1...204` - current leader is deeply underwater in BTC/ZEC/LIT.
4. `0x6f83a...fad` - profitable historically but current `xyz:MU` bag is large.
5. `0x140410...f27` - current copied HYPE is okay, but loss tails show bag-holding risk.

## Implication For The Live Bot

The bot's current losses are not random noise. The active book came largely from startup backfill into leaders with existing inventory. For the stated objective, a copy engine must distinguish:

- fresh leader entry with clean exit discipline
- old leader inventory already underwater
- additive trades into losing inventory
- leaders whose losers live much longer than winners

The current gate1 setup does not enforce that distinction strongly enough.
