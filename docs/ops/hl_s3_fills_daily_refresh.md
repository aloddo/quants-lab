# Hyperliquid S3 Fills Refresh

Canonical entry point:

```bash
scripts/hl_s3_fills_daily_refresh.sh
```

The maintained implementation is:

```bash
data_pipeline/hl_s3_fills_daily_refresh.py
```

It is the only S3 downloader for Hyperliquid fills. The old generic fills downloader, the v13 enriched downloader, and the separate all-market candle builder were removed because their behavior is now covered by this script.

## What It Does

- Downloads requester-pays S3 objects from `hl-mainnet-node-data/node_fills_by_block/hourly`.
- Streams each hourly object without persisting raw `.lz4` files.
- Reconstructs all-market 1m candles before wallet filtering.
- Writes filtered fills only for the selected wallet universe plus current live config wallets.
- Does not call Hyperliquid REST and does not load live trading credentials.
- Does not prune old data unless explicitly configured; the launchd wrapper passes `--no-prune`.

Default outputs:

```text
app/data/hl_s3_fills_v2_hot/YYYYMMDD.parquet
app/data/hl_s3_candles_1m_hot/YYYYMMDD.parquet
app/data/hl_s3_fills_v2_hot_manifest.json
```

## Filtered Fills Schema

The filtered fills parquet preserves the enriched accounting/order fields needed for equity reconstruction:

```text
wallet, coin, side, size, price, time, dir, closedPnl,
startPosition, fee, feeToken, builderFee, deployerFee, crossed,
hash, oid, tid, cloid, twapId, builder, notional, source
```

`closedPnl` is gross realized PnL. Net realized PnL should account for `fee`, `builderFee`, and `deployerFee`.

## Candle Schema

All-market 1m candles are reconstructed in the same S3 pass, before filtering to the wallet universe:

```text
coin, timestamp_utc, open, high, low, close, volume, n_trades, source
```

Candles use only `side == "B"` fills so each public trade is counted once. Open/close ordering uses `(time, tid)`.

## Daily Job

The LaunchAgent is:

```text
ops/launchd/com.quantslab.hl-s3-fills-daily.plist
```

It calls `scripts/hl_s3_fills_daily_refresh.sh`, which currently rewrites the latest three published days and writes no retention deletes.
