# Cross-Exchange Arb — Spread Analysis Report

**Generated:** 2026-04-18T11:23:43.669036+00:00
**Data:** Trade-level (Binance SPOT aggTrades + Bybit PERP trades)
**Pairs analyzed:** 45

## Summary (sorted by mean absolute spread)

| Symbol       | Days |   Mean |   P50 |   P95 |  HL(h) | Hurst | AC(1) | Dens>30 | Dens>60 | Dir(BN) |
|--------------|------|--------|-------|-------|--------|-------|-------|---------|---------|---------|
| OGNUSDT      |   90 | 128.5bp | 47.5 | 482.6 | 0.160h | 0.962 | 0.996 |  62.9% |  44.8% |   7.8% |
| ONGUSDT      |   90 | 110.7bp | 68.9 | 367.7 | 5.922h | 0.975 | 0.994 |  63.2% |  52.1% |   9.5% |
| ENJUSDT      |   90 | 104.6bp | 32.4 | 464.1 | 0.957h | 0.987 | 0.999 |  52.3% |  31.9% |  13.8% |
| PYRUSDT      |   90 |  72.3bp | 42.1 | 243.9 | 1.187h | 0.912 | 0.980 |  63.7% |  33.8% |  14.4% |
| BLURUSDT     |   90 |  71.0bp | 14.1 | 381.3 | 0.068h | 0.949 | 0.997 |  33.1% |  25.0% |   8.2% |
| ARPAUSDT     |   90 |  69.0bp | 41.0 | 226.4 | 1.080h | 0.982 | 0.989 |  58.8% |  38.5% |   8.2% |
| ALICEUSDT    |   90 |  67.7bp | 16.6 | 381.6 | 0.461h | 0.951 | 0.995 |  33.4% |  23.0% |  26.3% |
| FIOUSDT      |   90 |  62.6bp | 14.5 | 282.0 | 14.273h | 0.863 | 0.992 |  28.7% |  15.9% |  48.4% |
| ILVUSDT      |   90 |  60.5bp | 27.6 | 236.7 | 0.778h | 0.852 | 0.991 |  45.2% |  19.0% |   3.8% |
| ONTUSDT      |   90 |  54.3bp | 23.3 | 185.8 | 4.511h | 0.963 | 0.997 |  40.8% |  20.7% |   6.7% |
| AXLUSDT      |   90 |  47.7bp | 19.3 | 195.5 | 0.536h | 0.918 | 0.991 |  34.7% |  20.2% |  10.9% |
| KSMUSDT      |   90 |  37.6bp | 21.1 | 172.5 | 0.135h | 0.801 | 0.977 |  25.1% |  12.3% |   1.7% |
| COMPUSDT     |   90 |  36.8bp | 16.1 | 131.3 | 0.437h | 0.913 | 0.995 |  29.3% |  16.0% |   1.2% |
| XVSUSDT      |   90 |  35.4bp | 21.4 | 119.4 | 0.006h | 0.849 | 0.934 |  36.6% |  17.6% |  29.1% |
| BANDUSDT     |   90 |  34.0bp | 18.9 | 137.9 | 1.123h | 0.789 | 0.926 |  25.0% |  12.7% |   8.4% |
| PORTALUSDT   |   90 |  33.4bp | 13.6 | 143.5 | 7.049h | 0.836 | 0.984 |  19.2% |  10.7% |  17.2% |
| METISUSDT    |   90 |  29.9bp | 22.4 | 94.3 | 0.230h | 0.804 | 0.905 |  35.0% |  10.5% |  14.6% |
| HIGHUSDT     |   90 |  27.8bp | 19.1 | 82.8 | 0.006h | 0.780 | 0.642 |  33.4% |  10.7% |  26.9% |
| STGUSDT      |   90 |  25.3bp | 17.6 | 61.9 | 0.983h | 0.889 | 0.990 |  19.2% |   5.3% |   2.9% |
| ROSEUSDT     |   90 |  25.1bp | 20.2 | 64.5 | 0.096h | 0.865 | 0.944 |  27.2% |   6.2% |   3.6% |
| QNTUSDT      |   90 |  19.4bp | 19.1 | 29.7 | 0.004h | 0.831 | 0.655 |   4.7% |   0.0% |   0.3% |
| THETAUSDT    |   90 |  18.7bp | 13.1 | 52.7 | 0.001h | 0.707 | 0.619 |  15.6% |   3.7% |  17.8% |
| MANAUSDT     |   90 |  13.3bp | 12.9 | 24.2 | 0.001h | 0.830 | 0.722 |   1.4% |   0.1% |   2.1% |
| DEXEUSDT     |   90 |  12.8bp | 11.9 | 27.3 | 0.036h | 0.853 | 0.741 |   3.2% |   0.1% |  11.3% |
| CFXUSDT      |   90 |  12.0bp | 11.4 | 22.4 | 0.112h | 0.832 | 0.620 |   1.5% |   0.0% |   2.2% |
| TWTUSDT      |   90 |  11.4bp | 10.6 | 24.7 | 0.003h | 0.872 | 0.782 |   1.6% |   0.0% |  10.0% |
| GALAUSDT     |   90 |  11.3bp | 10.2 | 26.6 | 0.000h | 0.703 | 0.346 |   2.9% |   0.0% |  12.9% |
| APTUSDT      |   90 |  10.7bp | 10.4 | 18.5 | 0.001h | 0.770 | 0.592 |   0.1% |   0.0% |   1.3% |
| OPUSDT       |   90 |   9.9bp |  9.3 | 18.6 | 0.002h | 0.797 | 0.831 |   1.5% |   0.0% |   1.7% |
| UNIUSDT      |   90 |   9.4bp |  9.1 | 14.5 | 0.008h | 0.804 | 0.686 |   0.1% |   0.0% |   0.1% |
| DOTUSDT      |   90 |   8.7bp |  8.1 | 17.1 | 0.001h | 0.728 | 0.797 |   1.0% |   0.1% |   3.6% |
| ARBUSDT      |   90 |   8.6bp |  8.5 | 15.3 | 0.001h | 0.701 | 0.409 |   0.0% |   0.0% |   2.0% |
| NEARUSDT     |   90 |   8.2bp |  8.0 | 15.8 | 0.001h | 0.741 | 0.605 |   0.0% |   0.0% |   4.1% |
| BCHUSDT      |   90 |   8.2bp |  8.1 | 12.8 | 0.023h | 0.806 | 0.742 |   0.0% |   0.0% |   0.2% |
| AAVEUSDT     |   90 |   8.1bp |  8.1 | 11.6 | 0.001h | 0.783 | 0.760 |   0.0% |   0.0% |   0.2% |
| AVAXUSDT     |   90 |   7.1bp |  6.8 | 15.0 | 0.000h | 0.682 | 0.409 |   0.0% |   0.0% |   8.5% |
| ADAUSDT      |   90 |   7.1bp |  7.3 | 11.8 | 0.000h | 0.727 | 0.599 |   0.0% |   0.0% |   0.9% |
| LTCUSDT      |   90 |   6.9bp |  7.1 |  9.9 | 0.000h | 0.769 | 0.612 |   0.0% |   0.0% |   0.1% |
| LINKUSDT     |   90 |   6.8bp |  6.3 | 14.5 | 0.000h | 0.677 | 0.304 |   0.0% |   0.0% |   8.6% |
| SUIUSDT      |   90 |   6.3bp |  6.3 |  9.3 | 0.001h | 0.771 | 0.725 |   0.0% |   0.0% |   0.3% |
| XRPUSDT      |   14 |   5.4bp |  5.3 |  7.6 | 0.004h | 0.778 | 0.738 |   0.0% |   0.0% |   0.1% |
| ETHUSDT      |   14 |   5.1bp |  5.1 |  7.1 | 0.003h | 0.745 | 0.681 |   0.0% |   0.0% |   0.1% |
| SOLUSDT      |   14 |   5.1bp |  5.0 |  8.4 | 0.005h | 0.769 | 0.807 |   0.0% |   0.0% |   0.7% |
| BTCUSDT      |   14 |   5.1bp |  5.0 |  6.8 | 0.004h | 0.774 | 0.734 |   0.0% |   0.0% |   0.0% |
| DOGEUSDT     |   14 |   4.7bp |  4.9 |  8.1 | 0.005h | 0.748 | 0.821 |   0.0% |   0.0% |   2.3% |

## Tier Comparison

**T1 Majors** (4 pairs): mean spread 5.2bp, density >30bp 0.0%, density >60bp 0.0%

**T2 Mid-cap** (11 pairs): mean spread 7.4bp, density >30bp 0.1%, density >60bp 0.0%

**T3 Small-cap** (20 pairs): mean spread 37.0bp, density >30bp 21.2%, density >60bp 12.1%

**T4 Micro** (10 pairs): mean spread 52.2bp, density >30bp 37.5%, density >60bp 18.8%


## R1 Kill Gate Assessment

- Pairs with >2% opportunity density at 30bp: **24** (need >= 10)
- Pairs with OU half-life < 1h: **38**
- Pairs with Hurst < 0.5 (mean-reverting): **0**

**KILL GATE: PASS** (24 >= 10)

## Direction Analysis

Positive spread = Bybit expensive = BUY_BN_SELL_BB
Negative spread = Binance expensive = BUY_BB_SELL_BN

- OGNUSDT: BUY_BB dominant (92%)
- ONGUSDT: BUY_BB dominant (90%)
- ENJUSDT: BUY_BB dominant (86%)
- PYRUSDT: BUY_BB dominant (86%)
- BLURUSDT: BUY_BB dominant (92%)
- ARPAUSDT: BUY_BB dominant (92%)
- ALICEUSDT: BUY_BB dominant (74%)
- FIOUSDT: BUY_BB dominant (52%)
- ILVUSDT: BUY_BB dominant (96%)
- ONTUSDT: BUY_BB dominant (93%)
- AXLUSDT: BUY_BB dominant (89%)
- KSMUSDT: BUY_BB dominant (98%)
- COMPUSDT: BUY_BB dominant (99%)
- XVSUSDT: BUY_BB dominant (71%)
- BANDUSDT: BUY_BB dominant (92%)
- PORTALUSDT: BUY_BB dominant (83%)
- METISUSDT: BUY_BB dominant (85%)
- HIGHUSDT: BUY_BB dominant (73%)
- STGUSDT: BUY_BB dominant (97%)
- ROSEUSDT: BUY_BB dominant (96%)
- QNTUSDT: BUY_BB dominant (100%)
- THETAUSDT: BUY_BB dominant (82%)
- MANAUSDT: BUY_BB dominant (98%)
- DEXEUSDT: BUY_BB dominant (89%)
- CFXUSDT: BUY_BB dominant (98%)
- TWTUSDT: BUY_BB dominant (90%)
- GALAUSDT: BUY_BB dominant (87%)
- APTUSDT: BUY_BB dominant (99%)
- OPUSDT: BUY_BB dominant (98%)
- UNIUSDT: BUY_BB dominant (100%)
- DOTUSDT: BUY_BB dominant (96%)
- ARBUSDT: BUY_BB dominant (98%)
- NEARUSDT: BUY_BB dominant (96%)
- BCHUSDT: BUY_BB dominant (100%)
- AAVEUSDT: BUY_BB dominant (100%)
- AVAXUSDT: BUY_BB dominant (92%)
- ADAUSDT: BUY_BB dominant (99%)
- LTCUSDT: BUY_BB dominant (100%)
- LINKUSDT: BUY_BB dominant (91%)
- SUIUSDT: BUY_BB dominant (100%)
- XRPUSDT: BUY_BB dominant (100%)
- ETHUSDT: BUY_BB dominant (100%)
- SOLUSDT: BUY_BB dominant (99%)
- BTCUSDT: BUY_BB dominant (100%)
- DOGEUSDT: BUY_BB dominant (98%)

## Time-of-Day Patterns

| Symbol | Best Hour (UTC) | Spread | Worst Hour | Spread | Range |
|--------|----------------|--------|------------|--------|-------|
| OGNUSDT | 19:00 | 442.0bp | 23:00 | 32.6bp | 409.5bp |
| ONGUSDT | 09:00 | 210.1bp | 19:00 | 47.1bp | 162.9bp |
| ENJUSDT | 16:00 | 269.4bp | 05:00 | 43.1bp | 226.3bp |
| PYRUSDT | 15:00 | 214.6bp | 06:00 | 35.3bp | 179.3bp |
| BLURUSDT | 07:00 | 171.1bp | 18:00 | 12.5bp | 158.6bp |
| ARPAUSDT | 10:00 | 130.9bp | 15:00 | 24.4bp | 106.5bp |
| ALICEUSDT | 17:00 | 180.6bp | 09:00 | 14.1bp | 166.5bp |
| FIOUSDT | 22:00 | 375.7bp | 01:00 | 16.9bp | 358.8bp |
| ILVUSDT | 12:00 | 278.1bp | 06:00 | 22.9bp | 255.2bp |
| ONTUSDT | 11:00 | 145.3bp | 19:00 | 16.7bp | 128.7bp |
| AXLUSDT | 21:00 | 168.2bp | 18:00 | 12.9bp | 155.3bp |
| KSMUSDT | 07:00 | 112.2bp | 22:00 | 18.3bp | 93.9bp |
| COMPUSDT | 20:00 | 109.0bp | 07:00 | 16.5bp | 92.6bp |
| XVSUSDT | 08:00 | 73.2bp | 16:00 | 19.3bp | 53.9bp |
| BANDUSDT | 12:00 | 117.2bp | 03:00 | 15.7bp | 101.5bp |
| PORTALUSDT | 18:00 | 158.8bp | 01:00 | 10.9bp | 147.9bp |
| METISUSDT | 08:00 | 52.5bp | 04:00 | 17.9bp | 34.5bp |
| HIGHUSDT | 10:00 | 61.0bp | 01:00 | 18.6bp | 42.4bp |
| STGUSDT | 10:00 | 72.6bp | 19:00 | 14.7bp | 57.8bp |
| ROSEUSDT | 01:00 | 36.2bp | 17:00 | 18.6bp | 17.6bp |
| QNTUSDT | 12:00 | 20.2bp | 04:00 | 18.4bp | 1.8bp |
| THETAUSDT | 05:00 | 40.5bp | 03:00 | 14.8bp | 25.7bp |
| MANAUSDT | 00:00 | 17.3bp | 05:00 | 11.1bp | 6.2bp |
| DEXEUSDT | 11:00 | 13.9bp | 06:00 | 11.0bp | 2.9bp |
| CFXUSDT | 14:00 | 15.1bp | 19:00 | 11.1bp | 4.0bp |
| TWTUSDT | 01:00 | 12.4bp | 08:00 | 10.4bp | 2.1bp |
| GALAUSDT | 00:00 | 12.8bp | 04:00 | 10.2bp | 2.6bp |
| APTUSDT | 00:00 | 11.9bp | 04:00 | 9.8bp | 2.1bp |
| OPUSDT | 00:00 | 11.3bp | 14:00 | 9.1bp | 2.3bp |
| UNIUSDT | 00:00 | 10.2bp | 07:00 | 8.6bp | 1.6bp |
| DOTUSDT | 05:00 | 9.9bp | 07:00 | 7.9bp | 2.0bp |
| ARBUSDT | 15:00 | 8.9bp | 05:00 | 7.9bp | 1.0bp |
| NEARUSDT | 16:00 | 9.1bp | 21:00 | 6.9bp | 2.2bp |
| BCHUSDT | 07:00 | 8.7bp | 19:00 | 7.5bp | 1.2bp |
| AAVEUSDT | 15:00 | 8.6bp | 23:00 | 7.7bp | 0.9bp |
| AVAXUSDT | 17:00 | 7.7bp | 04:00 | 6.4bp | 1.3bp |
| ADAUSDT | 01:00 | 7.5bp | 07:00 | 6.7bp | 0.8bp |
| LTCUSDT | 00:00 | 7.5bp | 03:00 | 6.7bp | 0.8bp |
| LINKUSDT | 00:00 | 7.4bp | 20:00 | 6.4bp | 1.0bp |
| SUIUSDT | 07:00 | 6.5bp | 21:00 | 5.8bp | 0.7bp |
| XRPUSDT | 16:00 | 5.7bp | 00:00 | 4.9bp | 0.8bp |
| ETHUSDT | 14:00 | 5.5bp | 04:00 | 4.7bp | 0.8bp |
| SOLUSDT | 05:00 | 5.7bp | 00:00 | 4.5bp | 1.2bp |
| BTCUSDT | 19:00 | 5.4bp | 07:00 | 4.7bp | 0.7bp |
| DOGEUSDT | 14:00 | 5.2bp | 04:00 | 4.2bp | 1.0bp |
