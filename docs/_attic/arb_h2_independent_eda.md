# H2 Independent EDA

## Verdict

The raw `$185/day` claim is not robust. On the primary causal definition (trailing `6h` median, episode-level counting, `5m` entry delay, `4h` exit horizon, `$200` notional, `24`bp round-trip fees), the study finds `10749` executable episodes, `2.01` mean net PnL per episode when reversion is measured back to the frozen entry baseline, and `240.27` dollars/day across calendar time across the full overlapping book. That headline also needs about `7.4` overlapping `$200` slots on average (`~$1471` deployed), so it is only `32.66` dollars/day per `$200` capital slot.

The biggest artifacts are:

- Counting bars instead of episodes: `183684` threshold-hit bars collapse to `10750` distinct spike episodes on the `6h` baseline.
- Treating sparse event-time bars as continuous 1s data: median venue-pair coverage is only `2.64%` of clock seconds.
- Entering at the peak: after a `5m` delay, the threshold is already gone on `60.7%` of usable episodes.

## What Survived

Some spikes do compress. With zero delay and a `1h` exit horizon, the dynamic baseline shows `73.7%` reversion inside `25`bp, but the stricter frozen-baseline rate is only `64.5%`.
But realism removes most of the edge. With `5m` delay and `24h` patience, the dynamic baseline still shows `2.40` mean net PnL, while the frozen-baseline version drops to `2.21` with only `93.3%` of episodes above fees.

## Failure Modes

Sparse coverage is severe in many names:

| symbol   |   coverage_ratio |   median_gap_s |   p95_gap_s |
|:---------|-----------------:|---------------:|------------:|
| HIGHUSDT |       0.00232974 |             72 |      2001.6 |
| BANDUSDT |       0.00301922 |             61 |      1550.5 |
| ILVUSDT  |       0.00593241 |             29 |       798   |
| PYRUSDT  |       0.0062594  |             25 |       754   |
| XVSUSDT  |       0.00645948 |             20 |       793   |

The structural level is not perfectly stable even after smoothing with `6h`:

| symbol     |   episode_starts |   events_per_calendar_day |   baseline_p95_1h_drift_bps |   baseline_p99_1h_drift_bps |
|:-----------|-----------------:|--------------------------:|----------------------------:|----------------------------:|
| OGNUSDT    |             1413 |                  15.7002  |                    257.063  |                     592.376 |
| FIOUSDT    |              782 |                   8.6892  |                    380.602  |                     421.434 |
| ALICEUSDT  |             1143 |                  12.7001  |                    160.857  |                     356.454 |
| PYRUSDT    |              540 |                   6.00013 |                    242.693  |                     346.552 |
| ENJUSDT    |             1763 |                  19.5889  |                     96.235  |                     305.65  |
| PORTALUSDT |              192 |                   2.13338 |                    116.264  |                     213.618 |
| BLURUSDT   |              863 |                   9.58889 |                     88.6603 |                     209.632 |
| ONTUSDT    |              642 |                   7.13349 |                     69.294  |                     183.741 |
| ILVUSDT    |              250 |                   2.77786 |                    133.832  |                     176.882 |
| ONGUSDT    |             1208 |                  13.4238  |                     85.9229 |                     170.857 |

`3.4%` of episode starts occur on effectively ghost bars (<=2 trades across both venues or < $25 total USD volume), and `21.3%` start in below-median liquidity.

Spike clustering across pairs is real:

| metric                                 |   value |
|:---------------------------------------|--------:|
| max simultaneous starts at same second |       2 |
| 95th pct starts per minute             |       9 |
| max starts in one minute               |      15 |

Capital overlap is the real bottleneck:

|   delay_sec |   horizon_sec |   episodes |   entries_per_day |   mean_hold_sec |   median_hold_sec |   avg_concurrent_positions |   max_concurrent_positions |   avg_capital_required_usd |   max_capital_required_usd |   fixed_daily_pnl_usd |   fixed_daily_pnl_per_200usd_slot |
|------------:|--------------:|-----------:|------------------:|----------------:|------------------:|---------------------------:|---------------------------:|---------------------------:|---------------------------:|----------------------:|----------------------------------:|
|         300 |         14400 |      10749 |           119.434 |         5321.36 |              4287 |                    7.35589 |                        242 |                    1471.18 |                      48400 |               240.271 |                          32.6637  |
|         300 |         86400 |      10749 |           119.434 |        25555.1  |             16195 |                   35.3257  |                        381 |                    7065.14 |                      76200 |               263.366 |                           7.45537 |

## Worst Event

`BLURUSDT` on `2026-02-12T08:56:52+00:00` is the ugliest delayed-entry failure in the sample. It started at `169.3`bp excess, peaked at `205.1`bp, and still produced `-0.48` dollars net after a `5m` delayed entry and `24h` look-ahead.

## Carry Check

Funding alignment matters. Across pairs with cached funding summaries, the median share of episodes that are directionally aligned with funding carry is `59.6%`.

| symbol     |   carry_aligned_pct |   funding_mean_rate_pct |   funding_annual_pct |   d300s_h14400s_sum_net_pnl_usd |
|:-----------|--------------------:|------------------------:|---------------------:|--------------------------------:|
| COMPUSDT   |            1        |                 -0.0272 |               -29.81 |                        183.104  |
| THETAUSDT  |            1        |                 -0.0127 |               -13.86 |                         18.8215 |
| HIGHUSDT   |            1        |                 -0.0275 |               -30.1  |                          1.7094 |
| ONTUSDT    |            0.746106 |                 -0.0734 |               -80.37 |                       1456.36   |
| KSMUSDT    |            0.740741 |                 -0.0421 |               -46.12 |                        162.263  |
| METISUSDT  |            0.612903 |                 -0.1638 |              -179.33 |                         66.7397 |
| ENJUSDT    |            0.595576 |                 -0.1484 |              -162.52 |                       4267.11   |
| ONGUSDT    |            0.583609 |                 -0.0735 |               -80.52 |                       2560.02   |
| ALICEUSDT  |            0.554681 |                 -0.0531 |               -58.1  |                       2727.83   |
| PORTALUSDT |            0.458333 |                 -0.0185 |               -20.25 |                        375.703  |

## Pair Cross-Section

Best pairs under the strict `5m` delayed-entry / `4h` horizon lens:

| symbol     |   episodes |   events_per_calendar_day |   ghost_start_pct |   coverage_ratio |   d300s_h14400s_sum_net_pnl_usd |   d300s_h14400s_fixed_sum_net_pnl_usd |   d300s_h14400s_revert_rate |   d300s_h14400s_fixed_revert_rate |
|:-----------|-----------:|--------------------------:|------------------:|-----------------:|--------------------------------:|--------------------------------------:|----------------------------:|----------------------------------:|
| ENJUSDT    |       1763 |                 19.5889   |        0.0181509  |       0.0706341  |                        4267.11  |                              3833.04  |                    0.938174 |                          0.83097  |
| OGNUSDT    |       1413 |                 15.7002   |        0.0325548  |       0.0139653  |                        3464.19  |                              2815.94  |                    0.878981 |                          0.702052 |
| ALICEUSDT  |       1143 |                 12.7001   |        0.0341207  |       0.0204407  |                        2727.83  |                              2306.8   |                    0.909886 |                          0.702537 |
| ONGUSDT    |       1208 |                 13.4238   |        0.0339404  |       0.0185269  |                        2560.02  |                              2174.23  |                    0.959437 |                          0.804636 |
| BLURUSDT   |        863 |                  9.58889  |        0.0243337  |       0.0256735  |                        1951.15  |                              1825.95  |                    0.993048 |                          0.816918 |
| FIOUSDT    |        782 |                  8.6892   |        0.0588235  |       0.0132816  |                        1908.56  |                              1814.3   |                    0.996164 |                          0.92711  |
| ARPAUSDT   |        822 |                  9.13343  |        0.0364964  |       0.0200456  |                        1718.55  |                              1631.06  |                    1        |                          0.924574 |
| PYRUSDT    |        540 |                  6.00013  |        0.087037   |       0.0062594  |                        1655.95  |                              1229.38  |                    0.687037 |                          0.459259 |
| AXLUSDT    |        665 |                  7.38903  |        0.0345865  |       0.0266575  |                        1535.49  |                              1299.64  |                    1        |                          0.790977 |
| ONTUSDT    |        642 |                  7.13349  |        0.0109034  |       0.0476802  |                        1456.36  |                              1420.29  |                    0.989097 |                          0.939252 |
| ILVUSDT    |        250 |                  2.77786  |        0.052      |       0.00593241 |                         567.191 |                               325.442 |                    0.948    |                          0.212    |
| PORTALUSDT |        192 |                  2.13338  |        0.0364583  |       0.0075484  |                         375.703 |                               292.339 |                    1        |                          0.729167 |
| COMPUSDT   |         81 |                  0.900001 |        0.0123457  |       0.0276982  |                         183.104 |                               183.101 |                    1        |                          1        |
| KSMUSDT    |        135 |                  1.5      |        0.00740741 |       0.00903359 |                         162.263 |                               114.501 |                    1        |                          0.755556 |
| BANDUSDT   |         81 |                  0.900018 |        0.0246914  |       0.00301922 |                         148.225 |                               125.833 |                    1        |                          0.62963  |

Worst pairs under the same lens:

| symbol     |   episodes |   ghost_start_pct |   coverage_ratio |   d300s_h14400s_sum_net_pnl_usd |   d300s_h14400s_fixed_sum_net_pnl_usd |   d300s_h86400s_sum_net_pnl_usd |   d300s_h86400s_fixed_sum_net_pnl_usd |
|:-----------|-----------:|------------------:|-----------------:|--------------------------------:|--------------------------------------:|--------------------------------:|--------------------------------------:|
| PYRUSDT    |        540 |        0.087037   |       0.0062594  |                    1655.95      |                          1229.38      |                    1914.39      |                          1329.38      |
| AXLUSDT    |        665 |        0.0345865  |       0.0266575  |                    1535.49      |                          1299.64      |                    1541.03      |                          1415.2       |
| ONTUSDT    |        642 |        0.0109034  |       0.0476802  |                    1456.36      |                          1420.29      |                    1465.87      |                          1438.87      |
| ILVUSDT    |        250 |        0.052      |       0.00593241 |                     567.191     |                           325.442     |                     581.783     |                           326.315     |
| PORTALUSDT |        192 |        0.0364583  |       0.0075484  |                     375.703     |                           292.339     |                     383.145     |                           307.129     |
| COMPUSDT   |         81 |        0.0123457  |       0.0276982  |                     183.104     |                           183.101     |                     183.12      |                           183.11      |
| KSMUSDT    |        135 |        0.00740741 |       0.00903359 |                     162.263     |                           114.501     |                     162.902     |                           114.568     |
| BANDUSDT   |         81 |        0.0246914  |       0.00301922 |                     148.225     |                           125.833     |                     150.515     |                           125.846     |
| STGUSDT    |         38 |        0          |       0.0420949  |                     128.588     |                           122.095     |                     128.593     |                           128.585     |
| METISUSDT  |         62 |        0.0483871  |       0.00961771 |                      66.7397    |                            66.7184    |                      66.7397    |                            66.7211    |
| XVSUSDT    |         39 |        0.025641   |       0.00645948 |                      24.5725    |                            23.4098    |                      24.6227    |                            24.5868    |
| THETAUSDT  |         18 |        0          |       0.00925451 |                      18.8215    |                            18.8048    |                      18.8215    |                            18.8112    |
| HIGHUSDT   |         11 |        0.0909091  |       0.00232974 |                       1.7094    |                             1.56498   |                       1.73582   |                             1.58106   |
| ROSEUSDT   |          1 |        0          |       0.0523981  |                      -0.0313282 |                            -0.0315695 |                      -0.0313282 |                            -0.031318  |
| DEXEUSDT   |          1 |        0          |       0.0391994  |                      -0.0812347 |                            -0.0812347 |                      -0.0812347 |                            -0.0812347 |

## Bottom Line

H2 is directionally true but economically fragile: extreme spikes often compress, yet much of the headline edge comes from non-causal baseline choices, bar-level double counting, sparse-clock sampling, and assuming fills at the peak.
Once the structural level is frozen at entry instead of allowed to drift, the broad `$185/day` machine disappears. What remains is a selective, liquidity-aware excursion trade in a handful of pairs.
