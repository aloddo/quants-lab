- [P1] [line 1315](/Users/hermes/quants-lab/research/v15/v15_m07_engine.py:1315) — `peak_notional` samples only post-fill surviving size. Full closes return at line 1249; trims use reduced size; flips close the old leg first; liquidation bypasses this code. Example: long opened at 100 and closed/flipped at 200 reports peak 100, making `r_i` roughly twice correct even under fill-event-only sampling. Sample pre-fill `abs(p.szi)*mark` on every fill, including `_liq_close`.

- [P1] [line 1551](/Users/hermes/quants-lab/research/v15/v15_m07_engine.py:1551), [1576](/Users/hermes/quants-lab/research/v15/v15_m07_engine.py:1576), [1669](/Users/hermes/quants-lab/research/v15/v15_m07_engine.py:1669), [1696](/Users/hermes/quants-lab/research/v15/v15_m07_engine.py:1696), [566](/Users/hermes/quants-lab/research/v15/v15_m07_engine.py:566) — backstop, liquidation, ruin, and residual-trail closes omit the available timestamp, emitting `exit_ts=None`. This breaks cutoff/holding-time use precisely for forced-loss records. Pass `ts_ms`/`ts`.

- [P2] [line 1671](/Users/hermes/quants-lab/research/v15/v15_m07_engine.py:1671) — a 20% liquidation reduction survives without incrementing `n_trim`. Actual fill topology is understated for partially liquidated positions.

- [P2] [line 964](/Users/hermes/quants-lab/research/v15/v15_m07_engine.py:964) — causal carry-ins and supplied `start_state` positions never call `_rt_open`; if later closed, they emit no position record. This preserves aggregate reconciliation because legacy aggregates also exclude them, but contradicts the advertised `entry_ts=None` carry-in path and “every closed round-trip” coverage. Do not simply seed `_rt` unless aggregate behavior is deliberately preserved separately.

- [P2] [line 1926](/Users/hermes/quants-lab/research/v15/v15_m07_engine.py:1926) — when no round trips close, the writer produces a zero-column parquet. Any consumer selecting expected position columns will fail. Provide a fixed empty schema.

No aggregate arithmetic regression found: for tracked round trips, the exact same `pnl` feeds both `realized_pnl_total` and the emitted record, so count/PnL reconciliation holds. Underwater logic at [line 1259](/Users/hermes/quants-lab/research/v15/v15_m07_engine.py:1259) is correct: pre-update VWAP, long `mark < entry_px`, short `mark > entry_px`.

DO-NOT-SHIP
