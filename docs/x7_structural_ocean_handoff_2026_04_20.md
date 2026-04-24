# X7 Structural Non-Flow Research Handoff (2026-04-20)

## Objective
Shift X7 away from pure flow metrics into structural on-chain state variables that can change effective supply/demand.

## What Was Built
New structural research pipeline:
- `scripts/x7_structural_nonflow_research.py`

Core structural families implemented:
1. **Supply elasticity**: mint/burn/sink and net issuance state.
2. **Holder topology**: participant concentration, top-holder share, HHI.
3. **CEX-float structure**: exchange touch/inflow/outflow share + sender/receiver topology.

## Dune Execution Status
Completed successfully:
- `x7_structural_supply_elasticity` (execution `01KPNYVWG0P4ZYABZ1B8F4D9WH`, 10,775 rows)
- `x7_structural_holder_topology` (execution `01KPNYZN1XF0YE7QJ7YG7X4RN6`, 10,775 rows)

Failed / constrained:
- Original heavy dormancy query timed out after 30 minutes (execution `01KPNZ750ZVFRQ5WZR76HXSCNM`, ~213 credits consumed).
- Subsequent Dune calls hit credit/billing gate (`402 Payment Required`) for new queries/results.

## Main Artifacts
Primary structural panel and results:
- `app/data/cache/onchain_x7_structural/20260420_180341_structural_2q_feature_panel.csv`
- `app/data/cache/onchain_x7_structural/20260420_180420_structural_2q_no_vol_pooled_ic.csv`
- `app/data/cache/onchain_x7_structural/20260420_180420_structural_2q_no_vol_asset_ic.csv`
- `app/data/cache/onchain_x7_structural/20260420_180420_structural_2q_no_vol_selected.csv`
- `app/data/cache/onchain_x7_structural/20260420_180420_structural_2q_no_vol_summary.json`

Additional validation:
- `app/data/cache/onchain_x7_structural/20260420_180420_structural_24h_feature_wf_scan.csv`
- `app/data/cache/onchain_x7_structural/20260420_180420_structural_net_issuance_z72_robustness.csv`
- `app/data/cache/onchain_x7_structural/20260420_180420_structural_net_issuance_z72_per_asset.csv`
- `app/data/cache/onchain_x7_structural/20260420_180420_structural_gate_doge_leadlag.csv`
- `app/data/cache/onchain_x7_structural/20260420_180420_structural_gate_nonoverlap_doge.csv`

## Key Findings
1. **Structural signal family is real, mostly at 24h horizon, but weak-to-moderate effect size.**
- No-vol pooled IC run: `177` significant rows (`p<0.05, N>100`), `93` survive FDR (`q<0.10`).
- Strong recurring features:
  - `transfer_count_z72`, `transfer_count_z24` (activity/structure proxy)
  - `burned_to_dead_usd_z24`, `burned_to_dead_usd_z168`
  - `net_issuance_ratio_z24`, `net_issuance_usd_z24`
  - `participant_hhi`, `top*_participant_share` variants

2. **Cross-asset behavior is heterogeneous (sign flips exist).**
- BTC: `burned_to_dead_usd_z168` positive vs 24h returns.
- ETH: mint/burn counts (`mint_txn_count`, `burn_or_sink_txn_count`) negative vs 24h.
- SOL: sink/share metrics (`sink_vs_issuance_share_z72`) positive vs 24h.
- XRP: participant/transfer size metrics negative vs 24h.
- DOGE: burn/sink count features negative vs 24h.

3. **Single-feature structural strategy candidate emerged but is not statistically robust yet.**
- Candidate: `net_issuance_usd_z72` (24h horizon, daily L/S top2-bottom2).
- 30-day OOS scan:
  - mean net/day ~`0.221%`, annualized Sharpe ~`4.42`
  - but `t-test p ~ 0.223` (not significant with current sample length).
- Sensitivity:
  - best at lag=1; degrades materially at lag>=2.
  - survives only lower all-in costs; decays near higher fee/slippage assumptions.

4. **Structural gating did not rescue realistic subsecond DOGE lead-lag execution.**
- Bar-level proxy improved in some issuance-negative regimes.
- Under strict non-overlap event simulation (30s hold, round-trip costs), all tested structural gates remained negative.

## Practical Implications for X7
Use structural signals as **slow regime layer** (4h/24h) rather than direct micro-execution trigger:
- Good: feature ranking, leverage scaling, gross exposure throttle.
- Bad: directly forcing HFT entries from structural state.

## Recommended Next Validation (Engineer)
1. Recharge/restore Dune credits; rerun full 3-query pipeline with updated lightweight third query in `x7_structural_nonflow_research.py`.
2. Extend lookback to 365d for structural features to get statistical power (current 90d is too short for robust claim).
3. Build hierarchical model:
   - Layer A (daily): structural regime score.
   - Layer B (intra-day): execution alpha (existing lead-lag/microstructure signals).
   - Trade only when A and B align.
4. Add multiple-hypothesis discipline:
   - FDR threshold + holdout period + post-selection test.
5. For live:
   - treat `net_issuance_usd_z72` and burn/sink features as **risk multipliers**, not standalone directional bets, until 365d validation is complete.
