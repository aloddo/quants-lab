---
name: copy-research
description: "V15 copy-trading research & validation harness. The standard infra for testing ANY copy-wallet hypothesis (WHO to copy / HOW to copy) end-to-end: m02 event source -> sim -> canonical execution model -> 6-criteria verdict -> codex gate. Load for any copy-trading research."
---

# V15 Copy-Trading Research & Validation Harness

BINDING (Alberto 2026-06-10): V15 IS the standard infra for ALL copy-trading research + validation.
Every copy hypothesis goes through this harness, never a one-off script. Specs:
brain `projects/quant/v15/copy-research-infra` + `projects/quant/v15/execution-model`.

## The pipeline

1. **Events / universe** — `research/v15/leadlag_clean_rank_sim.py::load_events_from_m02`
   reads `app/data/v15/m02_actions.parquet` (reconstructed ENTRY/EXIT, full ~18k universe, Dec 2025+,
   NO API). Compact `array.array` store (~12B/event) + `bisect` windowing -> full 18k in ~8 min @ ~1-2GB.
2. **Sim** — `leadlag_clean_rank_sim.py`: causal trailing rank -> forward copy (FIFO lots) vs
   matched-null + decision-anchored beta. Codex-reviewed for look-ahead / leakage / null fairness.
   Reusable: swap the SELECTION rule for a new WHO hypothesis; keep the null + beta controls.
3. **Execution (MANDATORY)** — `research/v15/execution_model.py`: canonical per-coin slippage (L2 calib)
   + real HL fees + latency. NEVER hardcode costs in a sim. Change a HOW hypothesis HERE only.
4. **Verdict** — `research/v15/leadlag_clean_rank_report.py`: codex's 6 pass/fail criteria + VERDICT.
5. **Gate** — codex review of BOTH sim logic AND result before any deploy/kill is final (rule #13).

## Run it

```bash
# full universe, full history, 1-hourly
python research/v15/leadlag_clean_rank_sim.py --source m02 \
  --start 2025-12-01 --end 2026-05-23 --decision-step-hours 1 \
  --per-worker-gb 2.0 --headroom-gb 1.5 --out app/data/v15/<name>.parquet
python research/v15/leadlag_clean_rank_report.py --in app/data/v15/<name>.parquet
# slip-default sensitivity (codex): rerun with --slip-default-bps 2.4 and 7.0; verdict must be robust
```

## Hard rules (learned the hard way)

- **Memory**: streaming I/O + `plan_memory_budget` (aborts if box too tight). NO load-all-in-RAM
  (the candles 1.87GB->7GB and 86M-tuple peaks were this). Compact typed arrays, not list-of-dicts.
- **Execution**: canonical model only; slippage on entry AND exit; a skill benchmark pays the SAME
  execution so it cancels in top-minus-benchmark. Report `calibrated_share()` + slip sensitivity.
- **Look-ahead**: at decision t, rank uses only fills <= t; closes time-boxed to the window (a future
  close must NOT close an in-window lot — that faked the +62bps once).
- **Survivorship**: m02 universe is active-into-window -> fine for a conservative KILL, NOT for trusting
  a POSITIVE (needs a point-in-time-eligible rerun + maker/real execution before deploy).

## Two decisive diagnostics (find the binding constraint)

- **HOW** = ORACLE UPPER BOUND: copy hindsight-best wallets under real execution = the ceiling. If
  unprofitable, HOW is the wall; no selection saves it.
- **WHO** = ALPHA PERSISTENCE: does past beta-stripped alpha predict future. Only run if the ceiling is high.
Detail: brain `projects/quant/research/2026-06-10-copy-wallet-who-vs-how`.

## Status (2026-06-10): all copy horizons negative (static -12.9%, cross-week null, short-horizon KILL,
clean-rank full-18k KILL). Vehicle near-exhausted; pivot decision pending Alberto.
