---
name: quant-governance
description: "Top-level governor for all quant research and deployment. Enforces pipeline sequencing, hard gates, stop conditions, anti-overfitting checks, and prevents premature parallel development. Load this FIRST for any quant task."
---

# Quant Governance — Pipeline & Gates

Load this before any other quant skill. It governs sequencing, gates, and when to say NO.

---

## GATE 0 — BEFORE ANY BUILD OR CAPITAL (Alberto 2026-07-05, FIRED offense if skipped)

Full text in the charter Hard Rules + `projects/quant/lessons/2026-07-05-retro-rebuilt-existing-and-proposed-unvalidated-live`.

**A. ORIENTATION GATE** — the FIRST action on ANY build/sim/deploy/tool task, before any other tool call. Post +
fill, cannot proceed until done:
- EXISTS?  `grep -r <capability>` + `brain query <capability>` + read `projects/quant/state` (paste real results).
- GAP:     the specific thing genuinely missing (not "a cleaner version").
- CHEAPEST PATH: configure existing X / extend Y / build Z (only if truly absent) + token estimate.
Battle-tested version exists (e.g. `hl_copy_trader_v17.py`) -> CONFIGURE it, never rebuild. Plan says "rebuild a
live system" -> STOP, verify with Alberto. Subagent output "port existing X" -> STOP, X already does it.

**B. VALIDATION LEDGER** — required before proposing ANY live capital. Every row PASS on TRUSTED data or don't
propose (name the failing gate): `[edge on TRUSTED data PASS/FAIL+number] [walk-forward/OOS] [Fable+codex] [Alberto GO]`.
Liveness is NOT edge. A broken input (e.g. 30-55% position-data gaps) = everything downstream UNTRUSTED = auto-FAIL;
STOP and fix the input, do NOT reframe around it.

Root cause prevented: equating output with progress, and routing around blockers (existing code / broken data) with
rationalization instead of stopping. (RULE 0 below encodes the reuse half; GATE 0 adds the mandatory first-step form.)

---

## RESULT PRE-FLIGHT CHECKLIST (BINDING — run before ANY number reaches Alberto)

Full checklist + origin: brain `projects/quant/lessons/sim-result-preflight-checklist`. READ IT.
Before reporting any sim/backtest result that could feed a decision, PASTE the 9-point pass/fail + a confidence
rating (exploratory 1-2/5 vs confirmatory 4-5/5). If you cannot fill it, DO NOT report the number.

1. Sanity anchors pass (fee/cost decomposition = expected bps; placebo/null strongly negative; reconciles with a
   validated baseline).
2. No look-ahead (causal/trailing params only; OOS split run and effect survives).
3. Sample size + bootstrap/CI stated (no conclusion on n<30 without a CI).
4. No censoring/survivorship (all units included, or censored ones reported as a separate line — NEVER silent-drop).
5. Metric actually discriminates the hypotheses (not scale-invariant/tautological) AND does not hide risk (report
   exposure/tail, not just per-unit return).
6. Apples-to-apples (arms matched on the confound, e.g. exposure-matched, before any advantage claim).
7. Selection bias stated (ex-post winner set = exploratory).
8. Independent review: Fable (+ codex R13 when available) reviewed METHOD and RESULT before concluding.
9. Confidence rating stated explicitly to Alberto.

0. **USE THE CANONICAL HARNESS + THE BATTLE-TESTED LIVE ENGINE — never rebuild from scratch.**
   - LIVE EXECUTION: the copy bot is `strategies/live/hl_copy_trader_v17.py` — battle-tested WEEKS live across all
     dexes (main + builder xyz/flx), all coins (~250-coin whitelist), FIRST_CLOSE mirror-exit, disaster stops
     (sl_bps, per-coin kills, gross_backstop_x), no-pyramid (max_addon 1), cross-main/isolated-xyz, heavily
     codex-reviewed, built-in `--shadow`. A new strategy/probe is a CONFIG on v17 (`--config <json> --shadow`),
     NOT a new runner. NEVER rebuild the live executor from scratch. (Alberto TG10810, 2026-07-05: "why build from
     scratch instead of what we already built and tested for weeks... everything was already there and battle
     tested." I burned ~700k tokens + 6 subagents rebuilding copy_a when v17 already did it. There is NO testnet;
     v17's weeks of live running IS the battle-testing.) BEFORE writing ANY executor code: read v17 + its live
     config, confirm what it already does, and default to configuring it.
   - BACKTEST/SIM: test copy policy via the V15 M1-M7 harness + `copy_edge` (`research/v15/execution_model.py` =
     canonical slip/fees/latency). A /tmp one-off sim is BANNED for anything that feeds a decision — it
     reintroduces solved bugs (double fees, look-ahead, dropped rounds). (Alberto TG10778: "why are you building
     custom sims when we have an entire copy backtesting infrastructure.")

Repeat offenders (2026-07-05): reported a buggy column; claimed a +117bps edge that was in-sample look-ahead;
claimed "half exposure" that was a base-calibration artifact with identical tails; OOS silently dropped 44% of
rounds; hand-rolled /tmp sims instead of the shadow harness. Alberto: "think plan verify with fable and codex
before jumping to conclusions."

---

## The Pipeline (non-negotiable order)

```
IDEA -> SPEC -> BACKTEST -> ROBUSTNESS -> EXECUTION VALIDATION -> DEPLOY
```

Each arrow is a gate. You cannot skip one. You cannot run two simultaneously.

### What each phase answers

| Phase | Question |
|-------|----------|
| Idea | Is the inefficiency real and exploitable? |
| Spec | Is the hypothesis precise enough to test? |
| Backtest | Does it work on historical data across regimes? |
| Robustness | Does it survive stress, slippage, and Monte Carlo? |
| Execution validation | Can I actually get filled at the right price? |
| Deploy | Is the live system behaving as expected? |

---

## Current State

Check `app/engines/strategy_registry.py` for registered engines and their configuration.
All engines MUST have `deployment_mode="hb_native"`. The controller IS the strategy.
Check MongoDB `pair_historical` for current verdicts per engine.
Deploy: `python cli.py deploy --engine EN`. Status: `python cli.py bot-status --engine EN`.
Data pipeline runs on Mac Mini via TaskOrchestrator DAG (`config/hermes_pipeline.yml`).
Bots run as HB Docker containers via `quants-lab/hummingbot:demo` image.

---

## Hard Gates (STOP if not met -- no exceptions)

### Backtest -> Robustness
- Positive expectancy in >= 2 independent regime windows
- Trade count >= 30 in the primary validation window
- Long/short split analyzed separately

### Robustness -> Execution Validation
- Passes slippage tiers: 2 bps (must), 5 bps (should), 10 bps (acceptable to fail)
- No parameter boundary solutions in optimization
- Monte Carlo ruin probability < 1% at intended sizing
- Strategy identity preserved (trade count, time-in-market within 2x of train)

### Execution Validation -> Deploy
- >= 20 real signals resolved in paper trading
- Avg slippage < 15 bps
- Edge after slippage positive
- No hard stop conditions triggered

### Deploy -> Scale / Next Engine
- >= 30 live trades on current engine
- Execution metrics stable over >= 2 weeks
- No regime mismatch between live and backtest expectations

**Violation = STOP. Investigate the gate failure first.**


---

## Stop Conditions (pause all activity if triggered)

### Signal-level stops
- Backtest OOS contradicts validation by > 30% on Sharpe -> re-examine data split
- Top-5 trades contribute > 80% of PnL -> outlier dependence, not a system
- Max consecutive losses > 5 -> review regime alignment

### Execution-level stops
- Avg slippage > 15 bps -> PAUSE
- > 30% trades in danger bucket (> 20 bps) -> PAUSE
- Fill rate < 70% -> PAUSE
- Consistent missed fills on winners -> flag for manual review
- Edge after slippage flips negative -> NOT DEPLOYABLE

### System-level stops
- Any NEVER rule violation -> full stop, review
- Live behavior deviates from backtest by > 30% on win rate -> invalidate, re-examine
- Regime mismatch: strategy firing heavily in wrong regime -> review

---

## Strategy Identity Guard

After optimization or any parameter change, verify ALL of:
- Trade count change < 2x (train vs validation)
- Time-in-market not materially different (< 4x change)
- Signal definition unchanged (same entry conditions, same exit logic)
- Long/short ratio comparable

**If any violated -> this is a NEW strategy. Restart validation.**

---

## Anti-Overfitting Checklist

Run before declaring any optimization "done":
- [ ] Do best params hit a search space boundary? -> expand range, rerun
- [ ] Does performance collapse OOS (val vs train delta > 0.3 Sharpe)? -> overfit
- [ ] Is improvement driven by increased trade count? -> not edge, just exposure
- [ ] Is PF stable across all regime windows? -> if not, regime-specific artifact
- [ ] Do top-10 Optuna trials show parameter clustering? -> if scatter, unstable

**>= 2 yes answers -> likely overfit. Do not proceed.**

---

## Regime Enforcement

Every result report MUST include:
- Performance breakdown by regime (trend, range, shock)
- % PnL contribution by regime
- Failure regime identified explicitly

**A result without regime breakdown is incomplete and cannot be used for deployment decisions.**

---

## The One Question

Before any action, ask:
**"Does this move us forward on the current phase, or is it jumping ahead?"**

If jumping ahead -> stop. Finish the current phase first.