---
name: quant-governance
description: "Top-level governor for all quant research and deployment. Enforces pipeline sequencing, hard gates, stop conditions, anti-overfitting checks, and prevents premature parallel development. Load this FIRST for any quant task."
---

# Quant Governance — Pipeline & Gates

Load this before any other quant skill. It governs sequencing, gates, and when to say NO.

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

Check `app/engines/registry.py` for registered engines and their configuration.
Check MongoDB `pair_historical` for current verdicts per engine.
Check `candidates` collection for paper trading results.
Pipeline runs on Mac Mini via TaskOrchestrator DAG (`config/hermes_pipeline.yml`).

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