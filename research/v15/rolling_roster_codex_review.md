ROSTER-ONLY-IF-MAKER

A live rolling roster does not solve the cost barrier. It only replaces historical uncertainty with paid live uncertainty. Adaptive selection can discover rare winners, but at taker costs the discovery economics are overwhelmingly unfavorable unless positive wallets are much more common, stronger, and longer-lived than the existing evidence suggests.

## 1. The killer economics

Let:

- \(b\) = expected loss from an ordinary wallet
- \(a\) = net edge from a genuinely good wallet
- \(\pi\) = fraction of candidates that are genuinely good

If an ordinary wallet has approximately zero gross directional edge, its net edge is at best:

\[
-b=-8.64\text{ bps}
\]

This excludes latency, slippage, funding, and copy mismatch, so it is optimistic.

An equally weighted exploratory roster is positive only when:

\[
\pi a-(1-\pi)b>0
\quad\Rightarrow\quad
\pi>\frac{b}{a+b}
\]

| Real winner’s net edge | Required winner fraction |
|---:|---:|
| +5 bps | 63.3% |
| +10 bps | 46.4% |
| +20 bps | 30.2% |

That is not a “rare-winner” strategy. It requires an implausibly rich candidate pool.

If the cheap prior filter somehow reduces rejected wallets’ expected loss from −8.64 to −3 bps, the required fractions are still 37.5%, 23.1%, and 13.0%, respectively.

Adaptive elimination helps only if winners generate enough future turnover to repay all rejected candidates. If one winner is found per \(1/\pi\) candidates, and each rejected candidate is tested for turnover \(T\), a winner with net edge \(a\) must subsequently receive approximately:

\[
\frac{W}{T} >
\frac{b(1-\pi)}{\pi a}
\]

times as much exploitation turnover.

For a +10 bps winner:

| True winner prevalence | Required winner exploitation/test turnover |
|---:|---:|
| 1% | >85.5× |
| 5% | >16.4× |
| 10% | >7.8× |

At 1% prevalence and +5 bps net edge, the requirement rises to roughly 171×. This also assumes the edge remains stationary for that entire exploitation lifetime.

That is the central failure mode: rare edges must be unusually persistent, yet short-lived wallet edges are precisely what motivate the rolling-roster proposal.

The historical result does not prove \(\pi=0\). But 0/1,832 confirmations gives no basis for assuming the high prevalence or long winner lifetime required by taker economics.

## 2. Exploration budget for a $950 book

I would cap any live discovery experiment at 2% of the book:

- Total campaign loss cap: **$19**
- Daily exploration stop: **$2.85** or 0.30%
- Per-wallet loss cap: **$1.90** or 0.20%
- Maximum simultaneous candidates: **10**
- Maximum explorer gross exposure: **$95**
- Per-wallet exposure: approximately **$10**, or the smallest executable venue order
- No leverage or pyramiding in the exploration sleeve

The $19 fee-only turnover budget is:

\[
\$19 / 0.000864 = \$21,991
\]

of completed round-trip notional.

At $10 per trade, that buys only about **2,200 completed copied trades across the entire experiment**—roughly 220 trades per wallet for ten candidates.

The ongoing bleed is material:

| Roster | Trades/wallet/day | Ticket | Fee bleed/day | 30-day bleed |
|---:|---:|---:|---:|---:|
| 10 wallets | 20 | $5 | $0.86 | $25.92 |
| 10 wallets | 20 | $10 | $1.73 | $51.84 |
| 20 wallets | 20 | $10 | $3.46 | $103.68 |

The last case burns almost 11% of the book monthly before adverse selection or trading losses.

A $1.90 wallet cap safely bounds damage, but it does not buy reliable learning. That cap is likely to eliminate many viable wallets through variance before their mean becomes measurable. This is an unavoidable tradeoff: at $950, taker exploration can be risk-bounded or statistically informative, but generally not both.

## 3. Graduation and elimination

Classical SPRT is directionally appropriate because it handles continuous monitoring, but raw-trade SPRT assumptions are wrong here:

- Wallet trades are serially correlated.
- Multiple fills can belong to one position episode.
- Volatility and holding periods vary substantially.
- Wallet regimes change.
- Thousands of wallets create severe repeated-testing and winner’s-curse problems.

Use independent-ish position episodes or daily blocks as observations, normalized to net bps per unit notional. Do not count every fill as independent evidence.

A defensible graduation hypothesis would be:

\[
H_0:\mu\le0
\qquad
H_1:\mu\ge+5\text{ bps net}
\]

I would use approximately:

- Per-candidate \(\alpha=0.001\)
- \(\beta=0.20\)
- Online FDR or alpha-spending across the continuous candidate stream
- No resetting a failed test and trying again on the same data

Even \(\alpha=0.001\) produces about 1.8 expected false positives over 1,832 all-null candidates, so simple independent SPRTs remain insufficient.

For normally distributed episode returns, the approximate fixed-sample requirement is:

\[
n \approx
\left(z_{1-\alpha}+z_{1-\beta}\right)^2
\frac{\sigma^2}{\delta^2}
\]

With \(\alpha=0.001\) and \(\beta=0.20\):

| Episode standard deviation | Target net edge | Effective observations |
|---:|---:|---:|
| 50 bps | +5 bps | ~1,550 |
| 100 bps | +5 bps | ~6,200 |
| 100 bps | +10 bps | ~1,550 |

“Effective” means after accounting for autocorrelation. Raw trade count could be materially higher.

A normal SPRT with \(H_0=0\), \(H_1=+5\), \(\alpha=.001\), and \(\beta=.20\) would need approximately 5,350 effective observations on average when the true edge is exactly +5 bps and episode volatility is 100 bps.

It would eliminate a truly −8.64 bps wallet in roughly 290 effective observations. At a $10 ticket, that wallet loses about $2.50 in expectation before elimination. Ten ordinary wallets consume approximately $25—already beyond the proposed total experiment cap.

Therefore:

- A loss cap is a risk rule, not proof that a wallet is bad.
- SPRT is useful for formal promotion but cannot make weak edges measurable cheaply.
- Graduation should require sequential evidence plus calendar and regime diversity.
- Size should increase in stages only on subsequent data, so discovery luck is not immediately leveraged.
- Any detected decay should de-graduate the wallet quickly, but prior evidence should not be discarded completely.

## 4. Better online formulation

This is not primarily a bandit problem because much of the reward is counterfactually observable. Once a wallet trades, you can reconstruct what a latency-aware taker copy would have earned without actually placing the trade.

The better system is:

1. **Shadow-observe every candidate.** Compute latency-realistic, cost-adjusted counterfactual returns without paying taker fees.
2. **Use a hierarchical contextual model.** Pool information across wallet age, coin liquidity, turnover, holding time, quick-close behavior, direction, volatility regime, crowding, and agreement with other wallets.
3. **Give new wallets a feature-conditioned prior.** New wallets enter immediately, but lack of history produces uncertainty—not automatic live capital.
4. **Include cash/no-trade as an explicit arm.** A bandit without a reject option will allocate among all-negative wallets.
5. **Use live micro-orders only to estimate what shadow replay cannot:** maker queue position, fills, partial fills, adverse selection, latency, and cleanup costs.
6. **Aggregate signals before execution.** Net opposing wallets and trade only high-conviction consensus. One-to-one copying maximizes turnover and therefore maximizes the cost problem.

Thompson sampling alone is dangerous: it deliberately samples uncertain arms, exactly where the fee bleed lives. Hierarchical Thompson sampling with a cash arm, shadow observations, turnover penalties, and hard risk budgets is reasonable after execution economics have been proven.

## 5. Maker execution

If maker execution truly lowers the effective round-trip barrier from 8.64 to 4.32 bps, the equal-roster thresholds become:

| Winner’s maker net edge | Required winner fraction |
|---:|---:|
| +5 bps | 46.4% |
| +10 bps | 30.2% |
| +20 bps | 17.8% |

That is better, but it does not automatically validate the roster.

The relevant maker reward is:

\[
p_{\text{fill}}
\left(
\text{gross edge conditional on fill}
-\text{maker cost}
-\text{adverse selection}
-\text{cleanup cost}
\right)
\]

Maker fills are not random. They tend to occur when price moves against the resting order, while favorable moves may escape unfilled. Partial fills followed by taker cleanup can erase the apparent saving.

Consequently, “maker-only” should mean:

- No taker fallback except under an explicitly budgeted cleanup rule.
- Queue- and latency-aware fill simulation.
- Live micro-probes to calibrate fill probability.
- Evaluation based on edge conditional on fill, not hypothetical signal return.
- Graduation only if fill-adjusted net edge clears zero by a meaningful margin—ideally at least 3–5 bps.

Maker is a prerequisite experiment, not yet a proven solution.

## Bottom line

The CEO is right that historical significance gating excludes potentially valuable new wallets and can be underpowered. But the proposed cure confuses lack of statistical power with permission to pay for more noisy samples. Live selection does not manufacture edge; at taker cost it buys essentially the same evidence while steadily consuming the book.

The sound design is a rolling **shadow roster**, combined with hierarchical/contextual scoring and selective live maker probes. A broad live taker roster should not be built.

The single most important design decision is:

**Ban taker-funded exploration. Prove positive fill-adjusted maker economics—or another structural turnover/cost reduction—before allowing the roster to deploy meaningful capital.**
