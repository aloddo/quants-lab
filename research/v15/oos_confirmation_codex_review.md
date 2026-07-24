The relaxation is target-chasing. “Mean > 0” with only 10 journeys is barely a confirmation test; choosing it after seeing that the strict gate produced only three also consumes the current OOS sample as tuning data.

### 1. The 15-wallet set is probably noise-heavy

Under the stated null:

\[
E[R_0]=35(0.31)=10.85
\]

You observed 15 positives, so the crude plug-in false-discovery proportion is:

\[
\widehat{FDP}=\frac{10.85}{15}=72\%
\]

That suggests roughly 11 false positives and only four excess positives. It is not a formally identified FDR—true wallets can also fail the sign test—but it is the relevant warning estimate.

Moreover:

\[
P\{\mathrm{Binomial}(35,0.31)\ge15\}=9.3\%
\]

So the enrichment is not significant at 5%. That calculation already gives you the benefit of assuming independent candidates and a known 31% null rate; wallet/fold dependence and uncertainty in the baseline generally weaken it.

Conclusion: you cannot credibly treat the 15 as 15 validated wallets. The data are compatible with no selection skill.

### 2. The lower-bound gate is better, but not fully correct

`mean − 1.645×SE > 0` is a one-sided 5% test. It is vastly more defensible than a positive sign, but it does not control multiplicity across 35 wallets.

Under 35 true nulls:

- Expected false passes at \(p<0.05\): \(35(0.05)=1.75\)
- Probability of getting at least three passes: approximately 25.4%

Therefore, “three passed individual 95% lower bounds” is not itself strong family-level evidence. Actual p-values matter: three at 0.049 are weak; three at 0.0001 are strong.

And yes, power is poor: ten journeys over 14 days gives an unstable SE, especially with fat tails and correlated positions. Use journey-level returns and fold/time-block uncertainty, not an IID position-level normal SE.

### 3. Pool OOS windows—but only cross-fitted ones

Pooling is the right direction provided there is no retrospective leakage:

- A wallet-fold observation may be included only if the wallet qualified using information available before that fold.
- Do not select today’s wallets and backfill their earlier “OOS” performance. That is retrospective in-sample selection.
- Aggregate non-overlapping test windows and estimate uncertainty by fold/time blocks.
- Since the gate was changed after seeing these results, freeze the new procedure and validate it on an untouched future window before deployment.

Pooling should tighten bounds for persistent edges. It will not necessarily produce ten valid wallets—and it must not be engineered to do so.

### Exact recommended gate

For each wallet:

1. Require at least four eligible, non-overlapping OOS folds and at least 50 total OOS journeys.
2. Test the predeclared hypothesis  
   \[
   H_0:\mu_{\text{net}}\le\delta
   \]
   where \(\delta\) is zero plus an explicit slippage/model-risk margin.
3. Calculate one-sided p-values using fold-block/bootstrap or random-effects inference.
4. Apply Benjamini–Hochberg at \(q=10\%\) across every eligible wallet.
   - With 35 wallets, ten discoveries require \(p_{(10)}\le10(0.10)/35=0.0286\).
   - That corresponds to an expected false-discovery budget of at most roughly one among ten, under BH assumptions.
5. Use empirical-Bayes shrinkage for ranking and position sizing, not as a way to override failed confirmation.
6. Require stability across a prespecified early/late or pre/post-regime split.
7. If fewer than ten pass, deploy fewer than ten, wait for more data, or diversify into another independently validated strategy. “Need ten” is not statistical evidence.

Option **(c)**: pooled, genuinely cross-fitted OOS evidence plus multiplicity control and shrinkage. Option (a) is close, but an unadjusted lower-bound gate is insufficient. Option (b) implies an estimated noise burden around 72% and is unsuitable as a production confirmation rule.

**RELAXATION-UNSOUND—POOL VALID MULTI-WINDOW OOS DATA, APPLY BLOCK-ROBUST BH AT q=10%, AND DO NOT FORCE THE OUTPUT TO TEN WALLETS.**
