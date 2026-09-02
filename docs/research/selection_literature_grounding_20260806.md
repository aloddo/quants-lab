# Wallet-Selection Framework for Copy-Trading: Literature Grounding

Context: copying Hyperliquid perp traders, faithful entry/exit replication, fixed notional per position, ~$470 equity, 8.64 bps RT taker fees.

## 1. Performance persistence is short-horizon and concentrated in losers

**1.1** Carhart (1997, *Journal of Finance*, "On Persistence in Mutual Fund Performance"): almost all "hot hands" persistence is explained by momentum loadings and expenses; the only robust persistence is the *continued underperformance of the worst funds*. **Lever: use the loser leg as the strong signal - aggressively and permanently exclude wallets with demonstrated negative expectancy; treat the winner leg as weak evidence.**

**1.2** Hendricks, Patel & Zeckhauser (1993, *Journal of Finance*, "Hot Hands in Mutual Funds"): persistence exists but at a ~1-year horizon and decays; the "icy hands" (repeat-loser) effect is stronger than the hot-hands effect. **Lever: refresh the roster on a short cadence rather than annual re-selection; expect winner rank to decay within one evaluation period.**

**1.3** Agarwal & Naik (2000, *JFQA*, "Multi-Period Performance Persistence Analysis of Hedge Funds"): maximum hedge-fund persistence at the *quarterly* horizon, largely gone at annual horizons in the multi-period framework. Getmansky, Lo & Makarov (2004, *JFE*) show even that quarterly persistence is partly illiquidity-induced serial correlation, i.e., a data artifact, not skill. Jagannathan, Malakhov & Novikov (2010, *JF*) find some 3-year persistence, but only in the top funds of the *relative* cross-section. **Lever: lookback of roughly 60-120 days for ranking with monthly-or-faster refresh; and rank cross-sectionally (relative), never on absolute PnL thresholds alone.**

## 2. Multiple testing: you are running a factor zoo over thousands of wallets

**2.1** Harvey, Liu & Zhu (2016, *Review of Financial Studies*, "...and the Cross-Section of Expected Returns"): after accounting for the hundreds of tested factors, the discovery threshold must rise to t > 3.0, not 2.0; most published findings are likely false. Scanning a wallet census is exactly this setting. **Lever: require an effect size equivalent to t >= 3 (or FDR-adjusted p) before a wallet is eligible, not "positive Sharpe over lookback".**

**2.2** Bailey & Lopez de Prado (2012, "The Sharpe Ratio Efficient Frontier"; 2014, *Journal of Portfolio Management*, "The Deflated Sharpe Ratio"): the expected maximum Sharpe under the null grows with the number of trials; deflate observed SR by the number of wallets scanned, and compute Minimum Track Record Length (MinTRL) given skew/kurtosis of per-trade returns. Crypto perp PnL is heavy-tailed, so MinTRL is much longer than the Gaussian intuition. **Lever: impose a minimum trade count / track length per wallet computed from MinTRL with the observed higher moments, and rank on deflated (not raw) Sharpe with N = wallets screened.**

**2.3** Barras, Scaillet & Wermers (2010, *Journal of Finance*, "False Discoveries in Mutual Fund Performance"): ~75% of funds are zero-alpha and only ~0.6% truly skilled after FDR control; naive top-decile selection is dominated by lucky zero-alpha funds. **Lever: apply an FDR procedure (e.g., Benjamini-Hochberg / Storey) to the wallet cross-section and expect the true-skill base rate to be near zero - size the copied book assuming most selections are false positives.**

## 3. Shrinkage and combination beat winner-picking

**3.1** James & Stein (1961): the sample-mean estimator of many means is inadmissible; shrinking every wallet's estimated alpha toward the cross-sectional (near-zero) mean strictly improves selection MSE. **Lever: rank wallets on shrunken alpha (empirical-Bayes toward zero), which automatically penalizes short histories and extreme estimates.**

**3.2** Jones & Shanken (2005, *JFE*, "Performance Evaluation with Cross-Sectional Learning"): Bayesian alpha estimates that learn across funds compress the extremes; the best raw performers have the largest downward revisions. **Lever: a wallet's posterior alpha should borrow strength from the whole census - the top of the raw leaderboard is where estimation error concentrates.**

**3.3** DeMiguel, Garlappi & Uppal (2009, *RFS*, "Optimal Versus Naive Diversification"): 1/N beats 14 optimized allocation rules out-of-sample because estimation error swamps optimization gains at realistic sample sizes. **Lever: equal-weight a K-wallet basket (fixed notional per position already implements this per-trade); do not optimize weights per wallet, and prefer widening K over concentrating in the current best estimate.**

## 4. Copy/social trading evidence: leaders behave worse once copied, and leaderboard-chasing fails

**4.1** Pelster & Hofmann (2018, *Journal of Banking & Finance*, "About the Fear of Reputational Loss: Social Trading and the Disposition Effect"): leader traders being copied show a *stronger* disposition effect than uncopied traders, and it intensifies once they gain first-time followers. **Lever: monitor for post-selection behavior change (loss-holding time, realized-loss avoidance) and treat a rising disposition signature after we start copying as a disqualifier.**

**4.2** Apesteguia, Oechssler & Weidenholzer (2020, *Management Science*, "Copy Trading"): experimentally, showing rankings of others' success increases risk-taking, and the *option to copy* increases it further; copy trading induces excessive risk-taking on both sides. **Lever: cap per-wallet exposure and assume selected leaders are riskier than their track record implies.**

**4.3** Doering, Neumann & Paul (2015, EFMA, "A Primer on Social Trading Networks"): signal-provider returns across eToro/ZuluTrade/ayondo/Currensee show hedge-fund-like non-normality, high attrition, and payoff schemes that reward volatility (option-like incentives). **Lever: penalize wallets whose PnL profile looks like a short-vol/lottery payoff (many small wins, rare catastrophic losses), because the platform incentive structure manufactures exactly that shape.**

**4.4** Empirical platform studies (e.g., Roder & Walter on wikifolio; "Stranger Danger?", CHI 2024, on crypto copy-trading platforms) find that following recently top-ranked traders yields negative or zero subsequent abnormal returns, and copied crypto traders escalate risk. **Lever: explicitly exclude "recent leaderboard winner" as a ranking feature; recency-weighted PnL rank is the documented failure mode.**

## 5. Skill-vs-luck metrics robust to our failure modes

**5.1** Odean (1998, *Journal of Finance*, "Are Investors Reluctant to Realize Their Losses?"): retail traders realize gains ~1.5x more readily than losses, and the losses they hold subsequently underperform; disposition is a robust negative-skill marker. **Lever: compute per-wallet PGR/PLR (proportion of gains vs losses realized) and screen out high-disposition wallets regardless of PnL.**

**5.2** Mean per-trade PnL is dominated by tails; the median trade and the trade-level hit-rate-vs-payoff decomposition are robust to a single lucky moonshot. This is the practical consequence of the non-normality documented in 2.2 and 4.3. **Lever: rank on median per-trade alpha versus a same-coin/same-side benchmark priced through our execution model, not on mean or total PnL.**

**5.3** Drawdown-aware ratios: Calmar (Young 1991, *Futures*) and time-under-water penalize the martingale signature that Sharpe hides (smooth equity, then cliff). Bailey & Lopez de Prado (2014, "Stop-outs under Serial Correlation") formalize drawdown expectations under autocorrelated PnL. **Lever: gate on Calmar and max time-underwater, and run explicit doubling-down detection (position-size escalation conditional on open loss, adverse-excursion MAE distributions) as a hard veto.**

**5.4** MAE/"bag-risk": a wallet whose winners show deep maximum adverse excursion is harvesting premium for tail risk we will faithfully replicate at fixed notional. **Lever: cap acceptable MAE-per-winning-trade; faithful replication means we inherit their bags, so the bag distribution IS our risk.**

## 6. Capacity, liquidity, crowding

**6.1** Berk & Green (2004, *JPE*): skilled managers' alpha is eroded by inflows because strategies have finite capacity; performance is not persistent at scale even when skill is real. On HL, a leader's edge in thin-book alts erodes as copiers pile in, and their fills front-run ours. **Lever: at $470 equity and fixed small notional our own impact is negligible, so weight selection toward liquid-enough coins where leader-to-copier slippage (latency alpha decay) is small - measure per-wallet copy-lag alpha decay through `execution_model.py` rather than assuming leader PnL transfers.**

**6.2** Practitioner slippage evidence from copy platforms (documented follower-vs-leader return gaps on ZuluTrade/eToro) shows the copy gap alone can flip a profitable leader to an unprofitable copy. **Lever: require leader edge per trade >> fees (8.64 bps RT) + measured copy slippage; a leader whose median trade alpha is under ~2x our round-trip cost is uncopyable no matter how skilled.**

## 7. Regime dependence and sparse-label overfitting

**7.1** Bailey, Borwein, Lopez de Prado & Zhu (2014, *Notices of the AMS*, "Pseudo-Mathematics and Financial Charlatanism"): adding conditioning variables (regime labels) multiplies the effective number of trials and guarantees backtest overfitting at small N; a crypto history offers only a handful of regime episodes, so any per-regime wallet ranking is fit on 2-3 effective observations. Sullivan, Timmerman & White (1999) / White (2000, "Reality Check") make the same point for data-snooped conditional rules. **Lever: select wallets unconditionally on the full lookback; use regime only as a *stress test* (does the wallet survive the worst regime) never as a *switch* (different roster per regime).**

**7.2** The hedge-fund result that only "winners in tough times" repeat (Sun, Wang & Zheng 2018; Federal Reserve WP 2016-030) supports conditioning *robustness checks* on adverse windows: skill shows up as survival in bad regimes, not outperformance in good ones. **Lever: require non-catastrophic performance in the worst historical sub-window as a gate, in addition to full-sample ranking.**

---

## Levers the literature says to AVOID

- **Pure recent-winner rotation / leaderboard chasing** (Carhart 1997; wikifolio and crypto copy-platform studies): the winner leg of persistence is weak, decays fast, and platform rankings select for risk-takers.
- **Mean-based or total-PnL ranking** (Bailey & Lopez de Prado 2014; Doering et al. 2015): dominated by tails and lottery payoffs; use median per-trade alpha and deflated Sharpe.
- **Raw Sharpe over a short lookback without deflation/MinTRL** (Bailey & Lopez de Prado 2012/2014; Harvey-Liu-Zhu 2016): the max over thousands of wallets is noise by construction.
- **Selecting without FDR control** (Barras-Scaillet-Wermers 2010): top-decile cuts fill the roster with lucky zero-alpha wallets.
- **Per-wallet weight optimization** (DeMiguel-Garlappi-Uppal 2009): estimation error swamps gains; equal-weight fixed notional is the defensible choice.
- **Regime-switched rosters on sparse labels** (Bailey et al. 2014 AMS; White 2000): a handful of crypto regimes cannot support conditional selection; regimes are for stress-testing only.
- **Unconstrained K / concentrating in the single best wallet** (James-Stein 1961; Jones-Shanken 2005): the top raw estimate has the largest error; shrink and diversify.
- **Ignoring post-copy behavior drift** (Pelster & Hofmann 2018; Apesteguia et al. 2020): being followed changes leader behavior toward disposition and risk escalation; selection must include ongoing behavioral monitoring, not one-shot ranking.
- **Copying leaders whose edge is below ~2x round-trip cost + copy slippage** (Berk-Green capacity logic; platform slippage-gap evidence): the copy gap flips thin edges negative.

Sources: [Pelster & Hofmann SSRN](https://www.ssrn.com/abstract=3057533) · [Apesteguia et al., Copy Trading, Management Science](https://dl.acm.org/doi/abs/10.1287/mnsc.2019.3508) · [Doering/Neumann/Paul EFMA 2015](https://www.efmaefm.org/0efmameetings/EFMA%20ANNUAL%20MEETINGS/2015-Amsterdam/papers/EFMA2015_0306_fullpaper.pdf) · [Agarwal & Naik SSRN](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=190389) · [Bailey & Lopez de Prado, Deflated Sharpe Ratio SSRN](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551) · [MinTRL/PSR overview](https://portfoliooptimizer.io/blog/the-probabilistic-sharpe-ratio-hypothesis-testing-and-minimum-track-record-length-for-the-difference-of-sharpe-ratios/) · [Stranger Danger, CHI 2024](https://dl.acm.org/doi/10.1145/3613904.3642715) · [Hedge fund persistence over market conditions, Fed WP](https://www.federalreserve.gov/econresdata/feds/2016/files/2016030pap.pdf)