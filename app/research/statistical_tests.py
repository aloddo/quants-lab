"""
Rigorous statistical testing framework for quantitative research.

Every signal hypothesis must pass these tests before proceeding to backtesting.
No exceptions. No shortcuts. If a test fails, the signal is dead.

References:
- Lopez de Prado, "Advances in Financial Machine Learning" (2018)
- Bailey & Lopez de Prado, "The Deflated Sharpe Ratio" (2014)
- Benjamini & Hochberg, "Controlling the False Discovery Rate" (1995)
"""

import numpy as np
import pandas as pd
from scipy import stats
from scipy.special import erfc
from typing import Tuple, Dict, List, Optional
from dataclasses import dataclass
import warnings


# ── Data Classes ─────────────────────────────────────────────────────────────

@dataclass
class ICResult:
    """Information Coefficient analysis result."""
    ic: float                    # Spearman rank correlation
    ic_pvalue: float             # p-value of IC
    icir: float                  # IC Information Ratio (mean / std of rolling IC)
    ic_series: np.ndarray        # Rolling IC values
    ic_decay: Dict[int, float]   # IC at different lags
    pass_gate: bool              # IC > 0.02 and p < 0.01 and ICIR > 0.5


@dataclass
class PermutationResult:
    """Permutation test result."""
    real_stat: float             # Real test statistic
    null_distribution: np.ndarray  # Distribution under null
    p_value: float               # Fraction of null >= real
    percentile: float            # Where real falls in null distribution
    pass_gate: bool              # p < 0.01


@dataclass
class StationarityResult:
    """Stationarity test result."""
    adf_stat: float
    adf_pvalue: float
    kpss_stat: float
    kpss_pvalue: float
    is_stationary: bool          # ADF rejects unit root AND KPSS doesn't reject stationarity
    recommendation: str


@dataclass
class SignalReport:
    """Complete Phase 0 signal validation report."""
    hypothesis: str
    data_coverage: Dict[str, str]
    ic_result: ICResult
    permutation_result: PermutationResult
    stationarity_result: StationarityResult
    non_overlapping_n: int
    non_overlapping_mean_bps: float
    non_overlapping_t: float
    regime_breakdown: Dict[str, Dict]
    fdr_adjusted_pvalues: Optional[Dict[str, float]]
    overall_pass: bool
    warnings: List[str]


# ── Information Coefficient ──────────────────────────────────────────────────

def compute_ic(
    signal: np.ndarray,
    forward_returns: np.ndarray,
    method: str = "spearman",
) -> Tuple[float, float]:
    """
    Compute Information Coefficient between signal and forward returns.

    Args:
        signal: Signal values at time t
        forward_returns: Returns from t to t+h
        method: "spearman" (rank) or "pearson" (linear)

    Returns:
        (ic, p_value)
    """
    mask = np.isfinite(signal) & np.isfinite(forward_returns)
    if mask.sum() < 30:
        return 0.0, 1.0

    if method == "spearman":
        ic, pval = stats.spearmanr(signal[mask], forward_returns[mask])
    else:
        ic, pval = stats.pearsonr(signal[mask], forward_returns[mask])

    return float(ic), float(pval)


def compute_ic_analysis(
    signal: np.ndarray,
    prices: np.ndarray,
    lags: List[int] = [1, 4, 8, 24, 48, 168],
    rolling_window: int = 60,
) -> ICResult:
    """
    Full IC analysis: point IC, rolling IC (for ICIR), and IC decay curve.

    Args:
        signal: Signal values (same length as prices)
        prices: Price series (for computing forward returns)
        lags: Forward return horizons to test (in periods)
        rolling_window: Window for rolling IC (for ICIR computation)

    Returns:
        ICResult with all metrics
    """
    # IC at primary lag (first in list)
    primary_lag = lags[0] if lags else 4
    fwd_ret = np.full_like(prices, np.nan)
    fwd_ret[:-primary_lag] = (prices[primary_lag:] - prices[:-primary_lag]) / prices[:-primary_lag]

    ic, pval = compute_ic(signal, fwd_ret)

    # Rolling IC for ICIR
    ic_series = []
    for i in range(rolling_window, len(signal)):
        window_sig = signal[i - rolling_window:i]
        window_ret = fwd_ret[i - rolling_window:i]
        mask = np.isfinite(window_sig) & np.isfinite(window_ret)
        if mask.sum() >= 20:
            r, _ = stats.spearmanr(window_sig[mask], window_ret[mask])
            ic_series.append(r)

    ic_arr = np.array(ic_series)
    if len(ic_arr) > 0 and np.std(ic_arr) > 0:
        icir = np.mean(ic_arr) / np.std(ic_arr)
    else:
        icir = 0.0

    # IC decay curve
    ic_decay = {}
    for lag in lags:
        if lag >= len(prices):
            continue
        lag_ret = np.full_like(prices, np.nan)
        lag_ret[:-lag] = (prices[lag:] - prices[:-lag]) / prices[:-lag]
        ic_lag, _ = compute_ic(signal, lag_ret)
        ic_decay[lag] = ic_lag

    pass_gate = abs(ic) > 0.02 and pval < 0.01 and abs(icir) > 0.5

    return ICResult(
        ic=ic,
        ic_pvalue=pval,
        icir=icir,
        ic_series=ic_arr,
        ic_decay=ic_decay,
        pass_gate=pass_gate,
    )


# ── Permutation Test ─────────────────────────────────────────────────────────

def permutation_test(
    signal: np.ndarray,
    forward_returns: np.ndarray,
    n_permutations: int = 10_000,
    stat_func: str = "spearman",
    seed: int = 42,
) -> PermutationResult:
    """
    Non-parametric permutation test for signal predictive power.

    Shuffles signal timestamps and recomputes IC to build null distribution.
    Real IC must exceed 99th percentile.

    This is robust to:
    - Non-normal distributions
    - Autocorrelation (shuffles break temporal structure)
    - Fat tails
    """
    rng = np.random.RandomState(seed)

    mask = np.isfinite(signal) & np.isfinite(forward_returns)
    sig_clean = signal[mask]
    ret_clean = forward_returns[mask]

    if len(sig_clean) < 30:
        return PermutationResult(
            real_stat=0.0,
            null_distribution=np.zeros(100),
            p_value=1.0,
            percentile=50.0,
            pass_gate=False,
        )

    # Real statistic
    if stat_func == "spearman":
        real_stat, _ = stats.spearmanr(sig_clean, ret_clean)
    else:
        real_stat = np.mean(sig_clean * ret_clean)

    # Null distribution
    null_stats = np.zeros(n_permutations)
    for i in range(n_permutations):
        shuffled = rng.permutation(sig_clean)
        if stat_func == "spearman":
            null_stats[i], _ = stats.spearmanr(shuffled, ret_clean)
        else:
            null_stats[i] = np.mean(shuffled * ret_clean)

    # Two-sided p-value
    p_value = np.mean(np.abs(null_stats) >= np.abs(real_stat))
    percentile = np.mean(null_stats <= real_stat) * 100

    return PermutationResult(
        real_stat=real_stat,
        null_distribution=null_stats,
        p_value=p_value,
        percentile=percentile,
        pass_gate=p_value < 0.01,
    )


# ── Multiple Hypothesis Correction ──────────────────────────────────────────

def fdr_correction(
    p_values: Dict[str, float],
    alpha: float = 0.05,
    method: str = "bh",
) -> Dict[str, Tuple[float, bool]]:
    """
    Benjamini-Hochberg False Discovery Rate correction.

    When testing multiple hypotheses (e.g., 50 coins), raw p-values overstate
    significance. FDR controls the expected proportion of false positives.

    Args:
        p_values: {name: raw_p_value}
        alpha: FDR threshold
        method: "bh" (Benjamini-Hochberg) or "bonferroni"

    Returns:
        {name: (adjusted_p_value, is_significant)}
    """
    names = list(p_values.keys())
    raw = np.array([p_values[n] for n in names])
    m = len(raw)

    if method == "bonferroni":
        adjusted = np.minimum(raw * m, 1.0)
    else:
        # Benjamini-Hochberg
        sorted_idx = np.argsort(raw)
        adjusted = np.zeros(m)
        for rank, idx in enumerate(sorted_idx):
            adjusted[idx] = raw[idx] * m / (rank + 1)
        # Enforce monotonicity (step-up)
        for i in range(m - 2, -1, -1):
            adjusted[sorted_idx[i]] = min(
                adjusted[sorted_idx[i]],
                adjusted[sorted_idx[i + 1]] if i + 1 < m else 1.0,
            )
        adjusted = np.minimum(adjusted, 1.0)

    return {
        names[i]: (float(adjusted[i]), adjusted[i] < alpha)
        for i in range(m)
    }


# ── Stationarity Tests ──────────────────────────────────────────────────────

def test_stationarity(
    series: np.ndarray,
    method: str = "adf+kpss",
) -> StationarityResult:
    """
    Test signal stationarity using ADF and KPSS tests.

    ADF: null = unit root (non-stationary). Reject = stationary.
    KPSS: null = stationary. Reject = non-stationary.

    Both must agree for confident stationarity assessment.
    """
    from statsmodels.tsa.stattools import adfuller, kpss

    clean = series[np.isfinite(series)]
    if len(clean) < 50:
        return StationarityResult(
            adf_stat=0, adf_pvalue=1, kpss_stat=0, kpss_pvalue=0,
            is_stationary=False,
            recommendation="Insufficient data (need >= 50 observations)",
        )

    # ADF test
    adf_result = adfuller(clean, autolag="AIC")
    adf_stat, adf_pval = adf_result[0], adf_result[1]

    # KPSS test
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        kpss_result = kpss(clean, regression="c", nlags="auto")
    kpss_stat, kpss_pval = kpss_result[0], kpss_result[1]

    # Interpretation
    adf_rejects = adf_pval < 0.05  # rejects unit root → stationary
    kpss_rejects = kpss_pval < 0.05  # rejects stationarity → non-stationary

    if adf_rejects and not kpss_rejects:
        is_stationary = True
        recommendation = "Stationary (both tests agree)"
    elif not adf_rejects and kpss_rejects:
        is_stationary = False
        recommendation = "Non-stationary (both tests agree). Apply differencing or fractional differentiation."
    elif adf_rejects and kpss_rejects:
        is_stationary = False
        recommendation = "Trend-stationary (ADF rejects, KPSS rejects). Remove trend first."
    else:
        is_stationary = True
        recommendation = "Possibly stationary (neither test rejects). Treat with caution."

    return StationarityResult(
        adf_stat=float(adf_stat),
        adf_pvalue=float(adf_pval),
        kpss_stat=float(kpss_stat),
        kpss_pvalue=float(kpss_pval),
        is_stationary=is_stationary,
        recommendation=recommendation,
    )


# ── Non-Overlapping Signal Analysis ─────────────────────────────────────────

def non_overlapping_signals(
    signal: np.ndarray,
    prices: np.ndarray,
    threshold: float,
    hold_periods: int,
    cooldown: int = None,
    direction: str = "both",
) -> Dict:
    """
    Extract non-overlapping trade signals with cooldown.

    Overlapping forward returns inflate sample size. This function enforces
    a minimum gap between consecutive signals.

    Args:
        signal: Signal values
        prices: Price series
        threshold: Absolute z-score threshold for signal
        hold_periods: Forward return horizon
        cooldown: Minimum periods between signals (default = hold_periods)
        direction: "both", "long_only" (signal < -threshold), "short_only" (signal > threshold)

    Returns:
        Dict with trade stats
    """
    if cooldown is None:
        cooldown = hold_periods

    trades = []
    last_entry = -cooldown - 1

    for i in range(len(signal) - hold_periods):
        if i - last_entry < cooldown:
            continue

        if not np.isfinite(signal[i]):
            continue

        fwd_ret = (prices[i + hold_periods] - prices[i]) / prices[i]

        if direction in ("both", "short_only") and signal[i] > threshold:
            trades.append(-fwd_ret)  # SHORT
            last_entry = i
        elif direction in ("both", "long_only") and signal[i] < -threshold:
            trades.append(fwd_ret)  # LONG
            last_entry = i

    if not trades:
        return {
            "n": 0, "mean_bps": 0, "t_stat": 0, "win_rate": 0,
            "sharpe": 0, "max_dd_bps": 0, "trades": [],
        }

    arr = np.array(trades)
    mean_bps = np.mean(arr) * 10000
    std_bps = np.std(arr) * 10000
    t_stat = mean_bps / (std_bps / np.sqrt(len(arr))) if std_bps > 0 else 0

    # Max drawdown in cumulative bps
    cum = np.cumsum(arr) * 10000
    running_max = np.maximum.accumulate(cum)
    drawdowns = running_max - cum
    max_dd = np.max(drawdowns) if len(drawdowns) > 0 else 0

    return {
        "n": len(arr),
        "mean_bps": float(mean_bps),
        "median_bps": float(np.median(arr) * 10000),
        "t_stat": float(t_stat),
        "win_rate": float(np.mean(arr > 0) * 100),
        "sharpe": float(mean_bps / std_bps * np.sqrt(252 * 24 / max(1, hold_periods))) if std_bps > 0 else 0,
        "max_dd_bps": float(max_dd),
        "avg_win_bps": float(np.mean(arr[arr > 0]) * 10000) if np.any(arr > 0) else 0,
        "avg_loss_bps": float(np.mean(arr[arr < 0]) * 10000) if np.any(arr < 0) else 0,
        "trades": trades,
    }


# ── Deflated Sharpe Ratio ────────────────────────────────────────────────────

def deflated_sharpe_ratio(
    sharpe: float,
    n_trials: int,
    n_obs: int,
    skewness: float = 0,
    kurtosis: float = 3,
    sharpe_benchmark: float = 0,
) -> float:
    """
    Bailey & Lopez de Prado (2014) Deflated Sharpe Ratio.

    Adjusts the Sharpe ratio for the number of trials (strategies tested),
    non-normality (skewness, kurtosis), and sample size.

    A DSR < 0.5 means the observed Sharpe is likely due to chance.

    Args:
        sharpe: Observed Sharpe ratio
        n_trials: Number of strategies/parameter sets tested
        n_obs: Number of observations
        skewness: Return distribution skewness
        kurtosis: Return distribution excess kurtosis
        sharpe_benchmark: Expected Sharpe under null (usually 0)

    Returns:
        Probability that the Sharpe is genuine (0 to 1)
    """
    # Expected maximum Sharpe from n_trials under null
    e_max_sharpe = sharpe_benchmark
    if n_trials > 1:
        # Euler-Mascheroni approximation
        gamma = 0.5772156649
        e_max_sharpe = (
            (1 - gamma) * stats.norm.ppf(1 - 1 / n_trials)
            + gamma * stats.norm.ppf(1 - 1 / (n_trials * np.e))
        )

    # Standard error of Sharpe estimate
    se_sharpe = np.sqrt(
        (1 + 0.5 * sharpe**2 - skewness * sharpe + (kurtosis - 3) / 4 * sharpe**2)
        / (n_obs - 1)
    )

    if se_sharpe <= 0:
        return 0.0

    # Deflated Sharpe = P(true Sharpe > benchmark)
    z = (sharpe - e_max_sharpe) / se_sharpe
    dsr = float(stats.norm.cdf(z))

    return dsr


# ── Regime Detection (HMM) ──────────────────────────────────────────────────

def fit_regime_model(
    returns: np.ndarray,
    n_states: int = 3,
    seed: int = 42,
) -> Tuple[np.ndarray, Dict]:
    """
    Fit Hidden Markov Model to detect market regimes.

    Uses return + volatility features to identify states:
    typically bull/bear/ranging.

    Args:
        returns: Log returns series
        n_states: Number of hidden states (2 or 3)
        seed: Random seed for reproducibility

    Returns:
        (state_sequence, state_info)
        state_info: {state_id: {"mean_ret": float, "vol": float, "label": str}}
    """
    try:
        from hmmlearn.hmm import GaussianHMM
    except ImportError:
        raise ImportError("hmmlearn required: pip install hmmlearn")

    clean = returns[np.isfinite(returns)]
    if len(clean) < 100:
        raise ValueError(f"Need >= 100 observations, got {len(clean)}")

    # Features: returns + rolling volatility
    vol_window = min(24, len(clean) // 10)
    rolling_vol = pd.Series(clean).rolling(vol_window).std().values
    mask = np.isfinite(rolling_vol)

    X = np.column_stack([clean[mask], rolling_vol[mask]])

    model = GaussianHMM(
        n_components=n_states,
        covariance_type="full",
        n_iter=200,
        random_state=seed,
    )
    model.fit(X)
    states = model.predict(X)

    # Label states by mean return
    state_info = {}
    means = [(s, model.means_[s][0]) for s in range(n_states)]
    means.sort(key=lambda x: x[1])

    labels = ["bear", "ranging", "bull"] if n_states == 3 else ["bear", "bull"]
    for rank, (state_id, mean_ret) in enumerate(means):
        state_info[state_id] = {
            "mean_ret": float(mean_ret),
            "vol": float(model.means_[state_id][1]),
            "label": labels[rank] if rank < len(labels) else f"state_{rank}",
        }

    # Rebuild full-length state array (pad NaNs from rolling vol)
    full_states = np.full(len(returns), -1, dtype=int)
    valid_idx = np.where(np.isfinite(returns))[0]
    valid_vol_idx = valid_idx[mask]
    full_states[valid_vol_idx] = states

    return full_states, state_info


def regime_conditioned_ic(
    signal: np.ndarray,
    forward_returns: np.ndarray,
    regime_states: np.ndarray,
    state_info: Dict,
) -> Dict[str, Dict]:
    """
    Compute IC separately for each regime state.

    A signal that only works in one regime needs conditional gates.
    """
    results = {}
    for state_id, info in state_info.items():
        mask = (regime_states == state_id) & np.isfinite(signal) & np.isfinite(forward_returns)
        if mask.sum() < 30:
            results[info["label"]] = {"ic": 0, "p": 1, "n": int(mask.sum()), "skip": True}
            continue

        ic, pval = compute_ic(signal[mask], forward_returns[mask])
        results[info["label"]] = {
            "ic": float(ic),
            "p": float(pval),
            "n": int(mask.sum()),
            "skip": False,
        }

    return results


# ── Hurst Exponent (Mean-Reversion Detection) ───────────────────────────────

def hurst_exponent(series: np.ndarray, max_lag: int = 100) -> float:
    """
    Estimate Hurst exponent via rescaled range (R/S) analysis.

    H < 0.5: mean-reverting (good for fade strategies)
    H = 0.5: random walk (no edge)
    H > 0.5: trending (good for momentum strategies)

    Args:
        series: Price or signal series
        max_lag: Maximum lag for R/S computation

    Returns:
        Hurst exponent estimate
    """
    clean = series[np.isfinite(series)]
    n = len(clean)
    if n < 100:
        return 0.5  # insufficient data, assume random walk

    lags = range(2, min(max_lag, n // 4))
    rs_values = []
    lag_values = []

    for lag in lags:
        # Split into chunks
        n_chunks = n // lag
        if n_chunks < 2:
            continue

        rs_chunk = []
        for i in range(n_chunks):
            chunk = clean[i * lag:(i + 1) * lag]
            mean_chunk = np.mean(chunk)
            deviations = np.cumsum(chunk - mean_chunk)
            r = np.max(deviations) - np.min(deviations)
            s = np.std(chunk, ddof=1)
            if s > 0:
                rs_chunk.append(r / s)

        if rs_chunk:
            rs_values.append(np.mean(rs_chunk))
            lag_values.append(lag)

    if len(rs_values) < 5:
        return 0.5

    # Linear regression of log(R/S) vs log(lag)
    log_lags = np.log(lag_values)
    log_rs = np.log(rs_values)
    slope, _, _, _, _ = stats.linregress(log_lags, log_rs)

    return float(slope)


# ── Convenience: Full Phase 0 Validation ─────────────────────────────────────

def validate_signal(
    signal: np.ndarray,
    prices: np.ndarray,
    hypothesis: str,
    hold_period: int = 8,
    threshold: float = 2.0,
    lags: List[int] = [1, 4, 8, 24, 48, 168],
    n_permutations: int = 10_000,
) -> SignalReport:
    """
    Run complete Phase 0 validation on a signal.

    This is the ONE function to call for rigorous EDA.
    If overall_pass is False, the signal is dead. Move on.
    """
    warnings_list = []

    # 1. Stationarity
    stationarity = test_stationarity(signal)
    if not stationarity.is_stationary:
        warnings_list.append(f"Signal non-stationary: {stationarity.recommendation}")

    # 2. IC analysis
    ic_result = compute_ic_analysis(signal, prices, lags=lags)
    if not ic_result.pass_gate:
        warnings_list.append(f"IC gate failed: IC={ic_result.ic:.4f}, ICIR={ic_result.icir:.2f}")

    # 3. Forward returns for permutation test
    fwd_ret = np.full_like(prices, np.nan)
    if hold_period < len(prices):
        fwd_ret[:-hold_period] = (prices[hold_period:] - prices[:-hold_period]) / prices[:-hold_period]

    # 4. Permutation test
    perm_result = permutation_test(signal, fwd_ret, n_permutations=n_permutations)
    if not perm_result.pass_gate:
        warnings_list.append(f"Permutation test failed: p={perm_result.p_value:.4f}")

    # 5. Non-overlapping trades
    no_trades = non_overlapping_signals(signal, prices, threshold, hold_period)

    if no_trades["n"] < 100:
        warnings_list.append(f"Insufficient non-overlapping trades: {no_trades['n']} < 100")

    # 6. Regime conditioning
    log_returns = np.diff(np.log(prices))
    log_returns = np.concatenate([[0], log_returns])
    try:
        regime_states, state_info = fit_regime_model(log_returns)
        regime_breakdown = regime_conditioned_ic(signal, fwd_ret, regime_states, state_info)
    except Exception as e:
        regime_breakdown = {"error": {"message": str(e)}}
        warnings_list.append(f"Regime detection failed: {e}")

    # Overall pass
    overall_pass = (
        ic_result.pass_gate
        and perm_result.pass_gate
        and no_trades["n"] >= 100
        and no_trades["t_stat"] > 2.0
    )

    return SignalReport(
        hypothesis=hypothesis,
        data_coverage={"n_observations": str(len(signal)), "hold_period": str(hold_period)},
        ic_result=ic_result,
        permutation_result=perm_result,
        stationarity_result=stationarity,
        non_overlapping_n=no_trades["n"],
        non_overlapping_mean_bps=no_trades["mean_bps"],
        non_overlapping_t=no_trades["t_stat"],
        regime_breakdown=regime_breakdown,
        fdr_adjusted_pvalues=None,
        overall_pass=overall_pass,
        warnings=warnings_list,
    )


def print_signal_report(report: SignalReport):
    """Pretty-print a signal validation report."""
    print(f"\n{'='*70}")
    print(f"SIGNAL VALIDATION REPORT")
    print(f"{'='*70}")
    print(f"Hypothesis: {report.hypothesis}")
    print(f"Data: {report.data_coverage}")
    print()

    # IC
    ic = report.ic_result
    gate = "PASS" if ic.pass_gate else "FAIL"
    print(f"[{gate}] Information Coefficient:")
    print(f"  IC = {ic.ic:.4f} (p = {ic.ic_pvalue:.4f})")
    print(f"  ICIR = {ic.icir:.2f}")
    print(f"  IC decay: {ic.ic_decay}")
    print()

    # Permutation
    perm = report.permutation_result
    gate = "PASS" if perm.pass_gate else "FAIL"
    print(f"[{gate}] Permutation Test (10K shuffles):")
    print(f"  Real IC = {perm.real_stat:.4f}")
    print(f"  p-value = {perm.p_value:.4f} (percentile: {perm.percentile:.1f}%)")
    print()

    # Stationarity
    stat = report.stationarity_result
    gate = "PASS" if stat.is_stationary else "WARN"
    print(f"[{gate}] Stationarity:")
    print(f"  ADF: stat={stat.adf_stat:.3f}, p={stat.adf_pvalue:.4f}")
    print(f"  KPSS: stat={stat.kpss_stat:.3f}, p={stat.kpss_pvalue:.4f}")
    print(f"  {stat.recommendation}")
    print()

    # Non-overlapping trades
    gate = "PASS" if report.non_overlapping_n >= 100 and report.non_overlapping_t > 2.0 else "FAIL"
    print(f"[{gate}] Non-Overlapping Trades:")
    print(f"  n = {report.non_overlapping_n}")
    print(f"  Mean = {report.non_overlapping_mean_bps:+.1f} bps")
    print(f"  t-stat = {report.non_overlapping_t:.2f}")
    print()

    # Regime
    print(f"Regime Breakdown:")
    for regime, data in report.regime_breakdown.items():
        if isinstance(data, dict) and "ic" in data:
            print(f"  {regime}: IC={data['ic']:.4f} p={data['p']:.4f} n={data['n']}")
    print()

    # Warnings
    if report.warnings:
        print(f"WARNINGS:")
        for w in report.warnings:
            print(f"  - {w}")
    print()

    verdict = "PASS - Proceed to Phase 1 (backtest)" if report.overall_pass else "FAIL - Signal is dead. Move on."
    print(f"{'='*70}")
    print(f"VERDICT: {verdict}")
    print(f"{'='*70}")
