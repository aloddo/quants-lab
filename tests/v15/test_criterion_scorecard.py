"""Criterion scorecard tests: planted-signal PASS / planted-noise FAIL, NW-t unit test,
timestamp-fence violation, zero-fill seat retention, chunked-bootstrap equality."""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "research" / "v15"))
import criterion_scorecard as cs  # noqa: E402


# === planted predictive criterion PASSes, planted noise FAILs =====================
def _synthetic_panel(seed: int = 7, n_per_fold: int = 400) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    for k in cs.EVAL_FOLDS:
        x_signal = rng.normal(size=n_per_fold)
        x_noise = rng.normal(size=n_per_fold)
        y = 0.3 * x_signal + rng.normal(scale=0.5, size=n_per_fold)
        rows.append(pd.DataFrame({
            "primary_wallet": [f"0x{i:040x}" for i in range(n_per_fold)],
            "fold_id": k,
            "y_primary": y,
            "y_sens": y + rng.normal(scale=0.05, size=n_per_fold),
            "planted": x_signal,
            "noise": x_noise,
        }))
    return pd.concat(rows, ignore_index=True)


def test_planted_signal_passes_and_noise_fails():
    panel = _synthetic_panel()
    fam = ("planted", "noise")
    primary = cs.score_family(panel, criteria=fam, y_col="y_primary")
    sens = cs.score_family(panel, criteria=fam, y_col="y_sens")
    scored = cs.assign_tiers(primary, sens)
    assert scored.loc["planted", "tier"] == "PASS"
    assert scored.loc["planted", "nw_t"] >= 2.0
    assert scored.loc["planted", "q_bh"] <= 0.10
    assert scored.loc["planted", "sign_count"] >= 6
    assert scored.loc["noise", "tier"] == "FAIL"
    # decile monotonicity for the planted signal
    assert scored.loc["planted", "top_decile_y"] > scored.loc["planted", "bottom_decile_y"]


def test_sens_disagreement_blocks_pass():
    panel = _synthetic_panel()
    panel["y_sens"] = np.random.default_rng(0).normal(size=len(panel))  # sens verdict flips
    fam = ("planted", "noise")
    scored = cs.assign_tiers(cs.score_family(panel, criteria=fam, y_col="y_primary"),
                             cs.score_family(panel, criteria=fam, y_col="y_sens"))
    assert scored.loc["planted", "tier"] == "WEAK"  # clears gate (1) only -> not PASS


# === Newey-West lag-2 t unit test =================================================
def test_nw_tstat_matches_hand_computation():
    x = np.array([0.1, 0.3, 0.2, 0.4, 0.15, 0.35, 0.25, 0.3, 0.2])
    n = len(x)
    e = x - x.mean()
    g0 = np.dot(e, e) / n
    g1 = np.dot(e[1:], e[:-1]) / n
    g2 = np.dot(e[2:], e[:-2]) / n
    var = g0 + 2 * (1 - 1 / 3) * g1 + 2 * (1 - 2 / 3) * g2  # Bartlett, lag 2
    expected = x.mean() / np.sqrt(var / n)
    assert cs.nw_tstat(x, lag=2) == pytest.approx(expected, rel=1e-12)


def test_nw_tstat_lag0_equals_uncorrected():
    x = np.array([1.0, 2.0, 3.0, 2.0, 1.0])
    e = x - x.mean()
    expected = x.mean() / np.sqrt((np.dot(e, e) / len(x)) / len(x))
    assert cs.nw_tstat(x, lag=0) == pytest.approx(expected, rel=1e-12)
    assert np.isnan(cs.nw_tstat([1.0]))  # < 2 obs


# === allowlist / fence violation ==================================================
def test_fence_assert_raises_past_2026_07_13():
    cs._assert_fence("ok", cs.FENCE_MS)  # boundary is allowed
    with pytest.raises(ValueError, match="FENCE VIOLATION"):
        cs._assert_fence("bad", cs.FENCE_MS + 1)


def test_loader_rejects_fabricated_post_fence_summary(tmp_path):
    run = tmp_path / "run"
    (run / "m07_test").mkdir(parents=True)
    bad = pd.DataFrame({
        "entity_id": [1], "fold_id": [12], "conservative_roe": [0.1], "roe_engine": [0.1],
        "n_fills": [3],
        "window_end_ms": [cs.FENCE_MS + 3_600_000],  # fabricated post-07-13 timestamp
    })
    bad.to_parquet(run / "m07_test" / "m07_summary.parquet")
    with pytest.raises(ValueError, match="FENCE VIOLATION"):
        cs.load_summary(run)


def test_loader_rejects_fabricated_post_fence_positions(tmp_path):
    run = tmp_path / "run"
    (run / "m07_test").mkdir(parents=True)
    bad = pd.DataFrame({
        "entity_id": [1], "fold_id": [12], "coin": ["BTC"],
        "entry_ts": [cs.FENCE_MS - 1000], "exit_ts": [cs.FENCE_MS + 1000],
        "realized_pnl_after_cost": [1.0], "r_i": [0.01],
    })
    bad.to_parquet(run / "m07_test" / "m07_positions.parquet")
    with pytest.raises(ValueError, match="FENCE VIOLATION"):
        cs.load_positions(run)


# === zero-fill seat retention =====================================================
def _tiny_fixture():
    """Wallet A trades folds 1-3, has a ZERO-FILL seat in fold 4 (n_fills=0, y=0).
    Wallet B trades folds 1-4. Both must appear in the k=4 panel."""
    seats = []
    for w, folds_active in (("A", [1, 2, 3, 4]), ("B", [1, 2, 3, 4])):
        for f in folds_active:
            zero = (w == "A" and f == 4)
            seats.append({"entity_id": hash((w, f)) % 10**9, "fold_id": f,
                          "primary_wallet": w,
                          "conservative_roe": 0.0 if zero else 0.01 * f,
                          "roe_engine": 0.0 if zero else 0.01 * f,
                          "n_fills": 0 if zero else 5})
    seats = pd.DataFrame(seats)
    pos = []
    for w in ("A", "B"):
        for f in (1, 2, 3):
            for i in range(6):
                pos.append({"primary_wallet": w, "fold_id": f, "coin": "BTC" if i % 2 else "ETH",
                            "dur_min": 10.0 * (i + 1), "realized_pnl_after_cost": 1.0,
                            "abs_pnl": 1.0, "r_i": 0.01})
    positions = pd.DataFrame(pos)
    regimes = {1: "down", 2: "down", 3: "up", 4: "up"}
    return seats, positions, regimes


def test_zero_fill_seats_are_kept():
    seats, positions, regimes = _tiny_fixture()
    panel = cs.compute_panel(seats, positions, regimes, eval_folds=(4,))
    assert len(panel) == 2  # zero-fill seat NOT dropped
    a = panel[panel["primary_wallet"] == "A"].iloc[0]
    assert bool(a["zero_fill"]) is True
    assert a["y_primary"] == 0.0
    assert panel["zero_fill"].mean() == pytest.approx(0.5)  # reported share
    # criteria computed from prior folds only, present for both rows
    assert np.isfinite(a["C1_boot_p"]) and np.isfinite(a["C4_share_gt_5m"])
    assert a["C5_breadth"] == 2.0  # BTC + ETH both positive cumulative pnl


def test_c2_na_without_two_folds_per_regime_and_c4_definition():
    seats, positions, regimes = _tiny_fixture()
    panel = cs.compute_panel(seats, positions, regimes, eval_folds=(4,))
    a = panel[panel["primary_wallet"] == "A"].iloc[0]
    # prior folds 1-3 = 2 down + 1 up -> C2 NA (needs >=2 in EACH bucket)
    assert pd.isna(a["C2_regime_agree"])
    # C4: durations 10..60m, all pnl +1 -> share of pnl from dur > 30m = 3/6
    assert a["C4_share_gt_30m"] == pytest.approx(0.5)
    assert a["C4_share_gt_1m"] == pytest.approx(1.0)
    # C3 defined (3 prior folds), C6 defined (>=2 prior folds)
    assert np.isfinite(a["C3_trajectory"]) and np.isfinite(a["C6_decay_winshare"])


# === transplanted bootstrap statistic =============================================
def test_boot_p_chunked_equals_unchunked_and_degenerate_cases():
    rng = np.random.default_rng(3)
    x = rng.normal(0.002, 0.01, 250)
    p_one = cs._boot_p_mean_gt(x, 0.0, chunk_elems=10**9)   # single draw
    p_chunk = cs._boot_p_mean_gt(x, 0.0, chunk_elems=997)   # many tiny chunks
    assert p_one == p_chunk  # numpy Generator stream is sequential -> element-identical
    assert cs._boot_p_mean_gt(np.array([0.1, 0.2]), 0.0) == 1.0        # n < 5
    assert cs._boot_p_mean_gt(np.full(50, -0.01), 0.0) == 1.0          # obs <= margin
    assert cs._boot_p_mean_gt(rng.normal(0.05, 0.01, 200), 0.0) < 0.01  # clear positive mean


# === full valid fixture run dir (codex remediation batch) =========================
def _fixture_run_dir(tmp_path, n_wallets: int = 20) -> Path:
    """Minimal but loader-valid run dir: 12 folds on the real calendar, seats for every
    (wallet, fold), positions for folds 1..11. All timestamps pre-fence."""
    run = tmp_path / "run"
    (run / "m07_test").mkdir(parents=True)
    start0 = pd.Timestamp("2026-01-26")
    pd.DataFrame({
        "fold_id": range(1, 13),
        "test_start": [start0 + pd.Timedelta(days=14 * i) for i in range(12)],
        "test_end_excl": [start0 + pd.Timedelta(days=14 * (i + 1)) for i in range(12)],
    }).to_parquet(run / "m03_folds.parquet")
    rng = np.random.default_rng(11)
    seats, pos = [], []
    for f in range(1, 13):
        for i in range(n_wallets):
            eid = f * 1000 + i
            seats.append({"entity_id": eid, "primary_wallet": f"w{i}", "fold_id": f,
                          "in_shortlist": True, "as_of_ms": cs.FENCE_MS - 10_000,
                          "conservative_roe": float(rng.normal(0, 0.02)),
                          "roe_engine": float(rng.normal(0, 0.02)),
                          "n_fills": 5, "window_end_ms": cs.FENCE_MS - 10_000})
            if f <= 11:
                t0 = int(pd.Timestamp("2026-02-01").value // 1_000_000) + f * 1000
                for j in range(3):
                    pos.append({"entity_id": eid, "fold_id": f,
                                "coin": "BTC" if j % 2 else "ETH",
                                "entry_ts": t0 + j, "exit_ts": t0 + j + 600_000,
                                "realized_pnl_after_cost": float(rng.normal(0, 1.0)),
                                "r_i": float(rng.normal(0, 0.01))})
    sdf = pd.DataFrame(seats)
    sdf[["entity_id", "primary_wallet", "fold_id", "in_shortlist", "as_of_ms"]].to_parquet(
        run / "m06a_shortlist.parquet")
    sdf[["entity_id", "fold_id", "conservative_roe", "roe_engine", "n_fills",
         "window_end_ms"]].to_parquet(run / "m07_test" / "m07_summary.parquet")
    pd.DataFrame(pos).to_parquet(run / "m07_test" / "m07_positions.parquet")
    return run


# === blocker 1: symlink smuggling + file-access instrumentation ===================
def test_symlinked_allowlisted_name_is_rejected(tmp_path):
    run = _fixture_run_dir(tmp_path)
    smuggled = run / "forward_oos.parquet"  # "holdout" bytes under a holdout name
    pd.read_parquet(run / "m07_test" / "m07_summary.parquet").to_parquet(smuggled)
    (run / "m07_test" / "m07_summary.parquet").unlink()
    (run / "m07_test" / "m07_summary.parquet").symlink_to(smuggled)
    with pytest.raises(ValueError, match="ALLOWLIST VIOLATION"):
        cs.load_summary(run)


def test_symlinked_intermediate_dir_is_rejected(tmp_path):
    run = _fixture_run_dir(tmp_path)
    real = run / "gap2_fullmirror"  # holdout dir impersonating m07_test
    (run / "m07_test").rename(real)
    (run / "m07_test").symlink_to(real)
    with pytest.raises(ValueError, match="ALLOWLIST VIOLATION"):
        cs.load_positions(run)


def test_only_allowlisted_files_are_read(tmp_path, monkeypatch):
    """Instrumented pd.read_parquet: the four loaders touch EXACTLY the four allowlisted
    artifacts; decoy holdout material in the run dir is never opened."""
    run = _fixture_run_dir(tmp_path)
    decoy = pd.DataFrame({"x": [1]})
    decoy.to_parquet(run / "forward_oos.parquet")
    (run / "frozen10_fresh").mkdir()
    decoy.to_parquet(run / "frozen10_fresh" / "part.parquet")
    (run / "gap2_fullmirror").mkdir()
    decoy.to_parquet(run / "gap2_fullmirror" / "part.parquet")
    accessed = []
    real_read = cs.pd.read_parquet

    def recorder(path, *a, **k):
        accessed.append(Path(path).resolve())
        return real_read(path, *a, **k)

    monkeypatch.setattr(cs.pd, "read_parquet", recorder)
    cs.load_folds(run)
    cs.load_mapping(run)
    cs.load_summary(run)
    cs.load_positions(run)
    assert set(accessed) == {(run / rel).resolve() for rel in cs.ALLOWLISTED_INPUTS}
    base = run.resolve()
    for h in cs.HOLDOUT_LOCATIONS:
        hp = base / h
        assert all(p != hp and hp not in p.parents for p in accessed)


# === blocker 2: fail-closed fencing on folds / mapping / nulls / Mongo ============
def test_folds_fence_ordering_and_nulls(tmp_path):
    run = _fixture_run_dir(tmp_path)
    f = pd.read_parquet(run / "m03_folds.parquet")
    g = f.copy()
    g.loc[g.fold_id == 12, "test_end_excl"] = pd.Timestamp("2026-08-01")  # post-fence
    g.to_parquet(run / "m03_folds.parquet")
    with pytest.raises(ValueError, match="FENCE VIOLATION"):
        cs.load_folds(run)
    g = f.copy()
    g["test_start"] = g["test_start"].where(g.fold_id != 3, pd.NaT)  # null timestamp
    g.to_parquet(run / "m03_folds.parquet")
    with pytest.raises(ValueError, match="null test window"):
        cs.load_folds(run)
    g = f.copy()
    g.loc[g.fold_id == 3, "test_start"] = g.loc[g.fold_id == 3, "test_end_excl"].iloc[0]
    g.to_parquet(run / "m03_folds.parquet")
    with pytest.raises(ValueError, match="window ordering"):
        cs.load_folds(run)


def test_mapping_fence_and_nulls(tmp_path):
    run = _fixture_run_dir(tmp_path)
    m = pd.read_parquet(run / "m06a_shortlist.parquet")
    m["as_of_ms"] = m["as_of_ms"].astype("float64")
    m.loc[m.index[0], "as_of_ms"] = float(cs.FENCE_MS + 5)
    m.to_parquet(run / "m06a_shortlist.parquet")
    with pytest.raises(ValueError, match="FENCE VIOLATION"):
        cs.load_mapping(run)
    m.loc[m.index[0], "as_of_ms"] = np.nan
    m.to_parquet(run / "m06a_shortlist.parquet")
    with pytest.raises(ValueError, match="null timestamps"):
        cs.load_mapping(run)


def test_null_position_timestamps_fail_closed(tmp_path):
    run = _fixture_run_dir(tmp_path)
    p = pd.read_parquet(run / "m07_test" / "m07_positions.parquet")
    p["exit_ts"] = p["exit_ts"].astype("float64")
    p.loc[p.index[0], "exit_ts"] = np.nan  # a null row cannot prove it is pre-fence
    p.to_parquet(run / "m07_test" / "m07_positions.parquet")
    with pytest.raises(ValueError, match="null timestamps"):
        cs.load_positions(run)


class _StubCol:
    """Injectable Mongo collection stub."""
    def __init__(self, ts):
        self.ts = ts

    def find_one(self, q, sort=None):
        return {"close": 100.0, "timestamp_utc": self.ts}


def test_mongo_doc_past_fence_rejected():
    folds = pd.DataFrame({"fold_id": [1], "test_start": [pd.Timestamp("2026-01-26")],
                          "test_end_excl": [pd.Timestamp("2026-02-09")]})
    with pytest.raises(ValueError, match="FENCE VIOLATION"):
        cs.load_btc_regimes("unused", folds, collection=_StubCol(cs.FENCE_MS + 1))


def test_mongo_request_past_fence_rejected():
    folds = pd.DataFrame({"fold_id": [1], "test_start": [pd.Timestamp("2026-07-10")],
                          "test_end_excl": [pd.Timestamp("2026-07-30")]})  # end past fence
    with pytest.raises(ValueError, match="FENCE VIOLATION"):
        cs.load_btc_regimes("unused", folds, collection=_StubCol(cs.FENCE_MS - 1000))


# === blocker 4: BH multiplicity penalty pinned to the declared family =============
def test_bh_family_pinned_at_declared_size():
    p = pd.Series([0.001, 0.002, 0.003, 0.004, 0.005, 0.2, 0.4, 0.6, np.nan],
                  index=[f"c{i}" for i in range(9)])
    q = cs.bh_q(p, family_size=9)
    finite = p.dropna().sort_values()
    raw = finite.to_numpy() * 9 / np.arange(1, 9)  # m stays 9 despite only 8 finite p
    exp = np.minimum(np.minimum.accumulate(raw[::-1])[::-1], 1.0)
    for name, e in zip(finite.index, exp):
        assert q[name] == pytest.approx(e)
    assert np.isnan(q["c8"])
    # shrunk-family q (m=8) would be smaller for the top p -- the pin is conservative
    assert q["c0"] >= 0.001 * 9 / 8
    with pytest.raises(ValueError, match="family size"):
        cs.bh_q(pd.Series([0.1, 0.2, 0.3]), family_size=2)


# === blocker 6: criterion-definition locks ========================================
def _empty_positions() -> pd.DataFrame:
    return pd.DataFrame({"primary_wallet": pd.Series(dtype="object"),
                         "fold_id": pd.Series(dtype="int64"),
                         "coin": pd.Series(dtype="object"),
                         "dur_min": pd.Series(dtype="float64"),
                         "realized_pnl_after_cost": pd.Series(dtype="float64"),
                         "abs_pnl": pd.Series(dtype="float64"),
                         "r_i": pd.Series(dtype="float64")})


def test_c2_full_range():
    regimes = {1: "up", 2: "up", 3: "down", 4: "down", 5: "up"}
    profiles = {"P": {"up": 0.02, "down": 0.02},    # profitable in both regimes -> +2
                "M": {"up": 0.02, "down": -0.02},   # mixed -> 0
                "N": {"up": -0.02, "down": -0.02}}  # loses in both -> -2 (NOT rewarded)
    seats = []
    for w, prof in profiles.items():
        for f in range(1, 6):
            roe = 0.0 if f == 5 else prof[regimes[f]]
            seats.append({"entity_id": abs(hash((w, f))) % 10**9, "primary_wallet": w,
                          "fold_id": f, "conservative_roe": roe, "roe_engine": roe,
                          "n_fills": 1})
    panel = cs.compute_panel(pd.DataFrame(seats), _empty_positions(), regimes, eval_folds=(5,))
    got = panel.set_index("primary_wallet")["C2_regime_agree"]
    assert got["P"] == 2.0 and got["M"] == 0.0 and got["N"] == -2.0


def test_c4_signed_with_losses():
    seats = pd.DataFrame([
        {"entity_id": 1, "primary_wallet": "L", "fold_id": 1, "conservative_roe": 0.01,
         "roe_engine": 0.01, "n_fills": 2},
        {"entity_id": 2, "primary_wallet": "L", "fold_id": 2, "conservative_roe": 0.01,
         "roe_engine": 0.01, "n_fills": 2}])
    positions = pd.DataFrame([
        {"primary_wallet": "L", "fold_id": 1, "coin": "BTC", "dur_min": 40.0,
         "realized_pnl_after_cost": -2.0, "abs_pnl": 2.0, "r_i": -0.02},
        {"primary_wallet": "L", "fold_id": 1, "coin": "ETH", "dur_min": 10.0,
         "realized_pnl_after_cost": 1.0, "abs_pnl": 1.0, "r_i": 0.01}])
    panel = cs.compute_panel(seats, positions, {1: "up", 2: "up"}, eval_folds=(2,))
    row = panel.iloc[0]
    assert row["C4_share_gt_30m"] == pytest.approx(-2.0 / 3.0)  # loss held > 30m -> negative
    assert row["C4_share_gt_1m"] == pytest.approx(-1.0 / 3.0)   # net / sum|pnl|


def test_undefined_fold_ic_reasons_recorded():
    """Frozen reporting rule: EVERY undefined fold-IC is recorded with its reason --
    no-rows, insufficient rows, constant X, and constant y (incl. sens-panel degeneracy)."""
    rows = []
    for k, mode in ((1, "ok"), (2, "const_x"), (3, "const_y"), (4, "na_rows"), (5, "few")):
        n = 2 if mode == "few" else 12
        for i in range(n):
            rows.append({"fold_id": k, "primary_wallet": f"w{i}",
                         "y_primary": 0.5 if mode == "const_y" else float(i),
                         "y_sens": float(i),
                         "X": (np.nan if mode == "na_rows"
                               else (1.0 if mode == "const_x" else float(i)))})
    s = cs.score_family(pd.DataFrame(rows), criteria=("X",), eval_folds=(1, 2, 3, 4, 5))
    assert s.attrs["fold_ic_undefined"]["X"] == {
        2: "constant_X", 3: "constant_y", 4: "no_rows", 5: "n_lt_min_rows"}
    # sens scoring records its own degeneracy independently (y_sens varies in fold 3)
    s2 = cs.score_family(pd.DataFrame(rows), criteria=("X",), eval_folds=(1, 2, 3, 4, 5),
                         y_col="y_sens")
    assert 3 not in s2.attrs["fold_ic_undefined"]["X"]


def test_deciles_are_within_fold_not_global():
    rows = []
    for fold, off in ((1, 0.0), (2, 100.0)):  # X ranges disjoint across folds
        for i in range(1, 21):
            rows.append({"primary_wallet": f"w{i}", "fold_id": fold,
                         "y_primary": float(i), "y_sens": float(i), "X": off + i})
    s = cs.score_family(pd.DataFrame(rows), criteria=("X",), eval_folds=(1, 2))
    assert s.loc["X", "top_decile_y"] == pytest.approx(19.5)     # within-fold, then averaged
    assert s.loc["X", "bottom_decile_y"] == pytest.approx(1.5)   # global pooling would give 2.5


# === --rank-latest: PRELIMINARY cohort machinery ==================================
def _cohort_panel(n: int = 12) -> pd.DataFrame:
    """Latest-fold panel fixture: wallet w0 is best on C6+C2; w1 fails the C4@1m median
    filter; w2 has < 3 active folds; w3 will be vetoed by the gate flags."""
    rows = []
    for i in range(n):
        rows.append({
            "primary_wallet": f"w{i}", "fold_id": 12,
            "y_primary": 0.0, "y_sens": 0.0, "zero_fill": False,
            "n_prior_folds": 5, "n_prior_active_folds": 1 if i == 2 else 5,
            "prior_conservative_pnl": 100.0 * i,
            "C1_boot_p": -0.01,                    # flat -> median keeps everyone
            "C2_regime_agree": 2.0 if i in (0, 3) else 0.0,
            "C3_trajectory": 0.001,                # flat -> median keeps everyone
            "C4_share_gt_1m": -0.9 if i == 1 else 0.5,  # only w1 falls below the median
            "C5_breadth": 3.0, "C5_neg_hhi": -0.5,
            "C6_decay_winshare": 1.0 - 0.05 * i,   # w0 highest
        })
    return pd.DataFrame(rows)


def test_preliminary_cohort_filters_vetoes_and_ranking():
    panel = _cohort_panel()
    vetoes = pd.DataFrame({"primary_wallet": ["w0", "w3"],
                           "uw_add_ok": [True, False],       # w3 fails martingale veto
                           "mae_p90_ok": [True, True],
                           "liq_ok": [True, True]})
    cohort, meta = cs.build_preliminary_cohort(panel, vetoes, top_n=5)
    wallets = cohort["primary_wallet"].tolist()
    assert "w2" not in wallets           # < 3 prior active folds
    assert "w1" not in wallets           # below-median C4@1m
    assert "w3" not in wallets and meta["vetoed_excluded"] == 1  # standing hard veto
    assert wallets[0] == "w0"            # best C6 AND best C2 -> best average rank
    assert cohort.loc[0, "rank"] == 1
    assert (cohort["label"] == "PRELIMINARY").all()
    assert meta["universe"] == 12 and meta["after_active_filter"] == 11
    # combined rank is the average of the two within-universe ranks
    r = cohort.iloc[0]
    assert r["combined_rank"] == pytest.approx((r["rank_c6"] + r["rank_c2"]) / 2)


def test_preliminary_cohort_no_gate_data_is_not_a_veto():
    panel = _cohort_panel()
    vetoes = pd.DataFrame({"primary_wallet": ["w3"], "uw_add_ok": [False],
                           "mae_p90_ok": [True], "liq_ok": [True]})
    cohort, meta = cs.build_preliminary_cohort(panel, vetoes, top_n=20)
    assert "w0" in cohort["primary_wallet"].tolist()  # absent from report -> kept
    assert bool(cohort.loc[cohort["primary_wallet"] == "w0", "gate_data"].iloc[0]) is False
    assert meta["vetoed_excluded"] == 1


def test_preliminary_cohort_text_is_labeled_preliminary():
    panel = _cohort_panel()
    cohort, meta = cs.build_preliminary_cohort(panel, None, top_n=3)
    text = cs.format_cohort_text(cohort, meta)
    assert "PRELIMINARY" in text and "no capital decision" in text
    assert "2026-07-13" in text


def test_load_vetoes_parses_flags(tmp_path):
    run = tmp_path / "run"
    run.mkdir()
    pd.DataFrame({"wallet": ["0xAA", "0xBB"], "n_pos_total": [5, 9],
                  "uw_add_ok": [True, False], "mae_p90_ok": [True, True],
                  "liq_ok": [False, True], "ALL_PASS": [False, False]}).to_csv(
        run / "hard_gates_report.csv", index=False)
    flags, meta = cs.load_vetoes(run)
    assert meta["available"] and meta["n_wallets"] == 2
    assert set(flags.columns) == {"primary_wallet", *cs.VETO_FLAG_COLS}
    assert flags["primary_wallet"].tolist() == ["0xaa", "0xbb"]  # lower-cased join key
    flags2, meta2 = cs.load_vetoes(tmp_path / "empty")
    assert flags2 is None and meta2["available"] is False


def test_prior_fold_stats_counts_active_folds():
    seats = pd.DataFrame({
        "primary_wallet": ["A"] * 4 + ["B"] * 2, "fold_id": [1, 2, 3, 12, 1, 2],
        "n_fills": [5, 0, 3, 7, 0, 0], "conservative_pnl_total": [10.0, 0.0, -4.0, 99.0,
                                                                  0.0, 0.0]})
    out = cs.prior_fold_stats(seats, upto_fold=11)
    assert out.loc["A", "n_prior_active_folds"] == 2      # fold 12 excluded
    assert out.loc["A", "prior_conservative_pnl"] == pytest.approx(6.0)
    assert out.loc["B", "n_prior_active_folds"] == 0


# === --behavior-screen: LEADER-tier gates (build_roster_freeze reuse) =============
def _behavior_panels():
    july = pd.DataFrame({
        # wA clean; wB martingale (uw 0.5); wD two july rows -> n_pos-weighted uw
        "primary_wallet": ["wA", "wB", "wD", "wD"],
        "fold_id": [1, 1, 1, 2], "n_pos": [10, 10, 1, 99],
        "mean_underwater_add": [0.05, 0.50, 0.90, 0.10],
        "mae_p90": [0.05, 0.05, 0.05, 0.05],
        "liq_rate": [0.0, 0.0, 0.0, 0.0],
        "frac_long": [0.5, 0.5, 0.5, 0.5]})
    fresh = pd.DataFrame({
        # wC only in replica panel (leader NA); wE scalper (36s hold -> latency fail)
        "primary_wallet": ["wA", "wB", "wC", "wD", "wE"],
        "fold_id": [1] * 5, "n_pos": [10] * 5,
        "median_hold_h": [24.0, 24.0, 24.0, 24.0, 0.01],
        "frac_long": [0.5, 0.5, 0.5, 0.5, 0.5]})
    return july, fresh


def test_behavior_screen_gates_and_na_semantics():
    july, fresh = _behavior_panels()
    df = cs.screen_wallet_behavior(["wA", "wB", "wC", "wD", "wE"], july, fresh)
    df = df.set_index("primary_wallet")
    assert df.loc["wa", "verdict"] == "PASS"
    # measured martingale veto, flagged as a MEASURED fail
    assert df.loc["wb", "verdict"] == "VETO_MEASURED"
    assert df.loc["wb", "uw_add_gate"] == "fail"
    # leader-panel absence -> NA fails CLOSED, flagged distinctly; long-share falls back
    # to the replica panel and passes, so the verdict is NA-closed not measured
    assert df.loc["wc", "verdict"] == "VETO_NA_CLOSED"
    assert df.loc["wc", "uw_add_gate"] == "NA_fail"
    assert df.loc["wc", "long_share_gate"] == "pass"
    # n_pos-weighted mean: (0.9*1 + 0.1*99)/100 = 0.108 <= 0.20 (a plain mean 0.5 would veto)
    assert df.loc["wd", "uw_add"] == pytest.approx(0.108)
    assert df.loc["wd", "uw_add_gate"] == "pass"
    # 36s median hold -> latency ratio 4/36 >> 2% -> measured veto (leader gates NA too)
    assert df.loc["we", "latency_ratio_gate"] == "fail"
    assert df.loc["we", "verdict"] == "VETO_MEASURED"


def test_behavior_screen_thresholds_are_canonical():
    import build_roster_freeze as brf
    lv = brf.LADDER[0][1]
    assert (lv["uw"], lv["liq"], lv["lo"], lv["hi"]) == (0.20, 0.005, 0.25, 0.75)
    assert brf.MAE_LEADER_MAX == 0.15 and brf.LATENCY_MAX == 0.02
    assert brf.COPY_LATENCY_S == 4.0


def test_behavior_screen_text_labeled_preliminary():
    july, fresh = _behavior_panels()
    df = cs.screen_wallet_behavior(["wA"], july, fresh)
    df.insert(0, "rank", [1])
    text = cs.format_behavior_screen_text(df)
    assert "PRELIMINARY" in text and "no capital decision" in text
    assert "NA fails CLOSED" in text


# === --screen-extend: walk the ranked universe until target PASS ==================
def _walk_fixture(n: int = 10):
    universe = pd.DataFrame({
        "rank": range(1, n + 1),
        "primary_wallet": [f"w{i}" for i in range(1, n + 1)],
        "C6_decay_winshare": np.linspace(1.0, 0.5, n), "C2_regime_agree": 2.0})
    # every odd-ranked wallet passes, even-ranked fails (alternating)
    screened = pd.DataFrame({
        "primary_wallet": [f"w{i}" for i in range(1, n + 1)],
        "verdict": ["PASS" if i % 2 else "VETO_MEASURED" for i in range(1, n + 1)],
        **{f"{g}_gate": ["pass" if i % 2 else ("fail" if g == "leader_liq" else "pass")
                         for i in range(1, n + 1)] for g in cs.BEHAVIOR_GATES}})
    return universe, screened


def test_walk_until_target_cuts_at_cumulative_pass():
    universe, screened = _walk_fixture(10)
    walked, s = cs.walk_until_target(universe, screened, target_pass=3)
    # passes at ranks 1,3,5 -> depth 5
    assert s["depth"] == 5 and s["achieved_pass"] == 3 and not s["exhausted"]
    assert walked["cum_pass"].tolist() == [1, 1, 2, 2, 3]
    assert s["kill_rate_by_gate"]["leader_liq"]["measured_fail"] == 2  # ranks 2, 4
    assert s["verdict_counts"] == {"PASS": 3, "VETO_MEASURED": 2}


def test_walk_until_target_exhausts_universe():
    universe, screened = _walk_fixture(6)
    walked, s = cs.walk_until_target(universe, screened, target_pass=99)
    assert s["exhausted"] and s["depth"] == 6 and s["achieved_pass"] == 3
    with pytest.raises(ValueError, match="missing"):
        cs.walk_until_target(universe, screened.head(3), 2)  # unscreened wallet -> refuse


def test_build_preliminary_cohort_top_n_none_returns_full_universe():
    panel = _cohort_panel()
    cohort, meta = cs.build_preliminary_cohort(panel, None, top_n=None)
    assert len(cohort) == meta["after_vetoes"]                # nothing truncated
    assert cohort["rank"].tolist() == list(range(1, len(cohort) + 1))


def test_screened_text_labeled_preliminary():
    universe, screened = _walk_fixture(4)
    for c in ("C1_boot_p", "C4_share_gt_1m", "uw_add", "leader_liq", "long_share",
              "latency_ratio"):
        screened[c] = 0.1
    walked, s = cs.walk_until_target(universe, screened, target_pass=2)
    walked["cum_pass"] = walked["cum_pass"].astype(int)
    text = cs.format_screened_text(walked, s)
    assert "PRELIMINARY" in text and "no capital decision" in text
    assert f"depth {s['depth']}/{s['universe']}" in text


# === blocker 3/5: prereg-before-load ordering + provenance binds the run ==========
def test_preregistration_printed_before_any_load(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cs, "install_memory_guard", lambda **k: None)

    def boom(run_dir):
        raise RuntimeError("LOAD-SENTINEL")

    monkeypatch.setattr(cs, "load_folds", boom)
    with pytest.raises(RuntimeError, match="LOAD-SENTINEL"):
        cs.main(["--run-dir", str(tmp_path)])
    assert "PRE-REGISTRATION" in capsys.readouterr().out


def test_provenance_binds_the_run(tmp_path, monkeypatch):
    run = _fixture_run_dir(tmp_path)
    out = tmp_path / "out"
    monkeypatch.setattr(cs, "install_memory_guard", lambda **k: None)
    regimes = {f: ("up" if f % 2 else "down") for f in range(1, 13)}
    obs = [{"fold_id": f, "regime": regimes[f]} for f in range(1, 13)]
    monkeypatch.setattr(cs, "load_btc_regimes",
                        lambda uri, folds, collection=None: (regimes, obs))
    rc = cs.main(["--run-dir", str(run), "--out-dir", str(out),
                  "--mongo-uri", "mongodb://user:secretpw@localhost:27017"])
    assert rc == 0
    prov = json.loads((out / "provenance.json").read_text())
    assert prov["status"] == "complete"
    for name in ("m03_folds", "m06a_shortlist", "m07_summary", "m07_positions"):
        meta = prov["inputs"][name]
        assert len(meta["sha256"]) == 64 and meta["bytes"] > 0 and meta["ts_extrema_ms"]
        assert meta["sha256"] == cs._sha256_file(Path(meta["path"]))  # binds exact bytes
    assert prov["inputs"]["mongo"]["observations_sha256"]
    assert "secretpw" not in json.dumps(prov)  # credentials redacted
    assert prov["git"]["sha"] != "unknown" and prov["git"]["dirty"] in (True, False)
    assert len(prov["git"]["diff_sha256"]) == 64
    assert len(prov["git"]["status_porcelain_sha256"]) == 64  # binds untracked-file listing
    # binds the EXECUTED module source, independent of git tracking state
    assert prov["module_sha256"] == cs._sha256_file(Path(cs.__file__).resolve())
    assert set(prov["undefined_fold_ics"]) == {"own_y_primary", "own_y_sens",
                                               "intersection_y_primary",
                                               "intersection_y_sens"}
    assert set(prov["output_sha256"]) == {"criterion_scorecard.txt", "scorecard_own.csv",
                                          "scorecard_intersection.csv"}
    for name, h in prov["output_sha256"].items():
        assert h == cs._sha256_file(out / name)  # binds exact outputs
    assert prov["versions"]["pandas"] and prov["versions"]["numpy"]
    assert prov["criterion_spec_sha256"] == cs.CRITERION_SPEC_SHA256
    assert prov["preregistration_text"] == cs.PREREGISTRATION
