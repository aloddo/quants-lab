"""GOLDEN regression test for v15_m025_authenticity_gate trust-audit fixes (2026-07-10).

m025 is the authenticity HARD GATE: conservative by design, an inauthentic wallet must NEVER PASS on
missing/degenerate data. Locks the fail-CLOSED decision boundaries the codex audit hardened:
- P1 funding-farm: NaN funding_frac (undefined evidence) -> REVIEW (not PASS); the funding-dominated case
  (denom~0, funding present -> funding_frac=inf) -> EXCLUDE.
- P2 wash boundaries conservative: exact 0.50 -> EXCLUDE, exact 0.20 -> REVIEW (not PASS).
- HARD EXCLUDES ALWAYS WIN; PASS requires every gate clean.
Tests the pure decide_verdict() extracted from the COMBINE loop.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import v15_m025_authenticity_gate as m025  # noqa: E402


def _clean() -> "m025.WalletScores":
    """A wallet that PASSES every gate (single-wallet entity)."""
    s = m025.WalletScores(wallet="0xclean")
    s.days_since_last_fill = 1.0
    s.wash_frac = 0.0
    s.net_gross_ratio = 0.80          # >= NET_GROSS_NEUTRAL(0.20) and >= BORDER(0.30) -> not neutral/border
    s.price_pnl_var_frac = 0.70       # >= PRICE_VAR_NEUTRAL(0.30)
    s.funding_frac = 0.10             # <= FUNDING_FARM_FRAC(0.5)
    s.confidence = "HIGH"
    s.sharpe_vs_lev_flag = False
    s.unexecutable = False
    return s


def _v(s, ent_reason=None, is_primary=True, n_members=1):
    return m025.decide_verdict(s, ent_reason, is_primary, n_members)


def test_clean_wallet_passes():
    assert _v(_clean()) == ("PASS", [])


def test_funding_nan_reviews_not_passes():
    s = _clean(); s.funding_frac = float("nan")   # undefined funding evidence
    verdict, codes = _v(s)
    assert verdict == "REVIEW" and "nan_metric" in codes


def test_funding_inf_excludes():
    s = _clean(); s.funding_frac = float("inf")    # funding-dominated (denom~0, funding present)
    verdict, codes = _v(s)
    assert verdict == "EXCLUDE" and "funding_farm" in codes


def test_wash_exact_exclude_boundary():
    s = _clean(); s.wash_frac = m025.WASH_EXCLUDE   # exactly 0.50 -> EXCLUDE (conservative)
    verdict, codes = _v(s)
    assert verdict == "EXCLUDE" and "wash" in codes


def test_wash_exact_review_boundary():
    s = _clean(); s.wash_frac = m025.WASH_REVIEW    # exactly 0.20 -> REVIEW (not PASS)
    verdict, codes = _v(s)
    assert verdict == "REVIEW" and "wash_borderline" in codes


def test_wash_just_below_exclude_reviews():
    s = _clean(); s.wash_frac = m025.WASH_EXCLUDE - 1e-6   # 0.4999... -> REVIEW, not EXCLUDE
    verdict, codes = _v(s)
    assert verdict == "REVIEW" and "wash_borderline" in codes


def test_nan_l3_reviews():
    s = _clean(); s.net_gross_ratio = float("nan")   # unknown directionality
    verdict, codes = _v(s)
    assert verdict == "REVIEW" and "nan_metric" in codes


def test_delta_neutral_excludes():
    s = _clean(); s.net_gross_ratio = 0.10; s.price_pnl_var_frac = 0.10  # both low -> flat-book farmer
    verdict, codes = _v(s)
    assert verdict == "EXCLUDE" and "delta_neutral" in codes


def test_stale_excludes():
    s = _clean(); s.days_since_last_fill = m025.STALE_DAYS + 0.5
    verdict, codes = _v(s)
    assert verdict == "EXCLUDE" and "stale" in codes


def test_entity_fragment_hard_excludes_nonprimary():
    s = _clean()
    verdict, codes = _v(s, ent_reason=None, is_primary=False, n_members=3)
    assert verdict == "EXCLUDE" and "entity_fragment" in codes


def test_hard_exclude_beats_review_state():
    # a too-big entity (REVIEW state) with a per-wallet HARD exclude -> EXCLUDE wins (FIX 5).
    s = _clean(); s.wash_frac = 0.9
    verdict, codes = _v(s, ent_reason="entity_too_big_review", is_primary=True, n_members=10)
    assert verdict == "EXCLUDE" and "wash" in codes and "entity_too_big" not in codes


def test_low_confidence_reviews():
    s = _clean(); s.confidence = "LOW"; s.anchor_reason = "thin_history"
    verdict, codes = _v(s)
    assert verdict == "REVIEW" and "thin_history" in codes
