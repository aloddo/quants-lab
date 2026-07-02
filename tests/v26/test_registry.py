"""Registry: full-cross counts, family_forces_band / empty_by_construction pruning,
deterministic sha256. Synthetic candidate counts only (no artifacts)."""
import pandas as pd
import pytest

from v26_common import FULL_CROSS, canonical_sha256
from v26_families import allowed_bands, effective_band
from v26_registry import enumerate_registry

RUN_CELLS = {("F1a", "2-24h"), ("F1a", "any"), ("F1b", "2-24h"), ("F1b", "any"),
             ("F2", "2-15min"),
             ("F3", "2-15min"), ("F3", "15min-2h"), ("F3", "2-24h"), ("F3", "any"),
             ("F4a", "2-15min"), ("F4a", "15min-2h"), ("F4a", "2-24h"), ("F4a", "any"),
             ("F4b", "2-15min"), ("F4b", "15min-2h"), ("F4b", "2-24h"), ("F4b", "any")}


def counts(n=5):
    return {cell: n for cell in RUN_CELLS}


class TestBands:
    def test_family_forces_band_map(self):
        assert allowed_bands("F2") == {"2-15min": "RUN",
                                       "15min-2h": "family_forces_band",
                                       "2-24h": "family_forces_band",
                                       "any": "family_forces_band"}
        a = allowed_bands("F1a")
        assert a["2-15min"] == "family_forces_band"      # empty vs [2h,48h)
        assert a["15min-2h"] == "family_forces_band"     # half-open: hits exactly 2h
        assert a["2-24h"] == "RUN" and a["any"] == "RUN"
        assert all(v == "RUN" for v in allowed_bands("F3").values())

    def test_f2_any_collapses_by_dedup(self):
        # F2 x any is IDENTICAL to F2 x 2-15min after the family constraint: pruned,
        # not silently merged (amendment dedup rule)
        assert effective_band("F2", "any") == effective_band("F2", "2-15min")


class TestRegistry:
    def test_full_cross_and_run_counts(self):
        reg = enumerate_registry(counts())
        assert len(reg) == FULL_CROSS == 10368
        assert reg["config_id"].is_unique
        n_bandcells = len(RUN_CELLS)                     # 17
        assert int((reg["status"] == "RUN").sum()) == n_bandcells * 8 * 3 * 2 * 3 * 3
        by_reason = reg[reg["status"] == "PRUNED"]["prune_reason"].value_counts()
        assert by_reason["family_forces_band"] == (6 * 4 - n_bandcells) * 432

    def test_empty_by_construction(self):
        c = counts()
        c[("F2", "2-15min")] = 0
        reg = enumerate_registry(c)
        f2 = reg[(reg["family"] == "F2") & (reg["hold_band"] == "2-15min")]
        assert (f2["status"] == "PRUNED").all()
        assert (f2["prune_reason"] == "empty_by_construction").all()
        assert (f2["fold1_train_candidates"] == 0).all()

    def test_missing_count_fails_loud(self):
        c = counts()
        del c[("F3", "any")]
        with pytest.raises(ValueError):
            enumerate_registry(c)

    def test_deterministic_sha(self):
        a = enumerate_registry(counts())
        b = enumerate_registry(counts())
        assert canonical_sha256(a) == canonical_sha256(b)
        pd.testing.assert_frame_equal(a, b)
