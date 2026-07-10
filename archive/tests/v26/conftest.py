import sys
from pathlib import Path

import numpy as np
import pytest

REPO = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO / "research" / "v26"))
sys.path.insert(0, str(REPO / "research" / "v25"))
sys.path.insert(0, str(REPO / "research" / "v15"))


@pytest.fixture
def marks_dir(tmp_path):
    """Factory: write synthetic 1m close series (bar OPEN minutes + closes) into a temp
    marks cache dir. A bar opening at m closes (becomes readable) at m + 60s."""
    d = tmp_path / "marks"
    d.mkdir()

    def write(coin: str, minutes_ms, closes):
        import urllib.parse as u
        arr = np.vstack([np.asarray(minutes_ms, dtype="float64"),
                         np.asarray(closes, dtype="float64")])
        np.save(d / f"{u.quote(coin, safe='')}.npy", arr)
        return d

    write.dir = d
    return write


@pytest.fixture
def zero_fee_snapshot():
    return {"data": {"feeSchedule": {"cross": "0.0", "add": "0.0",
                                     "tiers": {"vip": []}},
                     "activeReferralDiscount": "0.0"}}


@pytest.fixture
def tier_snapshot():
    return {"data": {"feeSchedule": {"cross": "0.001", "add": "0.0002",
                                     "tiers": {"vip": [
                                         {"ntlCutoff": "1000.0", "cross": "0.0005",
                                          "add": "0.0001"}]}},
                     "activeReferralDiscount": "0.0"}}
