import sys
from pathlib import Path

import numpy as np
import pytest

REPO = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO / "research" / "v25"))
sys.path.insert(0, str(REPO / "research" / "v15"))


@pytest.fixture
def marks_dir(tmp_path):
    """Factory: write synthetic 1m close series into a temp marks cache dir."""
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
