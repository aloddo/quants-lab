"""Symlink-write guard tests (2026-07-30, Fable plan gate Step 3).

WHY THIS EXISTS: app/data/v15/ holds three parallel run dirs wired together with symlinks -- 18 in
funnel20k_20260728, 27 in census20k_20260728 of which 9 point INTO funnel20k, with a mix of relative and
absolute targets. Writing m05 output into census20k therefore writes THROUGH into funnel20k and silently
overwrites another run's inputs. On 2026-07-30 that was one command away from happening and was caught
only by running `ls -l` by hand.
"""
import os
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "research" / "v15"))
from _streaming_io import ShardedParquetWriter, assert_not_symlinked_output  # noqa: E402


def test_refuses_direct_symlink(tmp_path):
    real = tmp_path / "real.parquet"
    real.write_bytes(b"x")
    link = tmp_path / "link.parquet"
    link.symlink_to(real)
    with pytest.raises(ValueError, match="SYMLINK"):
        assert_not_symlinked_output(link)


def test_refuses_via_writer_chokepoint(tmp_path):
    """The guard must sit at the one path every v15 module funnels output through, so a future
    caller cannot forget it."""
    real = tmp_path / "real.parquet"
    real.write_bytes(b"x")
    link = tmp_path / "link.parquet"
    link.symlink_to(real)
    with pytest.raises(ValueError, match="SYMLINK"):
        ShardedParquetWriter(link)


def test_allows_normal_path_and_writes(tmp_path):
    w = ShardedParquetWriter(tmp_path / "ok.parquet")
    w.add_many([{"a": 1}, {"a": 2}])
    assert w.close() == 2
    assert (tmp_path / "ok.parquet").exists()


def test_no_false_positive_on_system_symlinks():
    """REGRESSION: the first version walked parents to `/`, and on macOS `/var` and `/tmp` are
    themselves symlinks (-> private/var), so EVERY legitimate temp write was refused. An over-broad
    guard is worse than none -- it is the kind operators learn to disable. Parent walk is scoped to the
    repo."""
    d = Path(tempfile.mkdtemp())          # lives under /var on macOS
    assert assert_not_symlinked_output(d / "fine.parquet") is not None


def test_override_is_possible_and_explicit(tmp_path):
    real = tmp_path / "real.parquet"
    real.write_bytes(b"x")
    link = tmp_path / "link.parquet"
    link.symlink_to(real)
    os.environ["QL_ALLOW_SYMLINK_WRITE"] = "1"
    try:
        assert_not_symlinked_output(link) is not None
    finally:
        del os.environ["QL_ALLOW_SYMLINK_WRITE"]
