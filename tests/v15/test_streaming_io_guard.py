"""install_memory_guard process-idempotence (codex 2026-08-10 #2).

m10 calls run_m09_chained() once per null seed (~1,000 at defaults) and each call installs the
guard; before the fix that produced ~1,001 permanent watchdog threads. One guard per process is
the whole guarantee, but a nested install with a TIGHTER cap must still win.

The test must not itself leak a watchdog (codex round 3 #1: resetting _GUARD_STATE in teardown
cannot stop a thread the test already started, which would defeat idempotence for every later
test in the process). So threading.Thread is stubbed out here and thread creation is COUNTED
rather than performed — that is also the exact property under test.
"""
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import _streaming_io as S  # noqa: E402


class _FakeThread:
    """Records construction; never starts anything real."""
    made: list = []

    def __init__(self, *_a, **kw):
        _FakeThread.made.append(kw.get("name", "unnamed"))

    def start(self):
        pass


@pytest.fixture()
def guard(monkeypatch):
    """Isolate global guard state AND make thread creation inert + countable."""
    _FakeThread.made = []
    # codex round 4 #1: patch the MODULE REFERENCE, not the shared threading module object
    # (S.threading is threading, so setattr on it would hand _FakeThread to every other module
    # creating a thread concurrently). A namespace proxy keeps the blast radius inside _streaming_io.
    monkeypatch.setattr(S, "threading", SimpleNamespace(Thread=_FakeThread))
    monkeypatch.setattr(S, "_GUARD_STATE", None)
    yield _FakeThread
    # monkeypatch restores _GUARD_STATE and threading.Thread; no real thread was ever created,
    # so nothing survives this test.


def test_guard_installs_one_thread_across_many_nested_installs(guard):
    S.install_memory_guard(soft_gb=12.0, label="outer")
    assert len(guard.made) == 1
    for i in range(50):                                   # the m10 null-seed loop
        S.install_memory_guard(soft_gb=12.0, label=f"nested{i}")
    assert len(guard.made) == 1, f"nested installs spawned {len(guard.made)} watchdogs"


def test_nested_cap_tightens_but_never_relaxes(guard):
    S.install_memory_guard(soft_gb=12.0, label="outer")
    S.install_memory_guard(soft_gb=4.0, label="tighter")
    assert S._GUARD_STATE["soft_gb"] == 4.0
    S.install_memory_guard(soft_gb=9.0, label="looser")
    assert S._GUARD_STATE["soft_gb"] == 4.0, "a looser nested cap must not relax the running guard"
    assert len(guard.made) == 1


def test_guard_state_is_clean_between_tests(guard):
    """Proves the fixture leaves no residue: state starts None every time."""
    assert S._GUARD_STATE is None
    S.install_memory_guard(soft_gb=8.0, label="solo")
    assert S._GUARD_STATE["soft_gb"] == 8.0 and len(guard.made) == 1
