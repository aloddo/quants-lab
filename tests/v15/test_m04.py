"""V15 M4 authenticity-tier tests. Asserts the codex-SHIP design contract (modules/m4-design)."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "research" / "v15"))
import v15_m025_authenticity_gate as g  # noqa: E402
import v15_m04_authenticity as m4  # noqa: E402


def mk(**kw):
    s = g.WalletScores(wallet=kw.pop("wallet", "0xw"))
    # sensible CLEAN defaults: directional, enough history
    s.net_gross_ratio = kw.pop("net_gross_ratio", 0.8)
    s.price_pnl_var_frac = kw.pop("price_pnl_var_frac", 0.7)
    s.funding_frac = kw.pop("funding_frac", 0.1)
    s.wash_frac = kw.pop("wash_frac", 0.0)
    s.confidence = kw.pop("confidence", "HIGH")
    s.n_anchors = kw.pop("n_anchors", 12)
    s.active_days = kw.pop("active_days", 60)
    # recompute l3_pass_standalone like stage_a does
    ng, pv = s.net_gross_ratio, s.price_pnl_var_frac
    neutral = (ng == ng and ng < g.NET_GROSS_NEUTRAL) and (pv == pv and pv < g.PRICE_VAR_NEUTRAL)
    s.l3_pass_standalone = kw.pop("l3_pass_standalone",
                                  (not neutral) and (ng == ng) and (pv == pv))
    for k, v in kw.items():
        setattr(s, k, v)
    return s


# === tier truth table (_own_tier) ===
def test_clean():
    assert m4._own_tier(mk())[0] == "CLEAN"


def test_kill_wash():
    assert m4._own_tier(mk(wash_frac=0.6))[0] == "KILL"


def test_kill_delta_neutral_requires_both():
    # both low -> KILL
    assert m4._own_tier(mk(net_gross_ratio=0.1, price_pnl_var_frac=0.1, l3_pass_standalone=False))[0] == "KILL"
    # only net_gross low (price var present) -> NOT KILL (CLEAN, quality-noted) — I5 + danger!=quality
    t, codes = m4._own_tier(mk(net_gross_ratio=0.1, price_pnl_var_frac=0.7))
    assert t == "CLEAN"
    assert any(c.startswith("q:low_net_gross") for c in codes)


def test_kill_carry():
    assert m4._own_tier(mk(funding_frac=0.8))[0] == "KILL"


def test_uncertain_thin():
    assert m4._own_tier(mk(confidence="LOW"))[0] == "UNCERTAIN"


def test_uncertain_nan():
    assert m4._own_tier(mk(net_gross_ratio=float("nan")))[0] == "UNCERTAIN"


def test_suspicious_wash_borderline():
    assert m4._own_tier(mk(wash_frac=0.35))[0] == "SUSPICIOUS"


# === I2: precedence (UNCERTAIN beats SUSPICIOUS) is SEPARATE from weight order ===
def test_uncertain_dominates_suspicious():
    # both thin (UNCERTAIN) and wash_borderline (SUSPICIOUS) -> UNCERTAIN wins precedence
    t, _ = m4._own_tier(mk(confidence="LOW", wash_frac=0.35))
    assert t == "UNCERTAIN"


def test_weight_order_differs_from_precedence():
    # precedence: KILL > UNCERTAIN > SUSPICIOUS > CLEAN
    assert m4.TIER_PRECEDENCE["UNCERTAIN"] < m4.TIER_PRECEDENCE["SUSPICIOUS"]
    # weight: UNCERTAIN (0.25) > SUSPICIOUS (0.10) — intentional mismatch
    assert m4.ALLOC_WEIGHT["UNCERTAIN"] > m4.ALLOC_WEIGHT["SUSPICIOUS"]
    assert m4.ALLOC_WEIGHT["KILL"] == 0.0 < m4.ALLOC_WEIGHT["CLEAN"] == 1.0


def test_kill_dominates_all():
    # wash KILL + thin -> KILL
    assert m4._own_tier(mk(wash_frac=0.6, confidence="LOW"))[0] == "KILL"


# === end-to-end run on synthetic wallets (monkeypatch stage_a + entities) ===
def _patch(monkeypatch, score_map, ent_id, ent_members, hedge_pairs=frozenset()):
    monkeypatch.setattr(g, "stage_a", lambda w, lo, hi: score_map[w])
    monkeypatch.setattr(g, "build_entities", lambda ws, lo, hi: (ent_id, ent_members))
    monkeypatch.setattr(m4.m01, "load_wallet_fills", lambda w, lo, hi: [])
    monkeypatch.setattr(g, "internal_hedge",
                        lambda fa, fb, lo, hi: False)  # overridden per-test if needed


def test_run_single_wallet_clean(monkeypatch):
    sm = {"0xa": mk(wallet="0xa")}
    _patch(monkeypatch, sm, {"0xa": 0}, {0: ["0xa"]})
    df, edf = m4.run(["0xa"], 0, 10, 10)
    assert df.iloc[0]["tier"] == "CLEAN"
    assert bool(df.iloc[0]["copyable"]) is True
    assert edf.iloc[0]["primary_wallet"] == "0xa"


def test_run_kill_not_copyable(monkeypatch):
    sm = {"0xa": mk(wallet="0xa", wash_frac=0.7)}
    _patch(monkeypatch, sm, {"0xa": 0}, {0: ["0xa"]})
    df, _ = m4.run(["0xa"], 0, 10, 10)
    assert df.iloc[0]["tier"] == "KILL"
    assert bool(df.iloc[0]["copyable"]) is False  # KILL-with-primary still not copyable


def test_run_entity_dedup_fragment(monkeypatch):
    # two wallets, one entity; 0xa is the better directional primary, 0xb is a fragment
    sm = {"0xa": mk(wallet="0xa", sharpe=3.0), "0xb": mk(wallet="0xb", sharpe=1.0)}
    _patch(monkeypatch, sm, {"0xa": 0, "0xb": 0}, {0: ["0xa", "0xb"]})
    df, edf = m4.run(["0xa", "0xb"], 0, 10, 10)
    a = df[df["wallet"] == "0xa"].iloc[0]
    b = df[df["wallet"] == "0xb"].iloc[0]
    assert bool(a["is_entity_primary"]) and bool(a["copyable"])
    assert not bool(b["is_entity_primary"]) and not bool(b["copyable"])  # fragment not copied
    assert "entity_fragment" in b["reason_codes"]
    assert edf.iloc[0]["n_members"] == 2 and edf.iloc[0]["primary_wallet"] == "0xa"


def test_run_internal_hedge_entity_kill(monkeypatch):
    sm = {"0xa": mk(wallet="0xa", sharpe=3.0), "0xb": mk(wallet="0xb", sharpe=1.0)}
    _patch(monkeypatch, sm, {"0xa": 0, "0xb": 0}, {0: ["0xa", "0xb"]})
    monkeypatch.setattr(g, "internal_hedge", lambda fa, fb, lo, hi: True)
    df, edf = m4.run(["0xa", "0xb"], 0, 10, 10)
    assert (df["tier"] == "KILL").all()
    assert not edf.iloc[0]["copyable"]


def test_run_member_kill_taints_whole_entity(monkeypatch):
    # codex code-r2: a provable own-KILL on ANY member (wash) is the manipulation/survivorship
    # trick (strategy 4c) -> the WHOLE entity is KILLed (not just that member, not masked).
    members = [f"0x{i}" for i in range(g.ENTITY_MAX_WALLETS + 2)]
    sm = {w: mk(wallet=w) for w in members}
    sm[members[0]] = mk(wallet=members[0], wash_frac=0.7)  # provable wash
    eid = {w: 0 for w in members}
    _patch(monkeypatch, sm, eid, {0: members})
    df, edf = m4.run(members, 0, 10, 10)
    assert (df["tier"] == "KILL").all()
    assert "entity_member_kill" in df.iloc[0]["reason_codes"]
    assert not edf.iloc[0]["copyable"]


def test_run_too_big_entity_uncertain_no_primary(monkeypatch):
    members = [f"0x{i}" for i in range(g.ENTITY_MAX_WALLETS + 2)]
    sm = {w: mk(wallet=w) for w in members}
    eid = {w: 0 for w in members}
    _patch(monkeypatch, sm, eid, {0: members})
    df, edf = m4.run(members, 0, 10, 10)
    assert (df["tier"] == "UNCERTAIN").all()
    assert "entity_too_big" in df.iloc[0]["reason_codes"]
    assert edf.iloc[0]["primary_wallet"] is None and not edf.iloc[0]["copyable"]
