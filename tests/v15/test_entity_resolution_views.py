import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "research" / "v15"))
import entity_resolution_views as views  # noqa: E402


def fixture():
    entities = pd.DataFrame([
        {"entity_id": 1, "primary_wallet": "0xa", "member_wallets": "0xa,0xb", "n_members": 2,
         "entity_tier": "CLEAN", "entity_alloc_weight": 1.0, "entity_link_evidence": "transfer",
         "entity_confidence": "medium", "copyable": True, "as_of_ms": 1},
        {"entity_id": 2, "primary_wallet": "0xc", "member_wallets": "0xc,0xd", "n_members": 2,
         "entity_tier": "CLEAN", "entity_alloc_weight": 1.0, "entity_link_evidence": "deterministic",
         "entity_confidence": "high", "copyable": True, "as_of_ms": 1},
    ])
    auth = pd.DataFrame([
        {"wallet": w, "entity_id": eid, "is_entity_primary": w in {"0xa", "0xc"},
         "n_entity_wallets": 2, "tier": "CLEAN", "alloc_weight": 1.0,
         "reason_codes": "", "copyable": True, "as_of_ms": 1}
        for w, eid in (("0xa", 1), ("0xb", 1), ("0xc", 2), ("0xd", 2))
    ])
    return entities, auth


def test_wallet_only_splits_all_links():
    entities, auth = fixture()
    ev, av = views.split_view(entities, auth, "wallet_only")
    assert len(ev) == 4 and (ev.n_members == 1).all()
    assert av.entity_id.nunique() == 4


def test_high_confidence_retains_only_high_links():
    entities, auth = fixture()
    ev, av = views.split_view(entities, auth, "high_confidence")
    assert len(ev) == 3
    assert sorted(ev.n_members) == [1, 1, 2]
    assert av[av.wallet.isin(["0xc", "0xd"])].entity_id.nunique() == 1


def test_broader_is_exact_copy():
    entities, auth = fixture()
    ev, av = views.split_view(entities, auth, "broader")
    pd.testing.assert_frame_equal(ev, entities)
    pd.testing.assert_frame_equal(av, auth)
