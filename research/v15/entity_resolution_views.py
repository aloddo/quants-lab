#!/usr/bin/env python3
"""Build point-in-time entity-resolution sensitivity views from fold-pure M4.

Views are hypotheses, not interchangeable labels:
  wallet_only     every wallet is an independent seat;
  high_confidence retain only links explicitly marked high-confidence;
  broader         retain every link accepted by the current M4 heuristic.

The output has the same per-fold M4 contracts consumed by M5-M8, allowing the
full selection/replay pipeline to be rerun under each assumption rather than
redistributing broad-entity performance after the fact.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


VIEWS = ("wallet_only", "high_confidence", "broader")


def wallet_entity_id(wallet: str) -> int:
    raw = hashlib.blake2b(wallet.lower().encode(), digest_size=8, person=b"m04-view").digest()
    return int.from_bytes(raw, "big") & ((1 << 63) - 1)


def _members(value: str) -> list[str]:
    return sorted({w.strip().lower() for w in str(value).split(",") if w.strip()})


def split_view(entities: pd.DataFrame, auth: pd.DataFrame, view: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if view not in VIEWS:
        raise ValueError(f"unknown view {view!r}")
    if view == "broader":
        return entities.copy(), auth.copy()

    keep_ids: set[int] = set()
    if view == "high_confidence":
        keep_ids = set(entities.loc[
            entities["entity_confidence"].astype(str).str.lower().eq("high"), "entity_id"
        ].astype(int))

    entity_rows: list[dict] = []
    split_ids: dict[str, int] = {}
    auth_by_wallet = auth.copy()
    auth_by_wallet["wallet"] = auth_by_wallet["wallet"].astype(str).str.lower()
    if auth_by_wallet["wallet"].duplicated().any():
        raise ValueError("fold-pure M4 authenticity has duplicate wallets")
    auth_index = auth_by_wallet.set_index("wallet", drop=False)
    for row in entities.itertuples(index=False):
        members = _members(row.member_wallets)
        if int(row.entity_id) in keep_ids:
            entity_rows.append(row._asdict())
            continue
        for wallet in members:
            if wallet not in auth_index.index:
                raise ValueError(f"fold-pure M4 has no authenticity row for {wallet}")
            a = auth_index.loc[wallet]
            eid = wallet_entity_id(wallet)
            split_ids[wallet] = eid
            entity_rows.append({
                "entity_id": eid,
                "primary_wallet": wallet,
                "member_wallets": wallet,
                "n_members": 1,
                "entity_tier": a["tier"],
                "entity_alloc_weight": float(a["alloc_weight"]),
                "entity_link_evidence": "none",
                "entity_confidence": "wallet_only",
                "copyable": bool(a["copyable"]),
                "as_of_ms": int(a["as_of_ms"]),
            })

    out_entities = pd.DataFrame(entity_rows, columns=entities.columns)
    out_auth = auth_by_wallet.copy()
    split_mask = out_auth["wallet"].isin(split_ids)
    out_auth.loc[split_mask, "entity_id"] = out_auth.loc[split_mask, "wallet"].map(split_ids).astype("int64")
    out_auth.loc[split_mask, "is_entity_primary"] = True
    out_auth.loc[split_mask, "n_entity_wallets"] = 1
    if out_auth["wallet"].duplicated().any():
        raise ValueError("view construction duplicated wallets")
    if out_entities["entity_id"].duplicated().any():
        raise ValueError("stable wallet entity-id collision")
    return out_entities.sort_values("entity_id").reset_index(drop=True), out_auth.sort_values("wallet").reset_index(drop=True)


def build_views(source: Path, output: Path, folds: list[int]) -> dict:
    summary = {view: {"folds": {}} for view in VIEWS}
    for fold in folds:
        entities = pd.read_parquet(source / f"m04_entities_f{fold}.parquet")
        auth = pd.read_parquet(source / f"m04_authenticity_f{fold}.parquet")
        source_wallets = set(auth["wallet"].astype(str).str.lower())
        for view in VIEWS:
            ev, av = split_view(entities, auth, view)
            if set(av["wallet"].astype(str).str.lower()) != source_wallets:
                raise ValueError(f"{view} fold {fold}: wallet population changed")
            dest = output / view
            dest.mkdir(parents=True, exist_ok=True)
            ev.to_parquet(dest / f"m04_entities_f{fold}.parquet", index=False)
            av.to_parquet(dest / f"m04_authenticity_f{fold}.parquet", index=False)
            summary[view]["folds"][str(fold)] = {
                "n_wallets": len(av), "n_entities": len(ev),
                "n_linked_entities": int((ev["n_members"] > 1).sum()),
                "max_component": int(ev["n_members"].max()),
                "n_copyable_entities": int(ev["copyable"].sum()),
            }
    (output / "entity_view_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--folds", default="1-12")
    args = parser.parse_args()
    if "-" in args.folds:
        lo, hi = map(int, args.folds.split("-", 1))
        folds = list(range(lo, hi + 1))
    else:
        folds = [int(x) for x in args.folds.split(",")]
    print(json.dumps(build_views(args.source, args.output, folds), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
