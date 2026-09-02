"""Daily Mongo candle sync — Fable-pinned test matrix (2026-08-08 plan gate, B1-B5).

Throwaway localhost collection with BOTH prod unique indexes (the dual-index geometry is what
makes the pair convention load-bearing — B1). No mongomock: real pymongo semantics required for
E11000 / $setOnInsert / update_many behavior.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest
from pymongo import MongoClient, UpdateOne

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "data_pipeline"))

import data_pipeline.hl_candles_mongo_sync as S                       # noqa: E402
from data_pipeline.backfill_hyperliquid_history import coin_to_pair   # noqa: E402

URI = "mongodb://localhost:27017"
DB = "quants_lab_test"


def _today_stem() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


@pytest.fixture()
def col():
    name = f"sync_test_{int(time.time() * 1000)}"
    c = MongoClient(URI)[DB][name]
    c.create_index([("coin", 1), ("interval", 1), ("timestamp_utc", 1)], unique=True)
    c.create_index([("pair", 1), ("interval", 1), ("timestamp_utc", 1)], unique=True)
    yield c
    c.drop()


@pytest.fixture()
def env(tmp_path, monkeypatch):
    """Redirect the module's cache dir + journal into tmp; build a 2-day hot-store fixture."""
    cache = tmp_path / "ohlc_cache"
    cache.mkdir()
    monkeypatch.setattr(S, "OHLC_CACHE_DIR", cache)
    monkeypatch.setattr(S, "JOURNAL", cache / ".mongo_sync_invalidate_journal.json")
    candles = tmp_path / "hot"
    candles.mkdir()
    t0 = 1786000020000
    rows = [
        {"coin": "BTC", "timestamp_utc": t0, "open": 1.0, "high": 2.0, "low": 0.5,
         "close": 1.5, "volume": 10.0, "n_trades": 3, "source": S.RECON_SOURCE},
        {"coin": "BTC", "timestamp_utc": t0 + 60_000, "open": 1.5, "high": 1.6, "low": 1.4,
         "close": 1.55, "volume": 5.0, "n_trades": 2, "source": S.RECON_SOURCE},
        {"coin": "kPEPE", "timestamp_utc": t0, "open": 9.0, "high": 9.0, "low": 9.0,
         "close": 9.0, "volume": 1.0, "n_trades": 1, "source": S.RECON_SOURCE},
        {"coin": "xyz:GOLD", "timestamp_utc": t0, "open": 4.0, "high": 4.0, "low": 4.0,
         "close": 4.0, "volume": 2.0, "n_trades": 1, "source": S.RECON_SOURCE},
    ]
    pd.DataFrame(rows).to_parquet(candles / f"{_today_stem()}.parquet", index=False)
    return {"cache": cache, "candles": candles, "t0": t0}


def _sync(col, env, dry_run=False, days=3):
    return S.sync(env["candles"], URI, days, dry_run, db=DB, collection=col.name)


def test_insert_pair_convention_and_idempotent(col, env):
    r1 = _sync(col, env)
    assert r1["inserted"] == 4
    assert col.find_one({"coin": "kPEPE"})["pair"] == "1000PEPE-USDT"     # B1 prefix map
    assert col.find_one({"coin": "xyz:GOLD"})["pair"] == "xyz:GOLD-USDT"
    assert col.find_one({"coin": "BTC"})["source"] == S.RECON_SOURCE
    r2 = _sync(col, env)                                                  # idempotent
    assert r2["inserted"] == 0 and r2["amended"] == 0


def test_api_doc_precedence_never_touched_and_api_overwrites_in_place(col, env):
    # Pre-seed an API-exact doc at a minute the parquet also carries, with DIFFERENT OHLC.
    key = {"coin": "BTC", "interval": "1m", "timestamp_utc": env["t0"]}
    col.insert_one({**key, "pair": "BTC-USDT", "source": "hl_api", "open": 7.0, "high": 7.0,
                    "low": 7.0, "close": 7.0, "volume": 0.0, "n_trades": 0})
    _sync(col, env)
    d = col.find_one(key)
    assert d["close"] == 7.0 and d["source"] == "hl_api"      # amend filter excluded it (B2)
    # The API loader's own pair-keyed $set upsert on a SYNC-written minute must match in place
    # (no E11000 on either unique index) and overwrite — API wins (B1).
    k2 = {"coin": "BTC", "interval": "1m", "timestamp_utc": env["t0"] + 60_000}
    res = col.bulk_write([UpdateOne(
        {"pair": coin_to_pair("BTC"), "interval": "1m", "timestamp_utc": k2["timestamp_utc"]},
        {"$set": {**k2, "pair": coin_to_pair("BTC"), "open": 8.0, "high": 8.0, "low": 8.0,
                  "close": 8.0, "volume": 1.0, "n_trades": 1, "source": "hl_api"}},
        upsert=True)])
    assert res.upserted_count == 0 and res.modified_count == 1
    assert col.find_one(k2)["close"] == 8.0


def test_amend_corrects_sync_sourced_partial_minute(col, env):
    # Pre-seed a sync-sourced doc with a WRONG close (a frozen partial from a pre-rewrite file).
    key = {"coin": "BTC", "interval": "1m", "timestamp_utc": env["t0"]}
    col.insert_one({**key, "pair": "BTC-USDT", "source": S.RECON_SOURCE, "open": 1.0,
                    "high": 1.0, "low": 1.0, "close": 999.0, "volume": 0.1, "n_trades": 1})
    r = _sync(col, env)
    assert col.find_one(key)["close"] == 1.5                  # amended to the rewritten value (B2)
    assert r["amended"] >= 1


def test_cache_invalidated_only_for_touched_coins(col, env):
    for c in ("BTC", "xyz:GOLD", "UNTOUCHED"):
        S._cache_path(c).write_bytes(b"x")                    # module-derived path (quote ':')
    _sync(col, env)
    assert not S._cache_path("BTC").exists()
    assert not S._cache_path("xyz:GOLD").exists()             # ':'-coin deletion, explicit
    assert S._cache_path("UNTOUCHED").exists()


def test_leftover_journal_processed_at_startup(col, env):
    S._cache_path("CRASHED").write_bytes(b"x")
    S.JOURNAL.write_text(json.dumps(["CRASHED"]))
    _sync(col, env)
    assert not S._cache_path("CRASHED").exists()              # B5: over-delete, never under-delete
    assert not S.JOURNAL.exists()


def test_dry_run_writes_deletes_journals_nothing(col, env):
    S._cache_path("BTC").write_bytes(b"x")
    r = _sync(col, env, dry_run=True)
    assert col.count_documents({}) == 0
    assert S._cache_path("BTC").exists()
    assert not S.JOURNAL.exists()
    assert r["inserted"] == 0


def test_stale_hot_store_exits_2(col, env, tmp_path):
    stale = tmp_path / "stale"
    stale.mkdir()
    (env["candles"] / f"{_today_stem()}.parquet").rename(stale / "20260101.parquet")
    with pytest.raises(SystemExit) as e:
        S.sync(stale, URI, 3, False, db=DB, collection=col.name)
    assert e.value.code == 2


def test_normalize_heal_fixes_pair_and_source_only(col, env):
    t = env["t0"]
    col.insert_one({"coin": "kPEPE", "interval": "1m", "timestamp_utc": t, "pair": "kPEPE",
                    "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0,
                    "n_trades": 1})                            # heal-era shape: bare pair, no source
    col.insert_one({"coin": "ETH", "interval": "1m", "timestamp_utc": t, "pair": "ETH-USDT",
                    "source": "hl_api", "open": 2.0, "high": 2.0, "low": 2.0, "close": 2.0,
                    "volume": 1.0, "n_trades": 1})
    n = S.normalize_heal(col, dry_run=False)
    assert n == 1
    d = col.find_one({"coin": "kPEPE"})
    assert d["pair"] == "1000PEPE-USDT" and d["source"] == S.RECON_SOURCE
    assert d["close"] == 1.0                                   # OHLCV untouched
    assert col.find_one({"coin": "ETH"})["source"] == "hl_api"  # -USDT docs untouched


def test_codex1_corrupt_journal_fails_closed(col, env):
    """A truncated/corrupt journal must NOT be read as an empty set and deleted (that is the silent
    under-invalidation B5 exists to prevent). Fail closed, leave the file for a human."""
    S._cache_path("BTC").write_bytes(b"x")
    S.JOURNAL.write_text('["BTC", "xyz:GO')          # torn write
    with pytest.raises(SystemExit, match="JOURNAL UNREADABLE"):
        _sync(col, env)
    assert S.JOURNAL.exists()                        # not deleted
    assert S._cache_path("BTC").exists()             # nothing silently blessed


def test_codex1_journal_write_is_atomic_and_complete(col, env):
    S._cache_path("BTC").write_bytes(b"x")
    _sync(col, env)
    assert not S.JOURNAL.exists()                    # cleared after successful invalidation
    assert not list(S.OHLC_CACHE_DIR.glob("*.tmp"))  # no temp residue


def test_codex2_normalize_heal_leaves_odd_api_docs_alone(col, env):
    """Only the exact heal-era shape (pair == coin AND no source) may be relabelled. A legacy/
    malformed API doc must NOT be turned into a reconstructed doc (that would invert precedence)."""
    t = env["t0"]
    col.insert_one({"coin": "BTC", "interval": "1m", "timestamp_utc": t, "pair": "BTC",
                    "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0,
                    "n_trades": 1})                                    # heal-era -> fix
    col.insert_one({"coin": "ETH", "interval": "1m", "timestamp_utc": t, "pair": "ETH-PERP",
                    "source": "hl_api", "open": 2.0, "high": 2.0, "low": 2.0, "close": 2.0,
                    "volume": 1.0, "n_trades": 1})                     # odd pair BUT api -> leave
    col.insert_one({"coin": "SOL", "interval": "1m", "timestamp_utc": t, "pair": "SOL-WEIRD",
                    "open": 3.0, "high": 3.0, "low": 3.0, "close": 3.0, "volume": 1.0,
                    "n_trades": 1})                                    # odd pair, no source -> leave
    assert S.normalize_heal(col, dry_run=False) == 1
    assert col.find_one({"coin": "BTC"})["source"] == S.RECON_SOURCE
    assert col.find_one({"coin": "ETH"})["source"] == "hl_api"
    assert col.find_one({"coin": "ETH"})["pair"] == "ETH-PERP"
    assert "source" not in col.find_one({"coin": "SOL"})


def test_codex1b_journal_tmp_never_leaks_on_failure(col, env, monkeypatch):
    """A raise anywhere in the atomic-write sequence must not leave a .tmp behind."""
    import json as _json
    monkeypatch.setattr(_json, "dump", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(RuntimeError):
        S._write_journal({"BTC"})
    assert not list(S.OHLC_CACHE_DIR.glob("*.tmp"))
    assert not S.JOURNAL.exists()
