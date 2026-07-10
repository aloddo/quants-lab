from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from research.phase1_data_audit import scan_enriched_fills, scan_wallet_partition_parity


def test_partition_alignment_is_utc_not_host_timezone(tmp_path: Path) -> None:
    # 2026-06-24 23:30 UTC is already 2026-06-25 in Europe/Sofia. The audit
    # must compare against the UTC partition date, independent of host TZ.
    table = pa.table(
        {
            "wallet": ["0x" + "1" * 40] * 4,
            "coin": ["BTC", "@107", "PURR/USDC", "#42"],
            "side": ["B"] * 4,
            "size": ["0.001"] * 4,
            "price": ["100000"] * 4,
            "time": [1782343800000] * 4,
            "dir": ["Open Long", "Buy", "Buy", "Buy"],
            "closedPnl": ["0"] * 4,
            "startPosition": ["0"] * 4,
            "fee": ["0.045"] * 4,
            "source": ["test"] * 4,
            "tid": [1, 2, 3, 4],
        }
    )
    pq.write_table(table, tmp_path / "20260624.parquet")

    report = scan_enriched_fills(tmp_path, full=False)

    assert report["partition_alignment"]["event_partition_mismatches"] == 0
    classes = {row["market_class"]: row["rows"] for row in report["market_class_counts"]}
    assert classes == {"perp": 1, "spot": 2, "hip4_outcome": 1}


def test_wallet_fast_path_matches_daily_source(tmp_path: Path) -> None:
    daily = tmp_path / "daily"
    wallet = tmp_path / "wallet"
    daily.mkdir()
    wallet.mkdir()
    address = "0x" + "a" * 40
    row = {
        "wallet": [address],
        "coin": ["BTC"],
        "side": ["B"],
        "size": ["0.001"],
        "price": ["100000"],
        "time": [1764547200001],
        "tid": [7],
        "dir": ["Open Long"],
        "closedPnl": ["0"],
        "startPosition": ["0"],
        "fee": ["0.045"],
        "builderFee": ["0"],
        "deployerFee": ["0"],
        "crossed": [True],
    }
    pq.write_table(pa.table(row), daily / "20251201.parquet")
    wallet_row = {key: value for key, value in row.items() if key != "wallet"}
    pq.write_table(pa.table(wallet_row), wallet / f"{address}.parquet")

    report = scan_wallet_partition_parity(daily, wallet)

    assert report["exact_aggregate_match"] is True
