#!/usr/bin/env python3
"""Reproducible, read-only audit of Hyperliquid fill parquet datasets.

The default ``metadata`` level reads only parquet footers. ``basic`` also scans
the enriched fill columns with DuckDB. ``full`` adds exact duplicate-key checks
and can be materially slower on the complete dataset.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]
CANONICAL_START_MS = 1_764_547_200_000  # 2025-12-01 00:00:00 UTC
DEFAULT_DATASETS = {
    "legacy_fills": ROOT / "app/data/hl_s3_fills",
    "enriched_fills": ROOT / "app/data/hl_s3_fills_v2",
    "wallet_fills": ROOT / "app/data/hl_s3_fills_v2_by_wallet",
}


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime, Path)):
        return str(value)
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def _schema_id(parquet_file: pq.ParquetFile) -> str:
    schema = parquet_file.schema_arrow.remove_metadata()
    return hashlib.sha256(str(schema).encode()).hexdigest()[:12]


def profile_parquet_metadata(path: Path, daily: bool) -> dict[str, Any]:
    files = sorted(path.glob("*.parquet"))
    rows: list[dict[str, Any]] = []
    schemas: Counter[str] = Counter()
    schema_text: dict[str, str] = {}
    unreadable: list[dict[str, str]] = []

    for file in files:
        try:
            parquet_file = pq.ParquetFile(file)
            schema_id = _schema_id(parquet_file)
            schemas[schema_id] += 1
            schema_text[schema_id] = str(parquet_file.schema_arrow.remove_metadata())
            rows.append(
                {
                    "file": file.name,
                    "rows": parquet_file.metadata.num_rows,
                    "row_groups": parquet_file.metadata.num_row_groups,
                    "schema_id": schema_id,
                }
            )
        except Exception as exc:  # audit must report corrupt inputs, not abort
            unreadable.append({"file": file.name, "error": repr(exc)})

    result: dict[str, Any] = {
        "path": str(path.relative_to(ROOT)),
        "file_count": len(files),
        "row_count": sum(row["rows"] for row in rows),
        "empty_files": [row["file"] for row in rows if row["rows"] == 0],
        "unreadable_files": unreadable,
        "schemas": [
            {"schema_id": schema_id, "file_count": count, "schema": schema_text[schema_id]}
            for schema_id, count in sorted(schemas.items())
        ],
    }

    if daily:
        parsed_dates: list[date] = []
        invalid_names: list[str] = []
        for row in rows:
            try:
                parsed_dates.append(datetime.strptime(Path(row["file"]).stem, "%Y%m%d").date())
            except ValueError:
                invalid_names.append(row["file"])
        missing_dates: list[str] = []
        if parsed_dates:
            present = set(parsed_dates)
            cursor = min(parsed_dates)
            while cursor <= max(parsed_dates):
                if cursor not in present:
                    missing_dates.append(cursor.isoformat())
                cursor += timedelta(days=1)
        result.update(
            {
                "first_partition": min(parsed_dates).isoformat() if parsed_dates else None,
                "last_partition": max(parsed_dates).isoformat() if parsed_dates else None,
                "missing_partition_count": len(missing_dates),
                "missing_partitions": missing_dates,
                "invalid_partition_names": invalid_names,
            }
        )
    return result


def _query_one(connection: duckdb.DuckDBPyConnection, sql: str) -> dict[str, Any]:
    cursor = connection.execute(sql)
    columns = [item[0] for item in cursor.description]
    values = cursor.fetchone()
    return dict(zip(columns, values, strict=True))


def _query_rows(connection: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    cursor = connection.execute(sql)
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def scan_enriched_fills(path: Path, full: bool) -> dict[str, Any]:
    connection = duckdb.connect()
    connection.execute("SET threads = 6")
    connection.execute("SET memory_limit = '8GB'")
    # DuckDB inherits the host timezone. Without this, converting epoch millis
    # and formatting the date makes UTC partitions look misaligned on non-UTC
    # hosts (and changes at DST boundaries).
    connection.execute("SET TimeZone = 'UTC'")
    parquet_glob = str(path / "*.parquet").replace("'", "''")
    source = f"read_parquet('{parquet_glob}', union_by_name=true, filename=true)"

    summary = _query_one(
        connection,
        f"""
        SELECT
            count(*) AS rows,
            min(time) AS min_time_ms,
            max(time) AS max_time_ms,
            approx_count_distinct(wallet) AS approximate_wallets,
            approx_count_distinct(coin) AS approximate_coins,
            count(*) FILTER (WHERE wallet IS NULL) AS wallet_nulls,
            count(*) FILTER (WHERE coin IS NULL) AS coin_nulls,
            count(*) FILTER (WHERE time IS NULL) AS time_nulls,
            count(*) FILTER (WHERE try_cast(size AS DOUBLE) IS NULL) AS invalid_size,
            count(*) FILTER (WHERE try_cast(price AS DOUBLE) IS NULL) AS invalid_price,
            count(*) FILTER (WHERE try_cast(fee AS DOUBLE) IS NULL) AS invalid_fee,
            count(*) FILTER (WHERE try_cast(startPosition AS DOUBLE) IS NULL) AS invalid_start_position,
            count(*) FILTER (WHERE tid IS NULL) AS tid_nulls,
            count(*) FILTER (WHERE tid = 0) AS tid_zero
        FROM {source}
        """,
    )
    for key in ("min_time_ms", "max_time_ms"):
        value = summary[key]
        summary[key.replace("_ms", "_utc")] = (
            datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat() if value is not None else None
        )

    result: dict[str, Any] = {
        "summary": summary,
        "side_counts": _query_rows(
            connection, f"SELECT side, count(*) AS rows FROM {source} GROUP BY side ORDER BY rows DESC"
        ),
        "direction_counts": _query_rows(
            connection, f"SELECT dir, count(*) AS rows FROM {source} GROUP BY dir ORDER BY rows DESC"
        ),
        "source_counts": _query_rows(
            connection, f"SELECT source, count(*) AS rows FROM {source} GROUP BY source ORDER BY rows DESC"
        ),
        "market_class_counts": _query_rows(
            connection,
            f"""
            SELECT CASE
                WHEN starts_with(coin, '@') OR contains(coin, '/') OR coin = 'USDC' THEN 'spot'
                WHEN starts_with(coin, '#') THEN 'hip4_outcome'
                ELSE 'perp'
            END AS market_class, count(*) AS rows,
            approx_count_distinct(wallet) AS approximate_wallets,
            approx_count_distinct(coin) AS approximate_coins
            FROM {source}
            GROUP BY market_class ORDER BY rows DESC
            """,
        ),
        "partition_alignment": _query_one(
            connection,
            f"""
            SELECT count(*) FILTER (
                WHERE strftime(to_timestamp(time / 1000), '%Y%m%d') !=
                      regexp_extract(filename, '(\\d{{8}})\\.parquet$', 1)
            ) AS event_partition_mismatches
            FROM {source}
            """,
        ),
    }

    if full:
        result["duplicate_nonzero_wallet_tid"] = _query_one(
            connection,
            f"""
            WITH duplicate_keys AS (
                SELECT wallet, tid, count(*) AS occurrences
                FROM {source}
                WHERE tid != 0
                GROUP BY wallet, tid
                HAVING count(*) > 1
            )
            SELECT
                count(*) AS duplicate_keys,
                coalesce(sum(occurrences - 1), 0) AS excess_rows,
                coalesce(max(occurrences), 0) AS maximum_occurrences
            FROM duplicate_keys
            """,
        )
    connection.close()
    return result


def scan_wallet_partition_parity(daily_path: Path, wallet_path: Path) -> dict[str, Any]:
    """Compare canonical-window dailies with the per-wallet fast-path cache.

    M1 always prefers the wallet cache when a file exists, so count equality is
    insufficient: use an order-independent hash over every shared source field.
    """
    connection = duckdb.connect()
    connection.execute("SET threads = 6")
    connection.execute("SET memory_limit = '8GB'")
    daily_glob = str(daily_path / "*.parquet").replace("'", "''")
    wallet_glob = str(wallet_path / "*.parquet").replace("'", "''")
    common_fields = (
        "wallet, coin, side, size, price, time, tid, dir, closedPnl, "
        "startPosition, fee, builderFee, deployerFee, crossed"
    )
    daily_source = f"""
        SELECT {common_fields}
        FROM read_parquet('{daily_glob}', union_by_name=true)
        WHERE time >= {CANONICAL_START_MS}
    """
    wallet_source = f"""
        SELECT
            regexp_extract(filename, '(0x[0-9a-f]{{40}})', 1) AS wallet,
            coin, side, size, price, time, tid, dir, closedPnl,
            startPosition, fee, builderFee, deployerFee, crossed
        FROM read_parquet('{wallet_glob}', union_by_name=true, filename=true)
    """

    def fingerprint(source: str) -> dict[str, Any]:
        return _query_one(
            connection,
            f"""
            SELECT
                count(*) AS rows,
                min(time) AS min_time_ms,
                max(time) AS max_time_ms,
                count(DISTINCT wallet) AS wallets,
                bit_xor(hash({common_fields})) AS content_fingerprint
            FROM ({source})
            """,
        )

    daily = fingerprint(daily_source)
    wallet = fingerprint(wallet_source)
    connection.close()
    return {"daily": daily, "wallet": wallet, "exact_aggregate_match": daily == wallet}


def build_report(level: str) -> dict[str, Any]:
    report: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "datasets": {
            name: profile_parquet_metadata(path, daily=name != "wallet_fills")
            for name, path in DEFAULT_DATASETS.items()
        },
    }
    if level in {"basic", "full"}:
        report["enriched_scan"] = scan_enriched_fills(
            DEFAULT_DATASETS["enriched_fills"], full=level == "full"
        )
        report["wallet_partition_parity"] = scan_wallet_partition_parity(
            DEFAULT_DATASETS["enriched_fills"], DEFAULT_DATASETS["wallet_fills"]
        )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--level", choices=("metadata", "basic", "full"), default="metadata")
    parser.add_argument("--output", type=Path, help="optional JSON output path")
    args = parser.parse_args()
    report = build_report(args.level)
    rendered = json.dumps(report, indent=2, default=_json_default) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered)
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
