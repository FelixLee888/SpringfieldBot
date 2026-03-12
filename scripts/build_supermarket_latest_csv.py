#!/usr/bin/env python3
"""Download the Kaggle-derived supermarket parquet snapshot and emit a latest-offers CSV."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from urllib.request import urlretrieve

PARQUET_URL = (
    "https://huggingface.co/datasets/Rif-SQL/time-series-uk-retail-supermarket-price-data/"
    "resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parent.parent / "data" / "community_supermarket_latest.csv"),
        help="CSV path to write",
    )
    parser.add_argument(
        "--metadata-output",
        default=str(Path(__file__).resolve().parent.parent / "data" / "community_supermarket_latest.meta.json"),
        help="Optional metadata JSON path",
    )
    parser.add_argument(
        "--parquet",
        default="",
        help="Existing parquet file path to reuse instead of downloading",
    )
    return parser.parse_args()


def main() -> int:
    try:
        import duckdb
    except ImportError as exc:
        raise SystemExit("duckdb is required to build the CSV snapshot") from exc

    args = parse_args()
    output_path = Path(args.output).expanduser().resolve()
    metadata_path = Path(args.metadata_output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    cleanup_parquet = False
    if args.parquet:
        parquet_path = Path(args.parquet).expanduser().resolve()
    else:
        fd, temp_name = tempfile.mkstemp(prefix="springfield-price-", suffix=".parquet")
        os.close(fd)
        parquet_path = Path(temp_name)
        cleanup_parquet = True
        urlretrieve(PARQUET_URL, parquet_path)

    connection = duckdb.connect()
    try:
        parquet_sql = str(parquet_path).replace("'", "''")
        output_sql = str(output_path).replace("'", "''")
        connection.execute(
            f"""
            COPY (
              WITH ranked AS (
                SELECT
                  supermarket_name,
                  CAST(price_gbp AS DOUBLE) AS price_gbp,
                  CAST(price_unit_gbp AS DOUBLE) AS price_unit_gbp,
                  unit,
                  product_name,
                  regexp_replace(lower(product_name), '[^a-z0-9]+', ' ', 'g') AS normalized_product_name,
                  CAST(capture_date AS VARCHAR) AS capture_date,
                  category_name,
                  CAST(is_own_brand AS BOOLEAN) AS is_own_brand,
                  ROW_NUMBER() OVER (
                    PARTITION BY supermarket_name, product_name
                    ORDER BY capture_date DESC, COALESCE(price_unit_gbp, price_gbp) ASC
                  ) AS row_num
                FROM read_parquet('{parquet_sql}')
              )
              SELECT
                supermarket_name,
                price_gbp,
                price_unit_gbp,
                unit,
                product_name,
                normalized_product_name,
                capture_date,
                category_name,
                is_own_brand
              FROM ranked
              WHERE row_num = 1
            ) TO '{output_sql}' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
            """
        )
        row_count = connection.execute(
            "SELECT COUNT(*) FROM read_csv_auto(?, HEADER=TRUE)", [str(output_path)]
        ).fetchone()[0]
    finally:
        connection.close()
        if cleanup_parquet and parquet_path.exists():
            parquet_path.unlink()

    metadata = {
        "source_url": PARQUET_URL,
        "output_csv": str(output_path),
        "row_count": int(row_count),
    }
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(metadata, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
