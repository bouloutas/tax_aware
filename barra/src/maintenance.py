"""Maintenance utilities for DuckDB analytics tables."""
from __future__ import annotations

import argparse

import duckdb

from .config import ANALYTICS_DB


def rebuild_style_exposure_index(conn: duckdb.DuckDBPyConnection | None = None) -> None:
    """Drop/recreate the unique index on style factor exposures."""
    should_close = False
    if conn is None:
        conn = duckdb.connect(ANALYTICS_DB.as_posix())
        should_close = True
    try:
        conn.execute(
            "DROP INDEX IF EXISTS analytics.idx_style_factor_exposures_unique"
        )
        conn.execute(
            "CREATE UNIQUE INDEX idx_style_factor_exposures_unique "
            "ON analytics.style_factor_exposures (month_end_date, gvkey, factor)"
        )
    finally:
        if should_close:
            conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Maintenance helpers for the analytics DB.")
    parser.add_argument(
        "--rebuild-style-index",
        action="store_true",
        help="Drop/recreate the style_factor_exposures unique index.",
    )
    args = parser.parse_args()
    if args.rebuild_style_index:
        rebuild_style_exposure_index()
        print("Rebuilt analytics.idx_style_factor_exposures_unique")


if __name__ == "__main__":
    main()
