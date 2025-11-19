"""
Utility helpers for interacting with the DuckDB files that back the Barra model.

Only thin wrappers are provided here; heavy ETL work happens through SQL scripts
checked into ../sql. These helpers ensure downstream code consistently opens
connections in read-only or read-write mode, introspects schemas, and runs
simple validation queries defined in tests.md.
"""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, List, Optional

import duckdb

from .config import ANALYTICS_DB, COMPUSTAT_DB, PRICE_DB


class DuckDBConnectionManager:
    """Context manager based connection helper."""

    def __init__(self, database: Path) -> None:
        self.database = Path(database)
        if not self.database.exists():
            raise FileNotFoundError(f"DuckDB file missing: {self.database}")

    @contextmanager
    def connect(self, read_only: bool = True) -> Iterator[duckdb.DuckDBPyConnection]:
        conn = duckdb.connect(self.database.as_posix(), read_only=read_only)
        try:
            yield conn
        finally:
            conn.close()

    def table_exists(self, table: str, schema: str = "main") -> bool:
        query = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_name = ?
        LIMIT 1;
        """
        with self.connect() as conn:
            return (
                conn.execute(query, [schema, table]).fetchone() is not None
            )

    def list_columns(self, table: str, schema: str = "main") -> List[str]:
        query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ?
          AND table_name = ?
        ORDER BY ordinal_position;
        """
        with self.connect() as conn:
            rows = conn.execute(query, [schema, table]).fetchall()
        return [name for (name,) in rows]


compustat_manager = DuckDBConnectionManager(COMPUSTAT_DB)
price_manager = DuckDBConnectionManager(PRICE_DB)
# Analytics DB might not exist yet; create lazily when Phase 1 materializes tables.
analytics_db_path = Path(ANALYTICS_DB)
if analytics_db_path.exists():
    analytics_manager: Optional[DuckDBConnectionManager] = DuckDBConnectionManager(
        analytics_db_path
    )
else:
    analytics_manager = None
