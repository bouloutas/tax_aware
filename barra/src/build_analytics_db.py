"""
Utility to materialize the analytics schema described in Phase 1.

Usage:
    python -m barra.src.build_analytics_db
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Sequence

import duckdb

from .config import ANALYTICS_DB

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
DEFAULT_SQL_FILES: Sequence[Path] = (
    SQL_DIR / "phase1_create_analytics_views.sql",
    SQL_DIR / "phase1_create_quarterly_views.sql",
)


def build_analytics_db(
    db_path: Path = ANALYTICS_DB, sql_paths: Iterable[Path] | None = None
) -> None:
    if sql_paths is None:
        sql_paths = DEFAULT_SQL_FILES

    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    for sql_path in sql_paths:
        sql_path = Path(sql_path)
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file missing: {sql_path}")
        run_sql_script(db_path, sql_path)


def run_sql_script(db_path: Path, sql_path: Path) -> None:
    conn = duckdb.connect(db_path.as_posix())
    try:
        for statement in iter_statements(sql_path.read_text()):
            conn.execute(statement)
    finally:
        conn.close()


def iter_statements(sql: str):
    """Generate SQL statements while stripping line comments."""
    buffer = []
    for line in sql.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        buffer.append(line)
        if ";" in line:
            chunk = "\n".join(buffer)
            before_semicolon, _, after = chunk.partition(";")
            yield before_semicolon.strip()
            # start new buffer with any trailing content after semicolon
            buffer = [after] if after.strip() else []
    if buffer:
        yield "\n".join(buffer).strip()


def main():
    parser = argparse.ArgumentParser(description="Build/refresh analytics DuckDB schema.")
    parser.add_argument(
        "--db",
        type=Path,
        default=ANALYTICS_DB,
        help="Path to analytics DuckDB file (default: %(default)s)",
    )
    parser.add_argument(
        "--sql",
        type=Path,
        action="append",
        help=(
            "Optional SQL script(s) to execute. Repeat the flag to run multiple files. "
            "Defaults to the standard analytics + quarterly builders."
        ),
    )
    args = parser.parse_args()
    build_analytics_db(args.db, args.sql)


if __name__ == "__main__":
    main()
