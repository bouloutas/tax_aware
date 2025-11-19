"""One-stop workflow to rebuild analytics, run pipeline, QA, and distribute reports."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
from pathlib import Path
from typing import Iterable, Sequence

from .build_analytics_db import build_analytics_db
from .run_pipeline import run_pipeline
from . import distribute_reports
from .export_reports import ARTIFACT_LOADERS

from tests import qa_checks

LOGGER = logging.getLogger(__name__)


def parse_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y-%m-%d").date()


def run_workflow(
    as_of: dt.date,
    rebuild_analytics: bool = False,
    run_qa_checks: bool = True,
    distribute: bool = True,
    release_root: Path | None = Path("exports/releases"),
    formats: Sequence[str] = ("csv", "parquet"),
    artifacts: Iterable[str] | None = None,
    portfolio_top_n: int = 100,
) -> dict:
    summary: dict[str, object] = {"as_of": as_of.isoformat()}

    if rebuild_analytics:
        LOGGER.info("Rebuilding analytics DuckDB before running pipeline")
        build_analytics_db()
        summary["rebuild_analytics"] = True
    else:
        summary["rebuild_analytics"] = False

    LOGGER.info("Running pipeline for %s", as_of)
    run_pipeline(as_of)
    summary["pipeline"] = "completed"

    if run_qa_checks:
        LOGGER.info("Running QA checks for %s", as_of)
        qa_results = qa_checks.run_checks(as_of)
        failures = [name for name, ok, _ in qa_results if not ok]
        summary["qa"] = {
            "failures": failures,
            "results": qa_results,
        }
        if failures:
            raise RuntimeError(f"QA checks failed for {as_of}: {failures}")
    else:
        summary["qa"] = None

    if distribute and release_root is not None:
        LOGGER.info("Distributing reports to %s", release_root)
        release_path = distribute_reports.distribute(
            as_of,
            release_root,
            formats=formats,
            artifacts=artifacts,
            portfolio_top_n=portfolio_top_n,
        )
        summary["release_dir"] = release_path.as_posix()
    else:
        summary["release_dir"] = None

    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run full Barra workflow (build, pipeline, QA, distribute)")
    parser.add_argument("--date", required=True, help="Month-end date (YYYY-MM-DD)")
    parser.add_argument(
        "--rebuild-analytics",
        action="store_true",
        help="Rebuild analytics DuckDB before running the pipeline",
    )
    parser.add_argument("--skip-qa", action="store_true", help="Skip QA checks")
    parser.add_argument("--skip-distribution", action="store_true", help="Skip report distribution")
    parser.add_argument(
        "--release-root",
        default="exports/releases",
        help="Directory to store release bundles (default: exports/releases)",
    )
    parser.add_argument(
        "--format",
        dest="formats",
        action="append",
        choices=("csv", "parquet"),
        help="Output formats (defaults to csv + parquet)",
    )
    parser.add_argument(
        "--artifact",
        dest="artifacts",
        action="append",
        choices=tuple(ARTIFACT_LOADERS.keys()),
        help="Artifacts to include (defaults to exporter defaults)",
    )
    parser.add_argument(
        "--portfolio-top-n",
        type=int,
        default=100,
        help="Top constituents for portfolio summary export (default: 100)",
    )
    parser.add_argument(
        "--json-out",
        help="Optional path to dump JSON summary",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level (default: INFO)",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(message)s")
    as_of = parse_date(args.date)
    formats = tuple(args.formats) if args.formats else ("csv", "parquet")
    release_root = None if args.skip_distribution else Path(args.release_root)
    summary = run_workflow(
        as_of,
        rebuild_analytics=args.rebuild_analytics,
        run_qa_checks=not args.skip_qa,
        distribute=not args.skip_distribution,
        release_root=release_root,
        formats=formats,
        artifacts=args.artifacts,
        portfolio_top_n=args.portfolio_top_n,
    )
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(summary, indent=2, default=str))
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
