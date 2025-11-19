"""Automate packaging of Barra exports under exports/releases/."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
from pathlib import Path
from typing import Iterable, Sequence

from . import export_reports

LOGGER = logging.getLogger(__name__)


def parse_date(value: str) -> dt.date:
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}', expected YYYY-MM-DD") from exc


def distribute(
    as_of: dt.date,
    release_root: Path,
    formats: Sequence[str] = ("csv", "parquet"),
    artifacts: Iterable[str] | None = None,
    portfolio_top_n: int = 100,
) -> Path:
    release_dir = release_root / as_of.isoformat()
    release_dir.mkdir(parents=True, exist_ok=True)
    outputs = export_reports.export_reports(
        as_of,
        release_dir,
        formats,
        artifacts=artifacts,
        portfolio_top_n=portfolio_top_n,
    )
    manifest = {
        "as_of": as_of.isoformat(),
        "artifacts": sorted(outputs.keys()),
        "formats": list(formats),
        "portfolio_top_n": portfolio_top_n,
    }
    manifest_path = release_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    LOGGER.info("Wrote manifest %s", manifest_path)
    latest_link = release_root / "latest"
    try:
        if latest_link.exists() or latest_link.is_symlink():
            if latest_link.is_symlink() or latest_link.is_file():
                latest_link.unlink()
            else:
                latest_link.rmdir()
    except FileNotFoundError:
        pass
    latest_link.symlink_to(release_dir, target_is_directory=True)
    LOGGER.info("Updated latest symlink -> %s", release_dir)
    return release_dir


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Package Barra exports for downstream distribution")
    parser.add_argument("--date", required=True, help="Month-end date (YYYY-MM-DD)")
    parser.add_argument(
        "--release-root",
        default="exports/releases",
        help="Directory for release folders (default: exports/releases)",
    )
    parser.add_argument(
        "--format",
        action="append",
        dest="formats",
        choices=("csv", "parquet"),
        help="Formats to emit (defaults to both)",
    )
    parser.add_argument(
        "--artifact",
        dest="artifacts",
        action="append",
        choices=tuple(export_reports.ARTIFACT_LOADERS.keys()),
        help="Artifacts to include (defaults to all exporter defaults)",
    )
    parser.add_argument(
        "--portfolio-top-n",
        type=int,
        default=100,
        help="Top constituents for portfolio summary (default: 100)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level (default: INFO)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(message)s")
    as_of = parse_date(args.date)
    release_root = Path(args.release_root)
    formats = tuple(args.formats) if args.formats else ("csv", "parquet")
    distribute(
        as_of,
        release_root,
        formats=formats,
        artifacts=args.artifacts,
        portfolio_top_n=args.portfolio_top_n,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
