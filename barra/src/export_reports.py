"""Export helpers for Barra reporting (Phase 4)."""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import inspect
from pathlib import Path
from typing import Iterable, Sequence

import duckdb
import pandas as pd
import numpy as np

from .config import ANALYTICS_DB

LOGGER = logging.getLogger(__name__)


def parse_date(value: str) -> dt.date:
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}', expected YYYY-MM-DD") from exc


def _fetch_dataframe(query: str, params: Sequence[object]) -> pd.DataFrame:
    with duckdb.connect(ANALYTICS_DB.as_posix(), read_only=True) as conn:
        return conn.execute(query, params).fetchdf()


def load_style_exposures(as_of: dt.date) -> pd.DataFrame:
    query = """
    SELECT month_end_date, gvkey, factor, exposure, flags
    FROM analytics.style_factor_exposures
    WHERE month_end_date = ?
    ORDER BY gvkey, factor
    """
    return _fetch_dataframe(query, [as_of])


def load_factor_returns(as_of: dt.date) -> pd.DataFrame:
    query = """
    SELECT month_end_date, factor, factor_return
    FROM analytics.factor_returns
    WHERE month_end_date = ?
    ORDER BY factor
    """
    return _fetch_dataframe(query, [as_of])


def load_factor_covariance(as_of: dt.date) -> pd.DataFrame:
    query = """
    SELECT month_end_date, factor_i, factor_j, covariance
    FROM analytics.factor_covariance
    WHERE month_end_date = ?
    ORDER BY factor_i, factor_j
    """
    return _fetch_dataframe(query, [as_of])


def load_specific_risk(as_of: dt.date) -> pd.DataFrame:
    query = """
    SELECT month_end_date, gvkey, specific_var
    FROM analytics.specific_risk
    WHERE month_end_date = ?
    ORDER BY gvkey
    """
    return _fetch_dataframe(query, [as_of])


def load_top_constituents(as_of: dt.date, top_n: int) -> pd.DataFrame:
    query = """
    SELECT gvkey, month_end_market_cap
    FROM analytics.monthly_returns
    WHERE month_end_date = ?
    ORDER BY month_end_market_cap DESC
    LIMIT ?
    """
    df = _fetch_dataframe(query, [as_of, top_n])
    return df.rename(columns={"GVKEY": "gvkey", "MONTH_END_MARKET_CAP": "month_end_market_cap"})


def load_portfolio_summary(as_of: dt.date, top_n: int = 50) -> pd.DataFrame:
    weights = load_top_constituents(as_of, top_n)
    if weights.empty:
        raise ValueError(f"No constituents available for {as_of}")
    weights = weights.copy()
    weights["weight"] = weights["month_end_market_cap"] / weights["month_end_market_cap"].sum()
    exposures = load_style_exposures(as_of)
    pivot = (
        exposures.pivot_table(index="gvkey", columns="factor", values="exposure", fill_value=0.0)
        .reindex(weights["gvkey"], fill_value=0.0)
    )
    factor_cols = list(pivot.columns)
    if not factor_cols:
        raise ValueError("No style exposures available for portfolio summary")
    cov = load_factor_covariance(as_of)
    cov_matrix = (
        cov.pivot(index="factor_i", columns="factor_j", values="covariance")
        .reindex(index=factor_cols, columns=factor_cols, fill_value=0.0)
    )
    cov_matrix = cov_matrix.fillna(0.0)
    X = pivot.values
    w = weights["weight"].values
    portfolio_exposure = w @ X
    cov_values = cov_matrix.values
    marginal = cov_values @ portfolio_exposure
    factor_contrib = portfolio_exposure * marginal
    summary = pd.DataFrame(
        {
            "factor": factor_cols,
            "portfolio_exposure": portfolio_exposure,
            "variance_contribution": factor_contrib,
            "type": "factor",
        }
    )
    specific = load_specific_risk(as_of).set_index("gvkey")["specific_var"]
    specific_vals = specific.reindex(weights["gvkey"]).fillna(0.0).values
    specific_var = float(((w ** 2) * specific_vals).sum())
    factor_var = float(portfolio_exposure @ cov_values @ portfolio_exposure)
    total_var = factor_var + specific_var
    agg = pd.DataFrame(
        [
            {
                "factor": "specific_risk",
                "portfolio_exposure": np.nan,
                "variance_contribution": specific_var,
                "type": "aggregate",
            },
            {
                "factor": "total",
                "portfolio_exposure": np.nan,
                "variance_contribution": total_var,
                "type": "aggregate",
            },
        ]
    )
    summary = pd.concat([summary, agg], ignore_index=True)
    summary["top_n"] = top_n
    return summary


def write_frame(df: pd.DataFrame, path: Path, fmt: str) -> Path:
    if fmt == "parquet":
        df.to_parquet(path, index=False)
    elif fmt == "csv":
        df.to_csv(path, index=False)
    else:
        raise ValueError(f"Unsupported format: {fmt}")
    return path


ARTIFACT_LOADERS = {
    "style_exposures": load_style_exposures,
    "factor_returns": load_factor_returns,
    "factor_covariance": load_factor_covariance,
    "specific_risk": load_specific_risk,
    "portfolio_summary": load_portfolio_summary,
}


def export_reports(
    as_of: dt.date,
    output_dir: Path,
    formats: Iterable[str],
    artifacts: Iterable[str] | None = None,
    portfolio_top_n: int = 50,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    selected = tuple(artifacts or ("style_exposures", "factor_returns"))
    invalid = [name for name in selected if name not in ARTIFACT_LOADERS]
    if invalid:
        raise ValueError(f"Unknown artifact(s): {', '.join(invalid)}")

    data_frames: dict[str, pd.DataFrame] = {}
    for name in selected:
        loader = ARTIFACT_LOADERS[name]
        kwargs = {}
        sig = inspect.signature(loader)
        if "top_n" in sig.parameters:
            kwargs["top_n"] = portfolio_top_n
        df = loader(as_of, **kwargs)
        if df.empty:
            raise ValueError(f"No data available for {name} on {as_of}")
        data_frames[name] = df

    outputs: dict[str, Path] = {}
    for fmt in formats:
        for name, df in data_frames.items():
            path = output_dir / f"{name}_{as_of}.{fmt}"
            write_frame(df, path, fmt)
            outputs[f"{name}_{fmt}"] = path
            LOGGER.info("Wrote %s", path.name)

    return outputs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export Barra analytics tables for reporting")
    parser.add_argument("--date", required=True, help="Month-end date (YYYY-MM-DD)")
    parser.add_argument(
        "--output-dir",
        default="exports",
        help="Directory to store exported files (default: exports)",
    )
    parser.add_argument(
        "--format",
        dest="formats",
        action="append",
        choices=("csv", "parquet"),
        help="Output format(s); specify multiple times for more than one.",
    )
    parser.add_argument(
        "--artifact",
        dest="artifacts",
        action="append",
        choices=tuple(ARTIFACT_LOADERS.keys()),
        help="Artifacts to export (defaults to style_exposures + factor_returns)",
    )
    parser.add_argument(
        "--portfolio-top-n",
        type=int,
        default=50,
        help="Number of largest names to include when computing portfolio_summary (default: 50)",
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
    formats = args.formats or ["parquet"]
    export_reports(
        as_of,
        Path(args.output_dir),
        formats,
        artifacts=args.artifacts,
        portfolio_top_n=args.portfolio_top_n,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
