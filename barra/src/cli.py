"""Application-layer CLI entry points for the Barra pipeline."""
from __future__ import annotations

import argparse
import datetime as dt
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Sequence

import pandas as pd

from .config import (
    CURRENCY_BETA_LOOKBACK_MONTHS,
    CURRENCY_BETA_MIN_OBS,
    IMPUTATION_WARNING_THRESHOLD,
)
from .covariance import CovarianceEngine
from .regression import RegressionEngine
from .run_factors import (
    compute_country_exposures,
    compute_industry_exposures,
    compute_style_exposures,
    persist_country_exposures,
    persist_industry_exposures,
    persist_style_exposures,
)
from .run_pipeline import run_pipeline

LOGGER = logging.getLogger(__name__)


@dataclass
class CommandResult:
    """Represents the structured outcome of a CLI invocation."""

    status: int = 0
    details: Dict[str, Any] = field(default_factory=dict)


def parse_month_end(value: str) -> dt.date:
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}', expected YYYY-MM-DD") from exc


def _summarize_style_coverage(exposures: pd.DataFrame) -> pd.DataFrame:
    if exposures.empty:
        return pd.DataFrame(columns=["factor", "total", "imputed", "imputed_pct"])
    coverage = (
        exposures.assign(imputed=exposures["flags"].fillna("").str.contains("imputed"))
        .groupby("factor")
        .agg(total=("gvkey", "count"), imputed=("imputed", "sum"))
        .reset_index()
    )
    coverage["imputed_pct"] = coverage["imputed"] / coverage["total"].replace(0, pd.NA)
    return coverage


def run_factors_command(args: argparse.Namespace) -> CommandResult:
    as_of = parse_month_end(args.date)
    exposures = compute_style_exposures(
        as_of,
        factors=args.factors,
        currency_lookback=args.currency_lookback,
        currency_min_obs=args.currency_min_obs,
    )
    style_rows = persist_style_exposures(exposures, as_of)
    msg_parts: List[str] = [f"style={style_rows}"]
    industry_rows = 0
    country_rows = 0
    if not args.skip_industry:
        industry_df = compute_industry_exposures(as_of)
        industry_rows = persist_industry_exposures(industry_df, as_of)
        msg_parts.append(f"industry={industry_rows}")
    if not args.skip_country:
        country_df = compute_country_exposures(as_of)
        country_rows = persist_country_exposures(country_df, as_of)
        msg_parts.append(f"country={country_rows}")

    LOGGER.info("Stored exposures for %s (%s)", as_of, ", ".join(msg_parts))
    coverage = _summarize_style_coverage(exposures)
    if not coverage.empty:
        LOGGER.debug("Style coverage summary:\n%s", coverage.to_string(index=False))
        flagged = coverage[coverage["imputed_pct"] > IMPUTATION_WARNING_THRESHOLD]
        if not flagged.empty:
            LOGGER.warning(
                "High imputation detected for %s (threshold %.0f%%)",
                ", ".join(flagged["factor"].tolist()),
                IMPUTATION_WARNING_THRESHOLD * 100,
            )
    return CommandResult(
        details={
            "as_of": as_of,
            "style_rows": style_rows,
            "industry_rows": industry_rows,
            "country_rows": country_rows,
            "coverage": coverage,
        }
    )


def run_risk_command(args: argparse.Namespace) -> CommandResult:
    as_of = parse_month_end(args.date)
    regression = RegressionEngine()
    try:
        result = regression.regress(as_of)
        regression.persist(result)
        LOGGER.info(
            "Persisted regression outputs for %s (%d factor returns, %d residuals)",
            as_of,
            len(result.factor_returns),
            len(result.residuals),
        )
    finally:
        regression.close()

    cov_rows = None
    if not args.skip_covariance:
        cov_engine = CovarianceEngine()
        try:
            cov_df = cov_engine.compute_covariance(as_of, lookback_months=args.cov_lookback)
            cov_engine.persist(cov_df)
            cov_rows = len(cov_df)
            LOGGER.info("Persisted covariance matrix for %s (%d rows)", as_of, cov_rows)
        except ValueError as exc:
            LOGGER.warning("Covariance step skipped for %s: %s", as_of, exc)
        finally:
            cov_engine.close()

    return CommandResult(
        details={
            "as_of": as_of,
            "factor_count": len(result.factor_returns),
            "residual_count": len(result.residuals),
            "covariance_rows": cov_rows,
        }
    )


def run_pipeline_command(args: argparse.Namespace) -> CommandResult:
    as_of = parse_month_end(args.date)
    run_pipeline(as_of)
    LOGGER.info("Completed full pipeline for %s", as_of)
    return CommandResult(details={"as_of": as_of})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unified CLI for the Barra model pipeline")
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level (default: INFO)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    factors = subparsers.add_parser("run-factors", help="Compute and store factor exposures")
    factors.add_argument("--date", required=True, help="Month-end date (YYYY-MM-DD)")
    factors.add_argument(
        "--factor",
        dest="factors",
        action="append",
        help="Specific style factor(s) to compute; defaults to all",
    )
    factors.add_argument(
        "--currency-lookback",
        type=int,
        default=CURRENCY_BETA_LOOKBACK_MONTHS,
        help="Lookback months for currency beta regression",
    )
    factors.add_argument(
        "--currency-min-obs",
        type=int,
        default=CURRENCY_BETA_MIN_OBS,
        help="Minimum overlap for currency beta regression",
    )
    factors.add_argument("--skip-industry", action="store_true", help="Skip industry exposures")
    factors.add_argument("--skip-country", action="store_true", help="Skip country exposures")
    factors.set_defaults(handler=run_factors_command)

    risk = subparsers.add_parser("run-risk", help="Run regression + covariance steps")
    risk.add_argument("--date", required=True, help="Month-end date (YYYY-MM-DD)")
    risk.add_argument(
        "--cov-lookback",
        type=int,
        default=None,
        help="Override the default covariance lookback window",
    )
    risk.add_argument("--skip-covariance", action="store_true", help="Skip covariance computation")
    risk.set_defaults(handler=run_risk_command)

    pipeline = subparsers.add_parser("run-pipeline", help="Run factors, regression, and covariance")
    pipeline.add_argument("--date", required=True, help="Month-end date (YYYY-MM-DD)")
    pipeline.set_defaults(handler=run_pipeline_command)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(message)s")
    result = args.handler(args)
    return getattr(result, "status", 0)


if __name__ == "__main__":
    raise SystemExit(main())
