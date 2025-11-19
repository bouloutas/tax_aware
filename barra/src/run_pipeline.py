"""End-to-end pipeline: exposures -> regression -> covariance."""
from __future__ import annotations

import argparse
import datetime as dt
import logging

from .run_factors import (
    compute_style_exposures,
    persist_style_exposures,
    compute_industry_exposures,
    persist_industry_exposures,
    compute_country_exposures,
    persist_country_exposures,
)
from .regression import RegressionEngine
from .covariance import CovarianceEngine

LOGGER = logging.getLogger(__name__)


def parse_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y-%m-%d").date()


def run_pipeline(as_of: dt.date) -> None:
    LOGGER.info("Starting Barra pipeline for %s", as_of)
    exposures = compute_style_exposures(as_of)
    persist_style_exposures(exposures, as_of)
    industry = compute_industry_exposures(as_of)
    persist_industry_exposures(industry, as_of)
    country = compute_country_exposures(as_of)
    persist_country_exposures(country, as_of)

    engine = RegressionEngine()
    try:
        result = engine.regress(as_of)
        engine.persist(result)
    finally:
        engine.close()

    cov_engine = CovarianceEngine()
    try:
        try:
            cov_df = cov_engine.compute_covariance(as_of)
            cov_engine.persist(cov_df)
            LOGGER.info("Stored covariance matrix for %s", as_of)
        except ValueError as exc:
            LOGGER.warning(
                "Covariance step skipped for %s: %s. Re-run once sufficient history is available.",
                as_of,
                exc,
            )
    finally:
        cov_engine.close()
    LOGGER.info("Pipeline completed for %s", as_of)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run full Barra pipeline for a month-end date.")
    parser.add_argument("--date", required=True, help="Month-end date (YYYY-MM-DD)")
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level to use (default: INFO)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(message)s")
    run_pipeline(parse_date(args.date))


if __name__ == "__main__":
    main()
