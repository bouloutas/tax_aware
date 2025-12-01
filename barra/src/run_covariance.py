"""CLI to compute factor covariance matrices."""
from __future__ import annotations

import argparse
import datetime as dt
import logging

from .covariance import CovarianceEngine
from .covariance_enhanced import EnhancedCovarianceEngine
from . import config


def parse_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y-%m-%d").date()


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute and store factor covariance matrix.")
    parser.add_argument("--date", required=True, help="Month-end date (YYYY-MM-DD)")
    parser.add_argument("--lookback", type=int, default=None, help="Months of factor returns history")
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level to use (default: INFO)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger(__name__)
    as_of = parse_date(args.date)
    
    # Choose engine based on feature flags
    use_enhanced = (
        config.BLEND_SHORT_LONG_COV or 
        config.SHRINK_COVARIANCE or 
        config.PSD_ENFORCEMENT
    )
    
    if use_enhanced:
        logger.info("Using EnhancedCovarianceEngine (Phase 3 features enabled)")
        engine = EnhancedCovarianceEngine()
        try:
            try:
                result = engine.compute_covariance(
                    as_of,
                    use_multi_horizon=config.BLEND_SHORT_LONG_COV,
                    use_shrinkage=config.SHRINK_COVARIANCE,
                    use_psd_enforcement=config.PSD_ENFORCEMENT
                )
                engine.persist(result)
                logger.info("Stored enhanced covariance matrix for %s", as_of)
            except ValueError as exc:
                logger.warning(
                    "Covariance step skipped for %s: %s. Re-run once sufficient history is available.",
                    as_of,
                    exc,
                )
        finally:
            engine.close()
    else:
        logger.info("Using baseline CovarianceEngine (Phase 3 features disabled)")
        engine = CovarianceEngine()
        try:
            try:
                cov_df = engine.compute_covariance(as_of, lookback_months=args.lookback)
                engine.persist(cov_df)
                logger.info("Stored covariance matrix for %s", as_of)
            except ValueError as exc:
                logger.warning(
                    "Covariance step skipped for %s: %s. Re-run once sufficient history is available.",
                    as_of,
                    exc,
                )
        finally:
            engine.close()


if __name__ == "__main__":
    main()

