"""Lightweight QA checks for analytics tables.

Run with:
    python tests/qa_checks.py --date YYYY-MM-DD
"""
from __future__ import annotations

import argparse
import datetime as dt
from collections import OrderedDict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import sys

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.config import ANALYTICS_DB

REQUIRED_TABLES: Tuple[str, ...] = (
    "fundamentals_annual",
    "fundamentals_quarterly",
    "monthly_prices",
    "monthly_returns",
    "market_index_returns",
    "gvkey_ticker_mapping",
    "style_factor_exposures",
    "industry_exposures",
    "country_exposures",
    "factor_returns",
    "specific_returns",
    "specific_risk",
    "factor_covariance",
)

IMPUTATION_WARNING_THRESHOLD = 0.30
RESIDUAL_MEAN_TOLERANCE = 1e-2  # allow slightly higher drift in early/volatile periods
FACTOR_MEAN_ABS_MAX = 0.15
FACTOR_STD_MIN = 0.5
FACTOR_STD_MAX = 1.5

def parse_date(value: str) -> dt.date:
    return pd.Timestamp(value).date()


def run_checks(as_of: dt.date) -> List[Tuple[str, bool, str]]:
    results: List[Tuple[str, bool, str]] = []
    with duckdb.connect(ANALYTICS_DB.as_posix(), read_only=True) as con:
        for table in REQUIRED_TABLES:
            exists = (
                con.execute(
                    """
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema='analytics' AND table_name=?
                    """,
                    [table],
                ).fetchone()[0]
                == 1
            )
            results.append((f"table_exists:{table}", exists, "present" if exists else "missing"))

        latest_date = con.execute("SELECT max(month_end_date) FROM analytics.monthly_returns").fetchone()[0]
        results.append(("latest_monthly_return_date", True, f"latest={latest_date}"))

        snapshot: Dict[str, int] = OrderedDict()
        for table in REQUIRED_TABLES:
            snapshot[table] = con.execute(f"SELECT COUNT(*) FROM analytics.{table}").fetchone()[0]
        results.append(("row_counts_snapshot", True, str(snapshot)))

        null_styles = con.execute(
            """
            SELECT COUNT(*) FROM analytics.style_factor_exposures
            WHERE month_end_date=? AND exposure IS NULL
            """,
            [as_of],
        ).fetchone()[0]
        results.append(("style_exposures_no_null", null_styles == 0, f"null_rows={null_styles}"))

        bad_industry = con.execute(
            """
            SELECT level, COUNT(*) AS bad_rows
            FROM (
                SELECT level, gvkey, ABS(SUM(exposure) - 1.0) AS diff
                FROM analytics.industry_exposures
                WHERE month_end_date=?
                GROUP BY level, gvkey
            )
            WHERE diff > 1e-9
            GROUP BY level
            """,
            [as_of],
        ).fetchall()
        results.append(("industry_one_hot", len(bad_industry) == 0, str(bad_industry)))

        bad_country = con.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT gvkey, SUM(exposure) AS total
                FROM analytics.country_exposures
                WHERE month_end_date=?
                GROUP BY gvkey
            )
            WHERE ABS(total - 1.0) > 1e-9
            """,
            [as_of],
        ).fetchone()[0]
        results.append(("country_exposure_sum", bad_country == 0, f"bad_rows={bad_country}"))

        flagged_imputations = con.execute(
            """
            SELECT COUNT(*) FROM analytics.style_factor_exposures
            WHERE month_end_date=? AND flags ILIKE '%imputed%'
            """,
            [as_of],
        ).fetchone()[0]
        total_exposures = con.execute(
            """
            SELECT COUNT(*) FROM analytics.style_factor_exposures
            WHERE month_end_date=?
            """,
            [as_of],
        ).fetchone()[0]
        share = (flagged_imputations / total_exposures) if total_exposures else 0.0
        results.append(
            (
                "imputation_share_under_threshold",
                share <= IMPUTATION_WARNING_THRESHOLD,
                f"share={share:.3f}, flagged={flagged_imputations}, total={total_exposures}",
            )
        )
        results.append(
            (
                "style_exposures_present",
                total_exposures > 0,
                f"rows={total_exposures}",
            )
        )

        factor_stats = con.execute(
            """
            SELECT factor, AVG(exposure) AS mean_exposure, STDDEV_SAMP(exposure) AS std_exposure
            FROM analytics.style_factor_exposures
            WHERE month_end_date=?
            GROUP BY factor
            """,
            [as_of],
        ).fetchdf()
        bad_means = factor_stats[abs(factor_stats["mean_exposure"]) > FACTOR_MEAN_ABS_MAX]
        bad_stds = factor_stats[
            (factor_stats["std_exposure"] < FACTOR_STD_MIN)
            | (factor_stats["std_exposure"] > FACTOR_STD_MAX)
        ]
        results.append(
            (
                "factor_mean_centered",
                bad_means.empty,
                bad_means.to_dict("records"),
            )
        )
        results.append(
            (
                "factor_std_reasonable",
                bad_stds.empty,
                bad_stds.to_dict("records"),
            )
        )

        factor_returns_count = con.execute(
            "SELECT COUNT(*) FROM analytics.factor_returns WHERE month_end_date=?",
            [as_of],
        ).fetchone()[0]
        unique_factors = con.execute(
            "SELECT COUNT(DISTINCT factor) FROM analytics.factor_returns WHERE month_end_date=?",
            [as_of],
        ).fetchone()[0]
        results.append(
            (
                "factor_count_consistency",
                factor_returns_count == unique_factors,
                f"rows={factor_returns_count}, unique={unique_factors}",
            )
        )

        effective_universe = con.execute(
            """
            SELECT COUNT(*) FROM analytics.monthly_returns
            WHERE month_end_date=? AND monthly_return IS NOT NULL AND month_end_market_cap IS NOT NULL
            """,
            [as_of],
        ).fetchone()[0]
        specific_returns_count = con.execute(
            "SELECT COUNT(*) FROM analytics.specific_returns WHERE month_end_date=?",
            [as_of],
        ).fetchone()[0]
        specific_risk_count = con.execute(
            "SELECT COUNT(*) FROM analytics.specific_risk WHERE month_end_date=?",
            [as_of],
        ).fetchone()[0]
        results.append(
            (
                "specific_counts_align",
                specific_returns_count == effective_universe and specific_risk_count == effective_universe,
                f"universe={effective_universe}, specific_returns={specific_returns_count}, specific_risk={specific_risk_count}",
            )
        )

        residual_mean, residual_var = con.execute(
            """
            SELECT AVG(residual), VAR_POP(residual)
            FROM analytics.specific_returns
            WHERE month_end_date=?
            """,
            [as_of],
        ).fetchone()
        results.append(
            (
                "residual_mean_near_zero",
                residual_mean is not None and abs(residual_mean) <= RESIDUAL_MEAN_TOLERANCE,
                f"mean={residual_mean}",
            )
        )
        results.append(
            (
                "residual_variance_positive",
                residual_var is not None and residual_var > 0,
                f"var={residual_var}",
            )
        )

        cov_rows = con.execute(
            "SELECT COUNT(*) FROM analytics.factor_covariance WHERE month_end_date=?",
            [as_of],
        ).fetchone()[0]
        distinct_cov_factors = con.execute(
            "SELECT COUNT(DISTINCT factor_i) FROM analytics.factor_covariance WHERE month_end_date=?",
            [as_of],
        ).fetchone()[0]
        results.append(
            (
                "covariance_dimension",
                cov_rows == distinct_cov_factors**2,
                f"cov_rows={cov_rows}, expected={distinct_cov_factors**2}",
            )
        )

        symmetry_gaps = con.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT f1.factor_i, f1.factor_j
                FROM analytics.factor_covariance f1
                LEFT JOIN analytics.factor_covariance f2
                  ON f1.month_end_date=f2.month_end_date
                 AND f1.factor_i=f2.factor_j
                 AND f1.factor_j=f2.factor_i
                WHERE f1.month_end_date=? AND f2.month_end_date IS NULL
            )
            """,
            [as_of],
        ).fetchone()[0]
        results.append(
            (
                "covariance_symmetry",
                symmetry_gaps == 0,
                f"missing_sym_pairs={symmetry_gaps}",
            )
        )

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Run analytics QA checks.")
    parser.add_argument("--date", required=True, help="Month-end date to validate (YYYY-MM-DD)")
    args = parser.parse_args()
    as_of = parse_date(args.date)
    results = run_checks(as_of)
    failed = False
    for check, passed, detail in results:
        status = "PASS" if passed else "FAIL"
        print(f"{check}: {status} - {detail}")
        failed = failed or not passed
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
