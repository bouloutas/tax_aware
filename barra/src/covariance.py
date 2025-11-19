"""Factor covariance estimation (Phase 3)."""
from __future__ import annotations

import datetime as dt

import duckdb
import numpy as np
import pandas as pd

from .config import ANALYTICS_DB, FACTOR_COV_WINDOW_MONTHS


class CovarianceEngine:
    def __init__(self, analytics_db: str = ANALYTICS_DB.as_posix()) -> None:
        self.path = analytics_db
        self.conn = duckdb.connect(analytics_db)

    def close(self) -> None:
        self.conn.close()

    def load_factor_returns(self, as_of: dt.date, lookback_months: int) -> pd.DataFrame:
        start = (pd.Timestamp(as_of) - pd.DateOffset(months=lookback_months)).date()
        query = """
        SELECT month_end_date, factor, factor_return
        FROM analytics.factor_returns
        WHERE month_end_date <= ?
          AND month_end_date > ?
        ORDER BY month_end_date
        """
        return self.conn.execute(query, [as_of, start]).fetchdf()

    def compute_covariance(self, as_of: dt.date, lookback_months: int | None = None) -> pd.DataFrame:
        lookback = lookback_months or FACTOR_COV_WINDOW_MONTHS
        returns = self.load_factor_returns(as_of, lookback)
        if returns.empty:
            raise ValueError("No factor returns available for covariance computation")
        pivot = returns.pivot_table(index="month_end_date", columns="factor", values="factor_return")
        pivot = pivot.dropna()
        if pivot.shape[0] < 2:
            raise ValueError("Insufficient history for covariance")
        matrix = pivot.values
        cov = np.cov(matrix, rowvar=False, ddof=1)
        cov_df = pd.DataFrame(cov, index=pivot.columns, columns=pivot.columns)
        cov_df = cov_df.rename_axis(index="factor_i", columns="factor_j")
        cov_long = cov_df.stack().rename("covariance").reset_index()
        cov_long["month_end_date"] = as_of
        return cov_long

    def persist(self, cov_df: pd.DataFrame) -> None:
        with duckdb.connect(self.path) as conn:
            conn.execute(
                "DELETE FROM analytics.factor_covariance WHERE month_end_date = ?",
                [cov_df["month_end_date"].iloc[0]],
            )
            conn.register("cov_df", cov_df)
            conn.execute(
                """
                INSERT INTO analytics.factor_covariance
                    (month_end_date, factor_i, factor_j, covariance)
                SELECT month_end_date, factor_i, factor_j, covariance
                FROM cov_df
                """
            )
