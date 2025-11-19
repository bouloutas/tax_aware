"""Factor return regression utilities (Phase 3)."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import List

import duckdb
import numpy as np
import pandas as pd

from .config import ANALYTICS_DB


@dataclass
class RegressionResult:
    as_of: dt.date
    factor_returns: pd.DataFrame
    residuals: pd.DataFrame
    specific_risk: pd.DataFrame


class RegressionEngine:
    def __init__(self, analytics_db: str = ANALYTICS_DB.as_posix()) -> None:
        self.analytics_path = analytics_db
        self.conn = duckdb.connect(analytics_db)

    def close(self) -> None:
        self.conn.close()

    def load_style_exposures(self, as_of: dt.date) -> pd.DataFrame:
        query = """
        SELECT GVKEY AS gvkey, factor, exposure
        FROM analytics.style_factor_exposures
        WHERE month_end_date = ?
        """
        return self.conn.execute(query, [as_of]).fetchdf()

    def load_industry_exposures(self, as_of: dt.date) -> pd.DataFrame:
        query = """
        SELECT GVKEY AS gvkey, factor, exposure
        FROM analytics.industry_exposures
        WHERE month_end_date = ?
        """
        return self.conn.execute(query, [as_of]).fetchdf()

    def load_country_exposures(self, as_of: dt.date) -> pd.DataFrame:
        query = """
        SELECT GVKEY AS gvkey, factor, exposure
        FROM analytics.country_exposures
        WHERE month_end_date = ?
        """
        return self.conn.execute(query, [as_of]).fetchdf()

    def load_returns(self, as_of: dt.date) -> pd.DataFrame:
        query = """
        SELECT
            GVKEY AS gvkey,
            monthly_return,
            month_end_market_cap
        FROM analytics.monthly_returns
        WHERE month_end_date = ?
        """
        return self.conn.execute(query, [as_of]).fetchdf()

    def regress(self, as_of: dt.date) -> RegressionResult:
        style = self.load_style_exposures(as_of)
        industry = self.load_industry_exposures(as_of)
        country = self.load_country_exposures(as_of)
        returns = self.load_returns(as_of)
        combined = [df for df in (style, industry, country) if not df.empty]
        if not combined or returns.empty:
            raise ValueError("Missing data for regression")
        exposures = pd.concat(combined, ignore_index=True)
        pivot = (
            exposures.pivot_table(index="gvkey", columns="factor", values="exposure", fill_value=0)
            .reset_index()
        )
        pivot.columns.name = None
        merged = returns.merge(pivot, on="gvkey", how="inner")
        merged = merged.dropna()
        y = merged["monthly_return"].values
        factor_cols = pivot.columns.drop("gvkey")
        X = merged[factor_cols].values
        weights = merged["month_end_market_cap"].values
        W = np.diag(weights / weights.sum())
        XtW = X.T @ W
        design = XtW @ X
        rhs = XtW @ y
        try:
            beta = np.linalg.solve(design, rhs)
        except np.linalg.LinAlgError:
            beta = np.linalg.pinv(design) @ rhs
        residuals = y - X @ beta
        factor_returns = pd.DataFrame({"factor": factor_cols, "return": beta})
        residual_df = pd.DataFrame(
            {
                "gvkey": merged["gvkey"],
                "residual": residuals,
            }
        )
        specific = residual_df.assign(specific_var=lambda df: df["residual"] ** 2)[
            ["gvkey", "specific_var"]
        ]
        return RegressionResult(
            as_of=as_of,
            factor_returns=factor_returns,
            residuals=residual_df,
            specific_risk=specific,
        )

    def persist(self, result: RegressionResult) -> None:
        conn = duckdb.connect(self.analytics_path, read_only=False)
        try:
            conn.execute(
                "DELETE FROM analytics.factor_returns WHERE month_end_date = ?",
                [result.as_of],
            )
            conn.execute(
                "DELETE FROM analytics.specific_returns WHERE month_end_date = ?",
                [result.as_of],
            )
            conn.register("factor_df", result.factor_returns.assign(month_end_date=result.as_of))
            conn.execute(
                """
                INSERT INTO analytics.factor_returns (month_end_date, factor, factor_return)
                SELECT month_end_date, factor, return
                FROM factor_df
                """
            )
            conn.register("residual_df", result.residuals.assign(month_end_date=result.as_of))
            conn.execute(
                """
                INSERT INTO analytics.specific_returns (month_end_date, gvkey, residual)
                SELECT month_end_date, gvkey, residual
                FROM residual_df
                """
            )
            conn.execute(
                "DELETE FROM analytics.specific_risk WHERE month_end_date = ?",
                [result.as_of],
            )
            conn.register("risk_df", result.specific_risk.assign(month_end_date=result.as_of))
            conn.execute(
                """
                INSERT INTO analytics.specific_risk (month_end_date, gvkey, specific_var)
                SELECT month_end_date, gvkey, specific_var
                FROM risk_df
                """
            )
        finally:
            conn.close()
