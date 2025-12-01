"""Factor return regression utilities (Phase 3)."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Any

import duckdb
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from .config import ANALYTICS_DB, USE_RIDGE_REGRESSION, RIDGE_ALPHA, SMOOTH_SPECIFIC_RISK, SPECIFIC_RISK_LAMBDA, SPECIFIC_RISK_SHRINKAGE
from .logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class RegressionResult:
    as_of: dt.date
    factor_returns: pd.DataFrame
    residuals: pd.DataFrame
    specific_risk: pd.DataFrame
    diagnostics: Dict[str, Any]  # Added for Phase 2


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
        
        # Normalize weights
        weights = weights / weights.sum()
        W = np.diag(weights)
        
        # Compute condition number
        XtW = X.T @ W
        design = XtW @ X
        cond_number = np.linalg.cond(design)
        
        # Determine regression method
        method = "wls"
        alpha = None
        use_ridge = USE_RIDGE_REGRESSION or cond_number > 1e10
        
        try:
            if use_ridge:
                # Use Ridge regression
                alpha = RIDGE_ALPHA if cond_number < 1e12 else 1e-2
                
                # Sklearn Ridge requires scaled data for weighted regression
                sqrt_W = np.sqrt(W)
                X_weighted = sqrt_W @ X
                y_weighted = sqrt_W @ y
                
                ridge = Ridge(alpha=alpha, fit_intercept=False)
                ridge.fit(X_weighted, y_weighted)
                beta = ridge.coef_
                method = "ridge"
                
                logger.info(f"Using Ridge regression (cond={cond_number:.2e}, alpha={alpha})", 
                           extra={"method": "ridge", "condition_number": cond_number, "alpha": alpha})
            else:
                # Standard WLS
                rhs = XtW @ y
                beta = np.linalg.solve(design, rhs)
                method = "wls"
                
                logger.info(f"Using WLS regression (cond={cond_number:.2e})", 
                           extra={"method": "wls", "condition_number": cond_number})
        except np.linalg.LinAlgError:
            # Fallback to pseudoinverse
            rhs = XtW @ y
            beta = np.linalg.pinv(design) @ rhs
            method = "pinv"
            
            logger.warning(f"Regression singular, using pseudoinverse", 
                          extra={"method": "pinv", "condition_number": cond_number})
        
        # Compute residuals
        residuals = y - X @ beta
        
        # Compute R²
        ss_tot = np.sum((y - y.mean()) ** 2)
        ss_res = np.sum(residuals ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
        
        # Prepare outputs
        factor_returns = pd.DataFrame({"factor": factor_cols, "return": beta})
        residual_df = pd.DataFrame({
            "gvkey": merged["gvkey"],
            "residual": residuals,
        })
        specific = residual_df.assign(specific_var=lambda df: df["residual"] ** 2)[
            ["gvkey", "specific_var"]
        ]
        
        # Diagnostics
        diagnostics = {
            "method": method,
            "condition_number": float(cond_number),
            "alpha": float(alpha) if alpha is not None else None,
            "r_squared": float(r_squared),
            "n_factors": len(factor_cols),
            "n_stocks": len(merged)
        }
        
        return RegressionResult(
            as_of=as_of,
            factor_returns=factor_returns,
            residuals=residual_df,
            specific_risk=specific,
            diagnostics=diagnostics
        )

    def persist(self, result: RegressionResult) -> None:
        conn = duckdb.connect(self.analytics_path, read_only=False)
        try:
            # Delete existing data for this date
            conn.execute(
                "DELETE FROM analytics.factor_returns WHERE month_end_date = ?",
                [result.as_of],
            )
            conn.execute(
                "DELETE FROM analytics.specific_returns WHERE month_end_date = ?",
                [result.as_of],
            )
            conn.execute(
                "DELETE FROM analytics.specific_risk WHERE month_end_date = ?",
                [result.as_of],
            )
            conn.execute(
                "DELETE FROM analytics.regression_diagnostics WHERE month_end_date = ?",
                [result.as_of],
            )
            
            # Insert factor returns
            conn.register("factor_df", result.factor_returns.assign(month_end_date=result.as_of))
            conn.execute(
                """
                INSERT INTO analytics.factor_returns (month_end_date, factor, factor_return)
                SELECT month_end_date, factor, return
                FROM factor_df
                """
            )
            
            # Insert specific returns
            conn.register("residual_df", result.residuals.assign(month_end_date=result.as_of))
            conn.execute(
                """
                INSERT INTO analytics.specific_returns (month_end_date, gvkey, residual)
                SELECT month_end_date, gvkey, residual
                FROM residual_df
                """
            )
            
            # Insert specific risk
            conn.register("risk_df", result.specific_risk.assign(month_end_date=result.as_of))
            conn.execute(
                """
                INSERT INTO analytics.specific_risk (month_end_date, gvkey, specific_var)
                SELECT month_end_date, gvkey, specific_var
                FROM risk_df
                """
            )
            
            # Insert regression diagnostics (Phase 2)
            diag_df = pd.DataFrame([{
                "month_end_date": result.as_of,
                "method": result.diagnostics["method"],
                "condition_number": result.diagnostics["condition_number"],
                "alpha": result.diagnostics.get("alpha"),
                "r_squared": result.diagnostics["r_squared"],
                "n_factors": result.diagnostics["n_factors"],
                "n_stocks": result.diagnostics["n_stocks"]
            }])
            conn.register("diag_df", diag_df)
            conn.execute(
                """
                INSERT INTO analytics.regression_diagnostics 
                    (month_end_date, method, condition_number, alpha, r_squared, n_factors, n_stocks)
                SELECT month_end_date, method, condition_number, alpha, r_squared, n_factors, n_stocks
                FROM diag_df
                """
            )
            
            logger.info(f"Persisted regression results for {result.as_of}", 
                       extra={"as_of": str(result.as_of), "diagnostics": result.diagnostics})
        finally:
            conn.close()
    
    def compute_smoothed_specific_risk(
        self,
        as_of: dt.date,
        lambda_ewma: float = None,
        shrinkage_weight: float = None
    ) -> pd.DataFrame:
        """Compute EWMA-smoothed specific risk with industry median shrinkage.
        
        Args:
            as_of: Month-end date
            lambda_ewma: EWMA decay parameter (default from config)
            shrinkage_weight: Shrinkage intensity toward industry median (default from config)
        
        Returns:
            DataFrame with smoothed specific risk
        """
        if not SMOOTH_SPECIFIC_RISK:
            # Return raw specific risk if smoothing disabled
            conn = duckdb.connect(self.analytics_path, read_only=True)
            result = conn.execute("""
                SELECT gvkey, specific_var
                FROM analytics.specific_risk
                WHERE month_end_date = ?
            """, [as_of]).fetchdf()
            conn.close()
            return result
        
        lambda_ewma = lambda_ewma or SPECIFIC_RISK_LAMBDA
        shrinkage_weight = shrinkage_weight or SPECIFIC_RISK_SHRINKAGE
        
        conn = duckdb.connect(self.analytics_path, read_only=True)
        
        # Load current raw specific risk
        current = conn.execute("""
            SELECT gvkey, specific_var
            FROM analytics.specific_risk
            WHERE month_end_date = ?
        """, [as_of]).fetchdf()
        
        if current.empty:
            conn.close()
            return current
        
        # Load prior month's smoothed risk
        prior_date = (pd.Timestamp(as_of) - pd.DateOffset(months=1)).date()
        prior = conn.execute("""
            SELECT gvkey, specific_var_shrunk as specific_var_prior
            FROM analytics.specific_risk_smoothed
            WHERE month_end_date = ?
        """, [prior_date]).fetchdf()
        
        # Load industry mapping for shrinkage
        industry_map = conn.execute("""
            SELECT DISTINCT 
                mp.GVKEY as gvkey,
                COALESCE(map.gics_industry, 'Unclassified') as gics_industry
            FROM analytics.monthly_prices mp
            LEFT JOIN analytics.gvkey_ticker_mapping map
                ON mp.GVKEY = map.GVKEY
            WHERE mp.month_end_date = ?
        """, [as_of]).fetchdf()
        
        conn.close()
        
        # Merge current with prior
        if not prior.empty:
            merged = current.merge(prior, on='gvkey', how='left')
            # EWMA update: risk_t = λ * risk_{t-1} + (1-λ) * raw_t
            merged['specific_var_ewma'] = np.where(
                merged['specific_var_prior'].notna(),
                lambda_ewma * merged['specific_var_prior'] + (1 - lambda_ewma) * merged['specific_var'],
                merged['specific_var']  # Initialize with raw if no prior
            )
        else:
            # First period: initialize with raw
            merged = current.copy()
            merged['specific_var_ewma'] = merged['specific_var']
        
        # Industry median shrinkage
        merged = merged.merge(industry_map, on='gvkey', how='left')
        merged['gics_industry'] = merged['gics_industry'].fillna('Unclassified')
        
        # Compute industry medians
        industry_medians = merged.groupby('gics_industry')['specific_var_ewma'].transform('median')
        
        # Shrink toward industry median
        merged['specific_var_shrunk'] = (
            (1 - shrinkage_weight) * merged['specific_var_ewma'] +
            shrinkage_weight * industry_medians
        )
        
        logger.info(f"Computed smoothed specific risk for {as_of}", 
                   extra={
                       "as_of": str(as_of),
                       "lambda_ewma": lambda_ewma,
                       "shrinkage_weight": shrinkage_weight,
                       "n_stocks": len(merged)
                   })
        
        return merged[['gvkey', 'specific_var', 'specific_var_ewma', 'specific_var_shrunk']]
    
    def persist_smoothed_specific_risk(self, as_of: dt.date, smoothed_df: pd.DataFrame) -> None:
        """Persist smoothed specific risk to database."""
        if smoothed_df.empty:
            return
        
        conn = duckdb.connect(self.analytics_path, read_only=False)
        try:
            conn.execute(
                "DELETE FROM analytics.specific_risk_smoothed WHERE month_end_date = ?",
                [as_of]
            )
            
            smoothed_df = smoothed_df.copy()
            smoothed_df['month_end_date'] = as_of
            smoothed_df['lambda_ewma'] = SPECIFIC_RISK_LAMBDA
            smoothed_df['shrinkage_weight'] = SPECIFIC_RISK_SHRINKAGE
            
            # Rename columns to match schema
            smoothed_df = smoothed_df.rename(columns={
                'specific_var': 'specific_var_raw',
                'specific_var_ewma': 'specific_var_ewma',
                'specific_var_shrunk': 'specific_var_shrunk'
            })
            
            conn.register("smoothed_df", smoothed_df)
            conn.execute("""
                INSERT INTO analytics.specific_risk_smoothed 
                    (month_end_date, gvkey, specific_var_raw, specific_var_ewma, 
                     specific_var_shrunk, lambda_ewma, shrinkage_weight)
                SELECT month_end_date, gvkey, specific_var_raw, specific_var_ewma,
                       specific_var_shrunk, lambda_ewma, shrinkage_weight
                FROM smoothed_df
            """)
            
            logger.info(f"Persisted smoothed specific risk for {as_of}", 
                       extra={"as_of": str(as_of), "n_rows": len(smoothed_df)})
        finally:
            conn.close()
