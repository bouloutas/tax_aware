"""
Barra risk model data loader.

Loads Barra factor exposures, factor covariance, and specific risk data
directly from the Barra analytics DuckDB database.
"""
from datetime import date
from pathlib import Path
from typing import Optional, List

import duckdb
import numpy as np
import pandas as pd

from src.core.config import Config


class BarraDataLoader:
    """Loads Barra risk model data from DuckDB."""

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize BarraDataLoader.

        Args:
            db_path: Path to Barra analytics DuckDB file (default from Config)
        """
        if db_path is None:
            db_path = Config.BARRA_DB_PATH
        
        self.db_path = db_path
        self._conn = None

    def _get_conn(self) -> duckdb.DuckDBPyConnection:
        """Get DuckDB connection."""
        if self._conn is None:
            # Connect in read-only mode
            self._conn = duckdb.connect(self.db_path, read_only=True)
        return self._conn

    def close(self):
        """Close DuckDB connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def find_latest_release(self) -> Optional[str]:
        """
        Find the latest date with complete risk model data.

        Returns:
            Release date string (YYYY-MM-DD) or None if no releases found
        """
        try:
            conn = self._get_conn()
            # Check for dates that have exposures, covariance, and specific risk
            # Optimized to operate on distinct dates rather than full table scans
            query = """
            WITH style_dates AS (
                SELECT DISTINCT month_end_date FROM analytics.style_factor_exposures
            ),
            cov_dates AS (
                SELECT DISTINCT month_end_date FROM analytics.factor_covariance
            ),
            risk_dates AS (
                SELECT DISTINCT month_end_date FROM analytics.specific_risk
            )
            SELECT MAX(s.month_end_date)
            FROM style_dates s
            JOIN cov_dates c ON s.month_end_date = c.month_end_date
            JOIN risk_dates r ON s.month_end_date = r.month_end_date
            """
            result = conn.execute(query).fetchone()
            if result and result[0]:
                return result[0].strftime("%Y-%m-%d")
            return None
        except Exception as e:
            print(f"Error finding latest Barra release: {e}")
            return None

    def load_style_exposures(self, release_date: Optional[str] = None) -> pd.DataFrame:
        """
        Load style factor exposures.

        Args:
            release_date: Release date (YYYY-MM-DD). If None, uses latest.

        Returns:
            DataFrame with columns: month_end_date, gvkey, factor, exposure
        """
        if release_date is None:
            release_date = self.find_latest_release()
            if release_date is None:
                raise FileNotFoundError("No Barra release found")

        conn = self._get_conn()
        query = """
        SELECT month_end_date, gvkey, factor, exposure
        FROM analytics.style_factor_exposures
        WHERE month_end_date = ?
        """
        df = conn.execute(query, [release_date]).fetchdf()
        df["month_end_date"] = pd.to_datetime(df["month_end_date"])
        return df

    def load_industry_exposures(self, release_date: Optional[str] = None) -> pd.DataFrame:
        """
        Load industry factor exposures.

        Args:
            release_date: Release date (YYYY-MM-DD). If None, uses latest.

        Returns:
            DataFrame with columns: month_end_date, gvkey, factor, exposure
        """
        if release_date is None:
            release_date = self.find_latest_release()
            if release_date is None:
                raise FileNotFoundError("No Barra release found")

        conn = self._get_conn()
        query = """
        SELECT month_end_date, gvkey, factor, exposure
        FROM analytics.industry_exposures
        WHERE month_end_date = ?
        """
        df = conn.execute(query, [release_date]).fetchdf()
        df["month_end_date"] = pd.to_datetime(df["month_end_date"])
        return df

    def load_factor_covariance(self, release_date: Optional[str] = None) -> pd.DataFrame:
        """
        Load factor covariance matrix (long format).

        Args:
            release_date: Release date (YYYY-MM-DD). If None, uses latest.

        Returns:
            DataFrame with columns: month_end_date, factor_i, factor_j, covariance
        """
        if release_date is None:
            release_date = self.find_latest_release()
            if release_date is None:
                raise FileNotFoundError("No Barra release found")

        conn = self._get_conn()
        query = """
        SELECT month_end_date, factor_i, factor_j, covariance
        FROM analytics.factor_covariance
        WHERE month_end_date = ?
        """
        df = conn.execute(query, [release_date]).fetchdf()
        df["month_end_date"] = pd.to_datetime(df["month_end_date"])
        return df

    def load_specific_risk(self, release_date: Optional[str] = None) -> pd.DataFrame:
        """
        Load specific risk (idiosyncratic risk) data.
        Prefers smoothed risk if available, falls back to raw.

        Args:
            release_date: Release date (YYYY-MM-DD). If None, uses latest.

        Returns:
            DataFrame with columns: month_end_date, gvkey, specific_var
        """
        if release_date is None:
            release_date = self.find_latest_release()
            if release_date is None:
                raise FileNotFoundError("No Barra release found")

        conn = self._get_conn()
        
        # Try to get smoothed risk first (Phase 2 enhancement)
        try:
            query = """
            SELECT month_end_date, gvkey, COALESCE(specific_var_shrunk, specific_var_ewma, specific_var_raw) as specific_var
            FROM analytics.specific_risk_smoothed
            WHERE month_end_date = ?
            """
            df = conn.execute(query, [release_date]).fetchdf()
            if not df.empty:
                df["month_end_date"] = pd.to_datetime(df["month_end_date"])
                return df
        except Exception:
            pass
            
        # Fallback to basic specific risk
        query = """
        SELECT month_end_date, gvkey, specific_var
        FROM analytics.specific_risk
        WHERE month_end_date = ?
        """
        df = conn.execute(query, [release_date]).fetchdf()
        df["month_end_date"] = pd.to_datetime(df["month_end_date"])
        return df

    def get_factor_covariance_matrix(self, release_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get factor covariance as a square matrix with scaling correction.

        Args:
            release_date: Release date (YYYY-MM-DD). If None, uses latest.

        Returns:
            Square DataFrame with factors as index and columns
        """
        cov_df = self.load_factor_covariance(release_date)

        # Get unique factors
        factors = sorted(set(cov_df["factor_i"].unique()) | set(cov_df["factor_j"].unique()))

        # Create square matrix
        cov_matrix = pd.DataFrame(index=factors, columns=factors, dtype=float)

        # Fill matrix
        for _, row in cov_df.iterrows():
            factor_i = row["factor_i"]
            factor_j = row["factor_j"]
            cov = row["covariance"]
            cov_matrix.loc[factor_i, factor_j] = cov
            cov_matrix.loc[factor_j, factor_i] = cov  # Symmetric

        # Fill diagonal if missing (shouldn't happen in valid data)
        cov_matrix = cov_matrix.fillna(0.0)

        # CRITICAL FIX: Check for scaling issues
        # The Barra model should have variance values roughly in range [0.0001, 1.0]
        # (0.01% to 100% annualized variance, or 1% to 100% vol)
        # If we see variances > 1.0, the data is likely in wrong units
        
        diagonal_values = np.diag(cov_matrix.values)
        max_var = diagonal_values.max()
        
        if max_var > 1.0:
            import warnings
            warnings.warn(
                f"Factor covariance has max variance {max_var:.2e} > 1.0. "
                f"This indicates scaling issues in the Barra model. "
                f"Applying automatic correction by clipping extreme values."
            )
            
            # Strategy: Cap variances at reasonable maximum (100% = 1.0)
            # and rescale the covariance matrix proportionally
            # This preserves correlation structure while fixing scale
            
            # Identify problematic factors (variance > 1.0)
            variances = pd.Series(diagonal_values, index=cov_matrix.index)
            problematic = variances[variances > 1.0]
            
            if len(problematic) > 0:
                print(f"WARNING: {len(problematic)} factors have variance > 1.0")
                print(f"  Problematic factors (showing top 5): {problematic.nlargest(5).to_dict()}")
                
                # For each problematic factor, rescale its row/column
                for factor in problematic.index:
                    current_var = variances[factor]
                    # Cap at 0.5 (about 70% vol, reasonable for most factors)
                    target_var = min(current_var, 0.5)
                    scale_factor = np.sqrt(target_var / current_var)
                    
                    # Rescale the factor's row and column
                    cov_matrix.loc[factor, :] *= scale_factor
                    cov_matrix.loc[:, factor] *= scale_factor
        
        # Final validation
        diagonal_values_fixed = np.diag(cov_matrix.values)
        max_var_fixed = diagonal_values_fixed.max()
        
        if max_var_fixed > 100:  # Still problematically large
            # Last resort: global rescaling
            # Detect if all values are in wrong magnitude (e.g., all 1e10)
            warnings.warn(
                f"Extreme scaling detected even after per-factor fix (max_var={max_var_fixed:.2e}). "
                f"Applying global rescaling."
            )
            # Heuristic: if median variance is > 1000, likely in percent^2 or similar
            median_var = np.median(diagonal_values_fixed[diagonal_values_fixed > 0])
            if median_var > 1000:
                # Assume values are in basis points squared or similar
                # Divide by 10000 to get to decimal
                cov_matrix = cov_matrix / 10000
                print(f"  Applied global scaling by 10000x")

        return cov_matrix

    def get_all_exposures(self, release_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get all factor exposures (style + industry + country) combined.
        
        Args:
            release_date: Release date (YYYY-MM-DD). If None, uses latest.
            
        Returns:
            DataFrame with columns: gvkey, factor, exposure
        """
        style = self.load_style_exposures(release_date)
        industry = self.load_industry_exposures(release_date)
        
        # Combine
        combined = pd.concat([
            style[["gvkey", "factor", "exposure"]],
            industry[["gvkey", "factor", "exposure"]]
        ], ignore_index=True)
        
        # Add country exposure (USA = 1.0 for all)
        # We can get the universe from style exposures
        universe = style["gvkey"].unique()
        country_df = pd.DataFrame({
            "gvkey": universe,
            "factor": "USA",
            "exposure": 1.0
        })
        
        combined = pd.concat([combined, country_df], ignore_index=True)
        
        return combined
