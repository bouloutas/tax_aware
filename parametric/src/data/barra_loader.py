"""
Barra risk model data loader.

Loads Barra factor exposures, factor covariance, and specific risk data
for use in portfolio optimization.
"""
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from src.core.config import Config


class BarraDataLoader:
    """Loads Barra risk model data."""

    def __init__(self, barra_data_dir: Optional[Path] = None):
        """
        Initialize BarraDataLoader.

        Args:
            barra_data_dir: Directory containing Barra data files (default: data/raw/barra)
        """
        if barra_data_dir is None:
            project_root = Path(__file__).parent.parent.parent
            barra_data_dir = project_root / "data" / "raw" / "barra"

        self.barra_data_dir = Path(barra_data_dir)

    def find_latest_release(self) -> Optional[str]:
        """
        Find the latest Barra release date.

        Returns:
            Release date string (YYYY-MM-DD) or None if no releases found
        """
        if not self.barra_data_dir.exists():
            return None

        # Find all CSV files with dates
        csv_files = list(self.barra_data_dir.glob("style_exposures_*.csv"))
        if not csv_files:
            return None

        # Extract dates from filenames
        dates = []
        for f in csv_files:
            # Format: style_exposures_YYYY-MM-DD.csv
            parts = f.stem.split("_")
            if len(parts) >= 3:
                date_str = "_".join(parts[2:])  # Handle YYYY-MM-DD
                try:
                    date.fromisoformat(date_str)
                    dates.append(date_str)
                except ValueError:
                    continue

        if not dates:
            return None

        # Return latest date
        dates.sort(reverse=True)
        return dates[0]

    def load_style_exposures(self, release_date: Optional[str] = None) -> pd.DataFrame:
        """
        Load style factor exposures.

        Args:
            release_date: Release date (YYYY-MM-DD). If None, uses latest.

        Returns:
            DataFrame with columns: month_end_date, gvkey, factor, exposure, flags
        """
        if release_date is None:
            release_date = self.find_latest_release()
            if release_date is None:
                raise FileNotFoundError("No Barra release found")

        file_path = self.barra_data_dir / f"style_exposures_{release_date}.csv"
        if not file_path.exists():
            raise FileNotFoundError(f"Style exposures file not found: {file_path}")

        df = pd.read_csv(file_path)
        df["month_end_date"] = pd.to_datetime(df["month_end_date"])
        return df

    def load_factor_covariance(self, release_date: Optional[str] = None) -> pd.DataFrame:
        """
        Load factor covariance matrix.

        Args:
            release_date: Release date (YYYY-MM-DD). If None, uses latest.

        Returns:
            DataFrame with columns: month_end_date, factor_i, factor_j, covariance
        """
        if release_date is None:
            release_date = self.find_latest_release()
            if release_date is None:
                raise FileNotFoundError("No Barra release found")

        file_path = self.barra_data_dir / f"factor_covariance_{release_date}.csv"
        if not file_path.exists():
            raise FileNotFoundError(f"Factor covariance file not found: {file_path}")

        df = pd.read_csv(file_path)
        df["month_end_date"] = pd.to_datetime(df["month_end_date"])
        return df

    def load_specific_risk(self, release_date: Optional[str] = None) -> pd.DataFrame:
        """
        Load specific risk (idiosyncratic risk) data.

        Args:
            release_date: Release date (YYYY-MM-DD). If None, uses latest.

        Returns:
            DataFrame with columns: month_end_date, gvkey, specific_var
        """
        if release_date is None:
            release_date = self.find_latest_release()
            if release_date is None:
                raise FileNotFoundError("No Barra release found")

        file_path = self.barra_data_dir / f"specific_risk_{release_date}.csv"
        if not file_path.exists():
            raise FileNotFoundError(f"Specific risk file not found: {file_path}")

        df = pd.read_csv(file_path)
        df["month_end_date"] = pd.to_datetime(df["month_end_date"])
        return df

    def load_factor_returns(self, release_date: Optional[str] = None) -> pd.DataFrame:
        """
        Load factor returns.

        Args:
            release_date: Release date (YYYY-MM-DD). If None, uses latest.

        Returns:
            DataFrame with columns: month_end_date, factor, factor_return
        """
        if release_date is None:
            release_date = self.find_latest_release()
            if release_date is None:
                raise FileNotFoundError("No Barra release found")

        file_path = self.barra_data_dir / f"factor_returns_{release_date}.csv"
        if not file_path.exists():
            raise FileNotFoundError(f"Factor returns file not found: {file_path}")

        df = pd.read_csv(file_path)
        df["month_end_date"] = pd.to_datetime(df["month_end_date"])
        return df

    def get_factor_covariance_matrix(self, release_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get factor covariance as a square matrix.

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

        # Fill diagonal if missing
        cov_matrix = cov_matrix.fillna(0)

        return cov_matrix

    def get_security_exposures(self, gvkey: str, release_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get factor exposures for a specific security (by GVKEY).

        Args:
            gvkey: GVKEY identifier
            release_date: Release date (YYYY-MM-DD). If None, uses latest.

        Returns:
            DataFrame with factor exposures for the security
        """
        exposures_df = self.load_style_exposures(release_date)
        security_exposures = exposures_df[exposures_df["gvkey"] == gvkey].copy()
        return security_exposures

    def get_security_specific_risk(self, gvkey: str, release_date: Optional[str] = None) -> float:
        """
        Get specific risk for a security (by GVKEY).

        Args:
            gvkey: GVKEY identifier
            release_date: Release date (YYYY-MM-DD). If None, uses latest.

        Returns:
            Specific variance (float)
        """
        risk_df = self.load_specific_risk(release_date)
        security_risk = risk_df[risk_df["gvkey"] == gvkey]

        if security_risk.empty:
            return 0.0

        return float(security_risk["specific_var"].iloc[0])

