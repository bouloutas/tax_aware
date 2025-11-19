"""
Simple risk model implementation.

Provides basic factor-based risk model for portfolio optimization.
Can use Barra risk model data if available.
"""
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from src.core.database import Security
from src.data.barra_loader import BarraDataLoader
from src.data.gvkey_mapper import GVKEYMapper
from src.data.market_data import MarketDataManager


class RiskModel:
    """
    Simple factor-based risk model.

    Uses historical returns to estimate:
    - Factor exposures (market, size, value, momentum)
    - Factor covariance
    - Specific risk (idiosyncratic)

    Can optionally use Barra risk model data if available.
    """

    def __init__(self, session: Session, use_barra: bool = True):
        """
        Initialize RiskModel with database session.

        Args:
            session: SQLAlchemy database session
            use_barra: If True, use Barra risk model data when available
        """
        self.session = session
        self.market_data_mgr = MarketDataManager(session)
        self.use_barra = use_barra

        # Initialize Barra loader if requested
        self.barra_loader = None
        self.gvkey_mapper = None
        if self.use_barra:
            try:
                self.barra_loader = BarraDataLoader()
                self.gvkey_mapper = GVKEYMapper()
                # Test if Barra data is available
                _ = self.barra_loader.find_latest_release()
            except Exception:
                # Barra data not available, fall back to simple model
                self.use_barra = False
                self.barra_loader = None
                self.gvkey_mapper = None

    def calculate_portfolio_risk(
        self,
        weights: pd.Series,
        lookback_days: int = 252,
    ) -> dict:
        """
        Calculate portfolio risk metrics.

        Args:
            weights: Series with security_id -> weight
            lookback_days: Number of trading days for risk calculation

        Returns:
            Dictionary with risk metrics:
            {
                'portfolio_volatility': float,
                'factor_exposures': dict,
                'specific_risk': float,
                'total_risk': float
            }
        """
        if weights.empty:
            return {
                "portfolio_volatility": 0.0,
                "factor_exposures": {},
                "specific_risk": 0.0,
                "total_risk": 0.0,
            }

        # Get returns for all securities
        security_returns = {}
        for security_id in weights.index:
            security_id_int = int(security_id)
            returns = self._get_security_returns(security_id_int, lookback_days)
            if returns is not None and not returns.empty:
                security_returns[security_id_int] = returns

        if not security_returns:
            return {
                "portfolio_volatility": 0.0,
                "factor_exposures": {},
                "specific_risk": 0.0,
                "total_risk": 0.0,
            }

        # Calculate portfolio returns
        portfolio_returns = self._calculate_portfolio_returns(weights, security_returns)

        # Calculate portfolio volatility (annualized)
        portfolio_volatility = portfolio_returns.std() * np.sqrt(252)

        # Calculate factor exposures (simplified)
        factor_exposures = self._calculate_factor_exposures(weights)

        # Estimate specific risk (simplified as residual volatility)
        specific_risk = portfolio_volatility * 0.3  # Assume 30% is specific risk

        return {
            "portfolio_volatility": float(portfolio_volatility),
            "factor_exposures": factor_exposures,
            "specific_risk": float(specific_risk),
            "total_risk": float(portfolio_volatility),
        }

    def get_covariance_matrix(self, security_ids: list[int], lookback_days: int = 252) -> pd.DataFrame:
        """
        Get covariance matrix for a set of securities.

        Args:
            security_ids: List of security IDs
            lookback_days: Number of trading days

        Returns:
            DataFrame with covariance matrix
        """
        # Get returns for all securities
        returns_dict = {}
        for security_id in security_ids:
            returns = self._get_security_returns(security_id, lookback_days)
            if returns is not None and not returns.empty:
                returns_dict[security_id] = returns

        if not returns_dict:
            return pd.DataFrame()

        # Align dates
        all_dates = set()
        for returns in returns_dict.values():
            all_dates.update(returns.index)

        if not all_dates:
            return pd.DataFrame()

        # Create DataFrame with aligned returns
        returns_df = pd.DataFrame(index=sorted(all_dates))
        for security_id, returns in returns_dict.items():
            returns_df[security_id] = returns

        # Fill missing values (forward fill, then backward fill)
        returns_df = returns_df.fillna(method="ffill").fillna(method="bfill").fillna(0)

        # Calculate covariance matrix (annualized)
        cov_matrix = returns_df.cov() * 252

        return cov_matrix

    def _get_security_returns(self, security_id: int, lookback_days: int) -> Optional[pd.Series]:
        """Get security returns."""
        from datetime import date, timedelta

        end_date = date.today()
        start_date = end_date - timedelta(days=lookback_days * 2)

        price_history = self.market_data_mgr.get_price_history(security_id, start_date, end_date)
        if price_history.empty:
            return None

        returns = price_history["close"].pct_change().dropna()
        return returns

    def _calculate_portfolio_returns(
        self, weights: pd.Series, security_returns: dict[int, pd.Series]
    ) -> pd.Series:
        """Calculate weighted portfolio returns."""
        # Align all return series
        all_dates = set()
        for returns in security_returns.values():
            all_dates.update(returns.index)

        if not all_dates:
            return pd.Series(dtype=float)

        # Create aligned returns DataFrame
        returns_df = pd.DataFrame(index=sorted(all_dates))
        for security_id, returns in security_returns.items():
            if security_id in weights.index:
                weight = weights[security_id]
                returns_df[security_id] = returns * weight

        returns_df = returns_df.fillna(0)

        # Sum weighted returns
        portfolio_returns = returns_df.sum(axis=1)

        return portfolio_returns

    def _calculate_factor_exposures(self, weights: pd.Series) -> dict:
        """
        Calculate factor exposures.

        Uses Barra data if available, otherwise returns simplified exposures.
        """
        if self.use_barra and self.barra_loader and self.gvkey_mapper:
            # Use Barra factor exposures
            try:
                release_date = self.barra_loader.find_latest_release()
                exposures_df = self.barra_loader.load_style_exposures(release_date)

                # Calculate portfolio-level exposures
                portfolio_exposures = {}
                for _, row in exposures_df.iterrows():
                    gvkey = str(row["gvkey"])
                    factor = row["factor"]
                    exposure = row["exposure"]

                    # Convert GVKEY to ticker and find weight
                    ticker = self.gvkey_mapper.gvkey_to_ticker(gvkey)
                    if ticker:
                        # Find security_id from ticker
                        security = (
                            self.session.query(Security)
                            .filter(Security.ticker == ticker.upper())
                            .first()
                        )
                        if security and security.security_id in weights.index:
                            weight = weights[security.security_id]
                            if factor not in portfolio_exposures:
                                portfolio_exposures[factor] = 0.0
                            portfolio_exposures[factor] += weight * exposure

                return portfolio_exposures
            except Exception:
                # Fall back to simple model if Barra fails
                pass

        # Simplified factor exposures (fallback)
        exposures = {
            "market": 1.0,  # All equities have market exposure
            "size": 0.0,  # Would calculate based on market cap
            "value": 0.0,  # Would calculate based on P/E, P/B ratios
            "momentum": 0.0,  # Would calculate based on recent returns
        }

        return exposures

