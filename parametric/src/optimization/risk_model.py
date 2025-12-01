"""
Risk model implementation integrating Barra risk factors.

Provides factor-based risk model for portfolio optimization.
Uses Barra risk model data (covariance, exposures, specific risk) from DuckDB.
"""
from typing import Optional, Dict, Tuple

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from src.core.database import Security
from src.data.barra_loader import BarraDataLoader
from src.data.gvkey_mapper import GVKEYMapper
from src.data.market_data import MarketDataManager


class RiskModel:
    """
    Factor-based risk model using Barra data.

    Components:
    - Factor exposures (X): N stocks x K factors
    - Factor covariance (Sigma_F): K x K matrix
    - Specific risk (D): N x N diagonal matrix (idiosyncratic variance)

    Total Covariance = X * Sigma_F * X.T + D
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

        # Initialize Barra loader
        self.barra_loader = None
        self.gvkey_mapper = None
        
        if self.use_barra:
            try:
                self.barra_loader = BarraDataLoader()
                self.gvkey_mapper = GVKEYMapper()
                # Test if Barra data is available
                self.latest_release = self.barra_loader.find_latest_release()
                if not self.latest_release:
                    print("Warning: No Barra release found. Risk model will be limited.")
                    self.use_barra = False
            except Exception as e:
                print(f"Warning: Failed to initialize Barra loader: {e}")
                self.use_barra = False

    def get_risk_components(
        self, 
        security_ids: list[int], 
        date: Optional[str] = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
        """
        Get risk model components aligned to the requested securities.

        Args:
            security_ids: List of security IDs to include
            date: Date for risk model data (YYYY-MM-DD). Uses latest if None.

        Returns:
            Tuple of:
            - factor_exposures (X): DataFrame (N stocks x K factors)
            - factor_covariance (Sigma_F): DataFrame (K factors x K factors)
            - specific_variance (D): Series (N stocks)
        """
        if not self.use_barra:
            raise ValueError("Barra data not available. Cannot compute risk components.")
            
        release_date = date or self.latest_release
        
        # 1. Load Factor Covariance (K x K)
        factor_cov = self.barra_loader.get_factor_covariance_matrix(release_date)
        
        # VALIDATION: Check covariance scale (should already be fixed by barra_loader)
        max_var = factor_cov.values.diagonal().max()
        if max_var > 1.0:
            # Values are still too large - apply emergency correction
            import warnings
            warnings.warn(f"Factor covariance max variance {max_var} > 1.0, applying scaling")
            
            if max_var > 10000:
                # Likely in basis points squared or percent squared
                factor_cov = factor_cov / 10000
            elif max_var > 100:
                # Likely in percent
                factor_cov = factor_cov / 100
        
        # 2. Load Exposures (N_total x K)
        # We need to map security_ids -> tickers -> gvkeys
        security_map = {}
        securities = self.session.query(Security).filter(Security.security_id.in_(security_ids)).all()
        
        # Create map: security_id -> gvkey
        # Note: This assumes we have a mapping. If GVKEYMapper relies on ticker, use that.
        for sec in securities:
            if sec.ticker:
                gvkey = self.gvkey_mapper.ticker_to_gvkey(sec.ticker)
                if gvkey:
                    security_map[sec.security_id] = str(gvkey)
        
        # Load all exposures for this date
        all_exposures = self.barra_loader.get_all_exposures(release_date)
        
        # Filter and align exposures to requested securities
        # Pivot to (gvkey x factor)
        exposure_matrix_full = all_exposures.pivot(
            index="gvkey", columns="factor", values="exposure"
        ).fillna(0.0)
        
        # Align with security_ids
        aligned_exposures = []
        valid_security_ids = []
        
        for sec_id in security_ids:
            gvkey = security_map.get(sec_id)
            if gvkey and gvkey in exposure_matrix_full.index:
                row = exposure_matrix_full.loc[gvkey]
                row.name = sec_id
                aligned_exposures.append(row)
                valid_security_ids.append(sec_id)
            else:
                # Handle missing securities (assign 0 exposure or market beta=1?)
                # For now, we'll just use 0 exposure which implies no systematic risk (only specific)
                # Ideally we should warn or impute
                pass
                
        if not aligned_exposures:
             # Return empty structures if no matching data
             return (
                 pd.DataFrame(index=security_ids, columns=factor_cov.columns).fillna(0.0),
                 factor_cov,
                 pd.Series(0.1, index=security_ids) # Default specific variance
             )
             
        factor_exposures = pd.DataFrame(aligned_exposures)
        # Reindex to include all requested securities (filling missing with 0)
        factor_exposures = factor_exposures.reindex(security_ids).fillna(0.0)
        # Ensure columns match covariance matrix
        factor_exposures = factor_exposures.reindex(columns=factor_cov.columns, fill_value=0.0)
        
        # 3. Load Specific Risk (N_total)
        all_specific_risk = self.barra_loader.load_specific_risk(release_date)
        specific_risk_map = all_specific_risk.set_index("gvkey")["specific_var"]
        
        specific_variances = []
        for sec_id in security_ids:
            gvkey = security_map.get(sec_id)
            if gvkey and gvkey in specific_risk_map.index:
                specific_variances.append(specific_risk_map.loc[gvkey])
            else:
                # Default fallback if missing
                specific_variances.append(0.10) # Conservative default
                
        specific_variance = pd.Series(specific_variances, index=security_ids)
        
        return factor_exposures, factor_cov, specific_variance

    def get_covariance_matrix(
        self, 
        security_ids: list[int], 
        lookback_days: int = 252
    ) -> pd.DataFrame:
        """
        Get full asset-asset covariance matrix.
        Sigma = X * Sigma_F * X.T + D
        
        Args:
            security_ids: List of security IDs
            lookback_days: Ignored if using Barra (kept for API compatibility)

        Returns:
            DataFrame with covariance matrix (N x N)
        """
        if self.use_barra:
            try:
                X, F, D = self.get_risk_components(security_ids)
                
                # Systematic risk: X @ F @ X.T
                systematic_cov = X.values @ F.values @ X.values.T
                
                # Add specific risk to diagonal
                total_cov = systematic_cov + np.diag(D.values)
                
                return pd.DataFrame(
                    total_cov, 
                    index=security_ids, 
                    columns=security_ids
                )
            except Exception as e:
                print(f"Error using Barra risk model: {e}. Falling back to historical.")
                
        # Fallback: Calculate from historical returns
        return self._get_historical_covariance(security_ids, lookback_days)

    def _get_historical_covariance(self, security_ids: list[int], lookback_days: int) -> pd.DataFrame:
        """Calculate covariance from historical prices."""
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

        returns_df = pd.DataFrame(index=sorted(all_dates))
        for security_id, returns in returns_dict.items():
            returns_df[security_id] = returns

        returns_df = returns_df.fillna(method="ffill").fillna(method="bfill").fillna(0)
        return returns_df.cov() * 252

    def _get_security_returns(self, security_id: int, lookback_days: int) -> Optional[pd.Series]:
        """Get security returns from market data."""
        from datetime import date, timedelta
        end_date = date.today()
        start_date = end_date - timedelta(days=lookback_days * 2) # Fetch extra for weekends/holidays

        price_history = self.market_data_mgr.get_price_history(security_id, start_date, end_date)
        if price_history.empty:
            return None

        returns = price_history["close"].pct_change().dropna()
        return returns
