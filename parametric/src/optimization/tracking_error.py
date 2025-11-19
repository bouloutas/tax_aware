"""
Tracking error calculation.

Calculates tracking error (standard deviation of active returns) and related metrics.
"""
from datetime import date
from decimal import Decimal
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from src.core.database import Account, Benchmark, BenchmarkConstituent, Position, Security
from src.data.benchmark_data import BenchmarkManager
from src.data.market_data import MarketDataManager


class TrackingErrorCalculator:
    """Calculates tracking error and related portfolio metrics."""

    def __init__(self, session: Session):
        """
        Initialize TrackingErrorCalculator with database session.

        Args:
            session: SQLAlchemy database session
        """
        self.session = session
        self.market_data_mgr = MarketDataManager(session)
        self.benchmark_mgr = BenchmarkManager(session)

    def calculate_tracking_error(
        self,
        account_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        lookback_days: int = 252,
    ) -> dict:
        """
        Calculate tracking error for an account's portfolio vs. benchmark.

        Args:
            account_id: Account ID
            start_date: Start date for calculation (optional)
            end_date: End date for calculation (optional, defaults to today)
            lookback_days: Number of trading days to look back if dates not specified

        Returns:
            Dictionary with tracking error metrics:
            {
                'tracking_error': float,
                'active_return': float,
                'information_ratio': float,
                'portfolio_return': float,
                'benchmark_return': float,
                'period': (start_date, end_date)
            }
        """
        # Get account and benchmark
        account = self.session.query(Account).filter(Account.account_id == account_id).first()
        if not account or not account.benchmark_id:
            raise ValueError(f"Account {account_id} not found or has no benchmark assigned")

        # Set date range
        if end_date is None:
            end_date = date.today()
        if start_date is None:
            from datetime import timedelta

            start_date = end_date - timedelta(days=lookback_days * 2)  # Extra buffer

        # Get portfolio returns
        portfolio_returns = self._get_portfolio_returns(account_id, start_date, end_date)

        # Get benchmark returns
        benchmark_returns = self._get_benchmark_returns(account.benchmark_id, start_date, end_date)

        if portfolio_returns.empty or benchmark_returns.empty:
            return {
                "tracking_error": 0.0,
                "active_return": 0.0,
                "information_ratio": 0.0,
                "portfolio_return": 0.0,
                "benchmark_return": 0.0,
                "period": (start_date, end_date),
            }

        # Align dates
        common_dates = portfolio_returns.index.intersection(benchmark_returns.index)
        if len(common_dates) < 10:  # Need at least 10 days
            return {
                "tracking_error": 0.0,
                "active_return": 0.0,
                "information_ratio": 0.0,
                "portfolio_return": 0.0,
                "benchmark_return": 0.0,
                "period": (start_date, end_date),
            }

        portfolio_aligned = portfolio_returns.loc[common_dates]
        benchmark_aligned = benchmark_returns.loc[common_dates]

        # Calculate active returns (portfolio - benchmark)
        active_returns = portfolio_aligned - benchmark_aligned

        # Calculate metrics
        tracking_error = active_returns.std() * np.sqrt(252)  # Annualized
        active_return_mean = active_returns.mean() * 252  # Annualized
        information_ratio = active_return_mean / tracking_error if tracking_error > 0 else 0.0

        portfolio_return = (1 + portfolio_aligned).prod() - 1
        benchmark_return = (1 + benchmark_aligned).prod() - 1

        return {
            "tracking_error": float(tracking_error),
            "active_return": float(active_return_mean),
            "information_ratio": float(information_ratio),
            "portfolio_return": float(portfolio_return),
            "benchmark_return": float(benchmark_return),
            "period": (start_date, end_date),
        }

    def get_current_weights(self, account_id: int) -> pd.DataFrame:
        """
        Get current portfolio weights.

        Args:
            account_id: Account ID

        Returns:
            DataFrame with columns: security_id, ticker, weight
        """
        # Get positions
        positions = self.session.query(Position).filter(Position.account_id == account_id).all()

        if not positions:
            return pd.DataFrame(columns=["security_id", "ticker", "weight"])

        # Get current prices and calculate market values
        total_value = Decimal("0")
        position_data = []

        for position in positions:
            current_price = self.market_data_mgr.get_latest_price(position.security_id)
            if current_price is None:
                continue

            market_value = float(position.quantity * current_price)
            total_value += Decimal(str(market_value))

            security = self.session.query(Security).filter(Security.security_id == position.security_id).first()
            ticker = security.ticker if security else f"SECURITY_{position.security_id}"

            position_data.append(
                {
                    "security_id": position.security_id,
                    "ticker": ticker,
                    "quantity": float(position.quantity),
                    "price": float(current_price),
                    "market_value": market_value,
                }
            )

        if total_value == 0:
            return pd.DataFrame(columns=["security_id", "ticker", "weight"])

        # Calculate weights
        for pos in position_data:
            pos["weight"] = pos["market_value"] / float(total_value)

        df = pd.DataFrame(position_data)
        return df[["security_id", "ticker", "weight"]]

    def get_benchmark_weights(self, account_id: int, effective_date: Optional[date] = None) -> pd.DataFrame:
        """
        Get benchmark weights for an account.

        Args:
            account_id: Account ID
            effective_date: Effective date (optional, uses most recent)

        Returns:
            DataFrame with columns: security_id, ticker, weight
        """
        account = self.session.query(Account).filter(Account.account_id == account_id).first()
        if not account or not account.benchmark_id:
            return pd.DataFrame(columns=["security_id", "ticker", "weight"])

        return self.benchmark_mgr.get_benchmark_weights(account.benchmark_id, effective_date)

    def _get_portfolio_returns(self, account_id: int, start_date: date, end_date: date) -> pd.Series:
        """Get portfolio returns time series."""
        # Get current weights (for now, assume weights are constant)
        # In a more sophisticated implementation, we'd track historical weights
        weights_df = self.get_current_weights(account_id)

        if weights_df.empty:
            return pd.Series(dtype=float)

        # Get returns for each security
        security_returns = {}
        for _, row in weights_df.iterrows():
            security_id = int(row["security_id"])
            price_history = self.market_data_mgr.get_price_history(security_id, start_date, end_date)
            if not price_history.empty:
                returns = price_history["close"].pct_change().dropna()
                security_returns[security_id] = returns

        if not security_returns:
            return pd.Series(dtype=float)

        # Combine into portfolio returns (weighted average)
        # Align all return series
        all_dates = set()
        for returns in security_returns.values():
            all_dates.update(returns.index)

        if not all_dates:
            return pd.Series(dtype=float)

        # Create DataFrame with all returns
        returns_df = pd.DataFrame(index=sorted(all_dates))
        for security_id, returns in security_returns.items():
            weight = weights_df[weights_df["security_id"] == security_id]["weight"].iloc[0]
            returns_df[security_id] = returns

        # Fill missing values with 0 (assume no return if no data)
        returns_df = returns_df.fillna(0)

        # Calculate weighted portfolio returns
        portfolio_returns = pd.Series(index=returns_df.index, dtype=float)
        for date_idx in returns_df.index:
            weighted_return = 0.0
            for security_id in security_returns.keys():
                weight = weights_df[weights_df["security_id"] == security_id]["weight"].iloc[0]
                if security_id in returns_df.columns:
                    weighted_return += weight * returns_df.loc[date_idx, security_id]
            portfolio_returns[date_idx] = weighted_return

        return portfolio_returns

    def _get_benchmark_returns(self, benchmark_id: int, start_date: date, end_date: date) -> pd.Series:
        """Get benchmark returns time series."""
        # Get benchmark weights
        weights_df = self.benchmark_mgr.get_benchmark_weights(benchmark_id)

        if weights_df.empty:
            return pd.Series(dtype=float)

        # Get returns for each security in benchmark
        security_returns = {}
        for _, row in weights_df.iterrows():
            security_id = int(row["security_id"])
            weight = row["weight"]
            price_history = self.market_data_mgr.get_price_history(security_id, start_date, end_date)
            if not price_history.empty:
                returns = price_history["close"].pct_change().dropna()
                security_returns[security_id] = (returns, weight)

        if not security_returns:
            return pd.Series(dtype=float)

        # Combine into benchmark returns (weighted average)
        all_dates = set()
        for returns, _ in security_returns.values():
            all_dates.update(returns.index)

        if not all_dates:
            return pd.Series(dtype=float)

        # Create DataFrame with all returns
        returns_df = pd.DataFrame(index=sorted(all_dates))
        for security_id, (returns, weight) in security_returns.items():
            returns_df[security_id] = returns * weight  # Pre-weight the returns

        # Fill missing values with 0
        returns_df = returns_df.fillna(0)

        # Sum weighted returns to get benchmark returns
        benchmark_returns = returns_df.sum(axis=1)

        return benchmark_returns

