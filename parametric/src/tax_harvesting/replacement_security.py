"""
Replacement security identification.

Finds suitable replacement securities that maintain portfolio characteristics
while avoiding wash sale violations.
"""
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

from src.core.database import Security
from src.data.market_data import MarketDataManager


class ReplacementSecurityFinder:
    """Finds replacement securities for tax-loss harvesting."""

    def __init__(self, session: Session):
        """
        Initialize ReplacementSecurityFinder with database session.

        Args:
            session: SQLAlchemy database session
        """
        self.session = session
        self.market_data_mgr = MarketDataManager(session)

    def find_replacement_securities(
        self,
        security_id: int,
        exclude_security_ids: list[int],
        max_replacements: int = 5,
        min_correlation: float = 0.7,
    ) -> list[dict]:
        """
        Find replacement securities that are similar but not substantially identical.

        Args:
            security_id: Security ID being sold
            exclude_security_ids: List of security IDs to exclude (wash sale securities)
            max_replacements: Maximum number of replacement candidates to return
            min_correlation: Minimum correlation threshold for replacement (0-1)

        Returns:
            List of dictionaries with replacement security info:
            {
                'security_id': int,
                'ticker': str,
                'company_name': str,
                'sector': str,
                'correlation': float,
                'similarity_score': float
            }
        """
        # Get the security being sold
        sold_security = self.session.query(Security).filter(Security.security_id == security_id).first()
        if not sold_security:
            return []

        # Find securities in the same sector (or similar sectors)
        # Priority: same sector > same industry > different sector but similar risk profile
        candidates_query = self.session.query(Security).filter(
            Security.security_id != security_id,
            Security.security_id.notin_(exclude_security_ids),
            Security.security_type == sold_security.security_type,  # Don't mix stocks and ETFs
        )

        # Prefer same sector
        same_sector_candidates = candidates_query.filter(Security.sector == sold_security.sector).all()

        # If not enough, also consider different sectors
        if len(same_sector_candidates) < max_replacements:
            other_sector_candidates = (
                candidates_query.filter(Security.sector != sold_security.sector)
                .limit(max_replacements - len(same_sector_candidates))
                .all()
            )
            candidates = same_sector_candidates + other_sector_candidates
        else:
            candidates = same_sector_candidates[:max_replacements]

        # Score candidates based on sector match and correlation (if we have price data)
        replacements = []
        for candidate in candidates:
            # Calculate similarity score
            similarity_score = self._calculate_similarity_score(sold_security, candidate)

            # Try to calculate correlation if we have price data
            correlation = self._calculate_correlation(security_id, candidate.security_id)

            # Only include if correlation meets threshold (or if we don't have enough data)
            if correlation is None or correlation >= min_correlation:
                replacements.append(
                    {
                        "security_id": candidate.security_id,
                        "ticker": candidate.ticker,
                        "company_name": candidate.company_name,
                        "sector": candidate.sector,
                        "industry": candidate.industry,
                        "correlation": correlation if correlation else 0.0,
                        "similarity_score": similarity_score,
                    }
                )

        # Sort by similarity score (highest first)
        replacements.sort(key=lambda x: x["similarity_score"], reverse=True)

        return replacements[:max_replacements]

    def _calculate_similarity_score(self, sold_security: Security, candidate: Security) -> float:
        """
        Calculate similarity score between two securities.

        Args:
            sold_security: Security being sold
            candidate: Candidate replacement security

        Returns:
            Similarity score (0-1, higher is more similar)
        """
        score = 0.0

        # Sector match: +0.5 points
        if sold_security.sector and candidate.sector:
            if sold_security.sector == candidate.sector:
                score += 0.5

        # Industry match: +0.3 points
        if sold_security.industry and candidate.industry:
            if sold_security.industry == candidate.industry:
                score += 0.3

        # Exchange match: +0.1 points
        if sold_security.exchange and candidate.exchange:
            if sold_security.exchange == candidate.exchange:
                score += 0.1

        # Security type match: +0.1 points
        if sold_security.security_type == candidate.security_type:
            score += 0.1

        return min(score, 1.0)

    def _calculate_correlation(
        self, security_id_1: int, security_id_2: int, lookback_days: int = 252
    ) -> Optional[float]:
        """
        Calculate correlation between two securities' returns.

        Args:
            security_id_1: First security ID
            security_id_2: Second security ID
            lookback_days: Number of trading days to look back

        Returns:
            Correlation coefficient (0-1) or None if insufficient data
        """
        try:
            # Get price history for both securities
            from datetime import date, timedelta

            end_date = date.today()
            start_date = end_date - timedelta(days=lookback_days * 2)  # Extra buffer for weekends/holidays

            prices_1 = self.market_data_mgr.get_price_history(security_id_1, start_date, end_date)
            prices_2 = self.market_data_mgr.get_price_history(security_id_2, start_date, end_date)

            if prices_1.empty or prices_2.empty:
                return None

            # Calculate returns
            returns_1 = prices_1["close"].pct_change().dropna()
            returns_2 = prices_2["close"].pct_change().dropna()

            # Align dates
            common_dates = returns_1.index.intersection(returns_2.index)
            if len(common_dates) < 30:  # Need at least 30 days of data
                return None

            returns_1_aligned = returns_1.loc[common_dates]
            returns_2_aligned = returns_2.loc[common_dates]

            # Calculate correlation
            correlation = returns_1_aligned.corr(returns_2_aligned)

            # Return absolute value (we care about magnitude, not direction)
            return abs(correlation) if pd.notna(correlation) else None

        except Exception:
            # If anything fails, return None
            return None

