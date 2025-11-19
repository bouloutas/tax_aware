"""
Tax-loss harvesting opportunity identification.

Main engine that identifies tax-loss harvesting opportunities by scanning
tax lots, checking wash sale constraints, finding replacements, and scoring opportunities.
"""
from datetime import date
from decimal import Decimal
from typing import Optional

from sqlalchemy.orm import Session

from src.core.config import Config
from src.core.database import Account, TaxLot
from src.data.market_data import MarketDataManager
from src.tax_harvesting.replacement_security import ReplacementSecurityFinder
from src.tax_harvesting.tax_benefit import TaxBenefitCalculator
from src.tax_harvesting.wash_sale import WashSaleDetector


class TaxLossHarvestingOpportunity:
    """Represents a tax-loss harvesting opportunity."""

    def __init__(
        self,
        tax_lot_id: int,
        security_id: int,
        ticker: str,
        unrealized_loss: Decimal,
        tax_benefit: Decimal,
        replacement_securities: list[dict],
        wash_sale_violation: bool = False,
        score: float = 0.0,
    ):
        """
        Initialize tax-loss harvesting opportunity.

        Args:
            tax_lot_id: Tax lot ID
            security_id: Security ID being sold
            ticker: Ticker symbol
            unrealized_loss: Unrealized loss amount
            tax_benefit: Tax benefit from realizing the loss
            replacement_securities: List of replacement security candidates
            wash_sale_violation: Whether this would violate wash sale rules
            score: Opportunity score (higher is better)
        """
        self.tax_lot_id = tax_lot_id
        self.security_id = security_id
        self.ticker = ticker
        self.unrealized_loss = unrealized_loss
        self.tax_benefit = tax_benefit
        self.replacement_securities = replacement_securities
        self.wash_sale_violation = wash_sale_violation
        self.score = score

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "tax_lot_id": self.tax_lot_id,
            "security_id": self.security_id,
            "ticker": self.ticker,
            "unrealized_loss": float(self.unrealized_loss),
            "tax_benefit": float(self.tax_benefit),
            "replacement_securities": self.replacement_securities,
            "wash_sale_violation": self.wash_sale_violation,
            "score": self.score,
        }


class TaxLossHarvestingFinder:
    """Finds tax-loss harvesting opportunities."""

    def __init__(self, session: Session):
        """
        Initialize TaxLossHarvestingFinder with database session.

        Args:
            session: SQLAlchemy database session
        """
        self.session = session
        self.wash_sale_detector = WashSaleDetector(session)
        self.replacement_finder = ReplacementSecurityFinder(session)
        self.tax_calculator = TaxBenefitCalculator(session)
        self.market_data_mgr = MarketDataManager(session)
        self.min_loss_threshold = Decimal(str(Config.MIN_TAX_LOSS_THRESHOLD))

    def find_opportunities(
        self,
        account_id: int,
        min_loss_threshold: Optional[Decimal] = None,
        max_opportunities: int = 10,
        sale_date: Optional[date] = None,
    ) -> list[TaxLossHarvestingOpportunity]:
        """
        Find tax-loss harvesting opportunities for an account.

        Args:
            account_id: Account ID
            min_loss_threshold: Minimum loss threshold (defaults to config value)
            max_opportunities: Maximum number of opportunities to return
            sale_date: Proposed sale date (defaults to today)

        Returns:
            List of TaxLossHarvestingOpportunity objects, sorted by score (highest first)
        """
        if min_loss_threshold is None:
            min_loss_threshold = self.min_loss_threshold

        if sale_date is None:
            sale_date = date.today()

        # Get all open tax lots for the account
        tax_lots = (
            self.session.query(TaxLot)
            .filter(TaxLot.account_id == account_id, TaxLot.status == "open")
            .all()
        )

        opportunities = []

        for tax_lot in tax_lots:
            # Get current price
            current_price = self.market_data_mgr.get_latest_price(tax_lot.security_id)
            if current_price is None:
                continue  # Skip if no price data

            # Calculate unrealized loss
            cost_basis_per_share = tax_lot.purchase_price
            unrealized_loss_per_share = current_price - cost_basis_per_share
            unrealized_loss = unrealized_loss_per_share * tax_lot.remaining_quantity

            # Only consider losses (negative values)
            if unrealized_loss >= 0:
                continue

            # Check minimum threshold
            if abs(unrealized_loss) < min_loss_threshold:
                continue

            # Check wash sale violation
            wash_sale_violation = self.wash_sale_detector.check_tax_lot_wash_sale(
                tax_lot_id=tax_lot.tax_lot_id, sale_date=sale_date
            )

            # Get excluded securities (for replacement finding)
            excluded_securities = self.wash_sale_detector.get_wash_sale_securities(
                account_id=account_id, security_id=tax_lot.security_id, sale_date=sale_date
            )

            # Find replacement securities
            replacement_securities = self.replacement_finder.find_replacement_securities(
                security_id=tax_lot.security_id,
                exclude_security_ids=excluded_securities,
                max_replacements=5,
            )

            # Calculate tax benefit
            tax_benefit_info = self.tax_calculator.calculate_tax_benefit(
                account_id=account_id,
                tax_lot_id=tax_lot.tax_lot_id,
                sale_price=current_price,
                sale_date=sale_date,
            )

            tax_benefit = tax_benefit_info["tax_benefit"]

            # Calculate opportunity score
            # Score = tax_benefit - penalty for wash sale - penalty for poor replacements
            score = self._calculate_opportunity_score(
                tax_benefit=tax_benefit,
                unrealized_loss=abs(unrealized_loss),
                wash_sale_violation=wash_sale_violation,
                replacement_securities=replacement_securities,
            )

            # Get security ticker for display
            from src.core.database import Security
            security = self.session.query(Security).filter(Security.security_id == tax_lot.security_id).first()
            ticker = security.ticker if security else f"SECURITY_{tax_lot.security_id}"

            opportunity = TaxLossHarvestingOpportunity(
                tax_lot_id=tax_lot.tax_lot_id,
                security_id=tax_lot.security_id,
                ticker=ticker,
                unrealized_loss=unrealized_loss,
                tax_benefit=tax_benefit,
                replacement_securities=replacement_securities,
                wash_sale_violation=wash_sale_violation,
                score=score,
            )

            opportunities.append(opportunity)

        # Sort by score (highest first)
        opportunities.sort(key=lambda x: x.score, reverse=True)

        return opportunities[:max_opportunities]

    def _calculate_opportunity_score(
        self,
        tax_benefit: Decimal,
        unrealized_loss: Decimal,
        wash_sale_violation: bool,
        replacement_securities: list[dict],
    ) -> float:
        """
        Calculate opportunity score.

        Higher score = better opportunity.

        Args:
            tax_benefit: Tax benefit amount
            unrealized_loss: Unrealized loss amount (absolute value)
            wash_sale_violation: Whether wash sale would be violated
            replacement_securities: List of replacement security candidates

        Returns:
            Opportunity score (float)
        """
        # Base score is tax benefit
        score = float(tax_benefit)

        # Penalty for wash sale violation (reduce score by 50%)
        if wash_sale_violation:
            score *= 0.5

        # Bonus for good replacement securities
        if replacement_securities:
            # Average similarity score of top replacements
            avg_similarity = sum(r.get("similarity_score", 0) for r in replacement_securities[:3]) / min(
                len(replacement_securities), 3
            )
            # Add bonus based on similarity (up to 20% of tax benefit)
            score += float(tax_benefit) * 0.2 * avg_similarity
        else:
            # Penalty if no replacements found (reduce by 30%)
            score *= 0.7

        # Normalize by loss amount (benefit per dollar of loss)
        if unrealized_loss > 0:
            score = score / float(unrealized_loss) * 1000  # Scale to reasonable range

        return score

