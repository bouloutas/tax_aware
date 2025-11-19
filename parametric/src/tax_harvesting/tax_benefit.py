"""
Tax benefit calculation.

Calculates the tax benefit of realizing losses and the applicable tax rates.
"""
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from sqlalchemy.orm import Session

from src.core.config import Config
from src.core.database import Account, TaxLot


class TaxBenefitCalculator:
    """Calculates tax benefits from tax-loss harvesting."""

    def __init__(self, session: Session):
        """
        Initialize TaxBenefitCalculator with database session.

        Args:
            session: SQLAlchemy database session
        """
        self.session = session
        self.long_term_holding_days = Config.LONG_TERM_HOLDING_DAYS

    def calculate_tax_benefit(
        self,
        account_id: int,
        tax_lot_id: int,
        sale_price: Decimal,
        sale_date: date,
    ) -> dict:
        """
        Calculate tax benefit from selling a tax lot at a loss.

        Args:
            account_id: Account ID
            tax_lot_id: Tax lot ID
            sale_price: Price per share at sale
            sale_date: Date of sale

        Returns:
            Dictionary with:
            {
                'unrealized_loss': Decimal,
                'realized_loss': Decimal,
                'holding_period_days': int,
                'is_long_term': bool,
                'tax_rate': Decimal,
                'tax_benefit': Decimal,
                'tax_benefit_percentage': float
            }
        """
        # Get tax lot
        tax_lot = self.session.query(TaxLot).filter(TaxLot.tax_lot_id == tax_lot_id).first()
        if not tax_lot:
            raise ValueError(f"Tax lot {tax_lot_id} not found")

        # Get account for tax rates
        account = self.session.query(Account).filter(Account.account_id == account_id).first()
        if not account:
            raise ValueError(f"Account {account_id} not found")

        # Calculate unrealized loss
        cost_basis_per_share = tax_lot.purchase_price
        unrealized_loss_per_share = sale_price - cost_basis_per_share
        unrealized_loss = unrealized_loss_per_share * tax_lot.remaining_quantity

        # Only calculate benefit if there's a loss
        if unrealized_loss >= 0:
            return {
                "unrealized_loss": Decimal("0"),
                "realized_loss": Decimal("0"),
                "holding_period_days": 0,
                "is_long_term": False,
                "tax_rate": Decimal("0"),
                "tax_benefit": Decimal("0"),
                "tax_benefit_percentage": 0.0,
            }

        # Calculate holding period
        holding_period = (sale_date - tax_lot.purchase_date).days
        is_long_term = holding_period >= self.long_term_holding_days

        # Determine tax rate
        if is_long_term:
            tax_rate = account.tax_rate_long_term
        else:
            tax_rate = account.tax_rate_short_term

        # Calculate tax benefit (loss * tax_rate)
        # Note: Loss is negative, so we take absolute value
        realized_loss = abs(unrealized_loss)
        tax_benefit = realized_loss * tax_rate
        tax_benefit_percentage = float(tax_benefit / realized_loss) if realized_loss > 0 else 0.0

        return {
            "unrealized_loss": unrealized_loss,
            "realized_loss": realized_loss,
            "holding_period_days": holding_period,
            "is_long_term": is_long_term,
            "tax_rate": tax_rate,
            "tax_benefit": tax_benefit,
            "tax_benefit_percentage": tax_benefit_percentage,
        }

    def calculate_portfolio_tax_benefit(
        self,
        account_id: int,
        tax_lot_ids: list[int],
        sale_prices: dict[int, Decimal],
        sale_date: date,
    ) -> dict:
        """
        Calculate total tax benefit from selling multiple tax lots.

        Args:
            account_id: Account ID
            tax_lot_ids: List of tax lot IDs to sell
            sale_prices: Dictionary mapping tax_lot_id -> sale_price
            sale_date: Date of sale

        Returns:
            Dictionary with aggregate tax benefit information
        """
        total_realized_loss = Decimal("0")
        total_tax_benefit = Decimal("0")
        short_term_loss = Decimal("0")
        long_term_loss = Decimal("0")
        short_term_benefit = Decimal("0")
        long_term_benefit = Decimal("0")

        account = self.session.query(Account).filter(Account.account_id == account_id).first()
        if not account:
            raise ValueError(f"Account {account_id} not found")

        for tax_lot_id in tax_lot_ids:
            if tax_lot_id not in sale_prices:
                continue

            benefit_info = self.calculate_tax_benefit(
                account_id=account_id,
                tax_lot_id=tax_lot_id,
                sale_price=sale_prices[tax_lot_id],
                sale_date=sale_date,
            )

            total_realized_loss += benefit_info["realized_loss"]
            total_tax_benefit += benefit_info["tax_benefit"]

            if benefit_info["is_long_term"]:
                long_term_loss += benefit_info["realized_loss"]
                long_term_benefit += benefit_info["tax_benefit"]
            else:
                short_term_loss += benefit_info["realized_loss"]
                short_term_benefit += benefit_info["tax_benefit"]

        return {
            "total_realized_loss": total_realized_loss,
            "total_tax_benefit": total_tax_benefit,
            "short_term_loss": short_term_loss,
            "long_term_loss": long_term_loss,
            "short_term_benefit": short_term_benefit,
            "long_term_benefit": long_term_benefit,
            "effective_tax_rate": (
                float(total_tax_benefit / total_realized_loss) if total_realized_loss > 0 else 0.0
            ),
        }

    def get_tax_rate(self, account_id: int, purchase_date: date, sale_date: date) -> Decimal:
        """
        Get applicable tax rate for a holding period.

        Args:
            account_id: Account ID
            purchase_date: Purchase date
            sale_date: Sale date

        Returns:
            Tax rate (Decimal)
        """
        account = self.session.query(Account).filter(Account.account_id == account_id).first()
        if not account:
            raise ValueError(f"Account {account_id} not found")

        holding_period = (sale_date - purchase_date).days
        if holding_period >= self.long_term_holding_days:
            return account.tax_rate_long_term
        else:
            return account.tax_rate_short_term

