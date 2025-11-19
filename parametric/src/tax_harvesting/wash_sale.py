"""
Wash sale detection algorithm.

The wash sale rule prevents claiming a loss if a substantially identical security
is purchased within 30 days before or after the sale (61-day window total).
"""
from datetime import date, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from src.core.config import Config
from src.core.database import TaxLot, Transaction


class WashSaleDetector:
    """Detects wash sale violations."""

    def __init__(self, session: Session):
        """
        Initialize WashSaleDetector with database session.

        Args:
            session: SQLAlchemy database session
        """
        self.session = session
        self.wash_sale_window_days = Config.WASH_SALE_WINDOW_DAYS

    def check_wash_sale_violation(
        self,
        account_id: int,
        security_id: int,
        sale_date: date,
        check_related_accounts: bool = False,
    ) -> bool:
        """
        Check if selling a security would violate wash sale rules.

        Args:
            account_id: Account ID
            security_id: Security ID to sell
            sale_date: Proposed sale date
            check_related_accounts: If True, also check related accounts (for future implementation)

        Returns:
            True if wash sale violation would occur, False otherwise
        """
        window_start = sale_date - timedelta(days=self.wash_sale_window_days)
        window_end = sale_date + timedelta(days=self.wash_sale_window_days)

        # Check for purchases of the same security in the 61-day window
        # (30 days before + sale date + 30 days after)
        purchases_in_window = (
            self.session.query(Transaction)
            .filter(
                Transaction.account_id == account_id,
                Transaction.security_id == security_id,
                Transaction.transaction_type == "buy",
                Transaction.transaction_date >= window_start,
                Transaction.transaction_date <= window_end,
            )
            .all()
        )

        if purchases_in_window:
            return True

        # Check for sales of the same security in the window (replacement purchases)
        # If we sold this security recently and then buy it back, that's also a wash sale
        sales_in_window = (
            self.session.query(Transaction)
            .filter(
                Transaction.account_id == account_id,
                Transaction.security_id == security_id,
                Transaction.transaction_type == "sell",
                Transaction.transaction_date >= window_start,
                Transaction.transaction_date <= window_end,
            )
            .all()
        )

        # If there were sales in the window, check if there were purchases after those sales
        # This catches the case: sell -> buy back within 30 days
        for sale_transaction in sales_in_window:
            purchases_after_sale = (
                self.session.query(Transaction)
                .filter(
                    Transaction.account_id == account_id,
                    Transaction.security_id == security_id,
                    Transaction.transaction_type == "buy",
                    Transaction.transaction_date > sale_transaction.transaction_date,
                    Transaction.transaction_date <= window_end,
                )
                .first()
            )
            if purchases_after_sale:
                return True

        # TODO: Check for substantially identical securities (same CUSIP, different ticker)
        # This would require additional logic to identify replacement securities

        return False

    def get_wash_sale_securities(
        self,
        account_id: int,
        security_id: int,
        sale_date: date,
    ) -> list[int]:
        """
        Get list of security IDs that would cause wash sale violations if purchased.

        Args:
            account_id: Account ID
            security_id: Security ID being sold
            sale_date: Proposed sale date

        Returns:
            List of security IDs that should be avoided to prevent wash sales
        """
        window_start = sale_date - timedelta(days=self.wash_sale_window_days)
        window_end = sale_date + timedelta(days=self.wash_sale_window_days)

        # Get securities purchased in the window
        purchases = (
            self.session.query(Transaction.security_id)
            .filter(
                Transaction.account_id == account_id,
                Transaction.transaction_type == "buy",
                Transaction.transaction_date >= window_start,
                Transaction.transaction_date <= window_end,
            )
            .distinct()
            .all()
        )

        # Return list of security IDs to exclude
        excluded_securities = [p[0] for p in purchases]

        # Also exclude the security being sold itself
        if security_id not in excluded_securities:
            excluded_securities.append(security_id)

        return excluded_securities

    def check_tax_lot_wash_sale(self, tax_lot_id: int, sale_date: date) -> bool:
        """
        Check if selling from a specific tax lot would violate wash sale rules.

        Args:
            tax_lot_id: Tax lot ID
            sale_date: Proposed sale date

        Returns:
            True if wash sale violation would occur
        """
        tax_lot = self.session.query(TaxLot).filter(TaxLot.tax_lot_id == tax_lot_id).first()
        if not tax_lot:
            return False

        return self.check_wash_sale_violation(
            account_id=tax_lot.account_id,
            security_id=tax_lot.security_id,
            sale_date=sale_date,
        )

