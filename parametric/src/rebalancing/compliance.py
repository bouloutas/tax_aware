"""
Pre-trade compliance checking.

Validates trades before execution to ensure compliance with rules and constraints.
"""
from datetime import date, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from src.core.config import Config
from src.core.database import Account, RebalancingTrade, TaxLot
from src.tax_harvesting import WashSaleDetector


class ComplianceChecker:
    """Checks trade compliance before execution."""

    def __init__(self, session: Session):
        """
        Initialize ComplianceChecker with database session.

        Args:
            session: SQLAlchemy database session
        """
        self.session = session
        self.wash_sale_detector = WashSaleDetector(session)

    def check_trades(self, trades: list[RebalancingTrade], account_id: int) -> dict:
        """
        Check all trades for compliance.

        Args:
            trades: List of RebalancingTrade objects
            account_id: Account ID

        Returns:
            Dictionary with:
            {
                'passed': bool,
                'errors': list[str],
                'warnings': list[str],
                'checked_trades': int
            }
        """
        errors = []
        warnings = []
        checked_count = 0

        # Get account
        account = self.session.query(Account).filter(Account.account_id == account_id).first()
        if not account:
            errors.append(f"Account {account_id} not found")
            return {"passed": False, "errors": errors, "warnings": warnings, "checked_trades": 0}

        # Group trades by security
        trades_by_security = {}
        for trade in trades:
            if trade.security_id not in trades_by_security:
                trades_by_security[trade.security_id] = []
            trades_by_security[trade.security_id].append(trade)

        # Check each trade
        for trade in trades:
            checked_count += 1

            # Check wash sale violations
            if trade.trade_type == "sell" and trade.tax_lot_id:
                tax_lot = self.session.query(TaxLot).filter(TaxLot.tax_lot_id == trade.tax_lot_id).first()
                if tax_lot:
                    wash_sale_violation = self.wash_sale_detector.check_tax_lot_wash_sale(
                        tax_lot_id=trade.tax_lot_id, sale_date=date.today()
                    )
                    if wash_sale_violation:
                        errors.append(
                            f"Trade {trade.trade_id}: Wash sale violation for security {trade.security_id} "
                            f"(tax lot {trade.tax_lot_id})"
                        )

            # Check if we have enough quantity to sell
            if trade.trade_type == "sell":
                if trade.tax_lot_id:
                    tax_lot = self.session.query(TaxLot).filter(TaxLot.tax_lot_id == trade.tax_lot_id).first()
                    if tax_lot:
                        if trade.quantity > tax_lot.remaining_quantity:
                            errors.append(
                                f"Trade {trade.trade_id}: Cannot sell {trade.quantity} shares, "
                                f"only {tax_lot.remaining_quantity} available in tax lot {trade.tax_lot_id}"
                            )
                else:
                    # Check total position
                    from src.core.position_manager import PositionManager

                    position_mgr = PositionManager(self.session)
                    position = position_mgr.get_position(account_id, trade.security_id)
                    if not position or position.quantity < trade.quantity:
                        errors.append(
                            f"Trade {trade.trade_id}: Insufficient shares to sell "
                            f"{trade.quantity} of security {trade.security_id}"
                        )

            # Check for duplicate trades (same security, same type, same rebalancing event)
            duplicate_trades = [
                t
                for t in trades
                if t.trade_id != trade.trade_id
                and t.security_id == trade.security_id
                and t.trade_type == trade.trade_type
                and t.rebalancing_id == trade.rebalancing_id
            ]
            if duplicate_trades:
                warnings.append(
                    f"Trade {trade.trade_id}: Potential duplicate trades for security {trade.security_id}"
                )

            # Check price reasonableness (optional - could add more sophisticated checks)
            if trade.price and trade.price <= 0:
                errors.append(f"Trade {trade.trade_id}: Invalid price {trade.price}")

            # Check quantity reasonableness
            if trade.quantity <= 0:
                errors.append(f"Trade {trade.trade_id}: Invalid quantity {trade.quantity}")

        # Check net position changes (warnings only)
        for security_id, security_trades in trades_by_security.items():
            net_quantity = sum(
                trade.quantity if trade.trade_type == "buy" else -trade.quantity for trade in security_trades
            )
            if abs(net_quantity) < 0.01:  # Near zero net change
                warnings.append(
                    f"Security {security_id}: Net position change is near zero - "
                    f"consider consolidating trades"
                )

        passed = len(errors) == 0

        return {
            "passed": passed,
            "errors": errors,
            "warnings": warnings,
            "checked_trades": checked_count,
        }

    def check_single_trade(self, trade: RebalancingTrade, account_id: int) -> dict:
        """
        Check a single trade for compliance.

        Args:
            trade: RebalancingTrade object
            account_id: Account ID

        Returns:
            Dictionary with compliance check results
        """
        return self.check_trades([trade], account_id)

