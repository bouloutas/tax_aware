"""
Trade generation from optimization results.

Converts optimization results into executable trades.
"""
from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy.orm import Session

from src.core.database import RebalancingEvent, RebalancingTrade, Security, TaxLot


class TradeGenerator:
    """Generates trades from optimization results."""

    def __init__(self, session: Session):
        """
        Initialize TradeGenerator with database session.

        Args:
            session: SQLAlchemy database session
        """
        self.session = session

    def generate_trades_from_optimization(
        self,
        rebalancing_event_id: int,
        opt_result: dict,
        tax_opportunities: list,
    ) -> list[RebalancingTrade]:
        """
        Generate trades from optimization results.

        Args:
            rebalancing_event_id: Rebalancing event ID
            opt_result: Optimization result dictionary
            tax_opportunities: List of tax-loss harvesting opportunities

        Returns:
            List of RebalancingTrade objects
        """
        trades = []

        # Get current positions
        from src.core.position_manager import PositionManager

        position_mgr = PositionManager(self.session)
        rebalancing_event = (
            self.session.query(RebalancingEvent).filter(RebalancingEvent.rebalancing_id == rebalancing_event_id).first()
        )
        if not rebalancing_event:
            raise ValueError(f"Rebalancing event {rebalancing_event_id} not found")

        account_id = rebalancing_event.account_id
        current_positions = position_mgr.get_positions(account_id)

        # Get current market values
        from src.data.market_data import MarketDataManager

        market_data_mgr = MarketDataManager(self.session)

        # Create position map
        current_position_map = {pos.security_id: pos for pos in current_positions}

        # Generate trades from optimal weights
        optimal_weights = opt_result.get("optimal_weights", {})
        if isinstance(optimal_weights, dict):
            optimal_weights = optimal_weights

        # Calculate target market values
        total_current_value = sum(
            float(pos.quantity * (market_data_mgr.get_latest_price(pos.security_id) or Decimal("0")))
            for pos in current_positions
        )

        if total_current_value == 0:
            return trades

        # Generate buy/sell trades
        for security_id, target_weight in optimal_weights.items():
            security_id_int = int(security_id)
            current_position = current_position_map.get(security_id_int)

            current_price = market_data_mgr.get_latest_price(security_id_int)
            if current_price is None:
                continue

            target_value = float(total_current_value) * float(target_weight)
            target_quantity = Decimal(str(target_value / float(current_price)))

            if current_position:
                current_quantity = current_position.quantity
                quantity_delta = target_quantity - current_quantity

                if abs(quantity_delta) > Decimal("0.01"):  # Only trade if significant difference
                    if quantity_delta > 0:
                        # Buy
                        trade = RebalancingTrade(
                            rebalancing_id=rebalancing_event_id,
                            security_id=security_id_int,
                            trade_type="buy",
                            quantity=quantity_delta,
                            price=current_price,
                            status="pending",
                        )
                        trades.append(trade)
                    else:
                        # Sell - need to identify which tax lots to sell
                        sell_quantity = abs(quantity_delta)
                        tax_lots_to_sell = self._select_tax_lots_for_sale(
                            account_id=account_id,
                            security_id=security_id_int,
                            quantity=sell_quantity,
                            tax_opportunities=tax_opportunities,
                        )

                        for tax_lot_id, lot_quantity in tax_lots_to_sell:
                            trade = RebalancingTrade(
                                rebalancing_id=rebalancing_event_id,
                                security_id=security_id_int,
                                trade_type="sell",
                                quantity=lot_quantity,
                                price=current_price,
                                tax_lot_id=tax_lot_id,
                                status="pending",
                            )
                            trades.append(trade)
            else:
                # New position - buy
                if target_quantity > Decimal("0.01"):
                    trade = RebalancingTrade(
                        rebalancing_id=rebalancing_event_id,
                        security_id=security_id_int,
                        trade_type="buy",
                        quantity=target_quantity,
                        price=current_price,
                        status="pending",
                    )
                    trades.append(trade)

        # Generate tax-loss harvesting trades (replacements)
        for opp in tax_opportunities[:10]:  # Top 10 opportunities
            if opp.wash_sale_violation or not opp.replacement_securities:
                continue

            replacement = opp.replacement_securities[0]  # Use top replacement

            # Sell the losing security
            tax_lot = (
                self.session.query(TaxLot).filter(TaxLot.tax_lot_id == opp.tax_lot_id).first()
            )
            if tax_lot and tax_lot.remaining_quantity > 0:
                current_price_sell = market_data_mgr.get_latest_price(opp.security_id)
                if current_price_sell:
                    trade_sell = RebalancingTrade(
                        rebalancing_id=rebalancing_event_id,
                        security_id=opp.security_id,
                        trade_type="sell",
                        quantity=tax_lot.remaining_quantity,
                        price=current_price_sell,
                        tax_lot_id=opp.tax_lot_id,
                        estimated_tax_benefit=opp.tax_benefit,
                        status="pending",
                    )
                    trades.append(trade_sell)

                    # Buy replacement
                    replacement_price = market_data_mgr.get_latest_price(replacement["security_id"])
                    if replacement_price:
                        # Buy equivalent dollar amount
                        buy_value = float(tax_lot.remaining_quantity * current_price_sell)
                        buy_quantity = Decimal(str(buy_value / float(replacement_price)))

                        trade_buy = RebalancingTrade(
                            rebalancing_id=rebalancing_event_id,
                            security_id=replacement["security_id"],
                            trade_type="buy",
                            quantity=buy_quantity,
                            price=replacement_price,
                            status="pending",
                        )
                        trades.append(trade_buy)

        # Save trades to database
        for trade in trades:
            self.session.add(trade)

        self.session.commit()

        return trades

    def _select_tax_lots_for_sale(
        self,
        account_id: int,
        security_id: int,
        quantity: Decimal,
        tax_opportunities: list,
    ) -> list[tuple[int, Decimal]]:
        """
        Select tax lots to sell (FIFO or tax-loss harvesting priority).

        Args:
            account_id: Account ID
            security_id: Security ID
            quantity: Quantity to sell
            tax_opportunities: List of tax-loss harvesting opportunities

        Returns:
            List of tuples (tax_lot_id, quantity)
        """
        # Get tax lots for this security
        from src.core.position_manager import PositionManager

        position_mgr = PositionManager(self.session)
        tax_lots = position_mgr.get_tax_lots(account_id, security_id=security_id, status="open")

        if not tax_lots:
            return []

        # Check if this security has tax-loss harvesting opportunities
        opp_tax_lot_ids = {opp.tax_lot_id for opp in tax_opportunities if opp.security_id == security_id}

        # Prioritize tax-loss harvesting lots
        selected_lots = []
        remaining_quantity = quantity

        # First, use tax-loss harvesting lots
        for tax_lot in tax_lots:
            if tax_lot.tax_lot_id in opp_tax_lot_ids and remaining_quantity > 0:
                lot_quantity = min(tax_lot.remaining_quantity, remaining_quantity)
                selected_lots.append((tax_lot.tax_lot_id, lot_quantity))
                remaining_quantity -= lot_quantity

        # Then, use FIFO for remaining quantity
        if remaining_quantity > 0:
            for tax_lot in sorted(tax_lots, key=lambda x: x.purchase_date):  # FIFO
                if tax_lot.tax_lot_id not in opp_tax_lot_ids and remaining_quantity > 0:
                    lot_quantity = min(tax_lot.remaining_quantity, remaining_quantity)
                    selected_lots.append((tax_lot.tax_lot_id, lot_quantity))
                    remaining_quantity -= lot_quantity

        return selected_lots

    def execute_trades(self, trades: list[RebalancingTrade], account_id: int) -> dict:
        """
        Execute trades (update positions and tax lots).

        Args:
            trades: List of RebalancingTrade objects
            account_id: Account ID

        Returns:
            Dictionary with execution results
        """
        from src.core.position_manager import PositionManager

        position_mgr = PositionManager(self.session)
        executed_count = 0
        failed_count = 0

        for trade in trades:
            try:
                if trade.trade_type == "buy":
                    # Create new tax lot
                    from datetime import date

                    tax_lot = position_mgr.create_tax_lot(
                        account_id=account_id,
                        security_id=trade.security_id,
                        purchase_date=date.today(),
                        purchase_price=trade.price,
                        quantity=trade.quantity,
                    )
                    trade.status = "executed"
                    trade.executed_at = datetime.now()
                    executed_count += 1

                elif trade.trade_type == "sell":
                    if trade.tax_lot_id:
                        # Sell from specific tax lot
                        from datetime import date

                        transaction, tax_lot = position_mgr.sell_from_tax_lot(
                            tax_lot_id=trade.tax_lot_id,
                            sell_date=date.today(),
                            sell_price=trade.price,
                            quantity=trade.quantity,
                            wash_sale_flag=False,  # Already checked in compliance
                        )
                        trade.status = "executed"
                        trade.executed_at = datetime.now()
                        executed_count += 1
                    else:
                        # Sell without specific tax lot (shouldn't happen, but handle it)
                        failed_count += 1
                        trade.status = "failed"

            except Exception as e:
                failed_count += 1
                trade.status = "failed"
                # Log error (in production, use proper logging)

        self.session.commit()

        return {
            "executed": executed_count,
            "failed": failed_count,
            "total": len(trades),
        }

