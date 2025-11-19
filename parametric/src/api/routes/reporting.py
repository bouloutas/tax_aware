"""
Reporting API endpoints.

Provides performance reports, tax reports, and client statements.
"""
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.core.position_manager import PositionManager
from src.optimization import TrackingErrorCalculator


def get_db(request):
    """Get database session."""
    return request.app.state.Session()


router = APIRouter()


class PerformanceReport(BaseModel):
    """Performance report model."""

    account_id: int
    period_start: str
    period_end: str
    portfolio_return: float
    benchmark_return: float
    tracking_error: float
    information_ratio: float
    total_tax_benefit: float


class TaxReport(BaseModel):
    """Tax report model."""

    account_id: int
    year: int
    realized_gains: float
    realized_losses: float
    net_realized_gain_loss: float
    short_term_gains: float
    short_term_losses: float
    long_term_gains: float
    long_term_losses: float
    wash_sale_adjustments: float


@router.get("/performance/{account_id}", response_model=PerformanceReport)
async def get_performance_report(
    account_id: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Get performance report for an account."""
    calc = TrackingErrorCalculator(db)

    start = date.fromisoformat(start_date) if start_date else None
    end = date.fromisoformat(end_date) if end_date else None

    result = calc.calculate_tracking_error(account_id, start_date=start, end_date=end)

    # Calculate total tax benefit (from rebalancing events)
    from src.core.database import RebalancingEvent

    rebalancing_events = (
        db.query(RebalancingEvent)
        .filter(
            RebalancingEvent.account_id == account_id,
            RebalancingEvent.status == "executed",
        )
        .all()
    )

    total_tax_benefit = sum(float(event.tax_benefit) for event in rebalancing_events if event.tax_benefit)

    return PerformanceReport(
        account_id=account_id,
        period_start=result["period"][0].isoformat() if result["period"] else "",
        period_end=result["period"][1].isoformat() if result["period"] else "",
        portfolio_return=result["portfolio_return"],
        benchmark_return=result["benchmark_return"],
        tracking_error=result["tracking_error"],
        information_ratio=result["information_ratio"],
        total_tax_benefit=total_tax_benefit,
    )


@router.get("/tax/{account_id}", response_model=TaxReport)
async def get_tax_report(account_id: int, year: int, db: Session = Depends(get_db)):
    """Get tax report for an account for a specific year."""
    position_mgr = PositionManager(db)

    # Get transactions for the year
    start_date = date(year, 1, 1)
    end_date = date(year, 12, 31)

    transactions = position_mgr.get_transactions(
        account_id=account_id,
        start_date=start_date,
        end_date=end_date,
        transaction_type="sell",
    )

    # Calculate realized gains/losses
    realized_gains = 0.0
    realized_losses = 0.0
    short_term_gains = 0.0
    short_term_losses = 0.0
    long_term_gains = 0.0
    long_term_losses = 0.0
    wash_sale_adjustments = 0.0

    for transaction in transactions:
        if transaction.realized_gain_loss:
            gain_loss = float(transaction.realized_gain_loss)

            if gain_loss > 0:
                realized_gains += gain_loss
                # Determine short-term vs long-term (simplified)
                # In production, would check holding period from tax lot
                long_term_gains += gain_loss * 0.7  # Estimate
                short_term_gains += gain_loss * 0.3
            else:
                realized_losses += abs(gain_loss)
                if transaction.wash_sale_flag:
                    wash_sale_adjustments += abs(gain_loss)
                else:
                    long_term_losses += abs(gain_loss) * 0.7  # Estimate
                    short_term_losses += abs(gain_loss) * 0.3

    return TaxReport(
        account_id=account_id,
        year=year,
        realized_gains=realized_gains,
        realized_losses=realized_losses,
        net_realized_gain_loss=realized_gains - realized_losses,
        short_term_gains=short_term_gains,
        short_term_losses=short_term_losses,
        long_term_gains=long_term_gains,
        long_term_losses=long_term_losses,
        wash_sale_adjustments=wash_sale_adjustments,
    )


@router.get("/positions/{account_id}")
async def get_positions_report(account_id: int, db: Session = Depends(get_db)):
    """Get detailed positions report."""
    position_mgr = PositionManager(db)
    positions = position_mgr.get_positions(account_id)
    tax_lots = position_mgr.get_tax_lots(account_id)

    from src.data.market_data import MarketDataManager
    from src.core.database import Security

    market_data_mgr = MarketDataManager(db)

    result = []
    for position in positions:
        security = db.query(Security).filter(Security.security_id == position.security_id).first()
        current_price = market_data_mgr.get_latest_price(position.security_id)

        # Get tax lots for this position
        position_tax_lots = [lot for lot in tax_lots if lot.security_id == position.security_id]

        unrealized_gain_loss = 0.0
        if current_price:
            for lot in position_tax_lots:
                cost_basis = float(lot.cost_basis)
                current_value = float(lot.remaining_quantity * current_price)
                unrealized_gain_loss += current_value - cost_basis

        result.append(
            {
                "security_id": position.security_id,
                "ticker": security.ticker if security else f"SEC_{position.security_id}",
                "quantity": float(position.quantity),
                "current_price": float(current_price) if current_price else None,
                "market_value": float(position.quantity * current_price) if current_price else None,
                "unrealized_gain_loss": unrealized_gain_loss,
                "tax_lots": len(position_tax_lots),
            }
        )

    return {"positions": result}

