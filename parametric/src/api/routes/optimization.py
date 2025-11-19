"""
Portfolio optimization API endpoints.
"""
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.optimization import PortfolioOptimizer, TrackingErrorCalculator
from src.tax_harvesting import TaxLossHarvestingFinder


def get_db(request):
    """Get database session."""
    return request.app.state.Session()


router = APIRouter()


class OptimizationRequest(BaseModel):
    """Optimization request model."""

    account_id: int
    max_tracking_error: Optional[float] = None
    include_tax_harvesting: bool = True


class OptimizationResponse(BaseModel):
    """Optimization response model."""

    status: str
    tracking_error: float
    tax_benefit: float
    optimal_weights: dict
    trades: dict
    objective_value: float


class TrackingErrorResponse(BaseModel):
    """Tracking error response model."""

    tracking_error: float
    active_return: float
    information_ratio: float
    portfolio_return: float
    benchmark_return: float


@router.post("/optimize", response_model=OptimizationResponse)
async def optimize_portfolio(request: OptimizationRequest, db: Session = Depends(get_db)):
    """Optimize portfolio for an account."""
    optimizer = PortfolioOptimizer(db)
    tax_opportunities = None

    if request.include_tax_harvesting:
        finder = TaxLossHarvestingFinder(db)
        tax_opportunities = finder.find_opportunities(
            account_id=request.account_id,
            max_opportunities=20,
        )

    result = optimizer.optimize_with_tax_harvesting(
        account_id=request.account_id,
        tax_loss_opportunities=tax_opportunities or [],
        max_tracking_error=request.max_tracking_error,
    )

    # Convert Series to dict for JSON serialization
    optimal_weights = {str(k): float(v) for k, v in result["optimal_weights"].items()}
    trades = {str(k): float(v) for k, v in result["trades"].items()}

    return OptimizationResponse(
        status=result["status"],
        tracking_error=result["tracking_error"],
        tax_benefit=result["tax_benefit"],
        optimal_weights=optimal_weights,
        trades=trades,
        objective_value=result["objective_value"],
    )


@router.get("/tracking-error/{account_id}", response_model=TrackingErrorResponse)
async def get_tracking_error(account_id: int, db: Session = Depends(get_db)):
    """Get tracking error for an account."""
    calc = TrackingErrorCalculator(db)
    result = calc.calculate_tracking_error(account_id)

    return TrackingErrorResponse(
        tracking_error=result["tracking_error"],
        active_return=result["active_return"],
        information_ratio=result["information_ratio"],
        portfolio_return=result["portfolio_return"],
        benchmark_return=result["benchmark_return"],
    )


@router.get("/weights/{account_id}")
async def get_current_weights(account_id: int, db: Session = Depends(get_db)):
    """Get current portfolio weights."""
    calc = TrackingErrorCalculator(db)
    weights_df = calc.get_current_weights(account_id)

    return {
        "weights": {str(row["security_id"]): float(row["weight"]) for _, row in weights_df.iterrows()},
        "benchmark_weights": {
            str(row["security_id"]): float(row["weight"])
            for _, row in calc.get_benchmark_weights(account_id).iterrows()
        },
    }

