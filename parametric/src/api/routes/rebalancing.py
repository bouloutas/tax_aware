"""
Rebalancing API endpoints.
"""
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.rebalancing import Rebalancer


def get_db(request):
    """Get database session."""
    return request.app.state.Session()


router = APIRouter()


class RebalancingRequest(BaseModel):
    """Rebalancing request model."""

    account_id: int
    rebalancing_type: str = "threshold"
    max_tracking_error: Optional[float] = None
    auto_execute: bool = False


class RebalancingResponse(BaseModel):
    """Rebalancing response model."""

    rebalancing_event_id: Optional[int]
    status: str
    trades: list
    tracking_error_before: float
    tracking_error_after: float
    tax_benefit: float
    message: str


class RebalancingCheckResponse(BaseModel):
    """Rebalancing check response model."""

    rebalancing_needed: bool
    reason: str
    current_tracking_error: float
    tax_opportunities: int
    details: dict


@router.post("/rebalance", response_model=RebalancingResponse)
async def rebalance_account(request: RebalancingRequest, db: Session = Depends(get_db)):
    """Rebalance an account's portfolio."""
    rebalancer = Rebalancer(db)

    result = rebalancer.rebalance_account(
        account_id=request.account_id,
        rebalancing_type=request.rebalancing_type,
        max_tracking_error=request.max_tracking_error,
        auto_execute=request.auto_execute,
    )

    return RebalancingResponse(
        rebalancing_event_id=result["rebalancing_event_id"],
        status=result["status"],
        trades=result["trades"],
        tracking_error_before=result["tracking_error_before"],
        tracking_error_after=result["tracking_error_after"],
        tax_benefit=result["tax_benefit"],
        message=result["message"],
    )


@router.get("/check/{account_id}", response_model=RebalancingCheckResponse)
async def check_rebalancing_needed(
    account_id: int,
    rebalancing_type: str = "threshold",
    db: Session = Depends(get_db),
):
    """Check if rebalancing is needed for an account."""
    rebalancer = Rebalancer(db)

    result = rebalancer.check_rebalancing_needed(
        account_id=account_id,
        rebalancing_type=rebalancing_type,
    )

    return RebalancingCheckResponse(
        rebalancing_needed=result["rebalancing_needed"],
        reason=result["reason"],
        current_tracking_error=result["current_tracking_error"],
        tax_opportunities=result["tax_opportunities"],
        details=result["details"],
    )


@router.get("/events/{account_id}")
async def get_rebalancing_events(account_id: int, db: Session = Depends(get_db)):
    """Get rebalancing events for an account."""
    from src.core.database import RebalancingEvent

    events = (
        db.query(RebalancingEvent)
        .filter(RebalancingEvent.account_id == account_id)
        .order_by(RebalancingEvent.rebalancing_date.desc())
        .limit(50)
        .all()
    )

    return [
        {
            "rebalancing_id": event.rebalancing_id,
            "rebalancing_date": event.rebalancing_date.isoformat(),
            "rebalancing_type": event.rebalancing_type,
            "tracking_error_before": float(event.tracking_error_before) if event.tracking_error_before else None,
            "tracking_error_after": float(event.tracking_error_after) if event.tracking_error_after else None,
            "tax_benefit": float(event.tax_benefit) if event.tax_benefit else None,
            "status": event.status,
        }
        for event in events
    ]

