"""
Tax-loss harvesting API endpoints.
"""
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.tax_harvesting import TaxLossHarvestingFinder, TaxLossHarvestingOpportunity


def get_db(request):
    """Get database session."""
    return request.app.state.Session()


router = APIRouter()


class OpportunityResponse(BaseModel):
    """Tax-loss harvesting opportunity response model."""

    tax_lot_id: int
    security_id: int
    ticker: str
    unrealized_loss: float
    tax_benefit: float
    wash_sale_violation: bool
    score: float
    replacement_securities: List[dict]


@router.get("/opportunities/{account_id}", response_model=List[OpportunityResponse])
async def get_opportunities(
    account_id: int,
    min_loss_threshold: Optional[float] = None,
    max_opportunities: int = 10,
    db: Session = Depends(get_db),
):
    """Get tax-loss harvesting opportunities for an account."""
    finder = TaxLossHarvestingFinder(db)

    min_threshold = Decimal(str(min_loss_threshold)) if min_loss_threshold else None

    opportunities = finder.find_opportunities(
        account_id=account_id,
        min_loss_threshold=min_threshold,
        max_opportunities=max_opportunities,
    )

    return [
        OpportunityResponse(
            tax_lot_id=opp.tax_lot_id,
            security_id=opp.security_id,
            ticker=opp.ticker,
            unrealized_loss=float(opp.unrealized_loss),
            tax_benefit=float(opp.tax_benefit),
            wash_sale_violation=opp.wash_sale_violation,
            score=opp.score,
            replacement_securities=opp.replacement_securities,
        )
        for opp in opportunities
    ]


@router.get("/opportunities/{account_id}/summary")
async def get_opportunities_summary(account_id: int, db: Session = Depends(get_db)):
    """Get summary of tax-loss harvesting opportunities."""
    finder = TaxLossHarvestingFinder(db)

    opportunities = finder.find_opportunities(
        account_id=account_id,
        min_loss_threshold=Decimal("1000"),
        max_opportunities=50,
    )

    total_loss = sum(abs(opp.unrealized_loss) for opp in opportunities)
    total_benefit = sum(opp.tax_benefit for opp in opportunities)
    significant_opportunities = [opp for opp in opportunities if opp.tax_benefit > Decimal("500")]

    return {
        "total_opportunities": len(opportunities),
        "significant_opportunities": len(significant_opportunities),
        "total_unrealized_loss": float(total_loss),
        "total_tax_benefit": float(total_benefit),
        "average_tax_benefit": float(total_benefit / len(opportunities)) if opportunities else 0.0,
    }

