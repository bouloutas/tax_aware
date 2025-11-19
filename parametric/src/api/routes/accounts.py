"""
Account management API endpoints.
"""
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.core.account_manager import AccountManager
from src.core.database import Account
from src.core.position_manager import PositionManager


def get_db(request):
    """Get database session."""
    Session = request.app.state.Session
    db = Session()
    try:
        yield db
    finally:
        db.close()


router = APIRouter()


class AccountCreate(BaseModel):
    """Account creation request model."""

    client_name: str
    account_type: str = "taxable"
    benchmark_id: Optional[int] = None
    tax_rate_short_term: float = 0.37
    tax_rate_long_term: float = 0.20


class AccountResponse(BaseModel):
    """Account response model."""

    account_id: int
    client_name: str
    account_type: str
    benchmark_id: Optional[int]
    tax_rate_short_term: float
    tax_rate_long_term: float

    class Config:
        from_attributes = True


class PositionResponse(BaseModel):
    """Position response model."""

    security_id: int
    ticker: str
    quantity: float
    market_value: Optional[float]

    class Config:
        from_attributes = True


@router.post("/", response_model=AccountResponse)
async def create_account(account_data: AccountCreate, db: Session = Depends(get_db)):
    """Create a new account."""
    account_mgr = AccountManager(db)
    from decimal import Decimal

    account = account_mgr.create_account(
        client_name=account_data.client_name,
        account_type=account_data.account_type,
        benchmark_id=account_data.benchmark_id,
        tax_rate_short_term=Decimal(str(account_data.tax_rate_short_term)),
        tax_rate_long_term=Decimal(str(account_data.tax_rate_long_term)),
    )
    return AccountResponse(
        account_id=account.account_id,
        client_name=account.client_name,
        account_type=account.account_type,
        benchmark_id=account.benchmark_id,
        tax_rate_short_term=float(account.tax_rate_short_term),
        tax_rate_long_term=float(account.tax_rate_long_term),
    )


@router.get("/", response_model=List[AccountResponse])
async def list_accounts(db: Session = Depends(get_db)):
    """List all accounts."""
    account_mgr = AccountManager(db)
    accounts = account_mgr.get_all_accounts()
    return [
        AccountResponse(
            account_id=acc.account_id,
            client_name=acc.client_name,
            account_type=acc.account_type,
            benchmark_id=acc.benchmark_id,
            tax_rate_short_term=float(acc.tax_rate_short_term),
            tax_rate_long_term=float(acc.tax_rate_long_term),
        )
        for acc in accounts
    ]


@router.get("/{account_id}", response_model=AccountResponse)
async def get_account(account_id: int, db: Session = Depends(get_db)):
    """Get account by ID."""
    account_mgr = AccountManager(db)
    account = account_mgr.get_account(account_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    return AccountResponse(
        account_id=account.account_id,
        client_name=account.client_name,
        account_type=account.account_type,
        benchmark_id=account.benchmark_id,
        tax_rate_short_term=float(account.tax_rate_short_term),
        tax_rate_long_term=float(account.tax_rate_long_term),
    )


@router.get("/{account_id}/positions", response_model=List[PositionResponse])
async def get_positions(account_id: int, db: Session = Depends(get_db)):
    """Get positions for an account."""
    position_mgr = PositionManager(db)
    positions = position_mgr.get_positions(account_id)

    from src.data.market_data import MarketDataManager
    from src.core.database import Security

    market_data_mgr = MarketDataManager(db)
    result = []

    for position in positions:
        security = db.query(Security).filter(Security.security_id == position.security_id).first()
        current_price = market_data_mgr.get_latest_price(position.security_id)
        market_value = float(position.quantity * current_price) if current_price else None

        result.append(
            PositionResponse(
                security_id=position.security_id,
                ticker=security.ticker if security else f"SEC_{position.security_id}",
                quantity=float(position.quantity),
                market_value=market_value,
            )
        )

    return result

