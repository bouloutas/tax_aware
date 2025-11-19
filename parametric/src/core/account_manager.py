"""
Account management functionality.
"""
from datetime import datetime
from decimal import Decimal
from typing import Optional
from sqlalchemy.orm import Session
from src.core.database import Account, Benchmark

class AccountManager:
    """Manager for account operations."""
    def __init__(self, session: Session):
        self.session = session
    
    def create_account(self, client_name: str, account_type: str = "taxable",
                      benchmark_id: Optional[int] = None,
                      tax_rate_short_term: Decimal = Decimal("0.37"),
                      tax_rate_long_term: Decimal = Decimal("0.20")) -> Account:
        account = Account(client_name=client_name, account_type=account_type,
                         benchmark_id=benchmark_id, tax_rate_short_term=tax_rate_short_term,
                         tax_rate_long_term=tax_rate_long_term)
        self.session.add(account)
        self.session.commit()
        self.session.refresh(account)
        return account
    
    def get_account(self, account_id: int) -> Optional[Account]:
        return self.session.query(Account).filter(Account.account_id == account_id).first()
    
    def get_all_accounts(self) -> list[Account]:
        return self.session.query(Account).all()
    
    def update_account(self, account_id: int, client_name: Optional[str] = None,
                     benchmark_id: Optional[int] = None,
                     tax_rate_short_term: Optional[Decimal] = None,
                     tax_rate_long_term: Optional[Decimal] = None) -> Optional[Account]:
        account = self.get_account(account_id)
        if not account:
            return None
        if client_name is not None:
            account.client_name = client_name
        if benchmark_id is not None:
            account.benchmark_id = benchmark_id
        if tax_rate_short_term is not None:
            account.tax_rate_short_term = tax_rate_short_term
        if tax_rate_long_term is not None:
            account.tax_rate_long_term = tax_rate_long_term
        account.updated_at = datetime.now()
        self.session.commit()
        self.session.refresh(account)
        return account
    
    def delete_account(self, account_id: int) -> bool:
        account = self.get_account(account_id)
        if not account:
            return False
        self.session.delete(account)
        self.session.commit()
        return True
