"""
Security master data management.
"""
from typing import Optional
import pandas as pd
from sqlalchemy.orm import Session
from src.core.database import Security

class SecurityMaster:
    """Manager for security master data operations."""
    def __init__(self, session: Session):
        self.session = session
    
    def get_or_create_security(self, ticker: str, cusip: Optional[str] = None,
                               isin: Optional[str] = None, company_name: Optional[str] = None,
                               sector: Optional[str] = None, industry: Optional[str] = None,
                               exchange: Optional[str] = None, security_type: str = "stock") -> Security:
        security = self.session.query(Security).filter(Security.ticker == ticker.upper()).first()
        if security:
            if cusip and not security.cusip:
                security.cusip = cusip
            if company_name:
                security.company_name = company_name
            if sector:
                security.sector = sector
            self.session.commit()
            self.session.refresh(security)
            return security
        security = Security(ticker=ticker.upper(), cusip=cusip, isin=isin,
                          company_name=company_name, sector=sector, industry=industry,
                          exchange=exchange, security_type=security_type)
        self.session.add(security)
        self.session.commit()
        self.session.refresh(security)
        return security
    
    def get_security_by_ticker(self, ticker: str):
        return self.session.query(Security).filter(Security.ticker == ticker.upper()).first()
    
    def get_security_by_id(self, security_id: int):
        return self.session.query(Security).filter(Security.security_id == security_id).first()
