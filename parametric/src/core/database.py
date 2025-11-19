"""
Database models and connection management using SQLAlchemy.

This module defines all database models for the tax-aware portfolio management system.
"""
from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    """Base class for all database models."""
    pass


# Security Master Data
class Security(Base):
    """Security master data table."""
    __tablename__ = "securities"
    security_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(10), unique=True, nullable=False, index=True)
    cusip: Mapped[Optional[str]] = mapped_column(String(9), unique=True, nullable=True, index=True)
    isin: Mapped[Optional[str]] = mapped_column(String(12), unique=True, nullable=True)
    company_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    sector: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, index=True)
    industry: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    exchange: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    security_type: Mapped[str] = mapped_column(String(20), nullable=False, default="stock")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
    
    positions: Mapped[list["Position"]] = relationship(back_populates="security")
    tax_lots: Mapped[list["TaxLot"]] = relationship(back_populates="security")
    transactions: Mapped[list["Transaction"]] = relationship(back_populates="security")
    market_data: Mapped[list["MarketData"]] = relationship(back_populates="security")
    benchmark_constituents: Mapped[list["BenchmarkConstituent"]] = relationship(back_populates="security")
    rebalancing_trades: Mapped[list["RebalancingTrade"]] = relationship(back_populates="security")

    def __repr__(self) -> str:
        return f"<Security(ticker={self.ticker}, name={self.company_name})>"


# Benchmark Data
class Benchmark(Base):
    """Benchmark definitions."""
    __tablename__ = "benchmarks"
    benchmark_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    benchmark_name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    benchmark_type: Mapped[str] = mapped_column(String(50), nullable=False, default="index")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
    
    accounts: Mapped[list["Account"]] = relationship(back_populates="benchmark")
    constituents: Mapped[list["BenchmarkConstituent"]] = relationship(back_populates="benchmark")

    def __repr__(self) -> str:
        return f"<Benchmark(name={self.benchmark_name})>"


class BenchmarkConstituent(Base):
    """Benchmark constituent weights."""
    __tablename__ = "benchmark_constituents"
    benchmark_id: Mapped[int] = mapped_column(Integer, ForeignKey("benchmarks.benchmark_id"), primary_key=True)
    security_id: Mapped[int] = mapped_column(Integer, ForeignKey("securities.security_id"), primary_key=True)
    effective_date: Mapped[datetime] = mapped_column(Date, primary_key=True, nullable=False)
    weight: Mapped[Decimal] = mapped_column(Numeric(10, 8), nullable=False)
    
    benchmark: Mapped["Benchmark"] = relationship(back_populates="constituents")
    security: Mapped["Security"] = relationship(back_populates="benchmark_constituents")

    def __repr__(self) -> str:
        return f"<BenchmarkConstituent(benchmark_id={self.benchmark_id}, security_id={self.security_id}, weight={self.weight})>"


# Account Management
class Account(Base):
    """Account information."""
    __tablename__ = "accounts"
    account_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    client_name: Mapped[str] = mapped_column(String(255), nullable=False)
    account_type: Mapped[str] = mapped_column(String(50), nullable=False, default="taxable")
    benchmark_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("benchmarks.benchmark_id"), nullable=True)
    tax_rate_short_term: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False, default=Decimal("0.37"))
    tax_rate_long_term: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False, default=Decimal("0.20"))
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    
    benchmark: Mapped[Optional["Benchmark"]] = relationship(back_populates="accounts")
    positions: Mapped[list["Position"]] = relationship(back_populates="account")
    tax_lots: Mapped[list["TaxLot"]] = relationship(back_populates="account")
    transactions: Mapped[list["Transaction"]] = relationship(back_populates="account")
    rebalancing_events: Mapped[list["RebalancingEvent"]] = relationship(back_populates="account")

    def __repr__(self) -> str:
        return f"<Account(account_id={self.account_id}, client={self.client_name})>"


# Position and Tax Lot Management
class Position(Base):
    """Current positions per account."""
    __tablename__ = "positions"
    position_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(Integer, ForeignKey("accounts.account_id"), nullable=False)
    security_id: Mapped[int] = mapped_column(Integer, ForeignKey("securities.security_id"), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(15, 6), nullable=False)
    market_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    last_updated: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    
    account: Mapped["Account"] = relationship(back_populates="positions")
    security: Mapped["Security"] = relationship(back_populates="positions")

    def __repr__(self) -> str:
        return f"<Position(account_id={self.account_id}, security_id={self.security_id}, quantity={self.quantity})>"


class TaxLot(Base):
    """Individual tax lots with cost basis."""
    __tablename__ = "tax_lots"
    tax_lot_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(Integer, ForeignKey("accounts.account_id"), nullable=False)
    security_id: Mapped[int] = mapped_column(Integer, ForeignKey("securities.security_id"), nullable=False)
    purchase_date: Mapped[datetime] = mapped_column(Date, nullable=False)
    purchase_price: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(15, 6), nullable=False)
    cost_basis: Mapped[Decimal] = mapped_column(Numeric(15, 2), nullable=False)
    remaining_quantity: Mapped[Decimal] = mapped_column(Numeric(15, 6), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="open")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
    
    account: Mapped["Account"] = relationship(back_populates="tax_lots")
    security: Mapped["Security"] = relationship(back_populates="tax_lots")
    transactions: Mapped[list["Transaction"]] = relationship(back_populates="tax_lot")
    rebalancing_trades: Mapped[list["RebalancingTrade"]] = relationship(back_populates="tax_lot")

    def __repr__(self) -> str:
        return f"<TaxLot(tax_lot_id={self.tax_lot_id}, security_id={self.security_id}, quantity={self.remaining_quantity}, status={self.status})>"


# Transaction History
class Transaction(Base):
    """Transaction history."""
    __tablename__ = "transactions"
    transaction_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(Integer, ForeignKey("accounts.account_id"), nullable=False)
    security_id: Mapped[int] = mapped_column(Integer, ForeignKey("securities.security_id"), nullable=False)
    transaction_type: Mapped[str] = mapped_column(String(20), nullable=False)
    transaction_date: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    quantity: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 6), nullable=True)
    price: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    total_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    tax_lot_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("tax_lots.tax_lot_id"), nullable=True)
    realized_gain_loss: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    wash_sale_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
    
    account: Mapped["Account"] = relationship(back_populates="transactions")
    security: Mapped["Security"] = relationship(back_populates="transactions")
    tax_lot: Mapped[Optional["TaxLot"]] = relationship(back_populates="transactions")

    def __repr__(self) -> str:
        return f"<Transaction(transaction_id={self.transaction_id}, type={self.transaction_type}, date={self.transaction_date})>"


# Market Data
class MarketData(Base):
    """Market data (price history)."""
    __tablename__ = "market_data"
    security_id: Mapped[int] = mapped_column(Integer, ForeignKey("securities.security_id"), primary_key=True)
    date: Mapped[datetime] = mapped_column(Date, primary_key=True, nullable=False)
    open_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    high_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    low_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    close_price: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    volume: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    adjusted_close: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    
    security: Mapped["Security"] = relationship(back_populates="market_data")

    def __repr__(self) -> str:
        return f"<MarketData(security_id={self.security_id}, date={self.date}, close={self.close_price})>"


# Rebalancing
class RebalancingEvent(Base):
    """Rebalancing events."""
    __tablename__ = "rebalancing_events"
    rebalancing_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(Integer, ForeignKey("accounts.account_id"), nullable=False)
    rebalancing_date: Mapped[datetime] = mapped_column(Date, nullable=False)
    rebalancing_type: Mapped[str] = mapped_column(String(50), nullable=False)
    tracking_error_before: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6), nullable=True)
    tracking_error_after: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6), nullable=True)
    tax_benefit: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
    
    account: Mapped["Account"] = relationship(back_populates="rebalancing_events")
    trades: Mapped[list["RebalancingTrade"]] = relationship(back_populates="rebalancing_event")

    def __repr__(self) -> str:
        return f"<RebalancingEvent(rebalancing_id={self.rebalancing_id}, account_id={self.account_id}, type={self.rebalancing_type}, status={self.status})>"


class RebalancingTrade(Base):
    """Proposed trades for rebalancing."""
    __tablename__ = "rebalancing_trades"
    trade_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    rebalancing_id: Mapped[int] = mapped_column(Integer, ForeignKey("rebalancing_events.rebalancing_id"), nullable=False)
    security_id: Mapped[int] = mapped_column(Integer, ForeignKey("securities.security_id"), nullable=False)
    trade_type: Mapped[str] = mapped_column(String(10), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(15, 6), nullable=False)
    price: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    tax_lot_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("tax_lots.tax_lot_id"), nullable=True)
    estimated_tax_benefit: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    executed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    rebalancing_event: Mapped["RebalancingEvent"] = relationship(back_populates="trades")
    security: Mapped["Security"] = relationship(back_populates="rebalancing_trades")
    tax_lot: Mapped[Optional["TaxLot"]] = relationship(back_populates="rebalancing_trades")

    def __repr__(self) -> str:
        return f"<RebalancingTrade(trade_id={self.trade_id}, type={self.trade_type}, quantity={self.quantity}, status={self.status})>"


# Database Connection Management
def create_database_engine(database_url: str, echo: bool = False):
    """Create SQLAlchemy database engine."""
    return create_engine(database_url, echo=echo, future=True)


def init_database(engine):
    """Initialize database by creating all tables."""
    Base.metadata.create_all(engine)


def get_session_factory(engine):
    """Get session factory for database operations."""
    from sqlalchemy.orm import sessionmaker
    return sessionmaker(bind=engine, future=True)
