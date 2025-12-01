"""
Database models and connection management using SQLAlchemy.

This module defines all database models for the tax-aware portfolio management system.
"""
from datetime import datetime, date
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


# Household Management
class Household(Base):
    """Household grouping multiple related accounts."""
    __tablename__ = "households"
    
    household_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    household_name: Mapped[str] = mapped_column(String(255), nullable=False)
    primary_tax_rate_st: Mapped[Decimal] = mapped_column(
        Numeric(5, 4), nullable=False, default=Decimal("0.37")
    )
    primary_tax_rate_lt: Mapped[Decimal] = mapped_column(
        Numeric(5, 4), nullable=False, default=Decimal("0.20")
    )
    annual_tax_budget: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
    
    # Relationships
    household_accounts: Mapped[list["HouseholdAccount"]] = relationship(back_populates="household")

    def __repr__(self) -> str:
        return f"<Household(household_id={self.household_id}, name={self.household_name})>"


class HouseholdAccount(Base):
    """Links accounts to households with roles."""
    __tablename__ = "household_accounts"
    
    household_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("households.household_id"), primary_key=True
    )
    account_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("accounts.account_id"), primary_key=True
    )
    account_role: Mapped[str] = mapped_column(String(50), nullable=False, default="primary_taxable")
    
    # Relationships
    household: Mapped["Household"] = relationship(back_populates="household_accounts")
    account: Mapped["Account"] = relationship()

    def __repr__(self) -> str:
        return f"<HouseholdAccount(household_id={self.household_id}, account_id={self.account_id}, role={self.account_role})>"


# Transition Management
class TransitionPlan(Base):
    """Multi-year transition plans for concentrated portfolios."""
    __tablename__ = "transition_plans"
    
    plan_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(Integer, ForeignKey("accounts.account_id"), nullable=False)
    target_benchmark_id: Mapped[int] = mapped_column(Integer, ForeignKey("benchmarks.benchmark_id"), nullable=False)
    transition_years: Mapped[int] = mapped_column(Integer, nullable=False)
    annual_tax_budget: Mapped[Decimal] = mapped_column(Numeric(15, 2), nullable=False)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
    
    # Relationships
    account: Mapped["Account"] = relationship()
    benchmark: Mapped["Benchmark"] = relationship()
    targets: Mapped[list["TransitionTarget"]] = relationship(back_populates="plan")

    def __repr__(self) -> str:
        return f"<TransitionPlan(plan_id={self.plan_id}, account_id={self.account_id}, years={self.transition_years}, status={self.status})>"


class TransitionTarget(Base):
    """Annual targets for transition plans."""
    __tablename__ = "transition_targets"
    
    plan_id: Mapped[int] = mapped_column(Integer, ForeignKey("transition_plans.plan_id"), primary_key=True)
    year_number: Mapped[int] = mapped_column(Integer, primary_key=True)
    target_tracking_error: Mapped[Decimal] = mapped_column(Numeric(8, 6), nullable=False)
    max_turnover: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)
    realized_tax: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    
    # Relationships
    plan: Mapped["TransitionPlan"] = relationship(back_populates="targets")

    def __repr__(self) -> str:
        return f"<TransitionTarget(plan_id={self.plan_id}, year={self.year_number}, TE={self.target_tracking_error})>"


class EmbeddedGain(Base):
    """Tracks embedded gains per tax lot over time."""
    __tablename__ = "embedded_gains"
    
    tax_lot_id: Mapped[int] = mapped_column(Integer, ForeignKey("tax_lots.tax_lot_id"), primary_key=True)
    as_of_date: Mapped[date] = mapped_column(Date, primary_key=True, nullable=False)
    embedded_gain: Mapped[Decimal] = mapped_column(Numeric(15, 2), nullable=False)
    holding_period_days: Mapped[int] = mapped_column(Integer, nullable=False)
    is_long_term: Mapped[bool] = mapped_column(Boolean, nullable=False)
    deferral_value: Mapped[Decimal] = mapped_column(Numeric(15, 2), nullable=False)
    
    # Relationships
    tax_lot: Mapped["TaxLot"] = relationship()

    def __repr__(self) -> str:
        return f"<EmbeddedGain(lot_id={self.tax_lot_id}, date={self.as_of_date}, gain={self.embedded_gain})>"


# Customization
class CustomBenchmarkDefinition(Base):
    """Custom benchmark definitions with factor tilts and constraints."""
    __tablename__ = "custom_benchmarks"
    
    custom_benchmark_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    base_benchmark_id: Mapped[int] = mapped_column(Integer, ForeignKey("benchmarks.benchmark_id"), nullable=False)
    account_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("accounts.account_id"), nullable=True)
    factor_tilts: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON: {"value": 0.2, "momentum": 0.1}
    sector_tilts: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON: {"Technology": 0.05}
    excluded_securities: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON: [1, 2, 3]
    esg_constraints: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON: ESG config
    min_weight: Mapped[Decimal] = mapped_column(Numeric(8, 6), nullable=False, default=Decimal("0.0001"))
    max_weight: Mapped[Decimal] = mapped_column(Numeric(8, 6), nullable=False, default=Decimal("0.10"))
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
    
    # Relationships
    base_benchmark: Mapped["Benchmark"] = relationship()
    account: Mapped[Optional["Account"]] = relationship()

    def __repr__(self) -> str:
        return f"<CustomBenchmarkDefinition(id={self.custom_benchmark_id}, name={self.name})>"


class ESGScore(Base):
    """ESG scores and metrics per security."""
    __tablename__ = "esg_scores"
    
    security_id: Mapped[int] = mapped_column(Integer, ForeignKey("securities.security_id"), primary_key=True)
    as_of_date: Mapped[date] = mapped_column(Date, primary_key=True, nullable=False)
    esg_score: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=True)  # 0-100 scale
    environmental_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2), nullable=True)
    social_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2), nullable=True)
    governance_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2), nullable=True)
    carbon_intensity: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)  # Tons CO2/$ million
    controversies: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON list
    esg_provider: Mapped[str] = mapped_column(String(50), nullable=False, default="unknown")
    
    # Relationships
    security: Mapped["Security"] = relationship()

    def __repr__(self) -> str:
        return f"<ESGScore(security_id={self.security_id}, date={self.as_of_date}, score={self.esg_score})>"


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
