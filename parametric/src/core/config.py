"""
Configuration management for the tax-aware portfolio management system.
"""
import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)

class Config:
    """Application configuration."""
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/tax_aware_portfolio")
    USE_SQLITE: bool = os.getenv("USE_SQLITE", "false").lower() == "true"
    SQLITE_PATH: str = os.getenv("SQLITE_PATH", "data/tax_aware_portfolio.db")
    MARKET_DATA_PROVIDER: str = os.getenv("MARKET_DATA_PROVIDER", "yfinance")
    MARKET_DATA_API_KEY: Optional[str] = os.getenv("MARKET_DATA_API_KEY", None)
    OPTIMIZATION_SOLVER: str = os.getenv("OPTIMIZATION_SOLVER", "OSQP")
    LAMBDA_TRANSACTION: float = float(os.getenv("LAMBDA_TRANSACTION", "0.001"))
    LAMBDA_TAX: float = float(os.getenv("LAMBDA_TAX", "1.0"))
    TRACKING_ERROR_THRESHOLD: float = float(os.getenv("TRACKING_ERROR_THRESHOLD", "0.005"))
    TURNOVER_LIMIT: float = float(os.getenv("TURNOVER_LIMIT", "0.50"))
    MIN_TAX_LOSS_THRESHOLD: float = float(os.getenv("MIN_TAX_LOSS_THRESHOLD", "1000.0"))
    WASH_SALE_WINDOW_DAYS: int = int(os.getenv("WASH_SALE_WINDOW_DAYS", "30"))
    LONG_TERM_HOLDING_DAYS: int = int(os.getenv("LONG_TERM_HOLDING_DAYS", "365"))
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    API_RELOAD: bool = os.getenv("API_RELOAD", "false").lower() == "true"
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    # Barra Risk Model Integration
    BARRA_DB_PATH: str = os.getenv(
        "BARRA_DB_PATH", 
        "/home/tasos/tax_aware/barra/barra_analytics.duckdb"
    )

    @classmethod
    def get_database_url(cls) -> str:
        """Get database URL based on configuration."""
        if cls.USE_SQLITE:
            return f"sqlite:///{cls.SQLITE_PATH}"
        return cls.DATABASE_URL
