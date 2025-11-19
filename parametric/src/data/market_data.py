"""
Market data ingestion and management.
"""
from datetime import date
from decimal import Decimal
from typing import Optional
import pandas as pd
import yfinance as yf
from sqlalchemy.orm import Session
from src.core.database import MarketData, Security

class MarketDataManager:
    """Manager for market data operations."""
    def __init__(self, session: Session):
        self.session = session
    
    def get_price_history(self, security_id: int, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Return price history for a security between dates as a DataFrame with columns: ['close'] indexed by date.
        If no data exists and USE_MOCK_MARKET_DATA=true, synthesize a simple price series.
        """
        from sqlalchemy import and_
        import os
        import numpy as np
        import pandas as pd
        from pandas.tseries.offsets import BDay

        # Query stored market data
        rows = (
            self.session.query(MarketData)
            .filter(
                MarketData.security_id == security_id,
                MarketData.date >= start_date,
                MarketData.date <= end_date,
            )
            .order_by(MarketData.date.asc())
            .all()
        )

        if rows:
            dates = [r.date for r in rows]
            closes = [float(r.close_price) for r in rows]
            df = pd.DataFrame({"close": closes}, index=pd.to_datetime(dates))
            df.index.name = "date"
            return df

        # Synthesize if mock data enabled
        if os.environ.get("USE_MOCK_MARKET_DATA", "").lower() in ("1", "true", "yes"):
            # Generate business day index
            idx = pd.date_range(start=start_date, end=end_date, freq=BDay())
            if len(idx) == 0:
                return pd.DataFrame(columns=["close"])
            # Simple random walk for prices
            rng = np.random.default_rng(abs(hash((security_id, str(start_date), str(end_date)))) % (2**32))
            # small daily vol
            daily_vol = 0.01
            returns = rng.normal(loc=0.0002, scale=daily_vol, size=len(idx))
            price0 = 100.0 + (security_id % 10)  # deterministic starting point per security
            prices = [price0]
            for r in returns[1:]:
                prices.append(prices[-1] * (1.0 + r))
            df = pd.DataFrame({"close": prices}, index=idx)
            df.index.name = "date"
            return df

        # No data
        return pd.DataFrame(columns=["close"])
    
    def download_and_store_price_data(self, ticker: str, start_date: Optional[date] = None,
                                      end_date: Optional[date] = None, period: str = "1y"):
        security = self.session.query(Security).filter(Security.ticker == ticker.upper()).first()
        if not security:
            raise ValueError(f"Security {ticker} not found")
        stock = yf.Ticker(ticker)
        if start_date and end_date:
            hist = stock.history(start=start_date, end=end_date)
        else:
            hist = stock.history(period=period)
        if hist.empty:
            return []
        market_data_list = []
        for date_idx, row in hist.iterrows():
            existing = self.session.query(MarketData).filter(
                MarketData.security_id == security.security_id,
                MarketData.date == date_idx.date()).first()
            if existing:
                existing.close_price = Decimal(str(row["Close"]))
                existing.volume = int(row["Volume"]) if pd.notna(row["Volume"]) else None
                market_data_list.append(existing)
            else:
                market_data = MarketData(security_id=security.security_id, date=date_idx.date(),
                                       close_price=Decimal(str(row["Close"])),
                                       volume=int(row["Volume"]) if pd.notna(row["Volume"]) else None)
                self.session.add(market_data)
                market_data_list.append(market_data)
        self.session.commit()
        return market_data_list
    
    def get_latest_price(self, security_id: int) -> Optional[Decimal]:
        latest = self.session.query(MarketData).filter(
            MarketData.security_id == security_id).order_by(MarketData.date.desc()).first()
        return latest.close_price if latest else None
    
    def get_price_on_date(self, security_id: int, price_date: date) -> Optional[Decimal]:
        market_data = self.session.query(MarketData).filter(
            MarketData.security_id == security_id, MarketData.date == price_date).first()
        return market_data.close_price if market_data else None
