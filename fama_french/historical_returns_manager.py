"""
Historical Returns Manager

This module provides a solution to bridge the timeline gap between universe scores
(2010-2025) and optimization returns (2018-2025) by using Compustat historical data.

The solution:
1. Uses ticker-GVKEY mapping from optimization_portfolio_monthly_returns
2. Retrieves historical returns from Compustat data_for_factor_construction
3. Provides a unified interface for historical returns data
"""

import pandas as pd
from duckdb_manager import DuckDBManager
from typing import List, Optional, Dict
import numpy as np

class HistoricalReturnsManager:
    """
    Manages historical returns data by combining Compustat historical data
    with current optimization returns data.
    """
    
    def __init__(self):
        self.duckdb_manager = DuckDBManager()
        self._ticker_mapping = None
        self._historical_cache = {}
        
    def get_ticker_mapping(self) -> pd.DataFrame:
        """
        Get ticker to GVKEY/IID mapping from optimization returns.
        """
        if self._ticker_mapping is None:
            query = """
                SELECT DISTINCT TICKER, GVKEY, IID
                FROM optimization_portfolio_monthly_returns 
                WHERE MONTH_END_DATE >= '2020-01-01'
                ORDER BY TICKER
            """
            self._ticker_mapping = self.duckdb_manager.read_sql(query, 'ff')
            print(f"Loaded ticker mapping: {len(self._ticker_mapping)} unique tickers")
        
        return self._ticker_mapping
    
    def get_historical_returns(self, tickers: List[str], start_date: str = '2010-01-01') -> pd.DataFrame:
        """
        Get historical returns for specified tickers from Compustat data.
        
        Args:
            tickers: List of ticker symbols
            start_date: Start date for historical data
            
        Returns:
            DataFrame with columns: RETURN_MONTH_END_DATE, MONTHLY_RETURN, TICKER, GVKEY, IID
        """
        mapping = self.get_ticker_mapping()
        
        # Filter mapping for requested tickers
        ticker_mapping = mapping[mapping['TICKER'].isin(tickers)]
        
        if len(ticker_mapping) == 0:
            print(f"Warning: No mapping found for tickers: {tickers}")
            return pd.DataFrame()
        
        historical_data = []
        
        for _, row in ticker_mapping.iterrows():
            ticker = row['TICKER']
            gvkey = row['GVKEY']
            iid = row['IID']
            
            # Check cache first
            cache_key = f"{ticker}_{gvkey}_{iid}_{start_date}"
            if cache_key in self._historical_cache:
                historical_data.append(self._historical_cache[cache_key])
                continue
            
            # Query Compustat for historical data
            query = f"""
                SELECT 
                    RETURN_MONTH_END_DATE,
                    MONTHLY_RETURN,
                    GVKEY,
                    IID
                FROM data_for_factor_construction 
                WHERE GVKEY = '{gvkey}' AND IID = '{iid}'
                AND RETURN_MONTH_END_DATE >= '{start_date}'
                ORDER BY RETURN_MONTH_END_DATE
            """
            
            try:
                hist_df = self.duckdb_manager.read_sql(query, 'compustat')
                if len(hist_df) > 0:
                    hist_df['TICKER'] = ticker
                    historical_data.append(hist_df)
                    self._historical_cache[cache_key] = hist_df
                    print(f"  {ticker}: {len(hist_df)} historical records")
                else:
                    print(f"  {ticker}: No historical data found")
            except Exception as e:
                print(f"  {ticker}: Error retrieving data - {e}")
        
        if historical_data:
            combined_df = pd.concat(historical_data, ignore_index=True)
            combined_df['RETURN_MONTH_END_DATE'] = pd.to_datetime(combined_df['RETURN_MONTH_END_DATE'])
            # Rename for consistency with other methods
            combined_df = combined_df.rename(columns={'RETURN_MONTH_END_DATE': 'MONTH_END_DATE'})
            return combined_df
        else:
            return pd.DataFrame()
    
    def get_current_returns(self, tickers: List[str]) -> pd.DataFrame:
        """
        Get current returns data from optimization_portfolio_monthly_returns.
        
        Args:
            tickers: List of ticker symbols
            
        Returns:
            DataFrame with current returns data
        """
        ticker_list = "', '".join(tickers)
        query = f"""
            SELECT TICKER, MONTH_END_DATE, MONTHLY_RETURN, GVKEY, IID
            FROM optimization_portfolio_monthly_returns 
            WHERE TICKER IN ('{ticker_list}')
            ORDER BY MONTH_END_DATE, TICKER
        """
        
        try:
            df = self.duckdb_manager.read_sql(query, 'ff')
            df['MONTH_END_DATE'] = pd.to_datetime(df['MONTH_END_DATE'])
            return df
        except Exception as e:
            print(f"Error retrieving current returns: {e}")
            return pd.DataFrame()
    
    def get_unified_returns(self, tickers: List[str], start_date: str = '2010-01-01') -> pd.DataFrame:
        """
        Get unified returns data combining historical and current data.
        
        Args:
            tickers: List of ticker symbols
            start_date: Start date for historical data
            
        Returns:
            DataFrame with unified returns data
        """
        # Get historical data
        historical_df = self.get_historical_returns(tickers, start_date)
        
        # Get current data
        current_df = self.get_current_returns(tickers)
        
        if len(historical_df) == 0 and len(current_df) == 0:
            return pd.DataFrame()
        
        # Combine the datasets
        if len(historical_df) > 0 and len(current_df) > 0:
            # Rename columns for consistency
            historical_df = historical_df.rename(columns={'RETURN_MONTH_END_DATE': 'MONTH_END_DATE'})
            
            # Combine and remove duplicates
            combined_df = pd.concat([historical_df, current_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['TICKER', 'MONTH_END_DATE'])
            combined_df = combined_df.sort_values(['MONTH_END_DATE', 'TICKER'])
            
            print(f"Unified returns: {len(combined_df)} records from {combined_df['MONTH_END_DATE'].min()} to {combined_df['MONTH_END_DATE'].max()}")
            return combined_df
        
        elif len(historical_df) > 0:
            historical_df = historical_df.rename(columns={'RETURN_MONTH_END_DATE': 'MONTH_END_DATE'})
            return historical_df
        
        else:
            return current_df
    
    def get_returns_pivot(self, tickers: List[str], start_date: str = '2010-01-01') -> pd.DataFrame:
        """
        Get returns data in pivot format (dates as index, tickers as columns).
        
        Args:
            tickers: List of ticker symbols
            start_date: Start date for historical data
            
        Returns:
            Pivot DataFrame with dates as index and tickers as columns
        """
        unified_df = self.get_unified_returns(tickers, start_date)
        
        if len(unified_df) == 0:
            return pd.DataFrame()
        
        # Create pivot table
        pivot_df = unified_df.pivot(index='MONTH_END_DATE', columns='TICKER', values='MONTHLY_RETURN')
        
        print(f"Returns pivot: {pivot_df.shape[0]} dates x {pivot_df.shape[1]} tickers")
        return pivot_df
    
    def get_available_tickers(self) -> List[str]:
        """
        Get list of all available tickers.
        
        Returns:
            List of ticker symbols
        """
        mapping = self.get_ticker_mapping()
        return mapping['TICKER'].tolist()
    
    def get_data_coverage(self, tickers: List[str]) -> pd.DataFrame:
        """
        Get data coverage information for specified tickers.
        
        Args:
            tickers: List of ticker symbols
            
        Returns:
            DataFrame with coverage information
        """
        mapping = self.get_ticker_mapping()
        ticker_mapping = mapping[mapping['TICKER'].isin(tickers)]
        
        coverage_data = []
        
        for _, row in ticker_mapping.iterrows():
            ticker = row['TICKER']
            gvkey = row['GVKEY']
            iid = row['IID']
            
            # Check historical data coverage
            hist_query = f"""
                SELECT 
                    MIN(RETURN_MONTH_END_DATE) as hist_min_date,
                    MAX(RETURN_MONTH_END_DATE) as hist_max_date,
                    COUNT(*) as hist_count
                FROM data_for_factor_construction 
                WHERE GVKEY = '{gvkey}' AND IID = '{iid}'
            """
            
            try:
                hist_coverage = self.duckdb_manager.read_sql(hist_query, 'compustat')
                coverage_data.append({
                    'TICKER': ticker,
                    'GVKEY': gvkey,
                    'IID': iid,
                    'HIST_MIN_DATE': hist_coverage.iloc[0]['hist_min_date'],
                    'HIST_MAX_DATE': hist_coverage.iloc[0]['hist_max_date'],
                    'HIST_COUNT': hist_coverage.iloc[0]['hist_count']
                })
            except Exception as e:
                coverage_data.append({
                    'TICKER': ticker,
                    'GVKEY': gvkey,
                    'IID': iid,
                    'HIST_MIN_DATE': None,
                    'HIST_MAX_DATE': None,
                    'HIST_COUNT': 0
                })
        
        return pd.DataFrame(coverage_data)


# Convenience function
def get_historical_returns_manager() -> HistoricalReturnsManager:
    """Get HistoricalReturnsManager instance."""
    return HistoricalReturnsManager()
