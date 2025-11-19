import pandas as pd
import numpy as np
from scipy.optimize import minimize
import statsmodels.api as sm
import os
from dotenv import load_dotenv
from duckdb_manager import DuckDBManager
import sys
import yfinance as yf # For SPY data, as a fallback or primary source

# ===============================================================
# --- SCRIPT CONFIGURATION ---
# ===============================================================
load_dotenv() # Load environment variables (e.g., for DB credentials)

# DuckDB Database Paths
COMPUSTAT_DB_PATH = '/home/tasos/T9_APFS/compustat.duckdb'
FF_DB_PATH = '/home/tasos/T9_APFS/fama_french.duckdb'

# MySQL Table Names (now used as DuckDB table names)
MYSQL_TABLE_COMBINED_FACTORS = 'final_combined_factors' # Your FF5 factors + RF
MYSQL_TABLE_STOCK_SPY_MONTHLY_RETURNS = 'optimization_portfolio_monthly_returns' # Stock/SPY returns
MYSQL_TABLE_DATA_FOR_FACTOR_CONSTRUCTION = 'data_for_factor_construction' # For dynamic universe ME

# Portfolio Optimization Parameters
BENCHMARK_TICKER = 'SPY'

# --- UNIVERSE SELECTION CONFIGURATION ---
# Option 1: Predefined list of tickers to consider in the portfolio
# Set to a list of strings, e.g., ['AAPL', 'MSFT', 'GOOG']
# If None, dynamic universe selection will be used (if USE_DYNAMIC_UNIVERSE is True)
CANDIDATE_TICKERS_PREDEFINED = ['IBM', 'MSFT', 'XOM', 'BLK', 'CP', 'COST', 'CB', 'BX', 'AZO', 'HHH']

# Option 2: Dynamic universe selection (e.g., top N largest stocks)
USE_DYNAMIC_UNIVERSE = False # Set to True to select top N stocks based on market cap
NUM_STOCKS_IN_OPTIMIZED_PORTFOLIO = 30 # Number of stocks if using dynamic selection (e.g. top 30)
                                   # If USE_DYNAMIC_UNIVERSE is False, CANDIDATE_TICKERS_PREDEFINED is used.
                                   # If CANDIDATE_TICKERS_PREDEFINED is also None and USE_DYNAMIC_UNIVERSE is False, script will exit.

# --- WEIGHT CONSTRAINTS ---
MIN_WEIGHT_PER_STOCK = 0.02  # Minimum weight if a stock is included (e.g., 2%)
MAX_WEIGHT_PER_STOCK = 0.25  # Maximum weight for any single stock (e.g., 25%)

# Historical period for beta estimation and optimization (e.g., trailing N years from latest data point)
OPTIMIZATION_LOOKBACK_YEARS = 5

# --- Helper Functions ---
def get_duckdb_manager():
    """Get DuckDB manager instance"""
    return DuckDBManager()

def calculate_factor_exposures_generic(returns_df, factors_df, id_col='TICKER', return_col='MONTHLY_RETURN', date_col='MONTH_END_DATE'):
    """Generic factor exposure calculation."""
    all_betas = []
    factors_df_renamed = factors_df.rename(columns={'Date': date_col}) # Align date column name

    for asset_id, group_df in returns_df.groupby(id_col):
        merged_df = pd.merge(group_df, factors_df_renamed, on=date_col, how='inner')
        if merged_df.empty or len(merged_df) < 24: # Min observations for regression
            print(f"Skipping beta calculation for {asset_id}: Insufficient data ({len(merged_df)} obs).")
            continue

        # Ensure no NaNs in critical columns before regression
        regression_columns = [return_col, 'RF', 'Mkt_RF', 'SMB', 'HML', 'RMW', 'CMA']
        regression_data = merged_df[regression_columns].dropna()
        
        if len(regression_data) < 24:
            print(f"Skipping beta calculation for {asset_id}: Insufficient non-NaN data ({len(regression_data)} obs).")
            continue

        Y = regression_data[return_col] - regression_data['RF'] # Excess return
        X_cols = ['Mkt_RF', 'SMB', 'HML', 'RMW', 'CMA']
        X = regression_data[X_cols]
        X = sm.add_constant(X) # Adds a constant column for alpha

        try:
            model = sm.OLS(Y.astype(float), X.astype(float)).fit()
            # Create a Series for this asset's betas and other stats
            beta_series = model.params.copy()
            beta_series = beta_series.rename(index={'const': 'alpha'})
            beta_series['r_squared'] = model.rsquared
            beta_series['alpha_t_stat'] = model.tvalues['const'] # T-stat for alpha
            beta_series['alpha_p_value'] = model.pvalues['const'] # P-value for alpha
            beta_series['num_obs'] = int(model.nobs)
            beta_series.name = asset_id # Name the series with the asset ID
            all_betas.append(beta_series)
        except Exception as e:
            print(f"Regression failed for {asset_id}: {e}")

    if not all_betas: return pd.DataFrame()
    return pd.DataFrame(all_betas) # Pandas will use the series names as index


def portfolio_return_series_calc(weights, stock_returns_matrix):
    """Calculates the time series of portfolio returns."""
    return np.dot(stock_returns_matrix, weights)

def tracking_error_squared_objective(weights, stock_returns_matrix, benchmark_returns_series):
    """Objective function: Minimize the squared tracking error."""
    portfolio_rets_ts = pd.Series(portfolio_return_series_calc(weights, stock_returns_matrix), index=stock_returns_matrix.index)
    
    # Align by index (dates) and keep only common dates
    aligned_benchmark, aligned_portfolio = benchmark_returns_series.align(portfolio_rets_ts, join='inner')
    
    if len(aligned_benchmark) < 2: return np.inf # Not enough data to calculate variance
    
    error_series = aligned_portfolio - aligned_benchmark
    return np.var(error_series)

# In portfolio_optimization.py

def optimize_portfolio_to_track_benchmark(
    candidate_tickers_list, # List of tickers to consider
    benchmark_ticker_symbol,
    period_monthly_returns_df, # DataFrame already filtered for the optimization period
    min_weight,
    max_weight
    # lookback_years argument removed
):
    print(f"\n--- Optimizing portfolio from {len(candidate_tickers_list)} candidates to track {benchmark_ticker_symbol} ---")
    print(f"Constraints: Min weight per stock = {min_weight*100:.2f}%, Max weight = {max_weight*100:.2f}%")
    # lookback_years print statement removed as it's implicit in period_monthly_returns_df

    benchmark_rets_series = period_monthly_returns_df[period_monthly_returns_df['TICKER'] == benchmark_ticker_symbol] \
        .set_index('MONTH_END_DATE')['MONTHLY_RETURN'].sort_index()
    
    candidate_returns_pivot = period_monthly_returns_df[period_monthly_returns_df['TICKER'].isin(candidate_tickers_list)] \
        .pivot_table(index='MONTH_END_DATE', columns='TICKER', values='MONTHLY_RETURN') \
        .sort_index()
    
    actual_candidates_with_data = [tick for tick in candidate_tickers_list if tick in candidate_returns_pivot.columns]
    candidate_returns_pivot = candidate_returns_pivot.reindex(columns=actual_candidates_with_data)

    # ... rest of the function remains the same, using candidate_returns_pivot and benchmark_rets_series ...

    if not actual_candidates_with_data:
        print("No candidate stocks have data for the selected period.")
        return None
    
    print(f"Actual candidates with data in period: {actual_candidates_with_data}")

    # Align dates: only use dates where benchmark and ALL candidate stocks have non-NaN returns
    # Start by dropping any all-NaN rows from candidates (can happen if all start late)
    candidate_returns_pivot.dropna(axis=0, how='all', inplace=True)
    common_dates = benchmark_rets_series.dropna().index.intersection(candidate_returns_pivot.index)
    
    benchmark_rets_aligned = benchmark_rets_series.loc[common_dates]
    # For candidates, first select common dates, then drop rows where ANY stock has NaN
    candidate_returns_aligned = candidate_returns_pivot.loc[common_dates].dropna(axis=0, how='any')
    
    # Re-align benchmark returns to the final set of common dates from candidate_returns_aligned
    benchmark_rets_aligned = benchmark_rets_aligned.reindex(candidate_returns_aligned.index)

    if benchmark_rets_aligned.empty or candidate_returns_aligned.empty or len(candidate_returns_aligned) < 12: # Min e.g. 1 year
        print(f"Not enough common historical data after aligning and dropping NaNs (need at least 12 months, got {len(candidate_returns_aligned)}).")
        return None

    n_assets = candidate_returns_aligned.shape[1]
    final_candidate_tickers = candidate_returns_aligned.columns.tolist() # These are the stocks actually used

    if n_assets == 0:
        print("No assets remaining after data alignment for optimization.")
        return None
        
    if n_assets * min_weight > 1.0001: # Add small tolerance for float precision
        print(f"Error: Minimum weight constraint ({min_weight*100:.2f}% per stock for {n_assets} stocks = {n_assets * min_weight * 100:.2f}%) exceeds 100%.")
        return None
        
    initial_weights = np.array([1.0 / n_assets] * n_assets)
    constraints = ({'type': 'eq', 'fun': lambda weights: np.sum(weights) - 1.0})
    bounds = tuple((min_weight, max_weight) for _ in range(n_assets))

    print(f"Optimizing weights for {n_assets} assets: {final_candidate_tickers}")
    optimization_result = minimize(
        tracking_error_squared_objective,
        initial_weights,
        args=(candidate_returns_aligned, benchmark_rets_aligned),
        method='SLSQP',
        bounds=bounds,
        constraints=constraints,
        options={'disp': False, 'ftol': 1e-10, 'maxiter': 1000}
    )

    if optimization_result.success:
        optimal_weights = optimization_result.x
        portfolio_df = pd.DataFrame({
            'Ticker': final_candidate_tickers,
            'OptimalWeight': optimal_weights
        })
        # Filter very small weights that are practically zero but satisfy bounds due to precision
        portfolio_df = portfolio_df[portfolio_df['OptimalWeight'] >= (min_weight - 1e-5)]
        if not portfolio_df.empty and portfolio_df['OptimalWeight'].sum() > 0 :
             portfolio_df['OptimalWeight'] = portfolio_df['OptimalWeight'] / portfolio_df['OptimalWeight'].sum() # Re-normalize
        return portfolio_df
    else:
        print(f"Optimization failed: {optimization_result.message}")
        return None

# --- Main Script Execution ---
# In portfolio_optimization.py

# ... (Keep all imports and helper functions from the previous full script) ...
# get_mysql_engine, calculate_factor_exposures_generic,
# portfolio_return_series_calc, tracking_error_squared_objective,
# optimize_portfolio_to_track_benchmark

# --- Main Script Execution ---
def main():
    print("--- Starting Portfolio Optimization Script ---")
    duckdb_manager = get_duckdb_manager()

    # 1. Load Fama-French Factors
    try:
        print(f"Loading Fama-French factors from DuckDB table: {MYSQL_TABLE_COMBINED_FACTORS}")
        factors_df = duckdb_manager.read_sql(f"SELECT * FROM {MYSQL_TABLE_COMBINED_FACTORS}", database='ff')
        factors_df['Date'] = pd.to_datetime(factors_df['Date'])
        factors_df.set_index('Date', inplace=True) # Set Date as index for easy alignment
        print(f"Loaded {len(factors_df)} rows of factor data from {factors_df.index.min()} to {factors_df.index.max()}.")
        if factors_df.empty: raise ValueError("Factor data is empty.")
    except Exception as e:
        print(f"CRITICAL: Error loading Fama-French factors: {e}"); sys.exit(1)

    # 2. Load Monthly Stock and SPY Returns
    try:
        print(f"Loading stock and SPY monthly returns from DuckDB: {MYSQL_TABLE_STOCK_SPY_MONTHLY_RETURNS}")
        all_monthly_returns_df = duckdb_manager.read_sql(f"SELECT TICKER, MONTH_END_DATE, GVKEY, MONTHLY_RETURN FROM {MYSQL_TABLE_STOCK_SPY_MONTHLY_RETURNS}", database='ff')
        all_monthly_returns_df['MONTH_END_DATE'] = pd.to_datetime(all_monthly_returns_df['MONTH_END_DATE'])
        print(f"Loaded {len(all_monthly_returns_df)} rows of monthly stock/SPY returns.")
        if all_monthly_returns_df.empty: raise ValueError("Stock/SPY returns data is empty.")
    except Exception as e:
        print(f"CRITICAL: Error loading monthly stock/SPY returns: {e}"); sys.exit(1)

    # 3. Determine Candidate Tickers for Optimization
    final_candidate_tickers = []
    if USE_DYNAMIC_UNIVERSE:
        print(f"Selecting Top {NUM_STOCKS_IN_OPTIMIZED_PORTFOLIO} stocks for dynamic universe...")
        try:
            latest_formation_year_query = f"SELECT MAX(FORMATION_YEAR_T) as max_year FROM {MYSQL_TABLE_DATA_FOR_FACTOR_CONSTRUCTION}"
            latest_year_result = duckdb_manager.read_sql(latest_formation_year_query, database='compustat').iloc[0]
            latest_formation_year = latest_year_result['max_year']
            if latest_formation_year is None: raise ValueError("Could not determine latest formation year.")

            top_stocks_gvkey_query = f"""
            SELECT DISTINCT GVKEY 
            FROM {MYSQL_TABLE_DATA_FOR_FACTOR_CONSTRUCTION}
            WHERE FORMATION_YEAR_T = {int(latest_formation_year)}
            ORDER BY ME_JUNE DESC
            LIMIT {NUM_STOCKS_IN_OPTIMIZED_PORTFOLIO};
            """
            top_stocks_gvkey_df = duckdb_manager.read_sql(top_stocks_gvkey_query, database='compustat')
            if top_stocks_gvkey_df.empty:
                raise ValueError(f"Could not fetch top {NUM_STOCKS_IN_OPTIMIZED_PORTFOLIO} GVKEYs.")
            
            universe_gvkeys = top_stocks_gvkey_df['GVKEY'].tolist()
            # Map GVKEYs to Tickers present in the returns data
            final_candidate_tickers = all_monthly_returns_df[all_monthly_returns_df['GVKEY'].isin(universe_gvkeys)]['TICKER'].unique().tolist()
            
            if len(final_candidate_tickers) < NUM_STOCKS_IN_OPTIMIZED_PORTFOLIO * 0.8: # Heuristic check
                print(f"Warning: Found only {len(final_candidate_tickers)} tickers for {len(universe_gvkeys)} selected GVKEYs.")
            if not final_candidate_tickers:
                 raise ValueError("Dynamic universe selection resulted in no usable tickers.")
            print(f"Dynamically selected candidate tickers: {final_candidate_tickers}")

        except Exception as e:
            print(f"Error selecting dynamic universe: {e}.")
            if CANDIDATE_TICKERS_PREDEFINED:
                print(f"Falling back to predefined tickers: {CANDIDATE_TICKERS_PREDEFINED}")
                final_candidate_tickers = CANDIDATE_TICKERS_PREDEFINED
            else:
                print("CRITICAL: Dynamic universe failed and no predefined tickers. Exiting."); sys.exit(1)
    else: # Use predefined tickers
        if CANDIDATE_TICKERS_PREDEFINED:
            final_candidate_tickers = CANDIDATE_TICKERS_PREDEFINED
            print(f"Using predefined candidate tickers: {final_candidate_tickers}")
        else:
            print("CRITICAL: USE_DYNAMIC_UNIVERSE is False and CANDIDATE_TICKERS_PREDEFINED is None. Exiting.")
            sys.exit(1)

    if not final_candidate_tickers: # Should be caught by logic above, but safety check
        print("No candidate tickers available for optimization. Exiting."); sys.exit(1)

    # Ensure BENCHMARK_TICKER has returns data
    if BENCHMARK_TICKER not in all_monthly_returns_df['TICKER'].unique():
        print(f"Benchmark ticker {BENCHMARK_TICKER} not found in loaded DuckDB returns. Attempting yfinance...")
        try:
            spy_data_yf = yf.download(BENCHMARK_TICKER,
                                      start=all_monthly_returns_df['MONTH_END_DATE'].min() - pd.DateOffset(months=1),
                                      end=all_monthly_returns_df['MONTH_END_DATE'].max() + pd.DateOffset(months=1),
                                      interval="1mo", progress=False)
            if not spy_data_yf.empty:
                spy_rets_yf = spy_data_yf[['Adj Close']].pct_change().dropna().reset_index()
                spy_rets_yf.rename(columns={'Adj Close': 'MONTHLY_RETURN', 'Date': 'OrigDate'}, inplace=True)
                spy_rets_yf['MONTH_END_DATE'] = pd.to_datetime(spy_rets_yf['OrigDate']) + pd.offsets.MonthEnd(0)
                spy_rets_yf['TICKER'] = BENCHMARK_TICKER
                spy_rets_yf['GVKEY'] = 'SPY_GVKEY' # Dummy GVKEY
                all_monthly_returns_df = pd.concat([all_monthly_returns_df, spy_rets_yf[['TICKER', 'MONTH_END_DATE', 'GVKEY', 'MONTHLY_RETURN']]], ignore_index=True).drop_duplicates(subset=['TICKER', 'MONTH_END_DATE'], keep='last')
                print(f"Fetched and appended {BENCHMARK_TICKER} returns from yfinance.")
            else: raise ValueError("yfinance fetch for SPY returned no data.")
        except Exception as e_yf:
            print(f"CRITICAL: Failed to fetch {BENCHMARK_TICKER} data via yfinance: {e_yf}. Exiting."); sys.exit(1)

    # 4. Filter returns for the optimization period and selected tickers
    tickers_for_optimization_run = final_candidate_tickers + [BENCHMARK_TICKER]
    max_date_available = all_monthly_returns_df['MONTH_END_DATE'].max()
    opt_end_date = max_date_available
    opt_start_date = opt_end_date - pd.DateOffset(years=OPTIMIZATION_LOOKBACK_YEARS) + pd.DateOffset(days=1)
    opt_start_date = opt_start_date - pd.offsets.MonthBegin(1)

    optimization_period_returns_df = all_monthly_returns_df[
        (all_monthly_returns_df['MONTH_END_DATE'] >= opt_start_date) &
        (all_monthly_returns_df['MONTH_END_DATE'] <= opt_end_date) &
        (all_monthly_returns_df['TICKER'].isin(tickers_for_optimization_run))
    ].copy()

    if optimization_period_returns_df.empty:
        print("No returns data available for the specified optimization period and tickers. Exiting."); sys.exit(1)

    # 5. Perform Optimization
    optimized_portfolio_df = optimize_portfolio_to_track_benchmark(
        final_candidate_tickers, # Only pass the actual candidates, not the benchmark
        BENCHMARK_TICKER,
        optimization_period_returns_df, # Pass the period-filtered returns
        MIN_WEIGHT_PER_STOCK,
        MAX_WEIGHT_PER_STOCK
    )

    if optimized_portfolio_df is not None and not optimized_portfolio_df.empty:
        print("\n--- Optimized Portfolio Weights ---")
        print(optimized_portfolio_df.sort_values(by='OptimalWeight', ascending=False))
        print(f"Sum of weights: {optimized_portfolio_df['OptimalWeight'].sum():.6f}")

        # Post-Optimization Analysis
        portfolio_tickers_in_optimized_solution = optimized_portfolio_df['Ticker'].tolist()
        
        analysis_returns_df = optimization_period_returns_df[ # Use already period-filtered data
            optimization_period_returns_df['TICKER'].isin(portfolio_tickers_in_optimized_solution + [BENCHMARK_TICKER])
        ].copy()

        portfolio_returns_matrix = analysis_returns_df[analysis_returns_df['TICKER'].isin(portfolio_tickers_in_optimized_solution)] \
            .pivot_table(index='MONTH_END_DATE', columns='TICKER', values='MONTHLY_RETURN') \
            .reindex(columns=portfolio_tickers_in_optimized_solution).fillna(0)

        benchmark_rets_series_for_analysis = analysis_returns_df[analysis_returns_df['TICKER'] == BENCHMARK_TICKER].set_index('MONTH_END_DATE')['MONTHLY_RETURN']
        
        common_idx_final = portfolio_returns_matrix.index.intersection(benchmark_rets_series_for_analysis.index)
        portfolio_returns_matrix_aligned = portfolio_returns_matrix.loc[common_idx_final]
        benchmark_rets_series_aligned = benchmark_rets_series_for_analysis.loc[common_idx_final]

        if not portfolio_returns_matrix_aligned.empty and not benchmark_rets_series_aligned.empty:
            final_weights_for_analysis = optimized_portfolio_df.set_index('Ticker')['OptimalWeight'].reindex(portfolio_returns_matrix_aligned.columns).fillna(0).values
            optimized_portfolio_return_ts = portfolio_return_series_calc(final_weights_for_analysis, portfolio_returns_matrix_aligned)
            
            tracking_error_sq_val = tracking_error_squared_objective(final_weights_for_analysis, portfolio_returns_matrix_aligned, benchmark_rets_series_aligned)
            annualized_tracking_error = np.sqrt(tracking_error_sq_val * 12) 
            print(f"\nAnnualized Tracking Error of Optimized Portfolio (In-Sample): {annualized_tracking_error:.4%}")

            # Factor exposure analysis on the common analysis period
            factors_for_exposure_calc = factors_df.loc[common_idx_final.min():common_idx_final.max()].copy()
            if factors_for_exposure_calc.empty : print("Warning: No overlapping factor data for exposure calculation period.")

            optimized_portfolio_ts_df = pd.DataFrame({
                'TICKER': 'OPTIMIZED_PORTFOLIO',
                'MONTH_END_DATE': common_idx_final,
                'MONTHLY_RETURN': optimized_portfolio_return_ts
            })
            optimized_portfolio_betas_df = calculate_factor_exposures_generic(optimized_portfolio_ts_df, factors_for_exposure_calc.reset_index()) # Pass factors with 'Date' column
            print("\nOptimized Portfolio Factor Exposures:")
            if not optimized_portfolio_betas_df.empty: print(optimized_portfolio_betas_df[['alpha', 'Mkt_RF', 'SMB', 'HML', 'RMW', 'CMA', 'r_squared']])
            else: print("Could not calculate optimized portfolio betas.")
            
            spy_returns_for_beta_df = benchmark_rets_series_aligned.reset_index()
            spy_returns_for_beta_df['TICKER'] = BENCHMARK_TICKER
            spy_betas_df = calculate_factor_exposures_generic(spy_returns_for_beta_df, factors_for_exposure_calc.reset_index())
            print(f"\n{BENCHMARK_TICKER} Factor Exposures (for same period):")
            if not spy_betas_df.empty: print(spy_betas_df[['alpha', 'Mkt_RF', 'SMB', 'HML', 'RMW', 'CMA', 'r_squared']])
            else: print(f"Could not calculate {BENCHMARK_TICKER} betas.")
    else:
        print("Could not generate an optimized portfolio with the current settings and data.")

    print("\n--- Portfolio Optimization Script Finished ---")

if __name__ == "__main__":
    main()