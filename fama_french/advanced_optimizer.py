 # advanced_optimizer.py (Final Version with Results Directory)

import pandas as pd
import numpy as np
from scipy.optimize import minimize
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import sys
import os
from datetime import datetime
from duckdb_manager import DuckDBManager
from historical_returns_manager import HistoricalReturnsManager

# ===============================================================
# --- SCRIPT CONFIGURATION ---
# ===============================================================
class Config:
    # --- DuckDB Database Settings ---
    COMPUSTAT_DB_PATH = '/home/tasos/T9_APFS/compustat.duckdb'
    FF_DB_PATH = '/home/tasos/T9_APFS/fama_french.duckdb'
    
    # Table names
    UNIVERSE_SCORES_TABLE = "universe_factor_scores"
    RETURNS_TABLE = "optimization_portfolio_monthly_returns"

    # --- NEW: Output Directory ---
    # This will be created inside your fama_french project directory
    RESULTS_DIR = "results"

    # --- Optimization Parameters ---
    LOOKBACK_MONTHS = 36 

    # --- Portfolio Constraints ---
    MAX_WEIGHT_LONG = 0.02
    MAX_WEIGHT_SHORT = -0.02

# ===============================================================
# --- MODIFIED: FUNCTION TO SAVE PORTFOLIO TO A DEDICATED DIRECTORY ---
# ===============================================================
def save_latest_portfolio(date, optimal_weights_series, long_candidates_df, short_candidates_df):
    """
    Saves the final optimized portfolio and candidates to a 'results' subdirectory.
    """
    print("\n--- Saving Latest Portfolio ---")
    
    # --- Create the results directory if it doesn't exist ---
    # os.makedirs is safe; it won't raise an error if the directory already exists.
    os.makedirs(Config.RESULTS_DIR, exist_ok=True)
    
    # 1. Prepare and save the optimized portfolio
    final_portfolio_df = optimal_weights_series.to_frame(name='OptimalWeight')
    final_portfolio_df = final_portfolio_df.reset_index().rename(columns={'index': 'ticker'})
    final_portfolio_df['position'] = np.where(final_portfolio_df['OptimalWeight'] > 0, 'long', 'short')
    final_portfolio_df = final_portfolio_df.sort_values(by='OptimalWeight', ascending=False, key=abs)
    
    date_str = pd.to_datetime(date).strftime('%Y%m%d')
    
    # --- Construct the full path for the output file ---
    output_filename = f"optimized_portfolio_{date_str}.csv"
    full_output_path = os.path.join(Config.RESULTS_DIR, output_filename)
    
    final_portfolio_df.to_csv(full_output_path, index=False)
    print(f"Successfully saved optimized portfolio to: {full_output_path}")
    
    # 2. Prepare and save the full candidate list
    all_candidates_df = pd.concat([long_candidates_df, short_candidates_df])
    
    # --- Construct the full path for the candidates file ---
    candidates_filename = f"portfolio_candidates_{date_str}.csv"
    full_candidates_path = os.path.join(Config.RESULTS_DIR, candidates_filename)

    all_candidates_df[['ticker', 'company_name', 'decile', 'factor_score']].sort_values('factor_score', ascending=False).to_csv(full_candidates_path, index=False)
    print(f"Successfully saved full candidate list to: {full_candidates_path}")


# ===============================================================
# --- HELPER & OPTIMIZATION FUNCTIONS (UNCHANGED) ---
# ===============================================================
def get_duckdb_manager():
    """Get DuckDB manager instance"""
    return DuckDBManager()

def load_data():
    """Load scored universe and historical returns using the new historical returns manager."""
    print("\n--- Loading Scored Universe and Historical Returns ---")
    duckdb_manager = get_duckdb_manager()
    historical_manager = HistoricalReturnsManager()
    
    # Load scores from DuckDB
    scores_df = duckdb_manager.read_sql(f"SELECT * FROM {Config.UNIVERSE_SCORES_TABLE}", database='ff')
    scores_df['datadate'] = pd.to_datetime(scores_df['datadate'])
    if 'decile' not in scores_df.columns:
        print("CRITICAL: 'decile' column not found. Rerun manual_factors_v3.py.")
        sys.exit(1)
    print(f"Loaded {len(scores_df)} rows from '{Config.UNIVERSE_SCORES_TABLE}'")
    
    # Get all unique tickers from scores
    all_tickers = scores_df['ticker'].unique().tolist()
    print(f"Found {len(all_tickers)} unique tickers in scores data")
    
    # Get unified returns data using historical manager
    print("Loading historical returns data...")
    returns_pivot = historical_manager.get_returns_pivot(all_tickers, '2010-01-01')
    
    if len(returns_pivot) == 0:
        print("CRITICAL: No returns data found. Check historical returns manager.")
        sys.exit(1)
    
    print(f"Loaded {len(returns_pivot)} historical monthly return dates for {returns_pivot.shape[1]} tickers.")
    print(f"Returns date range: {returns_pivot.index.min()} to {returns_pivot.index.max()}")
    
    return scores_df, returns_pivot

def portfolio_variance_objective(weights, cov_matrix):
    return np.dot(weights.T, np.dot(cov_matrix, weights))

# ===============================================================
# --- MAIN BACKTESTING ENGINE (UNCHANGED) ---
# ===============================================================
def run_backtest(scores_df, returns_pivot):
    all_results = []
    latest_successful_date = None
    latest_optimal_weights = None
    latest_long_candidates = None
    latest_short_candidates = None
    
    rebalance_dates = sorted(scores_df['datadate'].unique())
    start_date = pd.to_datetime('2013-01-02')
    
    print(f"\n--- Starting Minimum Variance Backtest for {len(rebalance_dates)} Weeks ---")
    print(f"   (Backtest will start after {start_date.date()} to ensure sufficient lookback history)")

    for i, date in enumerate(rebalance_dates):
        if date < start_date: continue

        print(f"  Processing {i+1}/{len(rebalance_dates)}: {pd.to_datetime(date).date()}", end="")
        
        current_universe = scores_df[scores_df['datadate'] == date]
        if current_universe.empty: print(" -> Skipped (No scores for this date)"); continue

        long_candidates = current_universe[current_universe['decile'].isin([1, 2])]
        short_candidates = current_universe[current_universe['decile'].isin([9, 10])]
        
        optimization_universe_df = pd.concat([long_candidates, short_candidates])
        if long_candidates.empty or short_candidates.empty: print(" -> Skipped (Not enough stocks in long/short deciles)"); continue
        
        tickers = optimization_universe_df['ticker'].tolist()
        
        hist_end_date = date - pd.Timedelta(days=1)
        hist_start_date = hist_end_date - pd.DateOffset(months=Config.LOOKBACK_MONTHS)
        available_tickers = [t for t in tickers if t in returns_pivot.columns]
        historical_returns = returns_pivot.loc[hist_start_date:hist_end_date, available_tickers]
        historical_returns.dropna(axis=1, how='all', inplace=True)
        
        if len(historical_returns.columns) < 20: 
            print(f" -> Skipped (Insufficient historical data: {len(historical_returns.columns)} stocks)"); continue

        final_tickers = historical_returns.columns.tolist()
        cov_matrix = historical_returns.cov()
        num_assets = len(final_tickers)
        
        long_mask = [1 if t in long_candidates['ticker'].values else 0 for t in final_tickers]
        short_mask = [1 if t in short_candidates['ticker'].values else 0 for t in final_tickers]
        bounds = [(0, Config.MAX_WEIGHT_LONG) if m == 1 else (Config.MAX_WEIGHT_SHORT, 0) for m in long_mask]
        constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w * long_mask) - 1.0}, {'type': 'eq', 'fun': lambda w: np.sum(w * short_mask) - (-1.0)}]
        initial_weights = np.zeros(num_assets)

        result = minimize(
            portfolio_variance_objective, initial_weights, args=(cov_matrix,), 
            method='SLSQP', bounds=bounds, constraints=constraints, options={'disp': False}
        )
        
        if result.success:
            optimal_weights = pd.Series(result.x, index=final_tickers)
            forward_returns_for_date = current_universe.set_index('ticker')['5d_forward_return']
            optimized_return = np.sum(optimal_weights * forward_returns_for_date.reindex(optimal_weights.index).fillna(0))
            
            long_leg_benchmark_returns = current_universe[current_universe['decile'] == 1]['5d_forward_return']
            short_leg_benchmark_returns = current_universe[current_universe['decile'] == 10]['5d_forward_return']
            ew_long_return = long_leg_benchmark_returns.mean()
            ew_short_return = short_leg_benchmark_returns.mean()
            ew_ls_return = ew_long_return - ew_short_return
            
            all_results.append({
                'date': date, 'optimized_return': optimized_return,
                'ew_long_return': ew_long_return, 'ew_short_return': ew_short_return,
                'ew_ls_return': ew_ls_return
            })
            
            latest_successful_date = date
            latest_optimal_weights = optimal_weights
            latest_long_candidates = long_candidates
            latest_short_candidates = short_candidates

            print(f" -> Success (MinVar R: {optimized_return:+.4f})")
        else:
            print(f" -> Skipped (Optimization Failed: {result.message})")
            
    if latest_successful_date:
        save_latest_portfolio(
            latest_successful_date, latest_optimal_weights,
            latest_long_candidates, latest_short_candidates
        )
    return pd.DataFrame(all_results)

def analyze_and_plot_performance(results_df):
    if results_df.empty: print("No results to analyze."); return
    print("\n--- Analyzing and Plotting Performance ---")
    results_df = results_df.set_index('date').sort_index().fillna(0)
    results_df['cum_optimized'] = (1 + results_df['optimized_return']).cumprod()
    results_df['cum_ew_long'] = (1 + results_df['ew_long_return']).cumprod()
    results_df['cum_ew_short'] = (1 + results_df['ew_short_return']).cumprod()
    results_df['cum_ew_ls'] = (1 + results_df['ew_ls_return']).cumprod()
    strat_returns = results_df['optimized_return']
    weeks_per_year = 52.1775
    annualized_return = (1 + strat_returns.mean())**weeks_per_year - 1
    annualized_volatility = strat_returns.std() * np.sqrt(weeks_per_year)
    annualized_sharpe = annualized_return / annualized_volatility if annualized_volatility != 0 else 0
    print("\nOptimized Strategy Performance Summary:")
    print(f"  - Annualized Return:    {annualized_return:.2%}")
    print(f"  - Annualized Volatility:  {annualized_volatility:.2%}")
    print(f"  - Annualized Sharpe Ratio:{annualized_sharpe:.2f}")
    plt.style.use('seaborn-v0_8-darkgrid')
    fig, ax = plt.subplots(figsize=(16, 9))
    ax.plot(results_df.index, results_df['cum_optimized'], label='Optimized Min-Variance Portfolio', color='darkviolet', linewidth=2.5, zorder=10)
    ax.plot(results_df.index, results_df['cum_ew_ls'], label='Benchmark L/S (Top vs Bottom Decile)', color='royalblue', linestyle='--', linewidth=1.5)
    ax.plot(results_df.index, results_df['cum_ew_long'], label='Long Book (Top Decile, Equal-Weighted)', color='green', linestyle=':', linewidth=1)
    ax.plot(results_df.index, 1 / results_df['cum_ew_short'], label='Short Book (Bottom Decile, Inverted)', color='darkorange', linestyle=':', linewidth=1)
    ax.set_title('Minimum Variance Long-Short Strategy vs. Benchmarks', fontsize=18, pad=20)
    ax.set_xlabel('Date', fontsize=12); ax.set_ylabel('Cumulative Return (Log Scale)', fontsize=12)
    ax.set_yscale('log'); ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))
    ax.grid(True, which='both', linestyle='--', linewidth=0.5); ax.legend(fontsize=11)
    plt.tight_layout(); plt.show()


if __name__ == "__main__":
    print("--- Starting Minimum Variance Optimizer & Backtest ---")
    scores_df, returns_pivot = load_data()
    results = run_backtest(scores_df, returns_pivot)
    analyze_and_plot_performance(results)
    print("\n--- Optimizer Script Finished ---")