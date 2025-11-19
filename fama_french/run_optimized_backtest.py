# run_optimized_backtest.py

import pandas as pd
import numpy as np
from scipy.optimize import minimize
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import subprocess
import sys
import os
from duckdb_manager import DuckDBManager

# ===============================================================
# --- SCRIPT CONFIGURATION ---
# ===============================================================
class Config:
    # --- Paths to Prerequisite Scripts ---
    # Path to the Fama-French data refresh pipeline
    FF_PIPELINE_SCRIPT_PATH = "/home/tasos/tax_aware/fama_french/run_FF_daily.sh"
    # Path to your custom factor scoring pipeline
    FACTOR_SCORING_SCRIPT_PATH = "/home/tasos/snowflake/spglobal/backtest/production/manual_factors_v3.py"

    # --- DuckDB Database Settings ---
    COMPUSTAT_DB_PATH = '/home/tasos/T9_APFS/compustat.duckdb'
    FF_DB_PATH = '/home/tasos/T9_APFS/fama_french.duckdb'
    
    # Table names
    UNIVERSE_SCORES_TABLE = "universe_factor_scores"
    RETURNS_TABLE = "optimization_portfolio_monthly_returns"

    # --- Optimization Parameters ---
    NUM_LONG_STOCKS = 75
    NUM_SHORT_STOCKS = 75
    
    # Historical lookback for calculating the covariance matrix
    LOOKBACK_MONTHS = 36 

    # The risk aversion parameter (lambda). A key tuning parameter.
    # Higher value = more penalty on risk (variance).
    # Lower value = more focus on maximizing score.
    LAMBDA_RISK_AVERSION = 0.5 

    # --- Portfolio Constraints ---
    MAX_WEIGHT_LONG = 0.02   # Max +2% weight for any long stock
    MAX_WEIGHT_SHORT = -0.02 # Max -2% weight (abs) for any short stock


# ===============================================================
# --- HELPER FUNCTIONS ---
# ===============================================================

def run_prerequisite_scripts():
    """Runs all prerequisite data pipeline scripts."""
    print("--- STEP 1: Refreshing All Prerequisite Data ---")
    
    # NOTE: The order matters here. We run the FF pipeline first, as other
    # scripts in your workflow might depend on its output (e.g. returns data).
    scripts_to_run = {
        "Fama-French Pipeline": Config.FF_PIPELINE_SCRIPT_PATH,
        "Factor Scoring Pipeline": Config.FACTOR_SCORING_SCRIPT_PATH,
    }
    
    for name, script_path in scripts_to_run.items():
        print(f"\n--- Running: {name} ---")
        if not os.path.exists(script_path):
            print(f"CRITICAL: Script not found at {script_path}. Please check the path.")
            sys.exit(1)
            
        try:
            command = []
            script_dir = os.path.dirname(script_path)
            script_filename = os.path.basename(script_path)

            if script_path.endswith('.sh'):
                # Run shell scripts with bash for Linux environment
                command = ['bash', script_path]
            elif script_path.endswith('.zsh'):
                # Legacy zsh support if any scripts remain
                command = ['zsh', script_path]
            elif script_path.endswith('.py'):
                # Run python scripts via conda run
                command = ['conda', 'run', '-n', 'dgx-spark', 'python', script_path]

            if not command:
                print(f"CRITICAL: Don't know how to run script: {script_path}")
                sys.exit(1)

            # We run the script from within its own directory to ensure
            # it can find any relative paths it might use.
            process = subprocess.run(
                command, 
                check=True, 
                capture_output=True, 
                text=True,
                timeout=1200, # 20 minute timeout
                cwd=script_dir # <-- IMPORTANT: Run script in its own directory
            )
            print(f"'{name}' script completed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"CRITICAL: Script '{name}' failed with exit code {e.returncode}.")
            print("--- STDOUT ---\n" + e.stdout)
            print("--- STDERR ---\n" + e.stderr)
            sys.exit(1)
        except subprocess.TimeoutExpired as e:
            print(f"CRITICAL: Script '{name}' timed out.")
            print("--- STDOUT ---\n" + e.stdout)
            sys.exit(1)


def get_duckdb_manager():
    """Get DuckDB manager instance"""
    return DuckDBManager()

def load_data():
    print("\n--- Loading Scored Universe and Historical Returns ---")
    duckdb_manager = get_duckdb_manager()
    
    # Load scores from DuckDB
    scores_df = duckdb_manager.read_sql(f"SELECT * FROM {Config.UNIVERSE_SCORES_TABLE}", database='ff')
    scores_df['datadate'] = pd.to_datetime(scores_df['datadate'])
    print(f"Loaded {len(scores_df)} rows from '{Config.UNIVERSE_SCORES_TABLE}'")

    # Load returns from DuckDB
    returns_df = duckdb_manager.read_sql(
        f"SELECT TICKER, MONTH_END_DATE, MONTHLY_RETURN FROM {Config.RETURNS_TABLE}", 
        database='ff'
    )
    returns_df['MONTH_END_DATE'] = pd.to_datetime(returns_df['MONTH_END_DATE'])
    returns_pivot = returns_df.pivot(index='MONTH_END_DATE', columns='TICKER', values='MONTHLY_RETURN')
    print(f"Loaded {len(returns_df)} historical monthly return rows for {returns_df['TICKER'].nunique()} tickers.")
    
    return scores_df, returns_pivot

def portfolio_objective_function(weights, factor_scores, cov_matrix, lambda_risk):
    # This function is unchanged
    portfolio_variance = np.dot(weights.T, np.dot(cov_matrix, weights))
    portfolio_score = np.dot(weights, factor_scores)
    return (lambda_risk * portfolio_variance) - portfolio_score

# ===============================================================
# --- MAIN BACKTESTING ENGINE ---
# ===============================================================
def run_backtest(scores_df, returns_pivot):
    # This entire function is unchanged from our previous discussion
    all_results = []
    rebalance_dates = sorted(scores_df['datadate'].unique())
    
    print(f"\n--- STEP 3: Starting Optimization Backtest for {len(rebalance_dates)} Weeks ---")

    for i, date in enumerate(rebalance_dates):
        print(f"  Processing {i+1}/{len(rebalance_dates)}: {pd.to_datetime(date).date()}", end="")
        
        current_universe = scores_df[scores_df['datadate'] == date].sort_values('factor_score', ascending=False)
        if current_universe.empty:
            print(" -> Skipped (No scores for this date)")
            continue
            
        long_candidates = current_universe.head(Config.NUM_LONG_STOCKS)
        short_candidates = current_universe.tail(Config.NUM_SHORT_STOCKS)
        
        optimization_universe_df = pd.concat([long_candidates, short_candidates])
        if len(optimization_universe_df) < (Config.NUM_LONG_STOCKS + Config.NUM_SHORT_STOCKS):
            print(" -> Skipped (Not enough stocks for full long/short universe)")
            continue
        
        tickers = optimization_universe_df['ticker'].tolist()
        
        hist_end_date = date - pd.Timedelta(days=1)
        hist_start_date = hist_end_date - pd.DateOffset(months=Config.LOOKBACK_MONTHS)
        
        available_tickers = [t for t in tickers if t in returns_pivot.columns]
        historical_returns = returns_pivot.loc[hist_start_date:hist_end_date, available_tickers]
        historical_returns.dropna(axis=1, how='all', inplace=True)
        
        if len(historical_returns) < 12 or len(historical_returns.columns) < (Config.NUM_LONG_STOCKS + Config.NUM_SHORT_STOCKS) * 0.8:
            print(f" -> Skipped (Insufficient historical data: {len(historical_returns.columns)} stocks)")
            continue

        final_tickers = historical_returns.columns.tolist()
        cov_matrix = historical_returns.cov()
        
        aligned_scores_df = optimization_universe_df.set_index('ticker').loc[final_tickers]
        aligned_factor_scores = aligned_scores_df['factor_score'].values
        
        num_assets = len(final_tickers)
        
        bounds = []
        for ticker in final_tickers:
            if ticker in long_candidates['ticker'].values:
                bounds.append((0, Config.MAX_WEIGHT_LONG))
            else:
                bounds.append((Config.MAX_WEIGHT_SHORT, 0))
        
        constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w)}]
        initial_weights = np.zeros(num_assets)

        result = minimize(
            portfolio_objective_function, initial_weights,
            args=(aligned_factor_scores, cov_matrix, Config.LAMBDA_RISK_AVERSION),
            method='SLSQP', bounds=bounds, constraints=constraints, options={'disp': False}
        )
        
        if result.success:
            optimal_weights = pd.Series(result.x, index=final_tickers)
            forward_returns = aligned_scores_df['5d_forward_return']
            optimized_return = np.sum(optimal_weights * forward_returns.fillna(0))
            
            ew_long_return = long_candidates['5d_forward_return'].mean()
            ew_short_return = short_candidates['5d_forward_return'].mean()
            ew_ls_return = ew_long_return - ew_short_return
            
            all_results.append({
                'date': date, 'optimized_return': optimized_return,
                'ew_long_return': ew_long_return, 'ew_short_return': ew_short_return,
                'ew_ls_return': ew_ls_return
            })
            print(f" -> Success (Opt R: {optimized_return:+.4f})")
        else:
            print(f" -> Skipped (Optimization Failed: {result.message})")
            
    return pd.DataFrame(all_results)

def analyze_and_plot_performance(results_df):
    # This function is unchanged
    if results_df.empty:
        print("No results to analyze.")
        return

    print("\n--- STEP 4: Analyzing and Plotting Performance ---")
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
    
    ax.plot(results_df.index, results_df['cum_optimized'], label='Optimized Long-Short Portfolio', color='crimson', linewidth=2.5, zorder=10)
    ax.plot(results_df.index, results_df['cum_ew_ls'], label='Benchmark L/S (Equal-Weighted)', color='royalblue', linestyle='--', linewidth=1.5)
    ax.plot(results_df.index, results_df['cum_ew_long'], label='Long Book (Equal-Weighted)', color='green', linestyle=':', linewidth=1)
    ax.plot(results_df.index, 1 / results_df['cum_ew_short'], label='Short Book (Equal-Weighted, Inverted)', color='darkorange', linestyle=':', linewidth=1)

    ax.set_title('Optimized Long-Short Strategy vs. Benchmarks', fontsize=18, pad=20)
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Cumulative Return (Log Scale)', fontsize=12)
    ax.set_yscale('log')
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    ax.legend(fontsize=11)
    
    plt.tight_layout()
    plt.show()


# ===============================================================
# --- SCRIPT EXECUTION ---
# ===============================================================
if __name__ == "__main__":
    # The new master execution flow
    run_prerequisite_scripts()
    
    scores_df, returns_pivot = load_data()
    results = run_backtest(scores_df, returns_pivot)
    analyze_and_plot_performance(results)
    
    print("\n--- Master Backtest Script Finished ---")
