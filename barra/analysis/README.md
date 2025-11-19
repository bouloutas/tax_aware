# Analysis Utilities

- `simple_portfolio_analysis.py`: CLI for computing model risk (factor/specific variance, exposures) and realized tracking error vs. SPY for ad-hoc portfolios.
  - Example (MSFT 30%, XOM 20%, LLY 20%, META 30%):
    ```
    python analysis/simple_portfolio_analysis.py \
      --as-of 2024-09-30 \
      --start 2024-09-30 --end 2024-12-31 \
      --weight MSFT=0.3 --weight XOM=0.2 --weight LLY=0.2 --weight META=0.3 \
      --json-out analysis/simple_portfolio_report.json
    ```
  - Output includes model variance (Barra clone exposures/covariance/specific risk) and realized daily/annual tracking error vs. SPY using `stock_analysis_backtest.daily_data` + `multiasset_new.compustat_data.ETF_DAILY_PRICES`.

Artifacts:
- `analysis/simple_portfolio_report.json`: sample output for the portfolio above (as-of 2024-09-30, TE window 2024-09-30 to 2024-12-31).
