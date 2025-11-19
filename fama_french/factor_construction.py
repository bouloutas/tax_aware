import pandas as pd
import numpy as np
import requests
import io
import zipfile # For handling zipped Ken French files
import statsmodels.api as sm # For OLS regression
import os
from dotenv import load_dotenv
from duckdb_manager import DuckDBManager

# --- Configuration ---
load_dotenv() # Load environment variables (e.g., for Ken French URL if you parametrize it)

# DuckDB Database Paths
COMPUSTAT_DB_PATH = '/home/tasos/T9_APFS/compustat.duckdb'
FF_DB_PATH = '/home/tasos/T9_APFS/fama_french.duckdb'

# Table Names
MYSQL_TABLE_FACTORDATA = 'data_for_factor_construction' # Table with prepped data
MYSQL_TABLE_KENFRENCH = 'ken_french_factors' # New table to store downloaded FF factors
MYSQL_TABLE_MYFACTORS = 'my_constructed_factors' # New table for your constructed factors
MYSQL_TABLE_FINAL_COMBINED_FACTORS = 'final_combined_factors' # Final combined factors table

KEN_FRENCH_URL_5_FACTOR_MONTHLY_CSV_ZIP = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_CSV.zip"

# --- DuckDB Connection ---
def get_duckdb_manager():
    """Get DuckDB manager instance"""
    return DuckDBManager()

def create_ken_french_factors_table(duckdb_manager):
    """Create Ken French factors table in DuckDB"""
    table_name = MYSQL_TABLE_KENFRENCH
    try:
        print(f"Ensuring DuckDB table '{table_name}' exists...")
        duckdb_manager.create_schema('ff')
        print(f"DuckDB Table '{table_name}' ensured to exist.")
    except Exception as err:
        print(f"Error creating DuckDB table '{table_name}': {err}")

def store_df_to_duckdb(df, table_name, duckdb_manager, database='ff', if_exists_action='replace'):
    """Generic function to store a DataFrame to DuckDB."""
    try:
        duckdb_manager.write_dataframe(df, table_name, database, if_exists_action)
        print(f"DataFrame successfully stored to DuckDB table '{table_name}' with action '{if_exists_action}'.")
    except Exception as e:
        print(f"Error storing DataFrame to DuckDB table '{table_name}': {e}")


# --- Ken French Data Download --- (Slightly refined parsing)
def download_and_parse_ken_french_factors(url):
    print(f"Downloading Ken French factors from {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))

        print("Files found in the downloaded zip archive:")
        for name_in_zip in zip_file.namelist():
            print(f"- {name_in_zip}")

        csv_file_name = ""
        for name in zip_file.namelist():
            if 'F-F_Research_Data_5_Factors_2x3' in name and \
               '.CSV' in name.upper() and \
               'Daily' not in name and \
               'Weekly' not in name:
                csv_file_name = name
                print(f"Selected CSV file from zip: {csv_file_name}")
                break
        
        if not csv_file_name: # Fallback
            for name in zip_file.namelist():
                 if '5_Factors_2x3' in name and '.CSV' in name.upper() and 'Daily' not in name and 'Weekly' not in name:
                    csv_file_name = name
                    print(f"Fallback: Selected CSV file from zip: {csv_file_name}")
                    break

        if not csv_file_name:
            raise ValueError("Could not find the expected monthly 5 factor CSV file in the zip. Check printed file list above.")

        csv_data = zip_file.read(csv_file_name).decode('utf-8')
        lines = csv_data.splitlines()
        
        data_rows = []
        header_found = False
        # Iterate through lines to find the header and then collect data rows
        # until we hit a non-data line (like the start of 'Annual Factors' or copyright)
        for i, line in enumerate(lines):
            line_stripped = line.strip()
            if not line_stripped: # Skip empty lines
                continue

            if not header_found:
                # A common header pattern for the monthly data
                if "Mkt-RF" in line_stripped and "SMB" in line_stripped and "HML" in line_stripped:
                    header_found = True
                    print(f"Header for monthly data found at line index {i}: {line_stripped}")
                    # The next line should be the first data line
                    continue # Move to next line to start collecting data
            
            if header_found:
                # Stop if we hit a line that indicates end of monthly data or start of another section
                if "Annual Factors" in line_stripped or \
                   "Copyright" in line_stripped or \
                   (line_stripped and not line_stripped.split(',')[0].strip().isdigit()): # If first col is not a digit
                    print(f"End of monthly data section detected at line index {i}: {line_stripped}")
                    break
                
                # If it looks like a data line (first column is typically YYYYMM)
                parts = [p.strip() for p in line_stripped.split(',')]
                if parts and parts[0].isdigit() and (len(parts[0]) == 6 or len(parts[0]) == 4): # YYYYMM or YYYY
                    data_rows.append(line_stripped)
                else:
                    # If we found the header but this line doesn't look like data,
                    # it might be an empty line between data or end of data block
                    if data_rows: # If we've already collected some data, assume this is the end
                        print(f"Suspected end of monthly data (non-digit start) at line index {i}: {line_stripped}")
                        break
        
        if not data_rows:
            raise ValueError("No data rows collected. Check header detection and data section in Ken French file.")

        print(f"Collected {len(data_rows)} data rows for monthly factors.")

        df = pd.read_csv(io.StringIO("\n".join(data_rows)), header=None, na_values=["-99.99", "-999", " "])
        
        if df.shape[1] < 7:
            raise ValueError(f"Expected at least 7 columns in Ken French data, found {df.shape[1]}. File content:\n" + "\n".join(data_rows[:5]))

        # Assign columns based on the standard 5-factor + RF structure
        # If there are more columns, they will be named 'extra_...'
        num_expected_cols = 7
        column_names = ['DateStr', 'Mkt_RF', 'SMB', 'HML', 'RMW', 'CMA', 'RF']
        if df.shape[1] > num_expected_cols:
            column_names.extend([f'extra_{j}' for j in range(df.shape[1] - num_expected_cols)])
        elif df.shape[1] < num_expected_cols: # Should be caught by the check above
            pass # Already raised error

        df.columns = column_names[:df.shape[1]] # Only assign names for existing columns

        df['Date'] = pd.to_datetime(df['DateStr'].astype(str).str.strip(), format='%Y%m') + pd.offsets.MonthEnd(0)
        factor_cols = ['Mkt_RF', 'SMB', 'HML', 'RMW', 'CMA', 'RF']
        for col in factor_cols:
            if col in df.columns:
                 df[col] = pd.to_numeric(df[col], errors='coerce') / 100.0
            else:
                raise ValueError(f"Expected factor column '{col}' not found in parsed DataFrame.")
        
        df = df.dropna(subset=factor_cols).reset_index(drop=True)
        df = df[['Date'] + factor_cols]
        print(f"Ken French factors downloaded and parsed successfully. {len(df)} rows.")
        return df
    except requests.exceptions.RequestException as e_req:
        print(f"Error during HTTP request for Ken French factors: {e_req}")
    except zipfile.BadZipFile as e_zip:
        print(f"Error: Downloaded file is not a valid zip file or is corrupted: {e_zip}")
    except ValueError as e_val:
        print(f"Error parsing Ken French factors (ValueError): {e_val}")
    except Exception as e_gen:
        print(f"An unexpected error occurred while processing Ken French factors: {e_gen}")
    return pd.DataFrame()


# --- Factor Construction (using functions from previous responses) ---
def get_data_for_formation_year(duckdb_manager, formation_year):
    query = f"""
    SELECT
        GVKEY, IID, RETURN_MONTH_END_DATE, MONTHLY_RETURN, EXCHG,
        ME_JUNE, BE_FY_T_MINUS_1, OP_FY_T_MINUS_1, INV_FY_T_MINUS_1, BM_T_MINUS_1
    FROM {MYSQL_TABLE_FACTORDATA}
    WHERE FORMATION_YEAR_T = {formation_year};
    """
    print(f"Fetching data for formation year: {formation_year} from DuckDB...")
    try:
        df = duckdb_manager.read_sql(query, database='compustat')
        df['RETURN_MONTH_END_DATE'] = pd.to_datetime(df['RETURN_MONTH_END_DATE'])
        print(f"Fetched {len(df)} rows for formation year {formation_year}.")
        return df
    except Exception as e:
        print(f"Error fetching data for formation year {formation_year} from DuckDB: {e}")
        return pd.DataFrame()

def construct_ff_factors_for_year(df_year_data, formation_year):
    # ... (same detailed logic as in previous Python outline)
    # Make sure to implement the full 3-way SMB if desired, or note the simplification.
    # For brevity, I'll just sketch it here. Refer to the more detailed one before.
    print(f"Constructing factors for portfolio formed June {formation_year}...")
    if df_year_data.empty:
        return pd.DataFrame()

    df_characteristics = df_year_data.groupby(['GVKEY', 'IID']).first().reset_index()
    df_characteristics = df_characteristics[[
        'GVKEY', 'IID', 'EXCHG', 'ME_JUNE',
        'BM_T_MINUS_1', 'OP_FY_T_MINUS_1', 'INV_FY_T_MINUS_1'
    ]].copy()
    df_characteristics.dropna(subset=['ME_JUNE', 'BM_T_MINUS_1', 'OP_FY_T_MINUS_1', 'INV_FY_T_MINUS_1'], inplace=True)

    nyse_stocks_chars = df_characteristics[df_characteristics['EXCHG'] == '11'] # VERIFY NYSE CODE
    if nyse_stocks_chars.empty or len(nyse_stocks_chars) < 10: # Need enough NYSE stocks
        print(f"Warning: Insufficient NYSE stocks ({len(nyse_stocks_chars)}) for breakpoints in {formation_year}.")
        return pd.DataFrame()

    size_breakpoint = nyse_stocks_chars['ME_JUNE'].median()
    bm_q = nyse_stocks_chars[nyse_stocks_chars['BM_T_MINUS_1'] > 0]['BM_T_MINUS_1'].quantile([0.3, 0.7])
    op_q = nyse_stocks_chars['OP_FY_T_MINUS_1'].quantile([0.3, 0.7])
    inv_q = nyse_stocks_chars['INV_FY_T_MINUS_1'].quantile([0.3, 0.7])

    if size_breakpoint is np.nan or bm_q.isnull().any() or op_q.isnull().any() or inv_q.isnull().any():
        print(f"Warning: Could not compute all NYSE breakpoints for {formation_year}.")
        return pd.DataFrame()

    df_characteristics['SizeGroup'] = np.where(df_characteristics['ME_JUNE'] < size_breakpoint, 'S', 'B')
    df_characteristics['BMGroup'] = pd.cut(df_characteristics['BM_T_MINUS_1'], bins=[-np.inf, bm_q.iloc[0], bm_q.iloc[1], np.inf], labels=['L', 'M', 'H'], include_lowest=True)
    df_characteristics['OPGroup'] = pd.cut(df_characteristics['OP_FY_T_MINUS_1'], bins=[-np.inf, op_q.iloc[0], op_q.iloc[1], np.inf], labels=['W', 'N_op', 'R'], include_lowest=True)
    df_characteristics['INVGroup'] = pd.cut(df_characteristics['INV_FY_T_MINUS_1'], bins=[-np.inf, inv_q.iloc[0], inv_q.iloc[1], np.inf], labels=['C', 'N_inv', 'A'], include_lowest=True)

    df_monthly_returns_with_portfolios = pd.merge(df_year_data,
                                                  df_characteristics[['GVKEY', 'IID', 'SizeGroup', 'BMGroup', 'OPGroup', 'INVGroup', 'ME_JUNE']],
                                                  on=['GVKEY', 'IID'], suffixes=('', '_char'))
    if 'ME_JUNE_char' in df_monthly_returns_with_portfolios.columns: # if suffix was applied
        df_monthly_returns_with_portfolios.rename(columns={'ME_JUNE_char':'ME_JUNE_weight'}, inplace=True)
    else:
         df_monthly_returns_with_portfolios.rename(columns={'ME_JUNE':'ME_JUNE_weight'}, inplace=True)


    monthly_factors_list = []
    for month_end_date, group in df_monthly_returns_with_portfolios.groupby('RETURN_MONTH_END_DATE'):
        if group.empty: continue
        def weighted_avg_return(df_pf, weight_col='ME_JUNE_weight', return_col='MONTHLY_RETURN'):
            df_pf_valid = df_pf.dropna(subset=[return_col, weight_col])
            if df_pf_valid.empty or df_pf_valid[weight_col].sum() == 0: return np.nan
            return np.average(df_pf_valid[return_col], weights=df_pf_valid[weight_col])

        # B/M sorts
        SL = weighted_avg_return(group[(group['SizeGroup'] == 'S') & (group['BMGroup'] == 'L')])
        SM = weighted_avg_return(group[(group['SizeGroup'] == 'S') & (group['BMGroup'] == 'M')])
        SH = weighted_avg_return(group[(group['SizeGroup'] == 'S') & (group['BMGroup'] == 'H')])
        BL = weighted_avg_return(group[(group['SizeGroup'] == 'B') & (group['BMGroup'] == 'L')])
        BM = weighted_avg_return(group[(group['SizeGroup'] == 'B') & (group['BMGroup'] == 'M')])
        BH = weighted_avg_return(group[(group['SizeGroup'] == 'B') & (group['BMGroup'] == 'H')])
        # OP sorts
        SR = weighted_avg_return(group[(group['SizeGroup'] == 'S') & (group['OPGroup'] == 'R')])
        SNop = weighted_avg_return(group[(group['SizeGroup'] == 'S') & (group['OPGroup'] == 'N_op')])
        SW = weighted_avg_return(group[(group['SizeGroup'] == 'S') & (group['OPGroup'] == 'W')])
        BR = weighted_avg_return(group[(group['SizeGroup'] == 'B') & (group['OPGroup'] == 'R')])
        BNop = weighted_avg_return(group[(group['SizeGroup'] == 'B') & (group['OPGroup'] == 'N_op')])
        BW = weighted_avg_return(group[(group['SizeGroup'] == 'B') & (group['OPGroup'] == 'W')])
        # INV sorts
        SC = weighted_avg_return(group[(group['SizeGroup'] == 'S') & (group['INVGroup'] == 'C')])
        SNinv = weighted_avg_return(group[(group['SizeGroup'] == 'S') & (group['INVGroup'] == 'N_inv')])
        SA = weighted_avg_return(group[(group['SizeGroup'] == 'S') & (group['INVGroup'] == 'A')])
        BC = weighted_avg_return(group[(group['SizeGroup'] == 'B') & (group['INVGroup'] == 'C')])
        BNinv = weighted_avg_return(group[(group['SizeGroup'] == 'B') & (group['INVGroup'] == 'N_inv')])
        BA = weighted_avg_return(group[(group['SizeGroup'] == 'B') & (group['INVGroup'] == 'A')])

        portfolios = {'SL': SL, 'SM': SM, 'SH': SH, 'BL': BL, 'BM': BM, 'BH': BH,
                      'SR': SR, 'SNop': SNop, 'SW': SW, 'BR': BR, 'BNop': BNop, 'BW': BW,
                      'SC': SC, 'SNinv': SNinv, 'SA': SA, 'BC': BC, 'BNinv': BNinv, 'BA': BA}

        # Check for enough valid portfolio returns to calculate factors
        if any(pd.isna(val) for val in [SL, SM, SH, BL, BM, BH, SR, SW, BR, BW, SC, SA, BC, BA]):
            print(f"  Skipping factor calculation for {month_end_date} due to missing portfolio returns.")
            monthly_factors_list.append({'Date': month_end_date, 'SMB': np.nan, 'HML': np.nan, 'RMW': np.nan, 'CMA': np.nan})
            continue

        smb_bm = (portfolios['SL'] + portfolios['SM'] + portfolios['SH']) / 3.0 - (portfolios['BL'] + portfolios['BM'] + portfolios['BH']) / 3.0
        smb_op = (portfolios['SR'] + portfolios['SNop'] + portfolios['SW']) / 3.0 - (portfolios['BR'] + portfolios['BNop'] + portfolios['BW']) / 3.0
        smb_inv = (portfolios['SC'] + portfolios['SNinv'] + portfolios['SA']) / 3.0 - (portfolios['BC'] + portfolios['BNinv'] + portfolios['BA']) / 3.0
        smb_factor = (smb_bm + smb_op + smb_inv) / 3.0

        hml_factor = (portfolios['SH'] + portfolios['BH']) / 2.0 - (portfolios['SL'] + portfolios['BL']) / 2.0
        rmw_factor = (portfolios['SR'] + portfolios['BR']) / 2.0 - (portfolios['SW'] + portfolios['BW']) / 2.0
        cma_factor = (portfolios['SC'] + portfolios['BC']) / 2.0 - (portfolios['SA'] + portfolios['BA']) / 2.0

        monthly_factors_list.append({'Date': month_end_date, 'SMB': smb_factor, 'HML': hml_factor, 'RMW': rmw_factor, 'CMA': cma_factor})

    return pd.DataFrame(monthly_factors_list)


def calculate_factor_exposures(stock_returns_df, factors_df, stock_id_col='GVKEY', date_col='Date', return_col='MONTHLY_RETURN'):
    # ... (same as in previous Python outline) ...
    all_betas = []
    for stock_id, group_df in stock_returns_df.groupby(stock_id_col):
        merged_df = pd.merge(group_df, factors_df, on=date_col, how='inner')
        if merged_df.empty or len(merged_df) < 24: # Min observations for regression
            print(f"Skipping {stock_id}: Insufficient data ({len(merged_df)} obs) after merging with factors.")
            continue

        # Ensure no NaNs in critical columns before regression
        regression_data = merged_df[[return_col, 'RF', 'Mkt_RF', 'SMB', 'HML', 'RMW', 'CMA']].dropna()
        if len(regression_data) < 24:
            print(f"Skipping {stock_id}: Insufficient non-NaN data ({len(regression_data)} obs) for regression.")
            continue

        Y = regression_data[return_col] - regression_data['RF']
        X = regression_data[['Mkt_RF', 'SMB', 'HML', 'RMW', 'CMA']]
        X = sm.add_constant(X)

        try:
            model = sm.OLS(Y.astype(float), X.astype(float)).fit()
            betas = model.params.to_dict()
            betas['alpha'] = betas.pop('const')
            betas[stock_id_col] = stock_id
            betas['r_squared'] = model.rsquared
            betas['num_obs'] = int(model.nobs) # Ensure num_obs is int
            betas['alpha_t_stat'] = model.tvalues['const']
            betas['alpha_p_value'] = model.pvalues['const']
            all_betas.append(betas)
        except Exception as e:
            print(f"Regression failed for {stock_id}: {e}")
    return pd.DataFrame(all_betas)


def main_factor_construction_and_analysis():
    print("--- Starting Factor Construction and Analysis ---")
    duckdb_manager = get_duckdb_manager()

    # 1. Download/Load Official Ken French Factors
    ff_factors_official_df = download_and_parse_ken_french_factors(KEN_FRENCH_URL_5_FACTOR_MONTHLY_CSV_ZIP)
    if ff_factors_official_df.empty:
        print("Could not get official Ken French factors. Exiting.")
        return
    ff_factors_official_df['Date'] = pd.to_datetime(ff_factors_official_df['Date'])
    # Store official FF factors to DuckDB
    create_ken_french_factors_table(duckdb_manager) # Ensure table exists
    store_df_to_duckdb(ff_factors_official_df, MYSQL_TABLE_KENFRENCH, duckdb_manager, 'ff', 'replace')


    # 2. Construct Your SMB, HML, RMW, CMA
    try:
        year_range_query = f"SELECT MIN(FORMATION_YEAR_T) as min_year, MAX(FORMATION_YEAR_T) as max_year FROM {MYSQL_TABLE_FACTORDATA}"
        year_range_df = duckdb_manager.read_sql(year_range_query, database='compustat')
        year_range = year_range_df.iloc[0].to_dict()
    except Exception as e:
        print(f"Error getting year range from DuckDB: {e}")
        return

    if not year_range or year_range['min_year'] is None:
        print("No formation years found in DuckDB table. Exiting.")
        return
    min_year, max_year = year_range['min_year'], year_range['max_year']

    all_my_factors_list = []
    for year in range(min_year, max_year + 1):
        df_year_data = get_data_for_formation_year(duckdb_manager, year)
        if not df_year_data.empty:
            monthly_factors_for_year_df = construct_ff_factors_for_year(df_year_data, year)
            if not monthly_factors_for_year_df.empty:
                all_my_factors_list.append(monthly_factors_for_year_df)

    if not all_my_factors_list:
        print("No factors constructed. Exiting.")
        return

    my_factors_df = pd.concat(all_my_factors_list).sort_values(by='Date').reset_index(drop=True)
    my_factors_df['Date'] = pd.to_datetime(my_factors_df['Date'])
    # Store your constructed factors to DuckDB
    store_df_to_duckdb(my_factors_df, MYSQL_TABLE_MYFACTORS, duckdb_manager, 'ff', 'replace')


    # 3. Combine and Compare
    final_factors_df = pd.merge(
        ff_factors_official_df[['Date', 'Mkt_RF', 'RF']],
        my_factors_df[['Date', 'SMB', 'HML', 'RMW', 'CMA']],
        on='Date',
        how='inner' # Use inner join to only operate on common period
    )
    final_factors_df = final_factors_df.dropna() # Drop rows if any factor is NaN after merge
    
    print("\n--- Your Combined Factors (Head) ---")
    print(final_factors_df.head())

    # Store the final_factors_df
    if not final_factors_df.empty:
        print(f"Storing final combined factors to DuckDB table: {MYSQL_TABLE_FINAL_COMBINED_FACTORS}")
        store_df_to_duckdb(final_factors_df, MYSQL_TABLE_FINAL_COMBINED_FACTORS, duckdb_manager, 'ff', 'replace')
    else:
        print("Warning: final_factors_df is empty, nothing to store in final_combined_factors.")
    

    # Compare your factors with official Ken French factors (excluding Mkt-RF and RF)
    comparison_df = pd.merge(final_factors_df, ff_factors_official_df, on="Date", suffixes=('_my', '_kf'), how="inner")
    if not comparison_df.empty:
        print("\n--- Correlation with Ken French Factors ---")
        for factor in ['SMB', 'HML', 'RMW', 'CMA']:
            if f'{factor}_my' in comparison_df and f'{factor}_kf' in comparison_df:
                corr = comparison_df[f'{factor}_my'].corr(comparison_df[f'{factor}_kf'])
                print(f"Correlation for {factor}: {corr:.4f}")
            else:
                print(f"Could not find both my and kf versions for factor {factor} in comparison_df")
    else:
        print("Comparison DataFrame is empty. Check date alignment or data availability.")


    # 4. Calculate Factor Exposures for Specific Stocks
    # Example: Stocks from your universe for a recent period
    stock_gvkeys_to_analyze = ['001690', '001075', '001300'] # Example: Apple, Microsoft, Amazon
    gvkey_placeholders = ', '.join(['%s'] * len(stock_gvkeys_to_analyze))

    stock_returns_query = f"""
    SELECT GVKEY, RETURN_MONTH_END_DATE AS Date, MONTHLY_RETURN
    FROM {MYSQL_TABLE_FACTORDATA}
    WHERE GVKEY IN ({gvkey_placeholders})
      AND RETURN_MONTH_END_DATE >= %s -- Start date for analysis period
      AND RETURN_MONTH_END_DATE <= %s -- End date for analysis period
    ORDER BY GVKEY, Date;
    """
    # Define analysis period (e.g., last 5 years of available factor data)
    if not final_factors_df.empty:
        analysis_end_date = final_factors_df['Date'].max()
        analysis_start_date = analysis_end_date - pd.DateOffset(years=5)

        try:
            stock_returns_for_analysis_df = duckdb_manager.read_sql(
                stock_returns_query,
                database='compustat'
            )
            stock_returns_for_analysis_df['Date'] = pd.to_datetime(stock_returns_for_analysis_df['Date'])

            if not stock_returns_for_analysis_df.empty:
                print(f"\n--- Calculating Factor Exposures for {stock_gvkeys_to_analyze} ---")
                exposure_df = calculate_factor_exposures(stock_returns_for_analysis_df, final_factors_df)
                if not exposure_df.empty:
                    print("\nFactor Exposures (Betas):")
                    print(exposure_df)
                else:
                    print("Could not calculate exposures for the sample stocks.")
            else:
                print("No stock returns data found for analysis example for the selected stocks/period.")
        except Exception as e:
            print(f"Error fetching stock returns for analysis: {e}")
    else:
        print("Final factors DataFrame is empty, cannot proceed with exposure calculation.")

    print("--- Factor Construction and Analysis Finished ---")

if __name__ == "__main__":
    # To run the full process:
    # 1. Ensure your data has been migrated to DuckDB using migrate_to_duckdb.py
    # 2. Then run this script.
    main_factor_construction_and_analysis()