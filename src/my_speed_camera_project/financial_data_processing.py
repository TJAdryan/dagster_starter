import pandas as pd
from dagster import asset, get_dagster_logger
from pathlib import Path
from datetime import datetime, timedelta

logger = get_dagster_logger()

@asset(
    description="Collects and loads daily raw violation Parquet files for the specified window.",
)
def raw_violations_monthly(context) -> pd.DataFrame:
    """
    Collects all raw daily violation Parquet files from the 'data/raw_daily_violations'
    folder for the 61-day to 31-day window (excluding the last 30 days).
    """
    monthly_data = []
    base_dir = Path("data/raw_daily_violations")

    if not base_dir.exists():
        logger.warning(f"Base directory {base_dir} does not exist. Returning empty DataFrame.")
        # Ensure a DataFrame with expected columns is returned if directory doesn't exist
        # This prevents downstream errors in cleaned_financial_data if it expects specific columns
        return pd.DataFrame(columns=['plate', 'state', 'license_type', 'summons_number', 'issue_date',
                                     'violation_time', 'violation', 'fine_amount', 'penalty_amount',
                                     'interest_amount', 'reduction_amount', 'payment_amount', 'amount_due',
                                     'precinct', 'county', 'issuing_agency', 'violation_status', 'judgment_entry_date'])

    today = datetime.now()
    # Collect dates from 31 days ago up to 61 days ago (inclusive)
    dates_to_collect = [today - timedelta(days=i) for i in range(31, 62)]
    
    logger.info(f"Attempting to collect data for {len(dates_to_collect)} days from {dates_to_collect[-1].strftime('%Y-%m-%d')} (oldest) to {dates_to_collect[0].strftime('%Y-%m-%d')} (newest).")

    for single_date_dt in dates_to_collect:
        filename_date_str = single_date_dt.strftime('%Y-%m-%d')
        filepath = base_dir / f"raw_violations_{filename_date_str}.parquet"
        
        if filepath.exists():
            try:
                df_day = pd.read_parquet(filepath)
                monthly_data.append(df_day)
                logger.info(f"Loaded data for {filename_date_str} from {filepath}.")
            except Exception as e:
                logger.error(f"Error loading {filepath}: {e}")
        else:
            logger.info(f"No data file found for {filename_date_str} at {filepath}. Skipping.")

    if not monthly_data:
        logger.warning("No daily Parquet files found for the specified 61-31 day window. Returning empty DataFrame.")
        # Ensure a DataFrame with expected columns is returned if no files loaded
        return pd.DataFrame(columns=['plate', 'state', 'license_type', 'summons_number', 'issue_date',
                                     'violation_time', 'violation', 'fine_amount', 'penalty_amount',
                                     'interest_amount', 'reduction_amount', 'payment_amount', 'amount_due',
                                     'precinct', 'county', 'issuing_agency', 'violation_status', 'judgment_entry_date'])

    df_monthly = pd.concat(monthly_data, ignore_index=True)
    logger.info(f"Collected {len(df_monthly)} records for the specified window.")
    return df_monthly


@asset(description="Cleans and transforms raw violation data for financial analysis.")
def cleaned_financial_data(raw_violations_monthly: pd.DataFrame) -> pd.DataFrame:
    """
    Performs cleaning and type conversion for financial and date columns.
    - Converts amount columns to numeric.
    - Converts issue_date to datetime.
    - Standardizes violation_status.
    """
    df = raw_violations_monthly.copy()

    if df.empty:
        logger.warning("Input DataFrame for cleaned_financial_data is empty. Returning empty.")
        # Ensure empty DataFrame has all expected output columns for downstream assets
        return pd.DataFrame(columns=['plate', 'state', 'license_type', 'summons_number', 'issue_date',
                                     'violation_time', 'violation', 'fine_amount', 'penalty_amount',
                                     'interest_amount', 'reduction_amount', 'payment_amount', 'amount_due',
                                     'precinct', 'county', 'issuing_agency', 'violation_status', 'judgment_entry_date',
                                     'is_outstanding', 'is_paid']) # Add flags here too for full schema

    # --- 1. Process Financial Columns ---
    financial_cols = ['fine_amount', 'penalty_amount', 'interest_amount', 'reduction_amount', 'payment_amount', 'amount_due']
    
    for col in financial_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(r'[$,]', '', regex=True),
                errors='coerce'
            )
            df[col] = df[col].fillna(0)
        else:
            logger.warning(f"Financial column '{col}' not found in raw data. Setting to 0.")
            df[col] = 0

    # --- 2. Process Date Column ---
    if 'issue_date' in df.columns:
        df['issue_date'] = pd.to_datetime(df['issue_date'], errors='coerce')
        df.dropna(subset=['issue_date'], inplace=True)
    else:
        logger.warning("Column 'issue_date' not found. Cannot perform date-based analysis.")
        # If 'issue_date' is critical and missing, and the DataFrame is not empty, you might still want to return empty here
        # For now, it will proceed, but date-based aggregations will be impacted.
        
    # --- 3. Standardize Violation Status ---
    if 'violation_status' in df.columns:
        df['violation_status'] = df['violation_status'].astype(str).str.upper().str.strip()
        df['is_outstanding'] = df['violation_status'].apply(lambda x: 'OUTSTANDING' in x or 'DUE' in x or 'HEARING' in x)
        df['is_paid'] = df['violation_status'].apply(lambda x: 'PAID' in x or 'CLOSED' in x)
        if 'summons_number' not in df.columns:
            logger.warning("Column 'summons_number' not found. Cannot ensure uniqueness of violations.")
            df['summons_number'] = df.index
    else:
        logger.warning("Column 'violation_status' not found. Cannot classify outstanding/paid status.")
        df['is_outstanding'] = False
        df['is_paid'] = False
        
    logger.info(f"Cleaned financial data: {len(df)} records remaining.")
    return df
