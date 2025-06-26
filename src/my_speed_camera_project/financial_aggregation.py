# my_speed_camera_project/src/my_speed_camera_project/financial_aggregation.py

import pandas as pd
from dagster import asset, get_dagster_logger
from pathlib import Path

logger = get_dagster_logger()

# Base directory for aggregated data
AGGREGATED_DATA_DIR = Path("data/aggregated_financial_data")
AGGREGATED_DATA_DIR.mkdir(parents=True, exist_ok=True)

@asset(description="Aggregates financial data by daily periods.")
def daily_financial_summary(cleaned_financial_data: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates financial metrics by issue_date (daily).
    Calculates total issued fines, paid amounts, outstanding amounts, and percentage paid.
    """
    df = cleaned_financial_data.copy()

    if df.empty or 'issue_date' not in df.columns:
        logger.warning("Input DataFrame for daily aggregation is empty or missing 'issue_date'. Returning empty.")
        return pd.DataFrame(columns=['issue_date', 'total_fines_issued', 'total_paid_amount', 'total_amount_due', 'total_outstanding', 'total_violations', 'percentage_paid'])

    df['issue_date_only'] = df['issue_date'].dt.date

    daily_summary = df.groupby('issue_date_only').agg(
        total_fines_issued=('fine_amount', 'sum'),
        total_paid_amount=('payment_amount', 'sum'),
        total_amount_due=('amount_due', 'sum'),
        total_outstanding=('amount_due', lambda x: x[df['is_outstanding']].sum()),
        total_violations=('summons_number', 'nunique')
    ).reset_index()

    if 'total_outstanding' not in daily_summary.columns:
        daily_summary['total_outstanding'] = daily_summary['total_fines_issued'] - daily_summary['total_paid_amount']
        daily_summary['total_outstanding'] = daily_summary['total_outstanding'].apply(lambda x: max(0, x))

    # --- NEW CALCULATION: Percentage Paid ---
    # Handle division by zero: if total_fines_issued is 0, percentage_paid is 0
    daily_summary['percentage_paid'] = (
        daily_summary['total_paid_amount'] / daily_summary['total_fines_issued'] * 100
    ).fillna(0) # Fill NaN (from division by zero) with 0
    daily_summary.loc[daily_summary['total_fines_issued'] == 0, 'percentage_paid'] = 0
    # --- END NEW CALCULATION ---

    daily_summary.rename(columns={'issue_date_only': 'date'}, inplace=True)
    daily_summary['date'] = pd.to_datetime(daily_summary['date'])

    logger.info(f"Daily financial summary generated with {len(daily_summary)} records.")

    daily_summary_path = AGGREGATED_DATA_DIR / "daily_financial_summary.parquet"
    daily_summary.to_parquet(daily_summary_path, index=False)
    logger.info(f"Daily financial summary saved to {daily_summary_path}")
    return daily_summary

@asset(description="Aggregates financial data by weekly periods.")
def weekly_financial_summary(daily_financial_summary: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates daily financial metrics to weekly.
    Retains 'days_in_week' column for display in the app.
    Calculates percentage paid for weekly.
    """
    df = daily_financial_summary.copy()

    if df.empty:
        logger.warning("Input DataFrame for weekly aggregation is empty. Returning empty.")
        return pd.DataFrame(columns=['date', 'total_fines_issued', 'total_paid_amount', 'total_amount_due', 'total_outstanding', 'total_violations', 'percentage_paid', 'days_in_week'])

    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    weekly_summary_raw = df.resample('W').agg(
        total_fines_issued=('total_fines_issued', 'sum'),
        total_paid_amount=('total_paid_amount', 'sum'),
        total_amount_due=('total_amount_due', 'sum'),
        total_outstanding=('total_outstanding', 'sum'),
        total_violations=('total_violations', 'sum'),
        days_in_week=('total_violations', 'size') # Count days that contributed to this week
    ).reset_index() # type: ignore [reportArgumentType] 

    # --- NEW CALCULATION: Percentage Paid for Weekly ---
    # Handle division by zero: if total_fines_issued is 0, percentage_paid is 0
    weekly_summary_raw['percentage_paid'] = (
        weekly_summary_raw['total_paid_amount'] / weekly_summary_raw['total_fines_issued'] * 100
    ).fillna(0) # Fill NaN (from division by zero) with 0
    weekly_summary_raw.loc[weekly_summary_raw['total_fines_issued'] == 0, 'percentage_paid'] = 0
    # --- END NEW CALCULATION ---

    weekly_summary = weekly_summary_raw.copy()
    
    logger.info(f"Weekly financial summary generated with {len(weekly_summary)} weeks (including partials) and 'days_in_week' column.")
    
    weekly_summary_path = AGGREGATED_DATA_DIR / "weekly_financial_summary.parquet"
    weekly_summary.to_parquet(weekly_summary_path, index=False)
    logger.info(f"Weekly financial summary saved to {weekly_summary_path}")
    return weekly_summary