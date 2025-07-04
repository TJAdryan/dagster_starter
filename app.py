# my_speed_camera_project/app.py

import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime

# --- Configuration ---
AGGREGATED_DATA_DIR = Path("data/aggregated_financial_data")
DAILY_SUMMARY_PATH = AGGREGATED_DATA_DIR / "daily_financial_summary.parquet"
WEEKLY_SUMMARY_PATH = AGGREGATED_DATA_DIR / "weekly_financial_summary.parquet"
TOP_VIOLATIONS_CHART_PATH = Path("results/top_violation_types_by_count.png")

# --- Helper function to load data ---
@st.cache_data
def load_data(filepath):
    if filepath.exists():
        try:
            return pd.read_parquet(filepath)
        except Exception as e:
            st.error(f"Error loading data from {filepath}: {e}")
            return pd.DataFrame()
    st.warning(f"Data file not found: {filepath}. Please run the Dagster pipeline to generate data.")
    return pd.DataFrame()

# --- Streamlit App Layout ---
st.set_page_config(layout="wide")

st.title("NYC Traffic Violation Financial Dashboard")

# --- Load Data ---
daily_df = load_data(DAILY_SUMMARY_PATH)
weekly_df = load_data(WEEKLY_SUMMARY_PATH)

earliest_date_str = "N/A"
latest_date_str = "N/A"

if not daily_df.empty and 'date' in daily_df.columns:
    daily_df['date'] = pd.to_datetime(daily_df['date'])
    earliest_date_str = daily_df['date'].min().strftime("%Y-%m-%d")
    latest_date_str = daily_df['date'].max().strftime("%Y-%m-%d")
    
    st.markdown(f"""
    This dashboard visualizes financial trends from NYC traffic violations data, 
    processed and aggregated by a Dagster pipeline.
    
    **Data covers the period from: {earliest_date_str} to {latest_date_str}**
    """)
else:
    st.markdown("""
    This dashboard visualizes financial trends from NYC traffic violations data, 
    processed and aggregated by a Dagster pipeline.
    """)
    st.info("No daily data loaded to determine the date range.")

# --- Display Raw Charts ---
st.header("Static Charts (Generated by Dagster)")
if TOP_VIOLATIONS_CHART_PATH.exists():
    st.image(str(TOP_VIOLATIONS_CHART_PATH), caption="Top Violations by Count (Generated by Dagster)")
else:
    st.info("Top Violations by Count chart not found. Run Dagster pipeline to generate.")

# --- Interactive Charts ---
st.header("Interactive Financial Trends")

# Define formatting for tables
FINANCIAL_COLS = ['total_fines_issued', 'total_paid_amount', 'total_amount_due', 'total_outstanding']
# --- NEW: Add 'percentage_paid' to a separate list for specific formatting ---
PERCENTAGE_COLS = ['percentage_paid']

def format_financial_df(df_input):
    df_styled = df_input.copy()
    if 'date' in df_styled.columns:
        df_styled['date'] = df_styled['date'].dt.strftime('%Y-%m-%d')
    
    format_dict = {}
    # Format financial columns as dollars
    for col in FINANCIAL_COLS:
        if col in df_styled.columns:
            format_dict[col] = "${:,.0f}" 
    
    # Format 'days_in_week' as integer
    if 'days_in_week' in df_styled.columns:
        format_dict['days_in_week'] = "{:,.0f}" 

    # --- NEW: Format percentage column ---
    for col in PERCENTAGE_COLS:
        if col in df_styled.columns:
            format_dict[col] = "{:.2f}%" # Format as percentage with 2 decimal places
    # --- END NEW ---

    return df_styled.style.format(format_dict)


if not daily_df.empty:
    st.subheader("Daily Financial Overview")
    
    fig_daily = px.line(
        daily_df,
        x="date",
        y=["total_fines_issued", "total_paid_amount", "total_outstanding"],
        title="Daily Violations: Issued, Paid, Outstanding",
        labels={
            "value": "Amount ($)",
            "variable": "Metric"
        }
    )
    fig_daily.update_layout(hovermode="x unified")
    st.plotly_chart(fig_daily, use_container_width=True)

    st.subheader("Daily Data Table")
    # Display all relevant columns, including percentage_paid
    st.dataframe(format_financial_df(daily_df))
    st.markdown("""
    * **Total amount due:** includes all fines not paid
    * **Total amount outstanding:** specifically targets violations with issued judgments unpaid
    """)

else:
    st.info("Daily financial summary data not available. Please ensure your Dagster pipeline ran successfully.")

if not weekly_df.empty:
    st.subheader("Weekly Financial Overview") 

    fig_weekly = px.line(
        weekly_df,
        x="date",
        y=["total_fines_issued", "total_paid_amount", "total_outstanding"],
        title="Weekly Violations: Issued, Paid, Outstanding",
        labels={
            "value": "Amount ($)",
            "variable": "Metric"
        }
    )
    fig_weekly.update_layout(hovermode="x unified")
    st.plotly_chart(fig_weekly, use_container_width=True)

    st.subheader("Weekly Data Table")
    # Display all relevant columns, including percentage_paid and days_in_week
    st.dataframe(format_financial_df(weekly_df)) 

else:
    st.info("Weekly financial summary data not available. Please ensure your Dagster pipeline ran successfully.")

last_updated_date = "Unknown"
if not daily_df.empty:
    last_updated_date = daily_df['date'].max().strftime("%Y-%m-%d")

st.markdown("---")
st.write(f"Data last updated: {last_updated_date}")