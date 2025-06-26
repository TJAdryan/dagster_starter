# my_speed_camera_project/src/my_speed_camera_project/data_ingestion.py

import requests
import pandas as pd
from dagster import asset, get_dagster_logger # <--- ENSURE get_dagster_logger IS IMPORTED
import time
from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import Optional

# Configuration for NYC Open Data
VIOLATIONS_DATA_URL = "https://data.cityofnewyork.us/resource/nc67-uf89.json" 

# --- ADD THIS LINE ---
logger = get_dagster_logger()
# --- END ADDITION ---

@asset(
    description="Downloads raw violation data for a single day and saves to Parquet.",
    required_resource_keys={"nyc_open_data_api_key_resource"},
    io_manager_key=None
)
def raw_violations_daily(context) -> None:
    """
    Downloads raw violation data for a single day and saves it as a Parquet file.
    It automatically finds the next missing day starting from 180 days ago
    up to 31 days ago, for up to 7 days.
    """
    output_dir = Path("data/raw_daily_violations")
    output_dir.mkdir(parents=True, exist_ok=True)

    pull_date_str = None
    
    today = datetime.now()
    search_start_date = today - timedelta(days=61)
    search_end_date = today - timedelta(days=31)
    
    max_days_to_attempt_download = 30
    days_attempted = 0

    current_check_date_dt = search_start_date
    logger.info(f"Starting search for missing daily file from {search_start_date.strftime('%Y-%m-%d')} up to {search_end_date.strftime('%Y-%m-%d')}.")

    while (current_check_date_dt <= search_end_date and 
           days_attempted < max_days_to_attempt_download):
        
        filename_date_check_str = current_check_date_dt.strftime('%Y-%m-%d')
        filepath_check = output_dir / f"raw_violations_{filename_date_check_str}.parquet"

        if not filepath_check.exists():
            pull_date_str = current_check_date_dt.strftime('%m/%d/%Y')
            logger.info(f"Found missing data for date: {pull_date_str}. Proceeding with download for this day.")
            break
        else:
            logger.info(f"Data for {filename_date_check_str} already exists. Checking next day.")
        
        current_check_date_dt += timedelta(days=1)
        days_attempted += 1
    
    if pull_date_str is None:
        logger.info(f"All daily Parquet files from {search_start_date.strftime('%Y-%m-%d')} up to {search_end_date.strftime('%Y-%m-%d')} already exist, or limit of {max_days_to_attempt_download} days checked. Nothing to download for this run.")
        return

    filename_date_str_for_save = datetime.strptime(pull_date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
    output_filepath_final = output_dir / f"raw_violations_{filename_date_str_for_save}.parquet"

    all_data = []
    offset = 0
    limit = 1000
    max_total_records_per_day = 1_000_000
    sleep_duration = 1.0
    
    headers = {}
    api_key = context.resources.nyc_open_data_api_key_resource
    
    if api_key:
        headers["X-App-Token"] = api_key
        logger.info("Using NYC Open Data API key.")
    else:
        logger.info("No NYC Open Data API key found/provided. Proceeding without.")

    logger.info(
        f"Starting download of raw violations for date: {pull_date_str} (limit per request: {limit})..."
    )
    
    date_filter = f"issue_date = '{pull_date_str}'"

    try:
        total_downloaded = 0
        has_more_data = True
        while has_more_data and total_downloaded < max_total_records_per_day:
            params = {
                "$limit": limit,
                "$offset": offset,
                "$where": date_filter,
            }
            try:
                response = requests.get(
                    VIOLATIONS_DATA_URL, params=params, headers=headers, timeout=30
                )
                response.raise_for_status()
                
                try:
                    new_data = json.loads(response.text)
                except json.JSONDecodeError as json_e:
                    logger.error(f"JSON decoding error for response from offset {offset}: {json_e}. Response text: {response.text[:500]}...")
                    has_more_data = False
                    break 
                
                if not new_data:
                    has_more_data = False
                else:
                    new_data_count = len(new_data)
                    if total_downloaded + new_data_count > max_total_records_per_day:
                        records_to_add = max_total_records_per_day - total_downloaded
                        all_data.extend(new_data[:records_to_add])
                        total_downloaded += records_to_add
                        logger.warning(f"Reached single day record limit for {pull_date_str}. Added {records_to_add} records.")
                        has_more_data = False
                    else:
                        all_data.extend(new_data)
                        total_downloaded += new_data_count
                        logger.info(
                            f"Downloaded {new_data_count} records. Total for {pull_date_str}: {total_downloaded} so far..."
                        )
                    offset += limit
                    time.sleep(sleep_duration)
            except requests.exceptions.RequestError as e:
                logger.error(f"Network/HTTP error downloading data for {pull_date_str} from offset {offset}: {e}")
                has_more_data = False
                break
            except Exception as e:
                logger.error(f"Unexpected error during API call or processing for {pull_date_str} at offset {offset}: {e}")
                has_more_data = False
                break
    finally:
        df = pd.DataFrame(all_data) 
        logger.info(f"Finished downloading {len(df)} raw violations records for {pull_date_str}.")
        
        if df.empty:
            logger.warning(
                f"No data was downloaded for {pull_date_str}. Skipping Parquet save."
            )
            return

        cols_for_financial_analysis = [
            "plate", "state", "license_type", "summons_number", "issue_date",
            "violation_time", "violation", "fine_amount", "penalty_amount",
            "interest_amount", "reduction_amount", "payment_amount", "amount_due",
            "precinct", "county", "issuing_agency", "violation_status", "judgment_entry_date", # Exclude summons_image
        ]

        df_filtered = df[[col for col in cols_for_financial_analysis if col in df.columns]]

        for col in df_filtered.columns:
            df_filtered[col] = df_filtered[col].astype(str)

        logger.info(f"Saving {len(df_filtered)} records for {pull_date_str} to {output_filepath_final}")
        df_filtered.to_parquet(output_filepath_final, index=False)
        logger.info(f"Successfully saved data for {pull_date_str} to {output_filepath_final}")
        return