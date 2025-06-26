# my_speed_camera_project/src/my_speed_camera_project/definitions.py

from dagster import Definitions, load_assets_from_modules, ScheduleDefinition, define_asset_job, in_process_executor

import my_speed_camera_project.data_ingestion as data_ingestion
import my_speed_camera_project.financial_data_processing as financial_data_processing
import my_speed_camera_project.financial_aggregation as financial_aggregation
import my_speed_camera_project.data_visualization as data_visualization
import my_speed_camera_project.resources as resources

# Load all assets from these modules
# This loads raw_violations_daily, raw_violations_monthly, cleaned_financial_data, etc.
all_assets = load_assets_from_modules([
    data_ingestion,
    financial_data_processing,
    financial_aggregation,
    data_visualization,
])

# Define the main job that includes all pipeline assets
# Using selection="*" ensures it picks up all currently loaded assets, including the renamed ones.
# If you are specifically trying to run a subset of assets, you must use their correct names.
my_full_pipeline_job = define_asset_job(
    "my_full_pipeline_job",
    selection="*", # This should correctly include raw_violations_daily
    executor_def=in_process_executor,
)

# Define your daily schedule
daily_data_update_schedule = ScheduleDefinition(
    job=my_full_pipeline_job,
    cron_schedule="0 0 * * *",  # At 00:00 (midnight) every day
    execution_timezone="America/New_York",
    name="daily_data_update_schedule",
)

# Define the overall Dagster project
defs = Definitions(
    assets=all_assets,
    jobs=[
        my_full_pipeline_job, # Ensure this job is used by schedules and runs
    ],
    resources={
        "nyc_open_data_api_key_resource": resources.nyc_open_data_api_key_resource,
    },
    schedules=[
        daily_data_update_schedule,
    ],
)