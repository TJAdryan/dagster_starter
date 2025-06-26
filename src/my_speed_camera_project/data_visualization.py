# my_speed_camera_project/src/my_speed_camera_project/data_visualization.py

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dagster import asset, get_dagster_logger, Nothing, Output, AssetMaterialization, MetadataValue
from pathlib import Path

logger = get_dagster_logger()

# (daily_financial_trend_chart and weekly_financial_trend_chart remain unchanged)

@asset(
    description="Generates a bar chart of top violation types by number of violations.", # Updated description
    io_manager_key=None
)
def top_violation_types_by_count(cleaned_financial_data: pd.DataFrame) -> Nothing: # <--- ASSET RENAMED
    """
    Identifies and visualizes the top N violation types by their total number of violations.
    Shows the number of days measured and total violations in the title/metadata.
    """
    df = cleaned_financial_data.copy()

    output_path = Path("results/top_violation_types_by_count.png") # Updated output filename
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # --- New calculations for chart context (number of days and total violations) ---
    num_days_measured = 0
    if not df.empty and 'issue_date' in df.columns:
        # Use .dt.date to get just the date part, then nunique for unique days
        num_days_measured = df['issue_date'].dt.date.nunique()
    else:
        logger.warning("Issue date column not found or DataFrame empty for 'days measured' calculation.")

    total_violations_overall = 0
    if not df.empty and 'summons_number' in df.columns:
        # Count unique summons_number for total violations
        total_violations_overall = df['summons_number'].nunique()
    else:
        logger.warning("Summons number column not found or DataFrame empty for 'total violations' calculation.")
    # --- End new calculations ---

    # --- Input check updated to match new aggregation needs (summons_number) ---
    if df.empty or 'violation' not in df.columns or 'summons_number' not in df.columns:
        logger.warning("Input DataFrame for top violation types by count chart is empty or missing required columns ('violation', 'summons_number'). Skipping chart generation.")
        yield AssetMaterialization(
            asset_key="top_violation_types_by_count", # <--- ASSET KEY UPDATED
            description="No chart generated: input DataFrame was empty or missing columns.",
            metadata={"status": MetadataValue.text("Skipped: Empty input/missing columns")}
        )
        yield Output(None)
        return

    df['violation'] = df['violation'].astype(str).fillna('UNKNOWN_VIOLATION')
    
    # --- MODIFIED AGGREGATION: Count unique summons_number per violation type ---
    violation_counts = df.groupby('violation')['summons_number'].nunique().reset_index(name='violation_count')
    # --- END MODIFIED AGGREGATION ---
    
    if violation_counts.empty:
        logger.info("No violations found to chart by type after grouping.")
        yield AssetMaterialization(
            asset_key="top_violation_types_by_count", # <--- ASSET KEY UPDATED
            description="No chart generated: no violations found after grouping.",
            metadata={"status": MetadataValue.text("Skipped: No violations found")}
        )
        yield Output(None)
        return

    # Get top 10 types by violation_count
    top_n = violation_counts.sort_values(by='violation_count', ascending=False).head(10)

    plt.figure(figsize=(12, 7))
    # --- MODIFIED PLOTTING: x-axis is 'violation_count' ---
    sns.barplot(x='violation_count', y='violation', data=top_n, palette='viridis')
    # --- UPDATED CHART TITLE WITH DATE RANGE AND TOTAL COUNT ---
    plt.title(f'Top 10 Violation Types by Count (Measured over {num_days_measured} Days, Total {total_violations_overall} Violations)')
    plt.xlabel('Number of Violations') # <--- UPDATED X-AXIS LABEL
    plt.ylabel('Violation Type')
    plt.tight_layout()

    plt.savefig(output_path)
    logger.info(f"Top violation types by count chart saved to {output_path}")
    
    # --- UPDATED ASSET MATERIALIZATION METADATA ---
    yield AssetMaterialization(
        asset_key="top_violation_types_by_count", # <--- ASSET KEY UPDATED
        description="Top 10 violation types by count chart generated.",
        metadata={
            "chart_file": MetadataValue.path(str(output_path)),
            "view_chart": MetadataValue.url(f"file://{output_path.absolute()}"),
            "chart_type": MetadataValue.text("Bar Chart by Count"),
            "top_violation": MetadataValue.text(top_n.iloc[0]['violation'] if not top_n.empty else "N/A"),
            "top_count": MetadataValue.int(int(top_n.iloc[0]['violation_count']) if not top_n.empty else 0), # Using int() for count
            "days_measured": MetadataValue.int(num_days_measured), # New metadata
            "total_violations_in_period": MetadataValue.int(total_violations_overall) # New metadata
        }
    )
    yield Output(None)