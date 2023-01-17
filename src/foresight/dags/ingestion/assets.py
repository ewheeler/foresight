import pandas as pd
from dagster import Definitions
from dagster import AssetSelection
from dagster import MonthlyPartitionsDefinition
from dagster import HourlyPartitionsDefinition
from dagster import TimeWindowPartitionsDefinition
from dagster import MultiPartitionsDefinition
from dagster import StaticPartitionsDefinition
from dagster import TimeWindowPartitionMapping
from dagster import AssetIn
from dagster import Output
from dagster import define_asset_job
from dagster import asset
from dagster import ExpectationResult
from dagster import MetadataValue

def calculate_bytes(df):
    return None

"""
fifteen_minutes_partitions_def = MultiPartitionsDefinition({
    "quarterhour": TimeWindowPartitionsDefinition(start="20200101010101", fmt='%Y%m%d%H%M%S', cron_schedule="*/15 * * * *"),
    "location": StaticPartitionsDefinition(["ETH", "MAM", "NGR"]),})

monthly_partitions_def = MultiPartitionsDefinition({
    "month": MonthlyPartitionsDefinition(start_date="2020-01-01"),
    "location": StaticPartitionsDefinition(["ETH", "MAM", "NGR"]),})
"""
fifteen_minutes_partitions_def = TimeWindowPartitionsDefinition(start="20200101000000",
                                                                fmt='%Y%m%d%H%M%S',
                                                                cron_schedule="*/15 * * * *")
monthly_partitions_def = MonthlyPartitionsDefinition(start_date="2020-01-01")

@asset(partitions_def=monthly_partitions_def, code_version="1")
def acled(context) -> pd.DataFrame:
    df = pd.DataFrame()
    context.log_event(
        ExpectationResult(
            success=len(df) > 0,
            description="ensure dataframe has rows",
            metadata={
                "text_metadata": "Text-based metadata for this event",
                "dashboard_url": MetadataValue.url(
                    "http://mycoolsite.com/url_for_my_data"
                ),
                "raw_count": len(df),
                "size (bytes)": calculate_bytes(df),
            },
        )
    )
    yield Output(df)

@asset(
    #partitions_def=fifteen_minutes_partitions_def,
    partitions_def=monthly_partitions_def,
    code_version="1",
)
def gdelt(context) -> pd.DataFrame:
    df = pd.DataFrame()
    context.log_event(
        ExpectationResult(
            success=len(df) > 0,
            description="ensure dataframe has rows",
            metadata={
                "text_metadata": "Text-based metadata for this event",
                "dashboard_url": MetadataValue.url(
                    "http://mycoolsite.com/url_for_my_data"
                ),
                "raw_count": len(df),
                "size (bytes)": calculate_bytes(df),
            },
        )
    )
    yield Output(df)

@asset(
    #partitions_def=fifteen_minutes_partitions_def,
    partitions_def=monthly_partitions_def,
    code_version="1",
    ins={
        "acled": AssetIn(
            partition_mapping=TimeWindowPartitionMapping(start_offset=0)
        ),
        "gdelt": AssetIn(
            partition_mapping=TimeWindowPartitionMapping(start_offset=0)
        )
    }
)
def training(context, gdelt, acled) -> pd.DataFrame:
    df = pd.DataFrame()
    context.log_event(
        ExpectationResult(
            success=len(df) > 0,
            description="ensure dataframe has rows",
            metadata={
                "text_metadata": "Text-based metadata for this event",
                "dashboard_url": MetadataValue.url(
                    "http://mycoolsite.com/url_for_my_data"
                ),
                "raw_count": len(df),
                "size (bytes)": calculate_bytes(df),
            },
        )
    )
    yield Output(df)

partitioned_asset_job = define_asset_job(
    name="acled_and_gdelt_job",
    selection=AssetSelection.assets(gdelt, acled, training),
    partitions_def=monthly_partitions_def,
)


defs = Definitions(
    assets=[acled, gdelt, training],
    jobs=[partitioned_asset_job],
)
