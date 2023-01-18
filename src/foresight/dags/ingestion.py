import json
import math
import calendar
import itertools
import urllib.request

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
from dagster import AssetMaterialization
from dagster import ExpectationResult
from dagster import MetadataValue


def calculate_bytes(df):
    return None

# map of month name to month number
month_map = dict([(m, n) for n, m in enumerate(calendar.month_name[1:], 1)])

# return quarter for given month number
quarter = lambda m: math.ceil(float(m) / 3)

class SerializableGenerator(list):
    """Generator that is serializable by JSON"""

    def __init__(self, iterable):
        tmp_body = iter(iterable)
        try:
            self._head = iter([next(tmp_body)])
            self.append(tmp_body)
        except StopIteration:
            self._head = []

    def __iter__(self):
        return itertools.chain(self._head, *self[:1])

def json_decode_many(s):
    # https://stackoverflow.com/a/68942444
    decoder = json.JSONDecoder()
    _w = json.decoder.WHITESPACE.match
    idx = 0
    while True:
        idx = _w(s, idx).end() # skip leading whitespace
        if idx >= len(s):
            break
        obj, idx = decoder.raw_decode(s, idx=idx)
        yield obj


"""
# TODO try multipartition
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

@asset(
	partitions_def=monthly_partitions_def,
	code_version="1",
	io_manager_key="io_manager"
)
def acled(context) -> pd.DataFrame:

    hdx_package_url = "http://data.humdata.org/api/3/action/package_show?id=political-violence-events-and-fatalities"
    with urllib.request.urlopen(hdx_package_url) as f:
        hdx_response = json.loads(f.read())
    # https://data.humdata.org/dataset/political-violence-events-and-fatalities
    # NOTE this url changes every week
    hdx_latest_resource_id = hdx_response['result']['resources'][0]['download_url'].split('/')[6]
    df = pd.read_excel(hdx_response['result']['resources'][0]['download_url'], sheet_name=1)
    df['Month'] = df['Month'].map(month_map)
    df['Quarter'] = df['Month'].map(quarter)

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
    # TODO use iomanager instead!
    #df.to_feather(f"data/acled_{hdx_latest_resource_id}.feather")
    """
    yield AssetMaterialization(
        asset_key=f"acled_{hdx_latest_resource_id}",
        metadata={
            "text_metadata": "Text-based metadata for this event",
            "dashboard_url": MetadataValue.url(
                "http://mycoolsite.com/url_for_my_data"
            ),
            "raw_count": len(df),
            "size (bytes)": calculate_bytes(df),
        },
    )
    """
    yield Output(df)

@asset(
    #partitions_def=fifteen_minutes_partitions_def,
    partitions_def=monthly_partitions_def,
    code_version="1",
	io_manager_key="io_manager"
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
	io_manager_key="io_manager",
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
