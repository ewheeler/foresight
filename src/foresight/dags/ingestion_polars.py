import json
import gzip
import shutil
import datetime
import pathlib
import functools
import urllib.request
from zipfile import ZipFile

import numpy as np
import polars as pl
from dagster import Definitions
from dagster import AssetSelection
from dagster import MonthlyPartitionsDefinition
from dagster import DailyPartitionsDefinition
from dagster import TimeWindowPartitionMapping
from dagster import AssetIn
from dagster import Output
from dagster import define_asset_job
from dagster import asset
from dagster import ExpectationResult

from dags import deployment_name
from dags import resources
from dags.utils import gdelt_files_between
from dags.utils import json_decode_many
from dags.utils import SerializableGenerator
from dags.utils import month_map
from dags.utils import quarter_map


daily_partitions_def = DailyPartitionsDefinition(start_date="20200101010101", fmt='%Y%m%d%H%M%S')
monthly_partitions_def = MonthlyPartitionsDefinition(start_date="202001", fmt='%Y%m')

@asset(
	code_version="1",
	io_manager_key="parquet_io_manager"
)
def acled(context) -> pl.DataFrame:

    hdx_package_url = "http://data.humdata.org/api/3/action/package_show?id=political-violence-events-and-fatalities"
    with urllib.request.urlopen(hdx_package_url) as f:
        hdx_response = json.loads(f.read())
    # https://data.humdata.org/dataset/political-violence-events-and-fatalities
    # NOTE this url changes every week
    hdx_latest_resource_id = hdx_response['result']['resources'][0]['download_url'].split('/')[6]
    df = pl.read_excel(hdx_response['result']['resources'][0]['download_url'], sheet_name=1)
    df['Month'] = df['Month'].map(month_map)
    df['Quarter'] = df['Month'].map(quarter_map)

    context.log_event(
        ExpectationResult(
            success=len(df) > 0,
            description="ensure dataframe has rows",
            metadata={
                "hdx_latest_resource_id": hdx_latest_resource_id,
                "raw_count": len(df),
            },
        )
    )
    yield Output(df)

@functools.cache
def get_gkg_meta() -> dict:
    gkg_schema_url = "https://raw.githubusercontent.com/linwoodc3/gdelt2HeaderRows/master/schema_csvs/GDELT_2.0_gdeltKnowledgeGraph_Column_Labels_Header_Row_Sep2016.tsv"
    gkg_schema = pl.read_csv(gkg_schema_url, sep='\t')
    gkg_headers = gkg_schema['tableId']
    # use nullable integer type Int64
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/gotchas.html#support-for-integer-na=
    gkg_meta = dict(zip(gkg_schema['tableId'],
                    gkg_schema['dataType'].str.replace('STRING', 'str').str.replace('INTEGER', 'Int64')))
    return gkg_headers, gkg_meta

# TODO make into op?
def fetch_gkg(timestamp, tmp_dir):
    gkg_headers, gkg_meta = get_gkg_meta()
    # timestamp is UTC "YYYYMMDDHHMMSS"
    # TODO is this utf8 or cp1252?
    gkg_filename = f"{timestamp}.gkg.csv.zip"
    gz_tmp = f"/{tmp_dir}/{gkg_filename}"

    gkg_url = f"http://data.gdeltproject.org/gdeltv2/{gkg_filename}"
    try:
        # get gdelt file
        if not pathlib.Path(gz_tmp).is_file():
            urllib.request.urlretrieve(gkg_url, gz_tmp)
        df = pl.read_csv(ZipFile(gz_tmp).read(f"{timestamp}.gkg.csv"),
                         sep='\t', encoding="utf8", ignore_errors=True, has_header=False)
                         
    except (UnicodeEncodeError, FileNotFoundError, EOFError, urllib.error.HTTPError):
        df = pl.DataFrame(np.zeros((2, 27))).select(pl.all().cast(str, strict=False))

    df.columns = gkg_headers
    df = df.with_column(pl.col("DATE").cast(str, strict=False).str.strptime(pl.Datetime, "%Y%m%d%H%M%S", strict=False).alias("DATE"))
    df = df.with_columns(pl.col("V2Locations").cast(str, strict=False).alias("V2Locations"))
    df = df.with_columns(pl.col("GKGRECORDID").cast(str, strict=False).alias("GKGRECORDID"))
    df = df.with_columns(pl.col("SourceCollectionIdentifier").cast(pl.Int64, strict=False).alias("SourceCollectionIdentifier"))
    df = df.with_column(pl.lit(timestamp).alias('gkg_file'))

    # extract countries from V2Locations
    # extract_all doesnt: https://github.com/pola-rs/polars/issues/4751
    df = df.with_columns(
             df.select(
                pl.col("V2Locations").str.extract_all(r'1#\w+#(?P<country>\w{2})#').arr.eval(pl.element().str.extract(r'1#\w+#(?P<country>\w{2})#')).alias('countries')))
    # find set of 10 first occurring countries
    df = df.with_columns(
            df.select(
                    pl.col("countries").arr.eval(pl.all().unique(maintain_order=True).head(10)).alias('top_countries')))

    # first three get their own columns
    df = df.with_columns(df.select([pl.col('top_countries').arr.get(0).alias("country-1"),
                                    pl.col('top_countries').arr.get(1).alias("country-2"),
                                    pl.col('top_countries').arr.get(2).alias("country-3"),]))
    # discard intermediary columns
    df.drop_in_place('top_countries')
    df.drop_in_place('countries')

    return df

@asset(
    partitions_def=daily_partitions_def,
    code_version="1",
	io_manager_key="parquet_io_manager"
)
def gkg(context) -> pl.DataFrame:
    # TODO make tmp_base configurable
    tmp_base ='/tmp/foresight'
    tmp_dir = f"{tmp_base}/{context.run.run_id}"

    partition_key = context.asset_partition_key_for_output()
    partition_date_str = partition_key.split('|')[0]
    partition_start = datetime.datetime.strptime(partition_date_str, '%Y%m%d%H%M%S')

    windows = gdelt_files_between(partition_start,
                                  partition_start + datetime.timedelta(days=1))

    # `fetch_gsg` is not an op, so it doesn't have access to `context`.
    # create a partial function that provides `tmp_dir`
    # to `fetch_gsg` that we can still use with `map`
    gkg_fetcher = functools.partial(fetch_gkg,
                                    tmp_dir=tmp_dir)
    df = pl.concat(list(map(gkg_fetcher, windows)))
    df = df.drop_nulls(subset=["DATE"])
    df = df.unique(subset=["DocumentIdentifier"])
    df = df.unique(subset=["GKGRECORDID"])

    context.log_event(
        ExpectationResult(
            success=len(df) > 0,
            description="ensure dataframe has rows",
            metadata={
                "partition_start": partition_date_str,
                "raw_count": len(df),
            },
        )
    )
    yield Output(df, metadata={"num_rows": len(df)})

# TODO make into op?
def fetch_gsg(timestamp, tmp_dir):
    pathlib.Path(tmp_dir).mkdir(parents=True, exist_ok=True)
    # timestamp is UTC "YYYYMMDDHHMMSS"
    gsg_filename = f"{timestamp}.gsg.docembed.json.gz"
    gsg_url = f"http://data.gdeltproject.org/gdeltv3/gsg_docembed/{gsg_filename}"

    gz_tmp = f"{tmp_dir}/{gsg_filename}"
    json_tmp = f"{tmp_dir}/{timestamp}.gsg.docembed.json"

    try:
        # get gdelt file
        if not pathlib.Path(gz_tmp).is_file():
            urllib.request.urlretrieve(gsg_url, gz_tmp)

        # make json generator
        with gzip.GzipFile(fileobj=open(gz_tmp, 'rb')) as gzipfile:
            content_str = gzipfile.read().decode('utf-8')
            records = json_decode_many(content_str)
    except (EOFError, urllib.error.HTTPError):
        records = [{'docembed': list(np.zeros(512)), 'date': '', 'url': '',
                    'lang': '', 'title': '', 'model': ''},]

    # write decoded ndjson
    with open(json_tmp, 'w') as f:
        encoder = json.JSONEncoder()
        for record in iter(SerializableGenerator(records)):
            # explode column 'docembed' (list of floats) into 512 columns
            embeds = dict([(f'docembed-{n}', i) for n, i in enumerate(record['docembed'])])
            _ = record.pop('docembed')
            record.update(embeds)
            f.write(encoder.encode(record) + '\n')

    # https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.scan_ndjson.html
    df = pl.read_ndjson(json_tmp)
    df = df.with_column(pl.lit(timestamp).alias('gsg_file'))
    return df

@asset(
    partitions_def=daily_partitions_def,
    code_version="1",
	io_manager_key="parquet_io_manager"
)
def gsg(context) -> pl.DataFrame:
    # TODO make tmp_base configurable
    tmp_base ='/tmp/foresight'
    tmp_dir = f"{tmp_base}/{context.run.run_id}"

    partition_key = context.asset_partition_key_for_output()
    partition_date_str = partition_key.split('|')[0]
    partition_start = datetime.datetime.strptime(partition_date_str, '%Y%m%d%H%M%S')

    windows = gdelt_files_between(partition_start,
                                  partition_start + datetime.timedelta(days=1))

    # `fetch_gsg` is not an op, so it doesn't have access to `context`.
    # create a partial function that provides `tmp_dir`
    # to `fetch_gsg` that we can still use with `map`
    gsg_fetcher = functools.partial(fetch_gsg,
                                    tmp_dir=tmp_dir)
    df = pl.concat(list(map(gsg_fetcher, windows)))
    df = df.unique(subset=["url"])

    context.log_event(
        ExpectationResult(
            success=len(df) > 0,
            description="ensure dataframe has rows",
            metadata={
                "partition_start": partition_date_str,
                "raw_count": len(df),
            },
        )
    )
    yield Output(df, metadata={"num_rows": len(df)})

@asset(
    partitions_def=daily_partitions_def,
    code_version="1",
	io_manager_key="parquet_io_manager",
    ins={
        "gsg": AssetIn(
            partition_mapping=TimeWindowPartitionMapping(start_offset=0)
        ),
        "gkg": AssetIn(
            partition_mapping=TimeWindowPartitionMapping(start_offset=0)
        )
    }
)
def gdelt(context, gkg, gsg) -> pl.DataFrame:
    df = gkg.join(gsg, left_on='DocumentIdentifier', right_on='url')

    df = df.with_column(pl.col('DATE').dt.year().cast(pl.Int16, strict=False).alias('year'))
    df = df.with_column(pl.col('DATE').dt.month().cast(str, strict=False).alias('month'))
    df = df.with_column(pl.col('DATE').dt.strftime('%Y%m').alias('yearmonth'))

    context.log_event(
        ExpectationResult(
            success=len(df) > 0,
            description="ensure dataframe has rows",
            metadata={
                "raw_count": len(df),
            },
        )
    )
    try:
        # TODO make tmp_base configurable
        tmp_base ='/tmp/foresight'
        tmp_dir = f"{tmp_base}/{context.run.run_id}"
        shutil.rmtree(tmp_dir)
    except OSError:
        pass
    yield Output(df, metadata={"num_rows": len(df)})

gdelt_job = define_asset_job(
    name="gdelt_job",
    selection=AssetSelection.assets(gsg, gkg, gdelt),
    partitions_def=daily_partitions_def,
)

defs = Definitions(
    assets=[gkg, gsg, gdelt],
    jobs=[gdelt_job],
    resources=resources[deployment_name]
)
