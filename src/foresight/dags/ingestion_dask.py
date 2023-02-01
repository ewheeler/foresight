import json
import math
import gzip
import shutil
import calendar
import datetime
import pathlib
import itertools
import functools
import urllib.request

import numpy as np
import pandas as pd
import dask.dataframe as dd
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


# map of month name to month number
month_map = dict([(m, n) for n, m in enumerate(calendar.month_name[1:], 1)])

# return quarter for given month number
quarter = lambda m: math.ceil(float(m) / 3)

def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

def every_n_mins_between(start, end, minutes=15):
    return (dt.strftime('%Y%m%d%H%M%S') for dt in datetime_range(start, end, datetime.timedelta(minutes=minutes)))

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

daily_partitions_def = DailyPartitionsDefinition(start_date="20200101010101", fmt='%Y%m%d%H%M%S')

monthly_partitions_def = MonthlyPartitionsDefinition(start_date="202001", fmt='%Y%m')

@asset(
	code_version="1",
	io_manager_key="parquet_io_manager"
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
                "hdx_latest_resource_id": hdx_latest_resource_id,
                "raw_count": len(df),
            },
        )
    )
    yield Output(df)

@functools.cache
def get_gkg_meta() -> dict:
    gkg_schema_url = "https://raw.githubusercontent.com/linwoodc3/gdelt2HeaderRows/master/schema_csvs/GDELT_2.0_gdeltKnowledgeGraph_Column_Labels_Header_Row_Sep2016.tsv"
    gkg_schema = pd.read_csv(gkg_schema_url, sep='\t')
    gkg_headers = gkg_schema['tableId'].values
    # use nullable integer type Int64
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/gotchas.html#support-for-integer-na=
    gkg_meta = dict(zip(gkg_schema.index.values,
                        gkg_schema['dataType'].map({'STRING': 'str', 'INTEGER': 'Int64'})))
    #yield Output(gkg_meta)
    return gkg_headers, gkg_meta

# TODO make into op?
def fetch_gkg(timestamp):
    gkg_headers, gkg_meta = get_gkg_meta()
    # timestamp is UTC "YYYYMMDDHHMMSS"
    # TODO unzip first? then dask can read csv in chunks of blocksize
    # TODO is this utf8 or cp1252?
    try:
        ddf = dd.read_csv(f"http://data.gdeltproject.org/gdeltv2/{timestamp}.gkg.csv.zip",
                          sep='\t', encoding="utf8", on_bad_lines='skip', header=None,
                          blocksize=None, compression='zip', dtype=gkg_meta,
                          encoding_errors="ignore")
    except (UnicodeEncodeError, FileNotFoundError, EOFError):
        ddf = dd.from_array(np.zeros((2, 27)))
    ddf.columns = gkg_headers
    ddf['gkg_file'] = timestamp

    # extract country codes from V2Locations
    matches = ddf['V2Locations'].str.extractall(r'1#\w+#(?P<country>\w{2})#').compute()

    top_countries = []
    for n in range(1, 11):
        # get the nth item from each group
        # where level_0 is the ddf record's index
        grouped = matches.groupby(level=0).nth(n)
        top_countries.append(pd.DataFrame({f"country-{n}": grouped.values.reshape(1, -1)[0]},
                                          index=grouped.index))
    # merge list of dataframes
    _matches = functools.reduce(lambda x, y: dd.merge(x, y,
                                                      left_index=True,
                                                      right_index=True), top_countries)

    # get first few unique countries
    _df = _matches.apply(lambda x : pd.Series(x.unique()[:4]),axis=1)
    _df = _df.rename(columns = dict([(n, f"country-{n+1}") for n in range(4)]))

    # join new country columns to ddf
    ddf = ddf.join(_df[[f"country-{n}" for n in range(1, 4)]])
    # drop records that don't have any country
    ddf = ddf.dropna(subset=['country-1'])
    return ddf

@asset(
    partitions_def=daily_partitions_def,
    code_version="1",
	io_manager_key="parquet_io_manager"
)
def gkg(context) -> dd.DataFrame:
    partition_key = context.asset_partition_key_for_output()
    partition_date_str = partition_key.split('|')[0]
    partition_start = datetime.datetime.strptime(partition_date_str, '%Y%m%d%H%M%S')

    every_fifteen_timestamps = list(every_n_mins_between(partition_start,
                                                         partition_start + datetime.timedelta(days=1)))

    ddf = dd.concat(list(map(fetch_gkg, every_fifteen_timestamps)))
    ddf = ddf.drop_duplicates(subset=['DocumentIdentifier'])
    ddf = ddf.drop_duplicates(subset=['GKGRECORDID'])
    ddf = ddf.repartition(partition_size='100MB')

    context.log_event(
        ExpectationResult(
            success=len(ddf) > 0,
            description="ensure dataframe has rows",
            metadata={
                "partition_start": partition_date_str,
                "raw_count": len(ddf),
            },
        )
    )
    yield Output(ddf, metadata={"num_rows": len(ddf)})

# TODO make into op?
def fetch_gsg(timestamp, tmp_dir):
    pathlib.Path(tmp_dir).mkdir(parents=True, exist_ok=True)
    # timestamp is UTC "YYYYMMDDHHMMSS"
    gsg_filename = f"{timestamp}.gsg.docembed.json.gz"
    gsg_url = f"http://data.gdeltproject.org/gdeltv3/gsg_docembed/{gsg_filename}"

    gz_tmp = f"{tmp_dir}/{gsg_filename}"
    json_tmp = f"{tmp_dir}/{timestamp}.gsg.docembed.json"

    # get gdelt file
    if not pathlib.Path(gz_tmp).is_file():
        urllib.request.urlretrieve(gsg_url, gz_tmp)

    try:
        # make json generator
        with gzip.GzipFile(fileobj=open(gz_tmp, 'rb')) as gzipfile:
            content_str = gzipfile.read().decode('utf-8')
            records = json_decode_many(content_str)
    except EOFError:
        records = dict()

    # write decoded ndjson
    with open(json_tmp, 'w') as f:
        encoder = json.JSONEncoder()
        for record in iter(SerializableGenerator(records)):
            # explode column 'docembed' (list of floats) into 512 columns
            embeds = dict([(f'docembed-{n}', i) for n, i in enumerate(record['docembed'])])
            # ditch original column containing 512 item lists
            _ = record.pop('docembed')
            record.update(embeds)
            # write each record as a json object on a new line
            f.write(encoder.encode(record) + '\n')

    # dask reads ndjson in chunks
    ddf = dd.read_json(json_tmp, blocksize=2**28)
    ddf['gsg_file'] = timestamp
    return ddf

@asset(
    partitions_def=daily_partitions_def,
    code_version="1",
	io_manager_key="parquet_io_manager"
)
def gsg(context) -> pd.DataFrame:
    partition_key = context.asset_partition_key_for_output()
    # TODO make tmp_base configurable
    tmp_base ='/tmp/foresight'
    tmp_dir = f"{tmp_base}/{context.run.run_id}"
    partition_date_str = partition_key.split('|')[0]
    partition_start = datetime.datetime.strptime(partition_date_str, '%Y%m%d%H%M%S')

    every_fifteen_timestamps = list(every_n_mins_between(partition_start,
                                                         partition_start + datetime.timedelta(days=1)))
    # `fetch_gsg` is not an op, so it doesn't have access to `context`.
    # create a partial function that provides `tmp_dir`
    # to `fetch_gsg` that we can still use with `map`
    gsg_fetcher = functools.partial(fetch_gsg,
                                    tmp_dir=tmp_dir)
    ddf = dd.concat(list(map(gsg_fetcher, every_fifteen_timestamps)))
    ddf = ddf.drop_duplicates(subset=['url'])
    ddf = ddf.repartition(partition_size='100MB')

    context.log_event(
        ExpectationResult(
            success=len(ddf) > 0,
            description="ensure dataframe has rows",
            metadata={
                "partition_start": partition_date_str,
                "raw_count": len(ddf),
            },
        )
    )
    yield Output(ddf, metadata={"num_rows": len(ddf)})

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
def gdelt(context, gkg, gsg) -> pd.DataFrame:
    ddf = gkg.merge(gsg, left_on='DocumentIdentifier', right_on='url')

    ddf['year'] = ddf['date'].dt.year
    ddf['month'] = ddf['date'].dt.month
    ddf['yearmonth'] = ddf['date'].dt.strftime('%Y%m')
    ddf = ddf.repartition(partition_size='100MB')

    context.log_event(
        ExpectationResult(
            success=len(ddf) > 0,
            description="ensure dataframe has rows",
            metadata={
                "raw_count": len(ddf),
            },
        )
    )
    # clean up gsg temp files for this run
    try:
        # TODO make tmp_base configurable
        tmp_base ='/tmp/foresight'
        tmp_dir = f"{tmp_base}/{context.run.run_id}"
        shutil.rmtree(tmp_dir)
    except OSError:
        pass
    
    yield Output(ddf, metadata={"num_rows": len(ddf)})

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
