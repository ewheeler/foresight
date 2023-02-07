import json
import gzip
import shutil
import datetime
import pathlib
import functools
import urllib.request

import numpy as np
import pandas as pd
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
from dags.utils import every_n_mins_between
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
def acled(context) -> pd.DataFrame:

    hdx_package_url = "http://data.humdata.org/api/3/action/package_show?id=political-violence-events-and-fatalities"
    with urllib.request.urlopen(hdx_package_url) as f:
        hdx_response = json.loads(f.read())
    # https://data.humdata.org/dataset/political-violence-events-and-fatalities
    # NOTE this url changes every week
    hdx_latest_resource_id = hdx_response['result']['resources'][0]['download_url'].split('/')[6]
    df = pd.read_excel(hdx_response['result']['resources'][0]['download_url'], sheet_name=1)
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
        df = pd.read_csv(f"http://data.gdeltproject.org/gdeltv2/{timestamp}.gkg.csv.zip",
                          sep='\t', encoding="utf8", on_bad_lines='skip', header=None,
                          compression='zip', encoding_errors="ignore")
                         
    except (UnicodeEncodeError, FileNotFoundError, EOFError):
        df = pd.DataFrame(np.zeros((2, 27)))
    df.columns = gkg_headers
    df['gkg_file'] = timestamp

    # extract country codes from V2Locations
    matches = df['V2Locations'].str.extractall(r'1#\w+#(?P<country>\w{2})#')
    # reformat results
    _matches = matches.unstack()
    _matches.columns = _matches.columns.droplevel()
    # drop duplicate mentions of country
    # list(dict.fromkeys()) trick gives a set that preserves order
    _df = pd.DataFrame(enumerate(list(map(lambda x: list(dict.fromkeys(x)),
                                          _matches.values))), index=_matches.index)

    _df = _df.rename(columns={1: 'countries'})
    # remove nans introduced by droplevel
    _df['countries'] = _df['countries'].map(lambda x: list(filter(lambda y: not pd.isna(y), x)))
    _df['num_countries'] = _df['countries'].map(len)
    df = df.join(_df[['countries', 'num_countries']])
    df['num_countries'] = df['num_countries'].fillna(0).astype('int')
    # add three columns with first three countries
    for n in range(1, 4):
        df[f"country-{n}"] = df['countries'].map(lambda c: c[n-1] if isinstance(c, list) and len(c) > n-1 else [])
    # replace empty lists in new country columns with empty strings
    for n in range(1, 4):
        df[f"country-{n}"] = df[f"country-{n}"].map(lambda c: '' if len(c)==0 else c)
    return df

@asset(
    partitions_def=daily_partitions_def,
    code_version="1",
	io_manager_key="parquet_io_manager"
)
def gkg(context) -> pd.DataFrame:
    partition_key = context.asset_partition_key_for_output()
    partition_date_str = partition_key.split('|')[0]
    partition_start = datetime.datetime.strptime(partition_date_str, '%Y%m%d%H%M%S')

    every_fifteen_timestamps = list(every_n_mins_between(partition_start,
                                                         partition_start + datetime.timedelta(days=1)))

    df = pd.concat(list(map(fetch_gkg, every_fifteen_timestamps)))
    df = df.drop_duplicates(subset=['DocumentIdentifier'])
    df = df.drop_duplicates(subset=['GKGRECORDID'])

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
    json_tmp = f"{tmp_dir}/{timestamp}.gsg.docembed.ndjson"

    # get gdelt file if its not present in tmp_dir
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
            _ = record.pop('docembed')
            record.update(embeds)
            f.write(encoder.encode(record) + '\n')

    # we wrote ndjson, so need the lines param
    df = pd.read_json(json_tmp, lines=True)
    df['gsg_file'] = timestamp
    return df

@asset(
    partitions_def=daily_partitions_def,
    code_version="1",
	io_manager_key="parquet_io_manager"
)
def gsg(context) -> pd.DataFrame:
    # TODO make tmp_base configurable
    tmp_base ='/tmp/foresight'
    tmp_dir = f"{tmp_base}/{context.run.run_id}"
    partition_key = context.asset_partition_key_for_output()
    partition_date_str = partition_key.split('|')[0]
    partition_start = datetime.datetime.strptime(partition_date_str, '%Y%m%d%H%M%S')

    every_fifteen_timestamps = list(every_n_mins_between(partition_start,
                                                         partition_start + datetime.timedelta(days=1)))

    # `fetch_gsg` is not an op, so it doesn't have access to `context`.
    # create a partial function that provides `tmp_dir`
    # to `fetch_gsg` that we can still use with `map`
    gsg_fetcher = functools.partial(fetch_gsg,
                                    tmp_dir=tmp_dir)
    df = pd.concat(list(map(gsg_fetcher, every_fifteen_timestamps)))
    df = df.drop_duplicates(subset=['url'])

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
def gdelt(context, gkg, gsg) -> pd.DataFrame:
    # pyarrow cannot currently join tables that have lists
    # https://github.com/apache/arrow/issues/32504
    # so instead of joining the two pyarrow.dataset.FileSystemDataset
    #df = gkg.join(gsg, keys='DocumentIdentifier', right_keys='url')
    # we convert to pyarrow.table and then to pandas.DataFrame to merge
    df_all = gkg.read_pandas().to_pandas().merge(gsg.read_pandas().to_pandas(),
                                                 left_on='DocumentIdentifier',
                                                 right_on='url')
    df_all['year'] = df_all['date'].dt.year
    df_all['month'] = df_all['date'].dt.month
    df_all['yearmonth'] = df_all['date'].dt.strftime('%Y%m')

    context.log_event(
        ExpectationResult(
            success=len(df_all) > 0,
            description="ensure dataframe has rows",
            metadata={
                "raw_count": len(df_all),
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
    yield Output(df_all, metadata={"num_rows": len(df_all)})

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
