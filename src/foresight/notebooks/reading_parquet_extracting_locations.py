# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# Using GCP storage from Colab

# +
GCP_PROJECT_ID = 'foresight-375620'
PROJECT_NUMBER = '4591936303'

import os
import sys
import json

# check for colab
IN_COLAB = 'google.colab' in sys.modules

if IN_COLAB:
    import google.auth
    from google.colab import auth

    # authenticate with gcp
    auth.authenticate_user()
    credentials, project_id = google.auth.default()

# +
# #!pip install universal_pathlib gcsfs
# -

# ## Reading Parquet datasets with PyArrow and Pandas

# In most cases, datasets will be read from cloud storage. To authenticate to GCP from your local machine:
#  - install gcloud cli (https://cloud.google.com/sdk/docs/install or https://formulae.brew.sh/cask/google-cloud-sdk)
#  - follow instructions to initialize/configure and when prompted, select project 'foresight-375620' as your default
#  - set up [Application Default Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc) by running `gcloud auth application-default login`
#  
# Now, tools that support [fsspec](https://filesystem-spec.readthedocs.io/en/latest) (like Dask, pandas, xarray, etc) will automagically use your ADC to authenticate with GCP when supplied with a file path starting with `gcs://` (and/or `gs://`).
#
# [universal_pathlib](https://github.com/fsspec/universal_pathlib) is an implementation of Python stdlib's `pathlib.Path` with fsspec support.
#
# Foresight datasets are stored in a bucket called `frsght` in a 'directory' called `datasets`:
#  - `gkg` are data from GDELT's Global Knowledge Graph, with a couple additions
#      - `gkg_file` is the name of the '\*csv.zip' file retrieved from GDELT
#      - `countries` is a set of ISO2 (presumably) country codes extracted from `V2Locations` (this process is shown below)
#  - `gsg` are data from GDELT's Global Similarity Graph with a `gsg_file` column added
#  - `gdelt` are a subset of the resulting columns after joining the `gkg` and `gsg` data on article url with a couple convenience columns derived from `date` for partitioning and grouping (`year`, `month`, `yearmonth`)
#       - `gdelt` is partitioned by `year` and then by `month`, so the parquet's individual part files for a given month would be on disk at `gcs://frsght/datasets/gdelt/2020/1/part-20200102-0.parquet`. Note that `year` and `month` may not appear as 'columns' when reading the parquet
#       - as the Dagster pipeline is partitioned by day, each part corresponds to a specific day which is included in the part's filename. Note there could be multiple parts for a single day (e.g., `part-20200102-0.parquet` and `part-20200102-1.parquet`)
#       - partitioning is for performance purposes as partitioned predicates can be 'pushed down' and handled by the storage layer during reads. tools for reading parquet files will handle this transparently

# +
import logging
import warnings
import operator
import datetime

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from upath import UPath
# -

# pandas can read parquet files directly (and can get a subset of columns)
# and although by default pandas uses pyarrow under the hood to read/write
# parquet files, it has the extra step of converting columns to numpy arrays
try:
    df = pd.read_parquet('../datasets/gkg', columns=['V2Locations'])
except FileNotFoundError:
    pass

# pyarrow has some additional, useful functionality compared to pandas.
# pyarrow.parquet.ParquetDataset can read metadata without loading data into RAM
# https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html#pyarrow.parquet.ParquetDataset
gkg_ds = pq.ParquetDataset('gcs://frsght/datasets/gkg', use_legacy_dataset=False)

# we can inspect the files that consitute the `gkg` parquet
# note that these are not partitioned, although each part's name includes it's day
gkg_ds.files

# we can inspect the schema
gkg_ds.schema

# Each day's `gdelt` dataset's compressed partition is around 500MB to 2.5GB in size and contains ~120k to ~700k rows.
# But! Not all records have anything in `countries` (there was nothing to extract from `V2Locations`), so we can use pyarrow to filter out records lacking values for `countries` without reading them into memory first.

# +
# %%time
# this is fast because we're just reading parquet metadata.
# we can use filters to load only certain rows conditionally
# and could also apply expressions as 'projections' that will
# transform data as it is read from disk
# https://arrow.apache.org/docs/python/dataset.html#projecting-columns
# https://arrow.apache.org/docs/python/compute.html

pqds = pq.ParquetDataset('gcs://frsght/datasets/gdelt',
#pqds = pq.ParquetDataset('../datasets/gdelt',
                         use_legacy_dataset=False,
                         filters=[('num_countries', '>', 0),
                                  ('lang', '=', 'ENGLISH')])
# -

pqds.files

pqds.schema

# +
# lets see how big the compressed parquet is on disk

pqds_size = sum((UPath(f"gcs://{f}").stat()['size'] for f in pqds.files))
# -

print("file size: {} MB".format(pqds_size >> 20))

emb_cols = [f"docembed-{n}" for n in range(512)]

# %%time
# read specific subset of columns from the dataset into memory as a pyarrow.Table
# (`read_pandas` param means it will read any pandas-specific metatdata that
#  was included when writing the parquet file, like the schema metadata
#  seen in the schema output above)
# NOTE that the pyarrow.Table API has some useful operations that
# may be more computationally efficient than pandas
# https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table
table = pqds.read_pandas(columns=['countries', 'date'] + emb_cols)

# lets see how big our filtered subset is in memory
print("RSS (RAM): {} MB".format(pa.total_allocated_bytes() >> 20))

table.shape

# +
# %%time
# we could convert to pandas DataFrame
# (though this may not be feasible- in the
#  worst case, 2x memory will be needed)

#df = table.to_pandas()

# pyarrow can convert in blocks and delete each column's
# arrow memory buffer along the way
df = table.to_pandas(split_blocks=True, self_destruct=True)
del table  # not necessary, but a good practice
# -

# add a time period to use as a window
df['window'] = df['date'].dt.to_period('D')
df

# calculate mean of embedddings for the window
# and collapse embedding columns back into lists
df_means = df.groupby('window').mean()[emb_cols].apply(np.array, axis=1)
df_means

# ## Reading Parquet datasets with Dask

# Dask lazily constructs a graph of operations that it can execute over a big dataset in chunks (and can distribute the computation over many processes or machines)

from dask.distributed import Client
client = Client(n_workers=4, threads_per_worker=2, processes=True,
                memory_limit='4GB', silence_logs=logging.ERROR)

client

from dask.diagnostics import ProgressBar
pbar = ProgressBar()
pbar.register()

import dask.dataframe as dd

filters=[('num_countries', '>', 0)]
ddf = dd.read_parquet('gcs://frsght/datasets/gdelt', columns=['countries', 'date'] + emb_cols,
                      split_row_groups=True, infer_divisions=True, filters=filters, engine='pyarrow')
#ddf = dd.read_parquet('../datasets/gdelt', columns=['countries', 'date'] + emb_cols)

_dates = ddf[['DATE', 'date']]
_dates.compute()

_dates['DATE'] = _dates['DATE'].map(str).map(lambda x: pd.to_datetime(x, format='%Y%m%d%H%M%S', errors='coerce'))

_dates['delta'] = _dates.apply(lambda r: r['DATE'] - r['date'], axis=1)

dates = _dates.compute()
dates

sum(dates['agree'])

dates[dates['delta'] > datetime.timedelta(minutes=5)]

ddf.npartitions

# %%time
ddf['date'] = ddf['date'].dt.tz_localize(None)

# %%time
ddf['window'] = ddf['date'].dt.to_period('D')

# %%time
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    # emits _lots_ of warnings about dropping timezone info
    window_record_counts = ddf['window'].value_counts().compute()

window_record_counts

# %%time
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    # emits _lots_ of warnings about dropping timezone info
    countries_record_counts = ddf['countries'].value_counts().compute()

countries_record_counts

countries_rows = ddf.explode('countries')

# %%time
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    # emits _lots_ of warnings about dropping timezone info
    ddf_means = ddf.groupby('window').mean()[emb_cols].apply(np.array, axis=1).compute()

ddf_means

# TODO why do the pd and dask means differ?!
df_means - ddf_means

# ## Extracting country codes from V2Locations

import re

# regex to find iso2 country code within a location starting with 1
re.search(r'1#\w+#(?P<country>\w{2})#', '1#Taiwan#TW#TW##24#121#TW#5874;').group('country')

# example V2Locations for a single record
txt = """
2#Iowa, United States#US#USIA##42.0046#-93.214#IA#6714;4#Wuhan, Hubei, China#CH#CH12#13124#30.5833#114.267#-1930776#726;1#Taiwan#TW#TW##24#121#TW#5774;1#Taiwan#TW#TW##24#121#TW#5874;3#University Of Nebraska Medical Center, Nebraska, United States#US#USNE#NE055#41.255#-95.9767#834298#3513;1#Americans#US#US##39.828175#-98.5795#US#3318;2#California, United States#US#USCA##36.17#-119.746#CA#2950;2#California, United States#US#USCA##36.17#-119.746#CA#3024;2#California, United States#US#USCA##36.17#-119.746#CA#3377;1#Chinese#CH#CH##35#105#CH#316;1#Chinese#CH#CH##35#105#CH#1921;1#Chinese#CH#CH##35#105#CH#1974;1#Chinese#CH#CH##35#105#CH#5362;3#Travis Air Force Base, California, United States#US#USCA#CA095#38.2685#-121.934#2512359#3103;4#Haneda, Wakayama, Japan#JA#JA43#36528#33.75#135.333#-228452#4354;4#London, London, City Of, United Kingdom#UK#UKH9#40110#51.5#-0.116667#-2601889#6738;2#Texas, United States#US#USTX##31.106#-97.6475#TX#3032;2#Texas, United States#US#USTX##31.106#-97.6475#TX#3283;3#Des Moines, Iowa, United States#US#USIA#IA153#41.6005#-93.6091#465961#6692;4#Beijing, Beijing, China#CH#CH22#13001#39.9289#116.388#-1898541#6658;4#Hubei, Guangdong, China#CH#CH30#13047#23.2656#116.054#10073728#762;4#Hubei, Guangdong, China#CH#CH30#13047#23.2656#116.054#10073728#1763;3#Lackland Air Force Base, Texas, United States#US#USTX#TX029#29.3878#-98.6064#2512527#3259;3#Lackland Air Force Base, Texas, United States#US#USTX#TX029#29.3878#-98.6064#2512527#4942;1#Japanese#JA#JA##36#138#JA#2423;1#Japanese#JA#JA##36#138#JA#4439;1#Japanese#JA#JA##36#138#JA#6068;1#Japanese#JA#JA##36#138#JA#6347;2#Nebraska, United States#US#USNE##41.1289#-98.2883#NE#2961;2#Nebraska, United States#US#USNE##41.1289#-98.2883#NE#3454;2#Nebraska, United States#US#USNE##41.1289#-98.2883#NE#3498;2#Nebraska, United States#US#USNE##41.1289#-98.2883#NE#3595;1#Japan#JA#JA##36#138#JA#4361;1#Japan#JA#JA##36#138#JA#6514;1#American#US#US##39.828175#-98.5795#US#2818;1#American#US#US##39.828175#-98.5795#US#4278;3#San Francisco, California, United States#US#USCA#CA075#37.7749#-122.419#277593#3139;3#San Francisco, California, United States#US#USCA#CA075#37.7749#-122.419#277593#6791;1#China#CH#CH##35#105#CH#25;1#China#CH#CH##35#105#CH#733;1#China#CH#CH##35#105#CH#756;1#China#CH#CH##35#105#CH#769;1#China#CH#CH##35#105#CH#826;1#China#CH#CH##35#105#CH#906;1#China#CH#CH##35#105#CH#1086;1#China#CH#CH##35#105#CH#1770;1#China#CH#CH##35#105#CH#1805;1#China#CH#CH##35#105#CH#2720;1#China#CH#CH##35#105#CH#5708;1#China#CH#CH##35#105#CH#5764;1#China#CH#CH##35#105#CH#5866;1#China#CH#CH##35#105#CH#6258;1#China#CH#CH##35#105#CH#6665
"""

# TODO confirm significance of the integers at the beginning
# of each location description
# use same regex to find all of them for a given record's V2Locations
re.findall(r'1#\w+#(?P<country>\w{2})#', txt)

try:
    df = pd.read_parquet('../datasets/gkg', columns=['V2Locations'])
except FileNotFoundError:
    pass

# + tags=[]
df.fillna("", inplace=True)
# -

pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', -1)

# get a few records
df.head()['V2Locations']

# use pandas' extractall to apply regex to dataframe
matches = df.head()['V2Locations'].str.extractall(r'1#\w+#(?P<country>\w{2})#')

# this returns a new multi-index dataframe of the results
# that we'll need to reformat in order to add a 'countries'
# column to the original dataframe
matches

# unstack multiindex
_matches = matches.unstack()

# drop one level
_matches.columns = _matches.columns.droplevel()

# now we have one row per record with
# variable numbers of column values
_matches

# create temporary dataframe with unique countries seen in each row
# as a column containing lists while preserving index that 
# references original dataframe indices
# list(dict.fromkeys()) trick gives a set that preserves order
_df = pd.DataFrame(enumerate(list(map(lambda x: list(dict.fromkeys(x)),_matches.values))), index=_matches.index)
print(_df)
_df = _df.rename(columns={1: 'countries'})
# lists contain NaNs since the number of extracted countries varies per record,
# so map a crazy double lambda expression to remove them
_df['countries'] = _df['countries'].map(lambda x: list(filter(lambda y: not pd.isna(y), x)))
# pyarrow filters don't support list columns, so
# add a column we can use to filter out records
# that don't have any countries
_df['num_countries'] = _df['countries'].map(len)
_df

# finally, join the new columns to the original dataframe
new_df = df.head().join(_df[['countries', 'num_countries']])
new_df['num_countries'] = new_df['num_countries'].fillna(0).astype('int')

print(new_df)

for n in range(1, 4):
    new_df[f"country-{n}"] = new_df['countries'].map(lambda c: c[n-1] if isinstance(c, list) and len(c) > n-1 else [])

for n in range(1, 4):
    new_df[f"country-{n}"] = new_df[f"country-{n}"].map(lambda c: '' if len(c)==0 else c)

new_df


