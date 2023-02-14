import os
from google.cloud import storage
import numpy as np
import pandas as pd

import dask.dataframe
import dask.array
from dask.diagnostics import ProgressBar
from dask.distributed import Client

import pyarrow.parquet as pq
import tensorflow.data as tfdata


pd.options.display.expand_frame_repr = False
pd.options.display.max_rows = 100

GCP_project = os.environ['GCP_project']

storage_client = storage.Client(project = GCP_project)

ACLED = pd.read_csv(os.path.normpath(os.environ['ACLED_Labels']))

codes = pd.read_html('https://www.worlddata.info/countrycodes.php')[0]

codes = dict(zip(codes['Country'], codes['Fips 10']))

#TODO fix FIPS code parsing for missing counties
ACLED['Code'] = ACLED['Country'].map(codes)

#GDELT = dask.dataframe.read_parquet('gcs://frsght/datasets/gdelt')

embedding_cols = [f'docembed-{i}' for i in range(512)]

@dask.delayed
def select_training_sample(dataframe, country, year, month):
    """
    Selects a training sample from a GDELT dataframe
    """
    embeddings = dataframe[
        ((dataframe['country-1'] == country)
           | (dataframe['country-2'] == country)
           | (dataframe['country-3'] == country))
        & (dataframe['month'] == month)
        & (dataframe['year'] == year)][embedding_cols]

    return embeddings



@dask.delayed
def load_training_sample(country, year, month):
    """
    Loads a training sample from GCP
    """
    filters = [('country-1' , '=', country),('year', '=', year), ('month', '=', month)]
    sample = dask.dataframe.read_parquet('gcs://frsght/datasets/gdelt', columns = embedding_cols, filters = filters)
    return sample

