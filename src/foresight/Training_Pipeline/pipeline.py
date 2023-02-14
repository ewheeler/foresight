import os
from google.cloud import storage
import numpy as np
import pandas as pd
import datetime

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
codes.update({
     'Bailiwick of Guernsey' : 'GK'
    ,'Bailiwick of Jersey' :'GE'
    ,'Czech Republic':'EZ'
    ,'Democratic Republic of Congo': 'CG'
    ,'eSwatini' :'WZ'
    ,'Micronesia':'FM'
    ,'Monaco':'MN'
    ,'Pitcairn':'PC'
    ,'Republic of Congo':'CF'
    ,'Saint-Barthelemy':'TB'
    ,'Saint-Martin':'RN'
    ,'South Georgia and the South Sandwich Islands': 'SX'
    ,'South Sudan':'OD'
    ,'United States': 'US'
})

ACLED['Code'] = ACLED['Country'].map(codes)

#GDELT = dask.dataframe.read_parquet('gcs://frsght/datasets/gdelt')

embedding_cols = [f'docembed-{i}' for i in range(512)]


def get_daterange(start_month, year, n_months):
    month_years = []
    month = start_month

    for i in range(n_months):
        if month <= 1:
            month = 12
            year = year - 1
        else:
            month = month - 1

        month_years.append((year, month))

    start_date = month_years[0]
    end_date = month_years[-1]

    date_range = (
        datetime.datetime(start_date[0], start_date[1], 1)
        , datetime.datetime(end_date[0], end_date[1], 1)
    )

    return date_range

@dask.delayed
def select_training_sample(dataframe, country, daterange):
    """
    Selects a training sample from a GDELT dataframe
    """
    embeddings = dataframe[
        ((dataframe['country-1'] == country)
           | (dataframe['country-2'] == country)
           | (dataframe['country-3'] == country))
        & (dataframe['DATE'] >= daterange[0])
        & (dataframe['DATE'] <= daterange[1])][embedding_cols]

    return embeddings



@dask.delayed
def load_training_sample(country, daterange):
    """
    Loads a training sample from GCP
    """
    start_date = daterange[0]
    end_date = daterange[1]
    filters = [('country-1' , '=', country),('DATE', '>=', start_date), ('DATE', '<=', end_date)]
    sample = dask.dataframe.read_parquet('gcs://frsght/datasets/gdelt', columns = embedding_cols, filters = filters)
    return sample


def create_labeled_sample(target_year, target_month, country, lookback, y_var, dataframe = None):
    """
    produces labeled training sample
    if dataframe provided, selects sample from frame
    otherwise, loads from GCP
    """
    daterange = get_daterange(target_month, target_year, lookback)
    y = ACLED[
          (ACLED['Year'] == target_year)
        & (ACLED['Month_Num'] == target_month)
        & (ACLED['Code'] == country)][y_var].values[0]

    if dataframe == None:
        X = load_training_sample(country, daterange)
    else:
        X = select_training_sample(dataframe,country, daterange)

    return X, y



