import os
from google.cloud import storage
import numpy as np
import pandas as pd
import datetime
import numpy as np


import sys

pd.options.display.expand_frame_repr = False
pd.options.display.max_rows = 100


GCP_project = 'foresight-375620'
GCPClient = storage.Client(project=GCP_project)
bucket = GCPClient.bucket('frsght')

base_dir = 'gcs://frsght/datasets_stacked/gdelt'

tfrecord_dir = 'gcs://frsght/datasets_stacked/tfrecords_test'

###building directory list###
blobs = [b.name for b in bucket.list_blobs(prefix=base_dir)]
blobs = [f'gcs://frsght/{b}' for b in blobs]


###reading in ACLED labels
ACLED = pd.read_csv(os.path.normpath(os.environ['ACLED_Labels']))
ACLED['yearmonth'] = ACLED['Year'].astype(str) + ACLED['Month_Num'].astype(str).str.pad(2, fillchar = '0')

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

embedding_cols = [f'docembed-{i}' for i in range(512)]


def parse_gdelt_data(df, n, target_date, norm_days=30):
    df = df.copy()
    df['date_delta'] = (pd.to_datetime(df['date']) - target_date).dt.days
    df['date_delta'] = df['date_delta']/norm_days

    embeddings = df[embedding_cols + ['date_delta']]

    sample_size = int(np.floor(len(df)/n))

    embeddings = embeddings.sample(sample_size)

    return[embeddings.iloc[n*i:n*i+n].values for i in range(sample_size)]





for csv in blobs:
    df = pd.read_csv(csv)
    yearmonth = df['yearmonth'][0].values
    embeddings = df[embedding_cols]
    label = ACLED['theta']

