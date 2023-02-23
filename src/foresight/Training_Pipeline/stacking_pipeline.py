import os
from google.cloud import storage
import numpy as np
import pandas as pd
import datetime

import sys

pd.options.display.expand_frame_repr = False
pd.options.display.max_rows = 100


def prog_bar(prog, total):
    pct = 100 * (prog / total)
    bar = '=' * int(pct) + '-' * (100 - int(pct))
    sys.stdout.write(f"\r|{bar}|{pct}")
    sys.stdout.flush()


###Setting up filepaths####
GCP_project = 'foresight-375620'
GCPClient = storage.Client(project=GCP_project)
bucket = GCPClient.bucket('frsght')

gdelt_dir = 'gcs://frsght/datasets_sample_b988c8_694a90_1f5902/gdelt'

metadata_path = 'gcs://frsght/datasets_stacked/metadata.csv'

stacked_dir = 'gcs://frsght/datasets_stacked/gdelt'

###Loading Logs####
try:
    metadata = pd.read_csv(metadata_path)
except:
    metadata = pd.DataFrame(columns=['filename', 'yearmonth', 'country', 'count'])

completed_months = metadata['yearmonth'].unique()

###building directory list###
blobs = [b.name for b in bucket.list_blobs(prefix='datasets_sample_b988c8_694a90_1f5902/gdelt')]
blobs = [b for b in blobs if b.endswith('parquet')]
blobs = [f'gcs://frsght/{b}' for b in blobs]


###yearmonths###
yearmonths = [f'{y}{m}' for y in ['2020', '2021', '2022', '2023'] for m in
              ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']]


###Main loop###
for ym in yearmonths:
    if ym not in completed_months:
        files = [b for b in blobs if ym in b]
        if len(files) > 0:
            df = pd.DataFrame()
            print('\nYearMonth:', ym)
            print('Loading Data')
            fprog = 0
            for file in files:
                df = pd.concat([df, pd.read_parquet(file)])
                fprog = fprog + 1
                prog_bar(fprog, len(files))
            countries = pd.concat([df['country-1'], df['country-2'], df['country-3']]).unique()
            countries = countries[countries != None]
            cprog = 0
            print('\nWriting Data:')
            for country in countries:
                country_df = df[
                    (df['country-1'] == country) | (df['country-2'] == country) | (df['country-3'] == country)]
                count = len(country_df)
                filename = f'{ym}_{country}.csv'
                metadata.loc[len(metadata.index)] = [filename, ym, country, count]
                country_df.to_csv(f'{stacked_dir}/{filename}', index = False)
                cprog = cprog + 1
                prog_bar(cprog, len(countries))
            metadata.to_csv(metadata_path, index = False)
        else:
            print('No files found for', ym)