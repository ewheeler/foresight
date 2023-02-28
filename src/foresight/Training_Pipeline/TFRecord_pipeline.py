import os
from google.cloud import storage
import numpy as np
import pandas as pd
import datetime
import numpy as np

import sys

pd.options.display.expand_frame_repr = False
pd.options.display.max_rows = 100

#VARS -- ADJUST THESE TO CREATE NEW DATASET

n_article = 50 #number of articles per sample
y_var = 'Trend_Increasing' #Column in ACLED to use as y
n_months = 1 # number of months to sample for features
lag_time = 2 # months between last feature month and label month

gdelt_dir = 'gcs://frsght/datasets_sample_b988c8_694a90_1f5902/gdelt' #WHERE TO GET FEATURES
tfrecord_dir = 'gcs://frsght/datasets_stacked/tfrecords_1' #WHERE TO SAVE OUTPUT

###Setting up Bucket####
GCP_project = 'foresight-375620'
GCPClient = storage.Client(project=GCP_project)
bucket = GCPClient.bucket('frsght')

blobs = [b.name for b in bucket.list_blobs(prefix='datasets_sample_b988c8_694a90_1f5902/gdelt')]
blobs = [b for b in blobs if b.endswith('parquet')]
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



#Prepping for parsing GDELT

yearmonths = [f'{y}{m}' for y in ['2020', '2021', '2022', '2023'] for m in
              ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']]

embedding_cols = [f'docembed-{i}' for i in range(512)]

def prog_bar(prog, total):
    pct = 100 * (prog / total)
    bar = '=' * int(pct) + '-' * (100 - int(pct))
    sys.stdout.write(f"\r|{bar}|{pct}")
    sys.stdout.flush()

def read_in_month(yearmonth, verbose = 1):
    files = [b for b in blobs if yearmonth in b]
    df = pd.DataFrame()
    prog = 0
    if len(files) > 0:
        for file in files:
            df = pd.concat([df, pd.read_parquet(file)])
            if verbose == 1:
                fprog = fprog + 1
                prog_bar(fprog, len(files))

    return df


def parse_gdelt_data(df, n):
    #todo implement better sequence embeddings
    df = df.copy()
    #df['date_delta'] = (pd.to_datetime(df['date']) - target_date).dt.days
    #df['date_delta'] = df['date_delta']/norm_days

   # embeddings = df[embedding_cols + ['date_delta']]

    n_samples = int(np.floor(len(df)/n))

    embeddings = df.sample(len(df))
    #returns list of nx512 np arrays. Each array is sorted by date within itself, but each array consists of a random sample of articles
    return[embeddings.iloc[n*i:n*i+n].sort_values('date')[embedding_cols].values for i in range(n_samples)]

def get_label(yearmonth, country, label_col):
    label = ACLED[(ACLED['yearmonth'] == yearmonth) & (ACLED['Code'] == country)][label_col].iloc[0]
    return label

def create_feature_dict(df, yearmonth, country, label_col, n, )

###Main loop###
##for ym in yearmonths[0:1]:
##    if ym not in completed_months:
##        files = [b for b in blobs if ym in b]
##        if len(files) > 0:
##            df = pd.DataFrame()
##            print('\nYearMonth:', ym)
##            print('Loading Data')
##            fprog = 0
##            for file in files:
##                df = pd.concat([df, pd.read_parquet(file)])
##                fprog = fprog + 1
##                prog_bar(fprog, len(files))
##            countries = pd.concat([df['country-1'], df['country-2'], df['country-3']]).unique()
##            countries = countries[countries != None]
##            cprog = 0
##            print('\nWriting Data:')
##            for country in countries:
##                country_df = df[
##                    (df['country-1'] == country) | (df['country-2'] == country) | (df['country-3'] == country)]
##                count = len(country_df)
##                filename = f'{ym}_{country}.csv'
##                metadata.loc[len(metadata.index)] = [filename, ym, country, count]
##                country_df.to_csv(f'{stacked_dir}/{filename}', index = False)
##                cprog = cprog + 1
##                prog_bar(cprog, len(countries))
##            metadata.to_csv(metadata_path, index = False)
##        else:
##            print('No files found for', ym)
##
##if __name__ == "__main__":

