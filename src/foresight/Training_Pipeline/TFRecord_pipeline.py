import os
from google.cloud import storage
import numpy as np
import pandas as pd
import datetime
import numpy as np

import tensorflow as tf
import sys

pd.options.display.expand_frame_repr = False
pd.options.display.max_rows = 100

#VARS -- ADJUST THESE TO CREATE NEW DATASET

n_article = 50 #number of articles per sample
y_var = 'Trend_Increasing' #Column in ACLED to use as y
y_var_feature_type = 'int' #kind of feature to use for y. Should be int or float
n_months = 1 # number of months to sample for features
lag_time = 2 # months between last feature month and label month

#gdelt_dir = 'datasets_sample_b988c8_694a90_1f5902/gdelt' #WHERE TO GET FEATURES
gdelt_dir = 'datasets_stacked/gdelt'
tfrecord_dir = 'gs://frsght/datasets_stacked/tfrecords_1' #WHERE TO SAVE OUTPUT

###Setting up Bucket####
GCP_project = 'foresight-375620'
GCPClient = storage.Client(project=GCP_project)
bucket = GCPClient.bucket('frsght')

blobs = [b.name for b in bucket.list_blobs(prefix=gdelt_dir)]
#blobs = [b for b in blobs if b.endswith('parquet')]
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

countries = set([b[-6:-4].strip() for b in blobs])


#LOGS

error_logs = []

readme = """




"""




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


def read_in_country_data(yearmonths, country,  verbose = 0):
    files = [b for b in blobs if any(ym in b for ym in yearmonths) and country in b]
    df = pd.DataFrame()
    prog = 0
    if len(files) > 0:
        for file in files:
            df = pd.concat([df, pd.read_csv(file)])
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
    if n_samples >0:
        embeddings = df.sample(len(df))
        embeddings = [embeddings.iloc[n * i:n * i + n].sort_values('date')[embedding_cols].values for i in range(n_samples)]
    else:
        embeddings = df.sort_values('date')[embedding_cols].values
        padding = n - embeddings.shape[0]
        embeddings = np.pad(embeddings, [(0, padding), (0, 0)])

    #returns list of nx512 np arrays. Each array is sorted by date within itself, but each array consists of a random sample of articles
    return embeddings

def get_label(yearmonth, country, label_col):
    label = ACLED[(ACLED['yearmonth'] == yearmonth) & (ACLED['Code'] == country)][label_col].iloc[0]
    return label

def parse_yearmonths(start, n_months):
    start_year = int(start[0:4])
    start_month = int(start[4:6])
    yearmonths = [start]
    year = start_year
    month = start_month
    for i in range(n_months):
        if month <= 1:
            month = 12
            year = year - 1
        else:
            month = month - 1

        write_year = str(year)
        write_month = str(month).rjust(2,'0')
        ym = write_year+write_month
        yearmonths.append(ym)

    return yearmonths

def build_example(target_country, target_yearmonth, lag, n_months, n_articles, label_col, y_type = 'int'):
    yearmonths = parse_yearmonths(target_yearmonth, lag+n_months)[lag:-1]
    df = read_in_country_data(yearmonths, target_country)
    embeddings = parse_gdelt_data(df, n_articles)
    label = get_label(target_yearmonth, target_country, label_col)


    examples = []

    for emb in embeddings:
        emb_shape = emb.shape
        emb_bytes = emb.tobytes()

        emb_feature = tf.train.Feature(bytes_list=tf.train.BytesList(value=[emb_bytes]))
        dim_feature_1 = tf.train.Feature(int64_list=tf.train.Int64List(value=[emb_shape[0]]))
        dim_feature_2 = tf.train.Feature(int64_list=tf.train.Int64List(value=[emb_shape[1]]))
        if y_type == 'int':
            label_feature = tf.train.Feature(int64_list=tf.train.Int64List(value=[int(label)]))
        elif y_type == 'float':
            label_feature = tf.train.Feature(int64_list=tf.train.FloatList(value=[int(label)]))

        feature_map = {'embeddings': emb_feature
                      ,'height'       : dim_feature_1
                      ,'width'        : dim_feature_2
                      ,'label'        : label_feature
                     }

        example = tf.train.Example(features=tf.train.Features(feature=feature_map))
        examples.append(example)

    return examples

def create_records(n_article = n_article, n_months = n_months, lag_time = lag_time, y_var = y_var, y_type = y_var_feature_type, verbose = 1):
    shard = []
    file_count = 0
    filename = f'record_{str(file_count).rjust(3)}.record'
    if verbose == 1:
        prog = 0
        prog_bar(prog, len(yearmonths))
    for ym in yearmonths:
        for country in countries:
            try:
                example = build_example(country, ym, lag_time, n_months, n_article, y_var, y_type = y_type)
                shard.extend(example)
            except:
                error_logs.append(f'Error on {ym}_{country}')
            if sys.getsizeof(shard) > 120000000:
                with tf.io.TFRecordWriter(f'{tfrecord_dir}/{filename}') as writer:
                    for example in shard:
                        writer.write(example.SerializeToString())
                file_count = file_count + 1
                filename = f'record_{str(file_count).rjust(3)}.record'
                shard = []
    if verbose == 1:
        prog = prog + 1
        prog_bar(prog, len(yearmonths))



