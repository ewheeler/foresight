# -*- coding: utf-8 -*-
"""modeling_with_real_data.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1KT4UZSqo4tWaeByHK7hNGkk6OJFRPfIy

This notebook uses `darts` python framework for timeseries models on mean embeddings.
"""

# !pip install darts

!curl ipinfo.io

# Commented out IPython magic to ensure Python compatibility.
# %matplotlib inline

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import random

from darts import TimeSeries
from darts.dataprocessing.transformers import Scaler
from darts.models import NBEATSModel

gcp_project = 'foresight-375620'
gcp_bucket = 'frsght'

import google.auth
from google.colab import auth

# authenticate with gcp
auth.authenticate_user()
credentials, project_id = google.auth.default()

!gcloud config set project $gcp_project
!echo "deb http://packages.cloud.google.com/apt gcsfuse-bionic main" > /etc/apt/sources.list.d/gcsfuse.list
!curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
!apt -qq update
!apt -qq install gcsfuse

# create colab instance directory
!mkdir -p $gcp_bucket
# mount gcp bucket to colab instance directory.
# at /content/gcp_bucket
!gcsfuse  --implicit-dirs --limit-bytes-per-sec -1 --limit-ops-per-sec -1 $gcp_bucket $gcp_bucket
!gcsfuse  --implicit-dirs  --stat-cache-ttl 12h --type-cache-ttl 12h --stat-cache-capacity 65536 --limit-bytes-per-sec -1 --limit-ops-per-sec -1 $gcp_bucket $gcp_bucket

!pip install gcsfs

"""The rest is run on a single country (AA)."""

df = pd.read_csv("/content/frsght/monthly_emb_means_w_acled_csv/AA_monthly_embedding_means.csv")
len(df)

df

def convert_emb_str_to_floats(row):
    return row['docembed_mean'].lstrip("'[").rstrip("]'").split()

emb_cols = [f'docembed_mean-{i}' for i in range(1, 513)]
df[emb_cols] = df['docembed_mean'].str.lstrip("'[").str.rstrip("]'").str.split(expand=True)
df.head()

series = TimeSeries.from_dataframe(
    df, time_col='window',
    value_cols=['Events', 'Fatalities']+emb_cols)
# plotting sometimes doesn't work in colab if running in the same runtime as
# darts installation. To fix restart runtime and start from imports (skip install)
series['Events'].plot()
series['Fatalities'].plot()

scaler = Scaler()
embeddings = series[emb_cols]
fatalities = series['Fatalities']
events = series['Events']
embeddings_scaled, fatalities_scaled, events_scaled = scaler.fit_transform([embeddings, fatalities, events])
embeddings_train, embeddings_val = embeddings_scaled.split_before(.85)
fatalities_train, fatalities_val = fatalities_scaled.split_before(.85)
events_train, events_val = events_scaled.split_before(.85)

fig, axs = plt.subplots(1, 2, figsize=(16, 6))
fatalities_train.plot(ax=axs[0], label='train')
fatalities_val.plot(ax=axs[0], label='val')
axs[0].set_title('Fatalities')
events_train.plot(ax=axs[1], label='train')
events_val.plot(ax=axs[1], label='val')
axs[1].set_title('Events')

"""Model without embeddings (only based on previous fatalities/events)."""

# Fatalities
model = NBEATSModel(input_chunk_length=3, output_chunk_length=1, random_state=42)
model.fit([fatalities_train, events_train], epochs=100, verbose=True);
pred_fatalities = model.predict(series=fatalities_train, n=6)
pred_events = model.predict(series=events_train, n=6)
_, pred_fatalities, pred_events = scaler.inverse_transform([embeddings_scaled, pred_fatalities, pred_events])

fig, axs = plt.subplots(1, 2, figsize=(16, 6))
fatalities.plot(ax=axs[0], label='actual')
pred_fatalities.plot(ax=axs[0], label='predicted')
axs[0].set_title('Fatalities')
events.plot(ax=axs[1], label='actual')
pred_events.plot(ax=axs[1], label='predicted')
axs[1].set_title('Events')

"""Model with embeddings."""

emb_model = NBEATSModel(input_chunk_length=3, output_chunk_length=1, random_state=42)
emb_model.fit([fatalities_train, events_train], epochs=100, past_covariates=[embeddings_scaled, embeddings_scaled], verbose=True)
emb_pred_fatalities = emb_model.predict(series=fatalities_train, past_covariates=embeddings_scaled, n=6)
emb_pred_events = emb_model.predict(series=fatalities_train, past_covariates=embeddings_scaled, n=6)
_, emb_pred_fatalities, emb_pred_events = scaler.inverse_transform([embeddings_scaled, emb_pred_fatalities, emb_pred_events])

fig, axs = plt.subplots(1, 2, figsize=(16, 6))
fatalities.plot(ax=axs[0], label='actual')
emb_pred_fatalities.plot(ax=axs[0], label='predicted')
axs[0].set_title('Fatalities')
events.plot(ax=axs[1], label='actual')
emb_pred_events.plot(ax=axs[1], label='predicted')
axs[1].set_title('Events')

