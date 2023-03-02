# -*- coding: utf-8 -*-
"""modeling_with_sample_data.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1ie_Iwnwfs_DNDaGsJsuoscidYBCF0FLN

This notebook explores `darts` python framework for timeseries models. The dataset used here is a fake sample dataset with a structure similar (but not identical) to the real dataset. Some of the fields are fake (randomly generated) to imitate a real dataset.
"""

# !pip install darts

# Commented out IPython magic to ensure Python compatibility.
# %matplotlib inline

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import random

from darts import TimeSeries
from darts.dataprocessing.transformers import Scaler
from darts.models import NBEATSModel

# Download sample data from drive (only relevant for this sample)
from google.colab import drive
drive.mount('/content/drive/')
data_dir = "/content/drive/My Drive/sample gdelt data"
df = pd.read_parquet(f'{data_dir}/gdelt_embeddings.parquet')
len(df)
# 163107

df = df[df['country_label'].str.isalpha()]
len(df)
# 162862

df['country_label'].nunique()
# 196

# Pick countries with a lot of data
countries = df.groupby('country_label').count().sort_values(by='docembed-0', ascending=False).head(10).index
countries

averages = df.groupby(['country_label', 'fake_yearmonth']).mean()
us = averages.loc[["US"]]
us['fatalities'] = [random.choice([0, random.randrange(0, 100)]) for _ in range(len(us))]
us.reset_index(level=['fake_yearmonth'], inplace=True)
us

embedding_columns = [col for col in us.columns if col.startswith('docembed')]
series = TimeSeries.from_dataframe(us, time_col='fake_yearmonth')
# plotting sometimes doesn't work in colab if running in the same runtime as
# darts installation. To fix restart runtime and start from imports (skip install)
series['fatalities'].plot()

scaler = Scaler()
embeddings = series[embedding_columns]
fatalities = series['fatalities']
embeddings_scaled, fatalities_scaled = scaler.fit_transform([embeddings, fatalities])
embeddings_train, embeddings_val = embeddings_scaled.split_before(.9)
fatalities_train, fatalities_val = fatalities_scaled.split_before(.9)

fatalities_train.plot()
fatalities_val.plot()

"""Model without embeddings (only based on previous fatalities)."""

model = NBEATSModel(input_chunk_length=6, output_chunk_length=3, random_state=42)
model.fit([fatalities_train], epochs=5, verbose=True);
pred = model.predict(series=fatalities_train, n=8)
_, pred_unscaled = scaler.inverse_transform([embeddings_train, pred])

fatalities.plot(label='actual')
pred_unscaled.plot(label='predicted')

"""Model with embeddings."""

emb_model = NBEATSModel(input_chunk_length=6, output_chunk_length=3, random_state=42)
emb_model.fit([fatalities_train], epochs=5, past_covariates=embeddings_scaled, verbose=True);
emb_pred = emb_model.predict(series=fatalities_train, past_covariates=embeddings_scaled, n=7)
_, emb_pred_unscaled = scaler.inverse_transform([embeddings_scaled, emb_pred])

fatalities.plot(label='actual')
emb_pred_unscaled.plot(label='predicted')