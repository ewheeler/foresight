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

# + colab={"base_uri": "https://localhost:8080/"} id="3hc2zoJez8HN" outputId="865f9fa7-ec65-4bd5-c3d2-b2ca59ffb299"
# frsght bucket is in Iowa (us-central1-c) and
# latency will be _much_ lower (and no egress costs)
# if colab runtime is located nearby.
# so disconnect/delete and reprovision runtime
# until landing in North America, at least
# !curl ipinfo.io

# + id="Ja58Y7M5vRaK"
gcp_project = 'foresight-375620'
gcp_bucket = 'frsght'

# + id="XjMwejpcvKBP"
import google.auth
from google.colab import auth

# authenticate with gcp
auth.authenticate_user()
credentials, project_id = google.auth.default()

# + colab={"base_uri": "https://localhost:8080/"} id="6HLE3_bBvMKD" outputId="d2696b8b-85a8-41ad-c102-44a74b2f07eb"
# !gcloud config set project $gcp_project
# !echo "deb http://packages.cloud.google.com/apt gcsfuse-bionic main" > /etc/apt/sources.list.d/gcsfuse.list
# !curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
# !apt -qq update
# !apt -qq install gcsfuse

# + colab={"base_uri": "https://localhost:8080/"} id="tv1IejqOw7jC" outputId="7b96c59f-7bcf-4ef9-d422-1d1794cca083"
# create colab instance directory
# !mkdir -p $gcp_bucket
# mount gcp bucket to colab instance directory.
# at /content/gcp_bucket
# #!gcsfuse  --implicit-dirs --limit-bytes-per-sec -1 --limit-ops-per-sec -1 $gcp_bucket $gcp_bucket
# !gcsfuse  --implicit-dirs  --stat-cache-ttl 12h --type-cache-ttl 12h --stat-cache-capacity 65536 --limit-bytes-per-sec -1 --limit-ops-per-sec -1 $gcp_bucket $gcp_bucket

# + colab={"base_uri": "https://localhost:8080/"} id="gBXwMFd7wg67" outputId="fdca7ca2-d3d0-41e7-c2e9-eb13043eb3bb"
# !pip install gcsfs

# + colab={"base_uri": "https://localhost:8080/"} id="aK3NUMMM7yyC" outputId="7ffc4ff1-52e6-4003-d1a5-0a717466e06e"
# !python -m pip install dask distributed --upgrade

# + id="sW-eLL9YuFBb"
from dask.diagnostics import ProgressBar
pbar = ProgressBar()
pbar.register()

# + id="XzNCyAJeuHQz"
import os
import datetime
from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd
import dask.dataframe as dd

# + id="OSyp08CluoYv"
emb_cols = [f"docembed-{n}" for n in range(512)]

# + id="vPDPO1nKuqxL"
country_cols = [f"country-{n}" for n in range(1, 4)]

# + colab={"base_uri": "https://localhost:8080/"} id="Gw68K7TJuJrG" outputId="e3471259-0f70-485d-a4a4-d43e87732457"
# %%time
ddf = dd.read_parquet('/content/frsght/datasets_sample_b988c8_694a90_1f5902/gdelt/',
                      columns=['date', 'yearmonth'] + emb_cols + country_cols,
                      infer_divisions=True, engine='pyarrow')

# + colab={"base_uri": "https://localhost:8080/", "height": 309} id="Ekga5834wrxD" outputId="9c1a3672-bfa0-43fe-ee37-00af2884eb14"
ddf

# + colab={"base_uri": "https://localhost:8080/"} id="KDTgBbTeuOSh" outputId="2d7d2888-488c-4f1f-b0dc-8b6a60eafe6e"
# %%time
ddf['date'] = ddf['date'].dt.tz_localize(None)

# + colab={"base_uri": "https://localhost:8080/"} id="UEf_yPJGuQfs" outputId="f79a4403-ad7b-431d-8b25-54b73f73e73b"
# %%time
ddf['window'] = ddf['date'].dt.to_period('M')

# + id="t9He1_1yuXW1"
countries_seen = np.load('/content/frsght/countries_seen.npy', allow_pickle=True)

# + colab={"base_uri": "https://localhost:8080/"} id="2GBBZbT1bv1F" outputId="eb4bc667-b26a-457c-852b-9fe388f4da5c"
months = []

month = datetime.datetime(2020, 1, 1, 0, 0, 0)
this_month = datetime.datetime.today()

while month <= this_month:
    months.append(month)
    month += relativedelta(months=1)
print(len(months))

# + colab={"base_uri": "https://localhost:8080/"} id="4VQD1vluclCz" outputId="ae58f85c-fb17-4a1f-a579-7d3fe6b2fbd2"
# %%time
for month in months:
    print(month.strftime("%Y%m"))
    mdf = ddf[ddf["yearmonth"] == month.strftime("%Y%m")].compute()
    for country in countries_seen:
        rows = list()
        for n in range(1, 4):
            country_records = mdf[mdf[f"country-{n}"] == country]
            rows.append(country_records)
        cdf = pd.concat(rows, ignore_index=True)
        cmeans = cdf.groupby('window').mean()[emb_cols].apply(np.array, axis=1)
        out = f"/content/frsght/monthly_emb_means_csv/{country}_monthly_embedding_means.csv"
        cmeans.to_csv(out, mode='a', header=not os.path.exists(out))

