import os

from .io_pandas import local_parquet_io_manager
from .io_pandas import gcp_parquet_io_manager

# generally not a fan of anything in this file, but dagster docs said to put this in __init__.py
# https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets#example-1-per-environment-configuration

resources = {
    "local": {
        "parquet_io_manager": local_parquet_io_manager.configured(
            # TODO better to load base_path from env
            {
                "base_path": "datasets",
            }
        ),
    },
    "production": {
        "parquet_io_manager": gcp_parquet_io_manager.configured(
            # TODO including ref to token file may not be necessary
            # when executing pipelines locally (in production mode).
            # i *think* that gcsfs will pick up credentials automagically
            # if the GOOGLE_APPLICATION_CREDENTIALS env var is set:
            # gcloud auth application-default login
            # export GOOGLE_APPLICATION_CREDENTIALS="/Users/ewheeler/.config/gcloud/application_default_credentials.json"
            # TODO better to load base_path and project from env
            {
                "base_path": "gs://frsght/datasets",
                "project": "foresight-375620",
                "token": "foresight-375620-01e36cd13a77.json"
            }
        ),
    },
}

deployment_name = os.getenv("DAGSTER_DEPLOYMENT", "local")
