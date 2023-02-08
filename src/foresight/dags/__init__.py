import os
import importlib

import yaml

# read workspace.yaml to see which ingest/io implementations
# we are using, so we can import the correct iomanagers
workspace_conf = yaml.load(open(f"{os.getenv('DAGSTER_HOME')}/workspace.yaml").read(), yaml.Loader)
dag_file = workspace_conf['load_from'][0]['python_file']['relative_path'].split('/')[-1]
ingest_dag = dag_file.split('.')[0]
io_dag = ingest_dag.replace('ingestion', 'io')

mod = importlib.import_module(f".{io_dag}", "dags")
local_parquet_io_manager = getattr(mod, "local_parquet_io_manager")
gcp_parquet_io_manager = getattr(mod, "gcp_parquet_io_manager")

base_path = os.getenv("FORESIGHT_DATASETS_DIR", "datasets_sample")

# generally not a fan of anything in this file, but dagster docs said to put this in __init__.py
# https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets#example-1-per-environment-configuration

resources = {
    "local": {
        "parquet_io_manager": local_parquet_io_manager.configured(
            # TODO better to load base_path from env
            {
                "base_path": base_path,
                # TODO read and use this
                #"sample_size": os.getenv("FORESIGHT_GDELT_SAMPLE", "0.2")
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
                "base_path": f"gs://frsght/{base_path}",
                # TODO read and use this
                #"sample_size": os.getenv("FORESIGHT_GDELT_SAMPLE", "0.2"),
                "project": "foresight-375620",
                "token": os.getenv("GOOGLE_APPLICATION_CREDENTIALS",
                                   "foresight-375620-01e36cd13a77.json")
            }
        ),
    },
}

deployment_name = os.getenv("DAGSTER_DEPLOYMENT", "local")
