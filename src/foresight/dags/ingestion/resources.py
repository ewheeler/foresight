
from dagster import resource
import fsspec

# TODO implement as IO manager
# https://docs.dagster.io/concepts/io-management/io-managers#defining-an-io-manager
@resource
def gcp_container(init_context):
    class GCPContainer:
        def __init__(self, resource_config):
            token = resource_config["token"]
            project = resource_config["project"]
            self.bucket = resource_config["bucket"]
            # storage_options can be passed to dask dataframe's `.to_parquet`
            self.storage_options = {"token": token,
                                    "project": project}
            # fspec-compatible filesystem interface to GCP storage
            self.fs = fsspec.filesystem('gcs', **self.storage_options)
            self.prefix = "gs://"
    return GCPContainer(init_context.resource_config)

# TODO implement as IO manager
# https://docs.dagster.io/concepts/io-management/io-managers#defining-an-io-manager
@resource
def local_storage(init_context):
    class LocalStorage:
        def __init__(self, resource_config):
            token = None
            project = resource_config["project"]
            self.bucket = resource_config["bucket"]
            # storage_options can be passed to dask dataframe's `.to_parquet`
            self.storage_options = {"token": None,
                                    "project": project}
            # fspec-compatible filesystem interface to local disk
            self.fs = fsspec.filesystem('file')
            self.prefix = "file://"
    return LocalStorage(init_context.resource_config)
