
from dagster import repository
from dagster import graph

from dags.ingestion.resources import gcp_container
from dags.ingestion.resources import local_storage
from dags.ingestion.assets import gdelt
from dags.ingestion.assets import acled 
from dags.ingestion.assets import training

@graph
def run_ingestion():
    return training(gdelt(), acled())

@repository
def local_ingestion_repository():
    #return [partitioned_asset_job.to_job(resource_defs={"foresight_gdelt": local_storage})]
    return [run_ingestion.to_job(resource_defs={"foresight_gdelt": local_storage})]

@repository
def cloud_ingestion_repository():
    #return [partitioned_asset_job.to_job(resource_defs={"foresight_gdelt": gcp_container})]
    return [run_ingestion.to_job(resource_defs={"foresight_gdelt": gcp_container})]
