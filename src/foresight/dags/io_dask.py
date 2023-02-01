import hashlib
import datetime
from upath import UPath

import gcsfs
import pyarrow as pa
import dask.dataframe as dd
from dagster import (
    Field,
    InitResourceContext,
    InputContext,
    OutputContext,
    StringSource,
    UPathIOManager,
    AssetObservation,
    io_manager,
)


class DaskParquetIOManager(UPathIOManager):
    extension: str = ".parquet"

    def _rewrite_path_day_to_month(self, path: UPath):
        day_start = datetime.datetime.strptime(str(path.stem), '%Y%m%d%H%M%S')
        # rather than write each day's asset partition as a separate 
        # parquet partition on disk, we will write one parquet per month
        month_start = day_start.replace(day=1)
        month_path = UPath(self._base_path) / path.parts[-2]/  f"{month_start.strftime('%Y%m%d%H%M%S')}{self.extension}"
        return month_path

    def dump_to_path(self, context: OutputContext, obj: dd, path: UPath):
        dataset_key = hashlib.md5(''.join(obj.columns.values).encode()).hexdigest()[:6]
        dataset_created = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
        dataset_path = UPath(self._base_path) / path.parts[-2]

        # object dtypes (like a list of embeddings), must be specified
        # so pyarrow can write the parquet file correctly
        schema = "infer"
        if 'docembed' in obj.columns:
            schema = {"docembed": pa.list_(pa.float32())}

        day_start = datetime.datetime.strptime(str(path.stem), '%Y%m%d%H%M%S')
        part_namer = lambda n: "part-{:%Y%m%d}-{}_{}.parquet".format(day_start, n, dataset_key)

        # `to_parquet` arguments for all datasets
        pq_args = {'path': dataset_path, 'schema': schema, 'ignore_divisions': True,
                   'append': dataset_path.exists(), 'engine': 'pyarrow',
                   'write_metadata_file': True, 'write_index': False,
                   'name_function': part_namer}
        if 'year' in obj.columns and 'month' in obj.columns:
            pq_args.update({'partition_on': ['year', 'month']})

        # TODO writes can fail if multiple runs try to append to a parquet
        # at the same time. there doesn't seem to be a great way to
        # handle this (on aws at least) https://stackoverflow.com/q/38964736
        # doesn't seem to lead to corrupt metadata as retrying the run works
        # for now we'll retry at the job/run level, but if it
        # happens a lot we can catch pyarrow.lib.ArrowInvalid and retry here
        obj.to_parquet(**pq_args)

        pq_partition = datetime.datetime.strptime(str(path.stem),
                                                  '%Y%m%d%H%M%S').strftime('%Y/%m')
        asset_obs_kwargs = {'asset_key': path.parts[-2],
                            'metadata': {"num_rows": len(obj),
                                         "schema_key": dataset_key,
                                         "dataset_created": dataset_created,
                                         "path": str(dataset_path),
                                         "pq_partition" : pq_partition,
                                         "pq_path": str(dataset_path / pq_partition),}}
                                         
        if context.has_asset_partitions:
            start, _ = context.asset_partitions_time_window
            dt_format = "%Y/%m"
            partition_str = start.strftime(dt_format)
            asset_obs_kwargs.update({'partition': partition_str})
        context.log_event(AssetObservation(**asset_obs_kwargs))

    def load_from_path(self, context: InputContext, path: UPath) -> dd:
        dataset_path = UPath(self._base_path) / path.parts[-2]
        return dd.read_parquet(dataset_path)

@io_manager(config_schema={"base_path": Field(str, is_required=False)})
def local_parquet_io_manager(
    init_context: InitResourceContext,
) -> DaskParquetIOManager:
    assert init_context.instance is not None  # to please mypy
    base_path = UPath(
        init_context.resource_config.get(
            "base_path", init_context.instance.storage_directory()
        )
    )
    return DaskParquetIOManager(base_path=base_path)

@io_manager(
    config_schema={
        "base_path": Field(str, is_required=True),
        "AWS_ACCESS_KEY_ID": StringSource,
        "AWS_SECRET_ACCESS_KEY": StringSource,
    }
)
def s3_parquet_io_manager(init_context: InitResourceContext) -> DaskParquetIOManager:
    # `UPath` will read boto env vars.
    # The credentials can also be taken from the config and passed to `UPath` directly.
    base_path = UPath(init_context.resource_config.get("base_path"))
    assert str(base_path).startswith("s3://"), base_path
    return DaskParquetIOManager(base_path=base_path)

@io_manager(
    config_schema={
        "base_path": Field(str, is_required=True),
        "project": StringSource,
        "token": StringSource,
    }
)
def gcp_parquet_io_manager(init_context: InitResourceContext) -> DaskParquetIOManager:
    # `UPath` will read boto env vars.
    # The credentials can also be taken from the config and passed to `UPath` directly.
    base_path = UPath(init_context.resource_config.get("base_path"))
    fs = gcsfs.GCSFileSystem(token=init_context.resource_config.get("token"),
                             project=init_context.resource_config.get("project"))
    base_path._accessor._fs = fs
    assert str(base_path).startswith("gs://"), base_path
    return DaskParquetIOManager(base_path=base_path)
