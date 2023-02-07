import hashlib
import datetime
from upath import UPath
from typing import Dict

import gcsfs
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from dagster import (
    Field,
    InitResourceContext,
    InputContext,
    OutputContext,
    StringSource,
    UPathIOManager,
    MetadataValue,
    AssetObservation,
    io_manager,
)


class PyArrowParquetIOManager(UPathIOManager):
    extension: str = ".parquet"

    def _month_dir(self, path: UPath):
        # TODO probably should just re-define UPathIOManager._get_paths_for_partitions
        # https://docs.dagster.io/_modules/dagster/_core/storage/upath_io_manager#UPathIOManager
        # like
        # https://github.com/dagster-io/dagster/blob/master/examples/project_fully_featured/project_fully_featured/resources/parquet_io_manager.py#L61
        day_start = datetime.datetime.strptime(str(path.stem), '%Y%m%d%H%M%S')
        # rather than write each day's asset partition as a separate 
        # parquet partition on disk, we will write one parquet per month
        month_start = day_start.replace(day=1)
        month_path = UPath(self._base_path) / path.parts[-2]/  f"{month_start.strftime('%Y%m%d')}"
        return month_path

    def get_metadata(self, context:OutputContext, obj: pd.DataFrame) -> Dict[str, MetadataValue]:
        # TODO
        return dict()

    def dump_to_path(self, context: OutputContext, obj: pd.DataFrame, path: UPath):
        # make key for dataset schema based on column names
        dataset_key = hashlib.md5(''.join(obj.columns.values).encode()).hexdigest()[:6]
        dataset_created = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
        dataset_path = UPath(self._base_path) / path.parts[-2]

        write_options = ds.ParquetFileFormat().make_write_options(compression='snappy')
        partitions = ds.partitioning(pa.schema([("year", pa.int16()),
                                                ("month", pa.string())]))
        table = pa.Table.from_pandas(obj, preserve_index=False)

        day_start = datetime.datetime.strptime(str(path.stem), '%Y%m%d%H%M%S')
        # NOTE pyarrow.write_dataset can write feather, orc, csv, etc
        # so we could read a format from init_context
        # for writing and reading--*if* partitioning/appending
        # works the same way (and use pa.write_dataset instead
        # of pq.write_to_dataset)
        basename_template = "part-{:%Y%m%d}-{{i}}_{}.parquet".format(day_start, dataset_key)

        written_paths = []
        written_metadata = []
        written_sizes = []

        def file_visitor(written_file):
            written_paths.append(written_file.path)
            written_metadata.append(written_file.metadata)
            written_sizes.append(written_file.size)

        ds.write_dataset(table, dataset_path,
                         basename_template=basename_template,
                         existing_data_behavior="overwrite_or_ignore",
                         format="parquet", file_options=write_options,
                         partitioning=partitions,
                         file_visitor=file_visitor)

        # Write the ``_common_metadata`` parquet file without row groups statistics
        #pq.write_metadata(table.schema, dataset_path / '_common_metadata')
        pq.write_metadata(table.schema, dataset_path / '_common_metadata')

        # Write the ``_metadata`` parquet file with row groups statistics of all files
        # this doesnt seem to be working..
        # https://github.com/apache/arrow/issues/19053
        # https://github.com/dask/dask/issues/4194
        # https://github.com/dask/dask/issues/6243
        #pq.write_metadata(table.schema, dataset_path / '_metadata',
        #                  metadata_collector=written_metadata)
        pq_partition = datetime.datetime.strptime(str(path.stem),
                                                  '%Y%m%d%H%M%S').strftime('%Y/%m')
        asset_obs_kwargs = {'asset_key': path.parts[-2],
                            'metadata': {"num_rows": len(obj),
                                         "schema_key": dataset_key,
                                         "dataset_created": dataset_created,
                                         "path": str(dataset_path),
                                         "pq_partition" : pq_partition,
                                         "pq_path": str(dataset_path / pq_partition),
                                         "size (bytes)": sum(written_sizes)}}
        if context.has_asset_partitions:
            start, _ = context.asset_partitions_time_window
            dt_format = "%Y/%m"
            partition_str = start.strftime(dt_format)
            asset_obs_kwargs.update({'partition': partition_str})
        context.log_event(AssetObservation(**asset_obs_kwargs))

    def load_from_path(self, context: InputContext, path: UPath) -> pq.ParquetDataset:
        dataset_path = UPath(self._base_path) / path.parts[-2]
        dataset = pq.ParquetDataset(dataset_path, use_legacy_dataset=False)
        return dataset

@io_manager(config_schema={"base_path": Field(str, is_required=False)})
def local_parquet_io_manager(
    init_context: InitResourceContext,
) -> PyArrowParquetIOManager:
    assert init_context.instance is not None  # to please mypy
    base_path = UPath(
        init_context.resource_config.get(
            "base_path", init_context.instance.storage_directory()
        )
    )
    return PyArrowParquetIOManager(base_path=base_path)

@io_manager(
    config_schema={
        "base_path": Field(str, is_required=True),
        "AWS_ACCESS_KEY_ID": StringSource,
        "AWS_SECRET_ACCESS_KEY": StringSource,
    }
)
def s3_parquet_io_manager(init_context: InitResourceContext) -> PyArrowParquetIOManager:
    # `UPath` will read boto env vars.
    # The credentials can also be taken from the config and passed to `UPath` directly.
    base_path = UPath(init_context.resource_config.get("base_path"))
    assert str(base_path).startswith("s3://"), base_path
    return PyArrowParquetIOManager(base_path=base_path)

@io_manager(
    config_schema={
        "base_path": Field(str, is_required=True),
        "project": StringSource,
        "token": StringSource,
    }
)
def gcp_parquet_io_manager(init_context: InitResourceContext) -> PyArrowParquetIOManager:
    # `UPath` will read boto env vars.
    # The credentials can also be taken from the config and passed to `UPath` directly.
    base_path = UPath(init_context.resource_config.get("base_path"))
    fs = gcsfs.GCSFileSystem(token=init_context.resource_config.get("token"),
                             project=init_context.resource_config.get("project"))
    base_path._accessor._fs = fs
    assert str(base_path).startswith("gs://"), base_path
    return PyArrowParquetIOManager(base_path=base_path)
