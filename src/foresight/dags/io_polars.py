import hashlib
import datetime
from upath import UPath

import gcsfs
import polars as pl
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from dagster import (
    Field,
    InitResourceContext,
    InputContext,
    OutputContext,
    StringSource,
    UPathIOManager,
    io_manager,
)


class PolarsParquetIOManager(UPathIOManager):
    extension: str = ".parquet"

    def dump_to_path(self, context: OutputContext, obj: pl.DataFrame, path: UPath):
        # make key for dataset schema based on column names
        dataset_key = hashlib.md5(''.join(obj.columns).encode()).hexdigest()[:6]
        dataset_created = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')

        write_options = ds.ParquetFileFormat().make_write_options(compression='snappy',
                                                                  coerce_timestamps='ms',
                                                                  allow_truncated_timestamps=True)
        table = obj.to_arrow()

        dataset, part = context.get_asset_identifier()
        dataset_path = UPath(self._base_path) / dataset
        dataset_part_path = dataset_path / f"{part}{self.extension}"

        basename_template = "part-{}-{{i}}_{}.parquet".format(part, dataset_key)

        written_paths = []
        written_metadata = []
        written_sizes = []

        def file_visitor(written_file):
            written_paths.append(written_file.path)
            written_metadata.append(written_file.metadata)
            written_sizes.append(written_file.size)

        # https://arrow.apache.org/docs/python/dataset.html#writing-large-amounts-of-data

        ds.write_dataset(table, dataset_part_path,
                         basename_template=basename_template,
                         existing_data_behavior="overwrite_or_ignore",
                         format="parquet", file_options=write_options,
                         max_rows_per_file=600000, max_rows_per_group=200000,
                         min_rows_per_group=20000,
                         file_visitor=file_visitor)

        try:
            # Write the ``_common_metadata`` parquet file without row groups statistics
            pq.write_metadata(table.schema, dataset_part_path / '_common_metadata')
        except FileNotFoundError:
            pass

        try:
            # Write the ``_metadata`` parquet file with row groups statistics of all files
            pq.write_metadata(table.schema, dataset_part_path / '_metadata',
                              metadata_collector=written_metadata)
        except FileNotFoundError:
            pass
        asset_obs_kwargs = {"num_rows": len(obj),
                            "size (bytes)": sum(written_sizes)}
        context.add_output_metadata(asset_obs_kwargs)

    def load_from_path(self, context: InputContext, path: UPath) -> pl.DataFrame:
        dataset, part = context.get_asset_identifier()
        dataset_path = UPath(self._base_path) / dataset
        dataset_part_path = dataset_path / f"{part}{self.extension}"

        context.log.debug(dataset_path)
        dataset = pq.ParquetDataset(dataset_part_path, use_legacy_dataset=False)
        return pl.from_arrow(dataset.read())

@io_manager(config_schema={"base_path": Field(str, is_required=False)})
def local_parquet_io_manager(
    init_context: InitResourceContext,
) -> PolarsParquetIOManager:
    assert init_context.instance is not None  # to please mypy
    base_path = UPath(
        init_context.resource_config.get(
            "base_path", init_context.instance.storage_directory()
        )
    )
    return PolarsParquetIOManager(base_path=base_path)

@io_manager(
    config_schema={
        "base_path": Field(str, is_required=True),
        "AWS_ACCESS_KEY_ID": StringSource,
        "AWS_SECRET_ACCESS_KEY": StringSource,
    }
)
def s3_parquet_io_manager(init_context: InitResourceContext) -> PolarsParquetIOManager:
    # `UPath` will read boto env vars.
    # The credentials can also be taken from the config and passed to `UPath` directly.
    base_path = UPath(init_context.resource_config.get("base_path"))
    assert str(base_path).startswith("s3://"), base_path
    return PolarsParquetIOManager(base_path=base_path)

@io_manager(
    config_schema={
        "base_path": Field(str, is_required=True),
        "project": StringSource,
        "token": StringSource,
    }
)
def gcp_parquet_io_manager(init_context: InitResourceContext) -> PolarsParquetIOManager:
    # `UPath` will read boto env vars.
    # The credentials can also be taken from the config and passed to `UPath` directly.
    base_path = UPath(init_context.resource_config.get("base_path"))
    fs = gcsfs.GCSFileSystem(token=init_context.resource_config.get("token"),
                             project=init_context.resource_config.get("project"))
    base_path._accessor._fs = fs
    assert str(base_path).startswith("gs://"), base_path
    return PolarsParquetIOManager(base_path=base_path)
