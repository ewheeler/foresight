import os
from typing import Union

import pandas
from dagster import (
    Field,
    InputContext,
    IOManager,
    OutputContext,
    _check as check,
    io_manager,
)
from dagster._seven.temp_dir import get_system_temp_directory


class PartitionedParquetIOManager(IOManager):
    """
    This IOManager will take in a pandas dataframe and store it in parquet at the
    specified path.

    It stores outputs for different partitions in different filepaths.

    Downstream ops can either load this dataframe into a spark session or simply retrieve a path
    to where the data is stored.
    """

    def __init__(self, base_path):
        self._base_path = base_path

    def handle_output(
        self, context: OutputContext, obj: pandas.DataFrame
    ):
        path = self._get_path(context)
        if "://" not in self._base_path:
            os.makedirs(os.path.dirname(path), exist_ok=True)

        if isinstance(obj, pandas.DataFrame):
            row_count = len(obj)
            context.log.info(f"Row count: {row_count}")
            obj.to_parquet(path=path, index=False)
        else:
            raise Exception(f"Outputs of type {type(obj)} not supported.")

        context.add_output_metadata({"row_count": row_count, "path": path})

    def load_input(self, context) -> str:
        path = self._get_path(context)

        return check.failed(
            f"Inputs of type {context.dagster_type} not supported. Please specify a valid type "
            "for this input either on the argument of the @asset-decorated function."
        )

    def _get_path(self, context: Union[InputContext, OutputContext]):
        key = context.asset_key.path[-1]  # type: ignore

        if context.has_asset_partitions:
            start, end = context.asset_partitions_time_window
            dt_format = "%Y%m%d%H%M%S"
            partition_str = start.strftime(dt_format) + "_" + end.strftime(dt_format)
            return os.path.join(self._base_path, key, f"{partition_str}.pq")
        else:
            return os.path.join(self._base_path, f"{key}.pq")


@io_manager(
    config_schema={"base_path": Field(str, is_required=False)},
)
def local_partitioned_parquet_io_manager(init_context):
    return PartitionedParquetIOManager(
        base_path=init_context.resource_config.get("base_path", get_system_temp_directory())
    )


@io_manager(required_resource_keys={"s3_bucket"})
def s3_partitioned_parquet_io_manager(init_context):
    return PartitionedParquetIOManager(base_path="s3://" + init_context.resources.s3_bucket)
