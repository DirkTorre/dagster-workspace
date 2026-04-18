import dagster as dg
from dagster_polars import PolarsParquetIOManager


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "polars_parquet_io_manager": PolarsParquetIOManager(
                base_path="data/imdb/transform"  # Specify where to store the parquet files
            ),
        }
    )
