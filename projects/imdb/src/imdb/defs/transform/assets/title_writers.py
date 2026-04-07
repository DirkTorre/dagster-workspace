import dagster as dg
import polars as pl
from .. import loaders


@dg.asset(
    deps=["title_crew_raw"],
    description="Data for writers table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres",
    },
)
def title_writers_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_crew")
    context.log.info(f"Reading raw data from {raw_data_path}")

    title_writers = (
        loaders.load_title_crew_memory(raw_data_path)
        .select(
            pl.col("tconst"),
            pl.col("writers").alias("nconst").str.split(","),
        )
        .explode("nconst")
        .drop_nulls("nconst")
    )

    pr = context.resources.postgres
    context.log.info("Writing title_writers to imdb.title_writers")
    pr.load_polars_dataframe(
        df=title_writers, table_name="title_writers", schema="imdb"
    )

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": title_writers.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
