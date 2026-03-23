import dagster as dg
import polars as pl
from .. import loaders

@dg.asset(
    deps=["title_crew_raw"],
    description="Data for writers table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres_resource",
    },
)
def title_writers_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_crew")
    context.log.info(f"Reading raw data from {raw_data_path}")

    df = loaders.load_title_crew_memory(raw_data_path)

    title_writers = df.select(
        pl.col("tconst"),
        pl.col("writers").str.split(","),
    ).explode("writers")
    # TODO: more transformation for dataabse ingestion (header names, data types, etc.)

    with context.resources.postgres_resource.connect() as conn:
        context.log.info(f"Writing title_writers to database")
        title_writers.write_database(
            table_name="imdb.title_writers",
            if_table_exists="replace",
            connection=conn
        )

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": title_writers.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )



