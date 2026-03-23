import dagster as dg
import polars as pl
from .. import loaders

@dg.asset(
    deps=["title_crew_raw"],
    description="Data for directors table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres_resource",
    },
)
def title_directors_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_crew")
    context.log.info(f"Reading raw data from {raw_data_path}")

    df = loaders.load_title_crew_memory(raw_data_path)

    title_directors = df.select(
        pl.col("tconst"),
        pl.col("directors").str.split(","),
    ).explode("directors")

    # TODO: more transformation for dataabse ingestion (header names, data types, etc.)

    with context.resources.postgres_resource.connect() as conn:
        context.log.info(f"Writing title_directors to database")
        title_directors.write_database(
            table_name="imdb.title_directors",
            if_table_exists="replace",)

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": title_directors.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )