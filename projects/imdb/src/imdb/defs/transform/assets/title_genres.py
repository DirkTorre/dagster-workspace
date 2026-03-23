import dagster as dg
import polars as pl
from projects.imdb.src.imdb.defs.transform.assets import title_directors
from .. import loaders

@dg.asset(
    deps=["title_basics_raw"],
    description="Data for genres table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres_resource",
    },
)
def title_genres_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_basics")
    context.log.info(f"Reading raw data from {raw_data_path}")

    title_basics = loaders.load_title_basics_memory(raw_data_path)

    # TODO: more transformation for dataabse ingestion (header names, data types, etc.)

    title_genres = title_basics.select(
        pl.col("tconst"), 
        pl.col("genres").str.split(",")
        ).explode("genres")

    with context.resources.postgres_resource.connect() as conn:
        context.log.info(f"Writing title_genres to database")
        title_genres.write_database(
            table_name="imdb.title_genres",
            if_table_exists="replace",
            connection=conn)

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": title_genres.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )










