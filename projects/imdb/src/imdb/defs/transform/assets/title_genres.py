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
        "postgres",
    },
)
def title_genres_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_basics")
    context.log.info(f"Reading raw data from {raw_data_path}")

    title_basics = loaders.load_title_basics_memory(raw_data_path)

    title_genres = (
        title_basics.select(
            pl.col("tconst"), pl.col("genres").str.split(",").alias("genre")
        )
        .explode("genre")
        .drop_nulls("genre")
    )

    pr = context.resources.postgres
    context.log.info("Writing title_genres to imdb.title_genres")
    pr.load_polars_dataframe(df=title_genres, table_name="title_genres", schema="imdb")

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": title_genres.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
