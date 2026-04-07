import dagster as dg
import polars as pl
from .. import loaders


@dg.asset(
    deps=["title_crew_raw"],
    description="Data for directors table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres",
    },
)
def title_directors_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_crew")
    context.log.info(f"Reading raw data from {raw_data_path}")

    title_directors = (
        loaders.load_title_crew_memory(raw_data_path)
        .select(
            pl.col("tconst"),
            pl.col("directors").alias("nconst").str.split(","),
        )
        .explode("nconst")
        .drop_nulls("nconst")
    )

    pr = context.resources.postgres
    context.log.info("Writing title_directors to imdb.title_directors")
    pr.load_polars_dataframe(
        df=title_directors, table_name="title_directors", schema="imdb"
    )

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": title_directors.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
