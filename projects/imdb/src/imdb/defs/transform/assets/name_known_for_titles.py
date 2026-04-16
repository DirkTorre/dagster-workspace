import dagster as dg
import polars as pl
from projects.imdb.src.imdb.defs.transform.assets import name_primary_profession
from .. import loaders


@dg.asset(
    deps=["imdb_download", "title_basics_loaded", "name_basics_loaded"],
    description="Data for name_known_for_titles table",
    group_name="transform_and_load",
    required_resource_keys={"file_registry", "postgres"},
)
def name_known_for_titles_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("name_basics")
    context.log.info(f"Reading raw data from {raw_data_path}")

    context.log.info("Loading name_known_for_titles table")

    name_known_for_titles = (
        loaders.load_name_basics_memory(raw_data_path)
        .select(
            pl.col("nconst"),
            pl.col("knownForTitles").alias("tconst").str.split(","),
        )
        .explode("tconst")
        .drop_nulls("tconst")
    )

    pr = context.resources.postgres
    context.log.info("removing relations for imdb.name_primary_profession")
    pr.execute_query(
        context,
        """
        ALTER TABLE imdb.name_known_for_titles DROP CONSTRAINT IF EXISTS name_known_for_titles_fk;
        """
    )

    context.log.info("Writing name_known_for_titles to imdb.name_known_for_titles")
    pr.load_polars_dataframe(
        context,
        df=name_known_for_titles,
        table_name="name_known_for_titles",
        schema="imdb",
    )

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": name_known_for_titles.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
