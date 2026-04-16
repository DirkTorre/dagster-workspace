import dagster as dg
import polars as pl
from .. import loaders


@dg.asset(
    deps=["imdb_download", "name_basics_loaded"],
    description="Data for name_primary_profession table",
    group_name="transform_and_load",
    required_resource_keys={"file_registry", "postgres"},
)
def name_primary_profession_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("name_basics")
    context.log.info(f"Reading raw data from {raw_data_path}")

    context.log.info("Loading name_basics table")

    name_primary_profession = (
        loaders.load_name_basics_memory(raw_data_path)
        .select(
            pl.col("nconst"),
            pl.col("primaryProfession").alias("primary_profession").str.split(","),
        )
        .explode("primary_profession")
    )

    pr = context.resources.postgres
    context.log.info("removing relations for imdb.name_primary_profession")
    pr.execute_query(
        context,
        """
        ALTER TABLE imdb.name_basics DROP CONSTRAINT IF EXISTS name_primary_profession_nconst_fkey;
        """
    )

    context.log.info("Writing name_primary_profession to imdb.name_primary_profession")
    pr.load_polars_dataframe(
        context,
        df=name_primary_profession,
        table_name="name_primary_profession",
        schema="imdb",
    )

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": name_primary_profession.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
