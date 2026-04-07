import dagster as dg
import polars as pl
from .. import loaders


@dg.asset(
    deps=["name_basics_raw"],
    description="Data for name_primary_profession table",
    group_name="transform_and_load",
    required_resource_keys={"file_registry", "postgres"},
)
def name_primary_profession_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("name_basics")
    context.log.info(f"Reading raw data from {raw_data_path}")

    context.log.info("Creating name_basics table")

    name_primary_profession = (
        loaders.load_name_basics_memory(raw_data_path)
        .select(
            pl.col("nconst"),
            pl.col("primaryProfession").alias("primary_profession").str.split(","),
        )
        .explode("primary_profession")
    )

    pr = context.resources.postgres
    context.log.info("Writing name_primary_profession to imdb.name_primary_profession")
    pr.load_polars_dataframe(
        df=name_primary_profession, table_name="name_primary_profession", schema="imdb"
    )

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": name_primary_profession.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
