import dagster as dg
import polars as pl
from .. import loaders

@dg.asset(
    deps=["name_basics_raw"],
    description="Data for name_primary_profession table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres_resource"
    },  
)
def name_primary_profession_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("name_basics")
    context.log.info(f"Reading raw data from {raw_data_path}")

    name_basics = loaders.load_name_basics_memory(raw_data_path)

    context.log.info("Creating name_basics table")
    name_basics = name_basics.drop(["primaryProfession", "knownForTitles"])

    name_primary_profession = name_basics.select(
        pl.col("nconst"),
        pl.col("primaryProfession").str.split(",")
    ).explode("primaryProfession")

    with context.resources.postgres_resource.connect() as conn:
        context.log.info(f"Writing name_primary_profession to database")
        name_primary_profession.write_database(
            table_name="imdb.name_primary_profession",
            if_table_exists="replace",
            connection=conn
        )

    # TODO: more transformation for dataabse ingestion (header names, data types, etc.)

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": name_primary_profession.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )


