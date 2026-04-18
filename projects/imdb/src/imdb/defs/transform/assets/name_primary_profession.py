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

    pre_load_message = "Removing relations of imdb.name_primary_profession."
    pre_load_query = """ ALTER TABLE imdb.name_primary_profession
        DROP CONSTRAINT IF EXISTS name_primary_profession_nconst_fkey;"""

    post_load_message = """Removing nconst mismatches from imdb.name_primary_profession.
        """

    post_load_query = """DELETE FROM imdb.name_primary_profession
        WHERE nconst IN (
            SELECT nconst
            FROM imdb.name_primary_profession
            EXCEPT
            SELECT nconst
            FROM imdb.name_basics
        );

        ALTER TABLE IF EXISTS imdb.name_primary_profession
        ADD CONSTRAINT name_primary_profession_nconst_fkey FOREIGN KEY (nconst)
        REFERENCES imdb.name_basics (nconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE;
        """

    loaders.reload_table_with_fk(
        context=context,
        df=name_primary_profession,
        table="name_primary_profession",
        schema="imdb",
        pre_load_message=pre_load_message,
        pre_load_query=pre_load_query,
        post_load_message=post_load_message,
        post_load_query=post_load_query,
    )

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": name_primary_profession.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
