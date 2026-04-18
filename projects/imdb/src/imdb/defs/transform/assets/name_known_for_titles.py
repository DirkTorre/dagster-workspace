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

    pre_load_message = "Removing relations of imdb.name_known_for_titles."
    pre_load_query = """ ALTER TABLE imdb.name_known_for_titles
        DROP CONSTRAINT IF EXISTS name_known_for_titles_fk;"""

    post_load_message = """Removing tconst mismatches from imdb.name_known_for_titles.
        Adding imdb.title_basics (tconst) constraint to imdb.name_known_for_titles."""

    post_load_query = """DELETE FROM imdb.name_known_for_titles
        WHERE tconst IN (
            SELECT tconst
            FROM imdb.name_known_for_titles
            EXCEPT
            SELECT tconst
            FROM imdb.title_basics
        );

        DELETE FROM imdb.name_known_for_titles
        WHERE tconst IN (
            SELECT nconst
            FROM imdb.name_known_for_titles
            EXCEPT
            SELECT nconst
            FROM imdb.name_basics
        );

        ALTER TABLE IF EXISTS imdb.name_known_for_titles
        ADD CONSTRAINT name_known_for_titles_fk_tconst FOREIGN KEY (tconst)
        REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE;

        ALTER TABLE IF EXISTS imdb.name_known_for_titles
        ADD CONSTRAINT name_known_for_titles_fk_nconst FOREIGN KEY (nconst)
        REFERENCES imdb.name_basics (nconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE;
        """

    loaders.reload_table_with_fk(
        context=context,
        df=name_known_for_titles,
        table="name_known_for_titles",
        schema="imdb",
        pre_load_message=pre_load_message,
        pre_load_query=pre_load_query,
        post_load_message=post_load_message,
        post_load_query=post_load_query,
    )

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": name_known_for_titles.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
