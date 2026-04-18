import dagster as dg
import polars as pl
from .. import loaders


@dg.asset(
    deps=["imdb_download", "title_basics_loaded", "name_basics_loaded"],
    description="Data for title_principals table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres",
    },  # needed to use file_registry in the asset function
)
def title_principals_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_principals")
    context.log.info(f"Reading raw data from {raw_data_path}")

    df = pl.read_csv(
        raw_data_path,
        has_header=True,
        separator="\t",
        truncate_ragged_lines=True,
        null_values="\\N",
        quote_char=None,
        schema={
            "tconst": pl.Utf8,
            "ordering": pl.Int16,
            "nconst": pl.Utf8,
            "category": pl.Utf8,
            "job": pl.Utf8,
            "characters": pl.Utf8,
        },
    )

    context.log.info("Creating title_principals")

    title_principals = df.with_columns(
        pl.col("characters").str.replace_all(r'[\[\]"]', "")
    )

    pre_load_message = "Removing relations of imdb.title_principals."
    pre_load_query = """ ALTER TABLE imdb.title_principals
        DROP CONSTRAINT IF EXISTS title_principals_fk;"""

    post_load_message = """Removing tconst mismatches from imdb.title_principals.
        Adding imdb.title_basics (tconst) constraint to imdb.title_principals."""

    post_load_query = """DELETE FROM imdb.title_principals
        WHERE tconst IN (
            SELECT tconst
            FROM imdb.title_principals
            EXCEPT
            SELECT tconst
            FROM imdb.title_basics
        );

        DELETE FROM imdb.title_principals
        WHERE nconst IN (
            SELECT nconst
            FROM imdb.title_principals
            EXCEPT
            SELECT nconst
            FROM imdb.name_basics
        );

        ALTER TABLE IF EXISTS imdb.title_principals
        ADD CONSTRAINT title_principals_fk_tconst FOREIGN KEY (tconst)
        REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE;

        ALTER TABLE IF EXISTS imdb.title_principals
        ADD CONSTRAINT title_principals_fk_nconst FOREIGN KEY (nconst)
        REFERENCES imdb.name_basics (nconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE;
        """

    loaders.reload_table_with_fk(
        context=context,
        df=title_principals,
        table="title_principals",
        schema="imdb",
        pre_load_message=pre_load_message,
        pre_load_query=pre_load_query,
        post_load_message=post_load_message,
        post_load_query=post_load_query,
    )

    return dg.MaterializeResult(
        value=title_principals,
        # TODO: schema
        metadata={
            "num_rows": title_principals.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
