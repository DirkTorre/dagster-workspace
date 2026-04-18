import dagster as dg
import polars as pl
from .. import loaders


@dg.asset(
    deps=["imdb_download", "title_basics_loaded"],
    description="Data for title_akas table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres",
    },
)
def title_akas_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_akas")
    context.log.info(f"Reading raw data from {raw_data_path}")

    title_akas = pl.read_csv(
        raw_data_path,
        has_header=True,
        separator="\t",
        truncate_ragged_lines=True,
        null_values="\\N",
        quote_char=None,
        schema={
            "titleId": pl.Utf8,
            "ordering": pl.UInt16,
            "title": pl.Utf8,
            "region": pl.Utf8,
            "language": pl.Utf8,
            "types": pl.Utf8,
            "attributes": pl.Utf8,
            "isOriginalTitle": pl.UInt8,
        },
    )

    title_akas = title_akas.with_columns(
        pl.col("isOriginalTitle").cast(pl.Boolean)
    ).rename({"titleId": "tconst", "isOriginalTitle": "is_original_title"})

    pre_load_message = "Removing relations of imdb.title_akas."
    pre_load_query = """ ALTER TABLE imdb.title_akas
        DROP CONSTRAINT IF EXISTS title_akas_tconst_fkey;"""

    post_load_message = "Removing tconst mismatches from imdb.title_akas."

    post_load_query = """
        DELETE FROM imdb.title_akas
        WHERE tconst IN (
            SELECT tconst
            FROM imdb.title_akas
            EXCEPT
            SELECT tconst
            FROM imdb.title_basics
        );

        ALTER TABLE IF EXISTS imdb.title_akas
        ADD CONSTRAINT title_akas_tconst_fkey FOREIGN KEY (tconst)
        REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;
        """

    loaders.reload_table_with_fk(
        context=context,
        df=title_akas,
        table="title_akas",
        schema="imdb",
        pre_load_message=pre_load_message,
        pre_load_query=pre_load_query,
        post_load_message=post_load_message,
        post_load_query=post_load_query,
    )

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": title_akas.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
