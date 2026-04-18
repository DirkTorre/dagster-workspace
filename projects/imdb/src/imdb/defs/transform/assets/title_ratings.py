import dagster as dg
import polars as pl
from ...transform.loaders import reload_table_with_fk


@dg.asset(
    deps=["imdb_download", "title_basics_loaded"],
    description="Data for title_ratings table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres",
    },
)
def title_ratings_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_ratings")
    context.log.info(f"Reading raw data from {raw_data_path}")

    title_ratings = pl.read_csv(
        raw_data_path,
        has_header=True,
        separator="\t",
        truncate_ragged_lines=True,
        null_values="\\N",
        quote_char=None,
        schema={
            "tconst": pl.Utf8,
            "averageRating": pl.Float16,
            "numVotes": pl.UInt32,
        },
    )

    title_ratings = title_ratings.rename(
        {"numVotes": "num_votes", "averageRating": "average_rating"}
    )

    pre_load_message = "removing relations of imdb.title_ratings"
    pre_load_query = """
        ALTER TABLE IF EXISTS imdb.title_ratings
        DROP CONSTRAINT IF EXISTS title_ratings_fk;
        """

    post_load_message = (
        "Adding imdb.title_basics (tconst) constraint to imdb.title_ratings"
    )

    post_load_query = """
        ALTER TABLE IF EXISTS imdb.title_ratings
        ADD CONSTRAINT title_ratings_fk FOREIGN KEY (tconst)
        REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE;
        """

    reload_table_with_fk(
        context=context,
        df=title_ratings,
        table="title_ratings",
        schema="imdb",
        pre_load_message=pre_load_message,
        pre_load_query=pre_load_query,
        post_load_message=post_load_message,
        post_load_query=post_load_query,
    )

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": title_ratings.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
