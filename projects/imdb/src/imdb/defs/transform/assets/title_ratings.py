import dagster as dg
import polars as pl


@dg.asset(
    deps=["title_ratings_raw"],
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

    pr = context.resources.postgres
    context.log.info("Writing title_ratings to imdb.title_ratings")
    pr.load_polars_dataframe(
        df=title_ratings, table_name="title_ratings", schema="imdb"
    )

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": title_ratings.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
