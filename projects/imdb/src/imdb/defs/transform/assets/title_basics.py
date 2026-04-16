import dagster as dg
import polars as pl
from .. import loaders


@dg.asset(
    deps=["imdb_download"],
    description="Data for title_basics table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres",
    },
)
def title_basics_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_basics")
    context.log.info(f"Reading raw data from {raw_data_path}")

    title_basics = (
        loaders.load_title_basics_memory(raw_data_path)
        .drop("genres")
        .rename(
            {
                "titleType": "title_type",
                "primaryTitle": "primary_title",
                "originalTitle": "original_title",
                "isAdult": "is_adult",
                "startYear": "start_year",
                "endYear": "end_year",
                "runtimeMinutes": "runtime_minutes",
            }
        )
    )

    pr = context.resources.postgres
    context.log.info("removing relations for with a reference to tconst of imdb.title_basics")
    pr.execute_query(
        context,
        """
        ALTER TABLE imdb.title_genres DROP CONSTRAINT IF EXISTS genres_tconst_fkey;
        ALTER TABLE imdb.name_known_for_titles DROP CONSTRAINT IF EXISTS known_for_titles_fk;
        ALTER TABLE imdb.title_directors DROP CONSTRAINT IF EXISTS title_directors_fk_tconst;
        ALTER TABLE imdb.title_principals DROP CONSTRAINT IF EXISTS title_principals_fk_tconst;
        ALTER TABLE imdb.title_ratings DROP CONSTRAINT IF EXISTS title_ratings_fk;
        ALTER TABLE imdb.title_writers DROP CONSTRAINT IF EXISTS title_writers_fk_tconst;
        """
    )

    context.log.info("Writing title_basics to imdb.title_basics")
    pr.load_polars_dataframe(
        context, df=title_basics, table_name="title_basics", schema="imdb"
    )

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": title_basics.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
