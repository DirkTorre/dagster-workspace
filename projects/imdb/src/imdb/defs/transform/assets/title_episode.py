import dagster as dg
import polars as pl
from .. import loaders


@dg.asset(
    deps=["imdb_download", "title_basics_loaded"],
    description="Data for title_episode table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres",
    },  # needed to use file_registry in the asset function
)
def title_episode_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_episode")
    context.log.info(f"Reading raw data from {raw_data_path}")

    title_episode = pl.read_csv(
        raw_data_path,
        has_header=True,
        separator="\t",
        truncate_ragged_lines=True,
        null_values="\\N",
        quote_char=None,
        schema={
            "tconst": pl.Utf8,
            "parentTconst": pl.Utf8,
            "seasonNumber": pl.UInt16,
            "episodeNumber": pl.UInt32,
        },
    )

    title_episode = title_episode.rename(
        {
            "parentTconst": "parent_tconst",
            "seasonNumber": "season_number",
            "episodeNumber": "episode_number",
        }
    )

    pre_load_message = "Removing relations of imdb.title_episode."
    pre_load_query = """ ALTER TABLE imdb.title_episode
        DROP CONSTRAINT IF EXISTS title_episode_fk;"""

    post_load_message = """Removing tconst mismatches from imdb.title_episode.
        Adding imdb.title_basics (tconst) constraint (2x) to imdb.title_episode."""

    post_load_query = """DELETE FROM imdb.title_episode
        WHERE tconst IN (
            SELECT tconst
            FROM imdb.title_episode
            EXCEPT
            SELECT tconst
            FROM imdb.title_basics
        );

        DELETE FROM imdb.title_episode
        WHERE parent_tconst IN (
            SELECT parent_tconst
            FROM imdb.title_episode
            EXCEPT
            SELECT tconst
            FROM imdb.title_basics
        );

        ALTER TABLE IF EXISTS imdb.title_episode
        ADD CONSTRAINT title_episode_fk_tconst FOREIGN KEY (tconst)
        REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE;

        ALTER TABLE IF EXISTS imdb.title_episode
        ADD CONSTRAINT title_episode_fk_parent_tconst FOREIGN KEY (parent_tconst)
        REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE;
        """

    loaders.reload_table_with_fk(
        context=context,
        df=title_episode,
        table="title_episode",
        schema="imdb",
        pre_load_message=pre_load_message,
        pre_load_query=pre_load_query,
        post_load_message=post_load_message,
        post_load_query=post_load_query,
    )

    return dg.MaterializeResult(
        value=title_episode,
        # TODO: schema
        metadata={
            "num_rows": title_episode.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
