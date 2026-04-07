import dagster as dg
import polars as pl

@dg.asset(
    deps=["title_episode_raw"],
    description="Data for title_episode table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres"
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
        {"parentTconst": "parent_tconst", "seasonNumber": "season_number", "episodeNumber": "episode_number"}
    )

    pr = context.resources.postgres

    context.log.info(f"Writing title_episode to imdb.title_episode")
    pr.load_polars_dataframe(
        df=title_episode, table_name="title_episode", schema="imdb"
    )

    return dg.MaterializeResult(
        value=title_episode,
        # TODO: schema
        metadata={
            "num_rows": title_episode.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )







    # FileRegistry = context.resources.file_registry
    # raw_data_path = FileRegistry.get_path("title_episode")
    # context.log.info(f"Reading raw data from {raw_data_path}")

    # title_episode = pl.read_csv(
    #     raw_data_path,
    #     has_header=True,
    #     separator="\t",
    #     truncate_ragged_lines=True,
    #     null_values="\\N",
    #     quote_char=None,
    #     schema={
    #         "tconst": pl.Utf8,
    #         "parentTconst": pl.Utf8,
    #         "seasonNumber": pl.UInt16,
    #         "episodeNumber": pl.UInt32,
    #     },
    # )

    # context.log.info(f"Loading raw data from {raw_data_path} into database")

    # with context.resources.postgres_resource.connect() as conn:
    #     context.log.info(f"Writing title_episode to database")
    #     title_episode.write_database(
    #         table_name="imdb.title_episode",
    #         if_table_exists="replace", 
    #         connection=conn
    #     )

    

    # return dg.MaterializeResult(
    #     value=title_episode,
    #     # TODO: schema
    #     metadata={
    #         "num_rows": title_episode.height
    #         # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
    #     },
    # )


