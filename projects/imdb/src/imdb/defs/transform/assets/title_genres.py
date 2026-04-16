import dagster as dg
import polars as pl
from projects.imdb.src.imdb.defs.transform.assets import title_directors
from .. import loaders

@dg.asset(
    deps=["imdb_download", "title_basics_loaded"],
    description="Data for genres table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres",
    },
)
def title_genres_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_basics")
    context.log.info(f"Reading raw data from {raw_data_path}")

    title_basics = loaders.load_title_basics_memory(raw_data_path)

    title_genres = (
        title_basics.select(
            pl.col("tconst"), pl.col("genres").str.split(",").alias("genre")
        )
        .explode("genre")
        .drop_nulls("genre")
    )

    pre_load_message = "Removing relations of imdb.title_genres."
    pre_load_query = """
        ALTER TABLE IF EXISTS imdb.title_genres
        DROP CONSTRAINT IF EXISTS title_genres_tconst_fkey;
        """

    post_load_message = \
        "Adding imdb.title_basics (tconst) constraint to imdb.title_genres."
    
    post_load_query = """
        ALTER TABLE IF EXISTS imdb.title_genres
        ADD CONSTRAINT title_genres_tconst_fkey FOREIGN KEY (tconst)
        REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE;
        """

    loaders.reload_table_with_fk(
        context=context,
        df=title_genres,
        table="title_genres",
        schema="imdb",
        pre_load_message=pre_load_message,
        pre_load_query=pre_load_query,
        post_load_message=post_load_message,
        post_load_query=post_load_query,
    )
    
    
    
    
    
    
    
    
    # pr = context.resources.postgres

    # context.log.info("removing relations of imdb.title_ratings")
    # pr.execute_query(
    #     context,
    #     """
    #     ALTER TABLE IF EXISTS imdb.title_genres
    #     DROP CONSTRAINT IF EXISTS title_genres_tconst_fkey;
    #     """,
    # )

    # context.log.info("Writing title_ratings to imdb.title_genres")
    # pr.load_polars_dataframe(
    #     context, df=title_genres, table_name="title_genres", schema="imdb"
    # )

    # context.log.info(
    #     "Adding imdb.title_basics (tconst) constraint to imdb.title_genres"
    # )

    # pr.execute_query(
    #     context,
    #     """
    #     ALTER TABLE IF EXISTS imdb.title_genres
    #     ADD CONSTRAINT title_genres_tconst_fkey FOREIGN KEY (tconst)
    #     REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
    #     ON UPDATE NO ACTION
    #     ON DELETE CASCADE;
    #     """,
    # )

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": title_genres.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
