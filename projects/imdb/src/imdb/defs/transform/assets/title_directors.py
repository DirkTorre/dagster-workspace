import dagster as dg
import polars as pl
from .. import loaders


@dg.asset(
    deps=["imdb_download", "title_basics_loaded", "name_basics_loaded"],
    description="Data for directors table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres",
    },
)
def title_directors_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_crew")
    context.log.info(f"Reading raw data from {raw_data_path}")

    title_directors = (
        loaders.load_title_crew_memory(raw_data_path)
        .select(
            pl.col("tconst"),
            pl.col("directors").alias("nconst").str.split(","),
        )
        .explode("nconst")
        .drop_nulls("nconst")
    )

    pre_load_message = "Removing relations of imdb.title_directors."
    pre_load_query = """
        ALTER TABLE IF EXISTS imdb.title_genres
        DROP CONSTRAINT IF EXISTS title_genres_tconst_fkey;
        """

    post_load_message = """
        Removing tconst mismatches from imdb.title_directors.
        Removing nconst mismatches from imdb.title_directors.
        Adding imdb.name_basics (nconst) constraint to imdb.title_directors.
        Adding imdb.title_basics (tconst) constraint to imdb.title_directors.
        """

    post_load_query = """
        DELETE FROM imdb.title_directors
        WHERE tconst IN (
            SELECT tconst FROM imdb.title_directors
            EXCEPT
            SELECT tconst FROM imdb.title_basics);
        
        DELETE FROM imdb.title_directors
        WHERE nconst IN (
            SELECT nconst FROM imdb.title_directors
            except
            SELECT nconst FROM imdb.name_basics);

        ALTER TABLE IF EXISTS imdb.title_directors
        ADD CONSTRAINT title_directors_fk_nconst FOREIGN KEY (nconst)
        REFERENCES imdb.name_basics (nconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

        ALTER TABLE IF EXISTS imdb.title_directors
        ADD CONSTRAINT title_directors_fk_tconst FOREIGN KEY (tconst)
        REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;
        """

    loaders.reload_table_with_fk(
        context=context,
        df=title_directors,
        table="title_directors",
        schema="imdb",
        pre_load_message=pre_load_message,
        pre_load_query=pre_load_query,
        post_load_message=post_load_message,
        post_load_query=post_load_query,
    )

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": title_directors.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
