import dagster as dg
import polars as pl
from .. import loaders


@dg.asset(
    deps=["imdb_download", "title_basics_loaded", "name_basics_loaded"],
    description="Data for writers table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres",
    },
)
def title_writers_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_crew")
    context.log.info(f"Reading raw data from {raw_data_path}")

    title_writers = (
        loaders.load_title_crew_memory(raw_data_path)
        .select(
            pl.col("tconst"),
            pl.col("writers").alias("nconst").str.split(","),
        )
        .explode("nconst")
        .drop_nulls("nconst")
    )

    pre_load_message = "Removing relations of imdb.title_writers."
    pre_load_query = """
        ALTER TABLE IF EXISTS imdb.title_writers
        DROP CONSTRAINT IF EXISTS title_writers_fk_nconst;
        ALTER TABLE IF EXISTS imdb.title_writers
        DROP CONSTRAINT IF EXISTS title_writers_fk_tconst;
        """

    post_load_message = """
        Removing tconst mismatches from imdb.title_writers.
        Removing nconst mismatches from imdb.title_writers.
        Adding imdb.name_basics (nconst) constraint to imdb.title_writers.
        Adding imdb.title_basics (tconst) constraint to imdb.title_writers.
        """

    post_load_query = """
        DELETE FROM imdb.title_writers
        WHERE tconst IN (
            SELECT tconst FROM imdb.title_writers
            EXCEPT
            SELECT tconst FROM imdb.title_basics);
        
        DELETE FROM imdb.title_writers
        WHERE nconst IN (
            SELECT nconst FROM imdb.title_writers
            except
            SELECT nconst FROM imdb.name_basics);

        ALTER TABLE IF EXISTS imdb.title_writers
        ADD CONSTRAINT title_writers_fk_nconst FOREIGN KEY (nconst)
        REFERENCES imdb.name_basics (nconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

        ALTER TABLE IF EXISTS imdb.title_writers
        ADD CONSTRAINT title_writers_fk_tconst FOREIGN KEY (tconst)
        REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;
        """

    loaders.reload_table_with_fk(
        context=context,
        df=title_writers,
        table="title_writers",
        schema="imdb",
        pre_load_message=pre_load_message,
        pre_load_query=pre_load_query,
        post_load_message=post_load_message,
        post_load_query=post_load_query,
    )

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": title_writers.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
