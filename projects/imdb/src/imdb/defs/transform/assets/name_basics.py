import dagster as dg
import polars as pl
from .. import loaders


@dg.asset(
    deps=["imdb_download"],
    description="Data for name_basics table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres",
    },
)
def name_basics_loaded(context: dg.AssetExecutionContext):

    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("name_basics")
    context.log.info(f"Reading raw data from {raw_data_path}")

    name_basics = (
        loaders.load_name_basics_memory(raw_data_path)
        .drop("primaryProfession", "knownForTitles")
        .rename(
            {
                "primaryName": "primary_name",
                "birthYear": "birth_year",
                "deathYear": "death_year",
            }
        )
    )

    pr = context.resources.postgres
    context.log.info("removing relations for imdb.name_basics")
    pr.execute_query(
        context,
        """
        ALTER TABLE imdb.name_basics DROP CONSTRAINT IF EXISTS primary_profession_nconst_fkey;
        ALTER TABLE imdb.name_basics DROP CONSTRAINT IF EXISTS title_directors_fk_nconst;
        ALTER TABLE imdb.name_basics DROP CONSTRAINT IF EXISTS title_principals_fk_nconst;
        ALTER TABLE imdb.name_basics DROP CONSTRAINT IF EXISTS title_writers_fk_nconst;
        """
    )

    context.log.info("Writing name_basics to imdb.name_basics")
    pr.load_polars_dataframe(
        context, df=name_basics, table_name="name_basics", schema="imdb"
    )

    return dg.MaterializeResult(
        value=name_basics,
        metadata={
            "num_rows": name_basics.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
