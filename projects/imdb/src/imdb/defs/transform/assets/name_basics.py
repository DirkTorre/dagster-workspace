import dagster as dg
import polars as pl
from .. import loaders
from dagster_polars import PolarsParquetIOManager


@dg.asset(
    deps=["name_basics_raw"],
    description="Transformed name_basics dataframe",
    group_name="transform",
    required_resource_keys={"file_registry"},
    io_manager_key="polars_parquet_io_manager", # only add this line if you return a (big polars dataframe)
)
def name_basics_transformed(context: dg.AssetExecutionContext) -> pl.DataFrame:
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("name_basics")
    context.log.info(f"Reading raw data from {raw_data_path}")

    name_basics = loaders.load_name_basics_memory(raw_data_path).drop(
        ["primaryProfession", "knownForTitles"]
    )

    # Return the DataFrame directly - the IO manager will handle storage
    return name_basics

@dg.asset(
    deps=["name_basics_transformed"],
    description="Data for name_basics table",
    group_name="loading",
    required_resource_keys={"postgres_resource"},
    # io_manager_key="polars_parquet_io_manager",
)
def name_basics_loaded(
    context: dg.AssetExecutionContext, 
    name_basics_transformed: pl.DataFrame
):
    chunk_size = 1_000_000
    
    with context.resources.postgres_resource.connect() as conn:
        context.log.info(f"Writing name_basics to database")
        
        for offset in range(0, name_basics_transformed.height, chunk_size):
            context.log.info(f"Writing name_basics chunk {offset // chunk_size + 1} to database")
            batch = name_basics_transformed.slice(offset, chunk_size)
            batch.write_database(
                table_name="imdb.name_basics", 
                if_table_exists="append", 
                connection=conn
            )


    # return dg.MaterializeResult(
    #     value=name_basics,
    #     metadata={
    #         "num_rows": name_basics.height
    #         # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
    #     },
    # )



# def 

#     context.log.info("Creating name_basics table")

#     with context.resources.postgres_resource.connect() as conn:
#         context.log.info(f"Writing name_basics to database")
#         name_basics.write_database(
#             table_name="imdb.name_basics", if_table_exists="replace", connection=conn
#         )

#     # TODO: more transformation for dataabse ingestion (header names, data types, etc.)

    