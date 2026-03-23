import dagster as dg
import polars as pl
from .. import loaders

@dg.asset(
    deps=["title_basics_raw"],
    description="Data for title_basics table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres_resource",
    },
)
def title_basics_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_basics")
    context.log.info(f"Reading raw data from {raw_data_path}")

    title_basics = loaders.load_title_basics_memory(raw_data_path)

    # TODO: more transformation for dataabse ingestion (header names, data types, etc.)
    title_basics = title_basics.drop("genres")

    with context.resources.postgres_resource.connect() as conn:
        context.log.info(f"Writing title_basics to database")
        title_basics.write_database(
            table_name="imdb.title_basics",
            if_table_exists="replace",
            connection=conn)

    return dg.MaterializeResult(
        # TODO: schema
        metadata={
            "num_rows": title_basics.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )




# @dg.asset(
#     deps=["title_basics_raw"],
#     description="Data for title_basics table",
#     group_name="staging",
#     required_resource_keys={
#         "file_registry"
#     },  # needed to use file_registry in the asset function
# )
# def title_basics(context: dg.AssetExecutionContext):
#     FileRegistry = context.resources.file_registry
#     raw_data_path = FileRegistry.get_path("title_basics")
#     context.log.info(f"Reading raw data from {raw_data_path}")

#     df = loaders.load_title_basics_memory(raw_data_path)

#     context.log.info("Creating title_basics")
#     title_basics: pl.DataFrame = df.drop(
#         "genres"
#     )

#     return dg.MaterializeResult(
#         value=title_basics,
#         # TODO: schema
#         metadata={
#             "num_rows": title_basics.height,
#             "preview": title_basics.head().to_pandas().to_markdown(),  # moet beter
#         },
#     )