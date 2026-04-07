# import dagster as dg
# import polars as pl


# @dg.asset(
#     deps=["title_ratings"],
#     description="title_ratings database table",
#     group_name="loading",
#     required_resource_keys={
#         "postgres_resource"
#     },  # needed to use file_registry in the asset function
# )
# def title_ratings_loaded(context: dg.AssetExecutionContext, title_ratings: pl.DataFrame) -> dg.MaterializeResult:
#     with context.resources.postgres_resource.connect() as conn:
#         context.log.info(f"Writing title_ratings to database")
#         title_ratings.write_database(
#             table_name="imdb.title_ratings",
#             if_table_exists="replace", 
#             connection=conn
#         )

#     return dg.MaterializeResult(
#         # value=title_ratings,
#         # TODO: schema
#         metadata={
#             "status": "successfully loaded title_ratings to database",
#             # "num_rows": title_ratings.height
#             # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
#         },
#     )

# @dg.asset(
#     deps=["title_episode"],
#     description="title_episode database table",
#     group_name="loading",
#     required_resource_keys={
#         "postgres_resource"
#     },  # needed to use file_registry in the asset function
# )
# def title_episode_loaded(context: dg.AssetExecutionContext, title_episode: pl.DataFrame) -> dg.MaterializeResult:
#     with context.resources.postgres_resource.connect() as conn:
#         context.log.info(f"Writing title_episode to database")
#         title_episode.write_database(
#             table_name="imdb.title_episode",
#             if_table_exists="replace", 
#             connection=conn
#         )

#     return dg.MaterializeResult(
#         # value=title_ratings,
#         # TODO: schema
#         metadata={
#             "status": "successfully loaded title_episode to database",
#             # "num_rows": title_ratings.height
#             # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
#         },
#     )


# @dg.asset(
#     deps=["directors"],
#     description="directors database table",
#     group_name="loading",
#     required_resource_keys={
#         "postgres_resource"
#     },  # needed to use file_registry in the asset function
# )
# def directors_loaded(context: dg.AssetExecutionContext, directors: pl.DataFrame) -> dg.MaterializeResult:
#     with context.resources.postgres_resource.connect() as conn:
#         context.log.info(f"Writing directors to database")
#         directors.write_database(
#             table_name="imdb.directors",
#             if_table_exists="replace",
#             connection=conn
#         )

#     return dg.MaterializeResult(
#         # value=title_ratings,
#         # TODO: schema
#         metadata={
#             "status": "successfully loaded title_episode to database",
#             # "num_rows": title_ratings.height
#             # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
#         },
#     )


# @dg.asset(
#     deps=["writers"],
#     description="writers database table",
#     group_name="loading",
#     required_resource_keys={
#         "postgres_resource"
#     },  # needed to use file_registry in the asset function
# )
# def writers_loaded(context: dg.AssetExecutionContext, writers: pl.DataFrame) -> dg.MaterializeResult:
#     with context.resources.postgres_resource.connect() as conn:
#         context.log.info(f"Writing writers to database")
#         writers.write_database(
#             table_name="imdb.writers",
#             if_table_exists="replace",
#             connection=conn
#         )

#     return dg.MaterializeResult(
#         # value=title_ratings,
#         # TODO: schema
#         metadata={
#             "status": "successfully loaded title_episode to database",
#             # "num_rows": title_ratings.height
#             # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
#         },
#     )



# @dg.asset(
#     deps=["name_basics"],
#     description="name_basics database table",
#     group_name="loading",
#     required_resource_keys={
#         "postgres_resource"
#     },  # needed to use file_registry in the asset function
# )
# def name_basics_loaded(context: dg.AssetExecutionContext, name_basics: pl.DataFrame) -> dg.MaterializeResult:
#     with context.resources.postgres_resource.connect() as conn:
#         context.log.info(f"Writing name_basics to database")
#         name_basics.write_database(
#             table_name="imdb.name_basics",
#             if_table_exists="replace",
#             connection=conn
#         )

#     return dg.MaterializeResult(
#         # value=title_ratings,
#         # TODO: schema
#         metadata={
#             "status": "successfully loaded name_basics to database",
#             # "num_rows": title_ratings.height
#             # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
#         },
#     )


# @dg.asset(
#     deps=["title_akas"],
#     description="title_akas database table",
#     group_name="loading",
#     required_resource_keys={
#         "postgres_resource"
#     },  # needed to use file_registry in the asset function
# )
# def title_akas_loaded(context: dg.AssetExecutionContext, title_akas: pl.DataFrame) -> dg.MaterializeResult:
#     with context.resources.postgres_resource.connect() as conn:
#         context.log.info(f"Writing title_akas to database")
#         title_akas.write_database(
#             table_name="imdb.title_akas",
#             if_table_exists="replace",
#             connection=conn
#         )

#     return dg.MaterializeResult(
#         # value=title_ratings,
#         # TODO: schema
#         metadata={
#             "status": "successfully loaded title_akas to database",
#             # "num_rows": title_ratings.height
#             # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
#         },
#     )


# TODO: use dagster partitions, because this loading like this crashes my 64GB RAM desktop
# @dg.asset(
#     deps=["title_principals"],
#     description="title_principals database table",
#     group_name="loading",
#     required_resource_keys={
#         "postgres_resource"
#     },  # needed to use file_registry in the asset function
# )
# def title_principals_loaded(context: dg.AssetExecutionContext, title_principals: pl.DataFrame) -> dg.MaterializeResult:
#     with context.resources.postgres_resource.connect() as conn:
#         context.log.info(f"Writing title_principals to database")
        
#         # we partition the dataframe by category to avoid memory issues when writing to the database
#         count = 0
#         option = "replace"
#         title_principals = title_principals.partition_by("ordering")
#         for partition in title_principals:
#             context.log.info(f"Writing title_principals partition {count} of {len(title_principals)} to database")
#             if count > 0:
#                 option = "append"
#             count += 1
#             partition.write_database(
#                 table_name="imdb.title_principals",
#                 if_table_exists=option,
#                 connection=conn
#             )

#     return dg.MaterializeResult(
#         # value=title_ratings,
#         # TODO: schema
#         metadata={
#             "status": "successfully loaded title_principals to database",
#             # "num_rows": title_ratings.height
#             # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
#         },
#     )

     