# import dagster as dg
# import polars as pl

# @dg.asset(
#     deps=["title_ratings", "title_episode", "directors", "writers"],  # assets that this asset depends on
#     description="",
#     group_name="production",
#     required_resource_keys={
#         "postgres_resource"
#     },
#     # required_resource_keys={
#     #     "file_registry"
#     # },  # needed to use file_registry in the asset function
# )
# def imdb_database(
#     context: dg.AssetExecutionContext, 
#     title_ratings: pl.DataFrame,
#     title_episode: pl.DataFrame,
#     directors: pl.DataFrame,
#     writers: pl.DataFrame,
#     ) -> None:
#     """Example asset that transforms the title_basics dataset"""
    
#     # load title basics data into database
#     # uri = "postgresql://username:password@server:port/database" 
#     # uri = "postgresql://username:password@server:port/database?currentSchema=schama_name"
    
#     # uri = "postgresql://{username}:{password}@{server}:{port}/{database}?currentSchema={schema_name}"
#     # DONE: make dagster database and an imdb schema
#     # DONE: kijk of de nieuwe conecetion string werkt (met schema)
#     # TODO: maak betere kolom namen
#     # TODO: maak een database resource die connection strings kan beheren en gebruiken in assets
#     # TODO: maak script data database en gebruiker aanmaakt
#     # TODO: maak script die relaties afbreekt en opbouwt tussen tabellen
#     # TODO: postgres resource maken

#     # from dagster import resource, job, op
#     # from dagster_postgres import PostgresResource

#     # # Define the Postgres resource
#     # postgres_resource = PostgresResource(
#     # username="your_username",
#     # password="your_password",
#     # hostname="localhost",
#     # db_name="your_database"
#     # )

#     # @op(required_resource_keys={"postgres"})
#     # def fetch_data_from_postgres(context):
#     # result = context.resources.postgres.execute_query("SELECT * FROM your_table;")
#     # context.log.info(f"Fetched {len(result)} rows from the database.")
#     # return result


#     # https://pypi.org/project/dagster-postgres/
    
#     # uri = "postgresql://dagster:4g4WF~E*Zsl217aSErNKu9olVTuP=,HO]dP/FAlH@localhost:5432/dagster"
    
#     # title_ratings.write_database(
#     #     table_name="imdb.title_ratings",
#     #     if_table_exists="replace", 
#     #     connection=uri
#     # )

#     # title_episode.write_database(
#     #     table_name="imdb.title_episode",
#     #     if_table_exists="replace", 
#     #     connection=uri
#     # )
    
#     with context.resources.postgres_resource.connect() as conn:
#         # context.log.info(f"Writing title_ratings to database")
#         # title_ratings.write_database(
#         #     table_name="imdb.title_ratings",
#         #     if_table_exists="replace", 
#         #     connection=conn
#         # )
        
#         # context.log.info(f"Writing title_episode to database")
#         # title_episode.write_database(
#         #     table_name="imdb.title_episode",
#         #     if_table_exists="replace", 
#         #     connection=conn
#         # )

#         # context.log.info(f"Writing directors to database")
#         directors.write_database(
#             table_name="imdb.directors",
#             if_table_exists="replace",
#             connection=conn
#         )

#         # context.log.info(f"Writing writers to database")
#         writers.write_database(
#             table_name="imdb.writers",
#             if_table_exists="replace",
#             connection=conn
#         )


    
    
    
#     # # Return the transformed DataFrame as a MaterializeResult
#     # return dg.MaterializeResult(
#     #     value=movies_df,
#     #     metadata={
#     #         "num_movies": num_movies,
#     #         "source_asset": "title_basics",
#     #     }    
#     # )
