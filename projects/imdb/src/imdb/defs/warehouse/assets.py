import dagster as dg
import polars as pl


# @dg.asset(
#     deps=["title_genres_loaded", "title_basics_loaded"],
#     group_name="warehouse",
#     required_resource_keys={
#         "postgres",
#     },
# )
# def genres(context: dg.AssetExecutionContext):
#     pr = context.resources.postgres

#     # context.log.info(
#     #     "Adding imdb.title_basics (tconst) constraint to imdb.title_genres"
#     # )

#     # pr.execute_query(
#     #     context,
#     #     """
#     #     ALTER TABLE IF EXISTS imdb.title_genres
#     #     ADD CONSTRAINT title_genres_tconst_fkey FOREIGN KEY (tconst)
#     #     REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
#     #     ON UPDATE NO ACTION
#     #     ON DELETE CASCADE;
#     #     """,
#     # )


@dg.asset(
    deps=["name_known_for_titles_loaded", "title_basics_loaded"],
    group_name="warehouse",
    required_resource_keys={
        "postgres",
    },
)
def name_known_for_titles(context: dg.AssetExecutionContext):
    pr = context.resources.postgres

    # TODO: works in postgres, must check if it works here

    context.log.info(
        "removing rows from imdb.name_known_for_titles that are don't have a tconst in imdb.title_basics"
    )

    pr.execute_query(
        context,
        """
        DELETE FROM IMDB.NAME_KNOWN_FOR_TITLES
        WHERE
            TCONST IN (
                SELECT
                    TCONST
                FROM
                    IMDB.NAME_KNOWN_FOR_TITLES
                EXCEPT
                SELECT
                    TCONST
                FROM
                    IMDB.TITLE_BASICS
            );        
        """,
    )

    context.log.info(
        "Adding imdb.title_basics (tconst) constraint to imdb.name_known_for_titles"
    )

    pr.execute_query(
        context,
        """
        ALTER TABLE IF EXISTS imdb.name_known_for_titles
        ADD CONSTRAINT name_known_for_titles_fk FOREIGN KEY (tconst)
        REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE;
        """,
    )


# @dg.asset(
#     deps=["title_basics_loaded", "title_ratings_loaded"],
#     group_name="warehouse",
#     required_resource_keys={
#         "postgres",
#     },
# )
# def title_ratings(context: dg.AssetExecutionContext):
#     pr = context.resources.postgres

#     context.log.info(
#         "Adding imdb.title_basics (tconst) constraint to imdb.title_ratings"
#     )

#     pr.execute_query(
#         context,
#         """
#         ALTER TABLE IF EXISTS imdb.title_ratings
#         ADD CONSTRAINT title_ratings_fk FOREIGN KEY (tconst)
#         REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
#         ON UPDATE NO ACTION
#         ON DELETE CASCADE;
#         """,
#     )


# @dg.asset(
#     deps=["title_basics_loaded", "title_writers_loaded", "name_basics_loaded"],
#     group_name="warehouse",
#     required_resource_keys={
#         "postgres",
#     },
# )
# def title_writers(context: dg.AssetExecutionContext):
#     pr = context.resources.postgres

#     context.log.info(
#         "Adding imdb.title_basics (tconst) and imdb.name_basics (nconst) constraint to imdb.title_writers"
#     )

#     pr.execute_query(
#         context,
#         """        
#         ALTER TABLE IF EXISTS imdb.title_writers
#         ADD CONSTRAINT title_writers_fk_nconst FOREIGN KEY (nconst)
#         REFERENCES imdb.name_basics (nconst) MATCH SIMPLE
#         ON UPDATE NO ACTION
#         ON DELETE NO ACTION;

#         ALTER TABLE IF EXISTS imdb.title_writers
#         ADD CONSTRAINT title_writers_fk_tconst FOREIGN KEY (tconst)
#         REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
#         ON UPDATE NO ACTION
#         ON DELETE NO ACTION;
#         """,
#     )


# @dg.asset(
#     deps=["title_basics_loaded", "title_directors_loaded", "name_basics_loaded"],
#     group_name="warehouse",
#     required_resource_keys={
#         "postgres",
#     },
# )
# def title_directors(context: dg.AssetExecutionContext):
#     pr = context.resources.postgres

#     context.log.info(
#         "Adding imdb.title_basics (tconst) and imdb.name_basics (nconst) constraint to imdb.title_directors"
#     )

#     pr.execute_query(
#         context,
#         """        
#         ALTER TABLE IF EXISTS imdb.title_directors
#         ADD CONSTRAINT title_directors_fk_nconst FOREIGN KEY (nconst)
#         REFERENCES imdb.name_basics (nconst) MATCH SIMPLE
#         ON UPDATE NO ACTION
#         ON DELETE NO ACTION;

#         ALTER TABLE IF EXISTS imdb.title_writers
#         ADD CONSTRAINT title_directors_fk_tconst FOREIGN KEY (tconst)
#         REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
#         ON UPDATE NO ACTION
#         ON DELETE NO ACTION;
#         """,
#     )


@dg.asset(
    deps=["name_basics_loaded", "name_primary_profession_loaded"],
    group_name="warehouse",
    required_resource_keys={
        "postgres",
    },
)
def name_primary_profession(context: dg.AssetExecutionContext):
    pr = context.resources.postgres
    context.log.info(
        "Adding imdb.title_basics (tconst) and imdb.name_basics (nconst) constraint to imdb.title_writers"
    )

    pr.execute_query(
        context,
        """ 
        ALTER TABLE IF EXISTS imdb.name_primary_profession
        ADD CONSTRAINT name_primary_profession_nconst_fkey FOREIGN KEY (nconst)
        REFERENCES imdb.name_basics (nconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE;
        """,
    )


# @dg.asset(
#     deps=["name_basics_loaded"],
#     group_name="warehouse",
#     required_resource_keys={
#         "postgres",
#     },
# )
# def name_basics(context: dg.AssetExecutionContext):
#     """
#     Doesn't have any database constraints that need to be build
#     """
#     pass


# TODO: gaf een error, nog even kijken waarom
@dg.asset(
    deps=["title_akas_loaded", "title_basics_loaded"],
    group_name="warehouse",
    required_resource_keys={
        "postgres",
    },
)
def title_akas(context: dg.AssetExecutionContext):
    pr = context.resources.postgres
    context.log.info(
        "Adding imdb.title_basics (tconst) and imdb.name_basics (nconst) constraint to imdb.title_writers"
    )

    pr.execute_query(
        context,
        """ 
        ALTER TABLE IF EXISTS imdb.title_akas
        ADD CONSTRAINT title_akas_title_id_fkey FOREIGN KEY (title_id)
        REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;
        """,
    )


# @dg.asset(
#     deps=["title_basics_loaded"],
#     group_name="warehouse",
# )
# def title_basics(context: dg.AssetExecutionContext):
#     """
#     Does not need to define relations, because they are defined by other tables.
#     """
#     pass


@dg.asset(
    deps=["title_episode_loaded", "title_basics_loaded"],
    group_name="warehouse",
    required_resource_keys={
        "postgres",
    },
)
def title_episode(context: dg.AssetExecutionContext):
    pr = context.resources.postgres
    context.log.info(
        "Adding imdb.title_basics (tconst) and imdb.name_basics (nconst) constraint to imdb.title_writers"
    )

    pr.execute_query(
        context,
        """ 
        ALTER TABLE IF EXISTS imdb.title_episode
        ADD CONSTRAINT title_akas_tconst_fkey FOREIGN KEY (tconst)
        REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;

        ALTER TABLE IF EXISTS imdb.title_episode
        ADD CONSTRAINT title_akas_parenttconst_fkey FOREIGN KEY (tconst)
        REFERENCES imdb.title_basics (tconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION;
        """,
    )


# @dg.asset(
#     deps=["title_basics_loaded", "title_writers_loaded", "name_basics_loaded"],
#     group_name="warehouse",
#     required_resource_keys={
#         "postgres",
#     }
# )
# def title_writers(context: dg.AssetExecutionContext):
#     pr = context.resources.postgres

#     context.log.info("Adding imdb.title_basics (tconst) and imdb.name_basics (nconst) constraint to imdb.title_writers")

#     pr.execute_query(
#         context,
#         """
#         """


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
