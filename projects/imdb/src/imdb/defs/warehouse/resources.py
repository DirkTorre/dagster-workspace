import dagster as dg
from sqlalchemy import create_engine
import os
# import polars as pl
# from dagster_postgres import PostgresResource


# imdb_database = PostgresResource(
#     host="localhost",
#     port=5432,
#     database="dagster",
#     user=dg.EnvVar("POSTGRES_USER"),
#     password=dg.EnvVar("POSTGRES_PASSWORD"),
# )

# Define a PostgreSQL resource
@dg.resource
def postgres_resource(context):
    """Creates a SQLAlchemy engine connected to PostgreSQL."""
    connection_url = (
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('HOST')}:{os.getenv('PORT')}/{os.getenv('DATABASE')}"
    )
    engine = create_engine(connection_url)
    return engine



@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={
        "postgres_resource": postgres_resource,
    })
