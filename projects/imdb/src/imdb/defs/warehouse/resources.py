# import dagster as dg
# from sqlalchemy import create_engine
# import os
# # import polars as pl
# # from dagster_postgres import PostgresResource


# # imdb_database = PostgresResource(
# #     host="localhost",
# #     port=5432,
# #     database="dagster",
# #     user=dg.EnvVar("POSTGRES_USER"),
# #     password=dg.EnvVar("POSTGRES_PASSWORD"),
# # )

# # Define a PostgreSQL resource
# @dg.resource
# def postgres_resource(context):
#     """Creates a SQLAlchemy engine connected to PostgreSQL."""
#     connection_url = (
#         f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
#         f"{os.getenv('HOST')}:{os.getenv('PORT')}/{os.getenv('DATABASE')}"
#     )
#     engine = create_engine(connection_url)
#     return engine


# @dg.definitions
# def resources() -> dg.Definitions:
#     return dg.Definitions(resources={
#         "postgres_resource": postgres_resource,
#     })


import os
from typing import Optional
import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import dagster as dg
from pydantic import Field
import psycopg2


# class PostgresPolarsResource(dg.ConfigurableResource):
#     """Resource for loading Polars DataFrames to PostgreSQL in chunks."""

#     # Configuration parameters using pydantic Field
#     host: str = Field(description="PostgreSQL host")
#     port: int = Field(default=5432, description="PostgreSQL port")
#     database: str = Field(description="PostgreSQL database name")
#     user: str = Field(description="PostgreSQL user")
#     password: str = Field(description="PostgreSQL password")
#     chunk_size: int = Field(
#         default=1_000_000,
#         description="Number of rows to write per chunk"
#     )

#     def _get_engine(self) -> Engine:
#         """Creates a SQLAlchemy engine connected to PostgreSQL."""
#         connection_url = (
#             f"postgresql://{self.user}:{self.password}@"
#             f"{self.host}:{self.port}/{self.database}"
#         )
#         return create_engine(connection_url)

#     def load_dataframe(
#         self,
#         df: pl.DataFrame,
#         table_name: str,
#         schema: str,
#         context: Optional[dg.AssetExecutionContext] = None,
#         truncate_before_load: bool = True
#     ) -> None:
#         """
#         Load a Polars DataFrame to PostgreSQL table in chunks.

#         Args:
#             df: Polars DataFrame to load
#             table_name: Name of the target table
#             schema: Schema where the table resides
#             context: Optional Dagster context for logging
#             truncate_before_load: Whether to empty the table before loading
#         """
#         engine = self._get_engine()
#         full_table_name = f"{schema}.{table_name}"

#         with engine.connect() as conn:
#             # Begin transaction
#             trans = conn.begin()

#             try:
#                 # Empty the table if requested
#                 if truncate_before_load:
#                     if context:
#                         context.log.info(f"Truncating table {full_table_name}")
#                     conn.execute(text(f"TRUNCATE TABLE {full_table_name} CASCADE"))

#                 # Calculate total chunks
#                 total_rows = df.height
#                 total_chunks = (total_rows + self.chunk_size - 1) // self.chunk_size

#                 if context:
#                     context.log.info(
#                         f"Loading {total_rows} rows into {full_table_name} "
#                         f"in {total_chunks} chunks of {self.chunk_size} rows"
#                     )

#                 # Load data in chunks
#                 for offset in range(0, total_rows, self.chunk_size):
#                     chunk_num = offset // self.chunk_size + 1

#                     if context:
#                         context.log.info(
#                             f"Writing chunk {chunk_num}/{total_chunks} "
#                             f"to {full_table_name}"
#                         )

#                     # Extract chunk
#                     batch = df.slice(offset, self.chunk_size)

#                     # Write to database
#                     batch.write_database(
#                         table_name=full_table_name,
#                         if_table_exists="append",
#                         connection=conn
#                     )

#                 # Commit transaction
#                 trans.commit()

#                 if context:
#                     context.log.info(
#                         f"Successfully loaded {total_rows} rows "
#                         f"into {full_table_name}"
#                     )

#             except Exception as e:
#                 # Rollback on error
#                 trans.rollback()
#                 if context:
#                     context.log.error(f"Failed to load data: {str(e)}")
#                 raise
#             finally:
#                 engine.dispose()


# Example usage with environment variables
# postgres_resource = PostgresPolarsResource(
#     host=dg.EnvVar("POSTGRES_HOST"),
#     port=dg.EnvVar.int("POSTGRES_PORT"),
#     database=dg.EnvVar("POSTGRES_DATABASE"),
#     user=dg.EnvVar("POSTGRES_USER"),
#     password=dg.EnvVar("POSTGRES_PASSWORD"),
#     chunk_size=1_000_000
# )


# not sure how configurable resource works, and don't know how to log without it


# # Alternative: Create a configured resource
# postgres_resource_configured = PostgresPolarsResource.configure_at_launch()


# # Example asset using the resource
# @dg.asset(
#     deps=["name_basics_transformed"],
#     description="Data for name_basics table",
#     group_name="loading",
# )
# def name_basics_loaded(
#     context: dg.AssetExecutionContext,
#     postgres: PostgresPolarsResource,
#     name_basics_transformed: pl.DataFrame
# ) -> None:
#     """Load name_basics data to PostgreSQL."""

#     postgres.load_dataframe(
#         df=name_basics_transformed,
#         table_name="name_basics",
#         schema="imdb",
#         context=context,
#         truncate_before_load=True
#     )

#     context.add_output_metadata({
#         "rows_loaded": name_basics_transformed.height,
#         "table": "imdb.name_basics"
#     })


# # old code that works
# class PostgresResource():
#     """Configuration schema for PostgreSQL resource."""

#     def __init__(self, host: str, port: int, database: str, user: str, password: str, chunk_size: int):
#         self.host: str = host
#         self.port: int = port
#         self.database: str = database
#         self.user: str = user
#         self.password: str = password
#         self.chunk_size: int = chunk_size | 300_000

#     def _get_connection(self) -> psycopg2.extensions.connection:
#             """Creates a psycopg2 connection to PostgreSQL."""

#             message = f"Connecting to PostgreSQL at {self.host}:{self.port} database: {self.database}"
#             print(message)

#             try:
#                 conn = psycopg2.connect(
#                     dbname=self.database,
#                     user=self.user,
#                     host=self.host,
#                     password=self.password,
#                     port=self.port
#                 )
#                 return conn
#             except Exception as e:
#                 print(f"Unable to connect to the database: {str(e)}")
#                 raise

#     def _get_connection_string(self) -> str:
#         """Constructs a PostgreSQL connection string."""
#         return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

#     def empty_table(self, table_name: str, schema: str) -> None:
#         """Truncate the specified table."""
#         conn = self._get_connection()

#         try:
#             with conn.cursor() as cur:
#                 full_table_name = f"{schema}.{table_name}"
#                 cur.execute(f"TRUNCATE TABLE {full_table_name} CASCADE")
#                 conn.commit()
#         except Exception as e:
#             message = f"Failed to truncate table {schema}.{table_name}: {str(e)}"
#             print(message)
#             raise
#         finally:
#             conn.close()

#     def get_table_columns(self, table_name: str, schema: str) -> list:
#         """Get column names of the specified table."""
#         conn = self._get_connection()
#         try:
#             with conn.cursor() as cur:
#                 cur.execute(
#                     f"""
#                     SELECT column_name
#                     FROM information_schema.columns
#                     where
#                         table_schema='{schema}'
#                         and
#                         table_name='{table_name}'
#                     order by ordinal_position;
#                     """

#                 )
#                 columns = [row[0] for row in cur.fetchall()]
#                 return columns
#         except Exception as e:
#             print(f"Failed to get table columns: {schema}.{table_name}: {str(e)}")
#             raise
#         finally:
#             conn.close()

#     def _check_table_columns(self, table_name: str, schema: str, df: pl.DataFrame) -> None:
#         """Check if DataFrame columns match the target table columns."""
#         table_columns = self.get_table_columns(table_name, schema)
#         df_columns = df.columns

#         missing_columns = set(table_columns) - set(df_columns)
#         extra_columns = set(df_columns) - set(table_columns)

#         if missing_columns:
#             raise ValueError(f"Missing columns in DataFrame: {missing_columns}")
#         if extra_columns:
#             raise ValueError(f"Extra columns in DataFrame: {extra_columns}")

#     def load_polars_dataframe(self, df: pl.DataFrame, table_name: str, schema: str) -> None:
#         """Load a Polars DataFrame to PostgreSQL in chunks."""
#         try:
#             self.empty_table(table_name, schema)
#             total_chunks = (df.height + self.chunk_size - 1) // self.chunk_size

#             connection_string = self._get_connection_string()

#             for offset in range(0, df.height, self.chunk_size):
#                 message = f"Loading chunk {offset // self.chunk_size + 1} of {total_chunks} into {schema}.{table_name}"
#                 print(message)

#                 batch = df.slice(offset, self.chunk_size)
#                 batch.write_database(
#                     table_name=f"{schema}.{table_name}",
#                     if_table_exists="append",
#                     connection=connection_string
#                 )
#         except Exception as e:
#             message = f"Failed to load data: {str(e)}"
#             print(message)
#             raise

#     def get_query_results(self, query: str) -> pl.DataFrame:
#         """Execute a SQL query and return results as a Polars DataFrame."""
#         conn = self._get_connection()
#         try:
#             with conn.cursor() as cur:
#                 cur.execute(query)
#                 columns = [desc[0] for desc in cur.description]
#                 data = cur.fetchall()
#                 return pl.DataFrame(data, schema=columns)
#         except Exception as e:
#             message = f"Failed to execute query: {str(e)}"
#             print(message)
#             raise
#         finally:
#             conn.close()


# postgres_resource = PostgresResource(
#     host=dg.EnvVar("POSTGRES_HOST").get_value(),
#     port=dg.EnvVar.int("POSTGRES_PORT").get_value(),
#     database=dg.EnvVar("POSTGRES_DATABASE").get_value(),
#     user=dg.EnvVar("POSTGRES_USER").get_value(),
#     password=dg.EnvVar("POSTGRES_PASSWORD").get_value(),
#     chunk_size=500_000
# )


# # Define your Dagster Definitions
# defs = dg.Definitions(
#     # assets=[name_basics_loaded],
#     resources={
#         "postgres": postgres_resource,
#     }
# )


from typing import Optional


class PostgresResource(dg.ConfigurableResource):
    """Configuration schema for PostgreSQL resource."""

    host: str
    port: int
    database: str
    user: str
    password: str
    chunk_size: int = 300_000

    def _get_connection(
        self, context: dg.AssetExecutionContext
    ) -> psycopg2.extensions.connection:
        """Creates a psycopg2 connection to PostgreSQL."""

        message = f"Connecting to PostgreSQL at {self.host}:{self.port} database: {self.database}"
        context.log.info(message)

        try:
            conn = psycopg2.connect(
                dbname=self.database,
                user=self.user,
                host=self.host,
                password=self.password,
                port=self.port,
            )
            return conn
        except Exception as e:
            context.log.error(f"Unable to connect to the database: {str(e)}")
            raise

    def _get_connection_string(self) -> str:
        """Constructs a PostgreSQL connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def __empty_table(
        self, context: dg.AssetExecutionContext, table_name: str, schema: str
    ) -> None:
        """Truncate the specified table."""
        conn = self._get_connection(context)

        try:
            with conn.cursor() as cur:
                full_table_name = f"{schema}.{table_name}"
                cur.execute(f"TRUNCATE TABLE {full_table_name} CASCADE")
                conn.commit()
        except Exception as e:
            message = f"Failed to truncate table {schema}.{table_name}: {str(e)}"
            context.log.error(message)
            raise
        finally:
            conn.close()

    def get_table_columns(
        self, context: dg.AssetExecutionContext, table_name: str, schema: str
    ) -> list:
        """Get column names of the specified table."""
        conn = self._get_connection(context)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT column_name
                    FROM information_schema.columns
                    where 
                        table_schema='{schema}'
                        and
                        table_name='{table_name}'
                    order by ordinal_position;
                    """
                )
                columns = [row[0] for row in cur.fetchall()]
                return columns
        except Exception as e:
            message = f"Failed to get table columns: {schema}.{table_name}: {str(e)}"
            context.log.error(message)
            raise
        finally:
            conn.close()

    def _check_table_columns(
        self, table_name: str, schema: str, df: pl.DataFrame
    ) -> None:
        """Check if DataFrame columns match the target table columns."""
        table_columns = self.get_table_columns(table_name, schema)
        df_columns = df.columns

        missing_columns = set(table_columns) - set(df_columns)
        extra_columns = set(df_columns) - set(table_columns)

        if missing_columns:
            raise ValueError(f"Missing columns in DataFrame: {missing_columns}")
        if extra_columns:
            raise ValueError(f"Extra columns in DataFrame: {extra_columns}")

    def load_polars_dataframe(
        self,
        context: dg.AssetExecutionContext,
        df: pl.DataFrame,
        table_name: str,
        schema: str,
    ) -> None:
        """Load a Polars DataFrame to PostgreSQL in chunks."""
        try:
            self.__empty_table(context, table_name, schema)
            total_chunks = (df.height + self.chunk_size - 1) // self.chunk_size

            connection_string = self._get_connection_string()

            for offset in range(0, df.height, self.chunk_size):
                message = f"Loading into {schema}.{table_name} chunk {offset // self.chunk_size + 1} of {total_chunks}"
                context.log.info(message)

                batch = df.slice(offset, self.chunk_size)
                batch.write_database(
                    table_name=f"{schema}.{table_name}",
                    if_table_exists="append",
                    connection=connection_string,
                )
        except Exception as e:
            context.log.info(f"Failed to load data: {str(e)}")
            raise

    def get_query_results(
        self, context: dg.AssetExecutionContext, query: str
    ) -> pl.DataFrame:
        """Execute a SQL query and return results as a Polars DataFrame."""
        conn = self._get_connection(context)
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                data = cur.fetchall()
                return pl.DataFrame(data, schema=columns)
        except Exception as e:
            context.log.info(f"Failed to execute query: {str(e)}")
            raise
        finally:
            conn.close()
    
    def execute_query(self, context: dg.AssetExecutionContext, query: str) -> None:
        """For executing queries that don't return results."""
        conn = self._get_connection(context)
        try:
            with conn.cursor() as cur:
                cur.execute(query)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as e:
            context.log.info(f"Failed to execute query: {str(e)}")
            raise
        finally:
            conn.close()



postgres_resource = PostgresResource(
    host=dg.EnvVar("POSTGRES_HOST").get_value(),
    port=dg.EnvVar.int("POSTGRES_PORT").get_value(),
    database=dg.EnvVar("POSTGRES_DATABASE").get_value(),
    user=dg.EnvVar("POSTGRES_USER").get_value(),
    password=dg.EnvVar("POSTGRES_PASSWORD").get_value(),
    chunk_size=500_000,
)


defs = dg.Definitions(
    resources={
        "postgres": postgres_resource,
    }
)
