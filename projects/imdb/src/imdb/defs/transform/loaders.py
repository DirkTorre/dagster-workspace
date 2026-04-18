import polars as pl


def load_title_basics_memory(raw_data_path: str) -> pl.DataFrame:
    title_basics = pl.read_csv(
        raw_data_path,
        has_header=True,
        separator="\t",
        truncate_ragged_lines=True,
        null_values="\\N",
        quote_char=None,
        schema={
            "tconst": pl.Utf8,
            "titleType": pl.Utf8,
            "primaryTitle": pl.Utf8,
            "originalTitle": pl.Utf8,
            "isAdult": pl.Int16,
            "startYear": pl.Int16,
            "endYear": pl.Int16,
            "runtimeMinutes": pl.Int32,
            "genres": pl.Utf8,
        },
    )
    return title_basics.with_columns(pl.col("isAdult").cast(pl.Boolean))


def load_name_basics_memory(raw_data_path: str) -> pl.DataFrame:
    return pl.read_csv(
        raw_data_path,
        has_header=True,
        separator="\t",
        truncate_ragged_lines=True,
        null_values="\\N",
        quote_char=None,
        schema={
            "nconst": pl.Utf8,
            "primaryName": pl.Utf8,
            "birthYear": pl.Int16,
            "deathYear": pl.Int16,
            "primaryProfession": pl.Utf8,
            "knownForTitles": pl.Utf8,
        },
    )


def load_title_crew_memory(raw_data_path: str) -> pl.DataFrame:
    return pl.read_csv(
        raw_data_path,
        has_header=True,
        separator="\t",
        truncate_ragged_lines=True,
        null_values="\\N",
        quote_char=None,
        schema={
            "tconst": pl.Utf8,
            "directors": pl.Utf8,
            "writers": pl.Utf8,
        },
    )


def reload_table_with_fk(
    context,
    df: pl.DataFrame,
    table: str,
    schema: str,
    pre_load_query: str,
    pre_load_message: str,
    post_load_query: str,
    post_load_message: str,
):
    """Drop FK, reload table, re-add FK."""
    pr = context.resources.postgres

    # 1. Drop FK's
    context.log.info(pre_load_message)
    pr.execute_query(
        context,
        pre_load_query,
    )

    # 2. Load data
    context.log.info(f"Writing dataframe to {schema}.{table}")
    pr.load_polars_dataframe(
        context,
        df=df,
        table_name=table,
        schema=schema,
    )

    # 3. Add FK
    context.log.info(post_load_message)
    pr.execute_query(
        context,
        post_load_query,
    )
