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
    return title_basics.with_columns(
        pl.col("isAdult").cast(pl.Boolean)
    )


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