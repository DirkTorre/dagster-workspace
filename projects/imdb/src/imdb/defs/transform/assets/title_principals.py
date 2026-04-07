import dagster as dg
import polars as pl


@dg.asset(
    deps=["title_principals_raw"],
    description="Data for title_principals table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres",
    },  # needed to use file_registry in the asset function
)
def title_principals(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_principals")
    context.log.info(f"Reading raw data from {raw_data_path}")

    df = pl.read_csv(
        raw_data_path,
        has_header=True,
        separator="\t",
        truncate_ragged_lines=True,
        null_values="\\N",
        quote_char=None,
        schema={
            "tconst": pl.Utf8,
            "ordering": pl.Int16,
            "nconst": pl.Utf8,
            "category": pl.Utf8,
            "job": pl.Utf8,
            "characters": pl.Utf8,
        },
    )

    context.log.info("Creating title_principals")

    title_principals = df.with_columns(
        pl.col("characters").str.replace_all(r'[\[\]"]', "")
    )

    pr = context.resources.postgres
    context.log.info("Writing title_principals to imdb.title_principals")
    pr.load_polars_dataframe(
        df=title_principals, table_name="title_principals", schema="imdb"
    )

    return dg.MaterializeResult(
        value=title_principals,
        # TODO: schema
        metadata={
            "num_rows": title_principals.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
