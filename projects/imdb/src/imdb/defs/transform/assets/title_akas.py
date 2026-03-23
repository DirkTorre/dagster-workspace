import dagster as dg
import polars as pl

@dg.asset(
    deps=["title_akas_raw"],
    description="Data for title_akas table",
    group_name="transform_and_load",
    required_resource_keys={
        "file_registry",
        "postgres_resource",
    },  # needed to use file_registry in the asset function
)
def title_akas_loaded(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_akas")
    context.log.info(f"Reading raw data from {raw_data_path}")

    title_akas = pl.read_csv(
        raw_data_path,
        has_header=True,
        separator="\t",
        truncate_ragged_lines=True,
        null_values="\\N",
        quote_char=None,
        schema={
            "titleId": pl.Utf8,
            "ordering": pl.UInt16,
            "title": pl.Utf8,
            "region": pl.Utf8,
            "language": pl.Utf8,
            "types": pl.Utf8,
            "attributes": pl.Utf8,
            "isOriginalTitle": pl.UInt8,
        },
    )

    title_akas = title_akas.with_columns(
        pl.col("isOriginalTitle").cast(pl.Boolean)
    )

    with context.resources.postgres_resource.connect() as conn:
        context.log.info(f"Writing title_akas to database")
        title_akas.write_database(
            table_name="imdb.title_akas",
            if_table_exists="replace",
            connection=conn
        )

    return dg.MaterializeResult(
        value=title_akas,
        # TODO: schema
        metadata={
            "num_rows": title_akas.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
