# TODO: big file, needs dagster partitioning

# import dagster as dg
# import polars as pl

# @dg.asset(
#     deps=["title_principals_raw"],
#     description="Data for title_principals table",
#     group_name="staging",
#     required_resource_keys={
#         "file_registry"
#     },  # needed to use file_registry in the asset function
# )
# def title_principals(context: dg.AssetExecutionContext):
#     FileRegistry = context.resources.file_registry
#     raw_data_path = FileRegistry.get_path("title_principals")
#     context.log.info(f"Reading raw data from {raw_data_path}")

#     df = pl.read_csv(
#         raw_data_path,
#         has_header=True,
#         separator="\t",
#         truncate_ragged_lines=True,
#         null_values="\\N",
#         quote_char=None,
#         schema={
#             "tconst": pl.Utf8,
#             "ordering": pl.Int16,
#             "nconst": pl.Utf8,
#             "category": pl.Utf8,
#             "job": pl.Utf8,
#             "characters": pl.Utf8,
#         },
#     )

#     context.log.info("Creating title_principals")

#     title_principals = df.with_columns(
#         pl.col("characters").str.replace_all(r'[\[\]"]', "")
#     )

#     # TODO: more transformation for dataabse ingestion (header names, data types, etc.)

#     return dg.MaterializeResult(
#         value=title_principals,
#         # TODO: schema
#         metadata={
#             "num_rows": title_principals.height
#             # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
#         },
#     )
