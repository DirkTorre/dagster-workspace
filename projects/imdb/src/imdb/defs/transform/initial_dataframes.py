import dagster as dg
import polars as pl
from . import loaders

"""This module contains dataframes from the raw data where only the datatypes have been corrected."""


# @dg.asset(
#     deps=["title_basics_raw"],
#     description="Data for title_basics table",
#     group_name="staging",
#     required_resource_keys={
#         "file_registry"
#     },  # needed to use file_registry in the asset function
# )
# def title_basics(context: dg.AssetExecutionContext):
#     FileRegistry = context.resources.file_registry
#     raw_data_path = FileRegistry.get_path("title_basics")
#     context.log.info(f"Reading raw data from {raw_data_path}")

#     df = loaders.load_title_basics_memory(raw_data_path)

#     context.log.info("Creating title_basics")
#     title_basics: pl.DataFrame = df.drop(
#         "genres"
#     )

#     return dg.MaterializeResult(
#         value=title_basics,
#         # TODO: schema
#         metadata={
#             "num_rows": title_basics.height,
#             "preview": title_basics.head().to_pandas().to_markdown(),  # moet beter
#         },
#     )


# @dg.asset(
#     deps=["title_basics_raw"],
#     description="Data for genres table",
#     group_name="staging",
#     required_resource_keys={
#         "file_registry"
#     },  # needed to use file_registry in the asset function
# )
# def genres(context: dg.AssetExecutionContext):
#     FileRegistry = context.resources.file_registry
#     raw_data_path = FileRegistry.get_path("title_basics")
#     context.log.info(f"Reading raw data from {raw_data_path}")

#     df = loaders.load_title_basics_memory(raw_data_path)

#     context.log.info("Creating genres")
#     genres = df.select(pl.col("tconst"), pl.col("genres").str.split(",")).explode(
#         "genres"
#     )

#     # TODO: more transformation for dataabse ingestion (header names, data types, etc.)

#     return dg.MaterializeResult(
#         value=genres,
#         # TODO: schema
#         metadata={
#             "num_rows": genres.height
#             # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
#         },
#     )


@dg.asset(
    deps=["name_basics_raw"],
    description="Data for name_basics table",
    group_name="staging",
    required_resource_keys={
        "file_registry"
    },  # needed to use file_registry in the asset function
)
def name_basics(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("name_basics")
    context.log.info(f"Reading raw data from {raw_data_path}")

    df = loaders.load_name_basics_memory(raw_data_path)

    context.log.info("Creating name_tables")
    name_basics = df.drop(["primaryProfession", "knownForTitles"])

    # TODO: more transformation for dataabse ingestion (header names, data types, etc.)

    return dg.MaterializeResult(
        value=name_basics,
        # TODO: schema
        metadata={
            "num_rows": name_basics.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )


# @dg.asset(
#     deps=["name_basics_raw"],
#     description="Data for primary_profession table",
#     group_name="staging",
#     required_resource_keys={
#         "file_registry"
#     },  # needed to use file_registry in the asset function
# )
# def primary_profession(context: dg.AssetExecutionContext):
#     FileRegistry = context.resources.file_registry
#     raw_data_path = FileRegistry.get_path("name_basics")
#     context.log.info(f"Reading raw data from {raw_data_path}")

#     df = loaders.load_name_basics_memory(raw_data_path)

#     context.log.info("Creating primary_profession")
#     primary_profession = df.select(
#         pl.col("nconst"), pl.col("primaryProfession").str.split(",")
#     ).explode("primaryProfession")

#     # TODO: more transformation for dataabse ingestion (header names, data types, etc.)

#     return dg.MaterializeResult(
#         value=primary_profession,
#         # TODO: schema
#         metadata={
#             "num_rows": primary_profession.height
#             # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
#         },
#     )


# @dg.asset(
#     deps=["name_basics_raw"],
#     description="Data for known_for_titles table",
#     group_name="staging",
#     required_resource_keys={
#         "file_registry"
#     },  # needed to use file_registry in the asset function
# )
# def known_for_titles(context: dg.AssetExecutionContext):
#     FileRegistry = context.resources.file_registry
#     raw_data_path = FileRegistry.get_path("name_basics")
#     context.log.info(f"Reading raw data from {raw_data_path}")

#     df = loaders.load_name_basics_memory(raw_data_path)

#     context.log.info("Creating known_for_titles")
#     known_for_titles = df.select(
#         pl.col("nconst"), pl.col("knownForTitles").str.split(",")
#     ).explode("knownForTitles")

#     # TODO: more transformation for dataabse ingestion (header names, data types, etc.)

#     return dg.MaterializeResult(
#         value=known_for_titles,
#         # TODO: schema
#         metadata={
#             "num_rows": known_for_titles.height
#             # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
#         },
#     )


@dg.asset(
    deps=["title_principals_raw"],
    description="Data for title_principals table",
    group_name="staging",
    required_resource_keys={
        "file_registry"
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

    # TODO: more transformation for dataabse ingestion (header names, data types, etc.)

    return dg.MaterializeResult(
        value=title_principals,
        # TODO: schema
        metadata={
            "num_rows": title_principals.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )


@dg.asset(
    deps=["title_crew_raw"],
    description="Data for directors table",
    group_name="staging",
    required_resource_keys={
        "file_registry"
    },  # needed to use file_registry in the asset function
)
def directors(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_crew")
    context.log.info(f"Reading raw data from {raw_data_path}")

    df = loaders.load_title_crew_memory(raw_data_path)

    directors = df.select(
        pl.col("tconst"),
        pl.col("directors").str.split(","),
    ).explode("directors")

    # TODO: more transformation for dataabse ingestion (header names, data types, etc.)

    return dg.MaterializeResult(
        value=directors,
        # TODO: schema
        metadata={
            "num_rows": directors.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )


@dg.asset(
    deps=["title_crew_raw"],
    description="Data for writers table",
    group_name="staging",
    required_resource_keys={
        "file_registry"
    },  # needed to use file_registry in the asset function
)
def writers(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_crew")
    context.log.info(f"Reading raw data from {raw_data_path}")

    df = loaders.load_title_crew_memory(raw_data_path)

    writers = df.select(
        pl.col("tconst"),
        pl.col("writers").str.split(","),
    ).explode("writers")
    # TODO: more transformation for dataabse ingestion (header names, data types, etc.)

    return dg.MaterializeResult(
        value=writers,
        # TODO: schema
        metadata={
            "num_rows": writers.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )


@dg.asset(
    deps=["title_ratings_raw"],
    description="Data for title_ratings table",
    group_name="staging",
    required_resource_keys={
        "file_registry"
    },  # needed to use file_registry in the asset function
)
def title_ratings(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_ratings")
    context.log.info(f"Reading raw data from {raw_data_path}")

    title_ratings = pl.read_csv(
        raw_data_path,
        has_header=True,
        separator="\t",
        truncate_ragged_lines=True,
        null_values="\\N",
        quote_char=None,
        schema={
            "tconst": pl.Utf8,
            "averageRating": pl.Float16,
            "numVotes": pl.UInt32,
        },
    )

    # TODO: more transformation for dataabse ingestion (header names, data types, etc.)

    return dg.MaterializeResult(
        value=title_ratings,
        # TODO: schema
        metadata={
            "num_rows": title_ratings.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )


@dg.asset(
    deps=["title_episode_raw"],
    description="Data for title_episode table",
    group_name="staging",
    required_resource_keys={
        "file_registry"
    },  # needed to use file_registry in the asset function
)
def title_episode(context: dg.AssetExecutionContext):
    FileRegistry = context.resources.file_registry
    raw_data_path = FileRegistry.get_path("title_episode")
    context.log.info(f"Reading raw data from {raw_data_path}")

    title_episode = pl.read_csv(
        raw_data_path,
        has_header=True,
        separator="\t",
        truncate_ragged_lines=True,
        null_values="\\N",
        quote_char=None,
        schema={
            "tconst": pl.Utf8,
            "parentTconst": pl.Utf8,
            "seasonNumber": pl.UInt16,
            "episodeNumber": pl.UInt32,
        },
    )

    # TODO: more transformation for dataabse ingestion (header names, data types, etc.)

    return dg.MaterializeResult(
        value=title_episode,
        # TODO: schema
        metadata={
            "num_rows": title_episode.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )


@dg.asset(
    deps=["title_akas_raw"],
    description="Data for title_akas table",
    group_name="staging",
    required_resource_keys={
        "file_registry"
    },  # needed to use file_registry in the asset function
)
def title_akas(context: dg.AssetExecutionContext):
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

    return dg.MaterializeResult(
        value=title_akas,
        # TODO: schema
        metadata={
            "num_rows": title_akas.height
            # "preview": title_basics.head().to_pandas().to_markdown() # moet beter
        },
    )
