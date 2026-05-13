from datetime import datetime

import dagster as dg
import requests
import subprocess
from pathlib import Path
import polars as pl
from xlsxwriter import Workbook
from pathlib import Path

from imdb.defs.ingestion.file_configs import FileConfig, FILE_CONFIGS


@dg.asset(
    deps=[
        "title_ratings_loaded",
        "title_basics_loaded",
        "title_directors_loaded",
        "name_basics_loaded",
        "title_genres",
    ],
    name="movie_list",
    description="Create excel file with movie list and watch status.",
    group_name="output",
    # automation_condition=dg.AutomationCondition.on_cron("@daily"),
    required_resource_keys={
        "file_registry",
        "postgres",
    },
)
def movie_list(context: dg.AssetExecutionContext):

    pr = context.resources.postgres

    # pre_load_message = ""
    # context.log.info(pre_load_message)

    movie_list_query = """
        SELECT
            WS.TCONST,
            WS.WATCHED,
            WS.PRIORITY,
            WS.NETFLIX,
            WS.PRIME,
            TR.AVERAGE_RATING,
            TR.NUM_VOTES,
            TB.PRIMARY_TITLE,
            TB.ORIGINAL_TITLE,
            TB.START_YEAR,
            TB.RUNTIME_MINUTES
        FROM
            IMDB.WATCH_STATUS AS WS
            LEFT JOIN IMDB.TITLE_RATINGS AS TR ON WS.TCONST = TR.TCONST
            LEFT JOIN IMDB.TITLE_BASICS AS TB ON WS.TCONST = TB.TCONST
        ORDER BY
            WATCHED,
            PRIORITY DESC,
            AVERAGE_RATING DESC;
        """

    genre_query = """
        SELECT
            WS.TCONST,
            GENRE
        FROM
            IMDB.WATCH_STATUS AS WS
            LEFT JOIN IMDB.TITLE_GENRES AS TG ON WS.TCONST = TG.TCONST;
    """

    watch_status_with_date_query = """
        SELECT
            WDS.TCONST,
            WDS.DATE,
            WDS.ENJOYMENT_SCORE,
            WDS.QUALITY_SCORE,
            TB.PRIMARY_TITLE,
            TB.ORIGINAL_TITLE,
            TB.START_YEAR
        FROM
            IMDB.WATCH_DATE_SCORES AS WDS
            LEFT JOIN IMDB.TITLE_BASICS AS TB ON WDS.TCONST = TB.TCONST
        WHERE
            DATE IS NOT NULL
        ORDER BY
            DATE DESC
        """

    watch_status_no_date_query = """
        SELECT
            WDS.TCONST,
            WDS.DATE,
            WDS.ENJOYMENT_SCORE,
            WDS.QUALITY_SCORE,
            TB.PRIMARY_TITLE,
            TB.ORIGINAL_TITLE,
            TB.START_YEAR
        FROM
            IMDB.WATCH_DATE_SCORES AS WDS
            LEFT JOIN IMDB.TITLE_BASICS AS TB ON WDS.TCONST = TB.TCONST
        WHERE
            DATE IS NULL
        ORDER BY
            TB.START_YEAR DESC;
    """

    directors_query = """
        SELECT
            WS.TCONST,
            ARRAY_AGG(NB.PRIMARY_NAME) AS DIRECTORS
        FROM
            IMDB.WATCH_STATUS AS WS
            LEFT JOIN IMDB.TITLE_DIRECTORS AS TD 
                ON WS.TCONST = TD.TCONST
            LEFT JOIN IMDB.NAME_BASICS AS NB
                ON TD.NCONST = NB.NCONST
        GROUP BY WS.TCONST;
    """

    movie_list = pr.get_query_results(
        context,
        movie_list_query,
    )

    genres = pr.get_query_results(
        context,
        genre_query,
    )

    watch_status_with_date = pr.get_query_results(
        context,
        watch_status_with_date_query,
    )

    watch_status_no_date = pr.get_query_results(
        context,
        watch_status_no_date_query,
    )

    directors = pr.get_query_results(context, directors_query)

    genres = genres.with_columns(pl.lit(True).alias("has_genre")).pivot(
        values="has_genre", index="tconst", columns="genre"
    )

    movie_list = (
        movie_list.join(directors, on="tconst", how="left")
        .join(genres, on="tconst", how="left")
        .sort(["watched", "priority", "average_rating"], descending=[False, True, True])
    )
    watch_status = pl.concat([watch_status_with_date, watch_status_no_date])

    output_dir = Path("data/imdb/outputs")
    output_dir.mkdir(parents=True, exist_ok=True)
    movie_list_path = output_dir / "movie_list.xlsx"

    context.log.info(f"writing to: {movie_list_path}")

    with Workbook(movie_list_path) as wb:
        ws_ml = wb.add_worksheet("Movie List")
        ws_wsd = wb.add_worksheet("Watch Status with Dates")

        movie_list.write_excel(
            workbook=wb,
            worksheet=ws_ml,
        )
        watch_status.write_excel(workbook=wb, worksheet=ws_wsd)

    # stats
    counts = movie_list.select(pl.col("watched").value_counts()).unnest("watched")
    watched = counts.filter(pl.col("watched") == "true")["count"].item()
    unwatched = counts.filter(pl.col("watched") == "false")["count"].item()
    priority = (
        movie_list["priority"]
        .value_counts()
        .filter(pl.col("priority") == "true")["count"]
        .item()
    )

    return dg.MaterializeResult(
        metadata={"watched": watched, "unwatched": unwatched, "priority": priority},
    )
