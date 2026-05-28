from datetime import datetime
import dagster as dg
from pathlib import Path
import polars as pl
from xlsxwriter import Workbook
from bokeh.plotting import figure
from bokeh.models import (
    ColumnDataSource,
    LinearColorMapper,
    ColorBar,
    TapTool,
    OpenURL,
    HoverTool,
    CategoricalColorMapper,
)
from bokeh.palettes import Turbo10, Turbo256
import numpy as np
from bokeh.io import output_file, save
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from bokeh.transform import linear_cmap


BASIC_DATA = """
    WITH 
        DIRECTORS AS (
            SELECT
                WS.TCONST,
                ARRAY_TO_STRING(ARRAY_AGG(NB.PRIMARY_NAME), ' - ') AS DIRECTORS
            FROM
                IMDB.WATCH_STATUS AS WS
                LEFT JOIN IMDB.TITLE_DIRECTORS AS TD ON WS.TCONST = TD.TCONST
                LEFT JOIN IMDB.NAME_BASICS AS NB ON TD.NCONST = NB.NCONST
            GROUP BY
                WS.TCONST
        ),
        WRITERS AS (
            SELECT
                WS.TCONST,
                ARRAY_TO_STRING(ARRAY_AGG(NB.PRIMARY_NAME), ' - ') AS WRITERS
            FROM
                IMDB.WATCH_STATUS AS WS
                LEFT JOIN IMDB.TITLE_WRITERS AS TD ON WS.TCONST = TD.TCONST
                LEFT JOIN IMDB.NAME_BASICS AS NB ON TD.NCONST = NB.NCONST
            GROUP BY
                WS.TCONST
        ),
        ACTORS AS (
            SELECT
                WS.TCONST,
                ARRAY_TO_STRING(ARRAY_AGG(NB.PRIMARY_NAME), ' - ') AS ACTORS
            FROM
                IMDB.WATCH_STATUS AS WS
                LEFT JOIN IMDB.TITLE_PRINCIPALS AS TD ON WS.TCONST = TD.TCONST
                LEFT JOIN IMDB.NAME_BASICS AS NB ON TD.NCONST = NB.NCONST
                WHERE CATEGORY='actor' OR CATEGORY='actress'
            GROUP BY WS.TCONST
        ),
        GENRES AS (
            SELECT
                WS.TCONST,
                ARRAY_TO_STRING(ARRAY_AGG(TG.GENRE), ' - ') AS GENRES
            FROM
                IMDB.WATCH_STATUS AS WS
                LEFT JOIN IMDB.TITLE_GENRES AS TG ON WS.TCONST = TG.TCONST
            GROUP BY
                WS.TCONST
        )
    SELECT
        WS.TCONST,
        TB.START_YEAR,
        TR.AVERAGE_RATING,
        TR.NUM_VOTES,
        TB.PRIMARY_TITLE,
        WS.PRIORITY,
        TB.TITLE_TYPE,
        GENRES.GENRES,
        DIRECTORS.DIRECTORS,
        WRITERS.WRITERS,
        ACTORS.ACTORS
    FROM
        IMDB.WATCH_STATUS AS WS
        LEFT JOIN IMDB.TITLE_BASICS AS TB ON WS.TCONST = TB.TCONST
        LEFT JOIN IMDB.TITLE_RATINGS AS TR ON TB.TCONST = TR.TCONST
        LEFT JOIN GENRES ON TB.TCONST = GENRES.TCONST
        LEFT JOIN DIRECTORS ON TB.TCONST = DIRECTORS.TCONST
        LEFT JOIN WRITERS ON TB.TCONST = WRITERS.TCONST
        LEFT JOIN ACTORS ON TB.TCONST = ACTORS.TCONST
    WHERE
        WS.WATCHED = FALSE
    ORDER BY
        START_YEAR;
    """


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
    WITH DIRECTORS AS (
            SELECT
                WS.TCONST,
                ARRAY_TO_STRING(ARRAY_AGG(NB.PRIMARY_NAME), ' - ') AS DIRECTORS
            FROM
                IMDB.WATCH_STATUS AS WS
                LEFT JOIN IMDB.TITLE_DIRECTORS AS TD ON WS.TCONST = TD.TCONST
                LEFT JOIN IMDB.NAME_BASICS AS NB ON TD.NCONST = NB.NCONST
            GROUP BY
                WS.TCONST
        ),
        WRITERS AS (
            SELECT
                WS.TCONST,
                ARRAY_TO_STRING(ARRAY_AGG(NB.PRIMARY_NAME), ' - ') AS WRITERS
            FROM
                IMDB.WATCH_STATUS AS WS
                LEFT JOIN IMDB.TITLE_WRITERS AS TD ON WS.TCONST = TD.TCONST
                LEFT JOIN IMDB.NAME_BASICS AS NB ON TD.NCONST = NB.NCONST
            GROUP BY
                WS.TCONST
        ),
        ACTORS AS (
            SELECT
                WS.TCONST,
                ARRAY_TO_STRING(ARRAY_AGG(NB.PRIMARY_NAME), ' - ') AS ACTORS
            FROM
                IMDB.WATCH_STATUS AS WS
                LEFT JOIN IMDB.TITLE_PRINCIPALS AS TD ON WS.TCONST = TD.TCONST
                LEFT JOIN IMDB.NAME_BASICS AS NB ON TD.NCONST = NB.NCONST
                WHERE CATEGORY='actor' OR CATEGORY='actress'
            GROUP BY WS.TCONST
        ),
        GENRES AS (
            SELECT
                WS.TCONST,
                ARRAY_TO_STRING(ARRAY_AGG(TG.GENRE), ' - ') AS GENRES
            FROM
                IMDB.WATCH_STATUS AS WS
                LEFT JOIN IMDB.TITLE_GENRES AS TG ON WS.TCONST = TG.TCONST
            GROUP BY
                WS.TCONST
        )
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
		TB.RUNTIME_MINUTES,
		DIRECTORS.DIRECTORS,
        WRITERS.WRITERS,
        ACTORS.ACTORS,
		TB.TITLE_TYPE
    FROM
        IMDB.WATCH_STATUS AS WS
        LEFT JOIN IMDB.TITLE_BASICS AS TB ON WS.TCONST = TB.TCONST
        LEFT JOIN IMDB.TITLE_RATINGS AS TR ON TB.TCONST = TR.TCONST
        LEFT JOIN GENRES ON TB.TCONST = GENRES.TCONST
        LEFT JOIN DIRECTORS ON TB.TCONST = DIRECTORS.TCONST
        LEFT JOIN WRITERS ON TB.TCONST = WRITERS.TCONST
        LEFT JOIN ACTORS ON TB.TCONST = ACTORS.TCONST
    ORDER BY
        WS.WATCHED, WS.PRIORITY DESC, TR.AVERAGE_RATING DESC;
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

    unwatched_last_5_years_query = """
        WITH
            AGGREGATED_GENRES AS (
                SELECT
                    WS.TCONST,
                    ARRAY_AGG(GENRE) AS GENRES
                FROM
                    IMDB.WATCH_STATUS AS WS
                    JOIN IMDB.TITLE_GENRES AS G ON WS.TCONST = G.TCONST
                GROUP BY
                    WS.TCONST
            )
        SELECT
            WS.TCONST,
            PRIORITY,
            AVERAGE_RATING,
            NUM_VOTES,
            PRIMARY_TITLE,
            START_YEAR,
            TITLE_TYPE,
            GENRES
        FROM
            IMDB.WATCH_STATUS AS WS
            JOIN IMDB.TITLE_BASICS AS TB ON WS.TCONST = TB.TCONST
            JOIN IMDB.TITLE_RATINGS AS TR ON TB.TCONST = TR.TCONST
            JOIN AGGREGATED_GENRES AS AG ON TB.TCONST = AG.TCONST
        WHERE
            WS.WATCHED = FALSE
            AND (
                EXTRACT(
                    YEAR
                    FROM
                        CURRENT_DATE
                ) - TB.START_YEAR
            ) < 5
        ORDER BY
            PRIORITY DESC,
            AVERAGE_RATING DESC;
    """

    top_10_per_genre_unwatched_query = """
                WITH
            RANKED AS (
                SELECT
                    WS.TCONST,
                    TR.AVERAGE_RATING,
                    TR.NUM_VOTES,
                    TB.PRIMARY_TITLE,
                    TB.START_YEAR,
                    TB.TITLE_TYPE,
                    TG.GENRE,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            TG.GENRE
                        ORDER BY
                            TR.AVERAGE_RATING DESC,
                            TR.NUM_VOTES DESC
                    ) AS RN
                FROM
                    IMDB.WATCH_STATUS AS WS
                    JOIN IMDB.TITLE_BASICS AS TB ON WS.TCONST = TB.TCONST
                    JOIN IMDB.TITLE_RATINGS AS TR ON TB.TCONST = TR.TCONST
                    JOIN IMDB.TITLE_GENRES AS TG ON TB.TCONST = TG.TCONST
                WHERE
                    WS.WATCHED = FALSE
            )
        SELECT
            GENRE,
            AVERAGE_RATING,
            NUM_VOTES,
            PRIMARY_TITLE,
            START_YEAR,
            TCONST
        FROM
            RANKED
        WHERE
            RN <= 10
        ORDER BY
            GENRE,
            RN;
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

    # unwatched_last_5_years = pr.get_query_results(
    #     context,
    #     unwatched_last_5_years_query,
    # )

    top_10_per_genre_unwatched = pr.get_query_results(
        context,
        top_10_per_genre_unwatched_query,
    )

    # directors = pr.get_query_results(context, directors_query)

    genres = genres.with_columns(pl.lit(True).alias("has_genre")).pivot(
        values="has_genre", index="tconst", columns="genre"
    )

    movie_list = (
        movie_list.join(genres, on="tconst", how="left")
        # .sort(["watched", "priority", "average_rating"], descending=[False, True, True])
    )
    current_year = datetime.today().year
    unwatched_last_5_years = movie_list.filter(
        (pl.col("start_year") > current_year - 5) & (~pl.col("watched"))
    )
    watch_status = pl.concat([watch_status_with_date, watch_status_no_date])

    output_dir = Path("data/imdb/outputs")
    output_dir.mkdir(parents=True, exist_ok=True)
    movie_list_path = output_dir / "movie_list.xlsx"

    context.log.info(f"writing to: {movie_list_path}")

    with Workbook(movie_list_path) as wb:
        ws_ml = wb.add_worksheet("Movie List")
        ws_wsd = wb.add_worksheet("Watch Status with Dates")
        ws_p5y = wb.add_worksheet("Uwatched Movies of Past 5 Years")
        ws_ut10 = wb.add_worksheet("Unwatched Top 10 per Genre")

        movie_list.write_excel(workbook=wb, worksheet=ws_ml)
        watch_status.write_excel(workbook=wb, worksheet=ws_wsd)
        unwatched_last_5_years.write_excel(workbook=wb, worksheet=ws_p5y)
        top_10_per_genre_unwatched.write_excel(workbook=wb, worksheet=ws_ut10)

    # stats
    # counts = movie_list.select(pl.col("watched").value_counts()).unnest("watched")
    
    
    priority = movie_list["priority"].value_counts().filter(pl.col("priority") == "true")["count"].item()
    # watched = counts.filter(pl.col("watched") == "true")["count"].item()
    # unwatched = counts.filter(pl.col("watched") == "false")["count"].item()
    # priority = (
    #     movie_list["priority"]
    #     .value_counts()
    #     .filter(pl.col("priority") == "true")["count"]
    #     .item()
    # )

    return dg.MaterializeResult(
        metadata={"priority": priority},
        # metadata={"watched": watched, "unwatched": unwatched, "priority": priority},
        # metadata={"watched": watched, "unwatched": unwatched},
    )


@dg.asset(
    deps=[
        "title_basics_loaded",
        "title_ratings_loaded",
        "title_genres_loaded",
        "title_directors_loaded",
        "name_basics_loaded",
    ],
    name="movie_graph",
    description="Interactive bokeh plot of unwatched movies",
    group_name="output",
    # automation_condition=dg.AutomationCondition.on_cron("@daily"),
    required_resource_keys={
        "file_registry",
        "postgres",
    },
)
def movie_graph(context: dg.AssetExecutionContext):
    query = """
        WITH  DIRECTORS AS (
            SELECT
                WS.TCONST,
                ARRAY_TO_STRING(ARRAY_AGG(NB.PRIMARY_NAME), ' - ') AS DIRECTORS
            FROM
                IMDB.WATCH_STATUS AS WS
                LEFT JOIN IMDB.TITLE_DIRECTORS AS TD ON WS.TCONST = TD.TCONST
                LEFT JOIN IMDB.NAME_BASICS AS NB ON TD.NCONST = NB.NCONST
            GROUP BY
                WS.TCONST
        ),
        WRITERS AS (
            SELECT
                WS.TCONST,
                ARRAY_TO_STRING(ARRAY_AGG(NB.PRIMARY_NAME), ' - ') AS WRITERS
            FROM
                IMDB.WATCH_STATUS AS WS
                LEFT JOIN IMDB.TITLE_WRITERS AS TD ON WS.TCONST = TD.TCONST
                LEFT JOIN IMDB.NAME_BASICS AS NB ON TD.NCONST = NB.NCONST
            GROUP BY
                WS.TCONST
        ),
        ACTORS AS (
            SELECT
                WS.TCONST,
                ARRAY_TO_STRING(ARRAY_AGG(NB.PRIMARY_NAME), ' - ') AS ACTORS
            FROM
                IMDB.WATCH_STATUS AS WS
                LEFT JOIN IMDB.TITLE_PRINCIPALS AS TD ON WS.TCONST = TD.TCONST
                LEFT JOIN IMDB.NAME_BASICS AS NB ON TD.NCONST = NB.NCONST
                WHERE CATEGORY='actor' OR CATEGORY='actress'
            GROUP BY WS.TCONST
        ),
        GENRES AS (
            SELECT
                WS.TCONST,
                ARRAY_TO_STRING(ARRAY_AGG(TG.GENRE), ' - ') AS GENRES
            FROM
                IMDB.WATCH_STATUS AS WS
                LEFT JOIN IMDB.TITLE_GENRES AS TG ON WS.TCONST = TG.TCONST
            GROUP BY
                WS.TCONST
        )
        SELECT
            TG.GENRE,
            TB.START_YEAR,
            TR.AVERAGE_RATING,
            TR.NUM_VOTES,
            TB.PRIMARY_TITLE,
            WS.TCONST,
            WS.PRIORITY,
            TB.TITLE_TYPE,
            GENRES.GENRES,
            DIRECTORS.DIRECTORS,
            WRITERS.WRITERS,
            ACTORS.ACTORS
        FROM
            IMDB.WATCH_STATUS AS WS
            LEFT JOIN IMDB.TITLE_BASICS AS TB ON WS.TCONST = TB.TCONST
            LEFT JOIN IMDB.TITLE_RATINGS AS TR ON TB.TCONST = TR.TCONST
            LEFT JOIN IMDB.TITLE_GENRES AS TG ON TB.TCONST = TG.TCONST
            LEFT JOIN GENRES ON TB.TCONST = GENRES.TCONST
            LEFT JOIN DIRECTORS ON TB.TCONST = DIRECTORS.TCONST
            LEFT JOIN WRITERS ON TB.TCONST = WRITERS.TCONST
            LEFT JOIN ACTORS ON TB.TCONST = ACTORS.TCONST
        WHERE
            WS.WATCHED = FALSE
        ORDER BY
            START_YEAR;
    """

    pr = context.resources.postgres

    data = pr.get_query_results(
        context,
        query,
    )

    movies = data.filter(
        pl.col("start_year").is_not_null(), pl.col("genre").is_not_null()
    )
    movies = movies.with_columns(
        pl.col("average_rating").cast(pl.Float64),
        ("https://www.imdb.com/title/" + pl.col("tconst") + "/").alias("url"),
    )

    # Scale num_votes to 0–10
    movies = movies.with_columns(
        (
            (pl.col("num_votes") - pl.col("num_votes").min())
            / (pl.col("num_votes").max() - pl.col("num_votes").min())
            * 1.5
            + 1.2
        ).alias("votes_scaled"),
    )

    source = ColumnDataSource(movies)

    color_mapper = LinearColorMapper(
        palette=Turbo10,
        low=movies["average_rating"].min(),
        high=movies["average_rating"].max(),
    )

    genres = movies["genre"].unique().sort(descending=True).to_list()
    tooltips = [
        ("Title", "@primary_title"),
        ("Year", "@start_year"),
        ("Selected genre", "@genre"),
        ("Genres", "@genres"),
        ("Average Rating", "@average_rating"),
        ("# votes", "@num_votes"),
        ("Directors", "@directors"),
        ("Writers", "@writers"),
        ("Actors", "@actors"),
    ]

    p = figure(
        height=400,
        width=1000,
        y_range=genres,
        tooltips=tooltips,
        tools="tap, box_zoom, wheel_zoom, reset, pan, hover, save",
        x_axis_type="log",
    )
    p.circle(
        x="start_year",
        y="genre",
        radius="votes_scaled",
        alpha=0.5,
        color={"field": "average_rating", "transform": color_mapper},
        source=source,
    )
    p.select_one(TapTool).callback = OpenURL(url="@url")
    color_bar = ColorBar(color_mapper=color_mapper, label_standoff=12)

    p.add_layout(color_bar, "right")

    output_dir = Path("data/imdb/outputs")
    output_dir.mkdir(parents=True, exist_ok=True)
    movie_graph_path = output_dir / "movie_graph.html"

    output_file(movie_graph_path)
    save(p)


@dg.asset(
    deps=[
        "title_principals_loaded",
        "title_basics_loaded",
        "title_ratings_loaded",
        "title_genres_loaded",
        "title_directors_loaded",
        "title_writers_loaded",
        "name_basics_loaded",
    ],
    name="movie_cluster_graph",
    description="Interactive bokeh plot of unwatched movies",
    group_name="output",
    # automation_condition=dg.AutomationCondition.on_cron("@daily"),
    required_resource_keys={
        "file_registry",
        "postgres",
    },
)
def movie_cluster_graph(context: dg.AssetExecutionContext):
    ml_query = """
        WITH unwatched AS (
            SELECT tconst
            FROM imdb.watch_status
            WHERE watched = FALSE
        ),

        -- Pull all principals in one go
        principals AS (
            SELECT
                uw.tconst,
                tp.nconst,
                tp.category
            FROM unwatched uw
            LEFT JOIN imdb.title_principals tp
                ON uw.tconst = tp.tconst
        ),

        -- Extract role‑specific subsets
        actors AS (
            SELECT tconst, nconst AS actor
            FROM principals
            WHERE category IN ('actor', 'actress')
        ),

        producers AS (
            SELECT tconst, nconst AS producer
            FROM principals
            WHERE category = 'producer'
        ),

        composers AS (
            SELECT tconst, nconst AS composer
            FROM principals
            WHERE category = 'composer'
        ),

        cinematographers AS (
            SELECT tconst, nconst AS cinematographer
            FROM principals
            WHERE category = 'cinematographer'
        ),

        -- Directors and writers come from their own tables
        directors AS (
            SELECT uw.tconst, td.nconst AS director
            FROM unwatched uw
            LEFT JOIN imdb.title_directors td
                ON uw.tconst = td.tconst
        ),

        writers AS (
            SELECT uw.tconst, tw.nconst AS writer
            FROM unwatched uw
            LEFT JOIN imdb.title_writers tw
                ON uw.tconst = tw.tconst
        ),

        genres AS (
            SELECT uw.tconst, tg.genre
            FROM unwatched uw
            LEFT JOIN imdb.title_genres tg
                ON uw.tconst = tg.tconst
        ),

        basics AS (
            SELECT uw.tconst, tb.start_year
            FROM unwatched uw
            LEFT JOIN imdb.title_basics tb
                ON uw.tconst = tb.tconst
        ),

        ratings AS (
            SELECT uw.tconst, tr.average_rating, tr.num_votes
            FROM unwatched uw
            LEFT JOIN imdb.title_ratings tr
                ON uw.tconst = tr.tconst
        )

        SELECT
            uw.tconst,
            actor,
            producer,
            composer,
            cinematographer,
            director,
            writer,
            genre,
            start_year,
            average_rating,
            num_votes
        FROM unwatched uw
        LEFT JOIN actors           ON uw.tconst = actors.tconst
        LEFT JOIN producers        ON uw.tconst = producers.tconst
        LEFT JOIN composers        ON uw.tconst = composers.tconst
        LEFT JOIN cinematographers ON uw.tconst = cinematographers.tconst
        LEFT JOIN directors        ON uw.tconst = directors.tconst
        LEFT JOIN writers          ON uw.tconst = writers.tconst
        LEFT JOIN genres           ON uw.tconst = genres.tconst
        LEFT JOIN basics           ON uw.tconst = basics.tconst
        LEFT JOIN ratings          ON uw.tconst = ratings.tconst;
    """

    pr = context.resources.postgres

    graph_data = pr.get_query_results(
        context,
        BASIC_DATA,
    )

    ml_data = pr.get_query_results(
        context,
        ml_query,
    )

    # ---------------------------------------------------------
    # 1. Load & clean data
    # ---------------------------------------------------------
    data = ml_data.drop_nulls().drop_nans()

    # ---------------------------------------------------------
    # 2. Helper: multi-hot encode any categorical column
    # ---------------------------------------------------------
    def multi_hot(df: pl.DataFrame, col: str) -> pl.DataFrame:
        return (
            df.select("tconst", pl.col(col).cast(pl.Categorical))
            .unique()
            .to_dummies(columns=[col])
            .group_by("tconst")
            .sum()
        )

    # ---------------------------------------------------------
    # 3. Multi-hot encode all categorical features
    # ---------------------------------------------------------
    categorical_cols = [
        "genre",
        "actor",
        "producer",
        "composer",
        "cinematographer",
        "director",
        "writer",
    ]

    encoded = [multi_hot(data, col) for col in categorical_cols]

    # Base ML table
    ml_data = data.select("tconst").unique()

    # Join all encoded tables
    for enc in encoded:
        ml_data = ml_data.join(enc, on="tconst", how="left")

    labels = ml_data["tconst"]
    X = ml_data.drop("tconst")

    # ---------------------------------------------------------
    # 4. PCA
    # ---------------------------------------------------------
    pca = PCA(n_components=2)
    pcs = pca.fit_transform(X)

    pcs_df = pl.DataFrame(pcs, schema=["pc1", "pc2"])
    pca_result = pl.concat([labels.to_frame(), pcs_df], how="horizontal")

    # ---------------------------------------------------------
    # 5. Merge PCA back into movie info
    # ---------------------------------------------------------
    data = graph_data.join(pca_result, on="tconst", how="left")

    data = data.drop_nulls().drop_nans()

    # ---------------------------------------------------------
    # 6. KMeans clustering
    # ---------------------------------------------------------
    kmeans = KMeans(n_clusters=30, random_state=42)
    clusters = kmeans.fit_predict(data[["pc1", "pc2"]])

    data = data.with_columns(
        [
            pl.Series("group", clusters).cast(pl.Utf8),
            ("https://www.imdb.com/title/" + pl.col("tconst") + "/").alias("url"),
            pl.col("average_rating").cast(pl.Float64),
        ]
    )

    # ---------------------------------------------------------
    # 7. Bokeh visualization
    # ---------------------------------------------------------
    source = ColumnDataSource(data.to_dict(as_series=False))

    groups = sorted(data["group"].unique())
    groups = [str(g) for g in groups]
    palette = [Turbo256[int(i)] for i in np.linspace(0, 255, len(groups))]
    color_mapper = CategoricalColorMapper(factors=groups, palette=palette)

    p = figure(
        width=900,
        height=600,
        title="Movie PCA Explorer",
        tools="tap,box_zoom,wheel_zoom,reset,pan,hover,save",
    )

    p.circle(
        "pc1",
        "pc2",
        size=20,
        alpha=0.6,
        color={"field": "group", "transform": color_mapper},
        source=source,
    )

    hover = HoverTool(
        tooltips=[
            ("Title", "@primary_title"),
            ("Year", "@start_year"),
            ("Selected genre", "@genre"),
            ("Genres", "@genres"),
            ("Average Rating", "@average_rating"),
            ("# votes", "@num_votes"),
            ("Directors", "@directors"),
            ("Writers", "@writers"),
            ("Actors", "@actors"),
        ]
    )

    p.add_tools(hover)
    p.select_one(TapTool).callback = OpenURL(url="@url")

    output_dir = Path("data/imdb/outputs")
    output_dir.mkdir(parents=True, exist_ok=True)
    movie_graph_path = output_dir / "movie_cluster_graph.html"

    output_file(movie_graph_path)
    save(p)


@dg.asset(
    deps=[
        "title_principals_loaded",
        "title_basics_loaded",
        "title_ratings_loaded",
        "title_genres_loaded",
        "title_directors_loaded",
        "title_writers_loaded",
        "name_basics_loaded",
    ],
    name="movie_rating_votes_graph",
    description="Interactive bokeh plot of unwatched movies",
    group_name="output",
    # automation_condition=dg.AutomationCondition.on_cron("@daily"),
    required_resource_keys={
        "file_registry",
        "postgres",
    },
)
def movie_rating_votes_graph(context: dg.AssetExecutionContext):
    pr = context.resources.postgres

    graph_data = pr.get_query_results(
        context,
        BASIC_DATA,
    )

    # ---------------------------------------------------------
    # 1. Load & clean data
    # ---------------------------------------------------------
    data = (
        graph_data.drop_nulls()
        .drop_nans()
        .with_columns(
            pl.col("average_rating").cast(pl.Float64),
            ("https://www.imdb.com/title/" + pl.col("tconst") + "/").alias("url"),
        )
    )

    source = ColumnDataSource(data.to_dict(as_series=False))

    cmap = linear_cmap(
        "start_year",
        palette="Viridis256",
        low=min(data["start_year"]),
        high=max(data["start_year"]),
    )

    p = figure(
        width=900,
        height=600,
        title="Unwatched movies with dimensions rating, votes and release year",
        tools="tap,box_zoom,wheel_zoom,reset,pan,hover,save",
        x_axis_type="log",
    )

    p.scatter(
        x="num_votes", y="average_rating", alpha=0.5, source=source, color=cmap, size=20
    )

    hover = HoverTool(
        tooltips=[
            ("Title", "@primary_title"),
            ("Year", "@start_year"),
            ("Selected genre", "@genre"),
            ("Genres", "@genres"),
            ("Average Rating", "@average_rating"),
            ("# votes", "@num_votes"),
            ("Directors", "@directors"),
            ("Writers", "@writers"),
            ("Actors", "@actors"),
        ]
    )

    p.add_tools(hover)
    p.select_one(TapTool).callback = OpenURL(url="@url")
    p.xaxis.axis_label = "number of votes"
    p.yaxis.axis_label = "average rating"

    output_dir = Path("data/imdb/outputs")
    output_dir.mkdir(parents=True, exist_ok=True)
    movie_graph_path = output_dir / "movie_rating_votes_graph.html"

    output_file(movie_graph_path)
    save(p)
