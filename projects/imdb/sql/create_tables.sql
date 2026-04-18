-- types
CREATE TYPE imdb.enum_titletype AS enum (
    'movie',
    'short',
    'tvEpisode',
    'tvMiniSeries',
    'tvMovie',
    'tvPilot',
    'tvSeries',
    'tvShort',
    'tvSpecial',
    'video',
    'videoGame'
);

CREATE TYPE imdb.enum_genre AS enum (
    'Action',
    'Adult',
    'Adventure',
    'Animation',
    'Biography',
    'Comedy',
    'Crime',
    'Documentary',
    'Drama',
    'Family',
    'Fantasy',
    'Film-Noir',
    'Game-Show',
    'History',
    'Horror',
    'Music',
    'Musical',
    'Mystery',
    'News',
    'Reality-TV',
    'Romance',
    'Sci-Fi',
    'Short',
    'Sport',
    'Talk-Show',
    'Thriller',
    'War',
    'Western'
);

CREATE TYPE imdb.enum_job_category AS enum (
    'actor',
    'actress',
    'archive_footage',
    'archive_sound',
    'casting_director',
    'category',
    'cinematographer',
    'composer',
    'director',
    'editor',
    'producer',
    'production_designer',
    'self',
    'writer'
);

-- name.basics.tsv
CREATE TABLE
    IF NOT EXISTS imdb.name_basics (
        nconst varchar(11) NOT NULL,
        primary_name varchar(120),
        birth_year smallint,
        death_year smallint,
        CONSTRAINT name_basics_pkey PRIMARY KEY (nconst)
    );

CREATE TABLE
    IF NOT EXISTS imdb.name_primary_profession (
        nconst varchar(11),
        primary_profession varchar(26)
    );

CREATE TABLE
    IF NOT EXISTS imdb.name_known_for_titles (
        nconst varchar(11) NOT NULL,
        tconst varchar(11) NOT NULL
    );

-- title.akas.tsv
CREATE TABLE
    IF NOT EXISTS imdb.title_akas (
        id serial,
        tconst varchar(11) NOT NULL,
        ordering smallint,
        title text,
        region varchar(5),
        language varchar(5),
        types text,
        attributes text,
        is_original_title boolean,
        CONSTRAINT akas_tconst_ordering_key UNIQUE (tconst, ordering),
        CONSTRAINT akas_pkey PRIMARY KEY (id)
    );

-- title.basics.tsv
CREATE TABLE
    IF NOT EXISTS imdb.title_basics (
        tconst varchar(11) NOT NULL,
        title_type imdb.enum_titletype,
        primary_title text,
        original_title text,
        is_adult boolean,
        start_year smallint,
        end_year smallint,
        runtime_minutes integer,
        CONSTRAINT title_basics_pkey PRIMARY KEY (tconst)
    );


CREATE TABLE
    IF NOT EXISTS imdb.title_genres (
        id serial,
        tconst varchar(11) NOT NULL,
        genre imdb.enum_genre NOT NULL,
        CONSTRAINT genres_tconst_genre_key UNIQUE (tconst, genre),
        CONSTRAINT genres_pkey PRIMARY KEY (id)
    );

-- title.crew.tsv
CREATE TABLE
    IF NOT EXISTS imdb.title_directors (
        id serial,
        tconst varchar(11),
        nconst varchar(11),
        CONSTRAINT title_directors_tconst_nconst_key UNIQUE (tconst, nconst),
        CONSTRAINT title_directors_pkey PRIMARY KEY (id)
    );

CREATE TABLE
    IF NOT EXISTS imdb.title_writers (
        id serial,
        tconst varchar(11),
        nconst varchar(11),
        CONSTRAINT title_writers_tconst_nconst_key UNIQUE (tconst, nconst),
        CONSTRAINT title_writers_pkey PRIMARY KEY (id)
    );

-- title.episode.tsv
CREATE TABLE
    IF NOT EXISTS imdb.title_episode (
        tconst varchar(11) NOT NULL,
        parent_tconst varchar(11) NOT NULL,
        season_number integer,
        episode_number integer,
        CONSTRAINT title_episode_pk PRIMARY KEY (tconst)
    );

-- title.principals.tsv
CREATE TABLE
    IF NOT EXISTS imdb.title_principals (
        tconst varchar(11) NOT NULL,
        ordering smallint NOT NULL,
        nconst varchar(11),
        category imdb.enum_job_category,
        job text,
        characters text,
        CONSTRAINT title_principals_pkey PRIMARY KEY (tconst, ordering)
    );

-- title.ratings.tsv
CREATE TABLE
    IF NOT EXISTS imdb.title_ratings (
        tconst varchar(11) NOT NULL,
        average_rating numeric(3, 1),
        num_votes integer,
        CONSTRAINT title_ratings_pk PRIMARY KEY (tconst)
    );

-- -- partitions
-- CREATE TABLE
--     imdb.title_basics_movie PARTITION OF imdb.title_basics FOR
-- VALUES
--     IN ('movie');

-- CREATE TABLE
--     imdb.title_basics_short PARTITION OF imdb.title_basics FOR
-- VALUES
--     IN ('short');

-- CREATE TABLE
--     imdb.title_basics_tvepisode PARTITION OF imdb.title_basics FOR
-- VALUES
--     IN ('tvEpisode');

-- CREATE TABLE
--     imdb.title_basics_tvminiSeries PARTITION OF imdb.title_basics FOR
-- VALUES
--     IN ('tvMiniSeries');

-- CREATE TABLE
--     imdb.title_basics_tvmovie PARTITION OF imdb.title_basics FOR
-- VALUES
--     IN ('tvMovie');

-- CREATE TABLE
--     imdb.title_basics_tvpilot PARTITION OF imdb.title_basics FOR
-- VALUES
--     IN ('tvPilot');

-- CREATE TABLE
--     imdb.title_basics_tvseries PARTITION OF imdb.title_basics FOR
-- VALUES
--     IN ('tvSeries');

-- CREATE TABLE
--     imdb.title_basics_tvshort PARTITION OF imdb.title_basics FOR
-- VALUES
--     IN ('tvShort');

-- CREATE TABLE
--     imdb.title_basics_tvspecial PARTITION OF imdb.title_basics FOR
-- VALUES
--     IN ('tvSpecial');

-- CREATE TABLE
--     imdb.title_basics_video PARTITION OF imdb.title_basics FOR
-- VALUES
--     IN ('video');

-- CREATE TABLE
--     imdb.title_basics_videoGame PARTITION OF imdb.title_basics FOR
-- VALUES
--     IN ('videoGame');

-- CREATE TABLE
--     imdb.title_principals_actor PARTITION OF imdb.title_principals FOR
-- VALUES
--     IN ('actor');

-- CREATE TABLE
--     imdb.title_principals_actress PARTITION OF imdb.title_principals FOR
-- VALUES
--     IN ('actress');

-- CREATE TABLE
--     imdb.title_principals_archive_footage PARTITION OF imdb.title_principals FOR
-- VALUES
--     IN ('archive_footage');

-- CREATE TABLE
--     imdb.title_principals_archive_sound PARTITION OF imdb.title_principals FOR
-- VALUES
--     IN ('archive_sound');

-- CREATE TABLE
--     imdb.title_principals_casting_director PARTITION OF imdb.title_principals FOR
-- VALUES
--     IN ('casting_director');

-- CREATE TABLE
--     imdb.title_principals_category PARTITION OF imdb.title_principals FOR
-- VALUES
--     IN ('category');

-- CREATE TABLE
--     imdb.title_principals_cinematographer PARTITION OF imdb.title_principals FOR
-- VALUES
--     IN ('cinematographer');

-- CREATE TABLE
--     imdb.title_principals_composer PARTITION OF imdb.title_principals FOR
-- VALUES
--     IN ('composer');

-- CREATE TABLE
--     imdb.title_principals_director PARTITION OF imdb.title_principals FOR
-- VALUES
--     IN ('director');

-- CREATE TABLE
--     imdb.title_principals_editor PARTITION OF imdb.title_principals FOR
-- VALUES
--     IN ('editor');

-- CREATE TABLE
--     imdb.title_principals_producer PARTITION OF imdb.title_principals FOR
-- VALUES
--     IN ('producer');

-- CREATE TABLE
--     imdb.title_principals_production_designer PARTITION OF imdb.title_principals FOR
-- VALUES
--     IN ('production_designer');

-- CREATE TABLE
--     imdb.title_principals_production_self PARTITION OF imdb.title_principals FOR
-- VALUES
--     IN ('self');

-- CREATE TABLE
--     imdb.title_principals_writer PARTITION OF imdb.title_principals FOR
-- VALUES
--     IN ('writer');

-- CREATE TABLE
--     imdb.name_primary_profession_null PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN (NULL);

-- CREATE TABLE
--     imdb.name_primary_profession_accountant PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('accountant');

-- CREATE TABLE
--     imdb.name_primary_profession_actor PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('actor');

-- CREATE TABLE
--     imdb.name_primary_profession_actress PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('actress');

-- CREATE TABLE
--     imdb.name_primary_profession_animation_department PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('animation_department');

-- CREATE TABLE
--     imdb.name_primary_profession_archive_footage PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('archive_footage');

-- CREATE TABLE
--     imdb.name_primary_profession_archive_sound PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('archive_sound');

-- CREATE TABLE
--     imdb.name_primary_profession_art_department PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('art_department');

-- CREATE TABLE
--     imdb.name_primary_profession_art_director PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('art_director');

-- CREATE TABLE
--     imdb.name_primary_profession_assistant PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('assistant');

-- CREATE TABLE
--     imdb.name_primary_profession_assistant_director PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('assistant_director');

-- CREATE TABLE
--     imdb.name_primary_profession_camera_department PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('camera_department');

-- CREATE TABLE
--     imdb.name_primary_profession_casting_department PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('casting_department');

-- CREATE TABLE
--     imdb.name_primary_profession_casting_director PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('casting_director');

-- CREATE TABLE
--     imdb.name_primary_profession_choreographer PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('choreographer');

-- CREATE TABLE
--     imdb.name_primary_profession_cinematographer PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('cinematographer');

-- CREATE TABLE
--     imdb.name_primary_profession_composer PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('composer');

-- CREATE TABLE
--     imdb.name_primary_profession_costume_department PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('costume_department');

-- CREATE TABLE
--     imdb.name_primary_profession_costume_designer PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('costume_designer');

-- CREATE TABLE
--     imdb.name_primary_profession_director PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('director');

-- CREATE TABLE
--     imdb.name_primary_profession_editor PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('editor');

-- CREATE TABLE
--     imdb.name_primary_profession_editorial_department PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('editorial_department');

-- CREATE TABLE
--     imdb.name_primary_profession_electrical_department PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('electrical_department');

-- CREATE TABLE
--     imdb.name_primary_profession_executive PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('executive');

-- CREATE TABLE
--     imdb.name_primary_profession_legal PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('legal');

-- CREATE TABLE
--     imdb.name_primary_profession_location_management PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('location_management');

-- CREATE TABLE
--     imdb.name_primary_profession_make_up_department PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('make_up_department');

-- CREATE TABLE
--     imdb.name_primary_profession_manager PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('manager');

-- CREATE TABLE
--     imdb.name_primary_profession_miscellaneous PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('miscellaneous');

-- CREATE TABLE
--     imdb.name_primary_profession_music_artist PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('music_artist');

-- CREATE TABLE
--     imdb.name_primary_profession_music_department PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('music_department');

-- CREATE TABLE
--     imdb.name_primary_profession_podcaster PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('podcaster');

-- CREATE TABLE
--     imdb.name_primary_profession_producer PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('producer');

-- CREATE TABLE
--     imdb.name_primary_profession_production_department PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('production_department');

-- CREATE TABLE
--     imdb.name_primary_profession_production_designer PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('production_designer');

-- CREATE TABLE
--     imdb.name_primary_profession_production_manager PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('production_manager');

-- CREATE TABLE
--     imdb.name_primary_profession_publicist PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('publicist');

-- CREATE TABLE
--     imdb.name_primary_profession_script_department PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('script_department');

-- CREATE TABLE
--     imdb.name_primary_profession_set_decorator PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('set_decorator');

-- CREATE TABLE
--     imdb.name_primary_profession_sound_department PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('sound_department');

-- CREATE TABLE
--     imdb.name_primary_profession_soundtrack PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('soundtrack');

-- CREATE TABLE
--     imdb.name_primary_profession_special_effects PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('special_effects');

-- CREATE TABLE
--     imdb.name_primary_profession_stunts PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('stunts');

-- CREATE TABLE
--     imdb.name_primary_profession_talent_agent PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('talent_agent');

-- CREATE TABLE
--     imdb.name_primary_profession_transportation_department PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('transportation_department');

-- CREATE TABLE
--     imdb.name_primary_profession_visual_effects PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('visual_effects');

-- CREATE TABLE
--     imdb.name_primary_profession_writer PARTITION OF imdb.name_primary_profession FOR
-- VALUES
--     IN ('writer');