-- types

CREATE TYPE imdb.enum_titletype AS enum ( 'movie', 'short', 'titleType', 'tvEpisode',
'tvMiniSeries', 'tvMovie', 'tvPilot', 'tvSeries', 'tvShort', 'tvSpecial',
'video', 'videoGame' );

CREATE TYPE imdb.enum_genre AS enum ( 'Action', 'Adult', 'Adventure', 'Animation',
'Biography', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Family', 'Fantasy',
'Film-Noir', 'Game-Show', 'History', 'Horror', 'Music', 'Musical', 'Mystery',
'News', 'Reality-TV', 'Romance', 'Sci-Fi', 'Short', 'Sport', 'Talk-Show',
'Thriller', 'War', 'Western' );

CREATE TYPE imdb.enum_job_category AS enum ( 'actor', 'actress', 'archive_footage',
'archive_sound', 'casting_director', 'category', 'cinematographer', 'composer',
'director', 'editor', 'producer', 'production_designer', 'self', 'writer' );


-- name.basics.tsv

CREATE TABLE IF NOT EXISTS imdb.name_basics
(
    nconst varchar(11) NOT NULL,
    primary_name varchar(120),
    birth_year smallint,
    death_year smallint,
    CONSTRAINT name_basics_pkey PRIMARY KEY (nconst)
);

CREATE TABLE IF NOT EXISTS imdb.name_primary_profession
(
    nconst varchar(11),
    primary_profession varchar(26)
);

CREATE TABLE IF NOT EXISTS imdb.name_known_for_titles
(
    nconst varchar(11) NOT NULL,
    tconst varchar(11) NOT NULL
);

-- title.akas.tsv

CREATE TABLE IF NOT EXISTS imdb.title_akas
(
    id serial,
    title_id varchar(11) NOT NULL,
    ordering smallint,
    title text,
    region varchar(5),
    language varchar(5),
    types text,
    attributes text,
    is_original_title boolean,
    CONSTRAINT akas_title_id_ordering_key UNIQUE (title_id, ordering),
    CONSTRAINT akas_pkey PRIMARY KEY (id)
);

-- title.basics.tsv

CREATE TABLE IF NOT EXISTS imdb.title_basics
(
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

CREATE TABLE IF NOT EXISTS imdb.title_genres
(
    id serial,
    tconst varchar(11) NOT NULL,
    genre imdb.enum_genre NOT NULL,
    CONSTRAINT genres_tconst_genre_key UNIQUE (tconst, genre),
    CONSTRAINT genres_pkey PRIMARY KEY (id)
);

-- title.crew.tsv

CREATE TABLE IF NOT EXISTS imdb.title_directors
(
    id serial,
    tconst varchar(11),
    nconst varchar(11),
    CONSTRAINT title_directors_tconst_nconst_key UNIQUE (tconst, nconst),
    CONSTRAINT title_directors_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS imdb.title_writers
(
    id serial,
    tconst varchar(11),
    nconst varchar(11),
    CONSTRAINT title_writers_tconst_nconst_key UNIQUE (tconst, nconst),
    CONSTRAINT title_writers_pkey PRIMARY KEY (id)
);

-- title.episode.tsv

CREATE TABLE IF NOT EXISTS imdb.title_episode
(
    tconst varchar(11) NOT NULL,
    parent_tconst varchar(11) NOT NULL,
    season_number integer,
    episode_number integer,
    CONSTRAINT title_episode_pk PRIMARY KEY (tconst)
);

-- title.principals.tsv

CREATE TABLE IF NOT EXISTS imdb.title_principals
(
    tconst varchar(11) NOT NULL,
    ordering smallint NOT NULL,
    nconst varchar(11),
    category imdb.enum_job_category,
    job text,
    characters text,
    CONSTRAINT title_principals_pkey PRIMARY KEY (tconst, ordering)
);


-- title.ratings.tsv

CREATE TABLE IF NOT EXISTS imdb.title_ratings
(
    tconst varchar(11) NOT NULL,
    average_rating numeric(3, 1),
    num_votes integer,
    CONSTRAINT title_ratings_pk PRIMARY KEY (tconst)
);