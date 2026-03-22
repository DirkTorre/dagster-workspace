CREATE TABLE IF NOT EXISTS genres
(
    id serial,
    tconst varchar(11) NOT NULL,
    genre enum_genre NOT NULL,
    CONSTRAINT genres_tconst_genre_key UNIQUE (tconst, genre),
    CONSTRAINT genres_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS known_for_titles
(
    nconst varchar(11) NOT NULL,
    tconst varchar(11) NOT NULL
);

CREATE TABLE IF NOT EXISTS name_basics
(
    nconst varchar(11) NOT NULL,
    primary_name varchar(120),
    birth_year smallint,
    death_year smallint,
    CONSTRAINT name_basics_pkey PRIMARY KEY (nconst)
);

CREATE TABLE IF NOT EXISTS primary_profession
(
    nconst varchar(11),
    primary_profession varchar(26)
);

CREATE TABLE IF NOT EXISTS title_basics
(
    tconst varchar(11) NOT NULL,
    title_type enum_titletype,
    primary_title text,
    original_title text,
    is_adult boolean,
    start_year smallint,
    end_year smallint,
    runtime_minutes integer,
    CONSTRAINT title_basics_pkey PRIMARY KEY (tconst)
);

CREATE TABLE IF NOT EXISTS title_directors
(
    id serial,
    tconst varchar(11),
    nconst varchar(11),
    CONSTRAINT title_directors_tconst_nconst_key UNIQUE (tconst, nconst),
    CONSTRAINT title_directors_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS title_principals
(
    tconst varchar(11) COLLATE pg_catalog."default" NOT NULL,
    ordering smallint NOT NULL,
    nconst varchar(11),
    category enum_job_category,
    job text,
    characters text,
    CONSTRAINT title_principals_pkey PRIMARY KEY (tconst, ordering)
);

CREATE TABLE IF NOT EXISTS title_ratings
(
    tconst varchar(11) COLLATE pg_catalog."default" NOT NULL,
    average_rating numeric(3, 1),
    num_votes integer,
    CONSTRAINT title_ratings_pk PRIMARY KEY (tconst)
);

CREATE TABLE IF NOT EXISTS title_writers
(
    id serial,
    tconst varchar(11),
    nconst varchar(11),
    CONSTRAINT title_writers_tconst_nconst_key UNIQUE (tconst, nconst),
    CONSTRAINT title_writers_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS title_episode
(
    tconst varchar(11) NOT NULL,
    parent_tconst varchar(11) NOT NULL,
    season_number smallint,
    episode_number smallint,
    CONSTRAINT title_episode_pk PRIMARY KEY (tconst)
)
