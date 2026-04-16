#!/bin/bash

set -a

# ex$POSTGRES_PORT env vars
source .env

sudo -u postgres bash -c "psql -h $POSTGRES_HOST -c \"
CREATE DATABASE $POSTGRES_DATABASE;
\""

sudo -u postgres bash -c "psql -h $POSTGRES_HOST -d $POSTGRES_DATABASE -c \"
CREATE SCHEMA IF NOT EXISTS $IMDB_SCHEMA;
\""

sudo -u postgres bash -c "psql -h $POSTGRES_HOST -d $POSTGRES_DATABASE -U postgres -p $POSTGRES_PORT -a -f projects/imdb/sql/create_tables.sql"


sudo -u postgres bash -c "psql -h $POSTGRES_HOST -d $POSTGRES_DATABASE -U postgres -p $POSTGRES_PORT -c \"
CREATE USER $POSTGRES_USER WITH PASSWORD '$POSTGRES_PASSWORD';

GRANT USAGE, CREATE ON SCHEMA $IMDB_SCHEMA TO $POSTGRES_USER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA $IMDB_SCHEMA TO $POSTGRES_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA $IMDB_SCHEMA
    GRANT ALL PRIVILEGES ON TABLES TO $POSTGRES_USER;


GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA $IMDB_SCHEMA TO $POSTGRES_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA $IMDB_SCHEMA GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO $POSTGRES_USER;

sudo -u postgres bash -c "psql -h $POSTGRES_HOST -d $POSTGRES_DATABASE -U postgres -p $POSTGRES_PORT -c \"

-- Change owner for the schema itself
ALTER SCHEMA $IMDB_SCHEMA OWNER TO $POSTGRES_USER;

-- Change owner for all tables in the schema
DO
\\$\\$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = '$IMDB_SCHEMA'
    LOOP
        EXECUTE format('ALTER TABLE $IMDB_SCHEMA.%I OWNER TO $POSTGRES_USER;', r.tablename);
    END LOOP;
END
\\$\\$;

-- Change owner for all sequences in the schema
DO
\\$\\$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT sequencename FROM pg_sequences WHERE schemaname = '$IMDB_SCHEMA'
    LOOP
        EXECUTE format('ALTER SEQUENCE $IMDB_SCHEMA.%I OWNER TO $POSTGRES_USER;', r.sequencename);
    END LOOP;
END
\\$\\$;

-- Change owner for all views in the schema
DO
\\$\\$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT table_name FROM information_schema.views WHERE table_schema = '$IMDB_SCHEMA'
    LOOP
        EXECUTE format('ALTER VIEW $IMDB_SCHEMA.%I OWNER TO $POSTGRES_USER;', r.table_name);
    END LOOP;
END
\\$\\$;

-- Change owner for all functions in the schema
DO
\\$\\$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT routine_name, routine_schema
        FROM information_schema.routines
        WHERE routine_schema = '$IMDB_SCHEMA'
    LOOP
        EXECUTE format('ALTER FUNCTION $IMDB_SCHEMA.%I() OWNER TO $POSTGRES_USER;', r.routine_name);
    END LOOP;
END
\\$\\$;

\""


# deletes env vars
set +a

# sudo -u postgres bash -c "psql -h $POSTGRES_HOST -d $DATABASE -c \"
# # GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
# # ON ALL TABLES
# # IN SCHEMA $IMDB_SCHEMA
# # TO $POSTGRES_USER;
# ALTER DATABASE $DATABASE OWNER TO $POSTGRES_USER;
# \""




# GRANT ALL PRIVILEGES ON SCHEMA $IMDB_SCHEMA TO $POSTGRES_USER;
# ALTER DATABASE $DATABASE OWNER TO $POSTGRES_USER;

# GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
#     ON ALL TABLES IN SCHEMA $IMDB_SCHEMA
#     TO $POSTGRES_USER;