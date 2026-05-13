#!/bin/bash

set -a

source .env

# Configuration
DB_NAME=$POSTGRES_DATABASE
USER=$POSTGRES_USER
SCHEMA=$IMDB_SCHEMA
OUTPUT_DIR="./backups"

# Create backup directory if it doesn't exist
mkdir -p $OUTPUT_DIR

echo "Starting dump for imdb.watch_status and imdb.watch_date_scores..."

# Dump specific tables
# -t specifies the table (schema-qualified)
# --clean includes commands to DROP tables before creating them
# --if-exists prevents errors during restoration if tables don't exist
pg_dump -h localhost -U $USER -d $DB_NAME \
    -t "$SCHEMA.watch_status" \
    -t "$SCHEMA.watch_date_scores" \
    --clean --if-exists \
    -f "$OUTPUT_DIR/imdb_watch_tables.sql"

echo "Dump completed: $OUTPUT_DIR/imdb_watch_tables.sql"


set +a


