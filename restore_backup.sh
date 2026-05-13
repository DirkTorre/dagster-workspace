#!/bin/bash

# Configuration
DB_NAME="your_database_name"
USER="your_username"
BACKUP_FILE="./backups/imdb_watch_tables.sql"

echo "Restoring tables from $BACKUP_FILE..."

# Execute the script
# This will drop the tables (if they exist) and recreate them with data
psql -h localhost -U $USER -d $DB_NAME -f "$BACKUP_FILE"

echo "Restore complete."