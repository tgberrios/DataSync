#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATION_FILE="$SCRIPT_DIR/system_logs_migration.sql"

if [ ! -f "$MIGRATION_FILE" ]; then
    echo "Error: Migration file not found: $MIGRATION_FILE"
    exit 1
fi

if [ -z "$DATABASE_URL" ] && [ -z "$PGHOST" ]; then
    echo "Error: Database connection not configured."
    echo "Please set DATABASE_URL or PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD"
    exit 1
fi

echo "Running system_logs migration..."
psql $DATABASE_URL -f "$MIGRATION_FILE"

if [ $? -eq 0 ]; then
    echo "System logs migration completed successfully!"
else
    echo "Error: Migration failed!"
    exit 1
fi

