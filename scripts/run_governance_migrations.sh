#!/bin/bash

# Script to run governance migrations
# Usage: ./run_governance_migrations.sh [host] [port] [database]

HOST=${1:-localhost}
PORT=${2:-5432}
DATABASE=${3:-DataLake}
USER=${4:-tomy.berrios}
PASSWORD=${5:-Yucaquemada1}

echo "Running governance migrations..."
echo "Host: $HOST"
echo "Port: $PORT"
echo "Database: $DATABASE"
echo "User: $USER"

export PGPASSWORD="$PASSWORD"

psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DATABASE" -f governance_migrations.sql

if [ $? -eq 0 ]; then
    echo "Migrations completed successfully!"
else
    echo "Migration failed!"
    exit 1
fi

unset PGPASSWORD
