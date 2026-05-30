#!/bin/sh

/usr/lib/trino/bin/launcher run --etc-dir /etc/trino &
pid=$!

until trino --execute 'SELECT 1' >/dev/null 2>&1; do
  echo "Waiting for Trino server..."
  sleep 5
done

echo "Trino server is ready. Starting ETL pipeline..."

trino --file /opt/trino-sql/01_create_dwh.sql
trino --file /opt/trino-sql/02_create_marts.sql

echo "Trino ETL pipeline completed. Resuming trino in foreground..."

wait "$pid"
