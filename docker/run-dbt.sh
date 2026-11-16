#!/bin/sh
set -eu
cd /tmp
for archive in /opt/spark/python/lib/*.zip; do
    PYTHONPATH="$archive:$PYTHONPATH"
done
export PYSPARK_SUBMIT_ARGS="--conf spark.sql.defaultCatalog=lake pyspark-shell"
exec /opt/paritylake/dbt/bin/dbt --project-dir /app/dbt --profiles-dir /app/dbt \
    --target-path /tmp/paritylake-dbt-target --log-path /tmp/paritylake-dbt-logs "$@"
