#!/bin/sh
set -eu
cd /app/dbt
export DBT_ENGINE_PROFILES_DIR=/app/dbt
export DBT_ENGINE_TARGET_PATH=/tmp/paritylake-dbt-target
export DBT_ENGINE_LOG_PATH=/tmp/paritylake-dbt-logs
for archive in /opt/spark/python/lib/*.zip; do
    PYTHONPATH="$archive:$PYTHONPATH"
done
export PYTHONPATH
mkdir -p /tmp/paritylake-dbt-derby
export PYSPARK_SUBMIT_ARGS="--conf spark.sql.defaultCatalog=lake --conf spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/paritylake-dbt-derby pyspark-shell"
exec /opt/paritylake/dbt/bin/dbt "$@"
