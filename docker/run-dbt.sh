#!/bin/sh
set -eu
cd /app/dbt
export DBT_ENGINE_PROFILES_DIR=/app/dbt
export DBT_ENGINE_TARGET_PATH=/tmp/paritylake-dbt-target
export DBT_ENGINE_LOG_PATH=/tmp/paritylake-dbt-logs
export DBT_LOG_PATH="$DBT_ENGINE_LOG_PATH"
export DBT_TARGET_PATH="$DBT_ENGINE_TARGET_PATH"
for archive in /opt/spark/python/lib/*.zip; do
    PYTHONPATH="$archive:$PYTHONPATH"
done
export PYTHONPATH
for config in dbt_project.yml profiles.yml; do
    if [ ! -r "$config" ]; then
        printf "dbt configuration is unreadable: %s/%s\n" "$PWD" "$config" >&2
        exit 1
    fi
done
for dir in "$DBT_ENGINE_TARGET_PATH" "$DBT_ENGINE_LOG_PATH" /tmp/paritylake-dbt-derby; do
    mkdir -p "$dir"
    if [ ! -w "$dir" ] || [ ! -x "$dir" ]; then
        printf "dbt output directory is inaccessible to UID %s: %s\n" "$(id -u)" "$dir" >&2
        exit 1
    fi
done
export PYSPARK_SUBMIT_ARGS="--conf spark.sql.defaultCatalog=lake --conf spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/paritylake-dbt-derby pyspark-shell"
if [ "${1:-}" = "--version" ]; then
    exec /opt/paritylake/dbt/bin/dbt "$@"
fi
exec /opt/paritylake/dbt/bin/dbt --log-path "$DBT_ENGINE_LOG_PATH" "$@"
