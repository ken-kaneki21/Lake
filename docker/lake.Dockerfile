FROM apache/spark:3.5.5-python3 AS spark-dist
RUN python3 -c 'import platform; print("Upstream Spark image:", platform.python_version(), platform.machine())'

FROM python:3.11-slim-bookworm
ENV SPARK_HOME=/opt/spark \
    PATH=/opt/paritylake/python/bin:/opt/spark/bin:$PATH \
    PYSPARK_PYTHON=/opt/paritylake/python/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/paritylake/python/bin/python \
    PYTHONPATH=/app \
    HOME=/home/spark
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 185 --gid 0 --create-home --home-dir /home/spark --shell /usr/sbin/nologin spark \
    && mkdir -p /opt/paritylake/ivy \
    && chown 185:0 /opt/paritylake/ivy
COPY --from=spark-dist /opt/spark /opt/spark

RUN /usr/local/bin/python3 -c 'import platform; print("Lake image:", platform.python_version(), platform.machine())' \
    && /usr/local/bin/python3 -m venv /opt/paritylake/python \
    && python -m pip install --no-cache-dir --only-binary=:all: \
        'psycopg[binary]==3.2.10' fastavro==1.12.1 great-expectations==0.18.22 \
    && python -m pip check \
    && /usr/local/bin/python3 -m venv /opt/paritylake/dbt \
    && /opt/paritylake/dbt/bin/python -m pip install --no-cache-dir \
        dbt-spark==1.11.0 dbt-core==1.11.0 \
    && /opt/paritylake/dbt/bin/python -m pip check

COPY docker/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY dbt dbt
COPY docker/run-dbt.sh /opt/paritylake/run-dbt.sh
COPY recon recon
COPY streaming streaming
COPY tests tests
USER 185
ENTRYPOINT ["/opt/spark/bin/spark-submit"]
