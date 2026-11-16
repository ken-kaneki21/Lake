FROM apache/spark:3.5.5-python3
USER root
WORKDIR /app
RUN mkdir -p /opt/paritylake/ivy && chown 185:0 /opt/paritylake/ivy \
    && python3 -m pip install --no-cache-dir --target /opt/paritylake/python \
        'psycopg[binary]==3.2.10' fastavro==1.12.1 great-expectations==0.18.22
COPY docker/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
RUN python3 -m pip install --no-cache-dir --target /opt/paritylake/dbt dbt-spark==1.11.0 dbt-core==1.11.0
COPY dbt dbt
COPY docker/run-dbt.sh /opt/paritylake/run-dbt.sh
COPY recon recon
COPY streaming streaming
COPY tests tests
USER 185
ENTRYPOINT ["/opt/spark/bin/spark-submit"]
