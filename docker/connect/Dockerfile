FROM confluentinc/cp-kafka-connect:7.8.0
ARG DEBEZIUM_VERSION=3.0.8.Final
USER root
RUN mkdir -p /opt/connect-plugins \
    && curl -fSL --retry 3 --max-time 120 \
        "https://repo.maven.apache.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz" \
        -o /tmp/postgres-plugin.tar.gz \
    && tar -xzf /tmp/postgres-plugin.tar.gz -C /opt/connect-plugins \
    && rm /tmp/postgres-plugin.tar.gz
USER appuser
