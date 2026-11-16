FROM python:3.12.12-slim
WORKDIR /app
COPY requirements-source.txt .
RUN pip install --no-cache-dir -r requirements-source.txt confluent-kafka==2.12.2 fastavro==1.12.1
COPY docker/connect/debezium-postgres.json docker/connect/debezium-postgres.json
COPY source_db source_db
COPY replayer replayer
COPY recon recon
COPY streaming streaming
COPY tests tests
COPY chaos chaos
RUN install -d -o 10001 -g 0 /var/lib/paritylake
USER 10001
