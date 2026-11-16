FROM confluentinc/cp-kafka-connect:7.8.0
ARG DEBEZIUM_VERSION=3.0.8.Final
USER root
RUN set -eu; \
    artifact="debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz"; \
    url="https://repo.maven.apache.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/${artifact}"; \
    tmp="$(mktemp -d)"; \
    trap 'rm -rf "$tmp"' EXIT; \
    cd "$tmp"; \
    download() { \
        curl --disable --fail --silent --show-error --location \
            --proto '=https' --proto-redir '=https' \
            --retry 6 --retry-max-time 180 --connect-timeout 10 --max-time 60 \
            --output "$2" "$1"; \
    }; \
    download "$url.sha1" "$artifact.sha1" || { echo "Maven Central checksum download failed." >&2; exit 1; }; \
    checksum="$(awk 'NR == 1 { print $1 }' "$artifact.sha1")"; \
    if ! printf '%s\n' "$checksum" | grep -Eq '^[[:xdigit:]]{40}$'; then \
        echo "Invalid Maven Central SHA-1 checksum for $artifact." >&2; exit 1; \
    fi; \
    download "$url" "$artifact" || { echo "Maven Central Debezium download failed." >&2; exit 1; }; \
    if ! printf '%s  %s\n' "$checksum" "$artifact" | sha1sum --check --strict; then \
        echo "Debezium archive checksum mismatch for $artifact." >&2; exit 1; \
    fi; \
    mkdir -p /opt/connect-plugins; \
    tar -xzf "$artifact" -C /opt/connect-plugins
USER appuser
