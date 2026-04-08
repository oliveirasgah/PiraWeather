FROM postgres:17

# Install Citus columnar extension from the official Citus apt repository.
# The package name is resolved dynamically so it doesn't need to be updated
# when Citus releases a new minor version.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && curl -fsSL https://install.citusdata.com/community/deb.sh | bash \
    && apt-get update \
    && PG_MAJOR="$(pg_config --major-version)" \
    && CITUS_PKG="$(apt-cache search "^postgresql-${PG_MAJOR}-citus" | sort -V | tail -1 | cut -d' ' -f1)" \
    && apt-get install -y --no-install-recommends "${CITUS_PKG}" \
    && rm -rf /var/lib/apt/lists/*
