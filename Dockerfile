FROM python:3.13-slim

# System deps for pyarrow, DuckDB, and psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev tini && \
    rm -rf /var/lib/apt/lists/*

# Install uv (official Dagster docs pattern for uv-based projects)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first for layer caching
COPY pyproject.toml uv.lock ./

# Copy source code
COPY src/ src/
COPY patches/ patches/

# Install all deps including dev group (dagster-webserver needed)
RUN uv sync --frozen

# Apply patches to installed packages (dlt patch removed — dlt no longer in stack)
RUN uv run python patches/duckdb_nullable_add_column.py

# Copy dagster instance config and workspace
COPY dagster.yaml /dagster-home/dagster.yaml
COPY workspace.yaml /app/workspace.yaml

ENV DAGSTER_HOME=/dagster-home
ENV PATH="/app/.venv/bin:$PATH"

# tini -g: kill entire process group on signal (catches orphan workers)
ENTRYPOINT ["tini", "-g", "--"]
