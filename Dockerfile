# syntax=docker/dockerfile:1.7
#
# The tool, for using it. For working on the code see Dockerfile.dev.

FROM python:3.12-slim

# osmium-tool: cut and filter OSM extracts (osmium extract / tags-filter)
RUN apt-get update \
    && apt-get install -y --no-install-recommends osmium-tool \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PYTHONPATH=/app \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /app

# Dependencies first so they stay cached when only the source changes
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project --no-dev

COPY gtfstoosm ./gtfstoosm
RUN uv sync --frozen --no-dev

COPY --chmod=755 docker/gtfstoosm /usr/local/bin/gtfstoosm

ENTRYPOINT ["python", "-m", "gtfstoosm.cli"]
CMD ["--help"]
