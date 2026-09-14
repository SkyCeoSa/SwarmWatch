# SwarmWatch Self-Hosted Observability Platform
# Single-container zero-cloud deployment: Backend + UI + Local SQLite Database

FROM python:3.12-slim AS runtime

WORKDIR /app

# Install system dependencies (curl for container healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create persistent storage directory for SQLite/ClickHouse data
RUN mkdir -p /data && chmod 777 /data

# Copy package and dashboard source files
COPY packages/sdk-python /app/packages/sdk-python
COPY dashboard.html /app/dashboard.html

# Install SwarmWatch with all dependencies (FastAPI, Uvicorn, Pydantic)
RUN pip install --no-cache-dir "/app/packages/sdk-python[all]"

# Environment defaults
ENV SWARMWATCH_HOST="0.0.0.0" \
    SWARMWATCH_PORT=8765 \
    SWARMWATCH_DB="/data/swarmwatch.db" \
    PYTHONUNBUFFERED=1

EXPOSE 8765

VOLUME ["/data"]

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8765/api/v1/health || exit 1

ENTRYPOINT ["swarmwatch"]
CMD ["ui", "--host", "0.0.0.0", "--port", "8765", "--db", "/data/swarmwatch.db", "--no-browser"]
