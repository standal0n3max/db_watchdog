# Образ графа: MCP-сервер с отсечкой и локальный индекс.
# Индекс и mirror монтируются в /data и в образ не входят.

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/db-pipeline/src
WORKDIR /opt/db-pipeline

RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "mcp>=1.9.0" "beartype>=0.20.0"

COPY src /opt/db-pipeline/src

EXPOSE 8765
CMD ["python", "-m", "db_pipeline.benchmark.graph.server"]
