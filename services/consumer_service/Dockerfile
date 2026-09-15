FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app:/app/app:/app/common

WORKDIR /app

# ── Build context must be the project root (stratahub-live-streaming/) ────────
# Run from project root:
#   docker build -f services/consumer_service/Dockerfile -t stratahub-consumer:latest .
# Or use docker compose build from project root (docker-compose.yml sets context: .)

COPY services/consumer_service/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# common/ → /app/common/app_common/  (imported as common.app_common.*)
COPY common /app/common

# app/  → /app/app/  (config, quix_consumer, clickhouse_manager, clickhouse_sink)
COPY services/consumer_service/app /app/app

CMD ["python", "-m", "app.main"]