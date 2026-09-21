# Shared consumer pool — one deployment for all live pipelines.
#   docker build -f services/consumer_pool/Dockerfile -t data-platform-consumer-pool:latest .
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app

COPY services/consumer_pool/requirements.txt /tmp/req.txt
RUN pip install --no-cache-dir -r /tmp/req.txt

# The task_manager tree is the canonical app_common (models, redis_repo, ch_naming).
COPY services/task_manager/common /app/common
COPY services/consumer_pool /app/consumer_pool

EXPOSE 9100
CMD ["python", "-m", "consumer_pool.app.main"]
