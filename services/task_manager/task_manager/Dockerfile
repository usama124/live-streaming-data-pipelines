FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app

# Build context is the project root, so telegraf/templates can be copied in —
# the runtime adapters render a per-pipeline Telegraf config and need them.
#   docker build -f services/task_manager/task_manager/Dockerfile -t <tag> .
COPY services/task_manager/task_manager/requirements.txt /tmp/req.txt
RUN pip install --no-cache-dir -r /tmp/req.txt

COPY services/task_manager/common /app/common
COPY services/task_manager/task_manager /app/task_manager
COPY telegraf/templates /opt/telegraf/templates
# KubernetesRuntime renders the producer Deployment from these.
COPY k8s /opt/k8s

EXPOSE 8000
CMD ["uvicorn", "task_manager.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
