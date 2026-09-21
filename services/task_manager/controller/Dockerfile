FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app

# Build context is the project root, so telegraf/templates can be copied in —
# the runtime adapters render a per-pipeline Telegraf config and need them.
#   docker build -f services/task_manager/controller/Dockerfile -t <tag> .
COPY services/task_manager/controller/requirements.txt /tmp/req.txt
RUN pip install --no-cache-dir -r /tmp/req.txt

COPY services/task_manager/common /app/common
COPY services/task_manager/controller /app/controller
COPY telegraf/templates /opt/telegraf/templates

CMD ["python", "-m", "controller.app.main"]
