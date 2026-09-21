FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app
COPY task_manager/requirements.txt /tmp/req.txt
RUN pip install --no-cache-dir -r /tmp/req.txt
COPY common /app/common
COPY task_manager /app/task_manager
EXPOSE 8000
CMD ["uvicorn", "task_manager.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
