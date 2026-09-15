# Data Platform Live Streaming Backend

This project implements a backend-only data platform skeleton for two pipeline types:

1. **Normal pipelines** controlled through Airflow APIs.
2. **Live streaming pipelines** controlled through Redis state and executed as per-pipeline producer/consumer containers.

The local setup uses:

- FastAPI Task Manager
- Redis for live pipeline state
- Kafka for event transport
- ClickHouse for analytical storage
- Docker Compose for local infrastructure
- Mock source generator instead of OPC UA

## Architecture decision

For live streaming producers, this implementation uses **one producer container per pipeline**.

Why:

- **Scalability:** each pipeline can scale independently. Heavy OPC UA/source connections do not block unrelated pipelines.
- **Fault isolation:** if one producer crashes, only that pipeline is affected.
- **Operational clarity:** logs, metrics, restarts, resource limits, and deployments are pipeline-specific.
- **Resource utilization:** slightly higher container overhead, but predictable CPU/memory isolation is better for near real-time production workloads.

A single dynamic producer service is cheaper for very small workloads, but it becomes harder to isolate failures, debug stuck source clients, and scale hot pipelines independently.

## Folder structure

```text
.
├── docker-compose.yml
├── .env.example
├── clickhouse
│   └── init.sql
├── common
│   └── app_common
│       ├── __init__.py
│       ├── models.py
│       └── redis_keys.py
└── services
    ├── task_manager
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── app
    │       ├── main.py
    │       ├── config.py
    │       ├── airflow_client.py
    │       ├── redis_repo.py
    │       └── runtime
    │           ├── base.py
    │           ├── docker_runtime.py
    │           └── factory.py
    ├── producer_service
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── app
    │       ├── main.py
    │       ├── config.py
    │       ├── kafka_publisher.py
    │       ├── redis_control.py
    │       └── sources
    │           ├── base.py
    │           └── mock_source.py
    └── consumer_service
        ├── Dockerfile
        ├── requirements.txt
        └── app
            ├── main.py
            ├── config.py
            ├── clickhouse_sink.py
            └── kafka_batch_consumer.py
```

## Start locally

### 1. Build images

```bash
docker compose build task-manager producer-template consumer-template
```

### 2. Start backend infrastructure and Task Manager

```bash
docker compose up -d redis kafka clickhouse task-manager
```

### 3. Check Task Manager health

```bash
curl http://localhost:8000/health
```

### 4. Create a live streaming pipeline

```bash
curl -X POST http://localhost:8000/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_id": "opcua-line-1",
    "pipeline_type": "live",
    "source_type": "mock",
    "topic": "pipeline.opcua-line-1.events",
    "batch_size": 500,
    "flush_interval_seconds": 60
  }'
```

### 5. Start the pipeline

```bash
curl -X POST http://localhost:8000/pipelines/opcua-line-1/start
```

Task Manager will:

1. Write desired state to Redis.
2. Publish a Redis state-change event.
3. Start one producer container for this pipeline.
4. Start one consumer container for this pipeline.

### 6. Check status

```bash
curl http://localhost:8000/pipelines/opcua-line-1/status
```

### 7. Query ClickHouse

```bash
docker exec -it clickhouse clickhouse-client \
  --query "SELECT pipeline_id, count() FROM data_platform.pipeline_events GROUP BY pipeline_id"
```

You can also inspect recent rows:

```bash
docker exec -it clickhouse clickhouse-client \
  --query "SELECT pipeline_id, event_time, sequence, source, payload_json FROM data_platform.pipeline_events ORDER BY ingest_time DESC LIMIT 5 FORMAT Vertical"
```

### 8. Stop the pipeline

```bash
curl -X POST http://localhost:8000/pipelines/opcua-line-1/stop
```

### 9. View containers

```bash
docker ps --filter "label=data-platform.pipeline_id=opcua-line-1"
```

## Normal Airflow pipelines

Create a normal pipeline:

```bash
curl -X POST http://localhost:8000/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_id": "daily-sales-load",
    "pipeline_type": "normal",
    "airflow_dag_id": "daily_sales_load"
  }'
```

Start:

```bash
curl -X POST http://localhost:8000/pipelines/daily-sales-load/start
```

For local demo, if Airflow is not running, set `AIRFLOW_ENABLED=false` in `.env`. The Task Manager will return a mocked response for normal pipelines.

## How new live pipelines are created

1. API receives pipeline metadata.
2. Metadata is stored in Redis at `pipeline:{pipeline_id}:config`.
3. Runtime status is stored at `pipeline:{pipeline_id}:state`.
4. On start, Task Manager starts containers using the same producer/consumer images but passes pipeline-specific environment variables.
5. Producer emits Kafka records to the pipeline topic.
6. Consumer reads the topic, batches rows, and writes to ClickHouse.

## Service boundaries

### Task Manager

Owns pipeline lifecycle API:

- Create pipeline
- Start pipeline
- Stop pipeline
- Read status
- For normal pipelines: calls Airflow APIs
- For live pipelines: writes Redis state and starts/stops producer/consumer runtime

### Producer service

One container per live pipeline.

- Reads its pipeline config from Redis/env
- Streams from a source connector
- Publishes events to Kafka
- Updates heartbeat/status in Redis
- Stops when Redis desired state changes to stopped

### Consumer service

One container per live pipeline.

- Reads Kafka topic
- Batches by size or time window
- Inserts batch into ClickHouse
- Updates heartbeat/status in Redis
- Flushes pending data before shutdown

## Production notes

For Kubernetes, replace `DockerRuntimeAdapter` with a `KubernetesRuntimeAdapter` that creates/deletes Deployments or Jobs per pipeline. The service interfaces are already separated under `services/task_manager/app/runtime`.

Recommended production extensions:

- Kubernetes resource limits per producer/consumer
- Dead-letter Kafka topic for malformed records
- Kafka topic creation through admin service
- Prometheus metrics endpoint for each service
- Redis persistence/AOF enabled
- ClickHouse replicated cluster for HA
- Idempotency keys or ReplacingMergeTree if duplicate inserts must be eliminated
- Secure secrets through Vault/Kubernetes Secrets
- mTLS/SASL for Kafka
- TLS/auth for Redis and ClickHouse
