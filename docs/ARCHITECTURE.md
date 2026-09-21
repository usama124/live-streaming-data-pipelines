# Architecture — Live Streaming Module (Target Design)

This document describes the system being built, not a legacy system being replaced. If code
in the repo disagrees with this document, either the code is mid-migration (check the
implementation plan's phase status) or this document is stale — flag it, don't silently
follow the code.

Batch/normal pipelines are out of scope. They run as Airflow DAGs elsewhere.

---

## 1. Why live pipelines can't be Airflow DAGs

Airflow orchestrates runs that terminate. A live pipeline never finishes, so a task blocking
forever breaks scheduler heartbeats, slot accounting, zombie detection, and retries. The
submit-and-exit alternative supervises nothing once it exits. This is why live pipelines get
their own control plane instead of reusing Airflow's.

---

## 2. Data and control path

```
POST /{id}/start
  → Kubernetes Deployment created for the pipeline (replicas: 1)
    → Telegraf pod: our source connector (execd subprocess) → Kafka topic pipeline.<id>.events
  → shared consumer pool (regex-subscribed to pipeline.*.events)
    → batched insert → ClickHouse table (one per pipeline)

POST /{id}/stop
  → Deployment scaled to replicas: 0
```

No Redis, no polling loop, no separate reconciler. Kubernetes' own control loop
(`kube-controller-manager`) reconciles `replicas` against actual pod state.

---

## 3. Components

| Component | Cardinality | Owns | Does not own |
|---|---|---|---|
| Task Manager (API) | Shared, horizontally scaled | Pipeline CRUD, calls the Kubernetes API to create/scale/delete Deployments | Any lifecycle polling — Kubernetes owns that |
| Producer (Telegraf + connector) | 1 per pipeline | Source connection, emitting records to stdout | Kafka delivery, retry/backoff, metrics — Telegraf owns those |
| Consumer pool | Shared, few replicas | Kafka→ClickHouse batching, dead-letter handling, per-pipeline table writes | Any per-pipeline process or state — it's stateless across pipelines |
| Kubernetes | N/A (the platform) | Restart-on-crash (`restartPolicy: Always`), scheduling, host-failure recovery | Reconnect-to-source logic — that's the connector's job |

### 3.1 Producer

Telegraf (`inputs.execd`) launches our source connector as a long-lived subprocess. The
connector's only job: connect to the source, emit one record per line to stdout. Telegraf
owns everything downstream — batching, the Kafka producer, retry/backoff, Prometheus metrics.

Standard protocols (MQTT) use Telegraf's native input directly — no custom connector needed.
Proprietary/industrial sources (OPC UA now, AVEVA planned) get a thin connector script.

**Known gap:** Telegraf restarts the subprocess only if it exits. A silently-dead source
session (process alive, no data) is not detected by Telegraf. This is covered by the
Kubernetes liveness probe (§3.4), not by Telegraf itself — do not treat a connector as
production-ready without it.

### 3.2 Consumer pool

aiokafka, subscribed to `pipeline.*.events` by pattern — not a fixed topic list. A new
pipeline's topic is picked up within `metadata_max_age_ms` with no restart of the pool.
This is mandatory: customers create pipelines at times outside our control, and no design
may restart shared infrastructure on pipeline creation.

Core loop: `getmany()` → accumulate per topic → **insert, then commit** (in that order, so a
crash mid-batch re-reads rather than drops rows). Topic name maps to ClickHouse table name.

**Dead-letter handling lives inside this write path.** No exception may escape it — one bad
record must not stall every other pipeline sharing the pool.

### 3.3 ClickHouse tables

One table per pipeline, created explicitly from a declared schema at pipeline-creation time
— **never inferred from the first event seen**. Inferring from the first event is how two
pipelines with different field sets collide on a shared or ambiguous table; it must not
happen in this design.

**Naming.** `user_<user_id>_collection_<collection_number>_<table_name>`, e.g.
`user_1_collection_12_sensor_data`. `tenant_id` is the derived prefix,
`user_<user_id>_collection_<collection_number>`, and is **also an explicit column on every
row** — nothing may parse tenancy back out of a table name. `table_name` is
customer-influenced and validated at pipeline-creation time against `^[a-z_][a-z0-9_]*$`,
lowercased, composed name capped at 128 characters: it reaches ClickHouse as an identifier,
so that check is a trust boundary. See the decision memo's Proposal F.

**Topic → table resolution.** The topic name (`pipeline.<id>.events`) does *not* encode the
table name. The consumer pool resolves topic → `(tenant_id, database, table)` from
`PipelineConfig`, looked up on cache miss and cached in memory — never by string surgery on
the topic. This keeps the Kafka naming contract stable while the table naming scheme is free
to change, and it must be a lookup, not a restart (§3.2).

### 3.3.1 Dead-letter handling

Owned by the consumer pool's write path. No exception may escape it. The reasoning and the
rejected alternatives are in the decision memo's Proposal E; this is the contract.

**Classify before you act.** Two errors reach the same `except` and get opposite treatment:

| Class | What it is | Response |
|---|---|---|
| `parse` / `schema` | Our own validation failed before the insert — unparseable payload, missing declared column, type mismatch | Dead-letter the record, continue |
| `insert` (data) | ClickHouse rejected the rows for a deterministic reason | Bisect the batch, dead-letter the offending rows, insert the rest |
| `transient` | ClickHouse down, timeout, network, `TOO_MANY_PARTS`, memory limit | Retry with backoff. **Never dead-letter.** Do not commit |
| `unclassified` | Anything not positively recognised | Treat as transient, retry the full budget, *then* dead-letter with `error_class='unclassified'` |

The default matters more than the list: **an unrecognised error is transient until proven
otherwise.** Treating unknown errors as poison is how a ClickHouse outage quietly drains a
whole stream into the DLQ.

Classification is by ClickHouse error code against an explicit allowlist of data errors —
starting set to verify against the deployed server version in Phase 2: `6`, `16`, `26`,
`27`, `38`, `41`, `43`, `47`, `53`, `69`, `70`, `72`, `117`, `349`. Everything else,
including `60 UNKNOWN_TABLE` (a provisioning bug, not a bad record), is transient.

Retry uses the existing `ClickHouseConnectionManager` backoff (base 1s, doubling, capped at
60s, 6 attempts) — it is already in the repo and already correct; do not write a second one.
When the budget is exhausted, **pause that topic's partitions only** (`consumer.pause()`) and
leave the rest of the pool running. Offsets stay uncommitted, so nothing is lost; that
pipeline goes visibly stale, which is the honest outcome.

**Batch isolation.** One bad row fails the whole insert. On a data-class error, bisect the
batch and recurse; below 10 rows, insert row by row. One poison record in 500 costs ~9 extra
inserts. Row-by-row over the whole batch is rejected: 500 single-row inserts provokes
`TOO_MANY_PARTS`, manufacturing an infrastructure failure out of a data one.

**Write sequence, per topic-batch — the order is the contract:**

1. Parse and validate rows; validation failures go to a dead-letter buffer, not an exception
2. Insert the good rows into the pipeline's table (bisect on a data-class error)
3. Insert the dead-letter buffer into `dead_letters`
4. **Only then** commit offsets

Steps 2 and 3 must both succeed before step 4. A failing dead-letter insert is itself a
transient error, so it retries and the offsets stay put — at-least-once holds for dead
letters too, not just for good records.

```sql
CREATE TABLE IF NOT EXISTS data_platform.dead_letters (
    failed_at      DateTime64(3) DEFAULT now64(3),
    tenant_id      LowCardinality(String),
    pipeline_id    LowCardinality(String),
    topic          LowCardinality(String),
    partition      Int32,
    offset         Int64,
    target_table   String,
    error_class    LowCardinality(String),  -- parse | schema | insert | unclassified
    error_code     Int32,                   -- ClickHouse error code, 0 if not applicable
    error_message  String,
    payload        String                   -- raw record as received, for replay
) ENGINE = MergeTree
PARTITION BY toYYYYMM(failed_at)
ORDER BY (tenant_id, pipeline_id, failed_at)
TTL toDateTime(failed_at) + INTERVAL 30 DAY;
```

30-day TTL: the DLQ is a diagnosis and replay surface, not an archive. Replay is deliberate
and manual — `SELECT payload FROM dead_letters WHERE pipeline_id = ...` re-produced to the
topic after the schema or source is fixed. There is no automatic replayer, by design: it
would re-feed the same poison into the same table on a loop.

Dead-letter *rate* is a monitoring concern, not a control path — the pool emits
`dlq_rows_total{pipeline_id, tenant_id, error_class}` and Phase 4 alerts on it. A sustained
rate means the declared schema and the source disagree, which is a human decision.

### 3.4 Orchestration

Kubernetes Deployments. `desired_state` ≡ replica count. A liveness probe on the producer
pod fails when the last successful source read exceeds a threshold (tuned per source type),
so kubelet recycles a hung-but-alive pod — this is the backstop for §3.1's Telegraf gap.

A failed start returns an error to the API caller directly, rather than surfacing on a later
poll — there is no poll; there is no controller loop to poll on.

### 3.5 Monitoring

Three layers, not one:
- **Is it flowing** — Kafka UI + Prometheus (lag, throughput, rows landed)
- **How stale** — OpenTelemetry tracing, source read → Kafka → ClickHouse insert. Lag alone
  is measured in messages, not seconds, and misses a stalled source that shows zero lag while
  data is an hour old.
- **What the customer sees** — a per-tenant health API on our own surface, not Grafana, which
  holds cross-tenant data that can never be shown to a tenant directly.

---

## 4. Config contract

- `PipelineConfig` must include an explicit table field — do not rely on a global
  `CLICKHOUSE_TABLE` setting shared across pipelines.
- `tenant_id` must be part of the schema from the start — as a column, not only as a table
  name prefix. It is `user_<user_id>_collection_<collection_number>`, derived from
  Stratahub's identifiers rather than minted here, and those identifiers must be immutable
  numeric IDs (a renameable handle in a table name means orphaned tables). See the decision
  memo's Proposal F.
- A table-name prefix is a naming convention, **not** an isolation boundary. Nothing that
  scopes a customer-facing answer — the Phase 4 per-tenant health API above all — may rely on
  naming alone.
- Kafka topic naming: `pipeline.<id>.events`, matched by the consumer pool's subscription
  pattern `pipeline.*.events`.

---

## 5. Explicitly not built here

- Apache Flink or any stateful/windowed stream processing — evaluated, not adopted. The
  consumer pool does no transformation by design; revisit only if that changes. See the
  decision memo's alternatives section.
- Per-record lineage / Apache NiFi — open question, unresolved, tracked in `BACKLOG.md`.
  A bigger architectural swing that should be settled independently before it affects
  anything above.
- AVEVA and MQTT connectors — the pattern supports them (§3.1), building them is follow-on
  work, tracked in `BACKLOG.md`.
