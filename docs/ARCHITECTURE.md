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
crash mid-batch re-reads rather than drops rows). The topic resolves to a `PipelineConfig`
through Redis, cached, and the config names the table — the topic name is never parsed into
a table name, and a cache miss is a lookup, never a restart.

**Dead-letter handling lives inside this write path.** No exception may escape it — one bad
record must not stall every other pipeline sharing the pool.

### 3.3 ClickHouse tables

One table per pipeline, created explicitly from a declared schema at pipeline-creation time
— **never inferred from the first event seen**. Inferring from the first event is how two
pipelines with different field sets collide on a shared or ambiguous table; it must not
happen in this design.

As built (Phase 2): `PipelineConfig.table_schema` declares the columns, the DDL lives in
`common/app_common/ch_schema.py`, and both the API (at create time) and the pool (before its
first write) call it — one source of DDL, so the two cannot disagree. A key a message carries
but the schema does not declare is dropped rather than added as a column.

### 3.3.1 Dead-letter handling

Owned by the consumer pool's write path (`services/consumer_pool/app/sink.py`). No exception
escapes it: the pool is shared, so an exception that gets out stalls every other pipeline
on it.

**Failures are isolated per record, not per batch.** Records that cannot be mapped are set
aside before the insert; if the insert still fails, each remaining record is retried on its
own so one poison row cannot discard the ~499 good ones beside it. The rest of the batch
inserts and commits normally.

Dead letters go to one shared `dead_letter_events` table — the schema is fixed, unlike
pipeline tables, so there is nothing to gain from one per pipeline. Each row carries the
original payload, its topic, partition and offset, the target table, the error and the
timestamp: enough to replay it later. `dlq_rows_total{pipeline_id}` counts them for
Prometheus.

**There is no automatic replay, and no retry loop over dead letters** — replay is deliberate
and manual, after the schema or the source is fixed. An automatic replayer re-feeds the same
poison into the same table on a loop.

A write that fails because ClickHouse itself is unreachable is *not* dead-lettered: the
offsets stay uncommitted and the batch is re-read when it comes back. Dead-lettering there
would drain a whole stream into the DLQ during an outage.

### 3.4 Orchestration

Kubernetes Deployments. `desired_state` ≡ replica count. A liveness probe on the producer
pod fails when the last successful source read exceeds a threshold (tuned per source type),
so kubelet recycles a hung-but-alive pod — this is the backstop for §3.1's Telegraf gap.

A failed start returns an error to the API caller directly, rather than surfacing on a later
poll — there is no poll; there is no controller loop to poll on.

As built (Phase 3): `KubernetesRuntimeAdapter`, the manifests in `k8s/`, and a per-pipeline
ConfigMap holding the rendered Telegraf config — a multi-line TOML document substituted into
YAML is a quoting accident waiting to happen. Stop scales to 0 rather than deleting, so a
stopped pipeline is still a pipeline. `k8s/dev/` runs the whole system inside kind for tests.

**Staleness threshold: 60s for OPC UA**, overridable per pipeline through
`source_options["staleness_threshold_s"]`. This is *not* a read interval. OPC UA publishes on
change, so a genuinely static sensor sends nothing and too low a number restarts healthy
pipelines in a loop; too high and a dead session goes unnoticed. The probe's
`initialDelaySeconds` is deliberately larger than the threshold, or a slow source is killed
before it ever connects. The number still wants confirming against a real plant — see
`BACKLOG.md`.

**Trap — `enableServiceLinks`.** Kubernetes injects legacy Docker-link environment variables
for every Service in the namespace, so a Service named `clickhouse` sets `CLICKHOUSE_PORT` to
`tcp://10.96.x.x:8123` and clobbers the application's own setting. Every pod spec here sets
`enableServiceLinks: false`; we address services by DNS name and want none of those vars.
This bites in any namespace, not just kind.

**The Compose path has no probe.** `DockerRuntime` stays supported for local dev, but nothing
there fails a health check and recycles a stalled producer, so §3.1's gap is closed under
Kubernetes only. Registry #7 stays a deliberate xfail for exactly that reason.

### 3.5 Monitoring

Three layers, not one:
- **Is it flowing** — Kafka UI + Prometheus (lag, throughput, rows landed)
- **How stale** — OpenTelemetry tracing, source read → Kafka → ClickHouse insert. Lag alone
  is measured in messages, not seconds, and misses a stalled source that shows zero lag while
  data is an hour old.
- **What the customer sees** — a per-user health API on our own surface, not Grafana, which
  holds data across every user's pipelines and can never be shown to one user directly.

---

## 4. Config contract

- `PipelineConfig` must include `user_id`, `collection_number`, and `table_name` — these are
  not derived at insert time, they come from the pipeline's own config, exactly as normal
  (batch) pipelines already do.
- **ClickHouse table identity — `ch_unique_identifier`.** Every table, live or batch, is
  named:

  ```
  user_<user_id>_collection_<collection_number>_<table_name>
  ```

  e.g. `user_1_collection_22_aveva_iot`. The live path generates this with its own function,
  `common/app_common/ch_naming.py` — every component calls it rather than rebuilding the
  string. The batch path builds the same shape independently (inline, in another repo), so
  the **format** is a cross-repo contract even though the code is not shared: both must name
  the same table for the same inputs. `tests/phase2/unit/test_ch_naming.py` pins it.
- Kafka topic naming (`pipeline.<id>.events`, matched by `pipeline.*.events`) is a separate,
  internal identifier — it is **not** the ClickHouse table name. The consumer pool resolves
  topic → `PipelineConfig` → `ch_unique_identifier` to know which table to write to.

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
