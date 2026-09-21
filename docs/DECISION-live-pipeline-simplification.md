# Decision Memo — Live Pipeline Architecture

**Audience:** whoever picks this repo up next, including a future Claude Code session.
**Status:** decided, sequencing in progress. See `live-streaming-implementation-plan.md`
for build status against these decisions.

---

## Constraints that decide the answers

1. **SaaS — customers create pipelines at times we do not control.** No design may restart
   shared infrastructure on pipeline creation.
2. **The platform is already Kubernetes-based**, so a cluster is not new platform cost.
3. **The team is Python.** A JVM cluster is a real operational cost, not a footnote.
4. **Per-pipeline topics and tables must stay separately queryable and separately stoppable.**

---

## Proposal A — Producer: Telegraf, not a custom container

**Decision: adopted.**

A custom producer container per pipeline means ~320 lines of plumbing (Kafka publish,
config, heartbeat, polling loop) rewritten for every new source. Only the source-specific
logic is actually ours to own.

Telegraf's `inputs.execd` runs our source connector as a subprocess and owns the rest:
batching, Kafka delivery, retry/backoff, Prometheus metrics. Standard protocols (MQTT) use
Telegraf's native input directly, no custom connector needed.

**License note — confirmed 2026-09-21 (Kamran).** Telegraf is MIT: no resale restriction,
no copyleft, no per-seat terms. The alternative runtime, Redpanda Connect, ships its binary
under Redpanda's Business Source License, which restricts offering it as a commercial
streaming service to others. Given the SaaS direction, that's a real legal question Telegraf
avoids entirely. This is why Telegraf is the recommendation, not Redpanda Connect.

The one obligation MIT does impose: *the copyright notice and permission notice must be
included in any substantial copy or distribution of the software.* What that means here,
concretely:

| What we do | Obligation triggered? | Action |
|---|---|---|
| Run unmodified upstream Telegraf in our own SaaS infrastructure | No — running is not distributing | None |
| Build a derived image (`FROM telegraf` + our connector script) | Yes, if that image leaves our infrastructure | Keep the upstream `LICENSE` file in the image — do not strip it during a multi-stage build |
| Ship an image, chart, or on-prem bundle to a customer | Yes | Ship `THIRD-PARTY-NOTICES.md` alongside it |
| Vendor Telegraf source or a self-built binary | Yes | Include the MIT text and InfluxData's copyright line |

The failure mode to avoid is a multi-stage Dockerfile that copies the Telegraf binary out of
the upstream image and leaves `LICENSE` behind. Phase 1 carries the task of shipping
`THIRD-PARTY-NOTICES.md` with the producer image.

**Known gap:** Telegraf detects a dead subprocess, not a hung-but-alive one (e.g., an OPC UA
session that silently drops). This is covered by Proposal B's liveness probe, not by
Telegraf — treat the two as a package, not independent wins.

---

## Proposal B — Consumer: one shared pool, not per-pipeline containers

**Decision: adopted.**

One consumer deployment, a few replicas, subscribed to a topic *pattern*
(`pipeline.*.events`) via aiokafka — not a fixed list. Topic name selects the destination
ClickHouse table, giving real per-pipeline tables for the first time.

*Creating a pipeline:* the API writes config, creates the Kafka topic and the ClickHouse
table, starts only the producer. The pool picks up the new topic within
`metadata_max_age_ms`; nothing registers with it, nothing restarts it. This is mandatory —
see constraint 1.

*Tradeoffs accepted knowingly:* fault isolation is lost, so dead-letter handling must live
**inside** the write path and never let an exception escape; offset-commit ordering becomes
ours (insert first, then commit); each new topic briefly rebalances the group.

---

## Proposal C — Orchestration: Kubernetes Deployments

**Decision: adopted.**

`desired_state` ≡ replica count; the reconcile loop ≡ `kube-controller-manager`; a crashed
container restarting ≡ `restartPolicy: Always`. No separate controller, watchdog, or leader
lock — Kubernetes already does all three.

*One gap to port, not several:* a liveness probe for the hung-but-not-crashed case (HTTP
endpoint failing when the last successful source read exceeds a threshold) — this is what
covers Proposal A's Telegraf gap, and it's the one piece with no automatic Kubernetes
equivalent.

*Gains beyond deletion:* host failure is handled for the first time; a failed start returns
an error to the API caller instead of surfacing on a later poll; Redis stops being a control
plane.

---

## Proposal E — Dead-letter handling: classify first, one shared table

**Decision: adopted, 2026-09-21.** This is the spec Proposal B and `BACKLOG.md` require to
exist *before* the Phase 2 consumer loop merges. The contract itself — table DDL,
classification rules, write sequence — is in `ARCHITECTURE.md` §3.3.

The shared pool trades fault isolation for cardinality (Proposal B), so the write path is
the only thing standing between one bad record and every other pipeline on the pool. That
makes the design question sharper than "add a DLQ": **what, exactly, is allowed to be
dead-lettered?**

### The distinction the design turns on

Two failures arrive at the same `except` clause and must not be handled the same way:

| | Poison record | Infrastructure failure |
|---|---|---|
| Example | Missing declared column, unparseable timestamp, type mismatch | ClickHouse down, network timeout, `TOO_MANY_PARTS`, memory limit |
| Will a retry ever succeed? | No — deterministic | Yes, usually within seconds |
| Right response | Dead-letter it, keep going | Retry with backoff; **never** dead-letter |
| Cost of getting it wrong | Retry forever → head-of-line block for that pipeline | A ClickHouse outage silently drains the entire stream into the DLQ |

The second error is the expensive one to get wrong, and it is the one a naive
`try/except: send_to_dlq()` gets wrong every time. So the rule is:

> **Dead-letter only on a positively recognised data error. Anything unrecognised is treated
> as transient and retried.** After the retry budget is exhausted, an unrecognised error is
> dead-lettered with `error_class = 'unclassified'` — visibly, not silently.

This fails safe in both directions: a novel error is never silently discarded, and no error
blocks a pipeline forever.

### Where dead letters go: one shared ClickHouse table

**Adopted:** a single `data_platform.dead_letters` table, `pipeline_id` and `tenant_id` as
columns, `ORDER BY (tenant_id, pipeline_id, failed_at)`.

- ClickHouse is already in the path, already has a connection manager with retry
  (`clickhouse_manager.py`), and is already where anyone debugging a pipeline is looking.
  Zero new infrastructure.
- Shared rather than per-pipeline because the DLQ schema is *fixed*. Per-pipeline tables
  exist because pipeline schemas differ (§3.3) — that reason does not apply here, and a
  second table per pipeline doubles table count for data nobody queries in the hot path.
- The apparent objection — "if ClickHouse is broken, the DLQ write fails too" — dissolves
  under the classification rule: we only dead-letter data errors, which means ClickHouse is
  up and answering. A failing DLQ insert is itself an infrastructure error, so it takes the
  retry path and the offsets stay uncommitted. The two rules compose.

**Rejected:**

| Option | Why not |
|---|---|
| A Kafka DLQ topic (`pipeline.<id>.dlq`) | A topic is only useful if something consumes it, and nothing would. It adds a component with no owner, plus topic-count pressure per pipeline. The source topic's own retention already covers "don't lose it" for the retention window; the table covers it beyond that, because it stores the raw payload. |
| A dead-letter table per pipeline | Doubles table count to hold a fixed schema. `pipeline_id` as a column with a matching sort key gives the same query performance. |
| Log the error and skip the record | Silent data loss, and the invariant explicitly forbids it. A log line is not a record you can replay. |
| Retry everything forever | Head-of-line blocking — exactly the "one bad record stalls every pipeline" failure the shared pool must not have. |
| Pause the whole pool on error | Punishes every tenant for one tenant's bad data. Pausing is scoped to the affected topic's partitions only. |

### Batches: isolate the poison, don't dead-letter its neighbours

An insert is a batch (default 500 rows); one bad row fails all of them. Dead-lettering the
whole batch would discard ~499 good records.

**Adopted: bisect on a data-class error.** Split the failing batch in half, retry each half,
recurse; below 10 rows, insert row by row. One poison record costs ~log₂(500) ≈ 9 extra
inserts instead of 500.

Row-by-row on the whole batch is fewer lines, and was rejected for a specific reason rather
than taste: 500 single-row inserts is how you provoke `TOO_MANY_PARTS` in ClickHouse — the
naive recovery path would manufacture an infrastructure failure out of a data failure.

### What is deliberately not built

- **No automatic replay.** Replay is a documented `SELECT payload FROM dead_letters WHERE
  pipeline_id = ...` plus a re-produce script, run deliberately after the schema or source is
  fixed. An automatic replayer re-feeds the same poison into the same table on a loop.
- **No per-pipeline circuit breaker.** A dead-letter *rate* is a monitoring concern, not a
  new control path: the pool emits `dlq_rows_total{pipeline_id, tenant_id, error_class}` and
  Phase 4 alerts on it. A sustained rate means the declared table schema and the source
  disagree — a human decision, not an automatic one.
- **No DLQ for records the connector never emitted.** A source that stops producing is the
  staleness problem (§3.5 / the liveness probe), not a dead-letter problem. They look similar
  on a dashboard and have nothing to do with each other.

---

## Proposal D — Monitoring: three layers, sequenced

**Decision: adopted, sequenced after the consumer pool ships.**

- **Is it flowing** — Kafka UI + Prometheus. Lag, throughput, rows landed.
- **How stale** — OpenTelemetry tracing, source read → Kafka → ClickHouse insert. Lag alone
  is measured in messages, not seconds, and misses a stalled source showing zero lag while
  data is an hour old. Cheap interim: stamp each event with its source-read timestamp, chart
  `now() - max(source_ts)` per table.
- **What the customer sees** — a per-tenant health API on our own surface. Grafana holds
  cross-tenant data and can never be shown to a tenant directly.

Sequenced after the consumer pool so dashboards aren't built against infrastructure that's
mid-migration.

---

## Rejected, with reasons

| Option | Why not |
|---|---|
| Airflow for live pipelines | Orchestrates runs that end; a never-ending task breaks scheduler heartbeats, slot accounting, zombie detection, retries. |
| ClickHouse Kafka table engine | Each Kafka table takes a slot in `background_message_broker_schedule_pool_size` and parses inside the DB server, competing with query CPU. No pause verb — stopping one pipeline means DETACH/ATTACH per node. Fine at ~10 pipelines, not 200. |
| Keeping a stream framework with an explicit topic list (e.g. Quix Streams) | Would require restarting the shared consumer on every pipeline creation — unacceptable given constraint 1. |
| Kafka Connect distributed mode | The ClickHouse sink side is genuinely production-grade, but the only OPC UA source is build-from-snapshot with unfinished docs. Connect only pays for its JVM cluster if used on both sides; the source side isn't dependable enough to justify it. |
| Apache Flink / Spark Structured Streaming | See the dedicated section below — evaluated in more depth after the initial rejection. |
| Conduit / Benthos as a full replacement | Same rewrite cost as Flink/Spark; no OPC UA input. |
| Airbyte / Meltano for live | Batch and CDC oriented, not built for never-ending streams. |
| ArgoCD as control UI | Git-driven; our Deployments are created by the API at runtime, not from a git repo. |
| Custom CRD + operator | More code than Proposal C, which just creates Deployments directly. Revisit only if `kubectl get livepipelines` becomes a real requirement. |
| KEDA for lifecycle | Load-driven; start/stop here is user intent, not load. May be useful later for sizing the consumer pool specifically. |
| Redpanda Connect (as the producer runtime) | See Proposal A's license note — BSL resale restriction is a real question for a SaaS product; Telegraf's MIT license avoids it entirely. |

---

## Alternative considered in depth — Apache Flink

**Decision: not adopted now. Revisit if the roadmap changes.**

Flink would only replace the consumer pool — it has no OPC UA/AVEVA/MQTT source connectors,
so the producer side (Telegraf) is unaffected either way.

| Aspect | Consumer pool (adopted) | With Flink |
|---|---|---|
| Topic subscription | Pattern subscribe + `getmany` loop, dynamic discovery | Same capability — Flink's Kafka source supports topic-pattern subscription with periodic partition discovery |
| Batching & insert | ~40 lines, hand-written | A DataStream job plus the official ClickHouse sink connector, built on `AsyncSinkBase` |
| Delivery guarantee | At-least-once, manual | Also at-least-once only — the official connector's exactly-once support is still being built, not shipped |
| Dead-letter handling | Must be built into the write path — open item either way | Still must be built by hand — the official sink has no DLQ support yet either |
| Orchestration | Plain Kubernetes Deployment + our liveness probe | Flink Kubernetes Operator — a separate CRD-based system, JobManager/TaskManager pods, checkpoint-based restart |
| Language | Python throughout | PyFlink DataStream API — workable, but UDFs bridge through the JVM, and the ecosystem still skews Java |

**Verdict:** the consumer's entire job is move records into ClickHouse with no
transformation. Flink is priced for windowed, stateful stream processing — adopting it now
means carrying a JVM streaming platform to do what an aiokafka pool already does, without
solving dead-letter handling or exactly-once delivery for free either. Revisit if the
roadmap adds real stream processing (rolling aggregates, anomaly detection, cross-pipeline
joins) that would justify the added runtime and the JVM dependency for a Python team.

---

## Net effect (target vs. the container-per-pipeline baseline this replaces)

| | Baseline | Target |
|---|---|---|
| Always-on services | 4 | 2 |
| Lines of code (reference implementation) | ~2,759 | ~2,050 |
| Containers/pods @ 50 pipelines | 104 | 55 |
| Containers/pods @ 500 pipelines | 1,004 | 505 |
| Per-pipeline ClickHouse tables | No — schema-collision risk | Yes |
| Host-failure recovery | No | Yes (Kubernetes) |
| Live data touches Airflow | Yes, in some prior designs | No, never |

The argument for this design is operational, not lines of code.

---

## Proposal F — Tenancy: identity in the name *and* in a column

**Decision: adopted, 2026-09-21 (Kamran).** Tenancy is `user_id` + collection number. A
pipeline's ClickHouse table is named:

```
user_<user_id>_collection_<collection_number>_<table_name>
```

so `user_1_collection_12_sensor_data`. The derived tenant identifier is
`tenant_id = "user_<user_id>_collection_<collection_number>"`.

**The table name is not the only place tenancy lives.** `tenant_id` is also an explicit
column on every row, on the shared dead-letter table, on every metric label, and on
`PipelineConfig`. Encoding it solely in the name would mean the shared DLQ, the Phase 4
per-tenant health API, and every metric would have to *parse tenancy back out of a table
name* — which breaks the first time a customer's `table_name` contains `_collection_`.
Deriving a name from an ID is safe; deriving an ID from a name is not. The column costs
nothing: it is constant per table, so it compresses to approximately zero.

*Constraints this naming scheme imposes, and which must hold:*

1. **`user_id` and `collection_number` must be immutable numeric IDs, never display names
   or handles.** A renameable identifier in a table name means either orphaned tables or a
   rename migration on every user edit.
2. **`table_name` is customer-influenced input and must be validated at pipeline-creation
   time** against `^[a-z_][a-z0-9_]*$`, lowercased, with the full composed name capped at
   128 characters. It reaches ClickHouse as an identifier — it is a trust boundary, not a
   formatting preference.
3. **Uniqueness is structural.** `(user_id, collection_number, table_name)` is unique by
   construction, so no collision handling is needed — and none should be added.

*Noted, not adopted:* a ClickHouse **database** per tenant (`user_1_collection_12.sensor_data`)
would give GRANT-based isolation and make tenant deletion a single `DROP DATABASE`, rather
than a prefix scan. The flat naming scheme above is the decision; the alternative is recorded
in `BACKLOG.md` because it is cheap to note now and expensive to retrofit later. Either way,
**a name prefix is a convention, not an isolation boundary** — it does not by itself stop a
query from reading another tenant's table. Real isolation (row policies, per-tenant users, or
separate databases) is a separate decision, and the Phase 4 health API must not rely on
naming alone to scope its answers.

---

## Proposal G — Sequencing: standalone now, merge into Stratahub after

**Decision: adopted, 2026-09-21 (Kamran).** This repo stays a standalone application through
Phases 0–4 and merges into Stratahub afterwards, as a single merge PR rather than
phase-by-phase integration.

*What this decides, in practice:*

- Phases ship as standalone repo releases. No phase is blocked on a Stratahub merge window.
- **Do not import from Stratahub internals**, and do not reshape this module's interfaces to
  anticipate them. The merge is a known, scheduled event; speculative coupling ahead of it is
  work that gets thrown away.
- The two divergent copies of `app_common` (`common/app_common/` and
  `services/task_manager/common/app_common/`) are a merge-time cost that grows. Keep them
  from diverging further; reconciling them once at merge is cheaper than three times.
- Proposal F's `user_id` and collection number must be **Stratahub's** identifiers, not
  identifiers minted here. That is what makes the merge a move rather than a data migration.

---

## Open, unresolved at time of writing

See `BACKLOG.md` for the live list. Notable ones that affect architecture decisions already
made above:

- **Per-record lineage / compliance requirement** — if customers require it, Apache NiFi's
  data provenance is the strongest open-source option, but adopting NiFi as the data plane is
  a bigger architectural swing than anything above and should be settled independently.
- **ClickHouse database-per-tenant vs. the adopted flat naming scheme** — see Proposal F.
- **Tenant isolation enforcement** (row policies / per-tenant users) — Proposal F settles
  *naming*, not *enforcement*. Blocks the Phase 4 per-tenant health API, not Phase 2.
