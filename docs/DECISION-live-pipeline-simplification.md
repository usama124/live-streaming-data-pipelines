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

**License note:** Telegraf is MIT — no resale restriction. The alternative runtime,
Redpanda Connect, ships its binary under Redpanda's Business Source License, which restricts
offering it as a commercial streaming service to others. Given the SaaS direction, that's a
real legal question Telegraf avoids entirely. This is why Telegraf is the recommendation,
not Redpanda Connect.

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

## Proposal D — Monitoring: three layers, sequenced

**Decision: adopted, sequenced after the consumer pool ships.**

- **Is it flowing** — Kafka UI + Prometheus. Lag, throughput, rows landed.
- **How stale** — OpenTelemetry tracing, source read → Kafka → ClickHouse insert. Lag alone
  is measured in messages, not seconds, and misses a stalled source showing zero lag while
  data is an hour old. Cheap interim: stamp each event with its source-read timestamp, chart
  `now() - max(source_ts)` per table.
- **What the customer sees** — a per-user health API on our own surface. Grafana holds data
  across every user's pipelines and can never be shown to one user directly.

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

## Open, unresolved at time of writing

See `BACKLOG.md` for the live list. Notable ones that affect architecture decisions already
made above:

- ~~**`tenant_id`** is not yet designed into the schema~~ — **Resolved.** No separate
  `tenant_id` concept exists in this system. Ownership is the `ch_unique_identifier` scheme
  `user_<user_id>_collection_<collection_number>_<table_name>` (e.g.
  `user_1_collection_22_aveva_iot`), which batch already uses. **The live path generates it
  with its own function** (`common/app_common/ch_naming.py`), decided 2026-09-21: there is no
  batch function to import (21 inline copies in another repo), that repo may not be modified
  from here, and importing its backend package for a string format would drag its dependency
  tree into the consumer pool and let a batch-side change silently rename live's tables. The
  format is therefore a cross-repo contract; the one-directional drift risk that leaves is
  logged in `BACKLOG.md`. See `ARCHITECTURE.md` §4.
- **Per-record lineage / compliance requirement** — if customers require it, Apache NiFi's
  data provenance is the strongest open-source option, but adopting NiFi as the data plane is
  a bigger architectural swing than anything above and should be settled independently.
- **Sequencing vs. any future platform merge** — affects whether this ships as a standalone
  release or a merge PR; not yet decided.
