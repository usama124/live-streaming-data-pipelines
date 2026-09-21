---
name: stratahub-live-streaming
description: Invariants, traps, and doc index for the Stratahub live-streaming module — the pipeline path where sources (OPC UA, AVEVA, MQTT) stream through Telegraf, Kafka, a shared aiokafka consumer pool, and per-pipeline ClickHouse tables, orchestrated by Kubernetes. Use this skill for ANY work in this repo: writing or editing the producer/connector code, the consumer pool, task_manager, Kubernetes manifests, or monitoring — even if the request looks like a small, self-contained change. Also use it before answering questions about why this architecture looks the way it does, whether to reach for Airflow/Redis/Flink/a custom container-per-pipeline pattern, or whether an idea has already been considered and rejected.
---

# Stratahub Live Streaming — Project Skill

Read this before touching any code in this repo. It exists to stop you from re-deriving (or
worse, silently reversing) decisions that were already made for specific, documented reasons.

## Doc index — read the relevant one before editing that area

| Doc | Read it when |
|---|---|
| `docs/ARCHITECTURE.md` | Touching the producer, consumer pool, orchestration, or monitoring |
| `docs/DECISION-live-pipeline-simplification.md` | Unsure *why* something is built a certain way, or tempted to reach for a rejected alternative |
| `docs/BACKLOG.md` | Before treating something as a new discovery — it may already be tracked |
| `docs/ONBOARDING.md` | Setting up locally, debugging, or adding a new source connector |
| `docs/live-streaming-implementation-plan.md` | Checking what phase is active and what's done |

## Invariants — never violate these

- **NEVER run any command against the real EKS cluster (`da-eks-cluster-dev` or any
  client-managed cluster) — no exceptions, no "just checking," no read-only assumptions.**
  This is the client's live environment. Any action there requires the user's TL's explicit
  approval first, obtained outside this session, every time — not inferred, not assumed from
  a past approval. Before running *any* command that touches a Kubernetes cluster
  (`kubectl`, Helm, cluster-scoped tooling), first check the active context
  (`kubectl config current-context`) and confirm it points at the local dev cluster (`kind`
  or equivalent), not `da-eks-cluster-dev` or anything resembling a shared/client context. If
  the context is wrong, or you're unsure which cluster a command would hit, **stop and ask —
  do not proceed and do not guess.** All Kubernetes-based development and testing in this
  repo happens on a local `kind` cluster for exactly this reason.
- **Never route live-pipeline data through Airflow.** Airflow orchestrates runs that end;
  live pipelines never finish. This was evaluated and rejected — don't reopen it in code.
- **Never let an exception escape the consumer pool's write path.** One bad record must not
  stall every other pipeline sharing the pool. Dead-letter handling catches it; there is no
  acceptable "let it crash" path here.
- **Never infer a ClickHouse table's schema from the first event seen.** Create tables
  explicitly, from a declared schema, at pipeline-creation time. Inferring from the first
  event is exactly how two pipelines with different fields collide.
- **Never restart the shared consumer pool on pipeline creation.** Customers create
  pipelines at times outside your control — this is a hard SaaS constraint, not a
  performance preference.
- **Insert before commit, always, in the consumer loop.** Never the reverse — a crash
  mid-batch must re-read, not drop, rows.
- **Never treat a Telegraf-based producer as done without the Kubernetes liveness probe.**
  Telegraf restarts a subprocess only on exit, not on a silent hang (e.g., a dead OPC UA
  session with the process still alive). The liveness probe is the only thing that catches
  that case — the two ship together or not at all.
- **Never touch batch/normal Airflow pipelines.** Out of scope, full stop — not even for
  code reuse.
- **Never reintroduce Redis as part of the lifecycle control plane.** The whole point of the
  Kubernetes move was to stop polling Redis for desired state.
- **Never delete `DockerRuntime` / Compose support.** It stays behind `RuntimeAdapter`
  alongside `KubernetesRuntime` intentionally, for local dev.
- **There is no `tenant_id` field in this system — don't add one.** Ownership is expressed
  through `user_id` + `collection_number`, folded into `ch_unique_identifier`:
  `user_<user_id>_collection_<collection_number>_<table_name>` (e.g.
  `user_1_collection_22_aveva_iot`). The live path generates this with **its own function**,
  `common/app_common/ch_naming.py` — call it, never rebuild the string inline. There is no
  shared function with batch to import; don't go looking for one (see the trap below).

## Traps — things that look right but aren't

- **There is no shared `ch_unique_identifier` function, and the format can drift.**
  `dataavalanche-be` builds the identifier as an inline f-string in 21 places across three
  files; nothing is importable, and this repo may not modify that path. So the live side owns
  `ch_naming.py`. Know the limit of the guarantee: `tests/phase2/unit/test_ch_naming.py`
  pins *our* composition against batch's, so **our** side cannot drift unnoticed — it does
  not read batch's code, so if batch changes its format, those tests still pass and the two
  silently name different tables. Batch does **no** case folding and passes a UUID user id on
  some paths; don't "tidy" either here. If you touch naming on either side, check the other.

- **`inputs.exec` vs `inputs.execd` in Telegraf.** `exec` runs a command periodically and
  captures full output each time. `execd` runs a long-lived subprocess and streams stdout.
  You want `execd` for any source connector that maintains an open connection. Using `exec`
  by mistake will look like it works in a quick test and then silently miss data.
- **Kafka pattern-subscription isn't instant.** A new topic is picked up within
  `metadata_max_age_ms` (partition/topic discovery interval), not the moment it's created.
  Don't write tests or docs implying synchronous pickup.
- **Don't reach for ClickHouse's Kafka table engine.** It was evaluated and rejected —
  each Kafka table consumes a slot in `background_message_broker_schedule_pool_size` and
  parses inside the DB server, competing with query CPU, and has no pause verb short of
  DETACH/ATTACH per node. Fine at ~10 pipelines, not at the scale this is built for.
- **Redpanda Connect is not a drop-in Telegraf substitute, licensing-wise.** Its binary
  ships under Redpanda's Business Source License, which restricts offering it as a
  commercial streaming service to others — a real question for a SaaS product. Telegraf's
  MIT license is why it was chosen. Don't swap runtimes without re-checking this.
- **Flink's official ClickHouse sink doesn't solve dead-letter handling or exactly-once "for
  free."** It offers at-least-once delivery and no DLQ support as of the last evaluation.
  If someone proposes adopting Flink to solve either problem, that premise is wrong — check
  `DECISION-live-pipeline-simplification.md`'s Flink section before agreeing to it.
- **Don't reimplement `ch_unique_identifier` generation for live pipelines.** Normal
  (batch) pipelines already compute
  `user_<user_id>_collection_<collection_number>_<table_name>` somewhere in the codebase.
  Find that logic and call it from the live path too — writing a second version invites the
  two paths to drift apart on edge cases (special characters, casing, id formatting).
- **A pipeline can look "up" while its data is stale.** Consumer lag measured in messages
  can be zero while a source-side connection is hung, not dead. Health checks based on
  process liveness or Kafka lag alone will miss this — see `ARCHITECTURE.md` §3.5.