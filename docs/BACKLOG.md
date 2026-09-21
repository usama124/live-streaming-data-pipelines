# Backlog — Live Streaming Module

Prioritized, each item names the component it affects. Move an item to "Resolved" with a
one-line pointer to the commit/PR that closed it — don't delete history from this file.

---

## Blocking — needed before or during the phase named

| Item | Blocks | Notes |
|---|---|---|
| Dead-letter design spec | Phase 2 (consumer pool) | No exception may escape the write path. Insert-then-commit ordering must be finalized before the consumer loop merges, not retrofitted after. |
| Liveness probe thresholds | Phase 3 (orchestration) | Needs an actual staleness number per source type (OPC UA now; AVEVA/Modbus later), not just the mechanism. |
| Telegraf MIT license sign-off | Phase 1 (producer) | No known blocker — needs a formal confirmation from legal, not a technical decision. |

## Follow-on work — not blocking, sequenced after the core build

| Item | Depends on | Notes |
|---|---|---|
| AVEVA connector | Phase 1 pattern | Follows the same thin-connector-under-execd pattern as OPC UA. Protocol/API specifics (Historian vs. PI System vs. System Platform) need confirming before scoping. |
| MQTT source | Phase 1 pattern | Likely needs zero custom connector code — Telegraf ships a native MQTT input. Confirm before assuming a custom connector is required. |
| Per-user health API | Phase 4 (monitoring) | Design only in Phase 4; a product-facing surface, not a Grafana dashboard, since Grafana holds data across every user's pipelines. |
| OpenTelemetry tracing (full) | Phase 4 (monitoring) | Interim cheap version (source_ts stamping + Grafana chart) can ship first if full tracing slips. |

## Open questions — unresolved, no phase assigned yet

| Item | Why it matters |
|---|---|
| Per-record lineage / compliance requirement | If required, Apache NiFi's data provenance is the strongest open-source option — but adopting NiFi as the data plane is a bigger architectural swing than anything currently planned. Settle this before it forces a redesign mid-build. |
| Sequencing vs. any future platform merge | Affects whether this ships as a standalone release or a merge PR. Not yet decided. |
| Docker Compose as a long-term supported deployment mode | Current assumption: yes, keep `DockerRuntime` behind `RuntimeAdapter` alongside `KubernetesRuntime`. Revisit if the cost of maintaining two runtimes outweighs the value. |

## Explicitly rejected — do not re-litigate without new information

See `DECISION-live-pipeline-simplification.md`'s "Rejected, with reasons" table for the full
list and reasoning (Airflow for live pipelines, ClickHouse Kafka table engine, Kafka Connect,
Flink/Spark, Conduit/Benthos, Airbyte/Meltano, ArgoCD, custom CRD+operator, KEDA, Redpanda
Connect as the producer runtime). If new information changes one of these — a library
matures, a constraint changes — reopen it explicitly in a new decision memo entry rather than
silently reversing course in code.

## Resolved

- **ClickHouse table naming / "tenant_id" schema design** — there is no separate `tenant_id`
  concept in this system. Live pipelines reuse the exact `ch_unique_identifier` scheme
  normal pipelines already use: `user_<user_id>_collection_<collection_number>_<table_name>`
  (e.g. `user_1_collection_22_aveva_iot`), via the same shared naming logic both paths call.
  See `ARCHITECTURE.md` §4 and `DECISION-live-pipeline-simplification.md`.
