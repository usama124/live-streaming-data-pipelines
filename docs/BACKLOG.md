# Backlog — Live Streaming Module

Prioritized, each item names the component it affects. Move an item to "Resolved" with a
one-line pointer to the commit/PR that closed it — don't delete history from this file.

---

## Blocking — needed before or during the phase named

| Item | Blocks | Notes |
|---|---|---|
| I13 — node-failure reschedule, on a real multi-node cluster | Rollout | **The one capability that is argued but not demonstrated.** Host-failure recovery is the headline gain of the Kubernetes move, and nothing has shown it happening: the kind cluster used for Phase 3 is single-node, so a cordon/drain there proves nothing. Needs a multi-node staging cluster — drain or kill a node carrying a producer pod, confirm it is rescheduled onto a healthy one. Until then treat host-failure recovery as expected, not verified. |
| 60s liveness threshold — validate against a real source | Rollout | The mechanism is built and tested (registry #19, I11); the number is a desk estimate. OPC UA publishes on *change*, so the risk is asymmetric: too low and pipelines whose sensors are legitimately static get restarted in a loop, which is worse than the stall it guards against. Measure the real inter-notification gap on a live source before rollout, and set a per-source number for AVEVA/Modbus when they land. |
| Telegraf MIT license sign-off | Phase 1 (producer) | No known blocker — needs a formal confirmation from legal, not a technical decision. |

## Follow-on work — not blocking, sequenced after the core build

| Item | Depends on | Notes |
|---|---|---|
| Delete `controller.py`, `watchdog.py` and the leader lock | Staging proof | Phase 3 made them redundant — the API drives the runtime directly and Kubernetes reconciles — but the plan gates deletion on staging, which has not run. They stay wired into the Compose stack, where the controller is still what starts containers. |
| No liveness probe on the Compose path | — | `DockerRuntime` has nothing that fails a health check and recycles a stalled producer, so registry #7 stays xfail there. Fine for local dev; not a sign Phase 3 is incomplete. |
| AVEVA connector | Phase 1 pattern | Follows the same thin-connector-under-execd pattern as OPC UA. Protocol/API specifics (Historian vs. PI System vs. System Platform) need confirming before scoping. |
| MQTT source | Phase 1 pattern | Likely needs zero custom connector code — Telegraf ships a native MQTT input. Confirm before assuming a custom connector is required. |
| Per-user health API | Phase 4 (monitoring) | Design only in Phase 4; a product-facing surface, not a Grafana dashboard, since Grafana holds data across every user's pipelines. |
| OpenTelemetry tracing (full) | Phase 4 (monitoring) | Interim cheap version (source_ts stamping + Grafana chart) can ship first if full tracing slips. |

## Open questions — unresolved, no phase assigned yet

| Item | Why it matters |
|---|---|
| Per-record lineage / compliance requirement | If required, Apache NiFi's data provenance is the strongest open-source option — but adopting NiFi as the data plane is a bigger architectural swing than anything currently planned. Settle this before it forces a redesign mid-build. |
| Sequencing vs. any future platform merge | Affects whether this ships as a standalone release or a merge PR. Not yet decided. |
| `ch_unique_identifier` format can drift from batch, one-directionally | Decided 2026-09-21: the live path owns `ch_naming.py` rather than importing from `dataavalanche-be` — there is no function there to import (21 inline copies), the repo may not be modified from here, and importing its backend package for a string format would drag its dependency tree into the consumer pool. **The residual risk is real and asymmetric:** `tests/phase2/unit/test_ch_naming.py` pins our composition against batch's, so our side cannot drift unnoticed, but it does not read batch's code — if batch changes format, our tests still pass and the two silently name different tables. Batch does no case folding and uses a UUID user id on some paths. Cheap fix, needs a change in the other repo and TL approval: a shared `(user_id, collection_number, table_name) -> expected name` fixture checked into both, so whichever side changes the format breaks its own test. |
| Docker Compose as a long-term supported deployment mode | Current assumption: yes, keep `DockerRuntime` behind `RuntimeAdapter` alongside `KubernetesRuntime`. Revisit if the cost of maintaining two runtimes outweighs the value. |

## Explicitly rejected — do not re-litigate without new information

See `DECISION-live-pipeline-simplification.md`'s "Rejected, with reasons" table for the full
list and reasoning (Airflow for live pipelines, ClickHouse Kafka table engine, Kafka Connect,
Flink/Spark, Conduit/Benthos, Airbyte/Meltano, ArgoCD, custom CRD+operator, KEDA, Redpanda
Connect as the producer runtime). If new information changes one of these — a library
matures, a constraint changes — reopen it explicitly in a new decision memo entry rather than
silently reversing course in code.

## Resolved

| Item | Closed by |
|---|---|
| Dead-letter design spec | Phase 2 — `ARCHITECTURE.md` §3.3.1 and `services/consumer_pool/app/sink.py`. Per-record isolation, shared `dead_letter_events` table, `dlq_rows_total{pipeline_id}`, no automatic replay. Registry #11 and #11b cover it. |
| `ch_unique_identifier` / table identity | Phase 2 — `common/app_common/ch_naming.py`. Decided 2026-09-21: the live path owns its own function; batch builds the same shape inline in another repo, so the *format* is the contract and `tests/phase2/unit/test_ch_naming.py` pins it. |

- **ClickHouse table naming / "tenant_id" schema design** — there is no separate `tenant_id`
  concept in this system. Ownership is `user_<user_id>_collection_<collection_number>_<table_name>`
  (e.g. `user_1_collection_22_aveva_iot`). The live path generates it with its own function,
  `common/app_common/ch_naming.py`; batch builds the same shape independently in another repo.
  See `ARCHITECTURE.md` §4.
