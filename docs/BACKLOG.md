# Backlog — Live Streaming Module

Prioritized, each item names the component it affects. Move an item to "Resolved" with a
one-line pointer to the commit/PR that closed it — don't delete history from this file.

---

## Blocking — needed before or during the phase named

| Item | Blocks | Notes |
|---|---|---|
| Liveness probe thresholds | Phase 3 (orchestration) | **The only unresolved blocking item.** Needs an actual staleness number per source type (OPC UA now; AVEVA/Modbus later), not just the mechanism. Cannot be picked from first principles — it depends on each source's real publishing interval, so it needs a measurement from a live source, not a guess. |

## Follow-on work — not blocking, sequenced after the core build

| Item | Depends on | Notes |
|---|---|---|
| AVEVA connector | Phase 1 pattern | Follows the same thin-connector-under-execd pattern as OPC UA. Protocol/API specifics (Historian vs. PI System vs. System Platform) need confirming before scoping. |
| MQTT source | Phase 1 pattern | Likely needs zero custom connector code — Telegraf ships a native MQTT input. Confirm before assuming a custom connector is required. |
| Per-tenant health API | Phase 4 (monitoring) | Design only in Phase 4; a product-facing surface, not a Grafana dashboard, since Grafana holds cross-tenant data. |
| OpenTelemetry tracing (full) | Phase 4 (monitoring) | Interim cheap version (source_ts stamping + Grafana chart) can ship first if full tracing slips. |

## Open questions — unresolved, no phase assigned yet

| Item | Why it matters |
|---|---|
| Per-record lineage / compliance requirement | If required, Apache NiFi's data provenance is the strongest open-source option — but adopting NiFi as the data plane is a bigger architectural swing than anything currently planned. Settle this before it forces a redesign mid-build. |
| Docker Compose as a long-term supported deployment mode | Current assumption: yes, keep `DockerRuntime` behind `RuntimeAdapter` alongside `KubernetesRuntime`. Revisit if the cost of maintaining two runtimes outweighs the value. |
| ClickHouse database-per-tenant vs. flat table naming | `user_1_collection_12.sensor_data` (a database per tenant) would give GRANT-based isolation and make tenant deletion a `DROP DATABASE` instead of a prefix scan. The flat scheme is the adopted decision (Proposal F); this is recorded because it is cheap to note now and expensive to retrofit once tables exist. |
| Tenant isolation *enforcement* | Proposal F settles tenancy **naming**, not enforcement. A name prefix does not stop a query reading another tenant's table. Row policies, per-tenant ClickHouse users, or separate databases are the real options. Blocks the Phase 4 per-tenant health API, not Phase 2. |
| `last_heartbeat_at` and `status=running` now reflect the consumer only | Phase 1 removed the producer's Redis writes with the rest of its plumbing. Both fields are still written — by the consumer — so nothing flaps and the watchdog does not misfire, but neither says anything about the producer any more. The controller only ever sets `starting` and `stopped`. A dead producer is caught solely by the watchdog's container-stopped check. Phase 3 resolves this properly (Kubernetes owns liveness, the watchdog is deleted); until then, do not read either field as producer health. |
| Two overlapping Compose files | The root `docker-compose.yml` owns the infrastructure and the network; `services/task_manager/docker-compose.yml` owns API + controller + watchdog and now joins that network as external. Neither runs the full system alone. Phase 3 deletes controller and watchdog — decide then whether the sub-stack folds into the root file or stays. |

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
| P0 clean-checkout blockers (stale build path, missing `clickhouse/init.sql`, hardcoded `/home/usama/Videos` mount, dead `172.22.0.1` pins, network name mismatch, no `.env.example` for the task_manager stack) | Phase 0 — verified by `tests/phase0/integration/test_clean_checkout.py` |
| `bitnami/kafka:latest` no longer resolves (Bitnami retired those tags to `bitnamilegacy/`) | Phase 0 — root Compose pinned to `apache/kafka:3.9.1`. Found by the Phase 0 smoke test, not on the original checklist. |
| Dead-letter design spec | Decided 2026-09-21 — `DECISION-live-pipeline-simplification.md` Proposal E (reasoning), `ARCHITECTURE.md` §3.3.1 (contract). Unblocks the Phase 2 consumer loop. |
| `tenant_id` schema design | Decided 2026-09-21 — Proposal F: `user_<user_id>_collection_<collection_number>_<table_name>`, with `tenant_id` also an explicit column. |
| Telegraf MIT license sign-off | Confirmed 2026-09-21 (Kamran). Attribution obligation written up per distribution form in Proposal A. `THIRD-PARTY-NOTICES.md` shipped in Phase 1 and copied into the producer image — upstream's telegraf image carries no LICENSE file of its own, so this is the only notice in it. |
| Sequencing vs. any future platform merge | Decided 2026-09-21 — Proposal G: standalone through Phase 4, then a single merge PR into Stratahub. |
