# Onboarding — Live Streaming Module

## 1. Prerequisites

- Docker + Docker Compose (local dev)
- `kubectl` + access to a cluster (staging/prod; not required for local dev)
- Python 3.11+
- A running Kafka broker and ClickHouse instance (via Compose locally)

## 2. Local setup

```bash
git clone <this repo>
cd <repo>
cp .env.example .env
docker compose up
```

This brings up: Kafka, ClickHouse, the Task Manager API, and (once Phase 1/2 land) a
Telegraf-based producer and the shared consumer pool, all via `DockerRuntime`.

Kubernetes-backed local dev is not required — `DockerRuntime` stays a supported mode behind
`RuntimeAdapter` specifically so Compose keeps working for this.

## 3. Repo layout (target — confirm against actual tree as phases land)

```
common/app_common/runtime/
  base.py                 # RuntimeAdapter — 5-method interface
  docker_runtime.py        # Compose-based implementation
  kubernetes_runtime.py    # K8s-based implementation (Phase 3)
task_manager/               # FastAPI: pipeline CRUD, calls RuntimeAdapter
connectors/
  opcua/                    # thin execd-compatible source script
telegraf/templates/         # per-pipeline Telegraf config generation
consumer_pool/               # aiokafka shared consumer
k8s/                          # Deployment manifests (producer, consumer pool)
monitoring/
  kafka-ui/
  prometheus/
  otel/
docs/                        # you are here
```

## 4. Debugging playbook

**A pipeline won't start:**
1. Check the Task Manager API response — under the target design it returns the error
   synchronously (no more "check back after the next poll").
2. If using `DockerRuntime`: `docker ps` for the pipeline's container, `docker logs`.
3. If using `KubernetesRuntime`: `kubectl get pods -l pipeline-id=<id>`, then
   `kubectl describe pod` / `kubectl logs`.

**A pipeline is running but no data is landing:**
1. Kafka UI — confirm the topic `pipeline.<id>.events` has messages.
2. If the topic is empty: the connector isn't emitting. Check the Telegraf pod's logs, not
   just the connector script in isolation — Telegraf's own log shows subprocess exit/restart
   events.
3. If the topic has messages but ClickHouse doesn't: check the consumer pool's logs for
   dead-letter entries. **Never assume a silent failure is unrelated to dead-letter
   handling** — that's exactly the case it exists to catch.

**A pipeline looks "up" but data is stale (not the same as "not running"):**
1. This is the case plain health checks don't catch — see `ARCHITECTURE.md` §3.5. Lag can be
   zero while data is genuinely old if the source-side connection is hung, not dead.
2. Check the OpenTelemetry trace (or the interim `now() - max(source_ts)` chart if full
   tracing hasn't landed yet) before assuming the pipeline is healthy just because Kafka
   shows no lag.

## 5. Adding a new source connector

Follow this recipe for any new source (AVEVA, Modbus, a future protocol):

1. **Check if Telegraf already has a native input for the protocol** (MQTT does — check
   before writing custom code). If so, skip to step 5.
2. If not, write a thin script whose only job is: connect to the source, emit one record
   per line to stdout, in the format Telegraf's `data_format` config expects (Influx line
   protocol or JSON).
3. Do not add Kafka publishing, retry logic, or heartbeat code to this script — that's
   Telegraf's job under `inputs.execd`. If you find yourself writing that, stop — you're
   duplicating what the runtime already provides.
4. Add a Telegraf config template for the new source under `telegraf/templates/`.
5. Add a liveness-probe threshold for the new source type in the Kubernetes Deployment
   template — do not ship a new source without one; see `ARCHITECTURE.md` §3.1's known gap.
6. Update `docs/BACKLOG.md` if the new source surfaces any open questions of its own.

## 6. Conventions

- Config lives in `PipelineConfig`, not scattered environment variables per component.
- Topic naming: `pipeline.<id>.events`. Table naming: one ClickHouse table per pipeline,
  created explicitly from a declared schema — never inferred from the first event.
- Every new component that could raise mid-write (especially in the consumer pool) needs
  its exception path to route to dead-letter handling, not to propagate and stall the shared
  pool.
- Anything that changes a decision recorded in `DECISION-live-pipeline-simplification.md`
  needs an explicit note there, not a silent deviation in code.
