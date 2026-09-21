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
docker compose up                              # Redis, Kafka, ClickHouse, API, controller
docker compose --profile templates build       # producer + consumer images
docker compose --profile opcua up -d           # mock OPC UA server on :4840
```

The root stack needs no `.env` — it passes its config inline. Running the tests needs the
dev dependencies: `python -m pip install -r requirements-dev.txt`, then `pytest tests/phase0
tests/phase1`. The Docker-backed tests build the images they need and bring up Kafka
themselves.

The root stack is Redis, Kafka, ClickHouse, the Task Manager API and the controller. Producer
containers (Telegraf + connector) are launched per pipeline by `DockerRuntime` when a pipeline
starts — they are not compose services. The shared consumer pool lands in Phase 2.

Kubernetes-backed local dev is not required — `DockerRuntime` stays a supported mode behind
`RuntimeAdapter` specifically so Compose keeps working for this.

## 3. Repo layout (target — confirm against actual tree as phases land)

```
connectors/opcua/
  connector.py              # execd source script — connect, emit JSON lines  [Phase 1 ✓]
  mock_server.py            # mock OPC UA server for dev/CI                   [Phase 1 ✓]
telegraf/templates/
  opcua.conf.tmpl           # per-pipeline Telegraf config template           [Phase 1 ✓]
services/producer_service/  # producer image: Telegraf + connector            [Phase 1 ✓]
services/task_manager/
  common/app_common/
    telegraf_config.py      # renders the template from a PipelineConfig      [Phase 1 ✓]
    ch_naming.py            # ch_unique_identifier — the table name            [Phase 2 ✓]
    ch_schema.py            # table DDL, shared by the API and the pool        [Phase 2 ✓]
    runtime/base.py         # RuntimeAdapter — 5-method interface
    runtime/docker_runtime.py    # Compose-based implementation
    runtime/kubernetes_runtime.py  # K8s implementation                       [Phase 3]
  task_manager/             # FastAPI: pipeline CRUD, folder watcher
  controller/, watchdog/    # deleted in Phase 3
services/consumer_pool/     # shared aiokafka pool                            [Phase 2 ✓]
  app/main.py               # getmany -> insert -> commit, pattern subscribe
  app/sink.py               # per-pipeline tables + dead_letter_events
  app/rows.py               # Telegraf message -> row, against the declared schema
k8s/                        # Deployment manifests                            [Phase 3]
monitoring/                 # kafka-ui, prometheus, otel                      [Phase 4]
tests/phaseN/{unit,integration}/
docs/                       # you are here
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
   handling** — that's exactly the case it exists to catch. Then query it directly:

   ```sql
   SELECT failed_at, error_message, payload
   FROM data_platform.dead_letter_events
   WHERE pipeline_id = '<id>' ORDER BY failed_at DESC LIMIT 20;
   ```

   `dlq_rows_total{pipeline_id}` on the pool's `:9100/metrics` says the same thing without
   a query. A pipeline whose rows are all dead-lettering usually means its declared
   `table_schema` and what the source actually emits have diverged.

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
   duplicating what the runtime already provides. Do not add an internal reconnect-backoff
   loop either: exit non-zero with a clear stderr line and let Telegraf restart you, so the
   connector can never sit alive and silent (see `ARCHITECTURE.md` §3.1).
4. Add a Telegraf config template for the new source under `telegraf/templates/`, named
   `<source_type>.conf.tmpl` — `telegraf_config.py` picks it up by that name. Copy
   `opcua.conf.tmpl`: it carries two settings that are easy to omit and painful to debug,
   `restart_delay` (what recovers a connector that exited) and `json_string_fields` (without
   which Telegraf drops non-numeric readings with no error).
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
