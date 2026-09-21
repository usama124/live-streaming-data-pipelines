# Live Streaming Module

Standalone prototype of Stratahub/DataAvalanche's live-pipeline path — pipelines that never
finish, so they can't be Airflow DAG runs. See `docs/ARCHITECTURE.md` for the target design
and `docs/live-streaming-implementation-plan.md` for build status.

## Quick start

```bash
docker compose up
```

Brings up Redis, Kafka, ClickHouse and the Task Manager API (http://localhost:8000/health)
on the `data-platform_backend` network. No `.env` needed — the root stack passes its own
config inline. Two optional profiles:

```bash
docker compose --profile templates build   # build the producer/consumer images
docker compose --profile opcua up -d       # mock OPC UA server on :4840
```

The `services/task_manager/` stack (API + controller + watchdog together) is separate and
does need config: `cp services/task_manager/.env.example services/task_manager/.env`, and
bring the root stack up first — it creates the shared network.

See `docs/ONBOARDING.md` for the full setup and debugging playbook.

## Layout

| Path | Contents |
|---|---|
| `common/app_common/` | Models and Redis key helpers shared by the producer/consumer images |
| `services/task_manager/` | Its own compose stack and its own copy of `app_common` (settings, Redis repo, `RuntimeAdapter`) |
| `services/task_manager/task_manager/` | FastAPI — pipeline CRUD, lifecycle signals, folder watcher |
| `services/task_manager/controller/` | Desired-state reconcile loop — deleted in Phase 3. Without it the API records intent and nothing launches |
| `services/task_manager/watchdog/` | Heartbeat monitor — deleted in Phase 3 |
| `connectors/opcua/` | The thin execd connector, plus a mock OPC UA server for dev/CI |
| `telegraf/templates/` | Per-pipeline Telegraf config templates, rendered at pipeline-create time |
| `services/producer_service/` | The producer image: Telegraf + the connector |
| `services/consumer_service/` | Quix Streams consumer — becomes the shared aiokafka pool in Phase 2 |
| `clickhouse/init.sql` | Database bootstrap, runs on first ClickHouse start |
| `k8s/` | Deployment templates — unused until Phase 3 |
| `scripts/` | `curl` helpers for the Task Manager API |
| `tests/phaseN/` | Phase-ordered suites, cumulative — see the plan's §3 testing policy |

## Documentation

| File | Contents |
|---|---|
| `docs/ARCHITECTURE.md` | Target system design — components, data path, control path, config contract |
| `docs/DECISION-live-pipeline-simplification.md` | The decision memo — what was chosen, what was rejected, and why |
| `docs/BACKLOG.md` | Known open items and follow-on work |
| `docs/ONBOARDING.md` | Setup, debugging playbook, connector recipe, conventions |
| `docs/live-streaming-implementation-plan.md` | Phased build checklist |
| `.claude/skills/stratahub-live-streaming/SKILL.md` | Claude Code project skill — invariants and traps |
