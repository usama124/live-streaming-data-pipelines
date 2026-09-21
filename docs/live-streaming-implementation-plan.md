# Live Streaming Module — Implementation Plan

**Status:** Approved direction, not yet built. This plan turns the architecture decision
(see `live-streaming-architecture-decision.pdf`) into ordered, file-level work.

**Scope:** Live pipelines only. Batch/normal pipelines keep running as Airflow DAGs,
untouched by any step below.

---

## 0. Before writing any code

Read these in order — they're the verified source of truth, not this plan:

1. `.claude/skills/stratahub-live-streaming/SKILL.md` — project index, invariants, traps
2. `docs/ARCHITECTURE.md` — as-built system, §4 Redis contract, §5 lifecycle, §8 config
3. `docs/BACKLOG.md` — known issues, each naming the exact file
4. `docs/ONBOARDING.md` — setup, debugging playbook (§5), connector recipe (§7), conventions (§10)
5. `docs/DECISION-live-pipeline-simplification.md` — the memo this plan implements

File paths below are taken from the current verified brief. Confirm each against the actual
repo before editing — if a path has moved, trust the repo over this document.

**Do not trust `README.md`** — it documents a pre-split layout that no longer exists. Fixing
it is Phase 0 work, not a reference for anything else.

---

## 1. Target end state, in one table

| Component | Today | Target |
|---|---|---|
| Producer | Custom container per pipeline, hand-rolled Kafka publish/retry/heartbeat/polling | Telegraf (`inputs.execd`) running our thin per-source connector as a subprocess |
| Consumer | Quix Streams, `sdf.sink(sink)`, fixed topic list, shared ClickHouse table | aiokafka, shared pool, regex topic subscription, per-pipeline ClickHouse table |
| Lifecycle | Controller + Watchdog + Leader Lock polling Redis every 3s | Kubernetes Deployments; `desired_state` = replica count |
| Monitoring | None | Kafka UI, Prometheus, OpenTelemetry tracing |

Net effect target: 2,759 → ~2,050 lines; 4 always-on services → 2; Redis drops out of the
control plane entirely.

---

## 2. Repo map — what happens to each file

| Path | Action |
|---|---|
| `task_manager/` (584 lines) | **Modify** — start/stop/restart calls change from Docker/Redis writes to Kubernetes API calls; response contract to the API caller improves (errors return synchronously instead of surfacing on next poll) |
| `controller.py` (185 lines) | **Delete** — only after Phase 3 is proven in staging |
| `watchdog.py` (138 lines) | **Delete** — same gate as controller |
| Leader lock (139 lines) | **Delete** — same gate; Kubernetes needs no leader election here |
| `producer_service` (581 lines, incl. 258-line OPC UA connector) | **Split — done (Phase 1).** All 581 lines of `app/` deleted. OPC UA logic now `connectors/opcua/connector.py` (~150 lines); the plumbing is gone, not moved. `services/producer_service/` is now just the image: Telegraf + the connector |
| `quix_consumer.py` (611 lines) | **Deleted (Phase 2).** All of `services/consumer_service` went, replaced by `services/consumer_pool`. The root `common/` tree went with it — it was dead once the Quix consumer did, leaving `services/task_manager/common` as the single `app_common` |
| `common/app_common/runtime/base.py` (`RuntimeAdapter`, 5 methods) | **Keep** — interface stays; add a second implementation |
| `docker_runtime.py` (155 lines) | **Keep** — Compose stays a supported deployment mode |
| `k8s/` | **Implement against** — templates exist but are unused; this is where `KubernetesRuntime` and the Deployment manifests land |
| `airflow_client.py` stub | **Delete** — confirmed dead, unused on the live path |
| Root `docker-compose.yml`, `.env` files | **Fix** — wrong Dockerfile path, missing `clickhouse/init.sql`, hardcoded `/home/usama/Videos/...` volume, dead `172.22.0.1` network, missing `DOCKER_NETWORK` creation, no `.env.example` |
| `README.md` | **Rewrite** — stale pre-split layout |
| `tests/` | **New** — see §3, Testing Policy. Phase-ordered, grows alongside every phase, never shrinks. |

---

## 3. Testing Policy

**Rule: no phase is "done" without (a) its task checklist complete, (b) its scenario test
cases passing, and (c) its full integration test suite passing against the stack as it
stands. All three are required — a phase is not complete on task checklist alone.**

Two tiers of tests, not one:

- **Scenario tests** (per component, fast, run constantly) — verify one specific behavior in
  isolation: a connector emits correctly, a dead-letter path catches a bad record, a probe
  fires at the right threshold. These are the ones listed under each phase below as "Test
  cases to write."
- **Integration tests** (per phase, run at the end of that phase, slower, exercise the real
  stack) — verify the pieces built so far actually work *together*: a pipeline created
  through the real API produces data that lands correctly in ClickHouse, under real
  concurrency, with real faults injected, not mocked component-by-component. These are
  listed under each phase as "Integration suite" and only make sense to run once that
  phase's pieces exist — Phase 2's integration suite needs the Phase 1 producer already
  built, for example.

**A phase does not advance to the next phase until its integration suite passes.** This is
the point of doing it per-phase rather than once at the end: catching a Phase 2 integration
problem before Phase 3 is built on top of it is cheap; catching it after is not.

Both tiers are **cumulative** — nothing gets deleted when a later phase lands. If Phase 3's
work breaks a Phase 2 integration test, that's a regression to fix, not a test to loosen.

**Directory layout:**

```
tests/
  phase0/
    integration/
  phase1/
    unit/
    integration/
  phase2/
    unit/
    integration/
  phase3/
    unit/
    integration/
  phase4/
    unit/
    integration/
```

A known, accepted gap (e.g., Phase 1's Telegraf-can't-detect-a-silent-hang gap) still gets a
test — marked `xfail`/skipped with a comment naming the phase that closes it — so the gap is
tracked in the suite itself, not only in prose that can go stale. See §12 for the full
cumulative registry, scenario and integration tests together.

---

## Phase 0 — Repo hygiene (do this first, independent of everything else)

Goal: a clean checkout works before any architectural change lands on top of it.

- [x] Fix root `docker-compose.yml`: correct Dockerfile path, add `clickhouse/init.sql`
- [x] Remove the hardcoded `/home/usama/Videos/...` volume mount
- [x] Fix `.env` files pinning the dead `172.22.0.1` network
- [x] Add `DOCKER_NETWORK` creation to Compose (or the setup script), not just a reference to it
- [x] Add `.env.example` for the `task_manager` stack
- [x] Delete `airflow_client.py` stub
- [x] Rewrite `README.md` to match the current split layout

**Integration suite (`tests/phase0/integration/`):**
- [x] Clean-checkout smoke test: `docker compose up` succeeds with no manual fixes, on CI,
      from a fresh clone. There's nothing else to integrate yet — this phase's "integration
      test" is just proving the foundation boots.

**Acceptance:** `docker compose up` on a clean checkout brings up the full stack with no
manual fixes, and the Phase 0 integration suite passes.

---

## Phase 1 — Producer: Telegraf + connector extraction

Goal: replace the custom producer container with Telegraf running a thin connector.

- [x] Extract the OPC UA logic (~258 lines) out of `producer_service` into a standalone
      script whose only job is: connect to source → emit one record per line to stdout
      (Influx line protocol or JSON, matching Telegraf's `data_format` config)
- [x] Delete the plumbing that stays behind: Kafka publish client, Redis heartbeat, desired-
      state polling loop, and the generic `sources/base.py` abstraction it no longer needs
- [x] Write a Telegraf config template using `inputs.execd` to launch the connector script,
      with `outputs.kafka` targeting `pipeline.<id>.events`
- [x] Confirm Telegraf's `restart_delay` behavior on subprocess exit matches what the
      watchdog used to guarantee, so nothing regresses before Phase 3 removes the watchdog
      — **confirmed:** `restart_delay = 10s` against the watchdog's 30s heartbeat timeout,
      so recovery is strictly faster. Verified by registry #4, which kills the connector
      inside a running Telegraf container and watches the sequence counter restart
- [x] Update `task_manager`'s pipeline-create path to generate the per-pipeline Telegraf
      config instead of the old producer container's env vars
- [x] Ship `THIRD-PARTY-NOTICES.md` with the producer image (Telegraf, MIT, © InfluxData).
      MIT requires the notice in any substantial distribution — the trap is a multi-stage
      Dockerfile that copies the binary out of the upstream image and leaves `LICENSE`
      behind. See the decision memo's Proposal A for which distribution forms trigger it
- [x] **Known gap, not fixed by this phase:** Telegraf restarts the subprocess only if it
      exits. A silently-dead OPC UA session (process alive, no data) is not caught here —
      this is what Phase 3's liveness probe exists to cover. Do not treat Phase 1 as done
      until Phase 3 lands, for any pipeline that depends on reconnect behavior.

**Unit/scenario tests (`tests/phase1/unit/`):**
- [x] Connector emits well-formed records to stdout against a healthy mock OPC UA source
- [x] Telegraf forwards those records to the correct Kafka topic (`pipeline.<id>.events`) —
      no cross-pipeline topic leakage when two connectors run side by side
- [x] Connector subprocess exits (simulated crash) → Telegraf restarts it within the
      configured `restart_delay`, with no manual intervention
- [x] Connector fails to connect to the source at all (auth failure, unreachable host) →
      fails with a clear log line, does not crash Telegraf itself, does not hang silently
- [x] Pipeline creation generates a Telegraf config whose topic name and connector args
      actually match the pipeline's `PipelineConfig`
- [x] **Known-gap test (expected to fail/skip until Phase 3):** connector process stays
      alive but stops producing output (simulated hung source) — assert this is *not* caught
      by Telegraf alone; mark it explicitly so Phase 3's liveness-probe test is the one that
      flips it to passing, not a silent gap

**Integration suite (`tests/phase1/integration/`):**
- [x] End-to-end against a real or realistic mock OPC UA source: create a pipeline through
      the actual `task_manager` API (against `DockerRuntime`, since Kubernetes doesn't exist
      yet) → verify the values landing on the Kafka topic actually match the source's known
      values, not just that "some message" arrived
- [x] Multiple pipelines' Telegraf instances running concurrently against different mock
      sources — verify topic isolation holds under real concurrent load, not just in a
      single-pipeline unit test
- [x] Full producer lifecycle through the real API: create → data flows for a sustained
      period → stop → confirm the Telegraf process and its resources are actually torn down

**Acceptance:** a pipeline created against a live OPC UA source produces events on its Kafka
topic via Telegraf, with no producer-side code beyond the connector script and the Telegraf
config, and the Phase 1 integration suite passes.

**Not in this phase, tracked for later:** AVEVA and MQTT connectors. MQTT likely needs no
custom connector at all — Telegraf ships a native MQTT input. AVEVA follows the OPC UA
pattern once its protocol/API is confirmed.

---

## Phase 2 — Consumer: shared pool (aiokafka)

Goal: one shared, regex-subscribed consumer pool replaces Quix Streams and the shared-table bug.

- [x] Stand up a new consumer service using aiokafka, subscribed to `pipeline.*.events` via
      `subscribe(pattern=...)`
- [x] Implement the core loop: `getmany()` → accumulate per topic → insert → commit, **in
      that order** (insert before commit, so a crash mid-batch re-reads rather than drops)
- [x] Fix the schema-collision bug at the root: stop inferring columns from the first event
      (`ClickHouseSink._ensure_table()`'s current behavior) and instead create the table from
      an explicit per-pipeline schema at pipeline-creation time (not the current global
      `CLICKHOUSE_TABLE` setting in `docker_runtime.py:94`)
- [x] Add `PipelineConfig.user_id`, `PipelineConfig.collection_number`,
      `PipelineConfig.table_name` (currently absent). The actual ClickHouse table is
      `ch_unique_identifier = f"user_{user_id}_collection_{collection_number}_{table_name}"`
      — **locate the function normal (batch) pipelines already use to generate this and call
      it from the live path**, do not write a second implementation. The consumer pool
      resolves topic → `PipelineConfig` → `ch_unique_identifier` to pick the write target;
      Kafka topic naming (`pipeline.<id>.events`) stays a separate, internal identifier
- [x] **Implement dead-letter handling before this ships**, not after: no exception may
      escape the write path, since one bad record must not stall every other pipeline
      sharing the pool. The spec is written — Proposal E in the decision memo for the
      reasoning, `ARCHITECTURE.md` §3.3.1 for the contract (error classification with
      transient as the fail-safe default, batch bisection, the shared `dead_letters` table,
      and the insert → dead-letter → commit ordering). Implement that; do not redesign it
- [x] Remove the Quix Streams dependency and the sync-to-async bridge it required
- [x] Update `task_manager`'s pipeline-create path to create the Kafka topic **and** the
      ClickHouse table before starting the producer (per the decision memo's creation flow)

**Unit/scenario tests (`tests/phase2/unit/`):**
- [x] A newly created pipeline's topic is picked up by the running consumer pool with **no
      restart of the pool** — this is the core SaaS constraint and the single most important
      test in this phase
- [x] Two pipelines with different event schemas run concurrently, each landing correctly in
      its own ClickHouse table — this is the direct regression test for the shared-table
      schema-collision bug this phase exists to fix
- [x] Simulated crash between insert and commit → on restart, the batch is re-processed, not
      dropped (at-least-once behavior, verified, not assumed)
- [x] A malformed or unexpected-schema record is routed to dead-letter without raising an
      exception that escapes the write path, and without blocking other pipelines' topics
      from continuing to process
- [x] Consumer pool pod restart resumes from the last committed offset — no data loss, no
      unbounded duplication
- [x] Topic-to-table mapping is correct under load: no pipeline's records land in another
      pipeline's table
- [x] Batch flush triggers correctly at the configured size/time thresholds

**Integration suite (`tests/phase2/integration/`):**
- [x] Full pipeline integration: create a pipeline via the real API → Telegraf produces →
      the consumer pool consumes → correct row lands in the correct ClickHouse table,
      verified end-to-end without manually inspecting intermediate steps
- [x] Concurrent multi-pipeline soak run: several pipelines with varying schemas running
      simultaneously over a sustained period — verify no cross-contamination and no dropped
      or duplicated data beyond expected at-least-once behavior
- [x] Fault injection under load: kill the consumer pool mid-stream while multiple pipelines
      are actively producing — verify all affected pipelines recover and resume correctly,
      not just a single isolated pipeline as in the unit-level test
- [x] A new pipeline is created while the pool is already under load from existing pipelines
      — verify it's picked up correctly without degrading existing pipelines' throughput

**Acceptance:** two pipelines with different OPC UA node sets (different event schemas) run
concurrently without a table-creation collision, each landing in its own ClickHouse table.
Killing the consumer pool mid-batch and restarting it does not drop or duplicate rows beyond
normal at-least-once behavior. The Phase 2 integration suite passes.

---

## Phase 3 — Orchestration: Kubernetes

Goal: Kubernetes Deployments replace Controller + Watchdog + Leader Lock.

- [ ] Implement `KubernetesRuntime` behind the existing `RuntimeAdapter` interface
      (`common/app_common/runtime/base.py`) — same 5 methods as `DockerRuntime`:
      `start_live_pipeline`, `stop_live_pipeline`, `restart_live_pipeline`, `get_logs`,
      `is_running`
- [ ] Build out the `k8s/` Deployment templates (they exist but are currently unused) for
      the producer pod (1 per pipeline) and the consumer pool (shared, few replicas)
- [ ] Map lifecycle semantics: `desired_state=running` → `replicas: 1`; `desired_state=stopped`
      → `replicas: 0`; no separate reconcile loop needed, `kube-controller-manager` owns it
- [ ] Add a liveness probe: an HTTP endpoint on the producer pod that fails when the last
      successful source read exceeds a threshold, so kubelet recycles a hung-but-alive pod.
      This is the backstop for Phase 1's known Telegraf reconnect gap — **do not skip it**
- [ ] Define the staleness threshold per source type (OPC UA now; leave room for AVEVA/Modbus
      tuning later) — this needs a number, not just the mechanism
- [ ] Update `task_manager` so a failed start returns an error to the API caller directly,
      instead of surfacing on the next 3s controller tick
- [ ] Switch `task_manager`'s runtime selection to `KubernetesRuntime` by default, with
      `DockerRuntime` kept selectable for Compose-based local dev
- [ ] Once proven in staging: delete `controller.py`, `watchdog.py`, and the leader lock
      module; stop writing `desired_state` to Redis as a control signal

**Unit/scenario tests (`tests/phase3/unit/`):**
- [ ] Start pipeline via API → Deployment created with `replicas: 1`
- [ ] Stop pipeline via API → Deployment scaled to `replicas: 0`
- [ ] Restart via API → pod recreated correctly, pipeline resumes producing data
- [ ] Producer pod crash → Kubernetes restarts it automatically (`restartPolicy: Always`)
      with no manual intervention
- [ ] **This is the closing test for Phase 1's known gap:** simulate a hung-but-alive
      connector (process running, last successful read older than the configured threshold)
      → liveness probe fails → kubelet recycles the pod. Flip Phase 1's marked xfail test to
      passing once this lands
- [ ] A failed start returns a synchronous error to the API caller — verify the actual
      response, not just that "something" eventually shows up in logs

**Integration suite (`tests/phase3/integration/`):**
- [ ] Full pipeline lifecycle through the real API, backed by `KubernetesRuntime`, against a
      real cluster (or kind/minikube): create → data flows correctly end-to-end → stop →
      confirm resources are actually cleaned up, not just marked stopped
- [ ] Chaos test: kill a producer pod and a consumer pool pod during active data flow —
      verify Kubernetes self-heals both and the pipeline resumes correctly with no manual
      intervention
- [ ] End-to-end liveness-probe test: induce a real hung-source condition (not a unit-level
      simulation) and verify the full loop — probe fails → pod recycled → data resumes
- [ ] **Regression run:** re-run the entire Phase 2 integration suite against
      `KubernetesRuntime` instead of `DockerRuntime`, confirming parity between the two
      runtimes rather than assuming it
- [ ] **Manual/staging verification, not CI:** simulate node failure (cordon/drain or kill a
      node in a staging cluster) → confirm the affected pod is rescheduled onto a healthy
      node. Note this explicitly as a staging-only check rather than skipping it

**Acceptance:** starting, stopping, and restarting a pipeline through the API produces the
correct pod state with no Redis polling involved. Killing a producer pod's host node (or
simulating node failure) results in the pod being rescheduled — this is the capability that
did not exist before. The Phase 3 integration suite passes, including the Phase 2 regression
run under the new runtime.

---

## Phase 4 — Monitoring

Goal: close the "no observability exists" gap, in the sequence that avoids building
dashboards against infrastructure about to be deleted.

- [ ] Deploy Kafka UI (Kafbat or AKHQ) for topics, throughput, consumer-group lag
- [ ] Deploy Prometheus; ClickHouse already exposes a Prometheus endpoint, wire it in
- [ ] Add OpenTelemetry tracing across source read → Kafka → ClickHouse insert, to answer
      "how stale, and where is the time going" — lag alone doesn't catch a stalled OPC UA
      subscription that shows zero lag while data is an hour old
- [ ] Cheap interim step if full tracing slips: stamp each event with its source-read
      timestamp and chart `now() - max(source_ts)` per table in Grafana
- [ ] Design (not necessarily build in this phase) a per-user pipeline health API — this
      is a product surface on your own API, not Grafana, since Grafana holds data across
      every user's pipelines and can never be shown to one customer directly

**Unit/scenario tests (`tests/phase4/unit/`):**
- [ ] Kafka UI reports correct lag/throughput for a running pipeline against known test
      traffic (verify the numbers, not just that the dashboard renders)
- [ ] Prometheus successfully scrapes metrics from Telegraf, the consumer pool, and the
      ClickHouse endpoint — a scrape target being silently down is itself a bug to catch
- [ ] An OpenTelemetry trace for a single record is reconstructable end-to-end, from source
      read through Kafka to the ClickHouse insert
- [ ] **Staleness detection scenario:** simulate zero Kafka lag with an old `source_ts`
      (a stalled-but-connected source) → monitoring surfaces this as stale, not healthy —
      this is the exact case plain lag-based health checks miss, so it needs its own test
- [ ] If the per-user health API is built in this phase: verify it returns only the
      requesting user's own pipelines, with no cross-user data leakage

**Integration suite (`tests/phase4/integration/`):**
- [ ] Full multi-pipeline integration run with monitoring attached: cross-check Kafka UI,
      Prometheus, and OTel trace numbers against known, actual test traffic — not just that
      dashboards render, but that the numbers are right
- [ ] Induce a real stalled-but-connected source during an active integration run and verify
      the monitoring stack surfaces it as stale in practice, not only in a unit-level
      calculation test
- [ ] If the per-user health API exists: run a multi-user integration test with several
      users' pipelines running concurrently, and verify API responses stay correctly scoped
      under real concurrent load

**Acceptance:** for a running pipeline, you can answer "is it flowing," "how stale is it,"
and "which pipeline is the customer asking about" without querying ClickHouse by hand. The
Phase 4 integration suite passes.

---

## Cross-cutting items — resolve alongside the phase they block

| Item | Blocks | Status |
|---|---|---|
| Telegraf MIT license | Phase 1 | **Resolved 2026-09-21.** MIT confirmed. Remaining work is attribution, not a decision — see Proposal A and the Phase 1 task below |
| Dead-letter design spec | Phase 2 | **Resolved 2026-09-21.** Spec written: Proposal E (reasoning), `ARCHITECTURE.md` §3.3.1 (contract) |
| ~~`tenant_id`~~ `ch_unique_identifier` | Phase 2 | **Resolved.** No separate `tenant_id` — live pipelines reuse the existing `user_<user_id>_collection_<collection_number>_<table_name>` naming that normal pipelines already generate. Locate and call the shared function; don't reimplement it. See `ARCHITECTURE.md` §4. |
| Sequencing vs. Stratahub merge | All phases | **Resolved 2026-09-21.** Proposal G: standalone through Phase 4, then one merge PR |
| Liveness probe thresholds | Phase 3 | **Open — the only one left.** Needs a measured number per source type, not a guess; depends on each source's real publishing interval |

---

## Rollout order (recap)

**Phase 0 → Phase 1 → Phase 2 → Phase 3 → Phase 4**, with Phase 3's controller/watchdog/
leader-lock deletion gated on staging proof, and Phase 4 sequenced after Phase 2 specifically
so dashboards aren't built against a consumer pool that's mid-migration.

Do not parallelize Phase 3 ahead of Phase 1/2 — the liveness probe in Phase 3 is the
documented backstop for a gap Phase 1 knowingly leaves open. Shipping orchestration without
it means running the Telegraf reconnect gap with no safety net.

**Do not start the next phase until the current phase's integration suite passes**, not just
its unit/scenario tests and task checklist. Integration failures are cheapest to catch
before the next phase builds on top of the broken piece.

Test cases follow the same rule: Phase 1's known-gap test stays marked as an expected gap
until Phase 3's liveness-probe test exists to close it. Don't mark Phase 1 fully green by
deleting or loosening that test — flip it once Phase 3 actually lands.

---

## Explicitly out of scope for this implementation

- Apache Flink adoption — evaluated, not adopted; revisit only if the roadmap adds real
  stateful/windowed stream processing (see the architecture decision PDF's alternatives
  section for the full comparison)
- AVEVA and MQTT connectors — architecture supports them (Phase 1's pattern), but building
  them is separate, follow-on work
- Per-record lineage / NiFi — open question, not decided; a bigger architectural swing that
  should be settled independently before it affects any of the above

---

## 12. Test Case Registry (cumulative, living)

One place to see full scenario and integration coverage at a glance. Update `Status` as
tests are written and passing — don't let this drift from the actual suite. Values:
`Not written` → `Written, failing` (expected for known gaps and TDD-style work) → `Passing`.

### Unit / scenario tests

| # | Scenario | Phase | Status |
|---|---|---|---|
| 1 | Clean-checkout smoke test (`docker compose up`) | 0 | Passing |
| 2 | Connector emits well-formed records against a healthy source | 1 | Passing |
| 3 | Telegraf forwards records to the correct topic, no cross-pipeline leakage | 1 | Passing |
| 4 | Connector subprocess crash → Telegraf restarts it | 1 | Passing |
| 5 | Connector fails to connect → fails clearly, doesn't hang or crash Telegraf | 1 | Passing |
| 6 | Pipeline creation generates a correct Telegraf config | 1 | Passing |
| 7 | Silent hang is NOT caught by Telegraf alone (known-gap marker) | 1 | Written, failing (strict xfail — #19 closes it) |
| 8 | New pipeline's topic picked up with no pool restart | 2 | Passing |
| 9 | Two differently-shaped pipelines land in separate correct tables (schema-collision regression) | 2 | Passing |
| 10 | Crash between insert and commit → re-processed, not dropped | 2 | Passing |
| 11 | Malformed record → dead-letter, no exception escapes, other pipelines unaffected | 2 | Passing |
| 12 | Consumer pool restart resumes from last committed offset | 2 | Passing |
| 13 | Topic-to-table mapping correctness under load | 2 | Passing |
| 14 | Batch flush triggers at configured thresholds | 2 | Passing |
| 15 | Start via API → correct replica count | 3 | Not written |
| 16 | Stop via API → scaled to zero | 3 | Not written |
| 17 | Restart via API → pod recreated, pipeline resumes | 3 | Not written |
| 18 | Producer pod crash → automatic Kubernetes restart | 3 | Not written |
| 19 | Hung-but-alive connector → liveness probe recycles pod (closes #7) | 3 | Not written |
| 20 | Failed start returns synchronous API error | 3 | Not written |
| 21 | Kafka UI reports correct lag/throughput | 4 | Not written |
| 22 | Prometheus scrapes all expected targets successfully | 4 | Not written |
| 23 | OTel trace reconstructable end-to-end for a single record | 4 | Not written |
| 24 | Staleness detected despite zero Kafka lag (stalled-but-connected source) | 4 | Not written |
| 25 | Per-user health API returns no cross-user data | 4 | Not written |

### Integration suites (run at the end of each phase, cumulative)

| # | Scenario | Phase | Status |
|---|---|---|---|
| I1 | Clean-checkout boot (foundation for everything below) | 0 | Passing |
| I2 | Real/mock source → Telegraf → Kafka, values verified against known source data | 1 | Passing |
| I3 | Multiple concurrent Telegraf instances, topic isolation under real concurrent load | 1 | Passing |
| I4 | Full producer lifecycle via real API: create → sustained data flow → stop → teardown verified | 1 | Passing |
| I5 | Full pipeline integration: API create → Telegraf → consumer pool → correct ClickHouse table | 2 | Passing |
| I6 | Concurrent multi-pipeline soak run, varying schemas, sustained period | 2 | Passing |
| I7 | Fault injection: kill consumer pool mid-stream under multi-pipeline load, verify full recovery | 2 | Passing |
| I8 | New pipeline created while pool is under existing load, no degradation | 2 | Passing |
| I9 | Full lifecycle via real API on `KubernetesRuntime` against a real/kind cluster | 3 | Not written |
| I10 | Chaos test: kill producer pod + consumer pool pod during active flow, verify self-heal | 3 | Not written |
| I11 | End-to-end liveness-probe loop: real hung source → probe fails → pod recycled → data resumes | 3 | Not written |
| I12 | Phase 2 integration suite re-run against `KubernetesRuntime` (runtime parity regression) | 3 | Not written |
| I13 | Node-failure reschedule (manual/staging only, not CI) | 3 | Not written |
| I14 | Multi-pipeline run with monitoring: dashboard/trace numbers cross-checked against known traffic | 4 | Not written |
| I15 | Real stalled-but-connected source during integration run, surfaced as stale end-to-end | 4 | Not written |
| I16 | Multi-user concurrent integration run, per-user API scoping verified under load | 4 | Not written |