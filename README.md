# Distributed Job Scheduler

A horizontally scalable job scheduler built with Java 21 and Spring Boot 3. Run any number of scheduler nodes against one PostgreSQL database; each due occurrence is claimed by exactly one node.

> Status: **slice 3 of 5**: concurrent-safe claiming, a transactional outbox to Kafka, stateless workers, and failure handling (retries with backoff, dead-lettering, leases with heartbeats, fencing, timeouts). Observability and load testing are next.

## Why this exists

Cron on a single box is a single point of failure, and naively running it on two boxes runs every job twice. This project explores the real problems behind "run this at 9 AM, exactly once": concurrent claiming, idempotency, retries, missed ticks, and observability.

## Architecture (current slice)

```
                  POST /api/v1/jobs
 client ─────────────────────────────▶ scheduler-service (N nodes)
                                        │  ① claim tick (1s)
                                        │     ONE transaction:
                                        │     lock due jobs (SKIP LOCKED)
                                        │     + insert job_run QUEUED
                                        │     + insert outbox row
                                        ▼
                                    PostgreSQL ──② outbox relay──▶ Kafka: job-runs (key=jobId)
                                        ▲                                   │
                 ④ guarded UPDATE       │                                   ▼ ③ consumer group "workers"
                 (QUEUED→RUNNING→done)  │                           worker-service (M nodes)
                                        │                           handler registry: log | sleep | flaky
                                        └──── Kafka: job-run-results ◀──────┘ STARTED / SUCCEEDED / FAILED
```

**Delivery guarantee: at-least-once execution, exactly-once *state*.** Kafka redelivers when a worker
crashes mid-run, and the relay can re-send after a crash. So a handler may run twice for one `runId`
(handlers must be idempotent), but the run's recorded state can't be corrupted: every status change is
a conditional `UPDATE … WHERE status IN (allowed)`, so duplicate and out-of-order events do nothing.

## Failure handling

```
 QUEUED ──STARTED──▶ RUNNING ──SUCCEEDED──▶ SUCCEEDED
   ▲                   │  ▲
   │                   │  └─ HEARTBEAT every 10s extends the lease (30s)
   │                   │
   │                   ├─ FAILED (retryable), timeout, or lease expired
   │                   │      ├─ attempts left ─▶ RETRY_SCHEDULED ──backoff elapses──┐
   │                   │      └─ none left ─────▶ DEAD ──▶ job-runs.dead (alerting)  │
   │                   └─ FAILED (non-retryable) ─▶ DEAD                             │
   └──────────────────────────────────────────────────────────────────────────────────┘
                     POST /api/v1/runs/{id}/replay : DEAD ─▶ RETRY_SCHEDULED
```

| Failure | Detected by | Outcome |
|---|---|---|
| Handler throws | Worker | `FAILED (retryable)` → retry with exponential backoff and full jitter (2s base, 5m cap) |
| Handler throws `NonRetryableException` / unknown handler | Worker | Straight to `DEAD`; retrying can't help |
| Handler exceeds `timeoutSeconds` | Worker (`Future.get(timeout)`) | Handler interrupted, `FAILED (retryable)` |
| Worker crashes / is partitioned / GC-paused | Scheduler lease reaper (no heartbeat for 30s) | Treated as a retryable failure |
| Message can't be deserialised (poison) | Spring Kafka `DefaultErrorHandler` | Retried in place, then parked on `*.DLT` so the partition isn't blocked |
| Kafka unreachable | Outbox relay | Rows stay unpublished; `scheduler.outbox.pending` grows; drains on recovery |

**Fencing tokens.** Every dispatch carries an `attempt` number, and every result echoes it back. A
result is applied only if it belongs to the attempt currently in flight. So a "zombie" worker (one
that was presumed dead and replaced but then woke up) cannot mark the new attempt as succeeded or failed.
Its side effects may still have happened, which is why handlers must be idempotent on `runId`.

**Two kinds of retry, deliberately separate.** *Business retries* (the job failed) go through the
scheduler. They are durable, visible in `job_runs`, backed off, and replayable. *Transport retries*
(the message itself couldn't be processed) stay inside the Kafka consumer and end in a DLT. Mixing the
two would either block partitions on long backoffs or hide business failures in a broker topic.

## Key design decisions

| Decision | Why | Alternative considered |
|---|---|---|
| `FOR UPDATE SKIP LOCKED` for claiming | Nodes poll concurrently without blocking each other; no separate coordinator needed | Redis/ZooKeeper leader election — adds infra and a failover window |
| `UNIQUE (job_id, scheduled_for)` on runs | DB-level backstop: an occurrence can't be materialised twice even under a bug | App-level checks only — racy |
| Next run computed from the *scheduled* time, missed ticks skipped | No schedule drift after a slow tick; no thundering backlog after downtime | Replay every missed occurrence (configurable "catch-up" policy is on the roadmap) |
| Transactional outbox instead of publishing to Kafka inside the claim | Avoids the dual-write problem: a DB commit with a failed publish would leave runs stuck QUEUED; a publish followed by a rollback would run phantom jobs | Kafka transactions + `ChainedTransactionManager`: deprecated and not truly atomic across DB and broker; CDC (Debezium) is the production-grade upgrade |
| Results flow back over Kafka; workers have no DB access | Workers stay stateless and independently scalable; the scheduler stays the single writer of run state | Workers update Postgres directly: a shared-database coupling, and connection pressure grows with worker count |
| Guarded state-transition UPDATEs | Make result handling idempotent and order-tolerant without locks or version checks | Optimistic locking: retries on conflict, more complex for the same guarantee |
| Partition key = jobId | Runs of the same job are processed in order; different jobs spread across 6 partitions | Random key: better spread, but loses per-job ordering |
| Retry state lives in Postgres, not in Kafka retry topics | Durable, queryable, replayable from an API; the backoff delay doesn't occupy a consumer | Spring Kafka `@RetryableTopic`: neat, but retry state is invisible outside the broker and per-delay topics multiply |
| Full jitter backoff | Spreads a burst of failures from one outage, avoiding a thundering herd on the recovering dependency | Fixed or plain exponential delay: synchronised retry storms |
| Leases use the scheduler's clock | Immune to worker clock skew | Trusting the worker's timestamps |
| Job timeout capped at 540s, below `max.poll.interval.ms` (600s) | A handler outliving the poll interval would trigger a rebalance and duplicate execution | Pausing the consumer during long runs: more moving parts, which a later slice could add |
| Idempotency key on create | Client retries after a timeout don't schedule duplicates | — |
| Flyway owns the schema, Hibernate `validate` | Schema changes are reviewed, versioned, reproducible | `ddl-auto=update` — silently drifts between environments |

## Run locally

```bash
docker compose up -d                                # postgres + kafka
mvn install -DskipTests                             # builds contracts for the services
mvn -pl scheduler-service spring-boot:run           # terminal 1
mvn -pl worker-service spring-boot:run              # terminal 2
WORKER_ID=w2 PORT=8082 mvn -pl worker-service spring-boot:run   # terminal 3: a second worker
```

```bash
# one-off job
curl -X POST localhost:8080/api/v1/jobs -H 'Content-Type: application/json' -d '{
  "name": "send-report", "handler": "log", "scheduleType": "ONE_OFF", "payload": {"to": "ops"},
  "idempotencyKey": "report-2026-11-10"
}'

# flaky recurring job: watch runs retry with backoff and eventually SUCCEED or go DEAD
curl -X POST localhost:8080/api/v1/jobs -H 'Content-Type: application/json' -d '{
  "name": "flaky", "handler": "flaky", "scheduleType": "CRON",
  "cronExpression": "*/5 * * * * *", "payload": {"failRate": 0.3}
}'

# recurring job (Spring 6-field cron: sec min hour day month weekday)
curl -X POST localhost:8080/api/v1/jobs -H 'Content-Type: application/json' -d '{
  "name": "heartbeat", "scheduleType": "CRON", "cronExpression": "*/10 * * * * *"
}'

# a job that is rejected non-retryably → DEAD immediately
curl -X POST localhost:8080/api/v1/jobs -H 'Content-Type: application/json' -d '{
  "name": "bad", "handler": "reject", "scheduleType": "ONE_OFF", "payload": {"error": "invalid account id"}
}'

curl localhost:8080/api/v1/jobs/{id}/runs
curl 'localhost:8080/api/v1/runs?status=DEAD'
curl -X POST localhost:8080/api/v1/runs/{runId}/replay

# crash test: start a sleep job, kill the worker mid-run (Ctrl+C), and watch the lease
# reaper reschedule it within ~30s onto the other worker
curl -X POST localhost:8080/api/v1/jobs -H 'Content-Type: application/json' -d '{
  "name": "long", "handler": "sleep", "scheduleType": "ONE_OFF", "payload": {"millis": 60000}
}'
curl localhost:8080/actuator/prometheus | grep scheduler_jobs_claimed
```

## Tests

`mvn test` (needs Docker; uses Testcontainers).

- **8 concurrent claimers over 200 due jobs**: every job is claimed exactly once, with exactly one outbox row per run.
- **Duplicate and out-of-order results** (SUCCEEDED before STARTED, each delivered twice, then a late FAILED): the final state is still SUCCEEDED.
- **Retry then dead-letter**: a run fails, is rescheduled with backoff, is re-dispatched as attempt 2, fails again, then goes `DEAD` with a dead-letter event.
- **Non-retryable failure** goes straight to `DEAD`.
- **Lease expiry**: a run whose worker stops heartbeating is reaped and rescheduled; a heartbeat extends the lease.
- **Zombie fencing**: after attempt 2 has started, late SUCCEEDED, FAILED and HEARTBEAT events from attempt 1 are all ignored.
- **Backoff unit tests**: doubling, cap, no overflow, and the jitter range.
- **Worker unit tests**: success, retryable vs. non-retryable failure, unknown handler, timeout and heartbeats, with no broker needed.

## Roadmap

- [x] **Slice 1** — Job API, Flyway schema, SKIP LOCKED claiming, idempotent create
- [x] **Slice 2** — Transactional outbox → Kafka; `worker-service` executes runs; idempotent result handling
- [x] **Slice 3** — Retries with exponential backoff + jitter, dead-lettering + replay, poison-message DLTs, run timeouts, leases + heartbeats, fencing tokens
- [ ] **Slice 4** — Prometheus + Grafana dashboards, load test with published numbers
- [ ] **Slice 5** — React dashboard (Vercel), backend deployment, architecture diagram
