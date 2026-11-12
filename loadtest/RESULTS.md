# Load test results

Numbers in this file come from real runs only. Each row records the machine, the setup and the
exact command, so anyone can reproduce it. Keep the empty rows until you have measured them.

## Environment

| | |
|---|---|
| Machine | _e.g. Windows 11, i7-12700H, 16 GB RAM_ |
| Docker Desktop | _version, memory limit_ |
| Postgres / Kafka | single node each, in Docker |
| Scheduler nodes | _1 / 2_ |
| Workers | _count × `WORKER_CONCURRENCY`_ |
| Handler | `sleep`, 50 ms |

## Runs

Results come from `GET /api/v1/stats?since=<test start>`, which reads ground truth from `job_runs`.

| # | Create rate | Duration | Scheduler nodes | Workers × concurrency | Succeeded | Throughput (runs/s) | Pickup p50 / p95 / p99 (ms) | End-to-end p95 (ms) | Retries | Dead | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 50/s | 2m | 1 | 1 × 3 | | | | | | | baseline |
| 2 | 100/s | 2m | 1 | 2 × 3 | | | | | | | |
| 3 | 200/s | 2m | 1 | 3 × 6 | | | | | | | |
| 4 | 200/s | 2m | 2 | 3 × 6 | | | | | | | two scheduler nodes: check that no runs are duplicated |
| 5 | 100/s | 2m | 1 | 2 × 3 | | | | | | | `HANDLER=flaky FAIL_RATE=0.2` |
| 6 | 100/s | 3m | 1 | 2 × 3 | | | | | | | kill one worker at 1m; record the recovery time |

## Observations

_What became the bottleneck first (DB pool, poll interval, partitions, worker concurrency)?
What did you change, and what happened?_ Interviewers ask about this section; the numbers alone matter less.
