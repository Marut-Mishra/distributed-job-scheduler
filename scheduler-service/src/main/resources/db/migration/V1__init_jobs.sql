-- Job definitions: what to run and when.
CREATE TABLE jobs (
    id               UUID PRIMARY KEY,
    name             VARCHAR(200) NOT NULL,
    schedule_type    VARCHAR(20)  NOT NULL CHECK (schedule_type IN ('ONE_OFF', 'CRON')),
    cron_expression  VARCHAR(120),
    payload          JSONB        NOT NULL DEFAULT '{}'::jsonb,
    status           VARCHAR(20)  NOT NULL CHECK (status IN ('ACTIVE', 'PAUSED', 'COMPLETED')),
    next_run_at      TIMESTAMPTZ,
    max_retries      INT          NOT NULL DEFAULT 3 CHECK (max_retries >= 0),
    idempotency_key  VARCHAR(120) UNIQUE,
    version          BIGINT       NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT cron_required CHECK (schedule_type <> 'CRON' OR cron_expression IS NOT NULL)
);

-- Partial index: the claimer only ever scans ACTIVE jobs ordered by due time.
CREATE INDEX idx_jobs_due ON jobs (next_run_at) WHERE status = 'ACTIVE';

-- One row per execution attempt-chain of a job occurrence.
CREATE TABLE job_runs (
    id              UUID PRIMARY KEY,
    job_id          UUID         NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    scheduled_for   TIMESTAMPTZ  NOT NULL,
    status          VARCHAR(20)  NOT NULL CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'DEAD')),
    attempt         INT          NOT NULL DEFAULT 0,
    worker_id       VARCHAR(120),
    error           TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    -- Guarantees a given occurrence is materialised at most once,
    -- even if two scheduler nodes race on the same tick.
    CONSTRAINT uq_job_occurrence UNIQUE (job_id, scheduled_for)
);

CREATE INDEX idx_job_runs_job ON job_runs (job_id, created_at DESC);
