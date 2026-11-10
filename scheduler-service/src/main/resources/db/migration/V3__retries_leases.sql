-- Slice 3: retries with backoff, dead-lettering, and worker leases.

-- New state: a failed run waiting for its backoff to elapse.
ALTER TABLE job_runs DROP CONSTRAINT job_runs_status_check;
ALTER TABLE job_runs ADD CONSTRAINT job_runs_status_check
    CHECK (status IN ('QUEUED', 'RUNNING', 'RETRY_SCHEDULED', 'SUCCEEDED', 'FAILED', 'DEAD'));

ALTER TABLE job_runs
    ADD COLUMN next_attempt_at    TIMESTAMPTZ,   -- when a RETRY_SCHEDULED run becomes due
    ADD COLUMN lease_expires_at   TIMESTAMPTZ,   -- RUNNING runs must heartbeat before this
    ADD COLUMN last_heartbeat_at  TIMESTAMPTZ;

-- Each poller scans only its own small slice of the table.
CREATE INDEX idx_runs_retry_due ON job_runs (next_attempt_at) WHERE status = 'RETRY_SCHEDULED';
CREATE INDEX idx_runs_lease     ON job_runs (lease_expires_at) WHERE status = 'RUNNING';

ALTER TABLE jobs ADD COLUMN timeout_seconds INT NOT NULL DEFAULT 300
    CHECK (timeout_seconds BETWEEN 1 AND 540);
