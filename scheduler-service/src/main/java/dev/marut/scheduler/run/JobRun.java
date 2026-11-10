package dev.marut.scheduler.run;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

/**
 * Lifecycle:
 * <pre>
 *  QUEUED ──STARTED──▶ RUNNING ──SUCCEEDED──▶ SUCCEEDED
 *    ▲                   │
 *    │                   ├─FAILED / lease expired─┬─ attempts left ─▶ RETRY_SCHEDULED ──backoff──┐
 *    │                   │                        └─ none left / non-retryable ─▶ DEAD          │
 *    └───────────────────┴──────────────────────────────────────────────────────────────────────┘
 * </pre>
 * {@code attempt} doubles as a fencing token: results carry the attempt they belong to, and
 * anything from an older attempt (e.g. a zombie worker whose lease expired) is rejected.
 */
@Entity
@Table(name = "job_runs")
public class JobRun {

    public enum Status { QUEUED, RUNNING, RETRY_SCHEDULED, SUCCEEDED, FAILED, DEAD }

    @Id
    private UUID id;

    @Column(name = "job_id", nullable = false)
    private UUID jobId;

    @Column(name = "scheduled_for", nullable = false)
    private Instant scheduledFor;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status;

    /** Number of the most recent attempt that started (or was failed). 0 = never attempted. */
    @Column(nullable = false)
    private int attempt;

    @Column(name = "worker_id")
    private String workerId;

    private String error;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    @Column(name = "next_attempt_at")
    private Instant nextAttemptAt;

    @Column(name = "lease_expires_at")
    private Instant leaseExpiresAt;

    @Column(name = "last_heartbeat_at")
    private Instant lastHeartbeatAt;

    protected JobRun() { }

    public static JobRun queued(UUID jobId, Instant scheduledFor) {
        JobRun r = new JobRun();
        r.id = UUID.randomUUID();
        r.jobId = jobId;
        r.scheduledFor = scheduledFor;
        r.status = Status.QUEUED;
        r.createdAt = Instant.now();
        return r;
    }

    /** The attempt number the next dispatch of this run represents. */
    public int nextAttemptNumber() { return attempt + 1; }

    /** True if a result for {@code resultAttempt} belongs to the attempt currently in flight. */
    public boolean isCurrent(int resultAttempt) {
        return (status == Status.RUNNING && attempt == resultAttempt)
            || (status == Status.QUEUED && attempt == resultAttempt - 1); // result raced ahead of STARTED
    }

    public void scheduleRetry(int failedAttempt, String error, Instant retryAt) {
        this.attempt = failedAttempt;
        this.status = Status.RETRY_SCHEDULED;
        this.error = error;
        this.nextAttemptAt = retryAt;
        this.leaseExpiresAt = null;
    }

    public void markDead(int failedAttempt, String error, Instant at) {
        this.attempt = failedAttempt;
        this.status = Status.DEAD;
        this.error = error;
        this.finishedAt = at;
        this.nextAttemptAt = null;
        this.leaseExpiresAt = null;
    }

    public void requeue() {
        this.status = Status.QUEUED;
        this.nextAttemptAt = null;
    }

    /** Operator replay of a DEAD run: grants a fresh attempt immediately. */
    public void replay(Instant at) {
        if (status != Status.DEAD) throw new IllegalStateException("Only DEAD runs can be replayed");
        this.status = Status.RETRY_SCHEDULED;
        this.nextAttemptAt = at;
        this.finishedAt = null;
    }

    public UUID getId() { return id; }
    public UUID getJobId() { return jobId; }
    public Instant getScheduledFor() { return scheduledFor; }
    public Status getStatus() { return status; }
    public int getAttempt() { return attempt; }
    public String getWorkerId() { return workerId; }
    public String getError() { return error; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getStartedAt() { return startedAt; }
    public Instant getFinishedAt() { return finishedAt; }
    public Instant getNextAttemptAt() { return nextAttemptAt; }
    public Instant getLeaseExpiresAt() { return leaseExpiresAt; }
    public Instant getLastHeartbeatAt() { return lastHeartbeatAt; }
}
