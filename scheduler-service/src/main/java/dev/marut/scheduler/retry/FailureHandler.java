package dev.marut.scheduler.retry;

import dev.marut.scheduler.contracts.DeadRun;
import dev.marut.scheduler.contracts.Topics;
import dev.marut.scheduler.job.Job;
import dev.marut.scheduler.job.JobRepository;
import dev.marut.scheduler.outbox.OutboxWriter;
import dev.marut.scheduler.run.JobRun;
import dev.marut.scheduler.run.JobRunRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Decides what a failure means: retry later, or give up (DEAD + dead-letter event).
 * Called for worker-reported failures and for expired leases (crashed workers).
 */
@Component
public class FailureHandler {

    private static final Logger log = LoggerFactory.getLogger(FailureHandler.class);

    private final JobRunRepository runs;
    private final JobRepository jobs;
    private final OutboxWriter outbox;
    private final Backoff backoff;
    private final MeterRegistry metrics;

    public FailureHandler(JobRunRepository runs, JobRepository jobs, OutboxWriter outbox,
                          Backoff backoff, MeterRegistry metrics) {
        this.runs = runs;
        this.jobs = jobs;
        this.outbox = outbox;
        this.backoff = backoff;
        this.metrics = metrics;
    }

    /** Worker-reported failure. Fenced: ignored unless it belongs to the attempt in flight. */
    @Transactional
    public void onReportedFailure(UUID runId, int attempt, String error, boolean retryable, Instant now) {
        runs.lockById(runId).ifPresent(run -> {
            if (!run.isCurrent(attempt)) {
                log.debug("Ignored stale FAILED for run {} attempt {} (current: {} {})",
                        runId, attempt, run.getStatus(), run.getAttempt());
                return;
            }
            apply(run, attempt, error, retryable, now);
        });
    }

    /** Caller already holds the row lock (lease reaper). */
    @Transactional(propagation = Propagation.MANDATORY)
    public void apply(JobRun run, int failedAttempt, String error, boolean retryable, Instant now) {
        Job job = jobs.findById(run.getJobId()).orElseThrow();
        boolean attemptsLeft = failedAttempt <= job.getMaxRetries(); // maxRetries=3 → up to 4 attempts

        if (retryable && attemptsLeft) {
            Instant retryAt = now.plus(backoff.delayFor(failedAttempt));
            run.scheduleRetry(failedAttempt, error, retryAt);
            metrics.counter("scheduler.runs.retried").increment();
            log.info("Run {} attempt {} failed, retry at {}: {}", run.getId(), failedAttempt, retryAt, error);
        } else {
            run.markDead(failedAttempt, error, now);
            outbox.append(Topics.DEAD_RUNS, run.getId().toString(), new DeadRun(
                    run.getId(), job.getId(), job.getName(), failedAttempt, error, retryable, now));
            metrics.counter("scheduler.runs.dead", "retryable", String.valueOf(retryable)).increment();
            log.warn("Run {} is DEAD after {} attempt(s): {}", run.getId(), failedAttempt, error);
        }
    }
}
