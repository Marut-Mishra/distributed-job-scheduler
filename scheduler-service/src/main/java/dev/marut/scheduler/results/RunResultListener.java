package dev.marut.scheduler.results;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.marut.scheduler.contracts.RunResult;
import dev.marut.scheduler.contracts.Topics;
import dev.marut.scheduler.retry.FailureHandler;
import dev.marut.scheduler.run.JobRunRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;

@Component
public class RunResultListener {

    private static final Logger log = LoggerFactory.getLogger(RunResultListener.class);

    private final JobRunRepository runs;
    private final FailureHandler failures;
    private final ObjectMapper json;
    private final Duration lease;

    public RunResultListener(JobRunRepository runs, FailureHandler failures, ObjectMapper json,
                             @Value("${scheduler.lease.duration:30s}") Duration lease) {
        this.runs = runs;
        this.failures = failures;
        this.json = json;
        this.lease = lease;
    }

    @KafkaListener(topics = Topics.RUN_RESULTS, groupId = "scheduler-results")
    public void onMessage(String message) throws Exception {
        apply(json.readValue(message, RunResult.class));
    }

    /**
     * Idempotent and fenced: replaying any result any number of times is safe, and results for
     * stale attempts are dropped. Leases use the scheduler's clock, never the worker's (clock skew).
     */
    @Transactional
    public void apply(RunResult r) {
        Instant now = Instant.now();
        int updated = switch (r.outcome()) {
            case STARTED -> runs.markRunning(r.runId(), r.attempt(), r.workerId(), now, now.plus(lease));
            case HEARTBEAT -> runs.heartbeat(r.runId(), r.attempt(), now, now.plus(lease));
            case SUCCEEDED -> runs.markSucceeded(r.runId(), r.attempt(), r.workerId(), now);
            case FAILED -> {
                failures.onReportedFailure(r.runId(), r.attempt(), r.error(), r.retryable(), now);
                yield 1;
            }
        };
        if (updated == 0) {
            log.debug("Ignored {} for run {} attempt {} (duplicate, out of order, or stale)",
                    r.outcome(), r.runId(), r.attempt());
        }
    }
}
