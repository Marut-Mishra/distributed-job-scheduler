package dev.marut.scheduler.contracts;

import java.time.Instant;
import java.util.UUID;

/** Published when a run is given up on. Consumers: alerting, an ops dashboard, ticketing. */
public record DeadRun(
        UUID runId,
        UUID jobId,
        String jobName,
        int attempts,
        String lastError,
        boolean retryable,
        Instant at
) { }
