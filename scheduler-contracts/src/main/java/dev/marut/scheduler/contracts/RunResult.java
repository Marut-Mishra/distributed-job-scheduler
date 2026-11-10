package dev.marut.scheduler.contracts;

import java.time.Instant;
import java.util.UUID;

/**
 * Worker → scheduler status event, applied as a fenced state transition.
 *
 * @param attempt   the attempt this event belongs to; events for stale attempts are dropped
 * @param retryable for FAILED only: false means retrying cannot help (bad input, unknown handler)
 */
public record RunResult(
        UUID runId,
        int attempt,
        Outcome outcome,
        String workerId,
        String error,
        boolean retryable,
        Instant at
) {
    public enum Outcome { STARTED, HEARTBEAT, SUCCEEDED, FAILED }

    public static RunResult of(RunDispatch d, Outcome outcome, String workerId) {
        return new RunResult(d.runId(), d.attempt(), outcome, workerId, null, true, Instant.now());
    }

    public static RunResult failed(RunDispatch d, String workerId, String error, boolean retryable) {
        return new RunResult(d.runId(), d.attempt(), Outcome.FAILED, workerId, error, retryable, Instant.now());
    }
}
