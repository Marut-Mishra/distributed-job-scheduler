package dev.marut.scheduler.contracts;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * "Execute attempt N of this run." Delivered at-least-once, so workers may see the same
 * (runId, attempt) twice; handlers must be idempotent (key side effects on runId).
 *
 * @param attempt        1-based attempt number; echoed back on every result as a fencing token
 * @param timeoutSeconds the worker cancels the handler after this long and reports a retryable failure
 */
public record RunDispatch(
        UUID runId,
        UUID jobId,
        String jobName,
        String handler,
        Map<String, Object> payload,
        Instant scheduledFor,
        int attempt,
        int timeoutSeconds,
        Instant dispatchedAt
) { }
