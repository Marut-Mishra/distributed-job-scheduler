package dev.marut.scheduler.retry;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.function.DoubleSupplier;

/**
 * Exponential backoff with full jitter: delay = random(0, min(cap, base * 2^(attempt-1))).
 * Jitter matters: without it, every run that failed because of the same outage retries at
 * the same instant and knocks the recovering dependency over again (thundering herd).
 */
@ConfigurationProperties(prefix = "scheduler.retry.backoff")
public record Backoff(Duration base, Duration cap) {

    public Backoff {
        if (base == null) base = Duration.ofSeconds(2);
        if (cap == null) cap = Duration.ofMinutes(5);
    }

    public Duration delayFor(int failedAttempt) {
        return delayFor(failedAttempt, Math::random);
    }

    /** Ceiling before jitter; exposed for tests and for documenting worst-case latency. */
    public Duration ceilingFor(int failedAttempt) {
        int exp = Math.min(Math.max(failedAttempt - 1, 0), 30); // clamp: 2^30 * base overflows nothing useful
        long ceiling = base.toMillis() * (1L << exp);
        return Duration.ofMillis(Math.min(cap.toMillis(), ceiling));
    }

    Duration delayFor(int failedAttempt, DoubleSupplier random) {
        return Duration.ofMillis((long) (ceilingFor(failedAttempt).toMillis() * random.getAsDouble()));
    }
}
