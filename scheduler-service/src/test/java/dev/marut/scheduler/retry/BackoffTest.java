package dev.marut.scheduler.retry;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class BackoffTest {

    private final Backoff backoff = new Backoff(Duration.ofSeconds(2), Duration.ofMinutes(5));

    @Test
    void ceilingDoublesPerAttemptUntilCap() {
        assertThat(backoff.ceilingFor(1)).isEqualTo(Duration.ofSeconds(2));
        assertThat(backoff.ceilingFor(2)).isEqualTo(Duration.ofSeconds(4));
        assertThat(backoff.ceilingFor(5)).isEqualTo(Duration.ofSeconds(32));
        assertThat(backoff.ceilingFor(20)).isEqualTo(Duration.ofMinutes(5));
        assertThat(backoff.ceilingFor(1000)).isEqualTo(Duration.ofMinutes(5)); // no overflow
    }

    @Test
    void fullJitterSpansZeroToCeiling() {
        assertThat(backoff.delayFor(3, () -> 0.0)).isEqualTo(Duration.ZERO);
        assertThat(backoff.delayFor(3, () -> 0.5)).isEqualTo(Duration.ofSeconds(4));
        assertThat(backoff.delayFor(3, () -> 0.999)).isLessThan(Duration.ofSeconds(8));
    }

    @Test
    void defaultsApplyWhenUnconfigured() {
        Backoff d = new Backoff(null, null);
        assertThat(d.base()).isEqualTo(Duration.ofSeconds(2));
        assertThat(d.cap()).isEqualTo(Duration.ofMinutes(5));
    }
}
