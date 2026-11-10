package dev.marut.worker;

import dev.marut.scheduler.contracts.RunDispatch;
import dev.marut.scheduler.contracts.RunResult;
import dev.marut.scheduler.contracts.RunResult.Outcome;
import dev.marut.worker.handlers.JobHandler;
import dev.marut.worker.handlers.NonRetryableException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class RunExecutorTest {

    private final List<RunResult> published = new CopyOnWriteArrayList<>();

    private RunExecutor executorWith(Duration heartbeat, JobHandler... handlers) {
        return new RunExecutor(List.of(handlers), published::add, new SimpleMeterRegistry(), "test-worker", heartbeat);
    }

    private RunExecutor executorWith(JobHandler... handlers) {
        return executorWith(Duration.ofSeconds(10), handlers);
    }

    interface Body { void run() throws Exception; }

    private static JobHandler handler(String name, Body body) {
        return new JobHandler() {
            public String name() { return name; }
            public void handle(RunDispatch run) throws Exception { body.run(); }
        };
    }

    private static RunDispatch dispatch(String handler, int attempt, int timeoutSeconds) {
        return new RunDispatch(UUID.randomUUID(), UUID.randomUUID(), "job", handler, Map.of(),
                Instant.now(), attempt, timeoutSeconds, Instant.now());
    }

    private List<Outcome> outcomes() {
        return published.stream().map(RunResult::outcome).filter(o -> o != Outcome.HEARTBEAT).toList();
    }

    @Test
    void successPublishesStartedThenSucceeded_withAttemptEchoed() {
        executorWith(handler("ok", () -> { })).execute(dispatch("ok", 3, 5));
        assertThat(outcomes()).containsExactly(Outcome.STARTED, Outcome.SUCCEEDED);
        assertThat(published).allMatch(r -> r.attempt() == 3);
    }

    @Test
    void ordinaryExceptionIsRetryable() {
        executorWith(handler("bad", () -> { throw new IllegalStateException("boom"); })).execute(dispatch("bad", 1, 5));
        RunResult failed = published.get(published.size() - 1);
        assertThat(failed.outcome()).isEqualTo(Outcome.FAILED);
        assertThat(failed.retryable()).isTrue();
        assertThat(failed.error()).contains("boom");
    }

    @Test
    void nonRetryableExceptionIsMarkedPermanent() {
        executorWith(handler("rej", () -> { throw new NonRetryableException("invalid payload"); })).execute(dispatch("rej", 1, 5));
        RunResult failed = published.get(published.size() - 1);
        assertThat(failed.retryable()).isFalse();
    }

    @Test
    void unknownHandlerFailsPermanentlyWithoutStarting() {
        executorWith(handler("ok", () -> { })).execute(dispatch("missing", 1, 5));
        assertThat(outcomes()).containsExactly(Outcome.FAILED);
        assertThat(published.get(0).retryable()).isFalse();
    }

    @Test
    void slowHandlerTimesOutAndIsRetryable() {
        executorWith(handler("slow", () -> Thread.sleep(10_000))).execute(dispatch("slow", 1, 1));
        RunResult failed = published.get(published.size() - 1);
        assertThat(failed.outcome()).isEqualTo(Outcome.FAILED);
        assertThat(failed.retryable()).isTrue();
        assertThat(failed.error()).contains("Timed out");
    }

    @Test
    void longRunningHandlerHeartbeats() {
        executorWith(Duration.ofMillis(100), handler("sleepy", () -> Thread.sleep(550))).execute(dispatch("sleepy", 1, 5));
        long beats = published.stream().filter(r -> r.outcome() == Outcome.HEARTBEAT).count();
        assertThat(beats).isGreaterThanOrEqualTo(3);
        assertThat(outcomes()).containsExactly(Outcome.STARTED, Outcome.SUCCEEDED);
    }
}
