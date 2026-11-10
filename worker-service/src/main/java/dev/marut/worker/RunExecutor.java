package dev.marut.worker;

import dev.marut.scheduler.contracts.RunDispatch;
import dev.marut.scheduler.contracts.RunResult;
import dev.marut.scheduler.contracts.RunResult.Outcome;
import dev.marut.worker.handlers.JobHandler;
import dev.marut.worker.handlers.NonRetryableException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Executes one dispatch: STARTED → (HEARTBEAT…) → SUCCEEDED | FAILED.
 * Independent of Kafka, so it is unit-testable without a broker.
 *
 * The handler runs on its own (virtual) thread so that this thread can enforce the timeout,
 * while a scheduled task keeps the scheduler's lease alive with heartbeats.
 */
@Component
public class RunExecutor {

    private static final Logger log = LoggerFactory.getLogger(RunExecutor.class);
    private static final int DEFAULT_TIMEOUT_S = 300;

    private final Map<String, JobHandler> handlers;
    private final ResultPublisher results;
    private final MeterRegistry metrics;
    private final String workerId;
    private final Duration heartbeatInterval;
    private final ExecutorService handlerThreads = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService heartbeats = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("heartbeat").daemon().factory());

    public RunExecutor(List<JobHandler> handlers, ResultPublisher results, MeterRegistry metrics,
                       @Value("${worker.id}") String workerId,
                       @Value("${worker.heartbeat-interval:10s}") Duration heartbeatInterval) {
        this.handlers = handlers.stream().collect(Collectors.toMap(JobHandler::name, Function.identity()));
        this.results = results;
        this.metrics = metrics;
        this.workerId = workerId;
        this.heartbeatInterval = heartbeatInterval;
    }

    public void execute(RunDispatch run) {
        JobHandler handler = handlers.get(run.handler());
        if (handler == null) {
            // Not retryable: no amount of retrying will make an unknown handler appear.
            results.publish(RunResult.failed(run, workerId, "Unknown handler: " + run.handler(), false));
            return;
        }

        results.publish(RunResult.of(run, Outcome.STARTED, workerId));
        ScheduledFuture<?> beat = heartbeats.scheduleAtFixedRate(() -> heartbeat(run),
                heartbeatInterval.toMillis(), heartbeatInterval.toMillis(), TimeUnit.MILLISECONDS);
        Timer.Sample sample = Timer.start(metrics);
        String outcome = "succeeded";
        Future<?> work = handlerThreads.submit(() -> { handler.handle(run); return null; });
        int timeout = run.timeoutSeconds() > 0 ? run.timeoutSeconds() : DEFAULT_TIMEOUT_S;
        try {
            work.get(timeout, TimeUnit.SECONDS);
            results.publish(RunResult.of(run, Outcome.SUCCEEDED, workerId));
        } catch (TimeoutException e) {
            work.cancel(true); // interrupts the handler; well-behaved handlers stop promptly
            outcome = "timeout";
            results.publish(RunResult.failed(run, workerId, "Timed out after " + timeout + "s", true));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            boolean retryable = !(cause instanceof NonRetryableException);
            outcome = retryable ? "failed" : "failed_permanent";
            log.warn("Run {} attempt {} failed (retryable={}): {}", run.runId(), run.attempt(), retryable, cause.getMessage());
            results.publish(RunResult.failed(run, workerId, describe(cause), retryable));
        } catch (InterruptedException e) {
            // Worker is shutting down. Report nothing: the offset isn't committed, so the
            // dispatch is redelivered, and the scheduler's lease reaper covers the gap.
            work.cancel(true);
            Thread.currentThread().interrupt();
            outcome = "interrupted";
        } finally {
            beat.cancel(false);
            sample.stop(metrics.timer("worker.run.duration", "handler", run.handler(), "outcome", outcome));
        }
    }

    private void heartbeat(RunDispatch run) {
        try {
            results.publish(RunResult.of(run, Outcome.HEARTBEAT, workerId));
        } catch (Exception e) {
            // A missed beat is tolerated (lease = 3 intervals). Never let it kill the scheduler thread.
            log.warn("Heartbeat for run {} failed: {}", run.runId(), e.getMessage());
        }
    }

    private static String describe(Throwable t) {
        String msg = t.getClass().getSimpleName() + ": " + t.getMessage();
        return msg.length() > 2000 ? msg.substring(0, 2000) : msg;
    }

    @PreDestroy
    void shutdown() {
        heartbeats.shutdownNow();
        handlerThreads.shutdownNow();
    }
}
