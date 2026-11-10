package dev.marut.scheduler;

import dev.marut.scheduler.claim.DueJobClaimer;
import dev.marut.scheduler.contracts.RunResult;
import dev.marut.scheduler.contracts.RunResult.Outcome;
import dev.marut.scheduler.contracts.Topics;
import dev.marut.scheduler.job.Job;
import dev.marut.scheduler.job.JobRepository;
import dev.marut.scheduler.job.JobService;
import dev.marut.scheduler.outbox.OutboxEvent;
import dev.marut.scheduler.outbox.OutboxRepository;
import dev.marut.scheduler.results.RunResultListener;
import dev.marut.scheduler.retry.RunMaintenance;
import dev.marut.scheduler.run.JobRun;
import dev.marut.scheduler.run.JobRunRepository;
import dev.marut.scheduler.web.CreateJobRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(properties = {
        "scheduler.claim.enabled=false",          // all loops driven manually
        "scheduler.outbox.enabled=false",         // no Kafka in this test
        "scheduler.maintenance.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false"
})
class SchedulerIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired JobService service;
    @Autowired DueJobClaimer claimer;
    @Autowired RunMaintenance maintenance;
    @Autowired JobRepository jobs;
    @Autowired JobRunRepository runs;
    @Autowired OutboxRepository outbox;
    @Autowired RunResultListener results;

    @BeforeEach
    void clean() { outbox.deleteAll(); runs.deleteAll(); jobs.deleteAll(); }

    // ---------- helpers ----------

    private Job oneOff(String name, int maxRetries) {
        return service.create(new CreateJobRequest(name, null, Job.ScheduleType.ONE_OFF, null,
                Instant.now().minusSeconds(1), Map.of(), maxRetries, null, 60)).job();
    }

    private UUID claimSingleRun() {
        assertThat(claimer.claimBatch(Instant.now())).isEqualTo(1);
        return runs.findAll().get(0).getId();
    }

    private void report(UUID runId, int attempt, Outcome outcome) {
        results.apply(new RunResult(runId, attempt, outcome, "w1", null, true, Instant.now()));
    }

    private void reportFailure(UUID runId, int attempt, boolean retryable) {
        results.apply(new RunResult(runId, attempt, Outcome.FAILED, "w1", "boom", retryable, Instant.now()));
    }

    private JobRun run(UUID id) { return runs.findById(id).orElseThrow(); }

    private long outboxOn(String topic) {
        return outbox.findAll().stream().map(OutboxEvent::getTopic).filter(topic::equals).count();
    }

    /** Far enough ahead that any jittered backoff (cap 5m) has elapsed. */
    private static Instant later() { return Instant.now().plus(Duration.ofHours(1)); }

    // ---------- slice 1–2 ----------

    @Test
    void idempotencyKeyReturnsSameJob() {
        var req = new CreateJobRequest("report", null, Job.ScheduleType.ONE_OFF, null, Instant.now(), Map.of(), 3, "key-1", null);
        var first = service.create(req);
        var second = service.create(req);

        assertThat(first.created()).isTrue();
        assertThat(second.created()).isFalse();
        assertThat(second.job().getId()).isEqualTo(first.job().getId());
        assertThat(jobs.count()).isEqualTo(1);
    }

    @Test
    void oneOffJobIsClaimedOnceAndCompleted() {
        var job = oneOff("once", 3);

        assertThat(claimer.claimBatch(Instant.now())).isEqualTo(1);
        assertThat(claimer.claimBatch(Instant.now())).isZero();

        assertThat(jobs.findById(job.getId()).orElseThrow().getStatus()).isEqualTo(Job.Status.COMPLETED);
        assertThat(runs.count()).isEqualTo(1);
    }

    /** The core guarantee: N concurrent claimers never double-claim an occurrence. */
    @Test
    void concurrentClaimersNeverDoubleClaim() throws Exception {
        int jobCount = 200;
        for (int i = 0; i < jobCount; i++) oneOff("j" + i, 3);

        int nodes = 8;
        ExecutorService pool = Executors.newFixedThreadPool(nodes);
        AtomicInteger total = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        var futures = new ArrayList<Future<?>>();
        for (int n = 0; n < nodes; n++) {
            futures.add(pool.submit(() -> {
                start.await();
                int c;
                while ((c = claimer.claimBatch(Instant.now())) > 0) total.addAndGet(c);
                return null;
            }));
        }
        start.countDown();
        for (var f : futures) f.get(30, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(total.get()).isEqualTo(jobCount);
        assertThat(runs.count()).isEqualTo(jobCount);
        assertThat(outboxOn(Topics.RUN_DISPATCH)).isEqualTo(jobCount); // exactly one dispatch per run
    }

    @Test
    void resultTransitionsAreIdempotentAndOrderTolerant() {
        oneOff("r", 3);
        UUID runId = claimSingleRun();

        // Out of order: SUCCEEDED arrives before STARTED, and everything is delivered twice.
        report(runId, 1, Outcome.SUCCEEDED);
        report(runId, 1, Outcome.STARTED);
        report(runId, 1, Outcome.SUCCEEDED);
        reportFailure(runId, 1, true);

        assertThat(run(runId).getStatus()).isEqualTo(JobRun.Status.SUCCEEDED);
        assertThat(run(runId).getError()).isNull();
    }

    // ---------- slice 3: retries, dead-lettering, leases, fencing ----------

    @Test
    void failedRunIsRetriedWithBackoffThenDeadLettered() {
        oneOff("flaky", 1); // 1 retry → at most 2 attempts
        UUID runId = claimSingleRun();

        report(runId, 1, Outcome.STARTED);
        reportFailure(runId, 1, true);
        JobRun afterFirst = run(runId);
        assertThat(afterFirst.getStatus()).isEqualTo(JobRun.Status.RETRY_SCHEDULED);
        assertThat(afterFirst.getNextAttemptAt()).isNotNull();

        // Backoff not yet elapsed → nothing dispatched.
        assertThat(maintenance.dispatchDueRetries(Instant.now().minusSeconds(60))).isZero();

        assertThat(maintenance.dispatchDueRetries(later())).isEqualTo(1);
        assertThat(run(runId).getStatus()).isEqualTo(JobRun.Status.QUEUED);
        assertThat(outboxOn(Topics.RUN_DISPATCH)).isEqualTo(2);

        report(runId, 2, Outcome.STARTED);
        reportFailure(runId, 2, true);

        assertThat(run(runId).getStatus()).isEqualTo(JobRun.Status.DEAD);
        assertThat(run(runId).getAttempt()).isEqualTo(2);
        assertThat(outboxOn(Topics.DEAD_RUNS)).isEqualTo(1);
    }

    @Test
    void nonRetryableFailureGoesStraightToDead() {
        oneOff("bad-input", 5);
        UUID runId = claimSingleRun();

        report(runId, 1, Outcome.STARTED);
        reportFailure(runId, 1, false);

        assertThat(run(runId).getStatus()).isEqualTo(JobRun.Status.DEAD);
        assertThat(outboxOn(Topics.DEAD_RUNS)).isEqualTo(1);
    }

    @Test
    void expiredLeaseIsReapedAndRetried() {
        oneOff("crashy", 3);
        UUID runId = claimSingleRun();
        report(runId, 1, Outcome.STARTED);   // lease = now + 30s, then the worker "crashes"

        assertThat(maintenance.reapExpiredLeases(Instant.now())).isZero(); // lease still valid
        assertThat(maintenance.reapExpiredLeases(later())).isEqualTo(1);

        JobRun reaped = run(runId);
        assertThat(reaped.getStatus()).isEqualTo(JobRun.Status.RETRY_SCHEDULED);
        assertThat(reaped.getError()).contains("Lease expired");
    }

    @Test
    void heartbeatExtendsLease() {
        oneOff("long", 3);
        UUID runId = claimSingleRun();
        report(runId, 1, Outcome.STARTED);
        Instant firstLease = run(runId).getLeaseExpiresAt();

        report(runId, 1, Outcome.HEARTBEAT);

        assertThat(run(runId).getLeaseExpiresAt()).isAfterOrEqualTo(firstLease);
        assertThat(run(runId).getLastHeartbeatAt()).isNotNull();
    }

    /** A zombie worker (lease expired, but it was only paused) must not overwrite the new attempt. */
    @Test
    void zombieWorkerResultsAreFencedOff() {
        oneOff("zombie", 3);
        UUID runId = claimSingleRun();
        report(runId, 1, Outcome.STARTED);

        Instant reapAt = later();
        maintenance.reapExpiredLeases(reapAt);                        // attempt 1 presumed dead
        assertThat(run(runId).getStatus()).isEqualTo(JobRun.Status.RETRY_SCHEDULED);

        // Dispatch well after the jittered backoff (≤ 2s for attempt 1) has elapsed.
        assertThat(maintenance.dispatchDueRetries(reapAt.plus(Duration.ofMinutes(10)))).isEqualTo(1);
        report(runId, 2, Outcome.STARTED);                            // attempt 2 is now in flight

        // The zombie wakes up and reports on attempt 1.
        report(runId, 1, Outcome.SUCCEEDED);
        reportFailure(runId, 1, false);
        report(runId, 1, Outcome.HEARTBEAT);

        JobRun r = run(runId);
        assertThat(r.getStatus()).isEqualTo(JobRun.Status.RUNNING);
        assertThat(r.getAttempt()).isEqualTo(2);

        report(runId, 2, Outcome.SUCCEEDED);
        assertThat(run(runId).getStatus()).isEqualTo(JobRun.Status.SUCCEEDED);
    }
}
