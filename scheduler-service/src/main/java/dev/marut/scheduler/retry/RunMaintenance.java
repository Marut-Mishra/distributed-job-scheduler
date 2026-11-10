package dev.marut.scheduler.retry;

import dev.marut.scheduler.claim.RunDispatcher;
import dev.marut.scheduler.job.Job;
import dev.marut.scheduler.job.JobRepository;
import dev.marut.scheduler.run.JobRun;
import dev.marut.scheduler.run.JobRunRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/** The two background sweeps that make failures self-healing. Both are SKIP LOCKED, so N nodes can run them. */
@Component
public class RunMaintenance {

    static final int BATCH = 100;

    private final JobRunRepository runs;
    private final JobRepository jobs;
    private final RunDispatcher dispatcher;
    private final FailureHandler failures;

    public RunMaintenance(JobRunRepository runs, JobRepository jobs, RunDispatcher dispatcher, FailureHandler failures) {
        this.runs = runs;
        this.jobs = jobs;
        this.dispatcher = dispatcher;
        this.failures = failures;
    }

    /** RETRY_SCHEDULED runs whose backoff has elapsed → QUEUED + new dispatch (attempt + 1). */
    @Transactional
    public int dispatchDueRetries(Instant now) {
        List<JobRun> due = runs.lockRetriesDue(now, BATCH);
        if (due.isEmpty()) return 0;
        Map<java.util.UUID, Job> jobById = jobs.findAllById(due.stream().map(JobRun::getJobId).toList())
                .stream().collect(Collectors.toMap(Job::getId, Function.identity()));
        for (JobRun run : due) {
            run.requeue();
            dispatcher.dispatch(jobById.get(run.getJobId()), run, now);
        }
        return due.size();
    }

    /**
     * RUNNING runs whose worker stopped heartbeating (crashed, partitioned, GC-paused) → treated as a
     * retryable failure. If the "dead" worker later wakes up and reports, its attempt number is stale
     * and the report is fenced off — but any side effect it already made has happened, which is why
     * handlers must be idempotent.
     */
    @Transactional
    public int reapExpiredLeases(Instant now) {
        List<JobRun> expired = runs.lockExpiredLeases(now, BATCH);
        for (JobRun run : expired) {
            failures.apply(run, run.getAttempt(),
                    "Lease expired: no heartbeat from worker " + run.getWorkerId(), true, now);
        }
        return expired.size();
    }
}
