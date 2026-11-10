package dev.marut.scheduler.claim;

import dev.marut.scheduler.job.Job;
import dev.marut.scheduler.job.JobRepository;
import dev.marut.scheduler.job.Schedules;
import dev.marut.scheduler.run.JobRun;
import dev.marut.scheduler.run.JobRunRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Turns due jobs into queued runs, all in ONE transaction:
 *   1. lock due jobs (SKIP LOCKED — safe with N scheduler nodes)
 *   2. insert a QUEUED run per occurrence (unique (job_id, scheduled_for) as a backstop)
 *   3. append a RunDispatch to the outbox (relayed to Kafka by OutboxRelay)
 *   4. advance next_run_at, or complete one-off jobs
 * Either all four happen or none do — no run without a dispatch, no dispatch without a run.
 */
@Component
public class DueJobClaimer {

    private static final Logger log = LoggerFactory.getLogger(DueJobClaimer.class);

    private final JobRepository jobs;
    private final JobRunRepository runs;
    private final RunDispatcher dispatcher;
    private final ClaimProperties props;
    private final Counter claimed;

    public DueJobClaimer(JobRepository jobs, JobRunRepository runs, RunDispatcher dispatcher,
                         ClaimProperties props, MeterRegistry metrics) {
        this.jobs = jobs;
        this.runs = runs;
        this.dispatcher = dispatcher;
        this.props = props;
        this.claimed = Counter.builder("scheduler.jobs.claimed").register(metrics);
    }

    @Transactional
    public int claimBatch(Instant now) {
        List<Job> due = jobs.lockDueJobs(now, props.batchSize());
        for (Job job : due) {
            Instant occurrence = job.getNextRunAt();
            JobRun run = runs.save(JobRun.queued(job.getId(), occurrence));
            dispatcher.dispatch(job, run, now);

            // Compute from the occurrence (not "now") so a late tick doesn't drift the schedule,
            // but skip past missed occurrences rather than replaying a backlog.
            Instant next = Schedules.nextOccurrence(job, occurrence);
            while (next != null && next.isBefore(now)) {
                next = Schedules.nextOccurrence(job, next);
            }
            job.advance(next);
        }
        if (!due.isEmpty()) {
            claimed.increment(due.size());
            log.info("Claimed {} due job(s)", due.size());
        }
        return due.size();
    }
}
