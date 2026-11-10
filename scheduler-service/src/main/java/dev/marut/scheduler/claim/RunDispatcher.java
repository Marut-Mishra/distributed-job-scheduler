package dev.marut.scheduler.claim;

import dev.marut.scheduler.contracts.RunDispatch;
import dev.marut.scheduler.contracts.Topics;
import dev.marut.scheduler.job.Job;
import dev.marut.scheduler.outbox.OutboxWriter;
import dev.marut.scheduler.run.JobRun;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/** Single place that builds dispatch messages, used by first runs and retries alike. */
@Component
public class RunDispatcher {

    private final OutboxWriter outbox;

    public RunDispatcher(OutboxWriter outbox) {
        this.outbox = outbox;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void dispatch(Job job, JobRun run, Instant now) {
        outbox.append(Topics.RUN_DISPATCH, job.getId().toString(), new RunDispatch(
                run.getId(), job.getId(), job.getName(), job.getHandler(), job.getPayload(),
                run.getScheduledFor(), run.nextAttemptNumber(), job.getTimeoutSeconds(), now));
    }
}
