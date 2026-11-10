package dev.marut.scheduler.job;

import dev.marut.scheduler.web.CreateJobRequest;
import jakarta.persistence.EntityNotFoundException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
public class JobService {

    private final JobRepository jobs;

    public JobService(JobRepository jobs) {
        this.jobs = jobs;
    }

    public record CreateResult(Job job, boolean created) { }

    /**
     * Idempotent create: retrying the same request with the same idempotency key
     * returns the original job instead of scheduling a duplicate.
     */
    @Transactional
    public CreateResult create(CreateJobRequest req) {
        if (req.idempotencyKey() != null) {
            var existing = jobs.findByIdempotencyKey(req.idempotencyKey());
            if (existing.isPresent()) return new CreateResult(existing.get(), false);
        }

        Instant firstRun;
        if (req.scheduleType() == Job.ScheduleType.CRON) {
            if (req.cronExpression() == null) throw new IllegalArgumentException("cronExpression is required for CRON jobs");
            Schedules.validateCron(req.cronExpression());
            firstRun = Schedules.nextCron(req.cronExpression(), Instant.now());
        } else {
            firstRun = req.runAt() != null ? req.runAt() : Instant.now();
        }

        Job job = Job.create(req.name(), req.handler(), req.scheduleType(), req.cronExpression(), req.payload(),
                firstRun, req.maxRetries() == null ? 3 : req.maxRetries(),
                req.timeoutSeconds() == null ? 300 : req.timeoutSeconds(), req.idempotencyKey());
        return new CreateResult(jobs.save(job), true);
    }

    @Transactional(readOnly = true)
    public Job get(UUID id) {
        return jobs.findById(id).orElseThrow(() -> new EntityNotFoundException("Job not found: " + id));
    }

    @Transactional
    public Job pause(UUID id) { Job j = get(id); j.pause(); return j; }

    @Transactional
    public Job resume(UUID id) { Job j = get(id); j.resume(); return j; }
}
