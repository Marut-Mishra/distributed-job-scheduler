package dev.marut.scheduler.web;

import dev.marut.scheduler.job.Job;
import dev.marut.scheduler.run.JobRun;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record JobResponse(UUID id, String name, String handler, Job.ScheduleType scheduleType, String cronExpression,
                          Job.Status status, Instant nextRunAt, int maxRetries, int timeoutSeconds,
                          Map<String, Object> payload, Instant createdAt) {

    public static JobResponse of(Job j) {
        return new JobResponse(j.getId(), j.getName(), j.getHandler(), j.getScheduleType(), j.getCronExpression(),
                j.getStatus(), j.getNextRunAt(), j.getMaxRetries(), j.getTimeoutSeconds(), j.getPayload(), j.getCreatedAt());
    }

    public record Run(UUID id, Instant scheduledFor, JobRun.Status status, int attempt, String workerId,
                      Instant nextAttemptAt, String error) {
        public static Run of(JobRun r) {
            return new Run(r.getId(), r.getScheduledFor(), r.getStatus(), r.getAttempt(), r.getWorkerId(),
                    r.getNextAttemptAt(), r.getError());
        }
    }
}
