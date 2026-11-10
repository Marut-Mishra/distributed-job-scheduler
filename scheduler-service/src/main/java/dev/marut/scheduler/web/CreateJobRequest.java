package dev.marut.scheduler.web;

import dev.marut.scheduler.job.Job;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.Map;

public record CreateJobRequest(
        @NotBlank @Size(max = 200) String name,
        @Size(max = 100) String handler,
        @NotNull Job.ScheduleType scheduleType,
        String cronExpression,
        Instant runAt,
        Map<String, Object> payload,
        @Min(0) @Max(20) Integer maxRetries,
        @Size(max = 120) String idempotencyKey,
        // Capped below the worker's Kafka max.poll.interval.ms (600s): a handler that runs longer
        // than that gets its partition revoked and the run redelivered to another worker.
        @Min(1) @Max(540) Integer timeoutSeconds
) { }
