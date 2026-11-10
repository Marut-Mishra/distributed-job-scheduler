package dev.marut.scheduler.job;

import org.springframework.scheduling.support.CronExpression;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/** Pure schedule arithmetic — no Spring context, trivially unit-testable. */
public final class Schedules {

    private Schedules() { }

    public static void validateCron(String cron) {
        if (!CronExpression.isValidExpression(cron)) {
            throw new IllegalArgumentException("Invalid cron expression: " + cron);
        }
    }

    /** Next fire time strictly after {@code after}, evaluated in UTC. */
    public static Instant nextCron(String cron, Instant after) {
        ZonedDateTime next = CronExpression.parse(cron).next(after.atZone(ZoneOffset.UTC));
        return next == null ? null : next.toInstant();
    }

    /** Next occurrence for a job after it fired at {@code firedAt}; null means the job is finished. */
    public static Instant nextOccurrence(Job job, Instant firedAt) {
        return switch (job.getScheduleType()) {
            case ONE_OFF -> null;
            case CRON -> nextCron(job.getCronExpression(), firedAt);
        };
    }
}
