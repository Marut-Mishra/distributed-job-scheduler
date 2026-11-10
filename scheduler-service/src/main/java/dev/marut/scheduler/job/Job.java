package dev.marut.scheduler.job;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "jobs")
public class Job {

    public enum ScheduleType { ONE_OFF, CRON }
    public enum Status { ACTIVE, PAUSED, COMPLETED }

    @Id
    private UUID id;

    @Column(nullable = false)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(name = "schedule_type", nullable = false)
    private ScheduleType scheduleType;

    @Column(name = "cron_expression")
    private String cronExpression;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private Map<String, Object> payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status;

    @Column(name = "next_run_at")
    private Instant nextRunAt;

    @Column(nullable = false)
    private String handler;

    @Column(name = "max_retries", nullable = false)
    private int maxRetries;

    @Column(name = "timeout_seconds", nullable = false)
    private int timeoutSeconds;

    @Column(name = "idempotency_key", unique = true)
    private String idempotencyKey;

    @Version
    private long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Job() { }

    public static Job create(String name, String handler, ScheduleType type, String cron, Map<String, Object> payload,
                             Instant firstRunAt, int maxRetries, int timeoutSeconds, String idempotencyKey) {
        Job j = new Job();
        j.id = UUID.randomUUID();
        j.name = name;
        j.handler = handler == null ? "log" : handler;
        j.scheduleType = type;
        j.cronExpression = cron;
        j.payload = payload == null ? Map.of() : payload;
        j.status = Status.ACTIVE;
        j.nextRunAt = firstRunAt;
        j.maxRetries = maxRetries;
        j.timeoutSeconds = timeoutSeconds;
        j.idempotencyKey = idempotencyKey;
        j.createdAt = Instant.now();
        j.updatedAt = j.createdAt;
        return j;
    }

    /** Advance to the next occurrence, or complete the job if there is none. */
    public void advance(Instant next) {
        if (next == null) {
            this.status = Status.COMPLETED;
            this.nextRunAt = null;
        } else {
            this.nextRunAt = next;
        }
        this.updatedAt = Instant.now();
    }

    public void pause()  { this.status = Status.PAUSED; this.updatedAt = Instant.now(); }
    public void resume() { if (status == Status.PAUSED) { this.status = Status.ACTIVE; this.updatedAt = Instant.now(); } }

    public UUID getId() { return id; }
    public String getName() { return name; }
    public String getHandler() { return handler; }
    public ScheduleType getScheduleType() { return scheduleType; }
    public String getCronExpression() { return cronExpression; }
    public Map<String, Object> getPayload() { return payload; }
    public Status getStatus() { return status; }
    public Instant getNextRunAt() { return nextRunAt; }
    public int getMaxRetries() { return maxRetries; }
    public int getTimeoutSeconds() { return timeoutSeconds; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public Instant getCreatedAt() { return createdAt; }
}
