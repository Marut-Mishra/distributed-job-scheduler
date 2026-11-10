package dev.marut.scheduler.run;

import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface JobRunRepository extends JpaRepository<JobRun, UUID> {

    List<JobRun> findByJobIdOrderByCreatedAtDesc(UUID jobId, Pageable page);

    List<JobRun> findByStatusOrderByCreatedAtDesc(JobRun.Status status, Pageable page);

    long countByStatus(JobRun.Status status);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT r FROM JobRun r WHERE r.id = :id")
    Optional<JobRun> lockById(@Param("id") UUID id);

    /*
     * Guarded, fenced transitions. Each UPDATE matches only when the event belongs to the attempt
     * currently in flight, so duplicates, out-of-order events and zombie workers are no-ops.
     * (Failures need retry logic, so they go through FailureHandler with the same fence in Java.)
     */

    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE JobRun r SET r.status = dev.marut.scheduler.run.JobRun.Status.RUNNING,
                   r.attempt = :attempt, r.startedAt = :at, r.workerId = :worker,
                   r.leaseExpiresAt = :leaseUntil, r.lastHeartbeatAt = :at
            WHERE r.id = :id
              AND r.status = dev.marut.scheduler.run.JobRun.Status.QUEUED
              AND r.attempt = :attempt - 1
            """)
    int markRunning(@Param("id") UUID id, @Param("attempt") int attempt, @Param("worker") String worker,
                    @Param("at") Instant at, @Param("leaseUntil") Instant leaseUntil);

    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE JobRun r SET r.leaseExpiresAt = :leaseUntil, r.lastHeartbeatAt = :at
            WHERE r.id = :id
              AND r.status = dev.marut.scheduler.run.JobRun.Status.RUNNING
              AND r.attempt = :attempt
            """)
    int heartbeat(@Param("id") UUID id, @Param("attempt") int attempt,
                  @Param("at") Instant at, @Param("leaseUntil") Instant leaseUntil);

    @Modifying(clearAutomatically = true)
    @Query("""
            UPDATE JobRun r SET r.status = dev.marut.scheduler.run.JobRun.Status.SUCCEEDED,
                   r.attempt = :attempt, r.finishedAt = :at, r.error = NULL,
                   r.workerId = COALESCE(r.workerId, :worker), r.leaseExpiresAt = NULL
            WHERE r.id = :id
              AND ((r.status = dev.marut.scheduler.run.JobRun.Status.RUNNING AND r.attempt = :attempt)
                OR (r.status = dev.marut.scheduler.run.JobRun.Status.QUEUED  AND r.attempt = :attempt - 1))
            """)
    int markSucceeded(@Param("id") UUID id, @Param("attempt") int attempt,
                      @Param("worker") String worker, @Param("at") Instant at);

    @Query(value = """
            SELECT * FROM job_runs
            WHERE status = 'RETRY_SCHEDULED' AND next_attempt_at <= :now
            ORDER BY next_attempt_at
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<JobRun> lockRetriesDue(@Param("now") Instant now, @Param("limit") int limit);

    @Query(value = """
            SELECT * FROM job_runs
            WHERE status = 'RUNNING' AND lease_expires_at < :now
            ORDER BY lease_expires_at
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<JobRun> lockExpiredLeases(@Param("now") Instant now, @Param("limit") int limit);
}
