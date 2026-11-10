package dev.marut.scheduler.job;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface JobRepository extends JpaRepository<Job, UUID> {

    Optional<Job> findByIdempotencyKey(String idempotencyKey);

    /**
     * Claims due jobs for this node. FOR UPDATE SKIP LOCKED lets several scheduler
     * nodes poll concurrently: each row is locked by exactly one transaction and the
     * others skip it instead of blocking, so no occurrence is claimed twice.
     */
    @Query(value = """
            SELECT * FROM jobs
            WHERE status = 'ACTIVE' AND next_run_at <= :now
            ORDER BY next_run_at
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<Job> lockDueJobs(@Param("now") Instant now, @Param("limit") int limit);
}
