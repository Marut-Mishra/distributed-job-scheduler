package dev.marut.scheduler.web;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Ground-truth numbers for a load-test window, computed from job_runs in SQL rather than from
 * in-memory metrics: they survive restarts and cover every scheduler node, not just this one.
 *
 *   GET /api/v1/stats?since=2026-11-10T11:00:00Z
 */
@RestController
@RequestMapping("/api/v1/stats")
public class StatsController {

    private final JdbcTemplate jdbc;

    public StatsController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping
    public Map<String, Object> stats(@RequestParam Instant since) {
        Timestamp from = Timestamp.from(since);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("since", since);

        Map<String, Long> byStatus = new LinkedHashMap<>();
        jdbc.query("SELECT status, count(*) FROM job_runs WHERE created_at >= ? GROUP BY status ORDER BY status",
                (org.springframework.jdbc.core.RowCallbackHandler) rs -> byStatus.put(rs.getString(1), rs.getLong(2)), from);
        out.put("runsByStatus", byStatus);

        // Pickup latency: scheduled time → a worker started it (claim + outbox + Kafka + consume).
        out.put("pickupLatencyMs", percentiles("started_at", from));
        // End-to-end: scheduled time → SUCCEEDED recorded (includes handler run time).
        out.put("endToEndLatencyMs", percentiles("finished_at", from));

        out.put("throughput", jdbc.queryForMap("""
                SELECT count(*) AS succeeded,
                       round(EXTRACT(EPOCH FROM (max(finished_at) - min(scheduled_for)))::numeric, 1) AS window_seconds,
                       round((count(*) / NULLIF(EXTRACT(EPOCH FROM (max(finished_at) - min(scheduled_for))), 0))::numeric, 1)
                           AS runs_per_second
                FROM job_runs
                WHERE status = 'SUCCEEDED' AND created_at >= ?
                """, from));

        out.put("retries", jdbc.queryForObject(
                "SELECT coalesce(sum(GREATEST(attempt - 1, 0)), 0) FROM job_runs WHERE created_at >= ?", Long.class, from));
        out.put("outboxPending", jdbc.queryForObject(
                "SELECT count(*) FROM outbox WHERE published_at IS NULL", Long.class));
        return out;
    }

    private Map<String, Object> percentiles(String endColumn, Timestamp from) {
        // endColumn is one of two fixed literals above, never user input.
        return jdbc.queryForMap("""
                SELECT count(*) AS samples,
                       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY ms)::numeric, 1) AS p50,
                       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms)::numeric, 1) AS p95,
                       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY ms)::numeric, 1) AS p99,
                       round(max(ms)::numeric, 1) AS max
                FROM (SELECT EXTRACT(EPOCH FROM (%s - scheduled_for)) * 1000 AS ms
                      FROM job_runs
                      WHERE created_at >= ? AND %s IS NOT NULL AND status = 'SUCCEEDED') t
                """.formatted(endColumn, endColumn), from);
    }
}
